// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <gtest/gtest.h>

#include <limits>

#include "common/exception.h"
#include "core/column/column_array.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_bitmap.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"
#include "testutil/column_helper.h"

namespace doris {
namespace {
using Arguments = std::vector<ColumnWithTypeAndName>;

template <typename T>
ColumnWithTypeAndName argument(typename T::FieldType value) {
    return {ColumnHelper::create_column<T>({value}), std::make_shared<T>(), ""};
}

ColumnWithTypeAndName quantiles(const std::vector<double>& values) {
    auto nested = ColumnHelper::create_nullable_column<DataTypeFloat64>(
            values, std::vector<UInt8>(values.size(), 0));
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->insert_value(values.size());
    auto type = std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeFloat64>()));
    return {ColumnArray::create(nested, std::move(offsets)), type, ""};
}

template <typename Operation>
void expect_incompatible(Operation&& operation) {
    try {
        operation();
        FAIL() << "Expected incompatible state parameters to be rejected";
    } catch (const Exception& e) {
        EXPECT_EQ(e.code(), ErrorCode::INVALID_ARGUMENT);
        EXPECT_NE(e.to_string().find("incompatible"), std::string::npos);
    }
}

class StateParameterChecks {
public:
    explicit StateParameterChecks(AggregateFunctionPtr function) : _function(std::move(function)) {}

    ~StateParameterChecks() {
        for (auto* place : _places) {
            _function->destroy(place);
        }
    }

    void check_mismatch(const Arguments& first, const Arguments& second) {
        auto* destination = create(first);
        auto* source = create(second);
        expect_incompatible([&] { _function->merge(destination, source, _arena); });
        auto serialized = serialize(source);
        expect_incompatible([&] {
            _function->deserialize_and_merge_from_column(destination, *serialized, _arena);
        });
        auto* fresh_direct_destination = create();
        _function->merge(fresh_direct_destination, create(first), _arena);
        expect_incompatible([&] { _function->merge(fresh_direct_destination, source, _arena); });
        auto* fresh_destination = create();
        auto serialized_first = serialize(create(first));
        _function->deserialize_and_merge_from_column(fresh_destination, *serialized_first, _arena);
        expect_incompatible([&] {
            _function->deserialize_and_merge_from_column(fresh_destination, *serialized, _arena);
        });
    }

    void check_merge_result(const Arguments& initial, const Arguments& incoming, bool serialized) {
        auto* destination = create();
        auto* source = create();
        auto* expected = create();
        if (!initial.empty()) {
            add(destination, initial);
            add(expected, initial);
        }
        if (!incoming.empty()) {
            add(source, incoming);
            add(expected, incoming);
        }
        if (serialized) {
            auto column = serialize(source);
            EXPECT_NO_THROW(
                    _function->deserialize_and_merge_from_column(destination, *column, _arena));
        } else {
            EXPECT_NO_THROW(_function->merge(destination, source, _arena));
        }
        EXPECT_TRUE(ColumnHelper::column_equal(result(destination), result(expected)));
    }

    void check_empty_and_reset(const Arguments& first, const Arguments& second) {
        check_merge_result({}, first, false);
        check_merge_result({}, first, true);
        check_merge_result(first, {}, false);
        check_merge_result(first, {}, true);
        auto* destination = create(first);
        auto* source = create();
        EXPECT_NO_THROW(_function->merge(destination, source, _arena));
        _function->reset(destination);
        auto* configured = create(second);
        auto serialized_reset = serialize(destination);
        _function->deserialize_and_merge_from_column(configured, *serialized_reset, _arena);
        EXPECT_TRUE(ColumnHelper::column_equal(result(configured), result(create(second))));
        auto serialized = serialize(create(second));
        EXPECT_NO_THROW(
                _function->deserialize_and_merge_from_column(destination, *serialized, _arena));
        EXPECT_TRUE(ColumnHelper::column_equal(result(destination), result(create(second))));
    }

    void check_mismatch_and_reset(const Arguments& first, const Arguments& second) {
        check_mismatch(first, second);
        auto* reset_state = create(first);
        _function->reset(reset_state);
        auto* configured = create(second);
        auto serialized = serialize(reset_state);
        EXPECT_NO_THROW(
                _function->deserialize_and_merge_from_column(configured, *serialized, _arena));
        EXPECT_TRUE(ColumnHelper::column_equal(result(configured), result(create(second))));
        EXPECT_NO_THROW(_function->merge(reset_state, create(second), _arena));
        EXPECT_TRUE(ColumnHelper::column_equal(result(reset_state), result(create(second))));
    }

    void check_configured_empty_payload(const Arguments& arguments) {
        // TopN with zero capacity keeps counters in memory but serializes no counters.
        // A decoded state must preserve configuration without changing compatible counters.
        auto serialized = serialize(create(arguments));
        auto* configured_empty = create();
        _function->deserialize_and_merge_from_column(configured_empty, *serialized, _arena);
        auto* populated = create(arguments);
        EXPECT_NO_THROW(
                _function->deserialize_and_merge_from_column(populated, *serialized, _arena));
        EXPECT_TRUE(ColumnHelper::column_equal(result(populated), result(create(arguments))));
        EXPECT_NO_THROW(_function->merge(configured_empty, create(arguments), _arena));
        EXPECT_TRUE(
                ColumnHelper::column_equal(result(configured_empty), result(create(arguments))));
    }

    void check_compatible(const Arguments& arguments) {
        auto* destination = create();
        auto serialized = serialize(create(arguments));
        EXPECT_NO_THROW(
                _function->deserialize_and_merge_from_column(destination, *serialized, _arena));
        EXPECT_NO_THROW(
                _function->deserialize_and_merge_from_column(destination, *serialized, _arena));
        auto* expected = create(arguments);
        add(expected, arguments);
        EXPECT_TRUE(ColumnHelper::column_equal(result(destination), result(expected)));
    }

    void check_invalid_outputs(const Arguments& arguments, const std::string& message) {
        auto* state = create(arguments);
        for (bool serialize_state : {false, true}) {
            SCOPED_TRACE(serialize_state);
            try {
                if (serialize_state) {
                    serialize(state);
                } else {
                    result(state);
                }
                FAIL() << "Expected invalid state parameters to be rejected";
            } catch (const Exception& e) {
                EXPECT_EQ(e.code(), ErrorCode::INVALID_ARGUMENT);
                EXPECT_NE(e.to_string().find(message), std::string::npos);
            }
        }
    }

private:
    AggregateDataPtr create() {
        auto* place = reinterpret_cast<AggregateDataPtr>(_arena.alloc(_function->size_of_data()));
        _function->create(place);
        _places.push_back(place);
        return place;
    }

    AggregateDataPtr create(const Arguments& arguments) {
        auto* place = create();
        add(place, arguments);
        return place;
    }

    void add(AggregateDataPtr place, const Arguments& arguments) {
        std::vector<const IColumn*> columns;
        for (const auto& arg : arguments) {
            columns.push_back(arg.column.get());
        }
        _function->add(place, columns.data(), 0, _arena);
    }

    MutableColumnPtr serialize(AggregateDataPtr place) {
        auto column = _function->create_serialize_column();
        _function->serialize_without_key_to_column(place, *column);
        return column;
    }

    ColumnPtr result(AggregateDataPtr place) {
        auto column = _function->get_return_type()->create_column();
        _function->insert_result_into(place, *column);
        return column;
    }

    AggregateFunctionPtr _function;
    Arena _arena;
    std::vector<AggregateDataPtr> _places;
};

void check_parameters(const std::string& name, const Arguments& first, const Arguments& second) {
    SCOPED_TRACE(name);
    DataTypes types;
    for (const auto& arg : first) {
        types.push_back(arg.type);
    }
    auto function = AggregateFunctionSimpleFactory::instance().get(
            name, types, nullptr, false, BeExecVersionManager::get_newest_version());
    ASSERT_NE(function, nullptr);
    function->set_version(BeExecVersionManager::get_newest_version());
    StateParameterChecks checks(function);
    for (bool reverse : {false, true}) {
        SCOPED_TRACE(reverse);
        const auto& lhs_args = reverse ? second : first;
        const auto& rhs_args = reverse ? first : second;
        checks.check_mismatch(lhs_args, rhs_args);
        checks.check_empty_and_reset(lhs_args, rhs_args);
        checks.check_compatible(lhs_args);
    }
}

void check_compatible_states(const std::string& name, const Arguments& first,
                             const Arguments& second) {
    SCOPED_TRACE(name);
    DataTypes types;
    for (const auto& arg : first) {
        types.push_back(arg.type);
    }
    auto function = AggregateFunctionSimpleFactory::instance().get(
            name, types, nullptr, false, BeExecVersionManager::get_newest_version());
    ASSERT_NE(function, nullptr);
    function->set_version(BeExecVersionManager::get_newest_version());
    StateParameterChecks checks(function);
    for (const auto& initial : {Arguments {}, first, second}) {
        for (const auto& incoming : {Arguments {}, first, second}) {
            for (bool serialized : {false, true}) {
                checks.check_merge_result(initial, incoming, serialized);
            }
        }
    }
}
} // namespace

TEST(AggregateStateParametersTest, TopN) {
    const auto value = argument<DataTypeString>("a");
    check_parameters("topn", {value, argument<DataTypeInt32>(1)},
                     {value, argument<DataTypeInt32>(3)});
    for (const auto& name : {"topn", "topn_array", "topn_weighted"}) {
        Arguments args {value};
        if (std::string(name) == "topn_weighted") {
            args.push_back(argument<DataTypeInt64>(1));
        }
        auto first = args;
        auto second = args;
        first.insert(first.end(), {argument<DataTypeInt32>(3), argument<DataTypeInt32>(2)});
        second.insert(second.end(), {argument<DataTypeInt32>(3), argument<DataTypeInt32>(5)});
        check_parameters(name, first, second);
        // Equal capacities do not make different N values compatible.
        second = args;
        second.insert(second.end(), {argument<DataTypeInt32>(1), argument<DataTypeInt32>(6)});
        check_parameters(name, first, second);
    }
    check_parameters("topn_array", {argument<DataTypeInt32>(1), argument<DataTypeInt32>(1)},
                     {argument<DataTypeInt32>(1), argument<DataTypeInt32>(3)});
}

TEST(AggregateStateParametersTest, Histogram) {
    auto value = argument<DataTypeInt32>(7);
    check_parameters("histogram", {value, argument<DataTypeInt32>(1)},
                     {value, argument<DataTypeInt32>(3)});
}

TEST(AggregateStateParametersTest, TopNZeroCapacityParameters) {
    for (const auto& name : {"topn", "topn_array", "topn_weighted"}) {
        SCOPED_TRACE(name);
        Arguments zero_capacity {argument<DataTypeString>("a")};
        if (std::string(name) == "topn_weighted") {
            zero_capacity.push_back(argument<DataTypeInt64>(1));
        }
        zero_capacity.push_back(argument<DataTypeInt32>(1));
        zero_capacity.push_back(argument<DataTypeInt32>(0));
        auto positive_capacity = zero_capacity;
        positive_capacity.back() = argument<DataTypeInt32>(2);
        DataTypes types;
        for (const auto& arg : zero_capacity) {
            types.push_back(arg.type);
        }
        auto function = AggregateFunctionSimpleFactory::instance().get(
                name, types, nullptr, false, BeExecVersionManager::get_newest_version());
        ASSERT_NE(function, nullptr);
        StateParameterChecks checks(function);
        // A zero capacity serializes no retained elements but still establishes N/capacity.
        checks.check_mismatch(zero_capacity, positive_capacity);
        checks.check_mismatch(positive_capacity, zero_capacity);
        checks.check_mismatch_and_reset(zero_capacity, positive_capacity);
        checks.check_merge_result({}, zero_capacity, false);
        checks.check_merge_result(zero_capacity, {}, false);
        checks.check_merge_result(zero_capacity, {}, true);
        checks.check_configured_empty_payload(zero_capacity);
    }
}

TEST(AggregateStateParametersTest, Percentiles) {
    auto value = argument<DataTypeFloat64>(7);
    for (const auto& name :
         {"percentile", "percentile_v2", "percentile_approx", "percentile_reservoir"}) {
        check_parameters(name, {value, argument<DataTypeFloat64>(0)},
                         {value, argument<DataTypeFloat64>(1)});
    }
    for (const auto& name :
         {"percentile_array", "percentile_array_v2", "percentile_approx_array"}) {
        check_parameters(name, {value, quantiles({0.25})}, {value, quantiles({0.75})});
        check_parameters(name, {value, quantiles({0.25})}, {value, quantiles({0.25, 0.75})});
        check_parameters(name, {value, quantiles({})}, {value, quantiles({0.25})});
    }
    check_parameters("percentile_approx",
                     {value, argument<DataTypeFloat64>(0.5), argument<DataTypeFloat64>(2048)},
                     {value, argument<DataTypeFloat64>(0.5), argument<DataTypeFloat64>(4096)});
    check_parameters("percentile_approx_weighted",
                     {value, argument<DataTypeFloat64>(1), argument<DataTypeFloat64>(0.25)},
                     {value, argument<DataTypeFloat64>(1), argument<DataTypeFloat64>(0.75)});
    check_parameters("percentile_approx_weighted",
                     {value, argument<DataTypeFloat64>(1), argument<DataTypeFloat64>(0.5),
                      argument<DataTypeFloat64>(2048)},
                     {value, argument<DataTypeFloat64>(1), argument<DataTypeFloat64>(0.5),
                      argument<DataTypeFloat64>(4096)});
}

TEST(AggregateStateParametersTest, PercentileConfiguredEmptyParameters) {
    const auto nan = argument<DataTypeFloat64>(std::numeric_limits<double>::quiet_NaN());
    const auto value = argument<DataTypeFloat64>(7);
    for (const auto& name : {"percentile_v2", "percentile_approx", "percentile_reservoir"}) {
        for (double level : {0.0, 0.25, 1.0}) {
            const auto quantile = argument<DataTypeFloat64>(level);
            for (const auto& sample : {nan, value}) {
                check_parameters(name, {nan, quantile}, {sample, argument<DataTypeFloat64>(0.75)});
            }
            check_compatible_states(name, {nan, quantile}, {value, quantile});
        }
    }
    for (const auto& name : {"percentile_array_v2", "percentile_approx_array"}) {
        check_parameters(name, {nan, quantiles({0.25})}, {value, quantiles({0.75})});
        check_parameters(name, {nan, quantiles({0.25})}, {nan, quantiles({0.75})});
        check_compatible_states(name, {nan, quantiles({0.25})}, {value, quantiles({0.25})});
    }
    check_parameters("percentile_approx_weighted",
                     {value, argument<DataTypeFloat64>(0), argument<DataTypeFloat64>(0.25)},
                     {value, argument<DataTypeFloat64>(1), argument<DataTypeFloat64>(0.75)});
    check_compatible_states("percentile_approx_weighted",
                            {value, argument<DataTypeFloat64>(0), argument<DataTypeFloat64>(0.25)},
                            {value, argument<DataTypeFloat64>(1), argument<DataTypeFloat64>(0.25)});
}

TEST(AggregateStateParametersTest, CollectAndConcat) {
    for (const auto& name : {"collect_list", "collect_set"}) {
        for (const auto& value : {argument<DataTypeInt32>(7), argument<DataTypeString>("a")}) {
            check_parameters(name, {value, argument<DataTypeInt32>(1)},
                             {value, argument<DataTypeInt32>(3)});
        }
    }
    check_parameters("collect_list", {quantiles({0.5}), argument<DataTypeInt32>(1)},
                     {quantiles({0.5}), argument<DataTypeInt32>(3)});
    auto value = argument<DataTypeString>("a");
    check_parameters("group_concat", {value, argument<DataTypeString>(",")},
                     {value, argument<DataTypeString>(";")});
}

TEST(AggregateStateParametersTest, CollectZeroAndNegativeLimits) {
    for (const auto& name : {"collect_list", "collect_set"}) {
        for (const auto& value : {argument<DataTypeInt32>(7), argument<DataTypeString>("a")}) {
            for (int limit : {std::numeric_limits<Int32>::min(), -2, -1, 0,
                              std::numeric_limits<Int32>::max()}) {
                check_parameters(name, {value, argument<DataTypeInt32>(limit)},
                                 {value, argument<DataTypeInt32>(1)});
            }
            check_parameters(name, {value, argument<DataTypeInt32>(-1)},
                             {value, argument<DataTypeInt32>(-2)});
        }
    }
    for (int limit : {-2, -1, 0}) {
        check_parameters("collect_list", {quantiles({0.5}), argument<DataTypeInt32>(limit)},
                         {quantiles({0.5}), argument<DataTypeInt32>(1)});
    }
}

TEST(AggregateStateParametersTest, GroupConcatEmptyStrings) {
    auto empty = argument<DataTypeString>("");
    auto comma = argument<DataTypeString>(",");
    auto semicolon = argument<DataTypeString>(";");
    check_parameters("group_concat", {empty, comma}, {empty, semicolon});
    check_parameters("group_concat", {empty, comma}, {argument<DataTypeString>("a"), semicolon});
    check_compatible_states("group_concat", {empty, comma}, {argument<DataTypeString>("a"), comma});
}

TEST(AggregateStateParametersTest, IntersectCount) {
    auto bitmap_column = ColumnBitmap::create();
    bitmap_column->insert_value(BitmapValue {uint64_t(1)});
    ColumnWithTypeAndName bitmap {std::move(bitmap_column), std::make_shared<DataTypeBitMap>(), ""};
    auto value = argument<DataTypeInt32>(1);
    check_parameters("intersect_count", {bitmap, value, argument<DataTypeInt32>(1)},
                     {bitmap, value, argument<DataTypeInt32>(2)});
    auto text = argument<DataTypeString>("a");
    check_parameters("intersect_count", {bitmap, text, argument<DataTypeString>("a")},
                     {bitmap, text, argument<DataTypeString>("b")});
    auto empty_column = ColumnBitmap::create();
    empty_column->insert_value(BitmapValue {});
    ColumnWithTypeAndName empty_bitmap {std::move(empty_column), std::make_shared<DataTypeBitMap>(),
                                        ""};
    check_parameters("intersect_count", {empty_bitmap, value, argument<DataTypeInt32>(1)},
                     {bitmap, value, argument<DataTypeInt32>(2)});
    check_parameters("intersect_count", {empty_bitmap, text, argument<DataTypeString>("a")},
                     {empty_bitmap, text, argument<DataTypeString>("b")});
}

TEST(AggregateStateParametersTest, ExponentialMovingAverage) {
    auto value = argument<DataTypeFloat64>(7);
    auto time = argument<DataTypeFloat64>(1);
    check_parameters("exponential_moving_average", {argument<DataTypeFloat64>(1), value, time},
                     {argument<DataTypeFloat64>(2), value, time});
    check_parameters("exponential_moving_average", {argument<DataTypeFloat64>(0), value, time},
                     {argument<DataTypeFloat64>(1), value, time});
    auto zero = argument<DataTypeFloat64>(0);
    check_parameters("exponential_moving_average", {zero, zero, zero},
                     {argument<DataTypeFloat64>(1), value, time});
    check_compatible_states("exponential_moving_average", {zero, zero, zero}, {zero, value, time});
}

TEST(AggregateStateParametersTest, ExponentialMovingAverageNaNOutputs) {
    auto type = std::make_shared<DataTypeFloat64>();
    auto function = AggregateFunctionSimpleFactory::instance().get(
            "exponential_moving_average", {type, type, type}, nullptr, false,
            BeExecVersionManager::get_newest_version());
    ASSERT_NE(function, nullptr);
    StateParameterChecks checks(function);
    auto value = argument<DataTypeFloat64>(7);
    auto time = argument<DataTypeFloat64>(1);
    checks.check_invalid_outputs(
            {argument<DataTypeFloat64>(std::numeric_limits<double>::quiet_NaN()), value, time},
            "half decay must not be NaN");
    for (double half_decay : {0.0, 1.0}) {
        checks.check_compatible({argument<DataTypeFloat64>(half_decay), value, time});
    }
}

TEST(AggregateStateParametersTest, SequenceAndWindowFunnel) {
    DateV2Value<DateTimeV2ValueType> time;
    time.unchecked_set_time(2024, 1, 1, 0, 0, 0, 0);
    auto timestamp = argument<DataTypeDateTimeV2>(time);
    auto yes = argument<DataTypeUInt8>(1);
    auto no = argument<DataTypeUInt8>(0);
    for (const auto& name : {"sequence_match", "sequence_count"}) {
        check_parameters(name, {argument<DataTypeString>("(?1)"), timestamp, yes, no},
                         {argument<DataTypeString>("(?2)"), timestamp, yes, no});
    }
    for (const auto& name : {"window_funnel_v1", "window_funnel_v2"}) {
        check_parameters(name,
                         {argument<DataTypeInt64>(1), argument<DataTypeString>("default"),
                          timestamp, yes, no},
                         {argument<DataTypeInt64>(3), argument<DataTypeString>("default"),
                          timestamp, yes, no});
        check_parameters(name,
                         {argument<DataTypeInt64>(1), argument<DataTypeString>("default"),
                          timestamp, yes, no},
                         {argument<DataTypeInt64>(1), argument<DataTypeString>("fixed"), timestamp,
                          yes, no});
    }
}

TEST(AggregateStateParametersTest, SequenceEventlessParameters) {
    DateV2Value<DateTimeV2ValueType> time;
    time.unchecked_set_time(2024, 1, 1, 0, 0, 0, 0);
    auto timestamp = argument<DataTypeDateTimeV2>(time);
    auto yes = argument<DataTypeUInt8>(1);
    auto no = argument<DataTypeUInt8>(0);
    for (const auto& name : {"sequence_match", "sequence_count"}) {
        SCOPED_TRACE(name);
        Arguments eventless {argument<DataTypeString>("(?1)"), timestamp, no, no};
        check_parameters(name, eventless, {argument<DataTypeString>("(?2)"), timestamp, yes, no});
        check_parameters(name, eventless, {argument<DataTypeString>("(?2)"), timestamp, no, no});
        DataTypes types;
        for (const auto& arg : eventless) {
            types.push_back(arg.type);
        }
        auto function = AggregateFunctionSimpleFactory::instance().get(
                name, types, nullptr, false, BeExecVersionManager::get_newest_version());
        ASSERT_NE(function, nullptr);
        function->set_version(BeExecVersionManager::get_newest_version());
        StateParameterChecks checks(function);
        Arguments contributing {argument<DataTypeString>("(?1)"), timestamp, yes, no};
        for (const auto& initial : {Arguments {}, eventless, contributing}) {
            for (const auto& incoming : {Arguments {}, eventless, contributing}) {
                for (bool serialized : {false, true}) {
                    checks.check_merge_result(initial, incoming, serialized);
                }
            }
        }
    }
}

TEST(AggregateStateParametersTest, WindowFunnelEventlessParameters) {
    DateV2Value<DateTimeV2ValueType> time;
    time.unchecked_set_time(2024, 1, 1, 0, 0, 0, 0);
    auto timestamp = argument<DataTypeDateTimeV2>(time);
    auto yes = argument<DataTypeUInt8>(1);
    auto no = argument<DataTypeUInt8>(0);
    for (const auto& name : {"window_funnel_v1", "window_funnel_v2"}) {
        SCOPED_TRACE(name);
        Arguments eventless {argument<DataTypeInt64>(0), argument<DataTypeString>("default"),
                             timestamp, no, no};
        for (const auto& event : {yes, no}) {
            check_parameters(name, eventless,
                             {argument<DataTypeInt64>(3), argument<DataTypeString>("default"),
                              timestamp, event, no});
            check_parameters(name, eventless,
                             {argument<DataTypeInt64>(0), argument<DataTypeString>("fixed"),
                              timestamp, event, no});
        }
        DataTypes types;
        for (const auto& arg : eventless) {
            types.push_back(arg.type);
        }
        auto function = AggregateFunctionSimpleFactory::instance().get(
                name, types, nullptr, false, BeExecVersionManager::get_newest_version());
        ASSERT_NE(function, nullptr);
        function->set_version(BeExecVersionManager::get_newest_version());
        StateParameterChecks checks(function);
        Arguments contributing {argument<DataTypeInt64>(0), argument<DataTypeString>("default"),
                                timestamp, yes, no};
        for (const auto& initial : {Arguments {}, eventless, contributing}) {
            for (const auto& incoming : {Arguments {}, eventless, contributing}) {
                for (bool serialized : {false, true}) {
                    checks.check_merge_result(initial, incoming, serialized);
                }
            }
        }
    }
    // Even arguments that equal the fresh-state sentinels establish configuration.
    check_parameters(
            "window_funnel_v2",
            {argument<DataTypeInt64>(-1), argument<DataTypeString>("invalid"), timestamp, no, no},
            {argument<DataTypeInt64>(0), argument<DataTypeString>("default"), timestamp, no, no});
}
} // namespace doris
