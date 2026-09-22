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

#include <cstdint>
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
        EXPECT_NO_THROW(merge(destination, source, serialized));
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

    void check_noncontributing(const Arguments& empty, const Arguments& populated,
                               bool decode_empty, bool reverse, bool serialized) {
        auto* empty_state = create(empty);
        if (decode_empty) {
            auto column = serialize(empty_state);
            empty_state = create();
            _function->deserialize_and_merge_from_column(empty_state, *column, _arena);
        }
        auto* destination = reverse ? create(populated) : empty_state;
        auto* source = reverse ? empty_state : create(populated);
        EXPECT_NO_THROW(merge(destination, source, serialized));
        auto* expected = create(populated);
        // A skipped configuration must not poison subsequent contributing merges.
        EXPECT_NO_THROW(_function->merge(destination, create(populated), _arena));
        add(expected, populated);
        EXPECT_TRUE(ColumnHelper::column_equal(result(destination), result(expected)));
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

    void check_serialization_size(const Arguments& arguments, size_t empty_size,
                                  size_t populated_size) {
        auto* state = create();
        for (int reuse = 0; reuse < 2; ++reuse) {
            SCOPED_TRACE(reuse);
            EXPECT_EQ(serialize(state)->get_data_at(0).size, empty_size);
            add(state, arguments);
            auto serialized = serialize(state);
            EXPECT_EQ(serialized->get_data_at(0).size, populated_size);

            auto* restored = create();
            _function->deserialize_and_merge_from_column(restored, *serialized, _arena);
            EXPECT_TRUE(ColumnHelper::column_equal(result(restored), result(state)));
            EXPECT_EQ(serialize(restored)->get_data_at(0).size, populated_size);
            _function->reset(state);
        }
    }

    void check_sample_free_arrays(const Arguments& first, const Arguments& second,
                                  const Arguments& populated) {
        for (bool serialized : {false, true}) {
            SCOPED_TRACE(serialized);
            auto* left = create(first);
            auto* right = create(second);
            check_empty_array_state(left);
            check_empty_array_state(right);
            merge(left, right, serialized);
            check_empty_array_state(left);

            // Check the empty intermediate before a later contributor can hide its shape.
            auto* restored = create();
            auto empty_column = serialize(left);
            _function->deserialize_and_merge_from_column(restored, *empty_column, _arena);
            check_empty_array_state(restored);
            merge(restored, create(populated), serialized);
            EXPECT_TRUE(ColumnHelper::column_equal(result(restored), result(create(populated))));

            // Both association orders must adopt the contributor's parameters.
            merge(left, create(populated), serialized);
            merge(right, create(populated), serialized);
            auto* other_left = create(first);
            merge(other_left, right, serialized);
            EXPECT_TRUE(ColumnHelper::column_equal(result(left), result(other_left)));
            EXPECT_TRUE(ColumnHelper::column_equal(result(left), result(create(populated))));
            _function->reset(left);
            check_empty_array_state(left);
        }
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
    void check_empty_array_state(AggregateDataPtr place) {
        const auto output = result(place);
        const auto& array = assert_cast<const ColumnArray&>(*output);
        EXPECT_EQ(array.size(), 1);
        EXPECT_EQ(array.get_data().size(), 0);
        EXPECT_EQ(serialize(place)->get_data_at(0).to_string(),
                  serialize(create())->get_data_at(0).to_string());
    }

    void merge(AggregateDataPtr destination, AggregateDataPtr source, bool serialized) {
        if (serialized) {
            auto column = serialize(source);
            _function->deserialize_and_merge_from_column(destination, *column, _arena);
        } else {
            _function->merge(destination, source, _arena);
        }
    }

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

void check_ignored_parameters(const std::string& name, const Arguments& empty,
                              const Arguments& populated, bool decode_empty = false) {
    SCOPED_TRACE(name);
    DataTypes types;
    for (const auto& arg : empty) {
        types.push_back(arg.type);
    }
    auto function = AggregateFunctionSimpleFactory::instance().get(
            name, types, nullptr, false, BeExecVersionManager::get_newest_version());
    ASSERT_NE(function, nullptr);
    function->set_version(BeExecVersionManager::get_newest_version());
    StateParameterChecks checks(function);
    for (bool reverse : {false, true}) {
        for (bool serialized : {false, true}) {
            SCOPED_TRACE(reverse);
            SCOPED_TRACE(serialized);
            checks.check_noncontributing(empty, populated, decode_empty, reverse, serialized);
        }
    }
    if (!decode_empty) {
        checks.check_empty_and_reset(empty, populated);
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

TEST(AggregateStateParametersTest, TopNUnlimitedParameters) {
    for (const auto& name : {"topn", "topn_array", "topn_weighted"}) {
        for (int rate : {0, -1, INT32_MIN}) {
            SCOPED_TRACE(rate);
            Arguments unlimited {argument<DataTypeString>("a")};
            if (std::string(name) == "topn_weighted") {
                unlimited.push_back(argument<DataTypeInt64>(1));
            }
            unlimited.push_back(argument<DataTypeInt32>(1));
            unlimited.push_back(argument<DataTypeInt32>(rate));
            auto finite = unlimited;
            finite.back() = argument<DataTypeInt32>(2);
            check_parameters(name, unlimited, finite);

            auto zero_rate = unlimited;
            zero_rate.back() = argument<DataTypeInt32>(0);
            check_compatible_states(name, unlimited, zero_rate);
        }
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
        check_ignored_parameters(name, {value, quantiles({})}, {value, quantiles({0.25})});
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

TEST(AggregateStateParametersTest, PercentileEmptyParametersAreIgnored) {
    const auto nan = argument<DataTypeFloat64>(std::numeric_limits<double>::quiet_NaN());
    const auto value = argument<DataTypeFloat64>(7);
    for (const auto& name : {"percentile_v2", "percentile_approx", "percentile_reservoir"}) {
        for (double level : {0.0, 0.25, 1.0}) {
            const auto quantile = argument<DataTypeFloat64>(level);
            for (const auto& sample : {nan, value}) {
                check_ignored_parameters(name, {nan, quantile},
                                         {sample, argument<DataTypeFloat64>(0.75)});
            }
            check_compatible_states(name, {nan, quantile}, {value, quantile});
        }
    }
    for (const auto& name : {"percentile_array_v2", "percentile_approx_array"}) {
        check_ignored_parameters(name, {nan, quantiles({0.25})}, {value, quantiles({0.75})});
        check_ignored_parameters(name, {nan, quantiles({0.25})}, {nan, quantiles({0.75})});
        check_compatible_states(name, {nan, quantiles({0.25})}, {value, quantiles({0.25})});
    }
    check_ignored_parameters(
            "percentile_approx_weighted",
            {value, argument<DataTypeFloat64>(0), argument<DataTypeFloat64>(0.25)},
            {value, argument<DataTypeFloat64>(1), argument<DataTypeFloat64>(0.75)});
    check_ignored_parameters(
            "percentile_approx",
            {nan, argument<DataTypeFloat64>(0.25), argument<DataTypeFloat64>(2048)},
            {value, argument<DataTypeFloat64>(0.75), argument<DataTypeFloat64>(4096)});
    check_ignored_parameters("percentile_approx_array",
                             {nan, quantiles({0.25}), argument<DataTypeFloat64>(2048)},
                             {value, quantiles({0.25, 0.75}), argument<DataTypeFloat64>(4096)});
    check_compatible_states("percentile_approx_weighted",
                            {value, argument<DataTypeFloat64>(0), argument<DataTypeFloat64>(0.25)},
                            {value, argument<DataTypeFloat64>(1), argument<DataTypeFloat64>(0.25)});
}

TEST(AggregateStateParametersTest, PercentileApproxArraySampleFreeStates) {
    const auto nan = argument<DataTypeFloat64>(std::numeric_limits<double>::quiet_NaN());
    const auto value = argument<DataTypeFloat64>(7);
    for (bool has_compression : {false, true}) {
        SCOPED_TRACE(has_compression);
        std::vector<Arguments> empty_cases {
                {nan, quantiles({0.25})}, {nan, quantiles({0.25, 0.75})}, {value, quantiles({})}};
        Arguments populated {value, quantiles({0.1, 0.5, 0.9})};
        if (has_compression) {
            for (size_t i = 0; i < empty_cases.size(); ++i) {
                empty_cases[i].push_back(argument<DataTypeFloat64>(2048 * (i + 1)));
            }
            populated.push_back(argument<DataTypeFloat64>(10000));
        }
        DataTypes types;
        for (const auto& arg : populated) {
            types.push_back(arg.type);
        }
        auto function = AggregateFunctionSimpleFactory::instance().get(
                "percentile_approx_array", types, nullptr, false,
                BeExecVersionManager::get_newest_version());
        ASSERT_NE(function, nullptr);
        StateParameterChecks checks(function);
        for (const auto& first : empty_cases) {
            for (const auto& second : empty_cases) {
                checks.check_sample_free_arrays(first, second, populated);
            }
        }
    }
    check_parameters("percentile_approx_array",
                     {value, quantiles({0.25}), argument<DataTypeFloat64>(2048)},
                     {value, quantiles({0.25}), argument<DataTypeFloat64>(4096)});
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
            check_ignored_parameters(name, {value, argument<DataTypeInt32>(0)},
                                     {value, argument<DataTypeInt32>(1)});
            for (int limit :
                 {std::numeric_limits<Int32>::min(), -2, -1, std::numeric_limits<Int32>::max()}) {
                check_parameters(name, {value, argument<DataTypeInt32>(limit)},
                                 {value, argument<DataTypeInt32>(1)});
            }
            check_parameters(name, {value, argument<DataTypeInt32>(-1)},
                             {value, argument<DataTypeInt32>(-2)});
        }
    }
    check_ignored_parameters("collect_list", {quantiles({0.5}), argument<DataTypeInt32>(0)},
                             {quantiles({0.5}), argument<DataTypeInt32>(1)});
    for (int limit : {-2, -1}) {
        check_parameters("collect_list", {quantiles({0.5}), argument<DataTypeInt32>(limit)},
                         {quantiles({0.5}), argument<DataTypeInt32>(1)});
    }
    check_parameters("collect_list", {quantiles({0.5}), argument<DataTypeInt32>(-1)},
                     {quantiles({0.5}), argument<DataTypeInt32>(-2)});
}

TEST(AggregateStateParametersTest, CollectNoLimitSerializationSize) {
    auto check = [](const char* name, const ColumnWithTypeAndName& value, size_t empty_size,
                    size_t populated_size) {
        SCOPED_TRACE(name);
        SCOPED_TRACE(value.type->get_name());
        auto function = AggregateFunctionSimpleFactory::instance().get(
                name, {value.type}, nullptr, false, BeExecVersionManager::get_newest_version());
        ASSERT_NE(function, nullptr);
        function->set_version(BeExecVersionManager::get_newest_version());
        StateParameterChecks checks(function);
        checks.check_serialization_size({value}, empty_size, populated_size);
    };

    // The legacy -1 field takes two bytes: one length byte and one ZigZag byte.
    for (const auto* name : {"collect_list", "group_array", "array_agg"}) {
        check(name, argument<DataTypeInt32>(7), 4, 8);
        check(name, argument<DataTypeString>("a"), 6, 7 + sizeof(IColumn::Offset));
        check(name, quantiles({}), sizeof(size_t) + 2, sizeof(size_t) + 6);
    }
    for (const auto* name : {"collect_set", "group_uniq_array"}) {
        check(name, argument<DataTypeInt32>(7), 4, 8);
        check(name, argument<DataTypeString>("a"), 4, 7);
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
        check_ignored_parameters(name, eventless,
                                 {argument<DataTypeString>("(?2)"), timestamp, yes, no});
        check_ignored_parameters(name, eventless,
                                 {argument<DataTypeString>("(?2)"), timestamp, no, no});
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
            // V1 retains all-false rows: they can break a chain in fixed mode.
            auto check = [&](const Arguments& incoming) {
                if (std::string(name) == "window_funnel_v1") {
                    check_parameters(name, eventless, incoming);
                } else {
                    check_ignored_parameters(name, eventless, incoming);
                }
            };
            check({argument<DataTypeInt64>(3), argument<DataTypeString>("default"), timestamp,
                   event, no});
            check({argument<DataTypeInt64>(0), argument<DataTypeString>("fixed"), timestamp, event,
                   no});
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
    check_ignored_parameters(
            "window_funnel_v2",
            {argument<DataTypeInt64>(-1), argument<DataTypeString>("invalid"), timestamp, no, no},
            {argument<DataTypeInt64>(0), argument<DataTypeString>("default"), timestamp, no, no});
}
} // namespace doris
