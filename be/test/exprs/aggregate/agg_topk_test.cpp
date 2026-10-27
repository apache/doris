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
#include <memory>
#include <string>
#include <vector>

#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/string_buffer.hpp"
#include "core/types.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"

namespace doris {

void register_aggregate_function_topk(AggregateFunctionSimpleFactory& factory);

namespace {

AggregateFunctionPtr create_function(AggregateFunctionSimpleFactory& factory,
                                     const std::string& name, const DataTypes& argument_types) {
    auto function = factory.get(name, argument_types, nullptr, false, -1);
    EXPECT_NE(function, nullptr);
    return function;
}

struct TopKResult {
    std::vector<int32_t> values;
    std::vector<int64_t> counts;
};

TopKResult read_result(const IColumn& column) {
    TopKResult result;
    const auto& array = assert_cast<const ColumnArray&>(column);
    const auto& pair_nullable = assert_cast<const ColumnNullable&>(array.get_data());
    const auto& pair_column = assert_cast<const ColumnStruct&>(pair_nullable.get_nested_column());
    const auto& value_nullable = assert_cast<const ColumnNullable&>(pair_column.get_column(0));
    const auto& count_nullable = assert_cast<const ColumnNullable&>(pair_column.get_column(1));
    const auto& value_column = assert_cast<const ColumnInt32&>(value_nullable.get_nested_column());
    const auto& count_column = assert_cast<const ColumnInt64&>(count_nullable.get_nested_column());
    for (size_t i = 0; i != value_column.size(); ++i) {
        EXPECT_EQ(pair_nullable.get_null_map_data()[i], 0);
        EXPECT_EQ(value_nullable.get_null_map_data()[i], 0);
        EXPECT_EQ(count_nullable.get_null_map_data()[i], 0);
        result.values.push_back(value_column.get_element(i));
        result.counts.push_back(count_column.get_element(i));
    }
    return result;
}

TopKResult aggregate(const AggregateFunctionPtr& function, Arena& arena,
                     const std::vector<int32_t>& values, int64_t k) {
    std::unique_ptr<char[]> memory(new char[function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    function->create(place);

    auto value_column = ColumnInt32::create();
    for (int32_t value : values) {
        value_column->insert_value(value);
    }
    auto k_column = ColumnInt64::create(value_column->size(), k);
    const IColumn* columns[2] = {value_column.get(), k_column.get()};
    for (size_t row = 0; row != value_column->size(); ++row) {
        function->add(place, columns, row, arena);
    }

    auto result_column = function->get_return_type()->create_column();
    function->insert_result_into(place, *result_column);
    TopKResult result = read_result(*result_column);
    function->destroy(place);
    return result;
}

} // namespace

TEST(TopKTest, MinAndMaxWithTruncation) {
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_topk(factory);
    DataTypes argument_types = {std::make_shared<DataTypeInt32>(),
                                std::make_shared<DataTypeInt64>()};
    Arena arena;

    // 1 x3, 2 x1, 3 x2, 4 x1; k = 2 keeps the two smallest.
    std::vector<int32_t> input = {3, 1, 2, 1, 3, 1, 4};
    auto min_result =
            aggregate(create_function(factory, "min_topk", argument_types), arena, input, 2);
    EXPECT_EQ(min_result.values, (std::vector<int32_t> {1, 2}));
    EXPECT_EQ(min_result.counts, (std::vector<int64_t> {3, 1}));

    auto max_result =
            aggregate(create_function(factory, "max_topk", argument_types), arena, input, 2);
    EXPECT_EQ(max_result.values, (std::vector<int32_t> {4, 3}));
    EXPECT_EQ(max_result.counts, (std::vector<int64_t> {1, 2}));
}

TEST(TopKTest, ExactlyKDistinctValues) {
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_topk(factory);
    DataTypes argument_types = {std::make_shared<DataTypeInt32>(),
                                std::make_shared<DataTypeInt64>()};
    Arena arena;

    // Exactly k distinct values is still not truncated.
    auto result =
            aggregate(create_function(factory, "min_topk", argument_types), arena, {1, 2, 2}, 2);
    EXPECT_EQ(result.values, (std::vector<int32_t> {1, 2}));
    EXPECT_EQ(result.counts, (std::vector<int64_t> {1, 2}));
}

TEST(TopKTest, KZeroTracksExtremeOnly) {
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_topk(factory);
    DataTypes argument_types = {std::make_shared<DataTypeInt32>(),
                                std::make_shared<DataTypeInt64>()};
    Arena arena;

    auto min_result =
            aggregate(create_function(factory, "min_topk", argument_types), arena, {5, 1, 9}, 0);
    EXPECT_EQ(min_result.values, (std::vector<int32_t> {1}));
    EXPECT_EQ(min_result.counts, (std::vector<int64_t> {1}));

    auto max_result =
            aggregate(create_function(factory, "max_topk", argument_types), arena, {5, 1, 9}, 0);
    EXPECT_EQ(max_result.values, (std::vector<int32_t> {9}));
    EXPECT_EQ(max_result.counts, (std::vector<int64_t> {1}));
}

TEST(TopKTest, EmptyInput) {
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_topk(factory);
    DataTypes argument_types = {std::make_shared<DataTypeInt32>(),
                                std::make_shared<DataTypeInt64>()};
    Arena arena;

    for (const auto& name : {"min_topk", "max_topk"}) {
        for (int64_t k : {0, 4}) {
            auto result = aggregate(create_function(factory, name, argument_types), arena, {}, k);
            EXPECT_TRUE(result.values.empty());
        }
    }
}

TEST(TopKTest, MergePartialStates) {
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_topk(factory);
    DataTypes argument_types = {std::make_shared<DataTypeInt32>(),
                                std::make_shared<DataTypeInt64>()};
    Arena arena;

    auto merge_states = [&](const std::string& name, const std::vector<int32_t>& lhs_input,
                            const std::vector<int32_t>& rhs_input, int64_t k) {
        auto function = create_function(factory, name, argument_types);
        std::unique_ptr<char[]> memory(new char[function->size_of_data() * 2]);
        AggregateDataPtr lhs = memory.get();
        AggregateDataPtr rhs = memory.get() + function->size_of_data();
        function->create(lhs);
        function->create(rhs);
        auto add_all = [&](AggregateDataPtr place, const std::vector<int32_t>& input) {
            auto value_column = ColumnInt32::create();
            for (int32_t value : input) {
                value_column->insert_value(value);
            }
            auto k_column = ColumnInt64::create(value_column->size(), k);
            const IColumn* columns[2] = {value_column.get(), k_column.get()};
            for (size_t row = 0; row != value_column->size(); ++row) {
                function->add(place, columns, row, arena);
            }
        };
        add_all(lhs, lhs_input);
        add_all(rhs, rhs_input);
        function->merge(lhs, rhs, arena);
        auto result_column = function->get_return_type()->create_column();
        function->insert_result_into(lhs, *result_column);
        TopKResult result = read_result(*result_column);
        function->destroy(lhs);
        function->destroy(rhs);
        return result;
    };

    // MIN: {1:1, 5:2} + {2:2, 9:1} -> the first two are {1, 2}.
    TopKResult min_merged = merge_states("min_topk", {5, 1, 5}, {2, 2, 9}, 2);
    EXPECT_EQ(min_merged.values, (std::vector<int32_t> {1, 2}));
    EXPECT_EQ(min_merged.counts, (std::vector<int64_t> {1, 2}));

    // MAX: {5:2, 1:1} + {9:1, 2:2} -> {9, 5}.
    TopKResult max_merged = merge_states("max_topk", {1, 5, 5}, {9, 2, 2}, 2);
    EXPECT_EQ(max_merged.values, (std::vector<int32_t> {9, 5}));
    EXPECT_EQ(max_merged.counts, (std::vector<int64_t> {1, 2}));
}

TEST(TopKTest, SerializeDeserializeRoundTrip) {
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_topk(factory);
    DataTypes argument_types = {std::make_shared<DataTypeInt32>(),
                                std::make_shared<DataTypeInt64>()};
    Arena arena;

    auto function = create_function(factory, "min_topk", argument_types);

    auto round_trip = [&](const std::vector<int32_t>& input, int64_t k) {
        std::unique_ptr<char[]> memory(new char[function->size_of_data() * 2]);
        AggregateDataPtr source = memory.get();
        AggregateDataPtr target = memory.get() + function->size_of_data();
        function->create(source);
        function->create(target);

        auto values = ColumnInt32::create();
        for (int32_t value : input) {
            values->insert_value(value);
        }
        auto k_column = ColumnInt64::create(values->size(), k);
        const IColumn* columns[2] = {values.get(), k_column.get()};
        for (size_t row = 0; row != values->size(); ++row) {
            function->add(source, columns, row, arena);
        }

        auto serialized = ColumnString::create();
        BufferWritable writer(*serialized);
        function->serialize(source, writer);
        writer.commit();

        StringRef ref = serialized->get_data_at(0);
        BufferReadable reader(ref);
        function->deserialize(target, reader, arena);

        auto source_result = function->get_return_type()->create_column();
        auto target_result = function->get_return_type()->create_column();
        function->insert_result_into(source, *source_result);
        function->insert_result_into(target, *target_result);
        TopKResult source_values = read_result(*source_result);
        TopKResult target_values = read_result(*target_result);
        EXPECT_EQ(source_values.values, target_values.values);
        EXPECT_EQ(source_values.counts, target_values.counts);

        function->destroy(source);
        function->destroy(target);
    };

    round_trip({3, 1, 2, 1, 4}, 2);
    // An empty state (no rows for the group, or every value NULL) keeps an empty count vector,
    // whose data() is null; the serialized form must not hand it to memcpy.
    round_trip({}, 2);
    // 65 distinct INTs = 260 bytes, above SERIALIZED_MEM_SIZE_LIMIT: the serde compresses the
    // value column and the encoded end pointer is earlier than the declared capacity.
    std::vector<int32_t> compressed;
    for (int32_t value = 0; value < 65; ++value) {
        compressed.push_back(value);
    }
    round_trip(compressed, 65);
}

TEST(TopKTest, MergeRejectsIncompatibleK) {
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_topk(factory);
    DataTypes argument_types = {std::make_shared<DataTypeInt32>(),
                                std::make_shared<DataTypeInt64>()};
    Arena arena;

    auto function = create_function(factory, "min_topk", argument_types);
    std::unique_ptr<char[]> memory(new char[function->size_of_data() * 2]);
    AggregateDataPtr lhs = memory.get();
    AggregateDataPtr rhs = memory.get() + function->size_of_data();
    function->create(lhs);
    function->create(rhs);

    auto add_one = [&](AggregateDataPtr place, int32_t value, int64_t k) {
        auto value_column = ColumnInt32::create();
        value_column->insert_value(value);
        auto k_column = ColumnInt64::create(1, k);
        const IColumn* columns[2] = {value_column.get(), k_column.get()};
        function->add(place, columns, 0, arena);
    };
    add_one(lhs, 1, 2);
    // The public _state/_merge/_union combinators do not encode k in the AggState type, so states
    // built with different literal k can reach one merge.
    add_one(rhs, 1, 0);

    EXPECT_THROW(function->merge(lhs, rhs, arena), Exception);

    function->destroy(lhs);
    function->destroy(rhs);
}

TEST(TopKTest, MergeOverflowIsReported) {
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_topk(factory);
    DataTypes argument_types = {std::make_shared<DataTypeInt32>(),
                                std::make_shared<DataTypeInt64>()};
    Arena arena;

    auto function = create_function(factory, "min_topk", argument_types);
    std::unique_ptr<char[]> memory(new char[function->size_of_data() * 2]);
    AggregateDataPtr max_state = memory.get();
    AggregateDataPtr one_state = memory.get() + function->size_of_data();
    function->create(max_state);
    function->create(one_state);

    // Build a state with count = INT64_MAX directly from the serialized format, since no number
    // of input rows can reach it in a test.
    {
        auto values = ColumnInt32::create();
        values->insert_value(1);
        auto value_type = std::make_shared<DataTypeInt32>();
        auto serialized = ColumnString::create();
        BufferWritable writer(*serialized);
        writer.write_binary(true);                    // has_k
        writer.write_binary(static_cast<int64_t>(2)); // k
        writer.write_binary(static_cast<int64_t>(1)); // entry count
        int64_t size_bytes = value_type->get_uncompressed_serialized_bytes(*values, 0);
        writer.write_binary(size_bytes);
        writer.resize(size_bytes);
        auto* pos = value_type->serialize(*values, writer.data(), 0);
        EXPECT_EQ(pos, writer.data() + size_bytes);
        writer.add_offset(size_bytes);
        int64_t count = std::numeric_limits<int64_t>::max();
        writer.write(reinterpret_cast<const char*>(&count), sizeof(count));
        writer.commit();

        StringRef ref = serialized->get_data_at(0);
        BufferReadable reader(ref);
        function->deserialize(max_state, reader, arena);
    }

    auto values = ColumnInt32::create();
    values->insert_value(1);
    auto k_column = ColumnInt64::create(1, 2);
    const IColumn* columns[2] = {values.get(), k_column.get()};
    function->add(one_state, columns, 0, arena);

    EXPECT_THROW(function->merge(max_state, one_state, arena), Exception);

    function->destroy(max_state);
    function->destroy(one_state);
}

TEST(TopKTest, NullEvaluatedCountIsRejected) {
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_topk(factory);
    // A constant count expression that FE cannot fold is evaluated on BE; when it is NULL the
    // nullable-argument adapter is bypassed so the NULL reaches this check instead of being
    // filtered out before add() runs.
    DataTypes argument_types = {std::make_shared<DataTypeInt32>(),
                                make_nullable(std::make_shared<DataTypeInt64>())};
    Arena arena;

    auto function = create_function(factory, "min_topk", argument_types);
    std::unique_ptr<char[]> memory(new char[function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    function->create(place);

    auto value_column = ColumnInt32::create();
    value_column->insert_value(1);
    auto count_column =
            ColumnNullable::create(ColumnInt64::create(1, 0), ColumnUInt8::create(1, 1));
    const IColumn* columns[2] = {value_column.get(), count_column.get()};
    EXPECT_THROW(function->add(place, columns, 0, arena), Exception);

    function->destroy(place);
}

TEST(TopKTest, NullValueRowsAreIgnored) {
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_topk(factory);
    DataTypes argument_types = {make_nullable(std::make_shared<DataTypeInt32>()),
                                std::make_shared<DataTypeInt64>()};
    Arena arena;

    auto function = create_function(factory, "max_topk", argument_types);
    std::unique_ptr<char[]> memory(new char[function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    function->create(place);

    auto nested = ColumnInt32::create();
    nested->insert_value(3);
    nested->insert_default();
    nested->insert_value(3);
    nested->insert_default();
    auto null_map = ColumnUInt8::create();
    for (uint8_t is_null : {0, 1, 0, 1}) {
        null_map->get_data().push_back(is_null);
    }
    auto value_column = ColumnNullable::create(std::move(nested), std::move(null_map));
    auto k_column = ColumnInt64::create(value_column->size(), 2);
    const IColumn* columns[2] = {value_column.get(), k_column.get()};
    for (size_t row = 0; row != value_column->size(); ++row) {
        function->add(place, columns, row, arena);
    }

    auto result_column = function->get_return_type()->create_column();
    function->insert_result_into(place, *result_column);
    TopKResult result = read_result(*result_column);
    EXPECT_EQ(result.values, (std::vector<int32_t> {3}));
    EXPECT_EQ(result.counts, (std::vector<int64_t> {2}));

    function->destroy(place);
}

} // namespace doris
