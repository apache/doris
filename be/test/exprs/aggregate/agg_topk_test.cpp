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
#include <map>
#include <memory>
#include <random>
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
#include "core/data_type/data_type_string.h"
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

struct StringTopKResult {
    std::vector<std::string> values;
    std::vector<int64_t> counts;
};

StringTopKResult read_string_result(const IColumn& column) {
    StringTopKResult result;
    const auto& array = assert_cast<const ColumnArray&>(column);
    const auto& pair_nullable = assert_cast<const ColumnNullable&>(array.get_data());
    const auto& pair_column = assert_cast<const ColumnStruct&>(pair_nullable.get_nested_column());
    const auto& value_nullable = assert_cast<const ColumnNullable&>(pair_column.get_column(0));
    const auto& count_nullable = assert_cast<const ColumnNullable&>(pair_column.get_column(1));
    const auto& value_column = assert_cast<const ColumnString&>(value_nullable.get_nested_column());
    const auto& count_column = assert_cast<const ColumnInt64&>(count_nullable.get_nested_column());
    for (size_t i = 0; i != value_column.size(); ++i) {
        StringRef value = value_column.get_data_at(i);
        result.values.emplace_back(value.data, value.size);
        result.counts.push_back(count_column.get_element(i));
    }
    return result;
}

void add_string_rows(const AggregateFunctionPtr& function, AggregateDataPtr place, Arena& arena,
                     const std::vector<std::string>& input, int64_t k) {
    auto value_column = ColumnString::create();
    for (const auto& value : input) {
        value_column->insert_data(value.data(), value.size());
    }
    auto k_column = ColumnInt64::create(value_column->size(), k);
    const IColumn* columns[2] = {value_column.get(), k_column.get()};
    for (size_t row = 0; row != value_column->size(); ++row) {
        function->add(place, columns, row, arena);
    }
}

StringTopKResult aggregate_strings(AggregateFunctionSimpleFactory& factory, Arena& arena,
                                   const std::string& name, const std::vector<std::string>& input,
                                   int64_t k) {
    DataTypes argument_types = {std::make_shared<DataTypeString>(),
                                std::make_shared<DataTypeInt64>()};
    auto function = create_function(factory, name, argument_types);
    std::unique_ptr<char[]> memory(new char[function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    function->create(place);
    add_string_rows(function, place, arena, input, k);
    auto result_column = function->get_return_type()->create_column();
    function->insert_result_into(place, *result_column);
    StringTopKResult result = read_string_result(*result_column);
    function->destroy(place);
    return result;
}

StringTopKResult merge_string_states(AggregateFunctionSimpleFactory& factory, Arena& arena,
                                     const std::string& name,
                                     const std::vector<std::string>& lhs_input,
                                     const std::vector<std::string>& rhs_input, int64_t k) {
    DataTypes argument_types = {std::make_shared<DataTypeString>(),
                                std::make_shared<DataTypeInt64>()};
    auto function = create_function(factory, name, argument_types);
    std::unique_ptr<char[]> memory(new char[function->size_of_data() * 2]);
    AggregateDataPtr lhs = memory.get();
    AggregateDataPtr rhs = memory.get() + function->size_of_data();
    function->create(lhs);
    function->create(rhs);
    add_string_rows(function, lhs, arena, lhs_input, k);
    add_string_rows(function, rhs, arena, rhs_input, k);
    function->merge(lhs, rhs, arena);
    auto result_column = function->get_return_type()->create_column();
    function->insert_result_into(lhs, *result_column);
    StringTopKResult result = read_string_result(*result_column);
    function->destroy(lhs);
    function->destroy(rhs);
    return result;
}

// Builds a state directly from the serialized format, for counts that no number of input rows
// can reach in a test.
void deserialize_state(const AggregateFunctionPtr& function, AggregateDataPtr place, Arena& arena,
                       const std::vector<std::pair<int32_t, int64_t>>& entries, int64_t k) {
    auto values = ColumnInt32::create();
    for (const auto& [value, count] : entries) {
        values->insert_value(value);
    }
    auto value_type = std::make_shared<DataTypeInt32>();
    auto serialized = ColumnString::create();
    BufferWritable writer(*serialized);
    writer.write_binary(true); // has_k
    writer.write_binary(k);    // k
    writer.write_binary(static_cast<int64_t>(entries.size()));
    int64_t size_bytes = value_type->get_uncompressed_serialized_bytes(*values, 0);
    writer.write_binary(size_bytes);
    writer.resize(size_bytes);
    auto* pos = value_type->serialize(*values, writer.data(), 0);
    EXPECT_EQ(pos, writer.data() + size_bytes);
    writer.add_offset(size_bytes);
    for (const auto& [value, count] : entries) {
        writer.write(reinterpret_cast<const char*>(&count), sizeof(count));
    }
    writer.commit();

    StringRef ref = serialized->get_data_at(0);
    BufferReadable reader(ref);
    function->deserialize(place, reader, arena);
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

TEST(TopKTest, EvictsContinuouslyAndCompacts) {
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_topk(factory);
    DataTypes argument_types = {std::make_shared<DataTypeInt32>(),
                                std::make_shared<DataTypeInt64>()};
    Arena arena;

    // Descending input for MIN makes every row beat the current worst, so the list evicts on
    // every row after the first k; with k = 500 the dead slots exceed the live ones several times
    // and force a compaction.
    std::vector<int32_t> descending;
    for (int32_t value = 1999; value >= 0; --value) {
        descending.push_back(value);
    }
    auto min_result =
            aggregate(create_function(factory, "min_topk", argument_types), arena, descending, 500);
    ASSERT_EQ(min_result.values.size(), 500);
    for (size_t i = 0; i < 500; ++i) {
        EXPECT_EQ(min_result.values[i], static_cast<int32_t>(i));
        EXPECT_EQ(min_result.counts[i], 1);
    }

    auto max_result =
            aggregate(create_function(factory, "max_topk", argument_types), arena, descending, 500);
    ASSERT_EQ(max_result.values.size(), 500);
    for (size_t i = 0; i < 500; ++i) {
        EXPECT_EQ(max_result.values[i], static_cast<int32_t>(1999 - i));
        EXPECT_EQ(max_result.counts[i], 1);
    }
}

TEST(TopKTest, MatchesReferenceOnRandomInput) {
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_topk(factory);
    DataTypes argument_types = {std::make_shared<DataTypeInt32>(),
                                std::make_shared<DataTypeInt64>()};
    Arena arena;

    // A mix of duplicates, replacements and rejections; the expected result comes from sorting
    // the frequencies, so the insertion path, the count updates and the evictions are all checked
    // against the definition rather than against a hand-traced sequence.
    std::mt19937 rng(20261028);
    std::uniform_int_distribution<int32_t> value_dist(0, 100000);
    std::vector<int32_t> input;
    for (int row = 0; row < 2000; ++row) {
        input.push_back(value_dist(rng));
    }
    std::map<int32_t, int64_t> frequencies;
    for (int32_t value : input) {
        ++frequencies[value];
    }

    for (int64_t k : {1, 3, 5, 16}) {
        std::vector<int32_t> min_values;
        std::vector<int64_t> min_counts;
        for (auto it = frequencies.begin();
             it != frequencies.end() && static_cast<int64_t>(min_values.size()) < k; ++it) {
            min_values.push_back(it->first);
            min_counts.push_back(it->second);
        }
        std::vector<int32_t> max_values;
        std::vector<int64_t> max_counts;
        for (auto it = frequencies.rbegin();
             it != frequencies.rend() && static_cast<int64_t>(max_values.size()) < k; ++it) {
            max_values.push_back(it->first);
            max_counts.push_back(it->second);
        }

        auto min_result =
                aggregate(create_function(factory, "min_topk", argument_types), arena, input, k);
        EXPECT_EQ(min_result.values, min_values);
        EXPECT_EQ(min_result.counts, min_counts);

        auto max_result =
                aggregate(create_function(factory, "max_topk", argument_types), arena, input, k);
        EXPECT_EQ(max_result.values, max_values);
        EXPECT_EQ(max_result.counts, max_counts);
    }
}

TEST(TopKTest, ShortStringValues) {
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_topk(factory);
    Arena arena;

    // One-byte values: the string comparator loads a 16-byte vector, which must stay inside the
    // store's padded arena.
    auto min_result = aggregate_strings(factory, arena, "min_topk", {"c", "a", "b", "a"}, 2);
    EXPECT_EQ(min_result.values, (std::vector<std::string> {"a", "b"}));
    EXPECT_EQ(min_result.counts, (std::vector<int64_t> {2, 1}));

    auto max_result = aggregate_strings(factory, arena, "max_topk", {"c", "a", "b", "a"}, 2);
    EXPECT_EQ(max_result.values, (std::vector<std::string> {"c", "b"}));
    EXPECT_EQ(max_result.counts, (std::vector<int64_t> {1, 1}));

    auto single = aggregate_strings(factory, arena, "min_topk", {"a", "a"}, 1);
    EXPECT_EQ(single.values, (std::vector<std::string> {"a"}));
    EXPECT_EQ(single.counts, (std::vector<int64_t> {2}));
}

TEST(TopKTest, MergeStringStates) {
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_topk(factory);
    Arena arena;

    // a:1, m:1, z:2 -> the two smallest distinct values are a and m.
    auto merged = merge_string_states(factory, arena, "min_topk", {"m", "z"}, {"a", "z"}, 2);
    EXPECT_EQ(merged.values, (std::vector<std::string> {"a", "m"}));
    EXPECT_EQ(merged.counts, (std::vector<int64_t> {1, 1}));

    // The left state is full and every right value is worse than its worst: the merge keeps the
    // left result and short-circuits.
    auto rejected = merge_string_states(factory, arena, "min_topk", {"a", "b", "c"}, {"z"}, 2);
    EXPECT_EQ(rejected.values, (std::vector<std::string> {"a", "b"}));
    EXPECT_EQ(rejected.counts, (std::vector<int64_t> {1, 1}));

    // The right best equals the left worst (b): only the count of b changes, and the right's c
    // is worse and cannot enter.
    auto counted = merge_string_states(factory, arena, "min_topk", {"a", "b"}, {"b", "c"}, 2);
    EXPECT_EQ(counted.values, (std::vector<std::string> {"a", "b"}));
    EXPECT_EQ(counted.counts, (std::vector<int64_t> {1, 2}));

    // A right best better than the left worst goes through the ordinary merge.
    auto replaced = merge_string_states(factory, arena, "min_topk", {"b", "c"}, {"a"}, 2);
    EXPECT_EQ(replaced.values, (std::vector<std::string> {"a", "b"}));
    EXPECT_EQ(replaced.counts, (std::vector<int64_t> {1, 1}));

    // A singleton equal to an interior retained value only raises that count.
    auto interior = merge_string_states(factory, arena, "min_topk", {"a", "b", "c"}, {"b"}, 3);
    EXPECT_EQ(interior.values, (std::vector<std::string> {"a", "b", "c"}));
    EXPECT_EQ(interior.counts, (std::vector<int64_t> {1, 2, 1}));

    // The same holds while the state is still below k.
    auto partial = merge_string_states(factory, arena, "min_topk", {"a", "b", "c"}, {"b"}, 5);
    EXPECT_EQ(partial.values, (std::vector<std::string> {"a", "b", "c"}));
    EXPECT_EQ(partial.counts, (std::vector<int64_t> {1, 2, 1}));
}

TEST(TopKTest, MergeIntoAnUnfullStateKeepsTheWorstCache) {
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_topk(factory);
    DataTypes argument_types = {std::make_shared<DataTypeString>(),
                                std::make_shared<DataTypeInt64>()};
    Arena arena;
    auto function = create_function(factory, "min_topk", argument_types);
    std::unique_ptr<char[]> memory(new char[function->size_of_data() * 2]);
    AggregateDataPtr state = memory.get();
    AggregateDataPtr other = memory.get() + function->size_of_data();
    function->create(state);
    function->create(other);

    // The merge appends z into a state that was below k, so the cached worst must become z. If it
    // stayed b, the next full-list comparison would reject c as "worse" and keep z.
    add_string_rows(function, state, arena, {"a", "b"}, 3);
    add_string_rows(function, other, arena, {"z"}, 3);
    function->merge(state, other, arena);
    add_string_rows(function, state, arena, {"c"}, 3);

    auto result_column = function->get_return_type()->create_column();
    function->insert_result_into(state, *result_column);
    StringTopKResult result = read_string_result(*result_column);
    EXPECT_EQ(result.values, (std::vector<std::string> {"a", "b", "c"}));
    EXPECT_EQ(result.counts, (std::vector<int64_t> {1, 1, 1}));

    function->destroy(state);
    function->destroy(other);
}

TEST(TopKTest, StringStateSurvivesEvictionAndSerde) {
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_topk(factory);
    DataTypes argument_types = {std::make_shared<DataTypeString>(),
                                std::make_shared<DataTypeInt64>()};
    Arena arena;
    auto function = create_function(factory, "min_topk", argument_types);

    // k = 8 is first filled with eight distinct 1000-byte values, then replaced by one-byte ones:
    // the evicted payload exceeds the live payload, so the byte-aware compaction runs, and the
    // serialized state must still round-trip.
    std::vector<std::string> input;
    for (int i = 0; i < 8; ++i) {
        input.push_back(std::string(999, 'z') + static_cast<char>('a' + i));
    }
    for (int i = 0; i < 8; ++i) {
        input.emplace_back(1, static_cast<char>('a' + i));
    }

    std::unique_ptr<char[]> memory(new char[function->size_of_data() * 2]);
    AggregateDataPtr source = memory.get();
    AggregateDataPtr target = memory.get() + function->size_of_data();
    function->create(source);
    function->create(target);
    add_string_rows(function, source, arena, input, 8);

    auto serialized = ColumnString::create();
    BufferWritable writer(*serialized);
    function->serialize(source, writer);
    writer.commit();
    StringRef ref = serialized->get_data_at(0);
    BufferReadable reader(ref);
    function->deserialize(target, reader, arena);

    auto source_result_column = function->get_return_type()->create_column();
    auto target_result_column = function->get_return_type()->create_column();
    function->insert_result_into(source, *source_result_column);
    function->insert_result_into(target, *target_result_column);
    StringTopKResult source_result = read_string_result(*source_result_column);
    StringTopKResult target_result = read_string_result(*target_result_column);

    std::vector<std::string> expected_values;
    std::vector<int64_t> expected_counts(8, 1);
    for (int i = 0; i < 8; ++i) {
        expected_values.emplace_back(1, static_cast<char>('a' + i));
    }
    EXPECT_EQ(source_result.values, expected_values);
    EXPECT_EQ(source_result.counts, expected_counts);
    EXPECT_EQ(target_result.values, expected_values);
    EXPECT_EQ(target_result.counts, expected_counts);

    function->destroy(source);
    function->destroy(target);
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
    // Evictions leave dead slots in the state; the serialized form is the live entries only.
    std::vector<int32_t> evicting;
    for (int32_t value = 100; value >= 0; --value) {
        evicting.push_back(value);
    }
    round_trip(evicting, 3);
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

TEST(TopKTest, MergeDoesNotCountTheDiscardedTail) {
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_topk(factory);
    DataTypes argument_types = {std::make_shared<DataTypeInt32>(),
                                std::make_shared<DataTypeInt64>()};
    Arena arena;

    auto function = create_function(factory, "min_topk", argument_types);
    std::unique_ptr<char[]> memory(new char[function->size_of_data() * 2]);
    AggregateDataPtr state = memory.get();
    AggregateDataPtr other = memory.get() + function->size_of_data();
    function->create(state);
    function->create(other);

    // A = [(0,1),(2,INT64_MAX)], B = [(1,1),(2,1)] with k = 2. The retained prefix is
    // [(0,1),(1,1)]; value 2 never enters the result, so its INT64_MAX + 1 must not be computed.
    const auto int64_max = std::numeric_limits<int64_t>::max();
    deserialize_state(function, state, arena, {{0, 1}, {2, int64_max}}, 2);
    auto values = ColumnInt32::create();
    values->insert_value(1);
    values->insert_value(2);
    auto k_column = ColumnInt64::create(2, 2);
    const IColumn* columns[2] = {values.get(), k_column.get()};
    for (size_t row = 0; row != 2; ++row) {
        function->add(other, columns, row, arena);
    }

    EXPECT_NO_THROW(function->merge(state, other, arena));

    auto result = function->get_return_type()->create_column();
    function->insert_result_into(state, *result);
    TopKResult merged = read_result(*result);
    EXPECT_EQ(merged.values, (std::vector<int32_t> {0, 1}));
    EXPECT_EQ(merged.counts, (std::vector<int64_t> {1, 1}));

    function->destroy(state);
    function->destroy(other);
}

TEST(TopKTest, MergeDoesNotCountTheDiscardedTailMax) {
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_topk(factory);
    DataTypes argument_types = {std::make_shared<DataTypeInt32>(),
                                std::make_shared<DataTypeInt64>()};
    Arena arena;

    auto function = create_function(factory, "max_topk", argument_types);
    std::unique_ptr<char[]> memory(new char[function->size_of_data() * 2]);
    AggregateDataPtr state = memory.get();
    AggregateDataPtr other = memory.get() + function->size_of_data();
    function->create(state);
    function->create(other);

    // MAX is the symmetric case: A = [(2,1),(0,INT64_MAX)], B = [(1,1),(0,1)], k = 2. The result
    // is [(2,1),(1,1)] and value 0 is discarded without its counts being combined.
    const auto int64_max = std::numeric_limits<int64_t>::max();
    deserialize_state(function, state, arena, {{2, 1}, {0, int64_max}}, 2);
    auto values = ColumnInt32::create();
    values->insert_value(1);
    values->insert_value(0);
    auto k_column = ColumnInt64::create(2, 2);
    const IColumn* columns[2] = {values.get(), k_column.get()};
    for (size_t row = 0; row != 2; ++row) {
        function->add(other, columns, row, arena);
    }

    EXPECT_NO_THROW(function->merge(state, other, arena));

    auto result = function->get_return_type()->create_column();
    function->insert_result_into(state, *result);
    TopKResult merged = read_result(*result);
    EXPECT_EQ(merged.values, (std::vector<int32_t> {2, 1}));
    EXPECT_EQ(merged.counts, (std::vector<int64_t> {1, 1}));

    function->destroy(state);
    function->destroy(other);
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
