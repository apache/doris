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

#include "exprs/aggregate/aggregate_function_distinct.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <string>
#include <type_traits>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "core/arena.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/string_buffer.hpp"
#include "exec/common/hash_table/phmap_fwd_decl.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"
#include "testutil/column_helper.h"

namespace doris {
namespace {

template <typename T>
class DistinctNumericMergeTest : public testing::Test {};

using IntegerTypes = testing::Types<std::integral_constant<PrimitiveType, TYPE_TINYINT>,
                                    std::integral_constant<PrimitiveType, TYPE_SMALLINT>,
                                    std::integral_constant<PrimitiveType, TYPE_INT>,
                                    std::integral_constant<PrimitiveType, TYPE_BIGINT>,
                                    std::integral_constant<PrimitiveType, TYPE_LARGEINT>>;
TYPED_TEST_SUITE(DistinctNumericMergeTest, IntegerTypes);

TYPED_TEST(DistinctNumericMergeTest, PreserveSourceAndDeduplicateRepeatedMerges) {
    using Data = AggregateFunctionDistinctSingleNumericData<TypeParam::value, false>;
    Arena arena;
    Data destination;
    Data source;
    source.data.insert({1, 2, 3});

    destination.merge(source, arena);
    EXPECT_EQ(destination.data, source.data);
    destination.data.insert(4);
    const auto capacity = destination.data.capacity();
    destination.merge(source, arena);
    EXPECT_EQ(destination.data.size(), 4);
    EXPECT_EQ(destination.data.capacity(), capacity);
    EXPECT_EQ(source.data.size(), 3);
    EXPECT_FALSE(source.data.contains(4));

    Data empty;
    destination.merge(empty, arena);
    EXPECT_EQ(destination.data.size(), 4);
    EXPECT_EQ(destination.data.capacity(), capacity);
    destination.clear();
    destination.merge(empty, arena);
    EXPECT_TRUE(destination.data.empty());
}

TYPED_TEST(DistinctNumericMergeTest, DeserializeIntoExistingDestinationWithoutPopulatingScratch) {
    using Data = AggregateFunctionDistinctSingleNumericData<TypeParam::value, false>;
    Arena arena;
    Data source;
    source.data.insert({1, 2, 3});
    ColumnString serialized;
    VectorBufferWriter writer(serialized);
    source.serialize(writer);
    writer.commit();

    Data destination;
    Data scratch;
    destination.data.insert(4);
    for (int repeat = 0; repeat < 2; ++repeat) {
        VectorBufferReader reader(serialized.get_data_at(0));
        destination.deserialize_and_merge(scratch, reader, arena);
        EXPECT_EQ(destination.data.size(), 4);
        for (int value : {1, 2, 3, 4}) {
            EXPECT_TRUE(destination.data.contains(value));
        }
        // The optimization must not materialize the serialized set in the scratch state.
        EXPECT_TRUE(scratch.data.empty());
        EXPECT_EQ(scratch.data.capacity(), 0);
    }

    Data empty;
    ColumnString serialized_empty;
    VectorBufferWriter empty_writer(serialized_empty);
    empty.serialize(empty_writer);
    empty_writer.commit();
    VectorBufferReader empty_reader(serialized_empty.get_data_at(0));
    destination.deserialize_and_merge(scratch, empty_reader, arena);
    EXPECT_EQ(destination.data.size(), 4);

    destination.clear();
    VectorBufferReader reader(serialized.get_data_at(0));
    destination.deserialize_and_merge(scratch, reader, arena);
    EXPECT_EQ(destination.data, source.data);
    EXPECT_TRUE(scratch.data.empty());
}

void add_column(const IAggregateFunction& function, AggregateDataPtr place, const IColumn& column,
                Arena& arena) {
    const IColumn* columns[] = {&column};
    function.add_batch_single_place(column.size(), place, columns, arena);
}

class DistinctMergeDispatchTest : public testing::TestWithParam<bool> {
protected:
    AggregateFunctionPtr function(const std::string& name, const DataTypePtr& input_type,
                                  const DataTypePtr& result_type, bool result_nullable) {
        AggregateFunctionAttr attr;
        attr.enable_aggregate_function_null_v2 = GetParam();
        return AggregateFunctionSimpleFactory::instance().get(
                name, {input_type}, result_type, result_nullable,
                BeExecVersionManager::get_newest_version(), attr);
    }
};

TEST_P(DistinctMergeDispatchTest, NullableSumMergesColumnRangesAndAllNullStates) {
    auto type = make_nullable(std::make_shared<DataTypeInt64>());
    auto aggregate = function("multi_distinct_sum", type, type, true);
    ASSERT_NE(aggregate, nullptr);
    Arena arena;
    AggregateFunctionGuard source(aggregate.get());
    AggregateFunctionGuard destination(aggregate.get());
    auto serialized = aggregate->get_serialized_type()->create_column();

    auto first = ColumnHelper::create_nullable_column<DataTypeInt64>({1, 2, 2, 99}, {0, 0, 0, 1});
    add_column(*aggregate, source.data(), *first, arena);
    aggregate->serialize_without_key_to_column(source.data(), *serialized);
    aggregate->reset(source.data());
    auto second = ColumnHelper::create_nullable_column<DataTypeInt64>({2, 3, 99}, {0, 0, 1});
    add_column(*aggregate, source.data(), *second, arena);
    aggregate->serialize_without_key_to_column(source.data(), *serialized);
    aggregate->reset(source.data());
    auto nulls = ColumnHelper::create_nullable_column<DataTypeInt64>({99, 99}, {1, 1});
    add_column(*aggregate, source.data(), *nulls, arena);
    aggregate->serialize_without_key_to_column(source.data(), *serialized);

    auto initial = ColumnHelper::create_nullable_column<DataTypeInt64>({10}, {0});
    add_column(*aggregate, destination.data(), *initial, arena);
    for (int repeat = 0; repeat < 2; ++repeat) {
        aggregate->deserialize_and_merge_from_column(destination.data(), *serialized, arena);
    }
    auto result = type->create_column();
    aggregate->insert_result_into(destination.data(), *result);
    EXPECT_TRUE(ColumnHelper::column_equal(
            std::move(result), ColumnHelper::create_nullable_column<DataTypeInt64>({16}, {0})));

    aggregate->reset(destination.data());
    aggregate->deserialize_and_merge_from_column_range(destination.data(), *serialized, 2, 2,
                                                       arena);
    result = type->create_column();
    aggregate->insert_result_into(destination.data(), *result);
    EXPECT_TRUE(ColumnHelper::column_equal(
            std::move(result), ColumnHelper::create_nullable_column<DataTypeInt64>({0}, {1})));
}

TEST_P(DistinctMergeDispatchTest, GroupedAndSelectedBatchMerges) {
    for (bool nullable : {false, true}) {
        DataTypePtr type = std::make_shared<DataTypeInt64>();
        if (nullable) {
            type = make_nullable(type);
        }
        auto aggregate = function("multi_distinct_sum", type, type, nullable);
        ASSERT_NE(aggregate, nullptr);
        Arena arena;
        AggregateFunctionGuard source(aggregate.get());
        AggregateFunctionGuard destination(aggregate.get());
        auto serialized = aggregate->get_serialized_type()->create_column();
        for (const auto& values : {std::vector<Int64> {1, 2, 2}, std::vector<Int64> {2, 3}}) {
            auto column = type->create_column();
            for (auto value : values) {
                column->insert(Field::create_field<TYPE_BIGINT>(value));
            }
            aggregate->reset(source.data());
            add_column(*aggregate, source.data(), *column, arena);
            aggregate->serialize_without_key_to_column(source.data(), *serialized);
        }

        auto* scratch = reinterpret_cast<AggregateDataPtr>(
                arena.aligned_alloc(2 * aggregate->size_of_data(), aggregate->align_of_data()));
        std::array<AggregateDataPtr, 2> places {destination.data(), destination.data()};
        aggregate->deserialize_and_merge_vec(places.data(), 0, scratch, serialized.get(), arena, 2);
        auto result = type->create_column();
        aggregate->insert_result_into(destination.data(), *result);
        EXPECT_EQ(type->to_string(*result, 0), "6");

        aggregate->reset(destination.data());
        places[0] = nullptr;
        aggregate->deserialize_and_merge_vec_selected(places.data(), 0, scratch, serialized.get(),
                                                      arena, 2);
        result = type->create_column();
        aggregate->insert_result_into(destination.data(), *result);
        EXPECT_EQ(type->to_string(*result, 0), "5");
    }
}

TEST_P(DistinctMergeDispatchTest, GenericStringStateOwnsKeysAfterInputBufferIsReleased) {
    auto type = std::make_shared<DataTypeString>();
    auto aggregate = function("multi_distinct_min", type, type, false);
    ASSERT_NE(aggregate, nullptr);
    auto serialized = ColumnString::create();
    const std::string first(512, 'a');
    const std::string second(512, 'b');
    {
        Arena source_arena;
        AggregateFunctionGuard source(aggregate.get());
        auto column = ColumnHelper::create_column<DataTypeString>({first, second, first});
        add_column(*aggregate, source.data(), *column, source_arena);
        aggregate->serialize_without_key_to_column(source.data(), *serialized);
    }

    Arena arena;
    AggregateFunctionGuard destination(aggregate.get());
    aggregate->deserialize_and_merge_from_column(destination.data(), *serialized, arena);
    std::fill(serialized->get_chars().begin(), serialized->get_chars().end(), '?');
    serialized.reset();
    auto result = type->create_column();
    aggregate->insert_result_into(destination.data(), *result);
    EXPECT_EQ(result->get_data_at(0).to_string(), first);
}

INSTANTIATE_TEST_SUITE_P(NullImplementations, DistinctMergeDispatchTest, testing::Bool());

constexpr size_t kValueSize = 201;

// `count` distinct strings of kValueSize bytes each.
std::vector<std::string> distinct_values(size_t count) {
    std::vector<std::string> values;
    for (size_t i = 0; i < count; ++i) {
        auto suffix = std::to_string(1000 + i);
        values.push_back(std::string(kValueSize - suffix.size(), 'x') + suffix);
    }
    return values;
}

// `rows` rows that cycle over `values`.
ColumnString::MutablePtr cycling_column(const std::vector<std::string>& values, size_t rows) {
    auto column = ColumnString::create();
    for (size_t row = 0; row < rows; ++row) {
        const auto& value = values[row % values.size()];
        column->insert_data(value.data(), value.size());
    }
    return column;
}

std::vector<std::string> column_values(const IColumn& column) {
    std::vector<std::string> values;
    for (size_t i = 0; i < column.size(); ++i) {
        values.push_back(column.get_data_at(i).to_string());
    }
    return values;
}

std::vector<std::string> sorted(std::vector<std::string> values) {
    std::ranges::sort(values);
    return values;
}

template <typename T>
class DistinctGenericArenaTest : public testing::Test {};

// The stable states keep the first-insertion order for DISTINCT ... ORDER BY.
using StableModes = testing::Types<std::false_type, std::true_type>;
TYPED_TEST_SUITE(DistinctGenericArenaTest, StableModes);

// The arena frees nothing before it is destroyed, so its used size is what the state keeps.
TYPED_TEST(DistinctGenericArenaTest, SingleAddCopiesEachDistinctValueOnce) {
    constexpr bool stable = TypeParam::value;
    using Data = AggregateFunctionDistinctSingleGenericData<stable>;
    // 100 values outgrow the arena's first chunk.
    auto values = distinct_values(100);
    values.emplace_back();
    auto column = cycling_column(values, 20000);
    const IColumn* columns[] = {column.get()};
    Data data;
    Arena arena;

    for (size_t row = 0; row < column->size(); ++row) {
        data.add(columns, 1, row, arena);
    }

    // Copying before the lookup kept a copy of every non-empty row: 19,802 * 201 bytes.
    EXPECT_EQ(arena.used_size(), 100 * kValueSize);
    std::ranges::fill(column->get_chars(), '?');
    auto arguments = data.get_arguments({std::make_shared<DataTypeString>()});
    if constexpr (stable) {
        EXPECT_EQ(column_values(*arguments[0]), values);
    } else {
        EXPECT_EQ(sorted(column_values(*arguments[0])), sorted(values));
    }
}

TYPED_TEST(DistinctGenericArenaTest, MultipleAddRollsBackDuplicateKeys) {
    constexpr bool stable = TypeParam::value;
    using Data = AggregateFunctionDistinctMultipleGenericData<stable>;
    const auto values = distinct_values(10);
    auto first = cycling_column(values, 20000);
    auto second = cycling_column({"|"}, first->size());
    const IColumn* columns[] = {first.get(), second.get()};
    Data data;
    Arena arena;

    for (size_t row = 0; row < values.size(); ++row) {
        data.add(columns, 2, row, arena);
    }
    const size_t used_by_distinct_keys = arena.used_size();
    for (size_t row = values.size(); row < first->size(); ++row) {
        data.add(columns, 2, row, arena);
    }

    // A duplicate is serialized to be looked up, then rolled back.
    EXPECT_EQ(arena.used_size(), used_by_distinct_keys);
    std::ranges::fill(first->get_chars(), '?');
    auto arguments = data.get_arguments(
            {std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()});
    if constexpr (stable) {
        EXPECT_EQ(column_values(*arguments[0]), values);
    } else {
        EXPECT_EQ(sorted(column_values(*arguments[0])), sorted(values));
    }
    EXPECT_EQ(column_values(*arguments[1]), std::vector<std::string>(values.size(), "|"));
}

TYPED_TEST(DistinctGenericArenaTest, MultipleAddRollsBackDuplicateKeyThatMovedToANewChunk) {
    constexpr bool stable = TypeParam::value;
    using Data = AggregateFunctionDistinctMultipleGenericData<stable>;
    auto first = cycling_column({"a"}, 100);
    auto second = cycling_column(distinct_values(1), first->size());
    const IColumn* columns[] = {first.get(), second.get()};
    const size_t first_size = columns[0]->serialize_size_at(0);
    const size_t key_size = first_size + columns[1]->serialize_size_at(0);
    // The first chunk holds one key, plus room for the first column of the next row only.
    Arena arena(key_size + first_size + 15 /* the arena's right padding */);
    Data data;

    data.add(columns, 2, 0, arena);
    EXPECT_EQ(arena.used_size(), key_size);
    // The duplicate's key outgrows the chunk and is copied, as one range, to a new chunk. The
    // rollback frees that range; the copy of the first column left in the old chunk stays.
    data.add(columns, 2, 1, arena);
    EXPECT_EQ(arena.used_size(), key_size + first_size);
    for (size_t row = 2; row < first->size(); ++row) {
        data.add(columns, 2, row, arena);
    }
    EXPECT_EQ(arena.used_size(), key_size + first_size);

    auto arguments = data.get_arguments(
            {std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()});
    EXPECT_EQ(column_values(*arguments[0]), std::vector<std::string> {"a"});
    EXPECT_EQ(column_values(*arguments[1]), distinct_values(1));
}

TEST(DistinctGenericArenaMergeTest, MergeCopiesOnlyNewKeys) {
    using Data = AggregateFunctionDistinctSingleGenericData<false>;
    const auto values = distinct_values(10);
    Data destination;
    Arena arena;
    {
        auto column = cycling_column(values, values.size());
        const IColumn* columns[] = {column.get()};
        Data source;
        Arena source_arena;
        for (size_t row = 0; row < column->size(); ++row) {
            source.add(columns, 1, row, source_arena);
        }
        for (int repeat = 0; repeat < 1000; ++repeat) {
            destination.merge(source, arena);
        }
    }

    // Copying before the lookup used 1,000 * 10 * 201 bytes here.
    EXPECT_EQ(arena.used_size(), 10 * kValueSize);
    auto arguments = destination.get_arguments({std::make_shared<DataTypeString>()});
    EXPECT_EQ(sorted(column_values(*arguments[0])), sorted(values));
}

TEST(DistinctGenericArenaMergeTest, DeserializeAndMergeCopiesOnlyNewKeys) {
    using Data = AggregateFunctionDistinctSingleGenericData<false>;
    const auto values = distinct_values(10);
    auto serialized = ColumnString::create();
    {
        auto column = cycling_column(values, values.size());
        const IColumn* columns[] = {column.get()};
        Data source;
        Arena source_arena;
        for (size_t row = 0; row < column->size(); ++row) {
            source.add(columns, 1, row, source_arena);
        }
        VectorBufferWriter writer(*serialized);
        source.serialize(writer);
        writer.commit();
    }

    Data destination;
    Arena arena;
    for (int repeat = 0; repeat < 100; ++repeat) {
        Data scratch;
        VectorBufferReader reader(serialized->get_data_at(0));
        destination.deserialize_and_merge(scratch, reader, arena);
    }

    // Copying before the lookup used 100 * 10 * 201 bytes here.
    EXPECT_EQ(arena.used_size(), 10 * kValueSize);
    std::ranges::fill(serialized->get_chars(), '?');
    auto arguments = destination.get_arguments({std::make_shared<DataTypeString>()});
    EXPECT_EQ(sorted(column_values(*arguments[0])), sorted(values));
}

} // namespace
} // namespace doris
