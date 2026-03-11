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

#include <new>
#include <set>

#include "core/data_type/data_type_number.h"
#include "exec/common/agg_utils.h"
#include "exec/common/columns_hashing.h"
#include "exec/common/hash_table/hash.h"
#include "exec/common/hash_table/hash_map_context.h"
#include "exec/common/hash_table/ph_hash_map.h"
#include "exec/common/hash_table/ph_hash_set.h"
#include "testutil/column_helper.h"

namespace doris {

template <typename HashMethodType>
void test_insert(HashMethodType& method, Columns column) {
    using State = typename HashMethodType::State;
    ColumnRawPtrs key_raw_columns;
    for (auto column : column) {
        key_raw_columns.push_back(column.get());
    }
    State state(key_raw_columns);
    const size_t rows = key_raw_columns[0]->size();
    method.init_serialized_keys(key_raw_columns, rows);

    for (int i = 0; i < rows; i++) {
        auto creator = [&](const auto& ctor, auto& key, auto& origin) { ctor(key, i); };

        auto creator_for_null_key = [&](auto& mapped) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                   "no null key"); // NOLINT
        };
        method.lazy_emplace(state, i, creator, creator_for_null_key);
    }
}

template <typename HashMethodType>
void test_find(HashMethodType& method, Columns column, const std::vector<int64_t>& except_result) {
    using State = typename HashMethodType::State;
    ColumnRawPtrs key_raw_columns;
    for (auto column : column) {
        key_raw_columns.push_back(column.get());
    }
    State state(key_raw_columns);
    const size_t rows = key_raw_columns[0]->size();
    method.init_serialized_keys(key_raw_columns, rows);
    for (size_t i = 0; i < rows; ++i) {
        auto find_result = method.find(state, i);
        if (find_result.is_found()) {
            EXPECT_EQ(except_result[i], find_result.get_mapped());
        } else {
            EXPECT_EQ(except_result[i], -1); // not found
        }
    }
}

TEST(HashTableMethodTest, testMethodOneNumber) {
    MethodOneNumber<UInt32, PHHashMap<UInt32, IColumn::ColumnIndex, HashCRC32<UInt32>>> method;

    test_insert(method, {ColumnHelper::create_column<DataTypeInt32>({1, 2, 3, 4, 5})});

    test_find(method, {ColumnHelper::create_column<DataTypeInt32>({1, 2, 3, 4, 5})},
              {0, 1, 2, 3, 4});

    test_find(method, {ColumnHelper::create_column<DataTypeInt32>({1, 2, 7, 4, 6, 5})},
              {0, 1, -1, 3, -1, 4});
}

TEST(HashTableMethodTest, testMethodFixed) {
    MethodKeysFixed<PHHashMap<UInt64, IColumn::ColumnIndex, HashCRC32<UInt64>>> method(
            Sizes {sizeof(int), sizeof(int)});

    test_insert(method, {ColumnHelper::create_column<DataTypeInt32>({1, 2, 3, 4, 5}),
                         ColumnHelper::create_column<DataTypeInt32>({1, 2, 3, 4, 5})});

    test_find(method,
              {ColumnHelper::create_column<DataTypeInt32>({1, 2, 3, 4, 5}),
               ColumnHelper::create_column<DataTypeInt32>({1, 2, 3, 4, 5})},
              {0, 1, 2, 3, 4});

    test_find(method,
              {ColumnHelper::create_column<DataTypeInt32>({1, 2, 7, 4, 6, 5}),
               ColumnHelper::create_column<DataTypeInt32>({1, 2, 7, 4, 6, 5})},
              {0, 1, -1, 3, -1, 4});
}

TEST(HashTableMethodTest, testMethodSerialized) {
    MethodSerialized<StringHashMap<IColumn::ColumnIndex>> method;

    test_insert(method, {ColumnHelper::create_column<DataTypeInt32>({1, 2, 3, 4, 5}),
                         ColumnHelper::create_column<DataTypeString>({"1", "2", "3", "4", "5"})});

    test_find(method,
              {ColumnHelper::create_column<DataTypeInt32>({1, 2, 3, 4, 5}),
               ColumnHelper::create_column<DataTypeString>({"1", "2", "3", "4", "5"})},
              {0, 1, 2, 3, 4});

    test_find(method,
              {ColumnHelper::create_column<DataTypeInt32>({1, 2, 7, 4, 6, 5}),
               ColumnHelper::create_column<DataTypeString>({"1", "2", "7", "4", "6", "5"})},
              {0, 1, -1, 3, -1, 4});
}

TEST(HashTableMethodTest, testMethodStringNoCache) {
    MethodStringNoCache<StringHashMap<IColumn::ColumnIndex>> method;

    test_insert(method, {ColumnHelper::create_column<DataTypeString>({"1", "2", "3", "4", "5"})});

    test_find(method, {ColumnHelper::create_column<DataTypeString>({"1", "2", "3", "4", "5"})},
              {0, 1, 2, 3, 4});

    test_find(method, {ColumnHelper::create_column<DataTypeString>({"1", "2", "7", "4", "6", "5"})},
              {0, 1, -1, 3, -1, 4});
}

static AggregateDataPtr make_mapped(size_t val) {
    return reinterpret_cast<AggregateDataPtr>(val);
}

struct TrackedNullAggregateState {
    explicit TrackedNullAggregateState(size_t& destroy_count_) : destroy_count(destroy_count_) {}
    ~TrackedNullAggregateState() { ++destroy_count; }

    size_t& destroy_count;
};

static void create_null_state_then_fail(AggregateDataPtr& mapped, void* storage,
                                        size_t& destroy_count) {
    auto* new_state = new (storage) TrackedNullAggregateState(destroy_count);
    commit_aggregate_state(
            mapped, reinterpret_cast<AggregateDataPtr>(new_state),
            [] {
                throw Exception(ErrorCode::INTERNAL_ERROR,
                                "post-construction null key creation failed");
            },
            [](AggregateDataPtr state) {
                reinterpret_cast<TrackedNullAggregateState*>(state)->~TrackedNullAggregateState();
            });
}

TEST(HashTableMethodTest, testNullableNullKeyCreationExceptionSafety) {
    using NullableMethod =
            MethodSingleNullableColumn<MethodOneNumber<UInt32, AggDataNullable<UInt32>>>;
    NullableMethod method;
    using State = NullableMethod::State;

    auto col = ColumnHelper::create_nullable_column<DataTypeInt32>({0}, {1});
    ColumnRawPtrs key_columns = {col.get()};
    State state(key_columns);
    method.init_serialized_keys(key_columns, 1);

    EXPECT_THROW(method.lazy_emplace(
                         state, 0, [](const auto&, auto&, auto&) {},
                         [](auto&) {
                             throw Exception(ErrorCode::INTERNAL_ERROR, "null key creation failed");
                         }),
                 Exception);
    EXPECT_FALSE(method.hash_table->has_null_key_data());
    EXPECT_TRUE(method.hash_table->empty());
    EXPECT_FALSE(method.find(state, 0).is_found());

    alignas(TrackedNullAggregateState) char state_storage[sizeof(TrackedNullAggregateState)];
    size_t destroy_count = 0;
    EXPECT_THROW(method.lazy_emplace(
                         state, 0, [](const auto&, auto&, auto&) {},
                         [&](auto& null_mapped) {
                             create_null_state_then_fail(null_mapped, state_storage, destroy_count);
                         }),
                 Exception);
    EXPECT_EQ(destroy_count, 1);
    EXPECT_FALSE(method.hash_table->has_null_key_data());
    EXPECT_EQ(method.hash_table->get_null_key_data<AggregateDataPtr>(), nullptr);
    EXPECT_TRUE(method.hash_table->empty());
    EXPECT_FALSE(method.find(state, 0).is_found());

    auto* mapped = method.lazy_emplace(
            state, 0, [](const auto&, auto&, auto&) {},
            [](auto& null_mapped) { null_mapped = make_mapped(123); });
    ASSERT_NE(mapped, nullptr);
    EXPECT_EQ(*mapped, make_mapped(123));
    EXPECT_TRUE(method.hash_table->has_null_key_data());
    EXPECT_EQ(method.hash_table->size(), 1);
}

TEST(HashTableMethodTest, testNullableVoidNullKeyCreationExceptionSafety) {
    using NullableMethod = MethodSingleNullableColumn<
            MethodOneNumber<UInt32, DataWithNullKey<PHHashSet<UInt32, HashCRC32<UInt32>>>>>;
    NullableMethod method;
    using State = NullableMethod::State;

    auto col = ColumnHelper::create_nullable_column<DataTypeInt32>({0}, {1});
    ColumnRawPtrs key_columns = {col.get()};
    State state(key_columns);
    method.init_serialized_keys(key_columns, 1);

    EXPECT_THROW(
            method.lazy_emplace(
                    state, 0, [](const auto&, auto&, auto&) {},
                    [] { throw Exception(ErrorCode::INTERNAL_ERROR, "null key creation failed"); }),
            Exception);
    EXPECT_FALSE(method.hash_table->has_null_key_data());
    EXPECT_TRUE(method.hash_table->empty());

    method.lazy_emplace(
            state, 0, [](const auto&, auto&, auto&) {}, [] {});
    EXPECT_TRUE(method.hash_table->has_null_key_data());
    EXPECT_EQ(method.hash_table->size(), 1);
}

TEST(HashTableMethodTest, testNullableStringBatchNullKeyCreationExceptionSafety) {
    using NullableMethod = MethodSingleNullableColumn<
            MethodStringNoCache<AggregatedDataWithNullableShortStringKey>>;
    NullableMethod method;
    using State = NullableMethod::State;

    auto col = ColumnHelper::create_nullable_column<DataTypeString>({""}, {1});
    ColumnRawPtrs key_columns = {col.get()};
    State state(key_columns);
    method.init_serialized_keys(key_columns, 1);

    EXPECT_THROW(lazy_emplace_batch(
                         method, state, 1, [](const auto&, auto&, auto&) {},
                         [](auto&) {
                             throw Exception(ErrorCode::INTERNAL_ERROR, "null key creation failed");
                         },
                         [](uint32_t, auto&) {}),
                 Exception);
    EXPECT_FALSE(method.hash_table->has_null_key_data());
    EXPECT_TRUE(method.hash_table->empty());

    alignas(TrackedNullAggregateState) char state_storage[sizeof(TrackedNullAggregateState)];
    size_t destroy_count = 0;
    EXPECT_THROW(lazy_emplace_batch(
                         method, state, 1, [](const auto&, auto&, auto&) {},
                         [&](auto& null_mapped) {
                             create_null_state_then_fail(null_mapped, state_storage, destroy_count);
                         },
                         [](uint32_t, auto&) {}),
                 Exception);
    EXPECT_EQ(destroy_count, 1);
    EXPECT_FALSE(method.hash_table->has_null_key_data());
    EXPECT_EQ(method.hash_table->get_null_key_data<AggregateDataPtr>(), nullptr);
    EXPECT_TRUE(method.hash_table->empty());

    bool result_handled = false;
    lazy_emplace_batch(
            method, state, 1, [](const auto&, auto&, auto&) {},
            [](auto& null_mapped) { null_mapped = make_mapped(456); },
            [&](uint32_t row, auto& mapped) {
                EXPECT_EQ(row, 0);
                EXPECT_EQ(mapped, make_mapped(456));
                result_handled = true;
            });
    EXPECT_TRUE(result_handled);
    EXPECT_TRUE(method.hash_table->has_null_key_data());
    EXPECT_EQ(method.hash_table->size(), 1);
}

// Verify that iterating a DataWithNullKey hash map via init_iterator()/begin/end
// does NOT visit the null key entry. The null key must be accessed separately
// through has_null_key_data()/get_null_key_data().
TEST(HashTableMethodTest, testNullableIteratorSkipsNullKey) {
    using NullableMethod = MethodSingleNullableColumn<MethodOneNumber<
            UInt32, DataWithNullKey<PHHashMap<UInt32, IColumn::ColumnIndex, HashCRC32<UInt32>>>>>;
    NullableMethod method;

    // data: {1, 0(null), 2, 0(null), 3}
    // null_map: {0, 1, 0, 1, 0} — positions 1 and 3 are null
    auto nullable_col =
            ColumnHelper::create_nullable_column<DataTypeInt32>({1, 0, 2, 0, 3}, {0, 1, 0, 1, 0});

    // Insert all rows including nulls
    {
        using State = typename NullableMethod::State;
        ColumnRawPtrs key_raw_columns {nullable_col.get()};
        State state(key_raw_columns);
        const size_t rows = nullable_col->size();
        method.init_serialized_keys(key_raw_columns, rows);

        for (size_t i = 0; i < rows; i++) {
            IColumn::ColumnIndex mapped_value = i;
            auto creator = [&](const auto& ctor, auto& key, auto& origin) {
                ctor(key, mapped_value);
            };
            auto creator_for_null_key = [&](auto& mapped) { mapped = mapped_value; };
            method.lazy_emplace(state, i, creator, creator_for_null_key);
        }
    }

    // hash_table->size() includes null key: 3 non-null + 1 null = 4
    EXPECT_EQ(method.hash_table->size(), 4);

    // The underlying hash map (excluding null) has 3 entries
    EXPECT_TRUE(method.hash_table->has_null_key_data());

    // Iterate via init_iterator — should only visit 3 non-null entries
    method.init_iterator();
    size_t iter_count = 0;
    std::set<IColumn::ColumnIndex> visited_values;
    auto iter = method.begin;
    while (iter != method.end) {
        visited_values.insert(iter.get_second());
        ++iter;
        ++iter_count;
    }

    // Iterator must visit exactly 3 entries (the non-null keys: 1, 2, 3)
    EXPECT_EQ(iter_count, 3);
    // Mapped values for non-null rows are 0 (key=1), 2 (key=2), 4 (key=3)
    EXPECT_TRUE(visited_values.count(0)); // row 0: key=1
    EXPECT_TRUE(visited_values.count(2)); // row 2: key=2
    EXPECT_TRUE(visited_values.count(4)); // row 4: key=3
    // The null key's mapped value (1, from the first null row) must NOT appear in iteration
    EXPECT_FALSE(visited_values.count(1));

    // Null key must be accessible separately
    auto null_mapped = method.hash_table->template get_null_key_data<IColumn::ColumnIndex>();
    EXPECT_EQ(null_mapped, 1); // first null insertion at row 1

    // find should locate null keys correctly
    {
        using State = typename NullableMethod::State;
        // Search: {1, null, 99, 2}
        auto search_col =
                ColumnHelper::create_nullable_column<DataTypeInt32>({1, 0, 99, 2}, {0, 1, 0, 0});
        ColumnRawPtrs key_raw_columns {search_col.get()};
        State state(key_raw_columns);
        method.init_serialized_keys(key_raw_columns, 4);

        // key=1 found
        auto r0 = method.find(state, 0);
        EXPECT_TRUE(r0.is_found());
        EXPECT_EQ(r0.get_mapped(), 0);

        // null found
        auto r1 = method.find(state, 1);
        EXPECT_TRUE(r1.is_found());
        EXPECT_EQ(r1.get_mapped(), 1);

        // key=99 not found
        auto r2 = method.find(state, 2);
        EXPECT_FALSE(r2.is_found());

        // key=2 found
        auto r3 = method.find(state, 3);
        EXPECT_TRUE(r3.is_found());
        EXPECT_EQ(r3.get_mapped(), 2);
    }
}

} // namespace doris
