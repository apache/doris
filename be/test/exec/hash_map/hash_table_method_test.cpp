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

#include <set>

#include "core/data_type/data_type_number.h"
#include "exec/common/agg_utils.h"
#include "exec/common/columns_hashing.h"
#include "exec/common/hash_table/hash.h"
#include "exec/common/hash_table/hash_map_context.h"
#include "exec/common/hash_table/ph_hash_map.h"
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

// Helper: create distinguishable AggregateDataPtr values for testing
static AggregateDataPtr make_mapped(size_t val) {
    return reinterpret_cast<AggregateDataPtr>(val);
}

// ========== MethodOneNumber<UInt32, AggData<UInt32>> ==========
// AggData<UInt32> = PHHashMap<UInt32, AggregateDataPtr, HashCRC32<UInt32>>
TEST(HashTableMethodTest, testMethodOneNumberAggInsertFindForEach) {
    MethodOneNumber<UInt32, AggData<UInt32>> method;
    using State = MethodOneNumber<UInt32, AggData<UInt32>>::State;

    auto col = ColumnHelper::create_column<DataTypeInt32>({10, 20, 30, 40, 50});
    ColumnRawPtrs key_columns = {col.get()};
    const size_t rows = 5;

    // Insert
    {
        State state(key_columns);
        method.init_serialized_keys(key_columns, rows);
        for (size_t i = 0; i < rows; i++) {
            method.lazy_emplace(
                    state, i,
                    [&](const auto& ctor, auto& key, auto& origin) {
                        ctor(key, make_mapped(i + 1));
                    },
                    [](auto& mapped) { FAIL() << "unexpected null"; });
        }
    }

    // Find existing keys
    {
        State state(key_columns);
        method.init_serialized_keys(key_columns, rows);
        for (size_t i = 0; i < rows; i++) {
            auto result = method.find(state, i);
            ASSERT_TRUE(result.is_found());
            EXPECT_EQ(result.get_mapped(), make_mapped(i + 1));
        }
    }

    // Find non-existing key
    {
        auto miss_col = ColumnHelper::create_column<DataTypeInt32>({999});
        ColumnRawPtrs miss_columns = {miss_col.get()};
        State state(miss_columns);
        method.init_serialized_keys(miss_columns, 1);
        auto result = method.find(state, 0);
        EXPECT_FALSE(result.is_found());
    }

    // for_each
    {
        size_t count = 0;
        method.hash_table->for_each([&](const auto& key, auto& mapped) {
            EXPECT_NE(mapped, nullptr);
            count++;
        });
        EXPECT_EQ(count, 5);
    }

    // for_each_mapped
    {
        size_t count = 0;
        method.hash_table->for_each_mapped([&](auto& mapped) {
            EXPECT_NE(mapped, nullptr);
            count++;
        });
        EXPECT_EQ(count, 5);
    }
}

// ========== MethodOneNumber Phase2 (HashMixWrapper) ==========
// AggregatedDataWithUInt32KeyPhase2 = PHHashMap<UInt32, AggregateDataPtr, HashMixWrapper<UInt32>>
TEST(HashTableMethodTest, testMethodOneNumberPhase2AggInsertFindForEach) {
    MethodOneNumber<UInt32, AggregatedDataWithUInt32KeyPhase2> method;
    using State = MethodOneNumber<UInt32, AggregatedDataWithUInt32KeyPhase2>::State;

    auto col = ColumnHelper::create_column<DataTypeInt32>({100, 200, 300});
    ColumnRawPtrs key_columns = {col.get()};
    const size_t rows = 3;

    // Insert
    {
        State state(key_columns);
        method.init_serialized_keys(key_columns, rows);
        for (size_t i = 0; i < rows; i++) {
            method.lazy_emplace(
                    state, i,
                    [&](const auto& ctor, auto& key, auto& origin) {
                        ctor(key, make_mapped(i + 100));
                    },
                    [](auto& mapped) { FAIL(); });
        }
    }

    // Find
    {
        State state(key_columns);
        method.init_serialized_keys(key_columns, rows);
        for (size_t i = 0; i < rows; i++) {
            auto result = method.find(state, i);
            ASSERT_TRUE(result.is_found());
            EXPECT_EQ(result.get_mapped(), make_mapped(i + 100));
        }
    }

    // for_each + for_each_mapped
    {
        size_t count = 0;
        method.hash_table->for_each([&](const auto& key, auto& mapped) { count++; });
        EXPECT_EQ(count, 3);
    }
    {
        size_t count = 0;
        method.hash_table->for_each_mapped([&](auto& mapped) { count++; });
        EXPECT_EQ(count, 3);
    }
}

// ========== MethodStringNoCache<AggregatedDataWithShortStringKey> ==========
// AggregatedDataWithShortStringKey = StringHashMap<AggregateDataPtr>
TEST(HashTableMethodTest, testMethodStringNoCacheAggInsertFindForEach) {
    MethodStringNoCache<AggregatedDataWithShortStringKey> method;
    using State = MethodStringNoCache<AggregatedDataWithShortStringKey>::State;

    // Include strings of varying lengths to exercise different StringHashMap sub-maps
    auto col = ColumnHelper::create_column<DataTypeString>(
            {"hello", "world", "foo", "bar", "longstring_exceeding_16_bytes"});
    ColumnRawPtrs key_columns = {col.get()};
    const size_t rows = 5;

    // Insert
    {
        State state(key_columns);
        method.init_serialized_keys(key_columns, rows);
        for (size_t i = 0; i < rows; i++) {
            method.lazy_emplace(
                    state, i,
                    [&](const auto& ctor, auto& key, auto& origin) {
                        ctor(key, make_mapped(i + 10));
                    },
                    [](auto& mapped) { FAIL(); });
        }
    }

    // Find
    {
        State state(key_columns);
        method.init_serialized_keys(key_columns, rows);
        for (size_t i = 0; i < rows; i++) {
            auto result = method.find(state, i);
            ASSERT_TRUE(result.is_found());
            EXPECT_EQ(result.get_mapped(), make_mapped(i + 10));
        }
    }

    // for_each
    {
        size_t count = 0;
        method.hash_table->for_each([&](const auto& key, auto& mapped) {
            EXPECT_NE(mapped, nullptr);
            count++;
        });
        EXPECT_EQ(count, 5);
    }

    // for_each_mapped
    {
        size_t count = 0;
        method.hash_table->for_each_mapped([&](auto& mapped) { count++; });
        EXPECT_EQ(count, 5);
    }
}

// ========== MethodSerialized<AggregatedDataWithStringKey> ==========
// AggregatedDataWithStringKey = PHHashMap<StringRef, AggregateDataPtr>
// StringRef keys require arena persistence to survive across init_serialized_keys calls.
TEST(HashTableMethodTest, testMethodSerializedAggInsertFindForEach) {
    MethodSerialized<AggregatedDataWithStringKey> method;
    using State = MethodSerialized<AggregatedDataWithStringKey>::State;

    auto col1 = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3});
    auto col2 = ColumnHelper::create_column<DataTypeString>({"a", "bb", "ccc"});
    ColumnRawPtrs key_columns = {col1.get(), col2.get()};
    const size_t rows = 3;

    // Use a separate arena to persist StringRef key data
    Arena persist_arena;

    // Insert
    {
        State state(key_columns);
        method.init_serialized_keys(key_columns, rows);
        for (size_t i = 0; i < rows; i++) {
            method.lazy_emplace(
                    state, i,
                    [&](const auto& ctor, auto& key, auto& origin) {
                        method.try_presis_key_and_origin(key, origin, persist_arena);
                        ctor(key, make_mapped(i + 50));
                    },
                    [](auto& mapped) { FAIL(); });
        }
    }

    // for_each (keys backed by persist_arena)
    {
        size_t count = 0;
        method.hash_table->for_each([&](const auto& key, auto& mapped) {
            EXPECT_GT(key.size, 0);
            EXPECT_NE(mapped, nullptr);
            count++;
        });
        EXPECT_EQ(count, 3);
    }

    // for_each_mapped
    {
        size_t count = 0;
        method.hash_table->for_each_mapped([&](auto& mapped) {
            EXPECT_NE(mapped, nullptr);
            count++;
        });
        EXPECT_EQ(count, 3);
    }

    // Find (re-init serialized keys for lookup)
    {
        State state(key_columns);
        method.init_serialized_keys(key_columns, rows);
        for (size_t i = 0; i < rows; i++) {
            auto result = method.find(state, i);
            ASSERT_TRUE(result.is_found());
            EXPECT_EQ(result.get_mapped(), make_mapped(i + 50));
        }
    }
}

// ========== MethodKeysFixed<AggData<UInt64>> ==========
// Fixed-width multi-column keys packed into UInt64
TEST(HashTableMethodTest, testMethodKeysFixedAggInsertFindForEach) {
    MethodKeysFixed<AggData<UInt64>> method(Sizes {sizeof(int32_t), sizeof(int32_t)});
    using State = MethodKeysFixed<AggData<UInt64>>::State;

    auto col1 = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3, 4});
    auto col2 = ColumnHelper::create_column<DataTypeInt32>({10, 20, 30, 40});
    ColumnRawPtrs key_columns = {col1.get(), col2.get()};
    const size_t rows = 4;

    // Insert
    {
        State state(key_columns);
        method.init_serialized_keys(key_columns, rows);
        for (size_t i = 0; i < rows; i++) {
            method.lazy_emplace(
                    state, i,
                    [&](const auto& ctor, auto& key, auto& origin) {
                        ctor(key, make_mapped(i + 200));
                    },
                    [](auto& mapped) { FAIL(); });
        }
    }

    // Find
    {
        State state(key_columns);
        method.init_serialized_keys(key_columns, rows);
        for (size_t i = 0; i < rows; i++) {
            auto result = method.find(state, i);
            ASSERT_TRUE(result.is_found());
            EXPECT_EQ(result.get_mapped(), make_mapped(i + 200));
        }
    }

    // for_each
    {
        size_t count = 0;
        method.hash_table->for_each([&](const auto& key, auto& mapped) {
            EXPECT_NE(mapped, nullptr);
            count++;
        });
        EXPECT_EQ(count, 4);
    }
}

// ========== Nullable MethodOneNumber (MethodSingleNullableColumn + MethodOneNumber) ==========
// AggDataNullable<UInt32> = DataWithNullKey<PHHashMap<UInt32, AggregateDataPtr, HashCRC32<UInt32>>>
// Tests null key insertion, find, and for_each (which excludes null from PHHashMap::for_each).
TEST(HashTableMethodTest, testNullableMethodOneNumberAggInsertFindForEach) {
    using NullableMethod =
            MethodSingleNullableColumn<MethodOneNumber<UInt32, AggDataNullable<UInt32>>>;
    NullableMethod method;
    using State = NullableMethod::State;

    // values: {10, 20, 30, 40, 50}, null at rows 1 and 3
    auto col = ColumnHelper::create_nullable_column<DataTypeInt32>({10, 20, 30, 40, 50},
                                                                   {0, 1, 0, 1, 0});
    ColumnRawPtrs key_columns = {col.get()};
    const size_t rows = 5;

    // Insert
    size_t null_create_count = 0;
    {
        State state(key_columns);
        method.init_serialized_keys(key_columns, rows);
        for (size_t i = 0; i < rows; i++) {
            method.lazy_emplace(
                    state, i,
                    [&](const auto& ctor, auto& key, auto& origin) {
                        ctor(key, make_mapped(i + 1));
                    },
                    [&](auto& mapped) {
                        null_create_count++;
                        mapped = make_mapped(999);
                    });
        }
    }

    // null_creator called once for first null row (index 1); second null (index 3) is deduplicated
    EXPECT_EQ(null_create_count, 1);

    auto& ht = *method.hash_table;
    EXPECT_TRUE(ht.has_null_key_data());
    EXPECT_EQ(ht.get_null_key_data<AggregateDataPtr>(), make_mapped(999));

    // Find
    {
        State state(key_columns);
        method.init_serialized_keys(key_columns, rows);

        // row 0: key=10, non-null
        auto r0 = method.find(state, 0);
        ASSERT_TRUE(r0.is_found());
        EXPECT_EQ(r0.get_mapped(), make_mapped(1));

        // row 1: null → returns null key data
        auto r1 = method.find(state, 1);
        ASSERT_TRUE(r1.is_found());
        EXPECT_EQ(r1.get_mapped(), make_mapped(999));

        // row 2: key=30, non-null
        auto r2 = method.find(state, 2);
        ASSERT_TRUE(r2.is_found());
        EXPECT_EQ(r2.get_mapped(), make_mapped(3));

        // row 4: key=50, non-null
        auto r4 = method.find(state, 4);
        ASSERT_TRUE(r4.is_found());
        EXPECT_EQ(r4.get_mapped(), make_mapped(5));
    }

    // for_each_mapped: PHHashMap::for_each_mapped iterates only non-null keys
    {
        size_t count = 0;
        ht.for_each_mapped([&](auto& mapped) { count++; });
        EXPECT_EQ(count, 3); // keys 10, 30, 50
    }

    // DataWithNullKey::size() includes null key
    EXPECT_EQ(ht.size(), 4); // 3 non-null + 1 null
}

// ========== Nullable MethodStringNoCache ==========
// AggregatedDataWithNullableShortStringKey = DataWithNullKey<StringHashMap<AggregateDataPtr>>
TEST(HashTableMethodTest, testNullableMethodStringNoCacheAggInsertFindForEach) {
    using NullableMethod = MethodSingleNullableColumn<
            MethodStringNoCache<AggregatedDataWithNullableShortStringKey>>;
    NullableMethod method;
    using State = NullableMethod::State;

    // values: {"hello", <null>, "world", <null>}
    auto col = ColumnHelper::create_nullable_column<DataTypeString>({"hello", "", "world", ""},
                                                                    {0, 1, 0, 1});
    ColumnRawPtrs key_columns = {col.get()};
    const size_t rows = 4;

    // Insert
    size_t null_create_count = 0;
    {
        State state(key_columns);
        method.init_serialized_keys(key_columns, rows);
        for (size_t i = 0; i < rows; i++) {
            method.lazy_emplace(
                    state, i,
                    [&](const auto& ctor, auto& key, auto& origin) {
                        ctor(key, make_mapped(i + 1));
                    },
                    [&](auto& mapped) {
                        null_create_count++;
                        mapped = make_mapped(888);
                    });
        }
    }

    EXPECT_EQ(null_create_count, 1);

    auto& ht = *method.hash_table;
    EXPECT_TRUE(ht.has_null_key_data());
    EXPECT_EQ(ht.get_null_key_data<AggregateDataPtr>(), make_mapped(888));

    // Find
    {
        State state(key_columns);
        method.init_serialized_keys(key_columns, rows);

        // row 0: "hello"
        auto r0 = method.find(state, 0);
        ASSERT_TRUE(r0.is_found());
        EXPECT_EQ(r0.get_mapped(), make_mapped(1));

        // row 1: null
        auto r1 = method.find(state, 1);
        ASSERT_TRUE(r1.is_found());
        EXPECT_EQ(r1.get_mapped(), make_mapped(888));

        // row 2: "world"
        auto r2 = method.find(state, 2);
        ASSERT_TRUE(r2.is_found());
        EXPECT_EQ(r2.get_mapped(), make_mapped(3));
    }

    // for_each: StringHashMap::for_each iterates only non-null keys
    {
        size_t count = 0;
        ht.for_each([&](const auto& key, auto& mapped) { count++; });
        EXPECT_EQ(count, 2); // "hello" and "world"
    }

    // DataWithNullKey::size() includes null key
    EXPECT_EQ(ht.size(), 3); // 2 non-null + 1 null
}

// ========== PHHashMap iterator: traverse, sort, verify, assignment ==========
TEST(HashTableMethodTest, testPHHashMapIterator) {
    MethodOneNumber<UInt32, AggData<UInt32>> method;
    using State = MethodOneNumber<UInt32, AggData<UInt32>>::State;

    auto col = ColumnHelper::create_column<DataTypeInt32>({50, 10, 40, 20, 30});
    ColumnRawPtrs key_columns = {col.get()};
    const size_t rows = 5;

    State state(key_columns);
    method.init_serialized_keys(key_columns, rows);
    for (size_t i = 0; i < rows; i++) {
        method.lazy_emplace(
                state, i,
                [&](const auto& ctor, auto& key, auto& origin) { ctor(key, make_mapped(i + 1)); },
                [](auto& mapped) { FAIL(); });
    }

    auto& ht = *method.hash_table;

    // Collect all (key, mapped) pairs via iterator
    std::vector<std::pair<UInt32, AggregateDataPtr>> entries;
    for (auto it = ht.begin(); it != ht.end(); ++it) {
        entries.emplace_back(it->get_first(), it->get_second());
    }
    ASSERT_EQ(entries.size(), 5);

    // Sort by key and verify
    std::sort(entries.begin(), entries.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });
    // Inserted: {50→1, 10→2, 40→3, 20→4, 30→5}
    EXPECT_EQ(entries[0].first, 10);
    EXPECT_EQ(entries[0].second, make_mapped(2));
    EXPECT_EQ(entries[1].first, 20);
    EXPECT_EQ(entries[1].second, make_mapped(4));
    EXPECT_EQ(entries[2].first, 30);
    EXPECT_EQ(entries[2].second, make_mapped(5));
    EXPECT_EQ(entries[3].first, 40);
    EXPECT_EQ(entries[3].second, make_mapped(3));
    EXPECT_EQ(entries[4].first, 50);
    EXPECT_EQ(entries[4].second, make_mapped(1));

    // Iterator assignment: it = begin(), it2 = it
    auto it = ht.begin();
    auto it2 = it; // copy
    EXPECT_TRUE(it == it2);
    EXPECT_EQ(it->get_first(), it2->get_first());
    EXPECT_EQ(it->get_second(), it2->get_second());

    ++it;
    EXPECT_TRUE(it != it2); // diverged after increment

    it2 = it; // reassignment
    EXPECT_TRUE(it == it2);
    EXPECT_EQ(it->get_first(), it2->get_first());

    // Empty hash table: begin == end
    MethodOneNumber<UInt32, AggData<UInt32>> empty_method;
    EXPECT_TRUE(empty_method.hash_table->begin() == empty_method.hash_table->end());
}

// ========== StringHashMap iterator: traverse, sort, verify, assignment ==========
TEST(HashTableMethodTest, testStringHashMapIterator) {
    MethodStringNoCache<AggregatedDataWithShortStringKey> method;
    using State = MethodStringNoCache<AggregatedDataWithShortStringKey>::State;

    // Different lengths to hit different sub-maps (m1: <=8, m2: <=16, m3: <=24, ms: >24)
    auto col = ColumnHelper::create_column<DataTypeString>(
            {"z", "ab", "hello_world_12345", "tiny", "a_very_long_string_over_24_chars"});
    ColumnRawPtrs key_columns = {col.get()};
    const size_t rows = 5;

    State state(key_columns);
    method.init_serialized_keys(key_columns, rows);
    for (size_t i = 0; i < rows; i++) {
        method.lazy_emplace(
                state, i,
                [&](const auto& ctor, auto& key, auto& origin) { ctor(key, make_mapped(i + 1)); },
                [](auto& mapped) { FAIL(); });
    }

    auto& ht = *method.hash_table;

    // Collect all (key_string, mapped) pairs via iterator
    std::vector<std::pair<std::string, AggregateDataPtr>> entries;
    for (auto it = ht.begin(); it != ht.end(); ++it) {
        auto key = it.get_first();
        entries.emplace_back(std::string(key.data, key.size), it.get_second());
    }
    ASSERT_EQ(entries.size(), 5);

    // Sort by key string and verify
    std::sort(entries.begin(), entries.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });
    // Sorted: "a_very_long...", "ab", "hello_world_12345", "tiny", "z"
    EXPECT_EQ(entries[0].first, "a_very_long_string_over_24_chars");
    EXPECT_EQ(entries[0].second, make_mapped(5));
    EXPECT_EQ(entries[1].first, "ab");
    EXPECT_EQ(entries[1].second, make_mapped(2));
    EXPECT_EQ(entries[2].first, "hello_world_12345");
    EXPECT_EQ(entries[2].second, make_mapped(3));
    EXPECT_EQ(entries[3].first, "tiny");
    EXPECT_EQ(entries[3].second, make_mapped(4));
    EXPECT_EQ(entries[4].first, "z");
    EXPECT_EQ(entries[4].second, make_mapped(1));

    // Iterator assignment: copy and reassign
    auto it = ht.begin();
    auto it2 = it;
    EXPECT_TRUE(it == it2);

    auto key1 = it.get_first();
    auto key2 = it2.get_first();
    EXPECT_EQ(key1, key2);

    ++it;
    EXPECT_TRUE(it != it2);

    it2 = it; // reassignment
    EXPECT_TRUE(it == it2);

    // Empty StringHashMap: begin == end
    MethodStringNoCache<AggregatedDataWithShortStringKey> empty_method;
    EXPECT_TRUE(empty_method.hash_table->begin() == empty_method.hash_table->end());
}

// ========== DataWithNullKey iterator: only non-null entries, null key accessed separately ==========
TEST(HashTableMethodTest, testDataWithNullKeyIterator) {
    using NullableMethod =
            MethodSingleNullableColumn<MethodOneNumber<UInt32, AggDataNullable<UInt32>>>;
    NullableMethod method;
    using State = NullableMethod::State;

    // values: {10, 20, 30}, null at row 1
    auto col = ColumnHelper::create_nullable_column<DataTypeInt32>({10, 20, 30}, {0, 1, 0});
    ColumnRawPtrs key_columns = {col.get()};
    const size_t rows = 3;

    State state(key_columns);
    method.init_serialized_keys(key_columns, rows);
    for (size_t i = 0; i < rows; i++) {
        method.lazy_emplace(
                state, i,
                [&](const auto& ctor, auto& key, auto& origin) { ctor(key, make_mapped(i + 1)); },
                [&](auto& mapped) { mapped = make_mapped(999); });
    }

    auto& ht = *method.hash_table;

    // Null key is present and must be accessed separately (not via iterator)
    EXPECT_TRUE(ht.has_null_key_data());
    EXPECT_EQ(ht.get_null_key_data<AggregateDataPtr>(), make_mapped(999));

    // DataWithNullKey::size() includes null key
    EXPECT_EQ(ht.size(), 3); // 2 non-null + 1 null

    // Iterator only visits non-null entries
    std::vector<std::pair<UInt32, AggregateDataPtr>> non_null_entries;
    for (auto it = ht.begin(); it != ht.end(); ++it) {
        non_null_entries.emplace_back(it->get_first(), it->get_second());
    }

    // Only 2 non-null entries in iteration (null key excluded)
    ASSERT_EQ(non_null_entries.size(), 2);
    std::sort(non_null_entries.begin(), non_null_entries.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });
    // Inserted: row0=10→1, row2=30→3
    EXPECT_EQ(non_null_entries[0].first, 10);
    EXPECT_EQ(non_null_entries[0].second, make_mapped(1));
    EXPECT_EQ(non_null_entries[1].first, 30);
    EXPECT_EQ(non_null_entries[1].second, make_mapped(3));

    // Iterator assignment
    auto it = ht.begin();
    auto it2 = it;
    EXPECT_TRUE(it == it2);
    EXPECT_EQ(it.get_second(), it2.get_second());

    ++it;
    EXPECT_TRUE(it != it2);

    it2 = it;
    EXPECT_TRUE(it == it2);
}
} // namespace doris
