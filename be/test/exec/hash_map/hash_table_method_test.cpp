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

#include "testutil/column_helper.h"
#include "vec/common/columns_hashing.h"
#include "vec/common/hash_table/hash.h"
#include "vec/common/hash_table/hash_map_context.h"
#include "vec/common/hash_table/ph_hash_map.h"
#include "vec/common/hash_table/string_hash_map.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

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

// Test to_string_ref bug: when T is smaller than 8 bytes (UInt16/UInt32),
// sizeof(T) - (__builtin_clzll(n) >> 3) underflows because __builtin_clzll
// always counts leading zeros in 64-bit, but the formula uses sizeof(T)
// instead of 8.
TEST(HashTableMethodTest, testToStringRefSmallKeys) {
    // to_string_ref<UInt16>: "a" → key=0x61
    //   clzll(0x61) = 57, 57>>3 = 7
    //   BUG: sizeof(UInt16) - 7 = 2 - 7 = underflow (huge size_t)
    //   FIX: 8 - 7 = 1 (correct)
    {
        StringKey2 key = to_string_key<StringKey2>(doris::StringRef("a", 1));
        auto ref = to_string_ref(key);
        EXPECT_EQ(ref.size, 1) << "to_string_ref(UInt16) returned size=" << ref.size
                                << ", expected 1. sizeof(T)-clzll>>3 underflow bug.";
        EXPECT_EQ(std::string(ref.data, ref.size), "a");
    }

    // to_string_ref<UInt16>: "ab" → key=0x6261
    //   clzll(0x6261) = 49, 49>>3 = 6
    //   BUG: sizeof(UInt16) - 6 = 2 - 6 = underflow
    //   FIX: 8 - 6 = 2 (correct)
    {
        StringKey2 key = to_string_key<StringKey2>(doris::StringRef("ab", 2));
        auto ref = to_string_ref(key);
        EXPECT_EQ(ref.size, 2) << "to_string_ref(UInt16) returned size=" << ref.size
                                << ", expected 2.";
        EXPECT_EQ(std::string(ref.data, ref.size), "ab");
    }

    // to_string_ref<UInt32>: "abc" → key=0x00636261
    //   clzll(0x636261) = 41, 41>>3 = 5
    //   BUG: sizeof(UInt32) - 5 = 4 - 5 = underflow
    //   FIX: 8 - 5 = 3 (correct)
    {
        StringKey4 key = to_string_key<StringKey4>(doris::StringRef("abc", 3));
        auto ref = to_string_ref(key);
        EXPECT_EQ(ref.size, 3) << "to_string_ref(UInt32) returned size=" << ref.size
                                << ", expected 3.";
        EXPECT_EQ(std::string(ref.data, ref.size), "abc");
    }

    // to_string_ref<UInt32>: "abcd" → key=0x64636261
    //   clzll(0x64636261) = 33, 33>>3 = 4
    //   BUG: sizeof(UInt32) - 4 = 4 - 4 = 0 (wrong, should be 4)
    //   FIX: 8 - 4 = 4 (correct)
    {
        StringKey4 key = to_string_key<StringKey4>(doris::StringRef("abcd", 4));
        auto ref = to_string_ref(key);
        EXPECT_EQ(ref.size, 4) << "to_string_ref(UInt32) returned size=" << ref.size
                                << ", expected 4.";
        EXPECT_EQ(std::string(ref.data, ref.size), "abcd");
    }

    // to_string_ref<UInt64>: should work correctly (sizeof == 8)
    {
        StringKey8 key = to_string_key<StringKey8>(doris::StringRef("abcde", 5));
        auto ref = to_string_ref(key);
        EXPECT_EQ(ref.size, 5) << "to_string_ref(UInt64) returned size=" << ref.size
                                << ", expected 5.";
        EXPECT_EQ(std::string(ref.data, ref.size), "abcde");
    }
}
} // namespace doris::vectorized