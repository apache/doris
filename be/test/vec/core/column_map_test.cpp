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

#include "vec/columns/column_map.h"

#include <gtest/gtest-death-test.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <cstdint>

#include "gtest/gtest_pred_impl.h"
#include "vec/columns/column.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris::vectorized {
TEST(ColumnMapTest2, StringKeyTest) {
    auto col_map_str64 = ColumnMap(ColumnString64::create(), ColumnInt64::create(),
                                   ColumnArray::ColumnOffsets::create());
    Array k1 = {"a", "b", "c"};
    Array v1 = {1, 2, 3};
    {
        Map map;
        map.push_back(k1);
        map.push_back(v1);
        col_map_str64.insert(map);
    }
    Array k2 = {"aa", "bb", "cc"};
    Array v2 = {11, 22, 33};
    {
        Map map;
        map.push_back(k2);
        map.push_back(v2);
        col_map_str64.insert(map);
    }
    Array k3 = {"aaa", "bbb", "ccc"};
    Array v3 = {111, 222, 333};
    {
        Map map;
        map.push_back(k3);
        map.push_back(v3);
        col_map_str64.insert(map);
    }

    // test insert ColumnMap<ColumnStr<uint64_t>, Column> into ColumnMap<ColumnStr<uint32_t>, Column>
    auto col_map_str32 = ColumnMap(ColumnString::create(), ColumnInt64::create(),
                                   ColumnArray::ColumnOffsets::create());
    std::vector<uint32_t> indices;
    indices.push_back(0);
    indices.push_back(2);
    col_map_str32.insert_indices_from(col_map_str64, indices.data(),
                                      indices.data() + indices.size());
    EXPECT_EQ(col_map_str32.size(), 2);

    auto map = get<Map>(col_map_str32[0]);
    auto k = get<Array>(map[0]);
    auto v = get<Array>(map[1]);
    EXPECT_EQ(k.size(), 3);
    for (size_t i = 0; i < k.size(); ++i) {
        EXPECT_EQ(k[i], k1[i]);
    }
    EXPECT_EQ(v.size(), 3);
    for (size_t i = 0; i < v.size(); ++i) {
        EXPECT_EQ(v[i], v1[i]);
    }

    map = get<Map>(col_map_str32[1]);
    k = get<Array>(map[0]);
    v = get<Array>(map[1]);
    EXPECT_EQ(k.size(), 3);
    for (size_t i = 0; i < k.size(); ++i) {
        EXPECT_EQ(k[i], k3[i]);
    }
    EXPECT_EQ(v.size(), 3);
    for (size_t i = 0; i < v.size(); ++i) {
        EXPECT_EQ(v[i], v3[i]);
    }
};

TEST(ColumnMapTest2, StringKeyTestDuplicatedKeys) {
    auto col_map_str = ColumnMap(
            ColumnNullable::create(ColumnString::create(), ColumnVector<uint8_t>::create()),
            ColumnInt32::create(), ColumnArray::ColumnOffsets::create());
    Array k1 = {"a", "b", "c", "a", "b", "c"};
    Array v1 = {1, 2, 3, 4, 5, 6};
    {
        Map map;
        map.push_back(k1);
        map.push_back(v1);
        col_map_str.insert(map);
    }
    {
        Map map;
        map.push_back(k1);
        map.push_back(v1);
        col_map_str.insert(map);
    }
    Array k2 = {"aa", "bb", "cc", "aa", "cc"};
    Array v2 = {11, 22, 33, 111, 333};
    {
        Map map;
        map.push_back(k2);
        map.push_back(v2);
        col_map_str.insert(map);
    }
    Array k3 = {"aaa", "bbb", Null(), "", "ccc", "ccc", "", Null()};
    Array v3 = {111, 222, 4321, 999, 333, 3333, 9988, 1234};
    {
        Map map;
        map.push_back(k3);
        map.push_back(v3);
        col_map_str.insert(map);
    }

    ASSERT_EQ(col_map_str.size(), 4);
    auto& keys = col_map_str.get_keys();
    auto& values = col_map_str.get_values();

    ASSERT_EQ(keys.size(), 25);
    ASSERT_EQ(keys.size(), values.size());

    auto st = col_map_str.deduplicate_keys();
    ASSERT_TRUE(st.ok()) << st.to_string();

    ASSERT_EQ(keys.size(), 14);
    ASSERT_EQ(keys.size(), values.size());

    auto& offsets = col_map_str.get_offsets();

    auto& nullable_keys = assert_cast<ColumnNullable&>(keys);
    auto& string_keys = assert_cast<ColumnString&>(nullable_keys.get_nested_column());
    auto& int_values = assert_cast<ColumnInt32&>(values);

    ASSERT_EQ(offsets.size(), 4);
    ASSERT_EQ(offsets[0], 3);
    ASSERT_EQ(offsets[1], 6);
    ASSERT_EQ(offsets[2], 9);
    ASSERT_EQ(offsets[3], 14);

    ASSERT_EQ(string_keys.get_element(0), "a");
    ASSERT_EQ(string_keys.get_element(1), "b");
    ASSERT_EQ(string_keys.get_element(2), "c");

    ASSERT_EQ(string_keys.get_element(3), "a");
    ASSERT_EQ(string_keys.get_element(4), "b");
    ASSERT_EQ(string_keys.get_element(5), "c");

    ASSERT_EQ(string_keys.get_element(6), "bb");
    ASSERT_EQ(string_keys.get_element(7), "aa");
    ASSERT_EQ(string_keys.get_element(8), "cc");

    ASSERT_EQ(string_keys.get_element(9), "aaa");
    ASSERT_EQ(string_keys.get_element(10), "bbb");
    ASSERT_EQ(string_keys.get_element(11), "ccc");
    ASSERT_EQ(string_keys.get_element(12), "");
    ASSERT_TRUE(nullable_keys.is_null_at(13));

    ASSERT_EQ(int_values.get_element(0), 4);
    ASSERT_EQ(int_values.get_element(1), 5);
    ASSERT_EQ(int_values.get_element(2), 6);

    ASSERT_EQ(int_values.get_element(3), 4);
    ASSERT_EQ(int_values.get_element(4), 5);
    ASSERT_EQ(int_values.get_element(5), 6);

    ASSERT_EQ(int_values.get_element(6), 22);
    ASSERT_EQ(int_values.get_element(7), 111);
    ASSERT_EQ(int_values.get_element(8), 333);

    ASSERT_EQ(int_values.get_element(9), 111);
    ASSERT_EQ(int_values.get_element(10), 222);
    ASSERT_EQ(int_values.get_element(11), 3333);
    ASSERT_EQ(int_values.get_element(12), 9988);
    ASSERT_EQ(int_values.get_element(13), 1234);
};

TEST(ColumnMapTest2, StringKeyTestDuplicatedKeysNestedMap) {
    auto col_map_str = ColumnMap(ColumnString::create(),
                                 ColumnMap::create(ColumnString::create(), ColumnInt32::create(),
                                                   ColumnArray::ColumnOffsets::create()),
                                 ColumnArray::ColumnOffsets::create());

    Map inner_map;
    {
        Array k1 = {"a", "b", "c", "a", "b", "c"};
        Array v1 = {1, 2, 3, 4, 5, 6};
        inner_map.push_back(k1);
        inner_map.push_back(v1);
    }

    Map inner_map2;
    {
        Array k1 = {"a", "b", "c", "a", "b", "c"};
        Array v1 = {1, 2, 3, 4, 5, 6};
        inner_map2.push_back(k1);
        inner_map2.push_back(v1);
    }

    Array k1 = {"a", "a"};
    Array v1 = {inner_map, inner_map2};
    {
        Map map;
        map.push_back(k1);
        map.push_back(v1);
        col_map_str.insert(map);
    }

    Map inner_map3;
    {
        Array k2 = {"aa", "bb", "cc", "aa", "cc"};
        Array v2 = {11, 22, 33, 111, 333};
        inner_map3.push_back(k2);
        inner_map3.push_back(v2);
    }

    Map inner_map4;
    {
        Array k2 = {"aa", "cc", "cc"};
        Array v2 = {11, 33, 333};
        inner_map4.push_back(k2);
        inner_map4.push_back(v2);
    }
    Array k2 = {"aa", "aa"};
    Array v2 = {inner_map3, inner_map4};
    {
        Map map;
        map.push_back(k2);
        map.push_back(v2);
        col_map_str.insert(map);
    }

    ASSERT_EQ(col_map_str.size(), 2);
    auto& keys = col_map_str.get_keys();
    auto& values = col_map_str.get_values();

    ASSERT_EQ(keys.size(), 4);
    ASSERT_EQ(keys.size(), values.size());

    auto st = col_map_str.deduplicate_keys(true);
    ASSERT_TRUE(st.ok()) << st.to_string();

    ASSERT_EQ(keys.size(), 2);
    ASSERT_EQ(keys.size(), values.size());

    auto& offsets = col_map_str.get_offsets();
    auto& string_keys = assert_cast<ColumnString&>(keys);
    auto& map_values = assert_cast<ColumnMap&>(values);

    ASSERT_EQ(offsets.size(), 2);
    ASSERT_EQ(offsets[0], 1);
    ASSERT_EQ(offsets[1], 2);

    ASSERT_EQ(string_keys.get_element(0), "a");
    ASSERT_EQ(string_keys.get_element(1), "aa");

    auto map_value1 = get<Array>(map_values[0]);
    auto map_value2 = get<Array>(map_values[1]);

    ASSERT_EQ(map_value1.size(), 2);
    ASSERT_EQ(map_value2.size(), 2);

    // keys
    auto v1_keys = get<Array>(map_value1[0]);
    ASSERT_EQ(v1_keys.size(), 3);
    ASSERT_EQ(get<std::string>(v1_keys[0]), "a");
    ASSERT_EQ(get<std::string>(v1_keys[1]), "b");
    ASSERT_EQ(get<std::string>(v1_keys[2]), "c");

    auto v2_keys = get<Array>(map_value2[0]);
    ASSERT_EQ(v2_keys.size(), 2);
    ASSERT_EQ(get<std::string>(v2_keys[0]), "aa");
    ASSERT_EQ(get<std::string>(v2_keys[1]), "cc");

    // values
    auto v1_values = get<Array>(map_value1[1]);
    ASSERT_EQ(v1_values.size(), 3);
    ASSERT_EQ(get<int32_t>(v1_values[0]), 4);
    ASSERT_EQ(get<int32_t>(v1_values[1]), 5);
    ASSERT_EQ(get<int32_t>(v1_values[2]), 6);

    auto v2_values = get<Array>(map_value2[1]);
    ASSERT_EQ(v2_values.size(), 2);
    ASSERT_EQ(get<int32_t>(v2_values[0]), 11);
    ASSERT_EQ(get<int32_t>(v2_values[1]), 333);
};

TEST(ColumnMapTest2, StringValueTest) {
    auto col_map_str64 = ColumnMap(ColumnInt64::create(), ColumnString64::create(),
                                   ColumnArray::ColumnOffsets::create());
    Array k1 = {1, 2, 3};
    Array v1 = {"a", "b", "c"};
    {
        Map map;
        map.push_back(k1);
        map.push_back(v1);
        col_map_str64.insert(map);
    }
    Array k2 = {11, 22, 33};
    Array v2 = {"aa", "bb", "cc"};
    {
        Map map;
        map.push_back(k2);
        map.push_back(v2);
        col_map_str64.insert(map);
    }
    Array k3 = {111, 222, 333};
    Array v3 = {"aaa", "bbb", "ccc"};
    {
        Map map;
        map.push_back(k3);
        map.push_back(v3);
        col_map_str64.insert(map);
    }

    // test insert ColumnMap<ColumnStr<uint64_t>, Column> into ColumnMap<ColumnStr<uint32_t>, Column>
    auto col_map_str32 = ColumnMap(ColumnInt64::create(), ColumnString::create(),
                                   ColumnArray::ColumnOffsets::create());
    std::vector<uint32_t> indices;
    indices.push_back(0);
    indices.push_back(2);
    col_map_str32.insert_indices_from(col_map_str64, indices.data(),
                                      indices.data() + indices.size());
    EXPECT_EQ(col_map_str32.size(), 2);

    auto map = get<Map>(col_map_str32[0]);
    auto k = get<Array>(map[0]);
    auto v = get<Array>(map[1]);
    EXPECT_EQ(k.size(), 3);
    for (size_t i = 0; i < k.size(); ++i) {
        EXPECT_EQ(k[i], k1[i]);
    }
    EXPECT_EQ(v.size(), 3);
    for (size_t i = 0; i < v.size(); ++i) {
        EXPECT_EQ(v[i], v1[i]);
    }

    map = get<Map>(col_map_str32[1]);
    k = get<Array>(map[0]);
    v = get<Array>(map[1]);
    EXPECT_EQ(k.size(), 3);
    for (size_t i = 0; i < k.size(); ++i) {
        EXPECT_EQ(k[i], k3[i]);
    }
    EXPECT_EQ(v.size(), 3);
    for (size_t i = 0; i < v.size(); ++i) {
        EXPECT_EQ(v[i], v3[i]);
    }
};
} // namespace doris::vectorized