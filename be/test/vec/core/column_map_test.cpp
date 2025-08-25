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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include "gtest/gtest_pred_impl.h"
#include "runtime/primitive_type.h"
#include "vec/columns/column.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/core/field.h"

namespace doris::vectorized {
TEST(ColumnMapTest2, StringKeyTest) {
    auto col_map_str64 = ColumnMap(ColumnString64::create(), ColumnInt64::create(),
                                   ColumnArray::ColumnOffsets::create());
    Array k1 = {Field::create_field<TYPE_STRING>("a"), Field::create_field<TYPE_STRING>("b"),
                Field::create_field<TYPE_STRING>("c")};
    Array v1 = {Field::create_field<TYPE_BIGINT>(1), Field::create_field<TYPE_BIGINT>(2),
                Field::create_field<TYPE_BIGINT>(3)};
    {
        Map map;
        map.push_back(Field::create_field<TYPE_ARRAY>(k1));
        map.push_back(Field::create_field<TYPE_ARRAY>(v1));
        col_map_str64.insert(Field::create_field<TYPE_MAP>(map));
    }
    Array k2 = {Field::create_field<TYPE_STRING>("aa"), Field::create_field<TYPE_STRING>("bb"),
                Field::create_field<TYPE_STRING>("cc")};
    Array v2 = {Field::create_field<TYPE_BIGINT>(11), Field::create_field<TYPE_BIGINT>(22),
                Field::create_field<TYPE_BIGINT>(33)};
    {
        Map map;
        map.push_back(Field::create_field<TYPE_ARRAY>(k2));
        map.push_back(Field::create_field<TYPE_ARRAY>(v2));
        col_map_str64.insert(Field::create_field<TYPE_MAP>(map));
    }
    Array k3 = {Field::create_field<TYPE_STRING>("aaa"), Field::create_field<TYPE_STRING>("bbb"),
                Field::create_field<TYPE_STRING>("ccc")};
    Array v3 = {Field::create_field<TYPE_BIGINT>(111), Field::create_field<TYPE_BIGINT>(222),
                Field::create_field<TYPE_BIGINT>(333)};
    {
        Map map;
        map.push_back(Field::create_field<TYPE_ARRAY>(k3));
        map.push_back(Field::create_field<TYPE_ARRAY>(v3));
        col_map_str64.insert(Field::create_field<TYPE_MAP>(map));
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

TEST(ColumnMapTest2, StringValueTest) {
    auto col_map_str64 = ColumnMap(ColumnInt64::create(), ColumnString64::create(),
                                   ColumnArray::ColumnOffsets::create());
    Array k1 = {Field::create_field<TYPE_BIGINT>(1), Field::create_field<TYPE_BIGINT>(2),
                Field::create_field<TYPE_BIGINT>(3)};
    Array v1 = {Field::create_field<TYPE_STRING>("a"), Field::create_field<TYPE_STRING>("b"),
                Field::create_field<TYPE_STRING>("c")};
    {
        Map map;
        map.push_back(Field::create_field<TYPE_ARRAY>(k1));
        map.push_back(Field::create_field<TYPE_ARRAY>(v1));
        col_map_str64.insert(Field::create_field<TYPE_MAP>(map));
    }
    Array k2 = {Field::create_field<TYPE_BIGINT>(11), Field::create_field<TYPE_BIGINT>(22),
                Field::create_field<TYPE_BIGINT>(33)};
    Array v2 = {Field::create_field<TYPE_STRING>("aa"), Field::create_field<TYPE_STRING>("bb"),
                Field::create_field<TYPE_STRING>("cc")};
    {
        Map map;
        map.push_back(Field::create_field<TYPE_ARRAY>(k2));
        map.push_back(Field::create_field<TYPE_ARRAY>(v2));
        col_map_str64.insert(Field::create_field<TYPE_MAP>(map));
    }
    Array k3 = {Field::create_field<TYPE_BIGINT>(111), Field::create_field<TYPE_BIGINT>(222),
                Field::create_field<TYPE_BIGINT>(333)};
    Array v3 = {Field::create_field<TYPE_STRING>("aaa"), Field::create_field<TYPE_STRING>("bbb"),
                Field::create_field<TYPE_STRING>("ccc")};
    {
        Map map;
        map.push_back(Field::create_field<TYPE_ARRAY>(k3));
        map.push_back(Field::create_field<TYPE_ARRAY>(v3));
        col_map_str64.insert(Field::create_field<TYPE_MAP>(map));
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