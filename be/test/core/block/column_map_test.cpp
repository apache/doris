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

#include "core/column/column_map.h"

#include <gtest/gtest-death-test.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <cstdint>

#include "core/column/column.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/define_primitive_type.h"
#include "core/data_type/primitive_type.h"
#include "core/field.h"
#include "gtest/gtest_pred_impl.h"

namespace doris {
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

    auto map = col_map_str32[0].get<TYPE_MAP>();
    auto k = map[0].get<TYPE_ARRAY>();
    auto v = map[1].get<TYPE_ARRAY>();
    EXPECT_EQ(k.size(), 3);
    for (size_t i = 0; i < k.size(); ++i) {
        EXPECT_EQ(k[i], k1[i]);
    }
    EXPECT_EQ(v.size(), 3);
    for (size_t i = 0; i < v.size(); ++i) {
        EXPECT_EQ(v[i], v1[i]);
    }

    map = col_map_str32[1].get<TYPE_MAP>();
    k = map[0].get<TYPE_ARRAY>();
    v = map[1].get<TYPE_ARRAY>();
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
            ColumnNullable::create(ColumnString::create(), ColumnVector<TYPE_BOOLEAN>::create()),
            ColumnInt32::create(), ColumnArray::ColumnOffsets::create());
    Array k1 = {Field::create_field<TYPE_STRING>("a"), Field::create_field<TYPE_STRING>("b"),
                Field::create_field<TYPE_STRING>("c"), Field::create_field<TYPE_STRING>("a"),
                Field::create_field<TYPE_STRING>("b"), Field::create_field<TYPE_STRING>("c")};
    Array v1 = {Field::create_field<TYPE_INT>(1), Field::create_field<TYPE_INT>(2),
                Field::create_field<TYPE_INT>(3), Field::create_field<TYPE_INT>(4),
                Field::create_field<TYPE_INT>(5), Field::create_field<TYPE_INT>(6)};
    {
        Map map;
        map.push_back(Field::create_field<TYPE_ARRAY>(k1));
        map.push_back(Field::create_field<TYPE_ARRAY>(v1));
        col_map_str.insert(Field::create_field<TYPE_MAP>(map));
    }
    {
        Map map;
        map.push_back(Field::create_field<TYPE_ARRAY>(k1));
        map.push_back(Field::create_field<TYPE_ARRAY>(v1));
        col_map_str.insert(Field::create_field<TYPE_MAP>(map));
    }

    Array k2 = {Field::create_field<TYPE_STRING>("aa"), Field::create_field<TYPE_STRING>("bb"),
                Field::create_field<TYPE_STRING>("cc"), Field::create_field<TYPE_STRING>("aa"),
                Field::create_field<TYPE_STRING>("cc")};
    Array v2 = {Field::create_field<TYPE_INT>(11), Field::create_field<TYPE_INT>(22),
                Field::create_field<TYPE_INT>(33), Field::create_field<TYPE_INT>(111),
                Field::create_field<TYPE_INT>(333)};
    {
        Map map;
        map.push_back(Field::create_field<TYPE_ARRAY>(k2));
        map.push_back(Field::create_field<TYPE_ARRAY>(v2));
        col_map_str.insert(Field::create_field<TYPE_MAP>(map));
    }

    Array k3 = {Field::create_field<TYPE_STRING>("aaa"),
                Field::create_field<TYPE_STRING>("bbb"),
                Field(),
                Field::create_field<TYPE_STRING>(""),
                Field::create_field<TYPE_STRING>("ccc"),
                Field::create_field<TYPE_STRING>("ccc"),
                Field::create_field<TYPE_STRING>(""),
                Field()};
    Array v3 = {Field::create_field<TYPE_INT>(111),  Field::create_field<TYPE_INT>(222),
                Field::create_field<TYPE_INT>(4321), Field::create_field<TYPE_INT>(999),
                Field::create_field<TYPE_INT>(333),  Field::create_field<TYPE_INT>(3333),
                Field::create_field<TYPE_INT>(9988), Field::create_field<TYPE_INT>(1234)};
    {
        Map map;
        map.push_back(Field::create_field<TYPE_ARRAY>(k3));
        map.push_back(Field::create_field<TYPE_ARRAY>(v3));
        col_map_str.insert(Field::create_field<TYPE_MAP>(map));
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
        Array k1 = {Field::create_field<TYPE_STRING>("a"), Field::create_field<TYPE_STRING>("b"),
                    Field::create_field<TYPE_STRING>("c"), Field::create_field<TYPE_STRING>("a"),
                    Field::create_field<TYPE_STRING>("b"), Field::create_field<TYPE_STRING>("c")};
        Array v1 = {Field::create_field<TYPE_INT>(1), Field::create_field<TYPE_INT>(2),
                    Field::create_field<TYPE_INT>(3), Field::create_field<TYPE_INT>(4),
                    Field::create_field<TYPE_INT>(5), Field::create_field<TYPE_INT>(6)};
        inner_map.push_back(Field::create_field<TYPE_ARRAY>(k1));
        inner_map.push_back(Field::create_field<TYPE_ARRAY>(v1));
    }

    Map inner_map2;
    {
        Array k1 = {Field::create_field<TYPE_STRING>("a"), Field::create_field<TYPE_STRING>("b"),
                    Field::create_field<TYPE_STRING>("c"), Field::create_field<TYPE_STRING>("a"),
                    Field::create_field<TYPE_STRING>("b"), Field::create_field<TYPE_STRING>("c")};
        Array v1 = {Field::create_field<TYPE_INT>(1), Field::create_field<TYPE_INT>(2),
                    Field::create_field<TYPE_INT>(3), Field::create_field<TYPE_INT>(4),
                    Field::create_field<TYPE_INT>(5), Field::create_field<TYPE_INT>(6)};
        inner_map2.push_back(Field::create_field<TYPE_ARRAY>(k1));
        inner_map2.push_back(Field::create_field<TYPE_ARRAY>(v1));
    }

    Array k1 = {Field::create_field<TYPE_STRING>("a"), Field::create_field<TYPE_STRING>("a")};
    Array v1 = {Field::create_field<TYPE_MAP>(inner_map),
                Field::create_field<TYPE_MAP>(inner_map2)};
    {
        Map map;
        map.push_back(Field::create_field<TYPE_ARRAY>(k1));
        map.push_back(Field::create_field<TYPE_ARRAY>(v1));
        col_map_str.insert(Field::create_field<TYPE_MAP>(map));
    }

    Map inner_map3;
    {
        Array k2 = {Field::create_field<TYPE_STRING>("aa"), Field::create_field<TYPE_STRING>("bb"),
                    Field::create_field<TYPE_STRING>("cc"), Field::create_field<TYPE_STRING>("aa"),
                    Field::create_field<TYPE_STRING>("cc")};
        Array v2 = {Field::create_field<TYPE_INT>(11), Field::create_field<TYPE_INT>(22),
                    Field::create_field<TYPE_INT>(33), Field::create_field<TYPE_INT>(111),
                    Field::create_field<TYPE_INT>(333)};
        inner_map3.push_back(Field::create_field<TYPE_ARRAY>(k2));
        inner_map3.push_back(Field::create_field<TYPE_ARRAY>(v2));
    }

    Map inner_map4;
    {
        Array k2 = {Field::create_field<TYPE_STRING>("aa"), Field::create_field<TYPE_STRING>("cc"),
                    Field::create_field<TYPE_STRING>("cc")};
        Array v2 = {Field::create_field<TYPE_INT>(11), Field::create_field<TYPE_INT>(33),
                    Field::create_field<TYPE_INT>(333)};
        inner_map4.push_back(Field::create_field<TYPE_ARRAY>(k2));
        inner_map4.push_back(Field::create_field<TYPE_ARRAY>(v2));
    }

    Array k2 = {Field::create_field<TYPE_STRING>("aa"), Field::create_field<TYPE_STRING>("aa")};
    Array v2 = {Field::create_field<TYPE_MAP>(inner_map3),
                Field::create_field<TYPE_MAP>(inner_map4)};
    {
        Map map;
        map.push_back(Field::create_field<TYPE_ARRAY>(k2));
        map.push_back(Field::create_field<TYPE_ARRAY>(v2));
        col_map_str.insert(Field::create_field<TYPE_MAP>(map));
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

    auto map_value1 = map_values[0].get<TYPE_MAP>();
    auto map_value2 = map_values[1].get<TYPE_MAP>();

    ASSERT_EQ(map_value1.size(), 2);
    ASSERT_EQ(map_value2.size(), 2);

    // keys
    auto v1_keys = map_value1[0].get<TYPE_ARRAY>();
    ASSERT_EQ(v1_keys.size(), 3);
    ASSERT_EQ(v1_keys[0].get<TYPE_STRING>(), "a");
    ASSERT_EQ(v1_keys[1].get<TYPE_STRING>(), "b");
    ASSERT_EQ(v1_keys[2].get<TYPE_STRING>(), "c");

    auto v2_keys = map_value2[0].get<TYPE_ARRAY>();
    ASSERT_EQ(v2_keys.size(), 2);
    ASSERT_EQ(v2_keys[0].get<TYPE_STRING>(), "aa");
    ASSERT_EQ(v2_keys[1].get<TYPE_STRING>(), "cc");

    // values
    auto v1_values = map_value1[1].get<TYPE_ARRAY>();
    ASSERT_EQ(v1_values.size(), 3);
    ASSERT_EQ(v1_values[0].get<TYPE_INT>(), 4);
    ASSERT_EQ(v1_values[1].get<TYPE_INT>(), 5);
    ASSERT_EQ(v1_values[2].get<TYPE_INT>(), 6);

    auto v2_values = map_value2[1].get<TYPE_ARRAY>();
    ASSERT_EQ(v2_values.size(), 2);
    ASSERT_EQ(v2_values[0].get<TYPE_INT>(), 11);
    ASSERT_EQ(v2_values[1].get<TYPE_INT>(), 333);
};

TEST(ColumnMapTest2, SharedCreatePreservesImmutableSubcolumns) {
    auto keys_mut = ColumnString::create();
    keys_mut->insert_data("k", 1);
    ColumnPtr keys = std::move(keys_mut);
    ColumnPtr keys_alias = keys;

    auto values_mut = ColumnInt32::create();
    values_mut->insert_value(1);
    ColumnPtr values = std::move(values_mut);
    ColumnPtr values_alias = values;

    auto offsets_mut = ColumnArray::ColumnOffsets::create();
    offsets_mut->get_data().push_back(1);
    ColumnPtr offsets = std::move(offsets_mut);
    ColumnPtr offsets_alias = offsets;

    auto map_column = ColumnMap::create(keys, values, offsets);
    EXPECT_EQ(map_column->get_keys_ptr().get(), keys_alias.get());
    EXPECT_EQ(map_column->get_values_ptr().get(), values_alias.get());
    EXPECT_EQ(map_column->get_offsets_ptr().get(), offsets_alias.get());
}

TEST(ColumnMapTest2, ConstFilterAndPermuteKeepInputAliasesUntouched) {
    auto keys_mut = ColumnString::create();
    keys_mut->insert_data("a", 1);
    keys_mut->insert_data("b", 1);
    keys_mut->insert_data("c", 1);
    ColumnPtr keys = std::move(keys_mut);
    ColumnPtr keys_alias = keys;

    auto values_mut = ColumnInt32::create();
    values_mut->insert_value(1);
    values_mut->insert_value(2);
    values_mut->insert_value(3);
    ColumnPtr values = std::move(values_mut);
    ColumnPtr values_alias = values;

    auto offsets_mut = ColumnArray::ColumnOffsets::create();
    offsets_mut->get_data().push_back(2);
    offsets_mut->get_data().push_back(3);
    ColumnPtr offsets = std::move(offsets_mut);
    ColumnPtr offsets_alias = offsets;

    auto map_column = ColumnMap::create(keys, values, offsets);

    IColumn::Filter filter;
    filter.push_back(0);
    filter.push_back(1);
    auto filtered = map_column->filter(filter, 1);
    const auto& filtered_map = assert_cast<const ColumnMap&>(*filtered);
    EXPECT_EQ(filtered_map.size(), 1);
    EXPECT_EQ(filtered_map.get_keys().size(), 1);
    EXPECT_EQ(assert_cast<const ColumnInt32&>(filtered_map.get_values()).get_element(0), 3);

    IColumn::Permutation perm;
    perm.push_back(1);
    perm.push_back(0);
    auto permuted = map_column->permute(perm, 0);
    const auto& permuted_map = assert_cast<const ColumnMap&>(*permuted);
    EXPECT_EQ(permuted_map.size(), 2);
    EXPECT_EQ(permuted_map.get_offsets()[0], 1);
    EXPECT_EQ(permuted_map.get_offsets()[1], 3);

    EXPECT_EQ(keys_alias->size(), 3);
    EXPECT_EQ(values_alias->size(), 3);
    EXPECT_EQ(offsets_alias->size(), 2);
}

TEST(ColumnMapTest2, DeduplicateNestedNullableMapValuesDetachesSharedValueColumn) {
    auto inner_values = ColumnMap::create(ColumnString::create(), ColumnInt32::create(),
                                          ColumnArray::ColumnOffsets::create());
    Map inner_map;
    inner_map.push_back(Field::create_field<TYPE_ARRAY>(
            Array {Field::create_field<TYPE_STRING>("a"), Field::create_field<TYPE_STRING>("a")}));
    inner_map.push_back(Field::create_field<TYPE_ARRAY>(
            Array {Field::create_field<TYPE_INT>(1), Field::create_field<TYPE_INT>(2)}));
    inner_values->insert(Field::create_field<TYPE_MAP>(inner_map));

    ColumnPtr shared_inner_values = std::move(inner_values);
    ColumnPtr inner_values_alias = shared_inner_values;

    auto null_map_mut = ColumnUInt8::create();
    null_map_mut->insert_value(0);
    ColumnPtr null_map = std::move(null_map_mut);
    ColumnPtr nullable_values = ColumnNullable::create(shared_inner_values, null_map);

    auto outer_keys_mut = ColumnString::create();
    outer_keys_mut->insert_data("outer", 5);
    ColumnPtr outer_keys = std::move(outer_keys_mut);

    auto outer_offsets_mut = ColumnArray::ColumnOffsets::create();
    outer_offsets_mut->get_data().push_back(1);
    ColumnPtr outer_offsets = std::move(outer_offsets_mut);

    auto outer_map = ColumnMap::create(outer_keys, nullable_values, outer_offsets);
    auto st = outer_map->deduplicate_keys(true);
    ASSERT_TRUE(st.ok()) << st.to_string();

    const auto& alias_inner_map = assert_cast<const ColumnMap&>(*inner_values_alias);
    EXPECT_EQ(alias_inner_map.get_keys().size(), 2);
    EXPECT_EQ(alias_inner_map.get_values().size(), 2);

    const auto& outer_map_ref = *outer_map;
    const auto& outer_values_nullable =
            assert_cast<const ColumnNullable&>(outer_map_ref.get_values());
    const auto& deduplicated_inner_map =
            assert_cast<const ColumnMap&>(outer_values_nullable.get_nested_column());
    EXPECT_EQ(deduplicated_inner_map.get_keys().size(), 1);
    EXPECT_EQ(deduplicated_inner_map.get_values().size(), 1);
    EXPECT_EQ(deduplicated_inner_map.get_keys().get_data_at(0).to_string(), "a");
    EXPECT_EQ(assert_cast<const ColumnInt32&>(deduplicated_inner_map.get_values()).get_element(0),
              2);
}

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

    auto map = col_map_str32[0].get<TYPE_MAP>();
    auto k = map[0].get<TYPE_ARRAY>();
    auto v = map[1].get<TYPE_ARRAY>();
    EXPECT_EQ(k.size(), 3);
    for (size_t i = 0; i < k.size(); ++i) {
        EXPECT_EQ(k[i], k1[i]);
    }
    EXPECT_EQ(v.size(), 3);
    for (size_t i = 0; i < v.size(); ++i) {
        EXPECT_EQ(v[i], v1[i]);
    }

    map = col_map_str32[1].get<TYPE_MAP>();
    k = map[0].get<TYPE_ARRAY>();
    v = map[1].get<TYPE_ARRAY>();
    EXPECT_EQ(k.size(), 3);
    for (size_t i = 0; i < k.size(); ++i) {
        EXPECT_EQ(k[i], k3[i]);
    }
    EXPECT_EQ(v.size(), 3);
    for (size_t i = 0; i < v.size(); ++i) {
        EXPECT_EQ(v[i], v3[i]);
    }
};
} // namespace doris
