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

#include "vec/columns/column_object.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include "vec/columns/common_column_test.h"

namespace doris::vectorized {

class ColumnObjectTest : public ::testing::Test {};

auto construct_dst_varint_column() {
    // 1. create an empty variant column
    vectorized::ColumnObject::Subcolumns dynamic_subcolumns;
    dynamic_subcolumns.create_root(vectorized::ColumnObject::Subcolumn(0, true, true /*root*/));
    dynamic_subcolumns.add(vectorized::PathInData("v.f"),
                           vectorized::ColumnObject::Subcolumn {0, true});
    dynamic_subcolumns.add(vectorized::PathInData("v.e"),
                           vectorized::ColumnObject::Subcolumn {0, true});
    dynamic_subcolumns.add(vectorized::PathInData("v.b"),
                           vectorized::ColumnObject::Subcolumn {0, true});
    dynamic_subcolumns.add(vectorized::PathInData("v.b.d"),
                           vectorized::ColumnObject::Subcolumn {0, true});
    dynamic_subcolumns.add(vectorized::PathInData("v.c.d"),
                           vectorized::ColumnObject::Subcolumn {0, true});
    return ColumnObject::create(std::move(dynamic_subcolumns), true);
}

TEST_F(ColumnObjectTest, permute) {
    auto column_variant = construct_dst_varint_column();
    {
        // test empty column and limit == 0
        IColumn::Permutation permutation(0);
        auto col = column_variant->clone_empty();
        col->permute(permutation, 0);
        EXPECT_EQ(col->size(), 0);
    }

    MutableColumns columns;
    columns.push_back(column_variant->get_ptr());
    assert_column_vector_permute(columns, 0);
    assert_column_vector_permute(columns, 1);
    assert_column_vector_permute(columns, column_variant->size());
    assert_column_vector_permute(columns, UINT64_MAX);
}

// TEST
TEST_F(ColumnObjectTest, test_pop_back) {
    ColumnObject::Subcolumn subcolumn(0, true /* is_nullable */, false /* is_root */);

    Field field_int(123);
    Field field_string("hello");

    subcolumn.insert(field_int);
    subcolumn.insert(field_string);

    subcolumn.pop_back(1);
    EXPECT_EQ(subcolumn.size(), 1);
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nullable(Int8)");

    subcolumn.pop_back(1);
    EXPECT_EQ(subcolumn.size(), 0);
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nothing");
}

TEST_F(ColumnObjectTest, test_pop_back_multiple_types) {
    ColumnObject::Subcolumn subcolumn(0, true /* is_nullable */, false /* is_root */);

    Field field_int8(42);
    subcolumn.insert(field_int8);
    EXPECT_EQ(subcolumn.size(), 1);
    EXPECT_EQ(subcolumn.data_types.size(), 1);
    EXPECT_EQ(subcolumn.data_types[0]->get_name(), "Nullable(Int8)");
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nullable(Int8)");

    Field field_int16(12345);
    subcolumn.insert(field_int16);
    EXPECT_EQ(subcolumn.size(), 2);
    EXPECT_EQ(subcolumn.data_types.size(), 2);
    EXPECT_EQ(subcolumn.data_types[0]->get_name(), "Nullable(Int8)");
    EXPECT_EQ(subcolumn.data_types[1]->get_name(), "Nullable(Int16)");
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nullable(Int16)");

    Field field_int32(1234567);
    subcolumn.insert(field_int32);
    EXPECT_EQ(subcolumn.size(), 3);
    EXPECT_EQ(subcolumn.data_types.size(), 3);
    EXPECT_EQ(subcolumn.data_types[0]->get_name(), "Nullable(Int8)");
    EXPECT_EQ(subcolumn.data_types[1]->get_name(), "Nullable(Int16)");
    EXPECT_EQ(subcolumn.data_types[2]->get_name(), "Nullable(Int32)");
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nullable(Int32)");

    subcolumn.pop_back(1);
    EXPECT_EQ(subcolumn.size(), 2);
    EXPECT_EQ(subcolumn.data_types.size(), 2);
    EXPECT_EQ(subcolumn.data_types[0]->get_name(), "Nullable(Int8)");
    EXPECT_EQ(subcolumn.data_types[1]->get_name(), "Nullable(Int16)");
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nullable(Int16)");

    subcolumn.pop_back(1);
    EXPECT_EQ(subcolumn.size(), 1);
    EXPECT_EQ(subcolumn.data_types.size(), 1);
    EXPECT_EQ(subcolumn.data_types[0]->get_name(), "Nullable(Int8)");
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nullable(Int8)");

    subcolumn.pop_back(1);
    EXPECT_EQ(subcolumn.size(), 0);
    EXPECT_EQ(subcolumn.data_types.size(), 0);
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nothing");

    subcolumn.insert(field_int32);
    EXPECT_EQ(subcolumn.size(), 1);
    EXPECT_EQ(subcolumn.data_types.size(), 1);
    EXPECT_EQ(subcolumn.data_types[0]->get_name(), "Nullable(Int32)");
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nullable(Int32)");

    subcolumn.insert(field_int16);
    EXPECT_EQ(subcolumn.size(), 2);
    EXPECT_EQ(subcolumn.data_types.size(), 1);
    EXPECT_EQ(subcolumn.data_types[0]->get_name(), "Nullable(Int32)");
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nullable(Int32)");

    subcolumn.insert(field_int8);
    EXPECT_EQ(subcolumn.size(), 3);
    EXPECT_EQ(subcolumn.data_types.size(), 1);
    EXPECT_EQ(subcolumn.data_types[0]->get_name(), "Nullable(Int32)");
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nullable(Int32)");

    subcolumn.pop_back(1);
    EXPECT_EQ(subcolumn.size(), 2);
    EXPECT_EQ(subcolumn.data_types.size(), 1);
    EXPECT_EQ(subcolumn.data_types[0]->get_name(), "Nullable(Int32)");
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nullable(Int32)");

    Field field_string("hello");
    subcolumn.insert(field_string);
    EXPECT_EQ(subcolumn.size(), 3);
    EXPECT_EQ(subcolumn.data_types.size(), 2);
    EXPECT_EQ(subcolumn.data_types[0]->get_name(), "Nullable(Int32)");
    EXPECT_EQ(subcolumn.data_types[1]->get_name(), "Nullable(JSONB)");
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nullable(JSONB)");

    subcolumn.pop_back(3);
    EXPECT_EQ(subcolumn.size(), 0);
    EXPECT_EQ(subcolumn.data_types.size(), 0);
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nothing");
}

TEST_F(ColumnObjectTest, test_insert_indices_from) {
    // Test case 1: Insert from scalar variant source to empty destination
    {
        // Create source column with scalar values
        auto src_column = ColumnObject::create(true);
        Field field_int(123);
        src_column->try_insert(field_int);
        Field field_int2(456);
        src_column->try_insert(field_int2);
        src_column->finalize();
        EXPECT_TRUE(src_column->is_scalar_variant());
        EXPECT_TRUE(src_column->is_finalized());
        EXPECT_EQ(src_column->size(), 2);

        // Create empty destination column
        auto dst_column = ColumnObject::create(true);
        EXPECT_EQ(dst_column->size(), 0);

        // Create indices
        std::vector<uint32_t> indices = {0, 1};

        // Insert using indices
        dst_column->insert_indices_from(*src_column, indices.data(),
                                        indices.data() + indices.size());

        // Verify results
        EXPECT_EQ(dst_column->size(), 2);
        EXPECT_TRUE(dst_column->is_scalar_variant());
        EXPECT_TRUE(dst_column->is_finalized());
        EXPECT_EQ(dst_column->get_root_type()->get_name(), src_column->get_root_type()->get_name());

        Field result1;
        dst_column->get(0, result1);
        EXPECT_EQ(result1.get<VariantMap>().at("").get<Int64>(), 123);

        Field result2;
        dst_column->get(1, result2);
        EXPECT_EQ(result2.get<VariantMap>().at("").get<Int64>(), 456);
    }

    // Test case 2: Insert from scalar variant source to non-empty destination of same type
    {
        // Create source column with scalar values
        auto src_column = ColumnObject::create(true);
        Field field_int(123);
        src_column->try_insert(field_int);
        Field field_int2(456);
        src_column->try_insert(field_int2);
        src_column->finalize();
        EXPECT_TRUE(src_column->is_scalar_variant());

        // Create destination column with same type
        auto dst_column = ColumnObject::create(true);
        Field field_int3(789);
        dst_column->try_insert(field_int3);
        dst_column->finalize();
        EXPECT_TRUE(dst_column->is_scalar_variant());
        EXPECT_EQ(dst_column->size(), 1);

        // Create indices for selecting specific elements
        std::vector<uint32_t> indices = {1, 0};

        // Insert using indices (reversed order)
        dst_column->insert_indices_from(*src_column, indices.data(),
                                        indices.data() + indices.size());

        // Verify results
        EXPECT_EQ(dst_column->size(), 3);

        Field result1, result2, result3;
        dst_column->get(0, result1);
        dst_column->get(1, result2);
        dst_column->get(2, result3);

        EXPECT_EQ(result1.get<VariantMap>().at("").get<Int64>(), 789);
        EXPECT_EQ(result2.get<VariantMap>().at("").get<Int64>(), 456);
        EXPECT_EQ(result3.get<VariantMap>().at("").get<Int64>(), 123);
    }

    // Test case 3: Insert from non-scalar or different type source (fallback to try_insert)
    {
        // Create source column with object values (non-scalar)
        auto src_column = ColumnObject::create(true);

        // Create a map with {"a": 123}
        Field field_map = VariantMap();
        auto& map1 = field_map.get<VariantMap&>();
        map1["a"] = 123;
        src_column->try_insert(field_map);

        // Create another map with {"b": "hello"}
        field_map = VariantMap();
        auto& map2 = field_map.get<VariantMap&>();
        map2["b"] = String("hello");
        src_column->try_insert(field_map);

        src_column->finalize();
        EXPECT_FALSE(src_column->is_scalar_variant());

        // Create destination column (empty)
        auto dst_column = ColumnObject::create(true);

        // Create indices
        std::vector<uint32_t> indices = {1, 0};

        // Insert using indices
        dst_column->insert_indices_from(*src_column, indices.data(),
                                        indices.data() + indices.size());

        // Verify results
        EXPECT_EQ(dst_column->size(), 2);

        Field result1, result2;
        dst_column->get(0, result1);
        dst_column->get(1, result2);

        EXPECT_TRUE(result1.get_type() == Field::Types::VariantMap);
        EXPECT_TRUE(result2.get_type() == Field::Types::VariantMap);

        const auto& result1_map = result1.get<const VariantMap&>();
        const auto& result2_map = result2.get<const VariantMap&>();

        EXPECT_EQ(result1_map.at("b").get<const String&>(), "hello");
        EXPECT_EQ(result2_map.at("a").get<Int64>(), 123);
    }
}

} // namespace doris::vectorized
