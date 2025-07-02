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

#include "vec/columns/column_variant.h"

#include <gen_cpp/internal_service.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <memory>

#include "runtime/define_primitive_type.h"
#include "vec/columns/common_column_test.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_nothing.h"
#include "vec/json/path_in_data.h"

namespace doris::vectorized {

class ColumnVariantTest : public ::testing::Test {};

auto construct_dst_varint_column() {
    // 1. create an empty variant column
    vectorized::ColumnVariant::Subcolumns dynamic_subcolumns;
    dynamic_subcolumns.create_root(vectorized::ColumnVariant::Subcolumn(0, true, true /*root*/));
    dynamic_subcolumns.add(vectorized::PathInData("v.f"),
                           vectorized::ColumnVariant::Subcolumn {0, true});
    dynamic_subcolumns.add(vectorized::PathInData("v.e"),
                           vectorized::ColumnVariant::Subcolumn {0, true});
    dynamic_subcolumns.add(vectorized::PathInData("v.b"),
                           vectorized::ColumnVariant::Subcolumn {0, true});
    dynamic_subcolumns.add(vectorized::PathInData("v.b.d"),
                           vectorized::ColumnVariant::Subcolumn {0, true});
    dynamic_subcolumns.add(vectorized::PathInData("v.c.d"),
                           vectorized::ColumnVariant::Subcolumn {0, true});
    return ColumnVariant::create(std::move(dynamic_subcolumns), true);
}

TEST_F(ColumnVariantTest, permute) {
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

// test ColumnVariant with ColumnNothing using update_hash_with_value
TEST_F(ColumnVariantTest, updateHashValueWithColumnNothingTest) {
    // Create a subcolumn with ColumnNothing type
    auto type = std::make_shared<DataTypeNothing>();
    auto column = type->create_column();
    column->insert_many_defaults(3);
    // Create a ColumnVariant with a subcolumn that contains ColumnNothing
    auto variant = ColumnVariant::create(true, type, std::move(column));

    // Finalize the variant column to ensure proper structure
    EXPECT_EQ(variant->size(), 3);

    // Test update_hash_with_value with ColumnNothing
    SipHash hash1, hash2, hash3;

    // Test that update_hash_with_value doesn't crash with ColumnNothing
    EXPECT_NO_THROW(variant->update_hash_with_value(0, hash1));
    EXPECT_NO_THROW(variant->update_hash_with_value(1, hash2));
    EXPECT_NO_THROW(variant->update_hash_with_value(2, hash3));

    // For ColumnNothing, the hash should be consistent since it doesn't contain actual data
    // However, the hash might include structural information, so we just verify it doesn't crash
    // and produces some hash value
    EXPECT_NE(hash1.get64(), 0);
    EXPECT_NE(hash2.get64(), 0);
    EXPECT_NE(hash3.get64(), 0);

    // Test update_hashes_with_value with ColumnNothing
    std::vector<uint64_t> hashes(3, 0);
    EXPECT_NO_THROW(variant->update_hashes_with_value(hashes.data()));

    // Test update_xxHash_with_value with ColumnNothing
    uint64_t xxhash = 0;
    EXPECT_NO_THROW(variant->update_xxHash_with_value(0, 3, xxhash, nullptr));

    // Test update_crc_with_value with ColumnNothing
    uint32_t crc_hash = 0;
    EXPECT_NO_THROW(variant->update_crc_with_value(0, 3, crc_hash, nullptr));

    // Test with null map
    std::vector<uint8_t> null_map(3, 0);
    null_map[1] = 1; // Mark second row as null

    std::vector<uint64_t> hashes_with_null(3, 0);
    EXPECT_NO_THROW(variant->update_hashes_with_value(hashes_with_null.data(), null_map.data()));

    uint64_t xxhash_with_null = 0;
    EXPECT_NO_THROW(variant->update_xxHash_with_value(0, 3, xxhash_with_null, null_map.data()));

    uint32_t crc_hash_with_null = 0;
    EXPECT_NO_THROW(variant->update_crc_with_value(0, 3, crc_hash_with_null, null_map.data()));
}

// TEST
TEST_F(ColumnVariantTest, test_pop_back) {
    ColumnVariant::Subcolumn subcolumn(0, true /* is_nullable */, false /* is_root */);

    Field field_int = Field::create_field<TYPE_INT>(123);
    Field field_string = Field::create_field<TYPE_STRING>("hello");

    subcolumn.insert(field_int);
    subcolumn.insert(field_string);

    subcolumn.pop_back(1);
    EXPECT_EQ(subcolumn.size(), 1);
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nullable(TINYINT)");

    subcolumn.pop_back(1);
    EXPECT_EQ(subcolumn.size(), 0);
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nothing");
}

TEST_F(ColumnVariantTest, test_pop_back_multiple_types) {
    ColumnVariant::Subcolumn subcolumn(0, true /* is_nullable */, false /* is_root */);

    Field field_int8 = Field::create_field<TYPE_TINYINT>(42);
    subcolumn.insert(field_int8);
    EXPECT_EQ(subcolumn.size(), 1);
    EXPECT_EQ(subcolumn.data_types.size(), 1);
    EXPECT_EQ(subcolumn.data_types[0]->get_name(), "Nullable(TINYINT)");
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nullable(TINYINT)");

    Field field_int16 = Field::create_field<TYPE_SMALLINT>(12345);
    subcolumn.insert(field_int16);
    EXPECT_EQ(subcolumn.size(), 2);
    EXPECT_EQ(subcolumn.data_types.size(), 2);
    EXPECT_EQ(subcolumn.data_types[0]->get_name(), "Nullable(TINYINT)");
    EXPECT_EQ(subcolumn.data_types[1]->get_name(), "Nullable(SMALLINT)");
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nullable(SMALLINT)");

    Field field_int32 = Field::create_field<TYPE_INT>(1234567);
    subcolumn.insert(field_int32);
    EXPECT_EQ(subcolumn.size(), 3);
    EXPECT_EQ(subcolumn.data_types.size(), 3);
    EXPECT_EQ(subcolumn.data_types[0]->get_name(), "Nullable(TINYINT)");
    EXPECT_EQ(subcolumn.data_types[1]->get_name(), "Nullable(SMALLINT)");
    EXPECT_EQ(subcolumn.data_types[2]->get_name(), "Nullable(INT)");
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nullable(INT)");

    subcolumn.pop_back(1);
    EXPECT_EQ(subcolumn.size(), 2);
    EXPECT_EQ(subcolumn.data_types.size(), 2);
    EXPECT_EQ(subcolumn.data_types[0]->get_name(), "Nullable(TINYINT)");
    EXPECT_EQ(subcolumn.data_types[1]->get_name(), "Nullable(SMALLINT)");
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nullable(SMALLINT)");

    subcolumn.pop_back(1);
    EXPECT_EQ(subcolumn.size(), 1);
    EXPECT_EQ(subcolumn.data_types.size(), 1);
    EXPECT_EQ(subcolumn.data_types[0]->get_name(), "Nullable(TINYINT)");
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nullable(TINYINT)");

    subcolumn.pop_back(1);
    EXPECT_EQ(subcolumn.size(), 0);
    EXPECT_EQ(subcolumn.data_types.size(), 0);
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nothing");

    subcolumn.insert(field_int32);
    EXPECT_EQ(subcolumn.size(), 1);
    EXPECT_EQ(subcolumn.data_types.size(), 1);
    EXPECT_EQ(subcolumn.data_types[0]->get_name(), "Nullable(INT)");
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nullable(INT)");

    subcolumn.insert(field_int16);
    EXPECT_EQ(subcolumn.size(), 2);
    EXPECT_EQ(subcolumn.data_types.size(), 1);
    EXPECT_EQ(subcolumn.data_types[0]->get_name(), "Nullable(INT)");
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nullable(INT)");

    subcolumn.insert(field_int8);
    EXPECT_EQ(subcolumn.size(), 3);
    EXPECT_EQ(subcolumn.data_types.size(), 1);
    EXPECT_EQ(subcolumn.data_types[0]->get_name(), "Nullable(INT)");
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nullable(INT)");

    subcolumn.pop_back(1);
    EXPECT_EQ(subcolumn.size(), 2);
    EXPECT_EQ(subcolumn.data_types.size(), 1);
    EXPECT_EQ(subcolumn.data_types[0]->get_name(), "Nullable(INT)");
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nullable(INT)");

    Field field_string = Field::create_field<TYPE_STRING>("hello");
    subcolumn.insert(field_string);
    EXPECT_EQ(subcolumn.size(), 3);
    EXPECT_EQ(subcolumn.data_types.size(), 2);
    EXPECT_EQ(subcolumn.data_types[0]->get_name(), "Nullable(INT)");
    EXPECT_EQ(subcolumn.data_types[1]->get_name(), "Nullable(JSONB)");
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nullable(JSONB)");

    subcolumn.pop_back(3);
    EXPECT_EQ(subcolumn.size(), 0);
    EXPECT_EQ(subcolumn.data_types.size(), 0);
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nothing");
}

TEST_F(ColumnVariantTest, test_insert_indices_from) {
    // Test case 1: Insert from scalar variant source to empty destination
    {
        // Create source column with scalar values
        auto src_column = ColumnVariant::create(true);
        Field field_int = Field::create_field<TYPE_INT>(123);
        src_column->try_insert(field_int);
        Field field_int2 = Field::create_field<TYPE_INT>(456);
        src_column->try_insert(field_int2);
        src_column->finalize();
        EXPECT_TRUE(src_column->is_scalar_variant());
        EXPECT_TRUE(src_column->is_finalized());
        EXPECT_EQ(src_column->size(), 2);

        // Create empty destination column
        auto dst_column = ColumnVariant::create(true);
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
        EXPECT_EQ(result1.get<VariantMap>().at({}).get<Int64>(), 123);

        Field result2;
        dst_column->get(1, result2);
        EXPECT_EQ(result2.get<VariantMap>().at({}).get<Int64>(), 456);
    }

    // Test case 2: Insert from scalar variant source to non-empty destination of same type
    {
        // Create source column with scalar values
        auto src_column = ColumnVariant::create(true);
        Field field_int = Field::create_field<TYPE_INT>(123);
        src_column->try_insert(field_int);
        Field field_int2 = Field::create_field<TYPE_INT>(456);
        src_column->try_insert(field_int2);
        src_column->finalize();
        EXPECT_TRUE(src_column->is_scalar_variant());

        // Create destination column with same type
        auto dst_column = ColumnVariant::create(true);
        Field field_int3 = Field::create_field<TYPE_INT>(789);
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

        EXPECT_EQ(result1.get<VariantMap>().at({}).get<Int64>(), 789);
        EXPECT_EQ(result2.get<VariantMap>().at({}).get<Int64>(), 456);
        EXPECT_EQ(result3.get<VariantMap>().at({}).get<Int64>(), 123);
    }

    // Test case 3: Insert from non-scalar or different type source (fallback to try_insert)
    {
        // Create source column with object values (non-scalar)
        auto src_column = ColumnVariant::create(true);

        // Create a map with {"a": 123}
        Field field_map = Field::create_field<TYPE_VARIANT>(VariantMap());
        auto& map1 = field_map.get<VariantMap&>();
        map1[PathInData("a")] = Field::create_field<TYPE_INT>(123);
        src_column->try_insert(field_map);

        // Create another map with {"b": "hello"}
        field_map = Field::create_field<TYPE_VARIANT>(VariantMap());
        auto& map2 = field_map.get<VariantMap&>();
        map2[PathInData("b")] = Field::create_field<TYPE_STRING>(String("hello"));
        src_column->try_insert(field_map);

        src_column->finalize();
        EXPECT_FALSE(src_column->is_scalar_variant());

        // Create destination column (empty)
        auto dst_column = ColumnVariant::create(true);

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

        EXPECT_TRUE(result1.get_type() == PrimitiveType::TYPE_VARIANT);
        EXPECT_TRUE(result2.get_type() == PrimitiveType::TYPE_VARIANT);

        const auto& result1_map = result1.get<const VariantMap&>();
        const auto& result2_map = result2.get<const VariantMap&>();

        EXPECT_EQ(result1_map.at(PathInData("b")).get<const String&>(), "hello");
        EXPECT_EQ(result2_map.at(PathInData("a")).get<Int64>(), 123);
    }
}

} // namespace doris::vectorized
