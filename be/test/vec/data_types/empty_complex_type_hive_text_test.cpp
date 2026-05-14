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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <memory>

#include "vec/columns/column_array.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/serde/data_type_serde.h"

namespace doris::vectorized {

// Test that deserializing an empty slice (as in CSV/Hive Text format)
// for array/map types produces an empty collection ([]/{}) instead of null,
// matching Spark's behavior.
class EmptyComplexTypeHiveTextTest : public ::testing::Test {
protected:
    DataTypeSerDe::FormatOptions options;
};

TEST_F(EmptyComplexTypeHiveTextTest, EmptyArrayFromHiveText) {
    // array<int>
    auto nested_type = std::make_shared<DataTypeInt32>();
    auto nullable_nested = std::make_shared<DataTypeNullable>(nested_type);
    auto array_type = std::make_shared<DataTypeArray>(nullable_nested);
    auto serde = array_type->get_serde(1);
    auto column = array_type->create_column();

    // Deserialize empty slice — should produce empty array, not error
    Slice empty_slice("");
    auto st = serde->deserialize_one_cell_from_hive_text(*column, empty_slice, options);
    EXPECT_TRUE(st.ok()) << "deserialize empty array failed: " << st.to_string();
    ASSERT_EQ(column->size(), 1u);

    // Verify the result is [] (empty array)
    const auto& array_col = assert_cast<const ColumnArray&>(*column);
    const auto& offsets = array_col.get_offsets();
    EXPECT_EQ(offsets[0], 0u) << "empty array should have offset 0";
}

TEST_F(EmptyComplexTypeHiveTextTest, EmptyMapFromHiveText) {
    // map<int, int>
    auto key_type = std::make_shared<DataTypeInt32>();
    auto val_type = std::make_shared<DataTypeInt32>();
    auto nullable_key = std::make_shared<DataTypeNullable>(key_type);
    auto nullable_val = std::make_shared<DataTypeNullable>(val_type);
    auto map_type = std::make_shared<DataTypeMap>(nullable_key, nullable_val);
    auto serde = map_type->get_serde(1);
    auto column = map_type->create_column();

    // Deserialize empty slice — should produce empty map, not error
    Slice empty_slice("");
    auto st = serde->deserialize_one_cell_from_hive_text(*column, empty_slice, options);
    EXPECT_TRUE(st.ok()) << "deserialize empty map failed: " << st.to_string();
    ASSERT_EQ(column->size(), 1u);

    // Verify the result is {} (empty map)
    const auto& map_col = assert_cast<const ColumnMap&>(*column);
    const auto& offsets = map_col.get_offsets();
    EXPECT_EQ(offsets[0], 0u) << "empty map should have offset 0";
}

TEST_F(EmptyComplexTypeHiveTextTest, NonEmptyArrayFromHiveText) {
    // Ensure non-empty array still works correctly
    auto nested_type = std::make_shared<DataTypeInt32>();
    auto nullable_nested = std::make_shared<DataTypeNullable>(nested_type);
    auto array_type = std::make_shared<DataTypeArray>(nullable_nested);
    auto serde = array_type->get_serde(1);
    auto column = array_type->create_column();

    // Deserialize "[1,2,3]" — standard hive text uses Ctrl-B as collection delimiter
    Slice data_slice("1\u0002 2\u0002 3");
    auto st = serde->deserialize_one_cell_from_hive_text(*column, data_slice, options);
    EXPECT_TRUE(st.ok()) << "deserialize non-empty array failed: " << st.to_string();
    ASSERT_EQ(column->size(), 1u);

    const auto& array_col = assert_cast<const ColumnArray&>(*column);
    const auto& offsets = array_col.get_offsets();
    EXPECT_EQ(offsets[0], 3u) << "array should have 3 elements";
}

TEST_F(EmptyComplexTypeHiveTextTest, NullableEmptyArrayFromHiveText) {
    // Test nullable(array<int>) — empty string should produce empty array, not null
    auto nested_type = std::make_shared<DataTypeInt32>();
    auto nullable_nested = std::make_shared<DataTypeNullable>(nested_type);
    auto array_type = std::make_shared<DataTypeArray>(nullable_nested);
    auto nullable_array_type = std::make_shared<DataTypeNullable>(array_type);
    auto serde = nullable_array_type->get_serde(1);
    auto column = nullable_array_type->create_column();

    // Deserialize empty slice — should produce NOT NULL empty array
    Slice empty_slice("");
    auto st = serde->deserialize_one_cell_from_hive_text(*column, empty_slice, options);
    EXPECT_TRUE(st.ok()) << "deserialize nullable empty array failed: " << st.to_string();
    ASSERT_EQ(column->size(), 1u);

    // The top-level nullable column should NOT be null
    const auto& nullable_col = assert_cast<const ColumnNullable&>(*column);
    EXPECT_FALSE(nullable_col.is_null_at(0))
            << "empty array in nullable wrapper should not be null";
}

TEST_F(EmptyComplexTypeHiveTextTest, NullableEmptyMapFromHiveText) {
    // Test nullable(map<int,int>) — empty string should produce empty map, not null
    auto key_type = std::make_shared<DataTypeInt32>();
    auto val_type = std::make_shared<DataTypeInt32>();
    auto nullable_key = std::make_shared<DataTypeNullable>(key_type);
    auto nullable_val = std::make_shared<DataTypeNullable>(val_type);
    auto map_type = std::make_shared<DataTypeMap>(nullable_key, nullable_val);
    auto nullable_map_type = std::make_shared<DataTypeNullable>(map_type);
    auto serde = nullable_map_type->get_serde(1);
    auto column = nullable_map_type->create_column();

    // Deserialize empty slice — should produce NOT NULL empty map
    Slice empty_slice("");
    auto st = serde->deserialize_one_cell_from_hive_text(*column, empty_slice, options);
    EXPECT_TRUE(st.ok()) << "deserialize nullable empty map failed: " << st.to_string();
    ASSERT_EQ(column->size(), 1u);

    // The top-level nullable column should NOT be null
    const auto& nullable_col = assert_cast<const ColumnNullable&>(*column);
    EXPECT_FALSE(nullable_col.is_null_at(0))
            << "empty map in nullable wrapper should not be null";
}

} // namespace doris::vectorized
