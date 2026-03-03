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
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_struct.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_struct.h"

namespace doris::vectorized {

class ColumnCheckConstOnlyInTopLevelTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

// Test ColumnNullable: nested column should not be const
TEST_F(ColumnCheckConstOnlyInTopLevelTest, ColumnNullableWithConstNested) {
    // Create a const column (nested column size must be 1 when create_with_empty=false)
    auto int_col = ColumnHelper::create_column<DataTypeInt32>({1});
    auto const_col = ColumnConst::create(int_col, 3, false, false);

    // Create a null map
    auto null_map = ColumnUInt8::create();
    null_map->insert_value(0);
    null_map->insert_value(0);
    null_map->insert_value(0);

    // Try to create ColumnNullable with const nested column - should throw
    EXPECT_THROW({ ColumnNullable::create(std::move(const_col), std::move(null_map)); },
                 doris::Exception);
}

// Test ColumnNullable: normal case (non-const nested column) should not throw
TEST_F(ColumnCheckConstOnlyInTopLevelTest, ColumnNullableWithNonConstNested) {
    auto int_col = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3});
    auto null_map = ColumnUInt8::create();
    null_map->insert_value(0);
    null_map->insert_value(0);
    null_map->insert_value(0);

    // Should not throw
    EXPECT_NO_THROW({
        auto nullable_col = ColumnNullable::create(std::move(int_col), std::move(null_map));
        EXPECT_EQ(nullable_col->size(), 3);
    });
}

// Test ColumnArray: nested data column should not be const
TEST_F(ColumnCheckConstOnlyInTopLevelTest, ColumnArrayWithConstData) {
    // Create a const column (nested column size must be 1 when create_with_empty=false)
    auto int_col = ColumnHelper::create_column<DataTypeInt32>({1});
    auto const_col = ColumnConst::create(int_col, 3, false, false);

    // Create offsets
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->insert_value(3);

    // Try to create ColumnArray with const data column - should throw
    EXPECT_THROW({ ColumnArray::create(std::move(const_col), std::move(offsets)); },
                 doris::Exception);
}

// Test ColumnArray: normal case should not throw
TEST_F(ColumnCheckConstOnlyInTopLevelTest, ColumnArrayWithNonConstData) {
    auto int_col = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3});
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->insert_value(3);

    // Should not throw
    EXPECT_NO_THROW({
        auto array_col = ColumnArray::create(std::move(int_col), std::move(offsets));
        EXPECT_EQ(array_col->size(), 1);
    });
}

// Test ColumnStruct: nested columns should not be const
TEST_F(ColumnCheckConstOnlyInTopLevelTest, ColumnStructWithConstElement) {
    // Create a const column (nested column size must be 1 when create_with_empty=false)
    auto int_col = ColumnHelper::create_column<DataTypeInt32>({1});
    auto const_col = ColumnConst::create(int_col, 3, false, false);

    MutableColumns columns;
    columns.push_back(std::move(const_col));

    // Try to create ColumnStruct with const element - should throw
    EXPECT_THROW({ ColumnStruct::create(std::move(columns)); }, doris::Exception);
}

// Test ColumnStruct: normal case should not throw
TEST_F(ColumnCheckConstOnlyInTopLevelTest, ColumnStructWithNonConstElements) {
    auto int_col1 = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3});
    auto int_col2 = ColumnHelper::create_column<DataTypeInt32>({4, 5, 6});

    MutableColumns columns;
    columns.push_back(int_col1->assume_mutable());
    columns.push_back(int_col2->assume_mutable());

    // Should not throw
    EXPECT_NO_THROW({
        auto struct_col = ColumnStruct::create(std::move(columns));
        EXPECT_EQ(struct_col->size(), 3);
    });
}

// Test ColumnMap: keys column should not be const
TEST_F(ColumnCheckConstOnlyInTopLevelTest, ColumnMapWithConstKeys) {
    // Create a const keys column (nested column size must be 1 when create_with_empty=false)
    auto keys_col = ColumnHelper::create_column<DataTypeInt32>({1});
    auto const_keys = ColumnConst::create(keys_col, 3, false, false);

    // Create normal values column
    auto values_col = ColumnHelper::create_column<DataTypeInt32>({10, 20, 30});

    // Create offsets
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->insert_value(3);

    // Try to create ColumnMap with const keys - should throw
    EXPECT_THROW(
            {
                ColumnMap::create(std::move(const_keys), std::move(values_col), std::move(offsets));
            },
            doris::Exception);
}

// Test ColumnMap: values column should not be const
TEST_F(ColumnCheckConstOnlyInTopLevelTest, ColumnMapWithConstValues) {
    // Create normal keys column
    auto keys_col = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3});

    // Create a const values column (nested column size must be 1 when create_with_empty=false)
    auto values_col = ColumnHelper::create_column<DataTypeInt32>({10});
    auto const_values = ColumnConst::create(values_col, 3, false, false);

    // Create offsets
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->insert_value(3);

    // Try to create ColumnMap with const values - should throw
    EXPECT_THROW(
            {
                ColumnMap::create(std::move(keys_col), std::move(const_values), std::move(offsets));
            },
            doris::Exception);
}

// Test ColumnMap: offsets column should not be const
TEST_F(ColumnCheckConstOnlyInTopLevelTest, ColumnMapWithConstOffsets) {
    // Create normal keys and values columns
    auto keys_col = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3});
    auto values_col = ColumnHelper::create_column<DataTypeInt32>({10, 20, 30});

    // Create a const offsets column (nested column size must be 1 when create_with_empty=false)
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->insert_value(3);
    auto const_offsets = ColumnConst::create(std::move(offsets), 1, false, false);

    // Try to create ColumnMap with const offsets - should throw
    EXPECT_THROW(
            {
                ColumnMap::create(std::move(keys_col), std::move(values_col),
                                  std::move(const_offsets));
            },
            doris::Exception);
}

// Test ColumnMap: normal case should not throw
TEST_F(ColumnCheckConstOnlyInTopLevelTest, ColumnMapWithNonConstColumns) {
    auto keys_col = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3});
    auto values_col = ColumnHelper::create_column<DataTypeInt32>({10, 20, 30});
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->insert_value(3);

    // Should not throw
    EXPECT_NO_THROW({
        auto map_col =
                ColumnMap::create(std::move(keys_col), std::move(values_col), std::move(offsets));
        EXPECT_EQ(map_col->size(), 1);
    });
}

// Test deeply nested const column - Array<Array<Int32>> with inner array being const
TEST_F(ColumnCheckConstOnlyInTopLevelTest, NestedArrayWithConstInnerArray) {
    // Create an inner array column (size 1 for const creation)
    auto inner_data = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3});
    auto inner_offsets = ColumnArray::ColumnOffsets::create();
    inner_offsets->insert_value(3);
    auto inner_array = ColumnArray::create(std::move(inner_data), std::move(inner_offsets));

    // Make the inner array const (nested column size must be 1 when create_with_empty=false)
    auto const_inner_array = ColumnConst::create(std::move(inner_array), 2, false, false);

    // Try to create outer array with const inner array - should throw
    auto outer_offsets = ColumnArray::ColumnOffsets::create();
    outer_offsets->insert_value(2);

    EXPECT_THROW({ ColumnArray::create(std::move(const_inner_array), std::move(outer_offsets)); },
                 doris::Exception);
}

// Test Nullable<Array<Int32>> with const array nested
TEST_F(ColumnCheckConstOnlyInTopLevelTest, NullableWithConstArray) {
    // Create an array column (size 1 for const creation)
    auto data = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3});
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->insert_value(3);
    auto array_col = ColumnArray::create(std::move(data), std::move(offsets));

    // Make the array const (nested column size must be 1 when create_with_empty=false)
    auto const_array = ColumnConst::create(std::move(array_col), 3, false, false);

    // Create null map
    auto null_map = ColumnUInt8::create();
    null_map->insert_value(0);
    null_map->insert_value(0);
    null_map->insert_value(0);

    // Try to create ColumnNullable with const array nested - should throw
    EXPECT_THROW({ ColumnNullable::create(std::move(const_array), std::move(null_map)); },
                 doris::Exception);
}

// Test Struct<Array<Int32>> with const array element
TEST_F(ColumnCheckConstOnlyInTopLevelTest, StructWithConstArrayElement) {
    // Create an array column (size 1 for const creation)
    auto data = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3});
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->insert_value(3);
    auto array_col = ColumnArray::create(std::move(data), std::move(offsets));

    // Make the array const (nested column size must be 1 when create_with_empty=false)
    auto const_array = ColumnConst::create(std::move(array_col), 3, false, false);

    MutableColumns columns;
    columns.push_back(std::move(const_array));

    // Try to create ColumnStruct with const array element - should throw
    EXPECT_THROW({ ColumnStruct::create(std::move(columns)); }, doris::Exception);
}

// Test check_const_only_in_top_level directly
TEST_F(ColumnCheckConstOnlyInTopLevelTest, DirectCheckConstOnlyInTopLevel) {
    // Normal column should not throw
    {
        auto int_col = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3});
        EXPECT_NO_THROW({ int_col->check_const_only_in_top_level(); });
    }

    // Normal nullable column should not throw
    {
        auto nullable_col =
                ColumnHelper::create_nullable_column<DataTypeInt32>({1, 2, 3}, {0, 0, 0});
        EXPECT_NO_THROW({ nullable_col->check_const_only_in_top_level(); });
    }

    // Normal array column should not throw
    {
        auto data = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3});
        auto offsets = ColumnArray::ColumnOffsets::create();
        offsets->insert_value(3);
        auto array_col = ColumnArray::create(std::move(data), std::move(offsets));
        EXPECT_NO_THROW({ array_col->check_const_only_in_top_level(); });
    }
}

// Test empty columns
TEST_F(ColumnCheckConstOnlyInTopLevelTest, EmptyColumnsNoThrow) {
    // Empty array should not throw
    {
        auto data = ColumnHelper::create_column<DataTypeInt32>({});
        auto array_col = ColumnArray::create(std::move(data));
        EXPECT_NO_THROW({ array_col->check_const_only_in_top_level(); });
    }

    // Empty map should not throw
    {
        auto keys = ColumnHelper::create_column<DataTypeInt32>({});
        auto values = ColumnHelper::create_column<DataTypeInt32>({});
        auto offsets = ColumnArray::ColumnOffsets::create();
        auto map_col = ColumnMap::create(std::move(keys), std::move(values), std::move(offsets));
        EXPECT_NO_THROW({ map_col->check_const_only_in_top_level(); });
    }
}

} // namespace doris::vectorized
