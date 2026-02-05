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

#include "runtime/primitive_type.h"
#include "testutil/column_helper.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_const.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

TEST(ColumnSelfCheckTest, const_check_test) {
    {
        ColumnPtr col = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3});
        EXPECT_EQ(col->const_nested_check(), true);
    }

    {
        ColumnPtr col = ColumnHelper::create_column<DataTypeInt32>({1});
        ColumnPtr const_col = ColumnConst::create(col, 3);
        EXPECT_EQ(const_col->const_nested_check(), true);
    }

    {
        ColumnPtr col = ColumnHelper::create_column<DataTypeInt32>({});

        auto array_const_col = ColumnArray::create(col);

        array_const_col->data = ColumnConst::create(col, 3, true);

        auto const_array = ColumnConst::create(std::move(array_const_col), 2, true);

        std::cout << const_array->dump_structure() << std::endl;

        std::cout << const_array->count_const_column() << std::endl;

        EXPECT_EQ(const_array->const_nested_check(), false);
    }

    {
        ColumnPtr col = ColumnHelper::create_column<DataTypeInt32>({});

        auto array_const_col = ColumnArray::create(col);

        array_const_col->data = ColumnConst::create(col, 3, true);

        auto const_array = ColumnConst::create(std::move(array_const_col), 2, true);

        Block block;
        block.insert({std::move(const_array),
                      std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>()),
                      "array_col"});

        EXPECT_FALSE(block.check_type_and_column());
    }
}

TEST(ColumnSelfCheckTest, nullmap_check_test) {
    {
        auto col = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3});
        EXPECT_EQ(col->null_map_check(), true);
    }

    {
        auto col = ColumnHelper::create_nullable_column<DataTypeInt32>({1, 2, 3}, {0, 0, 0});
        EXPECT_EQ(col->null_map_check(), true);
    }

    {
        auto col = ColumnHelper::create_nullable_column<DataTypeInt32>({1, 2, 3}, {0, 1, 0});
        EXPECT_EQ(col->null_map_check(), true);
    }

    {
        auto col = ColumnHelper::create_nullable_column<DataTypeInt32>({1, 2, 3}, {0, 2, 0});
        EXPECT_EQ(col->null_map_check(), false);
    }

    {
        auto col = ColumnHelper::create_nullable_column<DataTypeInt32>(
                {
                        1,
                },
                {3});

        auto col_const = ColumnConst::create(col, 2);

        EXPECT_EQ(col_const->null_map_check(), false);
    }

    {
        auto col = ColumnHelper::create_nullable_column<DataTypeInt32>(
                {
                        1,
                },
                {3});

        auto col_const = ColumnConst::create(col, 2);

        EXPECT_EQ(col_const->column_self_check(), false);
    }
}

TEST(ColumnSelfCheckTest, boolean_check) {
    {
        auto column_bool = ColumnHelper::create_column<DataTypeUInt8>({0, 1, 0, 1, 1});
        EXPECT_EQ(column_bool->column_boolean_check(), true);
    }
    {
        auto column_bool = ColumnHelper::create_column<DataTypeUInt8>({0, 1, 2, 1, 1});
        EXPECT_EQ(column_bool->column_boolean_check(), false);
    }

    {
        auto column_nullable_bool = ColumnHelper::create_nullable_column<DataTypeUInt8>(
                {0, 1, 0, 1, 1}, {0, 0, 0, 0, 0});
        EXPECT_EQ(column_nullable_bool->column_boolean_check(), true);
    }
    {
        auto column_nullable_bool = ColumnHelper::create_nullable_column<DataTypeUInt8>(
                {0, 1, 2, 1, 1}, {0, 0, 0, 0, 0});
        EXPECT_EQ(column_nullable_bool->column_boolean_check(), false);
    }

    {
        auto column_nullable_bool = ColumnHelper::create_nullable_column<DataTypeUInt8>(
                {0, 1, 2, 1, 1}, {0, 0, 1, 0, 0});
        EXPECT_EQ(column_nullable_bool->column_boolean_check(), true);
    }
}
} // namespace doris::vectorized