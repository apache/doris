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
#include "vec/columns/column_nullable.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

TEST(CheckAndGetColumnPtrTest, stdtest) {
    {
        std::shared_ptr<IColumn> column_ptr = std::make_shared<ColumnInt32>(42);

        EXPECT_EQ(column_ptr.use_count(), 1);
        auto column_i32 = std::dynamic_pointer_cast<ColumnInt32>(column_ptr);

        EXPECT_TRUE(column_i32);

        EXPECT_EQ(column_ptr.use_count(), 2);

        EXPECT_EQ(column_i32.use_count(), 2);
    }

    {
        std::shared_ptr<IColumn> column_ptr = std::make_shared<ColumnInt32>(42);

        EXPECT_EQ(column_ptr.use_count(), 1);
        auto column_i64 = std::dynamic_pointer_cast<ColumnInt64>(column_ptr);

        EXPECT_FALSE(column_i64);

        EXPECT_EQ(column_ptr.use_count(), 1);
    }
}

TEST(CheckAndGetColumnPtrTest, checktest) {
    {
        ColumnPtr column_ptr = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3});

        EXPECT_EQ(column_ptr->use_count(), 1);
        auto column_i32 = check_and_get_column_ptr<ColumnInt32>(column_ptr);

        EXPECT_TRUE(column_i32);

        EXPECT_EQ(column_ptr->use_count(), 2);

        EXPECT_EQ(column_i32->use_count(), 2);
    }

    {
        ColumnPtr column_ptr = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3});

        EXPECT_EQ(column_ptr->use_count(), 1);
        auto column_i64 = check_and_get_column_ptr<ColumnInt64>(column_ptr);

        EXPECT_FALSE(column_i64);

        EXPECT_EQ(column_ptr->use_count(), 1);
    }
}

TEST(CheckAndGetColumnPtrTest, errorTest) {
    {
        ColumnPtr column = ColumnConst::create(
                ColumnNullable::create(ColumnHelper::create_column<DataTypeInt32>({1}),
                                       ColumnHelper::create_column<DataTypeUInt8>({true})),
                3);

        ColumnPtr nested_column = column;

        nested_column = nested_column->convert_to_full_column_if_const();
        const auto* source_column = check_and_get_column<ColumnNullable>(nested_column.get());
        nested_column = source_column->get_nested_column_ptr();
        // The source_column is now a dangling pointer.
        // std::cout<<source_column->use_count()<<std::endl;
    }

    {
        ColumnPtr column = ColumnConst::create(
                ColumnNullable::create(ColumnHelper::create_column<DataTypeInt32>({1}),
                                       ColumnHelper::create_column<DataTypeUInt8>({true})),
                3);

        ColumnPtr nested_column = column;

        nested_column = nested_column->convert_to_full_column_if_const();
        const auto& source_column = assert_cast<const ColumnNullable&>(*nested_column);
        nested_column = source_column.get_nested_column_ptr();
        // The source_column is now a dangling pointer.
        // std::cout<<source_column.use_count()<<std::endl;
    }

    {
        ColumnPtr column = ColumnConst::create(
                ColumnNullable::create(ColumnHelper::create_column<DataTypeInt32>({1}),
                                       ColumnHelper::create_column<DataTypeUInt8>({true})),
                3);

        ColumnPtr nested_column = column;

        nested_column = nested_column->convert_to_full_column_if_const();
        const auto source_column = check_and_get_column_ptr<ColumnNullable>(nested_column);
        nested_column = source_column->get_nested_column_ptr();
        EXPECT_EQ(source_column->use_count(), 1);
    }
}

TEST(CheckAndGetColumnPtrTest, destructstest) {
    ColumnPtr column_ptr = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3});

    EXPECT_EQ(column_ptr->use_count(), 1);

    {
        auto column_i32 = check_and_get_column_ptr<ColumnInt32>(column_ptr);

        EXPECT_TRUE(column_i32);

        EXPECT_EQ(column_ptr->use_count(), 2);

        EXPECT_EQ(column_i32->use_count(), 2);
    }

    EXPECT_EQ(column_ptr->use_count(), 1);
}
} // namespace doris::vectorized