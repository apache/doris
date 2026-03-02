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

#include "vec/columns/column_execute_util.h"

#include <gtest/gtest.h>

#include "runtime/primitive_type.h"
#include "testutil/column_helper.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_string.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

// ==================== ColumnElementView Tests ====================

TEST(ColumnExecuteUtilTest, ColumnElementView_int32) {
    auto col = ColumnHelper::create_column<DataTypeInt32>({10, 20, 30});
    ColumnElementView<TYPE_INT> view(*col);

    EXPECT_EQ(view.get_element(0), 10);
    EXPECT_EQ(view.get_element(1), 20);
    EXPECT_EQ(view.get_element(2), 30);
}

TEST(ColumnExecuteUtilTest, ColumnElementView_int64) {
    auto col = ColumnHelper::create_column<DataTypeInt64>({100, 200, 300});
    ColumnElementView<TYPE_BIGINT> view(*col);

    EXPECT_EQ(view.get_element(0), 100);
    EXPECT_EQ(view.get_element(1), 200);
    EXPECT_EQ(view.get_element(2), 300);
}

TEST(ColumnExecuteUtilTest, ColumnElementView_double) {
    auto col = ColumnHelper::create_column<DataTypeFloat64>({1.5, 2.5, 3.5});
    ColumnElementView<TYPE_DOUBLE> view(*col);

    EXPECT_DOUBLE_EQ(view.get_element(0), 1.5);
    EXPECT_DOUBLE_EQ(view.get_element(1), 2.5);
    EXPECT_DOUBLE_EQ(view.get_element(2), 3.5);
}

TEST(ColumnExecuteUtilTest, ColumnElementView_string) {
    auto col = ColumnHelper::create_column<DataTypeString>({"hello", "world", "test"});
    ColumnElementView<TYPE_STRING> view(*col);

    EXPECT_EQ(view.get_element(0).to_string(), "hello");
    EXPECT_EQ(view.get_element(1).to_string(), "world");
    EXPECT_EQ(view.get_element(2).to_string(), "test");
}

// ==================== ColumnView Tests ====================

TEST(ColumnExecuteUtilTest, ColumnView_non_nullable_non_const) {
    auto col = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3});
    auto view = ColumnView<TYPE_INT>::create(col);

    EXPECT_EQ(view.count, 3);
    EXPECT_FALSE(view.is_const);
    EXPECT_EQ(view.null_map, nullptr);

    EXPECT_FALSE(view.is_null_at(0));
    EXPECT_FALSE(view.is_null_at(1));
    EXPECT_FALSE(view.is_null_at(2));

    EXPECT_EQ(view.value_at(0), 1);
    EXPECT_EQ(view.value_at(1), 2);
    EXPECT_EQ(view.value_at(2), 3);
}

TEST(ColumnExecuteUtilTest, ColumnView_nullable) {
    auto col = ColumnHelper::create_nullable_column<DataTypeInt32>({10, 20, 30}, {0, 1, 0});
    auto view = ColumnView<TYPE_INT>::create(col);

    EXPECT_EQ(view.count, 3);
    EXPECT_FALSE(view.is_const);
    EXPECT_NE(view.null_map, nullptr);

    EXPECT_FALSE(view.is_null_at(0));
    EXPECT_TRUE(view.is_null_at(1));
    EXPECT_FALSE(view.is_null_at(2));

    EXPECT_EQ(view.value_at(0), 10);
    EXPECT_EQ(view.value_at(2), 30);
}

TEST(ColumnExecuteUtilTest, ColumnView_const_column) {
    auto col = ColumnHelper::create_column<DataTypeInt32>({42});
    ColumnPtr const_col = ColumnConst::create(col, 5);
    auto view = ColumnView<TYPE_INT>::create(const_col);

    EXPECT_EQ(view.count, 5);
    EXPECT_TRUE(view.is_const);
    EXPECT_EQ(view.null_map, nullptr);

    // All positions should return the same value
    for (size_t i = 0; i < 5; ++i) {
        EXPECT_FALSE(view.is_null_at(i));
        EXPECT_EQ(view.value_at(i), 42);
    }
}

TEST(ColumnExecuteUtilTest, ColumnView_const_nullable_column) {
    auto col = ColumnHelper::create_nullable_column<DataTypeInt32>({100}, {0});
    ColumnPtr const_col = ColumnConst::create(col, 3);
    auto view = ColumnView<TYPE_INT>::create(const_col);

    EXPECT_EQ(view.count, 3);
    EXPECT_TRUE(view.is_const);
    EXPECT_NE(view.null_map, nullptr);

    for (size_t i = 0; i < 3; ++i) {
        EXPECT_FALSE(view.is_null_at(i));
        EXPECT_EQ(view.value_at(i), 100);
    }
}

TEST(ColumnExecuteUtilTest, ColumnView_const_nullable_null_column) {
    auto col = ColumnHelper::create_nullable_column<DataTypeInt32>({0}, {1});
    ColumnPtr const_col = ColumnConst::create(col, 3);
    auto view = ColumnView<TYPE_INT>::create(const_col);

    EXPECT_EQ(view.count, 3);
    EXPECT_TRUE(view.is_const);
    EXPECT_NE(view.null_map, nullptr);

    for (size_t i = 0; i < 3; ++i) {
        EXPECT_TRUE(view.is_null_at(i));
    }
}

TEST(ColumnExecuteUtilTest, ColumnView_string) {
    auto col = ColumnHelper::create_column<DataTypeString>({"a", "bb", "ccc"});
    auto view = ColumnView<TYPE_STRING>::create(col);

    EXPECT_EQ(view.count, 3);
    EXPECT_FALSE(view.is_const);
    EXPECT_EQ(view.null_map, nullptr);

    EXPECT_EQ(view.value_at(0).to_string(), "a");
    EXPECT_EQ(view.value_at(1).to_string(), "bb");
    EXPECT_EQ(view.value_at(2).to_string(), "ccc");
}

} // namespace doris::vectorized
