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

// ==================== CompileTimeColumnView Tests ====================

TEST(ColumnExecuteUtilTest, CompileTimeColumnView_non_const_non_nullable) {
    auto col = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3});
    auto view = ColumnView<TYPE_INT>::create(col);
    auto ct_view = create_compile_time_column_view(view);

    EXPECT_EQ(ct_view.index(), 0); // CompileTimeColumnView<TYPE_INT, false, false>

    std::visit(
            [](auto&& v) {
                EXPECT_EQ(v.value_at(0), 1);
                EXPECT_EQ(v.value_at(1), 2);
                EXPECT_EQ(v.value_at(2), 3);
                EXPECT_FALSE(v.is_null_at(0));
            },
            ct_view);
}

TEST(ColumnExecuteUtilTest, CompileTimeColumnView_const_non_nullable) {
    auto col = ColumnHelper::create_column<DataTypeInt32>({99});
    ColumnPtr const_col = ColumnConst::create(col, 4);
    auto view = ColumnView<TYPE_INT>::create(const_col);
    auto ct_view = create_compile_time_column_view(view);

    EXPECT_EQ(ct_view.index(), 1); // CompileTimeColumnView<TYPE_INT, true, false>

    std::visit(
            [](auto&& v) {
                // const column returns same value for all indices
                EXPECT_EQ(v.value_at(0), 99);
                EXPECT_EQ(v.value_at(10), 99); // Index doesn't matter for const
                EXPECT_FALSE(v.is_null_at(0));
            },
            ct_view);
}

TEST(ColumnExecuteUtilTest, CompileTimeColumnView_non_const_nullable) {
    auto col = ColumnHelper::create_nullable_column<DataTypeInt32>({5, 6, 7}, {0, 1, 0});
    auto view = ColumnView<TYPE_INT>::create(col);
    auto ct_view = create_compile_time_column_view(view);

    EXPECT_EQ(ct_view.index(), 2); // CompileTimeColumnView<TYPE_INT, false, true>

    std::visit(
            [](auto&& v) {
                EXPECT_EQ(v.value_at(0), 5);
                EXPECT_EQ(v.value_at(2), 7);
                EXPECT_FALSE(v.is_null_at(0));
                EXPECT_TRUE(v.is_null_at(1));
                EXPECT_FALSE(v.is_null_at(2));
            },
            ct_view);
}

TEST(ColumnExecuteUtilTest, CompileTimeColumnView_const_nullable) {
    auto col = ColumnHelper::create_nullable_column<DataTypeInt32>({50}, {0});
    ColumnPtr const_col = ColumnConst::create(col, 3);
    auto view = ColumnView<TYPE_INT>::create(const_col);
    auto ct_view = create_compile_time_column_view(view);

    EXPECT_EQ(ct_view.index(), 3); // CompileTimeColumnView<TYPE_INT, true, true>

    std::visit(
            [](auto&& v) {
                EXPECT_EQ(v.value_at(0), 50);
                EXPECT_EQ(v.value_at(1), 50);
                EXPECT_FALSE(v.is_null_at(0));
            },
            ct_view);
}

TEST(ColumnExecuteUtilTest, CompileTimeColumnViewOnlyConst_non_const) {
    auto col = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3});
    auto view = ColumnView<TYPE_INT>::create(col);
    auto ct_view = create_compile_time_column_view_only_const(view);

    EXPECT_EQ(ct_view.index(), 0); // CompileTimeColumnView<TYPE_INT, false, false>
}

TEST(ColumnExecuteUtilTest, CompileTimeColumnViewOnlyConst_const) {
    auto col = ColumnHelper::create_column<DataTypeInt32>({42});
    ColumnPtr const_col = ColumnConst::create(col, 3);
    auto view = ColumnView<TYPE_INT>::create(const_col);
    auto ct_view = create_compile_time_column_view_only_const(view);

    EXPECT_EQ(ct_view.index(), 1); // CompileTimeColumnView<TYPE_INT, true, false>
}

// ==================== ExecuteColumn Unary Tests ====================

TEST(ColumnExecuteUtilTest, ExecuteColumn_unary_runtime_basic) {
    auto col = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3});
    std::vector<Int32> results;

    auto null_func = [](size_t i) {};
    auto func = [&results](size_t i, Int32 val) { results.push_back(val * 2); };

    ExecuteColumn::execute_unary_runtime<TYPE_INT>(col, null_func, func);

    ASSERT_EQ(results.size(), 3);
    EXPECT_EQ(results[0], 2);
    EXPECT_EQ(results[1], 4);
    EXPECT_EQ(results[2], 6);
}

TEST(ColumnExecuteUtilTest, ExecuteColumn_unary_runtime_nullable) {
    auto col = ColumnHelper::create_nullable_column<DataTypeInt32>({10, 20, 30}, {0, 1, 0});
    std::vector<Int32> results;
    std::vector<size_t> null_indices;

    auto null_func = [&null_indices](size_t i) { null_indices.push_back(i); };
    auto func = [&results](size_t i, Int32 val) { results.push_back(val); };

    ExecuteColumn::execute_unary_runtime<TYPE_INT>(col, null_func, func);

    ASSERT_EQ(results.size(), 2);
    EXPECT_EQ(results[0], 10);
    EXPECT_EQ(results[1], 30);

    ASSERT_EQ(null_indices.size(), 1);
    EXPECT_EQ(null_indices[0], 1);
}

TEST(ColumnExecuteUtilTest, ExecuteColumn_unary_runtime_const) {
    auto col = ColumnHelper::create_column<DataTypeInt32>({5});
    ColumnPtr const_col = ColumnConst::create(col, 4);
    std::vector<Int32> results;

    auto null_func = [](size_t i) {};
    auto func = [&results](size_t i, Int32 val) { results.push_back(val); };

    ExecuteColumn::execute_unary_runtime<TYPE_INT>(const_col, null_func, func);

    ASSERT_EQ(results.size(), 4);
    for (auto r : results) {
        EXPECT_EQ(r, 5);
    }
}

TEST(ColumnExecuteUtilTest, ExecuteColumn_unary_compile_time) {
    auto col = ColumnHelper::create_column<DataTypeInt64>({100, 200, 300});
    std::vector<Int64> results;

    auto null_func = [](size_t i) {};
    auto func = [&results](size_t i, Int64 val) { results.push_back(val + 1); };

    ExecuteColumn::execute_unary_compile_time<TYPE_BIGINT>(col, null_func, func);

    ASSERT_EQ(results.size(), 3);
    EXPECT_EQ(results[0], 101);
    EXPECT_EQ(results[1], 201);
    EXPECT_EQ(results[2], 301);
}

TEST(ColumnExecuteUtilTest, ExecuteColumn_unary_compile_time_nullable) {
    auto col = ColumnHelper::create_nullable_column<DataTypeInt32>({1, 2, 3, 4}, {0, 1, 1, 0});
    std::vector<Int32> results;
    std::vector<size_t> null_indices;

    auto null_func = [&null_indices](size_t i) { null_indices.push_back(i); };
    auto func = [&results](size_t i, Int32 val) { results.push_back(val); };

    ExecuteColumn::execute_unary_compile_time<TYPE_INT>(col, null_func, func);

    ASSERT_EQ(results.size(), 2);
    EXPECT_EQ(results[0], 1);
    EXPECT_EQ(results[1], 4);

    ASSERT_EQ(null_indices.size(), 2);
    EXPECT_EQ(null_indices[0], 1);
    EXPECT_EQ(null_indices[1], 2);
}

TEST(ColumnExecuteUtilTest, ExecuteColumn_unary_compile_time_only_const) {
    auto col = ColumnHelper::create_column<DataTypeInt32>({7, 8, 9});
    std::vector<Int32> results;

    auto func = [&results](size_t i, Int32 val) { results.push_back(val * 10); };

    ExecuteColumn::execute_unary_compile_time_only_const<TYPE_INT>(col, func);

    ASSERT_EQ(results.size(), 3);
    EXPECT_EQ(results[0], 70);
    EXPECT_EQ(results[1], 80);
    EXPECT_EQ(results[2], 90);
}

TEST(ColumnExecuteUtilTest, ExecuteColumn_unary_string) {
    auto col = ColumnHelper::create_column<DataTypeString>({"abc", "defg", "hi"});
    std::vector<size_t> lengths;

    auto null_func = [](size_t i) {};
    auto func = [&lengths](size_t i, StringRef val) { lengths.push_back(val.size); };

    ExecuteColumn::execute_unary_runtime<TYPE_STRING>(col, null_func, func);

    ASSERT_EQ(lengths.size(), 3);
    EXPECT_EQ(lengths[0], 3);
    EXPECT_EQ(lengths[1], 4);
    EXPECT_EQ(lengths[2], 2);
}

// ==================== ExecuteColumn Binary Tests ====================

TEST(ColumnExecuteUtilTest, ExecuteColumn_binary_runtime_basic) {
    auto col_a = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3});
    auto col_b = ColumnHelper::create_column<DataTypeInt32>({10, 20, 30});
    std::vector<Int32> results;

    auto null_func = [](size_t i) {};
    auto func = [&results](size_t i, Int32 a, Int32 b) { results.push_back(a + b); };

    ExecuteColumn::execute_binary_runtime<TYPE_INT, TYPE_INT>(col_a, col_b, null_func, func);

    ASSERT_EQ(results.size(), 3);
    EXPECT_EQ(results[0], 11);
    EXPECT_EQ(results[1], 22);
    EXPECT_EQ(results[2], 33);
}

TEST(ColumnExecuteUtilTest, ExecuteColumn_binary_runtime_with_nulls) {
    auto col_a = ColumnHelper::create_nullable_column<DataTypeInt32>({1, 2, 3}, {0, 1, 0});
    auto col_b = ColumnHelper::create_nullable_column<DataTypeInt32>({10, 20, 30}, {0, 0, 1});
    std::vector<Int32> results;
    std::vector<size_t> null_indices;

    auto null_func = [&null_indices](size_t i) { null_indices.push_back(i); };
    auto func = [&results](size_t i, Int32 a, Int32 b) { results.push_back(a * b); };

    ExecuteColumn::execute_binary_runtime<TYPE_INT, TYPE_INT>(col_a, col_b, null_func, func);

    // Only index 0 should have valid data (both non-null)
    ASSERT_EQ(results.size(), 1);
    EXPECT_EQ(results[0], 10);

    // Index 1 has null in col_a, index 2 has null in col_b
    ASSERT_EQ(null_indices.size(), 2);
    EXPECT_EQ(null_indices[0], 1);
    EXPECT_EQ(null_indices[1], 2);
}

TEST(ColumnExecuteUtilTest, ExecuteColumn_binary_compile_time) {
    auto col_a = ColumnHelper::create_column<DataTypeInt32>({2, 4, 6});
    auto col_b = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3});
    std::vector<Int32> results;

    auto null_func = [](size_t i) {};
    auto func = [&results](size_t i, Int32 a, Int32 b) { results.push_back(a - b); };

    ExecuteColumn::execute_binary_compile_time<TYPE_INT, TYPE_INT>(col_a, col_b, null_func, func);

    ASSERT_EQ(results.size(), 3);
    EXPECT_EQ(results[0], 1);
    EXPECT_EQ(results[1], 2);
    EXPECT_EQ(results[2], 3);
}

TEST(ColumnExecuteUtilTest, ExecuteColumn_binary_compile_time_const_and_vector) {
    auto col_a = ColumnHelper::create_column<DataTypeInt32>({10});
    ColumnPtr const_col_a = ColumnConst::create(col_a, 3);
    auto col_b = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3});
    std::vector<Int32> results;

    auto null_func = [](size_t i) {};
    auto func = [&results](size_t i, Int32 a, Int32 b) { results.push_back(a + b); };

    ExecuteColumn::execute_binary_compile_time<TYPE_INT, TYPE_INT>(const_col_a, col_b, null_func,
                                                                   func);

    ASSERT_EQ(results.size(), 3);
    EXPECT_EQ(results[0], 11);
    EXPECT_EQ(results[1], 12);
    EXPECT_EQ(results[2], 13);
}

TEST(ColumnExecuteUtilTest, ExecuteColumn_binary_compile_time_only_const) {
    auto col_a = ColumnHelper::create_column<DataTypeFloat64>({1.0, 2.0, 3.0});
    auto col_b = ColumnHelper::create_column<DataTypeFloat64>({0.5, 1.5, 2.5});
    std::vector<double> results;

    auto func = [&results](size_t i, double a, double b) { results.push_back(a * b); };

    ExecuteColumn::execute_binary_compile_time_only_const<TYPE_DOUBLE, TYPE_DOUBLE>(col_a, col_b,
                                                                                    func);

    ASSERT_EQ(results.size(), 3);
    EXPECT_DOUBLE_EQ(results[0], 0.5);
    EXPECT_DOUBLE_EQ(results[1], 3.0);
    EXPECT_DOUBLE_EQ(results[2], 7.5);
}

TEST(ColumnExecuteUtilTest, ExecuteColumn_binary_different_types) {
    auto col_a = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3});
    auto col_b = ColumnHelper::create_column<DataTypeFloat64>({1.5, 2.5, 3.5});
    std::vector<double> results;

    auto null_func = [](size_t i) {};
    auto func = [&results](size_t i, Int32 a, double b) { results.push_back(a + b); };

    ExecuteColumn::execute_binary_compile_time<TYPE_INT, TYPE_DOUBLE>(col_a, col_b, null_func,
                                                                      func);

    ASSERT_EQ(results.size(), 3);
    EXPECT_DOUBLE_EQ(results[0], 2.5);
    EXPECT_DOUBLE_EQ(results[1], 4.5);
    EXPECT_DOUBLE_EQ(results[2], 6.5);
}

// ==================== ExecuteColumn Ternary Tests ====================

TEST(ColumnExecuteUtilTest, ExecuteColumn_ternary_runtime_basic) {
    auto col_a = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3});
    auto col_b = ColumnHelper::create_column<DataTypeInt32>({10, 20, 30});
    auto col_c = ColumnHelper::create_column<DataTypeInt32>({100, 200, 300});
    std::vector<Int32> results;

    auto null_func = [](size_t i) {};
    auto func = [&results](size_t i, Int32 a, Int32 b, Int32 c) { results.push_back(a + b + c); };

    ExecuteColumn::execute_ternary_runtime<TYPE_INT, TYPE_INT, TYPE_INT>(col_a, col_b, col_c,
                                                                         null_func, func);

    ASSERT_EQ(results.size(), 3);
    EXPECT_EQ(results[0], 111);
    EXPECT_EQ(results[1], 222);
    EXPECT_EQ(results[2], 333);
}

TEST(ColumnExecuteUtilTest, ExecuteColumn_ternary_runtime_with_nulls) {
    auto col_a = ColumnHelper::create_nullable_column<DataTypeInt32>({1, 2, 3}, {1, 0, 0});
    auto col_b = ColumnHelper::create_nullable_column<DataTypeInt32>({10, 20, 30}, {0, 1, 0});
    auto col_c = ColumnHelper::create_nullable_column<DataTypeInt32>({100, 200, 300}, {0, 0, 1});
    std::vector<Int32> results;
    std::vector<size_t> null_indices;

    auto null_func = [&null_indices](size_t i) { null_indices.push_back(i); };
    auto func = [&results](size_t i, Int32 a, Int32 b, Int32 c) { results.push_back(a + b + c); };

    ExecuteColumn::execute_ternary_runtime<TYPE_INT, TYPE_INT, TYPE_INT>(col_a, col_b, col_c,
                                                                         null_func, func);

    // All indices have at least one null
    EXPECT_TRUE(results.empty());
    ASSERT_EQ(null_indices.size(), 3);
}

TEST(ColumnExecuteUtilTest, ExecuteColumn_ternary_compile_time) {
    auto col_a = ColumnHelper::create_column<DataTypeInt32>({1, 2});
    auto col_b = ColumnHelper::create_column<DataTypeInt32>({3, 4});
    auto col_c = ColumnHelper::create_column<DataTypeInt32>({5, 6});
    std::vector<Int32> results;

    auto null_func = [](size_t i) {};
    auto func = [&results](size_t i, Int32 a, Int32 b, Int32 c) { results.push_back(a * b * c); };

    ExecuteColumn::execute_ternary_compile_time<TYPE_INT, TYPE_INT, TYPE_INT>(col_a, col_b, col_c,
                                                                              null_func, func);

    ASSERT_EQ(results.size(), 2);
    EXPECT_EQ(results[0], 15); // 1 * 3 * 5
    EXPECT_EQ(results[1], 48); // 2 * 4 * 6
}

TEST(ColumnExecuteUtilTest, ExecuteColumn_ternary_compile_time_with_const) {
    auto col_a = ColumnHelper::create_column<DataTypeInt32>({10});
    ColumnPtr const_col_a = ColumnConst::create(col_a, 3);
    auto col_b = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3});
    auto col_c = ColumnHelper::create_column<DataTypeInt32>({100, 200, 300});
    std::vector<Int32> results;

    auto null_func = [](size_t i) {};
    auto func = [&results](size_t i, Int32 a, Int32 b, Int32 c) { results.push_back(a + b + c); };

    ExecuteColumn::execute_ternary_compile_time<TYPE_INT, TYPE_INT, TYPE_INT>(
            const_col_a, col_b, col_c, null_func, func);

    ASSERT_EQ(results.size(), 3);
    EXPECT_EQ(results[0], 111); // 10 + 1 + 100
    EXPECT_EQ(results[1], 212); // 10 + 2 + 200
    EXPECT_EQ(results[2], 313); // 10 + 3 + 300
}

TEST(ColumnExecuteUtilTest, ExecuteColumn_ternary_compile_time_only_const) {
    auto col_a = ColumnHelper::create_column<DataTypeInt32>({1, 2});
    auto col_b = ColumnHelper::create_column<DataTypeInt32>({3, 4});
    auto col_c = ColumnHelper::create_column<DataTypeInt32>({5, 6});
    std::vector<Int32> results;

    auto func = [&results](size_t i, Int32 a, Int32 b, Int32 c) { results.push_back(a + b - c); };

    ExecuteColumn::execute_ternary_compile_time_only_const<TYPE_INT, TYPE_INT, TYPE_INT>(
            col_a, col_b, col_c, func);

    ASSERT_EQ(results.size(), 2);
    EXPECT_EQ(results[0], -1); // 1 + 3 - 5
    EXPECT_EQ(results[1], 0);  // 2 + 4 - 6
}

TEST(ColumnExecuteUtilTest, ExecuteColumn_ternary_string) {
    auto col_a = ColumnHelper::create_column<DataTypeString>({"hello", "world"});
    auto col_b = ColumnHelper::create_column<DataTypeString>({" ", "-"});
    auto col_c = ColumnHelper::create_column<DataTypeString>({"world", "hello"});
    std::vector<std::string> results;

    auto func = [&results](size_t i, StringRef a, StringRef b, StringRef c) {
        results.push_back(a.to_string() + b.to_string() + c.to_string());
    };

    ExecuteColumn::execute_ternary_compile_time_only_const<TYPE_STRING, TYPE_STRING, TYPE_STRING>(
            col_a, col_b, col_c, func);

    ASSERT_EQ(results.size(), 2);
    EXPECT_EQ(results[0], "hello world");
    EXPECT_EQ(results[1], "world-hello");
}

// ==================== Edge Cases ====================

TEST(ColumnExecuteUtilTest, ExecuteColumn_empty_column) {
    auto col = ColumnHelper::create_column<DataTypeInt32>({});
    std::vector<Int32> results;

    auto null_func = [](size_t i) {};
    auto func = [&results](size_t i, Int32 val) { results.push_back(val); };

    ExecuteColumn::execute_unary_runtime<TYPE_INT>(col, null_func, func);

    EXPECT_TRUE(results.empty());
}

TEST(ColumnExecuteUtilTest, ExecuteColumn_all_nulls) {
    auto col = ColumnHelper::create_nullable_column<DataTypeInt32>({1, 2, 3}, {1, 1, 1});
    std::vector<Int32> results;
    std::vector<size_t> null_indices;

    auto null_func = [&null_indices](size_t i) { null_indices.push_back(i); };
    auto func = [&results](size_t i, Int32 val) { results.push_back(val); };

    ExecuteColumn::execute_unary_runtime<TYPE_INT>(col, null_func, func);

    EXPECT_TRUE(results.empty());
    ASSERT_EQ(null_indices.size(), 3);
}

TEST(ColumnExecuteUtilTest, ExecuteColumn_single_element) {
    auto col = ColumnHelper::create_column<DataTypeInt32>({42});
    std::vector<Int32> results;

    auto null_func = [](size_t i) {};
    auto func = [&results](size_t i, Int32 val) { results.push_back(val); };

    ExecuteColumn::execute_unary_compile_time<TYPE_INT>(col, null_func, func);

    ASSERT_EQ(results.size(), 1);
    EXPECT_EQ(results[0], 42);
}

// ==================== Real-world Use Case Tests ====================

TEST(ColumnExecuteUtilTest, ExecuteColumn_real_use_case_double_values) {
    // Simulate doubling all values in a column
    auto col = ColumnHelper::create_column<DataTypeInt64>({10, 20, 30, 40, 50});
    auto result_col = ColumnInt64::create();
    result_col->get_data().resize(5);

    auto func = [&result_col](size_t i, Int64 val) { result_col->get_data()[i] = val * 2; };

    ExecuteColumn::execute_unary_compile_time_only_const<TYPE_BIGINT>(col, func);

    EXPECT_EQ(result_col->get_data()[0], 20);
    EXPECT_EQ(result_col->get_data()[1], 40);
    EXPECT_EQ(result_col->get_data()[2], 60);
    EXPECT_EQ(result_col->get_data()[3], 80);
    EXPECT_EQ(result_col->get_data()[4], 100);
}

TEST(ColumnExecuteUtilTest, ExecuteColumn_real_use_case_binary_add_with_null_handling) {
    // Simulate binary addition with null handling
    auto col_a = ColumnHelper::create_nullable_column<DataTypeInt32>({1, 2, 3, 4}, {0, 1, 0, 0});
    auto col_b =
            ColumnHelper::create_nullable_column<DataTypeInt32>({10, 20, 30, 40}, {0, 0, 1, 0});

    auto result_col = ColumnInt32::create(4, 0);
    auto result_null_map = ColumnUInt8::create(4, 0);

    auto null_func = [&result_null_map](size_t i) { result_null_map->get_data()[i] = 1; };
    auto func = [&result_col](size_t i, Int32 a, Int32 b) { result_col->get_data()[i] = a + b; };

    ExecuteColumn::execute_binary_compile_time<TYPE_INT, TYPE_INT>(col_a, col_b, null_func, func);

    // Check results
    EXPECT_EQ(result_col->get_data()[0], 11);
    EXPECT_EQ(result_null_map->get_data()[0], 0);

    EXPECT_EQ(result_null_map->get_data()[1], 1); // null because col_a[1] is null

    EXPECT_EQ(result_null_map->get_data()[2], 1); // null because col_b[2] is null

    EXPECT_EQ(result_col->get_data()[3], 44);
    EXPECT_EQ(result_null_map->get_data()[3], 0);
}

TEST(ColumnExecuteUtilTest, ExecuteColumn_real_use_case_const_broadcast) {
    // Test const column broadcast - common scenario in SQL like "SELECT col + 10"
    auto col = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3, 4, 5});
    auto const_val = ColumnHelper::create_column<DataTypeInt32>({10});
    ColumnPtr const_col = ColumnConst::create(const_val, 5);

    auto result_col = ColumnInt32::create();
    result_col->get_data().resize(5);

    auto func = [&result_col](size_t i, Int32 a, Int32 b) { result_col->get_data()[i] = a + b; };

    ExecuteColumn::execute_binary_compile_time_only_const<TYPE_INT, TYPE_INT>(col, const_col, func);

    EXPECT_EQ(result_col->get_data()[0], 11);
    EXPECT_EQ(result_col->get_data()[1], 12);
    EXPECT_EQ(result_col->get_data()[2], 13);
    EXPECT_EQ(result_col->get_data()[3], 14);
    EXPECT_EQ(result_col->get_data()[4], 15);
}

} // namespace doris::vectorized
