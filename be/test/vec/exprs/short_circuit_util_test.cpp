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

#include "vec/exprs/short_circuit_util.h"

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

// ==================== ColumnNullConstView Tests ====================

TEST(ShortCircuitUtilTest, ColumnNullConstView_non_nullable) {
    auto col = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3});
    auto view = ColumnNullConstView::create(col);

    EXPECT_EQ(view.column.size(), 3);
    EXPECT_EQ(view.null_map, nullptr);
    EXPECT_FALSE(view.is_const);
}

TEST(ShortCircuitUtilTest, ColumnNullConstView_nullable) {
    auto col = ColumnHelper::create_nullable_column<DataTypeInt32>({1, 2, 3}, {0, 1, 0});
    auto view = ColumnNullConstView::create(col);

    EXPECT_EQ(view.column.size(), 3);
    EXPECT_NE(view.null_map, nullptr);
    EXPECT_EQ((*view.null_map)[0], 0);
    EXPECT_EQ((*view.null_map)[1], 1);
    EXPECT_EQ((*view.null_map)[2], 0);
    EXPECT_FALSE(view.is_const);
}

TEST(ShortCircuitUtilTest, ColumnNullConstView_const_column) {
    auto col = ColumnHelper::create_column<DataTypeInt32>({42});
    ColumnPtr const_col = ColumnConst::create(col, 5);
    auto view = ColumnNullConstView::create(const_col);

    EXPECT_EQ(view.column.size(), 1);
    EXPECT_EQ(view.null_map, nullptr);
    EXPECT_TRUE(view.is_const);
}

TEST(ShortCircuitUtilTest, ColumnNullConstView_const_nullable_column) {
    auto col = ColumnHelper::create_nullable_column<DataTypeInt32>({42}, {0});
    ColumnPtr const_col = ColumnConst::create(col, 5);
    auto view = ColumnNullConstView::create(const_col);

    EXPECT_EQ(view.column.size(), 1);
    EXPECT_NE(view.null_map, nullptr);
    EXPECT_TRUE(view.is_const);
}

// ==================== ColumnNullConstViewScalar Tests ====================

TEST(ShortCircuitUtilTest, ColumnNullConstViewScalar_non_nullable) {
    auto col = ColumnHelper::create_column<DataTypeInt32>({10, 20, 30});
    auto view = ColumnNullConstViewScalar<TYPE_INT>::create(col);

    EXPECT_EQ(view.data.size(), 3);
    EXPECT_EQ(view.data[0], 10);
    EXPECT_EQ(view.data[1], 20);
    EXPECT_EQ(view.data[2], 30);
    EXPECT_EQ(view.null_map, nullptr);
    EXPECT_FALSE(view.is_const);
}

TEST(ShortCircuitUtilTest, ColumnNullConstViewScalar_nullable) {
    auto col = ColumnHelper::create_nullable_column<DataTypeInt32>({10, 20, 30}, {0, 1, 0});
    auto view = ColumnNullConstViewScalar<TYPE_INT>::create(col);

    EXPECT_EQ(view.data.size(), 3);
    EXPECT_EQ(view.data[0], 10);
    EXPECT_EQ(view.data[2], 30);
    EXPECT_NE(view.null_map, nullptr);
    EXPECT_EQ((*view.null_map)[1], 1);
    EXPECT_FALSE(view.is_const);
}

// ==================== MutableColumnNullView Tests ====================

TEST(ShortCircuitUtilTest, MutableColumnNullView_insert_from_non_nullable) {
    auto src_col = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3});
    auto src_view = ColumnNullConstView::create(src_col);

    MutableColumnPtr dst_col = ColumnInt32::create();
    auto dst_view = MutableColumnNullView::create(dst_col);

    dst_view.insert_from(src_view, 0);
    dst_view.insert_from(src_view, 1);
    dst_view.insert_from(src_view, 2);

    EXPECT_EQ(dst_col->size(), 3);
    EXPECT_EQ(dst_col->get_int(0), 1);
    EXPECT_EQ(dst_col->get_int(1), 2);
    EXPECT_EQ(dst_col->get_int(2), 3);
}

TEST(ShortCircuitUtilTest, MutableColumnNullView_insert_from_nullable_to_nullable) {
    auto src_col = ColumnHelper::create_nullable_column<DataTypeInt32>({1, 2, 3}, {0, 1, 0});
    auto src_view = ColumnNullConstView::create(src_col);

    auto nested_col = ColumnInt32::create();
    auto null_map = ColumnUInt8::create();
    MutableColumnPtr dst_col = ColumnNullable::create(std::move(nested_col), std::move(null_map));
    auto dst_view = MutableColumnNullView::create(dst_col);

    dst_view.insert_from(src_view, 0);
    dst_view.insert_from(src_view, 1);
    dst_view.insert_from(src_view, 2);

    EXPECT_EQ(dst_col->size(), 3);
    EXPECT_FALSE(dst_col->is_null_at(0));
    EXPECT_TRUE(dst_col->is_null_at(1));
    EXPECT_FALSE(dst_col->is_null_at(2));
}

TEST(ShortCircuitUtilTest, MutableColumnNullView_insert_null) {
    auto nested_col = ColumnInt32::create();
    auto null_map = ColumnUInt8::create();
    MutableColumnPtr dst_col = ColumnNullable::create(std::move(nested_col), std::move(null_map));
    auto dst_view = MutableColumnNullView::create(dst_col);

    dst_view.insert_null();
    dst_view.insert_null();

    EXPECT_EQ(dst_col->size(), 2);
    EXPECT_TRUE(dst_col->is_null_at(0));
    EXPECT_TRUE(dst_col->is_null_at(1));
}

// ==================== MutableColumnNullViewScalar Tests ====================

TEST(ShortCircuitUtilTest, MutableColumnNullViewScalar_insert_from) {
    auto src_col = ColumnHelper::create_column<DataTypeInt32>({100, 200, 300});
    auto src_view = ColumnNullConstViewScalar<TYPE_INT>::create(src_col);

    MutableColumnPtr dst_col = ColumnInt32::create();
    assert_cast<ColumnInt32*>(dst_col.get())->get_data().resize(5);
    auto dst_view = MutableColumnNullViewScalar<TYPE_INT>::create(dst_col);

    // 使用 selector 来指定插入位置
    Selector selector = {0, 2, 4};
    dst_view.insert_from(src_view, selector);

    EXPECT_EQ(dst_view.data[0], 100);
    EXPECT_EQ(dst_view.data[2], 200);
    EXPECT_EQ(dst_view.data[4], 300);
}

TEST(ShortCircuitUtilTest, MutableColumnNullViewScalar_insert_from_const) {
    auto src_col = ColumnHelper::create_column<DataTypeInt32>({42});
    ColumnPtr const_col = ColumnConst::create(src_col, 3);
    auto src_view = ColumnNullConstViewScalar<TYPE_INT>::create(const_col);

    EXPECT_TRUE(src_view.is_const);

    MutableColumnPtr dst_col = ColumnInt32::create();
    assert_cast<ColumnInt32*>(dst_col.get())->get_data().resize(3);
    auto dst_view = MutableColumnNullViewScalar<TYPE_INT>::create(dst_col);

    Selector selector = {0, 1, 2};
    dst_view.insert_from(src_view, selector);

    // 所有位置都应该是常量值 42
    EXPECT_EQ(dst_view.data[0], 42);
    EXPECT_EQ(dst_view.data[1], 42);
    EXPECT_EQ(dst_view.data[2], 42);
}

TEST(ShortCircuitUtilTest, MutableColumnNullViewScalar_insert_null) {
    auto nested_col = ColumnInt32::create();
    auto null_map = ColumnUInt8::create();
    MutableColumnPtr dst_col = ColumnNullable::create(std::move(nested_col), std::move(null_map));

    // 先 resize
    auto* nullable_col = assert_cast<ColumnNullable*>(dst_col.get());
    nullable_col->get_nested_column().resize(5);
    nullable_col->get_null_map_data().resize_fill(5, 0);

    auto dst_view = MutableColumnNullViewScalar<TYPE_INT>::create(dst_col);

    Selector null_selector = {1, 3};
    dst_view.insert_null(null_selector);

    EXPECT_EQ((*dst_view.null_map)[0], 0);
    EXPECT_EQ((*dst_view.null_map)[1], 1);
    EXPECT_EQ((*dst_view.null_map)[2], 0);
    EXPECT_EQ((*dst_view.null_map)[3], 1);
    EXPECT_EQ((*dst_view.null_map)[4], 0);
}

TEST(ShortCircuitUtilTest, MutableColumnNullViewScalar_insert_null_empty_selector) {
    auto nested_col = ColumnInt32::create();
    auto null_map = ColumnUInt8::create();
    MutableColumnPtr dst_col = ColumnNullable::create(std::move(nested_col), std::move(null_map));

    auto* nullable_col = assert_cast<ColumnNullable*>(dst_col.get());
    nullable_col->get_nested_column().resize(3);
    nullable_col->get_null_map_data().resize_fill(3, 0);

    auto dst_view = MutableColumnNullViewScalar<TYPE_INT>::create(dst_col);

    Selector empty_selector = {};
    dst_view.insert_null(empty_selector); // 不应该崩溃

    // null_map 应该保持不变
    EXPECT_EQ((*dst_view.null_map)[0], 0);
    EXPECT_EQ((*dst_view.null_map)[1], 0);
    EXPECT_EQ((*dst_view.null_map)[2], 0);
}

// ==================== ColumnAndSelector Tests ====================

TEST(ShortCircuitUtilTest, ColumnAndSelector_output_nulls) {
    {
        ColumnAndSelector cs;
        cs.column = nullptr;
        EXPECT_TRUE(cs.output_nulls());
    }
    {
        ColumnAndSelector cs;
        cs.column = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3});
        EXPECT_FALSE(cs.output_nulls());
    }
}

// ==================== ScalarFillWithSelector Tests ====================

TEST(ShortCircuitUtilTest, ScalarFillWithSelector_fill_true_false) {
    auto true_col = ColumnHelper::create_column<DataTypeInt32>({10, 20});
    auto false_col = ColumnHelper::create_column<DataTypeInt32>({30, 40, 50});

    Selector true_selector = {0, 2};
    Selector false_selector = {1, 3, 4};

    auto result_type = std::make_shared<DataTypeInt32>();
    auto result = ScalarFillWithSelector<TYPE_INT>::fill(result_type, true_col, true_selector,
                                                         false_col, false_selector, 5);

    EXPECT_EQ(result->size(), 5);
    EXPECT_EQ(result->get_int(0), 10); // from true_col[0]
    EXPECT_EQ(result->get_int(1), 30); // from false_col[0]
    EXPECT_EQ(result->get_int(2), 20); // from true_col[1]
    EXPECT_EQ(result->get_int(3), 40); // from false_col[1]
    EXPECT_EQ(result->get_int(4), 50); // from false_col[2]
}

TEST(ShortCircuitUtilTest, ScalarFillWithSelector_fill_nullable) {
    auto true_col = ColumnHelper::create_nullable_column<DataTypeInt32>({10, 20}, {0, 1});
    auto false_col = ColumnHelper::create_nullable_column<DataTypeInt32>({30}, {0});

    Selector true_selector = {0, 2};
    Selector false_selector = {1};

    auto result_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    auto result = ScalarFillWithSelector<TYPE_INT>::fill(result_type, true_col, true_selector,
                                                         false_col, false_selector, 3);

    EXPECT_EQ(result->size(), 3);
    EXPECT_FALSE(result->is_null_at(0)); // 10, not null
    EXPECT_FALSE(result->is_null_at(1)); // 30, not null
    EXPECT_TRUE(result->is_null_at(2));  // 20, null
}

TEST(ShortCircuitUtilTest, ScalarFillWithSelector_fill_with_columns_and_selectors) {
    std::vector<ColumnAndSelector> columns_and_selectors;

    // 第一个 column: 位置 0, 2
    ColumnAndSelector cs1;
    cs1.column = ColumnHelper::create_column<DataTypeInt32>({100, 200});
    cs1.selector = {0, 2};
    columns_and_selectors.push_back(std::move(cs1));

    // 第二个 column: 位置 1
    ColumnAndSelector cs2;
    cs2.column = ColumnHelper::create_column<DataTypeInt32>({300});
    cs2.selector = {1};
    columns_and_selectors.push_back(std::move(cs2));

    // 第三个: 输出 null，位置 3
    ColumnAndSelector cs3;
    cs3.column = nullptr;
    cs3.selector = {3};
    columns_and_selectors.push_back(std::move(cs3));

    auto result_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    auto result = ScalarFillWithSelector<TYPE_INT>::fill(result_type, columns_and_selectors, 4);

    EXPECT_EQ(result->size(), 4);
    const auto* nullable_result = assert_cast<const ColumnNullable*>(result.get());
    const auto& nested_col = assert_cast<const ColumnInt32&>(nullable_result->get_nested_column());
    EXPECT_EQ(nested_col.get_int(0), 100);
    EXPECT_EQ(nested_col.get_int(1), 300);
    EXPECT_EQ(nested_col.get_int(2), 200);
    EXPECT_TRUE(result->is_null_at(3));
}

// ==================== NonScalarFillWithSelector Tests ====================

TEST(ShortCircuitUtilTest, NonScalarFillWithSelector_fill_true_false) {
    auto true_col = ColumnHelper::create_column<DataTypeInt32>({10, 20});
    auto false_col = ColumnHelper::create_column<DataTypeInt32>({30, 40, 50});

    Selector true_selector = {0, 2};
    Selector false_selector = {1, 3, 4};

    auto result_type = std::make_shared<DataTypeInt32>();
    auto result = NonScalarFillWithSelector::fill(result_type, true_col, true_selector, false_col,
                                                  false_selector, 5);

    EXPECT_EQ(result->size(), 5);
    EXPECT_EQ(result->get_int(0), 10);
    EXPECT_EQ(result->get_int(1), 30);
    EXPECT_EQ(result->get_int(2), 20);
    EXPECT_EQ(result->get_int(3), 40);
    EXPECT_EQ(result->get_int(4), 50);
}

TEST(ShortCircuitUtilTest, NonScalarFillWithSelector_fill_string) {
    auto true_col = ColumnString::create();
    true_col->insert_data("hello", 5);
    true_col->insert_data("world", 5);

    auto false_col = ColumnString::create();
    false_col->insert_data("foo", 3);

    Selector true_selector = {0, 2};
    Selector false_selector = {1};

    auto result_type = std::make_shared<DataTypeString>();
    ColumnPtr true_col_ptr = std::move(true_col);
    ColumnPtr false_col_ptr = std::move(false_col);

    auto result = NonScalarFillWithSelector::fill(result_type, true_col_ptr, true_selector,
                                                  false_col_ptr, false_selector, 3);

    EXPECT_EQ(result->size(), 3);
    EXPECT_EQ(result->get_data_at(0).to_string(), "hello");
    EXPECT_EQ(result->get_data_at(1).to_string(), "foo");
    EXPECT_EQ(result->get_data_at(2).to_string(), "world");
}

TEST(ShortCircuitUtilTest, NonScalarFillWithSelector_fill_with_columns_and_selectors) {
    std::vector<ColumnAndSelector> columns_and_selectors;

    // 第一个 column
    ColumnAndSelector cs1;
    cs1.column = ColumnHelper::create_column<DataTypeInt32>({100, 200});
    cs1.selector = {0, 2};
    columns_and_selectors.push_back(std::move(cs1));

    // 第二个 column
    ColumnAndSelector cs2;
    cs2.column = ColumnHelper::create_column<DataTypeInt32>({300});
    cs2.selector = {1};
    columns_and_selectors.push_back(std::move(cs2));

    // 第三个: 输出 null
    ColumnAndSelector cs3;
    cs3.column = nullptr;
    cs3.selector = {3};
    columns_and_selectors.push_back(std::move(cs3));

    auto result_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    auto result = NonScalarFillWithSelector::fill(result_type, columns_and_selectors, 4);

    EXPECT_EQ(result->size(), 4);
    const auto* nullable_result = assert_cast<const ColumnNullable*>(result.get());
    const auto& nested_col = assert_cast<const ColumnInt32&>(nullable_result->get_nested_column());
    EXPECT_EQ(nested_col.get_int(0), 100);
    EXPECT_EQ(nested_col.get_int(1), 300);
    EXPECT_EQ(nested_col.get_int(2), 200);
    EXPECT_TRUE(result->is_null_at(3));
}

TEST(ShortCircuitUtilTest, NonScalarFillWithSelector_fill_string_with_null) {
    std::vector<ColumnAndSelector> columns_and_selectors;

    // 第一个 column: string
    auto str_col = ColumnString::create();
    str_col->insert_data("hello", 5);
    str_col->insert_data("world", 5);

    ColumnAndSelector cs1;
    cs1.column = std::move(str_col);
    cs1.selector = {0, 2};
    columns_and_selectors.push_back(std::move(cs1));

    // 第二个: null
    ColumnAndSelector cs2;
    cs2.column = nullptr;
    cs2.selector = {1};
    columns_and_selectors.push_back(std::move(cs2));

    auto result_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    auto result = NonScalarFillWithSelector::fill(result_type, columns_and_selectors, 3);

    EXPECT_EQ(result->size(), 3);
    EXPECT_FALSE(result->is_null_at(0));
    EXPECT_TRUE(result->is_null_at(1));
    EXPECT_FALSE(result->is_null_at(2));
    EXPECT_EQ(result->get_data_at(0).to_string(), "hello");
    EXPECT_EQ(result->get_data_at(2).to_string(), "world");
}

// ==================== Edge Cases ====================

TEST(ShortCircuitUtilTest, ScalarFillWithSelector_all_true) {
    auto true_col = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3});
    auto false_col = ColumnHelper::create_column<DataTypeInt32>({});

    Selector true_selector = {0, 1, 2};
    Selector false_selector = {};

    auto result_type = std::make_shared<DataTypeInt32>();
    auto result = ScalarFillWithSelector<TYPE_INT>::fill(result_type, true_col, true_selector,
                                                         false_col, false_selector, 3);

    EXPECT_EQ(result->size(), 3);
    EXPECT_EQ(result->get_int(0), 1);
    EXPECT_EQ(result->get_int(1), 2);
    EXPECT_EQ(result->get_int(2), 3);
}

TEST(ShortCircuitUtilTest, ScalarFillWithSelector_all_false) {
    auto true_col = ColumnHelper::create_column<DataTypeInt32>({});
    auto false_col = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3});

    Selector true_selector = {};
    Selector false_selector = {0, 1, 2};

    auto result_type = std::make_shared<DataTypeInt32>();
    auto result = ScalarFillWithSelector<TYPE_INT>::fill(result_type, true_col, true_selector,
                                                         false_col, false_selector, 3);

    EXPECT_EQ(result->size(), 3);
    EXPECT_EQ(result->get_int(0), 1);
    EXPECT_EQ(result->get_int(1), 2);
    EXPECT_EQ(result->get_int(2), 3);
}

TEST(ShortCircuitUtilTest, ScalarFillWithSelector_all_null) {
    std::vector<ColumnAndSelector> columns_and_selectors;

    ColumnAndSelector cs;
    cs.column = nullptr;
    cs.selector = {0, 1, 2};
    columns_and_selectors.push_back(std::move(cs));

    auto result_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    auto result = ScalarFillWithSelector<TYPE_INT>::fill(result_type, columns_and_selectors, 3);

    EXPECT_EQ(result->size(), 3);
    EXPECT_TRUE(result->is_null_at(0));
    EXPECT_TRUE(result->is_null_at(1));
    EXPECT_TRUE(result->is_null_at(2));
}

} // namespace doris::vectorized
