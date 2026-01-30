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

    // Use selector to specify insert positions
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

    // All positions should have the constant value 42
    EXPECT_EQ(dst_view.data[0], 42);
    EXPECT_EQ(dst_view.data[1], 42);
    EXPECT_EQ(dst_view.data[2], 42);
}

TEST(ShortCircuitUtilTest, MutableColumnNullViewScalar_insert_null) {
    auto nested_col = ColumnInt32::create();
    auto null_map = ColumnUInt8::create();
    MutableColumnPtr dst_col = ColumnNullable::create(std::move(nested_col), std::move(null_map));

    // Resize first
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
    dst_view.insert_null(empty_selector); // Should not crash

    // null_map should remain unchanged
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

TEST(ShortCircuitUtilTest, ColumnAndSelector_debug_string) {
    // Test with null column
    {
        ColumnAndSelector cs;
        cs.column = nullptr;
        cs.selector = {0, 1, 2};
        std::string debug_str = cs.debug_string();
        EXPECT_NE(debug_str.find("selector_size=3"), std::string::npos);
        EXPECT_NE(debug_str.find("output_nulls=1"), std::string::npos);
        EXPECT_NE(debug_str.find("column size=null"), std::string::npos);
    }
    // Test with non-null column
    {
        ColumnAndSelector cs;
        cs.column = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3, 4, 5});
        cs.selector = {0, 2};
        std::string debug_str = cs.debug_string();
        EXPECT_NE(debug_str.find("selector_size=2"), std::string::npos);
        EXPECT_NE(debug_str.find("output_nulls=0"), std::string::npos);
        EXPECT_NE(debug_str.find("column size=5"), std::string::npos);
    }
    // Test with empty selector
    {
        ColumnAndSelector cs;
        cs.column = ColumnHelper::create_column<DataTypeInt32>({1});
        cs.selector = {};
        std::string debug_str = cs.debug_string();
        EXPECT_NE(debug_str.find("selector_size=0"), std::string::npos);
        EXPECT_NE(debug_str.find("output_nulls=0"), std::string::npos);
        EXPECT_NE(debug_str.find("column size=1"), std::string::npos);
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

    // First column: positions 0, 2
    ColumnAndSelector cs1;
    cs1.column = ColumnHelper::create_column<DataTypeInt32>({100, 200});
    cs1.selector = {0, 2};
    columns_and_selectors.push_back(std::move(cs1));

    // Second column: position 1
    ColumnAndSelector cs2;
    cs2.column = ColumnHelper::create_column<DataTypeInt32>({300});
    cs2.selector = {1};
    columns_and_selectors.push_back(std::move(cs2));

    // Third: output null, position 3
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

    // First column
    ColumnAndSelector cs1;
    cs1.column = ColumnHelper::create_column<DataTypeInt32>({100, 200});
    cs1.selector = {0, 2};
    columns_and_selectors.push_back(std::move(cs1));

    // Second column
    ColumnAndSelector cs2;
    cs2.column = ColumnHelper::create_column<DataTypeInt32>({300});
    cs2.selector = {1};
    columns_and_selectors.push_back(std::move(cs2));

    // Third: output null
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

    // First column: string
    auto str_col = ColumnString::create();
    str_col->insert_data("hello", 5);
    str_col->insert_data("world", 5);

    ColumnAndSelector cs1;
    cs1.column = std::move(str_col);
    cs1.selector = {0, 2};
    columns_and_selectors.push_back(std::move(cs1));

    // Second: null
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

// ==================== ConditionColumnView Tests ====================

TEST(ShortCircuitUtilTest, ConditionColumnView_non_nullable_no_selector) {
    // Create a boolean column: [true, false, true, false]
    auto col = ColumnHelper::create_column<DataTypeUInt8>({1, 0, 1, 0});

    auto view = ConditionColumnView::create(col, nullptr, 4);

    Selector null_selector;
    Selector true_selector;
    Selector false_selector;

    auto null_func = [&](size_t self_index, size_t executor_index) {
        null_selector.push_back(executor_index);
    };
    auto true_func = [&](size_t self_index, size_t executor_index) {
        true_selector.push_back(executor_index);
    };
    auto false_func = [&](size_t self_index, size_t executor_index) {
        false_selector.push_back(executor_index);
    };

    view.for_each(null_func, true_func, false_func);

    EXPECT_EQ(null_selector.size(), 0);
    EXPECT_EQ(true_selector.size(), 2);
    EXPECT_EQ(false_selector.size(), 2);

    EXPECT_EQ(true_selector[0], 0);
    EXPECT_EQ(true_selector[1], 2);
    EXPECT_EQ(false_selector[0], 1);
    EXPECT_EQ(false_selector[1], 3);
}

TEST(ShortCircuitUtilTest, ConditionColumnView_nullable_no_selector) {
    // Create a nullable boolean column: [true, NULL, false, NULL]
    auto col = ColumnHelper::create_nullable_column<DataTypeUInt8>({1, 0, 0, 0}, {0, 1, 0, 1});

    auto view = ConditionColumnView::create(col, nullptr, 4);

    Selector null_selector;
    Selector true_selector;
    Selector false_selector;

    auto null_func = [&](size_t self_index, size_t executor_index) {
        null_selector.push_back(executor_index);
    };
    auto true_func = [&](size_t self_index, size_t executor_index) {
        true_selector.push_back(executor_index);
    };
    auto false_func = [&](size_t self_index, size_t executor_index) {
        false_selector.push_back(executor_index);
    };

    view.for_each(null_func, true_func, false_func);

    EXPECT_EQ(null_selector.size(), 2);
    EXPECT_EQ(true_selector.size(), 1);
    EXPECT_EQ(false_selector.size(), 1);

    EXPECT_EQ(null_selector[0], 1);
    EXPECT_EQ(null_selector[1], 3);
    EXPECT_EQ(true_selector[0], 0);
    EXPECT_EQ(false_selector[0], 2);
}

TEST(ShortCircuitUtilTest, ConditionColumnView_with_selector) {
    // Create a boolean column: [true, false, true, false, true]
    auto col = ColumnHelper::create_column<DataTypeUInt8>({1, 0, 1, 0, 1});

    // Only select positions 1, 3 (result indices should be 10, 20)
    Selector input_selector = {10, 20};
    auto view = ConditionColumnView::create(col, &input_selector, 2);

    Selector null_selector;
    Selector true_selector;
    Selector false_selector;

    auto null_func = [&](size_t self_index, size_t executor_index) {
        null_selector.push_back(executor_index);
    };
    auto true_func = [&](size_t self_index, size_t executor_index) {
        true_selector.push_back(executor_index);
    };
    auto false_func = [&](size_t self_index, size_t executor_index) {
        false_selector.push_back(executor_index);
    };

    view.for_each(null_func, true_func, false_func);

    EXPECT_EQ(null_selector.size(), 0);
    // col[0]=true -> result_index=10, col[1]=false -> result_index=20
    EXPECT_EQ(true_selector.size(), 1);
    EXPECT_EQ(false_selector.size(), 1);
    EXPECT_EQ(true_selector[0], 10);
    EXPECT_EQ(false_selector[0], 20);
}

TEST(ShortCircuitUtilTest, ConditionColumnView_const_true) {
    // Create a const boolean column: const(true, 5)
    auto col = ColumnHelper::create_column<DataTypeUInt8>({1});
    ColumnPtr const_col = ColumnConst::create(col, 5);

    auto view = ConditionColumnView::create(const_col, nullptr, 5);

    Selector null_selector;
    Selector true_selector;
    Selector false_selector;

    auto null_func = [&](size_t self_index, size_t executor_index) {
        null_selector.push_back(executor_index);
    };
    auto true_func = [&](size_t self_index, size_t executor_index) {
        true_selector.push_back(executor_index);
    };
    auto false_func = [&](size_t self_index, size_t executor_index) {
        false_selector.push_back(executor_index);
    };

    view.for_each(null_func, true_func, false_func);

    EXPECT_EQ(null_selector.size(), 0);
    EXPECT_EQ(true_selector.size(), 5);
    EXPECT_EQ(false_selector.size(), 0);

    for (size_t i = 0; i < 5; ++i) {
        EXPECT_EQ(true_selector[i], i);
    }
}

TEST(ShortCircuitUtilTest, ConditionColumnView_const_false) {
    // Create a const boolean column: const(false, 3)
    auto col = ColumnHelper::create_column<DataTypeUInt8>({0});
    ColumnPtr const_col = ColumnConst::create(col, 3);

    auto view = ConditionColumnView::create(const_col, nullptr, 3);

    Selector null_selector;
    Selector true_selector;
    Selector false_selector;

    auto null_func = [&](size_t self_index, size_t executor_index) {
        null_selector.push_back(executor_index);
    };
    auto true_func = [&](size_t self_index, size_t executor_index) {
        true_selector.push_back(executor_index);
    };
    auto false_func = [&](size_t self_index, size_t executor_index) {
        false_selector.push_back(executor_index);
    };

    view.for_each(null_func, true_func, false_func);

    EXPECT_EQ(null_selector.size(), 0);
    EXPECT_EQ(true_selector.size(), 0);
    EXPECT_EQ(false_selector.size(), 3);
}

TEST(ShortCircuitUtilTest, ConditionColumnView_const_null) {
    // Create a const nullable boolean column: const(NULL, 3)
    auto col = ColumnHelper::create_nullable_column<DataTypeUInt8>({0}, {1});
    ColumnPtr const_col = ColumnConst::create(col, 3);

    auto view = ConditionColumnView::create(const_col, nullptr, 3);

    Selector null_selector;
    Selector true_selector;
    Selector false_selector;

    auto null_func = [&](size_t self_index, size_t executor_index) {
        null_selector.push_back(executor_index);
    };
    auto true_func = [&](size_t self_index, size_t executor_index) {
        true_selector.push_back(executor_index);
    };
    auto false_func = [&](size_t self_index, size_t executor_index) {
        false_selector.push_back(executor_index);
    };

    view.for_each(null_func, true_func, false_func);

    EXPECT_EQ(null_selector.size(), 3);
    EXPECT_EQ(true_selector.size(), 0);
    EXPECT_EQ(false_selector.size(), 0);
}

// ==================== ConditionColumnNullView Tests ====================

TEST(ShortCircuitUtilTest, ConditionColumnNullView_non_nullable_no_selector) {
    // Non-nullable column: all values are not null
    auto col = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3, 4});

    auto view = ConditionColumnNullView::create(col, nullptr, 4);

    Selector null_selector;
    Selector not_null_selector;

    auto null_func = [&](size_t self_index, size_t executor_index) {
        null_selector.push_back(executor_index);
    };
    auto not_null_func = [&](size_t self_index, size_t executor_index) {
        not_null_selector.push_back(executor_index);
    };

    view.for_each(null_func, not_null_func);

    EXPECT_EQ(null_selector.size(), 0);
    EXPECT_EQ(not_null_selector.size(), 4);

    for (size_t i = 0; i < 4; ++i) {
        EXPECT_EQ(not_null_selector[i], i);
    }
}

TEST(ShortCircuitUtilTest, ConditionColumnNullView_nullable_no_selector) {
    // Nullable column: [1, NULL, 3, NULL, 5]
    auto col =
            ColumnHelper::create_nullable_column<DataTypeInt32>({1, 0, 3, 0, 5}, {0, 1, 0, 1, 0});

    auto view = ConditionColumnNullView::create(col, nullptr, 5);

    Selector null_selector;
    Selector not_null_selector;

    auto null_func = [&](size_t self_index, size_t executor_index) {
        null_selector.push_back(executor_index);
    };
    auto not_null_func = [&](size_t self_index, size_t executor_index) {
        not_null_selector.push_back(executor_index);
    };

    view.for_each(null_func, not_null_func);

    EXPECT_EQ(null_selector.size(), 2);
    EXPECT_EQ(not_null_selector.size(), 3);

    EXPECT_EQ(null_selector[0], 1);
    EXPECT_EQ(null_selector[1], 3);
    EXPECT_EQ(not_null_selector[0], 0);
    EXPECT_EQ(not_null_selector[1], 2);
    EXPECT_EQ(not_null_selector[2], 4);
}

TEST(ShortCircuitUtilTest, ConditionColumnNullView_with_selector) {
    // Nullable column: [1, NULL, 3, NULL, 5]
    auto col =
            ColumnHelper::create_nullable_column<DataTypeInt32>({1, 0, 3, 0, 5}, {0, 1, 0, 1, 0});

    // Only process first 3 elements, with custom result indices
    Selector input_selector = {100, 200, 300};
    auto view = ConditionColumnNullView::create(col, &input_selector, 3);

    Selector null_selector;
    Selector not_null_selector;

    auto null_func = [&](size_t self_index, size_t executor_index) {
        null_selector.push_back(executor_index);
    };
    auto not_null_func = [&](size_t self_index, size_t executor_index) {
        not_null_selector.push_back(executor_index);
    };

    view.for_each(null_func, not_null_func);

    // col[0]=1 not null -> executor_index=100
    // col[1]=NULL -> executor_index=200
    // col[2]=3 not null -> executor_index=300
    EXPECT_EQ(null_selector.size(), 1);
    EXPECT_EQ(not_null_selector.size(), 2);

    EXPECT_EQ(null_selector[0], 200);
    EXPECT_EQ(not_null_selector[0], 100);
    EXPECT_EQ(not_null_selector[1], 300);
}

TEST(ShortCircuitUtilTest, ConditionColumnNullView_const_not_null) {
    // Const column: const(42, 4)
    auto col = ColumnHelper::create_column<DataTypeInt32>({42});
    ColumnPtr const_col = ColumnConst::create(col, 4);

    auto view = ConditionColumnNullView::create(const_col, nullptr, 4);

    Selector null_selector;
    Selector not_null_selector;

    auto null_func = [&](size_t self_index, size_t executor_index) {
        null_selector.push_back(executor_index);
    };
    auto not_null_func = [&](size_t self_index, size_t executor_index) {
        not_null_selector.push_back(executor_index);
    };

    view.for_each(null_func, not_null_func);

    EXPECT_EQ(null_selector.size(), 0);
    EXPECT_EQ(not_null_selector.size(), 4);
}

TEST(ShortCircuitUtilTest, ConditionColumnNullView_const_null) {
    // Const nullable column: const(NULL, 3)
    auto col = ColumnHelper::create_nullable_column<DataTypeInt32>({0}, {1});
    ColumnPtr const_col = ColumnConst::create(col, 3);

    auto view = ConditionColumnNullView::create(const_col, nullptr, 3);

    Selector null_selector;
    Selector not_null_selector;

    auto null_func = [&](size_t self_index, size_t executor_index) {
        null_selector.push_back(executor_index);
    };
    auto not_null_func = [&](size_t self_index, size_t executor_index) {
        not_null_selector.push_back(executor_index);
    };

    view.for_each(null_func, not_null_func);

    EXPECT_EQ(null_selector.size(), 3);
    EXPECT_EQ(not_null_selector.size(), 0);
}

// ==================== Additional Edge Cases ====================

TEST(ShortCircuitUtilTest, MutableColumnNullView_insert_from_const) {
    // Source: const(100, 3)
    auto src_col_inner = ColumnHelper::create_column<DataTypeInt32>({100});
    ColumnPtr src_col = ColumnConst::create(src_col_inner, 3);
    auto src_view = ColumnNullConstView::create(src_col);

    MutableColumnPtr dst_col = ColumnInt32::create();
    auto dst_view = MutableColumnNullView::create(dst_col);

    // Insert from positions 0, 1, 2 (all should be 100 since it's const)
    dst_view.insert_from(src_view, 0);
    dst_view.insert_from(src_view, 1);
    dst_view.insert_from(src_view, 2);

    EXPECT_EQ(dst_col->size(), 3);
    EXPECT_EQ(dst_col->get_int(0), 100);
    EXPECT_EQ(dst_col->get_int(1), 100);
    EXPECT_EQ(dst_col->get_int(2), 100);
}

TEST(ShortCircuitUtilTest, MutableColumnNullView_insert_from_non_nullable_to_nullable) {
    // Source: non-nullable [1, 2, 3]
    auto src_col = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3});
    auto src_view = ColumnNullConstView::create(src_col);

    // Dest: nullable column
    auto nested_col = ColumnInt32::create();
    auto null_map = ColumnUInt8::create();
    MutableColumnPtr dst_col = ColumnNullable::create(std::move(nested_col), std::move(null_map));
    auto dst_view = MutableColumnNullView::create(dst_col);

    dst_view.insert_from(src_view, 0);
    dst_view.insert_from(src_view, 1);
    dst_view.insert_from(src_view, 2);

    EXPECT_EQ(dst_col->size(), 3);
    // All values should be non-null since source is non-nullable
    EXPECT_FALSE(dst_col->is_null_at(0));
    EXPECT_FALSE(dst_col->is_null_at(1));
    EXPECT_FALSE(dst_col->is_null_at(2));
}

TEST(ShortCircuitUtilTest, NonScalarFillWithSelector_empty_columns_and_selectors) {
    std::vector<ColumnAndSelector> columns_and_selectors;

    // All empty
    ColumnAndSelector cs;
    cs.column = ColumnHelper::create_column<DataTypeInt32>({});
    cs.selector = {};
    columns_and_selectors.push_back(std::move(cs));

    auto result_type = std::make_shared<DataTypeInt32>();
    auto result = NonScalarFillWithSelector::fill(result_type, columns_and_selectors, 0);

    EXPECT_EQ(result->size(), 0);
}

TEST(ShortCircuitUtilTest, NonScalarFillWithSelector_all_null) {
    std::vector<ColumnAndSelector> columns_and_selectors;

    ColumnAndSelector cs;
    cs.column = nullptr;
    cs.selector = {0, 1, 2};
    columns_and_selectors.push_back(std::move(cs));

    auto result_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    auto result = NonScalarFillWithSelector::fill(result_type, columns_and_selectors, 3);

    EXPECT_EQ(result->size(), 3);
    EXPECT_TRUE(result->is_null_at(0));
    EXPECT_TRUE(result->is_null_at(1));
    EXPECT_TRUE(result->is_null_at(2));
}

TEST(ShortCircuitUtilTest, ColumnNullConstViewScalar_const_column) {
    auto col = ColumnHelper::create_column<DataTypeInt32>({42});
    ColumnPtr const_col = ColumnConst::create(col, 5);
    auto view = ColumnNullConstViewScalar<TYPE_INT>::create(const_col);

    EXPECT_EQ(view.data.size(), 1);
    EXPECT_EQ(view.data[0], 42);
    EXPECT_EQ(view.null_map, nullptr);
    EXPECT_TRUE(view.is_const);
}

TEST(ShortCircuitUtilTest, ColumnNullConstViewScalar_const_nullable_column) {
    auto col = ColumnHelper::create_nullable_column<DataTypeInt32>({42}, {0});
    ColumnPtr const_col = ColumnConst::create(col, 5);
    auto view = ColumnNullConstViewScalar<TYPE_INT>::create(const_col);

    EXPECT_EQ(view.data.size(), 1);
    EXPECT_EQ(view.data[0], 42);
    EXPECT_NE(view.null_map, nullptr);
    EXPECT_EQ((*view.null_map)[0], 0);
    EXPECT_TRUE(view.is_const);
}

} // namespace doris::vectorized
