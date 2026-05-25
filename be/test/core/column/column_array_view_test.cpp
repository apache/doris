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

#include "core/column/column_array_view.h"

#include <gtest/gtest.h>

#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "testutil/column_helper.h"

namespace doris {

// Helper: build a ColumnArray with Nullable(ColumnInt32) nested data.
// arrays: each inner vector is one row's array elements
// element_nulls: parallel to the flattened data, 1 = null
// row_nulls: per-row outer null (empty means no outer nullable wrapper)
static ColumnPtr build_int32_array_column(const std::vector<std::vector<int32_t>>& arrays,
                                          const std::vector<uint8_t>& element_nulls,
                                          const std::vector<uint8_t>& row_nulls = {}) {
    // Build nested data column (Nullable(Int32))
    auto data_col = ColumnInt32::create();
    auto null_col = ColumnUInt8::create();
    size_t flat_idx = 0;
    for (const auto& arr : arrays) {
        for (auto val : arr) {
            data_col->insert_value(val);
            null_col->insert_value(flat_idx < element_nulls.size() ? element_nulls[flat_idx] : 0);
            flat_idx++;
        }
    }
    auto nullable_data = ColumnNullable::create(std::move(data_col), std::move(null_col));

    // Build offsets
    auto offsets = ColumnArray::ColumnOffsets::create();
    size_t offset = 0;
    for (const auto& arr : arrays) {
        offset += arr.size();
        offsets->insert_value(offset);
    }

    ColumnPtr array_col = ColumnArray::create(std::move(nullable_data), std::move(offsets));

    // Wrap in outer Nullable if row_nulls provided
    if (!row_nulls.empty()) {
        auto outer_null = ColumnUInt8::create();
        for (auto v : row_nulls) {
            outer_null->insert_value(v);
        }
        array_col = ColumnNullable::create(array_col->assume_mutable(), std::move(outer_null));
    }
    return array_col;
}

// Helper: build a ColumnArray with Nullable(ColumnString) nested data.
static ColumnPtr build_string_array_column(const std::vector<std::vector<std::string>>& arrays,
                                           const std::vector<uint8_t>& element_nulls,
                                           const std::vector<uint8_t>& row_nulls = {}) {
    auto data_col = ColumnString::create();
    auto null_col = ColumnUInt8::create();
    size_t flat_idx = 0;
    for (const auto& arr : arrays) {
        for (const auto& val : arr) {
            data_col->insert_data(val.data(), val.size());
            null_col->insert_value(flat_idx < element_nulls.size() ? element_nulls[flat_idx] : 0);
            flat_idx++;
        }
    }
    auto nullable_data = ColumnNullable::create(std::move(data_col), std::move(null_col));

    auto offsets = ColumnArray::ColumnOffsets::create();
    size_t offset = 0;
    for (const auto& arr : arrays) {
        offset += arr.size();
        offsets->insert_value(offset);
    }

    ColumnPtr array_col = ColumnArray::create(std::move(nullable_data), std::move(offsets));

    if (!row_nulls.empty()) {
        auto outer_null = ColumnUInt8::create();
        for (auto v : row_nulls) {
            outer_null->insert_value(v);
        }
        array_col = ColumnNullable::create(array_col->assume_mutable(), std::move(outer_null));
    }
    return array_col;
}

// ==================== ArrayDataView (index-based) Tests ====================

// Test basic non-nullable, non-const array column
// Row 0: [10, 20, 30], Row 1: [40], Row 2: [50, 60]
TEST(ColumnArrayViewTest, IndexAccess_basic) {
    auto col = build_int32_array_column({{10, 20, 30}, {40}, {50, 60}}, {0, 0, 0, 0, 0, 0});
    auto view = ColumnArrayView<TYPE_INT>::create(col);

    EXPECT_EQ(view.size(), 3);
    EXPECT_FALSE(view.is_const);

    // Row 0
    EXPECT_FALSE(view.is_null_at(0));
    auto arr0 = view[0];
    EXPECT_EQ(arr0.size(), 3);
    EXPECT_EQ(arr0.value_at(0), 10);
    EXPECT_EQ(arr0.value_at(1), 20);
    EXPECT_EQ(arr0.value_at(2), 30);
    EXPECT_FALSE(arr0.is_null_at(0));
    EXPECT_FALSE(arr0.is_null_at(1));
    EXPECT_FALSE(arr0.is_null_at(2));

    // Row 1
    auto arr1 = view[1];
    EXPECT_EQ(arr1.size(), 1);
    EXPECT_EQ(arr1.value_at(0), 40);

    // Row 2
    auto arr2 = view[2];
    EXPECT_EQ(arr2.size(), 2);
    EXPECT_EQ(arr2.value_at(0), 50);
    EXPECT_EQ(arr2.value_at(1), 60);
}

TEST(ColumnArrayViewTest, IndexAccess_get_data) {
    auto col = build_int32_array_column({{10, 20, 30}, {40}, {50, 60}}, {0, 0, 0, 0, 0, 0});
    auto view = ColumnArrayView<TYPE_INT>::create(col);

    auto arr0 = view[0];
    const auto* data0 = arr0.get_data();
    ASSERT_NE(data0, nullptr);
    EXPECT_EQ(data0[0], 10);
    EXPECT_EQ(data0[1], 20);
    EXPECT_EQ(data0[2], 30);

    auto arr1 = view[1];
    const auto* data1 = arr1.get_data();
    ASSERT_NE(data1, nullptr);
    EXPECT_EQ(data1[0], 40);

    auto arr2 = view[2];
    const auto* data2 = arr2.get_data();
    ASSERT_NE(data2, nullptr);
    EXPECT_EQ(data2[0], 50);
    EXPECT_EQ(data2[1], 60);
}

// Test with null elements inside arrays
// Row 0: [1, NULL, 3], Row 1: [NULL]
TEST(ColumnArrayViewTest, IndexAccess_with_null_elements) {
    auto col = build_int32_array_column({{1, 0, 3}, {0}}, {0, 1, 0, 1});
    auto view = ColumnArrayView<TYPE_INT>::create(col);

    EXPECT_EQ(view.size(), 2);

    auto arr0 = view[0];
    EXPECT_EQ(arr0.size(), 3);
    EXPECT_FALSE(arr0.is_null_at(0));
    EXPECT_TRUE(arr0.is_null_at(1));
    EXPECT_FALSE(arr0.is_null_at(2));
    EXPECT_EQ(arr0.value_at(0), 1);
    EXPECT_EQ(arr0.value_at(2), 3);

    auto arr1 = view[1];
    EXPECT_EQ(arr1.size(), 1);
    EXPECT_TRUE(arr1.is_null_at(0));
}

// Test with outer nullable (some rows are entirely null)
// Row 0: [1, 2], Row 1: NULL, Row 2: [5]
TEST(ColumnArrayViewTest, IndexAccess_outer_nullable) {
    auto col = build_int32_array_column({{1, 2}, {0}, {5}}, {0, 0, 0, 0}, {0, 1, 0});
    auto view = ColumnArrayView<TYPE_INT>::create(col);

    EXPECT_EQ(view.size(), 3);
    EXPECT_FALSE(view.is_null_at(0));
    EXPECT_TRUE(view.is_null_at(1));
    EXPECT_FALSE(view.is_null_at(2));

    auto arr0 = view[0];
    EXPECT_EQ(arr0.size(), 2);
    EXPECT_EQ(arr0.value_at(0), 1);
    EXPECT_EQ(arr0.value_at(1), 2);

    auto arr2 = view[2];
    EXPECT_EQ(arr2.size(), 1);
    EXPECT_EQ(arr2.value_at(0), 5);
}

// Test const column: Const(Array([10, 20])) with 4 rows
TEST(ColumnArrayViewTest, IndexAccess_const) {
    auto inner = build_int32_array_column({{10, 20}}, {0, 0});
    ColumnPtr const_col = ColumnConst::create(inner, 4);
    auto view = ColumnArrayView<TYPE_INT>::create(const_col);

    EXPECT_EQ(view.size(), 4);
    EXPECT_TRUE(view.is_const);

    for (size_t i = 0; i < 4; ++i) {
        EXPECT_FALSE(view.is_null_at(i));
        auto arr = view[i];
        EXPECT_EQ(arr.size(), 2);
        EXPECT_EQ(arr.value_at(0), 10);
        EXPECT_EQ(arr.value_at(1), 20);
    }
}

// Test Const(Nullable(Array([7, 8, 9]))) with 3 rows, non-null
TEST(ColumnArrayViewTest, IndexAccess_const_nullable) {
    auto inner = build_int32_array_column({{7, 8, 9}}, {0, 0, 0}, {0});
    ColumnPtr const_col = ColumnConst::create(inner, 3);
    auto view = ColumnArrayView<TYPE_INT>::create(const_col);

    EXPECT_EQ(view.size(), 3);
    EXPECT_TRUE(view.is_const);

    for (size_t i = 0; i < 3; ++i) {
        EXPECT_FALSE(view.is_null_at(i));
        auto arr = view[i];
        EXPECT_EQ(arr.size(), 3);
        EXPECT_EQ(arr.value_at(0), 7);
        EXPECT_EQ(arr.value_at(1), 8);
        EXPECT_EQ(arr.value_at(2), 9);
    }
}

// Test Const(Nullable(NULL)) with 3 rows, all null
TEST(ColumnArrayViewTest, IndexAccess_const_nullable_null) {
    // Build one-row array, then wrap as nullable with null=1, then const
    auto inner = build_int32_array_column({{0}}, {0}, {1});
    ColumnPtr const_col = ColumnConst::create(inner, 3);
    auto view = ColumnArrayView<TYPE_INT>::create(const_col);

    EXPECT_EQ(view.size(), 3);
    EXPECT_TRUE(view.is_const);

    for (size_t i = 0; i < 3; ++i) {
        EXPECT_TRUE(view.is_null_at(i));
    }
}

// Test empty array rows
// Row 0: [], Row 1: [100], Row 2: []
TEST(ColumnArrayViewTest, IndexAccess_empty_arrays) {
    auto col = build_int32_array_column({{}, {100}, {}}, {0});
    auto view = ColumnArrayView<TYPE_INT>::create(col);

    EXPECT_EQ(view.size(), 3);
    EXPECT_EQ(view[0].size(), 0);
    EXPECT_EQ(view[1].size(), 1);
    EXPECT_EQ(view[1].value_at(0), 100);
    EXPECT_EQ(view[2].size(), 0);
}

// Test string array
// Row 0: ["hello", "world"], Row 1: ["test"]
TEST(ColumnArrayViewTest, IndexAccess_string) {
    auto col = build_string_array_column({{"hello", "world"}, {"test"}}, {0, 0, 0});
    auto view = ColumnArrayView<TYPE_STRING>::create(col);

    EXPECT_EQ(view.size(), 2);
    auto arr0 = view[0];
    EXPECT_EQ(arr0.size(), 2);
    EXPECT_EQ(arr0.value_at(0).to_string(), "hello");
    EXPECT_EQ(arr0.value_at(1).to_string(), "world");

    auto arr1 = view[1];
    EXPECT_EQ(arr1.size(), 1);
    EXPECT_EQ(arr1.value_at(0).to_string(), "test");
}

} // namespace doris