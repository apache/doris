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

#include <cstddef>
#include <cstdint>

#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"

namespace doris {

class NLJAppendProbeDataWithNullTest : public testing::Test {};

// Reproduces the bug in _append_probe_data_with_null where the null map was
// only extended by 1 row instead of _probe_side_process_count rows when
// wrapping a non-nullable probe column into a nullable output column.
TEST_F(NLJAppendProbeDataWithNullTest, NullMapSizeMustMatchNestedColumnAfterInsertRange) {
    // Build a non-nullable source column with N rows (simulates probe side).
    constexpr size_t num_rows = 5;
    auto src_column = ColumnInt32::create();
    for (int32_t i = 0; i < static_cast<int32_t>(num_rows); ++i) {
        src_column->insert_value(i + 100);
    }
    ASSERT_EQ(src_column->size(), num_rows);
    ASSERT_FALSE(src_column->is_nullable());

    // Build a nullable destination column (simulates output column in mark join
    // with RIGHT_OUTER / FULL_OUTER where probe columns become nullable).
    auto nested_col = ColumnInt32::create();
    auto null_map = ColumnUInt8::create();
    auto dst_column = ColumnNullable::create(std::move(nested_col), std::move(null_map));
    ASSERT_TRUE(dst_column->is_nullable());

    auto origin_sz = dst_column->size();
    ASSERT_EQ(origin_sz, 0);

    // This is the exact pattern from _append_probe_data_with_null:
    // insert N rows into the nested column, then resize the null map.
    const size_t probe_side_process_count = num_rows;
    const size_t probe_block_start_pos = 0;

    assert_cast<ColumnNullable*>(dst_column.get())
            ->get_nested_column_ptr()
            ->insert_range_from(*src_column, probe_block_start_pos, probe_side_process_count);

    // The fix: resize_fill must use probe_side_process_count, not 1.
    assert_cast<ColumnNullable*>(dst_column.get())
            ->get_null_map_column()
            .get_data()
            .resize_fill(origin_sz + probe_side_process_count, 0);

    // Verify: nested column and null map must have the same size.
    auto* nullable = assert_cast<ColumnNullable*>(dst_column.get());
    ASSERT_EQ(nullable->get_nested_column().size(), num_rows);
    ASSERT_EQ(nullable->get_null_map_column().size(), num_rows);
    ASSERT_EQ(nullable->size(), num_rows);

    // Verify all null flags are 0 (not null).
    const auto& null_data = nullable->get_null_map_data();
    for (size_t i = 0; i < num_rows; ++i) {
        EXPECT_EQ(null_data[i], 0) << "row " << i << " should not be null";
    }

    // Verify data values are correct.
    const auto& nested = assert_cast<const ColumnInt32&>(nullable->get_nested_column());
    for (size_t i = 0; i < num_rows; ++i) {
        EXPECT_EQ(nested.get_element(i), static_cast<int32_t>(i + 100));
    }
}

// Verify the bug scenario: if null map is only extended by 1 when N > 1,
// the ColumnNullable invariant (nested.size() == null_map.size()) is violated.
TEST_F(NLJAppendProbeDataWithNullTest, BugReproNullMapSizeMismatchWhenExtendedByOne) {
    constexpr size_t num_rows = 5;
    auto src_column = ColumnInt32::create();
    for (int32_t i = 0; i < static_cast<int32_t>(num_rows); ++i) {
        src_column->insert_value(i);
    }

    auto nested_col = ColumnInt32::create();
    auto null_map_col = ColumnUInt8::create();
    auto dst_column = ColumnNullable::create(std::move(nested_col), std::move(null_map_col));

    auto origin_sz = dst_column->size();

    // Insert N rows into nested column.
    assert_cast<ColumnNullable*>(dst_column.get())
            ->get_nested_column_ptr()
            ->insert_range_from(*src_column, 0, num_rows);

    // Simulate the OLD buggy code: resize_fill(origin_sz + 1, 0)
    assert_cast<ColumnNullable*>(dst_column.get())
            ->get_null_map_column()
            .get_data()
            .resize_fill(origin_sz + 1, 0);

    auto* nullable = assert_cast<ColumnNullable*>(dst_column.get());
    // The nested column has 5 rows but null map only has 1 — invariant broken.
    EXPECT_EQ(nullable->get_nested_column().size(), num_rows);
    EXPECT_EQ(nullable->get_null_map_column().size(), 1);
    EXPECT_NE(nullable->get_nested_column().size(), nullable->get_null_map_column().size());
}

// When probe_side_process_count == 1, both old and new code produce the same result.
TEST_F(NLJAppendProbeDataWithNullTest, SingleRowInsertIsCorrect) {
    auto src_column = ColumnInt32::create();
    src_column->insert_value(42);

    auto nested_col = ColumnInt32::create();
    auto null_map_col = ColumnUInt8::create();
    auto dst_column = ColumnNullable::create(std::move(nested_col), std::move(null_map_col));

    auto origin_sz = dst_column->size();
    constexpr size_t count = 1;

    assert_cast<ColumnNullable*>(dst_column.get())
            ->get_nested_column_ptr()
            ->insert_range_from(*src_column, 0, count);
    assert_cast<ColumnNullable*>(dst_column.get())
            ->get_null_map_column()
            .get_data()
            .resize_fill(origin_sz + count, 0);

    auto* nullable = assert_cast<ColumnNullable*>(dst_column.get());
    ASSERT_EQ(nullable->get_nested_column().size(), 1);
    ASSERT_EQ(nullable->get_null_map_column().size(), 1);
    ASSERT_EQ(nullable->size(), 1);
    EXPECT_EQ(assert_cast<const ColumnInt32&>(nullable->get_nested_column()).get_element(0), 42);
}

// Append to a pre-existing nullable column (non-empty origin).
TEST_F(NLJAppendProbeDataWithNullTest, AppendToNonEmptyColumn) {
    auto src_column = ColumnInt32::create();
    for (int32_t i = 0; i < 3; ++i) {
        src_column->insert_value(i + 10);
    }

    auto nested_col = ColumnInt32::create();
    auto null_map_col = ColumnUInt8::create();
    // Pre-populate with 2 existing rows.
    nested_col->insert_value(1);
    nested_col->insert_value(2);
    null_map_col->insert_value(0);
    null_map_col->insert_value(0);
    auto dst_column = ColumnNullable::create(std::move(nested_col), std::move(null_map_col));

    auto origin_sz = dst_column->size();
    ASSERT_EQ(origin_sz, 2);

    constexpr size_t count = 3;
    assert_cast<ColumnNullable*>(dst_column.get())
            ->get_nested_column_ptr()
            ->insert_range_from(*src_column, 0, count);
    assert_cast<ColumnNullable*>(dst_column.get())
            ->get_null_map_column()
            .get_data()
            .resize_fill(origin_sz + count, 0);

    auto* nullable = assert_cast<ColumnNullable*>(dst_column.get());
    ASSERT_EQ(nullable->get_nested_column().size(), 5);
    ASSERT_EQ(nullable->get_null_map_column().size(), 5);
    ASSERT_EQ(nullable->size(), 5);
}

} // namespace doris
