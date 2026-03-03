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

#include "vec/olap/vertical_merge_iterator.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <random>

#include "common/config.h"
#include "util/faststring.h"
#include "util/rle_encoding.h"
#include "util/simd/bits.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"

namespace doris::vectorized {

class SparseColumnOptimizationTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Enable sparse column optimization for tests (use max threshold to always enable)
        original_threshold_percent_ = config::sparse_column_compaction_threshold_percent;
        config::sparse_column_compaction_threshold_percent = 1.0;
    }

    void TearDown() override {
        // Reset to original value
        config::sparse_column_compaction_threshold_percent = original_threshold_percent_;
    }

    // Helper function to create a nullable column with specific NULL pattern
    static ColumnNullable::MutablePtr create_nullable_column(const std::vector<Int64>& values,
                                                             const std::vector<bool>& null_flags) {
        EXPECT_EQ(values.size(), null_flags.size());

        auto nested_column = ColumnInt64::create();
        auto null_map = ColumnUInt8::create();

        for (size_t i = 0; i < values.size(); ++i) {
            nested_column->insert_value(values[i]);
            null_map->insert_value(null_flags[i] ? 1 : 0);
        }

        return ColumnNullable::create(std::move(nested_column), std::move(null_map));
    }

    // Helper to count non-NULL values using SIMD
    static size_t count_non_null(const ColumnNullable* col, size_t start, size_t count) {
        const auto& null_map = col->get_null_map_data();
        return simd::count_zero_num(reinterpret_cast<const int8_t*>(null_map.data() + start),
                                    count);
    }

    // Helper to check if all values in range are NULL
    static bool is_all_null(const ColumnNullable* col, size_t start, size_t count) {
        return count_non_null(col, start, count) == 0;
    }

    // Helper to check if all values in range are non-NULL
    static bool is_all_non_null(const ColumnNullable* col, size_t start, size_t count) {
        return count_non_null(col, start, count) == count;
    }

    // Simulate copy_rows logic for nullable columns with sparse optimization
    static void copy_rows_with_optimization(const ColumnNullable* src, size_t start, size_t count,
                                            IColumn* dst_col) {
        auto* dst_mut = dst_col->assume_mutable().get();

        const size_t non_null_count = count_non_null(src, start, count);

        if (non_null_count == 0) {
            // Case 1: All NULL - batch fill with defaults
            dst_mut->insert_many_defaults(count);
        } else if (non_null_count == count || non_null_count > count / 2) {
            // Case 2: All non-NULL or non-NULL ratio > 50% - direct copy
            dst_mut->insert_range_from(*src, start, count);
        } else {
            // Case 3: Sparse mixed (non-NULL < 50%) - fill NULL first, then replace
            const size_t dst_start = dst_mut->size();
            dst_mut->insert_many_defaults(count);

            auto* nullable_dst = assert_cast<ColumnNullable*>(dst_mut);
            const auto& null_map = src->get_null_map_data();

            for (size_t row = 0; row < count; row++) {
                if (null_map[start + row] == 0) { // 0 means non-NULL
                    nullable_dst->replace_column_data(*src, start + row, dst_start + row);
                }
            }
        }
    }

    // Original copy_rows logic (direct copy)
    static void copy_rows_original(const IColumn* src, size_t start, size_t count,
                                   IColumn* dst_col) {
        dst_col->assume_mutable()->insert_range_from(*src, start, count);
    }

    // Helper to compare two nullable columns
    static bool columns_equal(const ColumnNullable* col1, const ColumnNullable* col2) {
        if (col1->size() != col2->size()) {
            return false;
        }

        const auto& null_map1 = col1->get_null_map_data();
        const auto& null_map2 = col2->get_null_map_data();
        const auto& nested1 = col1->get_nested_column();
        const auto& nested2 = col2->get_nested_column();

        for (size_t i = 0; i < col1->size(); ++i) {
            if (null_map1[i] != null_map2[i]) {
                return false;
            }
            // Only compare nested data for non-NULL rows
            if (null_map1[i] == 0) {
                if (nested1.compare_at(i, i, nested2, 1) != 0) {
                    return false;
                }
            }
        }
        return true;
    }

    double original_threshold_percent_ = 0.0;
};

TEST_F(SparseColumnOptimizationTest, AllNullColumn) {
    // Test Case 1: All NULL column
    std::vector<Int64> values = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    std::vector<bool> null_flags = {true, true, true, true, true, true, true, true, true, true};

    auto src = create_nullable_column(values, null_flags);
    const auto* nullable_src = assert_cast<const ColumnNullable*>(src.get());

    // Create destination columns
    auto dst_optimized = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());
    auto dst_original = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());

    // Copy with optimization
    copy_rows_with_optimization(nullable_src, 0, 10, dst_optimized.get());
    // Copy with original method
    copy_rows_original(src.get(), 0, 10, dst_original.get());

    // Verify results are equal
    EXPECT_TRUE(columns_equal(assert_cast<const ColumnNullable*>(dst_optimized.get()),
                              assert_cast<const ColumnNullable*>(dst_original.get())));

    // Verify all are NULL
    const auto* result = assert_cast<const ColumnNullable*>(dst_optimized.get());
    EXPECT_EQ(result->size(), 10);
    EXPECT_TRUE(is_all_null(result, 0, 10));
}

TEST_F(SparseColumnOptimizationTest, AllNonNullColumn) {
    // Test Case 2: All non-NULL column
    std::vector<Int64> values = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    std::vector<bool> null_flags = {false, false, false, false, false,
                                    false, false, false, false, false};

    auto src = create_nullable_column(values, null_flags);
    const auto* nullable_src = assert_cast<const ColumnNullable*>(src.get());

    auto dst_optimized = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());
    auto dst_original = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());

    copy_rows_with_optimization(nullable_src, 0, 10, dst_optimized.get());
    copy_rows_original(src.get(), 0, 10, dst_original.get());

    EXPECT_TRUE(columns_equal(assert_cast<const ColumnNullable*>(dst_optimized.get()),
                              assert_cast<const ColumnNullable*>(dst_original.get())));

    // Verify all are non-NULL
    const auto* result = assert_cast<const ColumnNullable*>(dst_optimized.get());
    EXPECT_EQ(result->size(), 10);
    EXPECT_TRUE(is_all_non_null(result, 0, 10));

    // Verify values
    const auto& nested = assert_cast<const ColumnInt64&>(result->get_nested_column());
    for (size_t i = 0; i < 10; ++i) {
        EXPECT_EQ(nested.get_element(i), static_cast<Int64>(i + 1));
    }
}

TEST_F(SparseColumnOptimizationTest, SparseMixedColumn) {
    // Test Case 3: Sparse mixed column (< 50% non-NULL)
    // 20% non-NULL rate
    std::vector<Int64> values = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    std::vector<bool> null_flags = {false, true, true, true, true, false, true, true, true, true};

    auto src = create_nullable_column(values, null_flags);
    const auto* nullable_src = assert_cast<const ColumnNullable*>(src.get());

    auto dst_optimized = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());
    auto dst_original = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());

    copy_rows_with_optimization(nullable_src, 0, 10, dst_optimized.get());
    copy_rows_original(src.get(), 0, 10, dst_original.get());

    EXPECT_TRUE(columns_equal(assert_cast<const ColumnNullable*>(dst_optimized.get()),
                              assert_cast<const ColumnNullable*>(dst_original.get())));

    // Verify count
    const auto* result = assert_cast<const ColumnNullable*>(dst_optimized.get());
    EXPECT_EQ(result->size(), 10);
    EXPECT_EQ(count_non_null(result, 0, 10), 2);

    // Verify specific values
    EXPECT_FALSE(result->is_null_at(0));
    EXPECT_TRUE(result->is_null_at(1));
    EXPECT_FALSE(result->is_null_at(5));

    const auto& nested = assert_cast<const ColumnInt64&>(result->get_nested_column());
    EXPECT_EQ(nested.get_element(0), 1);
    EXPECT_EQ(nested.get_element(5), 6);
}

TEST_F(SparseColumnOptimizationTest, DenseMixedColumn) {
    // Test Case: Dense mixed column (> 50% non-NULL, should use direct copy)
    // 80% non-NULL rate
    std::vector<Int64> values = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    std::vector<bool> null_flags = {false, false, false, false, true,
                                    false, false, false, false, true};

    auto src = create_nullable_column(values, null_flags);
    const auto* nullable_src = assert_cast<const ColumnNullable*>(src.get());

    auto dst_optimized = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());
    auto dst_original = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());

    copy_rows_with_optimization(nullable_src, 0, 10, dst_optimized.get());
    copy_rows_original(src.get(), 0, 10, dst_original.get());

    EXPECT_TRUE(columns_equal(assert_cast<const ColumnNullable*>(dst_optimized.get()),
                              assert_cast<const ColumnNullable*>(dst_original.get())));

    const auto* result = assert_cast<const ColumnNullable*>(dst_optimized.get());
    EXPECT_EQ(count_non_null(result, 0, 10), 8);
}

TEST_F(SparseColumnOptimizationTest, PartialRangeCopy) {
    // Test partial range copy
    std::vector<Int64> values = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    std::vector<bool> null_flags = {false, true, true, true, true, true, true, true, true, false};

    auto src = create_nullable_column(values, null_flags);
    const auto* nullable_src = assert_cast<const ColumnNullable*>(src.get());

    // Copy middle range (indices 2-7, all NULL)
    auto dst_optimized = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());
    auto dst_original = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());

    copy_rows_with_optimization(nullable_src, 2, 6, dst_optimized.get());
    copy_rows_original(src.get(), 2, 6, dst_original.get());

    EXPECT_TRUE(columns_equal(assert_cast<const ColumnNullable*>(dst_optimized.get()),
                              assert_cast<const ColumnNullable*>(dst_original.get())));

    const auto* result = assert_cast<const ColumnNullable*>(dst_optimized.get());
    EXPECT_EQ(result->size(), 6);
    EXPECT_TRUE(is_all_null(result, 0, 6));
}

TEST_F(SparseColumnOptimizationTest, LargeSparseCopy) {
    // Test with large sparse column (5% non-NULL rate, typical for sparse wide tables)
    constexpr size_t num_rows = 1024;
    std::vector<Int64> values(num_rows);
    std::vector<bool> null_flags(num_rows);

    for (size_t i = 0; i < num_rows; ++i) {
        values[i] = static_cast<Int64>(i);
        // Every 20th row is non-NULL (5% non-NULL rate)
        null_flags[i] = (i % 20 != 0);
    }

    auto src = create_nullable_column(values, null_flags);
    const auto* nullable_src = assert_cast<const ColumnNullable*>(src.get());

    auto dst_optimized = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());
    auto dst_original = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());

    copy_rows_with_optimization(nullable_src, 0, num_rows, dst_optimized.get());
    copy_rows_original(src.get(), 0, num_rows, dst_original.get());

    EXPECT_TRUE(columns_equal(assert_cast<const ColumnNullable*>(dst_optimized.get()),
                              assert_cast<const ColumnNullable*>(dst_original.get())));

    const auto* result = assert_cast<const ColumnNullable*>(dst_optimized.get());
    EXPECT_EQ(result->size(), num_rows);
    // 1024 / 20 = 51.2 -> 52 non-NULL rows (0, 20, 40, ..., 1000, 1020)
    EXPECT_EQ(count_non_null(result, 0, num_rows), 52);
}

TEST_F(SparseColumnOptimizationTest, MultipleCopies) {
    // Test multiple sequential copies to the same destination
    std::vector<Int64> values = {1, 2, 3, 4, 5};
    std::vector<bool> null_flags = {false, true, true, true, false};

    auto src = create_nullable_column(values, null_flags);
    const auto* nullable_src = assert_cast<const ColumnNullable*>(src.get());

    auto dst_optimized = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());
    auto dst_original = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());

    // Copy the same source multiple times
    for (int i = 0; i < 3; ++i) {
        copy_rows_with_optimization(nullable_src, 0, 5, dst_optimized.get());
        copy_rows_original(src.get(), 0, 5, dst_original.get());
    }

    EXPECT_TRUE(columns_equal(assert_cast<const ColumnNullable*>(dst_optimized.get()),
                              assert_cast<const ColumnNullable*>(dst_original.get())));

    const auto* result = assert_cast<const ColumnNullable*>(dst_optimized.get());
    EXPECT_EQ(result->size(), 15);
    EXPECT_EQ(count_non_null(result, 0, 15), 6); // 2 non-NULL per copy * 3 copies
}

TEST_F(SparseColumnOptimizationTest, DisabledOptimization) {
    // Test with optimization disabled (threshold = 0 means disabled)
    config::sparse_column_compaction_threshold_percent = 0.0;

    std::vector<Int64> values = {1, 2, 3, 4, 5};
    std::vector<bool> null_flags = {true, true, true, true, true};

    auto src = create_nullable_column(values, null_flags);

    auto dst = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());

    // When disabled, should still work correctly via direct copy path
    copy_rows_original(src.get(), 0, 5, dst.get());

    const auto* result = assert_cast<const ColumnNullable*>(dst.get());
    EXPECT_EQ(result->size(), 5);
    EXPECT_TRUE(is_all_null(result, 0, 5));
}

TEST_F(SparseColumnOptimizationTest, ReplaceColumnDataRange) {
    // Test replace_column_data_range functionality
    std::vector<Int64> src_values = {1, 2, 3, 4, 5};
    std::vector<bool> src_null_flags = {false, true, false, true, false};

    auto src = create_nullable_column(src_values, src_null_flags);
    const auto* nullable_src = assert_cast<const ColumnNullable*>(src.get());

    // Create destination with pre-filled NULLs
    auto dst = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());
    auto* nullable_dst = assert_cast<ColumnNullable*>(dst.get());

    // Pre-fill with NULLs
    nullable_dst->get_null_map_column().get_data().resize_fill(5, 1);
    nullable_dst->get_nested_column().resize(5);

    // Replace with source data
    nullable_dst->replace_column_data_range(*nullable_src, 0, 5, 0);

    // Verify results
    EXPECT_EQ(nullable_dst->size(), 5);
    const auto& null_map = nullable_dst->get_null_map_data();
    EXPECT_EQ(null_map[0], 0); // non-NULL
    EXPECT_EQ(null_map[1], 1); // NULL
    EXPECT_EQ(null_map[2], 0); // non-NULL
    EXPECT_EQ(null_map[3], 1); // NULL
    EXPECT_EQ(null_map[4], 0); // non-NULL

    // Verify values for non-NULL positions
    const auto& nested = assert_cast<const ColumnInt64&>(nullable_dst->get_nested_column());
    EXPECT_EQ(nested.get_element(0), 1);
    EXPECT_EQ(nested.get_element(2), 3);
    EXPECT_EQ(nested.get_element(4), 5);
}

TEST_F(SparseColumnOptimizationTest, CountZeroNumSIMD) {
    // Test SIMD count_zero_num function
    std::vector<int8_t> data(128);

    // All zeros
    std::fill(data.begin(), data.end(), 0);
    EXPECT_EQ(simd::count_zero_num(data.data(), static_cast<size_t>(data.size())), 128);

    // All ones
    std::fill(data.begin(), data.end(), 1);
    EXPECT_EQ(simd::count_zero_num(data.data(), static_cast<size_t>(data.size())), 0);

    // Mixed: every 4th is zero
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = (i % 4 == 0) ? 0 : 1;
    }
    EXPECT_EQ(simd::count_zero_num(data.data(), static_cast<size_t>(data.size())), 32);

    // Small sizes
    EXPECT_EQ(simd::count_zero_num(data.data(), static_cast<size_t>(1)), 1);     // single zero
    EXPECT_EQ(simd::count_zero_num(data.data() + 1, static_cast<size_t>(1)), 0); // single one
}

// ==================== ColumnVector replace_column_data_range tests ====================

TEST_F(SparseColumnOptimizationTest, ColumnVectorReplaceDataRange) {
    // Test ColumnVector::replace_column_data_range with memcpy optimization
    auto src = ColumnInt64::create();
    for (Int64 i = 1; i <= 10; ++i) {
        src->insert_value(i * 100);
    }

    // Create destination column with pre-allocated space
    auto dst = ColumnInt64::create();
    for (int i = 0; i < 10; ++i) {
        dst->insert_value(0); // Pre-fill with zeros
    }

    // Replace range [2, 6) from src to dst at position 3
    dst->replace_column_data_range(*src, 2, 4, 3);

    // Verify: dst[3..6] should be src[2..5] = {300, 400, 500, 600}
    const auto& dst_data = dst->get_data();
    EXPECT_EQ(dst_data[0], 0);   // unchanged
    EXPECT_EQ(dst_data[1], 0);   // unchanged
    EXPECT_EQ(dst_data[2], 0);   // unchanged
    EXPECT_EQ(dst_data[3], 300); // src[2]
    EXPECT_EQ(dst_data[4], 400); // src[3]
    EXPECT_EQ(dst_data[5], 500); // src[4]
    EXPECT_EQ(dst_data[6], 600); // src[5]
    EXPECT_EQ(dst_data[7], 0);   // unchanged
}

TEST_F(SparseColumnOptimizationTest, ColumnVectorReplaceDataRangeFullCopy) {
    // Test full range copy
    constexpr size_t num_rows = 1024;
    auto src = ColumnInt64::create();
    for (size_t i = 0; i < num_rows; ++i) {
        src->insert_value(static_cast<Int64>(i));
    }

    auto dst = ColumnInt64::create();
    for (size_t i = 0; i < num_rows; ++i) {
        dst->insert_value(-1);
    }

    // Replace entire range
    dst->replace_column_data_range(*src, 0, num_rows, 0);

    // Verify all values copied correctly
    const auto& dst_data = dst->get_data();
    for (size_t i = 0; i < num_rows; ++i) {
        EXPECT_EQ(dst_data[i], static_cast<Int64>(i));
    }
}

TEST_F(SparseColumnOptimizationTest, ColumnVectorReplaceDataRangeSingleElement) {
    // Test single element replacement
    auto src = ColumnInt64::create();
    src->insert_value(42);
    src->insert_value(99);

    auto dst = ColumnInt64::create();
    for (int i = 0; i < 5; ++i) {
        dst->insert_value(0);
    }

    dst->replace_column_data_range(*src, 1, 1, 2);

    EXPECT_EQ(dst->get_data()[2], 99);
    EXPECT_EQ(dst->get_data()[0], 0); // unchanged
    EXPECT_EQ(dst->get_data()[4], 0); // unchanged
}

// ==================== ColumnDecimal replace_column_data_range tests ====================

TEST_F(SparseColumnOptimizationTest, ColumnDecimalReplaceDataRange) {
    // Test ColumnDecimal::replace_column_data_range with memcpy optimization
    // Use ColumnDecimal128V3 which is ColumnDecimal<TYPE_DECIMAL128I>
    auto src = ColumnDecimal128V3::create(0, 2); // scale = 2
    for (int i = 1; i <= 10; ++i) {
        src->insert_value(Decimal128V3(i * 100));
    }

    auto dst = ColumnDecimal128V3::create(0, 2);
    for (int i = 0; i < 10; ++i) {
        dst->insert_value(Decimal128V3(0));
    }

    // Replace range
    dst->replace_column_data_range(*src, 2, 4, 3);

    const auto& dst_data = dst->get_data();
    EXPECT_EQ(dst_data[3], Decimal128V3(300));
    EXPECT_EQ(dst_data[4], Decimal128V3(400));
    EXPECT_EQ(dst_data[5], Decimal128V3(500));
    EXPECT_EQ(dst_data[6], Decimal128V3(600));
    EXPECT_EQ(dst_data[0], Decimal128V3(0)); // unchanged
}

// ==================== RowBatch structure tests ====================

TEST_F(SparseColumnOptimizationTest, RowBatchConstruction) {
    // Test RowBatch construction and member access
    auto block = std::make_shared<Block>();

    // Add a column to the block
    auto col = ColumnInt64::create();
    col->insert_value(1);
    col->insert_value(2);
    col->insert_value(3);
    block->insert({std::move(col), std::make_shared<DataTypeInt64>(), "test_col"});

    // Create RowBatch
    RowBatch batch(block, 1, 2); // start_row=1, count=2

    EXPECT_EQ(batch.block.get(), block.get());
    EXPECT_EQ(batch.start_row, 1);
    EXPECT_EQ(batch.count, 2);

    // Verify block content accessible through batch
    const auto& batch_col = batch.block->get_by_position(0).column;
    EXPECT_EQ(batch_col->size(), 3);
}

TEST_F(SparseColumnOptimizationTest, RowBatchSharedPtrLifetime) {
    // Test that RowBatch keeps block alive via shared_ptr
    RowBatch batch(nullptr, 0, 0);

    {
        auto block = std::make_shared<Block>();
        auto col = ColumnInt64::create();
        col->insert_value(42);
        block->insert({std::move(col), std::make_shared<DataTypeInt64>(), "col"});

        batch = RowBatch(block, 0, 1);
        // block goes out of scope here, but batch keeps it alive
    }

    // Block should still be accessible
    EXPECT_NE(batch.block, nullptr);
    EXPECT_EQ(batch.block->columns(), 1);
    EXPECT_EQ(batch.block->get_by_position(0).column->size(), 1);
}

TEST_F(SparseColumnOptimizationTest, RowBatchVector) {
    // Test vector of RowBatches (simulating batches from unique_key_next_batch)
    std::vector<RowBatch> batches;

    // Create multiple blocks and batches
    for (int i = 0; i < 3; ++i) {
        auto block = std::make_shared<Block>();
        auto col = ColumnInt64::create();
        for (int j = 0; j < 10; ++j) {
            col->insert_value(i * 10 + j);
        }
        block->insert({std::move(col), std::make_shared<DataTypeInt64>(), "col"});

        batches.emplace_back(block, i * 2, 5); // Different start positions
    }

    EXPECT_EQ(batches.size(), 3);

    // Verify each batch
    for (size_t i = 0; i < batches.size(); ++i) {
        EXPECT_EQ(batches[i].start_row, i * 2);
        EXPECT_EQ(batches[i].count, 5);
        EXPECT_NE(batches[i].block, nullptr);
    }
}

// ==================== All-non-NULL batch optimization tests ====================

TEST_F(SparseColumnOptimizationTest, AllNonNullBatchOptimization) {
    // Test the complete flow for all-non-NULL scenario
    // This simulates what happens in vertical_block_reader when non_null_count == batch.count

    // Create source nullable column (all non-NULL)
    std::vector<Int64> values = {10, 20, 30, 40, 50};
    std::vector<bool> null_flags = {false, false, false, false, false};
    auto src = create_nullable_column(values, null_flags);
    const auto* nullable_src = assert_cast<const ColumnNullable*>(src.get());

    // Create destination with pre-filled NULLs (simulating sparse optimization)
    auto dst = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());
    auto* nullable_dst = assert_cast<ColumnNullable*>(dst.get());

    // Pre-fill with NULLs
    nullable_dst->get_null_map_column().get_data().resize_fill(5, 1); // all NULL
    nullable_dst->get_nested_column().resize(5);

    // Check non-NULL count using SIMD
    const auto& null_map = nullable_src->get_null_map_data();
    size_t non_null_count =
            simd::count_zero_num(reinterpret_cast<const int8_t*>(null_map.data()), 5);

    EXPECT_EQ(non_null_count, 5); // All non-NULL

    // Since all non-NULL, use batch replace (triggers memcpy optimization)
    nullable_dst->replace_column_data_range(*nullable_src, 0, 5, 0);

    // Verify results
    EXPECT_EQ(nullable_dst->size(), 5);
    for (size_t i = 0; i < 5; ++i) {
        EXPECT_FALSE(nullable_dst->is_null_at(i));
    }

    const auto& nested = assert_cast<const ColumnInt64&>(nullable_dst->get_nested_column());
    EXPECT_EQ(nested.get_element(0), 10);
    EXPECT_EQ(nested.get_element(1), 20);
    EXPECT_EQ(nested.get_element(2), 30);
    EXPECT_EQ(nested.get_element(3), 40);
    EXPECT_EQ(nested.get_element(4), 50);
}

TEST_F(SparseColumnOptimizationTest, BatchReplaceWithOffset) {
    // Test batch replace at non-zero offset (simulating multiple batches)
    std::vector<Int64> values1 = {1, 2, 3};
    std::vector<bool> null_flags1 = {false, false, false};
    auto src1 = create_nullable_column(values1, null_flags1);

    std::vector<Int64> values2 = {4, 5};
    std::vector<bool> null_flags2 = {false, false};
    auto src2 = create_nullable_column(values2, null_flags2);

    // Create destination pre-filled with NULLs
    auto dst = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());
    auto* nullable_dst = assert_cast<ColumnNullable*>(dst.get());

    nullable_dst->get_null_map_column().get_data().resize_fill(5, 1);
    nullable_dst->get_nested_column().resize(5);

    // Batch 1: replace at offset 0, count 3
    nullable_dst->replace_column_data_range(*src1, 0, 3, 0);

    // Batch 2: replace at offset 3, count 2
    nullable_dst->replace_column_data_range(*src2, 0, 2, 3);

    // Verify
    const auto& nested = assert_cast<const ColumnInt64&>(nullable_dst->get_nested_column());
    EXPECT_EQ(nested.get_element(0), 1);
    EXPECT_EQ(nested.get_element(1), 2);
    EXPECT_EQ(nested.get_element(2), 3);
    EXPECT_EQ(nested.get_element(3), 4);
    EXPECT_EQ(nested.get_element(4), 5);

    // All should be non-NULL
    for (size_t i = 0; i < 5; ++i) {
        EXPECT_FALSE(nullable_dst->is_null_at(i));
    }
}

TEST_F(SparseColumnOptimizationTest, MixedBatchProcessing) {
    // Test mixed scenario: some batches all-NULL, some all-non-NULL, some mixed
    auto dst = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());
    auto* nullable_dst = assert_cast<ColumnNullable*>(dst.get());

    // Pre-fill with 15 NULLs
    nullable_dst->get_null_map_column().get_data().resize_fill(15, 1);
    nullable_dst->get_nested_column().resize(15);

    // Batch 1 (offset 0-4): All NULL - skip (already pre-filled)
    // Nothing to do

    // Batch 2 (offset 5-9): All non-NULL - use batch replace
    std::vector<Int64> values2 = {50, 51, 52, 53, 54};
    std::vector<bool> null_flags2 = {false, false, false, false, false};
    auto src2 = create_nullable_column(values2, null_flags2);
    nullable_dst->replace_column_data_range(*src2, 0, 5, 5);

    // Batch 3 (offset 10-14): Mixed - use per-row replace
    std::vector<Int64> values3 = {100, 101, 102, 103, 104};
    std::vector<bool> null_flags3 = {false, true, false, true, false};
    auto src3 = create_nullable_column(values3, null_flags3);
    const auto* nullable_src3 = assert_cast<const ColumnNullable*>(src3.get());
    const auto& null_map3 = nullable_src3->get_null_map_data();

    for (size_t i = 0; i < 5; ++i) {
        if (null_map3[i] == 0) { // non-NULL
            nullable_dst->replace_column_data(*src3, i, 10 + i);
        }
    }

    // Verify results
    // Batch 1: All NULL
    for (size_t i = 0; i < 5; ++i) {
        EXPECT_TRUE(nullable_dst->is_null_at(i)) << "Position " << i << " should be NULL";
    }

    // Batch 2: All non-NULL
    const auto& nested = assert_cast<const ColumnInt64&>(nullable_dst->get_nested_column());
    for (size_t i = 5; i < 10; ++i) {
        EXPECT_FALSE(nullable_dst->is_null_at(i)) << "Position " << i << " should be non-NULL";
        EXPECT_EQ(nested.get_element(i), 50 + (i - 5));
    }

    // Batch 3: Mixed pattern
    EXPECT_FALSE(nullable_dst->is_null_at(10));
    EXPECT_EQ(nested.get_element(10), 100);
    EXPECT_TRUE(nullable_dst->is_null_at(11));
    EXPECT_FALSE(nullable_dst->is_null_at(12));
    EXPECT_EQ(nested.get_element(12), 102);
    EXPECT_TRUE(nullable_dst->is_null_at(13));
    EXPECT_FALSE(nullable_dst->is_null_at(14));
    EXPECT_EQ(nested.get_element(14), 104);
}

TEST_F(SparseColumnOptimizationTest, LargeBatchMemcpyPerformance) {
    // Test large batch to verify memcpy optimization works correctly
    constexpr size_t num_rows = 4096; // Typical batch size

    // Create source with all non-NULL values
    auto src_nested = ColumnInt64::create();
    auto src_null_map = ColumnUInt8::create();
    for (size_t i = 0; i < num_rows; ++i) {
        src_nested->insert_value(static_cast<Int64>(i * 2));
        src_null_map->insert_value(0); // all non-NULL
    }
    auto src = ColumnNullable::create(std::move(src_nested), std::move(src_null_map));

    // Create destination pre-filled with NULLs
    auto dst = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());
    auto* nullable_dst = assert_cast<ColumnNullable*>(dst.get());
    nullable_dst->get_null_map_column().get_data().resize_fill(num_rows, 1);
    nullable_dst->get_nested_column().resize(num_rows);

    // Batch replace (should use memcpy)
    nullable_dst->replace_column_data_range(*src, 0, num_rows, 0);

    // Verify correctness
    const auto& nested = assert_cast<const ColumnInt64&>(nullable_dst->get_nested_column());
    for (size_t i = 0; i < num_rows; ++i) {
        EXPECT_FALSE(nullable_dst->is_null_at(i));
        EXPECT_EQ(nested.get_element(i), static_cast<Int64>(i * 2));
    }
}

// ==================== RLE Batch Put Optimization Tests ====================

TEST_F(SparseColumnOptimizationTest, RleBatchPutLargeRunLength) {
    // Test RLE batch Put with large run_length (typical for sparse compaction)
    // This tests the fast path optimization in RleEncoder::Put()
    constexpr int bit_width = 1; // For boolean/null bitmap
    faststring buffer;

    RleEncoder<bool> encoder(&buffer, bit_width);

    // Simulate putting 4096 NULLs (value=true) in one call
    // This is the typical pattern when compacting a sparse column that's all NULL
    encoder.Put(true, 4096);
    encoder.Flush();

    int encoded_len = encoder.len();
    EXPECT_GT(encoded_len, 0);

    // Decode and verify
    RleDecoder<bool> decoder(buffer.data(), encoded_len, bit_width);
    for (int i = 0; i < 4096; ++i) {
        bool value;
        EXPECT_TRUE(decoder.Get(&value));
        EXPECT_TRUE(value) << "Position " << i << " should be true";
    }
}

TEST_F(SparseColumnOptimizationTest, RleBatchPutMultipleCalls) {
    // Test multiple batch Put calls with same value (should accumulate efficiently)
    constexpr int bit_width = 1;
    faststring buffer;

    RleEncoder<bool> encoder(&buffer, bit_width);

    // Multiple calls with same value - should use fast path after first 8
    encoder.Put(true, 100);
    encoder.Put(true, 200);
    encoder.Put(true, 300);
    encoder.Flush();

    int encoded_len = encoder.len();
    EXPECT_GT(encoded_len, 0);

    // Decode and verify total count
    RleDecoder<bool> decoder(buffer.data(), encoded_len, bit_width);
    int count = 0;
    bool value;
    while (decoder.Get(&value)) {
        EXPECT_TRUE(value);
        count++;
    }
    EXPECT_EQ(count, 600);
}

TEST_F(SparseColumnOptimizationTest, RleBatchPutMixedValues) {
    // Test mixed pattern: long run of same value, then different value
    constexpr int bit_width = 1;
    faststring buffer;

    RleEncoder<bool> encoder(&buffer, bit_width);

    // 1000 trues, then 500 falses, then 1000 trues
    encoder.Put(true, 1000);
    encoder.Put(false, 500);
    encoder.Put(true, 1000);
    encoder.Flush();

    int encoded_len = encoder.len();
    EXPECT_GT(encoded_len, 0);

    // Decode and verify
    RleDecoder<bool> decoder(buffer.data(), encoded_len, bit_width);
    int true_count = 0, false_count = 0;
    bool value;
    while (decoder.Get(&value)) {
        if (value) {
            true_count++;
        } else {
            false_count++;
        }
    }
    EXPECT_EQ(true_count, 2000);
    EXPECT_EQ(false_count, 500);
}

TEST_F(SparseColumnOptimizationTest, RleBatchPutSmallRunLength) {
    // Test with small run_length (should still work correctly)
    constexpr int bit_width = 1;
    faststring buffer;

    RleEncoder<bool> encoder(&buffer, bit_width);

    // Small batches
    encoder.Put(true, 3);
    encoder.Put(false, 2);
    encoder.Put(true, 1);
    encoder.Put(false, 4);
    encoder.Flush();

    int encoded_len = encoder.len();
    EXPECT_GT(encoded_len, 0);

    // Decode and verify pattern
    RleDecoder<bool> decoder(buffer.data(), encoded_len, bit_width);
    bool expected[] = {true, true, true, false, false, true, false, false, false, false};
    for (int i = 0; i < 10; ++i) {
        bool value;
        EXPECT_TRUE(decoder.Get(&value));
        EXPECT_EQ(value, expected[i]) << "Mismatch at position " << i;
    }
}

TEST_F(SparseColumnOptimizationTest, RleBatchPutIntValues) {
    // Test batch Put with integer values (used in RLE encoding of data)
    constexpr int bit_width = 8;
    faststring buffer;

    RleEncoder<uint8_t> encoder(&buffer, bit_width);

    // Put repeated integer values
    encoder.Put(42, 100);
    encoder.Put(0, 200); // Common case: repeated zeros
    encoder.Put(255, 50);
    encoder.Flush();

    int encoded_len = encoder.len();
    EXPECT_GT(encoded_len, 0);

    // Decode and verify
    RleDecoder<uint8_t> decoder(buffer.data(), encoded_len, bit_width);
    uint8_t value;

    for (int i = 0; i < 100; ++i) {
        EXPECT_TRUE(decoder.Get(&value));
        EXPECT_EQ(value, 42);
    }
    for (int i = 0; i < 200; ++i) {
        EXPECT_TRUE(decoder.Get(&value));
        EXPECT_EQ(value, 0);
    }
    for (int i = 0; i < 50; ++i) {
        EXPECT_TRUE(decoder.Get(&value));
        EXPECT_EQ(value, 255);
    }
}

// ==================== Additional Column Type Tests ====================

TEST_F(SparseColumnOptimizationTest, ColumnInt32ReplaceDataRange) {
    // Test ColumnInt32 replace_column_data_range
    auto src = ColumnInt32::create();
    for (Int32 i = 1; i <= 10; ++i) {
        src->insert_value(i * 10);
    }

    auto dst = ColumnInt32::create();
    for (int i = 0; i < 10; ++i) {
        dst->insert_value(0);
    }

    dst->replace_column_data_range(*src, 2, 4, 3);

    const auto& dst_data = dst->get_data();
    EXPECT_EQ(dst_data[0], 0);
    EXPECT_EQ(dst_data[3], 30); // src[2]
    EXPECT_EQ(dst_data[4], 40); // src[3]
    EXPECT_EQ(dst_data[5], 50); // src[4]
    EXPECT_EQ(dst_data[6], 60); // src[5]
    EXPECT_EQ(dst_data[7], 0);
}

TEST_F(SparseColumnOptimizationTest, ColumnFloat64ReplaceDataRange) {
    // Test ColumnFloat64 replace_column_data_range
    auto src = ColumnFloat64::create();
    for (int i = 1; i <= 10; ++i) {
        src->insert_value(i * 1.5);
    }

    auto dst = ColumnFloat64::create();
    for (int i = 0; i < 10; ++i) {
        dst->insert_value(0.0);
    }

    dst->replace_column_data_range(*src, 0, 5, 2);

    const auto& dst_data = dst->get_data();
    EXPECT_DOUBLE_EQ(dst_data[0], 0.0);
    EXPECT_DOUBLE_EQ(dst_data[1], 0.0);
    EXPECT_DOUBLE_EQ(dst_data[2], 1.5); // src[0]
    EXPECT_DOUBLE_EQ(dst_data[3], 3.0); // src[1]
    EXPECT_DOUBLE_EQ(dst_data[4], 4.5); // src[2]
    EXPECT_DOUBLE_EQ(dst_data[5], 6.0); // src[3]
    EXPECT_DOUBLE_EQ(dst_data[6], 7.5); // src[4]
    EXPECT_DOUBLE_EQ(dst_data[7], 0.0);
}

// ==================== SIMD count_zero_num Edge Cases ====================

TEST_F(SparseColumnOptimizationTest, CountZeroNumEmpty) {
    // Test empty input
    std::vector<int8_t> data;
    EXPECT_EQ(simd::count_zero_num(data.data(), 0), 0);
}

TEST_F(SparseColumnOptimizationTest, CountZeroNumUnalignedSizes) {
    // Test various unaligned sizes (not multiples of SIMD width)
    std::vector<int8_t> data(100, 0); // All zeros

    // Test sizes that are not multiples of 16 (typical SIMD width)
    EXPECT_EQ(simd::count_zero_num(data.data(), 1), 1);
    EXPECT_EQ(simd::count_zero_num(data.data(), 7), 7);
    EXPECT_EQ(simd::count_zero_num(data.data(), 15), 15);
    EXPECT_EQ(simd::count_zero_num(data.data(), 17), 17);
    EXPECT_EQ(simd::count_zero_num(data.data(), 31), 31);
    EXPECT_EQ(simd::count_zero_num(data.data(), 33), 33);
    EXPECT_EQ(simd::count_zero_num(data.data(), 63), 63);
    EXPECT_EQ(simd::count_zero_num(data.data(), 65), 65);
}

TEST_F(SparseColumnOptimizationTest, CountZeroNumLargeArray) {
    // Test large array (typical batch size)
    constexpr size_t size = 4096;
    std::vector<int8_t> data(size);

    // All zeros
    std::fill(data.begin(), data.end(), 0);
    EXPECT_EQ(simd::count_zero_num(data.data(), size), size);

    // All ones
    std::fill(data.begin(), data.end(), 1);
    EXPECT_EQ(simd::count_zero_num(data.data(), size), 0);

    // 10% zeros (sparse pattern)
    for (size_t i = 0; i < size; ++i) {
        data[i] = (i % 10 == 0) ? 0 : 1;
    }
    EXPECT_EQ(simd::count_zero_num(data.data(), size), 410); // 4096/10 = 409.6 -> 410
}

TEST_F(SparseColumnOptimizationTest, CountZeroNumRandomPattern) {
    // Test random pattern
    constexpr size_t size = 1000;
    std::vector<int8_t> data(size);

    std::mt19937 gen(42); // Fixed seed for reproducibility
    std::uniform_int_distribution<> dis(0, 1);

    size_t expected_zeros = 0;
    for (size_t i = 0; i < size; ++i) {
        data[i] = dis(gen);
        if (data[i] == 0) expected_zeros++;
    }

    EXPECT_EQ(simd::count_zero_num(data.data(), size), expected_zeros);
}

// ==================== Threshold Configuration Tests ====================

TEST_F(SparseColumnOptimizationTest, ThresholdZeroDisablesOptimization) {
    // Test that threshold = 0 means optimization disabled
    config::sparse_column_compaction_threshold_percent = 0.0;

    // With threshold = 0, optimization should be disabled
    // This affects the decision in Merger::vertical_merge_rowsets
    double density = 0.2;
    bool use_optimization = density <= config::sparse_column_compaction_threshold_percent;
    EXPECT_FALSE(use_optimization);
    EXPECT_DOUBLE_EQ(config::sparse_column_compaction_threshold_percent, 0.0);
}

TEST_F(SparseColumnOptimizationTest, ThresholdMaxAlwaysEnabled) {
    // Test that threshold = 1 means always enabled
    config::sparse_column_compaction_threshold_percent = 1.0;

    // With max threshold, any density in [0, 1] will be <= threshold
    double density = 1.0;
    EXPECT_LE(density, config::sparse_column_compaction_threshold_percent);
}

TEST_F(SparseColumnOptimizationTest, ThresholdBasedDecision) {
    // Test threshold-based decision logic
    // density <= threshold means sparse (enable optimization)

    config::sparse_column_compaction_threshold_percent = 0.1;

    // Case 1: density = 0.05 <= 0.1, should enable
    double density1 = 0.05;
    bool use_optimization1 = (density1 <= config::sparse_column_compaction_threshold_percent);
    EXPECT_TRUE(use_optimization1);

    // Case 2: density = 0.1 <= 0.1, should enable (boundary)
    double density2 = 0.1;
    bool use_optimization2 = (density2 <= config::sparse_column_compaction_threshold_percent);
    EXPECT_TRUE(use_optimization2);

    // Case 3: density = 0.11 > 0.1, should disable
    double density3 = 0.11;
    bool use_optimization3 = (density3 <= config::sparse_column_compaction_threshold_percent);
    EXPECT_FALSE(use_optimization3);

    // Case 4: density = 0.9 > 0.1, should disable
    double density4 = 0.9;
    bool use_optimization4 = (density4 <= config::sparse_column_compaction_threshold_percent);
    EXPECT_FALSE(use_optimization4);
}

TEST_F(SparseColumnOptimizationTest, AvgRowBytesCalculation) {
    // Test average row bytes calculation: data_disk_size / total_rows
    // Simulating the calculation done in Merger::vertical_merge_rowsets

    // Case 1: Small tablet with sparse data
    int64_t data_disk_size1 = 1000; // 1KB
    int64_t total_rows1 = 100;
    int64_t avg_row_bytes1 = data_disk_size1 / total_rows1;
    EXPECT_EQ(avg_row_bytes1, 10); // 10 bytes per row - very sparse

    // Case 2: Medium tablet
    int64_t data_disk_size2 = 100000; // 100KB
    int64_t total_rows2 = 1000;
    int64_t avg_row_bytes2 = data_disk_size2 / total_rows2;
    EXPECT_EQ(avg_row_bytes2, 100); // 100 bytes per row - boundary

    // Case 3: Dense tablet
    int64_t data_disk_size3 = 10000000; // 10MB
    int64_t total_rows3 = 10000;
    int64_t avg_row_bytes3 = data_disk_size3 / total_rows3;
    EXPECT_EQ(avg_row_bytes3, 1000); // 1000 bytes per row - dense
}

// ==================== Boundary and Edge Cases ====================

TEST_F(SparseColumnOptimizationTest, EmptyColumnReplace) {
    // Test replace_column_data_range with zero count
    auto src = ColumnInt64::create();
    src->insert_value(100);

    auto dst = ColumnInt64::create();
    dst->insert_value(0);

    // Replace 0 elements (should be no-op)
    dst->replace_column_data_range(*src, 0, 0, 0);

    EXPECT_EQ(dst->get_data()[0], 0); // Unchanged
}

TEST_F(SparseColumnOptimizationTest, SingleElementColumn) {
    // Test with single element columns
    auto src = ColumnInt64::create();
    src->insert_value(42);

    auto dst = ColumnInt64::create();
    dst->insert_value(0);

    dst->replace_column_data_range(*src, 0, 1, 0);

    EXPECT_EQ(dst->get_data()[0], 42);
}

TEST_F(SparseColumnOptimizationTest, NullableColumnAllNullReplace) {
    // Test replacing all-NULL range
    std::vector<Int64> src_values = {0, 0, 0, 0, 0};
    std::vector<bool> src_null_flags = {true, true, true, true, true};
    auto src = create_nullable_column(src_values, src_null_flags);

    auto dst = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());
    auto* nullable_dst = assert_cast<ColumnNullable*>(dst.get());

    // Pre-fill with non-NULL values
    for (int i = 0; i < 5; ++i) {
        nullable_dst->get_null_map_column().get_data().push_back(0); // non-NULL
        assert_cast<ColumnInt64&>(nullable_dst->get_nested_column()).insert_value(i * 10);
    }

    // Replace with all-NULL source
    nullable_dst->replace_column_data_range(*src, 0, 5, 0);

    // All should be NULL now
    for (size_t i = 0; i < 5; ++i) {
        EXPECT_TRUE(nullable_dst->is_null_at(i));
    }
}

} // namespace doris::vectorized
