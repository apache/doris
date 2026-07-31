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
#include <limits>
#include <string>

#include "common/config.h"
#include "core/block/block.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "storage/iterator/block_reader.h"
#include "storage/iterator/vcollect_iterator.h"

namespace doris {

// ============================================================================
// Part 1: Pure-computation tests for estimate_collected_enough()
// ============================================================================

class EstimateCollectedEnoughTest : public testing::Test {};

// Budget is 0 → always false (feature disabled).
TEST_F(EstimateCollectedEnoughTest, BudgetZeroReturnsFalse) {
    EXPECT_FALSE(estimate_collected_enough(/*present_bytes=*/1000, /*present_rows=*/10,
                                           /*rows_to_merge=*/5,
                                           /*preferred_block_size_bytes=*/0));
}

// No rows collected yet → always false (cannot estimate).
TEST_F(EstimateCollectedEnoughTest, ZeroRowsReturnsFalse) {
    EXPECT_FALSE(estimate_collected_enough(/*present_bytes=*/0, /*present_rows=*/0,
                                           /*rows_to_merge=*/5,
                                           /*preferred_block_size_bytes=*/1024));
}

// Present bytes already exceed budget → true.
TEST_F(EstimateCollectedEnoughTest, PresentBytesExceedBudget) {
    EXPECT_TRUE(estimate_collected_enough(/*present_bytes=*/2048, /*present_rows=*/10,
                                          /*rows_to_merge=*/0,
                                          /*preferred_block_size_bytes=*/1024));
}

// Present bytes exactly equal budget → true.
TEST_F(EstimateCollectedEnoughTest, PresentBytesEqualBudget) {
    EXPECT_TRUE(estimate_collected_enough(/*present_bytes=*/1024, /*present_rows=*/10,
                                          /*rows_to_merge=*/0,
                                          /*preferred_block_size_bytes=*/1024));
}

// Prediction: 500 bytes / 10 rows = 50 bytes/row.
// With 10 pending rows → predicted 500 + 500 = 1000 < 1024 → false.
TEST_F(EstimateCollectedEnoughTest, PredictionBelowBudget) {
    EXPECT_FALSE(estimate_collected_enough(/*present_bytes=*/500, /*present_rows=*/10,
                                           /*rows_to_merge=*/10,
                                           /*preferred_block_size_bytes=*/1024));
}

// Prediction: 500 bytes / 10 rows = 50 bytes/row.
// With 11 pending rows → predicted 500 * 21 / 10 = 1050 >= 1024 → true.
TEST_F(EstimateCollectedEnoughTest, PredictionMeetsBudget) {
    EXPECT_TRUE(estimate_collected_enough(/*present_bytes=*/500, /*present_rows=*/10,
                                          /*rows_to_merge=*/11,
                                          /*preferred_block_size_bytes=*/1024));
}

// Zero pending rows: prediction = present_bytes (no pending rows to flush).
// 500 < 1024 → false.
TEST_F(EstimateCollectedEnoughTest, ZeroPendingRows) {
    EXPECT_FALSE(estimate_collected_enough(/*present_bytes=*/500, /*present_rows=*/10,
                                           /*rows_to_merge=*/0,
                                           /*preferred_block_size_bytes=*/1024));
}

// Exact boundary: 512 bytes / 8 rows = 64 bytes/row. With 8 pending rows →
// predicted 512 * 16 / 8 = 1024 = budget → true.
TEST_F(EstimateCollectedEnoughTest, ExactBudgetBoundary) {
    EXPECT_TRUE(estimate_collected_enough(/*present_bytes=*/512, /*present_rows=*/8,
                                          /*rows_to_merge=*/8,
                                          /*preferred_block_size_bytes=*/1024));
}

// One below boundary: 512 bytes / 8 rows = 64 bytes/row. With 7 pending rows →
// predicted 512 * 15 / 8 = 960 < 1024 → false.
TEST_F(EstimateCollectedEnoughTest, OneBelowBudgetBoundary) {
    EXPECT_FALSE(estimate_collected_enough(/*present_bytes=*/512, /*present_rows=*/8,
                                           /*rows_to_merge=*/7,
                                           /*preferred_block_size_bytes=*/1024));
}

// Overflow guard: present_bytes close to SIZE_MAX; multiplication would wrap → true.
TEST_F(EstimateCollectedEnoughTest, OverflowGuardReturnsTrueForHugeBytes) {
    const size_t huge = std::numeric_limits<size_t>::max() / 2 + 1;
    EXPECT_TRUE(estimate_collected_enough(/*present_bytes=*/huge, /*present_rows=*/1,
                                          /*rows_to_merge=*/1,
                                          /*preferred_block_size_bytes=*/1024));
}

// Large but no overflow: present_bytes * total_rows fits in size_t.
TEST_F(EstimateCollectedEnoughTest, LargeButNoOverflow) {
    // 1GB present, 100 rows, 100 pending → 2GB total predicted.
    const size_t one_gb = 1ULL << 30;
    EXPECT_TRUE(estimate_collected_enough(/*present_bytes=*/one_gb, /*present_rows=*/100,
                                          /*rows_to_merge=*/100,
                                          /*preferred_block_size_bytes=*/one_gb + 1));
}

// Single present row with many pending rows should scale correctly.
// 100 bytes / 1 row → 100 bytes/row. With 99 pending → predicted 100 * 100 / 1 = 10000 >= 5000.
TEST_F(EstimateCollectedEnoughTest, SingleRowScalesCorrectly) {
    EXPECT_TRUE(estimate_collected_enough(/*present_bytes=*/100, /*present_rows=*/1,
                                          /*rows_to_merge=*/99,
                                          /*preferred_block_size_bytes=*/5000));
}

// ============================================================================
// Part 2: Integration tests — real MutableColumns + estimate_collected_enough
//
// These tests exercise the same code path as collected_enough_rows():
//   present_bytes = Block::columns_byte_size(columns)
//   present_rows  = columns[0]->size()
//   → estimate_collected_enough(present_bytes, present_rows, rows_to_merge, budget)
//
// Level1Iterator::collected_enough_rows() is a private inner class method, so
// we replicate its logic here with real columns to verify end-to-end correctness.
// ============================================================================

class CollectedEnoughWithColumnsTest : public testing::Test {
protected:
    void SetUp() override { _saved_adaptive = config::enable_adaptive_batch_size; }
    void TearDown() override { config::enable_adaptive_batch_size = _saved_adaptive; }

    // Replicate the logic of Level1Iterator::collected_enough_rows() with a
    // configurable budget, so we can test the column-byte integration path
    // without instantiating a Level1Iterator (private inner class).
    static bool collected_enough_rows_sim(const MutableColumns& columns, int rows_to_merge,
                                          size_t preferred_block_size_bytes) {
        if (!config::enable_adaptive_batch_size) {
            return false;
        }
        if (preferred_block_size_bytes == 0) {
            return false;
        }
        const auto present_bytes = Block::columns_byte_size(columns);
        const auto present_rows = columns.empty() ? 0 : columns[0]->size();
        return estimate_collected_enough(present_bytes, present_rows, rows_to_merge,
                                         preferred_block_size_bytes);
    }

    // Build a MutableColumns with N_cols ColumnInt32 columns, each having `nrows` rows.
    // Each Int32 is 4 bytes → total = 4 * nrows * ncols bytes.
    static MutableColumns make_int32_columns(size_t ncols, size_t nrows) {
        MutableColumns cols;
        for (size_t c = 0; c < ncols; ++c) {
            auto col = ColumnInt32::create();
            for (size_t r = 0; r < nrows; ++r) {
                col->insert_value(static_cast<Int32>(r));
            }
            cols.push_back(std::move(col));
        }
        return cols;
    }

    bool _saved_adaptive = false;
};

// Config disabled → always false regardless of data.
TEST_F(CollectedEnoughWithColumnsTest, DisabledConfigReturnsFalse) {
    config::enable_adaptive_batch_size = false;
    auto cols = make_int32_columns(2, 100);
    // 2 cols × 100 rows × 4 bytes = 800 bytes; budget = 100 bytes (well below)
    EXPECT_FALSE(collected_enough_rows_sim(cols, 0, /*budget=*/100));
}

// Config enabled, budget = 0 → always false.
TEST_F(CollectedEnoughWithColumnsTest, ZeroBudgetReturnsFalse) {
    config::enable_adaptive_batch_size = true;
    auto cols = make_int32_columns(2, 100);
    EXPECT_FALSE(collected_enough_rows_sim(cols, 0, /*budget=*/0));
}

// Empty columns → present_rows = 0 → cannot estimate → false.
TEST_F(CollectedEnoughWithColumnsTest, EmptyColumnsReturnsFalse) {
    config::enable_adaptive_batch_size = true;
    MutableColumns empty;
    EXPECT_FALSE(collected_enough_rows_sim(empty, 10, /*budget=*/1024));
}

// Columns with zero rows → present_rows = 0 → false.
TEST_F(CollectedEnoughWithColumnsTest, ZeroRowColumnsReturnsFalse) {
    config::enable_adaptive_batch_size = true;
    auto cols = make_int32_columns(3, 0);
    EXPECT_FALSE(collected_enough_rows_sim(cols, 5, /*budget=*/100));
}

// Single Int32 column, 256 rows → 1024 bytes.
// Budget = 1024 → already met → true (no pending rows needed).
TEST_F(CollectedEnoughWithColumnsTest, SingleColumnExactBudgetMet) {
    config::enable_adaptive_batch_size = true;
    auto cols = make_int32_columns(1, 256);
    // 256 rows × 4 bytes = 1024 bytes
    EXPECT_EQ(Block::columns_byte_size(cols), 1024);
    EXPECT_TRUE(collected_enough_rows_sim(cols, 0, /*budget=*/1024));
}

// Single Int32 column, 255 rows → 1020 bytes < 1024 budget.
// No pending rows → not enough → false.
TEST_F(CollectedEnoughWithColumnsTest, SingleColumnBelowBudgetNoPending) {
    config::enable_adaptive_batch_size = true;
    auto cols = make_int32_columns(1, 255);
    EXPECT_EQ(Block::columns_byte_size(cols), 1020);
    EXPECT_FALSE(collected_enough_rows_sim(cols, 0, /*budget=*/1024));
}

// Single Int32 column, 255 rows → 1020 bytes. With 1 pending row:
// bytes_per_row = 1020/255 = 4, predicted = 1020 * 256 / 255 = 1024 → meets budget.
TEST_F(CollectedEnoughWithColumnsTest, SingleColumnBelowBudgetWithPending) {
    config::enable_adaptive_batch_size = true;
    auto cols = make_int32_columns(1, 255);
    EXPECT_TRUE(collected_enough_rows_sim(cols, 1, /*budget=*/1024));
}

// Multi-column: 4 Int32 columns × 100 rows = 1600 bytes.
// Budget = 2000 → not met. With 25 pending rows:
// predicted = 1600 * 125 / 100 = 2000 → meets budget.
TEST_F(CollectedEnoughWithColumnsTest, MultiColumnPredictionMeetsBudget) {
    config::enable_adaptive_batch_size = true;
    auto cols = make_int32_columns(4, 100);
    EXPECT_EQ(Block::columns_byte_size(cols), 1600);
    EXPECT_FALSE(collected_enough_rows_sim(cols, 0, /*budget=*/2000));
    EXPECT_TRUE(collected_enough_rows_sim(cols, 25, /*budget=*/2000));
}

// Multi-column: just one row below the prediction threshold.
// 4 cols × 100 rows = 1600 bytes, 24 pending rows:
// predicted = 1600 * 124 / 100 = 1984 < 2000 → false.
TEST_F(CollectedEnoughWithColumnsTest, MultiColumnOneBelowPrediction) {
    config::enable_adaptive_batch_size = true;
    auto cols = make_int32_columns(4, 100);
    EXPECT_FALSE(collected_enough_rows_sim(cols, 24, /*budget=*/2000));
}

// Variable-width column: ColumnString with known data sizes.
// ColumnString::byte_size() = chars_size + offsets_size.
// Each offset is sizeof(IColumn::Offset) = 8 bytes (64-bit).
TEST_F(CollectedEnoughWithColumnsTest, StringColumnByteSizeIntegration) {
    config::enable_adaptive_batch_size = true;

    auto str_col = ColumnString::create();
    // Insert 10 strings of 10 chars each → chars = 100 bytes, offsets = 10 * 8 = 80.
    // Total byte_size = 180 bytes.
    for (int i = 0; i < 10; ++i) {
        std::string s(10, 'A' + (i % 26));
        str_col->insert_data(s.data(), s.size());
    }

    const size_t expected_bytes = 10 * 10 + 10 * sizeof(IColumn::Offset);
    EXPECT_EQ(str_col->byte_size(), expected_bytes);

    MutableColumns cols;
    cols.push_back(std::move(str_col));

    // Budget = expected_bytes → met → true.
    EXPECT_TRUE(collected_enough_rows_sim(cols, 0, /*budget=*/expected_bytes));
    // Budget = expected_bytes + 1 → not met with 0 pending → false.
    EXPECT_FALSE(collected_enough_rows_sim(cols, 0, /*budget=*/expected_bytes + 1));
}

// Mixed columns: Int32 + String together.
TEST_F(CollectedEnoughWithColumnsTest, MixedColumnTypes) {
    config::enable_adaptive_batch_size = true;

    auto int_col = ColumnInt32::create();
    auto str_col = ColumnString::create();
    for (int i = 0; i < 50; ++i) {
        int_col->insert_value(static_cast<Int32>(i));
        std::string s(20, 'x');
        str_col->insert_data(s.data(), s.size());
    }
    // Int32: 50 × 4 = 200 bytes
    // String: 50 × 20 chars + 50 × 8 offsets = 1000 + 400 = 1400 bytes
    // Total: 1600 bytes
    const size_t int_bytes = 50 * sizeof(Int32);
    const size_t str_bytes = 50 * 20 + 50 * sizeof(IColumn::Offset);
    EXPECT_EQ(int_col->byte_size(), int_bytes);
    EXPECT_EQ(str_col->byte_size(), str_bytes);

    MutableColumns cols;
    cols.push_back(std::move(int_col));
    cols.push_back(std::move(str_col));

    const size_t total = int_bytes + str_bytes;
    EXPECT_EQ(Block::columns_byte_size(cols), total);

    // Budget met exactly → true.
    EXPECT_TRUE(collected_enough_rows_sim(cols, 0, /*budget=*/total));
    // Budget slightly above → not met with 0 pending → false.
    EXPECT_FALSE(collected_enough_rows_sim(cols, 0, /*budget=*/total + 1));
    // With 1 pending row: predicted = total * 51 / 50 = total + total/50.
    // So meets budget = total + total/50.
    EXPECT_TRUE(collected_enough_rows_sim(cols, 1, /*budget=*/total + total / 50));
}

// Large number of rows to verify no integer issues with real column byte sizes.
TEST_F(CollectedEnoughWithColumnsTest, LargeRowCountInt32) {
    config::enable_adaptive_batch_size = true;
    auto cols = make_int32_columns(1, 100000);
    // 100000 × 4 = 400000 bytes
    EXPECT_EQ(Block::columns_byte_size(cols), 400000);
    EXPECT_TRUE(collected_enough_rows_sim(cols, 0, /*budget=*/400000));
    EXPECT_FALSE(collected_enough_rows_sim(cols, 0, /*budget=*/400001));
    // With 1 pending: predicted = 400000 * 100001 / 100000 = 400004 < 400005 → false
    EXPECT_FALSE(collected_enough_rows_sim(cols, 1, /*budget=*/400005));
    // predicted = 400004 >= 400004 → true
    EXPECT_TRUE(collected_enough_rows_sim(cols, 1, /*budget=*/400004));
}

// ============================================================================
// Part 3: BlockReader.preferred_block_size_bytes() override tests
//
// BlockReader overrides TabletReader::preferred_block_size_bytes() to gate on
// config::enable_adaptive_batch_size. These tests verify that behavior and
// ensure collected_enough_rows() would receive the correct budget value.
// ============================================================================

class BlockReaderByteBudgetTest : public testing::Test {
protected:
    void SetUp() override { _saved_adaptive = config::enable_adaptive_batch_size; }
    void TearDown() override { config::enable_adaptive_batch_size = _saved_adaptive; }
    bool _saved_adaptive = false;
};

// When adaptive is enabled, preferred_block_size_bytes() returns the configured value.
TEST_F(BlockReaderByteBudgetTest, ReturnsConfiguredBytesWhenEnabled) {
    config::enable_adaptive_batch_size = true;
    BlockReader reader;
    reader._reader_context.preferred_block_size_bytes = 65536;
    EXPECT_EQ(reader.preferred_block_size_bytes(), 65536);
}

// When adaptive is disabled, preferred_block_size_bytes() returns 0 regardless.
TEST_F(BlockReaderByteBudgetTest, ReturnsZeroWhenDisabled) {
    config::enable_adaptive_batch_size = false;
    BlockReader reader;
    reader._reader_context.preferred_block_size_bytes = 65536;
    EXPECT_EQ(reader.preferred_block_size_bytes(), 0);
}

// Default value of preferred_block_size_bytes in reader context is 8MB.
TEST_F(BlockReaderByteBudgetTest, DefaultIs8MB) {
    config::enable_adaptive_batch_size = true;
    BlockReader reader;
    EXPECT_EQ(reader.preferred_block_size_bytes(), 8388608UL);
}

// Virtual dispatch: BlockReader override is reachable through a TabletReader pointer.
TEST_F(BlockReaderByteBudgetTest, VirtualDispatchThroughTabletReaderPtr) {
    config::enable_adaptive_batch_size = true;
    BlockReader concrete;
    concrete._reader_context.preferred_block_size_bytes = 99999;
    TabletReader* base = &concrete;
    // Through the virtual dispatch, BlockReader's override should be called.
    EXPECT_EQ(base->preferred_block_size_bytes(), 99999);
}

} // namespace doris
