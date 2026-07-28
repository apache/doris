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

#include "storage/segment/adaptive_block_size_predictor.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "common/config.h"
#include "core/block/block.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "storage/olap_common.h"

namespace doris {

// ── helper functions ──────────────────────────────────────────────────────────

// Build a Block with N rows, each containing a single Int32 column of the given value.
static Block make_int32_block(size_t rows, int32_t value = 42) {
    auto col = ColumnVector<TYPE_INT>::create();
    col->reserve(rows);
    for (size_t i = 0; i < rows; ++i) {
        col->insert_value(value);
    }
    Block block;
    block.insert({std::move(col), std::make_shared<DataTypeInt32>(), "c0"});
    return block;
}

// Build a Block with N rows where each row holds a string of |str_len| bytes.
static Block make_string_block(size_t rows, size_t str_len) {
    auto col = ColumnString::create();
    col->reserve(rows);
    std::string s(str_len, 'x');
    for (size_t i = 0; i < rows; ++i) {
        col->insert_data(s.data(), s.size());
    }
    Block block;
    block.insert({std::move(col), std::make_shared<DataTypeString>(), "c0"});
    return block;
}

// ── AdaptiveBlockSizePredictorTest ───────────────────────────────────────────

class AdaptiveBlockSizePredictorTest : public testing::Test {
protected:
    // 8 MB target
    static constexpr size_t kBlockBytes = 8 * 1024 * 1024;
    static constexpr size_t kMaxRows = 4096;
};

// ── Test 1: no history ────────────────────────────────────────────────────────
// Before any update, has_history == false and bytes_per_row == 0.
// After the first update, has_history == true and bytes_per_row == block.bytes()/rows.
TEST_F(AdaptiveBlockSizePredictorTest, NoHistoryReturnsMaxRows) {
    AdaptiveBlockSizePredictor pred(kBlockBytes, 0.0);

    // Initially no history.
    EXPECT_FALSE(pred.has_history_for_test());
    EXPECT_DOUBLE_EQ(pred.bytes_per_row_for_test(), 0.0);

    // After one update the first sample is stored directly (no EWMA blending).
    Block blk = make_int32_block(100);
    std::vector<ColumnId> cols = {0};
    pred.update(blk);

    EXPECT_TRUE(pred.has_history_for_test());
    double expected_bpr = static_cast<double>(blk.bytes()) / 100.0;
    EXPECT_DOUBLE_EQ(pred.bytes_per_row_for_test(), expected_bpr);
}

// ── Test 2: EWMA convergence ──────────────────────────────────────────────────
// When every update delivers the same sample, the EWMA stays exactly at that
// value (0.9*v + 0.1*v == v for any v).
TEST_F(AdaptiveBlockSizePredictorTest, EwmaConvergence) {
    AdaptiveBlockSizePredictor pred(kBlockBytes, 0.0);

    std::vector<ColumnId> cols = {0};

    // Compute expected bytes-per-row from an actual block so the test does not
    // hard-code internal column memory layout assumptions.
    Block probe = make_string_block(100, 100);
    double expected_bpr = static_cast<double>(probe.bytes()) / 100.0;

    // First update seeds the EWMA directly.
    pred.update(probe);
    EXPECT_DOUBLE_EQ(pred.bytes_per_row_for_test(), expected_bpr);

    // All subsequent updates carry the same sample → EWMA stays constant.
    for (int i = 1; i < 50; ++i) {
        Block blk = make_string_block(100, 100);
        pred.update(blk);
    }
    EXPECT_NEAR(pred.bytes_per_row_for_test(), expected_bpr, 0.01);
}

// ── Test 4: zero rows block is ignored ───────────────────────────────────────
TEST_F(AdaptiveBlockSizePredictorTest, ZeroRowsBlockIgnored) {
    AdaptiveBlockSizePredictor pred(kBlockBytes, 0.0);

    // update() with an empty block must be a no-op.
    Block blk = make_int32_block(0);
    std::vector<ColumnId> cols = {0};
    pred.update(blk);

    EXPECT_FALSE(pred.has_history_for_test());
    EXPECT_DOUBLE_EQ(pred.bytes_per_row_for_test(), 0.0);

    // A subsequent real update must still work normally.
    Block blk2 = make_int32_block(50);
    pred.update(blk2);
    EXPECT_TRUE(pred.has_history_for_test());
    double expected_bpr = static_cast<double>(blk2.bytes()) / 50.0;
    EXPECT_DOUBLE_EQ(pred.bytes_per_row_for_test(), expected_bpr);
}

// ── Test 5: disabled when preferred_block_size_bytes == 0 ────────────────────
TEST_F(AdaptiveBlockSizePredictorTest, DisabledWhenBlockSizeIsZero) {
    AdaptiveBlockSizePredictor pred(0, 0.0);

    Block blk = make_int32_block(1000);
    std::vector<ColumnId> cols = {0};
    pred.update(blk);

    // update() still records history even when budget == 0.
    EXPECT_TRUE(pred.has_history_for_test());
    EXPECT_GT(pred.bytes_per_row_for_test(), 0.0);
}

// ── Test 6: config flag disables predictor ────────────────────────────────────
TEST_F(AdaptiveBlockSizePredictorTest, ConfigDefaultEnabled) {
    EXPECT_TRUE(config::enable_adaptive_batch_size);
}

// ── predict_next_rows tests ──────────────────────────────────────────────────

// ── Test: _block_size_bytes == 0 returns block_size_rows ─────────────────────
TEST_F(AdaptiveBlockSizePredictorTest, PredictReturnsBlockSizeRowsWhenDisabled) {
    AdaptiveBlockSizePredictor pred(0, 0.0);

    EXPECT_EQ(pred.predict_next_rows(), pred.block_size_rows_for_test());

    // Even after update, still returns block_size_rows because block_size_bytes == 0.
    Block blk = make_int32_block(100);
    std::vector<ColumnId> cols = {0};
    pred.update(blk);
    EXPECT_EQ(pred.predict_next_rows(), pred.block_size_rows_for_test());
}

// ── Test: no history, no metadata hint → probe_rows ────────────────────────
TEST_F(AdaptiveBlockSizePredictorTest, PredictNoHistoryNoHint) {
    AdaptiveBlockSizePredictor pred(kBlockBytes, 0.0);

    EXPECT_EQ(pred.predict_next_rows(), pred.probe_rows_for_test());
}

// ── Test: no history fallback is also bounded by the first-batch safety threshold
TEST_F(AdaptiveBlockSizePredictorTest, PredictNoHistoryNoHintUsesSafetyThreshold) {
    AdaptiveBlockSizePredictor pred(kBlockBytes, 0.0);

    EXPECT_EQ(pred.predict_next_rows(), pred.probe_rows_for_test());
}

// ── Test: no history, metadata hint computed successfully ────────────────────
TEST_F(AdaptiveBlockSizePredictorTest, PredictNoHistoryMetadataHint) {
    // Simulate: 1 column, 400000 raw bytes in a 1000-row segment.
    // bytes_per_row_hint = (400000 / 1000) * 1.2 = 480.0
    // predicted = 8MB / 480.0 = 17476
    double hint_bpr = (400000.0 / 1000.0) * 1.2; // 480.0
    AdaptiveBlockSizePredictor pred(kBlockBytes, hint_bpr);

    size_t result = pred.predict_next_rows();

    size_t expected = static_cast<size_t>(static_cast<double>(kBlockBytes) / hint_bpr);
    // No history: probe_rows clamps the result.
    expected = std::min(expected, pred.probe_rows_for_test());
    EXPECT_EQ(result, expected);
}

// ── Test: no history metadata hint is bounded by the first-batch safety threshold
TEST_F(AdaptiveBlockSizePredictorTest, PredictNoHistoryMetadataHintUsesSafetyThreshold) {
    double hint_bpr = (400000.0 / 1000.0) * 1.2; // 480.0
    AdaptiveBlockSizePredictor pred(kBlockBytes, hint_bpr);

    EXPECT_EQ(pred.predict_next_rows(), pred.probe_rows_for_test());
}

// ── Test: no history, second call reuses same hint ───────────────────────────
TEST_F(AdaptiveBlockSizePredictorTest, PredictNoHistoryCachedHint) {
    double hint_bpr = (400000.0 / 1000.0) * 1.2;
    AdaptiveBlockSizePredictor pred(kBlockBytes, hint_bpr);

    size_t first = pred.predict_next_rows();
    size_t second = pred.predict_next_rows();

    EXPECT_EQ(first, second);
}

// ── Test: has history, uses EWMA bytes_per_row ──────────────────────────────
TEST_F(AdaptiveBlockSizePredictorTest, PredictWithHistory) {
    AdaptiveBlockSizePredictor pred(kBlockBytes, 0.0);

    // Inject history: 100 bytes per row.
    pred.set_has_history_for_test(true, 100.0);

    size_t result = pred.predict_next_rows();
    // predicted = 8MB / 100.0 = 83886, clamped to block_size_rows = 65535.
    EXPECT_EQ(result, pred.block_size_rows_for_test());
}

// ── Test: has history, predicted < block_size_rows (no clamping at upper bound) ────
TEST_F(AdaptiveBlockSizePredictorTest, PredictWithHistoryNoClamping) {
    // 8 KB target, not 8 MB, so predicted is small.
    AdaptiveBlockSizePredictor pred(8 * 1024, 0.0);

    // 100 bytes per row → predicted = 8192 / 100 = 81.
    pred.set_has_history_for_test(true, 100.0);

    size_t result = pred.predict_next_rows();
    EXPECT_EQ(result, 81u);
}

// ── Test: predicted > block_size_rows → clamped to block_size_rows ─────────────
TEST_F(AdaptiveBlockSizePredictorTest, PredictClampedToBlockSizeRows) {
    AdaptiveBlockSizePredictor pred(kBlockBytes, 0.0);

    // 1 byte/row → predicted = 8MB / 1 = 8388608 >> block_size_rows.
    pred.set_has_history_for_test(true, 1.0);

    EXPECT_EQ(pred.predict_next_rows(), pred.block_size_rows_for_test());
}

// ── Test: huge bytes_per_row → predicted < 1 → clamped to 1 ────────────────
TEST_F(AdaptiveBlockSizePredictorTest, PredictClampedToOne) {
    AdaptiveBlockSizePredictor pred(kBlockBytes, 0.0);

    // bytes_per_row so large that predicted rounds to 0.
    pred.set_has_history_for_test(true, static_cast<double>(kBlockBytes) * 10.0);

    EXPECT_EQ(pred.predict_next_rows(), 1u);
}

// ── Test: metadata hint with multiple columns ───────────────────────────────
TEST_F(AdaptiveBlockSizePredictorTest, PredictNoHistoryMultiColumnMetadata) {
    // Simulate: 2 columns, uid=10 with 200000B, uid=20 with 600000B, 1000 rows.
    // total_bytes = 800000, hint_bpr = (800000/1000) * 1.2 = 960.0
    double hint_bpr = (800000.0 / 1000.0) * 1.2; // 960.0
    AdaptiveBlockSizePredictor pred(kBlockBytes, hint_bpr);

    size_t result = pred.predict_next_rows();
    size_t expected = static_cast<size_t>(static_cast<double>(kBlockBytes) / hint_bpr);
    // No history: probe_rows clamps the result.
    expected = std::min(expected, pred.probe_rows_for_test());
    EXPECT_EQ(result, expected);
}

// ── Test: no hint (simulates empty segment with 0 rows) ─────────────────────
TEST_F(AdaptiveBlockSizePredictorTest, ConstructorHandlesNoHint) {
    AdaptiveBlockSizePredictor pred(kBlockBytes, 0.0);

    // No hint available → falls back to the default first-batch probe limit.
    EXPECT_EQ(pred.predict_next_rows(), pred.probe_rows_for_test());
}

TEST_F(AdaptiveBlockSizePredictorTest, PredictUsesCustomProbeRowsWithoutHint) {
    constexpr size_t custom_probe_rows = 128;
    AdaptiveBlockSizePredictor pred(kBlockBytes, 0.0, custom_probe_rows);

    EXPECT_EQ(pred.probe_rows_for_test(), custom_probe_rows);
    EXPECT_EQ(pred.predict_next_rows(), custom_probe_rows);
}

TEST_F(AdaptiveBlockSizePredictorTest, PredictUsesCustomProbeRowsWithHint) {
    constexpr size_t custom_probe_rows = 128;
    double hint_bpr = 1.0;
    AdaptiveBlockSizePredictor pred(kBlockBytes, hint_bpr, custom_probe_rows);

    EXPECT_EQ(pred.predict_next_rows(), custom_probe_rows);
}

TEST_F(AdaptiveBlockSizePredictorTest, PredictProbeRowsZeroFallsBackToOne) {
    AdaptiveBlockSizePredictor pred(kBlockBytes, 0.0, 0);

    EXPECT_EQ(pred.probe_rows_for_test(), 0u);
    EXPECT_EQ(pred.predict_next_rows(), 1u);
}

TEST_F(AdaptiveBlockSizePredictorTest, PredictProbeRowsOneWorks) {
    AdaptiveBlockSizePredictor pred(kBlockBytes, 0.0, 1);

    EXPECT_EQ(pred.predict_next_rows(), 1u);
}

// ── batch_size tests ────────────────────────────────────────────────────────

TEST_F(AdaptiveBlockSizePredictorTest, DefaultBlockSizeRows) {
    AdaptiveBlockSizePredictor pred(kBlockBytes, 0.0);

    EXPECT_EQ(pred.block_size_rows_for_test(),
              AdaptiveBlockSizePredictor::default_block_size_rows_for_test());
    EXPECT_EQ(pred.block_size_rows_for_test(), 65535u);
}

TEST_F(AdaptiveBlockSizePredictorTest, CustomBlockSizeRows) {
    constexpr size_t custom_rows = 1024;
    AdaptiveBlockSizePredictor pred(kBlockBytes, 0.0, AdaptiveBlockSizePredictor::kDefaultProbeRows,
                                    custom_rows);

    EXPECT_EQ(pred.block_size_rows_for_test(), custom_rows);

    // With history: 1 byte/row → predicted = 8MB, clamped to custom_rows = 1024.
    pred.set_has_history_for_test(true, 1.0);
    EXPECT_EQ(pred.predict_next_rows(), custom_rows);
}

TEST_F(AdaptiveBlockSizePredictorTest, BlockSizeRowsClampsWithHistory) {
    constexpr size_t custom_rows = 500;
    AdaptiveBlockSizePredictor pred(kBlockBytes, 0.0, AdaptiveBlockSizePredictor::kDefaultProbeRows,
                                    custom_rows);

    // 100 bytes/row → predicted = 8MB/100 = 83886, clamped to custom_rows = 500.
    pred.set_has_history_for_test(true, 100.0);
    EXPECT_EQ(pred.predict_next_rows(), custom_rows);
}

TEST_F(AdaptiveBlockSizePredictorTest, BlockSizeRowsDoesNotAffectSmallPrediction) {
    constexpr size_t custom_rows = 10000;
    // 8 KB target, custom block_size_rows = 10000.
    AdaptiveBlockSizePredictor pred(8 * 1024, 0.0, AdaptiveBlockSizePredictor::kDefaultProbeRows,
                                    custom_rows);

    // 100 bytes/row → predicted = 8192/100 = 81 < custom_rows.
    pred.set_has_history_for_test(true, 100.0);
    EXPECT_EQ(pred.predict_next_rows(), 81u);
}

} // namespace doris
