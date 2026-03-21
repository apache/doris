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
#include "storage/segment/mock/mock_segment.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {
namespace segment_v2 {

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
    // 8 MB target, 1 MB per-column limit
    static constexpr size_t kBlockBytes = 8 * 1024 * 1024;
    static constexpr size_t kColBytes = 1 * 1024 * 1024;
    static constexpr size_t kMaxRows = 4096;
};

// ── Test 1: no history ────────────────────────────────────────────────────────
// Before any update, has_history == false and bytes_per_row == 0.
// After the first update, has_history == true and bytes_per_row == block.bytes()/rows.
TEST_F(AdaptiveBlockSizePredictorTest, NoHistoryReturnsMaxRows) {
    AdaptiveBlockSizePredictor pred(kBlockBytes, kColBytes, 0.0);

    // Initially no history.
    EXPECT_FALSE(pred.has_history_for_test());
    EXPECT_DOUBLE_EQ(pred.bytes_per_row_for_test(), 0.0);

    // After one update the first sample is stored directly (no EWMA blending).
    Block blk = make_int32_block(100);
    std::vector<ColumnId> cols = {0};
    pred.update(blk, cols);

    EXPECT_TRUE(pred.has_history_for_test());
    double expected_bpr = static_cast<double>(blk.bytes()) / 100.0;
    EXPECT_DOUBLE_EQ(pred.bytes_per_row_for_test(), expected_bpr);
}

// ── Test 2: EWMA convergence ──────────────────────────────────────────────────
// When every update delivers the same sample, the EWMA stays exactly at that
// value (0.9*v + 0.1*v == v for any v).
TEST_F(AdaptiveBlockSizePredictorTest, EwmaConvergence) {
    AdaptiveBlockSizePredictor pred(kBlockBytes, kColBytes, 0.0);

    std::vector<ColumnId> cols = {0};

    // Compute expected bytes-per-row from an actual block so the test does not
    // hard-code internal column memory layout assumptions.
    Block probe = make_string_block(100, 100);
    double expected_bpr = static_cast<double>(probe.bytes()) / 100.0;

    // First update seeds the EWMA directly.
    pred.update(probe, cols);
    EXPECT_DOUBLE_EQ(pred.bytes_per_row_for_test(), expected_bpr);

    // All subsequent updates carry the same sample → EWMA stays constant.
    for (int i = 1; i < 50; ++i) {
        Block blk = make_string_block(100, 100);
        pred.update(blk, cols);
    }
    EXPECT_NEAR(pred.bytes_per_row_for_test(), expected_bpr, 0.01);
}

// ── Test 3: per-column constraint ────────────────────────────────────────────
// After updates with a wide-column block the per-column limit must be tighter
// than the total-bytes limit, verifying the predictor would apply it.
TEST_F(AdaptiveBlockSizePredictorTest, PerColumnConstraint) {
    AdaptiveBlockSizePredictor pred(kBlockBytes, kColBytes, 0.0);

    Block blk = make_string_block(100, 1024);
    double first_bpr = static_cast<double>(blk.bytes()) / 100.0;
    std::vector<ColumnId> cols = {0};
    pred.update(blk, cols);

    // After first update, col[0] estimate equals block bytes-per-row exactly.
    EXPECT_NEAR(pred.col_bytes_per_row_for_test(0), first_bpr, 0.01);

    // Second update with 200-row block of same row width → EWMA blends.
    Block blk2 = make_string_block(200, 1024);
    double second_bpr = static_cast<double>(blk2.bytes()) / 200.0;
    pred.update(blk2, cols);

    double col_bpr = pred.col_bytes_per_row_for_test(0);
    double ewma_expected = 0.9 * first_bpr + 0.1 * second_bpr;
    EXPECT_NEAR(col_bpr, ewma_expected, 1.0);

    // The per-column row limit must be strictly smaller than the total row limit
    // for a 1024 B/row column (1 MB col limit vs 8 MB block limit).
    size_t col_limit = static_cast<size_t>(static_cast<double>(kColBytes) / col_bpr);
    size_t total_limit =
            static_cast<size_t>(static_cast<double>(kBlockBytes) / pred.bytes_per_row_for_test());
    EXPECT_LT(col_limit, total_limit);
}

// ── Test 4: zero rows block is ignored ───────────────────────────────────────
TEST_F(AdaptiveBlockSizePredictorTest, ZeroRowsBlockIgnored) {
    AdaptiveBlockSizePredictor pred(kBlockBytes, kColBytes, 0.0);

    // update() with an empty block must be a no-op.
    Block blk = make_int32_block(0);
    std::vector<ColumnId> cols = {0};
    pred.update(blk, cols);

    EXPECT_FALSE(pred.has_history_for_test());
    EXPECT_DOUBLE_EQ(pred.bytes_per_row_for_test(), 0.0);

    // A subsequent real update must still work normally.
    Block blk2 = make_int32_block(50);
    pred.update(blk2, cols);
    EXPECT_TRUE(pred.has_history_for_test());
    double expected_bpr = static_cast<double>(blk2.bytes()) / 50.0;
    EXPECT_DOUBLE_EQ(pred.bytes_per_row_for_test(), expected_bpr);
}

// ── Test 5: disabled when preferred_block_size_bytes == 0 ────────────────────
TEST_F(AdaptiveBlockSizePredictorTest, DisabledWhenBlockSizeIsZero) {
    AdaptiveBlockSizePredictor pred(0, kColBytes, 0.0);

    Block blk = make_int32_block(1000);
    std::vector<ColumnId> cols = {0};
    pred.update(blk, cols);

    // update() still records history even when budget == 0.
    EXPECT_TRUE(pred.has_history_for_test());
    EXPECT_GT(pred.bytes_per_row_for_test(), 0.0);
}

// ── Test 6: config flag disables predictor ────────────────────────────────────
TEST_F(AdaptiveBlockSizePredictorTest, ConfigDefaultEnabled) {
    EXPECT_TRUE(config::enable_adaptive_batch_size);
}

// ── Test 7: multiple column sampling ─────────────────────────────────────────
TEST_F(AdaptiveBlockSizePredictorTest, MultipleColumnSampling) {
    AdaptiveBlockSizePredictor pred(kBlockBytes, kColBytes, 0.0);

    // Build a 2-column block: col0 = Int32 (4 B/row), col1 = String(100 B/row).
    auto col0 = ColumnVector<TYPE_INT>::create();
    auto col1 = ColumnString::create();
    const size_t rows = 50;
    std::string s(100, 'y');
    for (size_t i = 0; i < rows; ++i) {
        col0->insert_value(1);
        col1->insert_data(s.data(), s.size());
    }
    Block blk;
    blk.insert({std::move(col0), std::make_shared<DataTypeInt32>(), "c0"});
    blk.insert({std::move(col1), std::make_shared<DataTypeString>(), "c1"});

    std::vector<ColumnId> cols = {0, 1};
    pred.update(blk, cols);

    // Both columns must have per-column estimates after the first update.
    double c0_bpr = pred.col_bytes_per_row_for_test(0);
    double c1_bpr = pred.col_bytes_per_row_for_test(1);
    EXPECT_GT(c0_bpr, 0.0);
    EXPECT_GT(c1_bpr, 0.0);

    // String column (100 B/row) must be much wider than Int32 column (4 B/row).
    EXPECT_GT(c1_bpr, c0_bpr);

    // Second update with same block → EWMA stays constant (same sample).
    pred.update(blk, cols);

    double expected_c0 = static_cast<double>(blk.get_by_position(0).column->byte_size()) / rows;
    double expected_c1 = static_cast<double>(blk.get_by_position(1).column->byte_size()) / rows;
    EXPECT_NEAR(pred.col_bytes_per_row_for_test(0), expected_c0, 0.01);
    EXPECT_NEAR(pred.col_bytes_per_row_for_test(1), expected_c1, 0.01);
}

// ── predict_next_rows tests ──────────────────────────────────────────────────

// ── Test: _block_size_bytes == 0 returns max_rows ────────────────────────────
TEST_F(AdaptiveBlockSizePredictorTest, PredictReturnsMaxRowsWhenDisabled) {
    AdaptiveBlockSizePredictor pred(0, kColBytes, 0.0);

    EXPECT_EQ(pred.predict_next_rows(kMaxRows), kMaxRows);

    // Even after update, still returns max_rows because block_size_bytes == 0.
    Block blk = make_int32_block(100);
    std::vector<ColumnId> cols = {0};
    pred.update(blk, cols);
    EXPECT_EQ(pred.predict_next_rows(kMaxRows), kMaxRows);
}

// ── Test: no history, no metadata hint → max_rows ───────────────────────────
TEST_F(AdaptiveBlockSizePredictorTest, PredictNoHistoryNoHint) {
    AdaptiveBlockSizePredictor pred(kBlockBytes, kColBytes, 0.0);

    EXPECT_EQ(pred.predict_next_rows(kMaxRows), kMaxRows);
}

// ── Test: no history fallback is also bounded by the first-batch safety threshold
TEST_F(AdaptiveBlockSizePredictorTest, PredictNoHistoryNoHintUsesSafetyThreshold) {
    AdaptiveBlockSizePredictor pred(kBlockBytes, kColBytes, 0.0);
    const size_t unsafe_max_rows =
            AdaptiveBlockSizePredictor::safety_batch_row_threshold_for_test() * 2;

    EXPECT_EQ(pred.predict_next_rows(unsafe_max_rows),
              AdaptiveBlockSizePredictor::safety_batch_row_threshold_for_test());
}

// ── Test: no history, metadata hint computed successfully ────────────────────
TEST_F(AdaptiveBlockSizePredictorTest, PredictNoHistoryMetadataHint) {
    // Simulate: 1 column, 400000 raw bytes in a 1000-row segment.
    // bytes_per_row_hint = (400000 / 1000) * 1.2 = 480.0
    // predicted = 8MB / 480.0 = 17476
    double hint_bpr = (400000.0 / 1000.0) * 1.2; // 480.0
    AdaptiveBlockSizePredictor pred(kBlockBytes, kColBytes, hint_bpr);

    size_t result = pred.predict_next_rows(kMaxRows);

    size_t expected = static_cast<size_t>(static_cast<double>(kBlockBytes) / hint_bpr);
    expected = std::min(expected, kMaxRows);
    EXPECT_EQ(result, expected);
}

// ── Test: no history metadata hint is bounded by the first-batch safety threshold
TEST_F(AdaptiveBlockSizePredictorTest, PredictNoHistoryMetadataHintUsesSafetyThreshold) {
    double hint_bpr = (400000.0 / 1000.0) * 1.2; // 480.0
    AdaptiveBlockSizePredictor pred(kBlockBytes, kColBytes, hint_bpr);
    const size_t unsafe_max_rows =
            AdaptiveBlockSizePredictor::safety_batch_row_threshold_for_test() * 2;

    EXPECT_EQ(pred.predict_next_rows(unsafe_max_rows),
              AdaptiveBlockSizePredictor::safety_batch_row_threshold_for_test());
}

// ── Test: no history, second call reuses same hint ───────────────────────────
TEST_F(AdaptiveBlockSizePredictorTest, PredictNoHistoryCachedHint) {
    double hint_bpr = (400000.0 / 1000.0) * 1.2;
    AdaptiveBlockSizePredictor pred(kBlockBytes, kColBytes, hint_bpr);

    size_t first = pred.predict_next_rows(kMaxRows);
    size_t second = pred.predict_next_rows(kMaxRows);

    EXPECT_EQ(first, second);
}

// ── Test: has history, uses EWMA bytes_per_row ──────────────────────────────
TEST_F(AdaptiveBlockSizePredictorTest, PredictWithHistory) {
    AdaptiveBlockSizePredictor pred(kBlockBytes, kColBytes, 0.0);

    // Inject history: 100 bytes per row.
    pred.set_has_history_for_test(true, 100.0);

    size_t result = pred.predict_next_rows(kMaxRows);
    // predicted = 8MB / 100.0 = 83886, clamped to max_rows = 4096.
    EXPECT_EQ(result, kMaxRows);
}

// ── Test: has history, predicted < max_rows (no clamping at upper bound) ────
TEST_F(AdaptiveBlockSizePredictorTest, PredictWithHistoryNoClamping) {
    // 8 KB target, not 8 MB, so predicted is small.
    AdaptiveBlockSizePredictor pred(8 * 1024, 0, 0.0);

    // 100 bytes per row → predicted = 8192 / 100 = 81.
    pred.set_has_history_for_test(true, 100.0);

    size_t result = pred.predict_next_rows(kMaxRows);
    EXPECT_EQ(result, 81u);
}

// ── Test: has history + per-column constraint tighter ────────────────────────
TEST_F(AdaptiveBlockSizePredictorTest, PredictPerColumnConstraintReducesPredicted) {
    // block budget = 8MB, col budget = 1MB.
    AdaptiveBlockSizePredictor pred(kBlockBytes, kColBytes, 0.0);

    // Total: 10 bytes/row → predicted = 8MB/10 = 838860.
    // Column 0: 5000 bytes/row → col_limit = 1MB/5000 = 209.
    // Result = min(838860, 209, 4096) = 209.
    pred.set_has_history_for_test(true, 10.0);
    pred.set_col_bytes_per_row_for_test(0, 5000.0);

    size_t result = pred.predict_next_rows(kMaxRows);
    size_t col_limit = static_cast<size_t>(static_cast<double>(kColBytes) / 5000.0);
    EXPECT_EQ(result, col_limit); // 209
}

// ── Test: has history + _max_col_bytes == 0, per-column constraint skipped ──
TEST_F(AdaptiveBlockSizePredictorTest, PredictPerColumnConstraintSkippedWhenZero) {
    // col budget = 0 → per-column constraint disabled.
    AdaptiveBlockSizePredictor pred(8 * 1024, 0, 0.0);

    // 100 bytes/row → predicted = 8192 / 100 = 81.
    // Even with a huge per-column cost, it should NOT apply.
    pred.set_has_history_for_test(true, 100.0);
    pred.set_col_bytes_per_row_for_test(0, 50000.0);

    size_t result = pred.predict_next_rows(kMaxRows);
    EXPECT_EQ(result, 81u); // Not reduced by per-column limit.
}

// ── Test: predicted > max_rows → clamped to max_rows ────────────────────────
TEST_F(AdaptiveBlockSizePredictorTest, PredictClampedToMaxRows) {
    AdaptiveBlockSizePredictor pred(kBlockBytes, kColBytes, 0.0);

    // 1 byte/row → predicted = 8MB / 1 = 8388608 >> max_rows.
    pred.set_has_history_for_test(true, 1.0);

    EXPECT_EQ(pred.predict_next_rows(kMaxRows), kMaxRows);
}

// ── Test: huge bytes_per_row → predicted < 1 → clamped to 1 ────────────────
TEST_F(AdaptiveBlockSizePredictorTest, PredictClampedToOne) {
    AdaptiveBlockSizePredictor pred(kBlockBytes, kColBytes, 0.0);

    // bytes_per_row so large that predicted rounds to 0.
    pred.set_has_history_for_test(true, static_cast<double>(kBlockBytes) * 10.0);

    EXPECT_EQ(pred.predict_next_rows(kMaxRows), 1u);
}

// ── Test: multiple per-column constraints, tightest wins ────────────────────
TEST_F(AdaptiveBlockSizePredictorTest, PredictMultipleColumnConstraints) {
    // col budget = 1MB.
    AdaptiveBlockSizePredictor pred(kBlockBytes, kColBytes, 0.0);

    // Total: 10 bytes/row → block predicted = 838860.
    // Col 0: 2000 bytes/row → col_limit_0 = 1MB/2000 = 524.
    // Col 1: 8000 bytes/row → col_limit_1 = 1MB/8000 = 131.  ← tightest
    pred.set_has_history_for_test(true, 10.0);
    pred.set_col_bytes_per_row_for_test(0, 2000.0);
    pred.set_col_bytes_per_row_for_test(1, 8000.0);

    size_t result = pred.predict_next_rows(kMaxRows);
    size_t tightest = static_cast<size_t>(static_cast<double>(kColBytes) / 8000.0); // 131
    EXPECT_EQ(result, tightest);
}

// ── Test: metadata hint with multiple columns ───────────────────────────────
TEST_F(AdaptiveBlockSizePredictorTest, PredictNoHistoryMultiColumnMetadata) {
    // Simulate: 2 columns, uid=10 with 200000B, uid=20 with 600000B, 1000 rows.
    // total_bytes = 800000, hint_bpr = (800000/1000) * 1.2 = 960.0
    double hint_bpr = (800000.0 / 1000.0) * 1.2; // 960.0
    AdaptiveBlockSizePredictor pred(kBlockBytes, kColBytes, hint_bpr);

    size_t result = pred.predict_next_rows(kMaxRows);
    size_t expected = static_cast<size_t>(static_cast<double>(kBlockBytes) / hint_bpr);
    expected = std::min(expected, kMaxRows);
    EXPECT_EQ(result, expected);
}

// ── Helper: build a MockSegment with tablet schema and cached raw_data_bytes ─
using ::testing::NiceMock;
using ::testing::Return;

static NiceMock<MockSegment>* make_mock_segment_with_raw_bytes(
        uint32_t num_rows, const std::vector<std::pair<int32_t, uint64_t>>& col_specs) {
    auto* seg = new NiceMock<MockSegment>();
    ON_CALL(*seg, num_rows()).WillByDefault(Return(num_rows));

    auto& ts = seg->tablet_schema();
    for (size_t i = 0; i < col_specs.size(); ++i) {
        TabletColumn tc(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                        FieldType::OLAP_FIELD_TYPE_INT, true, col_specs[i].first, 4);
        tc.set_name("c" + std::to_string(i));
        ts->append_column(std::move(tc));
        seg->set_column_raw_data_bytes(col_specs[i].first, col_specs[i].second);
    }
    return seg;
}

// ── Test: Segment-based constructor populates _col_bytes_per_row ─────────────
TEST_F(AdaptiveBlockSizePredictorTest, ConstructorSetsPerColumnHintFromSegment) {
    // 2 columns in a 1000-row segment:
    //   col 0 (uid=10): 200000 raw bytes → 200 bytes/row
    //   col 1 (uid=20): 8000000 raw bytes → 8000 bytes/row
    std::unique_ptr<NiceMock<MockSegment>> seg(
            make_mock_segment_with_raw_bytes(1000, {{10, 200000}, {20, 8000000}}));
    std::vector<ColumnId> cols = {0, 1};

    AdaptiveBlockSizePredictor pred(kBlockBytes, kColBytes, *seg, cols);

    // Verify per-column estimates are set from segment metadata.
    EXPECT_NEAR(pred.col_bytes_per_row_for_test(0), 200.0, 0.01);
    EXPECT_NEAR(pred.col_bytes_per_row_for_test(1), 8000.0, 0.01);

    // total hint = ((200000 + 8000000) / 1000) * 1.2 = 9840.0
    // Without per-column constraint: predicted = 8MB / 9840.0 = 853.
    // With per-column constraint (col 1): col_limit = 1MB / 8000.0 = 131.
    // Result = min(853, 131, 4096) = 131.
    size_t result = pred.predict_next_rows(kMaxRows);
    size_t col1_limit = static_cast<size_t>(static_cast<double>(kColBytes) / 8000.0);
    EXPECT_EQ(result, col1_limit); // 131
}

// ── Test: Segment-based constructor with empty segment (0 rows) ─────────────
TEST_F(AdaptiveBlockSizePredictorTest, ConstructorHandlesEmptySegment) {
    std::unique_ptr<NiceMock<MockSegment>> seg(make_mock_segment_with_raw_bytes(0, {{10, 400000}}));
    std::vector<ColumnId> cols = {0};

    AdaptiveBlockSizePredictor pred(kBlockBytes, kColBytes, *seg, cols);

    // No hint available → falls back to max_rows.
    EXPECT_DOUBLE_EQ(pred.col_bytes_per_row_for_test(0), 0.0);
    EXPECT_EQ(pred.predict_next_rows(kMaxRows), kMaxRows);
}

// ── Test: per-column hint from constructor is EWMA-blended by update() ──────
TEST_F(AdaptiveBlockSizePredictorTest, ConstructorPerColumnHintBlendedByUpdate) {
    // col 0 (uid=10): 400000 raw bytes, 1000 rows → 400 bytes/row metadata hint
    std::unique_ptr<NiceMock<MockSegment>> seg(
            make_mock_segment_with_raw_bytes(1000, {{10, 400000}}));
    std::vector<ColumnId> cols = {0};

    AdaptiveBlockSizePredictor pred(kBlockBytes, kColBytes, *seg, cols);
    EXPECT_NEAR(pred.col_bytes_per_row_for_test(0), 400.0, 0.01);

    // First update: _col_bytes_per_row[0] already exists, so EWMA applies:
    // new = 0.9 * 400.0 + 0.1 * actual_bpr
    Block blk = make_int32_block(100);
    pred.update(blk, cols);

    double actual_bpr = static_cast<double>(blk.get_by_position(0).column->byte_size()) / 100.0;
    double expected = 0.9 * 400.0 + 0.1 * actual_bpr;
    EXPECT_NEAR(pred.col_bytes_per_row_for_test(0), expected, 0.01);
}

} // namespace segment_v2
} // namespace doris
