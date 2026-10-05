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

#include "util/block_budget.h"

#include <gtest/gtest.h>

namespace doris {

class BlockBudgetTest : public ::testing::Test {};

// ── effective_max_rows ──────────────────────────────────────────────────────

TEST_F(BlockBudgetTest, EffectiveMaxRowsNoByteBudget) {
    BlockBudget b(4096, 0);
    EXPECT_EQ(b.effective_max_rows(100), 4096);
    EXPECT_EQ(b.effective_max_rows(0), 4096);
}

TEST_F(BlockBudgetTest, EffectiveMaxRowsZeroEstimate) {
    BlockBudget b(4096, 8 * 1024 * 1024);
    // When estimate is 0, fall back to max_rows.
    EXPECT_EQ(b.effective_max_rows(0), 4096);
}

TEST_F(BlockBudgetTest, EffectiveMaxRowsByteLimited) {
    // 8 MB budget, 10 KB per row → 819 rows (< 4096 max_rows)
    BlockBudget b(4096, 8 * 1024 * 1024);
    EXPECT_EQ(b.effective_max_rows(10 * 1024), 819);
}

TEST_F(BlockBudgetTest, EffectiveMaxRowsRowLimited) {
    // 8 MB budget, 10 bytes per row → 838860 rows, but max_rows = 4096
    BlockBudget b(4096, 8 * 1024 * 1024);
    EXPECT_EQ(b.effective_max_rows(10), 4096);
}

TEST_F(BlockBudgetTest, EffectiveMaxRowsReturnsAtLeastOne) {
    // Huge rows: 100 MB per row, 8 MB budget → 0, but clamped to 1
    BlockBudget b(4096, 8 * 1024 * 1024);
    EXPECT_EQ(b.effective_max_rows(100 * 1024 * 1024), 1);
}

// ── within_budget / exceeded ────────────────────────────────────────────────

TEST_F(BlockBudgetTest, WithinBudgetNoByteBudget) {
    BlockBudget b(100, 0);
    EXPECT_TRUE(b.within_budget(0, 0));
    EXPECT_TRUE(b.within_budget(99, 999999999));
    EXPECT_FALSE(b.within_budget(100, 0));
    EXPECT_FALSE(b.within_budget(200, 0));
}

TEST_F(BlockBudgetTest, WithinBudgetWithByteBudget) {
    BlockBudget b(100, 1000);
    EXPECT_TRUE(b.within_budget(50, 500));    // both under
    EXPECT_FALSE(b.within_budget(100, 500));  // rows hit
    EXPECT_FALSE(b.within_budget(50, 1000));  // bytes hit
    EXPECT_FALSE(b.within_budget(100, 1000)); // both hit
}

TEST_F(BlockBudgetTest, ExceededIsInverseOfWithinBudget) {
    BlockBudget b(100, 1000);
    // Note: exceeded uses >=, within_budget uses <, so they should be
    // perfect logical inverses.
    for (size_t r : {0, 50, 99, 100, 200}) {
        for (size_t bytes : {0, 500, 999, 1000, 2000}) {
            EXPECT_EQ(b.exceeded(r, bytes), !b.within_budget(r, bytes))
                    << "r=" << r << " bytes=" << bytes;
        }
    }
}

// ── remaining_rows ──────────────────────────────────────────────────────────

TEST_F(BlockBudgetTest, RemainingRowsNoByteBudget) {
    BlockBudget b(100, 0);
    EXPECT_EQ(b.remaining_rows(0, 0), 100);
    EXPECT_EQ(b.remaining_rows(60, 9999), 40);
    EXPECT_EQ(b.remaining_rows(100, 0), 0);
    EXPECT_EQ(b.remaining_rows(200, 0), 0);
}

TEST_F(BlockBudgetTest, RemainingRowsByteLimited) {
    // max_rows=100, max_bytes=1000, current: 50 rows, 600 bytes
    // avg = 12 bytes/row, byte_capacity = (1000-600)/12 = 33
    // row_capacity = 100 - 50 = 50
    // result = min(50, 33) = 33
    BlockBudget b(100, 1000);
    EXPECT_EQ(b.remaining_rows(50, 600), 33);
}

TEST_F(BlockBudgetTest, RemainingRowsAlreadyOverByteBudget) {
    BlockBudget b(100, 1000);
    EXPECT_EQ(b.remaining_rows(50, 1000), 0);
    EXPECT_EQ(b.remaining_rows(50, 2000), 0);
}

TEST_F(BlockBudgetTest, RemainingRowsZeroCurrentRows) {
    // No rows yet → can't estimate avg_row_bytes, fall back to row capacity
    BlockBudget b(100, 1000);
    EXPECT_EQ(b.remaining_rows(0, 0), 100);
}

TEST_F(BlockBudgetTest, RemainingRowsZeroCurrentBytes) {
    // Has rows but zero bytes → can't estimate avg, fall back to row capacity
    BlockBudget b(100, 1000);
    EXPECT_EQ(b.remaining_rows(50, 0), 50);
}

} // namespace doris
