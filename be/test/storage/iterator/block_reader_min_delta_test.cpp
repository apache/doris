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

#include <limits>
#include <vector>

#include "storage/iterator/changelog_block_reader_utils.h"

namespace doris {

using ResultType = changelog::AggregateFunctionMinDelta::ResultType;

class BlockReaderMinDeltaTest : public testing::Test {};
TEST_F(BlockReaderMinDeltaTest, ValidOperationPairs) {
    // Cover the 3x3 valid changelog op pairs to keep the min-delta mapping stable.
    struct Case {
        ChangelogOp first_op;
        ChangelogOp last_op;
        ResultType expected;
    };

    const Case cases[] = {
            {ChangelogOp::APPEND, ChangelogOp::APPEND, ResultType::INSERT},
            {ChangelogOp::APPEND, ChangelogOp::UPDATE, ResultType::INSERT},
            {ChangelogOp::APPEND, ChangelogOp::DELETE, ResultType::SKIP},
            {ChangelogOp::UPDATE, ChangelogOp::APPEND, ResultType::UPDATE_BEFORE_AFTER},
            {ChangelogOp::UPDATE, ChangelogOp::UPDATE, ResultType::UPDATE_BEFORE_AFTER},
            {ChangelogOp::UPDATE, ChangelogOp::DELETE, ResultType::DELETE},
            {ChangelogOp::DELETE, ChangelogOp::APPEND, ResultType::UPDATE_BEFORE_AFTER},
            {ChangelogOp::DELETE, ChangelogOp::UPDATE, ResultType::UPDATE_BEFORE_AFTER},
            {ChangelogOp::DELETE, ChangelogOp::DELETE, ResultType::DELETE},
    };

    for (const auto& c : cases) {
        EXPECT_EQ(c.expected,
                  changelog::AggregateFunctionMinDelta::calculate_result(c.first_op, c.last_op))
                << "first_op=" << static_cast<int64_t>(c.first_op)
                << ", last_op=" << static_cast<int64_t>(c.last_op);
    }
}

TEST_F(BlockReaderMinDeltaTest, InvalidOperationFallback) {
    // Invalid op codes (negative/out-of-range) should fall back to avoid OOB and keep changes conservatively.
    const int64_t invalid_values[] = {-1,
                                      3,
                                      4,
                                      100,
                                      std::numeric_limits<int64_t>::min(),
                                      std::numeric_limits<int64_t>::max()};

    for (int64_t invalid_op : invalid_values) {
        EXPECT_EQ(ResultType::UPDATE_BEFORE_AFTER,
                  changelog::AggregateFunctionMinDelta::calculate_result(
                          static_cast<ChangelogOp>(invalid_op), ChangelogOp::APPEND))
                << "invalid first_op=" << invalid_op;
        EXPECT_EQ(ResultType::UPDATE_BEFORE_AFTER,
                  changelog::AggregateFunctionMinDelta::calculate_result(
                          ChangelogOp::DELETE, static_cast<ChangelogOp>(invalid_op)))
                << "invalid last_op=" << invalid_op;
    }
}

TEST_F(BlockReaderMinDeltaTest, SemanticScenarios) {
    // Scenario 1: insert then delete yields no net change.
    EXPECT_EQ(ResultType::SKIP, changelog::AggregateFunctionMinDelta::calculate_result(
                                        ChangelogOp::APPEND, ChangelogOp::DELETE));

    // Scenario 2: update then delete emits DELETE (with pre-delete snapshot values).
    EXPECT_EQ(ResultType::DELETE, changelog::AggregateFunctionMinDelta::calculate_result(
                                          ChangelogOp::UPDATE, ChangelogOp::DELETE));

    // Scenario 3: delete then re-add means the key existed before the window and is treated as UPDATE.
    EXPECT_EQ(ResultType::UPDATE_BEFORE_AFTER,
              changelog::AggregateFunctionMinDelta::calculate_result(ChangelogOp::DELETE,
                                                                     ChangelogOp::APPEND));
}

TEST_F(BlockReaderMinDeltaTest, CrossRowsetSameKeyScenarios) {
    // Model same-key changelog ops read from multiple rowsets in commit order.
    // The min-delta result depends on the first and last op for that key, regardless of rowset boundaries.
    auto calc_from_rowsets =
            [](const std::vector<std::vector<ChangelogOp>>& rowsets) -> ResultType {
        bool found = false;
        ChangelogOp first_op = ChangelogOp::APPEND;
        ChangelogOp last_op = ChangelogOp::APPEND;
        for (const auto& rowset_ops : rowsets) {
            for (ChangelogOp op : rowset_ops) {
                if (!found) {
                    first_op = op;
                    found = true;
                }
                last_op = op;
            }
        }
        return found ? changelog::AggregateFunctionMinDelta::calculate_result(first_op, last_op)
                     : ResultType::SKIP;
    };

    // Scenario 1: key1 updated in rowset-1 and updated again in rowset-2 -> UPDATE_BEFORE_AFTER.
    EXPECT_EQ(ResultType::UPDATE_BEFORE_AFTER,
              calc_from_rowsets({{ChangelogOp::UPDATE}, {ChangelogOp::UPDATE}}));

    // Scenario 2: key1 appended/updated in rowset-1, then updated in rowset-2 -> INSERT.
    EXPECT_EQ(ResultType::INSERT, calc_from_rowsets({{ChangelogOp::APPEND, ChangelogOp::UPDATE},
                                                     {ChangelogOp::UPDATE}}));

    // Scenario 3: key1 appended in one rowset and deleted in a later rowset -> SKIP.
    EXPECT_EQ(ResultType::SKIP, calc_from_rowsets({{ChangelogOp::APPEND}, {ChangelogOp::DELETE}}));

    // Scenario 4: key1 deleted first, then appended in later rowset -> UPDATE_BEFORE_AFTER.
    EXPECT_EQ(ResultType::UPDATE_BEFORE_AFTER,
              calc_from_rowsets({{ChangelogOp::DELETE}, {ChangelogOp::APPEND}}));

    // Scenario 5: empty rowsets around the same key should not affect folding.
    EXPECT_EQ(ResultType::DELETE,
              calc_from_rowsets({{}, {ChangelogOp::UPDATE}, {}, {ChangelogOp::DELETE}, {}}));
}

TEST_F(BlockReaderMinDeltaTest, ChangelogOperationCodeLayoutGuard) {
    // The implementation uses op codes as 2D lookup indices, so guard the op layout to prevent implicit OOB.
    EXPECT_EQ(0, static_cast<int64_t>(ChangelogOp::APPEND));
    EXPECT_EQ(1, static_cast<int64_t>(ChangelogOp::UPDATE));
    EXPECT_EQ(2, static_cast<int64_t>(ChangelogOp::DELETE));
}

// Contract test for the group-boundary rule in ChangelogMinDeltaBlockIterator. A MIN_DELTA
// group is a run of consecutive rows sharing the same user key; the iterator folds each group
// into one net change via calculate_result(first_op, last_op). The iterator itself is driven end
// to end by block_reader_change_next_block_test.cpp and block_reader_table_models_test.cpp; this
// test pins the boundary rule itself.
//
// The subtle bug this guards against: the reader used to split groups on IteratorRowRef::is_same,
// which the segment merge sets only for CROSS-segment same-key matches (it drives dedup). When a
// compaction / changelog LMax quick-merge folds a key's whole change chain into ONE segment, those
// consecutive same-key rows carry is_same = false, so an is_same-based grouping shatters one key
// into many single-row groups. The fix groups by comparing the leading key columns directly.
namespace {
struct QmRow {
    int64_t key;
    ChangelogOp op;
    bool is_same; // as produced by the segment merge (cross-segment dedup marker)
};

// Old behavior: start a new group whenever is_same is false.
std::vector<ResultType> fold_by_is_same(const std::vector<QmRow>& rows) {
    std::vector<ResultType> out;
    size_t i = 0;
    while (i < rows.size()) {
        ChangelogOp first_op = rows[i].op;
        ChangelogOp last_op = rows[i].op;
        size_t j = i + 1;
        while (j < rows.size() && rows[j].is_same) {
            last_op = rows[j].op;
            ++j;
        }
        out.push_back(changelog::AggregateFunctionMinDelta::calculate_result(first_op, last_op));
        i = j;
    }
    return out;
}

// New behavior: start a new group when the user key changes (input is globally key-ordered).
std::vector<ResultType> fold_by_key(const std::vector<QmRow>& rows) {
    std::vector<ResultType> out;
    size_t i = 0;
    while (i < rows.size()) {
        ChangelogOp first_op = rows[i].op;
        ChangelogOp last_op = rows[i].op;
        size_t j = i + 1;
        while (j < rows.size() && rows[j].key == rows[i].key) {
            last_op = rows[j].op;
            ++j;
        }
        out.push_back(changelog::AggregateFunctionMinDelta::calculate_result(first_op, last_op));
        i = j;
    }
    return out;
}
} // namespace

TEST_F(BlockReaderMinDeltaTest, GroupBoundaryUsesKeyNotIsSame) {
    // Three keys, each with a whole change chain folded into a single quick-merge segment, so every
    // row's is_same is false (no cross-segment match). Rows are globally key-ordered by TSO.
    //   key 1: APPEND, UPDATE, UPDATE  -> folds to INSERT
    //   key 2: UPDATE, UPDATE          -> folds to UPDATE_BEFORE_AFTER
    //   key 3: APPEND, DELETE          -> folds to SKIP
    const std::vector<QmRow> rows = {
            {1, ChangelogOp::APPEND, false}, {1, ChangelogOp::UPDATE, false},
            {1, ChangelogOp::UPDATE, false}, {2, ChangelogOp::UPDATE, false},
            {2, ChangelogOp::UPDATE, false}, {3, ChangelogOp::APPEND, false},
            {3, ChangelogOp::DELETE, false},
    };

    // Grouping by key yields exactly one folded change per key.
    const std::vector<ResultType> by_key = fold_by_key(rows);
    ASSERT_EQ(3u, by_key.size());
    EXPECT_EQ(ResultType::INSERT, by_key[0]);
    EXPECT_EQ(ResultType::UPDATE_BEFORE_AFTER, by_key[1]);
    EXPECT_EQ(ResultType::SKIP, by_key[2]);

    // The old is_same-based grouping shatters each key into single-row groups: 7 rows -> 7 groups,
    // none of which reflect the true per-key net change (each APPEND/UPDATE alone folds to INSERT/
    // UPDATE, and the key-3 APPEND+DELETE that should cancel to SKIP is instead two separate rows).
    const std::vector<ResultType> by_is_same = fold_by_is_same(rows);
    EXPECT_EQ(7u, by_is_same.size());
    EXPECT_NE(by_key.size(), by_is_same.size());
}

TEST_F(BlockReaderMinDeltaTest, GroupBoundaryMixedIsSameStillGroupsByKey) {
    // Realistic mix: some same-key rows are cross-segment (is_same=true), others were folded into one
    // segment (is_same=false). Grouping by key must be insensitive to how is_same happened to be set.
    //   key 1: APPEND(false), UPDATE(true), UPDATE(false)  -> INSERT
    //   key 2: UPDATE(false), DELETE(false)                -> DELETE
    const std::vector<QmRow> rows = {
            {1, ChangelogOp::APPEND, false}, {1, ChangelogOp::UPDATE, true},
            {1, ChangelogOp::UPDATE, false}, {2, ChangelogOp::UPDATE, false},
            {2, ChangelogOp::DELETE, false},
    };

    const std::vector<ResultType> by_key = fold_by_key(rows);
    ASSERT_EQ(2u, by_key.size());
    EXPECT_EQ(ResultType::INSERT, by_key[0]);
    EXPECT_EQ(ResultType::DELETE, by_key[1]);
}

} // namespace doris
