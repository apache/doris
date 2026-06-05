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

#include "storage/iterator/block_reader_utils.h"

namespace doris {

using ResultType = AggregateFunctionMinDelta::ResultType;

class BlockReaderMinDeltaTest : public testing::Test {};
TEST_F(BlockReaderMinDeltaTest, ValidOperationPairs) {
    // Cover the 3x3 valid row binlog op pairs to keep the min-delta mapping stable.
    struct Case {
        int64_t first_op;
        int64_t last_op;
        ResultType expected;
    };

    const Case cases[] = {
            {ROW_BINLOG_APPEND, ROW_BINLOG_APPEND, ResultType::INSERT},
            {ROW_BINLOG_APPEND, ROW_BINLOG_UPDATE, ResultType::INSERT},
            {ROW_BINLOG_APPEND, ROW_BINLOG_DELETE, ResultType::SKIP},
            {ROW_BINLOG_UPDATE, ROW_BINLOG_APPEND, ResultType::UPDATE_BEFORE_AFTER},
            {ROW_BINLOG_UPDATE, ROW_BINLOG_UPDATE, ResultType::UPDATE_BEFORE_AFTER},
            {ROW_BINLOG_UPDATE, ROW_BINLOG_DELETE, ResultType::DELETE},
            {ROW_BINLOG_DELETE, ROW_BINLOG_APPEND, ResultType::INSERT},
            {ROW_BINLOG_DELETE, ROW_BINLOG_UPDATE, ResultType::INSERT},
            {ROW_BINLOG_DELETE, ROW_BINLOG_DELETE, ResultType::DELETE},
    };

    for (const auto& c : cases) {
        EXPECT_EQ(c.expected, AggregateFunctionMinDelta::calculate_result(c.first_op, c.last_op))
                << "first_op=" << c.first_op << ", last_op=" << c.last_op;
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
                  AggregateFunctionMinDelta::calculate_result(invalid_op, ROW_BINLOG_APPEND))
                << "invalid first_op=" << invalid_op;
        EXPECT_EQ(ResultType::UPDATE_BEFORE_AFTER,
                  AggregateFunctionMinDelta::calculate_result(ROW_BINLOG_DELETE, invalid_op))
                << "invalid last_op=" << invalid_op;
    }
}

TEST_F(BlockReaderMinDeltaTest, SemanticScenarios) {
    // Scenario 1: insert then delete yields no net change.
    EXPECT_EQ(ResultType::SKIP,
              AggregateFunctionMinDelta::calculate_result(ROW_BINLOG_APPEND, ROW_BINLOG_DELETE));

    // Scenario 2: update then delete emits DELETE (with pre-delete snapshot values).
    EXPECT_EQ(ResultType::DELETE,
              AggregateFunctionMinDelta::calculate_result(ROW_BINLOG_UPDATE, ROW_BINLOG_DELETE));

    // Scenario 3: delete then insert (rebuild) is treated as INSERT.
    EXPECT_EQ(ResultType::INSERT,
              AggregateFunctionMinDelta::calculate_result(ROW_BINLOG_DELETE, ROW_BINLOG_APPEND));
}

TEST_F(BlockReaderMinDeltaTest, CrossRowsetSameKeyScenarios) {
    // Model same-key row binlog ops read from multiple rowsets in commit order.
    // The min-delta result depends on the first and last op for that key, regardless of rowset boundaries.
    auto calc_from_rowsets = [](const std::vector<std::vector<int64_t>>& rowsets) -> ResultType {
        bool found = false;
        int64_t first_op = 0;
        int64_t last_op = 0;
        for (const auto& rowset_ops : rowsets) {
            for (int64_t op : rowset_ops) {
                if (!found) {
                    first_op = op;
                    found = true;
                }
                last_op = op;
            }
        }
        return found ? AggregateFunctionMinDelta::calculate_result(first_op, last_op)
                     : ResultType::SKIP;
    };

    // Scenario 1: key1 updated in rowset-1 and updated again in rowset-2 -> UPDATE_BEFORE_AFTER.
    EXPECT_EQ(ResultType::UPDATE_BEFORE_AFTER,
              calc_from_rowsets({{ROW_BINLOG_UPDATE}, {ROW_BINLOG_UPDATE}}));

    // Scenario 2: key1 appended/updated in rowset-1, then updated in rowset-2 -> INSERT.
    EXPECT_EQ(ResultType::INSERT,
              calc_from_rowsets({{ROW_BINLOG_APPEND, ROW_BINLOG_UPDATE}, {ROW_BINLOG_UPDATE}}));

    // Scenario 3: key1 appended in one rowset and deleted in a later rowset -> SKIP.
    EXPECT_EQ(ResultType::SKIP, calc_from_rowsets({{ROW_BINLOG_APPEND}, {ROW_BINLOG_DELETE}}));

    // Scenario 4: key1 deleted first, then appended in later rowset -> INSERT.
    EXPECT_EQ(ResultType::INSERT, calc_from_rowsets({{ROW_BINLOG_DELETE}, {ROW_BINLOG_APPEND}}));

    // Scenario 5: empty rowsets around the same key should not affect folding.
    EXPECT_EQ(ResultType::DELETE,
              calc_from_rowsets({{}, {ROW_BINLOG_UPDATE}, {}, {ROW_BINLOG_DELETE}, {}}));
}

TEST_F(BlockReaderMinDeltaTest, RowBinlogOperationCodeLayoutGuard) {
    // The implementation uses op codes as 2D lookup indices, so guard the op layout to prevent implicit OOB.
    EXPECT_EQ(0, ROW_BINLOG_APPEND);
    EXPECT_EQ(1, ROW_BINLOG_UPDATE);
    EXPECT_EQ(2, ROW_BINLOG_DELETE);
}

} // namespace doris
