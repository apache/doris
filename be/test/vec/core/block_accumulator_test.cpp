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

#include "vec/core/block_accumulator.h"

#include <gtest/gtest.h>

#include "testutil/column_helper.h"

namespace doris::vectorized {

static std::vector<int32_t> range_i32(int32_t start, int32_t end_inclusive) {
    std::vector<int32_t> v;
    v.reserve(end_inclusive - start + 1);
    for (int32_t i = start; i <= end_inclusive; ++i) v.emplace_back(i);
    return v;
}

TEST(BlockAccumulatorTest, BasicAccumulateAndFinish) {
    const size_t batch_size = 16;
    BlockAccumulator acc(batch_size);

    // Input: 10 rows then 20 rows -> expect outputs: 16 rows, then 14 rows (on finish)
    Block b1 = ColumnHelper::create_block<DataTypeInt32>(range_i32(1, 10));
    Block b2 = ColumnHelper::create_block<DataTypeInt32>(range_i32(11, 30));

    EXPECT_TRUE(acc.push(std::move(b1)).ok());
    EXPECT_TRUE(acc.push(std::move(b2)).ok());
    EXPECT_TRUE(acc.set_finish().ok());

    EXPECT_TRUE(acc.has_output());
    Block out1;
    EXPECT_TRUE(acc.pull(out1));
    Block expect1 = ColumnHelper::create_block<DataTypeInt32>(range_i32(1, 16));
    EXPECT_TRUE(ColumnHelper::block_equal(out1, expect1));

    EXPECT_TRUE(acc.has_output());
    Block out2;
    EXPECT_TRUE(acc.pull(out2));
    Block expect2 = ColumnHelper::create_block<DataTypeInt32>(range_i32(17, 30));
    EXPECT_TRUE(ColumnHelper::block_equal(out2, expect2));

    EXPECT_FALSE(acc.has_output());
    Block out3;
    EXPECT_FALSE(acc.pull(out3));
}

TEST(BlockAccumulatorTest, ExactMultipleOfBatch) {
    const size_t batch_size = 16;
    BlockAccumulator acc(batch_size);

    Block b1 = ColumnHelper::create_block<DataTypeInt32>(range_i32(1, 16));
    Block b2 = ColumnHelper::create_block<DataTypeInt32>(range_i32(17, 32));

    EXPECT_TRUE(acc.push(std::move(b1)).ok());
    EXPECT_TRUE(acc.push(std::move(b2)).ok());
    EXPECT_TRUE(acc.set_finish().ok());

    EXPECT_TRUE(acc.has_output());
    Block out1;
    EXPECT_TRUE(acc.pull(out1));
    Block expect1 = ColumnHelper::create_block<DataTypeInt32>(range_i32(1, 16));
    EXPECT_TRUE(ColumnHelper::block_equal(out1, expect1));

    EXPECT_TRUE(acc.has_output());
    Block out2;
    EXPECT_TRUE(acc.pull(out2));
    Block expect2 = ColumnHelper::create_block<DataTypeInt32>(range_i32(17, 32));
    EXPECT_TRUE(ColumnHelper::block_equal(out2, expect2));

    EXPECT_FALSE(acc.has_output());
}

TEST(BlockAccumulatorTest, CloseClearsState) {
    const size_t batch_size = 8;
    BlockAccumulator acc(batch_size);

    Block b = ColumnHelper::create_block<DataTypeInt32>(range_i32(1, 8));
    EXPECT_TRUE(acc.push(std::move(b)).ok());
    EXPECT_TRUE(acc.set_finish().ok());
    EXPECT_TRUE(acc.has_output());

    acc.close();
    EXPECT_FALSE(acc.has_output());
    Block out;
    EXPECT_FALSE(acc.pull(out));
}

TEST(BlockAccumulatorTest, Test) {
    const size_t batch_size = 8;
    BlockAccumulator acc(batch_size);

    Block b = ColumnHelper::create_block<DataTypeInt32>(range_i32(1, 4));

    EXPECT_TRUE(acc.push(std::move(b)).ok());
    EXPECT_FALSE(acc.has_output());

    Block b2 = ColumnHelper::create_block<DataTypeInt32>(range_i32(5, 10));
    EXPECT_TRUE(acc.push(std::move(b2)).ok());
    EXPECT_TRUE(acc.has_output());

    Block out1;
    EXPECT_TRUE(acc.pull(out1));
    Block expect1 = ColumnHelper::create_block<DataTypeInt32>(range_i32(1, 8));
    EXPECT_TRUE(ColumnHelper::block_equal(out1, expect1));

    EXPECT_FALSE(acc.has_output());

    EXPECT_TRUE(acc.set_finish().ok());
    EXPECT_TRUE(acc.has_output());

    Block out2;
    EXPECT_TRUE(acc.pull(out2));
    Block expect2 = ColumnHelper::create_block<DataTypeInt32>(range_i32(9, 10));
    EXPECT_TRUE(ColumnHelper::block_equal(out2, expect2));

    EXPECT_FALSE(acc.has_output());
}

TEST(PipelineBlockAccumulatorTest, OutputOnThresholdThenFinalOnFinish) {
    const size_t batch_size = 16;
    PipelineBlockAccumulator acc(batch_size);

    // Push 10 rows: no output yet
    Block b1 = ColumnHelper::create_block<DataTypeInt32>(range_i32(1, 10));
    EXPECT_TRUE(acc.push(std::move(b1)).ok());
    EXPECT_FALSE(acc.has_output());
    EXPECT_TRUE(acc.need_input());

    // Push 20 rows: rows(10) + rows(20) >= 16 -> output first block (10 rows)
    Block b2 = ColumnHelper::create_block<DataTypeInt32>(range_i32(11, 30));
    EXPECT_TRUE(acc.push(std::move(b2)).ok());
    EXPECT_TRUE(acc.has_output());

    auto out1 = acc.pull();
    EXPECT_NE(out1, nullptr);
    Block expect_first = ColumnHelper::create_block<DataTypeInt32>(range_i32(1, 10));
    EXPECT_TRUE(ColumnHelper::block_equal(*out1, expect_first));

    // After consuming output, should need input if not finished
    EXPECT_TRUE(acc.need_input());

    // Finish: should have final output (the second block of 20 rows)
    acc.set_finish();
    EXPECT_TRUE(acc.has_output());
    auto final_out = acc.pull();
    EXPECT_NE(final_out, nullptr);
    Block expect_final = ColumnHelper::create_block<DataTypeInt32>(range_i32(11, 30));
    EXPECT_TRUE(ColumnHelper::block_equal(*final_out, expect_final));
}

TEST(PipelineBlockAccumulatorTest, MergeBelowThresholdThenFinish) {
    const size_t batch_size = 16;
    PipelineBlockAccumulator acc(batch_size);

    // Two small blocks totaling below threshold: should merge, no immediate output
    Block b1 = ColumnHelper::create_block<DataTypeInt32>(range_i32(1, 7));
    Block b2 = ColumnHelper::create_block<DataTypeInt32>(range_i32(8, 12)); // total 12
    EXPECT_TRUE(acc.push(std::move(b1)).ok());
    EXPECT_FALSE(acc.has_output());
    EXPECT_TRUE(acc.push(std::move(b2)).ok());
    EXPECT_FALSE(acc.has_output());

    // Finish: one final merged output with 12 rows
    acc.set_finish();
    EXPECT_TRUE(acc.has_output());
    auto out = acc.pull();
    EXPECT_NE(out, nullptr);
    Block expect = ColumnHelper::create_block<DataTypeInt32>(range_i32(1, 12));
    EXPECT_TRUE(ColumnHelper::block_equal(*out, expect));
}

TEST(PipelineBlockAccumulatorTest, pushWithSelector) {
    const size_t batch_size = 8;
    PipelineBlockAccumulator acc(batch_size);

    Block b = ColumnHelper::create_block<DataTypeInt32>(range_i32(1, 10));
    // Selector to pick rows 0, 2, 4, 6,
    IColumn::Selector selector;
    selector.push_back(0);
    selector.push_back(2);
    selector.push_back(4);
    selector.push_back(6);

    EXPECT_TRUE(acc.push_with_selector(std::move(b), selector).ok());
    EXPECT_FALSE(acc.has_output());

    acc.set_finish();
    EXPECT_TRUE(acc.has_output());

    auto out = acc.pull();
    EXPECT_NE(out, nullptr);
    Block expect = ColumnHelper::create_block<DataTypeInt32>({1, 3, 5, 7});

    EXPECT_TRUE(ColumnHelper::block_equal(*out, expect));
}

TEST(PipelineBlockAccumulatorTest, error) {
    const size_t batch_size = 8;
    PipelineBlockAccumulator acc(batch_size);

    acc.set_finish();

    Block b = ColumnHelper::create_block<DataTypeInt32>(range_i32(1, 10));

    EXPECT_FALSE(acc.push(std::move(b)).ok());

    EXPECT_FALSE(acc.push_with_selector(std::move(b), IColumn::Selector {}).ok());
}

} // namespace doris::vectorized
