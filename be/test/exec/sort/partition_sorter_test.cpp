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

#include "exec/sort/partition_sorter.h"

#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/types.pb.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <random>
#include <utility>
#include <vector>

#include "common/object_pool.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "exec/common/partition_sort_utils.h"
#include "exec/sort/heap_sorter.h"
#include "exec/sort/sorter.h"
#include "exec/sort/topn_sorter.h"
#include "exprs/vexpr_fwd.h"
#include "runtime/runtime_state.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"

namespace doris {

struct PartitionSorterTest : public testing::Test {
    void SetUp() override {
        row_desc.reset(new MockRowDescriptor({std::make_shared<DataTypeInt64>()}, &pool));

        ordering_expr_ctxs =
                MockSlotRef::create_mock_contexts(0, std::make_shared<DataTypeInt64>());
    }
    MockRuntimeState _state;
    RuntimeProfile _profile {"test"};

    std::unique_ptr<PartitionSorter> sorter;

    std::unique_ptr<MockRowDescriptor> row_desc;

    ObjectPool pool;

    VExprContextSPtrs ordering_expr_ctxs;

    std::vector<bool> is_asc_order {true};
    std::vector<bool> nulls_first {false};
};

TEST_F(PartitionSorterTest, test_partition_sorter_read_row_num) {
    sorter = PartitionSorter::create_unique(ordering_expr_ctxs, -1, 0, &pool, is_asc_order,
                                            nulls_first, *row_desc, &_state, nullptr, false, 20,
                                            TopNAlgorithm::ROW_NUMBER, nullptr);
    sorter->init_profile(&_profile);
    {
        Block block = ColumnHelper::create_block<DataTypeInt64>({10, 9, 8, 7, 6, 5, 4, 3, 2, 1});
        EXPECT_TRUE(sorter->append_block(&block).ok());
    }

    {
        Block block = ColumnHelper::create_block<DataTypeInt64>({4, 5, 6, 7});
        EXPECT_TRUE(sorter->append_block(&block).ok());
    }

    {
        Block block = ColumnHelper::create_block<DataTypeInt64>({100, 111});
        EXPECT_TRUE(sorter->append_block(&block).ok());
    }

    {
        auto st = sorter->prepare_for_read(false);
        EXPECT_TRUE(st.ok()) << st.msg();
    }
    {
        bool eos = false;
        Block block;
        EXPECT_TRUE(sorter->get_next(&_state, &block, &eos).ok());
        std::cout << block.dump_data() << std::endl;
        EXPECT_TRUE(ColumnHelper::block_equal(
                block, ColumnHelper::create_block<DataTypeInt64>(
                               {1, 2, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 9, 10})));
    }

    {
        bool eos = false;
        Block block;
        EXPECT_TRUE(sorter->get_next(&_state, &block, &eos).ok());
        std::cout << block.dump_data() << std::endl;
        EXPECT_TRUE(ColumnHelper::block_equal(
                block, ColumnHelper::create_block<DataTypeInt64>({100, 111})));
    }
}

TEST_F(PartitionSorterTest, test_partition_sorter_DENSE_RANK) {
    SortCursorCmp previous_row;

    sorter = PartitionSorter::create_unique(ordering_expr_ctxs, -1, 0, &pool, is_asc_order,
                                            nulls_first, *row_desc, &_state, nullptr, false, 20,
                                            TopNAlgorithm::DENSE_RANK, &previous_row);
    sorter->init_profile(&_profile);
    {
        Block block = ColumnHelper::create_block<DataTypeInt64>({10, 9, 8, 7, 6, 5, 4, 3, 2, 1});
        EXPECT_TRUE(sorter->append_block(&block).ok());
    }

    {
        Block block = ColumnHelper::create_block<DataTypeInt64>({4, 5, 6, 7});
        EXPECT_TRUE(sorter->append_block(&block).ok());
    }

    {
        Block block = ColumnHelper::create_block<DataTypeInt64>({100, 111});
        EXPECT_TRUE(sorter->append_block(&block).ok());
    }

    {
        auto st = sorter->prepare_for_read(false);
        EXPECT_TRUE(st.ok()) << st.msg();
    }
    {
        bool eos = false;
        Block block;
        EXPECT_TRUE(sorter->get_next(&_state, &block, &eos).ok());
        std::cout << block.dump_data() << std::endl;
        EXPECT_TRUE(ColumnHelper::block_equal(
                block, ColumnHelper::create_block<DataTypeInt64>(
                               {1, 2, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 9, 10, 100, 111})));
    }

    sorter->reset_sorter_state(&_state);
}

TEST_F(PartitionSorterTest, test_partition_sorter_RANK) {
    SortCursorCmp previous_row;

    sorter = PartitionSorter::create_unique(ordering_expr_ctxs, -1, 0, &pool, is_asc_order,
                                            nulls_first, *row_desc, &_state, nullptr, false, 20,
                                            TopNAlgorithm::RANK, &previous_row);
    sorter->init_profile(&_profile);
    {
        Block block = ColumnHelper::create_block<DataTypeInt64>({10, 9, 8, 7, 6, 5, 4, 3, 2, 1});
        EXPECT_TRUE(sorter->append_block(&block).ok());
    }

    {
        Block block = ColumnHelper::create_block<DataTypeInt64>({4, 5, 6, 7});
        EXPECT_TRUE(sorter->append_block(&block).ok());
    }

    {
        Block block = ColumnHelper::create_block<DataTypeInt64>({100, 111});
        EXPECT_TRUE(sorter->append_block(&block).ok());
    }

    {
        auto st = sorter->prepare_for_read(false);
        EXPECT_TRUE(st.ok()) << st.msg();
    }
    {
        bool eos = false;
        Block block;
        EXPECT_TRUE(sorter->get_next(&_state, &block, &eos).ok());
        std::cout << block.dump_data() << std::endl;
        EXPECT_TRUE(ColumnHelper::block_equal(
                block, ColumnHelper::create_block<DataTypeInt64>(
                               {1, 2, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 9, 10, 100, 111})));
    }

    sorter->reset_sorter_state(&_state);
}

struct PartitionSorterRankTest : PartitionSorterTest,
                                 testing::WithParamInterface<TopNAlgorithm::type> {
    std::unique_ptr<PartitionBlocks> create_partition_blocks() {
        _state._batch_size = PARTITION_SORT_ROWS_THRESHOLD;
        auto sort_info = std::make_shared<PartitionSortInfo>(
                &ordering_expr_ctxs, -1, 0, &pool, is_asc_order, nulls_first, *row_desc, &_state,
                &_profile, false, 1, GetParam(), TPartTopNPhase::TWO_PHASE_LOCAL);
        return std::make_unique<PartitionBlocks>(std::move(sort_info), true);
    }

    void append_rows(PartitionBlocks& partition, const std::vector<int64_t>& values) {
        auto block = ColumnHelper::create_block<DataTypeInt64>(values);
        for (size_t i = 0; i < values.size(); ++i) {
            partition.add_row_idx(i);
        }
        ASSERT_TRUE(partition.append_block_by_selector(&block, false).ok());
    }

    void check_retained_rows(const PartitionBlocks& partition, size_t rows, int64_t value) {
        size_t retained_rows = 0;
        for (const auto& block : partition._blocks) {
            retained_rows += block->rows();
            EXPECT_TRUE(ColumnHelper::block_equal(
                    *block, ColumnHelper::create_block<DataTypeInt64>(
                                    std::vector<int64_t>(block->rows(), value))));
        }
        EXPECT_EQ(retained_rows, rows);
    }

    void check_output(int64_t limit, const std::vector<std::vector<int64_t>>& inputs,
                      const std::vector<int64_t>& expected) {
        _state._batch_size = 4;
        SortCursorCmp previous_row;
        auto rank_sorter = PartitionSorter::create_unique(
                ordering_expr_ctxs, -1, 0, &pool, is_asc_order, nulls_first, *row_desc, &_state,
                nullptr, false, limit, GetParam(), &previous_row);
        rank_sorter->init_profile(&_profile);
        for (const auto& values : inputs) {
            auto block = ColumnHelper::create_block<DataTypeInt64>(values);
            ASSERT_TRUE(rank_sorter->append_block(&block).ok());
        }
        ASSERT_TRUE(rank_sorter->prepare_for_read(false).ok());

        bool eos = false;
        size_t output_rows = 0;
        Block block;
        // Allow one final empty batch when the next peer group starts at a batch boundary.
        for (size_t batch = 0; !eos && batch <= expected.size() / _state.batch_size() + 1;
             ++batch) {
            block.clear_column_data();
            ASSERT_TRUE(rank_sorter->get_next(&_state, &block, &eos).ok());
            const auto rows = std::min<size_t>(_state.batch_size(), expected.size() - output_rows);
            ASSERT_EQ(block.rows(), rows);
            if (rows > 0) {
                const std::vector<int64_t> expected_batch(expected.begin() + output_rows,
                                                          expected.begin() + output_rows + rows);
                EXPECT_TRUE(ColumnHelper::block_equal(
                        block, ColumnHelper::create_block<DataTypeInt64>(expected_batch)));
            }
            output_rows += rows;
            if (output_rows < expected.size()) {
                ASSERT_FALSE(eos);
            }
        }
        EXPECT_TRUE(eos);
        EXPECT_EQ(output_rows, expected.size());
    }
};

TEST_P(PartitionSorterRankTest, BoundaryPeersAcrossBatches) {
    for (int peer_rows : {3, 4, 5, 9}) {
        for (bool has_next_group : {false, true}) {
            SCOPED_TRACE(testing::Message()
                         << "peer_rows=" << peer_rows << ", has_next_group=" << has_next_group);
            // Split the peer group between input blocks to also exercise merge cursor changes.
            std::vector<std::vector<int64_t>> inputs {{0}, std::vector<int64_t>(peer_rows - 1, 0)};
            if (has_next_group) {
                inputs.front().push_back(1);
            }
            check_output(1, inputs, std::vector<int64_t>(peer_rows, 0));
        }
    }
}

TEST_P(PartitionSorterRankTest, RankLimitBeyondFirstGroup) {
    const std::vector<std::vector<int64_t>> inputs {{0, 1, 1, 2}, {0, 1, 1, 1, 2}};
    check_output(2, inputs,
                 GetParam() == TopNAlgorithm::RANK ? std::vector<int64_t> {0, 0}
                                                   : std::vector<int64_t> {0, 0, 1, 1, 1, 1, 1});
    check_output(GetParam() == TopNAlgorithm::RANK ? 3 : 2, inputs, {0, 0, 1, 1, 1, 1, 1});
}

TEST_P(PartitionSorterRankTest, ShortBoundaryGroupAcrossBatches) {
    // The boundary group can span batches even when it is smaller than a batch.
    check_output(GetParam() == TopNAlgorithm::RANK ? 4 : 2, {{0, 0, 1}, {0, 1, 2}},
                 {0, 0, 0, 1, 1});
}

TEST_P(PartitionSorterRankTest, ExhaustInputBelowLimit) {
    check_output(10, {{0, 1, 2}, {0, 1}}, {0, 0, 1, 1, 2});
    check_output(10, {}, {});
}

TEST_P(PartitionSorterRankTest, ResetStartsNewPeerGroup) {
    _state._batch_size = 4;
    SortCursorCmp previous_row;
    auto rank_sorter = PartitionSorter::create_unique(ordering_expr_ctxs, -1, 0, &pool,
                                                      is_asc_order, nulls_first, *row_desc, &_state,
                                                      nullptr, false, 1, GetParam(), &previous_row);
    rank_sorter->init_profile(&_profile);
    for (int pass = 0; pass < 2; ++pass) {
        SCOPED_TRACE(pass);
        auto peers = ColumnHelper::create_block<DataTypeInt64>(std::vector<int64_t>(5, 0));
        auto next_group = ColumnHelper::create_block<DataTypeInt64>(std::vector<int64_t>(9, 1));
        ASSERT_TRUE(rank_sorter->append_block(&peers).ok());
        ASSERT_TRUE(rank_sorter->append_block(&next_group).ok());
        ASSERT_TRUE(rank_sorter->prepare_for_read(false).ok());

        bool eos = false;
        Block output;
        ASSERT_TRUE(rank_sorter->get_next(&_state, &output, &eos).ok());
        EXPECT_TRUE(ColumnHelper::block_equal(
                output, ColumnHelper::create_block<DataTypeInt64>({0, 0, 0, 0})));
        ASSERT_FALSE(eos);
        output.clear_column_data();
        ASSERT_TRUE(rank_sorter->get_next(&_state, &output, &eos).ok());
        EXPECT_TRUE(
                ColumnHelper::block_equal(output, ColumnHelper::create_block<DataTypeInt64>({0})));
        EXPECT_TRUE(eos);
        rank_sorter->reset_sorter_state(&_state);
    }
}

TEST_P(PartitionSorterRankTest, IntermediatePruningAmortizesRetainedPeers) {
    auto partition = create_partition_blocks();
    const size_t input_batches = 16;
    const size_t batch_rows = PARTITION_SORT_ROWS_THRESHOLD;
    size_t pruning_passes = 0;
    size_t processed_rows = 0;
    for (size_t batch = 1; batch <= input_batches; ++batch) {
        ASSERT_NO_FATAL_FAILURE(append_rows(*partition, std::vector<int64_t>(batch_rows, 0)));
        if (partition->_current_input_rows == 0) {
            ++pruning_passes;
            processed_rows += batch * batch_rows;
        }
    }
    // With no rows pruned, only the prefixes of 1, 2, 4, 8 and 16 batches are processed.
    EXPECT_EQ(pruning_passes, 5);
    EXPECT_LT(processed_rows, 2 * input_batches * batch_rows);
    check_retained_rows(*partition, input_batches * batch_rows, 0);
}

TEST_P(PartitionSorterRankTest, IntermediatePruningResumesAfterPeersShrink) {
    auto partition = create_partition_blocks();
    const size_t batch_rows = PARTITION_SORT_ROWS_THRESHOLD;
    for (size_t batch = 0; batch < 4; ++batch) {
        ASSERT_NO_FATAL_FAILURE(append_rows(*partition, std::vector<int64_t>(batch_rows, 0)));
    }
    check_retained_rows(*partition, 4 * batch_rows, 0);

    // A better key eliminates the retained peer group on the next pruning pass.
    auto values = std::vector<int64_t>(batch_rows, 0);
    values.front() = -1;
    ASSERT_NO_FATAL_FAILURE(append_rows(*partition, values));
    for (size_t batch = 1; batch < 4; ++batch) {
        ASSERT_NO_FATAL_FAILURE(append_rows(*partition, std::vector<int64_t>(batch_rows, 0)));
    }
    check_retained_rows(*partition, 1, -1);

    // Once few rows are retained, one base interval must trigger pruning again.
    ASSERT_NO_FATAL_FAILURE(append_rows(*partition, std::vector<int64_t>(batch_rows - 1, -2)));
    EXPECT_EQ(partition->_current_input_rows, batch_rows - 1);
    ASSERT_NO_FATAL_FAILURE(append_rows(*partition, {-2}));
    EXPECT_EQ(partition->_current_input_rows, 0);
    check_retained_rows(*partition, batch_rows, -2);
}

INSTANTIATE_TEST_SUITE_P(RankAlgorithms, PartitionSorterRankTest,
                         testing::Values(TopNAlgorithm::RANK, TopNAlgorithm::DENSE_RANK));

} // namespace doris
