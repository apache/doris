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

#include "common/object_pool.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
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

// Regression test for the _read_row_rank Defer bug.
//
// Setup:
//   - ASCENDING order, partition_inner_limit = 1 (i.e. WHERE dr = 1)
//   - dense_rank=1 group has 5000 rows of value 10 (>> MockRuntimeState
//     batch_size = 4096)
//   - dense_rank=2 group has 3 rows of value 20
//
// Expected (correct behavior):
//   - The full dense_rank=1 group (all 5000 rows of value=10) must be output,
//     spread across multiple get_next() calls. value=20 must NOT appear.
//
// Buggy behavior (before fix):
//   - First get_next() outputs ~4096 rows (= batch_size) and the Defer block
//     sets *eos=true because _get_enough_data() turned true after the first
//     row (since _output_distinct_rows = 1 = partition_inner_limit). The
//     caller (PartitionSortSourceOperator) then advances to the next sorter,
//     silently dropping the remaining ~904 rows of value=10.
//
// This test fails on cdw-doris-4.1.1-0731a76 baseline and passes after the
// fix in partition_sorter.cpp:_read_row_rank.
TEST_F(PartitionSorterTest, test_dense_rank_first_group_exceeds_batch_size_regression) {
    SortCursorCmp previous_row;

    sorter = PartitionSorter::create_unique(ordering_expr_ctxs, -1, 0, &pool, is_asc_order,
                                            nulls_first, *row_desc, &_state, nullptr,
                                            /*has_global_limit=*/false,
                                            /*partition_inner_limit=*/1,
                                            TopNAlgorithm::DENSE_RANK, &previous_row);
    sorter->init_profile(&_profile);

    constexpr int kDenseRank1Rows = 5000;
    constexpr int kDenseRank2Rows = 3;
    constexpr int64_t kSmallValue = 10;
    constexpr int64_t kLargerValue = 20;

    // Append 5000 rows of value=10 (split across two blocks to simulate
    // multi-cursor merging).
    {
        std::vector<int64_t> chunk(2500, kSmallValue);
        Block block = ColumnHelper::create_block<DataTypeInt64>(chunk);
        EXPECT_TRUE(sorter->append_block(&block).ok());
    }
    {
        std::vector<int64_t> chunk(2500, kSmallValue);
        Block block = ColumnHelper::create_block<DataTypeInt64>(chunk);
        EXPECT_TRUE(sorter->append_block(&block).ok());
    }
    // Append 3 rows of value=20 (dense_rank=2 group).
    {
        std::vector<int64_t> chunk(kDenseRank2Rows, kLargerValue);
        Block block = ColumnHelper::create_block<DataTypeInt64>(chunk);
        EXPECT_TRUE(sorter->append_block(&block).ok());
    }

    {
        auto st = sorter->prepare_for_read(false);
        EXPECT_TRUE(st.ok()) << st.msg();
    }

    // Pull rows in batches of batch_size, just like the source operator.
    int total_rows = 0;
    int num_value10 = 0;
    int num_value20 = 0;
    int num_get_next_calls = 0;
    bool eos = false;
    while (!eos) {
        Block out_block;
        EXPECT_TRUE(sorter->get_next(&_state, &out_block, &eos).ok());
        num_get_next_calls++;
        int rows = static_cast<int>(out_block.rows());
        total_rows += rows;
        if (rows > 0) {
            const auto& col =
                    assert_cast<const ColumnInt64&>(*out_block.get_columns()[0]);
            for (int i = 0; i < rows; ++i) {
                int64_t v = col.get_data()[i];
                if (v == kSmallValue) {
                    num_value10++;
                } else if (v == kLargerValue) {
                    num_value20++;
                }
            }
        }

        // Safety: avoid infinite loop in case of regression.
        if (num_get_next_calls > 100) {
            FAIL() << "Too many get_next calls (" << num_get_next_calls
                   << "); aborting to avoid infinite loop.";
            break;
        }
    }

    // Expectation: the entire dense_rank=1 group must be output.
    EXPECT_EQ(num_value10, kDenseRank1Rows)
            << "Lost rows from dense_rank=1 group. Expected " << kDenseRank1Rows
            << " rows of value=" << kSmallValue << ", got " << num_value10
            << ". This indicates the _read_row_rank Defer bug.";
    // dense_rank=2 (value=20) must NOT be output since partition_inner_limit=1.
    EXPECT_EQ(num_value20, 0)
            << "value=20 (dense_rank=2) leaked into output despite partition_inner_limit=1";
    // We must have made multiple get_next calls to drain the large group.
    EXPECT_GT(num_get_next_calls, 1)
            << "Only one get_next call was made; the source would have stopped here "
            << "and dropped the rest of the dense_rank=1 group.";

    sorter->reset_sorter_state(&_state);
}

// Same regression for RANK (which shares the _read_row_rank code path).
TEST_F(PartitionSorterTest, test_rank_first_group_exceeds_batch_size_regression) {
    SortCursorCmp previous_row;

    sorter = PartitionSorter::create_unique(ordering_expr_ctxs, -1, 0, &pool, is_asc_order,
                                            nulls_first, *row_desc, &_state, nullptr,
                                            /*has_global_limit=*/false,
                                            /*partition_inner_limit=*/1,
                                            TopNAlgorithm::RANK, &previous_row);
    sorter->init_profile(&_profile);

    constexpr int kRank1Rows = 5000;
    constexpr int kRank2Rows = 3;
    constexpr int64_t kSmallValue = 10;
    constexpr int64_t kLargerValue = 20;

    {
        std::vector<int64_t> chunk(2500, kSmallValue);
        Block block = ColumnHelper::create_block<DataTypeInt64>(chunk);
        EXPECT_TRUE(sorter->append_block(&block).ok());
    }
    {
        std::vector<int64_t> chunk(2500, kSmallValue);
        Block block = ColumnHelper::create_block<DataTypeInt64>(chunk);
        EXPECT_TRUE(sorter->append_block(&block).ok());
    }
    {
        std::vector<int64_t> chunk(kRank2Rows, kLargerValue);
        Block block = ColumnHelper::create_block<DataTypeInt64>(chunk);
        EXPECT_TRUE(sorter->append_block(&block).ok());
    }

    {
        auto st = sorter->prepare_for_read(false);
        EXPECT_TRUE(st.ok()) << st.msg();
    }

    int num_value10 = 0;
    int num_value20 = 0;
    int num_get_next_calls = 0;
    bool eos = false;
    while (!eos) {
        Block out_block;
        EXPECT_TRUE(sorter->get_next(&_state, &out_block, &eos).ok());
        num_get_next_calls++;
        int rows = static_cast<int>(out_block.rows());
        if (rows > 0) {
            const auto& col =
                    assert_cast<const ColumnInt64&>(*out_block.get_columns()[0]);
            for (int i = 0; i < rows; ++i) {
                int64_t v = col.get_data()[i];
                if (v == kSmallValue) {
                    num_value10++;
                } else if (v == kLargerValue) {
                    num_value20++;
                }
            }
        }
        if (num_get_next_calls > 100) {
            FAIL() << "Too many get_next calls; aborting";
            break;
        }
    }

    EXPECT_EQ(num_value10, kRank1Rows);
    EXPECT_EQ(num_value20, 0);
    EXPECT_GT(num_get_next_calls, 1);

    sorter->reset_sorter_state(&_state);
}

} // namespace doris