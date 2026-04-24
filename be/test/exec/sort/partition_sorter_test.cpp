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

#include "common/config.h"
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

TEST_F(PartitionSorterTest, test_partition_sorter_ROW_NUMBER_block_max_bytes) {
    // Test that block_max_bytes limits output block size for ROW_NUMBER.
    // With 16 rows of Int64 (8 bytes each), total ~128 bytes of column data.
    // Set block_max_bytes small so the output is split into multiple blocks.
    auto saved_adaptive = config::enable_adaptive_batch_size;
    config::enable_adaptive_batch_size = true;
    _state._query_options.__set_batch_size(1000);
    _state._query_options.__set_preferred_block_size_bytes(50);

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

    size_t total_rows = 0;
    int num_blocks = 0;
    bool eos = false;
    while (!eos) {
        Block block;
        EXPECT_TRUE(sorter->get_next(&_state, &block, &eos).ok());
        if (block.rows() > 0) {
            total_rows += block.rows();
            num_blocks++;
        }
    }
    // partition_inner_limit=20, but only 16 rows total
    EXPECT_EQ(total_rows, 16);
    EXPECT_GT(num_blocks, 1) << "block_max_bytes should cause multiple output blocks";

    config::enable_adaptive_batch_size = saved_adaptive;
}

TEST_F(PartitionSorterTest, test_partition_sorter_RANK_block_max_bytes) {
    // Test that block_max_bytes limits output for RANK algorithm.
    // The RANK path checks bytes every 256 rows, so we need > 256 rows to trigger.
    auto saved_adaptive = config::enable_adaptive_batch_size;
    config::enable_adaptive_batch_size = true;
    _state._query_options.__set_batch_size(2000);
    _state._query_options.__set_preferred_block_size_bytes(50);

    SortCursorCmp previous_row;
    sorter = PartitionSorter::create_unique(ordering_expr_ctxs, -1, 0, &pool, is_asc_order,
                                            nulls_first, *row_desc, &_state, nullptr, false, 1000,
                                            TopNAlgorithm::RANK, &previous_row);
    sorter->init_profile(&_profile);

    // Add 600 distinct rows so bytes check at row 256 and 512 can trigger
    {
        std::vector<int64_t> values;
        for (int i = 0; i < 600; i++) {
            values.push_back(i);
        }
        Block block = ColumnHelper::create_block<DataTypeInt64>(values);
        EXPECT_TRUE(sorter->append_block(&block).ok());
    }
    {
        auto st = sorter->prepare_for_read(false);
        EXPECT_TRUE(st.ok()) << st.msg();
    }

    size_t total_rows = 0;
    int num_blocks = 0;
    bool eos = false;
    while (!eos) {
        Block block;
        EXPECT_TRUE(sorter->get_next(&_state, &block, &eos).ok());
        if (block.rows() > 0) {
            total_rows += block.rows();
            num_blocks++;
        }
    }
    EXPECT_EQ(total_rows, 600);
    EXPECT_GT(num_blocks, 1) << "block_max_bytes should cause multiple output blocks in RANK path";

    config::enable_adaptive_batch_size = saved_adaptive;
    sorter->reset_sorter_state(&_state);
}

} // namespace doris