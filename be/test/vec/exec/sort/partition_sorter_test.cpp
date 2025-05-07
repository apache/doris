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

#include "vec/common/sort/partition_sorter.h"

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
#include "runtime/runtime_state.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/sort/heap_sorter.h"
#include "vec/common/sort/sorter.h"
#include "vec/common/sort/topn_sorter.h"
#include "vec/common/sort/vsort_exec_exprs.h"
#include "vec/core/block.h"

namespace doris::vectorized {

struct PartitionSorterTest : public testing::Test {
    void SetUp() override {
        row_desc.reset(new MockRowDescriptor({std::make_shared<DataTypeInt64>()}, &pool));

        sort_exec_exprs._sort_tuple_slot_expr_ctxs =
                MockSlotRef::create_mock_contexts(0, std::make_shared<DataTypeInt64>());

        sort_exec_exprs._materialize_tuple = false;

        sort_exec_exprs._ordering_expr_ctxs =
                MockSlotRef::create_mock_contexts(0, std::make_shared<DataTypeInt64>());

        sort_exec_exprs._sort_tuple_slot_expr_ctxs =
                MockSlotRef::create_mock_contexts(0, std::make_shared<DataTypeInt64>());
    }
    MockRuntimeState _state;
    RuntimeProfile _profile {"test"};

    std::unique_ptr<PartitionSorter> sorter;

    std::unique_ptr<MockRowDescriptor> row_desc;

    ObjectPool pool;

    VSortExecExprs sort_exec_exprs;

    std::vector<bool> is_asc_order {true};
    std::vector<bool> nulls_first {false};
};

TEST_F(PartitionSorterTest, test_partition_sorter_read_row_num) {
    sorter = PartitionSorter::create_unique(sort_exec_exprs, -1, 0, &pool, is_asc_order,
                                            nulls_first, *row_desc, nullptr, nullptr, false, 20,
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
        auto st = sorter->prepare_for_read();
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

    sorter = PartitionSorter::create_unique(sort_exec_exprs, -1, 0, &pool, is_asc_order,
                                            nulls_first, *row_desc, nullptr, nullptr, false, 20,
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
        auto st = sorter->prepare_for_read();
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

    sorter = PartitionSorter::create_unique(sort_exec_exprs, -1, 0, &pool, is_asc_order,
                                            nulls_first, *row_desc, nullptr, nullptr, false, 20,
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
        auto st = sorter->prepare_for_read();
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

} // namespace doris::vectorized