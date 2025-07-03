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

#include "pipeline/exec/distinct_streaming_aggregation_operator.h"

#include <gtest/gtest.h>

#include <memory>

#include "pipeline/exec/mock_operator.h"
#include "pipeline/operator/operator_helper.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_slot_ref.h"
#include "vec/core/block.h"
namespace doris::pipeline {

using namespace vectorized;
struct DistinctStreamingAggOperatorTest : public ::testing::Test {
    void SetUp() override {
        op = std::make_unique<DistinctStreamingAggOperatorX>();
        mock_op = std::make_shared<MockOperatorX>();
        state = std::make_shared<MockRuntimeState>();
        state->batsh_size = 10;
        op->_child = mock_op;
    }

    void create_op(DataTypes input_types, DataTypes output_types) {
        op->_probe_expr_ctxs = MockSlotRef::create_mock_contexts(input_types);

        op->_output_tuple_id = 0;
        output_desc_tbl = std::make_unique<MockDescriptorTbl>(output_types, &pool);
        state->set_desc_tbl(output_desc_tbl.get());

        op->init_make_nullable(state.get());

        create_local_state();
    }

    void create_local_state() {
        local_state_uptr = std::make_unique<DistinctStreamingAggLocalState>(state.get(), op.get());
        local_state = local_state_uptr.get();
        LocalStateInfo info {.parent_profile = &profile,
                             .scan_ranges = {},
                             .shared_state = nullptr,
                             .shared_state_map = {},
                             .task_idx = 0};
        EXPECT_TRUE(local_state->init(state.get(), info));
        state->resize_op_id_to_local_state(-100);
        state->emplace_local_state(op->operator_id(), std::move(local_state_uptr));
        EXPECT_TRUE(local_state->open(state.get()));
    }

    RuntimeProfile profile {"test"};
    std::unique_ptr<DistinctStreamingAggOperatorX> op;
    std::unique_ptr<MockDescriptorTbl> output_desc_tbl;
    std::shared_ptr<MockOperatorX> mock_op;

    std::unique_ptr<DistinctStreamingAggLocalState> local_state_uptr;

    DistinctStreamingAggLocalState* local_state;

    std::shared_ptr<MockRuntimeState> state;
    ObjectPool pool;
};

TEST_F(DistinctStreamingAggOperatorTest, test1) {
    op->_is_streaming_preagg = false;

    create_op({std::make_shared<DataTypeInt64>()}, {std::make_shared<DataTypeInt64>()});

    mock_op->_outout_blocks.push_back(
            ColumnHelper::create_block<DataTypeInt64>({1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4}));

    {
        bool eos = false;
        Block block;

        auto st = op->get_block(state.get(), &block, &eos);
        EXPECT_TRUE(st) << st.msg();
        EXPECT_TRUE(eos);
        EXPECT_TRUE(ColumnHelper::block_equal(
                block, ColumnHelper::create_block<DataTypeInt64>({1, 2, 3, 4})));
    }
}

TEST_F(DistinctStreamingAggOperatorTest, test2) {
    op->_is_streaming_preagg = false;
    op->_limit = 3;
    create_op({std::make_shared<DataTypeInt64>()}, {std::make_shared<DataTypeInt64>()});

    mock_op->_outout_blocks.push_back(
            ColumnHelper::create_block<DataTypeInt64>({1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4}));

    {
        bool eos = false;
        Block block;

        auto st = op->get_block(state.get(), &block, &eos);
        EXPECT_TRUE(st) << st.msg();
        EXPECT_TRUE(eos);
        EXPECT_TRUE(ColumnHelper::block_equal(
                block, ColumnHelper::create_block<DataTypeInt64>({1, 2, 3})));
    }
}

TEST_F(DistinctStreamingAggOperatorTest, test3) {
    // batch size  = 10
    op->_is_streaming_preagg = true;

    create_op({std::make_shared<DataTypeInt64>()}, {std::make_shared<DataTypeInt64>()});

    {
        auto block =
                ColumnHelper::create_block<DataTypeInt64>({1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4});
        EXPECT_TRUE(op->push(state.get(), &block, false));
        EXPECT_EQ(local_state->_cache_block.rows(), 0);
        EXPECT_EQ(local_state->_aggregated_block->rows(), 4);
        EXPECT_TRUE(op->need_more_input_data(state.get()));
    }

    {
        auto block = ColumnHelper::create_block<DataTypeInt64>({5, 6, 7, 8});
        EXPECT_TRUE(op->push(state.get(), &block, false));
        EXPECT_EQ(local_state->_cache_block.rows(), 0);
        EXPECT_EQ(local_state->_aggregated_block->rows(), 8);
        EXPECT_TRUE(op->need_more_input_data(state.get()));
    }

    {
        auto block = ColumnHelper::create_block<DataTypeInt64>({9, 10, 11, 12});
        EXPECT_TRUE(op->push(state.get(), &block, false));
        EXPECT_EQ(local_state->_cache_block.rows(), 2);
        EXPECT_EQ(local_state->_aggregated_block->rows(), 10);
        EXPECT_FALSE(op->need_more_input_data(state.get()));
    }

    {
        Block block;
        bool eos = false;
        EXPECT_TRUE(op->pull(state.get(), &block, &eos));
        EXPECT_FALSE(eos);
        EXPECT_EQ(local_state->_cache_block.rows(), 0);
        EXPECT_EQ(local_state->_aggregated_block->rows(), 2);
    }
    {
        local_state->_stop_emplace_flag = true;
        auto block = ColumnHelper::create_block<DataTypeInt64>({13, 14, 15});
        EXPECT_TRUE(op->push(state.get(), &block, false));
        EXPECT_EQ(local_state->_cache_block.rows(), 0);
        EXPECT_EQ(local_state->_aggregated_block->rows(), 5);
        EXPECT_FALSE(op->need_more_input_data(state.get()));
    }
    {
        Block block;
        bool eos = false;
        EXPECT_TRUE(op->pull(state.get(), &block, &eos));
        EXPECT_FALSE(eos);
        EXPECT_EQ(block.rows(), 5);
        EXPECT_EQ(local_state->_cache_block.rows(), 0);
        EXPECT_EQ(local_state->_aggregated_block->rows(), 0);
    }
    {
        EXPECT_TRUE(op->need_more_input_data(state.get()));
        local_state->_stop_emplace_flag = true;
        auto block = ColumnHelper::create_block<DataTypeInt64>({13, 14, 15});
        EXPECT_TRUE(op->push(state.get(), &block, false));
        EXPECT_EQ(local_state->_cache_block.rows(), 0);
        EXPECT_EQ(local_state->_aggregated_block->rows(), 3);
        EXPECT_FALSE(op->need_more_input_data(state.get()));
    }
    {
        Block block;
        bool eos = false;
        EXPECT_TRUE(op->pull(state.get(), &block, &eos));
        EXPECT_FALSE(eos);
        EXPECT_EQ(block.rows(), 3);
        EXPECT_EQ(local_state->_cache_block.rows(), 0);
        EXPECT_EQ(local_state->_aggregated_block->rows(), 0);
    }
    { EXPECT_TRUE(op->close(state.get())); }
}

} // namespace doris::pipeline
