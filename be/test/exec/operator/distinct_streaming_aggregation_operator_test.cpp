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

#include "exec/operator/distinct_streaming_aggregation_operator.h"

#include <gtest/gtest.h>

#include <memory>

#include "core/block/block.h"
#include "exec/exchange/local_exchange_source_operator.h"
#include "exec/operator/mock_operator.h"
#include "exec/operator/operator_helper.h"
#include "runtime/workload_management/memory_context.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_slot_ref.h"
namespace doris {

struct DistinctStreamingAggOperatorTest : public ::testing::Test {
    void SetUp() override {
        op = std::make_unique<DistinctStreamingAggOperatorX>();
        mock_op = std::make_shared<MockOperatorX>();
        state = std::make_shared<MockRuntimeState>();
        state->_batch_size = 10;
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

TEST_F(DistinctStreamingAggOperatorTest, require_hash_shuffle_after_non_hash_local_exchange) {
    state->_query_options.__set_enable_local_exchange_before_agg(false);
    op->_is_streaming_preagg = false;
    op->_partition_exprs.emplace_back();
    op->_probe_expr_ctxs = MockSlotRef::create_mock_contexts(0, std::make_shared<DataTypeInt64>());

    OperatorPtr child = std::make_shared<LocalExchangeSourceOperatorX>();
    EXPECT_TRUE(child->init(TLocalPartitionType::ADAPTIVE_PASSTHROUGH).ok());
    op->_child = child;

    const auto distribution = op->required_data_distribution(state.get());
    EXPECT_EQ(TLocalPartitionType::GLOBAL_EXECUTION_HASH_SHUFFLE, distribution.distribution_type);
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

TEST_F(DistinctStreamingAggOperatorTest, refresh_memory_limit) {
    op->_is_streaming_preagg = true;
    op->set_parallel_tasks(2);
    auto* memory_context = state->get_query_ctx()->resource_ctx()->memory_context();
    memory_context->set_mem_limit(1024LL * 1024 * 1024);
    create_op({std::make_shared<DataTypeInt64>()}, {std::make_shared<DataTypeInt64>()});

    // 1GB / 2 tasks / 5
    auto* memory_use_limit = local_state->custom_profile()->get_counter("MemoryUseLimit");
    ASSERT_NE(memory_use_limit, nullptr);
    EXPECT_EQ(memory_use_limit->value(), 1024LL * 1024 * 1024 / 2 / 5);

    auto block = ColumnHelper::create_block<DataTypeInt64>({1, 2, 3, 4});
    EXPECT_TRUE(op->push(state.get(), &block, false));
    EXPECT_FALSE(local_state->_stop_emplace_flag);

    // spill_streaming_agg_mem_limit is ignored while spilling is disabled ...
    op->_spill_streaming_agg_mem_limit = 4 * 1024 * 1024;
    block = ColumnHelper::create_block<DataTypeInt64>({5});
    EXPECT_TRUE(op->push(state.get(), &block, false));
    EXPECT_FALSE(local_state->_stop_emplace_flag);
    EXPECT_EQ(memory_use_limit->value(), 1024LL * 1024 * 1024 / 2 / 5);

    // ... and caps the distinct pre-agg once spilling is enabled.
    state->set_enable_spill(true);
    block = ColumnHelper::create_block<DataTypeInt64>({6});
    EXPECT_TRUE(op->push(state.get(), &block, false));
    EXPECT_FALSE(local_state->_stop_emplace_flag);
    EXPECT_EQ(memory_use_limit->value(), 4 * 1024 * 1024);

    // A tiny query limit: the floor is capped by the per-task share (10 / 2 = 5 bytes), the
    // hash table already exceeds it, so the pre-agg gives up and passes rows through.
    memory_context->set_mem_limit(10);
    block = ColumnHelper::create_block<DataTypeInt64>({1, 1});
    EXPECT_TRUE(op->push(state.get(), &block, false));
    EXPECT_TRUE(local_state->_stop_emplace_flag);
    EXPECT_EQ(memory_use_limit->value(), 5);
    EXPECT_EQ(local_state->_aggregated_block->rows(), 8);
}

TEST_F(DistinctStreamingAggOperatorTest, pushed_limit_with_memory_limit) {
    op->_is_streaming_preagg = true;
    op->_limit = 2;
    op->set_parallel_tasks(2);
    auto* memory_context = state->get_query_ctx()->resource_ctx()->memory_context();
    memory_context->set_mem_limit(1024LL * 1024 * 1024);
    create_op({std::make_shared<DataTypeInt64>()}, {std::make_shared<DataTypeInt64>()});

    // Within the budget a pushed-down LIMIT works as usual: duplicates are removed and the
    // operator stops once `limit` distinct keys are out.
    auto block = ColumnHelper::create_block<DataTypeInt64>({1, 1});
    EXPECT_TRUE(op->push(state.get(), &block, false));
    EXPECT_FALSE(local_state->_stop_emplace_flag);
    EXPECT_FALSE(local_state->_reach_limit);
    EXPECT_EQ(local_state->_aggregated_block->rows(), 1);

    // Budget exceeded: the operator latches into pass-through and no longer truncates, so the
    // global stage still sees every key that may be distinct (the limit is applied there).
    memory_context->set_mem_limit(10);
    block = ColumnHelper::create_block<DataTypeInt64>({1, 1, 2, 3});
    EXPECT_TRUE(op->push(state.get(), &block, false));
    EXPECT_TRUE(local_state->_stop_emplace_flag);
    EXPECT_FALSE(local_state->_reach_limit);
    EXPECT_EQ(local_state->_aggregated_block->rows(), 5);
}

TEST_F(DistinctStreamingAggOperatorTest, pass_through_does_not_consume_pushed_limit) {
    op->_is_streaming_preagg = true;
    op->_limit = 2;
    create_op({std::make_shared<DataTypeInt64>()}, {std::make_shared<DataTypeInt64>()});

    auto block = ColumnHelper::create_block<DataTypeInt64>({1});
    EXPECT_TRUE(op->push(state.get(), &block, false));
    EXPECT_EQ(local_state->_aggregated_block->rows(), 1);

    // Once the operator has permanently stopped deduplicating (low reduction rate / low-memory
    // mode), raw rows must not be truncated against the limit: the global stage needs every
    // key that may still be distinct.
    local_state->_stop_emplace_flag = true;
    block = ColumnHelper::create_block<DataTypeInt64>({1, 1, 2});
    EXPECT_TRUE(op->push(state.get(), &block, false));
    EXPECT_FALSE(local_state->_reach_limit);
    EXPECT_EQ(local_state->_aggregated_block->rows(), 4);
}

} // namespace doris
