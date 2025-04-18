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

#include "pipeline/exec/exchange_source_operator.h"

#include <gtest/gtest.h>

#include <memory>

#include "pipeline/exec/mock_operator.h"
#include "pipeline/operator/operator_helper.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_slot_ref.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_number.h"
namespace doris::pipeline {

using namespace vectorized;

struct MOCKVDataStreamRecvr : public VDataStreamRecvr {
    MOCKVDataStreamRecvr(RuntimeState* state, RuntimeProfile::HighWaterMarkCounter* counter,
                         RuntimeProfile* profile, int num_senders, bool is_merging)
            : VDataStreamRecvr(nullptr, counter, state, TUniqueId(), 0, num_senders, is_merging,
                               profile, 1) {};
    Status get_next(Block* block, bool* eos) override {
        block->swap(_block);
        *eos = _eos;
        return Status::OK();
    }

    Status create_merger(const VExprContextSPtrs& ordering_expr,
                         const std::vector<bool>& is_asc_order,
                         const std::vector<bool>& nulls_first, size_t batch_size, int64_t limit,
                         size_t offset) override {
        return Status::OK();
    }
    void set_block(std::vector<int64_t> dates) {
        Block new_block = ColumnHelper::create_block<DataTypeInt64>(dates);
        _block.swap(new_block);
    }
    void set_eos(bool eos) { _eos = eos; }
    void close() override {}

    Block _block;

    bool _eos = false;
};

struct MockExchangeSourceLocalState : public ExchangeLocalState {
    MockExchangeSourceLocalState(RuntimeState* state, OperatorXBase* parent)
            : ExchangeLocalState(state, parent) {}
    void create_stream_recvr(RuntimeState* state) override {}
};

struct ExchangeSourceOperatorXTest : public ::testing::Test {
    void SetUp() override {
        state = std::make_shared<MockRuntimeState>();
        state->batsh_size = 10;
    }

    void create_op(int num_senders, bool is_merging, int offset, int limit) {
        op = std::make_unique<ExchangeSourceOperatorX>(num_senders, is_merging, offset);
        op->_limit = limit;
        create_local_state();
    }

    void create_local_state() {
        local_state_uptr = std::make_unique<MockExchangeSourceLocalState>(state.get(), op.get());
        local_state = local_state_uptr.get();
        stream_recvr = std::make_shared<MOCKVDataStreamRecvr>(
                state.get(), _mock_counter.get(), &profile, op->num_senders(), op->is_merging());
        local_state->stream_recvr = stream_recvr;
        LocalStateInfo info {.parent_profile = &profile,
                             .scan_ranges = {},
                             .shared_state = nullptr,
                             .shared_state_map = {},
                             .task_idx = 0};
        auto st = local_state->init(state.get(), info);
        state->resize_op_id_to_local_state(-100);
        state->emplace_local_state(op->operator_id(), std::move(local_state_uptr));
        EXPECT_TRUE(local_state->open(state.get()));
    }

    RuntimeProfile profile {"test"};
    std::unique_ptr<ExchangeSourceOperatorX> op;

    std::unique_ptr<ExchangeLocalState> local_state_uptr;
    ExchangeLocalState* local_state;
    std::shared_ptr<MOCKVDataStreamRecvr> stream_recvr;
    std::shared_ptr<MockRuntimeState> state;
    std::unique_ptr<RuntimeProfile::HighWaterMarkCounter> _mock_counter =
            std::make_unique<RuntimeProfile::HighWaterMarkCounter>(TUnit::UNIT, 0, "test");
    ObjectPool pool;
};

TEST_F(ExchangeSourceOperatorXTest, test_merge) {
    create_op(3, true, 0, 10);
    {
        bool eos = false;
        Block block;
        // The first get block will create a merger
        EXPECT_TRUE(op->get_block(state.get(), &block, &eos));
        EXPECT_FALSE(eos);
        EXPECT_EQ(0, block.rows());
    }

    {
        stream_recvr->set_block({1, 2, 3});
        stream_recvr->set_eos(false);
        bool eos = false;
        Block block;
        EXPECT_TRUE(op->get_block(state.get(), &block, &eos));
        EXPECT_FALSE(eos);
        EXPECT_EQ(3, block.rows());
    }
    {
        stream_recvr->set_block({4, 5, 6, 7, 8, 9, 10, 11, 12, 13});
        stream_recvr->set_eos(true);
        bool eos = false;
        Block block;
        EXPECT_TRUE(op->get_block(state.get(), &block, &eos));
        EXPECT_TRUE(eos);
        EXPECT_EQ(7, block.rows());
    }

    {
        EXPECT_TRUE(op->close(state.get()));
        EXPECT_TRUE(local_state->close(state.get()));
    }
}

TEST_F(ExchangeSourceOperatorXTest, test_no_merge) {
    create_op(3, false, 3, 5);

    {
        stream_recvr->set_block({1, 2});
        stream_recvr->set_eos(false);
        bool eos = false;
        Block block;
        EXPECT_TRUE(op->get_block(state.get(), &block, &eos));
        EXPECT_FALSE(eos);
        EXPECT_EQ(0, block.rows());
    }

    {
        stream_recvr->set_block({1, 2});
        stream_recvr->set_eos(false);
        bool eos = false;
        Block block;
        EXPECT_TRUE(op->get_block(state.get(), &block, &eos));
        EXPECT_FALSE(eos);
        EXPECT_EQ(1, block.rows());
    }

    {
        stream_recvr->set_block({1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
        stream_recvr->set_eos(false);
        bool eos = false;
        Block block;
        // No eos, but limit has been reached
        EXPECT_TRUE(op->get_block(state.get(), &block, &eos));
        EXPECT_TRUE(eos);
        EXPECT_EQ(4, block.rows());
    }

    {
        EXPECT_TRUE(op->close(state.get()));
        EXPECT_TRUE(local_state->close(state.get()));
    }
}
} // namespace doris::pipeline