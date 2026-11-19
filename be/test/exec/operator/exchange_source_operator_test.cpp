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

#include "exec/operator/exchange_source_operator.h"

#include <gtest/gtest.h>

#include <memory>

#include "core/block/block.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "exec/operator/mock_operator.h"
#include "exec/operator/operator_helper.h"
#include "exec/pipeline/pipeline_fragment_context.h"
#include "exec/runtime_filter/runtime_filter_consumer.h"
#include "exec/runtime_filter/runtime_filter_consumer_helper.h"
#include "exec/runtime_filter/runtime_filter_producer.h"
#include "exec/runtime_filter/runtime_filter_test_utils.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_slot_ref.h"
namespace doris {

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
        state->_batch_size = 10;
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
class ExchangeRuntimeFilterTest : public RuntimeFilterTest {
protected:
    void SetUp() override {
        RuntimeFilterTest::SetUp();
        _context = std::make_shared<PipelineFragmentContext>(TUniqueId(), TPipelineFragmentParams(),
                                                             _query_ctx, ExecEnv::GetInstance(),
                                                             nullptr);
        _context->set_rec_cte_stage(3);
        _runtime_states[0]->set_task_execution_context(_context);
        auto* input_tuple = _pool.add(new TupleDescriptor());
        add_slot(input_tuple, 7);
        add_slot(input_tuple, 9);
        _tbl._tuple_desc_map[0] = input_tuple;
        auto* output_tuple = _pool.add(new TupleDescriptor());
        add_slot(output_tuple, 23);
        _tbl._tuple_desc_map[1] = output_tuple;
    }

    void TearDown() override {
        // Destroy local states while their operator and slot descriptors are still alive.
        _recvr.reset();
        _runtime_states[0]->_op_id_to_local_state.clear();
        RuntimeFilterTest::TearDown();
    }

    void add_slot(TupleDescriptor* tuple, int id) {
        auto* slot = _pool.add(new SlotDescriptor());
        slot->_id = id;
        slot->_type = std::make_shared<DataTypeInt64>();
        tuple->add_slot(slot);
        _tbl._slot_desc_map[id] = slot;
    }

    static TExpr slot_expr(int slot_id) {
        auto expr = TRuntimeFilterDescBuilder::get_default_expr();
        expr.nodes[0].type.types[0].scalar_type.__set_type(TPrimitiveType::BIGINT);
        expr.nodes[0].slot_ref.__set_slot_id(slot_id);
        return expr;
    }

    void init_op(bool merging = false, int offset = 0, int limit = -1, bool projection = false,
                 bool remote = false,
                 TRuntimeFilterType::type filter_type = TRuntimeFilterType::IN) {
        auto key_expr = slot_expr(9);
        key_expr.nodes[0].__set_is_nullable(_tbl._slot_desc_map[9]->type()->is_nullable());
        _desc = TRuntimeFilterDescBuilder(0, key_expr)
                        .set_type(filter_type)
                        .set_mode(!remote)
                        .set_bloom_filter_size_bytes(65536)
                        .add_planId_to_target_expr(17, key_expr)
                        // An unrelated target must not be used for this receiver.
                        .add_planId_to_target_expr(99, slot_expr(777))
                        .build();
        TExchangeNode exchange;
        exchange.__set_offset(offset);
        if (merging) {
            TSortInfo sort_info;
            sort_info.__set_ordering_exprs({slot_expr(9)});
            sort_info.__set_is_asc_order({true});
            sort_info.__set_nulls_first({false});
            exchange.__set_sort_info(sort_info);
        }
        auto tnode = TPlanNodeBuilder(17, TPlanNodeType::EXCHANGE_NODE, 0, limit)
                             .append_row_tuples(0, false)
                             .set_exchange_node(exchange)
                             .append_runtime_filters(_desc)
                             .build();
        if (projection) {
            tnode.__set_output_tuple_id(1);
            tnode.__set_projections({slot_expr(7)});
        }
        auto* state = _runtime_states[0].get();
        _op = std::make_unique<ExchangeSourceOperatorX>(&_pool, tnode, -1, _tbl, 1);
        ASSERT_TRUE(_op->init(tnode, state).ok());
        ASSERT_TRUE(_op->prepare(state).ok());
        auto local_state = std::make_unique<MockExchangeSourceLocalState>(state, _op.get());
        _local_state = local_state.get();
        _recvr = std::make_shared<MOCKVDataStreamRecvr>(state, _counter.get(), &_profile, 1,
                                                        merging);
        local_state->stream_recvr = _recvr;
        LocalStateInfo info {.parent_profile = &_profile,
                             .scan_ranges = {},
                             .shared_state = nullptr,
                             .shared_state_map = {},
                             .task_idx = 0};
        ASSERT_TRUE(local_state->init(state, info).ok());
        state->resize_op_id_to_local_state(-2);
        state->emplace_local_state(-1, std::move(local_state));
    }

    void publish_filter(
            const std::vector<int64_t>& keys,
            RuntimeFilterWrapper::State filter_state = RuntimeFilterWrapper::State::READY) {
        std::shared_ptr<RuntimeFilterProducer> producer;
        ASSERT_TRUE(RuntimeFilterProducer::create(_query_ctx.get(), &_desc, &producer).ok());
        ASSERT_TRUE(producer->init(keys.size()).ok());
        ASSERT_TRUE(producer->insert(ColumnHelper::create_column<DataTypeInt64>(keys), 0).ok());
        producer->set_wrapper_state_and_ready_to_publish(filter_state);
        ASSERT_TRUE(producer->_send_to_local_targets(_runtime_states[0].get(), producer.get(),
                                                     _desc.has_remote_targets)
                            .ok());
    }

    void feed(const std::vector<int64_t>& keys, bool eos = false) {
        std::vector<int64_t> payload;
        for (auto key : keys) {
            payload.push_back(key * 10);
        }
        _recvr->_block = ColumnHelper::create_block<DataTypeInt64>(payload, keys);
        _recvr->set_eos(eos);
    }

    ObjectPool _pool;
    std::shared_ptr<PipelineFragmentContext> _context;
    TRuntimeFilterDesc _desc;
    std::unique_ptr<ExchangeSourceOperatorX> _op;
    ExchangeLocalState* _local_state = nullptr;
    std::shared_ptr<MOCKVDataStreamRecvr> _recvr;
    std::unique_ptr<RuntimeProfile::HighWaterMarkCounter> _counter =
            std::make_unique<RuntimeProfile::HighWaterMarkCounter>(TUnit::UNIT, 0, "test");
};

TEST_F(ExchangeRuntimeFilterTest, DoesNotWaitForRuntimeFilter) {
    ASSERT_NO_FATAL_FAILURE(init_op());
    auto* state = _runtime_states[0].get();
    auto consumers = state->local_runtime_filter_mgr()->get_consume_filters(0);
    ASSERT_EQ(consumers.size(), 1);
    EXPECT_TRUE(consumers[0]->_filter_timer.empty());
    EXPECT_EQ(consumers[0]->stage(), 3);
    EXPECT_TRUE(_local_state->execution_dependencies().empty());
    ASSERT_TRUE(_local_state->open(state).ok());
    feed({1, 2, 3});
    bool eos = false;
    Block block;
    ASSERT_TRUE(_op->get_block_after_projects(state, &block, &eos).ok());
    EXPECT_EQ(block.rows(), 3);
    EXPECT_FALSE(eos);
}

TEST_F(ExchangeRuntimeFilterTest, FilterReadyBeforeOpenAndProfile) {
    ASSERT_NO_FATAL_FAILURE(init_op());
    ASSERT_NO_FATAL_FAILURE(publish_filter({2}));
    auto* state = _runtime_states[0].get();
    ASSERT_TRUE(_local_state->open(state).ok());
    feed({1, 2, 3});
    bool eos = false;
    Block block;
    ASSERT_TRUE(_op->get_block_after_projects(state, &block, &eos).ok());
    ASSERT_EQ(block.rows(), 1);
    EXPECT_EQ(block.get_by_position(1).column->get_int(0), 2);
    ASSERT_TRUE(_local_state->close(state).ok());
    auto* input_rows = _local_state->custom_profile()->get_counter("RF0 InputRows");
    auto* filter_rows = _local_state->custom_profile()->get_counter("RF0 FilterRows");
    ASSERT_NE(input_rows, nullptr);
    ASSERT_NE(filter_rows, nullptr);
    EXPECT_EQ(input_rows->value(), 3);
    EXPECT_EQ(filter_rows->value(), 2);
}

TEST_F(ExchangeRuntimeFilterTest, LateArrivalFiltersSubsequentBlocks) {
    ASSERT_NO_FATAL_FAILURE(init_op());
    auto* state = _runtime_states[0].get();
    ASSERT_TRUE(_local_state->open(state).ok());
    feed({1, 2, 3});
    bool eos = false;
    Block first;
    ASSERT_TRUE(_op->get_block_after_projects(state, &first, &eos).ok());
    EXPECT_EQ(first.rows(), 3);
    ASSERT_NO_FATAL_FAILURE(publish_filter({2}));
    for (int i = 0; i < 2; ++i) {
        feed({1, 2, 3});
        Block block;
        ASSERT_TRUE(_op->get_block_after_projects(state, &block, &eos).ok());
        ASSERT_EQ(block.rows(), 1);
        EXPECT_EQ(block.get_by_position(1).column->get_int(0), 2);
        EXPECT_FALSE(eos);
        EXPECT_EQ(_local_state->_runtime_filter_conjuncts.size(), 1);
    }
}

TEST_F(ExchangeRuntimeFilterTest, EmptyAndFullyFilteredBlocksDoNotEndStream) {
    ASSERT_NO_FATAL_FAILURE(init_op());
    auto* state = _runtime_states[0].get();
    ASSERT_TRUE(_local_state->open(state).ok());
    ASSERT_NO_FATAL_FAILURE(publish_filter({2}));
    for (const auto& keys : {std::vector<int64_t> {}, std::vector<int64_t> {1, 3}}) {
        feed(keys);
        Block block;
        bool eos = false;
        ASSERT_TRUE(_op->get_block_after_projects(state, &block, &eos).ok());
        EXPECT_EQ(block.rows(), 0);
        EXPECT_FALSE(eos);
    }
    feed({1, 2, 3}, true);
    Block last;
    bool eos = false;
    ASSERT_TRUE(_op->get_block_after_projects(state, &last, &eos).ok());
    ASSERT_EQ(last.rows(), 1);
    EXPECT_EQ(last.get_by_position(1).column->get_int(0), 2);
    EXPECT_TRUE(eos);
}

TEST_F(ExchangeRuntimeFilterTest, MergeReceiverFiltersFinalNonemptyBlock) {
    ASSERT_NO_FATAL_FAILURE(init_op(true));
    auto* state = _runtime_states[0].get();
    ASSERT_TRUE(_local_state->open(state).ok());
    Block first;
    bool eos = false;
    ASSERT_TRUE(_op->get_block_after_projects(state, &first, &eos).ok());
    EXPECT_EQ(first.rows(), 0);
    EXPECT_FALSE(eos);
    ASSERT_NO_FATAL_FAILURE(publish_filter({2}));
    feed({1, 2, 3}, true);
    Block last;
    ASSERT_TRUE(_op->get_block_after_projects(state, &last, &eos).ok());
    ASSERT_EQ(last.rows(), 1);
    EXPECT_EQ(last.get_by_position(1).column->get_int(0), 2);
    EXPECT_TRUE(eos);
}

TEST_F(ExchangeRuntimeFilterTest, PreservesLimitAndOffsetBeforeFiltering) {
    ASSERT_NO_FATAL_FAILURE(init_op(false, 1, 2));
    auto* state = _runtime_states[0].get();
    ASSERT_TRUE(_local_state->open(state).ok());
    ASSERT_NO_FATAL_FAILURE(publish_filter({1, 4}));
    // OFFSET 1 LIMIT 2 selects [2, 3]. Filtering first would instead return 4.
    feed({1, 2, 3, 4});
    Block block;
    bool eos = false;
    ASSERT_TRUE(_op->get_block_after_projects(state, &block, &eos).ok());
    EXPECT_EQ(block.rows(), 0);
    EXPECT_TRUE(eos);
    EXPECT_EQ(_local_state->num_rows_returned(), 2);
}

TEST_F(ExchangeRuntimeFilterTest, BindsLateFilterBeforeProjection) {
    ASSERT_NO_FATAL_FAILURE(init_op(false, 0, -1, true));
    auto* state = _runtime_states[0].get();
    ASSERT_TRUE(_local_state->open(state).ok());
    ASSERT_NO_FATAL_FAILURE(publish_filter({2}));
    feed({1, 2, 3});
    Block block;
    bool eos = false;
    ASSERT_TRUE(_op->get_block_after_projects(state, &block, &eos).ok());
    ASSERT_EQ(block.columns(), 1);
    ASSERT_EQ(block.rows(), 1);
    EXPECT_EQ(block.get_by_position(0).column->get_int(0), 20);
}

TEST_F(ExchangeRuntimeFilterTest, DisabledFilterDoesNotPruneRows) {
    ASSERT_NO_FATAL_FAILURE(init_op());
    auto* state = _runtime_states[0].get();
    ASSERT_TRUE(_local_state->open(state).ok());
    ASSERT_NO_FATAL_FAILURE(publish_filter({}, RuntimeFilterWrapper::State::DISABLED));
    feed({1, 2, 3}, true);
    Block block;
    bool eos = false;
    ASSERT_TRUE(_op->get_block_after_projects(state, &block, &eos).ok());
    EXPECT_EQ(block.rows(), 3);
    EXPECT_TRUE(eos);
    EXPECT_TRUE(_local_state->_runtime_filter_conjuncts.empty());
}

TEST_F(ExchangeRuntimeFilterTest, NullableProbeKeys) {
    _tbl._slot_desc_map[9]->_type = make_nullable(std::make_shared<DataTypeInt64>());
    ASSERT_NO_FATAL_FAILURE(init_op());
    auto* state = _runtime_states[0].get();
    ASSERT_TRUE(_local_state->open(state).ok());
    ASSERT_NO_FATAL_FAILURE(publish_filter({2}));
    feed({0, 2, 3});
    _recvr->_block.get_by_position(1) = {
            ColumnHelper::create_nullable_column<DataTypeInt64>({0, 2, 3}, {1, 0, 0}),
            make_nullable(std::make_shared<DataTypeInt64>()), "key"};
    Block block;
    bool eos = false;
    ASSERT_TRUE(_op->get_block_after_projects(state, &block, &eos).ok());
    ASSERT_EQ(block.rows(), 1);
    EXPECT_EQ(block.get_by_position(0).column->get_int(0), 20);
    EXPECT_FALSE(block.get_by_position(1).column->is_null_at(0));
}

class ExchangeRuntimeFilterTypeTest : public ExchangeRuntimeFilterTest,
                                      public testing::WithParamInterface<TRuntimeFilterType::type> {
};

TEST_P(ExchangeRuntimeFilterTypeTest, RemoteFilterPrunesReceivedRows) {
    ASSERT_NO_FATAL_FAILURE(init_op(false, 0, -1, false, true, GetParam()));
    auto* state = _runtime_states[0].get();
    ASSERT_TRUE(_local_state->open(state).ok());
    auto consumers = state->global_runtime_filter_mgr()->get_consume_filters(0);
    ASSERT_EQ(consumers.size(), 1);
    EXPECT_TRUE(consumers[0]->_filter_timer.empty());
    ASSERT_NO_FATAL_FAILURE(publish_filter({2}));
    feed({1, 2, 3});
    Block block;
    bool eos = false;
    ASSERT_TRUE(_op->get_block_after_projects(state, &block, &eos).ok());
    ASSERT_EQ(block.rows(), 1);
    EXPECT_EQ(block.get_by_position(1).column->get_int(0), 2);
}

INSTANTIATE_TEST_SUITE_P(FilterTypes, ExchangeRuntimeFilterTypeTest,
                         testing::Values(TRuntimeFilterType::IN, TRuntimeFilterType::BLOOM,
                                         TRuntimeFilterType::MIN_MAX,
                                         TRuntimeFilterType::IN_OR_BLOOM));

} // namespace doris
