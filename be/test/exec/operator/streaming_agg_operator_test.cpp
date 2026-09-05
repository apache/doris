
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

#include <memory>

#include "core/data_type/data_type_bitmap.h"
#include "core/data_type/data_type_number.h"
#include "core/value/bitmap_value.h"
#include "exec/exchange/local_exchange_source_operator.h"
#include "exec/operator/aggregation_sink_operator.h"
#include "exec/operator/aggregation_source_operator.h"
#include "exec/operator/mock_operator.h"
#include "exec/operator/operator_helper.h"
#include "exec/operator/streaming_agg_memory_limit.h"
#include "exec/operator/streaming_aggregation_operator.h"
#include "runtime/workload_management/io_context.h"
#include "runtime/workload_management/memory_context.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_agg_fn_evaluator.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"
#include "util/jsonb_document.h"

namespace doris {

struct MockStreamingAggOperatorX : public StreamingAggOperatorX {
    MockStreamingAggOperatorX() = default;

    Status _init_probe_expr_ctx(RuntimeState* state) override { return Status::OK(); }

    Status _init_aggregate_evaluators(RuntimeState* state) override { return Status::OK(); }
};

struct MockStreamingAggLocalState : public StreamingAggLocalState {
    MockStreamingAggLocalState(RuntimeState* state, OperatorXBase* parent)
            : StreamingAggLocalState(state, parent) {}

    bool _should_not_do_pre_agg(size_t rows) override {
        static_cast<void>(_should_expand_preagg_hash_tables()); // mock the function
        static_cast<void>(_memory_usage());                     // mock the function
        const bool real_decision = StreamingAggLocalState::_should_not_do_pre_agg(rows);
        return use_real_decision ? real_decision : should_not_do_pre_agg;
    }

    bool should_not_do_pre_agg = false;
    bool use_real_decision = false;
};

class MockStreamingAggOperatorChildOperator : public OperatorXBase {
public:
    Status get_block_after_projects(RuntimeState* state, Block* block, bool* eos) override {
        return Status::OK();
    }

    Status get_block_impl(RuntimeState* state, Block* block, bool* eos) override {
        return Status::OK();
    }
    Status setup_local_state(RuntimeState* state, LocalStateInfo& info) override {
        return Status::OK();
    }

    void set_mock_row_desc(std::unique_ptr<MockRowDescriptor> row_desc) {
        _mock_row_desc = std::move(row_desc);
        _row_descriptor = *_mock_row_desc;
    }

private:
    std::unique_ptr<MockRowDescriptor> _mock_row_desc;
};
struct StreamingAggOperatorTest : public testing::Test {
    void SetUp() override {
        state = std::make_shared<MockRuntimeState>();
        op = std::make_shared<MockStreamingAggOperatorX>();
        child_op = std::make_shared<MockStreamingAggOperatorChildOperator>();
        child_op->set_mock_row_desc(std::make_unique<MockRowDescriptor>(
                std::vector<DataTypePtr> {std::make_shared<DataTypeInt64>(),
                                          std::make_shared<DataTypeInt64>()},
                &pool));
    }

    std::shared_ptr<MockStreamingAggOperatorX> op;
    std::shared_ptr<MockStreamingAggOperatorChildOperator> child_op;

    std::shared_ptr<MockRuntimeState> state;

    RuntimeProfile profile {""};

    MockStreamingAggLocalState* local_state = nullptr;

    ObjectPool pool;
};

TEST(StreamingAggMemoryLimitTest, budget_floor_and_fixed_limit) {
    constexpr int64_t MB = 1024 * 1024;
    // One fifth of the per-task share of the query limit.
    EXPECT_EQ(streaming_agg_memory_limit(5000 * MB, 5, 0), size_t(200 * MB));
    // parallel_tasks <= 0 is treated as 1.
    EXPECT_EQ(streaming_agg_memory_limit(1000 * MB, 0, 0), size_t(200 * MB));
    // Floor: a small query limit still leaves twice the last cache tier (32MB) ...
    EXPECT_EQ(streaming_agg_memory_limit(2048 * MB, 16, 0), size_t(32 * MB));
    // ... but never more than the per-task share of the query limit.
    EXPECT_EQ(streaming_agg_memory_limit(64 * MB, 16, 0), size_t(4 * MB));
    EXPECT_EQ(streaming_agg_memory_limit(10, 2, 0), size_t(5));
    // A positive query limit smaller than the task count still yields a cap, never "no cap".
    EXPECT_EQ(streaming_agg_memory_limit(1, 2, 0), size_t(1));
    EXPECT_EQ(streaming_agg_memory_limit(1, 2, 256 * MB), size_t(1));
    // The fixed bound (spill_streaming_agg_mem_limit) is applied last: it never raises the
    // budget, and an explicit small value beats the floor.
    EXPECT_EQ(streaming_agg_memory_limit(5000 * MB, 5, 256 * MB), size_t(200 * MB));
    EXPECT_EQ(streaming_agg_memory_limit(50000 * MB, 5, 256 * MB), size_t(256 * MB));
    EXPECT_EQ(streaming_agg_memory_limit(2048 * MB, 16, 8 * MB), size_t(8 * MB));
    // Unknown query limit: only the fixed bound, or no cap at all.
    EXPECT_EQ(streaming_agg_memory_limit(0, 4, 256 * MB), size_t(256 * MB));
    EXPECT_EQ(streaming_agg_memory_limit(-1, 4, 0), size_t(0));
}

TEST_F(StreamingAggOperatorTest, test1) {
    auto* memory_context = state->get_query_ctx()->resource_ctx()->memory_context();
    memory_context->set_mem_limit(5000LL * 1024 * 1024);
    op->set_parallel_tasks(5);

    op->_aggregate_evaluators.push_back(create_mock_agg_fn_evaluator(
            pool, MockSlotRef::create_mock_contexts(1, std::make_shared<DataTypeInt64>()), false,
            false));
    op->_pool = &pool;
    op->_needs_finalize = false;

    EXPECT_TRUE(op->set_child(child_op));

    EXPECT_TRUE(op->prepare(state.get()).ok());
    op->_probe_expr_ctxs = MockSlotRef::create_mock_contexts(0, std::make_shared<DataTypeInt64>());

    {
        auto local_state = std::make_unique<MockStreamingAggLocalState>(state.get(), op.get());
        LocalStateInfo info {.parent_profile = &profile,
                             .scan_ranges = {},
                             .shared_state = nullptr,
                             .shared_state_map = {},
                             .task_idx = 0};

        EXPECT_TRUE(local_state->init(state.get(), info).ok());
        state->resize_op_id_to_local_state(-100);
        state->emplace_local_state(op->operator_id(), std::move(local_state));
    }

    {
        local_state =
                static_cast<MockStreamingAggLocalState*>(state->get_local_state(op->operator_id()));
        EXPECT_TRUE(local_state->open(state.get()).ok());
        auto* memory_use_limit = local_state->custom_profile()->get_counter("MemoryUseLimit");
        ASSERT_NE(memory_use_limit, nullptr);
        EXPECT_EQ(memory_use_limit->value(), 200 * 1024 * 1024);
    }

    {
        memory_context->set_mem_limit(2500LL * 1024 * 1024);
        Block block {
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 2, 2, 2, 3}),
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 100, 100, 100, 1000})};
        auto st = op->push(state.get(), &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();

        EXPECT_EQ(local_state->_get_hash_table_size(), 3);
        EXPECT_TRUE(op->need_more_input_data(state.get()));
        EXPECT_EQ(local_state->custom_profile()->get_counter("MemoryUseLimit")->value(),
                  100 * 1024 * 1024);
    }

    {
        // With spilling enabled, spill_streaming_agg_mem_limit caps the budget.
        state->set_enable_spill(true);
        op->_spill_streaming_agg_mem_limit = 16 * 1024 * 1024;
        Block block {ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3}),
                     ColumnHelper::create_column_with_name<DataTypeInt64>({1, 100, 1000})};
        auto st = op->push(state.get(), &block, false);
        EXPECT_TRUE(st.ok()) << st.msg();

        EXPECT_EQ(local_state->_get_hash_table_size(), 3);
        EXPECT_EQ(local_state->custom_profile()->get_counter("MemoryUseLimit")->value(),
                  16 * 1024 * 1024);
    }

    {
        // Low-memory mode never raises a tighter bound.
        op->_spill_streaming_agg_mem_limit = 512 * 1024;
        op->set_low_memory_mode(state.get());
        Block block {
                ColumnHelper::create_column_with_name<DataTypeInt64>({2, 2, 2, 2, 4, 4}),
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 100, 100, 100, 1000})};
        auto st = op->push(state.get(), &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();

        EXPECT_EQ(local_state->_get_hash_table_size(), 4);
        EXPECT_TRUE(op->need_more_input_data(state.get()));
        EXPECT_EQ(local_state->custom_profile()->get_counter("MemoryUseLimit")->value(),
                  512 * 1024);
    }

    { EXPECT_TRUE(local_state->close(state.get()).ok()); }
}

TEST_F(StreamingAggOperatorTest, memory_limit_pass_through_and_recover) {
    // A real aggregate function so that the pass-through serialization path is exercised.
    op->_aggregate_evaluators.push_back(create_agg_fn(pool, "sum",
                                                      {std::make_shared<DataTypeInt64>()},
                                                      std::make_shared<DataTypeInt64>(), false));
    op->_pool = &pool;
    op->_needs_finalize = false;
    // A LIMIT pushed down to this local stage (pure distinct through the regular operator).
    op->_limit = 2;
    op->set_parallel_tasks(5);

    EXPECT_TRUE(op->set_child(child_op));
    EXPECT_TRUE(op->prepare(state.get()).ok());
    op->_probe_expr_ctxs = MockSlotRef::create_mock_contexts(1, std::make_shared<DataTypeInt64>());

    {
        auto local_state = std::make_unique<MockStreamingAggLocalState>(state.get(), op.get());
        LocalStateInfo info {.parent_profile = &profile,
                             .scan_ranges = {},
                             .shared_state = nullptr,
                             .shared_state_map = {},
                             .task_idx = 0};
        EXPECT_TRUE(local_state->init(state.get(), info).ok());
        state->resize_op_id_to_local_state(-100);
        state->emplace_local_state(op->operator_id(), std::move(local_state));
    }
    local_state =
            static_cast<MockStreamingAggLocalState*>(state->get_local_state(op->operator_id()));
    EXPECT_TRUE(local_state->open(state.get()).ok());
    // Let the production decision drive push() instead of the mock's fixed answer.
    local_state->use_real_decision = true;
    auto* memory_context = state->get_query_ctx()->resource_ctx()->memory_context();
    auto* memory_use_limit = local_state->custom_profile()->get_counter("MemoryUseLimit");
    auto* io_context = state->get_query_ctx()->resource_ctx()->io_context();

    {
        memory_context->set_mem_limit(5000LL * 1024 * 1024);
        Block block {ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 100}),
                     ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 2})};
        auto st = op->push(state.get(), &block, false);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_EQ(local_state->_get_hash_table_size(), 2);
        EXPECT_EQ(local_state->_pre_aggregated_block->rows(), 0);
    }

    {
        // Query limit lowered below the current usage: the block passes through untouched, the
        // hash table does not grow, and the duplicate keys do not count against the limit.
        memory_context->set_mem_limit(10);
        Block block {ColumnHelper::create_column_with_name<DataTypeInt64>({100, 1000, 1000}),
                     ColumnHelper::create_column_with_name<DataTypeInt64>({2, 3, 3})};
        auto st = op->push(state.get(), &block, false);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_EQ(memory_use_limit->value(), 2); // 10 / 5 tasks, floor capped by the share
        EXPECT_EQ(local_state->_get_hash_table_size(), 2);
        EXPECT_EQ(local_state->_pre_aggregated_block->rows(), 3);

        // Pass-through rows are not counted against the pushed-down limit (no eos), but they
        // are still reported as processed rows.
        const int64_t process_rows_before = io_context->process_rows();
        Block out;
        bool eos = false;
        EXPECT_TRUE(op->pull(state.get(), &out, &eos).ok());
        EXPECT_EQ(out.rows(), 3);
        EXPECT_FALSE(eos);
        EXPECT_EQ(io_context->process_rows(), process_rows_before + 3);
        EXPECT_TRUE(op->need_more_input_data(state.get()));
    }

    {
        // Query limit restored: aggregation resumes against the retained hash table.
        memory_context->set_mem_limit(5000LL * 1024 * 1024);
        Block block {ColumnHelper::create_column_with_name<DataTypeInt64>({1000, 1}),
                     ColumnHelper::create_column_with_name<DataTypeInt64>({3, 4})};
        auto st = op->push(state.get(), &block, false);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_EQ(local_state->_get_hash_table_size(), 4);
        EXPECT_EQ(local_state->_pre_aggregated_block->rows(), 0);
    }

    { EXPECT_TRUE(local_state->close(state.get()).ok()); }
}

TEST_F(StreamingAggOperatorTest, require_hash_shuffle_after_non_hash_local_exchange) {
    state->_query_options.__set_enable_local_exchange_before_agg(false);
    op->_needs_finalize = false;
    op->_partition_exprs.emplace_back();

    OperatorPtr child = std::make_shared<LocalExchangeSourceOperatorX>();
    EXPECT_TRUE(child->init(TLocalPartitionType::ADAPTIVE_PASSTHROUGH).ok());
    EXPECT_TRUE(op->set_child(child));

    const auto distribution = op->required_data_distribution(state.get());
    EXPECT_EQ(TLocalPartitionType::GLOBAL_EXECUTION_HASH_SHUFFLE, distribution.distribution_type);
}

TEST_F(StreamingAggOperatorTest, test2) {
    op->_aggregate_evaluators.push_back(create_mock_agg_fn_evaluator(
            pool, MockSlotRef::create_mock_contexts(1, std::make_shared<DataTypeInt64>()), false,
            false));
    op->_pool = &pool;
    op->_needs_finalize = false;

    EXPECT_TRUE(op->set_child(child_op));

    EXPECT_TRUE(op->prepare(state.get()).ok());
    op->_probe_expr_ctxs = MockSlotRef::create_mock_contexts(0, std::make_shared<DataTypeInt64>());

    {
        auto local_state = std::make_unique<MockStreamingAggLocalState>(state.get(), op.get());
        LocalStateInfo info {.parent_profile = &profile,
                             .scan_ranges = {},
                             .shared_state = nullptr,
                             .shared_state_map = {},
                             .task_idx = 0};

        EXPECT_TRUE(local_state->init(state.get(), info).ok());
        state->resize_op_id_to_local_state(-100);
        state->emplace_local_state(op->operator_id(), std::move(local_state));
    }

    {
        local_state =
                static_cast<MockStreamingAggLocalState*>(state->get_local_state(op->operator_id()));
        EXPECT_TRUE(local_state->open(state.get()).ok());
    }

    {
        Block block {
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 2, 2, 2, 3}),
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 100, 100, 100, 1000})};
        auto st = op->push(state.get(), &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();

        EXPECT_EQ(local_state->_get_hash_table_size(), 3);
        EXPECT_TRUE(op->need_more_input_data(state.get()));
    }

    {
        local_state->should_not_do_pre_agg = true;
        Block block {
                ColumnHelper::create_column_with_name<DataTypeInt64>({2, 2, 2, 2, 4, 4}),
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 100, 100, 100, 1000})};
        auto st = op->push(state.get(), &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();

        EXPECT_EQ(local_state->_get_hash_table_size(), 3);
        EXPECT_FALSE(op->need_more_input_data(state.get()));
    }

    {
        bool eos = false;
        Block block;
        auto st = op->pull(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_FALSE(eos);
        EXPECT_EQ(block.rows(), 6);
    }

    {
        op->_make_nullable_keys = {0}; // make key nullable
        bool eos = false;
        Block block;
        auto st = op->pull(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(eos);
        EXPECT_EQ(block.rows(), 3);
    }

    { EXPECT_TRUE(local_state->close(state.get()).ok()); }
}

TEST_F(StreamingAggOperatorTest, test3) {
    op->_aggregate_evaluators.push_back(create_mock_agg_fn_evaluator(
            pool, MockSlotRef::create_mock_contexts(1, std::make_shared<DataTypeInt64>()), false,
            false));
    op->_pool = &pool;
    op->_needs_finalize = false;

    EXPECT_TRUE(op->set_child(child_op));

    EXPECT_TRUE(op->prepare(state.get()).ok());
    op->_probe_expr_ctxs = MockSlotRef::create_mock_contexts(
            0, std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()));

    {
        auto local_state = std::make_unique<MockStreamingAggLocalState>(state.get(), op.get());
        LocalStateInfo info {.parent_profile = &profile,
                             .scan_ranges = {},
                             .shared_state = nullptr,
                             .shared_state_map = {},
                             .task_idx = 0};

        EXPECT_TRUE(local_state->init(state.get(), info).ok());
        state->resize_op_id_to_local_state(-100);
        state->emplace_local_state(op->operator_id(), std::move(local_state));
    }

    {
        local_state =
                static_cast<MockStreamingAggLocalState*>(state->get_local_state(op->operator_id()));
        EXPECT_TRUE(local_state->open(state.get()).ok());
    }

    {
        Block block {
                ColumnHelper::create_nullable_column_with_name<DataTypeInt64>(
                        {1, 1, 2, 2, 2, 3}, {false, false, false, false, false, true}),
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 100, 100, 100, 1000})};
        auto st = op->push(state.get(), &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();

        EXPECT_EQ(local_state->_get_hash_table_size(), 3);
        EXPECT_TRUE(op->need_more_input_data(state.get()));
    }

    {
        local_state->should_not_do_pre_agg = true;
        Block block {
                ColumnHelper::create_nullable_column_with_name<DataTypeInt64>(
                        {2, 2, 2, 2, 4, 4}, {false, false, false, false, false, false}),
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 100, 100, 100, 1000})};
        auto st = op->push(state.get(), &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();

        EXPECT_EQ(local_state->_get_hash_table_size(), 3);
        EXPECT_FALSE(op->need_more_input_data(state.get()));
    }

    {
        bool eos = false;
        Block block;
        auto st = op->pull(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_FALSE(eos);
        EXPECT_EQ(block.rows(), 6);
    }

    {
        op->_make_nullable_keys = {0}; // make key nullable
        bool eos = false;
        Block block;
        auto st = op->pull(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(eos);
        EXPECT_EQ(block.rows(), 3);
    }

    { EXPECT_TRUE(local_state->close(state.get()).ok()); }
}

TEST_F(StreamingAggOperatorTest, test4) {
    op->_aggregate_evaluators.push_back(create_agg_fn(pool, "bitmap_union",
                                                      {std::make_shared<DataTypeBitMap>()},
                                                      std::make_shared<DataTypeBitMap>(), false));
    op->_pool = &pool;
    op->_needs_finalize = false;

    EXPECT_TRUE(op->set_child(child_op));

    EXPECT_TRUE(op->prepare(state.get()).ok());
    op->_probe_expr_ctxs = MockSlotRef::create_mock_contexts(
            1, std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()));

    {
        auto local_state = std::make_unique<MockStreamingAggLocalState>(state.get(), op.get());
        LocalStateInfo info {.parent_profile = &profile,
                             .scan_ranges = {},
                             .shared_state = nullptr,
                             .shared_state_map = {},
                             .task_idx = 0};

        EXPECT_TRUE(local_state->init(state.get(), info).ok());
        state->resize_op_id_to_local_state(-100);
        state->emplace_local_state(op->operator_id(), std::move(local_state));
    }

    {
        local_state =
                static_cast<MockStreamingAggLocalState*>(state->get_local_state(op->operator_id()));
        EXPECT_TRUE(local_state->open(state.get()).ok());
    }

    {
        std::vector<BitmapValue> bitmaps = {BitmapValue(1), BitmapValue(2), BitmapValue(3),
                                            BitmapValue(4), BitmapValue(5), BitmapValue(6)};

        Block block {ColumnHelper::create_column_with_name<DataTypeBitMap>(bitmaps),
                     ColumnHelper::create_nullable_column_with_name<DataTypeInt64>(
                             {1, 1, 2, 2, 2, 3}, {false, false, false, false, false, true})};
        local_state->should_not_do_pre_agg = false;
        local_state->_should_expand_hash_table = true;
        std::cout << block.dump_data() << std::endl;
        auto st = op->push(state.get(), &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();

        EXPECT_EQ(local_state->_get_hash_table_size(), 3);
        EXPECT_TRUE(op->need_more_input_data(state.get()));
    }

    {
        local_state->should_not_do_pre_agg = false;
        local_state->_should_expand_hash_table = false;
        std::vector<BitmapValue> bitmaps2 = {BitmapValue(6), BitmapValue(7),  BitmapValue(8),
                                             BitmapValue(9), BitmapValue(10), BitmapValue(11)};
        Block block {ColumnHelper::create_column_with_name<DataTypeBitMap>(bitmaps2),
                     ColumnHelper::create_nullable_column_with_name<DataTypeInt64>(
                             {2, 2, 2, 2, 4, 4}, {false, false, false, false, false, false})};
        std::cout << block.dump_data() << std::endl;
        auto st = op->push(state.get(), &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();

        EXPECT_EQ(local_state->_get_hash_table_size(), 4);
        EXPECT_TRUE(op->need_more_input_data(state.get()));
    }

    {
        bool eos = false;
        Block block;
        auto st = op->pull(state.get(), &block, &eos);
        std::cout << block.dump_data() << std::endl;
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(eos);
        EXPECT_EQ(block.rows(), 4);
        std::vector<BitmapValue> bitmaps_res = {BitmapValue({1, 2}),
                                                BitmapValue({3, 4, 5, 6, 7, 8, 9}),
                                                BitmapValue({10, 11}), BitmapValue(6)};
        Block res_block {ColumnHelper::create_nullable_column_with_name<DataTypeInt64>(
                                 {1, 2, 4, 5}, {false, false, false, true}),
                         ColumnHelper::create_column_with_name<DataTypeBitMap>(bitmaps_res)};
        // In the past, because of the to_string implementation problem of bitmap, the specific implementation of different interfaces of two to_strings was different, resulting in different results.
        // Annotate the case for the time being, and delete one of the bottoms in the futur
        // EXPECT_TRUE(ColumnHelper::block_equal_with_sort(block, res_block))
        //         << "Expected: " << res_block.dump_data() << ", but got: " << block.dump_data();
    }

    { EXPECT_TRUE(local_state->close(state.get()).ok()); }
}

} // namespace doris
