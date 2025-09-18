
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

#include "pipeline/exec/aggregation_sink_operator.h"
#include "pipeline/exec/aggregation_source_operator.h"
#include "pipeline/exec/mock_operator.h"
#include "pipeline/exec/streaming_aggregation_operator.h"
#include "pipeline/operator/operator_helper.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_agg_fn_evaluator.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"
#include "util/bitmap_value.h"
#include "util/jsonb_document.h"
#include "vec/data_types/data_type_bitmap.h"
#include "vec/data_types/data_type_number.h"

namespace doris::pipeline {

using namespace vectorized;

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
        static_cast<void>(
                StreamingAggLocalState::_should_not_do_pre_agg(rows)); // mock the function
        return should_not_do_pre_agg;
    }

    bool should_not_do_pre_agg = false;
};

class MockStreamingAggOperatorChildOperator : public OperatorXBase {
public:
    Status get_block_after_projects(RuntimeState* state, vectorized::Block* block,
                                    bool* eos) override {
        return Status::OK();
    }

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos) override {
        return Status::OK();
    }
    Status setup_local_state(RuntimeState* state, LocalStateInfo& info) override {
        return Status::OK();
    }

    const RowDescriptor& row_desc() const override { return *_mock_row_desc; }

private:
    std::unique_ptr<MockRowDescriptor> _mock_row_desc;
};
struct StreamingAggOperatorTest : public testing::Test {
    void SetUp() override {
        state = std::make_shared<MockRuntimeState>();
        op = std::make_shared<MockStreamingAggOperatorX>();
        child_op = std::make_shared<MockStreamingAggOperatorChildOperator>();
        child_op->_mock_row_desc.reset(
                new MockRowDescriptor {{std::make_shared<vectorized::DataTypeInt64>(),
                                        std::make_shared<vectorized::DataTypeInt64>()},
                                       &pool});
    }

    std::shared_ptr<MockStreamingAggOperatorX> op;
    std::shared_ptr<MockStreamingAggOperatorChildOperator> child_op;

    std::shared_ptr<MockRuntimeState> state;

    RuntimeProfile profile {""};

    MockStreamingAggLocalState* local_state = nullptr;

    ObjectPool pool;
};

TEST_F(StreamingAggOperatorTest, test1) {
    op->_aggregate_evaluators.push_back(vectorized::create_mock_agg_fn_evaluator(
            pool, MockSlotRef::create_mock_contexts(1, std::make_shared<DataTypeInt64>()), false,
            false));
    op->_pool = &pool;
    op->_needs_finalize = false;
    op->_is_merge = false;

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
        vectorized::Block block {
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 2, 2, 2, 3}),
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 100, 100, 100, 1000})};
        auto st = op->push(state.get(), &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();

        EXPECT_EQ(local_state->_get_hash_table_size(), 3);
        EXPECT_TRUE(op->need_more_input_data(state.get()));
    }

    {
        vectorized::Block block {
                ColumnHelper::create_column_with_name<DataTypeInt64>({2, 2, 2, 2, 4, 4}),
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 100, 100, 100, 1000})};
        auto st = op->push(state.get(), &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();

        EXPECT_EQ(local_state->_get_hash_table_size(), 4);
        EXPECT_TRUE(op->need_more_input_data(state.get()));
    }

    { EXPECT_TRUE(local_state->close(state.get()).ok()); }
}

TEST_F(StreamingAggOperatorTest, test2) {
    op->_aggregate_evaluators.push_back(vectorized::create_mock_agg_fn_evaluator(
            pool, MockSlotRef::create_mock_contexts(1, std::make_shared<DataTypeInt64>()), false,
            false));
    op->_pool = &pool;
    op->_needs_finalize = false;
    op->_is_merge = false;

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
        vectorized::Block block {
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 2, 2, 2, 3}),
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 100, 100, 100, 1000})};
        auto st = op->push(state.get(), &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();

        EXPECT_EQ(local_state->_get_hash_table_size(), 3);
        EXPECT_TRUE(op->need_more_input_data(state.get()));
    }

    {
        local_state->should_not_do_pre_agg = true;
        vectorized::Block block {
                ColumnHelper::create_column_with_name<DataTypeInt64>({2, 2, 2, 2, 4, 4}),
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 100, 100, 100, 1000})};
        auto st = op->push(state.get(), &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();

        EXPECT_EQ(local_state->_get_hash_table_size(), 3);
        EXPECT_FALSE(op->need_more_input_data(state.get()));
    }

    {
        bool eos = false;
        vectorized::Block block;
        auto st = op->pull(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_FALSE(eos);
        EXPECT_EQ(block.rows(), 6);
    }

    {
        op->_make_nullable_keys = {0}; // make key nullable
        bool eos = false;
        vectorized::Block block;
        auto st = op->pull(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(eos);
        EXPECT_EQ(block.rows(), 3);
    }

    { EXPECT_TRUE(local_state->close(state.get()).ok()); }
}

TEST_F(StreamingAggOperatorTest, test3) {
    op->_aggregate_evaluators.push_back(vectorized::create_mock_agg_fn_evaluator(
            pool, MockSlotRef::create_mock_contexts(1, std::make_shared<DataTypeInt64>()), false,
            false));
    op->_pool = &pool;
    op->_needs_finalize = false;
    op->_is_merge = false;

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
        vectorized::Block block {
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
        vectorized::Block block {
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
        vectorized::Block block;
        auto st = op->pull(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_FALSE(eos);
        EXPECT_EQ(block.rows(), 6);
    }

    {
        op->_make_nullable_keys = {0}; // make key nullable
        bool eos = false;
        vectorized::Block block;
        auto st = op->pull(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(eos);
        EXPECT_EQ(block.rows(), 3);
    }

    { EXPECT_TRUE(local_state->close(state.get()).ok()); }
}

TEST_F(StreamingAggOperatorTest, test4) {
    op->_aggregate_evaluators.push_back(vectorized::create_agg_fn(
            pool, "bitmap_union", {std::make_shared<DataTypeBitMap>()}, false));
    op->_pool = &pool;
    op->_needs_finalize = false;
    op->_is_merge = false;

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

        vectorized::Block block {
                ColumnHelper::create_column_with_name<DataTypeBitMap>(bitmaps),
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
        vectorized::Block block {
                ColumnHelper::create_column_with_name<DataTypeBitMap>(bitmaps2),
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
        vectorized::Block block;
        auto st = op->pull(state.get(), &block, &eos);
        std::cout << block.dump_data() << std::endl;
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(eos);
        EXPECT_EQ(block.rows(), 4);
        std::vector<BitmapValue> bitmaps_res = {BitmapValue({1, 2}),
                                                BitmapValue({3, 4, 5, 6, 7, 8, 9}),
                                                BitmapValue({10, 11}), BitmapValue(6)};
        vectorized::Block res_block {
                ColumnHelper::create_nullable_column_with_name<DataTypeInt64>(
                        {1, 2, 4, 5}, {false, false, false, true}),
                ColumnHelper::create_column_with_name<DataTypeBitMap>(bitmaps_res)};
        EXPECT_TRUE(ColumnHelper::block_equal_with_sort(block, res_block))
                << "Expected: " << res_block.dump_data() << ", but got: " << block.dump_data();
    }

    { EXPECT_TRUE(local_state->close(state.get()).ok()); }
}

} // namespace doris::pipeline
