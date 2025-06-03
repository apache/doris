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

#include "pipeline/exec/table_function_operator.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "operator_helper.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_literal_expr.h"
#include "testutil/mock/mock_slot_ref.h"
#include "vec/core/block.h"
namespace doris::pipeline {

using namespace vectorized;

class MockTableFunctionChildOperator : public OperatorXBase {
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

struct MockTableFunctionOperatorX : public TableFunctionOperatorX {
    MockTableFunctionOperatorX() = default;
    RowDescriptor& row_descriptor() override { return *_mock_row_descriptor; }
    std::unique_ptr<MockRowDescriptor> _mock_row_descriptor;
};

class MockTableFunction : public TableFunction {
public:
    MockTableFunction() = default;
    ~MockTableFunction() override = default;

    Status process_init(Block* block, RuntimeState* state) override {
        std::cout << "process_init" << std::endl;
        return Status::OK();
    }
    void process_row(size_t row_idx) override {
        std::cout << "process_row" << std::endl;

        _cur_size = 5;
    }
    void process_close() override {}
    void get_same_many_values(MutableColumnPtr& column, int length) override {
        std::cout << "get_same_many_values" << std::endl;
        column->insert_many_defaults(length);
    }
    int get_value(MutableColumnPtr& column, int max_step) override {
        std::cout << "get_value" << std::endl;
        max_step = std::min(max_step, (int)(_cur_size - _cur_offset));
        column->insert_many_defaults(max_step);
        forward(max_step);
        return max_step;
    }
};

struct MockTableFunctionLocalState : TableFunctionLocalState {
    MockTableFunctionLocalState(RuntimeState* state, OperatorXBase* parent)
            : TableFunctionLocalState(state, parent) {}

    Status _clone_table_function(RuntimeState* state) override {
        auto& p = _parent->cast<TableFunctionOperatorX>();
        _vfn_ctxs.resize(p._vfn_ctxs.size());
        for (size_t i = 0; i < _vfn_ctxs.size(); i++) {
            RETURN_IF_ERROR(p._vfn_ctxs[i]->clone(state, _vfn_ctxs[i]));
            vectorized::TableFunction* fn = p._fns[i];
            fn->set_expr_context(_vfn_ctxs[i]);
            _fns.push_back(fn);
        }
        return Status::OK();
    }
};

struct TableFunctionOperatorTest : public ::testing::Test {
    void SetUp() override {
        state = std::make_shared<MockRuntimeState>();
        op = std::make_shared<MockTableFunctionOperatorX>();
        child_op = std::make_shared<MockTableFunctionChildOperator>();
        op->_child = child_op;
        fns.clear();
    }

    std::shared_ptr<MockTableFunctionOperatorX> op;
    std::shared_ptr<MockTableFunctionChildOperator> child_op;

    std::shared_ptr<MockRuntimeState> state;

    RuntimeProfile profile {""};

    std::unique_ptr<MockTableFunctionLocalState> local_state_uptr;

    MockTableFunctionLocalState* local_state;

    std::vector<std::shared_ptr<TableFunction>> fns;

    ObjectPool pool;
};

TEST_F(TableFunctionOperatorTest, single_fn_test) {
    {
        op->_vfn_ctxs =
                MockSlotRef::create_mock_contexts(DataTypes {std::make_shared<DataTypeInt32>()});
        auto fn = std::make_shared<MockTableFunction>();
        fns.push_back(fn);
        op->_fns.push_back(fn.get());
        op->_output_slot_ids.push_back(true);
        child_op->_mock_row_desc.reset(new MockRowDescriptor {{}, &pool});
        op->_mock_row_descriptor.reset(
                new MockRowDescriptor {{std::make_shared<vectorized::DataTypeInt32>()}, &pool});
        op->_fn_num = 1;
        EXPECT_TRUE(op->prepare(state.get()));

        local_state_uptr = std::make_unique<MockTableFunctionLocalState>(state.get(), op.get());
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

    {
        Block block = ColumnHelper::create_block<DataTypeInt32>({1});
        *local_state->_child_block = ColumnHelper::create_block<DataTypeInt32>({1});
        auto st = op->push(state.get(), local_state->_child_block.get(), true);
        EXPECT_TRUE(st) << st.msg();
    }
    {
        Block block;
        bool eos = false;
        auto st = op->pull(state.get(), &block, &eos);
        EXPECT_TRUE(st) << st.msg();
        std::cout << block.dump_data() << std::endl;
        std::cout << eos << std::endl;
        EXPECT_TRUE(ColumnHelper::block_equal(
                block, ColumnHelper::create_block<DataTypeInt32>({0, 0, 0, 0, 0})));
    }
}

TEST_F(TableFunctionOperatorTest, single_fn_test2) {
    {
        op->_vfn_ctxs =
                MockSlotRef::create_mock_contexts(DataTypes {std::make_shared<DataTypeInt32>()});
        auto fn = std::make_shared<MockTableFunction>();
        fns.push_back(fn);
        op->_fns.push_back(fn.get());
        op->_output_slot_ids.push_back(true);
        child_op->_mock_row_desc.reset(
                new MockRowDescriptor {{std::make_shared<vectorized::DataTypeInt32>()}, &pool});
        op->_mock_row_descriptor.reset(
                new MockRowDescriptor {{std::make_shared<vectorized::DataTypeInt32>(),
                                        std::make_shared<vectorized::DataTypeInt32>()},
                                       &pool});
        op->_fn_num = 1;
        EXPECT_TRUE(op->prepare(state.get()));

        local_state_uptr = std::make_unique<MockTableFunctionLocalState>(state.get(), op.get());
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

    {
        Block block = ColumnHelper::create_block<DataTypeInt32>({1});
        *local_state->_child_block = ColumnHelper::create_block<DataTypeInt32>({1});
        auto st = op->push(state.get(), local_state->_child_block.get(), true);
        EXPECT_TRUE(st) << st.msg();
    }
    {
        Block block;
        bool eos = false;
        auto st = op->pull(state.get(), &block, &eos);
        EXPECT_TRUE(st) << st.msg();
        std::cout << block.dump_data() << std::endl;
        std::cout << eos << std::endl;
        EXPECT_TRUE(ColumnHelper::block_equal(block, ColumnHelper::create_block<DataTypeInt32>(
                                                             {1, 1, 1, 1, 1}, {0, 0, 0, 0, 0})));
    }
}

TEST_F(TableFunctionOperatorTest, single_two_test) {
    {
        op->_vfn_ctxs = MockSlotRef::create_mock_contexts(
                DataTypes {std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeInt32>()});
        {
            auto fn = std::make_shared<MockTableFunction>();
            fns.push_back(fn);
            op->_fns.push_back(fn.get());
            op->_output_slot_ids.push_back(true);
        }
        {
            auto fn = std::make_shared<MockTableFunction>();
            fns.push_back(fn);
            op->_fns.push_back(fn.get());
            op->_output_slot_ids.push_back(true);
        }

        child_op->_mock_row_desc.reset(new MockRowDescriptor {{}, &pool});
        op->_mock_row_descriptor.reset(
                new MockRowDescriptor {{std::make_shared<vectorized::DataTypeInt32>(),
                                        std::make_shared<vectorized::DataTypeInt32>()},
                                       &pool});
        op->_fn_num = 2;
        EXPECT_TRUE(op->prepare(state.get()));

        local_state_uptr = std::make_unique<MockTableFunctionLocalState>(state.get(), op.get());
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

    {
        Block block = ColumnHelper::create_block<DataTypeInt32>({1});
        *local_state->_child_block = ColumnHelper::create_block<DataTypeInt32>({1});
        auto st = op->push(state.get(), local_state->_child_block.get(), true);
        EXPECT_TRUE(st) << st.msg();
    }
    {
        Block block;
        bool eos = false;
        auto st = op->pull(state.get(), &block, &eos);
        EXPECT_TRUE(st) << st.msg();
        std::cout << block.dump_data() << std::endl;
        std::cout << eos << std::endl;
        EXPECT_TRUE(ColumnHelper::block_equal(
                block,
                ColumnHelper::create_block<DataTypeInt32>(
                        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                         0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})));
    }
}

TEST_F(TableFunctionOperatorTest, single_two_eos_test) {
    {
        op->_vfn_ctxs = MockSlotRef::create_mock_contexts(
                DataTypes {std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeInt32>()});
        {
            auto fn = std::make_shared<MockTableFunction>();
            fns.push_back(fn);
            op->_fns.push_back(fn.get());
            op->_output_slot_ids.push_back(true);
        }
        {
            auto fn = std::make_shared<MockTableFunction>();
            fns.push_back(fn);
            op->_fns.push_back(fn.get());
            op->_output_slot_ids.push_back(true);
        }

        child_op->_mock_row_desc.reset(new MockRowDescriptor {{}, &pool});
        op->_mock_row_descriptor.reset(
                new MockRowDescriptor {{std::make_shared<vectorized::DataTypeInt32>(),
                                        std::make_shared<vectorized::DataTypeInt32>()},
                                       &pool});
        op->_fn_num = 2;
        EXPECT_TRUE(op->prepare(state.get()));

        local_state_uptr = std::make_unique<MockTableFunctionLocalState>(state.get(), op.get());
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

    {
        Block block = ColumnHelper::create_block<DataTypeInt32>({1});
        *local_state->_child_block = ColumnHelper::create_block<DataTypeInt32>({1});
        auto st = op->push(state.get(), local_state->_child_block.get(), false);
        EXPECT_TRUE(st) << st.msg();
    }
    {
        Block block;
        bool eos = false;
        auto st = op->pull(state.get(), &block, &eos);
        EXPECT_TRUE(st) << st.msg();
        std::cout << block.dump_data() << std::endl;
        std::cout << eos << std::endl;
        EXPECT_TRUE(ColumnHelper::block_equal(
                block,
                ColumnHelper::create_block<DataTypeInt32>(
                        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                         0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})));
    }

    {
        Block block = ColumnHelper::create_block<DataTypeInt32>({1});
        *local_state->_child_block = ColumnHelper::create_block<DataTypeInt32>({1});
        local_state->_child_eos = true;
        auto st = op->push(state.get(), local_state->_child_block.get(), true);
        EXPECT_TRUE(st) << st.msg();
    }
    {
        Block block;
        bool eos = false;
        auto st = op->pull(state.get(), &block, &eos);
        EXPECT_TRUE(st) << st.msg();
        EXPECT_TRUE(eos);
    }
}

} // namespace doris::pipeline