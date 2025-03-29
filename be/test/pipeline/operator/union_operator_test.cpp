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
#include <vector>

#include "operator_helper.h"
#include "pipeline/exec/mock_operator.h"
#include "pipeline/exec/union_sink_operator.h"
#include "pipeline/exec/union_source_operator.h"
#include "pipeline/operator/operator_helper.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_literal_expr.h"
#include "testutil/mock/mock_slot_ref.h"
#include "vec/core/block.h"
namespace doris::pipeline {

using namespace vectorized;

struct MockUnionSourceOperator : public UnionSourceOperatorX {
    MockUnionSourceOperator(int32_t child_size, DataTypes types, ObjectPool* pool)
            : UnionSourceOperatorX(child_size), _mock_row_descriptor(types, pool) {}
    RowDescriptor& row_descriptor() override { return _mock_row_descriptor; }
    MockRowDescriptor _mock_row_descriptor;
};

struct MockUnionSinkOperator : public UnionSinkOperatorX {
    MockUnionSinkOperator(int child_size, int cur_child_id, int first_materialized_child_idx,
                          DataTypes types, ObjectPool* pool)
            : UnionSinkOperatorX(child_size, cur_child_id, first_materialized_child_idx),
              _mock_row_descriptor(types, pool) {}

    RowDescriptor& row_descriptor() override { return _mock_row_descriptor; }
    MockRowDescriptor _mock_row_descriptor;
};

struct UnionOperatorTest : public ::testing::Test {
    void SetUp() override {
        state = std::make_shared<MockRuntimeState>();
        state->batsh_size = 10;
        for (int i = 0; i < child_size; i++) {
            sink_state.push_back(std::make_shared<MockRuntimeState>());
            sink_ops.push_back(nullptr);
        }
    }

    std::shared_ptr<MockUnionSourceOperator> source_op;
    UnionSourceLocalState* source_local_state;

    std::shared_ptr<MockRuntimeState> state;

    RuntimeProfile profile {""};

    ObjectPool pool;

    const int child_size = 3;
    const int first_materialized_child_idx = 1;

    std::vector<std::shared_ptr<MockUnionSinkOperator>> sink_ops;
    std::vector<std::shared_ptr<MockRuntimeState>> sink_state;
};

TEST_F(UnionOperatorTest, test_all_const_expr) {
    state->batsh_size = 2;
    source_op.reset(new MockUnionSourceOperator {
            0,
            {std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt64>(),
             std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt64>()},
            &pool});
    EXPECT_TRUE(source_op->prepare(state.get()));
    source_op->_const_expr_lists.push_back(MockLiteral::create<DataTypeInt64>({1, 10, 100, 1000}));
    source_op->_const_expr_lists.push_back(MockLiteral::create<DataTypeInt64>({2, 20, 200, 2000}));
    source_op->_const_expr_lists.push_back(MockLiteral::create<DataTypeInt64>({3, 30, 300, 3000}));
    source_op->_const_expr_lists.push_back(MockLiteral::create<DataTypeInt64>({4, 40, 400, 4000}));
    source_op->_const_expr_lists.push_back(MockLiteral::create<DataTypeInt64>({5, 50, 500, 5000}));
    auto source_local_state_uptr =
            std::make_unique<UnionSourceLocalState>(state.get(), source_op.get());
    source_local_state = source_local_state_uptr.get();
    LocalStateInfo info {.parent_profile = &profile,
                         .scan_ranges = {},
                         .shared_state = nullptr,
                         .shared_state_map = {},
                         .task_idx = 0};
    EXPECT_TRUE(source_local_state->init(state.get(), info));
    state->resize_op_id_to_local_state(-100);
    state->emplace_local_state(source_op->operator_id(), std::move(source_local_state_uptr));
    EXPECT_TRUE(source_local_state->open(state.get()));
    EXPECT_EQ(source_local_state->dependencies().size(), 1);
    EXPECT_TRUE(OperatorHelper::is_ready(source_local_state->dependencies()));

    EXPECT_TRUE(source_local_state->_need_read_for_const_expr);
    EXPECT_EQ(source_local_state->_const_expr_list_idx, 0);

    EXPECT_TRUE(source_op->has_more_const(state.get()));

    {
        Block block;
        bool eos;
        EXPECT_TRUE(source_op->get_block(state.get(), &block, &eos).ok());
        EXPECT_EQ(block.rows(), 2);
        EXPECT_FALSE(eos);
        std::cout << block.dump_data() << std::endl;
        EXPECT_TRUE(ColumnHelper::block_equal(
                block, Block {
                               ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2}),
                               ColumnHelper::create_column_with_name<DataTypeInt64>({10, 20}),
                               ColumnHelper::create_column_with_name<DataTypeInt64>({100, 200}),
                               ColumnHelper::create_column_with_name<DataTypeInt64>({1000, 2000}),
                       }));
    }

    {
        Block block;
        bool eos;
        EXPECT_TRUE(source_op->get_block(state.get(), &block, &eos).ok());
        EXPECT_EQ(block.rows(), 2);
        EXPECT_FALSE(eos);
        std::cout << block.dump_data() << std::endl;
        EXPECT_TRUE(ColumnHelper::block_equal(
                block, Block {
                               ColumnHelper::create_column_with_name<DataTypeInt64>({3, 4}),
                               ColumnHelper::create_column_with_name<DataTypeInt64>({30, 40}),
                               ColumnHelper::create_column_with_name<DataTypeInt64>({300, 400}),
                               ColumnHelper::create_column_with_name<DataTypeInt64>({3000, 4000}),
                       }));
    }

    {
        Block block;
        bool eos;
        EXPECT_TRUE(source_op->get_block(state.get(), &block, &eos).ok());
        EXPECT_EQ(block.rows(), 1);
        EXPECT_TRUE(eos);
        std::cout << block.dump_data() << std::endl;
        EXPECT_TRUE(ColumnHelper::block_equal(
                block, Block {
                               ColumnHelper::create_column_with_name<DataTypeInt64>({5}),
                               ColumnHelper::create_column_with_name<DataTypeInt64>({50}),
                               ColumnHelper::create_column_with_name<DataTypeInt64>({500}),
                               ColumnHelper::create_column_with_name<DataTypeInt64>({5000}),
                       }));
    }
}

TEST_F(UnionOperatorTest, test_sink_and_source) {
    DataTypes types = {std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt64>()};
    source_op.reset(new MockUnionSourceOperator {child_size, types, &pool});
    EXPECT_TRUE(source_op->prepare(state.get()));
    source_op->_const_expr_lists.push_back(MockLiteral::create<DataTypeInt64>({1, 10}));
    source_op->_const_expr_lists.push_back(MockLiteral::create<DataTypeInt64>({2, 20}));
    source_op->_const_expr_lists.push_back(MockLiteral::create<DataTypeInt64>({3, 30}));
    source_op->_const_expr_lists.push_back(MockLiteral::create<DataTypeInt64>({4, 40}));
    source_op->_const_expr_lists.push_back(MockLiteral::create<DataTypeInt64>({5, 50}));

    for (int i = 0; i < child_size; i++) {
        sink_ops[i].reset(new MockUnionSinkOperator {child_size, i, first_materialized_child_idx,
                                                     types, &pool});
        sink_ops[i]->_child_expr = MockSlotRef::create_mock_contexts(types);
    }

    auto shared_state = sink_ops[0]->create_shared_state();

    //auto & data_queue = dynamic_cast<UnionSharedState*>(shared_state.get())->data_queue;
    EXPECT_TRUE(shared_state != nullptr);
    EXPECT_TRUE(sink_ops[1]->create_shared_state() == nullptr);
    EXPECT_TRUE(sink_ops[2]->create_shared_state() == nullptr);

    {
        auto source_local_state_uptr =
                std::make_unique<UnionSourceLocalState>(state.get(), source_op.get());
        source_local_state = source_local_state_uptr.get();

        LocalStateInfo info {.parent_profile = &profile,
                             .scan_ranges = {},
                             .shared_state = shared_state.get(),
                             .shared_state_map = {},
                             .task_idx = 0};
        EXPECT_TRUE(source_local_state->init(state.get(), info));
        state->resize_op_id_to_local_state(-100);
        state->emplace_local_state(source_op->operator_id(), std::move(source_local_state_uptr));
        EXPECT_TRUE(source_local_state->open(state.get()));
        EXPECT_EQ(source_local_state->dependencies().size(), 1);
        EXPECT_TRUE(OperatorHelper::is_block(source_local_state->dependencies()));
    }

    {
        for (int i = 0; i < child_size; i++) {
            auto sink_local_state_uptr =
                    std::make_unique<UnionSinkLocalState>(sink_ops[i].get(), sink_state[i].get());
            auto* sink_local_state = sink_local_state_uptr.get();
            LocalSinkStateInfo info {.task_idx = 0,
                                     .parent_profile = &profile,
                                     .sender_id = 0,
                                     .shared_state = shared_state.get(),
                                     .shared_state_map = {},
                                     .tsink = TDataSink {}};
            EXPECT_TRUE(sink_local_state->init(sink_state[i].get(), info));
            sink_state[i]->resize_op_id_to_local_state(-100);
            sink_state[i]->emplace_sink_local_state(sink_ops[i]->operator_id(),
                                                    std::move(sink_local_state_uptr));
            EXPECT_TRUE(sink_local_state->open(sink_state[i].get()));
            EXPECT_EQ(sink_local_state->dependencies().size(), 1);
            EXPECT_TRUE(OperatorHelper::is_ready(sink_local_state->dependencies()));
        }
    }

    {
        Block block = ColumnHelper::create_block<DataTypeInt64>({1, 2}, {3, 4});
        EXPECT_TRUE(sink_ops[0]->sink(sink_state[0].get(), &block, false));
    }
    {
        Block block;
        bool eos;
        EXPECT_TRUE(source_op->get_block(state.get(), &block, &eos).ok());
        EXPECT_EQ(block.rows(), 5);
        EXPECT_FALSE(eos);
        std::cout << block.dump_data() << std::endl;
        EXPECT_TRUE(ColumnHelper::block_equal(
                block,
                Block {
                        ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3, 4, 5}),
                        ColumnHelper::create_column_with_name<DataTypeInt64>({10, 20, 30, 40, 50}),
                }));
    }

    EXPECT_TRUE(OperatorHelper::is_ready(source_local_state->dependencies()));

    {
        Block block;
        bool eos;
        EXPECT_TRUE(source_op->get_block(state.get(), &block, &eos).ok());
        EXPECT_EQ(block.rows(), 2);
        EXPECT_FALSE(eos);
        std::cout << block.dump_data() << std::endl;
        EXPECT_TRUE(ColumnHelper::block_equal(
                block, ColumnHelper::create_block<DataTypeInt64>({1, 2}, {3, 4})));
    }

    EXPECT_TRUE(OperatorHelper::is_block(source_local_state->dependencies()));

    {
        for (int i = 0; i < child_size; i++) {
            sink_state[i]->batsh_size = 2;
            Block block = ColumnHelper::create_block<DataTypeInt64>({1, 2}, {3, 4});
            EXPECT_TRUE(sink_ops[i]->sink(sink_state[i].get(), &block, false));
        }
        for (int i = 0; i < child_size; i++) {
            Block block = ColumnHelper::create_block<DataTypeInt64>({1, 2}, {3, 4});
            EXPECT_TRUE(sink_ops[i]->sink(sink_state[i].get(), &block, false));
        }
        for (int i = 0; i < child_size; i++) {
            Block block = ColumnHelper::create_block<DataTypeInt64>({1, 2}, {3, 4});
            EXPECT_TRUE(sink_ops[i]->sink(sink_state[i].get(), &block, true));
        }
    }

    EXPECT_TRUE(OperatorHelper::is_ready(source_local_state->dependencies()));
    for (int i = 0; i < 8; i++) {
        Block block;
        bool eos;
        EXPECT_TRUE(source_op->get_block(state.get(), &block, &eos).ok());
        EXPECT_EQ(block.rows(), 2);
        EXPECT_FALSE(eos);
        std::cout << block.dump_data() << std::endl;
        EXPECT_TRUE(ColumnHelper::block_equal(
                block, ColumnHelper::create_block<DataTypeInt64>({1, 2}, {3, 4})));
    }

    {
        Block block;
        bool eos;
        EXPECT_TRUE(source_op->get_block(state.get(), &block, &eos).ok());
        EXPECT_EQ(block.rows(), 2);
        EXPECT_TRUE(eos);
        std::cout << block.dump_data() << std::endl;
        EXPECT_TRUE(ColumnHelper::block_equal(
                block, ColumnHelper::create_block<DataTypeInt64>({1, 2}, {3, 4})));
    }
}
} // namespace doris::pipeline