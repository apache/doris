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
#include "pipeline/exec/set_probe_sink_operator.h"
#include "pipeline/exec/set_sink_operator.h"
#include "pipeline/exec/set_source_operator.h"
#include "pipeline/operator/operator_helper.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_literal_expr.h"
#include "testutil/mock/mock_slot_ref.h"
#include "vec/core/block.h"
namespace doris::pipeline {

using namespace vectorized;

template <bool is_intersect>
struct MockSetSourceOperatorX : public SetSourceOperatorX<is_intersect> {
    MockSetSourceOperatorX(int child_size, DataTypes types, ObjectPool* pool)
            : SetSourceOperatorX<is_intersect>(child_size), _mock_row_descriptor(types, pool) {}
    RowDescriptor& row_descriptor() override { return _mock_row_descriptor; }
    MockRowDescriptor _mock_row_descriptor;
};

template <bool is_intersect>
struct SetOperatorTest : public ::testing::Test {
    void SetUp() override {
        state = std::make_shared<MockRuntimeState>();
        state->batsh_size = 5;
    }

    void init_op(int child_size, DataTypes output_type) {
        source_op.reset(new MockSetSourceOperatorX<is_intersect>(child_size, output_type, &pool));

        for (int i = 0; i < child_size; i++) {
            if (i == 0) {
                sink_op.reset(new SetSinkOperatorX<is_intersect>(child_size));
            } else {
                probe_sink_ops.push_back(std::make_shared<SetProbeSinkOperatorX<is_intersect>>(i));
                states.push_back(std::make_shared<MockRuntimeState>());
            }
        }
    }

    void init_local_state() {
        auto source_local_state_uptr =
                std::make_unique<SetSourceLocalState<is_intersect>>(state.get(), source_op.get());
        source_local_state = source_local_state_uptr.get();

        auto sink_local_state_uptr =
                std::make_unique<SetSinkLocalState<is_intersect>>(sink_op.get(), state.get());
        sink_local_state = sink_local_state_uptr.get();

        shared_state_sptr = sink_op->create_shared_state();

        EXPECT_TRUE(shared_state_sptr);

        {
            LocalStateInfo info {.parent_profile = &profile,
                                 .scan_ranges = {},
                                 .shared_state = shared_state_sptr.get(),
                                 .shared_state_map = {},
                                 .task_idx = 0};
            EXPECT_TRUE(source_local_state->init(state.get(), info));
            state->resize_op_id_to_local_state(-100);
            state->emplace_local_state(source_op->operator_id(),
                                       std::move(source_local_state_uptr));
        }

        {
            LocalSinkStateInfo info {.task_idx = 0,
                                     .parent_profile = &profile,
                                     .sender_id = 0,
                                     .shared_state = shared_state_sptr.get(),
                                     .shared_state_map = {},
                                     .tsink = TDataSink {}};
            EXPECT_TRUE(sink_local_state->init(state.get(), info));
            state->emplace_sink_local_state(sink_op->operator_id(),
                                            std::move(sink_local_state_uptr));
            EXPECT_TRUE(sink_local_state->open(state.get()));
        }

        for (int i = 0; i < probe_sink_ops.size(); i++) {
            auto fake_shared_state = probe_sink_ops[i]->create_shared_state();

            EXPECT_FALSE(fake_shared_state);

            auto probe_sink_local_state_uptr =
                    std::make_unique<SetProbeSinkLocalState<is_intersect>>(probe_sink_ops[i].get(),
                                                                           states[i].get());
            probe_sink_local_state.push_back(probe_sink_local_state_uptr.get());
            LocalSinkStateInfo info {.task_idx = 0,
                                     .parent_profile = &profile,
                                     .sender_id = 0,
                                     .shared_state = shared_state_sptr.get(),
                                     .shared_state_map = {},
                                     .tsink = TDataSink {}};
            EXPECT_TRUE(probe_sink_local_state[i]->init(states[i].get(), info));
            states[i]->emplace_sink_local_state(probe_sink_ops[i]->operator_id(),
                                                std::move(probe_sink_local_state_uptr));
            EXPECT_TRUE(probe_sink_local_state[i]->open(states[i].get()));
        }

        { EXPECT_TRUE(source_local_state->open(state.get())); }

        shared_state = source_local_state->_shared_state;
    }

    std::shared_ptr<MockSetSourceOperatorX<is_intersect>> source_op;
    SetSourceLocalState<is_intersect>* source_local_state;

    std::shared_ptr<MockRuntimeState> state;

    RuntimeProfile profile {""};

    ObjectPool pool;

    std::shared_ptr<SetSinkOperatorX<is_intersect>> sink_op;
    SetSinkLocalState<is_intersect>* sink_local_state;

    std::vector<std::shared_ptr<SetProbeSinkOperatorX<is_intersect>>> probe_sink_ops;
    std::vector<SetProbeSinkLocalState<is_intersect>*> probe_sink_local_state;
    std::vector<std::shared_ptr<MockRuntimeState>> states;

    std::shared_ptr<BasicSharedState> shared_state_sptr;

    SetSharedState* shared_state;
};

struct IntersectOperatorTest : public SetOperatorTest<true> {};
struct ExceptOperatorTest : public SetOperatorTest<false> {};
TEST_F(IntersectOperatorTest, test_all_const_expr) {
    init_op(2, {std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt64>(),
                std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt64>()});
    auto const_exprs = MockLiteral::create_const<DataTypeInt64>({1, 10, 100, 1000}, 3);
    sink_op->_child_exprs = const_exprs;
    probe_sink_ops[0]->_child_exprs = const_exprs;

    init_local_state();

    EXPECT_EQ(shared_state->probe_finished_children_dependency.size(), 2);
    EXPECT_EQ(probe_sink_local_state[0]->dependencies().size(), 1);
    EXPECT_TRUE(OperatorHelper::is_ready(sink_local_state->dependencies()));
    EXPECT_TRUE(OperatorHelper::is_block(probe_sink_local_state[0]->dependencies()));
    EXPECT_TRUE(OperatorHelper::is_block(source_local_state->dependencies()));

    {
        Block block = ColumnHelper::create_block<DataTypeInt64>({114514, 2, 3});
        EXPECT_TRUE(sink_op->sink(state.get(), &block, true));
    }

    EXPECT_TRUE(OperatorHelper::is_ready(probe_sink_local_state[0]->dependencies()));

    {
        Block block = ColumnHelper::create_block<DataTypeInt64>({114514, 2, 3});
        EXPECT_TRUE(probe_sink_ops[0]->sink(states[0].get(), &block, true));
    }

    {
        Block block;
        bool eos = false;
        EXPECT_TRUE(source_op->get_block(state.get(), &block, &eos));

        EXPECT_TRUE(ColumnHelper::block_equal(
                block, Block {ColumnHelper::create_column_with_name<DataTypeInt64>({1}),
                              ColumnHelper::create_column_with_name<DataTypeInt64>({10}),
                              ColumnHelper::create_column_with_name<DataTypeInt64>({100}),
                              ColumnHelper::create_column_with_name<DataTypeInt64>({1000})}));
    }
}

TEST_F(ExceptOperatorTest, test_all_const_expr) {
    init_op(2, {std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt64>(),
                std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt64>()});
    auto const_exprs = MockLiteral::create_const<DataTypeInt64>({1, 10, 100, 1000}, 3);
    sink_op->_child_exprs = const_exprs;
    probe_sink_ops[0]->_child_exprs = const_exprs;

    init_local_state();

    EXPECT_EQ(shared_state->probe_finished_children_dependency.size(), 2);
    EXPECT_EQ(probe_sink_local_state[0]->dependencies().size(), 1);
    EXPECT_TRUE(OperatorHelper::is_ready(sink_local_state->dependencies()));
    EXPECT_TRUE(OperatorHelper::is_block(probe_sink_local_state[0]->dependencies()));
    EXPECT_TRUE(OperatorHelper::is_block(source_local_state->dependencies()));

    {
        Block block = ColumnHelper::create_block<DataTypeInt64>({114514, 2, 3});
        EXPECT_TRUE(sink_op->sink(state.get(), &block, true));
    }

    EXPECT_TRUE(OperatorHelper::is_ready(probe_sink_local_state[0]->dependencies()));

    {
        Block block = ColumnHelper::create_block<DataTypeInt64>({114514, 2, 3});
        EXPECT_TRUE(probe_sink_ops[0]->sink(states[0].get(), &block, true));
    }

    {
        Block block;
        bool eos = false;
        EXPECT_TRUE(source_op->get_block(state.get(), &block, &eos));
        EXPECT_TRUE(block.empty());
    }
}

TEST_F(IntersectOperatorTest, test_build_not_ignore_null) {
    init_op(2, {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())});

    sink_op->_child_exprs =
            MockSlotRef::create_mock_contexts(DataTypes {std::make_shared<DataTypeInt64>()});
    probe_sink_ops[0]->_child_exprs = MockSlotRef::create_mock_contexts(
            DataTypes {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())});

    init_local_state();

    EXPECT_EQ(shared_state->probe_finished_children_dependency.size(), 2);
    EXPECT_EQ(probe_sink_local_state[0]->dependencies().size(), 1);
    EXPECT_TRUE(OperatorHelper::is_ready(sink_local_state->dependencies()));
    EXPECT_TRUE(OperatorHelper::is_block(probe_sink_local_state[0]->dependencies()));
    EXPECT_TRUE(OperatorHelper::is_block(source_local_state->dependencies()));

    EXPECT_EQ(shared_state->build_not_ignore_null.size(), 1);
    EXPECT_EQ(shared_state->build_not_ignore_null[0], true);

    {
        Block block = ColumnHelper::create_block<DataTypeInt64>({1, 2, 3, 4, 5});
        EXPECT_TRUE(sink_op->sink(state.get(), &block, true));
    }

    EXPECT_TRUE(OperatorHelper::is_ready(probe_sink_local_state[0]->dependencies()));

    {
        Block block = ColumnHelper::create_nullable_block<DataTypeInt64>(
                {0, 0, 0, 2, 4}, {true, true, true, false, false});
        EXPECT_TRUE(probe_sink_ops[0]->sink(states[0].get(), &block, true));
    }

    {
        Block block;
        bool eos = false;
        EXPECT_TRUE(source_op->get_block(state.get(), &block, &eos));

        std::cout << block.dump_data() << std::endl;
        EXPECT_TRUE(ColumnHelper::block_equal_with_sort(
                block, ColumnHelper::create_nullable_block<DataTypeInt64>({2, 4}, {false, false})));
    }
}

TEST_F(IntersectOperatorTest, test_output_null) {
    init_op(2, {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())});

    sink_op->_child_exprs = MockSlotRef::create_mock_contexts(
            DataTypes {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())});
    probe_sink_ops[0]->_child_exprs = MockSlotRef::create_mock_contexts(
            DataTypes {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())});

    init_local_state();

    EXPECT_EQ(shared_state->probe_finished_children_dependency.size(), 2);
    EXPECT_EQ(probe_sink_local_state[0]->dependencies().size(), 1);
    EXPECT_TRUE(OperatorHelper::is_ready(sink_local_state->dependencies()));
    EXPECT_TRUE(OperatorHelper::is_block(probe_sink_local_state[0]->dependencies()));
    EXPECT_TRUE(OperatorHelper::is_block(source_local_state->dependencies()));

    {
        Block block = ColumnHelper::create_nullable_block<DataTypeInt64>(
                {1, 2, 3, 2, 4}, {true, false, false, false, false});
        EXPECT_TRUE(sink_op->sink(state.get(), &block, true));
    }

    EXPECT_TRUE(OperatorHelper::is_ready(probe_sink_local_state[0]->dependencies()));

    {
        Block block = ColumnHelper::create_nullable_block<DataTypeInt64>(
                {10000, 0, 0, 2, 4}, {true, true, true, false, false});
        EXPECT_TRUE(probe_sink_ops[0]->sink(states[0].get(), &block, true));
    }

    {
        Block block;
        bool eos = false;
        EXPECT_TRUE(source_op->get_block(state.get(), &block, &eos));

        std::cout << block.dump_data() << std::endl;
        EXPECT_TRUE(ColumnHelper::block_equal_with_sort(
                block, ColumnHelper::create_nullable_block<DataTypeInt64>({2, 4, 0},
                                                                          {false, false, true})));
    }
}

TEST_F(ExceptOperatorTest, test_build_not_ignore_null) {
    init_op(2, {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())});

    sink_op->_child_exprs =
            MockSlotRef::create_mock_contexts(DataTypes {std::make_shared<DataTypeInt64>()});
    probe_sink_ops[0]->_child_exprs = MockSlotRef::create_mock_contexts(
            DataTypes {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())});

    init_local_state();

    EXPECT_EQ(shared_state->probe_finished_children_dependency.size(), 2);
    EXPECT_EQ(probe_sink_local_state[0]->dependencies().size(), 1);
    EXPECT_TRUE(OperatorHelper::is_ready(sink_local_state->dependencies()));
    EXPECT_TRUE(OperatorHelper::is_block(probe_sink_local_state[0]->dependencies()));
    EXPECT_TRUE(OperatorHelper::is_block(source_local_state->dependencies()));

    EXPECT_EQ(shared_state->build_not_ignore_null.size(), 1);
    EXPECT_EQ(shared_state->build_not_ignore_null[0], true);

    {
        Block block = ColumnHelper::create_block<DataTypeInt64>({1, 2, 3, 4, 5});
        EXPECT_TRUE(sink_op->sink(state.get(), &block, true));
    }

    EXPECT_TRUE(OperatorHelper::is_ready(probe_sink_local_state[0]->dependencies()));

    {
        Block block = ColumnHelper::create_nullable_block<DataTypeInt64>(
                {0, 0, 0, 2, 4}, {true, true, true, false, false});
        EXPECT_TRUE(probe_sink_ops[0]->sink(states[0].get(), &block, true));
    }

    {
        Block block;
        bool eos = false;
        EXPECT_TRUE(source_op->get_block(state.get(), &block, &eos));

        std::cout << block.dump_data() << std::endl;
        EXPECT_TRUE(ColumnHelper::block_equal_with_sort(
                block, ColumnHelper::create_nullable_block<DataTypeInt64>({1, 3, 5},
                                                                          {false, false, false})));
    }
}

TEST_F(ExceptOperatorTest, test_output_null_batsh_size) {
    init_op(2, {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())});

    state->batsh_size = 3; // set batch size to 3
    sink_op->_child_exprs = MockSlotRef::create_mock_contexts(
            DataTypes {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())});
    probe_sink_ops[0]->_child_exprs = MockSlotRef::create_mock_contexts(
            DataTypes {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())});

    init_local_state();

    {
        Block block = ColumnHelper::create_nullable_block<DataTypeInt64>(
                {1, 2, 3, 4}, {false, false, false, true});
        EXPECT_TRUE(sink_op->sink(state.get(), &block, true));
    }

    {
        Block block = ColumnHelper::create_nullable_block<DataTypeInt64>({}, {});
        EXPECT_TRUE(probe_sink_ops[0]->sink(states[0].get(), &block, true));
    }

    {
        bool eos = false;
        Block block;
        EXPECT_TRUE(source_op->get_block(state.get(), &block, &eos));
        DCHECK_EQ(eos, false);
        EXPECT_EQ(block.rows(), 3);
        block.clear();
        EXPECT_TRUE(source_op->get_block(state.get(), &block, &eos));
        DCHECK_EQ(eos, true);
        EXPECT_EQ(block.rows(), 1);
    }
}

TEST_F(IntersectOperatorTest, test_extract_probe_column) {
    init_op(2, {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()),
                std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())});

    sink_op->_child_exprs = MockSlotRef::create_mock_contexts(
            DataTypes {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()),
                       std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())});
    probe_sink_ops[0]->_child_exprs = MockSlotRef::create_mock_contexts(
            DataTypes {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()),
                       std::make_shared<DataTypeInt64>()});

    init_local_state();

    {
        Block block {ColumnHelper::create_nullable_column_with_name<DataTypeInt64>({1}, {false}),
                     ColumnHelper::create_nullable_column_with_name<DataTypeInt64>({1}, {false})};
        EXPECT_TRUE(sink_op->sink(state.get(), &block, true));
    }

    {
        Block block {ColumnHelper::create_column_with_name<DataTypeInt64>({1}),
                     ColumnHelper::create_nullable_column_with_name<DataTypeInt64>({1}, {false})};
        EXPECT_TRUE(probe_sink_ops[0]->sink(states[0].get(), &block, true));
    }

    {
        Block block;
        bool eos = false;
        EXPECT_TRUE(source_op->get_block(state.get(), &block, &eos));
        std::cout << block.dump_data() << std::endl;
        EXPECT_EQ(block.rows(), 1);
    }
}

TEST_F(IntersectOperatorTest, test_refresh_hash_table) {
    init_op(3, {std::make_shared<DataTypeInt64>()});

    sink_op->_child_exprs =
            MockSlotRef::create_mock_contexts(DataTypes {std::make_shared<DataTypeInt64>()});
    probe_sink_ops[0]->_child_exprs =
            MockSlotRef::create_mock_contexts(DataTypes {std::make_shared<DataTypeInt64>()});

    probe_sink_ops[1]->_child_exprs =
            MockSlotRef::create_mock_contexts(DataTypes {std::make_shared<DataTypeInt64>()});

    init_local_state();

    EXPECT_TRUE(OperatorHelper::is_ready(sink_local_state->dependencies()));
    EXPECT_TRUE(OperatorHelper::is_block(probe_sink_local_state[0]->dependencies()));
    EXPECT_TRUE(OperatorHelper::is_block(probe_sink_local_state[1]->dependencies()));
    EXPECT_TRUE(OperatorHelper::is_block(source_local_state->dependencies()));

    {
        Block block = ColumnHelper::create_block<DataTypeInt64>({1, 2, 3, 4, 5});
        EXPECT_TRUE(sink_op->sink(state.get(), &block, true));
    }

    {
        EXPECT_TRUE(OperatorHelper::is_ready(probe_sink_local_state[0]->dependencies()));
        Block block = ColumnHelper::create_block<DataTypeInt64>({3, 4, 5});
        EXPECT_TRUE(probe_sink_ops[0]->sink(states[0].get(), &block, true));
    }

    {
        EXPECT_TRUE(OperatorHelper::is_ready(probe_sink_local_state[1]->dependencies()));
        Block block = ColumnHelper::create_block<DataTypeInt64>({4, 5});
        EXPECT_TRUE(probe_sink_ops[1]->sink(states[1].get(), &block, true));
    }

    {
        EXPECT_TRUE(OperatorHelper::is_ready(source_local_state->dependencies()));
        Block block;
        bool eos = false;
        EXPECT_TRUE(source_op->get_block(state.get(), &block, &eos));
        EXPECT_TRUE(ColumnHelper::block_equal_with_sort(
                block, ColumnHelper::create_block<DataTypeInt64>({4, 5})));
    }
}

TEST_F(IntersectOperatorTest, test_refresh_hash_table_is_need_shrink) {
    init_op(3, {std::make_shared<DataTypeInt64>()});

    sink_op->_child_exprs =
            MockSlotRef::create_mock_contexts(DataTypes {std::make_shared<DataTypeInt64>()});
    probe_sink_ops[0]->_child_exprs =
            MockSlotRef::create_mock_contexts(DataTypes {std::make_shared<DataTypeInt64>()});

    probe_sink_ops[1]->_child_exprs =
            MockSlotRef::create_mock_contexts(DataTypes {std::make_shared<DataTypeInt64>()});

    init_local_state();

    EXPECT_TRUE(OperatorHelper::is_ready(sink_local_state->dependencies()));
    EXPECT_TRUE(OperatorHelper::is_block(probe_sink_local_state[0]->dependencies()));
    EXPECT_TRUE(OperatorHelper::is_block(probe_sink_local_state[1]->dependencies()));
    EXPECT_TRUE(OperatorHelper::is_block(source_local_state->dependencies()));

    {
        Block block = ColumnHelper::create_block<DataTypeInt64>({1, 2, 3, 4, 5});
        EXPECT_TRUE(sink_op->sink(state.get(), &block, true));
    }

    {
        EXPECT_TRUE(OperatorHelper::is_ready(probe_sink_local_state[0]->dependencies()));
        Block block = ColumnHelper::create_block<DataTypeInt64>({1, 2, 3, 4, 5});
        EXPECT_TRUE(probe_sink_ops[0]->sink(states[0].get(), &block, true));
    }

    {
        EXPECT_TRUE(OperatorHelper::is_ready(probe_sink_local_state[1]->dependencies()));
        Block block = ColumnHelper::create_block<DataTypeInt64>({4, 5});
        EXPECT_TRUE(probe_sink_ops[1]->sink(states[1].get(), &block, true));
    }

    {
        EXPECT_TRUE(OperatorHelper::is_ready(source_local_state->dependencies()));
        Block block;
        bool eos = false;
        EXPECT_TRUE(source_op->get_block(state.get(), &block, &eos));
        EXPECT_TRUE(ColumnHelper::block_equal_with_sort(
                block, ColumnHelper::create_block<DataTypeInt64>({4, 5})));
    }
}

TEST_F(ExceptOperatorTest, test_refresh_hash_table) {
    init_op(3, {std::make_shared<DataTypeInt64>()});

    sink_op->_child_exprs =
            MockSlotRef::create_mock_contexts(DataTypes {std::make_shared<DataTypeInt64>()});
    probe_sink_ops[0]->_child_exprs =
            MockSlotRef::create_mock_contexts(DataTypes {std::make_shared<DataTypeInt64>()});

    probe_sink_ops[1]->_child_exprs =
            MockSlotRef::create_mock_contexts(DataTypes {std::make_shared<DataTypeInt64>()});

    init_local_state();

    EXPECT_TRUE(OperatorHelper::is_ready(sink_local_state->dependencies()));
    EXPECT_TRUE(OperatorHelper::is_block(probe_sink_local_state[0]->dependencies()));
    EXPECT_TRUE(OperatorHelper::is_block(probe_sink_local_state[1]->dependencies()));
    EXPECT_TRUE(OperatorHelper::is_block(source_local_state->dependencies()));

    {
        Block block = ColumnHelper::create_block<DataTypeInt64>({1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
        EXPECT_TRUE(sink_op->sink(state.get(), &block, true));
    }

    {
        EXPECT_TRUE(OperatorHelper::is_ready(probe_sink_local_state[0]->dependencies()));
        Block block = ColumnHelper::create_block<DataTypeInt64>({1, 2, 3, 4, 5, 6, 7, 8, 9});
        EXPECT_TRUE(probe_sink_ops[0]->sink(states[0].get(), &block, true));
    }

    {
        EXPECT_TRUE(OperatorHelper::is_ready(probe_sink_local_state[1]->dependencies()));
        Block block = ColumnHelper::create_block<DataTypeInt64>({10});
        EXPECT_TRUE(probe_sink_ops[1]->sink(states[1].get(), &block, true));
    }

    {
        EXPECT_TRUE(OperatorHelper::is_ready(source_local_state->dependencies()));
        Block block;
        bool eos = false;
        EXPECT_TRUE(source_op->get_block(state.get(), &block, &eos));
        EXPECT_TRUE(block.empty());
    }
}
} // namespace doris::pipeline