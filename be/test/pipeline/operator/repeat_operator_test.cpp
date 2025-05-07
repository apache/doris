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

#include "pipeline/exec/repeat_operator.h"

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
struct RepeatOperatorTest : public ::testing::Test {
    void SetUp() override {
        op = std::make_unique<RepeatOperatorX>();
        mock_op = std::make_shared<MockOperatorX>();
        state = std::make_shared<MockRuntimeState>();
        state->batsh_size = 10;
        op->_child = mock_op;
    }

    void set_output_slots(DataTypes output_types) {
        output_desc = std::make_shared<MockRowDescriptor>(output_types, &pool);
        op->_output_slots = output_desc->tuple_descriptors()[0]->slots();
    }

    void create_local_state() {
        local_state_uptr = std::make_unique<RepeatLocalState>(state.get(), op.get());
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
    std::unique_ptr<RepeatOperatorX> op;
    std::shared_ptr<MockOperatorX> mock_op;

    std::unique_ptr<RepeatLocalState> local_state_uptr;

    RepeatLocalState* local_state;

    std::shared_ptr<MockRuntimeState> state;

    std::shared_ptr<MockRowDescriptor> output_desc;

    ObjectPool pool;
};

TEST_F(RepeatOperatorTest, test_without_expr) {
    set_output_slots({std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt64>(),
                      std::make_shared<DataTypeInt64>()});
    create_local_state();
    EXPECT_TRUE(op->need_more_input_data(state.get()));
    *local_state->_child_block = ColumnHelper::create_block<DataTypeInt64>({1, 2, 3, 4});
    EXPECT_TRUE(op->push(state.get(), local_state->_child_block.get(), false));
    EXPECT_EQ(local_state->_intermediate_block->rows(), 0);
    op->_repeat_id_list_size = 2;

    op->_grouping_list = {{1, 2}, {3, 4}, {5, 6}};

    /*
+---------------+---------------+---------------+
|(Int64)        |(Int64)        |(Int64)        |
+---------------+---------------+---------------+
|              1|              3|              5|
|              1|              3|              5|
|              1|              3|              5|
|              1|              3|              5|
+---------------+---------------+---------------+

+---------------+---------------+---------------+
|(Int64)        |(Int64)        |(Int64)        |
+---------------+---------------+---------------+
|              2|              4|              6|
|              2|              4|              6|
|              2|              4|              6|
|              2|              4|              6|
+---------------+---------------+---------------+
    */
    {
        Block block;
        bool eos = false;
        EXPECT_FALSE(op->need_more_input_data(state.get()));
        EXPECT_TRUE(op->pull(state.get(), &block, &eos));

        std::cout << block.dump_data() << std::endl;

        EXPECT_TRUE(ColumnHelper::block_equal(
                block, Block {
                               ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 1, 1}),
                               ColumnHelper::create_column_with_name<DataTypeInt64>({3, 3, 3, 3}),
                               ColumnHelper::create_column_with_name<DataTypeInt64>({5, 5, 5, 5}),
                       }));
    }

    {
        Block block;
        bool eos = false;
        EXPECT_FALSE(op->need_more_input_data(state.get()));
        EXPECT_TRUE(op->pull(state.get(), &block, &eos));

        std::cout << block.dump_data() << std::endl;

        EXPECT_TRUE(ColumnHelper::block_equal(
                block, Block {
                               ColumnHelper::create_column_with_name<DataTypeInt64>({2, 2, 2, 2}),
                               ColumnHelper::create_column_with_name<DataTypeInt64>({4, 4, 4, 4}),
                               ColumnHelper::create_column_with_name<DataTypeInt64>({6, 6, 6, 6}),
                       }));
    }

    EXPECT_TRUE(op->need_more_input_data(state.get()));
}

TEST_F(RepeatOperatorTest, test_with_expr) {
    set_output_slots({std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt64>(),
                      std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt64>(),
                      std::make_shared<DataTypeInt64>()});
    op->_expr_ctxs = MockSlotRef::create_mock_contexts(
            {std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt64>()});
    create_local_state();

    EXPECT_TRUE(op->need_more_input_data(state.get()));

    {
        *local_state->_child_block = Block {
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3, 4}),
                ColumnHelper::create_column_with_name<DataTypeInt64>({10, 20, 30, 40}),
        };
        EXPECT_TRUE(op->push(state.get(), local_state->_child_block.get(), false));
        EXPECT_EQ(local_state->_intermediate_block->rows(), 4);
    }

    op->_repeat_id_list_size = 2;
    op->_grouping_list = {{1, 2}, {3, 4}, {5, 6}};
    op->_slot_id_set_list.resize(2);

    op->_slot_id_set_list[0].insert(0);
    op->_slot_id_set_list[0].insert(1);
    op->_slot_id_set_list[0].insert(2);
    op->_slot_id_set_list[1].insert(0);
    op->_slot_id_set_list[1].insert(1);
    op->_slot_id_set_list[1].insert(2);
    /*
+---------------+---------------+---------------+---------------+---------------+
|(Int64)        |(Int64)        |(Int64)        |(Int64)        |(Int64)        |
+---------------+---------------+---------------+---------------+---------------+
|              1|             10|              1|              3|              5|
|              2|             20|              1|              3|              5|
|              3|             30|              1|              3|              5|
|              4|             40|              1|              3|              5|
+---------------+---------------+---------------+---------------+---------------+

+---------------+---------------+---------------+---------------+---------------+
|(Int64)        |(Int64)        |(Int64)        |(Int64)        |(Int64)        |
+---------------+---------------+---------------+---------------+---------------+
|              1|             10|              2|              4|              6|
|              2|             20|              2|              4|              6|
|              3|             30|              2|              4|              6|
|              4|             40|              2|              4|              6|
+---------------+---------------+---------------+---------------+---------------+
*/
    {
        Block block;
        bool eos = false;
        EXPECT_FALSE(op->need_more_input_data(state.get()));
        EXPECT_TRUE(op->pull(state.get(), &block, &eos));

        std::cout << block.dump_data() << std::endl;

        EXPECT_TRUE(ColumnHelper::block_equal(
                block,
                Block {
                        ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3, 4}),
                        ColumnHelper::create_column_with_name<DataTypeInt64>({10, 20, 30, 40}),
                        ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 1, 1}),
                        ColumnHelper::create_column_with_name<DataTypeInt64>({3, 3, 3, 3}),
                        ColumnHelper::create_column_with_name<DataTypeInt64>({5, 5, 5, 5}),
                }));
    }

    {
        Block block;
        bool eos = false;
        EXPECT_FALSE(op->need_more_input_data(state.get()));
        EXPECT_TRUE(op->pull(state.get(), &block, &eos));

        std::cout << block.dump_data() << std::endl;

        EXPECT_TRUE(ColumnHelper::block_equal(
                block,
                Block {
                        ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3, 4}),
                        ColumnHelper::create_column_with_name<DataTypeInt64>({10, 20, 30, 40}),
                        ColumnHelper::create_column_with_name<DataTypeInt64>({2, 2, 2, 2}),
                        ColumnHelper::create_column_with_name<DataTypeInt64>({4, 4, 4, 4}),
                        ColumnHelper::create_column_with_name<DataTypeInt64>({6, 6, 6, 6}),
                }));
    }
    EXPECT_TRUE(op->need_more_input_data(state.get()));
}

TEST_F(RepeatOperatorTest, test_with_expr2) {
    set_output_slots({std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()),
                      std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()),
                      std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()),
                      std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt64>(),
                      std::make_shared<DataTypeInt64>()});

    op->_expr_ctxs = MockSlotRef::create_mock_contexts(
            {std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt64>(),
             std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())});
    create_local_state();

    EXPECT_TRUE(op->need_more_input_data(state.get()));

    {
        *local_state->_child_block = Block {
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3, 4}),
                ColumnHelper::create_column_with_name<DataTypeInt64>({10, 20, 30, 40}),
                ColumnHelper::create_nullable_column_with_name<DataTypeInt64>(
                        {100, 200, 300, 400}, {false, false, false, true}),
        };
        EXPECT_TRUE(op->push(state.get(), local_state->_child_block.get(), false));
        EXPECT_EQ(local_state->_intermediate_block->rows(), 4);
    }

    op->_repeat_id_list_size = 2;
    op->_grouping_list = {{1, 2}, {3, 4}, {5, 6}};
    op->_all_slot_ids = {0, 1, 2};

    op->_slot_id_set_list.resize(op->_repeat_id_list_size);

    op->_output_slots[0]->_id = 0;

    op->_output_slots[1]->_id = 1;

    op->_output_slots[2]->_id = 2;

    op->_slot_id_set_list[0].insert(1);
    op->_slot_id_set_list[0].insert(2);
    op->_slot_id_set_list[1].insert(0);
    op->_slot_id_set_list[1].insert(1);
    op->_slot_id_set_list[1].insert(2);

    /*
+-----------------+-----------------+-----------------+---------------+---------------+---------------+
|(Nullable(Int64))|(Nullable(Int64))|(Nullable(Int64))|(Int64)        |(Int64)        |(Int64)        |
+-----------------+-----------------+-----------------+---------------+---------------+---------------+
|             NULL|               10|              100|              1|              3|              5|
|             NULL|               20|              200|              1|              3|              5|
|             NULL|               30|              300|              1|              3|              5|
|             NULL|               40|             NULL|              1|              3|              5|
+-----------------+-----------------+-----------------+---------------+---------------+---------------+

+-----------------+-----------------+-----------------+---------------+---------------+---------------+
|(Nullable(Int64))|(Nullable(Int64))|(Nullable(Int64))|(Int64)        |(Int64)        |(Int64)        |
+-----------------+-----------------+-----------------+---------------+---------------+---------------+
|                1|               10|              100|              2|              4|              6|
|                2|               20|              200|              2|              4|              6|
|                3|               30|              300|              2|              4|              6|
|                4|               40|             NULL|              2|              4|              6|    
+-----------------+-----------------+-----------------+---------------+---------------+---------------+  
*/
    {
        Block block;
        bool eos = false;
        EXPECT_FALSE(op->need_more_input_data(state.get()));
        EXPECT_TRUE(op->pull(state.get(), &block, &eos));

        std::cout << block.dump_data() << std::endl;

        EXPECT_TRUE(ColumnHelper::block_equal(
                block, Block {
                               ColumnHelper::create_nullable_column_with_name<DataTypeInt64>(
                                       {1, 2, 3, 4}, {true, true, true, true}),
                               ColumnHelper::create_nullable_column_with_name<DataTypeInt64>(
                                       {10, 20, 30, 40}, {false, false, false, false}),
                               ColumnHelper::create_nullable_column_with_name<DataTypeInt64>(
                                       {100, 200, 300, 400}, {false, false, false, true}),
                               ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 1, 1}),
                               ColumnHelper::create_column_with_name<DataTypeInt64>({3, 3, 3, 3}),
                               ColumnHelper::create_column_with_name<DataTypeInt64>({5, 5, 5, 5}),
                       }));
    }

    {
        Block block;
        bool eos = false;
        EXPECT_FALSE(op->need_more_input_data(state.get()));
        EXPECT_TRUE(op->pull(state.get(), &block, &eos));

        std::cout << block.dump_data() << std::endl;

        EXPECT_TRUE(ColumnHelper::block_equal(
                block, Block {
                               ColumnHelper::create_nullable_column_with_name<DataTypeInt64>(
                                       {1, 2, 3, 4}, {false, false, false, false}),
                               ColumnHelper::create_nullable_column_with_name<DataTypeInt64>(
                                       {10, 20, 30, 40}, {false, false, false, false}),
                               ColumnHelper::create_nullable_column_with_name<DataTypeInt64>(
                                       {100, 200, 300, 400}, {false, false, false, true}),
                               ColumnHelper::create_column_with_name<DataTypeInt64>({2, 2, 2, 2}),
                               ColumnHelper::create_column_with_name<DataTypeInt64>({4, 4, 4, 4}),
                               ColumnHelper::create_column_with_name<DataTypeInt64>({6, 6, 6, 6}),
                       }));
    }
    EXPECT_TRUE(op->need_more_input_data(state.get()));
}

} // namespace doris::pipeline