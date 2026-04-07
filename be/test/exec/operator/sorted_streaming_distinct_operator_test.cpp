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

#include "exec/operator/sorted_streaming_distinct_operator.h"

#include <gtest/gtest.h>

#include <memory>

#include "core/data_type/data_type_number.h"
#include "exec/operator/mock_operator.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"

namespace doris {

class MockSortedDistinctChildOperator : public OperatorXBase {
public:
    Status get_block_after_projects(RuntimeState* state, Block* block, bool* eos) override {
        return Status::OK();
    }
    Status get_block(RuntimeState* state, Block* block, bool* eos) override { return Status::OK(); }
    Status setup_local_state(RuntimeState* state, LocalStateInfo& info) override {
        return Status::OK();
    }
    const RowDescriptor& row_desc() const override { return *_mock_row_desc; }
    std::unique_ptr<MockRowDescriptor> _mock_row_desc;
};

class SortedStreamingDistinctOperatorTest : public ::testing::Test {
public:
    void SetUp() override {
        state = std::make_shared<MockRuntimeState>();
        op = std::make_shared<SortedStreamingDistinctOperatorX>();
        child_op = std::make_shared<MockSortedDistinctChildOperator>();
    }

    void init_op(DataTypes key_types) {
        child_op->_mock_row_desc.reset(new MockRowDescriptor(key_types, &pool));
        EXPECT_TRUE(op->set_child(child_op));

        // Replace descriptor table so init_make_nullable can find tuple descriptor
        mock_desc_tbl = std::make_unique<MockDescriptorTbl>(key_types, &pool);
        state->set_desc_tbl(mock_desc_tbl.get());

        EXPECT_TRUE(op->prepare(state.get()).ok());

        // Create and init local state
        auto ls = std::make_unique<SortedStreamingDistinctLocalState>(state.get(), op.get());
        local_state = ls.get();
        LocalStateInfo info {.parent_profile = &profile,
                             .scan_ranges = {},
                             .shared_state = nullptr,
                             .shared_state_map = {},
                             .task_idx = 0};
        EXPECT_TRUE(ls->init(state.get(), info).ok());
        state->resize_op_id_to_local_state(-100);
        state->emplace_local_state(op->operator_id(), std::move(ls));
        EXPECT_TRUE(local_state->open(state.get()).ok());

        // Set group-by expr contexts directly on local state (bypass clone)
        local_state->_group_by_expr_ctxs = MockSlotRef::create_mock_contexts(key_types);
    }

    std::shared_ptr<SortedStreamingDistinctOperatorX> op;
    std::shared_ptr<MockSortedDistinctChildOperator> child_op;
    std::shared_ptr<MockRuntimeState> state;
    SortedStreamingDistinctLocalState* local_state = nullptr;
    std::unique_ptr<MockDescriptorTbl> mock_desc_tbl;
    ObjectPool pool;
    RuntimeProfile profile {""};
};

// Basic distinct on a single sorted column: {1,1,2,2,2,3} -> {1,2,3}
TEST_F(SortedStreamingDistinctOperatorTest, basic_distinct) {
    init_op({std::make_shared<DataTypeInt64>()});

    {
        Block block {ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 2, 2, 2, 3})};
        auto st = op->push(state.get(), &block, false);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_FALSE(op->need_more_input_data(state.get()));
    }

    {
        Block block;
        bool eos = false;
        auto st = op->pull(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_EQ(block.rows(), 3);
        EXPECT_FALSE(eos);
    }

    {
        EXPECT_TRUE(local_state->close(state.get()).ok());
    }
}

// Cross-chunk dedup: chunk1 ends with 3, chunk2 starts with 3 -> 3 appears once
TEST_F(SortedStreamingDistinctOperatorTest, cross_chunk_dedup) {
    init_op({std::make_shared<DataTypeInt64>()});

    // First chunk: {1, 2, 3, 3} -> output {1, 2, 3}
    {
        Block block {ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3, 3})};
        auto st = op->push(state.get(), &block, false);
        EXPECT_TRUE(st.ok()) << st.msg();
    }
    {
        Block block;
        bool eos = false;
        auto st = op->pull(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_EQ(block.rows(), 3);
        EXPECT_FALSE(eos);
    }

    // Second chunk: {3, 3, 4, 5} -> output {4, 5} (3 is deduped from previous chunk)
    {
        Block block {ColumnHelper::create_column_with_name<DataTypeInt64>({3, 3, 4, 5})};
        auto st = op->push(state.get(), &block, false);
        EXPECT_TRUE(st.ok()) << st.msg();
    }
    {
        Block block;
        bool eos = false;
        auto st = op->pull(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_EQ(block.rows(), 2);
        EXPECT_FALSE(eos);
    }

    {
        EXPECT_TRUE(local_state->close(state.get()).ok());
    }
}

// All same key: {5,5,5,5,5} -> {5}
TEST_F(SortedStreamingDistinctOperatorTest, all_same_key) {
    init_op({std::make_shared<DataTypeInt64>()});

    {
        Block block {ColumnHelper::create_column_with_name<DataTypeInt64>({5, 5, 5, 5, 5})};
        auto st = op->push(state.get(), &block, false);
        EXPECT_TRUE(st.ok()) << st.msg();
    }
    {
        Block block;
        bool eos = false;
        auto st = op->pull(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_EQ(block.rows(), 1);
    }

    {
        EXPECT_TRUE(local_state->close(state.get()).ok());
    }
}

// Cross-chunk all duplicates: chunk2 is entirely duplicates of chunk1's last key
TEST_F(SortedStreamingDistinctOperatorTest, cross_chunk_all_duplicates) {
    init_op({std::make_shared<DataTypeInt64>()});

    // First chunk: {1, 2, 3}
    {
        Block block {ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3})};
        auto st = op->push(state.get(), &block, false);
        EXPECT_TRUE(st.ok()) << st.msg();
    }
    {
        Block block;
        bool eos = false;
        auto st = op->pull(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_EQ(block.rows(), 3);
    }

    // Second chunk: {3, 3, 3} -> all duplicates, output 0 rows
    {
        Block block {ColumnHelper::create_column_with_name<DataTypeInt64>({3, 3, 3})};
        auto st = op->push(state.get(), &block, false);
        EXPECT_TRUE(st.ok()) << st.msg();
        // Output block should be empty since all rows are duplicates
        EXPECT_TRUE(op->need_more_input_data(state.get()));
    }

    {
        EXPECT_TRUE(local_state->close(state.get()).ok());
    }
}

// Multi-column key: DISTINCT on two columns
TEST_F(SortedStreamingDistinctOperatorTest, multi_column_key) {
    init_op({std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt64>()});

    // Sorted by (col0, col1): (1,10),(1,10),(1,20),(2,10),(2,10) -> 3 distinct rows
    {
        Block block {ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 1, 2, 2}),
                     ColumnHelper::create_column_with_name<DataTypeInt64>({10, 10, 20, 10, 10})};
        auto st = op->push(state.get(), &block, false);
        EXPECT_TRUE(st.ok()) << st.msg();
    }
    {
        Block block;
        bool eos = false;
        auto st = op->pull(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_EQ(block.rows(), 3);
    }

    {
        EXPECT_TRUE(local_state->close(state.get()).ok());
    }
}

// All unique rows: no dedup needed
TEST_F(SortedStreamingDistinctOperatorTest, all_unique) {
    init_op({std::make_shared<DataTypeInt64>()});

    {
        Block block {ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3, 4, 5})};
        auto st = op->push(state.get(), &block, false);
        EXPECT_TRUE(st.ok()) << st.msg();
    }
    {
        Block block;
        bool eos = false;
        auto st = op->pull(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_EQ(block.rows(), 5);
    }

    {
        EXPECT_TRUE(local_state->close(state.get()).ok());
    }
}

// Empty block should be a no-op
TEST_F(SortedStreamingDistinctOperatorTest, empty_block) {
    init_op({std::make_shared<DataTypeInt64>()});

    {
        Block block;
        auto st = op->push(state.get(), &block, false);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(op->need_more_input_data(state.get()));
    }

    {
        EXPECT_TRUE(local_state->close(state.get()).ok());
    }
}

// Eos handling: after push with eos, pull should set eos=true
TEST_F(SortedStreamingDistinctOperatorTest, eos_handling) {
    init_op({std::make_shared<DataTypeInt64>()});

    local_state->_child_eos = true;

    {
        Block block {ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3})};
        auto st = op->push(state.get(), &block, false);
        EXPECT_TRUE(st.ok()) << st.msg();
    }
    {
        Block block;
        bool eos = false;
        auto st = op->pull(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_EQ(block.rows(), 3);
        EXPECT_TRUE(eos);
    }

    {
        EXPECT_TRUE(local_state->close(state.get()).ok());
    }
}

} // namespace doris
