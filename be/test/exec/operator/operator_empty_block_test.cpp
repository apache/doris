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

#include <vector>

#include "exec/operator/operator.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {

#ifndef NDEBUG

class EmptyBlockMockSourceOperator final : public OperatorX<DummyOperatorLocalState> {
public:
    explicit EmptyBlockMockSourceOperator(Block block)
            : OperatorX<DummyOperatorLocalState>(nullptr, 0, 0), _block(std::move(block)) {}

    Status get_block_impl(RuntimeState* /*state*/, Block* block, bool* eos) override {
        ++get_block_impl_calls;
        *block = _block;
        *eos = true;
        return Status::OK();
    }

    int get_block_impl_calls = 0;

private:
    Block _block;
};

class EmptyBlockMockSinkOperator final : public DataSinkOperatorX<DummySinkLocalState> {
public:
    EmptyBlockMockSinkOperator() : DataSinkOperatorX<DummySinkLocalState>(0, 0, 0) {}

    Status sink_impl(RuntimeState* /*state*/, Block* block, bool eos) override {
        input_rows.push_back(block->rows());
        input_eos.push_back(eos);
        if (fail_on_empty_block && block->empty()) {
            return Status::InternalError("failed to process empty block");
        }
        return Status::OK();
    }

    std::vector<size_t> input_rows;
    std::vector<bool> input_eos;
    bool fail_on_empty_block = false;
};

TEST(OperatorEmptyBlockTest, GetBlockReturnsEmptyBlockBeforeOriginalBlock) {
    MockRuntimeState state;
    RuntimeProfile profile("test");
    auto original_block = ColumnHelper::create_block<DataTypeInt64>({1, 2, 3});
    EmptyBlockMockSourceOperator source(original_block);

    auto local_state = DummyOperatorLocalState::create_unique(&state, &source);
    LocalStateInfo info {.parent_profile = &profile,
                         .scan_ranges = {},
                         .shared_state = nullptr,
                         .shared_state_map = {},
                         .task_idx = 0};
    ASSERT_TRUE(local_state->init(&state, info).ok());
    state.resize_op_id_to_local_state(-1);
    state.emplace_local_state(source.operator_id(), std::move(local_state));

    Block output_block;
    bool eos = true;
    ASSERT_TRUE(source.get_block(&state, &output_block, &eos).ok());
    EXPECT_EQ(output_block.columns(), original_block.columns());
    EXPECT_EQ(output_block.rows(), 0);
    EXPECT_FALSE(eos);
    EXPECT_EQ(source.get_block_impl_calls, 1);

    ASSERT_TRUE(source.get_block(&state, &output_block, &eos).ok());
    EXPECT_TRUE(ColumnHelper::block_equal(output_block, original_block));
    EXPECT_TRUE(eos);
    EXPECT_EQ(source.get_block_impl_calls, 1);
}

TEST(OperatorEmptyBlockTest, SinkReceivesEmptyBlockBeforeOriginalBlock) {
    MockRuntimeState state;
    EmptyBlockMockSinkOperator sink;
    auto original_block = ColumnHelper::create_block<DataTypeInt64>({1, 2, 3});

    ASSERT_TRUE(sink.sink(&state, &original_block, true).ok());
    EXPECT_EQ(sink.input_rows, std::vector<size_t>({0, 3}));
    EXPECT_EQ(sink.input_eos, std::vector<bool>({false, true}));
    EXPECT_EQ(original_block.rows(), 3);
}

TEST(OperatorEmptyBlockTest, SinkPropagatesEmptyBlockError) {
    MockRuntimeState state;
    EmptyBlockMockSinkOperator sink;
    sink.fail_on_empty_block = true;
    auto original_block = ColumnHelper::create_block<DataTypeInt64>({1, 2, 3});

    EXPECT_FALSE(sink.sink(&state, &original_block, true).ok());
    EXPECT_EQ(sink.input_rows, std::vector<size_t>({0}));
    EXPECT_EQ(sink.input_eos, std::vector<bool>({false}));
    EXPECT_EQ(original_block.rows(), 3);
}

#endif

} // namespace doris
