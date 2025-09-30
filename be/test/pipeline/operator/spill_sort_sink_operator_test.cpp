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

#include "pipeline/exec/spill_sort_sink_operator.h"

#include <gtest/gtest.h>

#include <memory>

#include "common/config.h"
#include "pipeline/dependency.h"
#include "pipeline/pipeline_task.h"
#include "spill_sort_test_helper.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_runtime_state.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_number.h"

namespace doris::pipeline {
class SpillSortSinkOperatorTest : public testing::Test {
protected:
    void SetUp() override { _helper.SetUp(); }
    void TearDown() override { _helper.TearDown(); }
    SpillSortTestHelper _helper;
};

TEST_F(SpillSortSinkOperatorTest, Basic) {
    auto [source_operator, sink_operator] = _helper.create_operators();
    ASSERT_TRUE(source_operator != nullptr);
    ASSERT_TRUE(sink_operator != nullptr);

    auto tnode = _helper.create_test_plan_node();
    auto st = sink_operator->init(TDataSink {});
    ASSERT_FALSE(st.ok());

    st = sink_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();

    st = sink_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "prepare failed: " << st.to_string();

    auto shared_state = sink_operator->create_shared_state();
    ASSERT_TRUE(shared_state != nullptr);

    LocalSinkStateInfo info {.task_idx = 0,
                             .parent_profile = _helper.operator_profile.get(),
                             .sender_id = 0,
                             .shared_state = shared_state.get(),
                             .shared_state_map = {},
                             .tsink = {}};

    st = sink_operator->setup_local_state(_helper.runtime_state.get(), info);
    ASSERT_TRUE(st.ok()) << "setup_local_state failed: " << st.to_string();

    auto* sink_local_state = _helper.runtime_state->get_sink_local_state();
    ASSERT_TRUE(sink_local_state != nullptr);

    st = sink_local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

    ASSERT_EQ(sink_operator->get_reserve_mem_size(_helper.runtime_state.get(), false), 0);
    ASSERT_EQ(sink_operator->get_reserve_mem_size(_helper.runtime_state.get(), true), 0);

    auto data_distribution = sink_operator->required_data_distribution();
    auto inner_data_distribution = sink_operator->_sort_sink_operator->required_data_distribution();
    ASSERT_EQ(data_distribution.distribution_type, inner_data_distribution.distribution_type);

    ASSERT_EQ(sink_operator->require_data_distribution(),
              sink_operator->_sort_sink_operator->require_data_distribution());

    st = sink_local_state->close(_helper.runtime_state.get(), st);
    ASSERT_TRUE(st.ok()) << "close failed: " << st.to_string();

    st = sink_operator->close(_helper.runtime_state.get(), st);
    ASSERT_TRUE(st.ok()) << "close failed: " << st.to_string();
}

TEST_F(SpillSortSinkOperatorTest, Sink) {
    auto [source_operator, sink_operator] = _helper.create_operators();
    ASSERT_TRUE(source_operator != nullptr);
    ASSERT_TRUE(sink_operator != nullptr);

    auto tnode = _helper.create_test_plan_node();
    auto st = sink_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();

    st = sink_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "prepare failed: " << st.to_string();

    auto shared_state =
            std::dynamic_pointer_cast<SpillSortSharedState>(sink_operator->create_shared_state());
    ASSERT_TRUE(shared_state != nullptr);

    auto* source_dependency = shared_state->create_source_dependency(
            sink_operator->operator_id(), sink_operator->node_id(), "SpillSortSinkOperatorTest");

    LocalSinkStateInfo info {.task_idx = 0,
                             .parent_profile = _helper.operator_profile.get(),
                             .sender_id = 0,
                             .shared_state = shared_state.get(),
                             .shared_state_map = {},
                             .tsink = {}};

    st = sink_operator->setup_local_state(_helper.runtime_state.get(), info);
    ASSERT_TRUE(st.ok()) << "setup_local_state failed: " << st.to_string();

    auto* sink_local_state = _helper.runtime_state->get_sink_local_state();
    ASSERT_TRUE(sink_local_state != nullptr);

    st = sink_local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

    auto input_block = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(
            {1, 2, 3, 4, 5, 5, 4, 3, 2, 1});

    input_block.insert(vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeInt64>(
            {10, 9, 8, 7, 6, 5, 4, 3, 2, 1}));

    st = sink_operator->sink(_helper.runtime_state.get(), &input_block, false);
    ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();

    vectorized::Block empty_block;
    st = sink_operator->sink(_helper.runtime_state.get(), &empty_block, true);
    ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();

    ASSERT_TRUE(source_dependency->is_blocked_by(nullptr) == nullptr);

    auto* inner_shared_state = shared_state->in_mem_shared_state;

    auto* sorter = inner_shared_state->sorter.get();
    ASSERT_TRUE(sorter != nullptr);

    bool eos = false;
    std::unique_ptr<vectorized::MutableBlock> mutable_column;
    while (!eos) {
        vectorized::Block block;
        st = sorter->get_next(_helper.runtime_state.get(), &block, &eos);
        ASSERT_TRUE(st.ok()) << "get_next failed: " << st.to_string();

        if (!block.empty()) {
            if (mutable_column == nullptr) {
                mutable_column = vectorized::MutableBlock::create_unique(std::move(block));
            } else {
                st = mutable_column->merge(std::move(block));
                ASSERT_TRUE(st.ok()) << "merge failed: " << st.to_string();
            }
        }
    }

    auto sorted_block = mutable_column->to_block();
    ASSERT_EQ(sorted_block.rows(), 10);

    auto& column1 = sorted_block.get_by_position(0).column;
    auto& column2 = sorted_block.get_by_position(1).column;

    ASSERT_EQ(column1->get_int(0), 1);
    ASSERT_EQ(column1->get_int(2), 2);
    ASSERT_EQ(column1->get_int(4), 3);
    ASSERT_EQ(column1->get_int(6), 4);
    ASSERT_EQ(column1->get_int(8), 5);

    ASSERT_EQ(column2->get_int(1), 10);
    ASSERT_EQ(column2->get_int(3), 9);
    ASSERT_EQ(column2->get_int(5), 8);
    ASSERT_EQ(column2->get_int(7), 7);
    ASSERT_EQ(column2->get_int(9), 6);

    st = sink_local_state->close(_helper.runtime_state.get(), st);
    ASSERT_TRUE(st.ok()) << "close failed: " << st.to_string();

    st = sink_operator->close(_helper.runtime_state.get(), st);
    ASSERT_TRUE(st.ok()) << "close failed: " << st.to_string();
}

TEST_F(SpillSortSinkOperatorTest, SinkWithSpill) {
    auto [source_operator, sink_operator] = _helper.create_operators();
    ASSERT_TRUE(source_operator != nullptr);
    ASSERT_TRUE(sink_operator != nullptr);

    auto tnode = _helper.create_test_plan_node();
    auto st = sink_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();

    st = sink_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "prepare failed: " << st.to_string();

    auto shared_state =
            std::dynamic_pointer_cast<SpillSortSharedState>(sink_operator->create_shared_state());
    ASSERT_TRUE(shared_state != nullptr);

    auto* source_dependency = shared_state->create_source_dependency(
            sink_operator->operator_id(), sink_operator->node_id(), "SpillSortSinkOperatorTest");

    LocalSinkStateInfo info {.task_idx = 0,
                             .parent_profile = _helper.operator_profile.get(),
                             .sender_id = 0,
                             .shared_state = shared_state.get(),
                             .shared_state_map = {},
                             .tsink = {}};

    st = sink_operator->setup_local_state(_helper.runtime_state.get(), info);
    ASSERT_TRUE(st.ok()) << "setup_local_state failed: " << st.to_string();

    auto* sink_local_state = reinterpret_cast<SpillSortSinkLocalState*>(
            _helper.runtime_state->get_sink_local_state());
    ASSERT_TRUE(sink_local_state != nullptr);

    st = sink_local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

    auto input_block = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(
            {1, 2, 3, 4, 5, 5, 4, 3, 2, 1});

    input_block.insert(vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeInt64>(
            {10, 9, 8, 7, 6, 5, 4, 3, 2, 1}));

    st = sink_operator->sink(_helper.runtime_state.get(), &input_block, false);
    ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();

    st = sink_operator->revoke_memory(_helper.runtime_state.get(), nullptr);
    ASSERT_TRUE(st.ok()) << "revoke_memory failed: " << st.to_string();

    auto input_block2 = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(
            {1, 2, 3, 4, 5, 5, 4, 3, 2, 1});

    input_block2.insert(
            vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeInt64>(
                    {10, 9, 8, 7, 6, 5, 4, 3, 2, 1}));

    st = sink_operator->sink(_helper.runtime_state.get(), &input_block2, false);
    ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();

    ASSERT_GT(sink_operator->revocable_mem_size(_helper.runtime_state.get()), 0);

    // Because there are some rows in the sorter,
    // the sink operator will revoke memory when sinking eos with empty block.
    vectorized::Block empty_block;
    st = sink_operator->sink(_helper.runtime_state.get(), &empty_block, true);
    ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();

    /// After the sink operator revoke memory, the source dependency should be ready.
    ASSERT_TRUE(source_dependency->is_blocked_by(nullptr) == nullptr);
}

TEST_F(SpillSortSinkOperatorTest, SinkWithSpill2) {
    auto [source_operator, sink_operator] = _helper.create_operators();
    ASSERT_TRUE(source_operator != nullptr);
    ASSERT_TRUE(sink_operator != nullptr);

    auto tnode = _helper.create_test_plan_node();
    auto st = sink_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();

    st = sink_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "prepare failed: " << st.to_string();

    auto shared_state =
            std::dynamic_pointer_cast<SpillSortSharedState>(sink_operator->create_shared_state());
    ASSERT_TRUE(shared_state != nullptr);

    auto* source_dependency = shared_state->create_source_dependency(
            sink_operator->operator_id(), sink_operator->node_id(), "SpillSortSinkOperatorTest");

    LocalSinkStateInfo info {.task_idx = 0,
                             .parent_profile = _helper.operator_profile.get(),
                             .sender_id = 0,
                             .shared_state = shared_state.get(),
                             .shared_state_map = {},
                             .tsink = {}};

    st = sink_operator->setup_local_state(_helper.runtime_state.get(), info);
    ASSERT_TRUE(st.ok()) << "setup_local_state failed: " << st.to_string();

    auto* sink_local_state = reinterpret_cast<SpillSortSinkLocalState*>(
            _helper.runtime_state->get_sink_local_state());
    ASSERT_TRUE(sink_local_state != nullptr);

    st = sink_local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

    auto input_block = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(
            {1, 2, 3, 4, 5, 5, 4, 3, 2, 1});

    input_block.insert(vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeInt64>(
            {10, 9, 8, 7, 6, 5, 4, 3, 2, 1}));

    st = sink_operator->sink(_helper.runtime_state.get(), &input_block, false);
    ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();

    st = sink_operator->revoke_memory(_helper.runtime_state.get(), nullptr);
    ASSERT_TRUE(st.ok()) << "revoke_memory failed: " << st.to_string();

    ASSERT_EQ(sink_operator->revocable_mem_size(_helper.runtime_state.get()), 0);

    vectorized::Block empty_block;
    st = sink_operator->sink(_helper.runtime_state.get(), &empty_block, true);
    ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();

    ASSERT_TRUE(source_dependency->is_blocked_by(nullptr) == nullptr);
}

TEST_F(SpillSortSinkOperatorTest, SinkWithSpillError) {
    auto [source_operator, sink_operator] = _helper.create_operators();
    ASSERT_TRUE(source_operator != nullptr);
    ASSERT_TRUE(sink_operator != nullptr);

    auto tnode = _helper.create_test_plan_node();
    auto st = sink_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();

    st = sink_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "prepare failed: " << st.to_string();

    auto shared_state =
            std::dynamic_pointer_cast<SpillSortSharedState>(sink_operator->create_shared_state());
    ASSERT_TRUE(shared_state != nullptr);

    shared_state->create_source_dependency(sink_operator->operator_id(), sink_operator->node_id(),
                                           "SpillSortSinkOperatorTest");

    LocalSinkStateInfo info {.task_idx = 0,
                             .parent_profile = _helper.operator_profile.get(),
                             .sender_id = 0,
                             .shared_state = shared_state.get(),
                             .shared_state_map = {},
                             .tsink = {}};

    st = sink_operator->setup_local_state(_helper.runtime_state.get(), info);
    ASSERT_TRUE(st.ok()) << "setup_local_state failed: " << st.to_string();

    auto* sink_local_state = reinterpret_cast<SpillSortSinkLocalState*>(
            _helper.runtime_state->get_sink_local_state());
    ASSERT_TRUE(sink_local_state != nullptr);

    st = sink_local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

    auto input_block = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(
            {1, 2, 3, 4, 5, 5, 4, 3, 2, 1});

    input_block.insert(vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeInt64>(
            {10, 9, 8, 7, 6, 5, 4, 3, 2, 1}));

    st = sink_operator->sink(_helper.runtime_state.get(), &input_block, false);
    ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();

    SpillableDebugPointHelper dp_helper("fault_inject::spill_stream::spill_block");

    st = sink_operator->revoke_memory(_helper.runtime_state.get(), nullptr);

    ASSERT_FALSE(st.ok()) << "spilll status should be failed";
}

} // namespace doris::pipeline