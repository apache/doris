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

#include "pipeline/exec/partitioned_aggregation_sink_operator.h"

#include <gtest/gtest.h>

#include <memory>

#include "common/config.h"
#include "partitioned_aggregation_test_helper.h"
#include "pipeline/exec/aggregation_sink_operator.h"
#include "pipeline/exec/partitioned_hash_join_probe_operator.h"
#include "pipeline/exec/partitioned_hash_join_sink_operator.h"
#include "pipeline/pipeline_task.h"
#include "runtime/fragment_mgr.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_runtime_state.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_number.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {
class PartitionedAggregationSinkOperatorTest : public testing::Test {
protected:
    void SetUp() override { _helper.SetUp(); }
    void TearDown() override { _helper.TearDown(); }
    PartitionedAggregationTestHelper _helper;
};

TEST_F(PartitionedAggregationSinkOperatorTest, Init) {
    auto [source_operator, sink_operator] = _helper.create_operators();
    ASSERT_TRUE(source_operator != nullptr);
    ASSERT_TRUE(sink_operator != nullptr);

    const auto tnode = _helper.create_test_plan_node();
    auto st = sink_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();

    st = sink_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "prepare failed: " << st.to_string();

    std::shared_ptr<MockPartitionedAggSharedState> shared_state =
            MockPartitionedAggSharedState::create_shared();

    LocalSinkStateInfo info {.task_idx = 0,
                             .parent_profile = _helper.operator_profile.get(),
                             .sender_id = 0,
                             .shared_state = shared_state.get(),
                             .shared_state_map = {},
                             .tsink = TDataSink()};
    st = sink_operator->setup_local_state(_helper.runtime_state.get(), info);
    ASSERT_TRUE(st.ok()) << "setup_local_state failed: " << st.to_string();

    auto* local_state = _helper.runtime_state->get_sink_local_state();
    ASSERT_TRUE(local_state != nullptr);

    st = local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

    st = sink_operator->close(_helper.runtime_state.get(), st);
    ASSERT_TRUE(st.ok()) << "close failed: " << st.to_string();

    st = local_state->close(_helper.runtime_state.get(), st);
    ASSERT_TRUE(st.ok()) << "close failed: " << st.to_string();
}

TEST_F(PartitionedAggregationSinkOperatorTest, Sink) {
    auto [source_operator, sink_operator] = _helper.create_operators();
    ASSERT_TRUE(source_operator != nullptr);
    ASSERT_TRUE(sink_operator != nullptr);

    const auto tnode = _helper.create_test_plan_node();
    auto st = sink_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();

    st = sink_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "prepare failed: " << st.to_string();

    auto shared_state = sink_operator->create_shared_state();
    auto* dep = shared_state->create_source_dependency(source_operator->operator_id(),
                                                       source_operator->node_id(),
                                                       "PartitionedAggSinkTestDep");

    LocalSinkStateInfo info {.task_idx = 0,
                             .parent_profile = _helper.operator_profile.get(),
                             .sender_id = 0,
                             .shared_state = shared_state.get(),
                             .shared_state_map = {},
                             .tsink = TDataSink()};
    st = sink_operator->setup_local_state(_helper.runtime_state.get(), info);
    ASSERT_TRUE(st.ok()) << "setup_local_state failed: " << st.to_string();

    auto* local_state = _helper.runtime_state->get_sink_local_state();
    ASSERT_TRUE(local_state != nullptr);

    st = local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

    auto block = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(
            {1, 2, 3, 4, 2, 3, 4, 3, 4, 4});

    block.insert(vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeInt32>(
            {1, 2, 3, 4, 2, 3, 4, 3, 4, 4}));

    ASSERT_GT(sink_operator->get_reserve_mem_size(_helper.runtime_state.get(), false), 0);
    st = sink_operator->sink(_helper.runtime_state.get(), &block, false);
    ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();

    ASSERT_GT(sink_operator->get_reserve_mem_size(_helper.runtime_state.get(), true), 0);
    st = sink_operator->sink(_helper.runtime_state.get(), &block, true);
    ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();
    ASSERT_FALSE(dep->is_blocked_by());

    st = sink_operator->close(_helper.runtime_state.get(), st);
    ASSERT_TRUE(st.ok()) << "close failed: " << st.to_string();
}

TEST_F(PartitionedAggregationSinkOperatorTest, SinkWithEmptyEOS) {
    auto [source_operator, sink_operator] = _helper.create_operators();
    ASSERT_TRUE(source_operator != nullptr);
    ASSERT_TRUE(sink_operator != nullptr);

    const auto tnode = _helper.create_test_plan_node();
    auto st = sink_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();

    st = sink_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "prepare failed: " << st.to_string();

    auto shared_state = sink_operator->create_shared_state();
    auto* dep = shared_state->create_source_dependency(source_operator->operator_id(),
                                                       source_operator->node_id(),
                                                       "PartitionedAggSinkTestDep");

    LocalSinkStateInfo info {.task_idx = 0,
                             .parent_profile = _helper.operator_profile.get(),
                             .sender_id = 0,
                             .shared_state = shared_state.get(),
                             .shared_state_map = {},
                             .tsink = TDataSink()};
    st = sink_operator->setup_local_state(_helper.runtime_state.get(), info);
    ASSERT_TRUE(st.ok()) << "setup_local_state failed: " << st.to_string();

    auto* local_state = _helper.runtime_state->get_sink_local_state();
    ASSERT_TRUE(local_state != nullptr);

    st = local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

    auto block = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(
            {1, 2, 3, 4, 2, 3, 4, 3, 4, 4});

    block.insert(vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeInt32>(
            {1, 2, 3, 4, 2, 3, 4, 3, 4, 4}));

    ASSERT_GT(sink_operator->get_reserve_mem_size(_helper.runtime_state.get(), false), 0);
    st = sink_operator->sink(_helper.runtime_state.get(), &block, false);
    ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();

    ASSERT_GT(sink_operator->get_reserve_mem_size(_helper.runtime_state.get(), true), 0);
    block.clear_column_data();
    st = sink_operator->sink(_helper.runtime_state.get(), &block, true);
    ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();
    ASSERT_FALSE(dep->is_blocked_by());

    st = sink_operator->close(_helper.runtime_state.get(), st);
    ASSERT_TRUE(st.ok()) << "close failed: " << st.to_string();
}

TEST_F(PartitionedAggregationSinkOperatorTest, SinkWithSpill) {
    auto [source_operator, sink_operator] = _helper.create_operators();
    ASSERT_TRUE(source_operator != nullptr);
    ASSERT_TRUE(sink_operator != nullptr);

    const auto tnode = _helper.create_test_plan_node();
    auto st = sink_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();

    st = sink_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "prepare failed: " << st.to_string();

    auto shared_state = sink_operator->create_shared_state();
    auto* dep = shared_state->create_source_dependency(source_operator->operator_id(),
                                                       source_operator->node_id(),
                                                       "PartitionedAggSinkTestDep");

    LocalSinkStateInfo info {.task_idx = 0,
                             .parent_profile = _helper.operator_profile.get(),
                             .sender_id = 0,
                             .shared_state = shared_state.get(),
                             .shared_state_map = {},
                             .tsink = TDataSink()};
    st = sink_operator->setup_local_state(_helper.runtime_state.get(), info);
    ASSERT_TRUE(st.ok()) << "setup_local_state failed: " << st.to_string();

    auto* local_state = reinterpret_cast<PartitionedAggSinkLocalState*>(
            _helper.runtime_state->get_sink_local_state());
    ASSERT_TRUE(local_state != nullptr);

    st = local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

    auto block = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(
            {1, 2, 3, 4, 2, 3, 4, 3, 4, 4});

    block.insert(vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeInt32>(
            {1, 2, 3, 4, 2, 3, 4, 3, 4, 4}));

    st = sink_operator->sink(_helper.runtime_state.get(), &block, false);
    ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();

    ASSERT_GT(sink_operator->revocable_mem_size(_helper.runtime_state.get()), 0);

    auto* inner_sink_local_state = reinterpret_cast<AggSinkLocalState*>(
            local_state->_runtime_state->get_sink_local_state());
    ASSERT_GT(inner_sink_local_state->_get_hash_table_size(), 0);

    st = sink_operator->revoke_memory(_helper.runtime_state.get(), nullptr);
    ASSERT_TRUE(st.ok()) << "revoke_memory failed: " << st.to_string();

    ASSERT_EQ(inner_sink_local_state->_get_hash_table_size(), 0);

    st = sink_operator->sink(_helper.runtime_state.get(), &block, true);
    ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();

    ASSERT_FALSE(dep->is_blocked_by());

    st = sink_operator->close(_helper.runtime_state.get(), st);
    ASSERT_TRUE(st.ok()) << "close failed: " << st.to_string();
}

TEST_F(PartitionedAggregationSinkOperatorTest, SinkWithSpillAndEmptyEOS) {
    auto [source_operator, sink_operator] = _helper.create_operators();
    ASSERT_TRUE(source_operator != nullptr);
    ASSERT_TRUE(sink_operator != nullptr);

    const auto tnode = _helper.create_test_plan_node();
    auto st = sink_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();

    st = sink_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "prepare failed: " << st.to_string();

    auto shared_state = sink_operator->create_shared_state();
    auto* dep = shared_state->create_source_dependency(source_operator->operator_id(),
                                                       source_operator->node_id(),
                                                       "PartitionedAggSinkTestDep");

    LocalSinkStateInfo info {.task_idx = 0,
                             .parent_profile = _helper.operator_profile.get(),
                             .sender_id = 0,
                             .shared_state = shared_state.get(),
                             .shared_state_map = {},
                             .tsink = TDataSink()};
    st = sink_operator->setup_local_state(_helper.runtime_state.get(), info);
    ASSERT_TRUE(st.ok()) << "setup_local_state failed: " << st.to_string();

    auto* local_state = reinterpret_cast<PartitionedAggSinkLocalState*>(
            _helper.runtime_state->get_sink_local_state());
    ASSERT_TRUE(local_state != nullptr);

    st = local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

    auto block = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(
            {1, 2, 3, 4, 2, 3, 4, 3, 4, 4});

    block.insert(vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeInt32>(
            {1, 2, 3, 4, 2, 3, 4, 3, 4, 4}));

    st = sink_operator->sink(_helper.runtime_state.get(), &block, false);
    ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();

    ASSERT_GT(sink_operator->revocable_mem_size(_helper.runtime_state.get()), 0);

    auto* inner_sink_local_state = reinterpret_cast<AggSinkLocalState*>(
            local_state->_runtime_state->get_sink_local_state());
    ASSERT_GT(inner_sink_local_state->_get_hash_table_size(), 0);

    st = sink_operator->revoke_memory(_helper.runtime_state.get(), nullptr);
    ASSERT_TRUE(st.ok()) << "revoke_memory failed: " << st.to_string();

    ASSERT_EQ(inner_sink_local_state->_get_hash_table_size(), 0);

    block.clear_column_data();
    st = sink_operator->sink(_helper.runtime_state.get(), &block, true);
    ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();
    ASSERT_FALSE(dep->is_blocked_by());

    st = sink_operator->close(_helper.runtime_state.get(), st);
    ASSERT_TRUE(st.ok()) << "close failed: " << st.to_string();
}

TEST_F(PartitionedAggregationSinkOperatorTest, SinkWithSpillLargeData) {
    auto [source_operator, sink_operator] = _helper.create_operators();
    ASSERT_TRUE(source_operator != nullptr);
    ASSERT_TRUE(sink_operator != nullptr);

    const auto tnode = _helper.create_test_plan_node();
    auto st = sink_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();

    st = sink_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "prepare failed: " << st.to_string();

    auto shared_state = sink_operator->create_shared_state();
    auto* dep = shared_state->create_source_dependency(source_operator->operator_id(),
                                                       source_operator->node_id(),
                                                       "PartitionedAggSinkTestDep");

    LocalSinkStateInfo info {.task_idx = 0,
                             .parent_profile = _helper.operator_profile.get(),
                             .sender_id = 0,
                             .shared_state = shared_state.get(),
                             .shared_state_map = {},
                             .tsink = TDataSink()};
    st = sink_operator->setup_local_state(_helper.runtime_state.get(), info);
    ASSERT_TRUE(st.ok()) << "setup_local_state failed: " << st.to_string();

    auto* local_state = reinterpret_cast<PartitionedAggSinkLocalState*>(
            _helper.runtime_state->get_sink_local_state());
    ASSERT_TRUE(local_state != nullptr);

    st = local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

    auto block = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(
            {1, 2, 3, 4, 2, 3, 4, 3, 4, 4});

    block.insert(vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeInt32>(
            {1, 2, 3, 4, 2, 3, 4, 3, 4, 4}));

    st = sink_operator->sink(_helper.runtime_state.get(), &block, false);
    ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();

    ASSERT_GT(sink_operator->revocable_mem_size(_helper.runtime_state.get()), 0);

    auto* inner_sink_local_state = reinterpret_cast<AggSinkLocalState*>(
            local_state->_runtime_state->get_sink_local_state());
    ASSERT_GT(inner_sink_local_state->_get_hash_table_size(), 0);

    st = sink_operator->revoke_memory(_helper.runtime_state.get(), nullptr);
    ASSERT_TRUE(st.ok()) << "revoke_memory failed: " << st.to_string();

    auto* spill_write_rows_counter = local_state->custom_profile()->get_counter("SpillWriteRows");
    ASSERT_TRUE(spill_write_rows_counter != nullptr);
    ASSERT_EQ(spill_write_rows_counter->value(), 4);

    ASSERT_EQ(inner_sink_local_state->_get_hash_table_size(), 0);

    const size_t count = 1048576;
    std::vector<int32_t> data(count);
    std::iota(data.begin(), data.end(), 0);
    block = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(data);

    block.insert(
            vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeInt32>(data));
    st = sink_operator->sink(_helper.runtime_state.get(), &block, false);
    ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();

    block.clear_column_data();
    st = sink_operator->sink(_helper.runtime_state.get(), &block, true);
    ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();
    ASSERT_FALSE(dep->is_blocked_by());

    st = sink_operator->close(_helper.runtime_state.get(), st);
    ASSERT_EQ(spill_write_rows_counter->value(), 1048576 + 4);
    ASSERT_TRUE(st.ok()) << "close failed: " << st.to_string();
}

TEST_F(PartitionedAggregationSinkOperatorTest, SinkWithSpilError) {
    auto [source_operator, sink_operator] = _helper.create_operators();
    ASSERT_TRUE(source_operator != nullptr);
    ASSERT_TRUE(sink_operator != nullptr);

    const auto tnode = _helper.create_test_plan_node();
    auto st = sink_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();

    st = sink_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "prepare failed: " << st.to_string();

    auto shared_state = sink_operator->create_shared_state();

    LocalSinkStateInfo info {.task_idx = 0,
                             .parent_profile = _helper.operator_profile.get(),
                             .sender_id = 0,
                             .shared_state = shared_state.get(),
                             .shared_state_map = {},
                             .tsink = TDataSink()};
    st = sink_operator->setup_local_state(_helper.runtime_state.get(), info);
    ASSERT_TRUE(st.ok()) << "setup_local_state failed: " << st.to_string();

    auto* local_state = reinterpret_cast<PartitionedAggSinkLocalState*>(
            _helper.runtime_state->get_sink_local_state());
    ASSERT_TRUE(local_state != nullptr);

    st = local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

    auto block = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(
            {1, 2, 3, 4, 2, 3, 4, 3, 4, 4});

    block.insert(vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeInt32>(
            {1, 2, 3, 4, 2, 3, 4, 3, 4, 4}));

    st = sink_operator->sink(_helper.runtime_state.get(), &block, false);
    ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();

    ASSERT_GT(sink_operator->revocable_mem_size(_helper.runtime_state.get()), 0);

    auto* inner_sink_local_state = reinterpret_cast<AggSinkLocalState*>(
            local_state->_runtime_state->get_sink_local_state());
    ASSERT_GT(inner_sink_local_state->_get_hash_table_size(), 0);

    SpillableDebugPointHelper dp_helper("fault_inject::spill_stream::spill_block");
    st = sink_operator->revoke_memory(_helper.runtime_state.get(), nullptr);
    ASSERT_FALSE(st.ok()) << "spilll status should be failed";
}

} // namespace doris::pipeline