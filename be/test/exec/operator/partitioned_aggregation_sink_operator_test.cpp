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

#include "exec/operator/partitioned_aggregation_sink_operator.h"

#include <gtest/gtest.h>

#include <memory>

#include "common/config.h"
#include "core/block/block.h"
#include "core/data_type/data_type_number.h"
#include "exec/operator/aggregation_sink_operator.h"
#include "exec/operator/partitioned_aggregation_test_helper.h"
#include "exec/operator/partitioned_hash_join_probe_operator.h"
#include "exec/operator/partitioned_hash_join_sink_operator.h"
#include "exec/pipeline/pipeline_task.h"
#include "exec/spill/spill_file_manager.h"
#include "runtime/fragment_mgr.h"
#include "runtime/runtime_profile.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {
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

    auto block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 2, 3, 4, 3, 4, 4});

    block.insert(
            ColumnHelper::create_column_with_name<DataTypeInt32>({1, 2, 3, 4, 2, 3, 4, 3, 4, 4}));

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

    auto block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 2, 3, 4, 3, 4, 4});

    block.insert(
            ColumnHelper::create_column_with_name<DataTypeInt32>({1, 2, 3, 4, 2, 3, 4, 3, 4, 4}));

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

    auto block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 2, 3, 4, 3, 4, 4});

    block.insert(
            ColumnHelper::create_column_with_name<DataTypeInt32>({1, 2, 3, 4, 2, 3, 4, 3, 4, 4}));

    st = sink_operator->sink(_helper.runtime_state.get(), &block, false);
    ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();

    auto* inner_sink_local_state = reinterpret_cast<AggSinkLocalState*>(
            local_state->_runtime_state->get_sink_local_state());
    ASSERT_GT(inner_sink_local_state->get_hash_table_size(), 0);

    st = sink_operator->revoke_memory(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "revoke_memory failed: " << st.to_string();

    ASSERT_EQ(inner_sink_local_state->get_hash_table_size(), 0);

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

    auto block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 2, 3, 4, 3, 4, 4});

    block.insert(
            ColumnHelper::create_column_with_name<DataTypeInt32>({1, 2, 3, 4, 2, 3, 4, 3, 4, 4}));

    st = sink_operator->sink(_helper.runtime_state.get(), &block, false);
    ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();

    auto* inner_sink_local_state = reinterpret_cast<AggSinkLocalState*>(
            local_state->_runtime_state->get_sink_local_state());
    ASSERT_GT(inner_sink_local_state->get_hash_table_size(), 0);

    st = sink_operator->revoke_memory(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "revoke_memory failed: " << st.to_string();

    ASSERT_EQ(inner_sink_local_state->get_hash_table_size(), 0);

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

    auto block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 2, 3, 4, 3, 4, 4});

    block.insert(
            ColumnHelper::create_column_with_name<DataTypeInt32>({1, 2, 3, 4, 2, 3, 4, 3, 4, 4}));

    st = sink_operator->sink(_helper.runtime_state.get(), &block, false);
    ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();

    auto* inner_sink_local_state = reinterpret_cast<AggSinkLocalState*>(
            local_state->_runtime_state->get_sink_local_state());
    ASSERT_GT(inner_sink_local_state->get_hash_table_size(), 0);

    st = sink_operator->revoke_memory(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "revoke_memory failed: " << st.to_string();

    auto* spill_write_rows_counter = local_state->custom_profile()->get_counter("SpillWriteRows");
    ASSERT_TRUE(spill_write_rows_counter != nullptr);
    ASSERT_EQ(spill_write_rows_counter->value(), 4);

    ASSERT_EQ(inner_sink_local_state->get_hash_table_size(), 0);

    const size_t count = 1048576;
    std::vector<int32_t> data(count);
    std::iota(data.begin(), data.end(), 0);
    block = ColumnHelper::create_block<DataTypeInt32>(data);

    block.insert(ColumnHelper::create_column_with_name<DataTypeInt32>(data));
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

    auto block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 2, 3, 4, 3, 4, 4});

    block.insert(
            ColumnHelper::create_column_with_name<DataTypeInt32>({1, 2, 3, 4, 2, 3, 4, 3, 4, 4}));

    st = sink_operator->sink(_helper.runtime_state.get(), &block, false);
    ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();

    auto* inner_sink_local_state = reinterpret_cast<AggSinkLocalState*>(
            local_state->_runtime_state->get_sink_local_state());
    ASSERT_GT(inner_sink_local_state->get_hash_table_size(), 0);

    SpillableDebugPointHelper dp_helper("fault_inject::spill_file::spill_block");
    st = sink_operator->revoke_memory(_helper.runtime_state.get());
    ASSERT_FALSE(st.ok()) << "spilll status should be failed";

    st = sink_operator->close(_helper.runtime_state.get(), st);
    ASSERT_TRUE(st.ok()) << "close status should be successful even if spill failed: "
                         << st.to_string();
}

// Test multiple consecutive revoke_memory calls to verify repeated spilling works.
TEST_F(PartitionedAggregationSinkOperatorTest, SinkWithMultipleRevokes) {
    auto [source_operator, sink_operator] = _helper.create_operators();
    ASSERT_TRUE(source_operator != nullptr);
    ASSERT_TRUE(sink_operator != nullptr);

    const auto tnode = _helper.create_test_plan_node();
    auto st = sink_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();

    st = sink_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "prepare failed: " << st.to_string();

    auto shared_state = std::dynamic_pointer_cast<PartitionedAggSharedState>(
            sink_operator->create_shared_state());
    shared_state->create_source_dependency(source_operator->operator_id(),
                                           source_operator->node_id(), "PartitionedAggSinkTestDep");

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

    auto* inner_sink_local_state = reinterpret_cast<AggSinkLocalState*>(
            local_state->_runtime_state->get_sink_local_state());

    // Round 1: sink → revoke
    auto block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});
    block.insert(ColumnHelper::create_column_with_name<DataTypeInt32>({1, 2, 3, 4, 5}));
    st = sink_operator->sink(_helper.runtime_state.get(), &block, false);
    ASSERT_TRUE(st.ok()) << "sink round 1 failed: " << st.to_string();
    ASSERT_GT(inner_sink_local_state->get_hash_table_size(), 0);
    st = sink_operator->revoke_memory(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "revoke round 1 failed: " << st.to_string();
    ASSERT_EQ(inner_sink_local_state->get_hash_table_size(), 0);

    // Round 2: sink more → revoke again
    auto block2 = ColumnHelper::create_block<DataTypeInt32>({6, 7, 8, 9, 10});
    block2.insert(ColumnHelper::create_column_with_name<DataTypeInt32>({6, 7, 8, 9, 10}));
    st = sink_operator->sink(_helper.runtime_state.get(), &block2, false);
    ASSERT_TRUE(st.ok()) << "sink round 2 failed: " << st.to_string();
    ASSERT_GT(inner_sink_local_state->get_hash_table_size(), 0);
    st = sink_operator->revoke_memory(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "revoke round 2 failed: " << st.to_string();
    ASSERT_EQ(inner_sink_local_state->get_hash_table_size(), 0);

    ASSERT_TRUE(shared_state->_is_spilled);

    // Verify spill counters accumulated across rounds
    auto* spill_write_rows_counter = local_state->custom_profile()->get_counter("SpillWriteRows");
    ASSERT_TRUE(spill_write_rows_counter != nullptr);
    ASSERT_EQ(spill_write_rows_counter->value(), 10) << "SpillWriteRows should be 10 (5 per round)";

    // Sink EOS
    block.clear_column_data();
    st = sink_operator->sink(_helper.runtime_state.get(), &block, true);
    ASSERT_TRUE(st.ok()) << "sink eos failed: " << st.to_string();
}

// Test revoke_memory when hash table is empty (no data sunk).
TEST_F(PartitionedAggregationSinkOperatorTest, RevokeMemoryEmpty) {
    auto [source_operator, sink_operator] = _helper.create_operators();

    const auto tnode = _helper.create_test_plan_node();
    auto st = sink_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok());
    st = sink_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok());

    auto shared_state = std::dynamic_pointer_cast<PartitionedAggSharedState>(
            sink_operator->create_shared_state());
    shared_state->create_source_dependency(source_operator->operator_id(),
                                           source_operator->node_id(), "PartitionedAggSinkTestDep");

    LocalSinkStateInfo info {.task_idx = 0,
                             .parent_profile = _helper.operator_profile.get(),
                             .sender_id = 0,
                             .shared_state = shared_state.get(),
                             .shared_state_map = {},
                             .tsink = TDataSink()};
    st = sink_operator->setup_local_state(_helper.runtime_state.get(), info);
    ASSERT_TRUE(st.ok());

    auto* local_state = reinterpret_cast<PartitionedAggSinkLocalState*>(
            _helper.runtime_state->get_sink_local_state());
    ASSERT_TRUE(local_state != nullptr);
    st = local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok());

    // Revoke with no data is a valid operation
    st = sink_operator->revoke_memory(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "revoke_memory on empty should succeed: " << st.to_string();
    ASSERT_TRUE(shared_state->_is_spilled);

    auto* spill_write_rows_counter = local_state->custom_profile()->get_counter("SpillWriteRows");
    ASSERT_TRUE(spill_write_rows_counter != nullptr);
    ASSERT_EQ(spill_write_rows_counter->value(), 0);
}

// Test that AggSinkOperatorX::get_hash_table_size() correctly delegates to local state.
// This validates the new operator-level public interface introduced to decouple
// PartitionedAggSinkOperatorX from the internal aggregate_data_container.
TEST_F(PartitionedAggregationSinkOperatorTest, GetHashTableSizeViaAggSinkOperator) {
    auto [source_operator, sink_operator] = _helper.create_operators();

    const auto tnode = _helper.create_test_plan_node();
    auto st = sink_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();

    st = sink_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "prepare failed: " << st.to_string();

    auto shared_state = std::dynamic_pointer_cast<PartitionedAggSharedState>(
            sink_operator->create_shared_state());
    shared_state->create_source_dependency(source_operator->operator_id(),
                                           source_operator->node_id(), "PartitionedAggSinkTestDep");

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

    auto* inner_sink_local_state = reinterpret_cast<AggSinkLocalState*>(
            local_state->_runtime_state->get_sink_local_state());

    // Hash table should be empty before any data is sinked
    ASSERT_EQ(inner_sink_local_state->get_hash_table_size(), 0);

    auto block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});
    block.insert(ColumnHelper::create_column_with_name<DataTypeInt32>({1, 2, 3, 4, 5}));
    st = sink_operator->sink(_helper.runtime_state.get(), &block, false);
    ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();

    // Hash table should have entries after sinked data
    ASSERT_GT(inner_sink_local_state->get_hash_table_size(), 0);

    st = sink_operator->revoke_memory(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "revoke_memory failed: " << st.to_string();

    // Hash table should be cleared after revoke
    ASSERT_EQ(inner_sink_local_state->get_hash_table_size(), 0);

    st = sink_operator->close(_helper.runtime_state.get(), st);
    ASSERT_TRUE(st.ok()) << "close failed: " << st.to_string();
}

} // namespace doris

namespace doris::pipeline {

// A test fixture that recreates the descriptor table with nullable=true.
// This is necessary to exercise the NULL key path in hash table aggregation,
// where DataWithNullKey::size() = Base::size() + has_null_key counts the NULL entry,
// but aggregate_data_container->total_count() does NOT (NULL key stored separately).
class PartitionedAggregationNullableKeySinkTest : public testing::Test {
protected:
    void SetUp() override {
        _helper.SetUp();
        // Recreate descriptor table with nullable=true so slot 0 (the GROUP BY key) is nullable.
        DescriptorTbl* desc_tbl;
        auto desc_table = _helper.create_test_table_descriptor(true);
        auto st = DescriptorTbl::create(_helper.obj_pool.get(), desc_table, &desc_tbl);
        ASSERT_TRUE(st.ok()) << "create nullable descriptor table failed: " << st.to_string();
        _helper.desc_tbl = desc_tbl;
        _helper.runtime_state->set_desc_tbl(desc_tbl);
    }
    void TearDown() override { _helper.TearDown(); }
    PartitionedAggregationTestHelper _helper;
};

// Test the core bug scenario: when only NULL key data is in the hash table at EOS,
// the old check (aggregate_data_container->total_count() > 0) returns 0 because
// NULL key data is stored outside the aggregate_data_container. This causes EOS to
// skip the final flush, losing the NULL key aggregated result.
//
// The fix uses get_hash_table_size() which calls DataWithNullKey::size():
//   size_t size() const { return Base::size() + has_null_key; }
// This correctly returns 1 when only a NULL key entry exists, triggering the flush.
TEST_F(PartitionedAggregationNullableKeySinkTest, SinkEOSFlushNullKeyOnly) {
    auto [source_operator, sink_operator] = _helper.create_operators();

    // Use a nullable key expression so the aggregation selects a DataWithNullKey hash method.
    auto tnode = _helper.create_test_plan_node();
    tnode.agg_node.grouping_exprs[0].nodes[0].__set_is_nullable(true);

    auto st = sink_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();

    st = sink_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "prepare failed: " << st.to_string();

    auto shared_state = std::dynamic_pointer_cast<PartitionedAggSharedState>(
            sink_operator->create_shared_state());
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

    auto* inner_sink_local_state = reinterpret_cast<AggSinkLocalState*>(
            local_state->_runtime_state->get_sink_local_state());

    // Sink first batch: key=NULL (null_map={1}), value=42.
    // All rows share the same NULL key → aggregated into hash_table.null_key_data.
    // aggregate_data_container->total_count() = 0  (NULL key not in container)
    // get_hash_table_size()                   = 1  (DataWithNullKey::size() = 0 + has_null_key)
    auto block1 = ColumnHelper::create_nullable_block<DataTypeInt32>(
            {0}, {1}); // placeholder value=0, null_map[0]=1 → key is NULL
    block1.insert(ColumnHelper::create_column_with_name<DataTypeInt32>({42}));
    st = sink_operator->sink(_helper.runtime_state.get(), &block1, false);
    ASSERT_TRUE(st.ok()) << "first sink failed: " << st.to_string();
    ASSERT_EQ(inner_sink_local_state->get_hash_table_size(), 1);

    // Spill to disk and mark as spilled.
    st = sink_operator->revoke_memory(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "revoke_memory failed: " << st.to_string();
    ASSERT_EQ(inner_sink_local_state->get_hash_table_size(), 0);
    ASSERT_TRUE(shared_state->_is_spilled);

    auto* spill_write_rows_counter = local_state->custom_profile()->get_counter("SpillWriteRows");
    ASSERT_TRUE(spill_write_rows_counter != nullptr);
    ASSERT_EQ(spill_write_rows_counter->value(), 1);

    // Sink second batch: again key=NULL, value=10.
    // Now _is_spilled=true; the NULL key row goes into hash_table.null_key_data again.
    // aggregate_data_container->total_count() = 0  (NULL key not in container)
    // get_hash_table_size()                   = 1  (DataWithNullKey::size() = 0 + has_null_key)
    auto block2 = ColumnHelper::create_nullable_block<DataTypeInt32>(
            {0}, {1}); // key=NULL, value placeholder
    block2.insert(ColumnHelper::create_column_with_name<DataTypeInt32>({10}));
    st = sink_operator->sink(_helper.runtime_state.get(), &block2, false);
    ASSERT_TRUE(st.ok()) << "second sink failed: " << st.to_string();
    ASSERT_EQ(inner_sink_local_state->get_hash_table_size(), 1);

    // EOS: send an empty block with eos=true.
    // Old code: aggregate_data_container->total_count() = 0 → SKIP flush → NULL key row LOST!
    // New code: get_hash_table_size() = 1 → trigger flush → NULL key row saved.
    Block empty_block;
    st = sink_operator->sink(_helper.runtime_state.get(), &empty_block, true);
    ASSERT_TRUE(st.ok()) << "EOS sink failed: " << st.to_string();

    // Hash table must be empty after EOS flush.
    ASSERT_EQ(inner_sink_local_state->get_hash_table_size(), 0);
    ASSERT_FALSE(dep->is_blocked_by());

    // Two NULL key aggregated rows were spilled (one per revoke/flush cycle).
    ASSERT_EQ(spill_write_rows_counter->value(), 2);

    st = sink_operator->close(_helper.runtime_state.get(), st);
    ASSERT_TRUE(st.ok()) << "close failed: " << st.to_string();
}

} // namespace doris::pipeline