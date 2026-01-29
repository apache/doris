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

#include "pipeline/exec/partitioned_aggregation_source_operator.h"

#include <gtest/gtest.h>

#include <memory>
#include <numeric>

#include "common/config.h"
#include "partitioned_aggregation_test_helper.h"
#include "pipeline/dependency.h"
#include "pipeline/exec/aggregation_sink_operator.h"
#include "pipeline/exec/operator.h"
#include "pipeline/exec/partitioned_aggregation_sink_operator.h"
#include "pipeline/exec/partitioned_hash_join_probe_operator.h"
#include "pipeline/exec/partitioned_hash_join_sink_operator.h"
#include "pipeline/pipeline_task.h"
#include "runtime/fragment_mgr.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_runtime_state.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_number.h"

namespace doris::pipeline {
class PartitionedAggregationSourceOperatorTest : public testing::Test {
protected:
    void SetUp() override { _helper.SetUp(); }
    void TearDown() override { _helper.TearDown(); }

    PartitionedAggregationTestHelper _helper;
};

TEST_F(PartitionedAggregationSourceOperatorTest, Init) {
    auto [source_operator, sink_operator] = _helper.create_operators();
    ASSERT_TRUE(source_operator != nullptr);
    ASSERT_TRUE(sink_operator != nullptr);

    const auto tnode = _helper.create_test_plan_node();
    auto st = source_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();

    st = source_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "prepare failed: " << st.to_string();

    std::shared_ptr<MockPartitionedAggSharedState> shared_state =
            MockPartitionedAggSharedState::create_shared();

    shared_state->in_mem_shared_state_sptr = std::make_shared<AggSharedState>();
    shared_state->in_mem_shared_state =
            reinterpret_cast<AggSharedState*>(shared_state->in_mem_shared_state_sptr.get());

    LocalStateInfo info {
            .parent_profile = _helper.operator_profile.get(),
            .scan_ranges = {},
            .shared_state = shared_state.get(),
            .shared_state_map = {},
            .task_idx = 0,
    };
    st = source_operator->setup_local_state(_helper.runtime_state.get(), info);
    ASSERT_TRUE(st.ok()) << "setup_local_state failed: " << st.to_string();

    auto* local_state = _helper.runtime_state->get_local_state(source_operator->operator_id());
    ASSERT_TRUE(local_state != nullptr);

    st = local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

    st = source_operator->close(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "close failed: " << st.to_string();

    st = local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();
}

TEST_F(PartitionedAggregationSourceOperatorTest, GetBlockEmpty) {
    auto [source_operator, sink_operator] = _helper.create_operators();
    ASSERT_TRUE(source_operator != nullptr);
    ASSERT_TRUE(sink_operator != nullptr);

    const auto tnode = _helper.create_test_plan_node();
    auto st = source_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();

    st = source_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "prepare failed: " << st.to_string();

    st = sink_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();

    st = sink_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "prepare failed: " << st.to_string();

    auto shared_state = sink_operator->create_shared_state();
    shared_state->create_source_dependency(source_operator->operator_id(),
                                           source_operator->node_id(), "PartitionedAggSinkTestDep");

    LocalSinkStateInfo sink_info {.task_idx = 0,
                                  .parent_profile = _helper.operator_profile.get(),
                                  .sender_id = 0,
                                  .shared_state = shared_state.get(),
                                  .shared_state_map = {},
                                  .tsink = TDataSink()};
    st = sink_operator->setup_local_state(_helper.runtime_state.get(), sink_info);
    ASSERT_TRUE(st.ok()) << "setup_local_state failed: " << st.to_string();

    auto* sink_local_state = _helper.runtime_state->get_sink_local_state();
    ASSERT_TRUE(sink_local_state != nullptr);

    st = sink_local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

    LocalStateInfo info {
            .parent_profile = _helper.operator_profile.get(),
            .scan_ranges = {},
            .shared_state = shared_state.get(),
            .shared_state_map = {},
            .task_idx = 0,
    };
    st = source_operator->setup_local_state(_helper.runtime_state.get(), info);
    ASSERT_TRUE(st.ok()) << "setup_local_state failed: " << st.to_string();

    auto* local_state = reinterpret_cast<PartitionedAggLocalState*>(
            _helper.runtime_state->get_local_state(source_operator->operator_id()));
    ASSERT_TRUE(local_state != nullptr);

    local_state->_copy_shared_spill_profile = false;

    st = local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

    vectorized::Block block;
    bool eos = false;
    st = source_operator->get_block(_helper.runtime_state.get(), &block, &eos);
    ASSERT_TRUE(st.ok()) << "get_block failed: " << st.to_string();

    ASSERT_EQ(block.rows(), 0);
    ASSERT_TRUE(eos);

    st = source_operator->close(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "close failed: " << st.to_string();

    st = local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();
}

TEST_F(PartitionedAggregationSourceOperatorTest, GetBlock) {
    auto [source_operator, sink_operator] = _helper.create_operators();
    ASSERT_TRUE(source_operator != nullptr);
    ASSERT_TRUE(sink_operator != nullptr);

    const auto tnode = _helper.create_test_plan_node();
    auto st = source_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();

    st = source_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "prepare failed: " << st.to_string();

    st = sink_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();

    st = sink_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "prepare failed: " << st.to_string();

    auto shared_state = sink_operator->create_shared_state();
    shared_state->create_source_dependency(source_operator->operator_id(),
                                           source_operator->node_id(), "PartitionedAggSinkTestDep");

    LocalSinkStateInfo sink_info {.task_idx = 0,
                                  .parent_profile = _helper.operator_profile.get(),
                                  .sender_id = 0,
                                  .shared_state = shared_state.get(),
                                  .shared_state_map = {},
                                  .tsink = TDataSink()};
    st = sink_operator->setup_local_state(_helper.runtime_state.get(), sink_info);
    ASSERT_TRUE(st.ok()) << "setup_local_state failed: " << st.to_string();

    auto* sink_local_state = reinterpret_cast<PartitionedAggSinkLocalState*>(
            _helper.runtime_state->get_sink_local_state());
    ASSERT_TRUE(sink_local_state != nullptr);

    st = sink_local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

    auto block = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(
            {1, 2, 3, 4, 2, 3, 4, 3, 4, 4});

    block.insert(vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeInt32>(
            {1, 2, 3, 4, 2, 3, 4, 3, 4, 4}));

    st = sink_operator->sink(_helper.runtime_state.get(), &block, false);
    ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();

    st = sink_operator->sink(_helper.runtime_state.get(), &block, true);
    ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();

    ASSERT_GT(sink_operator->revocable_mem_size(_helper.runtime_state.get()), 0);

    auto* inner_sink_local_state = reinterpret_cast<AggSinkLocalState*>(
            sink_local_state->_runtime_state->get_sink_local_state());
    ASSERT_GT(inner_sink_local_state->_get_hash_table_size(), 0);

    LocalStateInfo info {
            .parent_profile = _helper.operator_profile.get(),
            .scan_ranges = {},
            .shared_state = shared_state.get(),
            .shared_state_map = {},
            .task_idx = 0,
    };
    st = source_operator->setup_local_state(_helper.runtime_state.get(), info);
    ASSERT_TRUE(st.ok()) << "setup_local_state failed: " << st.to_string();

    auto* local_state = reinterpret_cast<PartitionedAggLocalState*>(
            _helper.runtime_state->get_local_state(source_operator->operator_id()));
    ASSERT_TRUE(local_state != nullptr);

    local_state->_copy_shared_spill_profile = false;

    st = local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

    block.clear();
    bool eos = false;
    st = source_operator->get_block(_helper.runtime_state.get(), &block, &eos);
    ASSERT_TRUE(st.ok()) << "get_block failed: " << st.to_string();

    ASSERT_TRUE(eos);
    DCHECK_EQ(block.rows(), 4);
    ASSERT_EQ(block.rows(), 4);

    st = source_operator->close(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "close failed: " << st.to_string();

    st = local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();
}

TEST_F(PartitionedAggregationSourceOperatorTest, GetBlockWithSpill) {
    auto [source_operator, sink_operator] = _helper.create_operators();
    ASSERT_TRUE(source_operator != nullptr);
    ASSERT_TRUE(sink_operator != nullptr);

    const auto tnode = _helper.create_test_plan_node();
    auto st = source_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();

    st = source_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "prepare failed: " << st.to_string();

    st = sink_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();

    st = sink_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "prepare failed: " << st.to_string();

    auto shared_state = std::dynamic_pointer_cast<PartitionedAggSharedState>(
            sink_operator->create_shared_state());
    shared_state->create_source_dependency(source_operator->operator_id(),
                                           source_operator->node_id(), "PartitionedAggSinkTestDep");

    LocalSinkStateInfo sink_info {.task_idx = 0,
                                  .parent_profile = _helper.operator_profile.get(),
                                  .sender_id = 0,
                                  .shared_state = shared_state.get(),
                                  .shared_state_map = {},
                                  .tsink = TDataSink()};
    st = sink_operator->setup_local_state(_helper.runtime_state.get(), sink_info);
    ASSERT_TRUE(st.ok()) << "setup_local_state failed: " << st.to_string();

    auto* sink_local_state = reinterpret_cast<PartitionedAggSinkLocalState*>(
            _helper.runtime_state->get_sink_local_state());
    ASSERT_TRUE(sink_local_state != nullptr);

    st = sink_local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

    auto block = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(
            {1, 2, 3, 4, 2, 3, 4, 3, 4, 4});

    block.insert(vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeInt32>(
            {1, 2, 3, 4, 2, 3, 4, 3, 4, 4}));

    st = sink_operator->sink(_helper.runtime_state.get(), &block, false);
    ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();

    st = sink_operator->revoke_memory(_helper.runtime_state.get(), nullptr);
    ASSERT_TRUE(st.ok()) << "revoke_memory failed: " << st.to_string();

    ASSERT_TRUE(shared_state->is_spilled);

    st = sink_operator->sink(_helper.runtime_state.get(), &block, true);
    ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();

    ASSERT_EQ(sink_operator->revocable_mem_size(_helper.runtime_state.get()), 0);

    auto* inner_sink_local_state = reinterpret_cast<AggSinkLocalState*>(
            sink_local_state->_runtime_state->get_sink_local_state());
    ASSERT_EQ(inner_sink_local_state->_get_hash_table_size(), 0);

    LocalStateInfo info {
            .parent_profile = _helper.operator_profile.get(),
            .scan_ranges = {},
            .shared_state = shared_state.get(),
            .shared_state_map = {},
            .task_idx = 0,
    };
    st = source_operator->setup_local_state(_helper.runtime_state.get(), info);
    ASSERT_TRUE(st.ok()) << "setup_local_state failed: " << st.to_string();

    auto* local_state = reinterpret_cast<PartitionedAggLocalState*>(
            _helper.runtime_state->get_local_state(source_operator->operator_id()));
    ASSERT_TRUE(local_state != nullptr);

    local_state->_copy_shared_spill_profile = false;

    st = local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

    block.clear();
    bool eos = false;
    size_t rows = 0;
    while (!eos) {
        st = source_operator->get_block(_helper.runtime_state.get(), &block, &eos);
        ASSERT_TRUE(st.ok()) << "get_block failed: " << st.to_string();

        rows += block.rows();
        block.clear_column_data();
    }

    ASSERT_TRUE(eos);
    ASSERT_EQ(rows, 4);

    st = source_operator->close(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "close failed: " << st.to_string();

    st = local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();
}

TEST_F(PartitionedAggregationSourceOperatorTest, GetBlockWithSpillError) {
    auto [source_operator, sink_operator] = _helper.create_operators();
    ASSERT_TRUE(source_operator != nullptr);
    ASSERT_TRUE(sink_operator != nullptr);

    const auto tnode = _helper.create_test_plan_node();
    auto st = source_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();

    st = source_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "prepare failed: " << st.to_string();

    st = sink_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();

    st = sink_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "prepare failed: " << st.to_string();

    auto shared_state = std::dynamic_pointer_cast<PartitionedAggSharedState>(
            sink_operator->create_shared_state());
    shared_state->create_source_dependency(source_operator->operator_id(),
                                           source_operator->node_id(), "PartitionedAggSinkTestDep");

    LocalSinkStateInfo sink_info {.task_idx = 0,
                                  .parent_profile = _helper.operator_profile.get(),
                                  .sender_id = 0,
                                  .shared_state = shared_state.get(),
                                  .shared_state_map = {},
                                  .tsink = TDataSink()};
    st = sink_operator->setup_local_state(_helper.runtime_state.get(), sink_info);
    ASSERT_TRUE(st.ok()) << "setup_local_state failed: " << st.to_string();

    auto* sink_local_state = reinterpret_cast<PartitionedAggSinkLocalState*>(
            _helper.runtime_state->get_sink_local_state());
    ASSERT_TRUE(sink_local_state != nullptr);

    st = sink_local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

    auto block = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(
            {1, 2, 3, 4, 2, 3, 4, 3, 4, 4});

    block.insert(vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeInt32>(
            {1, 2, 3, 4, 2, 3, 4, 3, 4, 4}));

    st = sink_operator->sink(_helper.runtime_state.get(), &block, false);
    ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();

    st = sink_operator->revoke_memory(_helper.runtime_state.get(), nullptr);
    ASSERT_TRUE(st.ok()) << "revoke_memory failed: " << st.to_string();

    ASSERT_TRUE(shared_state->is_spilled);

    st = sink_operator->sink(_helper.runtime_state.get(), &block, true);
    ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();

    ASSERT_EQ(sink_operator->revocable_mem_size(_helper.runtime_state.get()), 0);

    auto* inner_sink_local_state = reinterpret_cast<AggSinkLocalState*>(
            sink_local_state->_runtime_state->get_sink_local_state());
    ASSERT_EQ(inner_sink_local_state->_get_hash_table_size(), 0);

    LocalStateInfo info {
            .parent_profile = _helper.operator_profile.get(),
            .scan_ranges = {},
            .shared_state = shared_state.get(),
            .shared_state_map = {},
            .task_idx = 0,
    };
    st = source_operator->setup_local_state(_helper.runtime_state.get(), info);
    ASSERT_TRUE(st.ok()) << "setup_local_state failed: " << st.to_string();

    auto* local_state = reinterpret_cast<PartitionedAggLocalState*>(
            _helper.runtime_state->get_local_state(source_operator->operator_id()));
    ASSERT_TRUE(local_state != nullptr);

    local_state->_copy_shared_spill_profile = false;

    st = local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

    SpillableDebugPointHelper dp_helper("fault_inject::spill_stream::read_next_block");

    block.clear();
    bool eos = false;

    while (!eos && st.ok()) {
        st = source_operator->get_block(_helper.runtime_state.get(), &block, &eos);
        block.clear_column_data();
    }

    ASSERT_FALSE(st.ok());
}

TEST_F(PartitionedAggregationSourceOperatorTest, RevokeMemorySplitBySpillHashTable) {
    auto [source_operator, sink_operator] = _helper.create_operators();
    ASSERT_TRUE(source_operator != nullptr);
    ASSERT_TRUE(sink_operator != nullptr);

    const auto tnode = _helper.create_test_plan_node();
    auto st = source_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();
    st = source_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "prepare failed: " << st.to_string();

    st = sink_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();
    st = sink_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "prepare failed: " << st.to_string();

    auto shared_state = std::dynamic_pointer_cast<PartitionedAggSharedState>(
            sink_operator->create_shared_state());
    shared_state->create_source_dependency(source_operator->operator_id(),
                                           source_operator->node_id(), "PartitionedAggSinkTestDep");

    LocalSinkStateInfo sink_info {.task_idx = 0,
                                  .parent_profile = _helper.operator_profile.get(),
                                  .sender_id = 0,
                                  .shared_state = shared_state.get(),
                                  .shared_state_map = {},
                                  .tsink = TDataSink()};
    st = sink_operator->setup_local_state(_helper.runtime_state.get(), sink_info);
    ASSERT_TRUE(st.ok()) << "setup_local_state failed: " << st.to_string();
    auto* sink_local_state = reinterpret_cast<PartitionedAggSinkLocalState*>(
            _helper.runtime_state->get_sink_local_state());
    ASSERT_TRUE(sink_local_state != nullptr);
    st = sink_local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

    // In BE_TEST, batch_size in _spill_hash_table_to_children is fixed to 32768.
    // Feed more than 32768 distinct group keys to trigger `if (++row_count >= batch_size)`.
    constexpr int32_t row_count = 40000 * 8;
    std::vector<int32_t> group_keys(row_count);
    std::iota(group_keys.begin(), group_keys.end(), 0);
    std::vector<int32_t> agg_values(row_count, 1);
    auto block = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(group_keys);
    block.insert(vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeInt32>(
            agg_values));
    st = sink_operator->sink(_helper.runtime_state.get(), &block, false);
    ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();

    LocalStateInfo source_info {.parent_profile = _helper.operator_profile.get(),
                                .scan_ranges = {},
                                .shared_state = shared_state.get(),
                                .shared_state_map = {},
                                .task_idx = 0};
    st = source_operator->setup_local_state(_helper.runtime_state.get(), source_info);
    ASSERT_TRUE(st.ok()) << "setup_local_state failed: " << st.to_string();
    auto* local_state = reinterpret_cast<PartitionedAggLocalState*>(
            _helper.runtime_state->get_local_state(source_operator->operator_id()));
    ASSERT_TRUE(local_state != nullptr);
    local_state->_copy_shared_spill_profile = false;
    st = local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

    const SpillPartitionId parent_id {0, 0};
    auto& parent_partition = shared_state->get_or_create_agg_partition(parent_id);
    parent_partition.id = parent_id;
    parent_partition.is_split = false;
    parent_partition.spill_streams.clear();
    parent_partition.spilling_stream.reset();
    local_state->_blocks.clear();
    local_state->_current_partition_id = parent_id;
    local_state->_has_current_partition = true;
    local_state->_current_partition_eos = false;
    shared_state->is_spilled = true;

    const auto pending_size_before = shared_state->pending_partitions.size();
    st = source_operator->revoke_memory(_helper.runtime_state.get(), nullptr);
    ASSERT_TRUE(st.ok()) << "source revoke_memory failed: " << st.to_string();

    ASSERT_TRUE(parent_partition.is_split);
    ASSERT_FALSE(local_state->_has_current_partition);
    ASSERT_TRUE(local_state->_current_partition_eos);
    ASSERT_GE(shared_state->pending_partitions.size(), pending_size_before + kSpillFanout);

    size_t child_spilled_bytes = 0;
    for (uint32_t i = 0; i < kSpillFanout; ++i) {
        const auto child_id = parent_id.child(i);
        auto it = shared_state->spill_partitions.find(child_id.key());
        ASSERT_TRUE(it != shared_state->spill_partitions.end());
        child_spilled_bytes += it->second.spilled_bytes;
    }
    ASSERT_GT(child_spilled_bytes, 0);
}
} // namespace doris::pipeline
