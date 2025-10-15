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
} // namespace doris::pipeline
