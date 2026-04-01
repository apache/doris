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

#include "exec/operator/partitioned_aggregation_source_operator.h"

#include <gtest/gtest.h>

#include <memory>

#include "common/config.h"
#include "core/block/block.h"
#include "core/data_type/data_type_number.h"
#include "exec/common/agg_utils.h"
#include "exec/operator/aggregation_sink_operator.h"
#include "exec/operator/operator.h"
#include "exec/operator/partitioned_aggregation_sink_operator.h"
#include "exec/operator/partitioned_aggregation_test_helper.h"
#include "exec/operator/partitioned_hash_join_probe_operator.h"
#include "exec/operator/partitioned_hash_join_sink_operator.h"
#include "exec/pipeline/dependency.h"
#include "exec/pipeline/pipeline_task.h"
#include "runtime/fragment_mgr.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {
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

    shared_state->_in_mem_shared_state_sptr = std::make_shared<AggSharedState>();
    shared_state->_in_mem_shared_state =
            reinterpret_cast<AggSharedState*>(shared_state->_in_mem_shared_state_sptr.get());

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

    st = local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

    Block block;
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

    auto block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 2, 3, 4, 3, 4, 4});

    block.insert(
            ColumnHelper::create_column_with_name<DataTypeInt32>({1, 2, 3, 4, 2, 3, 4, 3, 4, 4}));

    st = sink_operator->sink(_helper.runtime_state.get(), &block, false);
    ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();

    st = sink_operator->sink(_helper.runtime_state.get(), &block, true);
    ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();

    auto* inner_sink_local_state = reinterpret_cast<AggSinkLocalState*>(
            sink_local_state->_runtime_state->get_sink_local_state());
    ASSERT_GT(inner_sink_local_state->get_hash_table_size(), 0);

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

    auto block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 2, 3, 4, 3, 4, 4});

    block.insert(
            ColumnHelper::create_column_with_name<DataTypeInt32>({1, 2, 3, 4, 2, 3, 4, 3, 4, 4}));

    st = sink_operator->sink(_helper.runtime_state.get(), &block, false);
    ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();

    st = sink_operator->revoke_memory(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "revoke_memory failed: " << st.to_string();

    ASSERT_TRUE(shared_state->_is_spilled);

    st = sink_operator->sink(_helper.runtime_state.get(), &block, true);
    ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();

    ASSERT_EQ(sink_operator->revocable_mem_size(_helper.runtime_state.get()), 0);

    auto* inner_sink_local_state = reinterpret_cast<AggSinkLocalState*>(
            sink_local_state->_runtime_state->get_sink_local_state());
    ASSERT_EQ(inner_sink_local_state->get_hash_table_size(), 0);

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

    auto block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 2, 3, 4, 3, 4, 4});

    block.insert(
            ColumnHelper::create_column_with_name<DataTypeInt32>({1, 2, 3, 4, 2, 3, 4, 3, 4, 4}));

    st = sink_operator->sink(_helper.runtime_state.get(), &block, false);
    ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();

    st = sink_operator->revoke_memory(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "revoke_memory failed: " << st.to_string();

    ASSERT_TRUE(shared_state->_is_spilled);

    st = sink_operator->sink(_helper.runtime_state.get(), &block, true);
    ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();

    ASSERT_EQ(sink_operator->revocable_mem_size(_helper.runtime_state.get()), 0);

    auto* inner_sink_local_state = reinterpret_cast<AggSinkLocalState*>(
            sink_local_state->_runtime_state->get_sink_local_state());
    ASSERT_EQ(inner_sink_local_state->get_hash_table_size(), 0);

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

    st = local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

    SpillableDebugPointHelper dp_helper("fault_inject::spill_file::read_next_block");

    block.clear();
    bool eos = false;

    while (!eos && st.ok()) {
        st = source_operator->get_block(_helper.runtime_state.get(), &block, &eos);
        block.clear_column_data();
    }

    ASSERT_FALSE(st.ok());
}

// Test spill → recover cycle with large data to verify all rows come back.
TEST_F(PartitionedAggregationSourceOperatorTest, GetBlockWithSpillLargeData) {
    auto [source_operator, sink_operator] = _helper.create_operators();

    const auto tnode = _helper.create_test_plan_node();
    auto st = source_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok());
    st = source_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok());

    st = sink_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok());
    st = sink_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok());

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
    ASSERT_TRUE(st.ok());

    auto* sink_local_state = reinterpret_cast<PartitionedAggSinkLocalState*>(
            _helper.runtime_state->get_sink_local_state());
    ASSERT_TRUE(sink_local_state != nullptr);
    st = sink_local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok());

    // Create data with many distinct values to test larger spill sizes
    const size_t count = 10000;
    std::vector<int32_t> data(count);
    std::iota(data.begin(), data.end(), 0);
    auto block = ColumnHelper::create_block<DataTypeInt32>(data);
    block.insert(ColumnHelper::create_column_with_name<DataTypeInt32>(data));

    st = sink_operator->sink(_helper.runtime_state.get(), &block, false);
    ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();

    st = sink_operator->revoke_memory(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "revoke_memory failed: " << st.to_string();
    ASSERT_TRUE(shared_state->_is_spilled);

    // Sink EOS
    block.clear_column_data();
    st = sink_operator->sink(_helper.runtime_state.get(), &block, true);
    ASSERT_TRUE(st.ok());

    // Now read back via source
    LocalStateInfo info {.parent_profile = _helper.operator_profile.get(),
                         .scan_ranges = {},
                         .shared_state = shared_state.get(),
                         .shared_state_map = {},
                         .task_idx = 0};
    st = source_operator->setup_local_state(_helper.runtime_state.get(), info);
    ASSERT_TRUE(st.ok());

    auto* local_state = reinterpret_cast<PartitionedAggLocalState*>(
            _helper.runtime_state->get_local_state(source_operator->operator_id()));
    ASSERT_TRUE(local_state != nullptr);
    st = local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok());

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
    // With GROUP BY, all distinct keys should come back
    ASSERT_EQ(rows, count) << "Expected " << count << " distinct rows";

    st = source_operator->close(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok());
}

// Test multiple spill+recover cycles: sink → revoke → sink more → revoke → eos → source.
TEST_F(PartitionedAggregationSourceOperatorTest, GetBlockWithMultipleSpills) {
    auto [source_operator, sink_operator] = _helper.create_operators();

    const auto tnode = _helper.create_test_plan_node();
    auto st = source_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok());
    st = source_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok());

    st = sink_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok());
    st = sink_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok());

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
    ASSERT_TRUE(st.ok());

    auto* sink_local_state = reinterpret_cast<PartitionedAggSinkLocalState*>(
            _helper.runtime_state->get_sink_local_state());
    ASSERT_TRUE(sink_local_state != nullptr);
    st = sink_local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok());

    // Round 1: sink {1,2,3,4} → revoke
    auto block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4});
    block.insert(ColumnHelper::create_column_with_name<DataTypeInt32>({1, 2, 3, 4}));
    st = sink_operator->sink(_helper.runtime_state.get(), &block, false);
    ASSERT_TRUE(st.ok());
    st = sink_operator->revoke_memory(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(shared_state->_is_spilled);

    // Round 2: sink {5,6,7,8} → revoke
    block = ColumnHelper::create_block<DataTypeInt32>({5, 6, 7, 8});
    block.insert(ColumnHelper::create_column_with_name<DataTypeInt32>({5, 6, 7, 8}));
    st = sink_operator->sink(_helper.runtime_state.get(), &block, false);
    ASSERT_TRUE(st.ok());
    st = sink_operator->revoke_memory(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok());

    // Sink EOS
    block.clear_column_data();
    st = sink_operator->sink(_helper.runtime_state.get(), &block, true);
    ASSERT_TRUE(st.ok());

    // Read back via source
    LocalStateInfo info {.parent_profile = _helper.operator_profile.get(),
                         .scan_ranges = {},
                         .shared_state = shared_state.get(),
                         .shared_state_map = {},
                         .task_idx = 0};
    st = source_operator->setup_local_state(_helper.runtime_state.get(), info);
    ASSERT_TRUE(st.ok());

    auto* local_state = reinterpret_cast<PartitionedAggLocalState*>(
            _helper.runtime_state->get_local_state(source_operator->operator_id()));
    ASSERT_TRUE(local_state != nullptr);
    st = local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok());

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
    ASSERT_EQ(rows, 8) << "Should recover all 8 distinct rows from 2 spill rounds";

    st = source_operator->close(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok());
}
// --- Tests for PartitionedAggSourceOperatorX::revocable_mem_size ---
// The function checks two early-exit conditions then sums three memory sources:
//   (1) !_is_spilled → return 0
//   (2) !_current_partition.spill_file → return 0
//   bytes = sum(_blocks.allocated_bytes) + hash_table_bytes + container.memory_usage()
//   return bytes > spill_min_revocable_mem() ? bytes : 0
//
// create_operators() sets min_revocable_mem=0, so effective threshold = max(0, 1MB) = 1MB.

// Condition 1: not spilled → immediate 0.
TEST_F(PartitionedAggregationSourceOperatorTest, RevocableMemSizeNotSpilledReturnsZero) {
    auto [source_operator, sink_operator] = _helper.create_operators();
    const auto tnode = _helper.create_test_plan_node();
    ASSERT_TRUE(source_operator->init(tnode, _helper.runtime_state.get()).ok());
    ASSERT_TRUE(source_operator->prepare(_helper.runtime_state.get()).ok());

    std::shared_ptr<MockPartitionedAggSharedState> shared_state;
    _helper.create_source_local_state(_helper.runtime_state.get(), source_operator.get(),
                                      shared_state);

    shared_state->_is_spilled = false;
    EXPECT_EQ(source_operator->revocable_mem_size(_helper.runtime_state.get()), 0UL);
}

// Condition 2: spilled but _current_partition.spill_file is null → 0.
TEST_F(PartitionedAggregationSourceOperatorTest, RevocableMemSizeSpilledNoSpillFileReturnsZero) {
    auto [source_operator, sink_operator] = _helper.create_operators();
    const auto tnode = _helper.create_test_plan_node();
    ASSERT_TRUE(source_operator->init(tnode, _helper.runtime_state.get()).ok());
    ASSERT_TRUE(source_operator->prepare(_helper.runtime_state.get()).ok());

    std::shared_ptr<MockPartitionedAggSharedState> shared_state;
    auto* local_state = _helper.create_source_local_state(_helper.runtime_state.get(),
                                                          source_operator.get(), shared_state);

    // create_source_local_state sets _is_spilled = true
    ASSERT_TRUE(shared_state->_is_spilled.load());
    // _current_partition.spill_file defaults to nullptr
    ASSERT_EQ(local_state->_current_partition.spill_file, nullptr);
    EXPECT_EQ(source_operator->revocable_mem_size(_helper.runtime_state.get()), 0UL);
}

// Spilled + valid spill_file, but total bytes < 1MB threshold → 0.
TEST_F(PartitionedAggregationSourceOperatorTest,
       RevocableMemSizeWithSmallBlocksBelowThresholdReturnsZero) {
    auto [source_operator, sink_operator] = _helper.create_operators();
    const auto tnode = _helper.create_test_plan_node();
    ASSERT_TRUE(source_operator->init(tnode, _helper.runtime_state.get()).ok());
    ASSERT_TRUE(source_operator->prepare(_helper.runtime_state.get()).ok());

    std::shared_ptr<MockPartitionedAggSharedState> shared_state;
    auto* local_state = _helper.create_source_local_state(_helper.runtime_state.get(),
                                                          source_operator.get(), shared_state);

    auto small_block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3});
    ASSERT_LT(small_block.allocated_bytes(), 1UL << 20); // < 1MB
    local_state->_blocks.push_back(std::move(small_block));

    SpillFileSPtr spill_file;
    ASSERT_TRUE(ExecEnv::GetInstance()
                        ->spill_file_mgr()
                        ->create_spill_file("ut/revocable_small", spill_file)
                        .ok());
    local_state->_current_partition.spill_file = spill_file;

    EXPECT_EQ(source_operator->revocable_mem_size(_helper.runtime_state.get()), 0UL);
}

// Spilled + valid spill_file + large blocks (>1MB) → returns block bytes.
TEST_F(PartitionedAggregationSourceOperatorTest, RevocableMemSizeWithLargeBlocksReturnsBytes) {
    auto [source_operator, sink_operator] = _helper.create_operators();
    const auto tnode = _helper.create_test_plan_node();
    ASSERT_TRUE(source_operator->init(tnode, _helper.runtime_state.get()).ok());
    ASSERT_TRUE(source_operator->prepare(_helper.runtime_state.get()).ok());

    std::shared_ptr<MockPartitionedAggSharedState> shared_state;
    auto* local_state = _helper.create_source_local_state(_helper.runtime_state.get(),
                                                          source_operator.get(), shared_state);

    std::vector<int32_t> large_data(300000);
    std::iota(large_data.begin(), large_data.end(), 0);
    auto large_block = ColumnHelper::create_block<DataTypeInt32>(large_data);
    const size_t block_bytes = large_block.allocated_bytes();
    ASSERT_GT(block_bytes, 1UL << 20); // > 1MB threshold
    local_state->_blocks.push_back(std::move(large_block));

    SpillFileSPtr spill_file;
    ASSERT_TRUE(ExecEnv::GetInstance()
                        ->spill_file_mgr()
                        ->create_spill_file("ut/revocable_blocks", spill_file)
                        .ok());
    local_state->_current_partition.spill_file = spill_file;

    EXPECT_EQ(source_operator->revocable_mem_size(_helper.runtime_state.get()), block_bytes);
}

// Spilled + valid spill_file + aggregate_data_container (>1MB) → returns container bytes.
TEST_F(PartitionedAggregationSourceOperatorTest, RevocableMemSizeWithAggContainerCountsMemory) {
    auto [source_operator, sink_operator] = _helper.create_operators();
    const auto tnode = _helper.create_test_plan_node();
    ASSERT_TRUE(source_operator->init(tnode, _helper.runtime_state.get()).ok());
    ASSERT_TRUE(source_operator->prepare(_helper.runtime_state.get()).ok());

    std::shared_ptr<MockPartitionedAggSharedState> shared_state;
    auto* local_state = _helper.create_source_local_state(_helper.runtime_state.get(),
                                                          source_operator.get(), shared_state);

    auto agg_sptr = std::make_shared<AggSharedState>();
    shared_state->_in_mem_shared_state_sptr = agg_sptr;
    shared_state->_in_mem_shared_state = agg_sptr.get();
    agg_sptr->aggregate_data_container =
            std::make_unique<AggregateDataContainer>(sizeof(uint32_t), 8);
    // ~13 sub-containers of 8192 entries each ≈ 1.28 MB → exceeds 1MB threshold
    for (uint32_t i = 0; i < 100000; ++i) {
        agg_sptr->aggregate_data_container->append_data<uint32_t>(i);
    }
    const size_t container_bytes = agg_sptr->aggregate_data_container->memory_usage();
    ASSERT_GT(container_bytes, 1UL << 20);

    SpillFileSPtr spill_file;
    ASSERT_TRUE(ExecEnv::GetInstance()
                        ->spill_file_mgr()
                        ->create_spill_file("ut/revocable_container", spill_file)
                        .ok());
    local_state->_current_partition.spill_file = spill_file;

    EXPECT_EQ(source_operator->revocable_mem_size(_helper.runtime_state.get()), container_bytes);
}

// --- Tests for PartitionedAggSourceOperatorX::revoke_memory ---
// revoke_memory:
//   if (!_is_spilled) return OK  (no-op)
//   else: _flush_and_repartition → reset _current_partition + _need_to_setup_partition

// Path 1: not spilled → immediate OK with no state change.
TEST_F(PartitionedAggregationSourceOperatorTest, RevokeMemoryNotSpilledIsNoOp) {
    auto [source_operator, sink_operator] = _helper.create_operators();
    const auto tnode = _helper.create_test_plan_node();
    ASSERT_TRUE(source_operator->init(tnode, _helper.runtime_state.get()).ok());
    ASSERT_TRUE(source_operator->prepare(_helper.runtime_state.get()).ok());

    std::shared_ptr<MockPartitionedAggSharedState> shared_state;
    auto* local_state = _helper.create_source_local_state(_helper.runtime_state.get(),
                                                          source_operator.get(), shared_state);
    shared_state->_is_spilled = false;

    const bool partition_before = local_state->_need_to_setup_partition;
    auto st = source_operator->revoke_memory(_helper.runtime_state.get());
    EXPECT_TRUE(st.ok()) << st.to_string();
    // State must be unchanged
    EXPECT_EQ(local_state->_current_partition.spill_file, nullptr);
    EXPECT_EQ(local_state->_need_to_setup_partition, partition_before);
}

// Path 2: spilled → _flush_and_repartition runs, current partition is reset.
TEST_F(PartitionedAggregationSourceOperatorTest, RevokeMemorySpilledResetsPartitionState) {
    auto [source_operator, sink_operator] = _helper.create_operators();
    const auto tnode = _helper.create_test_plan_node();
    ASSERT_TRUE(source_operator->init(tnode, _helper.runtime_state.get()).ok());
    ASSERT_TRUE(source_operator->prepare(_helper.runtime_state.get()).ok());
    ASSERT_TRUE(sink_operator->init(tnode, _helper.runtime_state.get()).ok());
    ASSERT_TRUE(sink_operator->prepare(_helper.runtime_state.get()).ok());

    auto shared_state = std::dynamic_pointer_cast<PartitionedAggSharedState>(
            sink_operator->create_shared_state());
    shared_state->create_source_dependency(source_operator->operator_id(),
                                           source_operator->node_id(), "RevokeMemoryTestDep");

    LocalSinkStateInfo sink_info {.task_idx = 0,
                                  .parent_profile = _helper.operator_profile.get(),
                                  .sender_id = 0,
                                  .shared_state = shared_state.get(),
                                  .shared_state_map = {},
                                  .tsink = TDataSink()};
    ASSERT_TRUE(sink_operator->setup_local_state(_helper.runtime_state.get(), sink_info).ok());
    auto* sink_local_state = reinterpret_cast<PartitionedAggSinkLocalState*>(
            _helper.runtime_state->get_sink_local_state());
    ASSERT_TRUE(sink_local_state->open(_helper.runtime_state.get()).ok());

    auto block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4});
    block.insert(ColumnHelper::create_column_with_name<DataTypeInt32>({1, 2, 3, 4}));
    ASSERT_TRUE(sink_operator->sink(_helper.runtime_state.get(), &block, false).ok());
    ASSERT_TRUE(sink_operator->revoke_memory(_helper.runtime_state.get()).ok());
    ASSERT_TRUE(shared_state->_is_spilled);
    ASSERT_TRUE(sink_operator->sink(_helper.runtime_state.get(), &block, true).ok());

    LocalStateInfo info {.parent_profile = _helper.operator_profile.get(),
                         .scan_ranges = {},
                         .shared_state = shared_state.get(),
                         .shared_state_map = {},
                         .task_idx = 0};
    ASSERT_TRUE(source_operator->setup_local_state(_helper.runtime_state.get(), info).ok());
    auto* local_state = reinterpret_cast<PartitionedAggLocalState*>(
            _helper.runtime_state->get_local_state(source_operator->operator_id()));
    ASSERT_TRUE(local_state != nullptr);
    ASSERT_TRUE(local_state->open(_helper.runtime_state.get()).ok());

    // First get_block: sets up partition queue and recovers blocks from disk (yields without merging)
    Block out;
    bool eos = false;
    ASSERT_TRUE(source_operator->get_block(_helper.runtime_state.get(), &out, &eos).ok());
    ASSERT_FALSE(eos);

    // revoke_memory flushes the (empty) hash table and repartitions recovered _blocks
    auto st = source_operator->revoke_memory(_helper.runtime_state.get());
    EXPECT_TRUE(st.ok()) << st.to_string();

    // Current partition must be reset; _need_to_setup_partition set back to true
    EXPECT_EQ(local_state->_current_partition.spill_file, nullptr);
    EXPECT_TRUE(local_state->_need_to_setup_partition);

    st = source_operator->close(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok());
}

// Path 3: spilled but partition level has reached max depth → InternalError.
// Uses -fno-access-control to set the private level field directly.
TEST_F(PartitionedAggregationSourceOperatorTest, RevokeMemoryAtMaxDepthReturnsError) {
    auto [source_operator, sink_operator] = _helper.create_operators();
    const auto tnode = _helper.create_test_plan_node();
    ASSERT_TRUE(source_operator->init(tnode, _helper.runtime_state.get()).ok());
    ASSERT_TRUE(source_operator->prepare(_helper.runtime_state.get()).ok());

    std::shared_ptr<MockPartitionedAggSharedState> shared_state;
    auto* local_state = _helper.create_source_local_state(_helper.runtime_state.get(),
                                                          source_operator.get(), shared_state);
    // _is_spilled = true is already set by create_source_local_state.

    // _flush_and_repartition checks: new_level = level + 1 >= _repartition_max_depth.
    // SpillRepartitioner::MAX_DEPTH = 8; set level = 7 so new_level = 8 >= 8 → error.
    local_state->_current_partition.level =
            static_cast<int>(source_operator->_repartition_max_depth) - 1;

    auto st = source_operator->revoke_memory(_helper.runtime_state.get());
    EXPECT_FALSE(st.ok());
    EXPECT_EQ(st.code(), ErrorCode::INTERNAL_ERROR);
}

// --- Tests for PartitionedAggLocalState::_flush_hash_table_to_sub_spill_files ---
// The function:
//   1. Calls aggregate_data_container->init_once() (requires non-null container).
//   2. Loops calling _agg_source_operator->get_serialized_block(); non-empty blocks are
//      routed via _repartitioner.route_block() (repartitioner must be set up first).
//   3. Calls _agg_source_operator->reset_hash_table() to clear the hash table.
//
// Tests use the full pipeline path (sink → revoke → source open) to ensure
// _runtime_state and _in_mem_shared_state are valid.

// Test 1: Hash table has merged data → blocks are serialised and routed successfully.
// Repartitioner is initialised manually before calling the function.
TEST_F(PartitionedAggregationSourceOperatorTest, FlushHashTableToSubSpillFilesSucceeds) {
    auto [source_operator, sink_operator] = _helper.create_operators();
    const auto tnode = _helper.create_test_plan_node();
    ASSERT_TRUE(source_operator->init(tnode, _helper.runtime_state.get()).ok());
    ASSERT_TRUE(source_operator->prepare(_helper.runtime_state.get()).ok());
    ASSERT_TRUE(sink_operator->init(tnode, _helper.runtime_state.get()).ok());
    ASSERT_TRUE(sink_operator->prepare(_helper.runtime_state.get()).ok());

    auto shared_state = std::dynamic_pointer_cast<PartitionedAggSharedState>(
            sink_operator->create_shared_state());
    shared_state->create_source_dependency(source_operator->operator_id(),
                                           source_operator->node_id(), "FlushHtTestDep");

    LocalSinkStateInfo sink_info {.task_idx = 0,
                                  .parent_profile = _helper.operator_profile.get(),
                                  .sender_id = 0,
                                  .shared_state = shared_state.get(),
                                  .shared_state_map = {},
                                  .tsink = TDataSink()};
    ASSERT_TRUE(sink_operator->setup_local_state(_helper.runtime_state.get(), sink_info).ok());
    auto* sink_local_state = reinterpret_cast<PartitionedAggSinkLocalState*>(
            _helper.runtime_state->get_sink_local_state());
    ASSERT_TRUE(sink_local_state->open(_helper.runtime_state.get()).ok());

    // Sink data then spill so the source path is exercised.
    auto block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 2, 3, 4, 3, 4, 4});
    block.insert(
            ColumnHelper::create_column_with_name<DataTypeInt32>({1, 2, 3, 4, 2, 3, 4, 3, 4, 4}));
    ASSERT_TRUE(sink_operator->sink(_helper.runtime_state.get(), &block, false).ok());
    ASSERT_TRUE(sink_operator->revoke_memory(_helper.runtime_state.get()).ok());
    ASSERT_TRUE(shared_state->_is_spilled);
    block.clear_column_data();
    ASSERT_TRUE(sink_operator->sink(_helper.runtime_state.get(), &block, true).ok());

    // Open source operator to set up _runtime_state and _in_mem_shared_state.
    LocalStateInfo info {.parent_profile = _helper.operator_profile.get(),
                         .scan_ranges = {},
                         .shared_state = shared_state.get(),
                         .shared_state_map = {},
                         .task_idx = 0};
    ASSERT_TRUE(source_operator->setup_local_state(_helper.runtime_state.get(), info).ok());
    auto* local_state = reinterpret_cast<PartitionedAggLocalState*>(
            _helper.runtime_state->get_local_state(source_operator->operator_id()));
    ASSERT_TRUE(local_state != nullptr);
    ASSERT_TRUE(local_state->open(_helper.runtime_state.get()).ok());

    // Phase 2: first get_block recovers serialised blocks from disk into _blocks.
    Block out;
    bool eos = false;
    ASSERT_TRUE(source_operator->get_block(_helper.runtime_state.get(), &out, &eos).ok());
    ASSERT_FALSE(eos);

    // Phase 3: second get_block merges _blocks into the hash table.
    // After this call aggregate_data_container is non-null and the hash table has data.
    out.clear();
    ASSERT_TRUE(source_operator->get_block(_helper.runtime_state.get(), &out, &eos).ok());
    ASSERT_FALSE(eos);

    auto* in_mem_state = shared_state->_in_mem_shared_state;
    ASSERT_NE(in_mem_state, nullptr);
    ASSERT_NE(in_mem_state->aggregate_data_container, nullptr);

    // Set up the repartitioner the same way _flush_and_repartition does.
    const int new_level = local_state->_current_partition.level + 1;
    const int fanout = static_cast<int>(source_operator->_partition_count);
    size_t num_keys = in_mem_state->probe_expr_ctxs.size();
    std::vector<size_t> key_column_indices(num_keys);
    std::vector<DataTypePtr> key_data_types(num_keys);
    for (size_t i = 0; i < num_keys; ++i) {
        key_column_indices[i] = i;
        key_data_types[i] = in_mem_state->probe_expr_ctxs[i]->root()->data_type();
    }
    std::vector<SpillFileSPtr> output_spill_files;
    ASSERT_TRUE(SpillRepartitioner::create_output_spill_files(
                        _helper.runtime_state.get(), source_operator->node_id(), "ut/flush_ht_test",
                        fanout, output_spill_files)
                        .ok());
    local_state->_repartitioner.init_with_key_columns(
            key_column_indices, key_data_types, local_state->operator_profile(), fanout, new_level);
    ASSERT_TRUE(local_state->_repartitioner
                        .setup_output(_helper.runtime_state.get(), output_spill_files)
                        .ok());

    // Call the function under test.
    auto st = local_state->_flush_hash_table_to_sub_spill_files(_helper.runtime_state.get());
    EXPECT_TRUE(st.ok()) << st.to_string();

    // Flush writers and release file resources.
    ASSERT_TRUE(local_state->_repartitioner.finalize().ok());

    st = source_operator->close(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok());
}

// Test 2: No merge phase — hash table is in monostate (no rows aggregated yet).
// get_serialized_block returns inner_eos=true immediately → route_block is never called,
// so the repartitioner does not need setup_output. reset_hash_table is still called.
TEST_F(PartitionedAggregationSourceOperatorTest,
       FlushHashTableToSubSpillFilesEmptyHashTableSucceeds) {
    auto [source_operator, sink_operator] = _helper.create_operators();
    const auto tnode = _helper.create_test_plan_node();
    ASSERT_TRUE(source_operator->init(tnode, _helper.runtime_state.get()).ok());
    ASSERT_TRUE(source_operator->prepare(_helper.runtime_state.get()).ok());
    ASSERT_TRUE(sink_operator->init(tnode, _helper.runtime_state.get()).ok());
    ASSERT_TRUE(sink_operator->prepare(_helper.runtime_state.get()).ok());

    auto shared_state = std::dynamic_pointer_cast<PartitionedAggSharedState>(
            sink_operator->create_shared_state());
    shared_state->create_source_dependency(source_operator->operator_id(),
                                           source_operator->node_id(), "FlushHtEmptyDep");

    LocalSinkStateInfo sink_info {.task_idx = 0,
                                  .parent_profile = _helper.operator_profile.get(),
                                  .sender_id = 0,
                                  .shared_state = shared_state.get(),
                                  .shared_state_map = {},
                                  .tsink = TDataSink()};
    ASSERT_TRUE(sink_operator->setup_local_state(_helper.runtime_state.get(), sink_info).ok());
    auto* sink_local_state = reinterpret_cast<PartitionedAggSinkLocalState*>(
            _helper.runtime_state->get_sink_local_state());
    ASSERT_TRUE(sink_local_state->open(_helper.runtime_state.get()).ok());

    // Spill data to disk so the source enters the spilled path on open.
    auto block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4});
    block.insert(ColumnHelper::create_column_with_name<DataTypeInt32>({1, 2, 3, 4}));
    ASSERT_TRUE(sink_operator->sink(_helper.runtime_state.get(), &block, false).ok());
    ASSERT_TRUE(sink_operator->revoke_memory(_helper.runtime_state.get()).ok());
    ASSERT_TRUE(shared_state->_is_spilled);
    block.clear_column_data();
    ASSERT_TRUE(sink_operator->sink(_helper.runtime_state.get(), &block, true).ok());

    // Open source operator: _runtime_state and _in_mem_shared_state are now valid
    // with a freshly created, empty hash table (no data merged yet).
    LocalStateInfo info {.parent_profile = _helper.operator_profile.get(),
                         .scan_ranges = {},
                         .shared_state = shared_state.get(),
                         .shared_state_map = {},
                         .task_idx = 0};
    ASSERT_TRUE(source_operator->setup_local_state(_helper.runtime_state.get(), info).ok());
    auto* local_state = reinterpret_cast<PartitionedAggLocalState*>(
            _helper.runtime_state->get_local_state(source_operator->operator_id()));
    ASSERT_TRUE(local_state != nullptr);
    ASSERT_TRUE(local_state->open(_helper.runtime_state.get()).ok());

    // aggregate_data_container is initialised by the sink's open phase (not null).
    // The hash table is in monostate (no data merged via merge_with_serialized_key_helper),
    // so get_serialized_block returns inner_eos=true immediately and route_block is
    // never reached — the repartitioner does not need setup_output.
    auto* in_mem_state = shared_state->_in_mem_shared_state;
    ASSERT_NE(in_mem_state, nullptr);
    ASSERT_NE(in_mem_state->aggregate_data_container, nullptr);
    auto st = local_state->_flush_hash_table_to_sub_spill_files(_helper.runtime_state.get());
    EXPECT_TRUE(st.ok()) << st.to_string();

    st = source_operator->close(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok());
}

// Null _in_mem_shared_state: only block bytes contribute, hash table + container skipped.
TEST_F(PartitionedAggregationSourceOperatorTest, RevocableMemSizeNullInMemStateSkipsHashTable) {
    auto [source_operator, sink_operator] = _helper.create_operators();
    const auto tnode = _helper.create_test_plan_node();
    ASSERT_TRUE(source_operator->init(tnode, _helper.runtime_state.get()).ok());
    ASSERT_TRUE(source_operator->prepare(_helper.runtime_state.get()).ok());

    std::shared_ptr<MockPartitionedAggSharedState> shared_state;
    auto* local_state = _helper.create_source_local_state(_helper.runtime_state.get(),
                                                          source_operator.get(), shared_state);

    std::vector<int32_t> large_data(300000);
    std::iota(large_data.begin(), large_data.end(), 0);
    auto large_block = ColumnHelper::create_block<DataTypeInt32>(large_data);
    const size_t block_bytes = large_block.allocated_bytes();
    local_state->_blocks.push_back(std::move(large_block));

    SpillFileSPtr spill_file;
    ASSERT_TRUE(ExecEnv::GetInstance()
                        ->spill_file_mgr()
                        ->create_spill_file("ut/revocable_null_state", spill_file)
                        .ok());
    local_state->_current_partition.spill_file = spill_file;

    // _in_mem_shared_state is null → hash table and container contribute 0 bytes
    ASSERT_EQ(shared_state->_in_mem_shared_state, nullptr);
    EXPECT_EQ(source_operator->revocable_mem_size(_helper.runtime_state.get()), block_bytes);
}

} // namespace doris
