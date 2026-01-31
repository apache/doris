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
#include <unordered_map>
#include <unordered_set>

#include "common/config.h"
#include "partitioned_aggregation_test_helper.h"
#include "pipeline/exec/aggregation_sink_operator.h"
#include "pipeline/exec/hierarchical_spill_partition.h"
#include "pipeline/exec/partitioned_hash_join_probe_operator.h"
#include "pipeline/exec/partitioned_hash_join_sink_operator.h"
#include "pipeline/pipeline_task.h"
#include "runtime/descriptors.h"
#include "runtime/fragment_mgr.h"
#include "testutil/column_helper.h"
#include "testutil/creators.h"
#include "testutil/mock/mock_runtime_state.h"
#include "util/runtime_profile.h"
#include "vec/columns/column_vector.h"
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

    auto shared_state = std::dynamic_pointer_cast<PartitionedAggSharedState>(
            sink_operator->create_shared_state());
    ASSERT_TRUE(shared_state != nullptr);
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

    auto shared_state = std::dynamic_pointer_cast<PartitionedAggSharedState>(
            sink_operator->create_shared_state());
    ASSERT_TRUE(shared_state != nullptr);
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

namespace {
// Helper function to create a batch of test data
vectorized::Block create_test_block_batch(size_t batch_size, size_t start_idx,
                                          size_t distinct_keys) {
    std::vector<int32_t> key_data;
    std::vector<int32_t> value_data;
    key_data.reserve(batch_size);
    value_data.reserve(batch_size);

    for (size_t i = 0; i < batch_size; ++i) {
        size_t idx = start_idx + i;
        // Key: use modulo to control distribution, with some hash collision
        int32_t key = static_cast<int32_t>(idx % distinct_keys);
        key_data.push_back(key);
        // Value: use index as value for easy verification
        value_data.push_back(static_cast<int32_t>(idx));
    }

    auto block = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(key_data);
    block.insert(vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeInt32>(
            value_data));
    return block;
}

int64_t expected_sum_for_key(size_t total_rows, size_t distinct_keys, int32_t key) {
    if (key < 0) {
        return 0;
    }
    const size_t first = static_cast<size_t>(key);
    if (first >= total_rows) {
        return 0;
    }
    const int64_t n = static_cast<int64_t>((total_rows - 1 - first) / distinct_keys + 1);
    const int64_t d = static_cast<int64_t>(distinct_keys);
    const int64_t a1 = static_cast<int64_t>(first);
    return n * (2 * a1 + (n - 1) * d) / 2;
}

int64_t expected_count_for_key(size_t total_rows, size_t distinct_keys, int32_t key) {
    if (key < 0) {
        return 0;
    }
    const size_t first = static_cast<size_t>(key);
    if (first >= total_rows) {
        return 0;
    }
    return static_cast<int64_t>((total_rows - 1 - first) / distinct_keys + 1);
}

double expected_avg_for_key(size_t total_rows, size_t distinct_keys, int32_t key) {
    const int64_t count = expected_count_for_key(total_rows, distinct_keys, key);
    if (count == 0) {
        return 0.0;
    }
    return static_cast<double>(expected_sum_for_key(total_rows, distinct_keys, key)) /
           static_cast<double>(count);
}

void configure_avg_agg(TPlanNode& tnode) {
    auto& agg_function = tnode.agg_node.aggregate_functions[0];
    auto& fn_node = agg_function.nodes[0];
    TFunctionName fn_name;
    fn_name.function_name = "avg";
    fn_node.fn.__set_name(fn_name);

    TTypeDesc ret_type;
    auto& ret_type_node = ret_type.types.emplace_back();
    ret_type_node.scalar_type.type = TPrimitiveType::DOUBLE;
    ret_type_node.__isset.scalar_type = true;
    ret_type_node.type = TTypeNodeType::SCALAR;
    ret_type.__set_is_nullable(false);
    fn_node.fn.__set_ret_type(ret_type);
}

TDescriptorTable create_avg_test_table_descriptor(bool nullable = false) {
    TTupleDescriptorBuilder tuple_builder;
    tuple_builder
            .add_slot(TSlotDescriptorBuilder()
                              .type(PrimitiveType::TYPE_INT)
                              .column_name("col1")
                              .column_pos(0)
                              .nullable(nullable)
                              .build())
            .add_slot(TSlotDescriptorBuilder()
                              .type(PrimitiveType::TYPE_INT)
                              .column_name("col2")
                              .column_pos(0)
                              .nullable(nullable)
                              .build());

    TDescriptorTableBuilder builder;
    tuple_builder.build(&builder);

    TTupleDescriptorBuilder()
            .add_slot(TSlotDescriptorBuilder()
                              .type(TYPE_INT)
                              .column_name("col3")
                              .column_pos(0)
                              .nullable(nullable)
                              .build())
            .add_slot(TSlotDescriptorBuilder()
                              .type(TYPE_DOUBLE)
                              .column_name("col4")
                              .column_pos(0)
                              .nullable(nullable)
                              .build())
            .build(&builder);

    TTupleDescriptorBuilder()
            .add_slot(TSlotDescriptorBuilder()
                              .type(TYPE_INT)
                              .column_name("col5")
                              .column_pos(0)
                              .nullable(nullable)
                              .build())
            .add_slot(TSlotDescriptorBuilder()
                              .type(TYPE_DOUBLE)
                              .column_name("col6")
                              .column_pos(0)
                              .nullable(nullable)
                              .build())
            .build(&builder);

    return builder.desc_tbl();
}

// Helper function to configure spill parameters
void configure_spill_params(MockRuntimeState* runtime_state) {
    runtime_state->set_enable_spill(true);
    // Access _query_options through the base class (it's protected, accessible in derived class)
    TQueryOptions& query_options = const_cast<TQueryOptions&>(runtime_state->query_options());
    // Set a smaller buffer limit to trigger partition split more easily
    // With 4 partitions, each partition needs to spill >= 2MB to trigger split
    // Using fewer partitions ensures each partition has more data, making split more likely
    query_options.__set_low_memory_mode_buffer_limit(512 * 1024);
    query_options.__set_min_revocable_mem(512 * 1024); // 512KB
    // Enable low memory mode by setting query context
    if (runtime_state->get_query_ctx()) {
        runtime_state->get_query_ctx()->set_low_memory_mode();
    }
}
} // namespace

TEST_F(PartitionedAggregationSinkOperatorTest, LargeDataSpillWithMultiLevelSplit) {
    // Configure spill parameters
    configure_spill_params(_helper.runtime_state.get());

    auto desc_table = create_avg_test_table_descriptor(false);
    DescriptorTbl* desc_tbl = nullptr;
    auto desc_status = DescriptorTbl::create(_helper.obj_pool.get(), desc_table, &desc_tbl);
    ASSERT_TRUE(desc_status.ok()) << "create descriptor table failed: " << desc_status.to_string();
    _helper.runtime_state->set_desc_tbl(desc_tbl);
    _helper.desc_tbl = desc_tbl;

    auto tnode = _helper.create_test_plan_node();
    tnode.agg_node.need_finalize = true;
    configure_avg_agg(tnode);

    auto [source_operator, sink_operator] = _helper.create_operators(tnode);
    ASSERT_TRUE(source_operator != nullptr);
    ASSERT_TRUE(sink_operator != nullptr);

    auto st = sink_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();

    st = sink_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "prepare failed: " << st.to_string();

    st = source_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();

    st = source_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "prepare failed: " << st.to_string();

    auto shared_state = std::dynamic_pointer_cast<PartitionedAggSharedState>(
            sink_operator->create_shared_state());
    ASSERT_TRUE(shared_state != nullptr);
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

    // Phase 1: Input large amount of data and trigger spill
    // Use configurable row count for testing (can be reduced for faster tests)
    const size_t total_rows = 5 * 1000 * 1000; // 5M rows
    const size_t distinct_keys =
            1000 * 1000 *
            4; // 4M distinct keys to ensure hash table is large enough to trigger spill
    const size_t batch_size = 1024 * 1024; // 1M rows per batch
    const size_t num_batches = (total_rows + batch_size - 1) / batch_size;

    LOG(INFO) << "Starting large data spill test: total_rows=" << total_rows
              << ", distinct_keys=" << distinct_keys << ", num_batches=" << num_batches;

    size_t total_input_rows = 0;
    for (size_t batch_idx = 0; batch_idx < num_batches; ++batch_idx) {
        size_t current_batch_size = std::min(batch_size, total_rows - batch_idx * batch_size);
        size_t start_idx = batch_idx * batch_size;

        auto block = create_test_block_batch(current_batch_size, start_idx, distinct_keys);

        bool eos = (batch_idx == num_batches - 1);
        st = sink_operator->sink(_helper.runtime_state.get(), &block, eos);
        ASSERT_TRUE(st.ok()) << "sink failed at batch " << batch_idx << ": " << st.to_string();

        total_input_rows += block.rows();

        // Trigger spill when memory is large enough
        const auto revocable_mem_size =
                sink_operator->revocable_mem_size(_helper.runtime_state.get());
        if (revocable_mem_size >=
            static_cast<size_t>(_helper.runtime_state->spill_min_revocable_mem())) {
            st = sink_operator->revoke_memory(_helper.runtime_state.get(), nullptr);
            ASSERT_TRUE(st.ok()) << "revoke_memory failed: " << st.to_string();
            std::cout << "batch_idx: " << batch_idx << "/" << num_batches << ", "
                      << revocable_mem_size << " bytes rovoked." << std::endl;
        } else {
            std::cout << "batch_idx: " << batch_idx << "/" << num_batches
                      << ", revocable_mem_size: " << revocable_mem_size
                      << ", _helper.runtime_state->spill_min_revocable_mem(): "
                      << _helper.runtime_state->spill_min_revocable_mem() << std::endl;
        }
    }

    // Finalize sink
    vectorized::Block empty_block;
    st = sink_operator->sink(_helper.runtime_state.get(), &empty_block, true);
    ASSERT_TRUE(st.ok()) << "final sink failed: " << st.to_string();

    // Verify spill was triggered
    ASSERT_TRUE(shared_state->is_spilled) << "Spill should have been triggered";

    // Debug: Print partition spill information
    const auto split_threshold =
            static_cast<size_t>(_helper.runtime_state->low_memory_mode_buffer_limit());
    std::cout << "Split threshold: " << split_threshold << " bytes (" << (split_threshold / 1024)
              << " KB)" << std::endl;

    // Phase 2: Setup source operator and recover data
    LocalStateInfo source_info {
            .parent_profile = _helper.operator_profile.get(),
            .scan_ranges = {},
            .shared_state = shared_state.get(),
            .shared_state_map = {},
            .task_idx = 0,
    };
    st = source_operator->setup_local_state(_helper.runtime_state.get(), source_info);
    ASSERT_TRUE(st.ok()) << "source setup_local_state failed: " << st.to_string();

    auto* source_local_state = reinterpret_cast<PartitionedAggLocalState*>(
            _helper.runtime_state->get_local_state(source_operator->operator_id()));
    ASSERT_TRUE(source_local_state != nullptr);

    source_local_state->_copy_shared_spill_profile = false;

    st = source_local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "source open failed: " << st.to_string();

    // Phase 3: Read all results from source
    vectorized::Block result_block;
    bool eos = false;
    size_t total_output_rows = 0;
    std::unordered_set<int32_t> seen_keys;
    seen_keys.reserve(distinct_keys);

    size_t counter = 0;
    while (!eos) {
        const auto revocable_mem_size =
                source_operator->revocable_mem_size(_helper.runtime_state.get());
        result_block.clear_column_data();
        const auto trigger_spill =
                revocable_mem_size >=
                        static_cast<size_t>(_helper.runtime_state->spill_min_revocable_mem()) &&
                counter++ % 4 == 0;
        _helper.runtime_state->get_query_ctx()->set_memory_sufficient(!trigger_spill);
        st = source_operator->get_block(_helper.runtime_state.get(), &result_block, &eos);
        ASSERT_TRUE(st.ok()) << "get_block failed: " << st.to_string();

        if (trigger_spill) {
            st = source_operator->revoke_memory(_helper.runtime_state.get(), nullptr);
            ASSERT_TRUE(st.ok()) << "source revoke_memory failed: " << st.to_string();
            continue;
        }

        if (result_block.rows() > 0) {
            total_output_rows += result_block.rows();

            // Validate results
            const auto& key_col = result_block.get_by_position(0).column;
            const auto& avg_col = result_block.get_by_position(1).column;
            const auto& avg_data =
                    assert_cast<const vectorized::ColumnFloat64&>(*avg_col).get_data();
            for (size_t i = 0; i < result_block.rows(); ++i) {
                int32_t key = key_col->get_int(i);
                double avg_value = avg_data[i];
                auto [it, inserted] = seen_keys.insert(key);
                EXPECT_TRUE(inserted) << "Duplicate key in output: " << key;
                EXPECT_DOUBLE_EQ(avg_value, expected_avg_for_key(total_rows, distinct_keys, key))
                        << "Avg mismatch for key " << key;
            }
        }
    }

    ASSERT_TRUE(eos) << "Source should reach EOS";
    // Verify split statistics
    auto* split_counter = source_local_state->custom_profile()->get_counter("AggPartitionSplits");
    DCHECK(split_counter != nullptr);
    EXPECT_GT(split_counter->value(), 0) << "Split counter should exist";
    std::cout << "Total partition splits: " << split_counter->value() << std::endl;

    // Phase 4: Verify results
    // Verify row count: output should have distinct_keys rows (after aggregation)
    EXPECT_EQ(seen_keys.size(), distinct_keys)
            << "Output should have " << distinct_keys << " distinct keys, got " << seen_keys.size();

    LOG(INFO) << "Large data spill test completed: input_rows=" << total_input_rows
              << ", output_rows=" << total_output_rows << ", distinct_keys=" << distinct_keys;

    st = source_operator->close(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "source close failed: " << st.to_string();
}

} // namespace doris::pipeline
