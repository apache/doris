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

#include "partitioned_hash_join_test_helper.h"

#include <gen_cpp/DataSinks_types.h>
#include <gen_cpp/Descriptors_types.h>
#include <gmock/gmock-actions.h>
#include <gmock/gmock-function-mocker.h>
#include <gmock/gmock-spec-builders.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <sstream>
#include <vector>

namespace doris::pipeline {
void PartitionedHashJoinTestHelper::SetUp() {
    runtime_state = std::make_unique<MockRuntimeState>();
    obj_pool = std::make_unique<ObjectPool>();

    runtime_profile = std::make_shared<RuntimeProfile>("test");

    query_ctx = generate_one_query();

    runtime_state->_query_ctx = query_ctx.get();
    runtime_state->_query_id = query_ctx->query_id();
    runtime_state->resize_op_id_to_local_state(-100);

    ADD_TIMER(runtime_profile.get(), "ExecTime");
    runtime_profile->AddHighWaterMarkCounter("MemoryUsed", TUnit::BYTES, "", 0);

    auto desc_table = create_test_table_descriptor();
    auto st = DescriptorTbl::create(obj_pool.get(), desc_table, &desc_tbl);
    DCHECK(!desc_table.slotDescriptors.empty());
    EXPECT_TRUE(st.ok()) << "create descriptor table failed: " << st.to_string();
    runtime_state->set_desc_tbl(desc_tbl);

    auto spill_data_dir = std::make_unique<vectorized::SpillDataDir>("/tmp/partitioned_join_test",
                                                                     1024L * 1024 * 4);
    st = io::global_local_filesystem()->create_directory(spill_data_dir->path(), false);
    EXPECT_TRUE(st.ok()) << "create directory: " << spill_data_dir->path()
                         << " failed: " << st.to_string();
    std::unordered_map<std::string, std::unique_ptr<vectorized::SpillDataDir>> data_map;
    data_map.emplace("test", std::move(spill_data_dir));
    auto* spill_stream_manager = new vectorized::SpillStreamManager(std::move(data_map));
    ExecEnv::GetInstance()->_spill_stream_mgr = spill_stream_manager;
    st = spill_stream_manager->init();
    EXPECT_TRUE(st.ok()) << "init spill stream manager failed: " << st.to_string();
}

void PartitionedHashJoinTestHelper::TearDown() {
    ExecEnv::GetInstance()->spill_stream_mgr()->async_cleanup_query(runtime_state->query_id());
    doris::ExecEnv::GetInstance()->spill_stream_mgr()->get_spill_io_thread_pool()->wait();
    doris::ExecEnv::GetInstance()->spill_stream_mgr()->stop();
    SAFE_DELETE(ExecEnv::GetInstance()->_spill_stream_mgr);
}

TPlanNode PartitionedHashJoinTestHelper::create_test_plan_node() {
    TPlanNode tnode;
    tnode.node_id = 0;
    tnode.node_type = TPlanNodeType::HASH_JOIN_NODE;
    tnode.num_children = 2;
    tnode.hash_join_node.join_op = TJoinOp::INNER_JOIN;

    TEqJoinCondition eq_cond;
    eq_cond.left = TExpr();
    eq_cond.right = TExpr();

    tnode.row_tuples.push_back(0);
    tnode.row_tuples.push_back(1);
    tnode.nullable_tuples.push_back(false);
    tnode.nullable_tuples.push_back(false);
    tnode.node_type = TPlanNodeType::HASH_JOIN_NODE;
    tnode.hash_join_node.join_op = TJoinOp::INNER_JOIN;
    tnode.__isset.hash_join_node = true;

    tnode.hash_join_node.vintermediate_tuple_id_list.emplace_back(0);
    tnode.hash_join_node.__isset.vintermediate_tuple_id_list = true;

    tnode.output_tuple_id = 0;
    tnode.__isset.output_tuple_id = true;

    //     TEqJoinCondition& eq_cond = tnode.hash_join_node.eq_join_conjuncts[0];
    eq_cond.left.nodes.emplace_back();
    eq_cond.right.nodes.emplace_back();
    eq_cond.left.nodes[0].node_type = TExprNodeType::SLOT_REF;
    eq_cond.right.nodes[0].node_type = TExprNodeType::SLOT_REF;

    TTypeNode type_node;
    type_node.type = TTypeNodeType::SCALAR;
    type_node.scalar_type.type = TPrimitiveType::INT;
    type_node.__isset.scalar_type = true;

    eq_cond.left.nodes[0].type.types.emplace_back(type_node);
    eq_cond.right.nodes[0].type.types.emplace_back(type_node);
    eq_cond.left.nodes[0].num_children = 0;
    eq_cond.right.nodes[0].num_children = 0;
    eq_cond.left.nodes[0].slot_ref.slot_id = 0;
    eq_cond.right.nodes[0].slot_ref.slot_id = 1;
    tnode.hash_join_node.eq_join_conjuncts.push_back(eq_cond);

    return tnode;
}

std::tuple<std::shared_ptr<PartitionedHashJoinProbeOperatorX>,
           std::shared_ptr<PartitionedHashJoinSinkOperatorX>>
PartitionedHashJoinTestHelper::create_operators() {
    TPlanNode tnode = create_test_plan_node();
    auto desc_tbl = runtime_state->desc_tbl();

    EXPECT_EQ(desc_tbl.get_tuple_descs().size(), 2);

    auto probe_operator = std::make_shared<PartitionedHashJoinProbeOperatorX>(
            obj_pool.get(), tnode, 0, desc_tbl, TEST_PARTITION_COUNT);
    auto sink_operator = std::make_shared<PartitionedHashJoinSinkOperatorX>(
            obj_pool.get(), 0, 0, tnode, desc_tbl, TEST_PARTITION_COUNT);

    auto child_operator = std::make_shared<MockChildOperator>();
    auto probe_side_source_operator = std::make_shared<MockChildOperator>();
    auto probe_side_sink_operator = std::make_shared<MockSinkOperator>();
    auto [probe_pipeline, _] = generate_hash_join_pipeline(probe_operator, child_operator,
                                                           probe_side_sink_operator, sink_operator);

    RowDescriptor row_desc(runtime_state->desc_tbl(), {1}, {false});
    child_operator->_row_descriptor = row_desc;

    RowDescriptor row_desc_probe(runtime_state->desc_tbl(), {0}, {false});
    probe_side_source_operator->_row_descriptor = row_desc_probe;

    EXPECT_TRUE(probe_operator->set_child(probe_side_source_operator));
    EXPECT_TRUE(probe_operator->set_child(child_operator));
    EXPECT_TRUE(sink_operator->set_child(child_operator));

    auto inner_sink_operator = std::make_shared<MockHashJoinBuildOperator>(
            obj_pool.get(), 0, 0, tnode, runtime_state->desc_tbl());
    auto inner_probe_operator = std::make_shared<MockHashJoinProbeOperator>(
            obj_pool.get(), tnode, 0, runtime_state->desc_tbl());

    EXPECT_TRUE(inner_sink_operator->set_child(child_operator));
    EXPECT_TRUE(inner_probe_operator->set_child(probe_side_source_operator));
    EXPECT_TRUE(inner_probe_operator->set_child(child_operator));

    auto st = inner_sink_operator->init(tnode, runtime_state.get());
    EXPECT_TRUE(st.ok()) << "Init inner sink operator failed: " << st.to_string();
    st = inner_probe_operator->init(tnode, runtime_state.get());
    EXPECT_TRUE(st.ok()) << "Init inner probe operator failed: " << st.to_string();

    probe_operator->set_inner_operators(inner_sink_operator, inner_probe_operator);

    sink_operator->set_inner_operators(inner_sink_operator, inner_probe_operator);

    // Setup task and state
    std::map<int, std::pair<std::shared_ptr<LocalExchangeSharedState>, std::shared_ptr<Dependency>>>
            le_state_map;
    pipeline_task = std::make_shared<PipelineTask>(probe_pipeline, 0, runtime_state.get(), nullptr,
                                                   nullptr, le_state_map, 0);
    runtime_state->set_task(pipeline_task.get());
    return {probe_operator, sink_operator};
}

PartitionedHashJoinProbeLocalState* PartitionedHashJoinTestHelper::create_probe_local_state(
        RuntimeState* state, PartitionedHashJoinProbeOperatorX* probe_operator,
        std::shared_ptr<MockPartitionedHashJoinSharedState>& shared_state) {
    auto local_state_uptr =
            std::make_unique<MockPartitionedHashJoinProbeLocalState>(state, probe_operator);
    auto local_state = local_state_uptr.get();
    shared_state = std::make_shared<MockPartitionedHashJoinSharedState>();
    local_state->init_counters();
    local_state->_shared_state = shared_state.get();
    shared_state->need_to_spill = true;

    ADD_TIMER(local_state->profile(), "ExecTime");
    local_state->profile()->AddHighWaterMarkCounter("MemoryUsage", TUnit::BYTES, "", 0);
    local_state->init_spill_read_counters();
    local_state->init_spill_write_counters();
    local_state->init_counters();
    local_state->_copy_shared_spill_profile = false;
    local_state->_internal_runtime_profile = std::make_unique<RuntimeProfile>("inner_test");

    local_state->_partitioned_blocks.resize(probe_operator->_partition_count);
    local_state->_probe_spilling_streams.resize(probe_operator->_partition_count);

    local_state->_spill_dependency =
            Dependency::create_shared(0, 0, "PartitionedHashJoinProbeOperatorTestSpillDep", true);
    shared_state->spilled_streams.resize(probe_operator->_partition_count);
    shared_state->partitioned_build_blocks.resize(probe_operator->_partition_count);

    shared_state->inner_runtime_state = std::make_unique<MockRuntimeState>();
    shared_state->inner_shared_state = std::make_shared<MockHashJoinSharedState>();

    state->emplace_local_state(probe_operator->operator_id(), std::move(local_state_uptr));
    return local_state;
}

PartitionedHashJoinSinkLocalState* PartitionedHashJoinTestHelper::create_sink_local_state(
        RuntimeState* state, PartitionedHashJoinSinkOperatorX* sink_operator,
        std::shared_ptr<MockPartitionedHashJoinSharedState>& shared_state) {
    auto local_state_uptr = std::make_unique<MockPartitionedHashJoinSinkLocalState>(
            sink_operator, state, obj_pool.get());
    auto local_state = local_state_uptr.get();
    shared_state = std::make_shared<MockPartitionedHashJoinSharedState>();
    local_state->init_spill_counters();
    local_state->_shared_state = shared_state.get();
    shared_state->need_to_spill = true;

    ADD_TIMER(local_state->profile(), "ExecTime");
    local_state->profile()->AddHighWaterMarkCounter("MemoryUsage", TUnit::BYTES, "", 0);
    local_state->_internal_runtime_profile = std::make_unique<RuntimeProfile>("inner_test");

    local_state->_dependency = shared_state->create_sink_dependency(
            sink_operator->dests_id().front(), sink_operator->operator_id(),
            "PartitionedHashJoinTestDep");

    local_state->_spill_dependency =
            Dependency::create_shared(0, 0, "PartitionedHashJoinSinkOperatorTestSpillDep", true);

    shared_state->spilled_streams.resize(sink_operator->_partition_count);
    shared_state->partitioned_build_blocks.resize(sink_operator->_partition_count);

    shared_state->inner_runtime_state = std::make_unique<MockRuntimeState>();
    shared_state->inner_shared_state = std::make_shared<MockHashJoinSharedState>();
    shared_state->setup_shared_profile(local_state->profile());

    state->emplace_sink_local_state(sink_operator->operator_id(), std::move(local_state_uptr));
    return local_state;
}
} // namespace doris::pipeline