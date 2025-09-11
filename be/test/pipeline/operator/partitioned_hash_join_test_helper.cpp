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
#include <vector>

#include "testutil/creators.h"
#include "testutil/mock/mock_operators.h"

namespace doris::pipeline {
TPlanNode PartitionedHashJoinTestHelper::create_test_plan_node() {
    TPlanNode tnode;
    tnode.node_id = 0;
    tnode.node_type = TPlanNodeType::HASH_JOIN_NODE;
    tnode.num_children = 2;
    tnode.hash_join_node.join_op = TJoinOp::INNER_JOIN;
    tnode.limit = -1;

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

TDescriptorTable PartitionedHashJoinTestHelper::create_test_table_descriptor(
        bool nullable = false) {
    TTupleDescriptorBuilder tuple_builder;
    tuple_builder.add_slot(TSlotDescriptorBuilder()
                                   .type(PrimitiveType::TYPE_INT)
                                   .column_name("col1")
                                   .column_pos(0)
                                   .nullable(nullable)
                                   .build());

    TDescriptorTableBuilder builder;

    tuple_builder.build(&builder);

    TTupleDescriptorBuilder()
            .add_slot(TSlotDescriptorBuilder()
                              .type(TYPE_INT)
                              .column_name("col2")
                              .column_pos(0)
                              .nullable(nullable)
                              .build())
            .build(&builder);

    return builder.desc_tbl();
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
    auto [probe_pipeline, _] = generate_hash_join_pipeline(probe_operator, probe_side_sink_operator,
                                                           sink_operator, child_operator);

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
    std::map<int,
             std::pair<std::shared_ptr<BasicSharedState>, std::vector<std::shared_ptr<Dependency>>>>
            shared_state_map;
    pipeline_task = std::make_shared<PipelineTask>(probe_pipeline, 0, runtime_state.get(), nullptr,
                                                   nullptr, shared_state_map, 0);
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
    shared_state->is_spilled = true;

    ADD_TIMER(local_state->common_profile(), "ExecTime");
    local_state->common_profile()->AddHighWaterMarkCounter("MemoryUsage", TUnit::BYTES, "", 0);
    local_state->init_spill_read_counters();
    local_state->init_spill_write_counters();
    local_state->init_counters();
    local_state->_copy_shared_spill_profile = false;
    local_state->_internal_runtime_profile = std::make_unique<RuntimeProfile>("inner_test");

    local_state->_partitioned_blocks.resize(probe_operator->_partition_count);
    local_state->_probe_spilling_streams.resize(probe_operator->_partition_count);

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
    shared_state->is_spilled = true;

    ADD_TIMER(local_state->common_profile(), "ExecTime");
    local_state->common_profile()->AddHighWaterMarkCounter("MemoryUsage", TUnit::BYTES, "", 0);
    local_state->_internal_runtime_profile = std::make_unique<RuntimeProfile>("inner_test");

    local_state->_dependency = shared_state->create_sink_dependency(
            sink_operator->dests_id().front(), sink_operator->operator_id(),
            "PartitionedHashJoinTestDep");

    shared_state->spilled_streams.resize(sink_operator->_partition_count);
    shared_state->partitioned_build_blocks.resize(sink_operator->_partition_count);

    shared_state->inner_runtime_state = std::make_unique<MockRuntimeState>();
    shared_state->inner_shared_state = std::make_shared<MockHashJoinSharedState>();
    shared_state->setup_shared_profile(local_state->custom_profile());

    state->emplace_sink_local_state(sink_operator->operator_id(), std::move(local_state_uptr));
    return local_state;
}
} // namespace doris::pipeline