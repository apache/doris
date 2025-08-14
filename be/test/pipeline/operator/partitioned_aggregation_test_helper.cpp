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

#include "partitioned_aggregation_test_helper.h"

#include <gen_cpp/DataSinks_types.h>
#include <gen_cpp/Descriptors_types.h>
#include <gmock/gmock-actions.h>
#include <gmock/gmock-function-mocker.h>
#include <gmock/gmock-spec-builders.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "runtime/define_primitive_type.h"
#include "testutil/creators.h"
#include "testutil/mock/mock_operators.h"

namespace doris::pipeline {
TPlanNode PartitionedAggregationTestHelper::create_test_plan_node() {
    TPlanNode tnode;
    tnode.node_id = 0;
    tnode.node_type = TPlanNodeType::AGGREGATION_NODE;
    tnode.num_children = 1;
    tnode.agg_node.use_streaming_preaggregation = false;
    tnode.agg_node.need_finalize = false;
    tnode.agg_node.intermediate_tuple_id = 1;
    tnode.agg_node.output_tuple_id = 2;
    tnode.limit = -1;

    auto& grouping_expr = tnode.agg_node.grouping_exprs.emplace_back();
    auto& expr_node = grouping_expr.nodes.emplace_back();
    expr_node.node_type = TExprNodeType::SLOT_REF;

    TTypeNode type_node;
    type_node.type = TTypeNodeType::SCALAR;
    type_node.scalar_type.type = TPrimitiveType::INT;
    type_node.__isset.scalar_type = true;

    expr_node.type.types.emplace_back(type_node);
    expr_node.__set_is_nullable(false);
    expr_node.num_children = 0;
    expr_node.slot_ref.slot_id = 0;
    expr_node.slot_ref.tuple_id = 0;

    auto& agg_function = tnode.agg_node.aggregate_functions.emplace_back();
    auto& fn_node = agg_function.nodes.emplace_back();
    fn_node.node_type = TExprNodeType::FUNCTION_CALL;
    fn_node.__set_is_nullable(false);
    fn_node.num_children = 1;

    TFunctionName fn_name;
    fn_name.function_name = "sum";

    fn_node.fn.__set_name(fn_name);

    TTypeDesc ret_type;
    auto& ret_type_node = ret_type.types.emplace_back();
    ret_type_node.scalar_type.type = TPrimitiveType::BIGINT;
    ret_type_node.__isset.scalar_type = true;
    ret_type_node.type = TTypeNodeType::SCALAR;
    ret_type.__set_is_nullable(false);

    TTypeDesc arg_type;
    auto& arg_type_node = arg_type.types.emplace_back();
    arg_type_node.scalar_type.type = TPrimitiveType::INT;
    arg_type_node.__isset.scalar_type = true;
    arg_type_node.type = TTypeNodeType::SCALAR;

    fn_node.fn.__set_ret_type(ret_type);

    fn_node.fn.__set_arg_types({arg_type});
    fn_node.agg_expr.__set_param_types({arg_type});

    auto& fn_child_node = agg_function.nodes.emplace_back();
    fn_child_node.node_type = TExprNodeType::SLOT_REF;
    fn_child_node.__set_is_nullable(false);
    fn_child_node.num_children = 0;
    fn_child_node.slot_ref.slot_id = 1;
    fn_child_node.slot_ref.tuple_id = 0;
    fn_child_node.type.types.emplace_back(type_node);

    tnode.row_tuples.push_back(0);
    tnode.nullable_tuples.push_back(false);

    return tnode;
}

TDescriptorTable PartitionedAggregationTestHelper::create_test_table_descriptor(
        bool nullable = false) {
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
                              .type(TYPE_BIGINT)
                              .column_name("col4")
                              .column_pos(0)
                              .nullable(true)
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
                              .type(TYPE_BIGINT)
                              .column_name("col6")
                              .column_pos(0)
                              .nullable(true)
                              .build())
            .build(&builder);

    return builder.desc_tbl();
}

std::tuple<std::shared_ptr<PartitionedAggSourceOperatorX>,
           std::shared_ptr<PartitionedAggSinkOperatorX>>
PartitionedAggregationTestHelper::create_operators() {
    TPlanNode tnode = create_test_plan_node();
    auto desc_tbl = runtime_state->desc_tbl();

    EXPECT_EQ(desc_tbl.get_tuple_descs().size(), 3);

    auto source_operator =
            std::make_shared<PartitionedAggSourceOperatorX>(obj_pool.get(), tnode, 0, desc_tbl);
    auto sink_operator = std::make_shared<PartitionedAggSinkOperatorX>(obj_pool.get(), 0, 0, tnode,
                                                                       desc_tbl, false);

    auto child_operator = std::make_shared<MockChildOperator>();
    auto probe_side_source_operator = std::make_shared<MockChildOperator>();
    auto source_side_sink_operator = std::make_shared<MockSinkOperator>();
    auto [source_pipeline, _] = generate_agg_pipeline(source_operator, source_side_sink_operator,
                                                      sink_operator, child_operator);

    RowDescriptor row_desc(runtime_state->desc_tbl(), {0}, {false});
    child_operator->_row_descriptor = row_desc;

    EXPECT_TRUE(sink_operator->set_child(child_operator));

    // Setup task and state
    std::map<int,
             std::pair<std::shared_ptr<BasicSharedState>, std::vector<std::shared_ptr<Dependency>>>>
            shared_state_map;
    pipeline_task = std::make_shared<PipelineTask>(source_pipeline, 0, runtime_state.get(), nullptr,
                                                   nullptr, shared_state_map, 0);
    return {std::move(source_operator), std::move(sink_operator)};
}

PartitionedAggLocalState* PartitionedAggregationTestHelper::create_source_local_state(
        RuntimeState* state, PartitionedAggSourceOperatorX* probe_operator,
        std::shared_ptr<MockPartitionedAggSharedState>& shared_state) {
    auto local_state_uptr = std::make_unique<MockPartitionedAggLocalState>(state, probe_operator);
    auto* local_state = local_state_uptr.get();
    shared_state = std::make_shared<MockPartitionedAggSharedState>();
    local_state->_shared_state = shared_state.get();
    shared_state->is_spilled = true;

    ADD_TIMER(local_state->common_profile(), "ExecTime");
    local_state->common_profile()->AddHighWaterMarkCounter("MemoryUsage", TUnit::BYTES, "", 0);
    local_state->init_spill_read_counters();
    local_state->init_spill_write_counters();
    local_state->_copy_shared_spill_profile = false;
    local_state->_internal_runtime_profile = std::make_unique<RuntimeProfile>("inner_test");

    local_state->_spill_dependency =
            Dependency::create_shared(0, 0, "PartitionedHashJoinProbeOperatorTestSpillDep", true);

    state->emplace_local_state(probe_operator->operator_id(), std::move(local_state_uptr));
    return local_state;
}

PartitionedAggSinkLocalState* PartitionedAggregationTestHelper::create_sink_local_state(
        RuntimeState* state, PartitionedAggSinkOperatorX* sink_operator,
        std::shared_ptr<MockPartitionedAggSharedState>& shared_state) {
    auto local_state_uptr = MockPartitionedAggSinkLocalState::create_unique(sink_operator, state);
    auto* local_state = local_state_uptr.get();
    shared_state = std::make_shared<MockPartitionedAggSharedState>();
    local_state->init_spill_counters();

    ADD_TIMER(local_state->common_profile(), "ExecTime");
    local_state->common_profile()->AddHighWaterMarkCounter("MemoryUsage", TUnit::BYTES, "", 0);
    local_state->_internal_runtime_profile = std::make_unique<RuntimeProfile>("inner_test");

    local_state->_dependency = shared_state->create_sink_dependency(
            sink_operator->dests_id().front(), sink_operator->operator_id(),
            "PartitionedHashJoinTestDep");

    local_state->_spill_dependency =
            Dependency::create_shared(0, 0, "PartitionedHashJoinSinkOperatorTestSpillDep", true);
    shared_state->setup_shared_profile(local_state->custom_profile());

    state->emplace_sink_local_state(sink_operator->operator_id(), std::move(local_state_uptr));
    return local_state;
}
} // namespace doris::pipeline