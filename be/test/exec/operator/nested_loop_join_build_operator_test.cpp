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

#include "exec/operator/nested_loop_join_build_operator.h"

#include <gen_cpp/DataSinks_types.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <vector>

#include "exec/runtime_filter/runtime_filter_test_utils.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_profile.h"
#include "testutil/mock/mock_operators.h"

namespace doris {
namespace {

TDescriptorTable create_desc_table() {
    TDescriptorTableBuilder builder;
    TTupleDescriptorBuilder()
            .add_slot(TSlotDescriptorBuilder()
                              .type(TYPE_INT)
                              .nullable(false)
                              .column_name("probe_col")
                              .column_pos(0)
                              .build())
            .build(&builder);
    TTupleDescriptorBuilder()
            .add_slot(TSlotDescriptorBuilder()
                              .type(TYPE_INT)
                              .nullable(false)
                              .column_name("build_col")
                              .column_pos(0)
                              .build())
            .build(&builder);
    return builder.desc_tbl();
}

TExpr build_slot_ref_expr() {
    auto expr = TRuntimeFilterDescBuilder::get_default_expr();
    expr.nodes[0].slot_ref.__set_slot_id(1);
    expr.nodes[0].slot_ref.__set_tuple_id(1);
    return expr;
}

TPlanNode create_nested_loop_join_plan_node() {
    TPlanNode node;
    node.node_id = 0;
    node.node_type = TPlanNodeType::CROSS_JOIN_NODE;
    node.num_children = 2;
    node.limit = -1;

    TNestedLoopJoinNode join_node;
    join_node.__set_join_op(TJoinOp::INNER_JOIN);
    node.__set_nested_loop_join_node(join_node);
    node.row_tuples.push_back(0);
    node.row_tuples.push_back(1);
    node.nullable_tuples.push_back(false);
    node.nullable_tuples.push_back(false);

    auto src_expr = build_slot_ref_expr();
    TRuntimeFilterDescBuilder runtime_filter_builder(0, src_expr, 0);
    node.__isset.runtime_filters = true;
    node.runtime_filters.push_back(runtime_filter_builder.build());
    return node;
}

} // namespace

class NestedLoopJoinBuildOperatorTest : public RuntimeFilterTest {};

TEST_F(NestedLoopJoinBuildOperatorTest, CloseSkipsRuntimeFilterProcessWhenCancelled) {
    ObjectPool pool;
    DescriptorTbl* desc_tbl = nullptr;
    auto desc_table = create_desc_table();
    auto status = DescriptorTbl::create(&pool, desc_table, &desc_tbl);
    ASSERT_TRUE(status.ok()) << status.to_string();

    auto* state = _runtime_states[0].get();
    state->set_desc_tbl(desc_tbl);

    auto plan_node = create_nested_loop_join_plan_node();
    auto sink_operator =
            std::make_shared<NestedLoopJoinBuildSinkOperatorX>(&pool, 0, 1, plan_node, *desc_tbl);

    auto child = std::make_shared<MockSourceOperator>();
    child->_row_descriptor = RowDescriptor(*desc_tbl, {1}, {false});
    ASSERT_TRUE(sink_operator->set_child(child));

    status = sink_operator->init(plan_node, state);
    ASSERT_TRUE(status.ok()) << status.to_string();
    status = sink_operator->prepare(state);
    ASSERT_TRUE(status.ok()) << status.to_string();

    auto shared_state = sink_operator->create_shared_state();
    ASSERT_NE(shared_state, nullptr);

    std::map<int,
             std::pair<std::shared_ptr<BasicSharedState>, std::vector<std::shared_ptr<Dependency>>>>
            shared_state_map;
    LocalSinkStateInfo info {.task_idx = 0,
                             .parent_profile = &_profile,
                             .sender_id = 0,
                             .shared_state = shared_state.get(),
                             .shared_state_map = shared_state_map,
                             .tsink = TDataSink()};
    status = sink_operator->setup_local_state(state, info);
    ASSERT_TRUE(status.ok()) << status.to_string();

    auto* local_state =
            dynamic_cast<NestedLoopJoinBuildSinkLocalState*>(state->get_sink_local_state());
    ASSERT_NE(local_state, nullptr);

    status = local_state->open(state);
    ASSERT_TRUE(status.ok()) << status.to_string();
    local_state->build_blocks().emplace_back();

    state->cancel(Status::Cancelled("nested loop join build close cancelled"));
    status = local_state->close(state, Status::OK());
    ASSERT_TRUE(status.ok()) << status.to_string();
}

} // namespace doris
