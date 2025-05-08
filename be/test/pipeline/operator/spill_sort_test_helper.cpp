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

#include "spill_sort_test_helper.h"

#include <gen_cpp/DataSinks_types.h>
#include <gen_cpp/Descriptors_types.h>
#include <gmock/gmock-actions.h>
#include <gmock/gmock-function-mocker.h>
#include <gmock/gmock-spec-builders.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "pipeline/exec/sort_sink_operator.h"
#include "pipeline/exec/sort_source_operator.h"
#include "runtime/define_primitive_type.h"
#include "testutil/creators.h"
#include "testutil/mock/mock_operators.h"

namespace doris::pipeline {

TPlanNode SpillSortTestHelper::create_test_plan_node() {
    TPlanNode tnode;
    tnode.node_id = 1;
    tnode.num_children = 0;
    tnode.node_type = TPlanNodeType::SORT_NODE;
    tnode.row_tuples.emplace_back(1);
    tnode.nullable_tuples.emplace_back(false);
    tnode.limit = -1;

    tnode.sort_node.sort_info.is_asc_order.emplace_back(true);
    tnode.sort_node.sort_info.is_asc_order.emplace_back(true);
    tnode.sort_node.sort_info.nulls_first.emplace_back(false);
    tnode.sort_node.sort_info.nulls_first.emplace_back(false);

    tnode.sort_node.algorithm = TSortAlgorithm::FULL_SORT;

    auto& sort_expr = tnode.sort_node.sort_info.ordering_exprs.emplace_back();
    auto& sort_expr_node = sort_expr.nodes.emplace_back();
    sort_expr_node.node_type = TExprNodeType::SLOT_REF;
    auto& type_node = sort_expr_node.type.types.emplace_back();
    type_node.scalar_type.type = TPrimitiveType::INT;
    type_node.__isset.scalar_type = true;
    sort_expr_node.slot_ref.slot_id = 2;
    sort_expr_node.slot_ref.tuple_id = 1;

    auto& sort_expr_node2 =
            tnode.sort_node.sort_info.ordering_exprs.emplace_back().nodes.emplace_back();
    sort_expr_node2.node_type = TExprNodeType::SLOT_REF;
    auto& type_node2 = sort_expr_node2.type.types.emplace_back();
    type_node2.scalar_type.type = TPrimitiveType::BIGINT;
    type_node2.__isset.scalar_type = true;
    sort_expr_node2.slot_ref.slot_id = 3;
    sort_expr_node2.slot_ref.tuple_id = 1;
    return tnode;
}

TDescriptorTable SpillSortTestHelper::create_test_table_descriptor(bool nullable) {
    TTupleDescriptorBuilder tuple_builder;
    tuple_builder
            .add_slot(TSlotDescriptorBuilder()
                              .type(PrimitiveType::TYPE_INT)
                              .column_name("col1")
                              .column_pos(0)
                              .nullable(nullable)
                              .build())
            .add_slot(TSlotDescriptorBuilder()
                              .type(PrimitiveType::TYPE_BIGINT)
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
                              .type(TYPE_BIGINT)
                              .column_name("col6")
                              .column_pos(0)
                              .nullable(true)
                              .build())
            .build(&builder);

    return builder.desc_tbl();
}

SpillSortLocalState* SpillSortTestHelper::create_source_local_state(
        RuntimeState* state, SpillSortSourceOperatorX* source_operator,
        std::shared_ptr<MockSpillSortSharedState>& shared_state) {
    return nullptr;
}

SpillSortSinkLocalState* SpillSortTestHelper::create_sink_local_state(
        RuntimeState* state, SpillSortSinkOperatorX* sink_operator,
        std::shared_ptr<MockSpillSortSharedState>& shared_state) {
    return nullptr;
}

std::tuple<std::shared_ptr<SpillSortSourceOperatorX>, std::shared_ptr<SpillSortSinkOperatorX>>
SpillSortTestHelper::create_operators() {
    TPlanNode tnode = create_test_plan_node();
    auto desc_tbl = runtime_state->desc_tbl();

    auto source_operator =
            std::make_shared<SpillSortSourceOperatorX>(obj_pool.get(), tnode, 0, desc_tbl);
    auto sink_operator =
            std::make_shared<SpillSortSinkOperatorX>(obj_pool.get(), 0, 0, tnode, desc_tbl, false);

    auto child_operator = std::make_shared<MockChildOperator>();
    auto probe_side_source_operator = std::make_shared<MockChildOperator>();
    auto source_side_sink_operator = std::make_shared<MockSinkOperator>();
    auto [source_pipeline, _] = generate_sort_pipeline(source_operator, source_side_sink_operator,
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

} // namespace doris::pipeline