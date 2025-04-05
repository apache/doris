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

#include "hash_join_test_helper.h"

#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>

#include <sstream>
#include <unordered_map>

#include "testutil/creators.h"
#include "testutil/mock/mock_operators.h"

namespace doris::pipeline {
TPlanNode HashJoinTestHelper::create_test_plan_node(
        const TJoinOp::type& join_op_type, const std::vector<TPrimitiveType::type>& key_types,
        const std::vector<bool>& left_keys_nullable, const std::vector<bool>& right_keys_nullable,
        const bool is_mark_join) {
    DCHECK_EQ(key_types.size(), left_keys_nullable.size());
    DCHECK_EQ(key_types.size(), right_keys_nullable.size());

    TPlanNode tnode;
    tnode.node_id = 0;
    tnode.node_type = TPlanNodeType::HASH_JOIN_NODE;
    tnode.num_children = 2;
    tnode.__set_hash_join_node(THashJoinNode());
    tnode.hash_join_node.join_op = join_op_type;
    tnode.limit = -1;
    tnode.hash_join_node.__set_is_mark(is_mark_join);

    doris::TSlotId col_unique_id = 0;
    for (size_t i = 0; i != key_types.size(); ++i) {
        const auto key_type = key_types[i];
        const auto left_key_nullable = left_keys_nullable[i];
        const auto right_key_nullable = right_keys_nullable[i];
        TEqJoinCondition eq_cond;
        eq_cond.left = TExpr();
        eq_cond.right = TExpr();
        eq_cond.opcode = TExprOpcode::EQ;

        TTypeNode type_node;
        type_node.type = TTypeNodeType::SCALAR;
        type_node.scalar_type.type = key_type;
        type_node.__isset.scalar_type = true;

        if (key_type == TPrimitiveType::CHAR || key_type == TPrimitiveType::VARCHAR ||
            key_type == TPrimitiveType::STRING) {
            type_node.scalar_type.__set_len(OLAP_STRING_MAX_LENGTH);
        } else if (key_type == TPrimitiveType::DECIMAL128I ||
                   key_type == TPrimitiveType::DECIMAL256 ||
                   key_type == TPrimitiveType::DECIMAL32 || key_type == TPrimitiveType::DECIMAL64 ||
                   key_type == TPrimitiveType::DECIMALV2) {
            type_node.scalar_type.__set_precision(18);
            type_node.scalar_type.__set_scale(18);
        } else if (key_type == TPrimitiveType::DATETIMEV2) {
            type_node.scalar_type.__set_scale(6);
        } else if (key_type == TPrimitiveType::TIMEV2) {
            type_node.scalar_type.__set_scale(0);
        }

        eq_cond.left.nodes.emplace_back();
        eq_cond.left.nodes[0].type.types.emplace_back(type_node);
        eq_cond.left.nodes[0].node_type = TExprNodeType::SLOT_REF;
        eq_cond.left.nodes[0].__set_is_nullable(left_key_nullable);
        eq_cond.left.nodes[0].num_children = 0;
        eq_cond.left.nodes[0].slot_ref.col_unique_id = col_unique_id++;

        eq_cond.right.nodes.emplace_back();
        eq_cond.right.nodes[0].type.types.emplace_back(type_node);
        eq_cond.right.nodes[0].node_type = TExprNodeType::SLOT_REF;
        eq_cond.right.nodes[0].__set_is_nullable(right_key_nullable);
        eq_cond.right.nodes[0].num_children = 0;
        eq_cond.right.nodes[0].slot_ref.col_unique_id = col_unique_id++;

        tnode.hash_join_node.eq_join_conjuncts.push_back(eq_cond);
    }

    tnode.row_tuples.push_back(0);
    tnode.row_tuples.push_back(1);
    tnode.nullable_tuples.push_back(false);
    tnode.nullable_tuples.push_back(false);
    tnode.__isset.hash_join_node = true;

    tnode.hash_join_node.vintermediate_tuple_id_list.emplace_back(2);
    tnode.hash_join_node.__isset.vintermediate_tuple_id_list = true;

    auto desc_table = create_test_table_descriptor(tnode);
    auto st = DescriptorTbl::create(obj_pool.get(), desc_table, &desc_tbl);
    DCHECK(!desc_table.slotDescriptors.empty());

    runtime_state->set_desc_tbl(desc_tbl);

    return tnode;
}

TDescriptorTable HashJoinTestHelper::create_test_table_descriptor(TPlanNode& tnode) {
    TTupleDescriptorBuilder left_tuple_builder, right_tuple_builder, intermediate_tuple_builder,
            output_tuple_builder;

    const auto is_left_half_join =
            tnode.hash_join_node.join_op == TJoinOp::LEFT_SEMI_JOIN ||
            tnode.hash_join_node.join_op == TJoinOp::LEFT_ANTI_JOIN ||
            tnode.hash_join_node.join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN ||
            tnode.hash_join_node.join_op == TJoinOp::NULL_AWARE_LEFT_SEMI_JOIN;
    const auto is_right_half_join = tnode.hash_join_node.join_op == TJoinOp::RIGHT_ANTI_JOIN ||
                                    tnode.hash_join_node.join_op == TJoinOp::RIGHT_SEMI_JOIN;

    const auto is_half_join = is_left_half_join || is_right_half_join;
    size_t keys_count = tnode.hash_join_node.eq_join_conjuncts.size();
    const auto total_keys_count = is_half_join ? keys_count : keys_count * 2;
    const auto total_output_slots_count = total_keys_count;
    std::vector<TSlotDescriptor> intermediate_slots(total_keys_count);
    std::vector<TSlotDescriptor> output_slots(total_output_slots_count);
    const auto keys_offset = is_half_join ? 0 : keys_count;

    size_t slots_count = 0;
    for (auto& eq_cond : tnode.hash_join_node.eq_join_conjuncts) {
        auto left_type = thrift_to_type(eq_cond.left.nodes[0].type.types[0].scalar_type.type);
        auto slot = TSlotDescriptorBuilder()
                            .type(left_type)
                            .nullable(eq_cond.left.nodes[0].is_nullable)
                            .build();
        slot.slotType.types[0].scalar_type = eq_cond.left.nodes[0].type.types[0].scalar_type;
        slot.col_unique_id = eq_cond.left.nodes[0].slot_ref.col_unique_id;
        left_tuple_builder.add_slot(slot);

        if (!is_right_half_join) {
            slot.col_unique_id = -1;
            if (tnode.hash_join_node.join_op == TJoinOp::FULL_OUTER_JOIN ||
                tnode.hash_join_node.join_op == TJoinOp::RIGHT_OUTER_JOIN) {
                slot.nullIndicatorByte = 0;
                slot.nullIndicatorBit = 0;
            }
            intermediate_slots[slots_count] = slot;
            output_slots[slots_count] = slot;
        }

        auto right_type = thrift_to_type(eq_cond.right.nodes[0].type.types[0].scalar_type.type);

        slot = TSlotDescriptorBuilder()
                       .type(right_type)
                       .nullable(eq_cond.right.nodes[0].is_nullable)
                       .build();
        slot.slotType.types[0].scalar_type = eq_cond.right.nodes[0].type.types[0].scalar_type;
        slot.col_unique_id = eq_cond.right.nodes[0].slot_ref.col_unique_id;
        right_tuple_builder.add_slot(slot);

        if (!is_left_half_join) {
            slot.col_unique_id = -1;
            if (tnode.hash_join_node.join_op == TJoinOp::FULL_OUTER_JOIN ||
                tnode.hash_join_node.join_op == TJoinOp::LEFT_OUTER_JOIN) {
                slot.nullIndicatorByte = 0;
                slot.nullIndicatorBit = 0;
            }
            intermediate_slots[slots_count + keys_offset] = slot;
            output_slots[slots_count + keys_offset] = slot;
        }
        ++slots_count;
    }

    TDescriptorTableBuilder builder;

    left_tuple_builder.build(&builder);
    right_tuple_builder.build(&builder);

    for (auto& slot : intermediate_slots) {
        intermediate_tuple_builder.add_slot(slot);
    }

    for (auto& slot : output_slots) {
        output_tuple_builder.add_slot(slot);
    }

    if (tnode.hash_join_node.is_mark) {
        auto type = thrift_to_type(TPrimitiveType::BOOLEAN);
        auto slot = TSlotDescriptorBuilder().type(type).nullable(true).build();
        slot.col_unique_id = -1;
        intermediate_tuple_builder.add_slot(slot);
        output_tuple_builder.add_slot(slot);
    }

    intermediate_tuple_builder.build(&builder);
    output_tuple_builder.build(&builder);
    tnode.__set_output_tuple_id(3);
    tnode.hash_join_node.__set_voutput_tuple_id(3);

    auto table_desc = builder.desc_tbl();

    std::unordered_map<int32_t, TTupleId> slots_map;
    for (auto& slot : table_desc.slotDescriptors) {
        slots_map[slot.col_unique_id] = slot.id;
    }

    for (auto& eq_cond : tnode.hash_join_node.eq_join_conjuncts) {
        auto col_unique_id = eq_cond.left.nodes[0].slot_ref.col_unique_id;
        eq_cond.left.nodes[0].slot_ref.slot_id = slots_map[col_unique_id];
        if (!is_right_half_join) {
            tnode.hash_join_node.hash_output_slot_ids.emplace_back(slots_map[col_unique_id]);
        }

        col_unique_id = eq_cond.right.nodes[0].slot_ref.col_unique_id;
        eq_cond.right.nodes[0].slot_ref.slot_id = slots_map[col_unique_id];

        if (!is_left_half_join) {
            tnode.hash_join_node.hash_output_slot_ids.emplace_back(slots_map[col_unique_id]);
        }
    }
    tnode.hash_join_node.__isset.hash_output_slot_ids = true;

    return table_desc;
}

void HashJoinTestHelper::add_mark_join_conjuncts(TPlanNode& join_node,
                                                 std::vector<TExpr>& conjuncts) {
    EXPECT_TRUE(join_node.__isset.hash_join_node);

    join_node.hash_join_node.__isset.mark_join_conjuncts = true;
    join_node.hash_join_node.mark_join_conjuncts.insert(
            join_node.hash_join_node.mark_join_conjuncts.end(), conjuncts.begin(), conjuncts.end());
}

void HashJoinTestHelper::add_other_join_conjuncts(TPlanNode& join_node,
                                                  std::vector<TExpr>& conjuncts) {
    EXPECT_TRUE(join_node.__isset.hash_join_node);

    join_node.hash_join_node.__isset.other_join_conjuncts = true;
    join_node.hash_join_node.other_join_conjuncts.insert(
            join_node.hash_join_node.other_join_conjuncts.end(), conjuncts.begin(),
            conjuncts.end());
}

std::pair<std::shared_ptr<HashJoinProbeOperatorX>, std::shared_ptr<HashJoinBuildSinkOperatorX>>
HashJoinTestHelper::create_operators(const TPlanNode& tnode) {
    auto sink_operator = std::make_shared<HashJoinBuildSinkOperatorX>(obj_pool.get(), 0, 0, tnode,
                                                                      runtime_state->desc_tbl());

    auto probe_operator = std::make_shared<HashJoinProbeOperatorX>(obj_pool.get(), tnode, 0,
                                                                   runtime_state->desc_tbl());

    auto child_operator = std::make_shared<MockSourceOperator>();
    auto probe_side_source_operator = std::make_shared<MockSourceOperator>();
    auto probe_side_sink_operator = std::make_shared<MockSinkOperator>();

    RowDescriptor row_desc(runtime_state->desc_tbl(), {1}, {false});
    child_operator->_row_descriptor = row_desc;

    RowDescriptor probe_side_row_desc(runtime_state->desc_tbl(), {0}, {false});
    probe_side_source_operator->_row_descriptor = probe_side_row_desc;

    EXPECT_TRUE(sink_operator->set_child(child_operator));
    EXPECT_TRUE(probe_operator->set_child(probe_side_source_operator));
    EXPECT_TRUE(probe_operator->set_child(child_operator));

    // Setup task and state
    std::map<int,
             std::pair<std::shared_ptr<BasicSharedState>, std::vector<std::shared_ptr<Dependency>>>>
            shared_state_map;
    auto [source_pipeline, _] = generate_sort_pipeline(probe_operator, probe_side_sink_operator,
                                                       sink_operator, child_operator);
    pipeline_task = std::make_shared<PipelineTask>(source_pipeline, 0, runtime_state.get(), nullptr,
                                                   nullptr, shared_state_map, 0);

    return {std::move(probe_operator), std::move(sink_operator)};
}

} // namespace doris::pipeline