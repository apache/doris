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

#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>

#include <cstddef>
#include <iostream>
#include <sstream>
#include <unordered_map>
#include <vector>

#include "testutil/creators.h"
#include "testutil/mock/mock_operators.h"

namespace doris::pipeline {
TPlanNode HashJoinTestHelper::create_test_plan_node(
        const TJoinOp::type& join_op_type, const std::vector<TPrimitiveType::type>& key_types,
        const std::vector<bool>& left_keys_nullable, const std::vector<bool>& right_keys_nullable,
        const bool is_mark_join, const size_t mark_join_conjuncts_size, const bool null_safe_equal,
        const bool has_other_join_conjuncts) {
    DCHECK_EQ(key_types.size(), left_keys_nullable.size());
    DCHECK_EQ(key_types.size(), right_keys_nullable.size());
    DCHECK_GE(key_types.size(), mark_join_conjuncts_size);

    TPlanNode tnode;
    tnode.node_id = 0;
    tnode.node_type = TPlanNodeType::HASH_JOIN_NODE;
    tnode.num_children = 2;
    tnode.__set_hash_join_node(THashJoinNode());
    tnode.hash_join_node.join_op = join_op_type;
    tnode.limit = -1;
    tnode.hash_join_node.__set_is_mark(is_mark_join);

    size_t mark_join_conjuncts_count = 0;

    doris::TSlotId col_unique_id = 0;
    for (size_t i = 0; i != key_types.size(); ++i) {
        const auto key_type = key_types[i];
        const auto left_key_nullable = left_keys_nullable[i];
        const auto right_key_nullable = right_keys_nullable[i];
        TEqJoinCondition eq_cond;
        eq_cond.left = TExpr();
        eq_cond.right = TExpr();
        if (null_safe_equal) {
            eq_cond.opcode = TExprOpcode::EQ_FOR_NULL;
        } else {
            eq_cond.opcode = TExprOpcode::EQ;
        }
        eq_cond.__isset.opcode = true;
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
        eq_cond.left.nodes[0].__isset.slot_ref = true;

        eq_cond.right.nodes.emplace_back();
        eq_cond.right.nodes[0].type.types.emplace_back(type_node);
        eq_cond.right.nodes[0].node_type = TExprNodeType::SLOT_REF;
        eq_cond.right.nodes[0].__set_is_nullable(right_key_nullable);
        eq_cond.right.nodes[0].num_children = 0;
        eq_cond.right.nodes[0].slot_ref.col_unique_id = col_unique_id++;
        eq_cond.right.nodes[0].__isset.slot_ref = true;

        if (mark_join_conjuncts_count < mark_join_conjuncts_size) {
            TExpr mark_join_cond;
            mark_join_cond.nodes.emplace_back();
            mark_join_cond.nodes[0].node_type = TExprNodeType::BINARY_PRED;
            mark_join_cond.nodes[0].opcode = TExprOpcode::EQ;
            mark_join_cond.nodes[0].type.types.emplace_back();
            mark_join_cond.nodes[0].type.types[0].scalar_type.type = TPrimitiveType::BOOLEAN;
            mark_join_cond.nodes[0].type.types[0].__isset.scalar_type = true;
            mark_join_cond.nodes[0].__set_is_nullable(true);
            mark_join_cond.nodes[0].num_children = 2;

            mark_join_cond.nodes[0].fn.name.function_name = "eq";
            mark_join_cond.nodes[0].__isset.fn = true;

            mark_join_cond.nodes.emplace_back();
            mark_join_cond.nodes[1] = eq_cond.left.nodes[0];
            mark_join_cond.nodes.emplace_back();
            mark_join_cond.nodes[2] = eq_cond.right.nodes[0];
            tnode.hash_join_node.mark_join_conjuncts.emplace_back(mark_join_cond);
            tnode.hash_join_node.__isset.mark_join_conjuncts = true;

            ++mark_join_conjuncts_count;
        } else {
            tnode.hash_join_node.eq_join_conjuncts.push_back(eq_cond);
        }
    }

    if (has_other_join_conjuncts) {
        TExpr other_join_cond;
        other_join_cond.nodes.emplace_back();
        other_join_cond.nodes[0].node_type = TExprNodeType::BINARY_PRED;
        other_join_cond.nodes[0].opcode = TExprOpcode::EQ;
        other_join_cond.nodes[0].type.types.emplace_back();
        other_join_cond.nodes[0].type.types[0].scalar_type.type = TPrimitiveType::BOOLEAN;
        other_join_cond.nodes[0].type.types[0].__isset.scalar_type = true;
        other_join_cond.nodes[0].__set_is_nullable(true);
        other_join_cond.nodes[0].num_children = 2;

        other_join_cond.nodes[0].fn.name.function_name = "gt";
        other_join_cond.nodes[0].__isset.fn = true;

        other_join_cond.nodes.emplace_back();
        other_join_cond.nodes[1].node_type = TExprNodeType::SLOT_REF;
        other_join_cond.nodes[1].type.types.emplace_back();
        other_join_cond.nodes[1].type.types[0].scalar_type.type = TPrimitiveType::INT;
        other_join_cond.nodes[1].__set_is_nullable(true);
        other_join_cond.nodes[1].type.types[0].__isset.scalar_type = true;
        other_join_cond.nodes[1].num_children = 0;
        other_join_cond.nodes[1].slot_ref.col_unique_id = col_unique_id++;
        other_join_cond.nodes[1].__isset.slot_ref = true;

        other_join_cond.nodes.emplace_back();
        other_join_cond.nodes[2].node_type = TExprNodeType::INT_LITERAL;
        other_join_cond.nodes[2].type.types.emplace_back();
        other_join_cond.nodes[2].type.types[0].scalar_type.type = TPrimitiveType::INT;
        other_join_cond.nodes[2].type.types[0].__isset.scalar_type = true;
        other_join_cond.nodes[2].num_children = 0;
        other_join_cond.nodes[2].int_literal.value = 100;
        other_join_cond.nodes[2].__isset.int_literal = true;

        tnode.hash_join_node.other_join_conjuncts.push_back(other_join_cond);

        other_join_cond.nodes[2].int_literal.value = 50;
        other_join_cond.nodes[1].slot_ref.col_unique_id = col_unique_id++;
        tnode.hash_join_node.other_join_conjuncts.push_back(other_join_cond);
        tnode.hash_join_node.__isset.other_join_conjuncts = true;
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

    const auto has_other_join_conjuncts = !tnode.hash_join_node.other_join_conjuncts.empty();
    const auto intermediate_need_output_left_side =
            !is_half_join || is_left_half_join ||
            !tnode.hash_join_node.mark_join_conjuncts.empty() || has_other_join_conjuncts;
    const auto need_output_left_side = !is_half_join || is_left_half_join;

    const auto intermediate_need_output_right_side =
            !is_half_join || is_right_half_join ||
            !tnode.hash_join_node.mark_join_conjuncts.empty() || has_other_join_conjuncts;
    const auto need_output_right_side = !is_half_join || is_right_half_join;

    const auto other_conjuncts_count = tnode.hash_join_node.other_join_conjuncts.empty() ? 0 : 1;

    const auto keys_count = tnode.hash_join_node.eq_join_conjuncts.size();
    const auto mark_keys_count = tnode.hash_join_node.mark_join_conjuncts.size();
    const auto total_output_slots_count =
            (need_output_left_side ? keys_count + mark_keys_count + other_conjuncts_count : 0) +
            (need_output_right_side ? keys_count + mark_keys_count + other_conjuncts_count : 0);
    const auto total_intermediate_output_slots_count =
            (intermediate_need_output_left_side
                     ? keys_count + mark_keys_count + other_conjuncts_count
                     : 0) +
            (intermediate_need_output_right_side
                     ? keys_count + mark_keys_count + other_conjuncts_count
                     : 0);
    std::vector<TSlotDescriptor> intermediate_slots(total_intermediate_output_slots_count);
    std::vector<TSlotDescriptor> output_slots(total_output_slots_count);
    const auto intermediate_keys_offset =
            intermediate_need_output_left_side
                    ? keys_count + mark_keys_count + other_conjuncts_count
                    : 0;
    const auto keys_offset =
            need_output_left_side ? keys_count + mark_keys_count + other_conjuncts_count : 0;

    size_t slots_count = 0;
    doris::TSlotId col_unique_id = 200;
    std::vector<doris::TSlotId> hash_output_unique_ids;
    std::vector<TExpr> projection_exprs(total_output_slots_count);
    for (auto& eq_cond : tnode.hash_join_node.mark_join_conjuncts) {
        auto left_type = thrift_to_type(eq_cond.nodes[1].type.types[0].scalar_type.type);
        auto slot = TSlotDescriptorBuilder()
                            .type(left_type)
                            .nullable(eq_cond.nodes[1].is_nullable)
                            .build();
        slot.slotType.types[0].scalar_type = eq_cond.nodes[1].type.types[0].scalar_type;
        slot.col_unique_id = eq_cond.nodes[1].slot_ref.col_unique_id;
        if (tnode.hash_join_node.join_op == TJoinOp::FULL_OUTER_JOIN ||
            tnode.hash_join_node.join_op == TJoinOp::RIGHT_OUTER_JOIN) {
            slot.nullIndicatorByte = 0;
            slot.nullIndicatorBit = 0;
        }

        intermediate_slots[slots_count] = slot;
        if (need_output_left_side) {
            auto& expr = projection_exprs[slots_count];
            auto& node = expr.nodes.emplace_back();
            node.node_type = TExprNodeType::SLOT_REF;
            node.__set_is_nullable(slot.nullIndicatorBit == 0 && slot.nullIndicatorByte == 0);
            node.num_children = 0;
            node.__isset.slot_ref = true;
            node.slot_ref.col_unique_id = slot.col_unique_id;
            node.slot_ref.slot_id = slot.id;
            node.slot_ref.__isset.col_unique_id = true;
            node.type.types.emplace_back();
            node.type.types[0].scalar_type = slot.slotType.types[0].scalar_type;
            node.type.types[0].__isset.scalar_type = true;

            slot.col_unique_id = col_unique_id++;
            output_slots[slots_count] = slot;
        }

        slot.col_unique_id = col_unique_id++;
        left_tuple_builder.add_slot(slot);
        hash_output_unique_ids.emplace_back(slot.col_unique_id);

        auto right_type = thrift_to_type(eq_cond.nodes[2].type.types[0].scalar_type.type);

        slot = TSlotDescriptorBuilder()
                       .type(right_type)
                       .nullable(eq_cond.nodes[2].is_nullable)
                       .build();
        slot.slotType.types[0].scalar_type = eq_cond.nodes[2].type.types[0].scalar_type;
        slot.col_unique_id = eq_cond.nodes[2].slot_ref.col_unique_id;
        if (tnode.hash_join_node.join_op == TJoinOp::FULL_OUTER_JOIN ||
            tnode.hash_join_node.join_op == TJoinOp::LEFT_OUTER_JOIN) {
            slot.nullIndicatorByte = 0;
            slot.nullIndicatorBit = 0;
        }
        intermediate_slots[slots_count + intermediate_keys_offset] = slot;
        if (need_output_right_side) {
            auto& expr = projection_exprs[slots_count + keys_offset];
            auto& node = expr.nodes.emplace_back();
            node.node_type = TExprNodeType::SLOT_REF;
            node.__set_is_nullable(slot.nullIndicatorBit == 0 && slot.nullIndicatorByte == 0);
            node.num_children = 0;
            node.__isset.slot_ref = true;
            node.slot_ref.col_unique_id = slot.col_unique_id;
            node.slot_ref.slot_id = slot.id;
            node.slot_ref.__isset.col_unique_id = true;
            node.type.types.emplace_back();
            node.type.types[0].scalar_type = slot.slotType.types[0].scalar_type;
            node.type.types[0].__isset.scalar_type = true;

            slot.col_unique_id = col_unique_id++;
            output_slots[slots_count + keys_offset] = slot;
        }
        slot.col_unique_id = col_unique_id++;
        right_tuple_builder.add_slot(slot);
        hash_output_unique_ids.emplace_back(slot.col_unique_id);
        ++slots_count;
    }

    for (auto& eq_cond : tnode.hash_join_node.eq_join_conjuncts) {
        auto left_type = thrift_to_type(eq_cond.left.nodes[0].type.types[0].scalar_type.type);
        auto slot = TSlotDescriptorBuilder()
                            .type(left_type)
                            .nullable(eq_cond.left.nodes[0].is_nullable)
                            .build();
        slot.slotType.types[0].scalar_type = eq_cond.left.nodes[0].type.types[0].scalar_type;
        slot.col_unique_id = eq_cond.left.nodes[0].slot_ref.col_unique_id;
        left_tuple_builder.add_slot(slot);
        if (intermediate_need_output_left_side || need_output_left_side) {
            hash_output_unique_ids.emplace_back(slot.col_unique_id);
        }

        slot.col_unique_id = col_unique_id++;
        if (tnode.hash_join_node.join_op == TJoinOp::FULL_OUTER_JOIN ||
            tnode.hash_join_node.join_op == TJoinOp::RIGHT_OUTER_JOIN) {
            slot.nullIndicatorByte = 0;
            slot.nullIndicatorBit = 0;
        }

        if (intermediate_need_output_left_side) {
            intermediate_slots[slots_count] = slot;
        }

        if (need_output_left_side) {
            auto& expr = projection_exprs[slots_count];
            auto& node = expr.nodes.emplace_back();
            node.node_type = TExprNodeType::SLOT_REF;
            node.__set_is_nullable(slot.nullIndicatorBit == 0 && slot.nullIndicatorByte == 0);
            node.num_children = 0;
            node.__isset.slot_ref = true;
            node.slot_ref.col_unique_id = slot.col_unique_id;
            node.slot_ref.slot_id = slot.id;
            node.slot_ref.__isset.col_unique_id = true;
            node.type.types.emplace_back();
            node.type.types[0].scalar_type = slot.slotType.types[0].scalar_type;
            node.type.types[0].__isset.scalar_type = true;

            slot.col_unique_id = col_unique_id++;
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
        if (need_output_right_side || intermediate_need_output_right_side) {
            hash_output_unique_ids.emplace_back(slot.col_unique_id);
        }
        slot.col_unique_id = col_unique_id++;
        if (tnode.hash_join_node.join_op == TJoinOp::FULL_OUTER_JOIN ||
            tnode.hash_join_node.join_op == TJoinOp::LEFT_OUTER_JOIN) {
            slot.nullIndicatorByte = 0;
            slot.nullIndicatorBit = 0;
        }

        if (intermediate_need_output_right_side) {
            intermediate_slots[slots_count + intermediate_keys_offset] = slot;
        }
        if (need_output_right_side) {
            auto& expr = projection_exprs[slots_count + keys_offset];
            auto& node = expr.nodes.emplace_back();
            node.node_type = TExprNodeType::SLOT_REF;
            node.__set_is_nullable(slot.nullIndicatorBit == 0 && slot.nullIndicatorByte == 0);
            node.num_children = 0;
            node.__isset.slot_ref = true;
            node.slot_ref.col_unique_id = slot.col_unique_id;
            node.slot_ref.slot_id = slot.id;
            node.slot_ref.__isset.col_unique_id = true;
            node.type.types.emplace_back();
            node.type.types[0].scalar_type = slot.slotType.types[0].scalar_type;
            node.type.types[0].__isset.scalar_type = true;

            slot.col_unique_id = col_unique_id++;
            output_slots[slots_count + keys_offset] = slot;
        }
        ++slots_count;
    }

    if (tnode.hash_join_node.other_join_conjuncts.size() == 2) {
        auto& left_cond = tnode.hash_join_node.other_join_conjuncts[0];
        auto& right_cond = tnode.hash_join_node.other_join_conjuncts[1];

        auto left_type = thrift_to_type(left_cond.nodes[1].type.types[0].scalar_type.type);
        auto slot = TSlotDescriptorBuilder()
                            .type(left_type)
                            .nullable(left_cond.nodes[1].is_nullable)
                            .build();
        slot.slotType.types[0].scalar_type = left_cond.nodes[1].type.types[0].scalar_type;

        slot.col_unique_id = col_unique_id++;
        left_tuple_builder.add_slot(slot);
        hash_output_unique_ids.emplace_back(slot.col_unique_id);

        slot.col_unique_id = left_cond.nodes[1].slot_ref.col_unique_id;
        if (tnode.hash_join_node.join_op == TJoinOp::FULL_OUTER_JOIN ||
            tnode.hash_join_node.join_op == TJoinOp::RIGHT_OUTER_JOIN) {
            slot.nullIndicatorByte = 0;
            slot.nullIndicatorBit = 0;
        }

        intermediate_slots[slots_count] = slot;

        if (need_output_left_side) {
            auto& expr = projection_exprs[slots_count];
            auto& node = expr.nodes.emplace_back();
            node.node_type = TExprNodeType::SLOT_REF;
            node.__set_is_nullable(slot.nullIndicatorBit == 0 && slot.nullIndicatorByte == 0);
            node.num_children = 0;
            node.__isset.slot_ref = true;
            node.slot_ref.col_unique_id = slot.col_unique_id;
            node.slot_ref.slot_id = slot.id;
            node.slot_ref.__isset.col_unique_id = true;
            node.type.types.emplace_back();
            node.type.types[0].scalar_type = slot.slotType.types[0].scalar_type;
            node.type.types[0].__isset.scalar_type = true;

            slot.col_unique_id = col_unique_id++;
            output_slots[slots_count] = slot;
        }

        auto right_type = thrift_to_type(right_cond.nodes[1].type.types[0].scalar_type.type);
        slot = TSlotDescriptorBuilder()
                       .type(right_type)
                       .nullable(right_cond.nodes[1].is_nullable)
                       .build();
        slot.slotType.types[0].scalar_type = right_cond.nodes[1].type.types[0].scalar_type;

        slot.col_unique_id = col_unique_id++;
        right_tuple_builder.add_slot(slot);
        hash_output_unique_ids.emplace_back(slot.col_unique_id);

        slot.col_unique_id = right_cond.nodes[1].slot_ref.col_unique_id;
        if (tnode.hash_join_node.join_op == TJoinOp::FULL_OUTER_JOIN ||
            tnode.hash_join_node.join_op == TJoinOp::LEFT_OUTER_JOIN) {
            slot.nullIndicatorByte = 0;
            slot.nullIndicatorBit = 0;
        }

        intermediate_slots[slots_count + intermediate_keys_offset] = slot;

        if (need_output_right_side) {
            auto& expr = projection_exprs[slots_count + keys_offset];
            auto& node = expr.nodes.emplace_back();
            node.node_type = TExprNodeType::SLOT_REF;
            node.__set_is_nullable(slot.nullIndicatorBit == 0 && slot.nullIndicatorByte == 0);
            node.num_children = 0;
            node.__isset.slot_ref = true;
            node.slot_ref.col_unique_id = slot.col_unique_id;
            node.slot_ref.slot_id = slot.id;
            node.slot_ref.__isset.col_unique_id = true;
            node.type.types.emplace_back();
            node.type.types[0].scalar_type = slot.slotType.types[0].scalar_type;
            node.type.types[0].__isset.scalar_type = true;

            slot.col_unique_id = col_unique_id++;
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
        slot.col_unique_id = col_unique_id++;
        intermediate_tuple_builder.add_slot(slot);

        auto& expr = projection_exprs.emplace_back();
        auto& node = expr.nodes.emplace_back();
        node.node_type = TExprNodeType::SLOT_REF;
        node.__set_is_nullable(true);
        node.num_children = 0;
        node.__isset.slot_ref = true;
        node.slot_ref.col_unique_id = slot.col_unique_id;
        node.slot_ref.slot_id = slot.id;
        node.slot_ref.__isset.col_unique_id = true;
        node.type.types.emplace_back();
        node.type.types[0].scalar_type = slot.slotType.types[0].scalar_type;
        node.type.types[0].__isset.scalar_type = true;

        slot.col_unique_id = col_unique_id++;
        output_tuple_builder.add_slot(slot);
    }

    tnode.projections = projection_exprs;
    tnode.__isset.projections = true;

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
        col_unique_id = eq_cond.left.nodes[0].slot_ref.col_unique_id;
        eq_cond.left.nodes[0].slot_ref.slot_id = slots_map[col_unique_id];

        col_unique_id = eq_cond.right.nodes[0].slot_ref.col_unique_id;
        eq_cond.right.nodes[0].slot_ref.slot_id = slots_map[col_unique_id];
    }

    for (size_t i = 0; i != mark_keys_count; ++i) {
        auto& slot_ref = tnode.hash_join_node.mark_join_conjuncts[i].nodes[1].slot_ref;
        slot_ref.slot_id = slots_map[slot_ref.col_unique_id];

        auto& slot_ref_right = tnode.hash_join_node.mark_join_conjuncts[i].nodes[2].slot_ref;
        slot_ref_right.slot_id = slots_map[slot_ref_right.col_unique_id];
    }

    for (auto& expr : tnode.projections) {
        for (auto& node : expr.nodes) {
            if (node.node_type == TExprNodeType::SLOT_REF) {
                auto col_unique_id = node.slot_ref.col_unique_id;
                node.slot_ref.slot_id = slots_map[col_unique_id];
            }
        }
    }

    if (tnode.hash_join_node.other_join_conjuncts.size() == 2) {
        auto& left_cond = tnode.hash_join_node.other_join_conjuncts[0];
        auto& right_cond = tnode.hash_join_node.other_join_conjuncts[1];

        left_cond.nodes[1].slot_ref.slot_id = slots_map[left_cond.nodes[1].slot_ref.col_unique_id];
        right_cond.nodes[1].slot_ref.slot_id =
                slots_map[right_cond.nodes[1].slot_ref.col_unique_id];
    }

    for (auto uid : hash_output_unique_ids) {
        tnode.hash_join_node.hash_output_slot_ids.emplace_back(slots_map[uid]);
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