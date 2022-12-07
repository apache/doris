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

#include "vec/exec/join/vjoin_node_base.h"

#include <sstream>

#include "gen_cpp/PlanNodes_types.h"
#include "runtime/runtime_state.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {

VJoinNodeBase::VJoinNodeBase(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _join_op(tnode.__isset.hash_join_node ? tnode.hash_join_node.join_op
                                                : (tnode.__isset.nested_loop_join_node
                                                           ? tnode.nested_loop_join_node.join_op
                                                           : TJoinOp::CROSS_JOIN)),
          _have_other_join_conjunct(tnode.__isset.hash_join_node
                                            ? tnode.hash_join_node.__isset.vother_join_conjunct
                                            : false),
          _match_all_probe(_join_op == TJoinOp::LEFT_OUTER_JOIN ||
                           _join_op == TJoinOp::FULL_OUTER_JOIN),
          _match_all_build(_join_op == TJoinOp::RIGHT_OUTER_JOIN ||
                           _join_op == TJoinOp::FULL_OUTER_JOIN),
          _build_unique(!_have_other_join_conjunct &&
                        (_join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN ||
                         _join_op == TJoinOp::LEFT_ANTI_JOIN ||
                         _join_op == TJoinOp::LEFT_SEMI_JOIN)),
          _is_right_semi_anti(_join_op == TJoinOp::RIGHT_ANTI_JOIN ||
                              _join_op == TJoinOp::RIGHT_SEMI_JOIN),
          _is_left_semi_anti(_join_op == TJoinOp::LEFT_ANTI_JOIN ||
                             _join_op == TJoinOp::LEFT_SEMI_JOIN ||
                             _join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN),
          _is_outer_join(_match_all_build || _match_all_probe),
          _short_circuit_for_null_in_build_side(_join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
    _init_join_op();
    if (tnode.__isset.hash_join_node) {
        _output_row_desc.reset(
                new RowDescriptor(descs, {tnode.hash_join_node.voutput_tuple_id}, {false}));
        _intermediate_row_desc.reset(new RowDescriptor(
                descs, tnode.hash_join_node.vintermediate_tuple_id_list,
                std::vector<bool>(tnode.hash_join_node.vintermediate_tuple_id_list.size())));
    } else if (tnode.__isset.nested_loop_join_node) {
        _output_row_desc.reset(
                new RowDescriptor(descs, {tnode.nested_loop_join_node.voutput_tuple_id}, {false}));
        _intermediate_row_desc.reset(new RowDescriptor(
                descs, tnode.nested_loop_join_node.vintermediate_tuple_id_list,
                std::vector<bool>(tnode.nested_loop_join_node.vintermediate_tuple_id_list.size())));
    } else {
        // Iff BE has been upgraded and FE has not yet, we should keep origin logics for CROSS JOIN.
        DCHECK_EQ(_join_op, TJoinOp::CROSS_JOIN);
    }
}

Status VJoinNodeBase::close(RuntimeState* state) {
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VJoinNodeBase::close");
    VExpr::close(_output_expr_ctxs, state);
    _join_block.clear();
    return ExecNode::close(state);
}

void VJoinNodeBase::_construct_mutable_join_block() {
    const auto& mutable_block_desc = intermediate_row_desc();
    for (const auto tuple_desc : mutable_block_desc.tuple_descriptors()) {
        for (const auto slot_desc : tuple_desc->slots()) {
            auto type_ptr = slot_desc->get_data_type_ptr();
            _join_block.insert({type_ptr->create_column(), type_ptr, slot_desc->col_name()});
        }
    }
}

Status VJoinNodeBase::_build_output_block(Block* origin_block, Block* output_block) {
    auto is_mem_reuse = output_block->mem_reuse();
    MutableBlock mutable_block =
            is_mem_reuse
                    ? MutableBlock(output_block)
                    : MutableBlock(VectorizedUtils::create_empty_columnswithtypename(row_desc()));
    auto rows = origin_block->rows();
    // TODO: After FE plan support same nullable of output expr and origin block and mutable column
    // we should replace `insert_column_datas` by `insert_range_from`

    auto insert_column_datas = [](auto& to, const auto& from, size_t rows) {
        if (to->is_nullable() && !from.is_nullable()) {
            auto& null_column = reinterpret_cast<ColumnNullable&>(*to);
            null_column.get_nested_column().insert_range_from(from, 0, rows);
            null_column.get_null_map_column().get_data().resize_fill(rows, 0);
        } else {
            to->insert_range_from(from, 0, rows);
        }
    };
    if (rows != 0) {
        auto& mutable_columns = mutable_block.mutable_columns();
        if (_output_expr_ctxs.empty()) {
            DCHECK(mutable_columns.size() == row_desc().num_materialized_slots());
            for (int i = 0; i < mutable_columns.size(); ++i) {
                insert_column_datas(mutable_columns[i], *origin_block->get_by_position(i).column,
                                    rows);
            }
        } else {
            DCHECK(mutable_columns.size() == row_desc().num_materialized_slots());
            SCOPED_TIMER(_projection_timer);
            for (int i = 0; i < mutable_columns.size(); ++i) {
                auto result_column_id = -1;
                RETURN_IF_ERROR(_output_expr_ctxs[i]->execute(origin_block, &result_column_id));
                auto column_ptr = origin_block->get_by_position(result_column_id)
                                          .column->convert_to_full_column_if_const();
                insert_column_datas(mutable_columns[i], *column_ptr, rows);
            }
        }

        if (!is_mem_reuse) {
            output_block->swap(mutable_block.to_block());
        }
        DCHECK(output_block->rows() == rows);
    }

    return Status::OK();
}

Status VJoinNodeBase::init(const TPlanNode& tnode, RuntimeState* state) {
    if (tnode.__isset.hash_join_node || tnode.__isset.nested_loop_join_node) {
        const auto& output_exprs = tnode.__isset.hash_join_node
                                           ? tnode.hash_join_node.srcExprList
                                           : tnode.nested_loop_join_node.srcExprList;
        for (const auto& expr : output_exprs) {
            VExprContext* ctx = nullptr;
            RETURN_IF_ERROR(VExpr::create_expr_tree(_pool, expr, &ctx));
            _output_expr_ctxs.push_back(ctx);
        }
    }
    // only use in outer join as the bool column to mark for function of `tuple_is_null`
    if (_is_outer_join) {
        _tuple_is_null_left_flag_column = ColumnUInt8::create();
        _tuple_is_null_right_flag_column = ColumnUInt8::create();
    }
    return ExecNode::init(tnode, state);
}

Status VJoinNodeBase::open(RuntimeState* state) {
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VJoinNodeBase::open");
    RETURN_IF_ERROR(ExecNode::open(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker_growh());
    RETURN_IF_CANCELLED(state);

    std::promise<Status> thread_status;
    std::thread([this, state, thread_status_p = &thread_status,
                 parent_span = opentelemetry::trace::Tracer::GetCurrentSpan()] {
        OpentelemetryScope scope {parent_span};
        this->_probe_side_open_thread(state, thread_status_p);
    }).detach();

    // Open the probe-side child so that it may perform any initialisation in parallel.
    // Don't exit even if we see an error, we still need to wait for the build thread
    // to finish.
    // ISSUE-1247, check open_status after buildThread execute.
    // If this return first, build thread will use 'thread_status'
    // which is already destructor and then coredump.
    Status status = _materialize_build_side(state);
    RETURN_IF_ERROR(thread_status.get_future().get());

    RETURN_IF_ERROR(VExpr::open(_output_expr_ctxs, state));
    return status;
}

void VJoinNodeBase::_reset_tuple_is_null_column() {
    if (_is_outer_join) {
        reinterpret_cast<ColumnUInt8&>(*_tuple_is_null_left_flag_column).clear();
        reinterpret_cast<ColumnUInt8&>(*_tuple_is_null_right_flag_column).clear();
    }
}

void VJoinNodeBase::_probe_side_open_thread(RuntimeState* state, std::promise<Status>* status) {
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VJoinNodeBase::_hash_table_build_thread");
    SCOPED_ATTACH_TASK(state);
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker_growh_shared());
    status->set_value(child(0)->open(state));
}

#define APPLY_FOR_JOINOP_VARIANTS(M) \
    M(INNER_JOIN)                    \
    M(LEFT_SEMI_JOIN)                \
    M(LEFT_ANTI_JOIN)                \
    M(LEFT_OUTER_JOIN)               \
    M(FULL_OUTER_JOIN)               \
    M(RIGHT_OUTER_JOIN)              \
    M(CROSS_JOIN)                    \
    M(RIGHT_SEMI_JOIN)               \
    M(RIGHT_ANTI_JOIN)               \
    M(NULL_AWARE_LEFT_ANTI_JOIN)

void VJoinNodeBase::_init_join_op() {
    switch (_join_op) {
#define M(NAME)                                                                            \
    case TJoinOp::NAME:                                                                    \
        _join_op_variants.emplace<std::integral_constant<TJoinOp::type, TJoinOp::NAME>>(); \
        break;
        APPLY_FOR_JOINOP_VARIANTS(M);
#undef M
    default:
        //do nothing
        break;
    }
}

} // namespace doris::vectorized
