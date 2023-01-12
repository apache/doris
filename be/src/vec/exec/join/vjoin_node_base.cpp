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
          _is_mark_join(tnode.__isset.nested_loop_join_node
                                ? (tnode.nested_loop_join_node.__isset.is_mark
                                           ? tnode.nested_loop_join_node.is_mark
                                           : false)
                        : tnode.hash_join_node.__isset.is_mark ? tnode.hash_join_node.is_mark
                                                               : false),
          _short_circuit_for_null_in_build_side(_join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
    _init_join_op();
    if (_is_mark_join) {
        DCHECK(_join_op == TJoinOp::LEFT_ANTI_JOIN || _join_op == TJoinOp::LEFT_SEMI_JOIN ||
               _join_op == TJoinOp::CROSS_JOIN)
                << "Mark join is only supported for left semi/anti join and cross join but this is "
                << _join_op;
    }
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
    return ExecNode::close(state);
}

void VJoinNodeBase::release_resource(RuntimeState* state) {
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VJoinNodeBase::release_resource");
    VExpr::close(_output_expr_ctxs, state);
    _join_block.clear();
    ExecNode::release_resource(state);
}

void VJoinNodeBase::_construct_mutable_join_block() {
    const auto& mutable_block_desc = intermediate_row_desc();
    for (const auto tuple_desc : mutable_block_desc.tuple_descriptors()) {
        for (const auto slot_desc : tuple_desc->slots()) {
            auto type_ptr = slot_desc->get_data_type_ptr();
            _join_block.insert({type_ptr->create_column(), type_ptr, slot_desc->col_name()});
        }
    }
    if (_is_mark_join) {
        _join_block.replace_by_position(
                _join_block.columns() - 1,
                remove_nullable(_join_block.get_by_position(_join_block.columns() - 1).column));
    }
    _join_block_column_num = _join_block.columns();
}

Status VJoinNodeBase::_build_output_block(Block* origin_block, Block* output_block) {
    auto is_mem_reuse = output_block->mem_reuse();
    auto rows = origin_block->rows();
    auto origin_col_num = origin_block->columns();
    if (rows != 0) {
        SCOPED_TIMER(_projection_timer);
        ColumnsWithTypeAndName empty_columns_with_type =
                VectorizedUtils::create_empty_block(row_desc());
        if (!is_mem_reuse) {
            MutableBlock mutable_block = MutableBlock(empty_columns_with_type);
            // If not mem reuse, it means output block is empty, so that we create empty column for it
            // Then the empty column maybe swap to origin_block, origin_block will reuse the column to
            // avoid call malloc and page fault
            output_block->swap(mutable_block.to_block());
        }
        Columns columns;
        // origin block id --> output block id
        // This mapping is used to help reuse the columns from output block to reduce the page fault
        std::map<int, int> col_mapping;
        Columns resusable_columns;
        size_t output_size = row_desc().num_materialized_slots();
        if (_output_expr_ctxs.empty()) {
            for (int i = 0; i < output_size; ++i) {
                columns.emplace_back(origin_block->get_by_position(i).column);
                col_mapping[i] = i;
            }
        } else {
            for (int i = 0; i < output_size; ++i) {
                int result_column_id = -1;
                RETURN_IF_ERROR(_output_expr_ctxs[i]->execute(origin_block, &result_column_id));
                auto column_ptr = origin_block->get_by_position(result_column_id)
                                          .column->convert_to_full_column_if_const();
                columns.emplace_back(column_ptr);
                if (result_column_id < origin_col_num) {
                    col_mapping[result_column_id] = i;
                }
            }
        }
        // TODO: After FE plan support same nullable of output expr and origin block and mutable column
        // we should replace `insert_column_datas` by `insert_range_from`
        // Sometimes the origin block's column nullable property is not equal to row descriptor
        // So that should change it here
        for (int i = 0; i < output_size; ++i) {
            if (empty_columns_with_type[i].type->is_nullable() && !columns[i]->is_nullable()) {
                columns[i] = vectorized::make_nullable(columns[i]);
            } else if (!empty_columns_with_type[i].type->is_nullable() &&
                       columns[i]->is_nullable()) {
                LOG(FATAL) << "To column is not nullable, but from column is nullable";
            }
        }

        for (int i = 0; i < origin_col_num; ++i) {
            if (col_mapping.find(i) == col_mapping.end()) {
                // It means this column is not transferred to output block, it is reuseable
                resusable_columns.emplace_back(origin_block->get_by_position(i).column);
            } else {
                // Move the column from output block
                if (empty_columns_with_type[col_mapping[i]].type->is_nullable() &&
                    !origin_block->get_by_position(i).column->is_nullable()) {
                    resusable_columns.emplace_back(
                            reinterpret_cast<const ColumnNullable*>(
                                    output_block->get_by_position(col_mapping[i]).column.get())
                                    ->get_nested_column_ptr());
                } else if (!empty_columns_with_type[i].type->is_nullable() &&
                           origin_block->get_by_position(i).column->is_nullable()) {
                    LOG(FATAL) << "To column is not nullable, but from column is nullable";
                } else {
                    resusable_columns.emplace_back(output_block->get_by_position(i).column);
                }
            }
        }
        output_block->set_columns(columns);
        DCHECK(output_block->rows() == rows);
        // Remove the redundant columns generated during expr calculation
        origin_block->prune_columns(origin_col_num);
        origin_block->set_columns(resusable_columns);
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
    try {
        std::thread([this, state, thread_status_p = &thread_status,
                     parent_span = opentelemetry::trace::Tracer::GetCurrentSpan()] {
            OpentelemetryScope scope {parent_span};
            this->_probe_side_open_thread(state, thread_status_p);
        }).detach();
    } catch (const std::system_error& e) {
        LOG(WARNING) << "In VJoinNodeBase::open create thread fail, " << e.what();
        return Status::InternalError(e.what());
    }

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

Status VJoinNodeBase::alloc_resource(doris::RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::alloc_resource(state));
    RETURN_IF_ERROR(VExpr::open(_output_expr_ctxs, state));
    return Status::OK();
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
