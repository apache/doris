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

#include "join_probe_operator.h"

#include "pipeline/exec/hashjoin_probe_operator.h"
#include "pipeline/exec/nested_loop_join_probe_operator.h"
#include "pipeline/exec/operator.h"

namespace doris::pipeline {

template <typename DependencyType, typename Derived>
Status JoinProbeLocalState<DependencyType, Derived>::init(RuntimeState* state,
                                                          LocalStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    auto& p = Base::_parent->template cast<typename Derived::Parent>();
    // only use in outer join as the bool column to mark for function of `tuple_is_null`
    if (p._is_outer_join) {
        _tuple_is_null_left_flag_column = vectorized::ColumnUInt8::create();
        _tuple_is_null_right_flag_column = vectorized::ColumnUInt8::create();
    }
    _output_expr_ctxs.resize(p._output_expr_ctxs.size());
    for (size_t i = 0; i < _output_expr_ctxs.size(); i++) {
        RETURN_IF_ERROR(p._output_expr_ctxs[i]->clone(state, _output_expr_ctxs[i]));
    }

    _probe_timer = ADD_TIMER(Base::profile(), "ProbeTime");
    _join_filter_timer = ADD_TIMER(Base::profile(), "JoinFilterTimer");
    _build_output_block_timer = ADD_TIMER(Base::profile(), "BuildOutputBlock");
    _probe_rows_counter = ADD_COUNTER(Base::profile(), "ProbeRows", TUnit::UNIT);

    return Status::OK();
}

template <typename DependencyType, typename Derived>
Status JoinProbeLocalState<DependencyType, Derived>::close(RuntimeState* state) {
    if (Base::_closed) {
        return Status::OK();
    }
    _join_block.clear();
    return Base::close(state);
}

template <typename DependencyType, typename Derived>
void JoinProbeLocalState<DependencyType, Derived>::_construct_mutable_join_block() {
    auto& p = Base::_parent->template cast<typename Derived::Parent>();
    const auto& mutable_block_desc = p._intermediate_row_desc;
    for (const auto tuple_desc : mutable_block_desc->tuple_descriptors()) {
        for (const auto slot_desc : tuple_desc->slots()) {
            auto type_ptr = slot_desc->get_data_type_ptr();
            _join_block.insert({type_ptr->create_column(), type_ptr, slot_desc->col_name()});
        }
    }
    if (p._is_mark_join) {
        DCHECK(!p._is_mark_join ||
               _join_block.get_by_position(_join_block.columns() - 1).column->is_nullable());
    }
}

template <typename DependencyType, typename Derived>
Status JoinProbeLocalState<DependencyType, Derived>::_build_output_block(
        vectorized::Block* origin_block, vectorized::Block* output_block, bool keep_origin) {
    auto& p = Base::_parent->template cast<typename Derived::Parent>();
    SCOPED_TIMER(_build_output_block_timer);
    auto is_mem_reuse = output_block->mem_reuse();
    vectorized::MutableBlock mutable_block =
            is_mem_reuse ? vectorized::MutableBlock(output_block)
                         : vectorized::MutableBlock(
                                   vectorized::VectorizedUtils::create_empty_columnswithtypename(
                                           p.row_desc()));
    auto rows = origin_block->rows();
    // TODO: After FE plan support same nullable of output expr and origin block and mutable column
    // we should replace `insert_column_datas` by `insert_range_from`

    auto insert_column_datas = [keep_origin](auto& to, vectorized::ColumnPtr& from, size_t rows) {
        if (to->is_nullable() && !from->is_nullable()) {
            if (keep_origin || !from->is_exclusive()) {
                auto& null_column = reinterpret_cast<vectorized::ColumnNullable&>(*to);
                null_column.get_nested_column().insert_range_from(*from, 0, rows);
                null_column.get_null_map_column().get_data().resize_fill(rows, 0);
            } else {
                to = make_nullable(from, false)->assume_mutable();
            }
        } else {
            if (keep_origin || !from->is_exclusive()) {
                to->insert_range_from(*from, 0, rows);
            } else {
                to = from->assume_mutable();
            }
        }
    };
    if (rows != 0) {
        auto& mutable_columns = mutable_block.mutable_columns();
        if (_output_expr_ctxs.empty()) {
            DCHECK(mutable_columns.size() == p.row_desc().num_materialized_slots());
            for (int i = 0; i < mutable_columns.size(); ++i) {
                insert_column_datas(mutable_columns[i], origin_block->get_by_position(i).column,
                                    rows);
            }
        } else {
            DCHECK(mutable_columns.size() == p.row_desc().num_materialized_slots());
            SCOPED_TIMER(Base::_projection_timer);
            for (int i = 0; i < mutable_columns.size(); ++i) {
                auto result_column_id = -1;
                RETURN_IF_ERROR(_output_expr_ctxs[i]->execute(origin_block, &result_column_id));
                auto& origin_column = origin_block->get_by_position(result_column_id).column;

                /// `convert_to_full_column_if_const` will create a pointer to the origin column if
                /// the origin column is not ColumnConst/ColumnArray, this make the column be not
                /// exclusive.
                /// TODO: maybe need a method to check if a column need to be converted to full
                /// column.
                if (is_column_const(*origin_column) ||
                    check_column<vectorized::ColumnArray>(origin_column)) {
                    auto column_ptr = origin_column->convert_to_full_column_if_const();
                    insert_column_datas(mutable_columns[i], column_ptr, rows);
                } else {
                    insert_column_datas(mutable_columns[i], origin_column, rows);
                }
            }
        }

        if (!is_mem_reuse || !keep_origin) {
            output_block->swap(mutable_block.to_block());
        }
        DCHECK(output_block->rows() == rows);
    }

    return Status::OK();
}

template <typename DependencyType, typename Derived>
void JoinProbeLocalState<DependencyType, Derived>::_reset_tuple_is_null_column() {
    if (Base::_parent->template cast<typename Derived::Parent>()._is_outer_join) {
        reinterpret_cast<vectorized::ColumnUInt8&>(*_tuple_is_null_left_flag_column).clear();
        reinterpret_cast<vectorized::ColumnUInt8&>(*_tuple_is_null_right_flag_column).clear();
    }
}

template <typename LocalStateType>
JoinProbeOperatorX<LocalStateType>::JoinProbeOperatorX(ObjectPool* pool, const TPlanNode& tnode,
                                                       const DescriptorTbl& descs)
        : Base(pool, tnode, descs),
          _join_op(tnode.__isset.hash_join_node ? tnode.hash_join_node.join_op
                                                : (tnode.__isset.nested_loop_join_node
                                                           ? tnode.nested_loop_join_node.join_op
                                                           : TJoinOp::CROSS_JOIN)),
          _have_other_join_conjunct(tnode.__isset.hash_join_node &&
                                    ((tnode.hash_join_node.__isset.other_join_conjuncts &&
                                      !tnode.hash_join_node.other_join_conjuncts.empty()) ||
                                     tnode.hash_join_node.__isset.vother_join_conjunct)),
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
                                                               : false) {
    if (tnode.__isset.hash_join_node) {
        _intermediate_row_desc.reset(new RowDescriptor(
                descs, tnode.hash_join_node.vintermediate_tuple_id_list,
                std::vector<bool>(tnode.hash_join_node.vintermediate_tuple_id_list.size())));
        _output_row_desc.reset(
                new RowDescriptor(descs, {tnode.hash_join_node.voutput_tuple_id}, {false}));
    } else if (tnode.__isset.nested_loop_join_node) {
        _intermediate_row_desc.reset(new RowDescriptor(
                descs, tnode.nested_loop_join_node.vintermediate_tuple_id_list,
                std::vector<bool>(tnode.nested_loop_join_node.vintermediate_tuple_id_list.size())));
        _output_row_desc.reset(
                new RowDescriptor(descs, {tnode.nested_loop_join_node.voutput_tuple_id}, {false}));
    } else {
        // Iff BE has been upgraded and FE has not yet, we should keep origin logics for CROSS JOIN.
        DCHECK_EQ(_join_op, TJoinOp::CROSS_JOIN);
    }
}

template <typename LocalStateType>
Status JoinProbeOperatorX<LocalStateType>::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(Base::init(tnode, state));
    if (tnode.__isset.hash_join_node || tnode.__isset.nested_loop_join_node) {
        const auto& output_exprs = tnode.__isset.hash_join_node
                                           ? tnode.hash_join_node.srcExprList
                                           : tnode.nested_loop_join_node.srcExprList;
        for (const auto& expr : output_exprs) {
            vectorized::VExprContextSPtr ctx;
            RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(expr, ctx));
            _output_expr_ctxs.push_back(ctx);
        }
    }

    return Status::OK();
}

template <typename LocalStateType>
Status JoinProbeOperatorX<LocalStateType>::open(doris::RuntimeState* state) {
    RETURN_IF_ERROR(Base::open(state));
    return vectorized::VExpr::open(_output_expr_ctxs, state);
}

template class JoinProbeLocalState<HashJoinDependency, HashJoinProbeLocalState>;
template class JoinProbeOperatorX<HashJoinProbeLocalState>;

template class JoinProbeLocalState<NestedLoopJoinDependency, NestedLoopJoinProbeLocalState>;
template class JoinProbeOperatorX<NestedLoopJoinProbeLocalState>;

} // namespace doris::pipeline
