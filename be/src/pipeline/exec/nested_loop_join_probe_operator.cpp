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

#include "nested_loop_join_probe_operator.h"

#include <memory>

#include "pipeline/exec/operator.h"
#include "vec/columns/column_filter_helper.h"
#include "vec/core/block.h"
#include "vec/exec/join/vnested_loop_join_node.h"

namespace doris {
class RuntimeState;
} // namespace doris

namespace doris::pipeline {

OPERATOR_CODE_GENERATOR(NestLoopJoinProbeOperator, StatefulOperator)

Status NestLoopJoinProbeOperator::prepare(doris::RuntimeState* state) {
    // just for speed up, the way is dangerous
    _child_block.reset(_node->get_left_block());
    return StatefulOperator::prepare(state);
}

Status NestLoopJoinProbeOperator::close(doris::RuntimeState* state) {
    _child_block.release();
    return StatefulOperator::close(state);
}

NestedLoopJoinProbeLocalState::NestedLoopJoinProbeLocalState(RuntimeState* state,
                                                             OperatorXBase* parent)
        : JoinProbeLocalState<NestedLoopJoinDependency, NestedLoopJoinProbeLocalState>(state,
                                                                                       parent),
          _matched_rows_done(false),
          _left_block_pos(0) {}

Status NestedLoopJoinProbeLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(JoinProbeLocalState::init(state, info));
    SCOPED_TIMER(profile()->total_time_counter());
    SCOPED_TIMER(_open_timer);
    auto& p = _parent->cast<NestedLoopJoinProbeOperatorX>();
    _join_conjuncts.resize(p._join_conjuncts.size());
    for (size_t i = 0; i < _join_conjuncts.size(); i++) {
        RETURN_IF_ERROR(p._join_conjuncts[i]->clone(state, _join_conjuncts[i]));
    }
    _construct_mutable_join_block();

    _loop_join_timer = ADD_TIMER(profile(), "LoopGenerateJoin");
    return Status::OK();
}

Status NestedLoopJoinProbeLocalState::close(RuntimeState* state) {
    SCOPED_TIMER(profile()->total_time_counter());
    SCOPED_TIMER(_close_timer);
    if (_closed) {
        return Status::OK();
    }
    _child_block->clear();

    _tuple_is_null_left_flag_column = nullptr;
    _tuple_is_null_right_flag_column = nullptr;
    return JoinProbeLocalState<NestedLoopJoinDependency, NestedLoopJoinProbeLocalState>::close(
            state);
}

void NestedLoopJoinProbeLocalState::_update_additional_flags(vectorized::Block* block) {
    auto& p = _parent->cast<NestedLoopJoinProbeOperatorX>();
    if (p._is_outer_join) {
        auto p0 = _tuple_is_null_left_flag_column->assume_mutable();
        auto p1 = _tuple_is_null_right_flag_column->assume_mutable();
        auto& left_null_map = reinterpret_cast<vectorized::ColumnUInt8&>(*p0);
        auto& right_null_map = reinterpret_cast<vectorized::ColumnUInt8&>(*p1);
        auto left_size = left_null_map.size();
        auto right_size = right_null_map.size();

        if (left_size < block->rows()) {
            left_null_map.get_data().resize_fill(block->rows(), 0);
        }
        if (right_size < block->rows()) {
            right_null_map.get_data().resize_fill(block->rows(), 0);
        }
    }
    if (p._is_mark_join) {
        auto mark_column = block->get_by_position(block->columns() - 1).column->assume_mutable();
        if (mark_column->size() < block->rows()) {
            vectorized::ColumnFilterHelper(*mark_column).resize_fill(block->rows(), 1);
        }
    }
}

void NestedLoopJoinProbeLocalState::_reset_with_next_probe_row() {
    // TODO: need a vector of left block to register the _probe_row_visited_flags
    _current_build_pos = 0;
    _left_block_pos++;
}

void NestedLoopJoinProbeLocalState::add_tuple_is_null_column(vectorized::Block* block) {
    auto& p = _parent->cast<NestedLoopJoinProbeOperatorX>();
    if (p._is_outer_join) {
        auto p0 = _tuple_is_null_left_flag_column->assume_mutable();
        auto p1 = _tuple_is_null_right_flag_column->assume_mutable();
        block->insert({std::move(p0), std::make_shared<vectorized::DataTypeUInt8>(),
                       "left_tuples_is_null"});
        block->insert({std::move(p1), std::make_shared<vectorized::DataTypeUInt8>(),
                       "right_tuples_is_null"});
    }
}

template <typename JoinOpType, bool set_build_side_flag, bool set_probe_side_flag>
Status NestedLoopJoinProbeLocalState::generate_join_block_data(RuntimeState* state,
                                                               JoinOpType& join_op_variants) {
    auto& p = _parent->cast<NestedLoopJoinProbeOperatorX>();
    constexpr bool ignore_null = JoinOpType::value == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN;
    _left_block_start_pos = _left_block_pos;
    _left_side_process_count = 0;
    DCHECK(!_need_more_input_data || !_matched_rows_done);

    vectorized::MutableBlock mutable_join_block(&_join_block);
    if (!_matched_rows_done && !_need_more_input_data) {
        // We should try to join rows if there still are some rows from probe side.
        while (_join_block.rows() < state->batch_size()) {
            while (_current_build_pos == _shared_state->build_blocks.size() ||
                   _left_block_pos == _child_block->rows()) {
                // if left block is empty(), do not need disprocess the left block rows
                if (_child_block->rows() > _left_block_pos) {
                    _left_side_process_count++;
                }

                _reset_with_next_probe_row();
                if (_left_block_pos < _child_block->rows()) {
                    if constexpr (set_probe_side_flag) {
                        _probe_offset_stack.push(mutable_join_block.rows());
                    }
                } else {
                    if (_shared_state->left_side_eos) {
                        _matched_rows_done = true;
                    } else {
                        _need_more_input_data = true;
                    }
                    break;
                }
            }

            // Do not have left row need to be disposed
            if (_matched_rows_done || _need_more_input_data) {
                break;
            }

            const auto& now_process_build_block = _shared_state->build_blocks[_current_build_pos++];
            if constexpr (set_build_side_flag) {
                _build_offset_stack.push(mutable_join_block.rows());
            }
            _process_left_child_block(mutable_join_block, now_process_build_block);
        }

        if constexpr (set_probe_side_flag) {
            Status status;
            RETURN_IF_CATCH_EXCEPTION(
                    (status = _do_filtering_and_update_visited_flags<
                             set_build_side_flag, set_probe_side_flag, ignore_null>(
                             &_join_block, !p._is_left_semi_anti)));
            _update_additional_flags(&_join_block);
            if (!status.ok()) {
                return status;
            }
            mutable_join_block = vectorized::MutableBlock(&_join_block);
            // If this join operation is left outer join or full outer join, when
            // `_left_side_process_count`, means all rows from build
            // side have been joined with _left_side_process_count, we should output current
            // probe row with null from build side.
            if (_left_side_process_count) {
                _finalize_current_phase<false, JoinOpType::value == TJoinOp::LEFT_SEMI_JOIN>(
                        mutable_join_block, state->batch_size());
            }
        }

        if (_left_side_process_count) {
            if (p._is_mark_join && _shared_state->build_blocks.empty()) {
                DCHECK_EQ(JoinOpType::value, TJoinOp::CROSS_JOIN);
                _append_left_data_with_null(mutable_join_block);
            }
        }
    }

    if constexpr (!set_probe_side_flag) {
        Status status;
        RETURN_IF_CATCH_EXCEPTION(
                (status = _do_filtering_and_update_visited_flags<set_build_side_flag,
                                                                 set_probe_side_flag, ignore_null>(
                         &_join_block, !p._is_right_semi_anti)));
        _update_additional_flags(&_join_block);
        mutable_join_block = vectorized::MutableBlock(&_join_block);
        if (!status.ok()) {
            return status;
        }
    }

    if constexpr (set_build_side_flag) {
        if (_matched_rows_done &&
            _output_null_idx_build_side < _shared_state->build_blocks.size()) {
            _finalize_current_phase<true, JoinOpType::value == TJoinOp::RIGHT_SEMI_JOIN>(
                    mutable_join_block, state->batch_size());
        }
    }
    return Status::OK();
}

void NestedLoopJoinProbeLocalState::_resize_fill_tuple_is_null_column(size_t new_size,
                                                                      int left_flag,
                                                                      int right_flag) {
    auto& p = _parent->cast<NestedLoopJoinProbeOperatorX>();
    if (p._is_outer_join) {
        reinterpret_cast<vectorized::ColumnUInt8*>(_tuple_is_null_left_flag_column.get())
                ->get_data()
                .resize_fill(new_size, left_flag);
        reinterpret_cast<vectorized::ColumnUInt8*>(_tuple_is_null_right_flag_column.get())
                ->get_data()
                .resize_fill(new_size, right_flag);
    }
}

template <bool BuildSide, bool IsSemi>
void NestedLoopJoinProbeLocalState::_finalize_current_phase(vectorized::MutableBlock& mutable_block,
                                                            size_t batch_size) {
    auto& p = _parent->cast<NestedLoopJoinProbeOperatorX>();
    auto& dst_columns = mutable_block.mutable_columns();
    DCHECK_GT(dst_columns.size(), 0);
    auto column_size = dst_columns[0]->size();
    if constexpr (BuildSide) {
        DCHECK(!p._is_mark_join);
        auto build_block_sz = _shared_state->build_blocks.size();
        size_t i = _output_null_idx_build_side;
        for (; i < build_block_sz && column_size < batch_size; i++) {
            const auto& cur_block = _shared_state->build_blocks[i];
            const auto* __restrict cur_visited_flags =
                    assert_cast<vectorized::ColumnUInt8*>(
                            _shared_state->build_side_visited_flags[i].get())
                            ->get_data()
                            .data();
            const auto num_rows = cur_block.rows();

            std::vector<int> selector(num_rows);
            size_t selector_idx = 0;
            for (size_t j = 0; j < num_rows; j++) {
                if constexpr (IsSemi) {
                    if (cur_visited_flags[j]) {
                        selector[selector_idx++] = j;
                    }
                } else {
                    if (!cur_visited_flags[j]) {
                        selector[selector_idx++] = j;
                    }
                }
            }

            column_size += selector_idx;
            for (size_t j = 0; j < p._num_probe_side_columns; ++j) {
                DCHECK(p._join_op == TJoinOp::RIGHT_OUTER_JOIN ||
                       p._join_op == TJoinOp::FULL_OUTER_JOIN ||
                       p._join_op == TJoinOp::RIGHT_ANTI_JOIN ||
                       p._join_op == TJoinOp::RIGHT_SEMI_JOIN);
                dst_columns[j]->insert_many_defaults(selector_idx);
            }
            for (size_t j = 0; j < p._num_build_side_columns; ++j) {
                auto src_column = cur_block.get_by_position(j);
                if (!src_column.column->is_nullable() &&
                    dst_columns[p._num_probe_side_columns + j]->is_nullable()) {
                    DCHECK(p._join_op == TJoinOp::FULL_OUTER_JOIN);
                    assert_cast<vectorized::ColumnNullable*>(
                            dst_columns[p._num_probe_side_columns + j].get())
                            ->get_nested_column_ptr()
                            ->insert_indices_from(*src_column.column, selector.data(),
                                                  selector.data() + selector_idx);
                    assert_cast<vectorized::ColumnNullable*>(
                            dst_columns[p._num_probe_side_columns + j].get())
                            ->get_null_map_column()
                            .get_data()
                            .resize_fill(column_size, 0);
                } else {
                    dst_columns[p._num_probe_side_columns + j]->insert_indices_from(
                            *src_column.column.get(), selector.data(),
                            selector.data() + selector_idx);
                }
            }
            _resize_fill_tuple_is_null_column(column_size, 1, 0);
        }
        _output_null_idx_build_side = i;
    } else {
        if (!p._is_mark_join) {
            auto new_size = column_size;
            DCHECK_LE(_left_block_start_pos + _left_side_process_count, _child_block->rows());
            for (int j = _left_block_start_pos;
                 j < _left_block_start_pos + _left_side_process_count; ++j) {
                if (_cur_probe_row_visited_flags[j] == IsSemi) {
                    new_size++;
                    for (size_t i = 0; i < p._num_probe_side_columns; ++i) {
                        const vectorized::ColumnWithTypeAndName src_column =
                                _child_block->get_by_position(i);
                        if (!src_column.column->is_nullable() && dst_columns[i]->is_nullable()) {
                            DCHECK(p._join_op == TJoinOp::FULL_OUTER_JOIN);
                            assert_cast<vectorized::ColumnNullable*>(dst_columns[i].get())
                                    ->get_nested_column_ptr()
                                    ->insert_many_from(*src_column.column, j, 1);
                            assert_cast<vectorized::ColumnNullable*>(dst_columns[i].get())
                                    ->get_null_map_column()
                                    .get_data()
                                    .resize_fill(new_size, 0);
                        } else {
                            dst_columns[i]->insert_many_from(*src_column.column, j, 1);
                        }
                    }
                }
            }
            if (new_size > column_size) {
                for (size_t i = 0; i < p._num_build_side_columns; ++i) {
                    dst_columns[p._num_probe_side_columns + i]->insert_many_defaults(new_size -
                                                                                     column_size);
                }
                _resize_fill_tuple_is_null_column(new_size, 0, 1);
            }
        } else {
            vectorized::ColumnFilterHelper mark_column(*dst_columns[dst_columns.size() - 1]);
            mark_column.reserve(mark_column.size() + _left_side_process_count);
            DCHECK_LE(_left_block_start_pos + _left_side_process_count, _child_block->rows());
            for (int j = _left_block_start_pos;
                 j < _left_block_start_pos + _left_side_process_count; ++j) {
                mark_column.insert_value(IsSemi == _cur_probe_row_visited_flags[j]);
            }
            for (size_t i = 0; i < p._num_probe_side_columns; ++i) {
                const vectorized::ColumnWithTypeAndName src_column =
                        _child_block->get_by_position(i);
                DCHECK(p._join_op != TJoinOp::FULL_OUTER_JOIN);
                dst_columns[i]->insert_range_from(*src_column.column, _left_block_start_pos,
                                                  _left_side_process_count);
            }
            for (size_t i = 0; i < p._num_build_side_columns; ++i) {
                dst_columns[p._num_probe_side_columns + i]->insert_many_defaults(
                        _left_side_process_count);
            }
            _resize_fill_tuple_is_null_column(_left_side_process_count, 0, 1);
        }
    }
}

void NestedLoopJoinProbeLocalState::_append_left_data_with_null(
        vectorized::MutableBlock& mutable_block) const {
    auto& p = _parent->cast<NestedLoopJoinProbeOperatorX>();
    auto& dst_columns = mutable_block.mutable_columns();
    DCHECK(p._is_mark_join);
    for (size_t i = 0; i < p._num_probe_side_columns; ++i) {
        const vectorized::ColumnWithTypeAndName& src_column = _child_block->get_by_position(i);
        if (!src_column.column->is_nullable() && dst_columns[i]->is_nullable()) {
            auto origin_sz = dst_columns[i]->size();
            DCHECK(p._join_op == TJoinOp::RIGHT_OUTER_JOIN ||
                   p._join_op == TJoinOp::FULL_OUTER_JOIN);
            assert_cast<vectorized::ColumnNullable*>(dst_columns[i].get())
                    ->get_nested_column_ptr()
                    ->insert_range_from(*src_column.column, _left_block_start_pos,
                                        _left_side_process_count);
            assert_cast<vectorized::ColumnNullable*>(dst_columns[i].get())
                    ->get_null_map_column()
                    .get_data()
                    .resize_fill(origin_sz + 1, 0);
        } else {
            dst_columns[i]->insert_range_from(*src_column.column, _left_block_start_pos,
                                              _left_side_process_count);
        }
    }
    for (size_t i = 0; i < p._num_build_side_columns; ++i) {
        dst_columns[p._num_probe_side_columns + i]->insert_many_defaults(_left_side_process_count);
    }
    auto& mark_column = *dst_columns[dst_columns.size() - 1];
    vectorized::ColumnFilterHelper(mark_column)
            .resize_fill(mark_column.size() + _left_side_process_count, 0);
}

void NestedLoopJoinProbeLocalState::_process_left_child_block(
        vectorized::MutableBlock& mutable_block,
        const vectorized::Block& now_process_build_block) const {
    auto& p = _parent->cast<NestedLoopJoinProbeOperatorX>();
    auto& dst_columns = mutable_block.mutable_columns();
    const int max_added_rows = now_process_build_block.rows();
    for (size_t i = 0; i < p._num_probe_side_columns; ++i) {
        const vectorized::ColumnWithTypeAndName& src_column = _child_block->get_by_position(i);
        if (!src_column.column->is_nullable() && dst_columns[i]->is_nullable()) {
            auto origin_sz = dst_columns[i]->size();
            DCHECK(p._join_op == TJoinOp::RIGHT_OUTER_JOIN ||
                   p._join_op == TJoinOp::FULL_OUTER_JOIN);
            assert_cast<vectorized::ColumnNullable*>(dst_columns[i].get())
                    ->get_nested_column_ptr()
                    ->insert_many_from(*src_column.column, _left_block_pos, max_added_rows);
            assert_cast<vectorized::ColumnNullable*>(dst_columns[i].get())
                    ->get_null_map_column()
                    .get_data()
                    .resize_fill(origin_sz + max_added_rows, 0);
        } else {
            dst_columns[i]->insert_many_from(*src_column.column, _left_block_pos, max_added_rows);
        }
    }
    for (size_t i = 0; i < p._num_build_side_columns; ++i) {
        const vectorized::ColumnWithTypeAndName& src_column =
                now_process_build_block.get_by_position(i);
        if (!src_column.column->is_nullable() &&
            dst_columns[p._num_probe_side_columns + i]->is_nullable()) {
            auto origin_sz = dst_columns[p._num_probe_side_columns + i]->size();
            DCHECK(p._join_op == TJoinOp::LEFT_OUTER_JOIN ||
                   p._join_op == TJoinOp::FULL_OUTER_JOIN);
            assert_cast<vectorized::ColumnNullable*>(
                    dst_columns[p._num_probe_side_columns + i].get())
                    ->get_nested_column_ptr()
                    ->insert_range_from(*src_column.column.get(), 0, max_added_rows);
            assert_cast<vectorized::ColumnNullable*>(
                    dst_columns[p._num_probe_side_columns + i].get())
                    ->get_null_map_column()
                    .get_data()
                    .resize_fill(origin_sz + max_added_rows, 0);
        } else {
            dst_columns[p._num_probe_side_columns + i]->insert_range_from(*src_column.column.get(),
                                                                          0, max_added_rows);
        }
    }
}

NestedLoopJoinProbeOperatorX::NestedLoopJoinProbeOperatorX(ObjectPool* pool, const TPlanNode& tnode,
                                                           int operator_id,
                                                           const DescriptorTbl& descs)
        : JoinProbeOperatorX<NestedLoopJoinProbeLocalState>(pool, tnode, operator_id, descs),
          _is_output_left_side_only(tnode.nested_loop_join_node.__isset.is_output_left_side_only &&
                                    tnode.nested_loop_join_node.is_output_left_side_only),
          _old_version_flag(!tnode.__isset.nested_loop_join_node) {}

Status NestedLoopJoinProbeOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(JoinProbeOperatorX<NestedLoopJoinProbeLocalState>::init(tnode, state));

    if (tnode.nested_loop_join_node.__isset.join_conjuncts &&
        !tnode.nested_loop_join_node.join_conjuncts.empty()) {
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(
                tnode.nested_loop_join_node.join_conjuncts, _join_conjuncts));
    } else if (tnode.nested_loop_join_node.__isset.vjoin_conjunct) {
        vectorized::VExprContextSPtr context;
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(
                tnode.nested_loop_join_node.vjoin_conjunct, context));
        _join_conjuncts.emplace_back(context);
    }

    return Status::OK();
}

Status NestedLoopJoinProbeOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(JoinProbeOperatorX<NestedLoopJoinProbeLocalState>::prepare(state));
    for (auto& conjunct : _join_conjuncts) {
        RETURN_IF_ERROR(conjunct->prepare(state, *_intermediate_row_desc));
    }
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_output_expr_ctxs, state, *_intermediate_row_desc));
    _num_probe_side_columns = _child_x->row_desc().num_materialized_slots();
    _num_build_side_columns = _build_side_child->row_desc().num_materialized_slots();
    return Status::OK();
}

Status NestedLoopJoinProbeOperatorX::open(RuntimeState* state) {
    RETURN_IF_ERROR(JoinProbeOperatorX<NestedLoopJoinProbeLocalState>::open(state));
    return vectorized::VExpr::open(_join_conjuncts, state);
}

bool NestedLoopJoinProbeOperatorX::need_more_input_data(RuntimeState* state) const {
    auto& local_state =
            state->get_local_state(operator_id())->cast<NestedLoopJoinProbeLocalState>();
    return local_state._need_more_input_data and !local_state._shared_state->left_side_eos and
           local_state._join_block.rows() == 0;
}

Status NestedLoopJoinProbeOperatorX::push(doris::RuntimeState* state, vectorized::Block* block,
                                          SourceState source_state) const {
    CREATE_LOCAL_STATE_RETURN_IF_ERROR(local_state);
    COUNTER_UPDATE(local_state._probe_rows_counter, block->rows());
    local_state._cur_probe_row_visited_flags.resize(block->rows());
    std::fill(local_state._cur_probe_row_visited_flags.begin(),
              local_state._cur_probe_row_visited_flags.end(), 0);
    local_state._left_block_pos = 0;
    local_state._need_more_input_data = false;
    local_state._shared_state->left_side_eos = source_state == SourceState::FINISHED;

    if (!_is_output_left_side_only) {
        auto func = [&](auto&& join_op_variants, auto set_build_side_flag,
                        auto set_probe_side_flag) {
            return local_state.generate_join_block_data<std::decay_t<decltype(join_op_variants)>,
                                                        set_build_side_flag, set_probe_side_flag>(
                    state, join_op_variants);
        };
        RETURN_IF_ERROR(
                std::visit(func, local_state._shared_state->join_op_variants,
                           vectorized::make_bool_variant(_match_all_build || _is_right_semi_anti),
                           vectorized::make_bool_variant(_match_all_probe || _is_left_semi_anti)));
    }
    return Status::OK();
}

Status NestedLoopJoinProbeOperatorX::pull(RuntimeState* state, vectorized::Block* block,
                                          SourceState& source_state) const {
    CREATE_LOCAL_STATE_RETURN_IF_ERROR(local_state);
    if (_is_output_left_side_only) {
        RETURN_IF_ERROR(local_state._build_output_block(local_state._child_block.get(), block));
        source_state =
                local_state._shared_state->left_side_eos ? SourceState::FINISHED : source_state;
        local_state._need_more_input_data = !local_state._shared_state->left_side_eos;
    } else {
        source_state = ((_match_all_build || _is_right_semi_anti)
                                ? local_state._output_null_idx_build_side ==
                                                  local_state._shared_state->build_blocks.size() &&
                                          local_state._matched_rows_done
                                : local_state._matched_rows_done)
                               ? SourceState::FINISHED
                               : source_state;

        {
            vectorized::Block tmp_block = local_state._join_block;

            // Here make _join_block release the columns' ptr
            local_state._join_block.set_columns(local_state._join_block.clone_empty_columns());

            local_state.add_tuple_is_null_column(&tmp_block);
            {
                SCOPED_TIMER(local_state._join_filter_timer);
                RETURN_IF_ERROR(vectorized::VExprContext::filter_block(
                        local_state._conjuncts, &tmp_block, tmp_block.columns()));
            }
            RETURN_IF_ERROR(local_state._build_output_block(&tmp_block, block, false));
            local_state._reset_tuple_is_null_column();
        }
        local_state._join_block.clear_column_data();

        if (!(source_state == SourceState::FINISHED) and !local_state._need_more_input_data) {
            auto func = [&](auto&& join_op_variants, auto set_build_side_flag,
                            auto set_probe_side_flag) {
                return local_state
                        .generate_join_block_data<std::decay_t<decltype(join_op_variants)>,
                                                  set_build_side_flag, set_probe_side_flag>(
                                state, join_op_variants);
            };
            SCOPED_TIMER(local_state._loop_join_timer);
            RETURN_IF_ERROR(std::visit(
                    func, local_state._shared_state->join_op_variants,
                    vectorized::make_bool_variant(_match_all_build || _is_right_semi_anti),
                    vectorized::make_bool_variant(_match_all_probe || _is_left_semi_anti)));
        }
    }

    local_state.reached_limit(block, source_state);
    return Status::OK();
}

} // namespace doris::pipeline
