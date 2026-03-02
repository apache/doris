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

#include "common/cast_set.h"
#include "common/exception.h"
#include "pipeline/exec/operator.h"
#include "vec/columns/column.h"
#include "vec/columns/column_filter_helper.h"
#include "vec/core/block.h"

namespace doris {
class RuntimeState;
} // namespace doris

namespace doris::pipeline {
#include "common/compile_check_begin.h"
NestedLoopJoinProbeLocalState::NestedLoopJoinProbeLocalState(RuntimeState* state,
                                                             OperatorXBase* parent)
        : JoinProbeLocalState<NestedLoopJoinSharedState, NestedLoopJoinProbeLocalState>(state,
                                                                                        parent),
          _matched_rows_done(false),
          _probe_block_pos(0) {}

Status NestedLoopJoinProbeLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(JoinProbeLocalState::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _loop_join_timer = ADD_TIMER(custom_profile(), "LoopGenerateJoin");
    _output_temp_blocks_timer = ADD_TIMER(custom_profile(), "OutputTempBlocksTime");
    _update_visited_flags_timer = ADD_TIMER(custom_profile(), "UpdateVisitedFlagsTime");
    _join_conjuncts_evaluation_timer = ADD_TIMER(custom_profile(), "JoinConjunctsEvaluationTime");
    _filtered_by_join_conjuncts_timer = ADD_TIMER(custom_profile(), "FilteredByJoinConjunctsTime");
    return Status::OK();
}

Status NestedLoopJoinProbeLocalState::open(RuntimeState* state) {
    RETURN_IF_ERROR(JoinProbeLocalState::open(state));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    auto& p = _parent->cast<NestedLoopJoinProbeOperatorX>();
    _join_conjuncts.resize(p._join_conjuncts.size());
    for (size_t i = 0; i < _join_conjuncts.size(); i++) {
        RETURN_IF_ERROR(p._join_conjuncts[i]->clone(state, _join_conjuncts[i]));
    }
    _construct_mutable_join_block();
    return Status::OK();
}

Status NestedLoopJoinProbeLocalState::close(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_close_timer);
    if (_closed) {
        return Status::OK();
    }
    _child_block->clear();

    return JoinProbeLocalState<NestedLoopJoinSharedState, NestedLoopJoinProbeLocalState>::close(
            state);
}

void NestedLoopJoinProbeLocalState::_update_additional_flags(vectorized::Block* block) {
    auto& p = _parent->cast<NestedLoopJoinProbeOperatorX>();
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
    _probe_block_pos++;
}

// process_probe_block and process_build_block are similar.
// One generates build rows based on a probe row; the other generates
// probe rows based on a build row. Their implementation approach and
// code structure are similar.

void process_probe_block(int64_t probe_block_pos, vectorized::Block& block,
                         const vectorized::Block& probe_block, size_t probe_side_columns,
                         const vectorized::Block& build_block, size_t build_side_columns) {
    auto dst_columns = block.mutate_columns();
    const size_t max_added_rows = build_block.rows();
    for (size_t i = 0; i < probe_side_columns; ++i) {
        const vectorized::ColumnWithTypeAndName& src_column = probe_block.get_by_position(i);
        if (!src_column.column->is_nullable() && dst_columns[i]->is_nullable()) {
            auto origin_sz = dst_columns[i]->size();
            assert_cast<vectorized::ColumnNullable*>(dst_columns[i].get())
                    ->get_nested_column_ptr()
                    ->insert_many_from(*src_column.column, probe_block_pos, max_added_rows);
            assert_cast<vectorized::ColumnNullable*>(dst_columns[i].get())
                    ->get_null_map_column()
                    .get_data()
                    .resize_fill(origin_sz + max_added_rows, 0);
        } else {
            // TODO: for cross join, maybe could insert one row, and wrap for a const column
            dst_columns[i]->insert_many_from(*src_column.column, probe_block_pos, max_added_rows);
        }
    }
    for (size_t i = 0; i < build_side_columns; ++i) {
        const vectorized::ColumnWithTypeAndName& src_column = build_block.get_by_position(i);
        if (!src_column.column->is_nullable() &&
            dst_columns[probe_side_columns + i]->is_nullable()) {
            auto origin_sz = dst_columns[probe_side_columns + i]->size();
            assert_cast<vectorized::ColumnNullable*>(dst_columns[probe_side_columns + i].get())
                    ->get_nested_column_ptr()
                    ->insert_range_from(*src_column.column.get(), 0, max_added_rows);
            assert_cast<vectorized::ColumnNullable*>(dst_columns[probe_side_columns + i].get())
                    ->get_null_map_column()
                    .get_data()
                    .resize_fill(origin_sz + max_added_rows, 0);
        } else {
            dst_columns[probe_side_columns + i]->insert_range_from(*src_column.column.get(), 0,
                                                                   max_added_rows);
        }
    }
    block.set_columns(std::move(dst_columns));
}

void process_build_block(int64_t build_block_pos, vectorized::Block& block,
                         const vectorized::Block& build_block, size_t build_side_columns,
                         const vectorized::Block& probe_block, size_t probe_side_columns) {
    auto dst_columns = block.mutate_columns();
    const size_t max_added_rows = probe_block.rows();
    for (size_t i = 0; i < probe_side_columns; ++i) {
        const vectorized::ColumnWithTypeAndName& src_column = probe_block.get_by_position(i);
        if (!src_column.column->is_nullable() && dst_columns[i]->is_nullable()) {
            auto origin_sz = dst_columns[i]->size();
            assert_cast<vectorized::ColumnNullable*>(dst_columns[i].get())
                    ->get_nested_column_ptr()
                    ->insert_range_from(*src_column.column.get(), 0, max_added_rows);
            assert_cast<vectorized::ColumnNullable*>(dst_columns[i].get())
                    ->get_null_map_column()
                    .get_data()
                    .resize_fill(origin_sz + max_added_rows, 0);
        } else {
            dst_columns[i]->insert_range_from(*src_column.column.get(), 0, max_added_rows);
        }
    }
    for (size_t i = 0; i < build_side_columns; ++i) {
        const vectorized::ColumnWithTypeAndName& src_column = build_block.get_by_position(i);
        if (!src_column.column->is_nullable() &&
            dst_columns[probe_side_columns + i]->is_nullable()) {
            auto origin_sz = dst_columns[probe_side_columns + i]->size();
            assert_cast<vectorized::ColumnNullable*>(dst_columns[probe_side_columns + i].get())
                    ->get_nested_column_ptr()
                    ->insert_many_from(*src_column.column, build_block_pos, max_added_rows);
            assert_cast<vectorized::ColumnNullable*>(dst_columns[probe_side_columns + i].get())
                    ->get_null_map_column()
                    .get_data()
                    .resize_fill(origin_sz + max_added_rows, 0);
        } else {
            dst_columns[probe_side_columns + i]->insert_many_from(*src_column.column,
                                                                  build_block_pos, max_added_rows);
        }
    }
    block.set_columns(std::move(dst_columns));
}

template <bool set_build_side_flag, bool set_probe_side_flag>
void NestedLoopJoinProbeLocalState::_generate_block_base_probe(RuntimeState* state,
                                                               vectorized::Block* probe_block) {
    auto& p = _parent->cast<NestedLoopJoinProbeOperatorX>();

    auto add_rows = [&]() -> size_t {
        auto& build_blocks = _shared_state->build_blocks;
        if (build_blocks.empty()) {
            return 0;
        }
        if (_current_build_pos == build_blocks.size()) {
            return build_blocks[0].rows();
        }
        return build_blocks[_current_build_pos].rows();
    };

    while (_join_block.rows() + add_rows() <= state->batch_size()) {
        while (_current_build_pos == _shared_state->build_blocks.size() ||
               _probe_block_pos == probe_block->rows()) {
            // if probe block is empty(), do not need disprocess the probe block rows
            if (_probe_block_pos < probe_block->rows()) {
                _probe_side_process_count++;
            }

            _reset_with_next_probe_row();
            if (_probe_block_pos < probe_block->rows()) {
                if constexpr (set_probe_side_flag) {
                    _probe_offset_stack.push(cast_set<uint16_t, size_t, false>(_join_block.rows()));
                }
            } else {
                if (_shared_state->probe_side_eos) {
                    _matched_rows_done = true;
                } else {
                    _need_more_input_data = true;
                }
                break;
            }
        }

        // Do not have probe row need to be disposed
        if (_matched_rows_done || _need_more_input_data) {
            break;
        }

        const auto& now_process_build_block = _shared_state->build_blocks[_current_build_pos++];
        if constexpr (set_build_side_flag) {
            _build_offset_stack.push(cast_set<uint16_t, size_t, false>(_join_block.rows()));
        }

        SCOPED_TIMER(_output_temp_blocks_timer);
        process_probe_block(_probe_block_pos, _join_block, *probe_block, p._num_probe_side_columns,
                            now_process_build_block, p._num_build_side_columns);
    }

    DCHECK_LE(_join_block.rows(), state->batch_size())
            << "join block rows:" << _join_block.rows()
            << ", state batch size:" << state->batch_size()
            << "probe_block rows:" << probe_block->rows()
            << " build blocks size:" << _shared_state->build_blocks.size();
}

// When the build side is small, generate data based on the build side.
// Currently a simple heuristic is used: check whether build_blocks.size() == 1
bool NestedLoopJoinProbeLocalState::use_generate_block_base_build() const {
    return _shared_state->build_blocks.size() == 1;
}

// for inner join only
// Generating data based on the build side follows the same logic
// as generating data based on the probe side.
// Only inner join calls this function, so both set_build_side_flag
// and set_probe_side_flag are false.
void NestedLoopJoinProbeLocalState::_generate_block_base_build(RuntimeState* state,
                                                               vectorized::Block* probe_block) {
    DCHECK(use_generate_block_base_build());
    auto& p = _parent->cast<NestedLoopJoinProbeOperatorX>();
    const auto& build_block = _shared_state->build_blocks[0];
    const size_t build_rows = build_block.rows();
    const auto probe_rows = static_cast<int>(probe_block->rows());

    // If the probe block is empty, return directly
    /// TODO: Reconsider this logic; it may need to be handled outside
    if (probe_rows == 0) {
        if (_shared_state->probe_side_eos) {
            _matched_rows_done = true;
        } else {
            _need_more_input_data = true;
        }
        return;
    }

    while (_join_block.rows() + probe_rows <= state->batch_size()) {
        // The current build row has processed the entire probe block; move to the next build row
        if (_probe_block_pos == probe_rows) {
            // Move to the next build row and reset the probe position
            _current_build_row_pos++;
            _probe_block_pos = 0;

            // All build rows have finished processing the current probe block
            if (_current_build_row_pos >= build_rows) {
                if (_shared_state->probe_side_eos) {
                    _matched_rows_done = true;
                } else {
                    _need_more_input_data = true;
                    // Reset build row position to prepare for the next probe block
                    _current_build_row_pos = 0;
                }
                break;
            }
        }

        // No data needs to be processed
        if (_matched_rows_done || _need_more_input_data) {
            break;
        }

        // Based on the current build row, add the entire probe block's data
        SCOPED_TIMER(_output_temp_blocks_timer);
        process_build_block(_current_build_row_pos, _join_block, build_block,
                            p._num_build_side_columns, *probe_block, p._num_probe_side_columns);

        // Mark the current probe block as processed; the next loop will move to the next build row
        _probe_block_pos = probe_rows;
    }

    DCHECK_LE(_join_block.rows(), state->batch_size())
            << "join block rows:" << _join_block.rows()
            << ", state batch size:" << state->batch_size()
            << "probe_block rows:" << probe_block->rows()
            << "build block rows:" << build_block.rows();
}

// for inner join only
Status NestedLoopJoinProbeLocalState::generate_inner_join_block_data(RuntimeState* state) {
    _probe_block_start_pos = _probe_block_pos;
    _probe_side_process_count = 0;
    DCHECK(!_need_more_input_data || !_matched_rows_done);
    auto& p = _parent->cast<NestedLoopJoinProbeOperatorX>();
    auto* probe_block = _child_block.get();

    if (!_matched_rows_done && !_need_more_input_data) {
        if (use_generate_block_base_build()) {
            _generate_block_base_build(state, probe_block);
        } else {
            _generate_block_base_probe<false, false>(state, probe_block);
            // Note: Here the condition is build_blocks.size() == 0; we won't discuss this in
            // _generate_block_base_build because use_generate_block_base_build requires size == 1
            if (_probe_side_process_count && p._is_mark_join &&
                _shared_state->build_blocks.empty()) {
                _append_probe_data_with_null(_join_block);
            }
        }
    }

    RETURN_IF_ERROR(
            (_do_filtering_and_update_visited_flags<false, false, false>(&_join_block, true)));
    _update_additional_flags(&_join_block);

    COUNTER_UPDATE(_probe_rows_counter, _join_block.rows());
    return Status::OK();
}

template <typename JoinOpType, bool set_build_side_flag, bool set_probe_side_flag>
Status NestedLoopJoinProbeLocalState::generate_other_join_block_data(RuntimeState* state,
                                                                     JoinOpType& join_op_variants) {
    constexpr bool ignore_null = JoinOpType::value == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN;
    _probe_block_start_pos = _probe_block_pos;
    _probe_side_process_count = 0;
    DCHECK(!_need_more_input_data || !_matched_rows_done);

    auto& p = _parent->cast<NestedLoopJoinProbeOperatorX>();
    auto* probe_block = _child_block.get();

    if (!_matched_rows_done && !_need_more_input_data) {
        // We should try to join rows if there still are some rows from probe side.
        // _probe_offset_stack and _build_offset_stack use u16 for storage
        // because on the FE side, it is guaranteed that the batch size will not exceed 65535 (the maximum value for u16).s
        _generate_block_base_probe<set_build_side_flag, set_probe_side_flag>(state, probe_block);
        {
            SCOPED_TIMER(_finish_probe_phase_timer);
            if constexpr (set_probe_side_flag) {
                RETURN_IF_ERROR(
                        (_do_filtering_and_update_visited_flags<set_build_side_flag,
                                                                set_probe_side_flag, ignore_null>(
                                &_join_block, !p._is_left_semi_anti)));
                _update_additional_flags(&_join_block);
                // If this join operation is left outer join or full outer join, when
                // `_left_side_process_count`, means all rows from build
                // side have been joined with _left_side_process_count, we should output current
                // probe row with null from build side.
                if (_probe_side_process_count) {
                    _finalize_current_phase<false, JoinOpType::value == TJoinOp::LEFT_SEMI_JOIN>(
                            _join_block, state->batch_size());
                }
            } else if (_probe_side_process_count && p._is_mark_join &&
                       _shared_state->build_blocks.empty()) {
                _append_probe_data_with_null(_join_block);
            }
        }
    }

    if constexpr (!set_probe_side_flag) {
        RETURN_IF_ERROR((_do_filtering_and_update_visited_flags<set_build_side_flag,
                                                                set_probe_side_flag, ignore_null>(
                &_join_block, !p._is_right_semi_anti)));
        _update_additional_flags(&_join_block);
    }

    if constexpr (set_build_side_flag) {
        if (_matched_rows_done &&
            _output_null_idx_build_side < _shared_state->build_blocks.size()) {
            _finalize_current_phase<true, JoinOpType::value == TJoinOp::RIGHT_SEMI_JOIN>(
                    _join_block, state->batch_size());
        }
    }
    COUNTER_UPDATE(_probe_rows_counter, _join_block.rows());
    return Status::OK();
}

template <bool BuildSide, bool IsSemi>
void NestedLoopJoinProbeLocalState::_finalize_current_phase(vectorized::Block& block,
                                                            size_t batch_size) {
    auto& p = _parent->cast<NestedLoopJoinProbeOperatorX>();
    auto dst_columns = block.mutate_columns();
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

            std::vector<uint32_t> selector(num_rows);
            size_t selector_idx = 0;
            for (uint32_t j = 0; j < num_rows; j++) {
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
        }
        _output_null_idx_build_side = i;
    } else {
        if (!p._is_mark_join) {
            auto new_size = column_size;
            DCHECK_LE(_probe_block_start_pos + _probe_side_process_count, _child_block->rows());
            for (int j = _probe_block_start_pos;
                 j < _probe_block_start_pos + _probe_side_process_count; ++j) {
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
            }
        } else {
            vectorized::ColumnFilterHelper mark_column(*dst_columns[dst_columns.size() - 1]);
            mark_column.reserve(mark_column.size() + _probe_side_process_count);
            DCHECK_LE(_probe_block_start_pos + _probe_side_process_count, _child_block->rows());
            for (int j = _probe_block_start_pos;
                 j < _probe_block_start_pos + _probe_side_process_count; ++j) {
                mark_column.insert_value(IsSemi == _cur_probe_row_visited_flags[j]);
            }
            for (size_t i = 0; i < p._num_probe_side_columns; ++i) {
                const vectorized::ColumnWithTypeAndName src_column =
                        _child_block->get_by_position(i);
                DCHECK(p._join_op != TJoinOp::FULL_OUTER_JOIN);
                dst_columns[i]->insert_range_from(*src_column.column, _probe_block_start_pos,
                                                  _probe_side_process_count);
            }
            for (size_t i = 0; i < p._num_build_side_columns; ++i) {
                dst_columns[p._num_probe_side_columns + i]->insert_many_defaults(
                        _probe_side_process_count);
            }
        }
    }
    block.set_columns(std::move(dst_columns));
}

void NestedLoopJoinProbeLocalState::_append_probe_data_with_null(vectorized::Block& block) const {
    auto& p = _parent->cast<NestedLoopJoinProbeOperatorX>();
    auto dst_columns = block.mutate_columns();
    DCHECK(p._is_mark_join);
    for (size_t i = 0; i < p._num_probe_side_columns; ++i) {
        const vectorized::ColumnWithTypeAndName& src_column = _child_block->get_by_position(i);
        if (!src_column.column->is_nullable() && dst_columns[i]->is_nullable()) {
            auto origin_sz = dst_columns[i]->size();
            DCHECK(p._join_op == TJoinOp::RIGHT_OUTER_JOIN ||
                   p._join_op == TJoinOp::FULL_OUTER_JOIN);
            assert_cast<vectorized::ColumnNullable*>(dst_columns[i].get())
                    ->get_nested_column_ptr()
                    ->insert_range_from(*src_column.column, _probe_block_start_pos,
                                        _probe_side_process_count);
            assert_cast<vectorized::ColumnNullable*>(dst_columns[i].get())
                    ->get_null_map_column()
                    .get_data()
                    .resize_fill(origin_sz + 1, 0);
        } else {
            dst_columns[i]->insert_range_from(*src_column.column, _probe_block_start_pos,
                                              _probe_side_process_count);
        }
    }
    for (size_t i = 0; i < p._num_build_side_columns; ++i) {
        dst_columns[p._num_probe_side_columns + i]->insert_many_defaults(_probe_side_process_count);
    }
    auto& mark_column = *dst_columns[dst_columns.size() - 1];
    vectorized::ColumnFilterHelper(mark_column)
            .resize_fill(mark_column.size() + _probe_side_process_count, 0);
    block.set_columns(std::move(dst_columns));
}

NestedLoopJoinProbeOperatorX::NestedLoopJoinProbeOperatorX(ObjectPool* pool, const TPlanNode& tnode,
                                                           int operator_id,
                                                           const DescriptorTbl& descs)
        : JoinProbeOperatorX<NestedLoopJoinProbeLocalState>(pool, tnode, operator_id, descs),
          _is_output_probe_side_only(tnode.nested_loop_join_node.__isset.is_output_left_side_only &&
                                     tnode.nested_loop_join_node.is_output_left_side_only),
          _old_version_flag(!tnode.__isset.nested_loop_join_node) {
    _keep_origin = _is_output_probe_side_only;
}

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
    _num_probe_side_columns = _child->row_desc().num_materialized_slots();
    _num_build_side_columns = _build_side_child->row_desc().num_materialized_slots();
    return vectorized::VExpr::open(_join_conjuncts, state);
}

bool NestedLoopJoinProbeOperatorX::need_more_input_data(RuntimeState* state) const {
    auto& local_state =
            state->get_local_state(operator_id())->cast<NestedLoopJoinProbeLocalState>();
    return local_state._need_more_input_data and !local_state._shared_state->probe_side_eos and
           local_state._join_block.rows() == 0;
}

Status NestedLoopJoinProbeOperatorX::push(doris::RuntimeState* state, vectorized::Block* block,
                                          bool eos) const {
    auto& local_state = get_local_state(state);
    COUNTER_UPDATE(local_state._probe_rows_counter, block->rows());
    COUNTER_SET(local_state._memory_used_counter, block->allocated_bytes());
    SCOPED_PEAK_MEM(&local_state.estimate_memory_usage());
    local_state._cur_probe_row_visited_flags.resize(block->rows());
    std::fill(local_state._cur_probe_row_visited_flags.begin(),
              local_state._cur_probe_row_visited_flags.end(), 0);
    local_state._probe_block_pos = 0;
    local_state._need_more_input_data = false;
    local_state._shared_state->probe_side_eos = eos;

    if (!_is_output_probe_side_only) {
        auto func = [&](auto&& join_op_variants, auto set_build_side_flag,
                        auto set_probe_side_flag) {
            using JoinOpType = std::decay_t<decltype(join_op_variants)>;

            if constexpr (JoinOpType::value == TJoinOp::INNER_JOIN ||
                          JoinOpType::value == TJoinOp::CROSS_JOIN) {
                DCHECK(!set_build_side_flag && !set_probe_side_flag)
                        << "Inner Join should not set visited flags but set_build_side_flag: "
                        << set_build_side_flag << ", set_probe_side_flag: " << set_probe_side_flag;
                return local_state.generate_inner_join_block_data(state);
            } else {
                return local_state.generate_other_join_block_data<JoinOpType, set_build_side_flag,
                                                                  set_probe_side_flag>(
                        state, join_op_variants);
            }
        };
        SCOPED_TIMER(local_state._loop_join_timer);
        RETURN_IF_ERROR(
                std::visit(func, local_state._shared_state->join_op_variants,
                           vectorized::make_bool_variant(_match_all_build || _is_right_semi_anti),
                           vectorized::make_bool_variant(_match_all_probe || _is_left_semi_anti)));
    }
    return Status::OK();
}

Status NestedLoopJoinProbeOperatorX::pull(RuntimeState* state, vectorized::Block* block,
                                          bool* eos) const {
    auto& local_state = get_local_state(state);
    if (_is_output_probe_side_only) {
        SCOPED_PEAK_MEM(&local_state._estimate_memory_usage);
        RETURN_IF_ERROR(local_state._build_output_block(local_state._child_block.get(), block));
        *eos = local_state._shared_state->probe_side_eos;
        local_state._need_more_input_data = !local_state._shared_state->probe_side_eos;
    } else {
        SCOPED_PEAK_MEM(&local_state._estimate_memory_usage);
        *eos = ((_match_all_build || _is_right_semi_anti)
                        ? local_state._output_null_idx_build_side ==
                                          local_state._shared_state->build_blocks.size() &&
                                  local_state._matched_rows_done
                        : local_state._matched_rows_done);

        size_t join_block_column_size = local_state._join_block.columns();
        {
            {
                SCOPED_TIMER(local_state._join_filter_timer);

                RETURN_IF_ERROR(local_state.filter_block(local_state._conjuncts,
                                                         &local_state._join_block,
                                                         local_state._join_block.columns()));
            }
            RETURN_IF_ERROR(local_state._build_output_block(&local_state._join_block, block));
        }
        local_state._join_block.clear_column_data(join_block_column_size);
        if (!(*eos) and !local_state._need_more_input_data) {
            auto func = [&](auto&& join_op_variants, auto set_build_side_flag,
                            auto set_probe_side_flag) {
                using JoinOpType = std::decay_t<decltype(join_op_variants)>;

                if constexpr (JoinOpType::value == TJoinOp::INNER_JOIN ||
                              JoinOpType::value == TJoinOp::CROSS_JOIN) {
                    DCHECK(!set_build_side_flag && !set_probe_side_flag)
                            << "Inner Join should not set visited flags but set_build_side_flag: "
                            << set_build_side_flag
                            << ", set_probe_side_flag: " << set_probe_side_flag;
                    return local_state.generate_inner_join_block_data(state);
                } else {
                    return local_state.generate_other_join_block_data<
                            std::decay_t<decltype(join_op_variants)>, set_build_side_flag,
                            set_probe_side_flag>(state, join_op_variants);
                }
            };
            SCOPED_TIMER(local_state._loop_join_timer);
            SCOPED_PEAK_MEM(&local_state._estimate_memory_usage);
            RETURN_IF_ERROR(std::visit(
                    func, local_state._shared_state->join_op_variants,
                    vectorized::make_bool_variant(_match_all_build || _is_right_semi_anti),
                    vectorized::make_bool_variant(_match_all_probe || _is_left_semi_anti)));
        }
    }

    local_state.reached_limit(block, eos);
    return Status::OK();
}

} // namespace doris::pipeline
