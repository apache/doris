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

#include "exec/operator/nested_loop_join_probe_operator.h"

#include <memory>

#include "common/cast_set.h"
#include "common/exception.h"
#include "core/block/block.h"
#include "core/column/column.h"
#include "core/column/column_const.h"
#include "core/column/column_filter_helper.h"
#include "core/column/column_nullable.h"
#include "exec/operator/operator.h"

namespace doris {
class RuntimeState;
} // namespace doris

namespace doris {
namespace {
constexpr int8_t MARK_FALSE = 0;
constexpr int8_t MARK_TRUE = 1;
constexpr int8_t MARK_NULL = -1;

ColumnPtr make_const_column_from_row(const ColumnWithTypeAndName& source, size_t row, size_t rows) {
    return ColumnConst::create(source.column->cut(row, 1), rows);
}

ColumnPtr align_eval_column_nullable(const ColumnWithTypeAndName& target, const ColumnPtr& column) {
    if (target.type->is_nullable() && !column->is_nullable()) {
        return make_nullable(column);
    }
    return column;
}

void append_many_from_column(MutableColumnPtr& dst_column, const IColumn& src_column, size_t row,
                             size_t rows) {
    if (!is_column_nullable(src_column) && is_column_nullable(*dst_column)) {
        if (src_column.is_nullable()) {
            auto full_src_column = src_column.convert_to_full_column_if_const();
            dst_column->insert_many_from(*full_src_column, row, rows);
            return;
        }
        const auto origin_size = dst_column->size();
        auto* nullable_column = assert_cast<ColumnNullable*>(dst_column.get());
        nullable_column->get_nested_column_ptr()->insert_many_from(src_column, row, rows);
        nullable_column->get_null_map_column().get_data().resize_fill(origin_size + rows, 0);
    } else {
        dst_column->insert_many_from(src_column, row, rows);
    }
}

void append_many_from_source(MutableColumnPtr& dst_column, const ColumnWithTypeAndName& src_column,
                             size_t row, size_t rows) {
    append_many_from_column(dst_column, *src_column.column, row, rows);
}

void append_range_from_column(MutableColumnPtr& dst_column, const IColumn& src_column, size_t start,
                              size_t length) {
    if (!is_column_nullable(src_column) && is_column_nullable(*dst_column)) {
        if (src_column.is_nullable()) {
            auto full_src_column = src_column.convert_to_full_column_if_const();
            dst_column->insert_range_from(*full_src_column, start, length);
            return;
        }
        const auto origin_size = dst_column->size();
        auto* nullable_column = assert_cast<ColumnNullable*>(dst_column.get());
        nullable_column->get_nested_column_ptr()->insert_range_from(src_column, start, length);
        nullable_column->get_null_map_column().get_data().resize_fill(origin_size + length, 0);
    } else {
        dst_column->insert_range_from(src_column, start, length);
    }
}

void append_indices_from_column(MutableColumnPtr& dst_column, const IColumn& src_column,
                                const uint32_t* indices_begin, const uint32_t* indices_end) {
    if (!is_column_nullable(src_column) && is_column_nullable(*dst_column)) {
        if (src_column.is_nullable()) {
            auto full_src_column = src_column.convert_to_full_column_if_const();
            dst_column->insert_indices_from(*full_src_column, indices_begin, indices_end);
            return;
        }
        const auto origin_size = dst_column->size();
        auto* nullable_column = assert_cast<ColumnNullable*>(dst_column.get());
        nullable_column->get_nested_column_ptr()->insert_indices_from(src_column, indices_begin,
                                                                      indices_end);
        nullable_column->get_null_map_column().get_data().resize_fill(
                origin_size + (indices_end - indices_begin), 0);
    } else {
        dst_column->insert_indices_from(src_column, indices_begin, indices_end);
    }
}

void append_filtered_from_source(MutableColumnPtr& dst_column,
                                 const ColumnWithTypeAndName& src_column,
                                 const IColumn::Filter& filter, size_t selected_rows) {
    if (selected_rows == 0) {
        return;
    }
    auto filtered_column = src_column.column->filter(filter, selected_rows);
    append_range_from_column(dst_column, *filtered_column, 0, selected_rows);
}

void append_mark_value(MutableColumnPtr& dst_column, int8_t mark_value) {
    auto* nullable_column = assert_cast<ColumnNullable*>(dst_column.get());
    auto& value_column = assert_cast<ColumnUInt8&>(nullable_column->get_nested_column());
    value_column.get_data().push_back(mark_value == MARK_TRUE);
    nullable_column->get_null_map_column().get_data().push_back(mark_value == MARK_NULL);
}
} // namespace

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
    _mark_join_conjuncts.resize(p._mark_join_conjuncts.size());
    for (size_t i = 0; i < _mark_join_conjuncts.size(); i++) {
        RETURN_IF_ERROR(p._mark_join_conjuncts[i]->clone(state, _mark_join_conjuncts[i]));
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

void NestedLoopJoinProbeLocalState::_update_additional_flags(Block* block) {
    auto& p = _parent->cast<NestedLoopJoinProbeOperatorX>();
    if (p._is_mark_join) {
        auto mark_column =
                IColumn::mutate(std::move(block->get_by_position(block->columns() - 1).column));
        if (mark_column->size() < block->rows()) {
            ColumnFilterHelper(*mark_column).resize_fill(block->rows(), 1);
        }
        block->replace_by_position(block->columns() - 1, std::move(mark_column));
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

void process_probe_block(int64_t probe_block_pos, Block& block, const Block& probe_block,
                         size_t probe_side_columns, const Block& build_block,
                         size_t build_side_columns) {
    auto dst_columns_guard = block.mutate_columns_scoped();
    auto& dst_columns = dst_columns_guard.mutable_columns();
    const size_t max_added_rows = build_block.rows();
    for (size_t i = 0; i < probe_side_columns; ++i) {
        const ColumnWithTypeAndName& src_column = probe_block.get_by_position(i);
        // TODO: for cross join, maybe could insert one row, and wrap for a const column
        append_many_from_source(dst_columns[i], src_column, probe_block_pos, max_added_rows);
    }
    for (size_t i = 0; i < build_side_columns; ++i) {
        const ColumnWithTypeAndName& src_column = build_block.get_by_position(i);
        append_range_from_column(dst_columns[probe_side_columns + i], *src_column.column, 0,
                                 max_added_rows);
    }
}

void process_build_block(int64_t build_block_pos, Block& block, const Block& build_block,
                         size_t build_side_columns, const Block& probe_block,
                         size_t probe_side_columns) {
    auto dst_columns_guard = block.mutate_columns_scoped();
    auto& dst_columns = dst_columns_guard.mutable_columns();
    const size_t max_added_rows = probe_block.rows();
    for (size_t i = 0; i < probe_side_columns; ++i) {
        const ColumnWithTypeAndName& src_column = probe_block.get_by_position(i);
        append_range_from_column(dst_columns[i], *src_column.column, 0, max_added_rows);
    }
    for (size_t i = 0; i < build_side_columns; ++i) {
        const ColumnWithTypeAndName& src_column = build_block.get_by_position(i);
        append_many_from_source(dst_columns[probe_side_columns + i], src_column, build_block_pos,
                                max_added_rows);
    }
}

void NestedLoopJoinProbeLocalState::_replace_lazy_placeholder_columns(size_t rows) {
    auto& p = _parent->cast<NestedLoopJoinProbeOperatorX>();
    for (size_t i = 0; i < _join_block.columns(); ++i) {
        if (p._materialize_column_ids.find(cast_set<int>(i)) != p._materialize_column_ids.end()) {
            continue;
        }
        const auto& column = _join_block.get_by_position(i);
        _join_block.replace_by_position(i,
                                        column.type->create_column_const_with_default_value(rows));
    }
}

Status NestedLoopJoinProbeLocalState::_append_lazy_rows(const IColumn::Filter& filter,
                                                        size_t selected_rows, bool fixed_side_probe,
                                                        int64_t fixed_side_pos,
                                                        const Block& probe_block,
                                                        const Block& build_block) {
    auto& p = _parent->cast<NestedLoopJoinProbeOperatorX>();
    const size_t old_rows = _join_block.rows();
    const size_t new_rows = old_rows + selected_rows;

    {
        auto dst_columns_guard = _join_block.mutate_columns_scoped();
        auto& dst_columns = dst_columns_guard.mutable_columns();
        for (int column_id : p._materialize_column_ids) {
            const auto column_idx = cast_set<size_t>(column_id);
            if (column_idx < p._num_probe_side_columns) {
                const auto& src_column = probe_block.get_by_position(column_idx);
                if (fixed_side_probe) {
                    append_many_from_source(dst_columns[column_idx], src_column, fixed_side_pos,
                                            selected_rows);
                } else {
                    append_filtered_from_source(dst_columns[column_idx], src_column, filter,
                                                selected_rows);
                }
            } else {
                const auto build_column_idx = column_idx - p._num_probe_side_columns;
                const auto& src_column = build_block.get_by_position(build_column_idx);
                if (fixed_side_probe) {
                    append_filtered_from_source(dst_columns[column_idx], src_column, filter,
                                                selected_rows);
                } else {
                    append_many_from_source(dst_columns[column_idx], src_column, fixed_side_pos,
                                            selected_rows);
                }
            }
        }
    }
    _replace_lazy_placeholder_columns(new_rows);
    DCHECK_EQ(_join_block.rows(), new_rows);
    return Status::OK();
}

Status NestedLoopJoinProbeLocalState::_append_lazy_probe_row_with_build_defaults(
        const Block& probe_block, int64_t probe_row_pos) {
    auto& p = _parent->cast<NestedLoopJoinProbeOperatorX>();
    const size_t new_rows = _join_block.rows() + 1;

    {
        auto dst_columns_guard = _join_block.mutate_columns_scoped();
        auto& dst_columns = dst_columns_guard.mutable_columns();
        for (int column_id : p._materialize_column_ids) {
            const auto column_idx = cast_set<size_t>(column_id);
            if (column_idx < p._num_probe_side_columns) {
                const auto& src_column = probe_block.get_by_position(column_idx);
                append_many_from_source(dst_columns[column_idx], src_column, probe_row_pos, 1);
            } else {
                dst_columns[column_idx]->insert_many_defaults(1);
            }
        }
    }
    _replace_lazy_placeholder_columns(new_rows);
    DCHECK_EQ(_join_block.rows(), new_rows);
    return Status::OK();
}

Status NestedLoopJoinProbeLocalState::_append_lazy_mark_probe_row_with_build_defaults(
        const Block& probe_block, int64_t probe_row_pos, int8_t mark_value) {
    auto& p = _parent->cast<NestedLoopJoinProbeOperatorX>();
    const size_t mark_column_id = p._num_probe_side_columns + p._num_build_side_columns;
    const size_t new_rows = _join_block.rows() + 1;

    {
        auto dst_columns_guard = _join_block.mutate_columns_scoped();
        auto& dst_columns = dst_columns_guard.mutable_columns();
        for (int column_id : p._materialize_column_ids) {
            const auto column_idx = cast_set<size_t>(column_id);
            if (column_idx < p._num_probe_side_columns) {
                const auto& src_column = probe_block.get_by_position(column_idx);
                append_many_from_source(dst_columns[column_idx], src_column, probe_row_pos, 1);
            } else if (column_idx == mark_column_id) {
                append_mark_value(dst_columns[column_idx], mark_value);
            } else {
                dst_columns[column_idx]->insert_many_defaults(1);
            }
        }
    }
    _replace_lazy_placeholder_columns(new_rows);
    DCHECK_EQ(_join_block.rows(), new_rows);
    return Status::OK();
}

Status NestedLoopJoinProbeLocalState::_append_lazy_build_rows_with_probe_defaults(
        const Block& build_block, const IColumn::Filter& filter, size_t selected_rows) {
    auto& p = _parent->cast<NestedLoopJoinProbeOperatorX>();
    const size_t new_rows = _join_block.rows() + selected_rows;

    {
        auto dst_columns_guard = _join_block.mutate_columns_scoped();
        auto& dst_columns = dst_columns_guard.mutable_columns();
        for (int column_id : p._materialize_column_ids) {
            const auto column_idx = cast_set<size_t>(column_id);
            if (column_idx < p._num_probe_side_columns) {
                dst_columns[column_idx]->insert_many_defaults(selected_rows);
            } else {
                const auto build_column_idx = column_idx - p._num_probe_side_columns;
                const auto& src_column = build_block.get_by_position(build_column_idx);
                append_filtered_from_source(dst_columns[column_idx], src_column, filter,
                                            selected_rows);
            }
        }
    }
    _replace_lazy_placeholder_columns(new_rows);
    DCHECK_EQ(_join_block.rows(), new_rows);
    return Status::OK();
}

Status NestedLoopJoinProbeLocalState::_finalize_lazy_probe_row(RuntimeState* state,
                                                               const Block& probe_block,
                                                               int64_t probe_row_pos,
                                                               bool* consumed) {
    *consumed = true;
    auto& p = _parent->cast<NestedLoopJoinProbeOperatorX>();
    if ((!p._enable_lazy_probe_finalize && !p._enable_lazy_mark_finalize) || probe_row_pos < 0 ||
        cast_set<size_t>(probe_row_pos) >= probe_block.rows()) {
        return Status::OK();
    }

    if (p._enable_lazy_mark_finalize) {
        if (_join_block.rows() >= state->batch_size()) {
            *consumed = false;
            return Status::OK();
        }
        int8_t mark_value = _cur_probe_row_mark_flags[probe_row_pos];
        if (p._join_op == TJoinOp::LEFT_ANTI_JOIN && mark_value != MARK_NULL) {
            mark_value = mark_value == MARK_TRUE ? MARK_FALSE : MARK_TRUE;
        }
        RETURN_IF_ERROR(_append_lazy_mark_probe_row_with_build_defaults(probe_block, probe_row_pos,
                                                                        mark_value));
        return Status::OK();
    }

    const bool matched = _cur_probe_row_visited_flags[probe_row_pos];
    bool should_output = false;
    if (p._join_op == TJoinOp::LEFT_OUTER_JOIN || p._join_op == TJoinOp::FULL_OUTER_JOIN) {
        should_output = !matched;
    } else if (p._join_op == TJoinOp::LEFT_SEMI_JOIN) {
        should_output = matched;
    } else if (p._join_op == TJoinOp::LEFT_ANTI_JOIN ||
               p._join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
        should_output = !matched;
    }
    if (!should_output) {
        return Status::OK();
    }
    if (_join_block.rows() >= state->batch_size()) {
        *consumed = false;
        return Status::OK();
    }
    RETURN_IF_ERROR(_append_lazy_probe_row_with_build_defaults(probe_block, probe_row_pos));
    return Status::OK();
}

Status NestedLoopJoinProbeLocalState::_finalize_lazy_build_side(RuntimeState* state) {
    auto& p = _parent->cast<NestedLoopJoinProbeOperatorX>();
    if (!p._enable_lazy_build_finalize) {
        return Status::OK();
    }

    while (_join_block.rows() < state->batch_size() &&
           _output_null_idx_build_side < _shared_state->build_blocks.size()) {
        const auto& build_block = _shared_state->build_blocks[_output_null_idx_build_side];
        const auto* __restrict visited_flags =
                assert_cast<ColumnUInt8*>(
                        _shared_state->build_side_visited_flags[_output_null_idx_build_side].get())
                        ->get_data()
                        .data();
        const size_t rows = build_block.rows();
        const size_t output_capacity = state->batch_size() - _join_block.rows();

        IColumn::Filter filter(rows, 0);
        auto* __restrict filter_data = filter.data();
        size_t selected_rows = 0;
        size_t row_idx = _output_null_row_idx_build_side;
        for (; row_idx < rows && selected_rows < output_capacity; ++row_idx) {
            const bool matched = visited_flags[row_idx] != 0;
            const bool selected = p._join_op == TJoinOp::RIGHT_SEMI_JOIN ? matched : !matched;
            filter_data[row_idx] = selected;
            selected_rows += selected;
        }

        if (selected_rows > 0) {
            RETURN_IF_ERROR(_append_lazy_build_rows_with_probe_defaults(build_block, filter,
                                                                        selected_rows));
        }
        if (row_idx == rows) {
            ++_output_null_idx_build_side;
            _output_null_row_idx_build_side = 0;
        } else {
            _output_null_row_idx_build_side = row_idx;
        }
    }
    return Status::OK();
}

Status NestedLoopJoinProbeLocalState::_advance_lazy_probe_row(RuntimeState* state,
                                                              const Block& probe_block) {
    if (_current_build_pos == _shared_state->build_blocks.size() &&
        _probe_block_pos < probe_block.rows()) {
        bool consumed = true;
        RETURN_IF_ERROR(_finalize_lazy_probe_row(state, probe_block, _probe_block_pos, &consumed));
        if (!consumed) {
            return Status::OK();
        }
    }

    if (_probe_block_pos < probe_block.rows()) {
        _probe_side_process_count++;
    }

    _reset_with_next_probe_row();
    if (_probe_block_pos < probe_block.rows()) {
        return Status::OK();
    }

    if (_shared_state->probe_side_eos) {
        _matched_rows_done = true;
    } else {
        _need_more_input_data = true;
    }
    return Status::OK();
}

void NestedLoopJoinProbeLocalState::_append_lazy_probe_eval_columns(
        ColumnsWithTypeAndName& eval_columns, const Block& probe_block, bool fixed_side_probe,
        int64_t fixed_side_pos, size_t rows) {
    auto& p = _parent->cast<NestedLoopJoinProbeOperatorX>();
    for (size_t i = 0; i < p._num_probe_side_columns; ++i) {
        const auto& block_column = _join_block.get_by_position(i);
        const auto& src_column = probe_block.get_by_position(i);
        if (p._lazy_eval_column_ids.find(cast_set<int>(i)) == p._lazy_eval_column_ids.end()) {
            eval_columns.emplace_back(
                    block_column.type->create_column_const_with_default_value(rows),
                    block_column.type, block_column.name);
        } else if (fixed_side_probe) {
            eval_columns.emplace_back(
                    align_eval_column_nullable(
                            block_column,
                            make_const_column_from_row(src_column, fixed_side_pos, rows)),
                    block_column.type, block_column.name);
        } else {
            eval_columns.emplace_back(align_eval_column_nullable(block_column, src_column.column),
                                      block_column.type, block_column.name);
        }
    }
}

void NestedLoopJoinProbeLocalState::_append_lazy_build_eval_columns(
        ColumnsWithTypeAndName& eval_columns, const Block& build_block, bool fixed_side_probe,
        int64_t fixed_side_pos, size_t rows) {
    auto& p = _parent->cast<NestedLoopJoinProbeOperatorX>();
    for (size_t i = 0; i < p._num_build_side_columns; ++i) {
        const auto column_idx = p._num_probe_side_columns + i;
        const auto& block_column = _join_block.get_by_position(column_idx);
        const auto& src_column = build_block.get_by_position(i);
        if (p._lazy_eval_column_ids.find(cast_set<int>(column_idx)) ==
            p._lazy_eval_column_ids.end()) {
            eval_columns.emplace_back(
                    block_column.type->create_column_const_with_default_value(rows),
                    block_column.type, block_column.name);
        } else if (fixed_side_probe) {
            eval_columns.emplace_back(align_eval_column_nullable(block_column, src_column.column),
                                      block_column.type, block_column.name);
        } else {
            eval_columns.emplace_back(
                    align_eval_column_nullable(
                            block_column,
                            make_const_column_from_row(src_column, fixed_side_pos, rows)),
                    block_column.type, block_column.name);
        }
    }
}

bool NestedLoopJoinProbeLocalState::_should_delay_lazy_probe_build_block(size_t candidate_rows,
                                                                         size_t batch_size) const {
    return _lazy_should_output_matched_rows() && _join_block.rows() + candidate_rows > batch_size;
}

bool NestedLoopJoinProbeLocalState::_lazy_should_output_matched_rows() const {
    auto& p = _parent->cast<NestedLoopJoinProbeOperatorX>();
    return p._join_op == TJoinOp::INNER_JOIN || p._join_op == TJoinOp::CROSS_JOIN ||
           p._join_op == TJoinOp::LEFT_OUTER_JOIN || p._join_op == TJoinOp::RIGHT_OUTER_JOIN ||
           p._join_op == TJoinOp::FULL_OUTER_JOIN;
}

void NestedLoopJoinProbeLocalState::_mark_lazy_build_rows_visited(size_t build_block_idx,
                                                                  const IColumn::Filter& filter) {
    auto& p = _parent->cast<NestedLoopJoinProbeOperatorX>();
    if (!p._enable_lazy_build_finalize) {
        return;
    }

    auto& build_side_flag = assert_cast<ColumnUInt8*>(
                                    _shared_state->build_side_visited_flags[build_block_idx].get())
                                    ->get_data();
    auto* __restrict build_side_flag_data = build_side_flag.data();
    const auto* __restrict filter_data = filter.data();
    DCHECK_EQ(build_side_flag.size(), filter.size());
    for (size_t i = 0; i < filter.size(); ++i) {
        build_side_flag_data[i] |= filter_data[i];
    }
}

void NestedLoopJoinProbeLocalState::_update_lazy_mark_join_state(
        const IColumn::Filter& mark_filter, const ColumnUInt8& mark_null_map,
        const IColumn::Filter& other_filter) {
    auto& mark_state = _cur_probe_row_mark_flags[_probe_block_pos];
    DCHECK_EQ(mark_filter.size(), mark_null_map.size());
    DCHECK_EQ(mark_filter.size(), other_filter.size());
    if (mark_state == MARK_TRUE) {
        return;
    }

    const auto* __restrict mark_filter_data = mark_filter.data();
    const auto* __restrict mark_null_data = mark_null_map.get_data().data();
    const auto* __restrict other_filter_data = other_filter.data();

    for (size_t i = 0; i < mark_filter.size(); ++i) {
        if (!other_filter_data[i]) {
            continue;
        }
        if (mark_null_data[i]) {
            mark_state = MARK_NULL;
        } else if (mark_filter_data[i]) {
            mark_state = MARK_TRUE;
            return;
        }
    }
}

Status NestedLoopJoinProbeLocalState::_process_lazy_probe_build_block(Block* probe_block,
                                                                      const Block& build_block,
                                                                      size_t build_block_idx,
                                                                      bool ignore_null) {
    auto& p = _parent->cast<NestedLoopJoinProbeOperatorX>();
    // A TRUE mark is terminal for lazy MARK LEFT SEMI/ANTI joins.
    if (p._enable_lazy_mark_finalize && _cur_probe_row_mark_flags[_probe_block_pos] == MARK_TRUE) {
        return Status::OK();
    }

    const size_t candidate_rows = build_block.rows();
    if (candidate_rows == 0) {
        return Status::OK();
    }

    ColumnsWithTypeAndName eval_columns;
    eval_columns.reserve(_join_block.columns());
    _append_lazy_probe_eval_columns(eval_columns, *probe_block, true, _probe_block_pos,
                                    candidate_rows);
    _append_lazy_build_eval_columns(eval_columns, build_block, true, 0, candidate_rows);
    Block eval_block(std::move(eval_columns));

    IColumn::Filter filter(candidate_rows, 1);
    bool can_filter_all = false;
    {
        SCOPED_TIMER(_join_conjuncts_evaluation_timer);
        RETURN_IF_ERROR(VExprContext::execute_conjuncts(_join_conjuncts, nullptr, ignore_null,
                                                        &eval_block, &filter, &can_filter_all));
    }
    if (can_filter_all) {
        return Status::OK();
    }

    const size_t selected_rows =
            candidate_rows -
            simd::count_zero_num(reinterpret_cast<int8_t*>(filter.data()), candidate_rows);
    DCHECK_GT(selected_rows, 0);

    if (p._enable_lazy_mark_finalize) {
        if (_mark_join_conjuncts.empty()) {
            _cur_probe_row_mark_flags[_probe_block_pos] = MARK_TRUE;
        } else {
            IColumn::Filter mark_filter(candidate_rows, 1);
            auto mark_null_map = ColumnUInt8::create(candidate_rows, 0);
            RETURN_IF_ERROR(VExprContext::execute_conjuncts(_mark_join_conjuncts, &eval_block,
                                                            *mark_null_map, mark_filter));
            _update_lazy_mark_join_state(mark_filter, *mark_null_map, filter);
        }
    } else if (p._enable_lazy_probe_finalize) {
        _cur_probe_row_visited_flags[_probe_block_pos] = true;
    }
    _mark_lazy_build_rows_visited(build_block_idx, filter);
    if (_lazy_should_output_matched_rows()) {
        RETURN_IF_ERROR(_append_lazy_rows(filter, selected_rows, true, _probe_block_pos,
                                          *probe_block, build_block));
    }
    return Status::OK();
}

Status NestedLoopJoinProbeLocalState::_generate_lazy_block_base_probe(RuntimeState* state,
                                                                      Block* probe_block,
                                                                      bool ignore_null) {
    auto& p = _parent->cast<NestedLoopJoinProbeOperatorX>();
    while (_join_block.rows() < state->batch_size()) {
        while (_current_build_pos == _shared_state->build_blocks.size() ||
               _probe_block_pos == probe_block->rows()) {
            RETURN_IF_ERROR(_advance_lazy_probe_row(state, *probe_block));
            if (_join_block.rows() >= state->batch_size()) {
                break;
            }
            if (_probe_block_pos >= probe_block->rows()) {
                break;
            }
        }

        if (_join_block.rows() >= state->batch_size() || _matched_rows_done ||
            _need_more_input_data) {
            break;
        }

        const size_t build_block_idx = _current_build_pos++;
        const auto& build_block = _shared_state->build_blocks[build_block_idx];
        if (_should_delay_lazy_probe_build_block(build_block.rows(), state->batch_size())) {
            --_current_build_pos;
            break;
        }
        RETURN_IF_ERROR(_process_lazy_probe_build_block(probe_block, build_block, build_block_idx,
                                                        ignore_null));
        if (p._enable_lazy_mark_finalize &&
            _cur_probe_row_mark_flags[_probe_block_pos] == MARK_TRUE) {
            _current_build_pos = _shared_state->build_blocks.size();
        }
    }
    return Status::OK();
}

Status NestedLoopJoinProbeLocalState::_generate_lazy_block_base_build(RuntimeState* state,
                                                                      Block* probe_block) {
    DCHECK(use_generate_block_base_build());
    const auto& build_block = _shared_state->build_blocks[0];
    const size_t build_rows = build_block.rows();
    const auto probe_rows = static_cast<int>(probe_block->rows());

    if (probe_rows == 0) {
        if (_shared_state->probe_side_eos) {
            _matched_rows_done = true;
        } else {
            _need_more_input_data = true;
        }
        return Status::OK();
    }

    size_t processed_rows = 0;
    while (processed_rows + probe_rows <= state->batch_size()) {
        if (_probe_block_pos == probe_rows) {
            _current_build_row_pos++;
            _probe_block_pos = 0;

            if (_current_build_row_pos >= build_rows) {
                if (_shared_state->probe_side_eos) {
                    _matched_rows_done = true;
                } else {
                    _need_more_input_data = true;
                    _current_build_row_pos = 0;
                }
                break;
            }
        }

        if (_matched_rows_done || _need_more_input_data) {
            break;
        }

        processed_rows += probe_rows;
        ColumnsWithTypeAndName eval_columns;
        eval_columns.reserve(_join_block.columns());
        _append_lazy_probe_eval_columns(eval_columns, *probe_block, false, 0, probe_rows);
        _append_lazy_build_eval_columns(eval_columns, build_block, false, _current_build_row_pos,
                                        probe_rows);
        Block eval_block(std::move(eval_columns));

        IColumn::Filter filter(probe_rows, 1);
        bool can_filter_all = false;
        {
            SCOPED_TIMER(_join_conjuncts_evaluation_timer);
            RETURN_IF_ERROR(VExprContext::execute_conjuncts(_join_conjuncts, nullptr, false,
                                                            &eval_block, &filter, &can_filter_all));
        }
        if (!can_filter_all) {
            const size_t selected_rows =
                    probe_rows -
                    simd::count_zero_num(reinterpret_cast<int8_t*>(filter.data()), probe_rows);
            DCHECK_GT(selected_rows, 0);
            RETURN_IF_ERROR(_append_lazy_rows(filter, selected_rows, false, _current_build_row_pos,
                                              *probe_block, build_block));
        }
        _probe_block_pos = probe_rows;
    }
    return Status::OK();
}

template <bool set_build_side_flag, bool set_probe_side_flag>
void NestedLoopJoinProbeLocalState::_generate_block_base_probe(RuntimeState* state,
                                                               Block* probe_block) {
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
                                                               Block* probe_block) {
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

    if (p._enable_lazy_materialize) {
        if (!_matched_rows_done && !_need_more_input_data) {
            if (use_generate_block_base_build()) {
                RETURN_IF_ERROR(_generate_lazy_block_base_build(state, probe_block));
            } else {
                RETURN_IF_ERROR(_generate_lazy_block_base_probe(state, probe_block, false));
            }
        }
        return Status::OK();
    }

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

    if (p._enable_lazy_materialize) {
        if (!_matched_rows_done && !_need_more_input_data) {
            RETURN_IF_ERROR(_generate_lazy_block_base_probe(state, probe_block, ignore_null));
        }
        if (_matched_rows_done) {
            RETURN_IF_ERROR(_finalize_lazy_build_side(state));
        }
        return Status::OK();
    }

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
    return Status::OK();
}

template <bool BuildSide, bool IsSemi>
// NOLINTNEXTLINE(readability-function-size,readability-function-cognitive-complexity): existing finalization handles multiple join variants.
void NestedLoopJoinProbeLocalState::_finalize_current_phase(Block& block, size_t batch_size) {
    auto& p = _parent->cast<NestedLoopJoinProbeOperatorX>();
    auto dst_columns_guard = block.mutate_columns_scoped();
    auto& dst_columns = dst_columns_guard.mutable_columns();
    DCHECK_GT(dst_columns.size(), 0);
    auto column_size = dst_columns[0]->size();
    if constexpr (BuildSide) {
        DCHECK(!p._is_mark_join);
        auto build_block_sz = _shared_state->build_blocks.size();
        size_t i = _output_null_idx_build_side;
        for (; i < build_block_sz && column_size < batch_size; i++) {
            const auto& cur_block = _shared_state->build_blocks[i];
            const auto* __restrict cur_visited_flags =
                    assert_cast<ColumnUInt8*>(_shared_state->build_side_visited_flags[i].get())
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
                append_indices_from_column(dst_columns[p._num_probe_side_columns + j],
                                           *src_column.column, selector.data(),
                                           selector.data() + selector_idx);
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
                        const ColumnWithTypeAndName src_column = _child_block->get_by_position(i);
                        append_many_from_source(dst_columns[i], src_column, j, 1);
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
            ColumnFilterHelper mark_column(*dst_columns[dst_columns.size() - 1]);
            mark_column.reserve(mark_column.size() + _probe_side_process_count);
            DCHECK_LE(_probe_block_start_pos + _probe_side_process_count, _child_block->rows());
            for (int j = _probe_block_start_pos;
                 j < _probe_block_start_pos + _probe_side_process_count; ++j) {
                mark_column.insert_value(IsSemi == _cur_probe_row_visited_flags[j]);
            }
            for (size_t i = 0; i < p._num_probe_side_columns; ++i) {
                const ColumnWithTypeAndName src_column = _child_block->get_by_position(i);
                DCHECK(p._join_op != TJoinOp::FULL_OUTER_JOIN);
                append_range_from_column(dst_columns[i], *src_column.column, _probe_block_start_pos,
                                         _probe_side_process_count);
            }
            for (size_t i = 0; i < p._num_build_side_columns; ++i) {
                dst_columns[p._num_probe_side_columns + i]->insert_many_defaults(
                        _probe_side_process_count);
            }
        }
    }
}

void NestedLoopJoinProbeLocalState::_append_probe_data_with_null(Block& block) const {
    auto& p = _parent->cast<NestedLoopJoinProbeOperatorX>();
    auto dst_columns_guard = block.mutate_columns_scoped();
    auto& dst_columns = dst_columns_guard.mutable_columns();
    DCHECK(p._is_mark_join);
    for (size_t i = 0; i < p._num_probe_side_columns; ++i) {
        const ColumnWithTypeAndName& src_column = _child_block->get_by_position(i);
        append_range_from_column(dst_columns[i], *src_column.column, _probe_block_start_pos,
                                 _probe_side_process_count);
    }
    for (size_t i = 0; i < p._num_build_side_columns; ++i) {
        dst_columns[p._num_probe_side_columns + i]->insert_many_defaults(_probe_side_process_count);
    }
    auto& mark_column = *dst_columns[dst_columns.size() - 1];
    ColumnFilterHelper(mark_column).resize_fill(mark_column.size() + _probe_side_process_count, 0);
}

NestedLoopJoinProbeOperatorX::NestedLoopJoinProbeOperatorX(ObjectPool* pool, const TPlanNode& tnode,
                                                           int operator_id,
                                                           const DescriptorTbl& descs)
        : JoinProbeOperatorX<NestedLoopJoinProbeLocalState>(pool, tnode, operator_id, descs),
          _has_materialized_slot_ids(tnode.__isset.nested_loop_join_node &&
                                     tnode.nested_loop_join_node.__isset.materialized_slot_ids),
          _materialized_slot_ids(_has_materialized_slot_ids
                                         ? tnode.nested_loop_join_node.materialized_slot_ids
                                         : std::vector<SlotId> {}) {}

Status NestedLoopJoinProbeOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(JoinProbeOperatorX<NestedLoopJoinProbeLocalState>::init(tnode, state));

    if (tnode.nested_loop_join_node.__isset.join_conjuncts &&
        !tnode.nested_loop_join_node.join_conjuncts.empty()) {
        RETURN_IF_ERROR(VExpr::create_expr_trees(tnode.nested_loop_join_node.join_conjuncts,
                                                 _join_conjuncts));
    } else if (tnode.nested_loop_join_node.__isset.vjoin_conjunct) {
        VExprContextSPtr context;
        RETURN_IF_ERROR(
                VExpr::create_expr_tree(tnode.nested_loop_join_node.vjoin_conjunct, context));
        _join_conjuncts.emplace_back(context);
    }
    if (tnode.nested_loop_join_node.__isset.mark_join_conjuncts &&
        !tnode.nested_loop_join_node.mark_join_conjuncts.empty()) {
        RETURN_IF_ERROR(VExpr::create_expr_trees(tnode.nested_loop_join_node.mark_join_conjuncts,
                                                 _mark_join_conjuncts));
        DORIS_CHECK(_is_mark_join);
    }

    return Status::OK();
}

Status NestedLoopJoinProbeOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(JoinProbeOperatorX<NestedLoopJoinProbeLocalState>::prepare(state));
    for (auto& conjunct : _join_conjuncts) {
        RETURN_IF_ERROR(conjunct->prepare(state, *_intermediate_row_desc));
    }
    for (auto& conjunct : _mark_join_conjuncts) {
        RETURN_IF_ERROR(conjunct->prepare(state, *_intermediate_row_desc));
    }
    _num_probe_side_columns = _child->row_desc().num_materialized_slots();
    _num_build_side_columns = _build_side_child->row_desc().num_materialized_slots();
    for (const auto& conjunct : _join_conjuncts) {
        conjunct->root()->collect_slot_column_ids(_lazy_eval_column_ids);
    }
    for (const auto& conjunct : _mark_join_conjuncts) {
        conjunct->root()->collect_slot_column_ids(_lazy_eval_column_ids);
    }
    if (_has_materialized_slot_ids) {
        for (const auto slot_id : _materialized_slot_ids) {
            const int column_id = intermediate_row_desc().get_column_id(slot_id);
            DORIS_CHECK(column_id >= 0);
            _materialize_column_ids.insert(column_id);
        }
    }
    _enable_lazy_mark_finalize = _is_mark_join && (_join_op == TJoinOp::LEFT_SEMI_JOIN ||
                                                   _join_op == TJoinOp::LEFT_ANTI_JOIN);
    if (_enable_lazy_mark_finalize) {
        _materialize_column_ids.insert(
                cast_set<int>(_num_probe_side_columns + _num_build_side_columns));
    }
    _enable_lazy_probe_finalize =
            _join_op == TJoinOp::LEFT_OUTER_JOIN || _join_op == TJoinOp::LEFT_SEMI_JOIN ||
            _join_op == TJoinOp::LEFT_ANTI_JOIN || _join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN ||
            _join_op == TJoinOp::FULL_OUTER_JOIN;
    _enable_lazy_build_finalize =
            _join_op == TJoinOp::RIGHT_OUTER_JOIN || _join_op == TJoinOp::RIGHT_SEMI_JOIN ||
            _join_op == TJoinOp::RIGHT_ANTI_JOIN || _join_op == TJoinOp::FULL_OUTER_JOIN;
    bool supported_lazy_join = _join_op == TJoinOp::INNER_JOIN || _join_op == TJoinOp::CROSS_JOIN ||
                               _enable_lazy_probe_finalize || _enable_lazy_build_finalize ||
                               _enable_lazy_mark_finalize;
    _enable_lazy_materialize = _has_materialized_slot_ids &&
                               (!_is_mark_join || _enable_lazy_mark_finalize) &&
                               supported_lazy_join && !projections().empty() &&
                               &projections_row_desc() == &intermediate_row_desc();
    RETURN_IF_ERROR(VExpr::open(_join_conjuncts, state));
    return VExpr::open(_mark_join_conjuncts, state);
}

bool NestedLoopJoinProbeOperatorX::need_more_input_data(RuntimeState* state) const {
    auto& local_state =
            state->get_local_state(operator_id())->cast<NestedLoopJoinProbeLocalState>();
    return local_state._need_more_input_data and !local_state._shared_state->probe_side_eos and
           local_state._join_block.rows() == 0;
}

Status NestedLoopJoinProbeOperatorX::push(doris::RuntimeState* state, Block* block,
                                          bool eos) const {
    auto& local_state = get_local_state(state);
    COUNTER_UPDATE(local_state._probe_rows_counter, block->rows());
    COUNTER_SET(local_state._memory_used_counter, block->allocated_bytes());
    SCOPED_PEAK_MEM(&local_state.estimate_memory_usage());
    local_state._cur_probe_row_visited_flags.resize(block->rows());
    std::fill(local_state._cur_probe_row_visited_flags.begin(),
              local_state._cur_probe_row_visited_flags.end(), 0);
    if (_enable_lazy_mark_finalize) {
        local_state._cur_probe_row_mark_flags.resize(block->rows());
        std::fill(local_state._cur_probe_row_mark_flags.begin(),
                  local_state._cur_probe_row_mark_flags.end(), MARK_FALSE);
    }
    local_state._probe_block_pos = 0;
    local_state._need_more_input_data = false;
    local_state._shared_state->probe_side_eos = eos;

    auto func = [&](auto&& join_op_variants, auto set_build_side_flag, auto set_probe_side_flag) {
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
    RETURN_IF_ERROR(std::visit(func, local_state._shared_state->join_op_variants,
                               make_bool_variant(_match_all_build || _is_right_semi_anti),
                               make_bool_variant(_match_all_probe || _is_left_semi_anti)));
    return Status::OK();
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity): existing pull dispatch handles all NLJ variants.
Status NestedLoopJoinProbeOperatorX::pull(RuntimeState* state, Block* block, bool* eos) const {
    auto& local_state = get_local_state(state);
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

            RETURN_IF_ERROR(
                    local_state.filter_block(local_state._conjuncts, &local_state._join_block));
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
                        << set_build_side_flag << ", set_probe_side_flag: " << set_probe_side_flag;
                return local_state.generate_inner_join_block_data(state);
            } else {
                return local_state
                        .generate_other_join_block_data<std::decay_t<decltype(join_op_variants)>,
                                                        set_build_side_flag, set_probe_side_flag>(
                                state, join_op_variants);
            }
        };
        SCOPED_TIMER(local_state._loop_join_timer);
        SCOPED_PEAK_MEM(&local_state._estimate_memory_usage);
        RETURN_IF_ERROR(std::visit(func, local_state._shared_state->join_op_variants,
                                   make_bool_variant(_match_all_build || _is_right_semi_anti),
                                   make_bool_variant(_match_all_probe || _is_left_semi_anti)));
    }

    local_state.reached_limit(block, eos);
    return Status::OK();
}

} // namespace doris
