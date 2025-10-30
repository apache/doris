
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

#include "analytic_sink_operator.h"

#include <glog/logging.h>

#include <cstddef>
#include <cstdint>
#include <string>

#include "pipeline/exec/operator.h"
#include "runtime/runtime_state.h"
#include "vec/exprs/vectorized_agg_fn.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"

Status AnalyticSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(PipelineXSinkLocalState<AnalyticSharedState>::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _evaluation_timer = ADD_TIMER(custom_profile(), "EvaluationTime");
    _compute_agg_data_timer = ADD_TIMER(custom_profile(), "ComputeAggDataTime");
    _compute_partition_by_timer = ADD_TIMER(custom_profile(), "ComputePartitionByTime");
    _compute_order_by_timer = ADD_TIMER(custom_profile(), "ComputeOrderByTime");
    _compute_range_between_function_timer = ADD_TIMER(custom_profile(), "ComputeRangeBetweenTime");
    _partition_search_timer = ADD_TIMER(custom_profile(), "PartitionSearchTime");
    _order_search_timer = ADD_TIMER(custom_profile(), "OrderSearchTime");
    _remove_rows_timer = ADD_TIMER(custom_profile(), "RemoveRowsTime");
    _remove_rows = ADD_COUNTER(custom_profile(), "RemoveRows", TUnit::UNIT);
    _remove_count = ADD_COUNTER(custom_profile(), "RemoveCount", TUnit::UNIT);
    _blocks_memory_usage =
            common_profile()->AddHighWaterMarkCounter("Blocks", TUnit::BYTES, "MemoryUsage", 1);
    auto& p = _parent->cast<AnalyticSinkOperatorX>();
    if (!p._has_window || (!p._has_window_start && !p._has_window_end)) {
        // haven't set window, Unbounded:  [unbounded preceding,unbounded following]
        // For window frame `ROWS|RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`
        _executor.get_next_impl = &AnalyticSinkLocalState::_get_next_for_partition;
    } else if (p._has_range_window) {
        if (!p._has_window_start &&
            p._window.window_end.type == TAnalyticWindowBoundaryType::CURRENT_ROW) {
            // For window frame `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`
            _executor.get_next_impl = &AnalyticSinkLocalState::_get_next_for_unbounded_range;
            _streaming_mode = true;
        } else {
            _executor.get_next_impl = &AnalyticSinkLocalState::_get_next_for_range_between;
        }

    } else {
        if (!p._has_window_start) {
            _executor.get_next_impl = &AnalyticSinkLocalState::_get_next_for_unbounded_rows;
        } else {
            _executor.get_next_impl = &AnalyticSinkLocalState::_get_next_for_sliding_rows;
        }
        _streaming_mode = true;
        _support_incremental_calculate = (p._has_window_start && p._has_window_end);

        // TAnalyticWindowBoundaryType::PRECEDING -> negative
        // TAnalyticWindowBoundaryType::CURRENT_ROW -> set zero
        // TAnalyticWindowBoundaryType::FOLLOWING -> positive
        if (p._has_window_start) { //calculate start boundary
            TAnalyticWindowBoundary b = p._window.window_start;
            if (b.__isset.rows_offset_value) { //[offset     ,   ]
                _rows_start_offset = b.rows_offset_value;
                if (b.type == TAnalyticWindowBoundaryType::PRECEDING) {
                    _rows_start_offset *= -1;
                }
            } else {
                DCHECK_EQ(b.type, TAnalyticWindowBoundaryType::CURRENT_ROW); //[current row,   ]
                _rows_start_offset = 0;
            }
        }

        if (p._has_window_end) { //calculate end boundary
            TAnalyticWindowBoundary b = p._window.window_end;
            if (b.__isset.rows_offset_value) { //[       , offset]
                _rows_end_offset = b.rows_offset_value;
                if (b.type == TAnalyticWindowBoundaryType::PRECEDING) {
                    _rows_end_offset *= -1;
                }
            } else {
                DCHECK_EQ(b.type, TAnalyticWindowBoundaryType::CURRENT_ROW); //[   ,current row]
                _rows_end_offset = 0;
            }
        }
    }
    custom_profile()->add_info_string("streaming mode: ", std::to_string(_streaming_mode));
    return Status::OK();
}

Status AnalyticSinkLocalState::open(RuntimeState* state) {
    RETURN_IF_ERROR(PipelineXSinkLocalState<AnalyticSharedState>::open(state));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    auto& p = _parent->cast<AnalyticSinkOperatorX>();

    _agg_functions_size = p._agg_functions_size;
    _agg_expr_ctxs.resize(_agg_functions_size);
    _agg_functions.resize(_agg_functions_size);
    _agg_input_columns.resize(_agg_functions_size);
    _offsets_of_aggregate_states.resize(_agg_functions_size);
    _result_column_nullable_flags.resize(_agg_functions_size);
    _result_column_could_resize.resize(_agg_functions_size);
    _use_null_result.resize(_agg_functions_size, 0);
    _could_use_previous_result.resize(_agg_functions_size, 0);

    for (int i = 0; i < _agg_functions_size; ++i) {
        _agg_functions[i] = p._agg_functions[i]->clone(state, state->obj_pool());
        _agg_input_columns[i].resize(p._num_agg_input[i]);
        _agg_expr_ctxs[i].resize(p._agg_expr_ctxs[i].size());
        for (int j = 0; j < p._agg_expr_ctxs[i].size(); ++j) {
            RETURN_IF_ERROR(p._agg_expr_ctxs[i][j]->clone(state, _agg_expr_ctxs[i][j]));
            _agg_input_columns[i][j] = _agg_expr_ctxs[i][j]->root()->data_type()->create_column();
        }
        _offsets_of_aggregate_states[i] = p._offsets_of_aggregate_states[i];
        _result_column_nullable_flags[i] =
                !_agg_functions[i]->function()->get_return_type()->is_nullable() &&
                _agg_functions[i]->data_type()->is_nullable();
        _result_column_could_resize[i] =
                _agg_functions[i]->function()->result_column_could_resize();
        if (PARTITION_FUNCTION_SET.contains(_agg_functions[i]->function()->get_name())) {
            _streaming_mode = false;
        }
        _support_incremental_calculate &=
                _agg_functions[i]->function()->supported_incremental_mode();
    }

    _partition_exprs_size = p._partition_by_eq_expr_ctxs.size();
    _partition_by_eq_expr_ctxs.resize(_partition_exprs_size);
    _partition_by_columns.resize(_partition_exprs_size);
    for (size_t i = 0; i < _partition_exprs_size; i++) {
        RETURN_IF_ERROR(
                p._partition_by_eq_expr_ctxs[i]->clone(state, _partition_by_eq_expr_ctxs[i]));
        _partition_by_columns[i] =
                _partition_by_eq_expr_ctxs[i]->root()->data_type()->create_column();
    }

    _order_by_exprs_size = p._order_by_eq_expr_ctxs.size();
    _order_by_eq_expr_ctxs.resize(_order_by_exprs_size);
    _order_by_columns.resize(_order_by_exprs_size);
    for (size_t i = 0; i < _order_by_exprs_size; i++) {
        RETURN_IF_ERROR(p._order_by_eq_expr_ctxs[i]->clone(state, _order_by_eq_expr_ctxs[i]));
        _order_by_columns[i] = _order_by_eq_expr_ctxs[i]->root()->data_type()->create_column();
    }

    // only support one order by column, so need two columns upper and lower bound
    _range_between_expr_ctxs.resize(p._range_between_expr_ctxs.size());
    _range_result_columns.resize(_range_between_expr_ctxs.size());
    for (size_t i = 0; i < _range_between_expr_ctxs.size(); i++) {
        RETURN_IF_ERROR(p._range_between_expr_ctxs[i]->clone(state, _range_between_expr_ctxs[i]));
        _range_result_columns[i] =
                _range_between_expr_ctxs[i]->root()->data_type()->create_column();
    }

    _fn_place_ptr = _agg_arena_pool.aligned_alloc(p._total_size_of_aggregate_states,
                                                  p._align_aggregate_states);
    _create_agg_status();
    return Status::OK();
}

Status AnalyticSinkLocalState::close(RuntimeState* state, Status exec_status) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_close_timer);
    if (_closed) {
        return Status::OK();
    }

    _destroy_agg_status();
    _fn_place_ptr = nullptr;
    _result_window_columns.clear();
    _agg_input_columns.clear();
    _partition_by_columns.clear();
    _order_by_columns.clear();
    _range_result_columns.clear();
    return PipelineXSinkLocalState<AnalyticSharedState>::close(state, exec_status);
}

bool AnalyticSinkLocalState::_get_next_for_sliding_rows(int64_t current_block_rows,
                                                        int64_t current_block_base_pos) {
    const bool is_n_following_frame = _rows_end_offset > 0;
    while (_current_row_position < _partition_by_pose.end) {
        int64_t current_row_start = _current_row_position + _rows_start_offset;
        int64_t current_row_end = _current_row_position + _rows_end_offset + 1;

        if (is_n_following_frame && !_partition_by_pose.is_ended &&
            current_row_end > _partition_by_pose.end) {
            _need_more_data = true;
            break;
        }
        if (_support_incremental_calculate) {
            _execute_for_function<true>(_partition_by_pose.start, _partition_by_pose.end,
                                        current_row_start, current_row_end);
        } else {
            _reset_agg_status();
            // Eg: rows between unbounded preceding and 10 preceding
            // Make sure range_start <= range_end
            current_row_start = std::min(current_row_start, current_row_end);
            _execute_for_function(_partition_by_pose.start, _partition_by_pose.end,
                                  current_row_start, current_row_end);
        }

        int64_t pos = current_pos_in_block();
        _insert_result_info(pos, pos + 1);
        _current_row_position++;
        // means the current row is the last row in the block, could output the block
        if (_current_row_position - current_block_base_pos >= current_block_rows) {
            return true;
        }
    }
    return false;
}

bool AnalyticSinkLocalState::_get_next_for_unbounded_rows(int64_t current_block_rows,
                                                          int64_t current_block_base_pos) {
    const bool is_n_following_frame = _rows_end_offset > 0;
    while (_current_row_position < _partition_by_pose.end) {
        int64_t current_row_end = _current_row_position + _rows_end_offset + 1;
        // [preceding, current_row], [current_row, following] rewrite it's same
        // as could reuse the previous calculate result, so don't call _reset_agg_status function
        // going on calculate, add up data, no need to reset state
        if (is_n_following_frame && !_partition_by_pose.is_ended &&
            current_row_end > _partition_by_pose.end) {
            _need_more_data = true;
            break;
        }
        if (is_n_following_frame && _current_row_position == _partition_by_pose.start) {
            _execute_for_function(_partition_by_pose.start, _partition_by_pose.end,
                                  _partition_by_pose.start, current_row_end - 1);
        }
        _execute_for_function(_partition_by_pose.start, _partition_by_pose.end, current_row_end - 1,
                              current_row_end);
        int64_t pos = current_pos_in_block();
        _insert_result_info(pos, pos + 1);
        _current_row_position++;
        // means the current row is the last row in the block, could output the block
        if (_current_row_position - current_block_base_pos >= current_block_rows) {
            return true;
        }
    }
    return false;
}

bool AnalyticSinkLocalState::_get_next_for_partition(int64_t current_block_rows,
                                                     int64_t current_block_base_pos) {
    if (_current_row_position == _partition_by_pose.start) {
        _execute_for_function(_partition_by_pose.start, _partition_by_pose.end,
                              _partition_by_pose.start, _partition_by_pose.end);
    }

    // the end pos maybe after multis blocks, but should output by batch size and should not exceed partition end
    auto window_end_pos = _current_row_position + current_block_rows;
    window_end_pos = std::min<int64_t>(window_end_pos, _partition_by_pose.end);

    auto previous_window_frame_width = _current_row_position - current_block_base_pos;
    auto current_window_frame_width = window_end_pos - current_block_base_pos;
    // should not exceed block batch size
    current_window_frame_width = std::min<int64_t>(current_window_frame_width, current_block_rows);
    auto real_deal_with_width = current_window_frame_width - previous_window_frame_width;
    int64_t pos = current_pos_in_block();
    _insert_result_info(pos, pos + real_deal_with_width);
    _current_row_position += real_deal_with_width;
    return _current_row_position - current_block_base_pos >= current_block_rows;
}

bool AnalyticSinkLocalState::_get_next_for_unbounded_range(int64_t current_block_rows,
                                                           int64_t current_block_base_pos) {
    _update_order_by_range();
    if (!_order_by_pose.is_ended) {
        DCHECK(!_partition_by_pose.is_ended);
        _need_more_data = true;
        return false;
    }
    while (_current_row_position < _order_by_pose.end) {
        if (_current_row_position == _order_by_pose.start) {
            _execute_for_function(_partition_by_pose.start, _partition_by_pose.end,
                                  _order_by_pose.start, _order_by_pose.end);
        }
        auto previous_window_frame_width = _current_row_position - current_block_base_pos;
        auto current_window_frame_width = _order_by_pose.end - current_block_base_pos;
        current_window_frame_width =
                std::min<int64_t>(current_window_frame_width, current_block_rows);
        auto real_deal_with_width = current_window_frame_width - previous_window_frame_width;
        int64_t pos = current_pos_in_block();
        _insert_result_info(pos, pos + real_deal_with_width);
        _current_row_position += real_deal_with_width;
        if (_current_row_position - current_block_base_pos >= current_block_rows) {
            return true;
        }
    }
    return false;
}

bool AnalyticSinkLocalState::_get_next_for_range_between(int64_t current_block_rows,
                                                         int64_t current_block_base_pos) {
    while (_current_row_position < _partition_by_pose.end) {
        _reset_agg_status();
        if (!_parent->cast<AnalyticSinkOperatorX>()._window.__isset.window_start) {
            _order_by_pose.start = _partition_by_pose.start;
        } else {
            _order_by_pose.start = find_first_not_equal(
                    _range_result_columns[0].get(), _order_by_columns[0].get(),
                    _current_row_position, _order_by_pose.start, _partition_by_pose.end);
        }

        if (!_parent->cast<AnalyticSinkOperatorX>()._window.__isset.window_end) {
            _order_by_pose.end = _partition_by_pose.end;
        } else {
            _order_by_pose.end = find_first_not_equal(
                    _range_result_columns[1].get(), _order_by_columns[0].get(),
                    _current_row_position, _order_by_pose.end, _partition_by_pose.end);
        }
        _execute_for_function(_partition_by_pose.start, _partition_by_pose.end,
                              _order_by_pose.start, _order_by_pose.end);
        int64_t pos = current_pos_in_block();
        _insert_result_info(pos, pos + 1);
        _current_row_position++;
        if (_current_row_position - current_block_base_pos >= current_block_rows) {
            return true;
        }
    }
    if (_current_row_position == _partition_by_pose.end) {
        _order_by_pose.start = _partition_by_pose.end; // update to next partition pos
        _order_by_pose.end = _partition_by_pose.end;
    }
    return false;
}

Status AnalyticSinkLocalState::_execute_impl() {
    while (_output_block_index < _input_blocks.size()) {
        {
            _get_partition_by_end();
            // streaming_mode means no need get all parition data, could calculate data when it's arrived
            if (!_partition_by_pose.is_ended && (!_streaming_mode || _need_more_data)) {
                _need_more_data = false;
                break;
            }
            _init_result_columns();
            auto current_block_rows = _input_blocks[_output_block_index].rows();
            auto current_block_base_pos =
                    _input_block_first_row_positions[_output_block_index] - _have_removed_rows;
            bool should_output = false;

            {
                SCOPED_TIMER(_evaluation_timer);
                should_output = (this->*_executor.get_next_impl)(current_block_rows,
                                                                 current_block_base_pos);
            }

            if (should_output) {
                vectorized::Block block;
                _output_current_block(&block);
                _refresh_buffer_and_dependency_state(&block);
            }
            if (_current_row_position == _partition_by_pose.end && _partition_by_pose.is_ended) {
                _reset_state_for_next_partition();
            }
        }
    }
    return Status::OK();
}

template <bool incremental>
void AnalyticSinkLocalState::_execute_for_function(int64_t partition_start, int64_t partition_end,
                                                   int64_t frame_start, int64_t frame_end) {
    // here is the core function, should not add timer
    for (size_t i = 0; i < _agg_functions_size; ++i) {
        std::vector<const vectorized::IColumn*> agg_columns;
        for (int j = 0; j < _agg_input_columns[i].size(); ++j) {
            agg_columns.push_back(_agg_input_columns[i][j].get());
        }
        if constexpr (incremental) {
            _agg_functions[i]->function()->execute_function_with_incremental(
                    partition_start, partition_end, frame_start, frame_end,
                    _fn_place_ptr + _offsets_of_aggregate_states[i], agg_columns.data(),
                    _agg_arena_pool, false, false, false, &_use_null_result[i],
                    &_could_use_previous_result[i]);
        } else {
            _agg_functions[i]->function()->add_range_single_place(
                    partition_start, partition_end, frame_start, frame_end,
                    _fn_place_ptr + _offsets_of_aggregate_states[i], agg_columns.data(),
                    _agg_arena_pool, &(_use_null_result[i]), &_could_use_previous_result[i]);
        }
    }
}

void AnalyticSinkLocalState::_insert_result_info(int64_t start, int64_t end) {
    // here is the core function, should not add timer
    for (size_t i = 0; i < _agg_functions_size; ++i) {
        if (_result_column_nullable_flags[i]) {
            if (_use_null_result[i]) {
                _result_window_columns[i]->insert_many_defaults(end - start);
            } else {
                auto* dst =
                        assert_cast<vectorized::ColumnNullable*>(_result_window_columns[i].get());
                dst->get_null_map_data().resize_fill(
                        dst->get_null_map_data().size() + static_cast<uint32_t>(end - start), 0);
                _agg_functions[i]->function()->insert_result_into_range(
                        _fn_place_ptr + _offsets_of_aggregate_states[i], dst->get_nested_column(),
                        start, end);
            }
        } else {
            _agg_functions[i]->function()->insert_result_into_range(
                    _fn_place_ptr + _offsets_of_aggregate_states[i], *_result_window_columns[i],
                    start, end);
        }
    }
}

void AnalyticSinkLocalState::_output_current_block(vectorized::Block* block) {
    block->swap(std::move(_input_blocks[_output_block_index]));
    _blocks_memory_usage->add(-block->allocated_bytes());
    DCHECK(_parent->cast<AnalyticSinkOperatorX>()._change_to_nullable_flags.size() ==
           _result_window_columns.size());
    for (size_t i = 0; i < _result_window_columns.size(); ++i) {
        DCHECK(_result_window_columns[i]);
        DCHECK(_agg_functions[i]);
        if (_parent->cast<AnalyticSinkOperatorX>()._change_to_nullable_flags[i]) {
            block->insert({make_nullable(std::move(_result_window_columns[i])),
                           make_nullable(_agg_functions[i]->data_type()), ""});
        } else {
            block->insert(
                    {std::move(_result_window_columns[i]), _agg_functions[i]->data_type(), ""});
        }
    }

    _output_block_index++;
}

void AnalyticSinkLocalState::_init_result_columns() {
    if (_current_row_position + _have_removed_rows ==
        _input_block_first_row_positions[_output_block_index]) {
        _result_window_columns.resize(_agg_functions_size);
        // return type create result column
        for (size_t i = 0; i < _agg_functions_size; ++i) {
            _result_window_columns[i] = _agg_functions[i]->data_type()->create_column();
            if (_result_column_could_resize[i]) {
                _result_window_columns[i]->resize(_input_blocks[_output_block_index].rows());
            } else {
                _result_window_columns[i]->reserve(_input_blocks[_output_block_index].rows());
            }
        }
    }
}

void AnalyticSinkLocalState::_refresh_buffer_and_dependency_state(vectorized::Block* block) {
    size_t buffer_size = 0;
    {
        std::unique_lock<std::mutex> lc(_shared_state->buffer_mutex);
        _shared_state->blocks_buffer.push(std::move(*block));
        buffer_size = _shared_state->blocks_buffer.size();
    }
    if (buffer_size > 128) {
        // buffer have enough data, could block the sink
        _dependency->block();
    }
    // buffer have push data, could signal the source to read
    _dependency->set_ready_to_read();
}

void AnalyticSinkLocalState::_reset_state_for_next_partition() {
    _partition_column_statistics.update(_partition_by_pose.end - _partition_by_pose.start);
    _order_by_column_statistics.reset();
    _partition_by_pose.start = _partition_by_pose.end;
    _current_row_position = _partition_by_pose.start;
    _reset_agg_status();
}

void AnalyticSinkLocalState::_update_order_by_range() {
    // still have more data
    if (_order_by_pose.is_ended && _current_row_position < _order_by_pose.end) {
        return;
    }
    SCOPED_TIMER(_order_search_timer);
    while (!_next_order_by_ends.empty()) {
        int64_t peek = _next_order_by_ends.front();
        _next_order_by_ends.pop();
        if (peek > _order_by_pose.end) {
            _order_by_pose.start = _order_by_pose.end;
            _order_by_pose.end = peek;
            _order_by_pose.is_ended = true;
            _order_by_column_statistics.update(_order_by_pose.end - _order_by_pose.start);
            return;
        }
    }

    if (_order_by_pose.is_ended) {
        _order_by_pose.start = _order_by_pose.end;
    }
    _order_by_pose.end = _partition_by_pose.end;

    {
        if (_order_by_pose.start < _order_by_pose.end) {
            for (size_t i = 0; i < _order_by_exprs_size; ++i) {
                _order_by_pose.end = find_first_not_equal(
                        _order_by_columns[i].get(), _order_by_columns[i].get(),
                        _order_by_pose.start, _order_by_pose.start, _order_by_pose.end);
            }
        }
    }

    if (_order_by_pose.end < _partition_by_pose.end) {
        _order_by_column_statistics.update(_order_by_pose.end - _order_by_pose.start);
        _order_by_pose.is_ended = true;
        _find_next_order_by_ends();
        return;
    }
    DCHECK_EQ(_partition_by_pose.end, _order_by_pose.end);
    if (_partition_by_pose.is_ended) {
        _order_by_pose.is_ended = true;
        return;
    }
    _order_by_pose.is_ended = false;
}

void AnalyticSinkLocalState::_get_partition_by_end() {
    //still have data, return partition_by_end directly
    if (_partition_by_pose.is_ended && _current_row_position < _partition_by_pose.end) {
        return;
    }
    //no partition_by, the all block is end
    if (_partition_by_eq_expr_ctxs.empty() || (_input_total_rows == 0)) {
        _partition_by_pose.end = _input_total_rows - _have_removed_rows;
        _partition_by_pose.is_ended = _input_eos;
        return;
    }
    SCOPED_TIMER(_partition_search_timer);
    while (!_next_partition_ends.empty()) {
        int64_t peek = _next_partition_ends.front();
        _next_partition_ends.pop();
        if (peek > _partition_by_pose.end) {
            _partition_by_pose.end = peek;
            _partition_by_pose.is_ended = true;
            return;
        }
    }

    const auto start = _partition_by_pose.end;
    const auto target = (_partition_by_pose.is_ended || _partition_by_pose.end == 0)
                                ? _partition_by_pose.end
                                : _partition_by_pose.end - 1;
    DCHECK(_partition_exprs_size > 0);
    const auto partition_column_rows = _partition_by_columns[0]->size();
    _partition_by_pose.end = partition_column_rows;

    {
        if (start < _partition_by_pose.end) {
            for (size_t i = 0; i < _partition_exprs_size; ++i) {
                _partition_by_pose.end = find_first_not_equal(
                        _partition_by_columns[i].get(), _partition_by_columns[i].get(), target,
                        start, _partition_by_pose.end);
            }
        }
    }

    if (_partition_by_pose.end < partition_column_rows) {
        _partition_by_pose.is_ended = true;
        _find_next_partition_ends();
        return;
    }

    DCHECK_EQ(_partition_by_pose.end, partition_column_rows);
    _partition_by_pose.is_ended = _input_eos;
}

void AnalyticSinkLocalState::_find_next_partition_ends() {
    if (!_partition_column_statistics.is_high_cardinality()) {
        return;
    }

    SCOPED_TIMER(_partition_search_timer);
    for (size_t i = _partition_by_pose.end + 1; i < _partition_by_columns[0]->size(); ++i) {
        for (auto& column : _partition_by_columns) {
            auto cmp = column->compare_at(i - 1, i, *column, 1);
            if (cmp != 0) {
                _next_partition_ends.push(i);
                break;
            }
        }
    }
}

void AnalyticSinkLocalState::_find_next_order_by_ends() {
    if (!_order_by_column_statistics.is_high_cardinality()) {
        return;
    }

    SCOPED_TIMER(_order_search_timer);
    for (size_t i = _order_by_pose.end + 1; i < _partition_by_pose.end; ++i) {
        for (auto& column : _order_by_columns) {
            auto cmp = column->compare_at(i - 1, i, *column, 1);
            if (cmp != 0) {
                _next_order_by_ends.push(i);
                break;
            }
        }
    }
}

// Compares (*this)[n] and rhs[m]
int64_t AnalyticSinkLocalState::find_first_not_equal(vectorized::IColumn* reference_column,
                                                     vectorized::IColumn* compared_column,
                                                     int64_t target, int64_t start, int64_t end) {
    while (start + 1 < end) {
        int64_t mid = start + (end - start) / 2;
        if (reference_column->compare_at(target, mid, *compared_column, 1) == 0) {
            start = mid;
        } else {
            end = mid;
        }
    }
    if (reference_column->compare_at(target, end - 1, *compared_column, 1) == 0) {
        return end;
    }
    return end - 1;
}

AnalyticSinkOperatorX::AnalyticSinkOperatorX(ObjectPool* pool, int operator_id, int dest_id,
                                             const TPlanNode& tnode, const DescriptorTbl& descs,
                                             bool require_bucket_distribution)
        : DataSinkOperatorX(operator_id, tnode, dest_id),
          _pool(pool),
          _intermediate_tuple_id(tnode.analytic_node.intermediate_tuple_id),
          _output_tuple_id(tnode.analytic_node.output_tuple_id),
          _buffered_tuple_id(tnode.analytic_node.__isset.buffered_tuple_id
                                     ? tnode.analytic_node.buffered_tuple_id
                                     : 0),
          _is_colocate(tnode.analytic_node.__isset.is_colocate && tnode.analytic_node.is_colocate),
          _require_bucket_distribution(require_bucket_distribution),
          _partition_exprs(tnode.__isset.distribute_expr_lists && require_bucket_distribution
                                   ? tnode.distribute_expr_lists[0]
                                   : tnode.analytic_node.partition_exprs),
          _window(tnode.analytic_node.window),
          _has_window(tnode.analytic_node.__isset.window),
          _has_range_window(tnode.analytic_node.window.type == TAnalyticWindowType::RANGE),
          _has_window_start(tnode.analytic_node.window.__isset.window_start),
          _has_window_end(tnode.analytic_node.window.__isset.window_end) {}

Status AnalyticSinkOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX::init(tnode, state));
    const TAnalyticNode& analytic_node = tnode.analytic_node;
    _agg_functions_size = analytic_node.analytic_functions.size();
    _agg_expr_ctxs.resize(_agg_functions_size);
    _num_agg_input.resize(_agg_functions_size);
    for (int i = 0; i < _agg_functions_size; ++i) {
        const TExpr& desc = analytic_node.analytic_functions[i];
        vectorized::AggFnEvaluator* evaluator = nullptr;
        // Window function treats all NullableAggregateFunction as AlwaysNullable.
        // Its behavior is same with executed without group by key.
        // https://github.com/apache/doris/pull/40693
        RETURN_IF_ERROR(vectorized::AggFnEvaluator::create(_pool, desc, {}, /*without_key*/ true,
                                                           true, &evaluator));
        _agg_functions.emplace_back(evaluator);

        int node_idx = 0;
        _num_agg_input[i] = desc.nodes[0].num_children;
        for (int j = 0; j < desc.nodes[0].num_children; ++j) {
            ++node_idx;
            vectorized::VExprSPtr expr;
            vectorized::VExprContextSPtr ctx;
            RETURN_IF_ERROR(
                    vectorized::VExpr::create_tree_from_thrift(desc.nodes, &node_idx, expr, ctx));
            _agg_expr_ctxs[i].emplace_back(ctx);
        }
    }

    RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(analytic_node.partition_exprs,
                                                         _partition_by_eq_expr_ctxs));
    RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(analytic_node.order_by_exprs,
                                                         _order_by_eq_expr_ctxs));
    RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(analytic_node.range_between_offset_exprs,
                                                         _range_between_expr_ctxs));
    return Status::OK();
}

Status AnalyticSinkOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX<AnalyticSinkLocalState>::prepare(state));
    for (const auto& ctx : _agg_expr_ctxs) {
        RETURN_IF_ERROR(vectorized::VExpr::prepare(ctx, state, _child->row_desc()));
    }
    _intermediate_tuple_desc = state->desc_tbl().get_tuple_descriptor(_intermediate_tuple_id);
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_output_tuple_id);
    _change_to_nullable_flags.resize(_agg_functions_size);
    for (size_t i = 0; i < _agg_functions_size; ++i) {
        SlotDescriptor* intermediate_slot_desc = _intermediate_tuple_desc->slots()[i];
        SlotDescriptor* output_slot_desc = _output_tuple_desc->slots()[i];
        RETURN_IF_ERROR(_agg_functions[i]->prepare(state, _child->row_desc(),
                                                   intermediate_slot_desc, output_slot_desc));
        _agg_functions[i]->set_version(state->be_exec_version());
        _change_to_nullable_flags[i] =
                output_slot_desc->is_nullable() && (!_agg_functions[i]->data_type()->is_nullable());
    }
    if (!_partition_by_eq_expr_ctxs.empty() || !_order_by_eq_expr_ctxs.empty()) {
        std::vector<TTupleId> tuple_ids;
        tuple_ids.push_back(_child->row_desc().tuple_descriptors()[0]->id());
        tuple_ids.push_back(_buffered_tuple_id);
        RowDescriptor cmp_row_desc(state->desc_tbl(), tuple_ids, std::vector<bool>(2, false));
        if (!_partition_by_eq_expr_ctxs.empty()) {
            RETURN_IF_ERROR(
                    vectorized::VExpr::prepare(_partition_by_eq_expr_ctxs, state, cmp_row_desc));
        }
        if (!_order_by_eq_expr_ctxs.empty()) {
            RETURN_IF_ERROR(
                    vectorized::VExpr::prepare(_order_by_eq_expr_ctxs, state, cmp_row_desc));
        }
    }
    if (!_range_between_expr_ctxs.empty()) {
        DCHECK(_range_between_expr_ctxs.size() == 2);
        RETURN_IF_ERROR(
                vectorized::VExpr::prepare(_range_between_expr_ctxs, state, _child->row_desc()));
    }
    RETURN_IF_ERROR(vectorized::VExpr::open(_range_between_expr_ctxs, state));
    RETURN_IF_ERROR(vectorized::VExpr::open(_partition_by_eq_expr_ctxs, state));
    RETURN_IF_ERROR(vectorized::VExpr::open(_order_by_eq_expr_ctxs, state));
    for (size_t i = 0; i < _agg_functions_size; ++i) {
        RETURN_IF_ERROR(_agg_functions[i]->open(state));
        RETURN_IF_ERROR(vectorized::VExpr::open(_agg_expr_ctxs[i], state));
    }

    _offsets_of_aggregate_states.resize(_agg_functions_size);
    for (size_t i = 0; i < _agg_functions_size; ++i) {
        _offsets_of_aggregate_states[i] = _total_size_of_aggregate_states;
        const auto& agg_function = _agg_functions[i]->function();
        // aggregate states are aligned based on maximum requirement
        _align_aggregate_states = std::max(_align_aggregate_states, agg_function->align_of_data());
        _total_size_of_aggregate_states += agg_function->size_of_data();
        // If not the last aggregate_state, we need pad it so that next aggregate_state will be aligned.
        if (i + 1 < _agg_functions_size) {
            size_t alignment_of_next_state = _agg_functions[i + 1]->function()->align_of_data();
            if ((alignment_of_next_state & (alignment_of_next_state - 1)) != 0) {
                return Status::RuntimeError("Logical error: align_of_data is not 2^N");
            }
            /// Extend total_size to next alignment requirement
            /// Add padding by rounding up 'total_size_of_aggregate_states' to be a multiplier of alignment_of_next_state.
            _total_size_of_aggregate_states =
                    (_total_size_of_aggregate_states + alignment_of_next_state - 1) /
                    alignment_of_next_state * alignment_of_next_state;
        }
    }
    return Status::OK();
}

Status AnalyticSinkOperatorX::sink(doris::RuntimeState* state, vectorized::Block* input_block,
                                   bool eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)input_block->rows());
    local_state._input_eos = eos;
    local_state._remove_unused_rows();
    local_state._reserve_mem_size = 0;
    SCOPED_PEAK_MEM(&local_state._reserve_mem_size);
    RETURN_IF_ERROR(_add_input_block(state, input_block));
    RETURN_IF_ERROR(local_state._execute_impl());
    if (local_state._input_eos) {
        std::unique_lock<std::mutex> lc(local_state._shared_state->sink_eos_lock);
        local_state._shared_state->sink_eos = true;
        local_state._dependency->set_ready_to_read(); // ready for source to read
    }
    return Status::OK();
}

Status AnalyticSinkOperatorX::_add_input_block(doris::RuntimeState* state,
                                               vectorized::Block* input_block) {
    if (input_block->rows() <= 0) {
        return Status::OK();
    }
    auto& local_state = get_local_state(state);
    local_state._input_block_first_row_positions.emplace_back(local_state._input_total_rows);
    size_t block_rows = input_block->rows();
    local_state._input_total_rows += block_rows;

    // record origin columns, maybe be after this, could cast some column but no need to output
    auto column_to_keep = input_block->columns();
    {
        SCOPED_TIMER(local_state._compute_agg_data_timer);
        //insert _agg_input_columns, execute calculate for its, and those columns maybe could remove have used data
        for (size_t i = 0; i < _agg_functions_size; ++i) {
            for (size_t j = 0; j < local_state._agg_expr_ctxs[i].size(); ++j) {
                RETURN_IF_ERROR(_insert_range_column(input_block, local_state._agg_expr_ctxs[i][j],
                                                     local_state._agg_input_columns[i][j].get(),
                                                     block_rows));
            }
        }
    }
    {
        SCOPED_TIMER(local_state._compute_partition_by_timer);
        for (size_t i = 0; i < local_state._partition_by_eq_expr_ctxs.size(); ++i) {
            RETURN_IF_ERROR(
                    _insert_range_column(input_block, local_state._partition_by_eq_expr_ctxs[i],
                                         local_state._partition_by_columns[i].get(), block_rows));
        }
    }
    {
        SCOPED_TIMER(local_state._compute_order_by_timer);
        for (size_t i = 0; i < local_state._order_by_eq_expr_ctxs.size(); ++i) {
            RETURN_IF_ERROR(_insert_range_column(input_block, local_state._order_by_eq_expr_ctxs[i],
                                                 local_state._order_by_columns[i].get(),
                                                 block_rows));
        }
    }
    {
        SCOPED_TIMER(local_state._compute_range_between_function_timer);
        for (size_t i = 0; i < local_state._range_between_expr_ctxs.size(); ++i) {
            RETURN_IF_ERROR(
                    _insert_range_column(input_block, local_state._range_between_expr_ctxs[i],
                                         local_state._range_result_columns[i].get(), block_rows));
        }
    }
    vectorized::Block::erase_useless_column(input_block, column_to_keep);
    COUNTER_UPDATE(local_state._memory_used_counter, input_block->allocated_bytes());
    COUNTER_UPDATE(local_state._blocks_memory_usage, input_block->allocated_bytes());
    local_state._input_blocks.emplace_back(std::move(*input_block));
    return Status::OK();
}

void AnalyticSinkLocalState::_remove_unused_rows() {
    // test column overflow 4G
    DBUG_EXECUTE_IF("AnalyticSinkLocalState._remove_unused_rows", { return; });
#ifdef BE_TEST
    const size_t block_num = 1;
#else
    const size_t block_num = 256;
#endif

    if (_removed_block_index + block_num + 1 >= _input_block_first_row_positions.size()) {
        return;
    }
    const int64_t unused_rows_pos =
            _input_block_first_row_positions[_removed_block_index + block_num];

    if (_streaming_mode) {
        auto idx = _output_block_index - 1;
        if (idx < 0 || _input_block_first_row_positions[idx] <= unused_rows_pos) {
            return;
        }
    } else {
        if (_have_removed_rows + _partition_by_pose.start <= unused_rows_pos) {
            return;
        }
    }

    const int64_t remove_rows = unused_rows_pos - _have_removed_rows;
    {
        SCOPED_TIMER(_remove_rows_timer);
        for (size_t i = 0; i < _agg_functions_size; i++) {
            for (size_t j = 0; j < _agg_expr_ctxs[i].size(); j++) {
                _agg_input_columns[i][j]->erase(0, remove_rows);
            }
        }
        for (size_t i = 0; i < _partition_exprs_size; i++) {
            _partition_by_columns[i]->erase(0, remove_rows);
        }
        for (size_t i = 0; i < _order_by_exprs_size; i++) {
            _order_by_columns[i]->erase(0, remove_rows);
        }
    }
    COUNTER_UPDATE(_remove_count, 1);
    COUNTER_UPDATE(_remove_rows, remove_rows);
    _current_row_position -= remove_rows;
    _partition_by_pose.remove_unused_rows(remove_rows);
    _order_by_pose.remove_unused_rows(remove_rows);
    int64_t candidate_partition_end_size = _next_partition_ends.size();
    while (--candidate_partition_end_size >= 0) {
        auto peek = _next_partition_ends.front();
        _next_partition_ends.pop();
        _next_partition_ends.push(peek - remove_rows);
    }
    int64_t candidate_peer_group_end_size = _next_order_by_ends.size();
    while (--candidate_peer_group_end_size >= 0) {
        auto peek = _next_order_by_ends.front();
        _next_order_by_ends.pop();
        _next_order_by_ends.push(peek - remove_rows);
    }
    _removed_block_index += block_num;
    _have_removed_rows += remove_rows;

    DCHECK_GE(_current_row_position, 0);
    DCHECK_GE(_partition_by_pose.end, 0);
}

size_t AnalyticSinkOperatorX::get_reserve_mem_size(RuntimeState* state, bool eos) {
    auto& local_state = get_local_state(state);
    return local_state._reserve_mem_size;
}

Status AnalyticSinkOperatorX::_insert_range_column(vectorized::Block* block,
                                                   const vectorized::VExprContextSPtr& expr,
                                                   vectorized::IColumn* dst_column, size_t length) {
    int result_col_id = -1;
    RETURN_IF_ERROR(expr->execute(block, &result_col_id));
    DCHECK_GE(result_col_id, 0);
    auto column = block->get_by_position(result_col_id).column->convert_to_full_column_if_const();
    // iff dst_column is string, maybe overflow of 4G, so need ignore overflow
    // the column is used by compare_at self to find the range, it's need convert it when overflow?
    dst_column->insert_range_from_ignore_overflow(*column, 0, length);
    return Status::OK();
}

void AnalyticSinkLocalState::_reset_agg_status() {
    _use_null_result.assign(_agg_functions_size, 0);
    _could_use_previous_result.assign(_agg_functions_size, 0);
    for (size_t i = 0; i < _agg_functions_size; ++i) {
        _agg_functions[i]->reset(_fn_place_ptr + _offsets_of_aggregate_states[i]);
    }
}

void AnalyticSinkLocalState::_create_agg_status() {
    for (size_t i = 0; i < _agg_functions_size; ++i) {
        try {
            _agg_functions[i]->create(_fn_place_ptr + _offsets_of_aggregate_states[i]);
        } catch (...) {
            for (int j = 0; j < i; ++j) {
                _agg_functions[j]->destroy(_fn_place_ptr + _offsets_of_aggregate_states[j]);
            }
            throw;
        }
    }
    _agg_functions_created = true;
}

void AnalyticSinkLocalState::_destroy_agg_status() {
    if (UNLIKELY(_fn_place_ptr == nullptr || !_agg_functions_created)) {
        return;
    }
    for (size_t i = 0; i < _agg_functions_size; ++i) {
        _agg_functions[i]->destroy(_fn_place_ptr + _offsets_of_aggregate_states[i]);
    }
}

template class DataSinkOperatorX<AnalyticSinkLocalState>;

} // namespace doris::pipeline
