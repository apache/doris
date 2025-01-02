
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

#include <string>

#include "pipeline/exec/operator.h"
#include "vec/exprs/vectorized_agg_fn.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"

Status AnalyticSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(PipelineXSinkLocalState<AnalyticSharedState>::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _evaluation_timer = ADD_TIMER(profile(), "GetPartitionBoundTime");
    _compute_agg_data_timer = ADD_TIMER(profile(), "ComputeAggDataTime");
    _compute_partition_by_timer = ADD_TIMER(profile(), "ComputePartitionByTime");
    _compute_order_by_timer = ADD_TIMER(profile(), "ComputeOrderByTime");
    _execute_timer = ADD_TIMER(profile(), "ExecuteTime");
    _get_next_timer = ADD_TIMER(profile(), "GetNextTime");
    _get_result_timer = ADD_TIMER(profile(), "GetResultsTime");
    _agg_arena_pool = std::make_unique<vectorized::Arena>();
    auto& p = _parent->cast<AnalyticSinkOperatorX>();
    if (!p._has_window) { //haven't set window, Unbounded:  [unbounded preceding,unbounded following]
        _executor.get_next_impl = &AnalyticSinkLocalState::_get_next_for_partition;
    } else if (p._has_range_window) {
        // RANGE windows must have UNBOUNDED PRECEDING
        // RANGE window end bound must be CURRENT ROW or UNBOUNDED FOLLOWING
        if (!p._has_window_end) { //haven't set end, so same as PARTITION, [unbounded preceding, unbounded following]
            _executor.get_next_impl = &AnalyticSinkLocalState::_get_next_for_partition;

        } else {
            _executor.get_next_impl = &AnalyticSinkLocalState::_get_next_for_range;
        }
    } else {
        //haven't set start and end, same as PARTITION
        if (!p._has_window_start && !p._has_window_end) {
            _executor.get_next_impl = &AnalyticSinkLocalState::_get_next_for_partition;
        } else if (!p._has_window_start) {
            _executor.get_next_impl = &AnalyticSinkLocalState::_get_next_for_rows;
        } else {
            _executor.get_next_impl = &AnalyticSinkLocalState::_get_next_for_sliding_rows;
        }

        if (p._has_window_start) { //calculate start boundary
            TAnalyticWindowBoundary b = p._window.window_start;
            if (b.__isset.rows_offset_value) { //[offset     ,   ]
                _rows_start_offset = b.rows_offset_value;
                if (b.type == TAnalyticWindowBoundaryType::PRECEDING) {
                    _rows_start_offset *= -1;                                //preceding--> negative
                }                                                            //current_row  0
            } else {                                                         //following    positive
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

    for (int i = 0; i < _agg_functions_size; ++i) {
        _agg_functions[i] = p._agg_functions[i]->clone(state, state->obj_pool());
        _agg_input_columns[i].resize(p._num_agg_input[i]);
        _agg_expr_ctxs[i].resize(p._agg_expr_ctxs[i].size());
        for (int j = 0; j < p._agg_expr_ctxs[i].size(); ++j) {
            RETURN_IF_ERROR(p._agg_expr_ctxs[i][j]->clone(state, _agg_expr_ctxs[i][j]));
            _agg_input_columns[i][j] = _agg_expr_ctxs[i][j]->root()->data_type()->create_column();
        }
    }
    _partition_exprs_size = p._partition_by_eq_expr_ctxs.size();
    _partition_by_eq_expr_ctxs.resize(_partition_exprs_size);
    _partition_by_column_idxs.resize(_partition_exprs_size);
    for (size_t i = 0; i < _partition_exprs_size; i++) {
        RETURN_IF_ERROR(
                p._partition_by_eq_expr_ctxs[i]->clone(state, _partition_by_eq_expr_ctxs[i]));
    }
    _order_by_exprs_size = p._order_by_eq_expr_ctxs.size();
    _order_by_eq_expr_ctxs.resize(_order_by_exprs_size);
    _order_by_column_idxs.resize(_order_by_exprs_size);
    for (size_t i = 0; i < _order_by_exprs_size; i++) {
        RETURN_IF_ERROR(p._order_by_eq_expr_ctxs[i]->clone(state, _order_by_eq_expr_ctxs[i]));
    }
    _fn_place_ptr = _agg_arena_pool->aligned_alloc(p._total_size_of_aggregate_states,
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
    _agg_arena_pool = nullptr;

    std::vector<vectorized::MutableColumnPtr> tmp_result_window_columns;
    _result_window_columns.swap(tmp_result_window_columns);
    return PipelineXSinkLocalState<AnalyticSharedState>::close(state, exec_status);
}

Status AnalyticSinkLocalState::_get_next_for_sliding_rows() {
    if (!_has_input_data()) {
        return Status::OK();
    }
    do {
        auto batch_size = _input_blocks[_output_block_index].rows();
        auto current_block_base_pos = _input_block_first_row_positions[_output_block_index];
        // auto remain_size = _current_row_position - current_block_base_pos - batch_size;
        auto remain_size = batch_size - (_current_row_position - current_block_base_pos);
        _init_result_columns();
        _get_partition_by_end();
        while (_current_row_position < _partition_by_pose.end.pos && remain_size > 0) {
            // return {_current_row_position + _rows_start_offset, _current_row_position + _rows_end_offset + 1};
            const bool is_n_following_frame = _rows_end_offset > 0;
            auto range_start = _current_row_position + _rows_start_offset;
            auto range_end = _current_row_position + _rows_end_offset + 1;
            // For window clause like `ROWS BETWEEN N PRECEDING AND M FOLLOWING`,
            // if the current chunk has not reach the partition boundary, it may need more data.
            if (is_n_following_frame && !_partition_by_pose.is_ended &&
                range_end > _partition_by_pose.end.pos) {
                return Status::OK();
            }
            _reset_agg_status();
            _execute_for_win_func(_partition_by_pose.start.pos, _partition_by_pose.end.pos,
                                  range_start, range_end);
            _insert_result_info(1);
            _current_row_position++;
            remain_size--;
        }
        if (_partition_by_pose.is_ended && _current_row_position == _partition_by_pose.end.pos) {
            _reset_state_for_next_partition();
        }

        if (_current_row_position - current_block_base_pos >= batch_size) {
            vectorized::Block block;
            RETURN_IF_ERROR(output_current_block(&block));
            _refresh_buffer_and_dependency_state(&block);
        }
    } while (_has_input_data());
    return Status::OK();
}

Status AnalyticSinkLocalState::_get_next_for_rows() {
    if (!_has_input_data()) {
        return Status::OK();
    }
    do {
        auto batch_size = _input_blocks[_output_block_index].rows();
        auto current_block_base_pos = _input_block_first_row_positions[_output_block_index];
        // auto remain_size = _current_row_position - current_block_base_pos - batch_size;
        auto remain_size = batch_size - (_current_row_position - current_block_base_pos);
        _init_result_columns();
        _get_partition_by_end();
        while (_current_row_position < _partition_by_pose.end.pos && remain_size > 0) {
            // return {_partition.start, _current_row_position + _rows_end_offset + 1};
            const bool is_n_following_frame = _rows_end_offset > 0;
            auto current_row_end = _current_row_position + _rows_end_offset + 1;
            // if the current chunk has not reach the partition boundary, it may need more data.
            if (is_n_following_frame && !_partition_by_pose.is_ended &&
                current_row_end > _partition_by_pose.end.pos) {
                return Status::OK();
            }

            if (is_n_following_frame && _current_row_position == _partition_by_pose.start.pos) {
                _execute_for_win_func(_partition_by_pose.start.pos, _partition_by_pose.end.pos,
                                      _partition_by_pose.start.pos, current_row_end - 1);
            }
            _execute_for_win_func(_partition_by_pose.start.pos, _partition_by_pose.end.pos,
                                  current_row_end - 1, current_row_end);
            _insert_result_info(1);
            _current_row_position++;
            remain_size--;
        }
        if (_partition_by_pose.is_ended && _current_row_position == _partition_by_pose.end.pos) {
            _reset_state_for_next_partition();
        }
        if (_current_row_position - current_block_base_pos >= batch_size) {
            vectorized::Block block;
            RETURN_IF_ERROR(output_current_block(&block));
            _refresh_buffer_and_dependency_state(&block);
        }
    } while (_has_input_data());
    return Status::OK();
}

Status AnalyticSinkLocalState::_get_next_for_partition() {
    while (_has_input_data()) {
        {
            SCOPED_TIMER(_evaluation_timer);
            _get_partition_by_end();
            if (!_partition_by_pose.is_ended) {
                break;
            }
            _init_result_columns();
            if (_current_row_position == _partition_by_pose.start.pos) {
                _execute_for_win_func(_partition_by_pose.start.pos, _partition_by_pose.end.pos,
                                      _partition_by_pose.start.pos, _partition_by_pose.end.pos);
            }
            auto batch_size = _input_blocks[_output_block_index].rows();
            auto current_block_base_pos = _input_block_first_row_positions[_output_block_index];

            // the end pos maybe after multis blocks, but should output by batch size and should not exceed partition end
            auto window_end_pos = _current_row_position + batch_size;
            window_end_pos = std::min<int64_t>(window_end_pos, _partition_by_pose.end.pos);

            auto previous_window_frame_width = _current_row_position - current_block_base_pos;
            auto current_window_frame_width = window_end_pos - current_block_base_pos;
            // should not exceed block batch size
            current_window_frame_width = std::min<int64_t>(current_window_frame_width, batch_size);
            auto real_deal_with_width = current_window_frame_width - previous_window_frame_width;

            _insert_result_info(real_deal_with_width);
            _current_row_position += real_deal_with_width;

            if (_current_row_position - current_block_base_pos >= batch_size) {
                vectorized::Block block;
                RETURN_IF_ERROR(output_current_block(&block));
                _refresh_buffer_and_dependency_state(&block);
            }
            if (_current_row_position == _partition_by_pose.end.pos) {
                _reset_state_for_next_partition();
            }
        }
    }
    return Status::OK();
}

Status AnalyticSinkLocalState::_get_next_for_range() {
    bool has_finish_current_partition = true;
    while (_has_input_data()) {
        if (has_finish_current_partition) {
            _get_partition_by_end();
        }
        _update_order_by_range();
        if (!_order_by_pose.is_ended) {
            break;
        }
        // maybe need break the loop
        if (_current_row_position < _order_by_pose.end.pos) {
            // real frame is [partition_start, order_by_end]
            // but the real deal with frame is [order_by_start, order_by_end]
            _execute_for_win_func(
                    _partition_by_pose.start.pos, _partition_by_pose.end.pos,
                    // _execute_for_win_func(_order_by_pose.start.pos, _order_by_pose.end.pos,
                    _order_by_pose.start.pos, _order_by_pose.end.pos);
        }

        while (_current_row_position < _order_by_pose.end.pos) {
            _init_result_columns();
            auto batch_size = _input_blocks[_output_block_index].rows();
            auto current_block_base_pos = _input_block_first_row_positions[_output_block_index];

            auto previous_window_frame_width = _current_row_position - current_block_base_pos;
            auto current_window_frame_width = _order_by_pose.end.pos - current_block_base_pos;
            current_window_frame_width = std::min<int64_t>(current_window_frame_width, batch_size);
            auto real_deal_with_width = current_window_frame_width - previous_window_frame_width;

            _insert_result_info(real_deal_with_width);
            _current_row_position += real_deal_with_width;
            if (_current_row_position - current_block_base_pos >= batch_size) {
                vectorized::Block block;
                RETURN_IF_ERROR(output_current_block(&block));
                _refresh_buffer_and_dependency_state(&block);
            }
        }

        if (_partition_by_pose.is_ended && _current_row_position == _partition_by_pose.end.pos) {
            has_finish_current_partition = true;
            _reset_state_for_next_partition();
        } else {
            has_finish_current_partition = false;
        }
    }
    return Status::OK();
}

void AnalyticSinkLocalState::_execute_for_win_func(int64_t partition_start, int64_t partition_end,
                                                   int64_t frame_start, int64_t frame_end) {
    SCOPED_TIMER(_execute_timer);
    for (size_t i = 0; i < _agg_functions_size; ++i) {
        std::vector<const vectorized::IColumn*> agg_columns;
        for (int j = 0; j < _agg_input_columns[i].size(); ++j) {
            agg_columns.push_back(_agg_input_columns[i][j].get());
        }
        _agg_functions[i]->function()->add_range_single_place(
                partition_start, partition_end, frame_start, frame_end,
                _fn_place_ptr +
                        _parent->cast<AnalyticSinkOperatorX>()._offsets_of_aggregate_states[i],
                agg_columns.data(), _agg_arena_pool.get());

        // If the end is not greater than the start, the current window should be empty.
        _current_window_empty =
                std::min(frame_end, partition_end) <= std::max(frame_start, partition_start);
    }
}

void AnalyticSinkLocalState::_insert_result_info(int64_t real_deal_with_width) {
    SCOPED_TIMER(_get_result_timer);
    const auto& offsets_of_aggregate_states =
            _parent->cast<AnalyticSinkOperatorX>()._offsets_of_aggregate_states;
    for (size_t i = 0; i < _agg_functions_size; ++i) {
        for (size_t j = 0; j < real_deal_with_width; ++j) {
            if (!_agg_functions[i]->function()->get_return_type()->is_nullable() &&
                _result_window_columns[i]->is_nullable()) {
                if (_current_window_empty) {
                    _result_window_columns[i]->insert_default();
                } else {
                    auto* dst = assert_cast<vectorized::ColumnNullable*>(
                            _result_window_columns[i].get());
                    dst->get_null_map_data().push_back(0);
                    _agg_functions[i]->insert_result_info(
                            _fn_place_ptr + offsets_of_aggregate_states[i],
                            &dst->get_nested_column());
                }
                continue;
            }
            _agg_functions[i]->insert_result_info(_fn_place_ptr + offsets_of_aggregate_states[i],
                                                  _result_window_columns[i].get());
        }
    }
}

Status AnalyticSinkLocalState::output_current_block(vectorized::Block* block) {
    block->swap(std::move(_input_blocks[_output_block_index]));
    // _blocks_memory_usage->add(-block->allocated_bytes());
    if (_input_col_ids.size() < block->columns()) {
        block->erase_not_in(_input_col_ids);
    }

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
    return Status::OK();
}

void AnalyticSinkLocalState::_init_result_columns() {
    if (_current_row_position == _input_block_first_row_positions[_output_block_index]) {
        _result_window_columns.resize(_agg_functions_size);
        for (size_t i = 0; i < _agg_functions_size; ++i) {
            _result_window_columns[i] =
                    _agg_functions[i]->data_type()->create_column(); //return type
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
    _partition_by_pose.start = _partition_by_pose.end;
    _current_row_position = _partition_by_pose.start.pos;
    _reset_agg_status();
}

//_partition_by_columns,_order_by_columns save in blocks, so if need to calculate the boundary, may find in which blocks firstly
BlockRowPos AnalyticSinkLocalState::_compare_row_to_find_end(int64_t idx, BlockRowPos start,
                                                             BlockRowPos end,
                                                             bool need_check_first) {
    int64_t start_init_row_num = start.row_num;
    DCHECK_LT(start.block_num, _input_blocks.size());
    DCHECK_LT(idx, _input_blocks[start.block_num].columns())
            << _input_blocks[start.block_num].dump_structure();
    vectorized::ColumnPtr start_column = _input_blocks[start.block_num].get_by_position(idx).column;
    vectorized::ColumnPtr start_next_block_column = start_column;

    DCHECK_LE(start.block_num, end.block_num);
    DCHECK_LE(start.block_num, _input_blocks.size() - 1);
    int64_t start_block_num = start.block_num;
    int64_t end_block_num = end.block_num;
    int64_t mid_block_num = end.block_num;
    // To fix this problem: https://github.com/apache/doris/issues/15951
    // in this case, the partition by column is last row of block, so it's pointed to a new block at row = 0, range is: [left, right)
    // From the perspective of order by column, the two values are exactly equal.
    // so the range will be get wrong because it's compare_at == 0 with next block at row = 0
    if (need_check_first && end.block_num > 0 && end.row_num == 0) {
        end.block_num--;
        end_block_num--;
        end.row_num = _input_blocks[end_block_num].rows();
    }
    //binary search find in which block
    while (start_block_num < end_block_num) {
        mid_block_num = (start_block_num + end_block_num + 1) >> 1;
        start_next_block_column = _input_blocks[mid_block_num].get_by_position(idx).column;
        //Compares (*this)[n] and rhs[m], this: start[init_row]  rhs: mid[0]
        if (start_column->compare_at(start_init_row_num, 0, *start_next_block_column, 1) == 0) {
            start_block_num = mid_block_num;
        } else {
            end_block_num = mid_block_num - 1;
        }
    }

    // have check the start.block_num:  start_column[start_init_row_num] with mid_block_num start_next_block_column[0]
    // now next block must not be result, so need check with end_block_num: start_next_block_column[last_row]
    if (end_block_num == mid_block_num - 1) {
        start_next_block_column = _input_blocks[end_block_num].get_by_position(idx).column;
        int64_t block_size = _input_blocks[end_block_num].rows();
        if ((start_column->compare_at(start_init_row_num, block_size - 1, *start_next_block_column,
                                      1) == 0)) {
            start.block_num = end_block_num + 1;
            start.row_num = 0;
            return start;
        }
    }

    //check whether need get column again, maybe same as first init
    // if the start_block_num have move to forword, so need update start block num and compare it from row_num=0
    if (start_block_num != start.block_num) {
        start_init_row_num = 0;
        start.block_num = start_block_num;
        start_column = _input_blocks[start.block_num].get_by_position(idx).column;
    }
    //binary search, set start and end pos
    int64_t start_pos = start_init_row_num;
    int64_t end_pos = _input_blocks[start.block_num].rows();
    //if end_block_num haven't moved, only start_block_num go to the end block
    //so could use the end.row_num for binary search
    if (start.block_num == end.block_num) {
        end_pos = end.row_num;
    }
    while (start_pos < end_pos) {
        int64_t mid_pos = (start_pos + end_pos) >> 1;
        if (start_column->compare_at(start_init_row_num, mid_pos, *start_column, 1)) {
            end_pos = mid_pos;
        } else {
            start_pos = mid_pos + 1;
        }
    }
    start.row_num = start_pos; //update row num, return the find end
    return start;
}

void AnalyticSinkLocalState::_update_order_by_range() {
    if (_order_by_pose.is_ended && _current_row_position < _order_by_pose.end.pos) {
        return;
    }

    if (_order_by_pose.is_ended) {
        _order_by_pose.start = _order_by_pose.end;
    }
    _order_by_pose.end = _partition_by_pose.end;

    for (size_t i = 0; i < _order_by_exprs_size; ++i) {
        _order_by_pose.end = _compare_row_to_find_end(
                _order_by_column_idxs[i], _order_by_pose.start, _order_by_pose.end, true);
    }
    _order_by_pose.start.pos = _input_block_first_row_positions[_order_by_pose.start.block_num] +
                               _order_by_pose.start.row_num;
    _order_by_pose.end.pos = _input_block_first_row_positions[_order_by_pose.end.block_num] +
                             _order_by_pose.end.row_num;
    // `_order_by_end` will be assigned to `_order_by_start` next time,
    // so make it a valid position.
    if (_order_by_pose.end.row_num == _input_blocks[_order_by_pose.end.block_num].rows()) {
        _order_by_pose.end.block_num++;
        _order_by_pose.end.row_num = 0;
    }

    if (_order_by_pose.end.pos < _partition_by_pose.end.pos) {
        _order_by_pose.is_ended = true;
        return;
    }
    DCHECK_EQ(_partition_by_pose.end.pos, _order_by_pose.end.pos);
    if (_partition_by_pose.is_ended) {
        _order_by_pose.is_ended = true;
        return;
    }
    _order_by_pose.is_ended = false;
}

void AnalyticSinkLocalState::_get_partition_by_end() {
    //still have data, return partition_by_end directly
    if (_partition_by_pose.is_ended && _current_row_position < _partition_by_pose.end.pos) {
        return;
    }
    //no partition_by, the all block is end
    if (_partition_by_eq_expr_ctxs.empty() || (_input_total_rows == 0)) {
        _partition_by_pose.end.block_num = _input_blocks.size() - 1;
        _partition_by_pose.end.row_num = _input_blocks.back().rows();
        _partition_by_pose.end.pos = _input_total_rows;
        _partition_by_pose.is_ended = _input_eos;
        return;
    }

    BlockRowPos cal_end = _all_block_end;
    //have partition_by, binary search the partition end
    for (size_t i = 0; i < _partition_by_eq_expr_ctxs.size(); ++i) {
        cal_end = _compare_row_to_find_end(_partition_by_column_idxs[i], _partition_by_pose.end,
                                           cal_end);
    }
    cal_end.pos = _input_block_first_row_positions[cal_end.block_num] + cal_end.row_num;
    _partition_by_pose.end = cal_end;
    if (_partition_by_pose.end.pos < _input_total_rows) {
        _partition_by_pose.is_ended = true;
        return;
    }
    DCHECK_EQ(_partition_by_pose.end.pos, _input_total_rows);
    _partition_by_pose.is_ended = _input_eos;
}

AnalyticSinkOperatorX::AnalyticSinkOperatorX(ObjectPool* pool, int operator_id, int dest_id,
                                             const TPlanNode& tnode, const DescriptorTbl& descs,
                                             bool require_bucket_distribution)
        : DataSinkOperatorX(operator_id, tnode.node_id, dest_id),
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
          _has_window_end(tnode.analytic_node.window.__isset.window_end) {
    _is_serial_operator = tnode.__isset.is_serial_operator && tnode.is_serial_operator;
    _fn_scope = AnalyticFnScope::PARTITION;
    if (_has_window && _has_range_window) {
        // haven't set end, so same as PARTITION, [unbounded preceding, unbounded following]
        if (_has_window_end) {
            _fn_scope = AnalyticFnScope::RANGE; // range:  [unbounded preceding,current row]
        }
    } else if (_has_window) {
        if (_has_window_start || _has_window_end) {
            // both not set, same as PARTITION
            _fn_scope = AnalyticFnScope::ROWS;
        }
    }
}

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
                                                           &evaluator));
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
    return Status::OK();
}

Status AnalyticSinkOperatorX::open(RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX<AnalyticSinkLocalState>::open(state));
    for (const auto& ctx : _agg_expr_ctxs) {
        RETURN_IF_ERROR(vectorized::VExpr::prepare(ctx, state, _child->row_desc()));
    }
    _intermediate_tuple_desc = state->desc_tbl().get_tuple_descriptor(_intermediate_tuple_id);
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_output_tuple_id);
    for (size_t i = 0; i < _agg_functions_size; ++i) {
        SlotDescriptor* intermediate_slot_desc = _intermediate_tuple_desc->slots()[i];
        SlotDescriptor* output_slot_desc = _output_tuple_desc->slots()[i];
        RETURN_IF_ERROR(_agg_functions[i]->prepare(state, _child->row_desc(),
                                                   intermediate_slot_desc, output_slot_desc));
        _agg_functions[i]->set_version(state->be_exec_version());
        _change_to_nullable_flags.push_back(output_slot_desc->is_nullable() &&
                                            !_agg_functions[i]->data_type()->is_nullable());
    }
    if (!_partition_by_eq_expr_ctxs.empty() || !_order_by_eq_expr_ctxs.empty()) {
        vector<TTupleId> tuple_ids;
        tuple_ids.push_back(_child->row_desc().tuple_descriptors()[0]->id());
        tuple_ids.push_back(_buffered_tuple_id);
        RowDescriptor cmp_row_desc(state->desc_tbl(), tuple_ids, vector<bool>(2, false));
        if (!_partition_by_eq_expr_ctxs.empty()) {
            RETURN_IF_ERROR(
                    vectorized::VExpr::prepare(_partition_by_eq_expr_ctxs, state, cmp_row_desc));
        }
        if (!_order_by_eq_expr_ctxs.empty()) {
            RETURN_IF_ERROR(
                    vectorized::VExpr::prepare(_order_by_eq_expr_ctxs, state, cmp_row_desc));
        }
    }

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
    RETURN_IF_ERROR(_add_input_block(state, input_block));
    RETURN_IF_ERROR((local_state.*(local_state._executor.get_next_impl))());

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

    local_state._all_block_end.block_num = local_state._input_blocks.size();
    local_state._all_block_end.row_num = block_rows;
    local_state._all_block_end.pos = local_state._input_total_rows;

    // record origin columns, maybe be after this, could cast some column but no need to output
    if (local_state._input_col_ids.empty()) {
        for (int c = 0; c < input_block->columns(); ++c) {
            local_state._input_col_ids.emplace_back(c);
        }
    }

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
            int result_col_id = -1;
            RETURN_IF_ERROR(local_state._partition_by_eq_expr_ctxs[i]->execute(input_block,
                                                                               &result_col_id));
            DCHECK_GE(result_col_id, 0);
            local_state._partition_by_column_idxs[i] = result_col_id;
        }
    }

    {
        SCOPED_TIMER(local_state._compute_order_by_timer);
        for (size_t i = 0; i < local_state._order_by_eq_expr_ctxs.size(); ++i) {
            int result_col_id = -1;
            RETURN_IF_ERROR(
                    local_state._order_by_eq_expr_ctxs[i]->execute(input_block, &result_col_id));
            DCHECK_GE(result_col_id, 0);
            local_state._order_by_column_idxs[i] = result_col_id;
        }
    }

    COUNTER_UPDATE(local_state._memory_used_counter, input_block->allocated_bytes());
    local_state._input_blocks.emplace_back(std::move(*input_block));
    return Status::OK();
}

Status AnalyticSinkOperatorX::_insert_range_column(vectorized::Block* block,
                                                   const vectorized::VExprContextSPtr& expr,
                                                   vectorized::IColumn* dst_column, size_t length) {
    int result_col_id = -1;
    RETURN_IF_ERROR(expr->execute(block, &result_col_id));
    DCHECK_GE(result_col_id, 0);
    auto column = block->get_by_position(result_col_id).column->convert_to_full_column_if_const();
    dst_column->insert_range_from(*column, 0, length);
    return Status::OK();
}

void AnalyticSinkLocalState::_reset_agg_status() {
    for (size_t i = 0; i < _agg_functions_size; ++i) {
        _agg_functions[i]->reset(
                _fn_place_ptr +
                _parent->cast<AnalyticSinkOperatorX>()._offsets_of_aggregate_states[i]);
    }
}

void AnalyticSinkLocalState::_create_agg_status() {
    for (size_t i = 0; i < _agg_functions_size; ++i) {
        try {
            _agg_functions[i]->create(
                    _fn_place_ptr +
                    _parent->cast<AnalyticSinkOperatorX>()._offsets_of_aggregate_states[i]);
        } catch (...) {
            for (int j = 0; j < i; ++j) {
                _agg_functions[j]->destroy(
                        _fn_place_ptr +
                        _parent->cast<AnalyticSinkOperatorX>()._offsets_of_aggregate_states[j]);
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
        _agg_functions[i]->destroy(
                _fn_place_ptr +
                _parent->cast<AnalyticSinkOperatorX>()._offsets_of_aggregate_states[i]);
    }
}

template class DataSinkOperatorX<AnalyticSinkLocalState>;

} // namespace doris::pipeline
