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

#include "analytic_source_operator.h"

#include <string>

#include "pipeline/exec/operator.h"
#include "vec/columns/column_nullable.h"
#include "vec/exprs/vectorized_agg_fn.h"

namespace doris::pipeline {

AnalyticLocalState::AnalyticLocalState(RuntimeState* state, OperatorXBase* parent)
        : PipelineXLocalState<AnalyticSharedState>(state, parent),
          _output_block_index(0),
          _window_end_position(0),
          _next_partition(false),
          _rows_start_offset(0),
          _rows_end_offset(0),
          _fn_place_ptr(nullptr),
          _agg_functions_size(0),
          _agg_functions_created(false) {}

//_partition_by_columns,_order_by_columns save in blocks, so if need to calculate the boundary, may find in which blocks firstly
BlockRowPos AnalyticLocalState::_compare_row_to_find_end(int idx, BlockRowPos start,
                                                         BlockRowPos end, bool need_check_first) {
    auto& shared_state = *_shared_state;
    int64_t start_init_row_num = start.row_num;
    vectorized::ColumnPtr start_column =
            shared_state.input_blocks[start.block_num].get_by_position(idx).column;
    vectorized::ColumnPtr start_next_block_column = start_column;

    DCHECK_LE(start.block_num, end.block_num);
    DCHECK_LE(start.block_num, shared_state.input_blocks.size() - 1);
    int64_t start_block_num = start.block_num;
    int64_t end_block_num = end.block_num;
    int64_t mid_blcok_num = end.block_num;
    // To fix this problem: https://github.com/apache/doris/issues/15951
    // in this case, the partition by column is last row of block, so it's pointed to a new block at row = 0, range is: [left, right)
    // From the perspective of order by column, the two values are exactly equal.
    // so the range will be get wrong because it's compare_at == 0 with next block at row = 0
    if (need_check_first && end.block_num > 0 && end.row_num == 0) {
        end.block_num--;
        end_block_num--;
        end.row_num = shared_state.input_blocks[end_block_num].rows();
    }
    //binary search find in which block
    while (start_block_num < end_block_num) {
        mid_blcok_num = (start_block_num + end_block_num + 1) >> 1;
        start_next_block_column =
                shared_state.input_blocks[mid_blcok_num].get_by_position(idx).column;
        //Compares (*this)[n] and rhs[m], this: start[init_row]  rhs: mid[0]
        if (start_column->compare_at(start_init_row_num, 0, *start_next_block_column, 1) == 0) {
            start_block_num = mid_blcok_num;
        } else {
            end_block_num = mid_blcok_num - 1;
        }
    }

    // have check the start.block_num:  start_column[start_init_row_num] with mid_blcok_num start_next_block_column[0]
    // now next block must not be result, so need check with end_block_num: start_next_block_column[last_row]
    if (end_block_num == mid_blcok_num - 1) {
        start_next_block_column =
                shared_state.input_blocks[end_block_num].get_by_position(idx).column;
        int64_t block_size = shared_state.input_blocks[end_block_num].rows();
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
        start_column = shared_state.input_blocks[start.block_num].get_by_position(idx).column;
    }
    //binary search, set start and end pos
    int64_t start_pos = start_init_row_num;
    int64_t end_pos = shared_state.input_blocks[start.block_num].rows();
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

BlockRowPos AnalyticLocalState::_get_partition_by_end() {
    auto& shared_state = *_shared_state;
    if (shared_state.current_row_position <
        shared_state.partition_by_end.pos) { //still have data, return partition_by_end directly
        return shared_state.partition_by_end;
    }

    if (shared_state.partition_by_eq_expr_ctxs.empty() ||
        (shared_state.input_total_rows == 0)) { //no partition_by, the all block is end
        return shared_state.all_block_end;
    }

    BlockRowPos cal_end = shared_state.all_block_end;
    for (size_t i = 0; i < shared_state.partition_by_eq_expr_ctxs.size();
         ++i) { //have partition_by, binary search the partiton end
        cal_end = _compare_row_to_find_end(shared_state.partition_by_column_idxs[i],
                                           shared_state.partition_by_end, cal_end);
    }
    cal_end.pos = shared_state.input_block_first_row_positions[cal_end.block_num] + cal_end.row_num;
    return cal_end;
}

bool AnalyticLocalState::_whether_need_next_partition(BlockRowPos& found_partition_end) {
    auto& shared_state = *_shared_state;
    if (shared_state.input_eos ||
        (shared_state.current_row_position <
         shared_state.partition_by_end.pos)) { //now still have partition data
        return false;
    }
    if ((shared_state.partition_by_eq_expr_ctxs.empty() && !shared_state.input_eos) ||
        (found_partition_end.pos == 0)) { //no partition, get until fetch to EOS
        return true;
    }
    if (!shared_state.partition_by_eq_expr_ctxs.empty() &&
        found_partition_end.pos == shared_state.all_block_end.pos &&
        !shared_state.input_eos) { //current partition data calculate done
        return true;
    }
    return false;
}

Status AnalyticLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(PipelineXLocalState<AnalyticSharedState>::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _blocks_memory_usage =
            profile()->AddHighWaterMarkCounter("Blocks", TUnit::BYTES, "MemoryUsage", 1);
    _evaluation_timer = ADD_TIMER(profile(), "EvaluationTime");
    return Status::OK();
}

Status AnalyticLocalState::open(RuntimeState* state) {
    RETURN_IF_ERROR(PipelineXLocalState<AnalyticSharedState>::open(state));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    _agg_arena_pool = std::make_unique<vectorized::Arena>();

    auto& p = _parent->cast<AnalyticSourceOperatorX>();
    _agg_functions_size = p._agg_functions.size();

    _agg_functions.resize(p._agg_functions.size());
    for (size_t i = 0; i < _agg_functions.size(); i++) {
        _agg_functions[i] = p._agg_functions[i]->clone(state, state->obj_pool());
    }

    _fn_place_ptr = _agg_arena_pool->aligned_alloc(p._total_size_of_aggregate_states,
                                                   p._align_aggregate_states);

    if (!p._has_window) { //haven't set window, Unbounded:  [unbounded preceding,unbounded following]
        _executor.get_next = std::bind<Status>(&AnalyticLocalState::_get_next_for_partition, this,
                                               std::placeholders::_1);

    } else if (p._has_range_window) {
        if (!p._has_window_end) { //haven't set end, so same as PARTITION, [unbounded preceding, unbounded following]
            _executor.get_next = std::bind<Status>(&AnalyticLocalState::_get_next_for_partition,
                                                   this, std::placeholders::_1);

        } else {
            _executor.get_next = std::bind<Status>(&AnalyticLocalState::_get_next_for_range, this,
                                                   std::placeholders::_1);
        }

    } else {
        if (!p._has_window_start &&
            !p._has_window_end) { //haven't set start and end, same as PARTITION
            _executor.get_next = std::bind<Status>(&AnalyticLocalState::_get_next_for_partition,
                                                   this, std::placeholders::_1);

        } else {
            if (p._has_window_start) { //calculate start boundary
                TAnalyticWindowBoundary b = p._window.window_start;
                if (b.__isset.rows_offset_value) { //[offset     ,   ]
                    _rows_start_offset = b.rows_offset_value;
                    if (b.type == TAnalyticWindowBoundaryType::PRECEDING) {
                        _rows_start_offset *= -1; //preceding--> negative
                    }                             //current_row  0
                } else {                          //following    positive
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

            _executor.get_next = std::bind<Status>(&AnalyticLocalState::_get_next_for_rows, this,
                                                   std::placeholders::_1);
        }
    }
    _executor.insert_result =
            std::bind<void>(&AnalyticLocalState::_insert_result_info, this, std::placeholders::_1);
    _executor.execute =
            std::bind<void>(&AnalyticLocalState::_execute_for_win_func, this, std::placeholders::_1,
                            std::placeholders::_2, std::placeholders::_3, std::placeholders::_4);

    _create_agg_status();
    return Status::OK();
}

void AnalyticLocalState::_reset_agg_status() {
    for (size_t i = 0; i < _agg_functions_size; ++i) {
        _agg_functions[i]->reset(
                _fn_place_ptr +
                _parent->cast<AnalyticSourceOperatorX>()._offsets_of_aggregate_states[i]);
    }
}

void AnalyticLocalState::_create_agg_status() {
    for (size_t i = 0; i < _agg_functions_size; ++i) {
        try {
            _agg_functions[i]->create(
                    _fn_place_ptr +
                    _parent->cast<AnalyticSourceOperatorX>()._offsets_of_aggregate_states[i]);
        } catch (...) {
            for (int j = 0; j < i; ++j) {
                _agg_functions[j]->destroy(
                        _fn_place_ptr +
                        _parent->cast<AnalyticSourceOperatorX>()._offsets_of_aggregate_states[j]);
            }
            throw;
        }
    }
    _agg_functions_created = true;
}

void AnalyticLocalState::_destroy_agg_status() {
    if (UNLIKELY(_fn_place_ptr == nullptr || !_agg_functions_created)) {
        return;
    }
    for (size_t i = 0; i < _agg_functions_size; ++i) {
        _agg_functions[i]->destroy(
                _fn_place_ptr +
                _parent->cast<AnalyticSourceOperatorX>()._offsets_of_aggregate_states[i]);
    }
}

void AnalyticLocalState::_execute_for_win_func(int64_t partition_start, int64_t partition_end,
                                               int64_t frame_start, int64_t frame_end) {
    for (size_t i = 0; i < _agg_functions_size; ++i) {
        std::vector<const vectorized::IColumn*> agg_columns;
        for (int j = 0; j < _shared_state->agg_input_columns[i].size(); ++j) {
            agg_columns.push_back(_shared_state->agg_input_columns[i][j].get());
        }
        _agg_functions[i]->function()->add_range_single_place(
                partition_start, partition_end, frame_start, frame_end,
                _fn_place_ptr +
                        _parent->cast<AnalyticSourceOperatorX>()._offsets_of_aggregate_states[i],
                agg_columns.data(), _agg_arena_pool.get());

        // If the end is not greater than the start, the current window should be empty.
        _current_window_empty =
                std::min(frame_end, partition_end) <= std::max(frame_start, partition_start);
    }
}

void AnalyticLocalState::_insert_result_info(int64_t current_block_rows) {
    int64_t current_block_row_pos =
            _shared_state->input_block_first_row_positions[_output_block_index];
    int64_t get_result_start = _shared_state->current_row_position - current_block_row_pos;
    if (_parent->cast<AnalyticSourceOperatorX>()._fn_scope == AnalyticFnScope::PARTITION) {
        int64_t get_result_end =
                std::min<int64_t>(_shared_state->current_row_position + current_block_rows,
                                  _shared_state->partition_by_end.pos);
        _window_end_position =
                std::min<int64_t>(get_result_end - current_block_row_pos, current_block_rows);
        _shared_state->current_row_position += (_window_end_position - get_result_start);
    } else if (_parent->cast<AnalyticSourceOperatorX>()._fn_scope == AnalyticFnScope::RANGE) {
        _window_end_position =
                std::min<int64_t>(_order_by_end.pos - current_block_row_pos, current_block_rows);
        _shared_state->current_row_position += (_window_end_position - get_result_start);
    } else {
        _window_end_position++;
        _shared_state->current_row_position++;
    }

    const auto& offsets_of_aggregate_states =
            _parent->cast<AnalyticSourceOperatorX>()._offsets_of_aggregate_states;
    for (int i = 0; i < _agg_functions_size; ++i) {
        for (int j = get_result_start; j < _window_end_position; ++j) {
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

Status AnalyticLocalState::_get_next_for_rows(size_t current_block_rows) {
    while (_shared_state->current_row_position < _shared_state->partition_by_end.pos &&
           _window_end_position < current_block_rows) {
        int64_t range_start, range_end;
        if (!_parent->cast<AnalyticSourceOperatorX>()._window.__isset.window_start &&
            _parent->cast<AnalyticSourceOperatorX>()._window.window_end.type ==
                    TAnalyticWindowBoundaryType::
                            CURRENT_ROW) { //[preceding, current_row],[current_row, following]
            range_start = _shared_state->current_row_position;
            range_end = _shared_state->current_row_position +
                        1; //going on calculate,add up data, no need to reset state
        } else {
            _reset_agg_status();
            if (!_parent->cast<AnalyticSourceOperatorX>()
                         ._window.__isset
                         .window_start) { //[preceding, offset]        --unbound: [preceding, following]
                range_start = _partition_by_start.pos;
            } else {
                range_start = _shared_state->current_row_position + _rows_start_offset;
            }
            range_end = _shared_state->current_row_position + _rows_end_offset + 1;
        }
        _executor.execute(_partition_by_start.pos, _shared_state->partition_by_end.pos, range_start,
                          range_end);
        _executor.insert_result(current_block_rows);
    }
    return Status::OK();
}

Status AnalyticLocalState::_get_next_for_partition(size_t current_block_rows) {
    if (_next_partition) {
        _executor.execute(_partition_by_start.pos, _shared_state->partition_by_end.pos,
                          _partition_by_start.pos, _shared_state->partition_by_end.pos);
    }
    _executor.insert_result(current_block_rows);
    return Status::OK();
}

Status AnalyticLocalState::_get_next_for_range(size_t current_block_rows) {
    while (_shared_state->current_row_position < _shared_state->partition_by_end.pos &&
           _window_end_position < current_block_rows) {
        if (_shared_state->current_row_position >= _order_by_end.pos) {
            _update_order_by_range();
            _executor.execute(_partition_by_start.pos, _shared_state->partition_by_end.pos,
                              _order_by_start.pos, _order_by_end.pos);
        }
        _executor.insert_result(current_block_rows);
    }
    return Status::OK();
}

void AnalyticLocalState::_update_order_by_range() {
    _order_by_start = _order_by_end;
    _order_by_end = _shared_state->partition_by_end;
    for (size_t i = 0; i < _shared_state->order_by_eq_expr_ctxs.size(); ++i) {
        _order_by_end = _compare_row_to_find_end(_shared_state->ordey_by_column_idxs[i],
                                                 _order_by_start, _order_by_end, true);
    }
    _order_by_start.pos =
            _shared_state->input_block_first_row_positions[_order_by_start.block_num] +
            _order_by_start.row_num;
    _order_by_end.pos = _shared_state->input_block_first_row_positions[_order_by_end.block_num] +
                        _order_by_end.row_num;
    // `_order_by_end` will be assigned to `_order_by_start` next time,
    // so make it a valid position.
    if (_order_by_end.row_num == _shared_state->input_blocks[_order_by_end.block_num].rows()) {
        _order_by_end.block_num++;
        _order_by_end.row_num = 0;
    }
}

void AnalyticLocalState::init_result_columns() {
    if (!_window_end_position) {
        _result_window_columns.resize(_agg_functions_size);
        for (size_t i = 0; i < _agg_functions_size; ++i) {
            _result_window_columns[i] =
                    _agg_functions[i]->data_type()->create_column(); //return type
        }
    }
}

//calculate pos have arrive partition end, so it's needed to init next partition, and update the boundary of partition
bool AnalyticLocalState::init_next_partition(BlockRowPos found_partition_end) {
    if ((_shared_state->current_row_position >= _shared_state->partition_by_end.pos) &&
        ((_shared_state->partition_by_end.pos == 0) ||
         (_shared_state->partition_by_end.pos != found_partition_end.pos))) {
        _partition_by_start = _shared_state->partition_by_end;
        _shared_state->partition_by_end = found_partition_end;
        _shared_state->current_row_position = _partition_by_start.pos;
        _reset_agg_status();
        return true;
    }
    return false;
}

Status AnalyticLocalState::output_current_block(vectorized::Block* block) {
    block->swap(std::move(_shared_state->input_blocks[_output_block_index]));
    _blocks_memory_usage->add(-block->allocated_bytes());
    mem_tracker()->consume(-block->allocated_bytes());
    if (_shared_state->origin_cols.size() < block->columns()) {
        block->erase_not_in(_shared_state->origin_cols);
    }

    DCHECK(_parent->cast<AnalyticSourceOperatorX>()._change_to_nullable_flags.size() ==
           _result_window_columns.size());
    for (size_t i = 0; i < _result_window_columns.size(); ++i) {
        if (_parent->cast<AnalyticSourceOperatorX>()._change_to_nullable_flags[i]) {
            block->insert({make_nullable(std::move(_result_window_columns[i])),
                           make_nullable(_agg_functions[i]->data_type()), ""});
        } else {
            block->insert(
                    {std::move(_result_window_columns[i]), _agg_functions[i]->data_type(), ""});
        }
    }

    _output_block_index++;
    _window_end_position = 0;

    return Status::OK();
}

AnalyticSourceOperatorX::AnalyticSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode,
                                                 int operator_id, const DescriptorTbl& descs)
        : OperatorX<AnalyticLocalState>(pool, tnode, operator_id, descs),
          _window(tnode.analytic_node.window),
          _intermediate_tuple_id(tnode.analytic_node.intermediate_tuple_id),
          _output_tuple_id(tnode.analytic_node.output_tuple_id),
          _has_window(tnode.analytic_node.__isset.window),
          _has_range_window(tnode.analytic_node.window.type == TAnalyticWindowType::RANGE),
          _has_window_start(tnode.analytic_node.window.__isset.window_start),
          _has_window_end(tnode.analytic_node.window.__isset.window_end) {
    _fn_scope = AnalyticFnScope::PARTITION;
    if (tnode.analytic_node.__isset.window &&
        tnode.analytic_node.window.type == TAnalyticWindowType::RANGE) {
        DCHECK(!_window.__isset.window_start) << "RANGE windows must have UNBOUNDED PRECEDING";
        DCHECK(!_window.__isset.window_end ||
               _window.window_end.type == TAnalyticWindowBoundaryType::CURRENT_ROW)
                << "RANGE window end bound must be CURRENT ROW or UNBOUNDED FOLLOWING";

        if (_window.__isset
                    .window_end) { //haven't set end, so same as PARTITION, [unbounded preceding, unbounded following]
            _fn_scope = AnalyticFnScope::RANGE; //range:  [unbounded preceding,current row]
        }

    } else if (tnode.analytic_node.__isset.window) {
        if (_window.__isset.window_start || _window.__isset.window_end) {
            _fn_scope = AnalyticFnScope::ROWS;
        }
    }
}

Status AnalyticSourceOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(OperatorX<AnalyticLocalState>::init(tnode, state));
    const TAnalyticNode& analytic_node = tnode.analytic_node;
    size_t agg_size = analytic_node.analytic_functions.size();

    for (int i = 0; i < agg_size; ++i) {
        vectorized::AggFnEvaluator* evaluator = nullptr;
        RETURN_IF_ERROR(vectorized::AggFnEvaluator::create(
                _pool, analytic_node.analytic_functions[i], {}, &evaluator));
        _agg_functions.emplace_back(evaluator);
    }

    return Status::OK();
}

Status AnalyticSourceOperatorX::get_block(RuntimeState* state, vectorized::Block* block,
                                          bool* eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    if (local_state._shared_state->input_eos &&
        (local_state._output_block_index == local_state._shared_state->input_blocks.size() ||
         local_state._shared_state->input_total_rows == 0)) {
        *eos = true;
        return Status::OK();
    }

    while (!local_state._shared_state->input_eos ||
           local_state._output_block_index < local_state._shared_state->input_blocks.size()) {
        {
            SCOPED_TIMER(local_state._evaluation_timer);
            local_state._shared_state->found_partition_end = local_state._get_partition_by_end();
        }
        if (local_state._refresh_need_more_input()) {
            return Status::OK();
        }
        local_state._next_partition =
                local_state.init_next_partition(local_state._shared_state->found_partition_end);
        local_state.init_result_columns();
        size_t current_block_rows =
                local_state._shared_state->input_blocks[local_state._output_block_index].rows();
        static_cast<void>(local_state._executor.get_next(current_block_rows));
        if (local_state._window_end_position == current_block_rows) {
            break;
        }
    }
    RETURN_IF_ERROR(local_state.output_current_block(block));
    RETURN_IF_ERROR(vectorized::VExprContext::filter_block(local_state._conjuncts, block,
                                                           block->columns()));
    local_state.reached_limit(block, eos);
    return Status::OK();
}

Status AnalyticLocalState::close(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_close_timer);
    if (_closed) {
        return Status::OK();
    }

    _destroy_agg_status();
    _agg_arena_pool = nullptr;

    std::vector<vectorized::MutableColumnPtr> tmp_result_window_columns;
    _result_window_columns.swap(tmp_result_window_columns);
    return PipelineXLocalState<AnalyticSharedState>::close(state);
}

Status AnalyticSourceOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorX<AnalyticLocalState>::prepare(state));
    DCHECK(_child_x->row_desc().is_prefix_of(_row_descriptor));
    _intermediate_tuple_desc = state->desc_tbl().get_tuple_descriptor(_intermediate_tuple_id);
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_output_tuple_id);
    for (size_t i = 0; i < _agg_functions.size(); ++i) {
        SlotDescriptor* intermediate_slot_desc = _intermediate_tuple_desc->slots()[i];
        SlotDescriptor* output_slot_desc = _output_tuple_desc->slots()[i];
        RETURN_IF_ERROR(_agg_functions[i]->prepare(state, _child_x->row_desc(),
                                                   intermediate_slot_desc, output_slot_desc));
        _agg_functions[i]->set_version(state->be_exec_version());
        _change_to_nullable_flags.push_back(output_slot_desc->is_nullable() &&
                                            !_agg_functions[i]->data_type()->is_nullable());
    }

    _offsets_of_aggregate_states.resize(_agg_functions.size());
    for (size_t i = 0; i < _agg_functions.size(); ++i) {
        _offsets_of_aggregate_states[i] = _total_size_of_aggregate_states;
        const auto& agg_function = _agg_functions[i]->function();
        // aggregate states are aligned based on maximum requirement
        _align_aggregate_states = std::max(_align_aggregate_states, agg_function->align_of_data());
        _total_size_of_aggregate_states += agg_function->size_of_data();
        // If not the last aggregate_state, we need pad it so that next aggregate_state will be aligned.
        if (i + 1 < _agg_functions.size()) {
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

Status AnalyticSourceOperatorX::open(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorX<AnalyticLocalState>::open(state));
    for (auto* agg_function : _agg_functions) {
        RETURN_IF_ERROR(agg_function->open(state));
    }
    return Status::OK();
}

} // namespace doris::pipeline
