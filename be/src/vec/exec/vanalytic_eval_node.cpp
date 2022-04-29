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

#include "vec/exec/vanalytic_eval_node.h"

#include "exprs/agg_fn_evaluator.h"
#include "exprs/anyval_util.h"
#include "runtime/descriptors.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "udf/udf_internal.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {

VAnalyticEvalNode::VAnalyticEvalNode(ObjectPool* pool, const TPlanNode& tnode,
                                     const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _intermediate_tuple_id(tnode.analytic_node.intermediate_tuple_id),
          _output_tuple_id(tnode.analytic_node.output_tuple_id),
          _window(tnode.analytic_node.window) {
    if (tnode.analytic_node.__isset.buffered_tuple_id) {
        _buffered_tuple_id = tnode.analytic_node.buffered_tuple_id;
    }

    _fn_scope = AnalyticFnScope::PARTITION;
    if (!tnode.analytic_node.__isset
                 .window) { //haven't set window, Unbounded:  [unbounded preceding,unbounded following]
        _executor.get_next = std::bind<Status>(&VAnalyticEvalNode::_get_next_for_partition, this,
                                               std::placeholders::_1, std::placeholders::_2,
                                               std::placeholders::_3);

    } else if (tnode.analytic_node.window.type == TAnalyticWindowType::RANGE) {
        DCHECK(!_window.__isset.window_start) << "RANGE windows must have UNBOUNDED PRECEDING";
        DCHECK(!_window.__isset.window_end ||
               _window.window_end.type == TAnalyticWindowBoundaryType::CURRENT_ROW)
                << "RANGE window end bound must be CURRENT ROW or UNBOUNDED FOLLOWING";

        if (!_window.__isset
                     .window_end) { //haven't set end, so same as PARTITION, [unbounded preceding, unbounded following]
            _executor.get_next = std::bind<Status>(&VAnalyticEvalNode::_get_next_for_partition,
                                                   this, std::placeholders::_1,
                                                   std::placeholders::_2, std::placeholders::_3);
        } else {
            _fn_scope = AnalyticFnScope::RANGE; //range:  [unbounded preceding,current row]
            _executor.get_next = std::bind<Status>(&VAnalyticEvalNode::_get_next_for_range, this,
                                                   std::placeholders::_1, std::placeholders::_2,
                                                   std::placeholders::_3);
        }

    } else {
        if (!_window.__isset.window_start &&
            !_window.__isset.window_end) { //haven't set start and end, same as PARTITION
            _executor.get_next = std::bind<Status>(&VAnalyticEvalNode::_get_next_for_partition,
                                                   this, std::placeholders::_1,
                                                   std::placeholders::_2, std::placeholders::_3);
        } else {
            if (_window.__isset.window_start) { //calculate start boundary
                TAnalyticWindowBoundary b = _window.window_start;
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

            if (_window.__isset.window_end) { //calculate end boundary
                TAnalyticWindowBoundary b = _window.window_end;
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

            _fn_scope = AnalyticFnScope::ROWS;
            _executor.get_next = std::bind<Status>(&VAnalyticEvalNode::_get_next_for_rows, this,
                                                   std::placeholders::_1, std::placeholders::_2,
                                                   std::placeholders::_3);
        }
    }
    VLOG_ROW << "tnode=" << apache::thrift::ThriftDebugString(tnode)
             << " AnalyticFnScope: " << _fn_scope;
}

Status VAnalyticEvalNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    const TAnalyticNode& analytic_node = tnode.analytic_node;
    size_t agg_size = analytic_node.analytic_functions.size();
    _agg_expr_ctxs.resize(agg_size);
    _agg_intput_columns.resize(agg_size);

    for (int i = 0; i < agg_size; ++i) {
        const TExpr& desc = analytic_node.analytic_functions[i];
        int node_idx = 0;
        _agg_intput_columns[i].resize(desc.nodes[0].num_children);
        for (int j = 0; j < desc.nodes[0].num_children; ++j) {
            ++node_idx;
            VExpr* expr = nullptr;
            VExprContext* ctx = nullptr;
            RETURN_IF_ERROR(VExpr::create_tree_from_thrift(_pool, desc.nodes, nullptr, &node_idx,
                                                           &expr, &ctx));
            _agg_expr_ctxs[i].emplace_back(ctx);
        }

        AggFnEvaluator* evaluator = nullptr;
        RETURN_IF_ERROR(
                AggFnEvaluator::create(_pool, analytic_node.analytic_functions[i], &evaluator));
        _agg_functions.emplace_back(evaluator);
        for (size_t j = 0; j < _agg_expr_ctxs[i].size(); ++j) {
            _agg_intput_columns[i][j] = _agg_expr_ctxs[i][j]->root()->data_type()->create_column();
        }
    }

    RETURN_IF_ERROR(VExpr::create_expr_trees(_pool, analytic_node.partition_exprs,
                                             &_partition_by_eq_expr_ctxs));
    RETURN_IF_ERROR(
            VExpr::create_expr_trees(_pool, analytic_node.order_by_exprs, &_order_by_eq_expr_ctxs));
    _partition_by_column_idxs.resize(_partition_by_eq_expr_ctxs.size());
    _ordey_by_column_idxs.resize(_order_by_eq_expr_ctxs.size());
    _agg_functions_size = _agg_functions.size();
    return Status::OK();
}

Status VAnalyticEvalNode::prepare(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::prepare(state));
    SCOPED_SWITCH_TASK_THREAD_LOCAL_MEM_TRACKER(mem_tracker());
    DCHECK(child(0)->row_desc().is_prefix_of(row_desc()));
    _mem_pool.reset(new MemPool(mem_tracker().get()));
    _evaluation_timer = ADD_TIMER(runtime_profile(), "EvaluationTime");
    SCOPED_TIMER(_evaluation_timer);

    _intermediate_tuple_desc = state->desc_tbl().get_tuple_descriptor(_intermediate_tuple_id);
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_output_tuple_id);
    for (size_t i = 0; i < _agg_functions_size; ++i) {
        SlotDescriptor* intermediate_slot_desc = _intermediate_tuple_desc->slots()[i];
        SlotDescriptor* output_slot_desc = _output_tuple_desc->slots()[i];
        RETURN_IF_ERROR(_agg_functions[i]->prepare(state, child(0)->row_desc(), _mem_pool.get(),
                                                   intermediate_slot_desc, output_slot_desc,
                                                   mem_tracker()));
    }

    _offsets_of_aggregate_states.resize(_agg_functions_size);
    for (size_t i = 0; i < _agg_functions_size; ++i) {
        _offsets_of_aggregate_states[i] = _total_size_of_aggregate_states;
        const auto& agg_function = _agg_functions[i]->function();
        // aggreate states are aligned based on maximum requirement
        _align_aggregate_states = std::max(_align_aggregate_states, agg_function->align_of_data());
        _total_size_of_aggregate_states += agg_function->size_of_data();
        // If not the last aggregate_state, we need pad it so that next aggregate_state will be aligned.
        if (i + 1 < _agg_functions_size) {
            size_t alignment_of_next_state = _agg_functions[i + 1]->function()->align_of_data();
            if ((alignment_of_next_state & (alignment_of_next_state - 1)) != 0) {
                return Status::RuntimeError(fmt::format("Logical error: align_of_data is not 2^N"));
            }
            /// Extend total_size to next alignment requirement
            /// Add padding by rounding up 'total_size_of_aggregate_states' to be a multiplier of alignment_of_next_state.
            _total_size_of_aggregate_states =
                    (_total_size_of_aggregate_states + alignment_of_next_state - 1) /
                    alignment_of_next_state * alignment_of_next_state;
        }
    }
    _fn_place_ptr =
            _agg_arena_pool.aligned_alloc(_total_size_of_aggregate_states, _align_aggregate_states);
    _create_agg_status();
    _executor.insert_result =
            std::bind<void>(&VAnalyticEvalNode::_insert_result_info, this, std::placeholders::_1);
    _executor.execute =
            std::bind<void>(&VAnalyticEvalNode::_execute_for_win_func, this, std::placeholders::_1,
                            std::placeholders::_2, std::placeholders::_3, std::placeholders::_4);

    for (const auto& ctx : _agg_expr_ctxs) {
        VExpr::prepare(ctx, state, child(0)->row_desc(), expr_mem_tracker());
    }
    if (!_partition_by_eq_expr_ctxs.empty() || !_order_by_eq_expr_ctxs.empty()) {
        vector<TTupleId> tuple_ids;
        tuple_ids.push_back(child(0)->row_desc().tuple_descriptors()[0]->id());
        tuple_ids.push_back(_buffered_tuple_id);
        RowDescriptor cmp_row_desc(state->desc_tbl(), tuple_ids, vector<bool>(2, false));
        if (!_partition_by_eq_expr_ctxs.empty()) {
            RETURN_IF_ERROR(VExpr::prepare(_partition_by_eq_expr_ctxs, state, cmp_row_desc,
                                           expr_mem_tracker()));
        }
        if (!_order_by_eq_expr_ctxs.empty()) {
            RETURN_IF_ERROR(VExpr::prepare(_order_by_eq_expr_ctxs, state, cmp_row_desc,
                                           expr_mem_tracker()));
        }
    }
    return Status::OK();
}

Status VAnalyticEvalNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    SCOPED_SWITCH_TASK_THREAD_LOCAL_MEM_TRACKER(mem_tracker());
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(child(0)->open(state));
    RETURN_IF_ERROR(VExpr::open(_partition_by_eq_expr_ctxs, state));
    RETURN_IF_ERROR(VExpr::open(_order_by_eq_expr_ctxs, state));
    for (size_t i = 0; i < _agg_functions_size; ++i) {
        RETURN_IF_ERROR(VExpr::open(_agg_expr_ctxs[i], state));
    }
    return Status::OK();
}

Status VAnalyticEvalNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    ExecNode::close(state);
    _destory_agg_status();
    return Status::OK();
}

Status VAnalyticEvalNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    return Status::NotSupported("Not Implemented VAnalyticEvalNode::get_next.");
}

Status VAnalyticEvalNode::get_next(RuntimeState* state, vectorized::Block* block, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    SCOPED_SWITCH_TASK_THREAD_LOCAL_EXISTED_MEM_TRACKER(mem_tracker());
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    RETURN_IF_CANCELLED(state);

    if (_input_eos && _output_block_index == _input_blocks.size()) {
        *eos = true;
        return Status::OK();
    }
    RETURN_IF_ERROR(_executor.get_next(state, block, eos));

    RETURN_IF_ERROR(VExprContext::filter_block(_vconjunct_ctx_ptr, block, block->columns()));
    reached_limit(block, eos);
    return Status::OK();
}

Status VAnalyticEvalNode::_get_next_for_partition(RuntimeState* state, Block* block, bool* eos) {
    while (!_input_eos || _output_block_index < _input_blocks.size()) {
        bool next_partition = false;
        RETURN_IF_ERROR(_consumed_block_and_init_partition(state, &next_partition, eos));
        if (*eos) {
            break;
        }

        size_t current_block_rows = _input_blocks[_output_block_index].rows();
        if (next_partition) {
            _executor.execute(_partition_by_start, _partition_by_end, _partition_by_start,
                              _partition_by_end);
        }
        _executor.insert_result(current_block_rows);
        if (_window_end_position == current_block_rows) {
            return _output_current_block(block);
        }
    }
    return Status::OK();
}

Status VAnalyticEvalNode::_get_next_for_range(RuntimeState* state, Block* block, bool* eos) {
    while (!_input_eos || _output_block_index < _input_blocks.size()) {
        bool next_partition = false;
        RETURN_IF_ERROR(_consumed_block_and_init_partition(state, &next_partition, eos));
        if (*eos) {
            break;
        }

        size_t current_block_rows = _input_blocks[_output_block_index].rows();
        while (_current_row_position < _partition_by_end.pos &&
               _window_end_position < current_block_rows) {
            if (_current_row_position >= _order_by_end.pos) {
                _update_order_by_range();
                _executor.execute(_order_by_start, _order_by_end, _order_by_start, _order_by_end);
            }
            _executor.insert_result(current_block_rows);
        }
        if (_window_end_position == current_block_rows) {
            return _output_current_block(block);
        }
    }
    return Status::OK();
}

Status VAnalyticEvalNode::_get_next_for_rows(RuntimeState* state, Block* block, bool* eos) {
    while (!_input_eos || _output_block_index < _input_blocks.size()) {
        bool next_partition = false;
        RETURN_IF_ERROR(_consumed_block_and_init_partition(state, &next_partition, eos));
        if (*eos) {
            break;
        }

        size_t current_block_rows = _input_blocks[_output_block_index].rows();
        while (_current_row_position < _partition_by_end.pos &&
               _window_end_position < current_block_rows) {
            BlockRowPos range_start, range_end;
            if (!_window.__isset.window_start &&
                _window.window_end.type ==
                        TAnalyticWindowBoundaryType::
                                CURRENT_ROW) { //[preceding, current_row],[current_row, following]
                range_start.pos = _current_row_position;
                range_end.pos = _current_row_position +
                                1; //going on calculate,add up data, no need to reset state
            } else {
                _reset_agg_status();
                if (!_window.__isset
                             .window_start) { //[preceding, offset]        --unbound: [preceding, following]
                    range_start.pos = _partition_by_start.pos;
                } else {
                    range_start.pos = _current_row_position + _rows_start_offset;
                }
                range_end.pos = _current_row_position + _rows_end_offset + 1;
            }
            _executor.execute(_partition_by_start, _partition_by_end, range_start, range_end);
            _executor.insert_result(current_block_rows);
        }
        if (_window_end_position == current_block_rows) {
            return _output_current_block(block);
        }
    }
    return Status::OK();
}

Status VAnalyticEvalNode::_consumed_block_and_init_partition(RuntimeState* state,
                                                             bool* next_partition, bool* eos) {
    BlockRowPos found_partition_end = _get_partition_by_end(); //claculate current partition end
    while (whether_need_next_partition(
            found_partition_end)) { //check whether need get next partition, if current partition haven't execute done, return false
        RETURN_IF_ERROR(_fetch_next_block_data(state)); //return true, fetch next block
        found_partition_end = _get_partition_by_end();  //claculate new partition end
    }
    if (_input_eos && _input_total_rows == 0) {
        *eos = true;
        return Status::OK();
    }
    SCOPED_TIMER(_evaluation_timer);
    *next_partition = _init_next_partition(found_partition_end);
    RETURN_IF_ERROR(_init_result_columns());
    return Status::OK();
}

BlockRowPos VAnalyticEvalNode::_get_partition_by_end() {
    SCOPED_TIMER(_evaluation_timer);

    if (_current_row_position <
        _partition_by_end.pos) { //still have data, return partition_by_end directly
        return _partition_by_end;
    }

    if (_partition_by_eq_expr_ctxs.empty() ||
        (_input_total_rows == 0)) { //no partition_by, the all block is end
        return _all_block_end;
    }

    BlockRowPos cal_end = _all_block_end;
    for (size_t i = 0; i < _partition_by_eq_expr_ctxs.size();
         ++i) { //have partition_by, binary search the partiton end
        cal_end =
                _compare_row_to_find_end(_partition_by_column_idxs[i], _partition_by_end, cal_end);
    }
    cal_end.pos = input_block_first_row_positions[cal_end.block_num] + cal_end.row_num;
    return cal_end;
}

//_partition_by_columns,_order_by_columns save in blocks, so if need to calculate the boundary, may find in which blocks firstly
BlockRowPos VAnalyticEvalNode::_compare_row_to_find_end(int idx, BlockRowPos start,
                                                        BlockRowPos end) {
    int64_t start_init_row_num = start.row_num;
    ColumnPtr start_column = _input_blocks[start.block_num].get_by_position(idx).column;
    ColumnPtr start_next_block_column = start_column;

    DCHECK_LE(start.block_num, end.block_num);
    DCHECK_LE(start.block_num, _input_blocks.size() - 1);
    int64_t start_block_num = start.block_num;
    int64_t end_block_num = end.block_num;
    int64_t mid_blcok_num = end.block_num;
    //binary search find in which block
    while (start_block_num < end_block_num) {
        mid_blcok_num = (start_block_num + end_block_num + 1) >> 1;
        start_next_block_column = _input_blocks[mid_blcok_num].get_by_position(idx).column;
        if (start_column->compare_at(start_init_row_num, 0, *start_next_block_column, 1) == 0) {
            start_block_num = mid_blcok_num;
        } else {
            end_block_num = mid_blcok_num - 1;
        }
    }

    if (end_block_num == mid_blcok_num - 1) {
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
    if (start_column.get() != start_next_block_column.get()) {
        start_init_row_num = 0;
        start.block_num = start_block_num;
        start_column = _input_blocks[start.block_num].get_by_position(idx).column;
    }
    //binary search, set start and end pos
    int64_t start_pos = start_init_row_num;
    int64_t end_pos = _input_blocks[start.block_num].rows() - 1;
    if (start.block_num == end.block_num) {
        end_pos = end.row_num;
    }
    while (start_pos < end_pos) {
        int64_t mid_pos = (start_pos + end_pos) >> 1;
        if (start_column->compare_at(start_init_row_num, mid_pos, *start_column, 1))
            end_pos = mid_pos;
        else
            start_pos = mid_pos + 1;
    }
    start.row_num = start_pos; //upadte row num, return the find end
    return start;
}

//according to partition end check whether need next partition data
bool VAnalyticEvalNode::whether_need_next_partition(BlockRowPos found_partition_end) {
    if (_input_eos ||
        (_current_row_position < _partition_by_end.pos)) { //now still have partition data
        return false;
    }
    if ((_partition_by_eq_expr_ctxs.empty() && !_input_eos) ||
        (found_partition_end.pos == 0)) { //no partition, get until fetch to EOS
        return true;
    }
    if (!_partition_by_eq_expr_ctxs.empty() && found_partition_end.pos == _all_block_end.pos &&
        !_input_eos) { //current partition data calculate done
        return true;
    }
    return false;
}

Status VAnalyticEvalNode::_fetch_next_block_data(RuntimeState* state) {
    Block block;
    RETURN_IF_CANCELLED(state);
    do {
        RETURN_IF_ERROR(_children[0]->get_next(state, &block, &_input_eos));
    } while (!_input_eos && block.rows() == 0);

    if (_input_eos && block.rows() == 0) {
        return Status::OK();
    }

    input_block_first_row_positions.emplace_back(_input_total_rows);
    size_t block_rows = block.rows();
    _input_total_rows += block_rows;
    _all_block_end.block_num = _input_blocks.size();
    _all_block_end.row_num = block_rows;
    _all_block_end.pos = _input_total_rows;

    if (_origin_cols
                .empty()) { //record origin columns, maybe be after this, could cast some column but no need to save
        for (int c = 0; c < block.columns(); ++c) {
            _origin_cols.emplace_back(c);
        }
    }

    for (size_t i = 0; i < _agg_functions_size;
         ++i) { //insert _agg_intput_columns, execute calculate for its
        for (size_t j = 0; j < _agg_expr_ctxs[i].size(); ++j) {
            RETURN_IF_ERROR(_insert_range_column(&block, _agg_expr_ctxs[i][j],
                                                 _agg_intput_columns[i][j].get(), block_rows));
        }
    }
    //record column idx in block
    for (size_t i = 0; i < _partition_by_eq_expr_ctxs.size(); ++i) {
        int result_col_id = -1;
        RETURN_IF_ERROR(_partition_by_eq_expr_ctxs[i]->execute(&block, &result_col_id));
        DCHECK_GE(result_col_id, 0);
        _partition_by_column_idxs[i] = result_col_id;
    }

    for (size_t i = 0; i < _order_by_eq_expr_ctxs.size(); ++i) {
        int result_col_id = -1;
        RETURN_IF_ERROR(_order_by_eq_expr_ctxs[i]->execute(&block, &result_col_id));
        DCHECK_GE(result_col_id, 0);
        _ordey_by_column_idxs[i] = result_col_id;
    }
    //TODO: if need improvement, the is a tips to maintain a free queue,
    //so the memory could reuse, no need to new/delete again;
    _input_blocks.emplace_back(std::move(block));
    return Status::OK();
}

Status VAnalyticEvalNode::_insert_range_column(vectorized::Block* block, VExprContext* expr,
                                               IColumn* dst_column, size_t length) {
    int result_col_id = -1;
    RETURN_IF_ERROR(expr->execute(block, &result_col_id));
    DCHECK_GE(result_col_id, 0);
    auto column = block->get_by_position(result_col_id).column->convert_to_full_column_if_const();
    dst_column->insert_range_from(*column, 0, length);
    return Status::OK();
}

//calculate pos have arrive partition end, so it's needed to init next partition, and update the boundary of partition
bool VAnalyticEvalNode::_init_next_partition(BlockRowPos found_partition_end) {
    if ((_current_row_position >= _partition_by_end.pos) &&
        ((_partition_by_end.pos == 0) || (_partition_by_end.pos != found_partition_end.pos))) {
        _partition_by_start = _partition_by_end;
        _partition_by_end = found_partition_end;
        _current_row_position = _partition_by_start.pos;
        _reset_agg_status();
        return true;
    }
    return false;
}

void VAnalyticEvalNode::_insert_result_info(int64_t current_block_rows) {
    int64_t current_block_row_pos = input_block_first_row_positions[_output_block_index];
    int64_t get_result_start = _current_row_position - current_block_row_pos;
    if (_fn_scope == AnalyticFnScope::PARTITION) {
        int64_t get_result_end = std::min<int64_t>(_current_row_position + current_block_rows,
                                                   _partition_by_end.pos);
        _window_end_position =
                std::min<int64_t>(get_result_end - current_block_row_pos, current_block_rows);
        _current_row_position += (_window_end_position - get_result_start);
    } else if (_fn_scope == AnalyticFnScope::RANGE) {
        _window_end_position =
                std::min<int64_t>(_order_by_end.pos - current_block_row_pos, current_block_rows);
        _current_row_position += (_window_end_position - get_result_start);
    } else {
        _window_end_position++;
        _current_row_position++;
    }

    for (int i = 0; i < _agg_functions_size; ++i) {
        for (int j = get_result_start; j < _window_end_position; ++j) {
            _agg_functions[i]->insert_result_info(_fn_place_ptr + _offsets_of_aggregate_states[i],
                                                  _result_window_columns[i].get());
        }
    }
}

Status VAnalyticEvalNode::_output_current_block(Block* block) {
    block->swap(std::move(_input_blocks[_output_block_index]));
    if (_origin_cols.size() < block->columns()) {
        block->erase_not_in(_origin_cols);
    }

    for (size_t i = 0; i < _result_window_columns.size(); ++i) {
        block->insert({std::move(_result_window_columns[i]), _agg_functions[i]->data_type(), ""});
    }

    _output_block_index++;
    _window_end_position = 0;

    return Status::OK();
}

//now is execute for lead/lag row_number/rank/dense_rank functions
//sum min max count avg first_value last_value functions
void VAnalyticEvalNode::_execute_for_win_func(BlockRowPos partition_start,
                                              BlockRowPos partition_end, BlockRowPos frame_start,
                                              BlockRowPos frame_end) {
    for (size_t i = 0; i < _agg_functions_size; ++i) {
        std::vector<const IColumn*> _agg_columns;
        for (int j = 0; j < _agg_intput_columns[i].size(); ++j) {
            _agg_columns.push_back(_agg_intput_columns[i][j].get());
        }
        _agg_functions[i]->function()->add_range_single_place(
                partition_start.pos, partition_end.pos, frame_start.pos, frame_end.pos,
                _fn_place_ptr + _offsets_of_aggregate_states[i], _agg_columns.data(), nullptr);
    }
}

//binary search for range to calculate peer group
void VAnalyticEvalNode::_update_order_by_range() {
    _order_by_start = _order_by_end;
    _order_by_end = _partition_by_end;
    for (size_t i = 0; i < _order_by_eq_expr_ctxs.size(); ++i) {
        _order_by_end =
                _compare_row_to_find_end(_ordey_by_column_idxs[i], _order_by_start, _order_by_end);
    }
    _order_by_start.pos =
            input_block_first_row_positions[_order_by_start.block_num] + _order_by_start.row_num;
    _order_by_end.pos =
            input_block_first_row_positions[_order_by_end.block_num] + _order_by_end.row_num;
}

Status VAnalyticEvalNode::_init_result_columns() {
    if (!_window_end_position) {
        _result_window_columns.resize(_agg_functions_size);
        for (size_t i = 0; i < _agg_functions_size; ++i) {
            _result_window_columns[i] =
                    _agg_functions[i]->data_type()->create_column(); //return type
        }
    }
    return Status::OK();
}

Status VAnalyticEvalNode::_reset_agg_status() {
    for (size_t i = 0; i < _agg_functions_size; ++i) {
        _agg_functions[i]->reset(_fn_place_ptr + _offsets_of_aggregate_states[i]);
    }
    return Status::OK();
}

Status VAnalyticEvalNode::_create_agg_status() {
    for (size_t i = 0; i < _agg_functions_size; ++i) {
        _agg_functions[i]->create(_fn_place_ptr + _offsets_of_aggregate_states[i]);
    }
    return Status::OK();
}

Status VAnalyticEvalNode::_destory_agg_status() {
    for (size_t i = 0; i < _agg_functions_size; ++i) {
        _agg_functions[i]->destroy(_fn_place_ptr + _offsets_of_aggregate_states[i]);
    }
    return Status::OK();
}

std::string VAnalyticEvalNode::debug_string() {
    std::stringstream ss;
    if (_fn_scope == PARTITION) {
        ss << "NO WINDOW";
        return ss.str();
    }
    ss << "{type=";
    if (_fn_scope == RANGE) {
        ss << "RANGE";
    } else {
        ss << "ROWS";
    }
    ss << ", start=";
    if (_window.__isset.window_start) {
        TAnalyticWindowBoundary start = _window.window_start;
        ss << debug_window_bound_string(start);
    } else {
        ss << "UNBOUNDED_PRECEDING";
    }
    ss << ", end=";
    if (_window.__isset.window_end) {
        TAnalyticWindowBoundary end = _window.window_end;
        ss << debug_window_bound_string(end) << "}";
    } else {
        ss << "UNBOUNDED_FOLLOWING";
    }
    return ss.str();
}

std::string VAnalyticEvalNode::debug_window_bound_string(TAnalyticWindowBoundary b) {
    if (b.type == TAnalyticWindowBoundaryType::CURRENT_ROW) {
        return "CURRENT_ROW";
    }
    std::stringstream ss;
    if (b.__isset.rows_offset_value) {
        ss << b.rows_offset_value;
    } else {
        DCHECK(false) << "Range offsets not yet implemented";
    }
    if (b.type == TAnalyticWindowBoundaryType::PRECEDING) {
        ss << " PRECEDING";
    } else {
        DCHECK_EQ(b.type, TAnalyticWindowBoundaryType::FOLLOWING);
        ss << " FOLLOWING";
    }
    return ss.str();
}

} // namespace doris::vectorized
