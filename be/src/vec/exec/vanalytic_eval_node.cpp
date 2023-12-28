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

#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/Metrics_types.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <algorithm>
#include <ostream>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/exception.h"
#include "common/logging.h"
#include "runtime/descriptors.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "vec/columns/column_nullable.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/exprs/vectorized_agg_fn.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
class ObjectPool;
} // namespace doris

namespace doris::vectorized {

VAnalyticEvalNode::VAnalyticEvalNode(ObjectPool* pool, const TPlanNode& tnode,
                                     const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _fn_place_ptr(nullptr),
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
                                               std::placeholders::_1);

    } else if (tnode.analytic_node.window.type == TAnalyticWindowType::RANGE) {
        DCHECK(!_window.__isset.window_start) << "RANGE windows must have UNBOUNDED PRECEDING";
        DCHECK(!_window.__isset.window_end ||
               _window.window_end.type == TAnalyticWindowBoundaryType::CURRENT_ROW)
                << "RANGE window end bound must be CURRENT ROW or UNBOUNDED FOLLOWING";

        if (!_window.__isset
                     .window_end) { //haven't set end, so same as PARTITION, [unbounded preceding, unbounded following]
            _executor.get_next = std::bind<Status>(&VAnalyticEvalNode::_get_next_for_partition,
                                                   this, std::placeholders::_1);

        } else {
            _fn_scope = AnalyticFnScope::RANGE; //range:  [unbounded preceding,current row]
            _executor.get_next = std::bind<Status>(&VAnalyticEvalNode::_get_next_for_range, this,
                                                   std::placeholders::_1);
        }

    } else {
        if (!_window.__isset.window_start &&
            !_window.__isset.window_end) { //haven't set start and end, same as PARTITION
            _executor.get_next = std::bind<Status>(&VAnalyticEvalNode::_get_next_for_partition,
                                                   this, std::placeholders::_1);

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
                                                   std::placeholders::_1);
        }
    }
    _agg_arena_pool = std::make_unique<Arena>();
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
            VExprSPtr expr;
            VExprContextSPtr ctx;
            RETURN_IF_ERROR(VExpr::create_tree_from_thrift(desc.nodes, &node_idx, expr, ctx));
            _agg_expr_ctxs[i].emplace_back(ctx);
        }

        AggFnEvaluator* evaluator = nullptr;
        RETURN_IF_ERROR(
                AggFnEvaluator::create(_pool, analytic_node.analytic_functions[i], {}, &evaluator));
        _agg_functions.emplace_back(evaluator);
        for (size_t j = 0; j < _agg_expr_ctxs[i].size(); ++j) {
            _agg_intput_columns[i][j] = _agg_expr_ctxs[i][j]->root()->data_type()->create_column();
        }
    }

    RETURN_IF_ERROR(
            VExpr::create_expr_trees(analytic_node.partition_exprs, _partition_by_eq_expr_ctxs));
    RETURN_IF_ERROR(VExpr::create_expr_trees(analytic_node.order_by_exprs, _order_by_eq_expr_ctxs));
    _partition_by_column_idxs.resize(_partition_by_eq_expr_ctxs.size());
    _ordey_by_column_idxs.resize(_order_by_eq_expr_ctxs.size());
    _agg_functions_size = _agg_functions.size();
    return Status::OK();
}

Status VAnalyticEvalNode::prepare(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::prepare(state));
    DCHECK(child(0)->row_desc().is_prefix_of(_row_descriptor));

    _memory_usage_counter = ADD_LABEL_COUNTER(runtime_profile(), "MemoryUsage");
    _blocks_memory_usage =
            runtime_profile()->AddHighWaterMarkCounter("Blocks", TUnit::BYTES, "MemoryUsage");
    _evaluation_timer = ADD_TIMER(runtime_profile(), "EvaluationTime");
    SCOPED_TIMER(_evaluation_timer);

    _intermediate_tuple_desc = state->desc_tbl().get_tuple_descriptor(_intermediate_tuple_id);
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_output_tuple_id);
    for (size_t i = 0; i < _agg_functions_size; ++i) {
        SlotDescriptor* intermediate_slot_desc = _intermediate_tuple_desc->slots()[i];
        SlotDescriptor* output_slot_desc = _output_tuple_desc->slots()[i];
        RETURN_IF_ERROR(_agg_functions[i]->prepare(state, child(0)->row_desc(),
                                                   intermediate_slot_desc, output_slot_desc));
        _change_to_nullable_flags.push_back(output_slot_desc->is_nullable() &&
                                            !_agg_functions[i]->data_type()->is_nullable());
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
                return Status::RuntimeError("Logical error: align_of_data is not 2^N");
            }
            /// Extend total_size to next alignment requirement
            /// Add padding by rounding up 'total_size_of_aggregate_states' to be a multiplier of alignment_of_next_state.
            _total_size_of_aggregate_states =
                    (_total_size_of_aggregate_states + alignment_of_next_state - 1) /
                    alignment_of_next_state * alignment_of_next_state;
        }
    }
    _fn_place_ptr = _agg_arena_pool->aligned_alloc(_total_size_of_aggregate_states,
                                                   _align_aggregate_states);
    RETURN_IF_CATCH_EXCEPTION(static_cast<void>(_create_agg_status()));
    _executor.insert_result =
            std::bind<void>(&VAnalyticEvalNode::_insert_result_info, this, std::placeholders::_1);
    _executor.execute =
            std::bind<void>(&VAnalyticEvalNode::_execute_for_win_func, this, std::placeholders::_1,
                            std::placeholders::_2, std::placeholders::_3, std::placeholders::_4);

    for (const auto& ctx : _agg_expr_ctxs) {
        static_cast<void>(VExpr::prepare(ctx, state, child(0)->row_desc()));
    }
    if (!_partition_by_eq_expr_ctxs.empty() || !_order_by_eq_expr_ctxs.empty()) {
        vector<TTupleId> tuple_ids;
        tuple_ids.push_back(child(0)->row_desc().tuple_descriptors()[0]->id());
        tuple_ids.push_back(_buffered_tuple_id);
        RowDescriptor cmp_row_desc(state->desc_tbl(), tuple_ids, vector<bool>(2, false));
        if (!_partition_by_eq_expr_ctxs.empty()) {
            RETURN_IF_ERROR(VExpr::prepare(_partition_by_eq_expr_ctxs, state, cmp_row_desc));
        }
        if (!_order_by_eq_expr_ctxs.empty()) {
            RETURN_IF_ERROR(VExpr::prepare(_order_by_eq_expr_ctxs, state, cmp_row_desc));
        }
    }
    return Status::OK();
}

Status VAnalyticEvalNode::open(RuntimeState* state) {
    RETURN_IF_ERROR(alloc_resource(state));
    RETURN_IF_ERROR(child(0)->open(state));
    return Status::OK();
}

Status VAnalyticEvalNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    release_resource(state);
    return ExecNode::close(state);
}

Status VAnalyticEvalNode::alloc_resource(RuntimeState* state) {
    {
        SCOPED_TIMER(_runtime_profile->total_time_counter());
        RETURN_IF_ERROR(ExecNode::alloc_resource(state));
        RETURN_IF_CANCELLED(state);
    }
    RETURN_IF_ERROR(VExpr::open(_partition_by_eq_expr_ctxs, state));
    RETURN_IF_ERROR(VExpr::open(_order_by_eq_expr_ctxs, state));
    for (size_t i = 0; i < _agg_functions_size; ++i) {
        RETURN_IF_ERROR(VExpr::open(_agg_expr_ctxs[i], state));
    }
    for (auto* agg_function : _agg_functions) {
        RETURN_IF_ERROR(agg_function->open(state));
    }
    return Status::OK();
}

Status VAnalyticEvalNode::pull(doris::RuntimeState* /*state*/, vectorized::Block* output_block,
                               bool* eos) {
    SCOPED_TIMER(_exec_timer);
    if (_input_eos && (_output_block_index == _input_blocks.size() || _input_total_rows == 0)) {
        *eos = true;
        return Status::OK();
    }

    while (!_input_eos || _output_block_index < _input_blocks.size()) {
        _found_partition_end = _get_partition_by_end();
        _need_more_input = whether_need_next_partition(_found_partition_end);
        if (_need_more_input) {
            return Status::OK();
        }
        _next_partition = _init_next_partition(_found_partition_end);
        static_cast<void>(_init_result_columns());
        size_t current_block_rows = _input_blocks[_output_block_index].rows();
        static_cast<void>(_executor.get_next(current_block_rows));
        if (_window_end_position == current_block_rows) {
            break;
        }
    }
    RETURN_IF_ERROR(_output_current_block(output_block));
    RETURN_IF_ERROR(VExprContext::filter_block(_conjuncts, output_block, output_block->columns()));
    reached_limit(output_block, eos);
    return Status::OK();
}

void VAnalyticEvalNode::release_resource(RuntimeState* state) {
    SCOPED_TIMER(_exec_timer);
    if (is_closed()) {
        return;
    }

    static_cast<void>(_destroy_agg_status());
    _release_mem();
    return ExecNode::release_resource(state);
}

//TODO: maybe could have better strategy, not noly when need data to sink data
//even could get some resources in advance as soon as possible
bool VAnalyticEvalNode::can_write() {
    return _need_more_input;
}

bool VAnalyticEvalNode::can_read() {
    if (_need_more_input) {
        return false;
    }
    return true;
}

Status VAnalyticEvalNode::get_next(RuntimeState* state, vectorized::Block* block, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_CANCELLED(state);

    if (_input_eos && _output_block_index == _input_blocks.size()) {
        *eos = true;
        return Status::OK();
    }

    while (!_input_eos || _output_block_index < _input_blocks.size()) {
        RETURN_IF_ERROR(_consumed_block_and_init_partition(state, &_next_partition, eos));
        if (*eos) {
            return Status::OK();
        }
        size_t current_block_rows = _input_blocks[_output_block_index].rows();
        RETURN_IF_ERROR(_executor.get_next(current_block_rows));
        if (_window_end_position == current_block_rows) {
            break;
        }
    }

    SCOPED_TIMER(_exec_timer);
    RETURN_IF_ERROR(_output_current_block(block));
    RETURN_IF_ERROR(VExprContext::filter_block(_conjuncts, block, block->columns()));
    reached_limit(block, eos);
    return Status::OK();
}

Status VAnalyticEvalNode::_get_next_for_partition(size_t current_block_rows) {
    if (_next_partition) {
        _executor.execute(_partition_by_start.pos, _partition_by_end.pos, _partition_by_start.pos,
                          _partition_by_end.pos);
    }
    _executor.insert_result(current_block_rows);
    return Status::OK();
}

Status VAnalyticEvalNode::_get_next_for_range(size_t current_block_rows) {
    while (_current_row_position < _partition_by_end.pos &&
           _window_end_position < current_block_rows) {
        if (_current_row_position >= _order_by_end.pos) {
            _update_order_by_range();
            _executor.execute(_order_by_start.pos, _order_by_end.pos, _order_by_start.pos,
                              _order_by_end.pos);
        }
        _executor.insert_result(current_block_rows);
    }
    return Status::OK();
}

Status VAnalyticEvalNode::_get_next_for_rows(size_t current_block_rows) {
    while (_current_row_position < _partition_by_end.pos &&
           _window_end_position < current_block_rows) {
        int64_t range_start, range_end;
        if (!_window.__isset.window_start &&
            _window.window_end.type ==
                    TAnalyticWindowBoundaryType::
                            CURRENT_ROW) { //[preceding, current_row],[current_row, following]
            range_start = _current_row_position;
            range_end = _current_row_position +
                        1; //going on calculate,add up data, no need to reset state
        } else {
            static_cast<void>(_reset_agg_status());
            if (!_window.__isset
                         .window_start) { //[preceding, offset]        --unbound: [preceding, following]
                range_start = _partition_by_start.pos;
            } else {
                range_start = _current_row_position + _rows_start_offset;
            }
            range_end = _current_row_position + _rows_end_offset + 1;
        }
        _executor.execute(_partition_by_start.pos, _partition_by_end.pos, range_start, range_end);
        _executor.insert_result(current_block_rows);
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
BlockRowPos VAnalyticEvalNode::_compare_row_to_find_end(int idx, BlockRowPos start, BlockRowPos end,
                                                        bool need_check_first) {
    int64_t start_init_row_num = start.row_num;
    ColumnPtr start_column = _input_blocks[start.block_num].get_by_position(idx).column;
    ColumnPtr start_next_block_column = start_column;

    DCHECK_LE(start.block_num, end.block_num);
    DCHECK_LE(start.block_num, _input_blocks.size() - 1);
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
        end.row_num = _input_blocks[end_block_num].rows();
    }
    //binary search find in which block
    while (start_block_num < end_block_num) {
        mid_blcok_num = (start_block_num + end_block_num + 1) >> 1;
        start_next_block_column = _input_blocks[mid_blcok_num].get_by_position(idx).column;
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
        RETURN_IF_ERROR(_children[0]->get_next_after_projects(
                state, &block, &_input_eos,
                std::bind((Status(ExecNode::*)(RuntimeState*, vectorized::Block*, bool*)) &
                                  ExecNode::get_next,
                          _children[0], std::placeholders::_1, std::placeholders::_2,
                          std::placeholders::_3)));
    } while (!_input_eos && block.rows() == 0);

    RETURN_IF_ERROR(sink(state, &block, _input_eos));
    return Status::OK();
}

Status VAnalyticEvalNode::sink(doris::RuntimeState* /*state*/, vectorized::Block* input_block,
                               bool eos) {
    SCOPED_TIMER(_exec_timer);
    _input_eos = eos;
    if (_input_eos && input_block->rows() == 0) {
        _need_more_input = false;
        return Status::OK();
    }

    input_block_first_row_positions.emplace_back(_input_total_rows);
    size_t block_rows = input_block->rows();
    _input_total_rows += block_rows;
    _all_block_end.block_num = _input_blocks.size();
    _all_block_end.row_num = block_rows;
    _all_block_end.pos = _input_total_rows;

    if (_origin_cols
                .empty()) { //record origin columns, maybe be after this, could cast some column but no need to save
        for (int c = 0; c < input_block->columns(); ++c) {
            _origin_cols.emplace_back(c);
        }
    }

    for (size_t i = 0; i < _agg_functions_size;
         ++i) { //insert _agg_intput_columns, execute calculate for its
        for (size_t j = 0; j < _agg_expr_ctxs[i].size(); ++j) {
            RETURN_IF_ERROR(_insert_range_column(input_block, _agg_expr_ctxs[i][j],
                                                 _agg_intput_columns[i][j].get(), block_rows));
        }
    }
    //record column idx in block
    for (size_t i = 0; i < _partition_by_eq_expr_ctxs.size(); ++i) {
        int result_col_id = -1;
        RETURN_IF_ERROR(_partition_by_eq_expr_ctxs[i]->execute(input_block, &result_col_id));
        DCHECK_GE(result_col_id, 0);
        _partition_by_column_idxs[i] = result_col_id;
    }

    for (size_t i = 0; i < _order_by_eq_expr_ctxs.size(); ++i) {
        int result_col_id = -1;
        RETURN_IF_ERROR(_order_by_eq_expr_ctxs[i]->execute(input_block, &result_col_id));
        DCHECK_GE(result_col_id, 0);
        _ordey_by_column_idxs[i] = result_col_id;
    }

    mem_tracker()->consume(input_block->allocated_bytes());
    _blocks_memory_usage->add(input_block->allocated_bytes());

    //TODO: if need improvement, the is a tips to maintain a free queue,
    //so the memory could reuse, no need to new/delete again;
    _input_blocks.emplace_back(std::move(*input_block));
    _found_partition_end = _get_partition_by_end();
    _need_more_input = whether_need_next_partition(_found_partition_end);
    return Status::OK();
}

Status VAnalyticEvalNode::_insert_range_column(vectorized::Block* block,
                                               const VExprContextSPtr& expr, IColumn* dst_column,
                                               size_t length) {
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
        static_cast<void>(_reset_agg_status());
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
            if (!_agg_functions[i]->function()->get_return_type()->is_nullable() &&
                _result_window_columns[i]->is_nullable()) {
                if (_current_window_empty) {
                    _result_window_columns[i]->insert_default();
                } else {
                    auto* dst = assert_cast<ColumnNullable*>(_result_window_columns[i].get());
                    dst->get_null_map_data().push_back(0);
                    _agg_functions[i]->insert_result_info(
                            _fn_place_ptr + _offsets_of_aggregate_states[i],
                            &dst->get_nested_column());
                }
                continue;
            }
            _agg_functions[i]->insert_result_info(_fn_place_ptr + _offsets_of_aggregate_states[i],
                                                  _result_window_columns[i].get());
        }
    }
}

Status VAnalyticEvalNode::_output_current_block(Block* block) {
    block->swap(std::move(_input_blocks[_output_block_index]));
    _blocks_memory_usage->add(-block->allocated_bytes());
    mem_tracker()->consume(-block->allocated_bytes());
    if (_origin_cols.size() < block->columns()) {
        block->erase_not_in(_origin_cols);
    }

    DCHECK(_change_to_nullable_flags.size() == _result_window_columns.size());
    for (size_t i = 0; i < _result_window_columns.size(); ++i) {
        if (_change_to_nullable_flags[i]) {
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

//now is execute for lead/lag row_number/rank/dense_rank/ntile functions
//sum min max count avg first_value last_value functions
void VAnalyticEvalNode::_execute_for_win_func(int64_t partition_start, int64_t partition_end,
                                              int64_t frame_start, int64_t frame_end) {
    for (size_t i = 0; i < _agg_functions_size; ++i) {
        std::vector<const IColumn*> _agg_columns;
        for (int j = 0; j < _agg_intput_columns[i].size(); ++j) {
            _agg_columns.push_back(_agg_intput_columns[i][j].get());
        }
        _agg_functions[i]->function()->add_range_single_place(
                partition_start, partition_end, frame_start, frame_end,
                _fn_place_ptr + _offsets_of_aggregate_states[i], _agg_columns.data(), nullptr);
    }

    // If the end is not greater than the start, the current window should be empty.
    _current_window_empty =
            std::min(frame_end, partition_end) <= std::max(frame_start, partition_start);
}

//binary search for range to calculate peer group
void VAnalyticEvalNode::_update_order_by_range() {
    _order_by_start = _order_by_end;
    _order_by_end = _partition_by_end;
    for (size_t i = 0; i < _order_by_eq_expr_ctxs.size(); ++i) {
        _order_by_end = _compare_row_to_find_end(_ordey_by_column_idxs[i], _order_by_start,
                                                 _order_by_end, true);
    }
    _order_by_start.pos =
            input_block_first_row_positions[_order_by_start.block_num] + _order_by_start.row_num;
    _order_by_end.pos =
            input_block_first_row_positions[_order_by_end.block_num] + _order_by_end.row_num;
    // `_order_by_end` will be assigned to `_order_by_start` next time,
    // so make it a valid position.
    if (_order_by_end.row_num == _input_blocks[_order_by_end.block_num].rows()) {
        _order_by_end.block_num++;
        _order_by_end.row_num = 0;
    }
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
    return Status::OK();
}

Status VAnalyticEvalNode::_destroy_agg_status() {
    if (UNLIKELY(_fn_place_ptr == nullptr || !_agg_functions_created)) {
        return Status::OK();
    }
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

void VAnalyticEvalNode::_release_mem() {
    _agg_arena_pool = nullptr;

    std::vector<Block> tmp_input_blocks;
    _input_blocks.swap(tmp_input_blocks);

    std::vector<std::vector<MutableColumnPtr>> tmp_agg_intput_columns;
    _agg_intput_columns.swap(tmp_agg_intput_columns);

    std::vector<MutableColumnPtr> tmp_result_window_columns;
    _result_window_columns.swap(tmp_result_window_columns);
}

} // namespace doris::vectorized
