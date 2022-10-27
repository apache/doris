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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/exec/analytic-eval-node.cc
// and modified by Doris

#include "exec/analytic_eval_node.h"

#include "exprs/agg_fn_evaluator.h"
#include "runtime/descriptors.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "udf/udf_internal.h"

namespace doris {

using doris_udf::BigIntVal;

AnalyticEvalNode::AnalyticEvalNode(ObjectPool* pool, const TPlanNode& tnode,
                                   const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _window(tnode.analytic_node.window),
          _intermediate_tuple_desc(
                  descs.get_tuple_descriptor(tnode.analytic_node.intermediate_tuple_id)),
          _result_tuple_desc(descs.get_tuple_descriptor(tnode.analytic_node.output_tuple_id)),
          _buffered_tuple_desc(nullptr),
          _partition_by_eq_expr_ctx(nullptr),
          _order_by_eq_expr_ctx(nullptr),
          _rows_start_offset(0),
          _rows_end_offset(0),
          _has_first_val_null_offset(false),
          _first_val_null_offset(0),
          _last_result_idx(-1),
          _prev_pool_last_result_idx(-1),
          _prev_pool_last_window_idx(-1),
          _curr_tuple(nullptr),
          _dummy_result_tuple(nullptr),
          _curr_partition_idx(-1),
          _prev_input_row(nullptr),
          _block_mgr_client(nullptr),
          _input_eos(false),
          _evaluation_timer(nullptr) {
    if (tnode.analytic_node.__isset.buffered_tuple_id) {
        _buffered_tuple_desc = descs.get_tuple_descriptor(tnode.analytic_node.buffered_tuple_id);
    }

    if (!tnode.analytic_node.__isset.window) {
        _fn_scope = AnalyticEvalNode::PARTITION;
    } else if (tnode.analytic_node.window.type == TAnalyticWindowType::RANGE) {
        _fn_scope = AnalyticEvalNode::RANGE;
        DCHECK(!_window.__isset.window_start) << "RANGE windows must have UNBOUNDED PRECEDING";
        DCHECK(!_window.__isset.window_end ||
               _window.window_end.type == TAnalyticWindowBoundaryType::CURRENT_ROW)
                << "RANGE window end bound must be CURRENT ROW or UNBOUNDED FOLLOWING";
    } else {
        DCHECK_EQ(tnode.analytic_node.window.type, TAnalyticWindowType::ROWS);
        _fn_scope = AnalyticEvalNode::ROWS;

        if (_window.__isset.window_start) {
            TAnalyticWindowBoundary b = _window.window_start;

            if (b.__isset.rows_offset_value) {
                _rows_start_offset = b.rows_offset_value;

                if (b.type == TAnalyticWindowBoundaryType::PRECEDING) {
                    _rows_start_offset *= -1;
                }
            } else {
                DCHECK_EQ(b.type, TAnalyticWindowBoundaryType::CURRENT_ROW);
                _rows_start_offset = 0;
            }
        }

        if (_window.__isset.window_end) {
            TAnalyticWindowBoundary b = _window.window_end;

            if (b.__isset.rows_offset_value) {
                _rows_end_offset = b.rows_offset_value;

                if (b.type == TAnalyticWindowBoundaryType::PRECEDING) {
                    _rows_end_offset *= -1;
                }
            } else {
                DCHECK_EQ(b.type, TAnalyticWindowBoundaryType::CURRENT_ROW);
                _rows_end_offset = 0;
            }
        }
    }

    VLOG_ROW << "tnode=" << apache::thrift::ThriftDebugString(tnode);
}

Status AnalyticEvalNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    const TAnalyticNode& analytic_node = tnode.analytic_node;
    bool has_lead_fn = false;

    for (int i = 0; i < analytic_node.analytic_functions.size(); ++i) {
        AggFnEvaluator* evaluator = nullptr;
        RETURN_IF_ERROR(AggFnEvaluator::create(_pool, analytic_node.analytic_functions[i], true,
                                               &evaluator));
        _evaluators.push_back(evaluator);
        const TFunction& fn = analytic_node.analytic_functions[i].nodes[0].fn;
        _is_lead_fn.push_back("lead" == fn.name.function_name);
        has_lead_fn = has_lead_fn || _is_lead_fn.back();
    }

    DCHECK(!has_lead_fn || !_window.__isset.window_start);
    DCHECK(_fn_scope != PARTITION || analytic_node.order_by_exprs.empty());
    DCHECK(_window.__isset.window_end || !_window.__isset.window_start)
            << "UNBOUNDED FOLLOWING is only supported with UNBOUNDED PRECEDING.";

    if (analytic_node.__isset.partition_by_eq) {
        DCHECK(analytic_node.__isset.buffered_tuple_id);
        RETURN_IF_ERROR(Expr::create_expr_tree(_pool, analytic_node.partition_by_eq,
                                               &_partition_by_eq_expr_ctx));
    }

    if (analytic_node.__isset.order_by_eq) {
        DCHECK(analytic_node.__isset.buffered_tuple_id);
        RETURN_IF_ERROR(
                Expr::create_expr_tree(_pool, analytic_node.order_by_eq, &_order_by_eq_expr_ctx));
    }

    return Status::OK();
}

Status AnalyticEvalNode::prepare(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::prepare(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());
    DCHECK(child(0)->row_desc().is_prefix_of(row_desc()));
    _child_tuple_desc = child(0)->row_desc().tuple_descriptors()[0];
    _curr_tuple_pool.reset(new MemPool(mem_tracker()));
    _prev_tuple_pool.reset(new MemPool(mem_tracker()));
    _mem_pool.reset(new MemPool(mem_tracker()));

    _evaluation_timer = ADD_TIMER(runtime_profile(), "EvaluationTime");
    DCHECK_EQ(_result_tuple_desc->slots().size(), _evaluators.size());

    for (int i = 0; i < _evaluators.size(); ++i) {
        doris_udf::FunctionContext* ctx;
        RETURN_IF_ERROR(_evaluators[i]->prepare(state, child(0)->row_desc(), _mem_pool.get(),
                                                _intermediate_tuple_desc->slots()[i],
                                                _result_tuple_desc->slots()[i], &ctx));
        _fn_ctxs.push_back(ctx);
        state->obj_pool()->add(ctx);
    }

    if (_partition_by_eq_expr_ctx != nullptr || _order_by_eq_expr_ctx != nullptr) {
        DCHECK(_buffered_tuple_desc != nullptr);
        std::vector<TTupleId> tuple_ids;
        tuple_ids.push_back(child(0)->row_desc().tuple_descriptors()[0]->id());
        tuple_ids.push_back(_buffered_tuple_desc->id());
        RowDescriptor cmp_row_desc(state->desc_tbl(), tuple_ids, std::vector<bool>(2, false));

        if (_partition_by_eq_expr_ctx != nullptr) {
            RETURN_IF_ERROR(_partition_by_eq_expr_ctx->prepare(state, cmp_row_desc));
            //AddExprCtxToFree(_partition_by_eq_expr_ctx);
        }

        if (_order_by_eq_expr_ctx != nullptr) {
            RETURN_IF_ERROR(_order_by_eq_expr_ctx->prepare(state, cmp_row_desc));
            //AddExprCtxToFree(_order_by_eq_expr_ctx);
        }
    }

    _child_tuple_cmp_row = reinterpret_cast<TupleRow*>(_mem_pool->allocate(sizeof(Tuple*) * 2));
    return Status::OK();
}

Status AnalyticEvalNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());
    RETURN_IF_CANCELLED(state);
    //RETURN_IF_ERROR(QueryMaintenance(state));
    RETURN_IF_ERROR(child(0)->open(state));
    RETURN_IF_ERROR(state->block_mgr2()->register_client(2, state, &_block_mgr_client));
    _input_stream.reset(new BufferedTupleStream2(state, child(0)->row_desc(), state->block_mgr2(),
                                                 _block_mgr_client, false, true));
    RETURN_IF_ERROR(_input_stream->init(id(), runtime_profile(), true));

    bool got_read_buffer;
    RETURN_IF_ERROR(_input_stream->prepare_for_read(true, &got_read_buffer));
    if (!got_read_buffer) {
        std::string msg(
                "Failed to acquire initial read buffer for analytic function "
                "evaluation. Reducing query concurrency or increasing the memory limit may "
                "help this query to complete successfully.");
        RETURN_LIMIT_EXCEEDED(state, msg);
    }

    DCHECK_EQ(_evaluators.size(), _fn_ctxs.size());

    for (int i = 0; i < _evaluators.size(); ++i) {
        RETURN_IF_ERROR(_evaluators[i]->open(state, _fn_ctxs[i]));

        if ("first_value_rewrite" == _evaluators[i]->fn_name() &&
            _fn_ctxs[i]->get_num_args() == 2) {
            DCHECK(!_has_first_val_null_offset);
            _first_val_null_offset =
                    reinterpret_cast<BigIntVal*>(_fn_ctxs[i]->get_constant_arg(1))->val;
            VLOG_FILE << id() << " FIRST_VAL rewrite null offset: " << _first_val_null_offset;
            _has_first_val_null_offset = true;
        }
    }

    if (_partition_by_eq_expr_ctx != nullptr) {
        RETURN_IF_ERROR(_partition_by_eq_expr_ctx->open(state));
    }
    if (_order_by_eq_expr_ctx != nullptr) {
        RETURN_IF_ERROR(_order_by_eq_expr_ctx->open(state));
    }

    // An intermediate tuple is only allocated once and is reused.
    _curr_tuple = Tuple::create(_intermediate_tuple_desc->byte_size(), _mem_pool.get());
    AggFnEvaluator::init(_evaluators, _fn_ctxs, _curr_tuple);
    _dummy_result_tuple = Tuple::create(_result_tuple_desc->byte_size(), _mem_pool.get());

    // Initialize state for the first partition.
    init_next_partition(0);

    // Fetch the first input batch so that some _prev_input_row can be set here to avoid
    // special casing in GetNext().
    _prev_child_batch.reset(new RowBatch(child(0)->row_desc(), state->batch_size()));
    _curr_child_batch.reset(new RowBatch(child(0)->row_desc(), state->batch_size()));

    while (!_input_eos && _prev_input_row == nullptr) {
        RETURN_IF_ERROR(child(0)->get_next(state, _curr_child_batch.get(), &_input_eos));
        if (_curr_child_batch->num_rows() > 0) {
            _prev_input_row = _curr_child_batch->get_row(0);
            process_child_batches(state);
        } else {
            // Empty batch, still need to reset.
            _curr_child_batch->reset();
        }
    }

    if (_prev_input_row == nullptr) {
        DCHECK(_input_eos);
        // Delete _curr_child_batch to indicate there is no batch to process in GetNext()
        _curr_child_batch.reset();
    }

    return Status::OK();
}

std::string debug_window_bound_string(const TAnalyticWindowBoundary& b) {
    if (b.type == TAnalyticWindowBoundaryType::CURRENT_ROW) {
        return "CURRENT_ROW";
    }

    std::stringstream ss;

    if (b.__isset.rows_offset_value) {
        ss << b.rows_offset_value;
    } else {
        // TODO: Return debug string when range offsets are supported
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

std::string AnalyticEvalNode::debug_window_string() const {
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
        ss << debug_window_bound_string(_window.window_start);
    } else {
        ss << "UNBOUNDED_PRECEDING";
    }

    ss << ", end=";

    if (_window.__isset.window_end) {
        ss << debug_window_bound_string(_window.window_end) << "}";
    } else {
        ss << "UNBOUNDED_FOLLOWING";
    }

    return ss.str();
}

std::string AnalyticEvalNode::debug_state_string(bool detailed) const {
    std::stringstream ss;
    ss << "num_returned=" << _input_stream->rows_returned()
       << " num_rows=" << _input_stream->num_rows()
       << " _curr_partition_idx=" << _curr_partition_idx << " last_result_idx=" << _last_result_idx;

    if (detailed) {
        ss << " result_tuples idx: [";

        for (std::list<std::pair<int64_t, Tuple*>>::const_iterator it = _result_tuples.begin();
             it != _result_tuples.end(); ++it) {
            ss << it->first;

            if (*it != _result_tuples.back()) {
                ss << ", ";
            }
        }

        ss << "]";

        if (_fn_scope == ROWS && _window.__isset.window_start) {
            ss << " window_tuples idx: [";

            for (std::list<std::pair<int64_t, Tuple*>>::const_iterator it = _window_tuples.begin();
                 it != _window_tuples.end(); ++it) {
                ss << it->first;

                if (*it != _window_tuples.back()) {
                    ss << ", ";
                }
            }

            ss << "]";
        }
    } else {
        if (_fn_scope == ROWS && _window.__isset.window_start) {
            if (_window_tuples.empty()) {
                ss << " window_tuples empty";
            } else {
                ss << " window_tuples idx range: (" << _window_tuples.front().first << ","
                   << _window_tuples.back().first << ")";
            }
        }

        if (_result_tuples.empty()) {
            ss << " result_tuples empty";
        } else {
            ss << " result_tuples idx range: (" << _result_tuples.front().first << ","
               << _result_tuples.back().first << ")";
        }
    }

    return ss.str();
}

void AnalyticEvalNode::add_result_tuple(int64_t stream_idx) {
    VLOG_ROW << id() << " add_result_tuple idx=" << stream_idx;
    DCHECK(_curr_tuple != nullptr);
    Tuple* result_tuple = Tuple::create(_result_tuple_desc->byte_size(), _curr_tuple_pool.get());

    AggFnEvaluator::get_value(_evaluators, _fn_ctxs, _curr_tuple, result_tuple);
    DCHECK_GT(stream_idx, _last_result_idx);
    _result_tuples.push_back(std::pair<int64_t, Tuple*>(stream_idx, result_tuple));
    _last_result_idx = stream_idx;
    VLOG_ROW << id() << " Added result tuple, final state: " << debug_state_string(true);
}

inline void AnalyticEvalNode::try_add_result_tuple_for_prev_row(bool next_partition,
                                                                int64_t stream_idx, TupleRow* row) {
    // The analytic fns are finalized after the previous row if we found a new partition
    // or the window is a RANGE and the order by exprs changed. For ROWS windows we do not
    // need to compare the current row to the previous row.
    VLOG_ROW << id() << " try_add_result_tuple_for_prev_row partition=" << next_partition
             << " idx=" << stream_idx;
    if (_fn_scope == ROWS) {
        return;
    }
    if (next_partition || (_fn_scope == RANGE && _window.__isset.window_end &&
                           !prev_row_compare(_order_by_eq_expr_ctx))) {
        add_result_tuple(stream_idx - 1);
    }
}

inline void AnalyticEvalNode::try_add_result_tuple_for_curr_row(int64_t stream_idx, TupleRow* row) {
    VLOG_ROW << id() << " try_add_result_tuple_for_curr_row idx=" << stream_idx;

    // We only add results at this point for ROWS windows (unless unbounded following)
    if (_fn_scope != ROWS || !_window.__isset.window_end) {
        return;
    }

    // Nothing to add if the end offset is before the start of the partition.
    if (stream_idx - _rows_end_offset < _curr_partition_idx) {
        return;
    }

    add_result_tuple(stream_idx - _rows_end_offset);
}

inline void AnalyticEvalNode::try_remove_rows_before_window(int64_t stream_idx) {
    if (_fn_scope != ROWS || !_window.__isset.window_start) {
        return;
    }

    // The start of the window may have been before the current partition, in which case
    // there is no tuple to remove in _window_tuples. Check the index of the row at which
    // tuples from _window_tuples should begin to be removed.
    int64_t remove_idx =
            stream_idx - _rows_end_offset + std::min<int64_t>(_rows_start_offset, 0L) - 1;

    if (remove_idx < _curr_partition_idx) {
        return;
    }

    VLOG_ROW << id() << " Remove idx=" << remove_idx << " stream_idx=" << stream_idx;
    DCHECK(!_window_tuples.empty()) << debug_state_string(true);
    DCHECK_EQ(remove_idx + std::max<int64_t>(_rows_start_offset, 0L), _window_tuples.front().first)
            << debug_state_string(true);
    TupleRow* remove_row = reinterpret_cast<TupleRow*>(&_window_tuples.front().second);
    AggFnEvaluator::remove(_evaluators, _fn_ctxs, remove_row, _curr_tuple);
    _window_tuples.pop_front();
}

inline void AnalyticEvalNode::try_add_remaining_results(int64_t partition_idx,
                                                        int64_t prev_partition_idx) {
    DCHECK_LT(prev_partition_idx, partition_idx);

    // For PARTITION, RANGE, or ROWS with UNBOUNDED PRECEDING: add a result tuple for the
    // remaining rows in the partition that do not have an associated result tuple yet.
    if (_fn_scope != ROWS || !_window.__isset.window_end) {
        if (_last_result_idx < partition_idx - 1) {
            add_result_tuple(partition_idx - 1);
        }

        return;
    }

    // lead() is re-written to a ROWS window with an end bound FOLLOWING. Any remaining
    // results need the default value (set by Init()). If this is the case, the start bound
    // is UNBOUNDED PRECEDING (DCHECK in Init()).
    for (int i = 0; i < _evaluators.size(); ++i) {
        if (_is_lead_fn[i]) {
            _evaluators[i]->init(_fn_ctxs[i], _curr_tuple);
        }
    }

    // If the start bound is not UNBOUNDED PRECEDING and there are still rows in the
    // partition for which we need to produce result tuples, we need to continue removing
    // input tuples at the start of the window from each row that we're adding results for.
    VLOG_ROW << id() << " try_add_remaining_results prev_partition_idx=" << prev_partition_idx
             << " " << debug_state_string(true);

    for (int64_t next_result_idx = _last_result_idx + 1; next_result_idx < partition_idx;
         ++next_result_idx) {
        if (_window_tuples.empty()) {
            break;
        }

        if (next_result_idx + _rows_start_offset > _window_tuples.front().first) {
            DCHECK_EQ(next_result_idx + _rows_start_offset - 1, _window_tuples.front().first);
            // For every tuple that is removed from the window: Remove() from the evaluators
            // and add the result tuple at the next index.
            VLOG_ROW << id() << " Remove window_row_idx=" << _window_tuples.front().first
                     << " for result row at idx=" << next_result_idx;
            TupleRow* remove_row = reinterpret_cast<TupleRow*>(&_window_tuples.front().second);
            AggFnEvaluator::remove(_evaluators, _fn_ctxs, remove_row, _curr_tuple);
            _window_tuples.pop_front();
        }

        add_result_tuple(_last_result_idx + 1);
    }

    // If there are still rows between the row with the last result (add_result_tuple() may
    // have updated _last_result_idx) and the partition boundary, add the current results
    // for the remaining rows with the same result tuple (_curr_tuple is not modified).
    if (_last_result_idx < partition_idx - 1) {
        add_result_tuple(partition_idx - 1);
    }
}

inline void AnalyticEvalNode::init_next_partition(int64_t stream_idx) {
    VLOG_FILE << id() << " init_next_partition idx=" << stream_idx;
    DCHECK_LT(_curr_partition_idx, stream_idx);
    int64_t prev_partition_stream_idx = _curr_partition_idx;
    _curr_partition_idx = stream_idx;

    // If the window has an end bound preceding the current row, we will have output
    // tuples for rows beyond the partition so they should be removed. If there was only
    // one result tuple left in the partition it will remain in _result_tuples because it
    // is the empty result tuple (i.e. called Init() and never Update()) that was added
    // when initializing the previous partition so that the first rows have the default
    // values (where there are no preceding rows in the window).
    bool removed_results_past_partition = false;

    while (!_result_tuples.empty() && _last_result_idx >= _curr_partition_idx) {
        removed_results_past_partition = true;
        DCHECK(_window.__isset.window_end &&
               _window.window_end.type == TAnalyticWindowBoundaryType::PRECEDING);
        VLOG_ROW << id() << " Removing result past partition idx: " << _result_tuples.back().first;
        Tuple* prev_result_tuple = _result_tuples.back().second;
        _result_tuples.pop_back();

        if (_result_tuples.empty() || _result_tuples.back().first < prev_partition_stream_idx) {
            // prev_result_tuple was the last result tuple in the partition, add it back with
            // the index of the last row in the partition so that all output rows in this
            // partition get the default result tuple.
            _result_tuples.push_back(
                    std::pair<int64_t, Tuple*>(_curr_partition_idx - 1, prev_result_tuple));
        }

        _last_result_idx = _result_tuples.back().first;
    }

    if (removed_results_past_partition) {
        VLOG_ROW << id() << " After removing results past partition: " << debug_state_string(true);
        DCHECK_EQ(_last_result_idx, _curr_partition_idx - 1);
        DCHECK_LE(_input_stream->rows_returned(), _last_result_idx);
    }

    if (_fn_scope == ROWS && stream_idx > 0 &&
        (!_window.__isset.window_end ||
         _window.window_end.type == TAnalyticWindowBoundaryType::FOLLOWING)) {
        try_add_remaining_results(stream_idx, prev_partition_stream_idx);
    }

    _window_tuples.clear();

    // Re-initialize _curr_tuple.
    VLOG_ROW << id() << " Reset curr_tuple";
    // Call finalize to release resources; result is not needed but the dst tuple must be
    // a tuple described by _result_tuple_desc.
    AggFnEvaluator::finalize(_evaluators, _fn_ctxs, _curr_tuple, _dummy_result_tuple);
    _curr_tuple->init(_intermediate_tuple_desc->byte_size());
    AggFnEvaluator::init(_evaluators, _fn_ctxs, _curr_tuple);

    // Add a result tuple containing values set by Init() (e.g. nullptr for sum(), 0 for
    // count()) for output rows that have no input rows in the window. We need to add this
    // result tuple before any input rows are consumed and the evaluators are updated.
    if (_fn_scope == ROWS && _window.__isset.window_end &&
        _window.window_end.type == TAnalyticWindowBoundaryType::PRECEDING) {
        if (_has_first_val_null_offset) {
            // Special handling for FIRST_VALUE which has the window rewritten in the FE
            // in order to evaluate the fn efficiently with a trivial agg fn implementation.
            // This occurs when the original analytic window has a start bound X PRECEDING. In
            // that case, the window is rewritten to have an end bound X PRECEDING which would
            // normally mean we add the newly Init()'d result tuple X rows down (so that those
            // first rows have the initial value because they have no rows in their windows).
            // However, the original query did not actually have X PRECEDING so we need to do
            // one of the following:
            // 1) Do not insert the initial result tuple with at all, indicated by
            //    _first_val_null_offset == -1. This happens when the original end bound was
            //    actually CURRENT ROW or Y FOLLOWING.
            // 2) Insert the initial result tuple at _first_val_null_offset. This happens when
            //    the end bound was actually Y PRECEDING.
            if (_first_val_null_offset != -1) {
                add_result_tuple(_curr_partition_idx + _first_val_null_offset - 1);
            }
        } else {
            add_result_tuple(_curr_partition_idx - _rows_end_offset - 1);
        }
    }
}

inline bool AnalyticEvalNode::prev_row_compare(ExprContext* pred_ctx) {
    DCHECK(pred_ctx != nullptr);
    doris_udf::BooleanVal result = pred_ctx->get_boolean_val(_child_tuple_cmp_row);
    DCHECK(!result.is_null);

    return result.val;
}

Status AnalyticEvalNode::process_child_batches(RuntimeState* state) {
    // Consume child batches until eos or there are enough rows to return more than an
    // output batch. Ensuring there is at least one more row left after returning results
    // allows us to simplify the logic dealing with _last_result_idx and _result_tuples.
    while (_curr_child_batch.get() != nullptr &&
           num_output_rows_ready() < state->batch_size() + 1) {
        RETURN_IF_CANCELLED(state);
        //RETURN_IF_ERROR(QueryMaintenance(state));
        RETURN_IF_ERROR(process_child_batch(state));

        // TODO: DCHECK that the size of _result_tuples is bounded. It shouldn't be larger
        // than 2x the batch size unless the end bound has an offset preceding, in which
        // case it may be slightly larger (proportional to the offset but still bounded).
        if (_input_eos) {
            // Already processed the last child batch. Clean up and break.
            _curr_child_batch.reset();
            _prev_child_batch.reset();
            break;
        }

        _prev_child_batch->reset();
        _prev_child_batch.swap(_curr_child_batch);
        RETURN_IF_ERROR(child(0)->get_next(state, _curr_child_batch.get(), &_input_eos));
    }

    return Status::OK();
}

Status AnalyticEvalNode::process_child_batch(RuntimeState* state) {
    // TODO: DCHECK input is sorted (even just first row vs _prev_input_row)
    VLOG_FILE << id() << " process_child_batch: " << debug_state_string(false)
              << " input batch size:" << _curr_child_batch->num_rows()
              << " tuple pool size:" << _curr_tuple_pool->total_allocated_bytes();
    SCOPED_TIMER(_evaluation_timer);
    // BufferedTupleStream::num_rows() returns the total number of rows that have been
    // inserted into the stream (it does not decrease when we read rows), so the index of
    // the next input row that will be inserted will be the current size of the stream.
    int64_t stream_idx = _input_stream->num_rows();
    // Stores the stream_idx of the row that was last inserted into _window_tuples.
    int64_t last_window_tuple_idx = -1;

    for (int i = 0; i < _curr_child_batch->num_rows(); ++i, ++stream_idx) {
        TupleRow* row = _curr_child_batch->get_row(i);
        _child_tuple_cmp_row->set_tuple(0, _prev_input_row->get_tuple(0));
        _child_tuple_cmp_row->set_tuple(1, row->get_tuple(0));
        try_remove_rows_before_window(stream_idx);

        // Every row is compared against the previous row to determine if (a) the row
        // starts a new partition or (b) the row does not share the same values for the
        // ordering exprs. When either of these occurs, the _evaluators are finalized and
        // the result tuple is added to _result_tuples so that it may be added to output
        // rows in get_next_output_batch(). When a new partition is found (a), a new, empty
        // result tuple is created and initialized over the _evaluators. If the row has
        // different values for the ordering exprs (b), then a new tuple is created but
        // copied from _curr_tuple because the original is used for one or more previous
        // row(s) but the incremental state still applies to the current row.
        bool next_partition = false;

        if (_partition_by_eq_expr_ctx != nullptr) {
            // _partition_by_eq_expr_ctx checks equality over the predicate exprs
            next_partition = !prev_row_compare(_partition_by_eq_expr_ctx);
        }

        try_add_result_tuple_for_prev_row(next_partition, stream_idx, row);

        if (next_partition) {
            init_next_partition(stream_idx);
        }

        // The _evaluators are updated with the current row.
        if (_fn_scope != ROWS || !_window.__isset.window_start ||
            stream_idx - _rows_start_offset >= _curr_partition_idx) {
            VLOG_ROW << id() << " Update idx=" << stream_idx;
            AggFnEvaluator::add(_evaluators, _fn_ctxs, row, _curr_tuple);

            if (_window.__isset.window_start) {
                VLOG_ROW << id() << " Adding tuple to window at idx=" << stream_idx;
                Tuple* tuple =
                        row->get_tuple(0)->deep_copy(*_child_tuple_desc, _curr_tuple_pool.get());
                _window_tuples.push_back(std::pair<int64_t, Tuple*>(stream_idx, tuple));
                last_window_tuple_idx = stream_idx;
            }
        }

        try_add_result_tuple_for_curr_row(stream_idx, row);

        Status status = Status::OK();
        // Buffer the entire input row to be returned later with the analytic eval results.
        if (UNLIKELY(!_input_stream->add_row(row, &status))) {
            // AddRow returns false if an error occurs (available via status()) or there is
            // not enough memory (status() is OK). If there isn't enough memory, we unpin
            // the stream and continue writing/reading in unpinned mode.
            // TODO: Consider re-pinning later if the output stream is fully consumed.
            add_runtime_exec_option("Spilled");
            RETURN_IF_ERROR(status);
            RETURN_IF_ERROR(_input_stream->unpin_stream());
            VLOG_FILE << id() << " Unpin input stream while adding row idx=" << stream_idx;

            if (!_input_stream->add_row(row, &status)) {
                // Rows should be added in unpinned mode unless an error occurs.
                RETURN_IF_ERROR(status);
                DCHECK(false);
            }
        }

        _prev_input_row = row;
    }

    // We need to add the results for the last row(s).
    if (_input_eos) {
        try_add_remaining_results(stream_idx, _curr_partition_idx);
    }

    // Transfer resources to _prev_tuple_pool when enough resources have accumulated
    // and the _prev_tuple_pool has already been transferred to an output batch.

    // The memory limit of _curr_tuple_pool is set by the fixed value
    // The size is specified as 8MB, which is used in the extremely strict memory limit.
    // Eg: exec_mem_limit < 100MB may cause memory exceeded limit problem. So change it to half of max block size to prevent the problem.
    // TODO: Should we keep the buffer of _curr_tuple_pool or release the memory occupied ASAP?
    if (_curr_tuple_pool->total_allocated_bytes() > state->block_mgr2()->max_block_size() / 2 &&
        (_prev_pool_last_result_idx == -1 || _prev_pool_last_window_idx == -1)) {
        _prev_tuple_pool->acquire_data(_curr_tuple_pool.get(), false);
        _prev_pool_last_result_idx = _last_result_idx;
        _prev_pool_last_window_idx = last_window_tuple_idx;
        VLOG_FILE << id() << " Transfer resources from curr to prev pool at idx: " << stream_idx
                  << ", stores tuples with last result idx: " << _prev_pool_last_result_idx
                  << " last window idx: " << _prev_pool_last_window_idx;
    }

    return Status::OK();
}

Status AnalyticEvalNode::get_next_output_batch(RuntimeState* state, RowBatch* output_batch,
                                               bool* eos) {
    SCOPED_TIMER(_evaluation_timer);
    VLOG_FILE << id() << " get_next_output_batch: " << debug_state_string(false)
              << " tuple pool size:" << _curr_tuple_pool->total_allocated_bytes();

    if (_input_stream->rows_returned() == _input_stream->num_rows()) {
        *eos = true;
        return Status::OK();
    }

    const int num_child_tuples = child(0)->row_desc().tuple_descriptors().size();
    ExprContext** ctxs = &_conjunct_ctxs[0];
    int num_ctxs = _conjunct_ctxs.size();

    RowBatch input_batch(child(0)->row_desc(), output_batch->capacity());
    int64_t stream_idx = _input_stream->rows_returned();
    RETURN_IF_ERROR(_input_stream->get_next(&input_batch, eos));

    for (int i = 0; i < input_batch.num_rows(); ++i) {
        if (reached_limit()) {
            break;
        }

        DCHECK(!output_batch->is_full());
        DCHECK(!_result_tuples.empty());
        VLOG_ROW << id() << " Output row idx=" << stream_idx << " " << debug_state_string(true);

        // CopyRow works as expected: input_batch tuples form a prefix of output_batch
        // tuples.
        TupleRow* dest = output_batch->get_row(output_batch->add_row());
        // input_batch is from a tuple_buffer_stream,
        // It can only guarantee that the life cycle is valid in a batch stage.
        // If the ancestor node is a no-spilling blocking node (such as hash_join_node except_node ...)
        // these node may acquire a invalid tuple pointer,
        // so we should use deep_copy, and copy tuple to the tuple_pool, to ensure tuple not finalized.
        // reference issue #5466
        input_batch.get_row(i)->deep_copy(dest, child(0)->row_desc().tuple_descriptors(),
                                          output_batch->tuple_data_pool(), false);
        dest->set_tuple(num_child_tuples, _result_tuples.front().second);

        if (ExecNode::eval_conjuncts(ctxs, num_ctxs, dest)) {
            output_batch->commit_last_row();
            ++_num_rows_returned;
        }

        // Remove the head of _result_tuples if all rows using that evaluated tuple
        // have been returned.
        DCHECK_LE(stream_idx, _result_tuples.front().first);

        if (stream_idx >= _result_tuples.front().first) {
            _result_tuples.pop_front();
        }

        ++stream_idx;
    }

    input_batch.transfer_resource_ownership(output_batch);

    if (reached_limit()) {
        *eos = true;
    }

    return Status::OK();
}

inline int64_t AnalyticEvalNode::num_output_rows_ready() const {
    if (_result_tuples.empty()) {
        return 0;
    }

    int64_t rows_to_return = _last_result_idx - _input_stream->rows_returned();

    if (_last_result_idx > _input_stream->num_rows()) {
        // This happens when we were able to add a result tuple before consuming child rows,
        // e.g. initializing a new partition with an end bound that is X preceding. The first
        // X rows get the default value and we add that tuple to _result_tuples before
        // consuming child rows. It's possible the result is negative, and that's fine
        // because this result is only used to determine if the number of rows to return
        // is at least as big as the batch size.
        rows_to_return -= _last_result_idx - _input_stream->num_rows();
    } else {
        DCHECK_GE(rows_to_return, 0);
    }

    return rows_to_return;
    return 0;
}

Status AnalyticEvalNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());
    RETURN_IF_CANCELLED(state);
    //RETURN_IF_ERROR(QueryMaintenance(state));
    RETURN_IF_ERROR(state->check_query_state("Analytic eval, while get_next."));
    VLOG_FILE << id() << " GetNext: " << debug_state_string(false);

    if (reached_limit()) {
        *eos = true;
        return Status::OK();
    } else {
        *eos = false;
    }

    RETURN_IF_ERROR(process_child_batches(state));
    bool output_eos = false;
    RETURN_IF_ERROR(get_next_output_batch(state, row_batch, &output_eos));

    if (_curr_child_batch.get() == nullptr && output_eos) {
        *eos = true;
    }

    // Transfer resources to the output row batch if enough have accumulated and they're
    // no longer needed by output rows to be returned later.
    if (_prev_pool_last_result_idx != -1 &&
        _prev_pool_last_result_idx < _input_stream->rows_returned() &&
        _prev_pool_last_window_idx < _window_tuples.front().first) {
        VLOG_FILE << id() << " Transfer prev pool to output batch, "
                  << " pool size: " << _prev_tuple_pool->total_allocated_bytes()
                  << " last result idx: " << _prev_pool_last_result_idx
                  << " last window idx: " << _prev_pool_last_window_idx;
        row_batch->tuple_data_pool()->acquire_data(_prev_tuple_pool.get(), !*eos);
        _prev_pool_last_result_idx = -1;
        _prev_pool_last_window_idx = -1;
    }

    COUNTER_SET(_rows_returned_counter, _num_rows_returned);
    return Status::OK();
}

Status AnalyticEvalNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }

    if (_input_stream.get() != nullptr) {
        _input_stream->close();
    }

    if (_block_mgr_client != nullptr) {
        state->block_mgr2()->clear_reservations(_block_mgr_client);
    }
    // Close all evaluators and fn ctxs. If an error occurred in Init or prepare there may
    // be fewer ctxs than evaluators. We also need to Finalize if _curr_tuple was created
    // in Open.
    DCHECK_LE(_fn_ctxs.size(), _evaluators.size());
    DCHECK(_curr_tuple == nullptr || _fn_ctxs.size() == _evaluators.size());

    for (int i = 0; i < _evaluators.size(); ++i) {
        // Need to make sure finalize is called in case there is any state to clean up.
        if (_curr_tuple != nullptr) {
            _evaluators[i]->finalize(_fn_ctxs[i], _curr_tuple, _dummy_result_tuple);
        }

        _evaluators[i]->close(state);
    }

    for (int i = 0; i < _fn_ctxs.size(); ++i) {
        _fn_ctxs[i]->impl()->close();
    }

    if (_partition_by_eq_expr_ctx != nullptr) {
        _partition_by_eq_expr_ctx->close(state);
    }
    if (_order_by_eq_expr_ctx != nullptr) {
        _order_by_eq_expr_ctx->close(state);
    }
    if (_prev_child_batch.get() != nullptr) {
        _prev_child_batch.reset();
    }

    if (_curr_child_batch.get() != nullptr) {
        _curr_child_batch.reset();
    }

    if (_curr_tuple_pool.get() != nullptr) {
        _curr_tuple_pool->free_all();
    }
    if (_prev_tuple_pool.get() != nullptr) {
        _prev_tuple_pool->free_all();
    }
    if (_mem_pool.get() != nullptr) {
        _mem_pool->free_all();
    }
    ExecNode::close(state);
    return Status::OK();
}

void AnalyticEvalNode::debug_string(int indentation_level, std::stringstream* out) const {
    *out << string(indentation_level * 2, ' ');
    *out << "AnalyticEvalNode("
         << " window=" << debug_window_string();

    if (_partition_by_eq_expr_ctx != nullptr) {
        // *out << " partition_exprs=" << _partition_by_eq_expr_ctx->debug_string();
    }

    if (_order_by_eq_expr_ctx != nullptr) {
        // *out << " order_by_exprs=" << _order_by_eq_expr_ctx->debug_string();
    }

    *out << AggFnEvaluator::debug_string(_evaluators);
    ExecNode::debug_string(indentation_level, out);
    *out << ")";
}

//Status AnalyticEvalNode::QueryMaintenance(RuntimeState* state) {
//  for (int i = 0; i < evaluators_.size(); ++i) {
//    Expr::FreeLocalAllocations(evaluators_[i]->input_expr_ctxs());
//  }
//  return ExecNode::QueryMaintenance(state);
//}

} // namespace doris
