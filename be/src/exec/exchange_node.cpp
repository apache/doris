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

#include "exec/exchange_node.h"

#include "gen_cpp/PlanNodes_types.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/data_stream_recvr.h"
#include "runtime/exec_env.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"

namespace doris {

ExchangeNode::ExchangeNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _num_senders(0),
          _stream_recvr(nullptr),
          _input_row_desc(descs, tnode.exchange_node.input_row_tuples,
                          std::vector<bool>(tnode.nullable_tuples.begin(),
                                            tnode.nullable_tuples.begin() +
                                                    tnode.exchange_node.input_row_tuples.size())),
          _next_row_idx(0),
          _is_merging(tnode.exchange_node.__isset.sort_info),
          _offset(tnode.exchange_node.__isset.offset ? tnode.exchange_node.offset : 0),
          _num_rows_skipped(0) {
    DCHECK_GE(_offset, 0);
    DCHECK(_is_merging || (_offset == 0));
}

Status ExchangeNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    if (!_is_merging) {
        return Status::OK();
    }

    RETURN_IF_ERROR(_sort_exec_exprs.init(tnode.exchange_node.sort_info, _pool));
    _is_asc_order = tnode.exchange_node.sort_info.is_asc_order;
    _nulls_first = tnode.exchange_node.sort_info.nulls_first;
    return Status::OK();
}

Status ExchangeNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));
    _convert_row_batch_timer = ADD_TIMER(runtime_profile(), "ConvertRowBatchTime");
    // TODO: figure out appropriate buffer size
    DCHECK_GT(_num_senders, 0);
    _sub_plan_query_statistics_recvr.reset(new QueryStatisticsRecvr());
    _stream_recvr = state->exec_env()->stream_mgr()->create_recvr(
            state, _input_row_desc, state->fragment_instance_id(), _id, _num_senders,
            config::exchg_node_buffer_size_bytes, _runtime_profile.get(), _is_merging,
            _sub_plan_query_statistics_recvr);
    if (_is_merging) {
        RETURN_IF_ERROR(_sort_exec_exprs.prepare(state, _row_descriptor, _row_descriptor,
                                                 expr_mem_tracker()));
        // AddExprCtxsToFree(_sort_exec_exprs);
    }
    return Status::OK();
}

Status ExchangeNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    if (_is_merging) {
        RETURN_IF_ERROR(_sort_exec_exprs.open(state));
        TupleRowComparator less_than(_sort_exec_exprs, _is_asc_order, _nulls_first);
        // create_merger() will populate its merging heap with batches from the _stream_recvr,
        // so it is not necessary to call fill_input_row_batch().
        if (state->enable_exchange_node_parallel_merge()) {
            RETURN_IF_ERROR(_stream_recvr->create_parallel_merger(less_than, state->batch_size(),
                                                                  mem_tracker().get()));
        } else {
            RETURN_IF_ERROR(_stream_recvr->create_merger(less_than));
        }
    } else {
        RETURN_IF_ERROR(fill_input_row_batch(state));
    }
    return Status::OK();
}

Status ExchangeNode::collect_query_statistics(QueryStatistics* statistics) {
    RETURN_IF_ERROR(ExecNode::collect_query_statistics(statistics));
    statistics->merge(_sub_plan_query_statistics_recvr.get());
    return Status::OK();
}

Status ExchangeNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    if (_is_merging) {
        _sort_exec_exprs.close(state);
    }
    if (_stream_recvr != nullptr) {
        _stream_recvr->close();
    }
    // _stream_recvr.reset();
    return ExecNode::close(state);
}

Status ExchangeNode::fill_input_row_batch(RuntimeState* state) {
    DCHECK(!_is_merging);
    Status ret_status;
    {
        // SCOPED_TIMER(state->total_network_receive_timer());
        ret_status = _stream_recvr->get_batch(&_input_batch);
    }
    VLOG_FILE << "exch: has batch=" << (_input_batch == nullptr ? "false" : "true")
              << " #rows=" << (_input_batch != nullptr ? _input_batch->num_rows() : 0)
              << " is_cancelled=" << (ret_status.is_cancelled() ? "true" : "false")
              << " instance_id=" << state->fragment_instance_id();
    return ret_status;
}

Status ExchangeNode::get_next(RuntimeState* state, RowBatch* output_batch, bool* eos) {
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    if (reached_limit()) {
        _stream_recvr->transfer_all_resources(output_batch);
        *eos = true;
        return Status::OK();
    } else {
        *eos = false;
    }

    if (_is_merging) {
        return get_next_merging(state, output_batch, eos);
    }

    ExprContext* const* ctxs = &_conjunct_ctxs[0];
    int num_ctxs = _conjunct_ctxs.size();

    while (true) {
        {
            SCOPED_TIMER(_convert_row_batch_timer);
            RETURN_IF_CANCELLED(state);
            // copy rows until we hit the limit/capacity or until we exhaust _input_batch
            while (!reached_limit() && !output_batch->at_capacity() && _input_batch != nullptr &&
                   _next_row_idx < _input_batch->num_rows()) {
                TupleRow* src = _input_batch->get_row(_next_row_idx);

                if (ExecNode::eval_conjuncts(ctxs, num_ctxs, src)) {
                    int j = output_batch->add_row();
                    TupleRow* dest = output_batch->get_row(j);
                    // if the input row is shorter than the output row, make sure not to leave
                    // uninitialized Tuple* around
                    output_batch->clear_row(dest);
                    // this works as expected if rows from input_batch form a prefix of
                    // rows in output_batch
                    _input_batch->copy_row(src, dest);
                    output_batch->commit_last_row();
                    ++_num_rows_returned;
                }

                ++_next_row_idx;
            }

            if (VLOG_ROW_IS_ON) {
                VLOG_ROW << "ExchangeNode output batch: " << output_batch->to_string();
            }

            COUNTER_SET(_rows_returned_counter, _num_rows_returned);

            if (reached_limit()) {
                _stream_recvr->transfer_all_resources(output_batch);
                *eos = true;
                return Status::OK();
            }

            if (output_batch->at_capacity()) {
                *eos = false;
                return Status::OK();
            }
        }

        // we need more rows
        if (_input_batch != nullptr) {
            _input_batch->transfer_resource_ownership(output_batch);
        }

        RETURN_IF_ERROR(fill_input_row_batch(state));
        *eos = (_input_batch == nullptr);
        if (*eos) {
            return Status::OK();
        }

        _next_row_idx = 0;
        DCHECK(_input_batch->row_desc().layout_is_prefix_of(output_batch->row_desc()));
    }
}

Status ExchangeNode::get_next_merging(RuntimeState* state, RowBatch* output_batch, bool* eos) {
    DCHECK_EQ(output_batch->num_rows(), 0);
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(state->check_query_state("Exchange, while merging next."));

    RETURN_IF_ERROR(_stream_recvr->get_next(output_batch, eos));
    while ((_num_rows_skipped < _offset)) {
        _num_rows_skipped += output_batch->num_rows();
        // Throw away rows in the output batch until the offset is skipped.
        int rows_to_keep = _num_rows_skipped - _offset;
        if (rows_to_keep > 0) {
            output_batch->copy_rows(0, output_batch->num_rows() - rows_to_keep, rows_to_keep);
            output_batch->set_num_rows(rows_to_keep);
        } else {
            output_batch->set_num_rows(0);
        }
        if (rows_to_keep > 0 || *eos || output_batch->at_capacity()) {
            break;
        }
        RETURN_IF_ERROR(_stream_recvr->get_next(output_batch, eos));
    }

    _num_rows_returned += output_batch->num_rows();
    if (reached_limit()) {
        output_batch->set_num_rows(output_batch->num_rows() - (_num_rows_returned - _limit));
        *eos = true;
    }

    // On eos, transfer all remaining resources from the input batches maintained
    // by the merger to the output batch.
    if (*eos) {
        _stream_recvr->transfer_all_resources(output_batch);
    }

    COUNTER_SET(_rows_returned_counter, _num_rows_returned);
    return Status::OK();
}

void ExchangeNode::debug_string(int indentation_level, std::stringstream* out) const {
    *out << string(indentation_level * 2, ' ');
    *out << "ExchangeNode(#senders=" << _num_senders;
    ExecNode::debug_string(indentation_level, out);
    *out << ")";
}

} // namespace doris
