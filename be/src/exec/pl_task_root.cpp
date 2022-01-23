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

#include "exec/pl_task_root.h"

namespace doris {

ExchangeNode::ExchangeNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs), _num_senders(0), _stream_recvr(nullptr), _next_row_idx(0) {}

ExchangeNode::~ExchangeNode() {}

Status ExchangeNode::init(const TPlanNode& tnode, RuntimeState* state) {
    return ExecNode::init(tnode, state);
}

Status ExchangeNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));

    _convert_row_batch_timer = ADD_TIMER(runtime_profile(), "ConvertRowBatchTime");

    // TODO: figure out appropriate buffer size
    DCHECK_GT(_num_senders, 0);
    _stream_recvr = state->create_recvr(_row_descriptor, _id, _num_senders,
                                        config::exchg_node_buffer_size_bytes, runtime_profile());
    return Status::OK();
}

Status ExchangeNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    return Status::OK();
}

Status ExchangeNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    return ExecNode::close(state);
}

Status ExchangeNode::get_next(RuntimeState* state, RowBatch* output_batch, bool* eos) {
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    if (reached_limit()) {
        *eos = true;
        return Status::OK();
    }

    ExprContext* const* ctxs = &_conjunct_ctxs[0];
    int num_ctxs = _conjunct_ctxs.size();

    while (true) {
        {
            SCOPED_TIMER(_convert_row_batch_timer);

            // copy rows until we hit the limit/capacity or until we exhaust _input_batch
            while (!reached_limit() && !output_batch->is_full() && _input_batch.get() != nullptr &&
                   _next_row_idx < _input_batch->capacity()) {
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

            COUNTER_SET(_rows_returned_counter, _num_rows_returned);

            if (reached_limit()) {
                *eos = true;
                return Status::OK();
            }

            if (output_batch->is_full()) {
                *eos = false;
                return Status::OK();
            }
        }

        // we need more rows
        if (_input_batch.get() != nullptr) {
            _input_batch->transfer_resource_ownership(output_batch);
        }

        bool is_cancelled = true;
        _input_batch.reset(_stream_recvr->get_batch(&is_cancelled));
        VLOG_FILE << "exch: has batch=" << (_input_batch.get() == nullptr ? "false" : "true")
                  << " #rows=" << (_input_batch.get() != nullptr ? _input_batch->num_rows() : 0)
                  << " is_cancelled=" << (is_cancelled ? "true" : "false")
                  << " instance_id=" << state->fragment_instance_id();

        if (is_cancelled) {
            return Status::Cancelled("Cancelled");
        }

        *eos = (_input_batch.get() == nullptr);

        if (*eos) {
            return Status::OK();
        }

        _next_row_idx = 0;
        DCHECK(_input_batch->row_desc().is_prefix_of(output_batch->row_desc()));
    }
}

void ExchangeNode::debug_string(int indentation_level, std::stringstream* out) const {
    *out << string(indentation_level * 2, ' ');
    *out << "ExchangeNode(#senders=" << _num_senders;
    ExecNode::debug_string(indentation_level, out);
    *out << ")";
}

} // namespace doris
