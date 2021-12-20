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

#include "exec/cross_join_node.h"

#include <sstream>

#include "exprs/expr.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "util/debug_util.h"
#include "util/runtime_profile.h"

namespace doris {

CrossJoinNode::CrossJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : BlockingJoinNode("CrossJoinNode", TJoinOp::CROSS_JOIN, pool, tnode, descs) {}

Status CrossJoinNode::prepare(RuntimeState* state) {
    DCHECK(_join_op == TJoinOp::CROSS_JOIN);
    RETURN_IF_ERROR(BlockingJoinNode::prepare(state));
    _build_batch_pool.reset(new ObjectPool());
    return Status::OK();
}

Status CrossJoinNode::close(RuntimeState* state) {
    // avoid double close
    if (is_closed()) {
        return Status::OK();
    }
    _build_batches.reset();
    _build_batch_pool.reset();
    BlockingJoinNode::close(state);
    return Status::OK();
}

Status CrossJoinNode::construct_build_side(RuntimeState* state) {
    // Do a full scan of child(1) and store all build row batches.
    RETURN_IF_ERROR(child(1)->open(state));

    while (true) {
        RowBatch* batch = _build_batch_pool->add(
                new RowBatch(child(1)->row_desc(), state->batch_size(), mem_tracker().get()));

        RETURN_IF_CANCELLED(state);
        // TODO(zhaochun):
        // RETURN_IF_ERROR(state->CheckQueryState());
        bool eos = false;
        RETURN_IF_ERROR(child(1)->get_next(state, batch, &eos));

        // to prevent use too many memory
        RETURN_IF_LIMIT_EXCEEDED(state, "Cross join, while getting next from the child 1.");

        SCOPED_TIMER(_build_timer);
        _build_batches.add_row_batch(batch);
        VLOG_ROW << build_list_debug_string();
        COUNTER_SET(_build_row_counter, static_cast<int64_t>(_build_batches.total_num_rows()));

        if (eos) {
            break;
        }
    }

    return Status::OK();
}

void CrossJoinNode::init_get_next(TupleRow* first_left_row) {
    _current_build_row = _build_batches.iterator();
}

Status CrossJoinNode::get_next(RuntimeState* state, RowBatch* output_batch, bool* eos) {
    // RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT, state));
    RETURN_IF_CANCELLED(state);
    *eos = false;
    // TOOD(zhaochun)
    // RETURN_IF_ERROR(state->check_query_state());
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    if (reached_limit() || _eos) {
        *eos = true;
        return Status::OK();
    }

    ScopedTimer<MonotonicStopWatch> timer(_left_child_timer);

    while (!_eos) {
        // Compute max rows that should be added to output_batch
        int64_t max_added_rows = output_batch->capacity() - output_batch->num_rows();

        if (limit() != -1) {
            max_added_rows = std::min(max_added_rows, limit() - rows_returned());
        }

        // Continue processing this row batch
        _num_rows_returned +=
                process_left_child_batch(output_batch, _left_batch.get(), max_added_rows);
        COUNTER_SET(_rows_returned_counter, _num_rows_returned);

        if (reached_limit() || output_batch->is_full()) {
            *eos = reached_limit();
            break;
        }

        // Check to see if we're done processing the current left child batch
        if (_current_build_row.at_end() && _left_batch_pos == _left_batch->num_rows()) {
            _left_batch->transfer_resource_ownership(output_batch);
            _left_batch_pos = 0;

            if (output_batch->is_full()) {
                break;
            }

            if (_left_side_eos) {
                *eos = _eos = true;
                break;
            } else {
                timer.stop();
                RETURN_IF_ERROR(child(0)->get_next(state, _left_batch.get(), &_left_side_eos));
                timer.start();
                COUNTER_UPDATE(_left_child_row_counter, _left_batch->num_rows());
            }
        }
    }

    return Status::OK();
}

std::string CrossJoinNode::build_list_debug_string() {
    std::stringstream out;
    out << "BuildList(";
    out << _build_batches.debug_string(child(1)->row_desc());
    out << ")";
    return out.str();
}

// TODO: this can be replaced with a codegen'd function
int CrossJoinNode::process_left_child_batch(RowBatch* output_batch, RowBatch* batch,
                                            int max_added_rows) {
    int row_idx = output_batch->add_rows(max_added_rows);
    DCHECK(row_idx != RowBatch::INVALID_ROW_INDEX);
    uint8_t* output_row_mem = reinterpret_cast<uint8_t*>(output_batch->get_row(row_idx));
    TupleRow* output_row = reinterpret_cast<TupleRow*>(output_row_mem);

    int rows_returned = 0;
    ExprContext* const* ctxs = &_conjunct_ctxs[0];
    int ctx_size = _conjunct_ctxs.size();

    while (true) {
        while (!_current_build_row.at_end()) {
            create_output_row(output_row, _current_left_child_row, _current_build_row.get_row());
            _current_build_row.next();

            if (!eval_conjuncts(ctxs, ctx_size, output_row)) {
                continue;
            }

            ++rows_returned;

            // Filled up out batch or hit limit
            if (UNLIKELY(rows_returned == max_added_rows)) {
                output_batch->commit_rows(rows_returned);
                return rows_returned;
            }

            // Advance to next out row
            output_row_mem += output_batch->row_byte_size();
            output_row = reinterpret_cast<TupleRow*>(output_row_mem);
        }

        DCHECK(_current_build_row.at_end());

        // Advance to the next row in the left child batch
        if (UNLIKELY(_left_batch_pos == batch->num_rows())) {
            output_batch->commit_rows(rows_returned);
            return rows_returned;
        }

        _current_left_child_row = batch->get_row(_left_batch_pos++);
        _current_build_row = _build_batches.iterator();
    }

    output_batch->commit_rows(rows_returned);
    return rows_returned;
}
} // namespace doris
