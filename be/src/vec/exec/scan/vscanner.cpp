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

#include "vec/exec/scan/vscanner.h"

#include "vec/exec/scan/vscan_node.h"

namespace doris::vectorized {

VScanner::VScanner(RuntimeState* state, VScanNode* parent, int64_t limit)
        : _state(state),
          _parent(parent),
          _limit(limit),
          _input_tuple_desc(parent->input_tuple_desc()),
          _output_tuple_desc(parent->output_tuple_desc()) {
    _real_tuple_desc = _input_tuple_desc != nullptr ? _input_tuple_desc : _output_tuple_desc;
    _total_rf_num = _parent->runtime_filter_num();
    _is_load = (_input_tuple_desc != nullptr);
}

Status VScanner::get_block(RuntimeState* state, Block* block, bool* eof) {
    // only empty block should be here
    DCHECK(block->rows() == 0);

    int64_t raw_rows_threshold = raw_rows_read() + config::doris_scanner_row_num;
    if (!block->mem_reuse()) {
        for (const auto slot_desc : _output_tuple_desc->slots()) {
            block->insert(ColumnWithTypeAndName(slot_desc->get_empty_mutable_column(),
                                                slot_desc->get_data_type_ptr(),
                                                slot_desc->col_name()));
        }
    }

    {
        do {
            // 1. Get input block from scanner
            {
                SCOPED_TIMER(_parent->_scan_timer);
                RETURN_IF_ERROR(_get_block_impl(state, block, eof));
                if (*eof) {
                    DCHECK(block->rows() == 0);
                    break;
                }
                _num_rows_read += block->rows();
            }

            // 2. Filter the output block finally.
            {
                SCOPED_TIMER(_parent->_filter_timer);
                RETURN_IF_ERROR(_filter_output_block(block));
            }
        } while (block->rows() == 0 && !(*eof) && raw_rows_read() < raw_rows_threshold);
    }

    return Status::OK();
}

Status VScanner::_filter_output_block(Block* block) {
    auto old_rows = block->rows();
    Status st =
            VExprContext::filter_block(_vconjunct_ctx, block, _output_tuple_desc->slots().size());
    _counter.num_rows_unselected += old_rows - block->rows();
    return st;
}

Status VScanner::try_append_late_arrival_runtime_filter() {
    if (_applied_rf_num == _total_rf_num) {
        return Status::OK();
    }
    DCHECK(_applied_rf_num < _total_rf_num);

    int arrived_rf_num = 0;
    RETURN_IF_ERROR(_parent->try_append_late_arrival_runtime_filter(&arrived_rf_num));

    if (arrived_rf_num == _applied_rf_num) {
        // No newly arrived runtime filters, just return;
        return Status::OK();
    }

    // There are newly arrived runtime filters,
    // renew the vconjunct_ctx_ptr
    if (_vconjunct_ctx) {
        _discard_conjuncts();
    }
    // Notice that the number of runtiem filters may be larger than _applied_rf_num.
    // But it is ok because it will be updated at next time.
    RETURN_IF_ERROR(_parent->clone_vconjunct_ctx(&_vconjunct_ctx));
    _applied_rf_num = arrived_rf_num;
    return Status::OK();
}

Status VScanner::close(RuntimeState* state) {
    if (_is_closed) {
        return Status::OK();
    }
    for (auto& ctx : _stale_vexpr_ctxs) {
        ctx->close(state);
    }
    if (_vconjunct_ctx) {
        _vconjunct_ctx->close(state);
    }

    COUNTER_UPDATE(_parent->_scanner_wait_worker_timer, _scanner_wait_worker_timer);
    _is_closed = true;
    return Status::OK();
}

void VScanner::_update_counters_before_close() {
    LOG(INFO) << "cmy _update_counters_before_close: _counter.num_rows_filtered: "
              << _counter.num_rows_filtered
              << ", _counter.num_rows_unselected: " << _counter.num_rows_unselected;
    if (!_state->enable_profile() && !_is_load) return;
    COUNTER_UPDATE(_parent->_rows_read_counter, _num_rows_read);
    // Update stats for load
    _state->update_num_rows_load_filtered(_counter.num_rows_filtered);
    _state->update_num_rows_load_unselected(_counter.num_rows_unselected);
}

} // namespace doris::vectorized
