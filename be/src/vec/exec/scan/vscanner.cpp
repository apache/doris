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

#include <glog/logging.h>

#include "common/config.h"
#include "common/logging.h"
#include "runtime/descriptors.h"
#include "util/runtime_profile.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/exec/scan/vscan_node.h"
#include "vec/exprs/vexpr_context.h"

namespace doris::vectorized {

VScanner::VScanner(RuntimeState* state, VScanNode* parent, int64_t limit, RuntimeProfile* profile)
        : _state(state),
          _parent(parent),
          _limit(limit),
          _profile(profile),
          _output_tuple_desc(parent->output_tuple_desc()) {
    _total_rf_num = _parent->runtime_filter_num();
}

Status VScanner::prepare(RuntimeState* state, const VExprContextSPtrs& conjuncts) {
    if (!conjuncts.empty()) {
        _conjuncts.resize(conjuncts.size());
        for (size_t i = 0; i != conjuncts.size(); ++i) {
            RETURN_IF_ERROR(conjuncts[i]->clone(state, _conjuncts[i]));
        }
    }

    return Status::OK();
}

Status VScanner::get_block(RuntimeState* state, Block* block, bool* eof) {
    // only empty block should be here
    DCHECK(block->rows() == 0);
    SCOPED_RAW_TIMER(&_per_scanner_timer);
    int64_t rows_read_threshold = _num_rows_read + config::doris_scanner_row_num;
    if (!block->mem_reuse()) {
        for (const auto slot_desc : _output_tuple_desc->slots()) {
            if (!slot_desc->need_materialize()) {
                // should be ignore from reading
                continue;
            }
            block->insert(ColumnWithTypeAndName(slot_desc->get_empty_mutable_column(),
                                                slot_desc->get_data_type_ptr(),
                                                slot_desc->col_name()));
        }
    }

    {
        do {
            // if step 2 filter all rows of block, and block will be reused to get next rows,
            // must clear row_same_bit of block, or will get wrong row_same_bit.size() which not equal block.rows()
            block->clear_same_bit();
            // 1. Get input block from scanner
            {
                SCOPED_TIMER(_parent->_scan_timer);
                RETURN_IF_ERROR(_get_block_impl(state, block, eof));
                if (*eof) {
                    DCHECK(block->rows() == 0);
                    break;
                }
                _num_rows_read += block->rows();
                _num_byte_read += block->allocated_bytes();
            }

            // 2. Filter the output block finally.
            {
                SCOPED_TIMER(_parent->_filter_timer);
                RETURN_IF_ERROR(_filter_output_block(block));
            }
            // record rows return (after filter) for _limit check
            _num_rows_return += block->rows();
        } while (!state->is_cancelled() && block->rows() == 0 && !(*eof) &&
                 _num_rows_read < rows_read_threshold);
    }

    if (state->is_cancelled()) {
        return Status::Cancelled("cancelled");
    }

    // set eof to true if per scanner limit is reached
    // currently for query: ORDER BY key LIMIT n
    if (_limit > 0 && _num_rows_return >= _limit) {
        *eof = true;
    }

    return Status::OK();
}

Status VScanner::_filter_output_block(Block* block) {
    if (block->has(BeConsts::BLOCK_TEMP_COLUMN_SCANNER_FILTERED)) {
        // scanner filter_block is already done (only by _topn_next currently), just skip it
        return Status::OK();
    }
    auto old_rows = block->rows();
    Status st = VExprContext::filter_block(_conjuncts, block, block->columns());
    _counter.num_rows_unselected += old_rows - block->rows();
    auto all_column_names = block->get_names();
    for (auto& name : all_column_names) {
        if (name.rfind(BeConsts::BLOCK_TEMP_COLUMN_PREFIX, 0) == 0) {
            block->erase(name);
        }
    }
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
    // renew the _conjuncts
    if (!_conjuncts.empty()) {
        _discard_conjuncts();
    }
    // Notice that the number of runtime filters may be larger than _applied_rf_num.
    // But it is ok because it will be updated at next time.
    RETURN_IF_ERROR(_parent->clone_conjunct_ctxs(_conjuncts));
    _applied_rf_num = arrived_rf_num;
    return Status::OK();
}

Status VScanner::close(RuntimeState* state) {
    if (_is_closed) {
        return Status::OK();
    }

    COUNTER_UPDATE(_parent->_scanner_wait_worker_timer, _scanner_wait_worker_timer);
    _is_closed = true;
    return Status::OK();
}

void VScanner::_update_counters_before_close() {
    if (_parent) {
        COUNTER_UPDATE(_parent->_scan_cpu_timer, _scan_cpu_timer);
        COUNTER_UPDATE(_parent->_rows_read_counter, _num_rows_read);
        COUNTER_UPDATE(_parent->_byte_read_counter, _num_byte_read);
    }
    if (!_state->enable_profile() && !_is_load) return;
    // Update stats for load
    _state->update_num_rows_load_filtered(_counter.num_rows_filtered);
    _state->update_num_rows_load_unselected(_counter.num_rows_unselected);
}

} // namespace doris::vectorized
