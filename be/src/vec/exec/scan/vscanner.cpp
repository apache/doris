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

VScanner::VScanner(RuntimeState* state, VScanNode* parent, int64_t limit, MemTracker* mem_tracker)
        : _state(state),
          _parent(parent),
          _limit(limit),
          _mem_tracker(mem_tracker),
          _input_tuple_desc(parent->input_tuple_desc()),
          _output_tuple_desc(parent->output_tuple_desc()) {
    _real_tuple_desc = _input_tuple_desc != nullptr ? _input_tuple_desc : _output_tuple_desc;
    _total_rf_num = _parent->runtime_filter_num();
    _is_load = (_input_tuple_desc != nullptr);
}

Status VScanner::get_block(RuntimeState* state, Block* block, bool* eof) {
    // only empty block should be here
    DCHECK(block->rows() == 0);
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker);

    int64_t raw_rows_threshold = raw_rows_read() + config::doris_scanner_row_num;
    if (!block->mem_reuse()) {
        for (const auto slot_desc : _output_tuple_desc->slots()) {
            block->insert(ColumnWithTypeAndName(slot_desc->get_empty_mutable_column(),
                                                slot_desc->get_data_type_ptr(),
                                                slot_desc->col_name()));
        }
    }

    _init_input_block(block);
    {
        do {
            // 1. Get input block from scanner
            {
                SCOPED_TIMER(_parent->_scan_timer);
                RETURN_IF_ERROR(_get_block_impl(state, _input_block_ptr, eof));
                if (*eof) {
                    DCHECK(_input_block_ptr->rows() == 0);
                    break;
                }
                _num_rows_read += _input_block_ptr->rows();
            }

            // 2. For load, use prefilter to filter the input block first.
            {
                SCOPED_TIMER(_parent->_prefilter_timer);
                RETURN_IF_ERROR(_filter_input_block(_input_block_ptr));
            }

            // 3. For load, convert input block to output block
            {
                SCOPED_TIMER(_parent->_convert_block_timer);
                RETURN_IF_ERROR(_convert_to_output_block(block));
            }
ÅÅ
            // 4. Filter the output block finally.
            //    NOTE that step 2/3 may be skipped, for Query.
            {
                SCOPED_TIMER(_parent->_filter_timer);
                RETURN_IF_ERROR(_filter_output_block(block));
            }
        } while (block->rows() == 0 && !(*eof) && raw_rows_read() < raw_rows_threshold);
    }

    return Status::OK();
}

void VScanner::_init_input_block(Block* output_block) {
    if (_input_tuple_desc == nullptr) {
        _input_block_ptr = output_block;
        return;
    }

    // init the input block used for scanner.
    _input_block.clear();
    _input_block_ptr = &_input_block;
    DCHECK(_input_block.columns() == 0);

    for (auto& slot_desc : _input_tuple_desc->slots()) {
        auto data_type = slot_desc->get_data_type_ptr();
        _input_block.insert(vectorized::ColumnWithTypeAndName(
                data_type->create_column(), slot_desc->get_data_type_ptr(), slot_desc->col_name()));
    }
}

Status VScanner::_filter_input_block(Block* block) {
    if (_pre_conjunct_ctx_ptr == nullptr) {
        return Status::OK();
    }
    auto origin_column_num = block->columns();
    auto old_rows = block->rows();
    RETURN_IF_ERROR(vectorized::VExprContext::filter_block(_pre_conjunct_ctx_ptr, block,
                                                           origin_column_num));
    // _counter->num_rows_unselected += old_rows - _src_block.rows();
    return Status::OK();
}

Status VScanner::_convert_to_output_block(Block* output_block) {
    if (_input_block_ptr == output_block) {
        return Status::OK();
    }
    // TODO: implement

    return Status::OK();
}

Status VScanner::_filter_output_block(Block* block) {
    return VExprContext::filter_block(_vconjunct_ctx, block, _output_tuple_desc->slots().size());
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
    if (_pre_conjunct_ctx) {
        _pre_conjunct_ctx->close(state);
    }

    COUNTER_UPDATE(_parent->_scanner_wait_worker_timer, _scanner_wait_worker_timer);
    _is_closed = true;
    return Status::OK();
}

void VScanner::_update_counters_before_close() {
    if (!_state->enable_profile()) return;
    COUNTER_UPDATE(_parent->_rows_read_counter, _num_rows_read);
}

} // namespace doris::vectorized
