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
#include "pipeline/exec/scan_operator.h"
#include "runtime/descriptors.h"
#include "util/defer_op.h"
#include "util/runtime_profile.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/exec/scan/vscan_node.h"
#include "vec/exprs/vexpr_context.h"

namespace doris::vectorized {

VScanner::VScanner(RuntimeState* state, pipeline::ScanLocalStateBase* local_state, int64_t limit,
                   RuntimeProfile* profile)
        : _state(state),
          _local_state(local_state),
          _limit(limit),
          _profile(profile),
          _output_tuple_desc(_local_state->output_tuple_desc()),
          _output_row_descriptor(_local_state->_parent->output_row_descriptor()) {
    _total_rf_num = _local_state->runtime_filter_num();
}

Status VScanner::prepare(RuntimeState* state, const VExprContextSPtrs& conjuncts) {
    if (!conjuncts.empty()) {
        _conjuncts.resize(conjuncts.size());
        for (size_t i = 0; i != conjuncts.size(); ++i) {
            RETURN_IF_ERROR(conjuncts[i]->clone(state, _conjuncts[i]));
        }
    }

    const auto& projections = _local_state->_projections;
    if (!projections.empty()) {
        _projections.resize(projections.size());
        for (size_t i = 0; i != projections.size(); ++i) {
            RETURN_IF_ERROR(projections[i]->clone(state, _projections[i]));
        }
    }

    const auto& intermediate_projections = _local_state->_intermediate_projections;
    if (!intermediate_projections.empty()) {
        _intermediate_projections.resize(intermediate_projections.size());
        for (int i = 0; i < intermediate_projections.size(); i++) {
            _intermediate_projections[i].resize(intermediate_projections[i].size());
            for (int j = 0; j < intermediate_projections[i].size(); j++) {
                RETURN_IF_ERROR(intermediate_projections[i][j]->clone(
                        state, _intermediate_projections[i][j]));
            }
        }
    }

    return Status::OK();
}

Status VScanner::get_block_after_projects(RuntimeState* state, vectorized::Block* block,
                                          bool* eos) {
    auto& row_descriptor = _local_state->_parent->row_descriptor();
    if (_output_row_descriptor) {
        _origin_block.clear_column_data(row_descriptor.num_materialized_slots());
        auto status = get_block(state, &_origin_block, eos);
        if (UNLIKELY(!status.ok())) return status;
        return _do_projections(&_origin_block, block);
    }
    return get_block(state, block, eos);
}

Status VScanner::get_block(RuntimeState* state, Block* block, bool* eof) {
    // only empty block should be here
    DCHECK(block->rows() == 0);
    // scanner running time
    SCOPED_RAW_TIMER(&_per_scanner_timer);
    int64_t rows_read_threshold = _num_rows_read + config::doris_scanner_row_num;
    if (!block->mem_reuse()) {
        for (auto* const slot_desc : _output_tuple_desc->slots()) {
            if (!slot_desc->need_materialize()) {
                // should be ignore from reading
                continue;
            }
            block->insert(ColumnWithTypeAndName(slot_desc->get_empty_mutable_column(),
                                                slot_desc->get_data_type_ptr(),
                                                slot_desc->col_name()));
        }
    }

    int64_t old_scan_rows = _num_rows_read;
    int64_t old_scan_bytes = _num_byte_read;
    {
        do {
            // if step 2 filter all rows of block, and block will be reused to get next rows,
            // must clear row_same_bit of block, or will get wrong row_same_bit.size() which not equal block.rows()
            block->clear_same_bit();
            // 1. Get input block from scanner
            {
                // get block time
                auto* timer = _local_state->_scan_timer;
                SCOPED_TIMER(timer);
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
                auto* timer = _local_state->_filter_timer;
                SCOPED_TIMER(timer);
                RETURN_IF_ERROR(_filter_output_block(block));
            }
            // record rows return (after filter) for _limit check
            _num_rows_return += block->rows();
        } while (!_should_stop && !state->is_cancelled() && block->rows() == 0 && !(*eof) &&
                 _num_rows_read < rows_read_threshold);
    }

    if (_query_statistics) {
        _query_statistics->add_scan_rows(_num_rows_read - old_scan_rows);
        _query_statistics->add_scan_bytes(_num_byte_read - old_scan_bytes);
    }

    if (state->is_cancelled()) {
        return Status::Cancelled("cancelled");
    }
    *eof = *eof || _should_stop;
    // set eof to true if per scanner limit is reached
    // currently for query: ORDER BY key LIMIT n
    *eof = *eof || (_limit > 0 && _num_rows_return >= _limit);

    return Status::OK();
}

Status VScanner::_filter_output_block(Block* block) {
    Defer clear_tmp_block([&]() {
        auto all_column_names = block->get_names();
        for (auto& name : all_column_names) {
            if (name.rfind(BeConsts::BLOCK_TEMP_COLUMN_PREFIX, 0) == 0) {
                block->erase(name);
            }
        }
    });
    if (block->has(BeConsts::BLOCK_TEMP_COLUMN_SCANNER_FILTERED)) {
        // scanner filter_block is already done (only by _topn_next currently), just skip it
        return Status::OK();
    }
    auto old_rows = block->rows();
    Status st = VExprContext::filter_block(_conjuncts, block, block->columns());
    _counter.num_rows_unselected += old_rows - block->rows();
    return st;
}

Status VScanner::_do_projections(vectorized::Block* origin_block, vectorized::Block* output_block) {
    SCOPED_RAW_TIMER(&_per_scanner_timer);
    SCOPED_RAW_TIMER(&_projection_timer);

    const size_t rows = origin_block->rows();
    if (rows == 0) {
        return Status::OK();
    }
    vectorized::Block input_block = *origin_block;

    std::vector<int> result_column_ids;
    for (auto& projections : _intermediate_projections) {
        result_column_ids.resize(projections.size());
        for (int i = 0; i < projections.size(); i++) {
            RETURN_IF_ERROR(projections[i]->execute(&input_block, &result_column_ids[i]));
        }
        input_block.shuffle_columns(result_column_ids);
    }

    DCHECK_EQ(rows, input_block.rows());
    MutableBlock mutable_block =
            VectorizedUtils::build_mutable_mem_reuse_block(output_block, *_output_row_descriptor);

    auto& mutable_columns = mutable_block.mutable_columns();

    DCHECK_EQ(mutable_columns.size(), _projections.size());

    for (int i = 0; i < mutable_columns.size(); ++i) {
        auto result_column_id = -1;
        RETURN_IF_ERROR(_projections[i]->execute(&input_block, &result_column_id));
        auto column_ptr = input_block.get_by_position(result_column_id)
                                  .column->convert_to_full_column_if_const();
        //TODO: this is a quick fix, we need a new function like "change_to_nullable" to do it
        if (mutable_columns[i]->is_nullable() xor column_ptr->is_nullable()) {
            DCHECK(mutable_columns[i]->is_nullable() && !column_ptr->is_nullable());
            reinterpret_cast<ColumnNullable*>(mutable_columns[i].get())
                    ->insert_range_from_not_nullable(*column_ptr, 0, rows);
        } else {
            mutable_columns[i]->insert_range_from(*column_ptr, 0, rows);
        }
    }
    DCHECK(mutable_block.rows() == rows);
    output_block->set_columns(std::move(mutable_columns));

    return Status::OK();
}

Status VScanner::try_append_late_arrival_runtime_filter() {
    if (_applied_rf_num == _total_rf_num) {
        return Status::OK();
    }
    DCHECK(_applied_rf_num < _total_rf_num);

    int arrived_rf_num = 0;
    RETURN_IF_ERROR(_local_state->try_append_late_arrival_runtime_filter(&arrived_rf_num));

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
    RETURN_IF_ERROR(_local_state->clone_conjunct_ctxs(_conjuncts));
    _applied_rf_num = arrived_rf_num;
    return Status::OK();
}

Status VScanner::close(RuntimeState* state) {
    if (_is_closed) {
        return Status::OK();
    }

    COUNTER_UPDATE(_local_state->_scanner_wait_worker_timer, _scanner_wait_worker_timer);
    _is_closed = true;
    return Status::OK();
}

void VScanner::_collect_profile_before_close() {
    COUNTER_UPDATE(_local_state->_scan_cpu_timer, _scan_cpu_timer);
    COUNTER_UPDATE(_local_state->_rows_read_counter, _num_rows_read);
    if (!_state->enable_profile() && !_is_load) return;
    // Update stats for load
    _state->update_num_rows_load_filtered(_counter.num_rows_filtered);
    _state->update_num_rows_load_unselected(_counter.num_rows_unselected);
}

} // namespace doris::vectorized
