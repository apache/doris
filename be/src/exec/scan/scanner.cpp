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

#include "exec/scan/scanner.h"

#include <glog/logging.h>

#include <algorithm>

#include "common/config.h"
#include "common/status.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column_nothing.h"
#include "exec/common/util.hpp"
#include "exec/operator/scan_operator.h"
#include "exec/scan/scan_node.h"
#include "exprs/vexpr_context.h"
#include "exprs/vslot_ref.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_profile.h"
#include "storage/segment/adaptive_block_size_predictor.h"
#include "util/concurrency_stats.h"
#include "util/defer_op.h"

namespace doris {

Scanner::Scanner(RuntimeState* state, ScanLocalStateBase* local_state, int64_t limit,
                 RuntimeProfile* profile)
        : _state(state),
          _local_state(local_state),
          _limit(limit),
          _profile(profile),
          _output_tuple_desc(_local_state->output_tuple_desc()),
          _output_row_descriptor(_local_state->_parent->output_row_descriptor()),
          _has_prepared(false) {
    _total_rf_num = cast_set<int>(_local_state->_helper.runtime_filter_nums());
    DorisMetrics::instance()->scanner_cnt->increment(1);
}

Status Scanner::init(RuntimeState* state, const VExprContextSPtrs& conjuncts) {
    // All scanners share a remaining-limit counter so a LIMIT query can
    // stop once enough rows have been collected across scanners.
    // Key TopN scans have no ordinary scan LIMIT, so each scanner can
    // independently produce its full local top-N.
    _shared_scan_limit = _local_state->shared_scan_limit_ptr();

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

    // Pure slot-ref projections do not compute new values; they only reorder or forward scan
    // columns. Record the source column ids once here so the hot projection path can move the
    // existing ColumnPtr instead of executing VSlotRef and then mutating it, which would clone
    // the column when the input block still shares the same pointer.
    if (!_projections.empty() && _intermediate_projections.empty()) {
        _direct_slot_ref_projection_column_ids.reserve(_projections.size());
        for (const auto& projection : _projections) {
            auto slot_ref = std::dynamic_pointer_cast<VSlotRef>(projection->root());
            if (slot_ref == nullptr) {
                _direct_slot_ref_projection_column_ids.clear();
                break;
            }
            _direct_slot_ref_projection_column_ids.push_back(slot_ref->column_id());
        }
        if (!_direct_slot_ref_projection_column_ids.empty() && _conjuncts.empty() && _limit <= 0 &&
            _shared_scan_limit == nullptr && _output_row_descriptor != nullptr &&
            _output_row_descriptor->num_materialized_slots() ==
                    _direct_slot_ref_projection_column_ids.size()) {
            size_t row_bytes = 0;
            bool all_fixed_width = true;
            for (const auto& tuple_desc : _output_row_descriptor->tuple_descriptors()) {
                for (const auto* slot_desc : tuple_desc->slots()) {
                    const auto& type = slot_desc->get_data_type_ptr();
                    if (!type->have_maximum_size_of_value()) {
                        all_fixed_width = false;
                        break;
                    }
                    row_bytes += std::max<size_t>(1, type->get_size_of_value_in_memory());
                }
                if (!all_fixed_width) {
                    break;
                }
            }
            if (all_fixed_width && row_bytes > 0) {
                // This enables OLAP storage to produce fewer, larger blocks for the hot arithmetic
                // scan shape. Do it before page decoding rather than by merging blocks later:
                // MutableBlock::merge() appends through generic Column APIs and would materialize
                // the page-backed fixed-width spans that the storage decoder can otherwise forward
                // without copying. Varlen and complex slots stay on the session batch size because
                // their per-row memory is data-dependent and large blocks can amplify memory spikes.
                _direct_slot_ref_projection_row_bytes = row_bytes;
            }
        }
    }

    return Status::OK();
}

int Scanner::_storage_read_batch_size(RuntimeState* state) const {
    const int session_batch_size = state->batch_size();
    if (_direct_slot_ref_projection_row_bytes == 0) {
        return session_batch_size;
    }

    // Keep the same hard upper bound used by the segment adaptive reader and by the public
    // batch_size contract. The byte budget decides whether a fixed-width scan can safely use that
    // many rows; the session batch size remains the lower bound so this path never shrinks existing
    // scans.
    static constexpr size_t kMaxReadBatchRows = AdaptiveBlockSizePredictor::kDefaultBlockSizeRows;
    const size_t rows_by_bytes =
            state->preferred_block_size_bytes() / _direct_slot_ref_projection_row_bytes;
    if (rows_by_bytes == 0) {
        return session_batch_size;
    }
    const size_t target_rows =
            std::max<size_t>(session_batch_size, std::min(kMaxReadBatchRows, rows_by_bytes));
    return static_cast<int>(target_rows);
}

Status Scanner::get_block_after_projects(RuntimeState* state, Block* block, bool* eos) {
    SCOPED_CONCURRENCY_COUNT(ConcurrencyStatsManager::instance().vscanner_get_block);
    auto& row_descriptor = _local_state->_parent->row_descriptor();
    if (_output_row_descriptor) {
        _origin_block.clear_column_data(row_descriptor.num_materialized_slots());
        const auto min_batch_size = std::max(state->batch_size() / 2, 1);
        const auto block_max_bytes = state->preferred_block_size_bytes();
        while (_padding_block.rows() < min_batch_size && _padding_block.bytes() < block_max_bytes &&
               !*eos) {
            RETURN_IF_ERROR(get_block(state, &_origin_block, eos));
            if (*eos) {
                // For the final block, merge any padding directly and return eos in this call.
                // The merged tail can be larger than the target batch, but each source block is
                // already bounded by the lower scanner.
                RETURN_IF_ERROR(_merge_padding_block());
                _origin_block.clear_column_data(row_descriptor.num_materialized_slots());
                break;
            }
            if (_origin_block.rows() >= min_batch_size) {
                break;
            }

            if (_origin_block.rows() + _padding_block.rows() <= state->batch_size() &&
                _origin_block.bytes() + _padding_block.bytes() <= block_max_bytes) {
                RETURN_IF_ERROR(_merge_padding_block());
                _origin_block.clear_column_data(row_descriptor.num_materialized_slots());
            } else {
                if (_origin_block.rows() < _padding_block.rows()) {
                    _padding_block.swap(_origin_block);
                }
                break;
            }
        }

        if (_origin_block.empty() && !_padding_block.empty()) {
            _padding_block.swap(_origin_block);
        }
        return _do_projections(&_origin_block, block);
    } else {
        return get_block(state, block, eos);
    }
}

Status Scanner::get_block(RuntimeState* state, Block* block, bool* eof) {
    // only empty block should be here
    DCHECK(block->rows() == 0);

    // Stop early if other scanners have already collected enough rows
    // for the SQL LIMIT. Skipped when _shared_scan_limit is null (topn
    // path or no LIMIT).
    if (_shared_scan_limit && _shared_scan_limit->load(std::memory_order_acquire) <= 0) {
        *eof = true;
        return Status::OK();
    }

    // scanner running time
    SCOPED_RAW_TIMER(&_per_scanner_timer);
    int64_t rows_read_threshold = _num_rows_read + config::doris_scanner_row_num;
    if (!block->mem_reuse()) {
        for (auto* const slot_desc : _output_tuple_desc->slots()) {
            block->insert(ColumnWithTypeAndName(slot_desc->get_empty_mutable_column(),
                                                slot_desc->get_data_type_ptr(),
                                                slot_desc->col_name()));
        }
    }

    {
        do {
            // 1. Get input block from scanner
            {
                // get block time
                SCOPED_TIMER(_local_state->_scan_timer);
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
                SCOPED_TIMER(_local_state->_filter_timer);
                RETURN_IF_ERROR(_filter_output_block(block));
            }
            // record rows return (after filter) for _limit check
            _num_rows_return += block->rows();
            // Publish progress to the shared counter so peer scanners can
            // observe it. The counter may go negative when several scanners
            // subtract concurrently; that is harmless because the operator's
            // reached_limit() makes the final cut.
            if (_shared_scan_limit && block->rows() > 0) {
                _shared_scan_limit->fetch_sub(block->rows(), std::memory_order_acq_rel);
            }
        } while (!_should_stop && !state->is_cancelled() && block->rows() == 0 && !(*eof) &&
                 _num_rows_read < rows_read_threshold);
    }

    if (state->is_cancelled()) {
        // TODO: Should return the specific ErrorStatus instead of just Cancelled.
        return Status::Cancelled("cancelled");
    }
    *eof = *eof || _should_stop;
    // set eof to true if per scanner limit is reached
    // currently for query: ORDER BY key LIMIT n
    *eof = *eof || (_limit > 0 && _num_rows_return >= _limit);
    *eof = *eof || (_shared_scan_limit && _shared_scan_limit->load(std::memory_order_acquire) <= 0);

    return Status::OK();
}

Status Scanner::_filter_output_block(Block* block) {
    auto old_rows = block->rows();
    Status st = VExprContext::filter_block(_conjuncts, block, block->columns());
    _counter.num_rows_unselected += old_rows - block->rows();
    return st;
}

Status Scanner::_do_projections(Block* origin_block, Block* output_block) {
    SCOPED_RAW_TIMER(&_per_scanner_timer);
    SCOPED_RAW_TIMER(&_projection_timer);

    const size_t rows = origin_block->rows();
    if (rows == 0) {
        return Status::OK();
    }
    Block input_block = *origin_block;

    std::vector<int> result_column_ids;
    for (auto& projections : _intermediate_projections) {
        result_column_ids.resize(projections.size());
        for (int i = 0; i < projections.size(); i++) {
            RETURN_IF_ERROR(projections[i]->execute(&input_block, &result_column_ids[i]));
        }
        input_block.shuffle_columns(result_column_ids);
    }

    DCHECK_EQ(rows, input_block.rows());
    // Fast path for the common scan shape: PhysicalProject only forwards slot refs and the
    // output slot type is exactly the same as the scan column type. The exact type check keeps
    // casts/nullability changes on the generic expression path; only a pure column ownership
    // transfer is handled here.
    if (!_direct_slot_ref_projection_column_ids.empty() &&
        _direct_slot_ref_projection_column_ids.size() == _projections.size()) {
        bool can_use_direct_projection = true;
        auto direct_projection_columns =
                VectorizedUtils::create_columns_with_type_and_name(*_output_row_descriptor);
        if (direct_projection_columns.size() != _direct_slot_ref_projection_column_ids.size()) {
            return Status::InternalError(
                    "direct slot ref projection size mismatch, output columns {}, projections {}",
                    direct_projection_columns.size(),
                    _direct_slot_ref_projection_column_ids.size());
        }
        for (size_t i = 0; i < _direct_slot_ref_projection_column_ids.size(); ++i) {
            auto column_id = _direct_slot_ref_projection_column_ids[i];
            if (column_id < 0 || column_id >= origin_block->columns()) {
                return Status::InternalError(
                        "slot ref projection column id {} is out of range, origin block columns {}",
                        column_id, origin_block->columns());
            }
            const auto& src = origin_block->get_by_position(column_id);
            if (!direct_projection_columns[i].type->equals(*src.type)) {
                can_use_direct_projection = false;
                break;
            }
        }
        if (can_use_direct_projection) {
            return _do_direct_slot_ref_projection(origin_block, output_block,
                                                  std::move(direct_projection_columns));
        }
    }

    auto scoped_mutable_block = VectorizedUtils::build_scoped_mutable_mem_reuse_block(
            output_block, *_output_row_descriptor);
    auto& mutable_block = scoped_mutable_block.mutable_block();

    auto& mutable_columns = mutable_block.mutable_columns();

    DCHECK_EQ(mutable_columns.size(), _projections.size());

    for (int i = 0; i < mutable_columns.size(); ++i) {
        ColumnPtr column_ptr;
        RETURN_IF_ERROR(_projections[i]->execute(&input_block, column_ptr));
        column_ptr = column_ptr->convert_to_full_column_if_const();
        if (mutable_columns[i]->is_nullable() != column_ptr->is_nullable()) {
            throw Exception(ErrorCode::INTERNAL_ERROR, "Nullable mismatch");
        }
        mutable_columns[i] = IColumn::mutate(std::move(column_ptr));
    }

    scoped_mutable_block.restore();

    // origin columns was moved into output_block, so we need to set origin_block to empty columns
    auto empty_columns = origin_block->clone_empty_columns();
    origin_block->set_columns(std::move(empty_columns));
    DCHECK_EQ(output_block->rows(), rows);

    return Status::OK();
}

Status Scanner::_do_direct_slot_ref_projection(Block* origin_block, Block* output_block,
                                               ColumnsWithTypeAndName&& output_columns) {
    const size_t rows = origin_block->rows();

    for (size_t i = 0; i < _direct_slot_ref_projection_column_ids.size(); ++i) {
        auto column_id = _direct_slot_ref_projection_column_ids[i];
        if (column_id < 0 || column_id >= origin_block->columns()) {
            return Status::InternalError(
                    "slot ref projection column id {} is out of range, origin block columns {}",
                    column_id, origin_block->columns());
        }

        auto column =
                origin_block->get_by_position(column_id).column->convert_to_full_column_if_const();
        if (column->size() != rows) {
            return Status::InternalError(
                    "direct slot ref projection result column size {} not equal input rows {}",
                    column->size(), rows);
        }
        output_columns[i].column = std::move(column);
    }

    Block projected_block(std::move(output_columns));
    output_block->swap(projected_block);

    // The output block now owns the scan columns. Replace the origin block with empty columns so
    // later block reuse/destruction does not keep an extra shared reference that would make later
    // mutable paths clone the forwarded columns.
    auto empty_columns = origin_block->clone_empty_columns();
    origin_block->set_columns(std::move(empty_columns));
    DCHECK_EQ(output_block->rows(), rows);

    return Status::OK();
}

Status Scanner::try_append_late_arrival_runtime_filter() {
    if (_applied_rf_num == _total_rf_num) {
        return Status::OK();
    }
    DCHECK(_applied_rf_num < _total_rf_num);
    int arrived_rf_num = 0;
    RETURN_IF_ERROR(_local_state->update_late_arrival_runtime_filter(_state, arrived_rf_num));

    if (arrived_rf_num == _applied_rf_num) {
        // No newly arrived runtime filters, just return;
        return Status::OK();
    }

    // avoid conjunct destroy in used by storage layer
    _conjuncts.clear();
    RETURN_IF_ERROR(_local_state->clone_conjunct_ctxs(_conjuncts));
    _applied_rf_num = arrived_rf_num;
    return Status::OK();
}

Status Scanner::close(RuntimeState* state) {
#ifndef BE_TEST
    COUNTER_UPDATE(_local_state->_scanner_wait_worker_timer, _scanner_wait_worker_timer);
#endif
    return Status::OK();
}

bool Scanner::_try_close() {
    bool expected = false;
    return _is_closed.compare_exchange_strong(expected, true);
}

void Scanner::_collect_profile_before_close() {
    COUNTER_UPDATE(_local_state->_scan_cpu_timer, _scan_cpu_timer);
    COUNTER_UPDATE(_local_state->_rows_read_counter, _num_rows_read);

    // Update stats for load. See _should_update_load_counters() for why this is gated.
    if (_should_update_load_counters()) {
        _state->update_num_rows_load_filtered(_counter.num_rows_filtered);
        _state->update_num_rows_load_unselected(_counter.num_rows_unselected);
    }
}

void Scanner::_update_scan_cpu_timer() {
    int64_t cpu_time = _cpu_watch.elapsed_time();
    _scan_cpu_timer += cpu_time;
    if (_state && _state->get_query_ctx()) {
        _state->get_query_ctx()->resource_ctx()->cpu_context()->update_cpu_cost_ms(cpu_time);
    }
}

} // namespace doris
