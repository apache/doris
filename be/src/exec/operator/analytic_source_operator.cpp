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

#include "exec/operator/analytic_source_operator.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <ranges>
#include <string>

#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "exec/operator/operator.h"
#include "exec/spill/spill_file.h"
#include "exec/spill/spill_file_reader.h"
#include "exprs/vectorized_agg_fn.h"

namespace doris {

AnalyticLocalState::AnalyticLocalState(RuntimeState* state, OperatorXBase* parent)
        : Base(state, parent) {}

Status AnalyticLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _get_next_timer = ADD_TIMER(custom_profile(), "GetNextTime");
    _filtered_rows_counter = ADD_COUNTER(custom_profile(), "FilteredRows", TUnit::UNIT);
    _partition_replay_timer = ADD_TIMER(custom_profile(), "PartitionReplayTime");
    return Status::OK();
}

Status AnalyticLocalState::close(RuntimeState* state) {
    if (_closed) {
        return Status::OK();
    }
    _finish_spill_batch();
    return Base::close(state);
}

void AnalyticLocalState::_finish_spill_batch() {
    if (_batch_reader) {
        auto st = _batch_reader->close();
        LOG_IF(WARNING, !st.ok()) << "close analytic spill batch reader failed: " << st;
        _batch_reader.reset();
    }
    if (_peer_group_reader) {
        auto st = _peer_group_reader->close();
        LOG_IF(WARNING, !st.ok()) << "close analytic spill peer group reader failed: " << st;
        _peer_group_reader.reset();
    }
    COUNTER_UPDATE(_memory_used_counter, -_in_memory_batch_bytes);
    _in_memory_batch_bytes = 0;
    _current_batch.reset();
    _replay_block.clear();
    _replay_block_position = 0;
    _peer_group_block.clear();
}

Status AnalyticLocalState::_open_spill_batch(RuntimeState* state,
                                             std::shared_ptr<AnalyticSpillBatch> batch) {
    DCHECK(batch != nullptr);
    DCHECK_GT(batch->rows, 0);
    DORIS_CHECK(!batch->partition_ends.empty());
    DORIS_CHECK_EQ(batch->partition_ends.back(), batch->rows);
    DORIS_CHECK_EQ(batch->strategies.size(), batch->result_types.size());
    DORIS_CHECK_EQ(batch->strategies.size(), batch->peer_functions.size());
    DORIS_CHECK_EQ(batch->strategies.size(), batch->partition_results.size());
    DORIS_CHECK_EQ(batch->strategies.size(), batch->function_parameters.size());
    DORIS_CHECK_EQ(batch->strategies.size(), batch->change_to_nullable_flags.size());
    _current_batch = std::move(batch);
    _in_memory_block_index = 0;
    _batch_output_position = 0;
    _partition_index = 0;
    _partition_start = 0;
    _partition_end = _current_batch->partition_ends[0];
    _peer_group_start = 0;
    _peer_group_end = 0;
    _peer_group_block_position = 0;
    _in_memory_peer_group_index = 0;
    _peer_group_file_eos = false;
    for (const auto& block : _current_batch->blocks) {
        _in_memory_batch_bytes += block.allocated_bytes();
    }
    COUNTER_UPDATE(_memory_used_counter, _in_memory_batch_bytes);

    if (_current_batch->data_file) {
        _batch_reader = _current_batch->data_file->create_reader(state, operator_profile());
        RETURN_IF_ERROR(_batch_reader->open());
    }
    if (_current_batch->peer_group_file) {
        _peer_group_reader =
                _current_batch->peer_group_file->create_reader(state, operator_profile());
        RETURN_IF_ERROR(_peer_group_reader->open());
    }

    _has_peer_group_function =
            std::ranges::any_of(_current_batch->strategies, [](WindowSpillStrategy strategy) {
                return strategy == WindowSpillStrategy::PEER_GROUP;
            });
    if (_has_peer_group_function) {
        RETURN_IF_ERROR(_next_peer_group_end(state));
    }
    return Status::OK();
}

Status AnalyticLocalState::_read_batch_block(RuntimeState* state, Block* block, bool* batch_eos) {
    RETURN_IF_CANCELLED(state);
    if (_batch_reader) {
        return _batch_reader->read(block, batch_eos);
    }
    if (_in_memory_block_index >= _current_batch->blocks.size()) {
        *batch_eos = true;
        block->clear();
        return Status::OK();
    }
    auto& next_block = _current_batch->blocks[_in_memory_block_index++];
    const auto block_bytes = static_cast<int64_t>(next_block.allocated_bytes());
    COUNTER_UPDATE(_memory_used_counter, -block_bytes);
    _in_memory_batch_bytes -= block_bytes;
    block->swap(std::move(next_block));
    *batch_eos = false;
    return Status::OK();
}

Status AnalyticLocalState::_next_replay_rows(RuntimeState* state, Block* block, bool* batch_eos) {
    while (_replay_block_position >= _replay_block.rows()) {
        _replay_block.clear();
        _replay_block_position = 0;
        RETURN_IF_ERROR(_read_batch_block(state, &_replay_block, batch_eos));
        if (*batch_eos) {
            return Status::OK();
        }
    }
    *batch_eos = false;
    // Spilled Blocks are coalesced up to the spill buffer size, so a replayed Block can be much
    // larger than the batch size expected by downstream operators.
    DCHECK_GT(state->batch_size(), 0);
    const auto batch_size = static_cast<size_t>(state->batch_size());
    const size_t rows = std::min(batch_size, _replay_block.rows() - _replay_block_position);
    if (_replay_block_position == 0 && rows == _replay_block.rows()) {
        block->swap(_replay_block);
        _replay_block.clear();
        return Status::OK();
    }
    Block slice;
    for (const auto& column : _replay_block) {
        slice.insert({column.column->cut(_replay_block_position, rows), column.type, column.name});
    }
    block->swap(slice);
    _replay_block_position += rows;
    return Status::OK();
}

size_t AnalyticLocalState::_spill_replay_reserve_bytes(RuntimeState* state) const {
    if (!_shared_state->spill_enabled.load() || _replay_block_position < _replay_block.rows()) {
        return 0;
    }
    // The next call reads a new Block. A spilled record holds Blocks coalesced up to about the
    // spill buffer size and is deserialized into a new Block. Opening the reader of the next
    // batch additionally allocates the buffer for its largest serialized record, and it is not
    // known yet whether that batch was spilled.
    const auto spill_buffer_bytes = static_cast<size_t>(state->spill_buffer_size_bytes());
    if (_current_batch && _batch_output_position < _current_batch->rows) {
        return _current_batch->data_file ? spill_buffer_bytes : 0;
    }
    return 2 * spill_buffer_bytes;
}

Status AnalyticLocalState::_next_peer_group_end(RuntimeState* state) {
    RETURN_IF_CANCELLED(state);
    _peer_group_start = _peer_group_end;
    if (!_peer_group_reader) {
        DORIS_CHECK_LT(_in_memory_peer_group_index, _current_batch->peer_group_ends.size());
        _peer_group_end = _current_batch->peer_group_ends[_in_memory_peer_group_index++];
    } else {
        while (_peer_group_block_position >= _peer_group_block.rows()) {
            _peer_group_block.clear();
            RETURN_IF_ERROR(_peer_group_reader->read(&_peer_group_block, &_peer_group_file_eos));
            _peer_group_block_position = 0;
            DORIS_CHECK(!_peer_group_file_eos || !_peer_group_block.empty());
        }
        const auto& column =
                assert_cast<const ColumnInt64&>(*_peer_group_block.get_by_position(0).column);
        _peer_group_end = column.get_data()[_peer_group_block_position++];
    }
    DORIS_CHECK_GT(_peer_group_end, _peer_group_start);
    DORIS_CHECK_LE(_peer_group_end, _current_batch->rows);
    return Status::OK();
}

void AnalyticLocalState::_next_spill_partition() {
    ++_partition_index;
    DORIS_CHECK_LT(_partition_index, _current_batch->partition_ends.size());
    _partition_start = _partition_end;
    _partition_end = _current_batch->partition_ends[_partition_index];
    DORIS_CHECK_GT(_partition_end, _partition_start);
}

Status AnalyticLocalState::_append_spill_results(RuntimeState* state, Block* block) {
    const auto rows = block->rows();
    DCHECK_GT(rows, 0);
    DCHECK_LE(_batch_output_position + static_cast<int64_t>(rows), _current_batch->rows);
    const auto function_count = _current_batch->strategies.size();
    std::vector<MutableColumnPtr> results(function_count);
    std::vector<IColumn*> computed_result_columns(function_count, nullptr);
    _create_spill_result_columns(rows, results, computed_result_columns);

    size_t offset = 0;
    while (offset < rows) {
        while (_batch_output_position >= _partition_end) {
            _next_spill_partition();
        }
        const size_t segment_rows =
                std::min<size_t>(rows - offset, _partition_end - _batch_output_position);
        _append_partition_results(segment_rows, computed_result_columns, results);
        if (_has_peer_group_function) {
            RETURN_IF_ERROR(
                    _append_peer_group_results(state, segment_rows, computed_result_columns));
        }
        _batch_output_position += segment_rows;
        offset += segment_rows;
    }
    _insert_spill_result_columns(block, results);
    return Status::OK();
}

void AnalyticLocalState::_create_spill_result_columns(
        size_t rows, std::vector<MutableColumnPtr>& results,
        std::vector<IColumn*>& computed_result_columns) const {
    const auto function_count = _current_batch->strategies.size();
    for (size_t i = 0; i < function_count; ++i) {
        results[i] = _current_batch->result_types[i]->create_column();
        results[i]->reserve(rows);
        if (_current_batch->strategies[i] == WindowSpillStrategy::PARTITION_CARDINALITY ||
            _current_batch->strategies[i] == WindowSpillStrategy::PEER_GROUP) {
            if (_current_batch->result_types[i]->is_nullable()) {
                auto* nullable = assert_cast<ColumnNullable*>(results[i].get());
                nullable->get_null_map_data().resize_fill(rows, 0);
                computed_result_columns[i] = &nullable->get_nested_column();
            } else {
                computed_result_columns[i] = results[i].get();
            }
        }
    }
}

void AnalyticLocalState::_append_partition_results(
        size_t rows, const std::vector<IColumn*>& computed_result_columns,
        std::vector<MutableColumnPtr>& results) const {
    const auto function_count = _current_batch->strategies.size();
    for (size_t i = 0; i < function_count; ++i) {
        switch (_current_batch->strategies[i]) {
        case WindowSpillStrategy::PARTITION_REDUCE:
            DCHECK(_current_batch->partition_results[i]);
            results[i]->insert_many_from(*_current_batch->partition_results[i], _partition_index,
                                         rows);
            break;
        case WindowSpillStrategy::PARTITION_CARDINALITY:
            _append_ntile_result(i, rows, computed_result_columns[i]);
            break;
        case WindowSpillStrategy::PEER_GROUP:
            break;
        case WindowSpillStrategy::UNSUPPORTED:
            DORIS_CHECK(false);
            break;
        }
    }
}

void AnalyticLocalState::_append_ntile_result(size_t function_index, size_t rows,
                                              IColumn* result_column) const {
    const int64_t bucket_num =
            _current_batch->function_parameters[function_index][_partition_index];
    DORIS_CHECK_GT(bucket_num, 0);
    const int64_t partition_size = _partition_end - _partition_start;
    const int64_t small_bucket_size = partition_size / bucket_num;
    const int64_t big_bucket_num = partition_size % bucket_num;
    const int64_t first_small_bucket_row = big_bucket_num * (small_bucket_size + 1);
    const int64_t first_row_index = _batch_output_position - _partition_start;
    auto& data = assert_cast<ColumnInt64&>(*result_column).get_data();
    for (size_t row = 0; row < rows; ++row) {
        const int64_t row_index = first_row_index + row;
        const int64_t bucket =
                row_index >= first_small_bucket_row
                        ? big_bucket_num + 1 +
                                  (row_index - first_small_bucket_row) / small_bucket_size
                        : row_index / (small_bucket_size + 1) + 1;
        data.push_back(bucket);
    }
}

Status AnalyticLocalState::_append_peer_group_results(
        RuntimeState* state, size_t rows, const std::vector<IColumn*>& computed_result_columns) {
    const auto function_count = _current_batch->strategies.size();
    for (size_t row = 0; row < rows; ++row) {
        if ((row & 4095) == 0) {
            RETURN_IF_CANCELLED(state);
        }
        const int64_t batch_row = _batch_output_position + row;
        while (batch_row >= _peer_group_end) {
            RETURN_IF_ERROR(_next_peer_group_end(state));
        }
        DCHECK_GE(_peer_group_start, _partition_start);
        DCHECK_LE(_peer_group_end, _partition_end);
        for (size_t i = 0; i < function_count; ++i) {
            if (_current_batch->strategies[i] != WindowSpillStrategy::PEER_GROUP) {
                continue;
            }
            assert_cast<ColumnFloat64&>(*computed_result_columns[i])
                    .get_data()
                    .push_back(_current_peer_group_result(i));
        }
    }
    return Status::OK();
}

double AnalyticLocalState::_current_peer_group_result(size_t function_index) const {
    const auto peer_function = _current_batch->peer_functions[function_index];
    DCHECK(peer_function != WindowSpillPeerFunction::NONE);
    const int64_t partition_rows = _partition_end - _partition_start;
    if (peer_function == WindowSpillPeerFunction::PERCENT_RANK) {
        return partition_rows <= 1 ? 0.0
                                   : static_cast<double>(_peer_group_start - _partition_start) /
                                             static_cast<double>(partition_rows - 1);
    }
    DCHECK(peer_function == WindowSpillPeerFunction::CUME_DIST);
    return static_cast<double>(_peer_group_end - _partition_start) /
           static_cast<double>(partition_rows);
}

void AnalyticLocalState::_insert_spill_result_columns(
        Block* block, std::vector<MutableColumnPtr>& results) const {
    const auto function_count = _current_batch->strategies.size();
    for (size_t i = 0; i < function_count; ++i) {
        DCHECK_EQ(results[i]->size(), block->rows());
        if (_current_batch->change_to_nullable_flags[i]) {
            block->insert({make_nullable(std::move(results[i])),
                           make_nullable(_current_batch->result_types[i]), ""});
        } else {
            block->insert({std::move(results[i]), _current_batch->result_types[i], ""});
        }
    }
}

Status AnalyticLocalState::_get_spill_block(RuntimeState* state, Block* block, bool* eos) {
    SCOPED_TIMER(_partition_replay_timer);
    while (true) {
        RETURN_IF_CANCELLED(state);
        if (!_current_batch) {
            std::shared_ptr<AnalyticSpillBatch> batch;
            {
                LockGuard lock(_shared_state->buffer_mutex);
                if (!_shared_state->spill_batches.empty()) {
                    batch = std::move(_shared_state->spill_batches.front());
                    _shared_state->spill_batches.pop();
                    if (_shared_state->spill_batches.empty()) {
                        _dependency->set_ready_to_write();
                    }
                } else {
                    LockGuard eos_lock(_shared_state->sink_eos_lock);
                    *eos = _shared_state->sink_eos;
                    if (!*eos) {
                        _dependency->block();
                        _dependency->set_ready_to_write();
                    }
                    return Status::OK();
                }
            }
            RETURN_IF_ERROR(_open_spill_batch(state, std::move(batch)));
        }

        bool batch_eos = false;
        RETURN_IF_ERROR(_next_replay_rows(state, block, &batch_eos));
        if (batch_eos) {
            DORIS_CHECK_EQ(_batch_output_position, _current_batch->rows);
            _finish_spill_batch();
            continue;
        }
        RETURN_IF_ERROR(_append_spill_results(state, block));
        *eos = false;
        return Status::OK();
    }
}

AnalyticSourceOperatorX::AnalyticSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode,
                                                 int operator_id, const DescriptorTbl& descs)
        : OperatorX<AnalyticLocalState>(pool, tnode, operator_id, descs) {}

Status AnalyticSourceOperatorX::get_block_impl(RuntimeState* state, Block* output_block,
                                               bool* eos) {
    RETURN_IF_CANCELLED(state);
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    SCOPED_TIMER(local_state._get_next_timer);
    local_state._estimate_memory_usage = 0;
    SCOPED_PEAK_MEM(&local_state._estimate_memory_usage);
    output_block->clear_column_data();
    if (local_state._shared_state->spill_enabled.load()) {
        RETURN_IF_ERROR(local_state._get_spill_block(state, output_block, eos));
        const size_t output_rows = output_block->rows();
        RETURN_IF_ERROR(local_state.filter_block(local_state._conjuncts, output_block));
        local_state.reached_limit(output_block, eos);
        if (output_rows > 0) {
            COUNTER_UPDATE(local_state._filtered_rows_counter, output_rows - output_block->rows());
        }
        return Status::OK();
    }
    size_t output_rows = 0;
    {
        LockGuard lock(local_state._shared_state->buffer_mutex);
        if (!local_state._shared_state->blocks_buffer.empty()) {
            local_state._shared_state->blocks_buffer.front().swap(*output_block);
            local_state._shared_state->blocks_buffer.pop();
            output_rows = output_block->rows();
            //if buffer have no data and sink not eos, block reading and wait for signal again
            RETURN_IF_ERROR(local_state.filter_block(local_state._conjuncts, output_block));
            if (local_state._shared_state->blocks_buffer.empty()) {
                // add this mutex to check, as in some case maybe is doing block(), and the sink is doing set eos.
                // so have to hold mutex to set block(), avoid to sink have set eos and set ready, but here set block() by mistake
                LockGuard lc(local_state._shared_state->sink_eos_lock);
                if (!local_state._shared_state->sink_eos) {
                    local_state._dependency->block();              // block self source
                    local_state._dependency->set_ready_to_write(); // ready for sink write
                }
            }
        } else {
            //iff buffer have no data and sink eos, set eos
            LockGuard lc(local_state._shared_state->sink_eos_lock);
            *eos = local_state._shared_state->sink_eos;
        }
    }
    local_state.reached_limit(output_block, eos);
    if (!output_block->empty()) {
        auto return_rows = output_block->rows();
        COUNTER_UPDATE(local_state._filtered_rows_counter, output_rows - return_rows);
    }
    return Status::OK();
}

size_t AnalyticSourceOperatorX::get_reserve_mem_size(RuntimeState* state) {
    auto& local_state = get_local_state(state);
    return OperatorX<AnalyticLocalState>::get_reserve_mem_size(state) +
           local_state._spill_replay_reserve_bytes(state);
}

} // namespace doris
