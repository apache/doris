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

#include <cstddef>
#include <cstdint>
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
    _finish_spill_partition();
    return Base::close(state);
}

void AnalyticLocalState::_finish_spill_partition() {
    if (_partition_reader) {
        (void)_partition_reader->close();
        _partition_reader.reset();
    }
    if (_peer_group_reader) {
        (void)_peer_group_reader->close();
        _peer_group_reader.reset();
    }
    _current_partition.reset();
    _peer_group_block.clear();
    _in_memory_block_index = 0;
    _partition_output_position = 0;
    _peer_group_start = 0;
    _peer_group_end = 0;
    _peer_group_block_position = 0;
    _in_memory_peer_group_index = 0;
    _peer_group_file_eos = false;
}

Status AnalyticLocalState::_open_spill_partition(
        RuntimeState* state, std::shared_ptr<AnalyticSpillPartition> partition) {
    DCHECK(partition != nullptr);
    DCHECK_GT(partition->rows, 0);
    DORIS_CHECK_EQ(partition->strategies.size(), partition->result_types.size());
    DORIS_CHECK_EQ(partition->strategies.size(), partition->peer_functions.size());
    DORIS_CHECK_EQ(partition->strategies.size(), partition->partition_results.size());
    DORIS_CHECK_EQ(partition->strategies.size(), partition->function_parameters.size());
    DORIS_CHECK_EQ(partition->strategies.size(), partition->change_to_nullable_flags.size());
    _current_partition = std::move(partition);
    _in_memory_block_index = 0;
    _partition_output_position = 0;
    _peer_group_start = 0;
    _peer_group_end = 0;
    _peer_group_block_position = 0;
    _in_memory_peer_group_index = 0;
    _peer_group_file_eos = false;

    if (_current_partition->data_file) {
        _partition_reader = _current_partition->data_file->create_reader(state, operator_profile());
        RETURN_IF_ERROR(_partition_reader->open());
    }
    if (_current_partition->peer_group_file) {
        _peer_group_reader =
                _current_partition->peer_group_file->create_reader(state, operator_profile());
        RETURN_IF_ERROR(_peer_group_reader->open());
    }

    bool has_peer_group_function = false;
    for (const auto strategy : _current_partition->strategies) {
        if (strategy == WindowSpillStrategy::PEER_GROUP) {
            has_peer_group_function = true;
            break;
        }
    }
    if (has_peer_group_function) {
        RETURN_IF_ERROR(_next_peer_group_end(state));
    }
    return Status::OK();
}

Status AnalyticLocalState::_read_partition_block(RuntimeState* state, Block* block,
                                                 bool* partition_eos) {
    RETURN_IF_CANCELLED(state);
    if (_partition_reader) {
        return _partition_reader->read(block, partition_eos);
    }
    if (_in_memory_block_index >= _current_partition->blocks.size()) {
        *partition_eos = true;
        block->clear();
        return Status::OK();
    }
    block->swap(std::move(_current_partition->blocks[_in_memory_block_index++]));
    *partition_eos = false;
    return Status::OK();
}

Status AnalyticLocalState::_next_peer_group_end(RuntimeState* state) {
    RETURN_IF_CANCELLED(state);
    _peer_group_start = _peer_group_end;
    if (!_peer_group_reader) {
        DORIS_CHECK_LT(_in_memory_peer_group_index, _current_partition->peer_group_ends.size());
        _peer_group_end = _current_partition->peer_group_ends[_in_memory_peer_group_index++];
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
    DORIS_CHECK_LE(_peer_group_end, _current_partition->rows);
    return Status::OK();
}

Status AnalyticLocalState::_append_spill_results(RuntimeState* state, Block* block) {
    const auto rows = block->rows();
    DCHECK_GT(rows, 0);
    DCHECK_LE(_partition_output_position + rows, _current_partition->rows);
    const auto function_count = _current_partition->strategies.size();
    std::vector<MutableColumnPtr> results(function_count);
    std::vector<IColumn*> computed_result_columns(function_count, nullptr);
    _create_spill_result_columns(rows, results, computed_result_columns);
    _append_non_peer_results(rows, computed_result_columns, results);
    RETURN_IF_ERROR(_append_peer_group_results(state, rows, computed_result_columns));
    _insert_spill_result_columns(block, results);
    _partition_output_position += rows;
    return Status::OK();
}

void AnalyticLocalState::_create_spill_result_columns(
        size_t rows, std::vector<MutableColumnPtr>& results,
        std::vector<IColumn*>& computed_result_columns) const {
    const auto function_count = _current_partition->strategies.size();
    for (size_t i = 0; i < function_count; ++i) {
        results[i] = _current_partition->result_types[i]->create_column();
        if (_current_partition->strategies[i] == WindowSpillStrategy::PARTITION_CARDINALITY ||
            _current_partition->strategies[i] == WindowSpillStrategy::PEER_GROUP) {
            if (_current_partition->result_types[i]->is_nullable()) {
                auto* nullable = assert_cast<ColumnNullable*>(results[i].get());
                nullable->get_null_map_data().resize_fill(rows, 0);
                computed_result_columns[i] = &nullable->get_nested_column();
            } else {
                computed_result_columns[i] = results[i].get();
            }
        }
    }
}

void AnalyticLocalState::_append_non_peer_results(
        size_t rows, const std::vector<IColumn*>& computed_result_columns,
        std::vector<MutableColumnPtr>& results) const {
    const auto function_count = _current_partition->strategies.size();
    for (size_t i = 0; i < function_count; ++i) {
        switch (_current_partition->strategies[i]) {
        case WindowSpillStrategy::PARTITION_REDUCE:
            DCHECK(_current_partition->partition_results[i]);
            results[i]->insert_many_from(*_current_partition->partition_results[i], 0, rows);
            break;
        case WindowSpillStrategy::PARTITION_CARDINALITY:
            _append_ntile_result(i, rows, computed_result_columns[i]);
            break;
        case WindowSpillStrategy::PEER_GROUP:
            break;
        case WindowSpillStrategy::UNSUPPORTED:
            DCHECK(false);
            break;
        }
    }
}

void AnalyticLocalState::_append_ntile_result(size_t function_index, size_t rows,
                                              IColumn* result_column) const {
    const int64_t bucket_num = _current_partition->function_parameters[function_index];
    DORIS_CHECK_GT(bucket_num, 0);
    const int64_t partition_size = _current_partition->rows;
    const int64_t small_bucket_size = partition_size / bucket_num;
    const int64_t big_bucket_num = partition_size % bucket_num;
    const int64_t first_small_bucket_row = big_bucket_num * (small_bucket_size + 1);
    auto& data = assert_cast<ColumnInt64&>(*result_column).get_data();
    data.reserve(rows);
    for (size_t row = 0; row < rows; ++row) {
        const int64_t row_index = _partition_output_position + row;
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
    if (_peer_group_end == 0) {
        return Status::OK();
    }
    const auto function_count = _current_partition->strategies.size();
    for (size_t row = 0; row < rows; ++row) {
        if ((row & 4095) == 0) {
            RETURN_IF_CANCELLED(state);
        }
        const int64_t partition_row = _partition_output_position + row;
        while (partition_row >= _peer_group_end) {
            RETURN_IF_ERROR(_next_peer_group_end(state));
        }
        for (size_t i = 0; i < function_count; ++i) {
            if (_current_partition->strategies[i] != WindowSpillStrategy::PEER_GROUP) {
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
    const auto peer_function = _current_partition->peer_functions[function_index];
    DCHECK(peer_function != WindowSpillPeerFunction::NONE);
    if (peer_function == WindowSpillPeerFunction::PERCENT_RANK) {
        return _current_partition->rows <= 1
                       ? 0.0
                       : static_cast<double>(_peer_group_start) /
                                 static_cast<double>(_current_partition->rows - 1);
    }
    DCHECK(peer_function == WindowSpillPeerFunction::CUME_DIST);
    return static_cast<double>(_peer_group_end) / static_cast<double>(_current_partition->rows);
}

void AnalyticLocalState::_insert_spill_result_columns(
        Block* block, std::vector<MutableColumnPtr>& results) const {
    const auto function_count = _current_partition->strategies.size();
    for (size_t i = 0; i < function_count; ++i) {
        DCHECK_EQ(results[i]->size(), block->rows());
        if (_current_partition->change_to_nullable_flags[i]) {
            block->insert({make_nullable(std::move(results[i])),
                           make_nullable(_current_partition->result_types[i]), ""});
        } else {
            block->insert({std::move(results[i]), _current_partition->result_types[i], ""});
        }
    }
}

Status AnalyticLocalState::_get_spill_block(RuntimeState* state, Block* block, bool* eos) {
    SCOPED_TIMER(_partition_replay_timer);
    while (true) {
        RETURN_IF_CANCELLED(state);
        if (!_current_partition) {
            std::shared_ptr<AnalyticSpillPartition> partition;
            {
                LockGuard lock(_shared_state->buffer_mutex);
                if (!_shared_state->spill_partitions.empty()) {
                    partition = std::move(_shared_state->spill_partitions.front());
                    _shared_state->spill_partitions.pop();
                    if (_shared_state->spill_partitions.empty()) {
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
            RETURN_IF_ERROR(_open_spill_partition(state, std::move(partition)));
        }

        bool partition_eos = false;
        RETURN_IF_ERROR(_read_partition_block(state, block, &partition_eos));
        if (partition_eos) {
            DORIS_CHECK_EQ(_partition_output_position, _current_partition->rows);
            _finish_spill_partition();
            continue;
        }
        if (block->empty()) {
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

Status AnalyticSourceOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorX<AnalyticLocalState>::prepare(state));
    DCHECK(_child->operator_row_desc_after_projection().is_prefix_of(_row_descriptor));
    return Status::OK();
}

} // namespace doris
