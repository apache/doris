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

#include "exec/spill/spill_repartitioner.h"

#include <glog/logging.h>

#include <limits>
#include <memory>
#include <vector>

#include "core/block/block.h"
#include "core/column/column.h"
#include "exec/partitioner/partitioner.h"
#include "exec/spill/spill_file.h"
#include "exec/spill/spill_file_manager.h"
#include "exec/spill/spill_file_reader.h"
#include "exec/spill/spill_file_writer.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "util/uid_util.h"

namespace doris {

void SpillRepartitioner::init(std::unique_ptr<PartitionerBase> partitioner, RuntimeProfile* profile,
                              int fanout, int repartition_level) {
    _partitioner = std::move(partitioner);
    _use_column_index_mode = false;
    _fanout = fanout;
    _repartition_level = repartition_level;
    _operator_profile = profile;
    _repartition_timer = ADD_TIMER_WITH_LEVEL(profile, "SpillRepartitionTime", 1);
    _repartition_rows = ADD_COUNTER_WITH_LEVEL(profile, "SpillRepartitionRows", TUnit::UNIT, 1);
}

void SpillRepartitioner::init_with_key_columns(std::vector<size_t> key_column_indices,
                                               std::vector<DataTypePtr> key_data_types,
                                               RuntimeProfile* profile, int fanout,
                                               int repartition_level) {
    _key_column_indices = std::move(key_column_indices);
    _key_data_types = std::move(key_data_types);
    _use_column_index_mode = true;
    _partitioner.reset();
    _fanout = fanout;
    _repartition_level = repartition_level;
    _operator_profile = profile;
    _repartition_timer = ADD_TIMER_WITH_LEVEL(profile, "SpillRepartitionTime", 1);
    _repartition_rows = ADD_COUNTER_WITH_LEVEL(profile, "SpillRepartitionRows", TUnit::UNIT, 1);
}

Status SpillRepartitioner::setup_output(RuntimeState* state,
                                        std::vector<SpillFileSPtr>& output_spill_files) {
    DCHECK_EQ(output_spill_files.size(), _fanout);
    _output_spill_files = &output_spill_files;
    _output_writers.resize(_fanout);
    for (int i = 0; i < _fanout; ++i) {
        RETURN_IF_ERROR(
                output_spill_files[i]->create_writer(state, _operator_profile, _output_writers[i]));
    }
    // Reset reader state from any previous repartition session
    _input_reader.reset();
    _current_input_file.reset();
    return Status::OK();
}

Status SpillRepartitioner::repartition(RuntimeState* state, SpillFileSPtr& input_spill_file,
                                       bool* done) {
    DCHECK(_output_spill_files != nullptr) << "setup_output() must be called first";
    SCOPED_TIMER(_repartition_timer);

    *done = false;
    size_t accumulated_bytes = 0;

    // Create or reuse input reader. If the input file changed, create a new reader.
    if (_current_input_file != input_spill_file) {
        _current_input_file = input_spill_file;
        _input_reader = input_spill_file->create_reader(state, _operator_profile);
        RETURN_IF_ERROR(_input_reader->open());
    }

    // Per-partition write buffers to batch small writes
    std::vector<std::unique_ptr<MutableBlock>> output_buffers(_fanout);

    bool eos = false;
    while (!eos && !state->is_cancelled()) {
        Block block;
        RETURN_IF_ERROR(_input_reader->read(&block, &eos));

        if (block.empty()) {
            continue;
        }

        accumulated_bytes += block.allocated_bytes();
        COUNTER_UPDATE(_repartition_rows, block.rows());

        if (_use_column_index_mode) {
            RETURN_IF_ERROR(_route_block_by_columns(state, block, output_buffers));
        } else {
            RETURN_IF_ERROR(_route_block(state, block, output_buffers));
        }

        // Yield after processing MAX_BATCH_BYTES to let pipeline scheduler re-schedule
        if (accumulated_bytes >= MAX_BATCH_BYTES && !eos) {
            break;
        }
    }

    // Flush all remaining buffers
    RETURN_IF_ERROR(_flush_all_buffers(state, output_buffers, /*force=*/true));

    if (eos) {
        *done = true;
        // Reset reader for this input file
        _input_reader.reset();
        _current_input_file.reset();
    }

    return Status::OK();
}

Status SpillRepartitioner::repartition(RuntimeState* state, SpillFileReaderSPtr& reader,
                                       bool* done) {
    DCHECK(_output_spill_files != nullptr) << "setup_output() must be called first";
    DCHECK(reader != nullptr) << "reader must not be null";
    SCOPED_TIMER(_repartition_timer);

    *done = false;
    size_t accumulated_bytes = 0;

    // Per-partition write buffers to batch small writes
    std::vector<std::unique_ptr<MutableBlock>> output_buffers(_fanout);

    bool eos = false;
    while (!eos && !state->is_cancelled()) {
        Block block;
        RETURN_IF_ERROR(reader->read(&block, &eos));

        if (block.empty()) {
            continue;
        }

        accumulated_bytes += block.allocated_bytes();
        COUNTER_UPDATE(_repartition_rows, block.rows());

        if (_use_column_index_mode) {
            RETURN_IF_ERROR(_route_block_by_columns(state, block, output_buffers));
        } else {
            RETURN_IF_ERROR(_route_block(state, block, output_buffers));
        }

        // Yield after processing MAX_BATCH_BYTES to let pipeline scheduler re-schedule
        if (accumulated_bytes >= MAX_BATCH_BYTES && !eos) {
            break;
        }
    }

    // Flush all remaining buffers
    RETURN_IF_ERROR(_flush_all_buffers(state, output_buffers, /*force=*/true));

    if (eos) {
        *done = true;
        reader.reset();
    }

    return Status::OK();
}

Status SpillRepartitioner::route_block(RuntimeState* state, Block& block) {
    DCHECK(_output_spill_files != nullptr) << "setup_output() must be called first";
    if (UNLIKELY(_output_spill_files == nullptr)) {
        return Status::InternalError("SpillRepartitioner::setup_output() must be called first");
    }
    SCOPED_TIMER(_repartition_timer);

    if (block.empty()) {
        return Status::OK();
    }

    COUNTER_UPDATE(_repartition_rows, block.rows());

    std::vector<std::unique_ptr<MutableBlock>> output_buffers(_fanout);
    if (_use_column_index_mode) {
        RETURN_IF_ERROR(_route_block_by_columns(state, block, output_buffers));
    } else {
        RETURN_IF_ERROR(_route_block(state, block, output_buffers));
    }
    RETURN_IF_ERROR(_flush_all_buffers(state, output_buffers, /*force=*/true));
    return Status::OK();
}

Status SpillRepartitioner::finalize() {
    DCHECK(_output_spill_files != nullptr) << "setup_output() must be called first";
    if (UNLIKELY(_output_spill_files == nullptr)) {
        return Status::InternalError("SpillRepartitioner::setup_output() must be called first");
    }
    // Close all writers (Writer::close() automatically updates SpillFile stats)
    for (int i = 0; i < _fanout; ++i) {
        if (_output_writers[i]) {
            RETURN_IF_ERROR(_output_writers[i]->close());
        }
    }
    _output_writers.clear();
    _output_spill_files = nullptr;
    _input_reader.reset();
    _current_input_file.reset();
    return Status::OK();
}

Status SpillRepartitioner::create_output_spill_files(
        RuntimeState* state, int node_id, const std::string& label_prefix, int fanout,
        std::vector<SpillFileSPtr>& output_spill_files) {
    output_spill_files.resize(fanout);
    for (int i = 0; i < fanout; ++i) {
        auto relative_path = fmt::format("{}/{}_sub{}-{}-{}-{}", print_id(state->query_id()),
                                         label_prefix, i, node_id, state->task_id(),
                                         ExecEnv::GetInstance()->spill_file_mgr()->next_id());
        RETURN_IF_ERROR(ExecEnv::GetInstance()->spill_file_mgr()->create_spill_file(
                relative_path, output_spill_files[i]));
    }
    return Status::OK();
}

Status SpillRepartitioner::_route_block(
        RuntimeState* state, Block& block,
        std::vector<std::unique_ptr<MutableBlock>>& output_buffers) {
    // Compute raw hash values for every row in the block.
    RETURN_IF_ERROR(_partitioner->do_partitioning(state, &block));
    const auto& hash_vals = _partitioner->get_channel_ids();
    const auto rows = block.rows();

    // Build per-partition row index lists
    std::vector<std::vector<uint32_t>> partition_row_indexes(_fanout);
    for (uint32_t i = 0; i < rows; ++i) {
        auto partition_idx = _map_hash_to_partition(hash_vals[i]);
        partition_row_indexes[partition_idx].emplace_back(i);
    }

    // Scatter rows into per-partition buffers
    for (int p = 0; p < _fanout; ++p) {
        if (partition_row_indexes[p].empty()) {
            continue;
        }

        // Lazily initialize the buffer
        if (!output_buffers[p]) {
            output_buffers[p] = MutableBlock::create_unique(block.clone_empty());
        }

        RETURN_IF_ERROR(output_buffers[p]->add_rows(
                &block, partition_row_indexes[p].data(),
                partition_row_indexes[p].data() + partition_row_indexes[p].size()));

        // Flush large buffers immediately to keep memory bounded
        if (output_buffers[p]->allocated_bytes() >= MAX_BATCH_BYTES) {
            RETURN_IF_ERROR(_flush_buffer(state, p, output_buffers[p]));
        }
    }

    return Status::OK();
}

Status SpillRepartitioner::_route_block_by_columns(
        RuntimeState* state, Block& block,
        std::vector<std::unique_ptr<MutableBlock>>& output_buffers) {
    const auto rows = block.rows();
    if (rows == 0) {
        return Status::OK();
    }

    // Compute CRC32 hash on key columns
    std::vector<uint32_t> hash_vals(rows, 0);
    auto* __restrict hashes = hash_vals.data();
    for (size_t j = 0; j < _key_column_indices.size(); ++j) {
        auto col_idx = _key_column_indices[j];
        DCHECK_LT(col_idx, block.columns());
        const auto& column = block.get_by_position(col_idx).column;
        column->update_crcs_with_value(hashes, _key_data_types[j]->get_primitive_type(),
                                       static_cast<uint32_t>(rows));
    }

    // Map hash values to output channels with level-aware mixing.
    for (size_t i = 0; i < rows; ++i) {
        hashes[i] = _map_hash_to_partition(hashes[i]);
    }

    // Build per-partition row index lists
    std::vector<std::vector<uint32_t>> partition_row_indexes(_fanout);
    for (uint32_t i = 0; i < rows; ++i) {
        partition_row_indexes[hashes[i]].emplace_back(i);
    }

    // Scatter rows into per-partition buffers
    for (int p = 0; p < _fanout; ++p) {
        if (partition_row_indexes[p].empty()) {
            continue;
        }

        if (!output_buffers[p]) {
            output_buffers[p] = MutableBlock::create_unique(block.clone_empty());
        }

        RETURN_IF_ERROR(output_buffers[p]->add_rows(
                &block, partition_row_indexes[p].data(),
                partition_row_indexes[p].data() + partition_row_indexes[p].size()));

        if (output_buffers[p]->allocated_bytes() >= MAX_BATCH_BYTES) {
            RETURN_IF_ERROR(_flush_buffer(state, p, output_buffers[p]));
        }
    }

    return Status::OK();
}

Status SpillRepartitioner::_flush_buffer(RuntimeState* state, int partition_idx,
                                         std::unique_ptr<MutableBlock>& buffer) {
    if (!buffer || buffer->rows() == 0) {
        return Status::OK();
    }
    DCHECK(partition_idx < _fanout && _output_writers[partition_idx]);
    if (UNLIKELY(partition_idx >= _fanout || !_output_writers[partition_idx])) {
        return Status::InternalError(
                "SpillRepartitioner output writer is not initialized for partition {}",
                partition_idx);
    }
    auto out_block = buffer->to_block();
    buffer.reset();
    return _output_writers[partition_idx]->write_block(state, out_block);
}

Status SpillRepartitioner::_flush_all_buffers(
        RuntimeState* state, std::vector<std::unique_ptr<MutableBlock>>& output_buffers,
        bool force) {
    for (int i = 0; i < _fanout; ++i) {
        if (!output_buffers[i] || output_buffers[i]->rows() == 0) {
            continue;
        }
        if (force || output_buffers[i]->allocated_bytes() >= MAX_BATCH_BYTES) {
            RETURN_IF_ERROR(_flush_buffer(state, i, output_buffers[i]));
        }
    }
    return Status::OK();
}

uint32_t SpillRepartitioner::_map_hash_to_partition(uint32_t hash) const {
    DCHECK_GT(_fanout, 0);
    // Use a level-dependent salt so each repartition level has a different
    // projection from hash-space to partition-space.
    constexpr uint32_t LEVEL_SALT_BASE = 0x9E3779B9U;
    auto salt = static_cast<uint32_t>(_repartition_level + 1) * LEVEL_SALT_BASE;
    auto mixed = crc32c_shuffle_mix(hash ^ salt);
    return ((mixed >> 16) | (mixed << 16)) % static_cast<uint32_t>(_fanout);
}

} // namespace doris
