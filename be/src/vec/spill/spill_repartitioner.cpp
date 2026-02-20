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

#include "vec/spill/spill_repartitioner.h"

#include <glog/logging.h>

#include <algorithm>
#include <limits>
#include <memory>
#include <vector>

#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"
#include "util/uid_util.h"
#include "vec/columns/column.h"
#include "vec/core/block.h"
#include "vec/runtime/partitioner.h"
#include "vec/spill/spill_stream.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"

void SpillRepartitioner::init(std::unique_ptr<vectorized::PartitionerBase> partitioner,
                              RuntimeProfile* profile, int fanout) {
    _partitioner = std::move(partitioner);
    _use_column_index_mode = false;
    _fanout = fanout;
    _repartition_timer = ADD_TIMER_WITH_LEVEL(profile, "SpillRepartitionTime", 1);
    _repartition_rows = ADD_COUNTER_WITH_LEVEL(profile, "SpillRepartitionRows", TUnit::UNIT, 1);
}

void SpillRepartitioner::init_with_key_columns(std::vector<size_t> key_column_indices,
                                               std::vector<vectorized::DataTypePtr> key_data_types,
                                               RuntimeProfile* profile, int fanout) {
    _key_column_indices = std::move(key_column_indices);
    _key_data_types = std::move(key_data_types);
    _use_column_index_mode = true;
    _partitioner.reset();
    _fanout = fanout;
    _repartition_timer = ADD_TIMER_WITH_LEVEL(profile, "SpillRepartitionTime", 1);
    _repartition_rows = ADD_COUNTER_WITH_LEVEL(profile, "SpillRepartitionRows", TUnit::UNIT, 1);
}

Status SpillRepartitioner::repartition(RuntimeState* state,
                                       vectorized::SpillStreamSPtr& input_stream,
                                       std::vector<vectorized::SpillStreamSPtr>& output_streams,
                                       bool* done) {
    DCHECK_EQ(output_streams.size(), _fanout);
    SCOPED_TIMER(_repartition_timer);

    *done = false;
    size_t accumulated_bytes = 0;

    // Per-partition write buffers to batch small writes
    std::vector<std::unique_ptr<vectorized::MutableBlock>> output_buffers(_fanout);

    bool eos = false;
    while (!eos && !state->is_cancelled()) {
        vectorized::Block block;
        RETURN_IF_ERROR(input_stream->read_next_block_sync(&block, &eos));

        if (block.empty()) {
            continue;
        }

        accumulated_bytes += block.allocated_bytes();
        COUNTER_UPDATE(_repartition_rows, block.rows());

        if (_use_column_index_mode) {
            RETURN_IF_ERROR(_route_block_by_columns(state, block, output_streams, output_buffers));
        } else {
            RETURN_IF_ERROR(_route_block(state, block, output_streams, output_buffers));
        }

        // Yield after processing MAX_BATCH_BYTES to let pipeline scheduler re-schedule
        if (accumulated_bytes >= MAX_BATCH_BYTES && !eos) {
            break;
        }
    }

    // Flush all remaining buffers. When yielding (not eos), we must still flush
    // because output_buffers is local and would be lost on return.
    RETURN_IF_ERROR(_flush_all_buffers(state, output_streams, output_buffers, /*force=*/true));

    if (eos) {
        *done = true;
    }

    return Status::OK();
}

Status SpillRepartitioner::route_block(RuntimeState* state, vectorized::Block& block,
                                       std::vector<vectorized::SpillStreamSPtr>& output_streams) {
    DCHECK_EQ(output_streams.size(), _fanout);
    SCOPED_TIMER(_repartition_timer);

    if (block.empty()) {
        return Status::OK();
    }

    COUNTER_UPDATE(_repartition_rows, block.rows());

    std::vector<std::unique_ptr<vectorized::MutableBlock>> output_buffers(_fanout);
    if (_use_column_index_mode) {
        RETURN_IF_ERROR(_route_block_by_columns(state, block, output_streams, output_buffers));
    } else {
        RETURN_IF_ERROR(_route_block(state, block, output_streams, output_buffers));
    }
    RETURN_IF_ERROR(_flush_all_buffers(state, output_streams, output_buffers, /*force=*/true));
    return Status::OK();
}

Status SpillRepartitioner::finalize(std::vector<vectorized::SpillStreamSPtr>& output_streams) {
    for (auto& stream : output_streams) {
        if (stream && stream->get_written_bytes() > 0) {
            RETURN_IF_ERROR(stream->close());
        }
    }
    return Status::OK();
}

Status SpillRepartitioner::create_output_streams(
        RuntimeState* state, int node_id, const std::string& label_prefix,
        RuntimeProfile* operator_profile, std::vector<vectorized::SpillStreamSPtr>& output_streams,
        int fanout) {
    output_streams.resize(fanout);
    auto query_id_str = print_id(state->query_id());
    for (int i = 0; i < fanout; ++i) {
        auto label = fmt::format("{}_sub{}", label_prefix, i);
        RETURN_IF_ERROR(ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
                state, output_streams[i], query_id_str, label, node_id,
                std::numeric_limits<size_t>::max(), operator_profile));
    }
    return Status::OK();
}

Status SpillRepartitioner::_route_block(
        RuntimeState* state, vectorized::Block& block,
        std::vector<vectorized::SpillStreamSPtr>& output_streams,
        std::vector<std::unique_ptr<vectorized::MutableBlock>>& output_buffers) {
    // Compute partition assignment for every row in the block
    RETURN_IF_ERROR(_partitioner->do_partitioning(state, &block));
    const auto& channel_ids = _partitioner->get_channel_ids();
    const auto rows = block.rows();

    // Build per-partition row index lists
    std::vector<std::vector<uint32_t>> partition_row_indexes(_fanout);
    for (uint32_t i = 0; i < rows; ++i) {
        partition_row_indexes[channel_ids[i]].emplace_back(i);
    }

    // Scatter rows into per-partition buffers
    for (int p = 0; p < _fanout; ++p) {
        if (partition_row_indexes[p].empty()) {
            continue;
        }

        // Lazily initialize the buffer
        if (!output_buffers[p]) {
            output_buffers[p] = vectorized::MutableBlock::create_unique(block.clone_empty());
        }

        RETURN_IF_ERROR(output_buffers[p]->add_rows(
                &block, partition_row_indexes[p].data(),
                partition_row_indexes[p].data() + partition_row_indexes[p].size()));

        // Flush large buffers immediately to keep memory bounded
        if (output_buffers[p]->allocated_bytes() >= MAX_BATCH_BYTES) {
            RETURN_IF_ERROR(_flush_buffer(state, output_streams[p], output_buffers[p]));
        }
    }

    return Status::OK();
}

Status SpillRepartitioner::_route_block_by_columns(
        RuntimeState* state, vectorized::Block& block,
        std::vector<vectorized::SpillStreamSPtr>& output_streams,
        std::vector<std::unique_ptr<vectorized::MutableBlock>>& output_buffers) {
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

    // Apply SpillPartitionChannelIds: ((hash >> 16) | (hash << 16)) % _fanout
    for (size_t i = 0; i < rows; ++i) {
        hashes[i] = ((hashes[i] >> 16) | (hashes[i] << 16)) % _fanout;
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
            output_buffers[p] = vectorized::MutableBlock::create_unique(block.clone_empty());
        }

        RETURN_IF_ERROR(output_buffers[p]->add_rows(
                &block, partition_row_indexes[p].data(),
                partition_row_indexes[p].data() + partition_row_indexes[p].size()));

        if (output_buffers[p]->allocated_bytes() >= MAX_BATCH_BYTES) {
            RETURN_IF_ERROR(_flush_buffer(state, output_streams[p], output_buffers[p]));
        }
    }

    return Status::OK();
}

Status SpillRepartitioner::_flush_buffer(RuntimeState* state, vectorized::SpillStreamSPtr& stream,
                                         std::unique_ptr<vectorized::MutableBlock>& buffer) {
    if (!buffer || buffer->rows() == 0) {
        return Status::OK();
    }
    auto out_block = buffer->to_block();
    buffer.reset();
    return stream->spill_block(state, out_block, false);
}

Status SpillRepartitioner::_flush_all_buffers(
        RuntimeState* state, std::vector<vectorized::SpillStreamSPtr>& output_streams,
        std::vector<std::unique_ptr<vectorized::MutableBlock>>& output_buffers, bool force) {
    for (int i = 0; i < _fanout; ++i) {
        if (!output_buffers[i] || output_buffers[i]->rows() == 0) {
            continue;
        }
        if (force || output_buffers[i]->allocated_bytes() >= MAX_BATCH_BYTES) {
            RETURN_IF_ERROR(_flush_buffer(state, output_streams[i], output_buffers[i]));
        }
    }
    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris::pipeline
