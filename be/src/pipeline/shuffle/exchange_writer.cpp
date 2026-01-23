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

#include "exchange_writer.h"

#include <glog/logging.h>

#include <algorithm>
#include <cstdint>
#include <vector>

#include "common/logging.h"
#include "common/status.h"
#include "pipeline/exec/exchange_sink_operator.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/sink/tablet_sink_hash_partitioner.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"

ExchangeWriterBase::ExchangeWriterBase(ExchangeSinkLocalState& local_state)
        : _local_state(local_state), _partitioner(local_state.partitioner()) {}

template <typename ChannelPtrType>
Status ExchangeWriterBase::_handle_eof_channel(RuntimeState* state, ChannelPtrType channel,
                                               Status st) const {
    channel->set_receiver_eof(st);
    // Channel will not send RPC to the downstream when eof, so close channel by OK status.
    return channel->close(state);
}

// NOLINTBEGIN(readability-function-cognitive-complexity)
Status ExchangeWriterBase::_add_rows_impl(
        RuntimeState* state, std::vector<std::shared_ptr<vectorized::Channel>>& channels,
        size_t channel_count, vectorized::Block* block, bool eos) {
    Status status = Status::OK();
    uint32_t offset = 0;
    for (size_t i = 0; i < channel_count; ++i) {
        uint32_t size = _channel_rows_histogram[i];
        if (!channels[i]->is_receiver_eof() && size > 0) {
            VLOG_DEBUG << fmt::format("partition {} of {}, block:\n{}, start: {}, size: {}", i,
                                      channel_count, block->dump_data(), offset, size);
            status = channels[i]->add_rows(block, _origin_row_idx.data(), offset, size, false);
            HANDLE_CHANNEL_STATUS(state, channels[i], status);
        }
        offset += size;
    }
    if (eos) {
        for (int i = 0; i < channel_count; ++i) {
            if (!channels[i]->is_receiver_eof()) {
                VLOG_DEBUG << fmt::format("EOS partition {} of {}, block:\n{}", i, channel_count,
                                          block->dump_data());
                status = channels[i]->add_rows(block, _origin_row_idx.data(), 0, 0, true);
                HANDLE_CHANNEL_STATUS(state, channels[i], status);
            }
        }
    }
    return Status::OK();
}
// NOLINTEND(readability-function-cognitive-complexity)

Status ExchangeOlapWriter::write(RuntimeState* state, vectorized::Block* block, bool eos) {
    vectorized::Block prior_block;
    auto* tablet_partitioner = assert_cast<vectorized::TabletSinkHashPartitioner*>(_partitioner);
    RETURN_IF_ERROR(tablet_partitioner->try_cut_in_line(prior_block));
    if (!prior_block.empty()) {
        // prior_block (batching rows) cuts in line, deal it first.
        RETURN_IF_ERROR(_write_impl(state, &prior_block));
        tablet_partitioner->finish_cut_in_line();
    }

    RETURN_IF_ERROR(_write_impl(state, block));

    // all data wrote. consider batched rows before eos.
    if (eos) {
        // get all batched rows
        tablet_partitioner->mark_last_block();
        vectorized::Block final_batching_block;
        RETURN_IF_ERROR(tablet_partitioner->try_cut_in_line(final_batching_block));
        if (!final_batching_block.empty()) {
            RETURN_IF_ERROR(_write_impl(state, &final_batching_block, true));
        } else {
            // No batched rows, send empty block with eos signal.
            vectorized::Block empty_block = block->clone_empty();
            RETURN_IF_ERROR(_write_impl(state, &empty_block, true));
        }
    }
    return Status::OK();
}

Status ExchangeOlapWriter::_write_impl(RuntimeState* state, vectorized::Block* block, bool eos) {
    auto rows = block->rows();
    auto* tablet_partitioner = assert_cast<vectorized::TabletSinkHashPartitioner*>(_partitioner);
    {
        SCOPED_TIMER(_local_state.split_block_hash_compute_timer());
        RETURN_IF_ERROR(tablet_partitioner->do_partitioning(state, block));
    }
    {
        SCOPED_TIMER(_local_state.distribute_rows_into_channels_timer());
        const auto& channel_ids = tablet_partitioner->get_channel_ids();
        const auto invalid_val = tablet_partitioner->invalid_sentinel();
        DCHECK_EQ(channel_ids.size(), rows);

        // decrease not sinked rows this time
        COUNTER_UPDATE(_local_state.rows_input_counter(),
                       -1LL * std::ranges::count(channel_ids, invalid_val));

        RETURN_IF_ERROR(_channel_add_rows(state, _local_state.channels,
                                          _local_state.channels.size(), channel_ids, rows, block,
                                          eos, invalid_val));
    }
    return Status::OK();
}

Status ExchangeTrivialWriter::write(RuntimeState* state, vectorized::Block* block, bool eos) {
    auto rows = block->rows();
    {
        SCOPED_TIMER(_local_state.split_block_hash_compute_timer());
        RETURN_IF_ERROR(_partitioner->do_partitioning(state, block));
    }
    {
        SCOPED_TIMER(_local_state.distribute_rows_into_channels_timer());
        const auto& channel_ids = _partitioner->get_channel_ids();

        RETURN_IF_ERROR(_channel_add_rows(state, _local_state.channels,
                                          _local_state.channels.size(), channel_ids, rows, block,
                                          eos));
    }

    return Status::OK();
}

Status ExchangeOlapWriter::_channel_add_rows(
        RuntimeState* state, std::vector<std::shared_ptr<vectorized::Channel>>& channels,
        size_t channel_count, const std::vector<HashValType>& channel_ids, size_t rows,
        vectorized::Block* block, bool eos, HashValType invalid_val) {
    size_t effective_rows = 0;
    effective_rows =
            std::ranges::count_if(channel_ids, [=](int64_t cid) { return cid != invalid_val; });

    // row index will skip all skipped rows.
    _origin_row_idx.resize(effective_rows);
    _channel_rows_histogram.assign(channel_count, 0U);
    _channel_pos_offsets.resize(channel_count);
    for (size_t i = 0; i < rows; ++i) {
        if (channel_ids[i] == invalid_val) {
            continue;
        }
        auto cid = channel_ids[i];
        _channel_rows_histogram[cid]++;
    }
    _channel_pos_offsets[0] = 0;
    for (size_t i = 1; i < channel_count; ++i) {
        _channel_pos_offsets[i] = _channel_pos_offsets[i - 1] + _channel_rows_histogram[i - 1];
    }
    for (uint32_t i = 0; i < rows; ++i) {
        if (channel_ids[i] == invalid_val) {
            continue;
        }
        auto cid = channel_ids[i];
        auto pos = _channel_pos_offsets[cid]++;
        _origin_row_idx[pos] = i;
    }

    return _add_rows_impl(state, channels, channel_count, block, eos);
}

Status ExchangeTrivialWriter::_channel_add_rows(
        RuntimeState* state, std::vector<std::shared_ptr<vectorized::Channel>>& channels,
        size_t channel_count, const std::vector<HashValType>& channel_ids, size_t rows,
        vectorized::Block* block, bool eos) {
    _origin_row_idx.resize(rows);
    _channel_rows_histogram.assign(channel_count, 0U);
    _channel_pos_offsets.resize(channel_count);
    for (size_t i = 0; i < rows; ++i) {
        _channel_rows_histogram[channel_ids[i]]++;
    }
    _channel_pos_offsets[0] = 0;
    for (size_t i = 1; i < channel_count; ++i) {
        _channel_pos_offsets[i] = _channel_pos_offsets[i - 1] + _channel_rows_histogram[i - 1];
    }
    for (uint32_t i = 0; i < rows; i++) {
        auto cid = channel_ids[i];
        auto pos = _channel_pos_offsets[cid]++;
        _origin_row_idx[pos] = i;
    }

    return _add_rows_impl(state, channels, channel_count, block, eos);
}

} // namespace doris::pipeline
