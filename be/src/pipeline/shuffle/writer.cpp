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

#include "writer.h"

#include <glog/logging.h>

#include <algorithm>
#include <cstdint>

#include "common/logging.h"
#include "common/status.h"
#include "pipeline/exec/exchange_sink_operator.h"
#include "vec/core/block.h"
#include "vec/sink/tablet_sink_hash_partitioner.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"

template <typename ChannelPtrType>
Status WriterBase::_handle_eof_channel(RuntimeState* state, ChannelPtrType channel,
                                       Status st) const {
    channel->set_receiver_eof(st);
    // Chanel will not send RPC to the downstream when eof, so close channel by OK status.
    return channel->close(state);
}

// NOLINTBEGIN(readability-function-cognitive-complexity)
Status WriterBase::_add_rows_impl(RuntimeState* state,
                                  std::vector<std::shared_ptr<vectorized::Channel>>& channels,
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

Status OlapWriter::write(ExchangeSinkLocalState* local_state, RuntimeState* state,
                         vectorized::Block* block, bool eos) {
    Status st = _write_normal(local_state, state, block);
    // auto partition's batched block cut in line. send this unprocessed block again.
    if (st.is<ErrorCode::NEED_SEND_AGAIN>()) {
        RETURN_IF_ERROR(_write_normal(local_state, state, block));
    } else if (!st.ok()) {
        return st;
    }
    // the block is already processed normally. in `_write_last` we only need to consider batched rows.
    if (eos) {
        vectorized::Block empty_block = block->clone_empty();
        RETURN_IF_ERROR(_write_last(local_state, state, &empty_block));
    }
    return Status::OK();
}

Status OlapWriter::_write_normal(ExchangeSinkLocalState* local_state, RuntimeState* state,
                                 vectorized::Block* block) {
    auto* partitioner =
            static_cast<vectorized::TabletSinkHashPartitioner*>(local_state->partitioner());
    vectorized::Block* store_block = block;
    vectorized::Block prior_block;
    RETURN_IF_ERROR(partitioner->try_cut_in_line(prior_block));
    if (!prior_block.empty()) {
        // prior_block cuts in line. deal it first.
        block = &prior_block;
    }

    auto rows = block->rows();
    {
        SCOPED_TIMER(local_state->split_block_hash_compute_timer());
        RETURN_IF_ERROR(partitioner->do_partitioning(state, block));
    }
    {
        SCOPED_TIMER(local_state->distribute_rows_into_channels_timer());
        const auto* channel_ids = partitioner->get_channel_ids().get<int64_t>();
        DCHECK_EQ(partitioner->get_channel_ids().len, sizeof(int64_t));

        // decrease not sinked rows this time
        COUNTER_UPDATE(local_state->rows_input_counter(),
                       -1LL * std::ranges::count(channel_ids, channel_ids + rows, -1));

        RETURN_IF_ERROR(_channel_add_rows<true>(state, local_state->channels,
                                                local_state->channels.size(), channel_ids, rows,
                                                block, false));
    }

    if (!prior_block.empty()) {
        // swap back the input data and caller will call with it again.
        block = store_block;
        partitioner->finish_cut_in_line();
        return Status::NeedSendAgain("");
    }
    return Status::OK();
}

Status OlapWriter::_write_last(ExchangeSinkLocalState* local_state, RuntimeState* state,
                               vectorized::Block* block) {
    auto* partitioner =
            static_cast<vectorized::TabletSinkHashPartitioner*>(local_state->partitioner());
    // get all batched rows
    partitioner->mark_last_block();
    RETURN_IF_ERROR(partitioner->try_cut_in_line(*block));
    // if no batched rows, block is empty but has legal structure.

    auto rows = block->rows();
    {
        SCOPED_TIMER(local_state->split_block_hash_compute_timer());
        RETURN_IF_ERROR(partitioner->do_partitioning(state, block));
    }
    {
        SCOPED_TIMER(local_state->distribute_rows_into_channels_timer());
        const auto channel_field = partitioner->get_channel_ids();
        DCHECK_EQ(channel_field.len, sizeof(int64_t));

        RETURN_IF_ERROR(_channel_add_rows<false>(state, local_state->channels,
                                                 local_state->channels.size(),
                                                 channel_field.get<int64_t>(), rows, block, true));
    }

    return Status::OK();
}

Status TrivialWriter::write(ExchangeSinkLocalState* local_state, RuntimeState* state,
                            vectorized::Block* block, bool eos) {
    auto rows = block->rows();
    {
        SCOPED_TIMER(local_state->split_block_hash_compute_timer());
        RETURN_IF_ERROR(local_state->partitioner()->do_partitioning(state, block));
    }
    {
        SCOPED_TIMER(local_state->distribute_rows_into_channels_timer());
        const auto channel_field = local_state->partitioner()->get_channel_ids();

        // now for crc32 and scale writer, channel id is uint32_t.
        DCHECK_EQ(channel_field.len, sizeof(uint32_t));
        RETURN_IF_ERROR(_channel_add_rows(state, local_state->channels,
                                          local_state->channels.size(),
                                          channel_field.get<uint32_t>(), rows, block, eos));
    }

    return Status::OK();
}

template <bool NeedCheck>
Status OlapWriter::_channel_add_rows(RuntimeState* state,
                                     std::vector<std::shared_ptr<vectorized::Channel>>& channels,
                                     size_t channel_count, const int64_t* __restrict channel_ids,
                                     size_t rows, vectorized::Block* block, bool eos) {
    size_t effective_rows = 0;
    if constexpr (NeedCheck) {
        effective_rows = std::ranges::count_if(channel_ids, channel_ids + rows,
                                               [](int64_t cid) { return cid >= 0; });
    } else {
        effective_rows = rows;
    }

    // row index will skip all skipped rows.
    _origin_row_idx.resize(effective_rows);
    _channel_rows_histogram.resize(channel_count);
    _channel_pos_offsets.resize(channel_count);
    for (size_t i = 0; i < channel_count; ++i) {
        _channel_rows_histogram[i] = 0;
    }
    for (size_t i = 0; i < rows; ++i) {
        if constexpr (NeedCheck) {
            if (channel_ids[i] < 0) {
                continue;
            }
        }
        auto cid = static_cast<uint32_t>(channel_ids[i]);
        _channel_rows_histogram[cid]++;
    }
    _channel_pos_offsets[0] = 0;
    for (size_t i = 1; i < channel_count; ++i) {
        _channel_pos_offsets[i] = _channel_pos_offsets[i - 1] + _channel_rows_histogram[i - 1];
    }
    for (uint32_t i = 0; i < rows; ++i) {
        if constexpr (NeedCheck) {
            if (channel_ids[i] < 0) {
                continue;
            }
        }
        auto cid = static_cast<uint32_t>(channel_ids[i]);
        auto pos = _channel_pos_offsets[cid]++;
        _origin_row_idx[pos] = i;
    }

    return _add_rows_impl(state, channels, channel_count, block, eos);
}

Status TrivialWriter::_channel_add_rows(RuntimeState* state,
                                        std::vector<std::shared_ptr<vectorized::Channel>>& channels,
                                        size_t channel_count,
                                        const uint32_t* __restrict channel_ids, size_t rows,
                                        vectorized::Block* block, bool eos) {
    _origin_row_idx.resize(rows);
    _channel_rows_histogram.resize(channel_count);
    _channel_pos_offsets.resize(channel_count);
    for (size_t i = 0; i < channel_count; ++i) {
        _channel_rows_histogram[i] = 0;
    }
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
