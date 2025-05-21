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

#include <algorithm>

#include "common/logging.h"
#include "common/status.h"
#include "pipeline/exec/exchange_sink_operator.h"
#include "vec/core/block.h"
#include "vec/sink/tablet_sink_hash_partitioner.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"

template <typename ChannelPtrType>
void WriterBase::_handle_eof_channel(RuntimeState* state, ChannelPtrType channel, Status st) const {
    channel->set_receiver_eof(st);
    // Chanel will not send RPC to the downstream when eof, so close chanel by OK status.
    static_cast<void>(channel->close(state));
}

Status OlapWriter::write(ExchangeSinkLocalState* local_state, RuntimeState* state,
                         vectorized::Block* block, bool eos) const {
    Status st = _write_normal(local_state, state, block);
    // auto partition's batched block cut in line. send this unprocessed block again.
    if (st.is<ErrorCode::NEED_SEND_AGAIN>()) {
        RETURN_IF_ERROR(_write_normal(local_state, state, block));
    } else if (!st.ok()) {
        return st;
    }
    if (eos) {
        vectorized::Block empty_block = block->clone_empty();
        RETURN_IF_ERROR(_write_last(local_state, state, &empty_block));
    }
    return Status::OK();
}

Status OlapWriter::_write_normal(ExchangeSinkLocalState* local_state, RuntimeState* state,
                                 vectorized::Block* block) const {
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
        const auto channel_field = partitioner->get_channel_ids();
        const std::vector<bool>& skipped = partitioner->get_skipped(cast_set<int>(rows));
        // decrease not sinked rows this time
        COUNTER_UPDATE(local_state->rows_input_counter(), -1LL * std::ranges::count(skipped, true));

        if (channel_field.len == sizeof(uint32_t)) {
            RETURN_IF_ERROR(
                    _channel_add_rows(state, local_state->channels, local_state->channels.size(),
                                      channel_field.get<uint32_t>(), rows, block, skipped, false));
        } else {
            RETURN_IF_ERROR(
                    _channel_add_rows(state, local_state->channels, local_state->channels.size(),
                                      channel_field.get<int64_t>(), rows, block, skipped, false));
        }
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
                               vectorized::Block* block) const {
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
        const std::vector<bool> skipped = std::vector<bool>(rows, false);

        if (channel_field.len == sizeof(uint32_t)) {
            RETURN_IF_ERROR(
                    _channel_add_rows(state, local_state->channels, local_state->channels.size(),
                                      channel_field.get<uint32_t>(), rows, block, skipped, true));
        } else {
            RETURN_IF_ERROR(
                    _channel_add_rows(state, local_state->channels, local_state->channels.size(),
                                      channel_field.get<int64_t>(), rows, block, skipped, true));
        }
    }

    return Status::OK();
}

Status TrivialWriter::write(ExchangeSinkLocalState* local_state, RuntimeState* state,
                            vectorized::Block* block, bool eos) const {
    auto rows = block->rows();
    {
        SCOPED_TIMER(local_state->split_block_hash_compute_timer());
        RETURN_IF_ERROR(local_state->partitioner()->do_partitioning(state, block));
    }
    {
        SCOPED_TIMER(local_state->distribute_rows_into_channels_timer());
        const auto channel_field = local_state->partitioner()->get_channel_ids();

        if (channel_field.len == sizeof(uint32_t)) {
            RETURN_IF_ERROR(_channel_add_rows(state, local_state->channels,
                                              local_state->channels.size(),
                                              channel_field.get<uint32_t>(), rows, block, eos));
        } else {
            RETURN_IF_ERROR(_channel_add_rows(state, local_state->channels,
                                              local_state->channels.size(),
                                              channel_field.get<int64_t>(), rows, block, eos));
        }
    }

    return Status::OK();
}

// NOLINTBEGIN(readability-function-cognitive-complexity)
template <typename ChannelIdType>
Status OlapWriter::_channel_add_rows(RuntimeState* state,
                                     std::vector<std::shared_ptr<vectorized::Channel>>& channels,
                                     size_t partition_count,
                                     const ChannelIdType* __restrict channel_ids, size_t rows,
                                     vectorized::Block* block, const std::vector<bool> skipped,
                                     bool eos) const {
    std::vector<uint32_t> partition_rows_histogram;

    size_t effective_rows = 0;
    effective_rows = 0;
    for (size_t i = 0; i < rows; ++i) {
        if (!skipped[i]) {
            effective_rows++;
        }
    }
    auto row_idx = vectorized::PODArray<uint32_t>(effective_rows);
    { // row index will skip all skipped rows.
        partition_rows_histogram.assign(partition_count + 2, 0);
        for (size_t i = 0; i < rows; ++i) {
            if (!skipped[i]) {
                partition_rows_histogram[channel_ids[i] + 1]++;
            }
        }
        for (size_t i = 1; i <= partition_count + 1; ++i) {
            partition_rows_histogram[i] += partition_rows_histogram[i - 1];
        }
        for (int32_t i = cast_set<int32_t>(rows) - 1; i >= 0; --i) {
            if (!skipped[i]) {
                row_idx[partition_rows_histogram[channel_ids[i] + 1] - 1] = i;
                partition_rows_histogram[channel_ids[i] + 1]--;
            }
        }
    }
    VLOG_DEBUG << fmt::format("skipped: {}\nrow_idx: {}", fmt::join(skipped, ","),
                              fmt::join(row_idx, ","));
    Status status = Status::OK();
    for (size_t i = 0; i < partition_count; ++i) {
        uint32_t start = partition_rows_histogram[i + 1];
        uint32_t size = partition_rows_histogram[i + 2] - start;
        if (!channels[i]->is_receiver_eof() && size > 0) {
            VLOG_DEBUG << fmt::format("partition {} of {}, block:\n{}, start: {}, size: {}", i,
                                      partition_count, block->dump_data(), start, size);
            status = channels[i]->add_rows(block, row_idx.data(), start, size, false);
            HANDLE_CHANNEL_STATUS(state, channels[i], status);
        }
    }
    if (eos) {
        for (int i = 0; i < partition_count; ++i) {
            if (!channels[i]->is_receiver_eof()) {
                VLOG_DEBUG << fmt::format("EOS partition {} of {}, block:\n{}", i, partition_count,
                                          block->dump_data());
                status = channels[i]->add_rows(block, row_idx.data(), 0, 0, true);
                HANDLE_CHANNEL_STATUS(state, channels[i], status);
            }
        }
    }
    return Status::OK();
}

template <typename ChannelIdType>
Status TrivialWriter::_channel_add_rows(RuntimeState* state,
                                        std::vector<std::shared_ptr<vectorized::Channel>>& channels,
                                        size_t partition_count,
                                        const ChannelIdType* __restrict channel_ids, size_t rows,
                                        vectorized::Block* block, bool eos) const {
    std::vector<uint32_t> partition_rows_histogram;

    auto row_idx = vectorized::PODArray<uint32_t>(rows);
    { // row index will skip all skipped rows.
        partition_rows_histogram.assign(partition_count + 2, 0);
        for (size_t i = 0; i < rows; ++i) {
            partition_rows_histogram[channel_ids[i] + 1]++;
        }
        for (size_t i = 1; i <= partition_count + 1; ++i) {
            partition_rows_histogram[i] += partition_rows_histogram[i - 1];
        }
        for (int32_t i = cast_set<int32_t>(rows) - 1; i >= 0; --i) {
            row_idx[partition_rows_histogram[channel_ids[i] + 1] - 1] = i;
            partition_rows_histogram[channel_ids[i] + 1]--;
        }
    }
    Status status = Status::OK();
    for (size_t i = 0; i < partition_count; ++i) {
        uint32_t start = partition_rows_histogram[i + 1];
        uint32_t size = partition_rows_histogram[i + 2] - start;
        if (!channels[i]->is_receiver_eof() && size > 0) {
            VLOG_DEBUG << fmt::format("partition {} of {}, block:\n{}, start: {}, size: {}", i,
                                      partition_count, block->dump_data(), start, size);
            status = channels[i]->add_rows(block, row_idx.data(), start, size, false);
            HANDLE_CHANNEL_STATUS(state, channels[i], status);
        }
    }
    if (eos) {
        for (int i = 0; i < partition_count; ++i) {
            if (!channels[i]->is_receiver_eof()) {
                VLOG_DEBUG << fmt::format("EOS partition {} of {}, block:\n{}", i, partition_count,
                                          block->dump_data());
                status = channels[i]->add_rows(block, row_idx.data(), 0, 0, true);
                HANDLE_CHANNEL_STATUS(state, channels[i], status);
            }
        }
    }
    return Status::OK();
}
// NOLINTEND(readability-function-cognitive-complexity)

} // namespace doris::pipeline
