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

#include "pipeline/exec/exchange_sink_operator.h"
#include "vec/core/block.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"
template <typename ChannelPtrType>
void Writer::_handle_eof_channel(RuntimeState* state, ChannelPtrType channel, Status st) const {
    channel->set_receiver_eof(st);
    // Chanel will not send RPC to the downstream when eof, so close chanel by OK status.
    static_cast<void>(channel->close(state));
}

Status Writer::write(ExchangeSinkLocalState* local_state, RuntimeState* state,
                     vectorized::Block* block, bool eos) const {
    bool already_sent = false;
    {
        SCOPED_TIMER(local_state->split_block_hash_compute_timer());
        RETURN_IF_ERROR(
                local_state->partitioner()->do_partitioning(state, block, eos, &already_sent));
    }
    if (already_sent) {
        // The same block may be sent twice by TabletSinkHashPartitioner. To get the correct
        // result, we should not send any rows the last time.
        return Status::OK();
    }
    auto rows = block->rows();
    {
        SCOPED_TIMER(local_state->distribute_rows_into_channels_timer());
        const auto& channel_filed = local_state->partitioner()->get_channel_ids();
        if (channel_filed.len == sizeof(uint32_t)) {
            RETURN_IF_ERROR(_channel_add_rows(state, local_state->channels,
                                              local_state->channels.size(),
                                              channel_filed.get<uint32_t>(), rows, block, eos));
        } else {
            RETURN_IF_ERROR(_channel_add_rows(state, local_state->channels,
                                              local_state->channels.size(),
                                              channel_filed.get<int64_t>(), rows, block, eos));
        }
    }
    return Status::OK();
}

template <typename ChannelIdType>
Status Writer::_channel_add_rows(RuntimeState* state,
                                 std::vector<std::shared_ptr<vectorized::Channel>>& channels,
                                 size_t partition_count,
                                 const ChannelIdType* __restrict channel_ids, size_t rows,
                                 vectorized::Block* block, bool eos) const {
    std::vector<uint32_t> partition_rows_histogram;
    auto row_idx = vectorized::PODArray<uint32_t>(rows);
    {
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
            status = channels[i]->add_rows(block, row_idx.data(), start, size, false);
            HANDLE_CHANNEL_STATUS(state, channels[i], status);
        }
    }
    if (eos) {
        for (int i = 0; i < partition_count; ++i) {
            if (!channels[i]->is_receiver_eof()) {
                status = channels[i]->add_rows(block, row_idx.data(), 0, 0, true);
                HANDLE_CHANNEL_STATUS(state, channels[i], status);
            }
        }
    }
    return Status::OK();
}

} // namespace doris::pipeline
