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

#include "pipeline/exec/exchange_sink_buffer.h"
#include "pipeline/exec/exchange_sink_operator.h"
#include "pipeline/local_exchange/local_exchanger.h"
#include "vec/core/block.h"
#include "vec/sink/vdata_stream_sender.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"
Status Writer::write(ExchangeSinkLocalState* local_state, RuntimeState* state,
                     vectorized::Block* block, bool eos) const {
    RETURN_IF_ERROR(local_state->exchanger()->sink(
            state, block, eos,
            {local_state->split_block_hash_compute_timer(),
             local_state->distribute_rows_into_channels_timer(), nullptr,
             local_state->enqueue_blocks_counter(), nullptr, nullptr},
            {local_state->channel_id(), local_state->partitioner(), nullptr,
             local_state->channel_selector()}));

    return Status::OK();
}

Status Writer::send_to_channels(
        ExchangeSinkLocalState* local_state, RuntimeState* state, vectorized::Channel* channel,
        int channel_id, bool eos,
        std::shared_ptr<vectorized::BroadcastPBlockHolder> broadcasted_block,
        RuntimeProfile::Counter* test_timer1, RuntimeProfile::Counter* test_timer2,
        RuntimeProfile::Counter* test_timer3, RuntimeProfile::Counter* test_timer4) const {
    if (!channel->is_receiver_eof()) {
        Status status;
        int64_t old_channel_mem_usage = channel->mem_usage();
        {
            SCOPED_TIMER(test_timer4);
            RETURN_IF_ERROR(local_state->exchanger()->get_block(
                    state, channel->serializer()->get_result_block(), &eos,
                    {nullptr, nullptr, local_state->copy_shuffled_data_timer(), nullptr,
                     local_state->dequeue_blocks_counter(),
                     local_state->get_block_failed_counter()},
                    {channel_id, nullptr, channel->serializer()->get_block()}));
            if (channel->serializer()->get_block()->data_types().empty()) {
                // Initialize this block.
                *channel->serializer()->get_block() =
                        channel->serializer()->get_result_block()->clone_empty();
            } else if (!channel->serializer()->get_block()->empty() &&
                       channel->serializer()->get_result_block()->empty()) {
                DCHECK_EQ(local_state->part_type(),
                          TPartitionType::TABLET_SINK_SHUFFLE_PARTITIONED);
                // TODO: `TABLET_SINK_SHUFFLE_PARTITIONED` has a strange logics here. For a new
                //  partition, partitioner will enter `ExchangeSinkOperatorX::sink` twice using the
                //  same block. For the second entrance, this selected channel may get nothing so we
                //  just keep the origin block.
            } else {
                // Data is stored in `channel->serializer()->get_result_block()` and
                // `channel->serializer()->get_block()` just keep empty columns.
                // So we just move the result columns here.
                channel->serializer()->get_block()->set_mutable_columns(
                        channel->serializer()->get_result_block()->mutate_columns());
                channel->serializer()->get_result_block()->clear();
            }
        }
        {
            if (broadcasted_block && !channel->is_local()) {
                SCOPED_TIMER(test_timer1);
                status = channel->send_broadcast_block(broadcasted_block, eos);
            } else {
                bool sent = false;
                SCOPED_TIMER(test_timer3);
                status = channel->send_block(channel->serializer()->get_block(), &sent);
                // If block is sent by a local channel, it will be moved to downstream fragment, and
                // we should never share the columns otherwise we just clear the data and keep the
                // memory.
                if (!channel->is_local() && sent) {
                    channel->serializer()->get_block()->clear_column_data();
                }
            }
        }

        if (status.is<ErrorCode::END_OF_FILE>()) {
            channel->set_receiver_eof(status);
            RETURN_IF_ERROR(finish(local_state->exchanger(), state, channel, channel_id, status));
        } else {
            RETURN_IF_ERROR(status);
        }
        int64_t new_channel_mem_usage = channel->mem_usage();
        COUNTER_UPDATE(local_state->memory_used_counter(),
                       new_channel_mem_usage - old_channel_mem_usage);
    }
    return Status::OK();
}

Status Writer::finish(ExchangerBase* exchanger, RuntimeState* state, vectorized::Channel* channel,
                      int channel_id, Status status) const {
    exchanger->close({channel_id, nullptr});
    RETURN_IF_ERROR(channel->close(state, status));
    return Status::OK();
}

} // namespace doris::pipeline
