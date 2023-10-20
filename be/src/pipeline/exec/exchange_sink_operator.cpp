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

#include "exchange_sink_operator.h"

#include <gen_cpp/DataSinks_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/types.pb.h>

#include <random>

#include "common/status.h"
#include "exchange_sink_buffer.h"
#include "pipeline/exec/operator.h"
#include "vec/columns/column_const.h"
#include "vec/exprs/vexpr.h"
#include "vec/sink/vdata_stream_sender.h"

namespace doris {
class DataSink;
} // namespace doris

namespace doris::pipeline {

ExchangeSinkOperatorBuilder::ExchangeSinkOperatorBuilder(int32_t id, DataSink* sink,
                                                         int mult_cast_id)
        : DataSinkOperatorBuilder(id, "ExchangeSinkOperator", sink), _mult_cast_id(mult_cast_id) {}

OperatorPtr ExchangeSinkOperatorBuilder::build_operator() {
    return std::make_shared<ExchangeSinkOperator>(this, _sink, _mult_cast_id);
}

ExchangeSinkOperator::ExchangeSinkOperator(OperatorBuilderBase* operator_builder, DataSink* sink,
                                           int mult_cast_id)
        : DataSinkOperator(operator_builder, sink), _mult_cast_id(mult_cast_id) {}

Status ExchangeSinkOperator::init(const TDataSink& tsink) {
    // -1 means not the mult cast stream sender
    if (_mult_cast_id == -1) {
        _dest_node_id = tsink.stream_sink.dest_node_id;
    } else {
        _dest_node_id = tsink.multi_cast_stream_sink.sinks[_mult_cast_id].dest_node_id;
    }
    return Status::OK();
}

Status ExchangeSinkOperator::prepare(RuntimeState* state) {
    _state = state;
    PUniqueId id;
    id.set_hi(_state->query_id().hi);
    id.set_lo(_state->query_id().lo);
    _sink_buffer = std::make_unique<ExchangeSinkBuffer<vectorized::VDataStreamSender>>(
            id, _dest_node_id, _sink->_sender_id, _state->be_number(), state->get_query_ctx());

    RETURN_IF_ERROR(DataSinkOperator::prepare(state));
    _sink->registe_channels(_sink_buffer.get());
    return Status::OK();
}

bool ExchangeSinkOperator::can_write() {
    return _sink_buffer->can_write() && _sink->channel_all_can_write();
}

bool ExchangeSinkOperator::is_pending_finish() const {
    return _sink_buffer->is_pending_finish();
}

Status ExchangeSinkOperator::close(RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperator::close(state));
    _sink_buffer->update_profile(_sink->profile());
    _sink_buffer->close();
    return Status::OK();
}

Status ExchangeSinkLocalState::serialize_block(vectorized::Block* src, PBlock* dest,
                                               int num_receivers) {
    return _parent->cast<ExchangeSinkOperatorX>().serialize_block(*this, src, dest, num_receivers);
}

bool ExchangeSinkLocalState::transfer_large_data_by_brpc() const {
    return _parent->cast<ExchangeSinkOperatorX>()._transfer_large_data_by_brpc;
}

Status ExchangeSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(PipelineXSinkLocalState<>::init(state, info));
    SCOPED_TIMER(profile()->total_time_counter());
    SCOPED_TIMER(_open_timer);
    _sender_id = info.sender_id;

    _bytes_sent_counter = ADD_COUNTER(_profile, "BytesSent", TUnit::BYTES);
    _uncompressed_bytes_counter = ADD_COUNTER(_profile, "UncompressedRowBatchSize", TUnit::BYTES);
    _local_sent_rows = ADD_COUNTER(_profile, "LocalSentRows", TUnit::UNIT);
    _serialize_batch_timer = ADD_TIMER(_profile, "SerializeBatchTime");
    _compress_timer = ADD_TIMER(_profile, "CompressTime");
    _brpc_send_timer = ADD_TIMER(_profile, "BrpcSendTime");
    _brpc_wait_timer = ADD_TIMER(_profile, "BrpcSendTime.Wait");
    _local_send_timer = ADD_TIMER(_profile, "LocalSendTime");
    _split_block_hash_compute_timer = ADD_TIMER(_profile, "SplitBlockHashComputeTime");
    _split_block_distribute_by_channel_timer =
            ADD_TIMER(_profile, "SplitBlockDistributeByChannelTime");
    _blocks_sent_counter = ADD_COUNTER_WITH_LEVEL(_profile, "BlocksSent", TUnit::UNIT, 1);
    _overall_throughput = _profile->add_derived_counter(
            "OverallThroughput", TUnit::BYTES_PER_SECOND,
            std::bind<int64_t>(&RuntimeProfile::units_per_second, _bytes_sent_counter,
                               _profile->total_time_counter()),
            "");
    _merge_block_timer = ADD_TIMER(profile(), "MergeBlockTime");
    _local_bytes_send_counter = ADD_COUNTER(_profile, "LocalBytesSent", TUnit::BYTES);
    _memory_usage_counter = ADD_LABEL_COUNTER(_profile, "MemoryUsage");
    _peak_memory_usage_counter =
            _profile->AddHighWaterMarkCounter("PeakMemoryUsage", TUnit::BYTES, "MemoryUsage");

    static const std::string timer_name = "WaitForDependencyTime";
    _wait_for_dependency_timer = ADD_TIMER(_profile, timer_name);
    _wait_queue_timer = ADD_CHILD_TIMER(_profile, "WaitForRpcBufferQueue", timer_name);

    auto& p = _parent->cast<ExchangeSinkOperatorX>();

    std::map<int64_t, int64_t> fragment_id_to_channel_index;
    for (int i = 0; i < p._dests.size(); ++i) {
        // Select first dest as transfer chain.
        bool is_transfer_chain = (i == 0);
        const auto& fragment_instance_id = p._dests[i].fragment_instance_id;
        if (fragment_id_to_channel_index.find(fragment_instance_id.lo) ==
            fragment_id_to_channel_index.end()) {
            channel_shared_ptrs.emplace_back(new vectorized::PipChannel<ExchangeSinkLocalState>(
                    this, p._row_desc, p._dests[i].brpc_server, fragment_instance_id,
                    p._dest_node_id, is_transfer_chain, p._send_query_statistics_with_every_batch));
            fragment_id_to_channel_index.emplace(fragment_instance_id.lo,
                                                 channel_shared_ptrs.size() - 1);
            channels.push_back(channel_shared_ptrs.back().get());
        } else {
            channel_shared_ptrs.emplace_back(
                    channel_shared_ptrs[fragment_id_to_channel_index[fragment_instance_id.lo]]);
        }
    }
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());

    int local_size = 0;
    for (int i = 0; i < channels.size(); ++i) {
        RETURN_IF_ERROR(channels[i]->init(state));
        if (channels[i]->is_local()) {
            local_size++;
        }
    }
    if (p._part_type == TPartitionType::UNPARTITIONED || p._part_type == TPartitionType::RANDOM) {
        std::random_device rd;
        std::mt19937 g(rd());
        shuffle(channels.begin(), channels.end(), g);
    }
    only_local_exchange = local_size == channels.size();

    PUniqueId id;
    id.set_hi(_state->query_id().hi);
    id.set_lo(_state->query_id().lo);
    _sink_buffer = std::make_unique<ExchangeSinkBuffer<ExchangeSinkLocalState>>(
            id, p._dest_node_id, _sender_id, _state->be_number(), state->get_query_ctx());

    register_channels(_sink_buffer.get());

    _exchange_sink_dependency = AndDependency::create_shared(_parent->operator_id());
    _queue_dependency = ExchangeSinkQueueDependency::create_shared(_parent->operator_id());
    _sink_buffer->set_dependency(_queue_dependency, _finish_dependency);
    _exchange_sink_dependency->add_child(_queue_dependency);
    if ((p._part_type == TPartitionType::UNPARTITIONED || channels.size() == 1) &&
        !only_local_exchange) {
        _broadcast_dependency = BroadcastDependency::create_shared(_parent->operator_id());
        _broadcast_dependency->set_available_block(config::num_broadcast_buffer);
        _broadcast_pb_blocks.reserve(config::num_broadcast_buffer);
        for (size_t i = 0; i < config::num_broadcast_buffer; i++) {
            _broadcast_pb_blocks.emplace_back(
                    vectorized::BroadcastPBlockHolder(_broadcast_dependency.get()));
        }
        _exchange_sink_dependency->add_child(_broadcast_dependency);

        _wait_broadcast_buffer_timer =
                ADD_CHILD_TIMER(_profile, "WaitForBroadcastBuffer", timer_name);
    } else if (local_size > 0) {
        size_t dep_id = 0;
        _channels_dependency.resize(local_size);
        _wait_channel_timer.resize(local_size);
        auto deps_for_channels = AndDependency::create_shared(_parent->operator_id());
        for (auto channel : channels) {
            if (channel->is_local()) {
                _channels_dependency[dep_id] =
                        ChannelDependency::create_shared(_parent->operator_id());
                channel->set_dependency(_channels_dependency[dep_id]);
                deps_for_channels->add_child(_channels_dependency[dep_id]);
                _wait_channel_timer[dep_id] =
                        ADD_CHILD_TIMER(_profile, "WaitForLocalExchangeBuffer", timer_name);
                dep_id++;
            }
        }
        _exchange_sink_dependency->add_child(deps_for_channels);
    }
    if (p._part_type == TPartitionType::HASH_PARTITIONED) {
        _partition_count = channels.size();
        _partitioner.reset(new vectorized::HashPartitioner(channels.size()));
        RETURN_IF_ERROR(_partitioner->init(p._texprs));
        RETURN_IF_ERROR(_partitioner->prepare(state, p._row_desc));
    } else if (p._part_type == TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED) {
        _partition_count = channel_shared_ptrs.size();
        _partitioner.reset(new vectorized::BucketHashPartitioner(channel_shared_ptrs.size()));
        RETURN_IF_ERROR(_partitioner->init(p._texprs));
        RETURN_IF_ERROR(_partitioner->prepare(state, p._row_desc));
    }

    return Status::OK();
}

Status ExchangeSinkLocalState::open(RuntimeState* state) {
    RETURN_IF_ERROR(PipelineXSinkLocalState<>::open(state));
    auto& p = _parent->cast<ExchangeSinkOperatorX>();
    if (p._part_type == TPartitionType::HASH_PARTITIONED ||
        p._part_type == TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED) {
        RETURN_IF_ERROR(_partitioner->open(state));
    }
    return Status::OK();
}

segment_v2::CompressionTypePB& ExchangeSinkLocalState::compression_type() {
    return _parent->cast<ExchangeSinkOperatorX>()._compression_type;
}

ExchangeSinkOperatorX::ExchangeSinkOperatorX(
        RuntimeState* state, const RowDescriptor& row_desc, int operator_id,
        const TDataStreamSink& sink, const std::vector<TPlanFragmentDestination>& destinations,
        bool send_query_statistics_with_every_batch)
        : DataSinkOperatorX(operator_id, sink.dest_node_id),
          _texprs(sink.output_partition.partition_exprs),
          _row_desc(row_desc),
          _part_type(sink.output_partition.type),
          _dests(destinations),
          _send_query_statistics_with_every_batch(send_query_statistics_with_every_batch),
          _dest_node_id(sink.dest_node_id),
          _transfer_large_data_by_brpc(config::transfer_large_data_by_brpc) {
    DCHECK_GT(destinations.size(), 0);
    DCHECK(sink.output_partition.type == TPartitionType::UNPARTITIONED ||
           sink.output_partition.type == TPartitionType::HASH_PARTITIONED ||
           sink.output_partition.type == TPartitionType::RANDOM ||
           sink.output_partition.type == TPartitionType::RANGE_PARTITIONED ||
           sink.output_partition.type == TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED);
    _name = "ExchangeSinkOperatorX";
}

Status ExchangeSinkOperatorX::init(const TDataSink& tsink) {
    RETURN_IF_ERROR(DataSinkOperatorX::init(tsink));
    if (_part_type == TPartitionType::RANGE_PARTITIONED) {
        return Status::InternalError("TPartitionType::RANGE_PARTITIONED should not be used");
    }
    return Status::OK();
}

Status ExchangeSinkOperatorX::prepare(RuntimeState* state) {
    _state = state;
    _mem_tracker = std::make_unique<MemTracker>("ExchangeSinkOperatorX:");
    return Status::OK();
}

Status ExchangeSinkOperatorX::open(RuntimeState* state) {
    DCHECK(state != nullptr);
    _compression_type = state->fragement_transmission_compression_type();
    return Status::OK();
}

template <typename ChannelPtrType>
void ExchangeSinkOperatorX::_handle_eof_channel(RuntimeState* state, ChannelPtrType channel,
                                                Status st) {
    channel->set_receiver_eof(st);
    // Chanel will not send RPC to the downstream when eof, so close chanel by OK status.
    static_cast<void>(channel->close(state, Status::OK()));
}

Status ExchangeSinkOperatorX::sink(RuntimeState* state, vectorized::Block* block,
                                   SourceState source_state) {
    CREATE_SINK_LOCAL_STATE_RETURN_IF_ERROR(local_state);
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)block->rows());
    SCOPED_TIMER(local_state.profile()->total_time_counter());
    local_state._peak_memory_usage_counter->set(_mem_tracker->peak_consumption());
    bool all_receiver_eof = true;
    for (auto channel : local_state.channels) {
        if (!channel->is_receiver_eof()) {
            all_receiver_eof = false;
            break;
        }
    }
    if (all_receiver_eof) {
        return Status::EndOfFile("all data stream channels EOF");
    }

    if (_part_type == TPartitionType::UNPARTITIONED || local_state.channels.size() == 1) {
        // 1. serialize depends on it is not local exchange
        // 2. send block
        // 3. rollover block
        if (local_state.only_local_exchange) {
            if (!block->empty()) {
                Status status;
                for (auto channel : local_state.channels) {
                    if (!channel->is_receiver_eof()) {
                        status = channel->send_local_block(block);
                        HANDLE_CHANNEL_STATUS(state, channel, status);
                    }
                }
            }
        } else {
            vectorized::BroadcastPBlockHolder* block_holder = nullptr;
            RETURN_IF_ERROR(local_state.get_next_available_buffer(&block_holder));
            {
                SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
                bool serialized = false;
                RETURN_IF_ERROR(local_state._serializer.next_serialized_block(
                        block, block_holder->get_block(), local_state.channels.size(), &serialized,
                        source_state == SourceState::FINISHED));
                if (serialized) {
                    auto cur_block = local_state._serializer.get_block()->to_block();
                    if (!cur_block.empty()) {
                        static_cast<void>(local_state._serializer.serialize_block(
                                &cur_block, block_holder->get_block(),
                                local_state.channels.size()));
                    } else {
                        block_holder->get_block()->Clear();
                    }
                    Status status;
                    bool sent = false;
                    for (auto channel : local_state.channels) {
                        if (!channel->is_receiver_eof()) {
                            if (channel->is_local()) {
                                status = channel->send_local_block(&cur_block);
                            } else {
                                SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
                                status = channel->send_broadcast_block(
                                        block_holder, &sent, source_state == SourceState::FINISHED);
                            }
                            HANDLE_CHANNEL_STATUS(state, channel, status);
                        }
                    }
                    if (sent) {
                        local_state._broadcast_dependency->take_available_block();
                    }
                    cur_block.clear_column_data();
                    local_state._serializer.get_block()->set_muatable_columns(
                            cur_block.mutate_columns());
                }
            }
        }
    } else if (_part_type == TPartitionType::RANDOM) {
        // 1. select channel
        vectorized::PipChannel<ExchangeSinkLocalState>* current_channel =
                local_state.channels[local_state.current_channel_idx];
        if (!current_channel->is_receiver_eof()) {
            // 2. serialize, send and rollover block
            if (current_channel->is_local()) {
                auto status = current_channel->send_local_block(block);
                HANDLE_CHANNEL_STATUS(state, current_channel, status);
            } else {
                SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
                RETURN_IF_ERROR(local_state._serializer.serialize_block(
                        block, current_channel->ch_cur_pb_block()));
                auto status = current_channel->send_remote_block(
                        current_channel->ch_cur_pb_block(), source_state == SourceState::FINISHED);
                HANDLE_CHANNEL_STATUS(state, current_channel, status);
                current_channel->ch_roll_pb_block();
            }
        }
        local_state.current_channel_idx =
                (local_state.current_channel_idx + 1) % local_state.channels.size();
    } else if (_part_type == TPartitionType::HASH_PARTITIONED ||
               _part_type == TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED) {
        auto rows = block->rows();
        SCOPED_TIMER(local_state._split_block_hash_compute_timer);
        RETURN_IF_ERROR(
                local_state._partitioner->do_partitioning(state, block, _mem_tracker.get()));
        if (_part_type == TPartitionType::HASH_PARTITIONED) {
            RETURN_IF_ERROR(channel_add_rows(state, local_state.channels,
                                             local_state._partition_count,
                                             (uint64_t*)local_state._partitioner->get_hash_values(),
                                             rows, block, source_state == SourceState::FINISHED));
        } else {
            RETURN_IF_ERROR(channel_add_rows(state, local_state.channel_shared_ptrs,
                                             local_state._partition_count,
                                             (uint32_t*)local_state._partitioner->get_hash_values(),
                                             rows, block, source_state == SourceState::FINISHED));
        }
    } else {
        // Range partition
        // 1. calculate range
        // 2. dispatch rows to channel
    }
    return Status::OK();
}

Status ExchangeSinkOperatorX::serialize_block(ExchangeSinkLocalState& state, vectorized::Block* src,
                                              PBlock* dest, int num_receivers) {
    {
        SCOPED_TIMER(state.serialize_batch_timer());
        dest->Clear();
        size_t uncompressed_bytes = 0, compressed_bytes = 0;
        RETURN_IF_ERROR(src->serialize(_state->be_exec_version(), dest, &uncompressed_bytes,
                                       &compressed_bytes, _compression_type,
                                       _transfer_large_data_by_brpc));
        COUNTER_UPDATE(state.bytes_sent_counter(), compressed_bytes * num_receivers);
        COUNTER_UPDATE(state.uncompressed_bytes_counter(), uncompressed_bytes * num_receivers);
        COUNTER_UPDATE(state.compress_timer(), src->get_compress_time());
    }

    return Status::OK();
}

void ExchangeSinkLocalState::register_channels(
        pipeline::ExchangeSinkBuffer<ExchangeSinkLocalState>* buffer) {
    for (auto channel : channels) {
        ((vectorized::PipChannel<ExchangeSinkLocalState>*)channel)->registe(buffer);
    }
}

Status ExchangeSinkLocalState::get_next_available_buffer(
        vectorized::BroadcastPBlockHolder** holder) {
    // This condition means we need use broadcast buffer, so we should make sure
    // there are available buffer before running pipeline
    for (size_t broadcast_pb_block_idx = 0; broadcast_pb_block_idx < _broadcast_pb_blocks.size();
         broadcast_pb_block_idx++) {
        if (_broadcast_pb_blocks[broadcast_pb_block_idx].available()) {
            *holder = &_broadcast_pb_blocks[broadcast_pb_block_idx];
            return Status::OK();
        }
    }
    return Status::InternalError("No broadcast buffer left!");
}

template <typename Channels, typename HashValueType>
Status ExchangeSinkOperatorX::channel_add_rows(RuntimeState* state, Channels& channels,
                                               int num_channels,
                                               const HashValueType* __restrict channel_ids,
                                               int rows, vectorized::Block* block, bool eos) {
    std::vector<int> channel2rows[num_channels];

    for (int i = 0; i < rows; i++) {
        channel2rows[channel_ids[i]].emplace_back(i);
    }

    Status status;
    for (int i = 0; i < num_channels; ++i) {
        if (!channels[i]->is_receiver_eof() && !channel2rows[i].empty()) {
            status = channels[i]->add_rows(block, channel2rows[i], false);
            HANDLE_CHANNEL_STATUS(state, channels[i], status);
            channel2rows[i].clear();
        }
    }

    if (eos) {
        for (int i = 0; i < num_channels; ++i) {
            if (!channels[i]->is_receiver_eof()) {
                status = channels[i]->add_rows(block, channel2rows[i], true);
                HANDLE_CHANNEL_STATUS(state, channels[i], status);
            }
        }
    }

    return Status::OK();
}

Status ExchangeSinkOperatorX::try_close(RuntimeState* state, Status exec_status) {
    CREATE_SINK_LOCAL_STATE_RETURN_IF_ERROR(local_state);
    local_state._serializer.reset_block();
    Status final_st = Status::OK();
    Status final_status = exec_status;
    for (int i = 0; i < local_state.channels.size(); ++i) {
        Status st = local_state.channels[i]->close(state, exec_status);
        if (!st.ok() && final_st.ok()) {
            final_st = st;
        }
    }
    return final_st;
}

Status ExchangeSinkLocalState::close(RuntimeState* state, Status exec_status) {
    if (_closed) {
        return Status::OK();
    }
    SCOPED_TIMER(profile()->total_time_counter());
    SCOPED_TIMER(_close_timer);
    COUNTER_UPDATE(_wait_queue_timer, _queue_dependency->write_watcher_elapse_time());
    if (_broadcast_dependency) {
        COUNTER_UPDATE(_wait_broadcast_buffer_timer,
                       _broadcast_dependency->write_watcher_elapse_time());
    }
    for (size_t i = 0; i < _channels_dependency.size(); i++) {
        COUNTER_UPDATE(_wait_channel_timer[i],
                       _channels_dependency[i]->write_watcher_elapse_time());
    }
    _sink_buffer->update_profile(profile());
    _sink_buffer->close();
    return PipelineXSinkLocalState<>::close(state, exec_status);
}

WriteDependency* ExchangeSinkOperatorX::wait_for_dependency(RuntimeState* state) {
    CREATE_SINK_LOCAL_STATE_RETURN_NULL_IF_ERROR(local_state);
    return local_state._exchange_sink_dependency->write_blocked_by();
}

FinishDependency* ExchangeSinkOperatorX::finish_blocked_by(RuntimeState* state) const {
    auto& local_state = state->get_sink_local_state(operator_id())->cast<ExchangeSinkLocalState>();
    return local_state._finish_dependency->finish_blocked_by();
}

} // namespace doris::pipeline
