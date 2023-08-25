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
#include "pipeline/pipeline_x/pipeline_x_fragment_context.h"
#include "vec/columns/column_const.h"
#include "vec/exprs/vexpr.h"
#include "vec/sink/vdata_stream_sender.h"

namespace doris {
class DataSink;
} // namespace doris

namespace doris::pipeline {

ExchangeSinkOperatorBuilder::ExchangeSinkOperatorBuilder(int32_t id, DataSink* sink,
                                                         PipelineFragmentContext* context,
                                                         int mult_cast_id)
        : DataSinkOperatorBuilder(id, "ExchangeSinkOperator", sink),
          _context(context),
          _mult_cast_id(mult_cast_id) {}

OperatorPtr ExchangeSinkOperatorBuilder::build_operator() {
    return std::make_shared<ExchangeSinkOperator>(this, _sink, _context, _mult_cast_id);
}

ExchangeSinkOperator::ExchangeSinkOperator(OperatorBuilderBase* operator_builder, DataSink* sink,
                                           PipelineFragmentContext* context, int mult_cast_id)
        : DataSinkOperator(operator_builder, sink),
          _context(context),
          _mult_cast_id(mult_cast_id) {}

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
            id, _dest_node_id, _sink->_sender_id, _state->be_number(), _context);

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
    RETURN_IF_ERROR(PipelineXSinkLocalState::init(state, info));
    _sender_id = info.sender_id;
    _broadcast_pb_blocks.resize(config::num_broadcast_buffer);
    _broadcast_pb_block_idx = 0;
    auto& p = _parent->cast<ExchangeSinkOperatorX>();

    std::map<int64_t, int64_t> fragment_id_to_channel_index;
    for (int i = 0; i < p._dests.size(); ++i) {
        const auto& fragment_instance_id = p._dests[i].fragment_instance_id;
        if (fragment_id_to_channel_index.find(fragment_instance_id.lo) ==
            fragment_id_to_channel_index.end()) {
            channel_shared_ptrs.emplace_back(new vectorized::PipChannel<ExchangeSinkLocalState>(
                    this, p._row_desc, p._dests[i].brpc_server, fragment_instance_id,
                    p._dest_node_id, false, p._send_query_statistics_with_every_batch));
        }
        fragment_id_to_channel_index.emplace(fragment_instance_id.lo,
                                             channel_shared_ptrs.size() - 1);
        channels.push_back(channel_shared_ptrs.back().get());
    }

    std::vector<std::string> instances;
    for (const auto& channel : channels) {
        instances.emplace_back(channel->get_fragment_instance_id_str());
    }
    std::string title = "VDataStreamSender (dst_id={}, dst_fragments=[{}])";
    _profile = p._pool->add(new RuntimeProfile(title));
    SCOPED_TIMER(_profile->total_time_counter());
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
    } else {
        partition_expr_ctxs.resize(p._partition_expr_ctxs.size());
        for (size_t i = 0; i < p._partition_expr_ctxs.size(); i++) {
            RETURN_IF_ERROR(p._partition_expr_ctxs[i]->clone(state, partition_expr_ctxs[i]));
        }
    }
    only_local_exchange = local_size == channels.size();

    PUniqueId id;
    id.set_hi(_state->query_id().hi);
    id.set_lo(_state->query_id().lo);
    _sink_buffer = std::make_unique<ExchangeSinkBuffer<ExchangeSinkLocalState>>(
            id, p._dest_node_id, _sender_id, _state->be_number(), p._context);

    register_channels(_sink_buffer.get());

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
    _blocks_sent_counter = ADD_COUNTER(_profile, "BlocksSent", TUnit::UNIT);
    _overall_throughput = _profile->add_derived_counter(
            "OverallThroughput", TUnit::BYTES_PER_SECOND,
            std::bind<int64_t>(&RuntimeProfile::units_per_second, _bytes_sent_counter,
                               _profile->total_time_counter()),
            "");
    _merge_block_timer = ADD_TIMER(profile(), "MergeBlockTime");
    _local_bytes_send_counter = ADD_COUNTER(_profile, "LocalBytesSent", TUnit::BYTES);
    _memory_usage_counter = ADD_LABEL_COUNTER(profile(), "MemoryUsage");
    _peak_memory_usage_counter =
            profile()->AddHighWaterMarkCounter("PeakMemoryUsage", TUnit::BYTES, "MemoryUsage");
    return Status::OK();
}

segment_v2::CompressionTypePB& ExchangeSinkLocalState::compression_type() {
    return _parent->cast<ExchangeSinkOperatorX>()._compression_type;
}

ExchangeSinkOperatorX::ExchangeSinkOperatorX(
        RuntimeState* state, ObjectPool* pool, const RowDescriptor& row_desc,
        const TDataStreamSink& sink, const std::vector<TPlanFragmentDestination>& destinations,
        bool send_query_statistics_with_every_batch, PipelineXFragmentContext* context)
        : DataSinkOperatorX(sink.dest_node_id),
          _context(context),
          _pool(pool),
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

ExchangeSinkOperatorX::ExchangeSinkOperatorX(
        ObjectPool* pool, const RowDescriptor& row_desc, PlanNodeId dest_node_id,
        const std::vector<TPlanFragmentDestination>& destinations,
        bool send_query_statistics_with_every_batch, PipelineXFragmentContext* context)
        : DataSinkOperatorX(dest_node_id),
          _context(context),
          _pool(pool),
          _row_desc(row_desc),
          _part_type(TPartitionType::UNPARTITIONED),
          _dests(destinations),
          _send_query_statistics_with_every_batch(send_query_statistics_with_every_batch),
          _dest_node_id(dest_node_id) {
    _cur_pb_block = &_pb_block1;
    _name = "ExchangeSinkOperatorX";
}

ExchangeSinkOperatorX::ExchangeSinkOperatorX(ObjectPool* pool, const RowDescriptor& row_desc,
                                             bool send_query_statistics_with_every_batch,
                                             PipelineXFragmentContext* context)
        : DataSinkOperatorX(0),
          _context(context),
          _pool(pool),
          _row_desc(row_desc),
          _send_query_statistics_with_every_batch(send_query_statistics_with_every_batch),
          _dest_node_id(0) {
    _cur_pb_block = &_pb_block1;
    _name = "ExchangeSinkOperatorX";
}

Status ExchangeSinkOperatorX::init(const TDataSink& tsink) {
    RETURN_IF_ERROR(DataSinkOperatorX::init(tsink));
    const TDataStreamSink& t_stream_sink = tsink.stream_sink;
    if (_part_type == TPartitionType::HASH_PARTITIONED ||
        _part_type == TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED) {
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(
                t_stream_sink.output_partition.partition_exprs, _partition_expr_ctxs));
    } else if (_part_type == TPartitionType::RANGE_PARTITIONED) {
        return Status::InternalError("TPartitionType::RANGE_PARTITIONED should not be used");
    } else {
        // UNPARTITIONED
    }
    return Status::OK();
}

Status ExchangeSinkOperatorX::setup_local_state(RuntimeState* state, LocalSinkStateInfo& info) {
    auto local_state = ExchangeSinkLocalState::create_shared(this, state);
    state->emplace_sink_local_state(id(), local_state);
    return local_state->init(state, info);
}

Status ExchangeSinkOperatorX::prepare(RuntimeState* state) {
    _state = state;

    std::string title = fmt::format("VDataStreamSender (dst_id={})", _dest_node_id);
    _profile = _pool->add(new RuntimeProfile(title));
    SCOPED_TIMER(_profile->total_time_counter());
    _mem_tracker = std::make_unique<MemTracker>("ExchangeSinkOperatorX:");
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());

    if (!(_part_type == TPartitionType::UNPARTITIONED) && !(_part_type == TPartitionType::RANDOM)) {
        RETURN_IF_ERROR(vectorized::VExpr::prepare(_partition_expr_ctxs, state, _row_desc));
    }
    return Status::OK();
}

Status ExchangeSinkOperatorX::open(RuntimeState* state) {
    DCHECK(state != nullptr);

    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
    RETURN_IF_ERROR(vectorized::VExpr::open(_partition_expr_ctxs, state));

    _compression_type = state->fragement_transmission_compression_type();
    return Status::OK();
}

template <typename ChannelPtrType>
void ExchangeSinkOperatorX::_handle_eof_channel(RuntimeState* state, ChannelPtrType channel,
                                                Status st) {
    channel->set_receiver_eof(st);
    channel->close(state);
}

Status ExchangeSinkOperatorX::sink(RuntimeState* state, vectorized::Block* block,
                                   SourceState source_state) {
    auto& local_state = state->get_sink_local_state(id())->cast<ExchangeSinkLocalState>();
    SCOPED_TIMER(_profile->total_time_counter());
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
                        local_state._serializer.serialize_block(
                                &cur_block, block_holder->get_block(), local_state.channels.size());
                    } else {
                        block_holder->get_block()->Clear();
                    }
                    Status status;
                    for (auto channel : local_state.channels) {
                        if (!channel->is_receiver_eof()) {
                            if (channel->is_local()) {
                                status = channel->send_local_block(&cur_block);
                            } else {
                                SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
                                status = channel->send_block(block_holder,
                                                             source_state == SourceState::FINISHED);
                            }
                            HANDLE_CHANNEL_STATUS(state, channel, status);
                        }
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
                auto status = current_channel->send_block(current_channel->ch_cur_pb_block(),
                                                          source_state == SourceState::FINISHED);
                HANDLE_CHANNEL_STATUS(state, current_channel, status);
                current_channel->ch_roll_pb_block();
            }
        }
        local_state.current_channel_idx =
                (local_state.current_channel_idx + 1) % local_state.channels.size();
    } else if (_part_type == TPartitionType::HASH_PARTITIONED ||
               _part_type == TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED) {
        // will only copy schema
        // we don't want send temp columns
        auto column_to_keep = block->columns();

        int result_size = _partition_expr_ctxs.size();
        int result[result_size];

        // vectorized calculate hash
        int rows = block->rows();
        auto element_size = _part_type == TPartitionType::HASH_PARTITIONED
                                    ? local_state.channels.size()
                                    : local_state.channel_shared_ptrs.size();
        std::vector<uint64_t> hash_vals(rows);
        auto* __restrict hashes = hash_vals.data();

        if (rows > 0) {
            {
                SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
                RETURN_IF_ERROR(get_partition_column_result(block, result));
            }
            // TODO: after we support new shuffle hash method, should simple the code
            if (_part_type == TPartitionType::HASH_PARTITIONED) {
                SCOPED_TIMER(local_state._split_block_hash_compute_timer);
                // result[j] means column index, i means rows index, here to calculate the xxhash value
                for (int j = 0; j < result_size; ++j) {
                    // complex type most not implement get_data_at() method which column_const will call
                    unpack_if_const(block->get_by_position(result[j]).column)
                            .first->update_hashes_with_value(hashes);
                }

                for (int i = 0; i < rows; i++) {
                    hashes[i] = hashes[i] % element_size;
                }

                {
                    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
                    vectorized::Block::erase_useless_column(block, column_to_keep);
                }
            } else {
                for (int j = 0; j < result_size; ++j) {
                    // complex type most not implement get_data_at() method which column_const will call
                    unpack_if_const(block->get_by_position(result[j]).column)
                            .first->update_crcs_with_value(
                                    hash_vals, _partition_expr_ctxs[j]->root()->type().type);
                }
                for (int i = 0; i < rows; i++) {
                    hashes[i] = hashes[i] % element_size;
                }

                {
                    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
                    vectorized::Block::erase_useless_column(block, column_to_keep);
                }
            }
        }
        if (_part_type == TPartitionType::HASH_PARTITIONED) {
            RETURN_IF_ERROR(channel_add_rows(state, local_state.channels, element_size, hashes,
                                             rows, block, source_state == SourceState::FINISHED));
        } else {
            RETURN_IF_ERROR(channel_add_rows(state, local_state.channel_shared_ptrs, element_size,
                                             hashes, rows, block,
                                             source_state == SourceState::FINISHED));
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

bool ExchangeSinkLocalState::channel_all_can_write() {
    auto& p = _parent->cast<ExchangeSinkOperatorX>();
    if ((p._part_type == TPartitionType::UNPARTITIONED || channels.size() == 1) &&
        !only_local_exchange) {
        // This condition means we need use broadcast buffer, so we should make sure
        // there are available buffer before running pipeline
        if (_broadcast_pb_block_idx == _broadcast_pb_blocks.size()) {
            _broadcast_pb_block_idx = 0;
        }

        for (; _broadcast_pb_block_idx < _broadcast_pb_blocks.size(); _broadcast_pb_block_idx++) {
            if (_broadcast_pb_blocks[_broadcast_pb_block_idx].available()) {
                return true;
            }
        }
        return false;
    } else {
        for (auto channel : channels) {
            if (!channel->can_write()) {
                return false;
            }
        }
        return true;
    }
}

void ExchangeSinkLocalState::register_channels(
        pipeline::ExchangeSinkBuffer<ExchangeSinkLocalState>* buffer) {
    for (auto channel : channels) {
        ((vectorized::PipChannel<ExchangeSinkLocalState>*)channel)->registe(buffer);
    }
}

Status ExchangeSinkLocalState::get_next_available_buffer(
        vectorized::BroadcastPBlockHolder** holder) {
    DCHECK(_broadcast_pb_blocks[_broadcast_pb_block_idx].available());
    *holder = &_broadcast_pb_blocks[_broadcast_pb_block_idx];
    _broadcast_pb_block_idx++;
    return Status::OK();
}

template <typename Channels>
Status ExchangeSinkOperatorX::channel_add_rows(RuntimeState* state, Channels& channels,
                                               int num_channels,
                                               const uint64_t* __restrict channel_ids, int rows,
                                               vectorized::Block* block, bool eos) {
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

Status ExchangeSinkOperatorX::try_close(RuntimeState* state) {
    auto& local_state = state->get_sink_local_state(id())->cast<ExchangeSinkLocalState>();
    local_state._serializer.reset_block();
    Status final_st = Status::OK();
    for (int i = 0; i < local_state.channels.size(); ++i) {
        Status st = local_state.channels[i]->close(state);
        if (!st.ok() && final_st.ok()) {
            final_st = st;
        }
    }
    return final_st;
}

Status ExchangeSinkOperatorX::close(RuntimeState* state) {
    auto& local_state = state->get_sink_local_state(id())->cast<ExchangeSinkLocalState>();
    if (_closed) {
        return Status::OK();
    }
    RETURN_IF_ERROR(DataSinkOperatorX::close(state));
    local_state._sink_buffer->update_profile(local_state.profile());
    local_state._sink_buffer->close();
    return Status::OK();
}

bool ExchangeSinkOperatorX::can_write(RuntimeState* state) {
    auto& local_state = state->get_sink_local_state(id())->cast<ExchangeSinkLocalState>();
    return local_state._sink_buffer->can_write() && local_state.channel_all_can_write();
}

bool ExchangeSinkOperatorX::is_pending_finish(RuntimeState* state) const {
    auto& local_state = state->get_sink_local_state(id())->cast<ExchangeSinkLocalState>();
    return local_state._sink_buffer->is_pending_finish();
}

} // namespace doris::pipeline
