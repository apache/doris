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
#include <gen_cpp/Partitions_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/types.pb.h>

#include <memory>
#include <mutex>
#include <random>

#include "common/status.h"
#include "exchange_sink_buffer.h"
#include "pipeline/dependency.h"
#include "pipeline/exec/operator.h"
#include "pipeline/exec/sort_source_operator.h"
#include "pipeline/local_exchange/local_exchange_sink_operator.h"
#include "pipeline/pipeline_fragment_context.h"
#include "util/runtime_profile.h"
#include "util/uid_util.h"
#include "vec/columns/column_const.h"
#include "vec/exprs/vexpr.h"
#include "vec/sink/tablet_sink_hash_partitioner.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"
bool ExchangeSinkLocalState::transfer_large_data_by_brpc() const {
    return _parent->cast<ExchangeSinkOperatorX>()._transfer_large_data_by_brpc;
}

static const std::string timer_name = "WaitForDependencyTime";

Status ExchangeSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _sender_id = info.sender_id;

    _bytes_sent_counter = ADD_COUNTER(_profile, "BytesSent", TUnit::BYTES);
    _uncompressed_bytes_counter = ADD_COUNTER(_profile, "UncompressedRowBatchSize", TUnit::BYTES);
    _local_sent_rows = ADD_COUNTER(_profile, "LocalSentRows", TUnit::UNIT);
    _serialize_batch_timer = ADD_TIMER(_profile, "SerializeBatchTime");
    _compress_timer = ADD_TIMER(_profile, "CompressTime");
    _local_send_timer = ADD_TIMER(_profile, "LocalSendTime");
    _split_block_hash_compute_timer = ADD_TIMER(_profile, "SplitBlockHashComputeTime");
    _distribute_rows_into_channels_timer = ADD_TIMER(_profile, "DistributeRowsIntoChannelsTime");
    _send_new_partition_timer = ADD_TIMER(_profile, "SendNewPartitionTime");
    _blocks_sent_counter = ADD_COUNTER_WITH_LEVEL(_profile, "BlocksProduced", TUnit::UNIT, 1);
    _overall_throughput = _profile->add_derived_counter(
            "OverallThroughput", TUnit::BYTES_PER_SECOND,
            [this]() {
                return RuntimeProfile::units_per_second(_bytes_sent_counter,
                                                        _profile->total_time_counter());
            },
            "");
    _merge_block_timer = ADD_TIMER(profile(), "MergeBlockTime");
    _local_bytes_send_counter = ADD_COUNTER(_profile, "LocalBytesSent", TUnit::BYTES);
    _wait_for_dependency_timer = ADD_TIMER_WITH_LEVEL(_profile, timer_name, 1);
    _wait_queue_timer =
            ADD_CHILD_TIMER_WITH_LEVEL(_profile, "WaitForRpcBufferQueue", timer_name, 1);

    auto& p = _parent->cast<ExchangeSinkOperatorX>();
    _part_type = p._part_type;
    std::map<int64_t, int64_t> fragment_id_to_channel_index;
    for (int i = 0; i < p._dests.size(); ++i) {
        const auto& fragment_instance_id = p._dests[i].fragment_instance_id;
        if (fragment_id_to_channel_index.find(fragment_instance_id.lo) ==
            fragment_id_to_channel_index.end()) {
            channels.push_back(std::make_shared<vectorized::Channel>(
                    this, p._dests[i].brpc_server, fragment_instance_id, p._dest_node_id));
            fragment_id_to_channel_index.emplace(fragment_instance_id.lo, channels.size() - 1);

            if (fragment_instance_id.hi != -1 && fragment_instance_id.lo != -1) {
                _working_channels_count++;
            }
        } else {
            channels.emplace_back(channels[fragment_id_to_channel_index[fragment_instance_id.lo]]);
        }
    }

    // Make sure brpc stub is ready before execution.
    for (int i = 0; i < channels.size(); ++i) {
        RETURN_IF_ERROR(channels[i]->init(state));
        _wait_channel_timer.push_back(_profile->add_nonzero_counter(
                fmt::format("WaitForLocalExchangeBuffer{}", i), TUnit ::TIME_NS, timer_name, 1));
    }
    _wait_broadcast_buffer_timer = ADD_CHILD_TIMER(_profile, "WaitForBroadcastBuffer", timer_name);

    size_t local_size = 0;
    for (int i = 0; i < channels.size(); ++i) {
        if (channels[i]->is_local()) {
            local_size++;
            _last_local_channel_idx = i;
        }
    }
    only_local_exchange = local_size == channels.size();
    _rpc_channels_num = channels.size() - local_size;

    if (!only_local_exchange) {
        _sink_buffer = p.get_sink_buffer(state->fragment_instance_id().lo);
        register_channels(_sink_buffer.get());
        _queue_dependency = Dependency::create_shared(_parent->operator_id(), _parent->node_id(),
                                                      "ExchangeSinkQueueDependency", true);
        _sink_buffer->set_dependency(state->fragment_instance_id().lo, _queue_dependency, this);
    }

    if (_part_type == TPartitionType::HASH_PARTITIONED) {
        _partition_count = channels.size();
        _partitioner =
                std::make_unique<vectorized::Crc32HashPartitioner<vectorized::ShuffleChannelIds>>(
                        channels.size());
        RETURN_IF_ERROR(_partitioner->init(p._texprs));
        RETURN_IF_ERROR(_partitioner->prepare(state, p._row_desc));
        _profile->add_info_string("Partitioner",
                                  fmt::format("Crc32HashPartitioner({})", _partition_count));
    } else if (_part_type == TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED) {
        _partition_count = channels.size();
        _partitioner =
                std::make_unique<vectorized::Crc32HashPartitioner<vectorized::ShuffleChannelIds>>(
                        channels.size());
        RETURN_IF_ERROR(_partitioner->init(p._texprs));
        RETURN_IF_ERROR(_partitioner->prepare(state, p._row_desc));
        _profile->add_info_string("Partitioner",
                                  fmt::format("Crc32HashPartitioner({})", _partition_count));
    } else if (_part_type == TPartitionType::TABLET_SINK_SHUFFLE_PARTITIONED) {
        _partition_count = channels.size();
        _profile->add_info_string("Partitioner",
                                  fmt::format("Crc32HashPartitioner({})", _partition_count));
        _partitioner = std::make_unique<vectorized::TabletSinkHashPartitioner>(
                _partition_count, p._tablet_sink_txn_id, p._tablet_sink_schema,
                p._tablet_sink_partition, p._tablet_sink_location, p._tablet_sink_tuple_id, this);
        RETURN_IF_ERROR(_partitioner->init({}));
        RETURN_IF_ERROR(_partitioner->prepare(state, {}));
    } else if (_part_type == TPartitionType::TABLE_SINK_HASH_PARTITIONED) {
        _partition_count =
                channels.size() * config::table_sink_partition_write_max_partition_nums_per_writer;
        _partitioner = std::make_unique<vectorized::ScaleWriterPartitioner>(
                channels.size(), _partition_count, channels.size(), 1,
                config::table_sink_partition_write_min_partition_data_processed_rebalance_threshold /
                                        state->task_num() ==
                                0
                        ? config::table_sink_partition_write_min_partition_data_processed_rebalance_threshold
                        : config::table_sink_partition_write_min_partition_data_processed_rebalance_threshold /
                                  state->task_num(),
                config::table_sink_partition_write_min_data_processed_rebalance_threshold /
                                        state->task_num() ==
                                0
                        ? config::table_sink_partition_write_min_data_processed_rebalance_threshold
                        : config::table_sink_partition_write_min_data_processed_rebalance_threshold /
                                  state->task_num());

        RETURN_IF_ERROR(_partitioner->init(p._texprs));
        RETURN_IF_ERROR(_partitioner->prepare(state, p._row_desc));
        _profile->add_info_string("Partitioner",
                                  fmt::format("Crc32HashPartitioner({})", _partition_count));
    }

    return Status::OK();
}

void ExchangeSinkLocalState::on_channel_finished(InstanceLoId channel_id) {
    std::lock_guard<std::mutex> lock(_finished_channels_mutex);

    if (_finished_channels.contains(channel_id)) {
        LOG(WARNING) << "query: " << print_id(_state->query_id())
                     << ", on_channel_finished on already finished channel: " << channel_id;
        return;
    } else {
        _finished_channels.emplace(channel_id);
        if (_working_channels_count.fetch_sub(1) == 1) {
            set_reach_limit();
            if (_finish_dependency) {
                _finish_dependency->set_ready();
            }
        }
    }
}

Status ExchangeSinkLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(Base::open(state));
    _writer.reset(new Writer());
    auto& p = _parent->cast<ExchangeSinkOperatorX>();

    if (_part_type == TPartitionType::UNPARTITIONED || _part_type == TPartitionType::RANDOM ||
        _part_type == TPartitionType::TABLE_SINK_RANDOM_PARTITIONED) {
        std::random_device rd;
        std::mt19937 g(rd());
        shuffle(channels.begin(), channels.end(), g);
    }
    for (int i = 0; i < channels.size(); ++i) {
        RETURN_IF_ERROR(channels[i]->open(state));
        if (channels[i]->is_local()) {
            _last_local_channel_idx = i;
        }
    }

    PUniqueId id;
    id.set_hi(_state->query_id().hi);
    id.set_lo(_state->query_id().lo);

    if ((_part_type == TPartitionType::UNPARTITIONED || channels.size() == 1) &&
        !only_local_exchange) {
        _broadcast_dependency = Dependency::create_shared(
                _parent->operator_id(), _parent->node_id(), "BroadcastDependency", true);
        _broadcast_pb_mem_limiter =
                vectorized::BroadcastPBlockHolderMemLimiter::create_shared(_broadcast_dependency);
    } else if (!only_local_exchange) {
        size_t dep_id = 0;
        for (auto& channel : channels) {
            if (channel->is_local()) {
                if (auto dep = channel->get_local_channel_dependency()) {
                    _local_channels_dependency.push_back(dep);
                    DCHECK(_local_channels_dependency[dep_id] != nullptr);
                    dep_id++;
                } else {
                    LOG(WARNING) << "local recvr is null: query id = "
                                 << print_id(state->query_id()) << " node id = " << p.node_id();
                }
            }
        }
    }

    if (_part_type == TPartitionType::HASH_PARTITIONED ||
        _part_type == TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED ||
        _part_type == TPartitionType::TABLE_SINK_HASH_PARTITIONED ||
        _part_type == TPartitionType::TABLET_SINK_SHUFFLE_PARTITIONED) {
        RETURN_IF_ERROR(_partitioner->open(state));
    }
    return Status::OK();
}

std::string ExchangeSinkLocalState::name_suffix() {
    std::string name = " (id=" + std::to_string(_parent->node_id());
    auto& p = _parent->cast<ExchangeSinkOperatorX>();
    name += ",dst_id=" + std::to_string(p._dest_node_id);
    name += ")";
    return name;
}

segment_v2::CompressionTypePB ExchangeSinkLocalState::compression_type() const {
    return _parent->cast<ExchangeSinkOperatorX>()._compression_type;
}

ExchangeSinkOperatorX::ExchangeSinkOperatorX(
        RuntimeState* state, const RowDescriptor& row_desc, int operator_id,
        const TDataStreamSink& sink, const std::vector<TPlanFragmentDestination>& destinations,
        const std::vector<TUniqueId>& fragment_instance_ids)
        : DataSinkOperatorX(operator_id, sink.dest_node_id),
          _texprs(sink.output_partition.partition_exprs),
          _row_desc(row_desc),
          _part_type(sink.output_partition.type),
          _dests(destinations),
          _dest_node_id(sink.dest_node_id),
          _transfer_large_data_by_brpc(config::transfer_large_data_by_brpc),
          _tablet_sink_schema(sink.tablet_sink_schema),
          _tablet_sink_partition(sink.tablet_sink_partition),
          _tablet_sink_location(sink.tablet_sink_location),
          _tablet_sink_tuple_id(sink.tablet_sink_tuple_id),
          _tablet_sink_txn_id(sink.tablet_sink_txn_id),
          _t_tablet_sink_exprs(&sink.tablet_sink_exprs),
          _enable_local_merge_sort(state->enable_local_merge_sort()),
          _dest_is_merge(sink.__isset.is_merge && sink.is_merge),
          _fragment_instance_ids(fragment_instance_ids) {
    DCHECK_GT(destinations.size(), 0);
    DCHECK(sink.output_partition.type == TPartitionType::UNPARTITIONED ||
           sink.output_partition.type == TPartitionType::HASH_PARTITIONED ||
           sink.output_partition.type == TPartitionType::RANDOM ||
           sink.output_partition.type == TPartitionType::RANGE_PARTITIONED ||
           sink.output_partition.type == TPartitionType::TABLET_SINK_SHUFFLE_PARTITIONED ||
           sink.output_partition.type == TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED ||
           sink.output_partition.type == TPartitionType::TABLE_SINK_HASH_PARTITIONED ||
           sink.output_partition.type == TPartitionType::TABLE_SINK_RANDOM_PARTITIONED);
    _name = "ExchangeSinkOperatorX";
    _pool = std::make_shared<ObjectPool>();
    if (sink.__isset.output_tuple_id) {
        _output_tuple_id = sink.output_tuple_id;
    }
}

Status ExchangeSinkOperatorX::init(const TDataSink& tsink) {
    RETURN_IF_ERROR(DataSinkOperatorX::init(tsink));
    if (_part_type == TPartitionType::RANGE_PARTITIONED) {
        return Status::InternalError("TPartitionType::RANGE_PARTITIONED should not be used");
    }
    if (_part_type == TPartitionType::TABLET_SINK_SHUFFLE_PARTITIONED) {
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(*_t_tablet_sink_exprs,
                                                             _tablet_sink_expr_ctxs));
    }
    return Status::OK();
}

Status ExchangeSinkOperatorX::open(RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX<ExchangeSinkLocalState>::open(state));
    _state = state;
    _compression_type = state->fragement_transmission_compression_type();
    if (_part_type == TPartitionType::TABLET_SINK_SHUFFLE_PARTITIONED) {
        if (_output_tuple_id == -1) {
            RETURN_IF_ERROR(
                    vectorized::VExpr::prepare(_tablet_sink_expr_ctxs, state, _child->row_desc()));
        } else {
            auto* output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_output_tuple_id);
            auto* output_row_desc = _pool->add(new RowDescriptor(output_tuple_desc, false));
            RETURN_IF_ERROR(
                    vectorized::VExpr::prepare(_tablet_sink_expr_ctxs, state, *output_row_desc));
        }
        RETURN_IF_ERROR(vectorized::VExpr::open(_tablet_sink_expr_ctxs, state));
    }
    std::vector<InstanceLoId> ins_ids;
    for (auto fragment_instance_id : _fragment_instance_ids) {
        ins_ids.push_back(fragment_instance_id.lo);
    }
    _sink_buffer = _create_buffer(ins_ids);
    return Status::OK();
}

template <typename ChannelPtrType>
void ExchangeSinkOperatorX::_handle_eof_channel(RuntimeState* state, ChannelPtrType channel,
                                                Status st) {
    channel->set_receiver_eof(st);
    // Chanel will not send RPC to the downstream when eof, so close chanel by OK status.
    static_cast<void>(channel->close(state));
}

Status ExchangeSinkOperatorX::sink(RuntimeState* state, vectorized::Block* block, bool eos) {
    auto& local_state = get_local_state(state);
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)block->rows());
    SCOPED_TIMER(local_state.exec_time_counter());
    bool all_receiver_eof = true;
    for (auto& channel : local_state.channels) {
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
                size_t idx = 0;
                for (auto& channel : local_state.channels) {
                    if (!channel->is_receiver_eof()) {
                        // If this channel is the last, we can move this block to downstream pipeline.
                        // Otherwise, this block also need to be broadcasted to other channels so should be copied.
                        DCHECK_GE(local_state._last_local_channel_idx, 0);
                        status = channel->send_local_block(
                                block, eos, idx == local_state._last_local_channel_idx);
                        HANDLE_CHANNEL_STATUS(state, channel, status);
                    }
                    idx++;
                }
            }
        } else {
            auto block_holder = vectorized::BroadcastPBlockHolder::create_shared();
            {
                bool serialized = false;
                RETURN_IF_ERROR(local_state._serializer.next_serialized_block(
                        block, block_holder->get_block(), local_state._rpc_channels_num,
                        &serialized, eos));
                if (serialized) {
                    auto cur_block = local_state._serializer.get_block()->to_block();
                    if (!cur_block.empty()) {
                        DCHECK(eos || local_state._serializer.is_local()) << debug_string(state, 0);
                        RETURN_IF_ERROR(local_state._serializer.serialize_block(
                                &cur_block, block_holder->get_block(),
                                local_state._rpc_channels_num));
                    } else {
                        block_holder->reset_block();
                    }

                    local_state._broadcast_pb_mem_limiter->acquire(*block_holder);

                    size_t idx = 0;
                    bool moved = false;
                    for (auto& channel : local_state.channels) {
                        if (!channel->is_receiver_eof()) {
                            Status status;
                            if (channel->is_local()) {
                                // If this channel is the last, we can move this block to downstream pipeline.
                                // Otherwise, this block also need to be broadcasted to other channels so should be copied.
                                DCHECK_GE(local_state._last_local_channel_idx, 0);
                                status = channel->send_local_block(
                                        &cur_block, eos,
                                        idx == local_state._last_local_channel_idx);
                                moved = idx == local_state._last_local_channel_idx;
                            } else {
                                status = channel->send_broadcast_block(block_holder, eos);
                            }
                            HANDLE_CHANNEL_STATUS(state, channel, status);
                        }
                        idx++;
                    }
                    if (moved) {
                        local_state._serializer.reset_block();
                    } else {
                        cur_block.clear_column_data();
                        local_state._serializer.get_block()->set_mutable_columns(
                                cur_block.mutate_columns());
                    }
                }
            }
        }
    } else if (_part_type == TPartitionType::RANDOM) {
        // 1. select channel
        auto& current_channel = local_state.channels[local_state.current_channel_idx];
        if (!current_channel->is_receiver_eof()) {
            // 2. serialize, send and rollover block
            if (current_channel->is_local()) {
                auto status = current_channel->send_local_block(block, eos, true);
                HANDLE_CHANNEL_STATUS(state, current_channel, status);
            } else {
                auto pblock = std::make_unique<PBlock>();
                RETURN_IF_ERROR(local_state._serializer.serialize_block(block, pblock.get()));
                auto status = current_channel->send_remote_block(std::move(pblock), eos);
                HANDLE_CHANNEL_STATUS(state, current_channel, status);
            }
        }
        local_state.current_channel_idx =
                (local_state.current_channel_idx + 1) % local_state.channels.size();
    } else if (_part_type == TPartitionType::HASH_PARTITIONED ||
               _part_type == TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED ||
               _part_type == TPartitionType::TABLET_SINK_SHUFFLE_PARTITIONED ||
               _part_type == TPartitionType::TABLE_SINK_HASH_PARTITIONED) {
        RETURN_IF_ERROR(local_state._writer->write(&local_state, state, block, eos));
    } else if (_part_type == TPartitionType::TABLE_SINK_RANDOM_PARTITIONED) {
        // Control the number of channels according to the flow, thereby controlling the number of table sink writers.
        // 1. select channel
        auto& current_channel = local_state.channels[local_state.current_channel_idx];
        if (!current_channel->is_receiver_eof()) {
            // 2. serialize, send and rollover block
            if (current_channel->is_local()) {
                auto status = current_channel->send_local_block(block, eos, true);
                HANDLE_CHANNEL_STATUS(state, current_channel, status);
            } else {
                auto pblock = std::make_unique<PBlock>();
                RETURN_IF_ERROR(local_state._serializer.serialize_block(block, pblock.get()));
                auto status = current_channel->send_remote_block(std::move(pblock), eos);
                HANDLE_CHANNEL_STATUS(state, current_channel, status);
            }
            _data_processed += block->bytes();
        }

        if (_writer_count < local_state.channels.size()) {
            if (_data_processed >=
                _writer_count *
                        config::table_sink_non_partition_write_scaling_data_processed_threshold) {
                _writer_count++;
            }
        }
        local_state.current_channel_idx = (local_state.current_channel_idx + 1) % _writer_count;
    } else {
        // Range partition
        // 1. calculate range
        // 2. dispatch rows to channel
    }

    Status final_st = Status::OK();
    if (eos) {
        local_state._serializer.reset_block();
        for (auto& channel : local_state.channels) {
            Status st = channel->close(state);
            if (!st.ok() && final_st.ok()) {
                final_st = st;
            }
        }
    }
    return final_st;
}

void ExchangeSinkLocalState::register_channels(pipeline::ExchangeSinkBuffer* buffer) {
    for (auto& channel : channels) {
        channel->set_exchange_buffer(buffer);
    }
}

std::string ExchangeSinkLocalState::debug_string(int indentation_level) const {
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer, "{}", Base::debug_string(indentation_level));
    if (_sink_buffer) {
        fmt::format_to(debug_string_buffer,
                       ", Sink Buffer: (_is_finishing = {}, blocks in queue: {}, queue capacity: "
                       "{}, queue dep: {}), _reach_limit: {}, working channels: {}",
                       _sink_buffer->_is_failed.load(), _sink_buffer->_total_queue_size,
                       _sink_buffer->_queue_capacity, (void*)_queue_dependency.get(),
                       _reach_limit.load(), _working_channels_count.load());
    }
    return fmt::to_string(debug_string_buffer);
}

Status ExchangeSinkLocalState::close(RuntimeState* state, Status exec_status) {
    if (_closed) {
        return Status::OK();
    }
    SCOPED_TIMER(exec_time_counter());
    if (_partitioner) {
        RETURN_IF_ERROR(_partitioner->close(state));
    }
    SCOPED_TIMER(_close_timer);
    if (_queue_dependency) {
        COUNTER_UPDATE(_wait_queue_timer, _queue_dependency->watcher_elapse_time());
    }

    COUNTER_SET(_wait_for_finish_dependency_timer, _finish_dependency->watcher_elapse_time());
    if (_broadcast_dependency) {
        COUNTER_UPDATE(_wait_broadcast_buffer_timer, _broadcast_dependency->watcher_elapse_time());
    }
    for (size_t i = 0; i < _local_channels_dependency.size(); i++) {
        COUNTER_UPDATE(_wait_channel_timer[i],
                       _local_channels_dependency[i]->watcher_elapse_time());
    }
    if (_sink_buffer) {
        _sink_buffer->update_profile(profile());
        _sink_buffer->close();
    }
    return Base::close(state, exec_status);
}

DataDistribution ExchangeSinkOperatorX::required_data_distribution() const {
    if (_child && _enable_local_merge_sort) {
        // SORT_OPERATOR -> DATA_STREAM_SINK_OPERATOR
        // SORT_OPERATOR -> LOCAL_MERGE_SORT -> DATA_STREAM_SINK_OPERATOR
        if (auto sort_source = std::dynamic_pointer_cast<SortSourceOperatorX>(_child);
            sort_source && sort_source->use_local_merge()) {
            // Sort the data local
            return ExchangeType::LOCAL_MERGE_SORT;
        }
    }
    return DataSinkOperatorX<ExchangeSinkLocalState>::required_data_distribution();
}

std::shared_ptr<ExchangeSinkBuffer> ExchangeSinkOperatorX::_create_buffer(
        const std::vector<InstanceLoId>& sender_ins_ids) {
    PUniqueId id;
    id.set_hi(_state->query_id().hi);
    id.set_lo(_state->query_id().lo);
    auto sink_buffer =
            std::make_unique<ExchangeSinkBuffer>(id, _dest_node_id, state(), sender_ins_ids);
    for (const auto& _dest : _dests) {
        sink_buffer->construct_request(_dest.fragment_instance_id);
    }
    return sink_buffer;
}

// For a normal shuffle scenario, if the concurrency is n,
// there can be up to n * n RPCs in the current fragment.
// Therefore, a shared sink buffer is used here to limit the number of concurrent RPCs.
// (Note: This does not reduce the total number of RPCs.)
// In a merge sort scenario, there are only n RPCs, so a shared sink buffer is not needed.
std::shared_ptr<ExchangeSinkBuffer> ExchangeSinkOperatorX::get_sink_buffer(
        InstanceLoId sender_ins_id) {
    // When the child is SortSourceOperatorX or LocalExchangeSourceOperatorX,
    // it is an order-by scenario.
    // In this case, there is only one target instance, and no n * n RPC concurrency will occur.
    // Therefore, sharing a sink buffer is not necessary.
    if (_dest_is_merge) {
        return _create_buffer({sender_ins_id});
    }
    if (_state->enable_shared_exchange_sink_buffer()) {
        return _sink_buffer;
    }
    return _create_buffer({sender_ins_id});
}
} // namespace doris::pipeline
