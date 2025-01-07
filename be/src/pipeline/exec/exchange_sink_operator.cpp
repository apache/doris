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
#include "vec/sink/scale_writer_partitioning_exchanger.hpp"
#include "vec/sink/tablet_sink_hash_partitioner.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"

inline std::string get_partition_type_name(TPartitionType::type part_type) {
    std::map<int, const char*>::const_iterator i;
    i = _TPartitionType_VALUES_TO_NAMES.find(part_type);

    if (i != _TPartitionType_VALUES_TO_NAMES.end()) {
        return i->second;
    }

    return "Unknown partition type";
}

bool ExchangeSinkLocalState::transfer_large_data_by_brpc() const {
    return _parent->cast<ExchangeSinkOperatorX>()._transfer_large_data_by_brpc;
}

static const std::string timer_name = "WaitForDependencyTime";

Status ExchangeSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _sender_id = info.sender_id;

    _enqueue_blocks_counter = ADD_COUNTER(_profile, "EnqueueRows", TUnit::UNIT);
    _dequeue_blocks_counter = ADD_COUNTER(_profile, "DequeueRows", TUnit::UNIT);
    _get_block_failed_counter = ADD_COUNTER(_profile, "GetBlockFailedTime", TUnit::UNIT);
    _bytes_sent_counter = ADD_COUNTER(_profile, "BytesSent", TUnit::BYTES);
    _uncompressed_bytes_counter = ADD_COUNTER(_profile, "UncompressedRowBatchSize", TUnit::BYTES);
    _local_sent_rows = ADD_COUNTER(_profile, "LocalSentRows", TUnit::UNIT);
    _serialize_batch_timer = ADD_TIMER(_profile, "SerializeBatchTime");
    _copy_shuffled_data_timer = ADD_TIMER(_profile, "CopyShuffledDataTime");
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

    _test_timer1 = ADD_TIMER(_profile, "TestTime1");
    _test_timer2 = ADD_TIMER(_profile, "TestTime2");
    _test_timer3 = ADD_TIMER(_profile, "TestTime3");
    _test_timer4 = ADD_TIMER(_profile, "TestTime4");
    _test_timer5 = ADD_TIMER(_profile, "TestTime5");
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

    int num_dests = cast_set<int>(p._dests.size());
    const int free_block_limit = 0;
    if (num_dests == 1 || _part_type == TPartitionType::UNPARTITIONED) {
        _exchanger = BroadcastExchanger<SerializedBlockQueue<BroadcastBlock>>::create_shared(
                1, num_dests, free_block_limit);
        _channel_selector = std::make_unique<AllChannelsSelector>(channels.size());
    }
    switch (_part_type) {
    case TPartitionType::HASH_PARTITIONED:
    case TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED:
        _partition_count = channels.size();
        _partitioner =
                vectorized::Crc32HashPartitioner<vectorized::ShuffleChannelIds>::create_unique(
                        _partition_count);
        if (_partition_count != 1) {
            _exchanger = ShuffleExchanger<SerializedBlockQueue<PartitionedBlock>>::create_shared(
                    1, _partition_count, _partition_count, free_block_limit);
            _channel_selector = std::make_unique<SelectedChannelsSelector>(_partition_count);
        }
        break;
    case TPartitionType::TABLET_SINK_SHUFFLE_PARTITIONED:
        _partition_count = channels.size();
        _partitioner = vectorized::TabletSinkHashPartitioner::create_unique(
                _partition_count, p._tablet_sink_txn_id, p._tablet_sink_schema,
                p._tablet_sink_partition, p._tablet_sink_location, p._tablet_sink_tuple_id, this);
        if (num_dests != 1) {
            _exchanger = ShuffleExchanger<SerializedBlockQueue<PartitionedBlock>>::create_shared(
                    1, num_dests, num_dests, free_block_limit);
            _channel_selector = std::make_unique<SelectedChannelsSelector>(channels.size());
        }
        break;
    case TPartitionType::TABLE_SINK_HASH_PARTITIONED:
        _partition_count =
                channels.size() * config::table_sink_partition_write_max_partition_nums_per_writer;
        _partitioner = vectorized::ScaleWriterPartitioner::create_unique(
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
        if (num_dests != 1) {
            _exchanger = ShuffleExchanger<SerializedBlockQueue<PartitionedBlock>>::create_shared(
                    1, num_dests, num_dests, free_block_limit);
            _channel_selector = std::make_unique<SelectedChannelsSelector>(channels.size());
        }
        break;
    case TPartitionType::UNPARTITIONED:
        break;
    case TPartitionType::RANDOM:
        if (num_dests != 1) {
            _channel_selector = std::make_unique<RoundRobinSelector>(channels.size());
            _exchanger =
                    PassthroughExchanger<SerializedBlockQueue<BlockWrapperSPtr>>::create_shared(
                            1, num_dests, free_block_limit);
            _could_be_moved = local_size > 0;
        }
        break;
    case TPartitionType::TABLE_SINK_RANDOM_PARTITIONED:
        if (num_dests != 1) {
            _channel_selector = std::make_unique<TableSinkRandomSelector>(channels.size());
            _exchanger =
                    PassthroughExchanger<SerializedBlockQueue<BlockWrapperSPtr>>::create_shared(
                            1, num_dests, free_block_limit);
            _could_be_moved = local_size > 0;
        }
        break;
    default:
        return Status::InternalError("Unsupported exchange type : " +
                                     std::to_string((int)_part_type));
    }
    _profile->add_info_string("PartitionType", get_partition_type_name(_part_type));
    _profile->add_info_string("Exchanger", _exchanger->debug_string());
    if (_partitioner) {
        _profile->add_info_string("Partitioner", _partitioner->debug_string());
        RETURN_IF_ERROR(_partitioner->init(p._texprs));
        RETURN_IF_ERROR(_partitioner->prepare(state, p._row_desc));
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
    auto& p = _parent->cast<ExchangeSinkOperatorX>();

    if (_part_type == TPartitionType::UNPARTITIONED || _part_type == TPartitionType::RANDOM ||
        _part_type == TPartitionType::TABLE_SINK_RANDOM_PARTITIONED) {
        std::random_device rd;
        std::mt19937 g(rd());
        shuffle(channels.begin(), channels.end(), g);
    }
    for (int i = 0; i < channels.size(); ++i) {
        RETURN_IF_ERROR(
                channels[i]->open(state, _exchanger->get_type() == ExchangeType::HASH_SHUFFLE));
    }

    PUniqueId id;
    id.set_hi(_state->query_id().hi);
    id.set_lo(_state->query_id().lo);

    _broadcast_dependency = Dependency::create_shared(_parent->operator_id(), _parent->node_id(),
                                                      "BroadcastDependency", true);
    _broadcast_pb_mem_limiter =
            vectorized::BroadcastPBlockHolderMemLimiter::create_shared(_broadcast_dependency);
    if (!only_local_exchange) {
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

    if (_partitioner) {
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
    if (_part_type == TPartitionType::RANGE_PARTITIONED ||
        _part_type == TPartitionType::LIST_PARTITIONED) {
        return Status::InternalError("TPartitionType::RANGE_PARTITIONED should not be used");
    }
    if (_part_type == TPartitionType::TABLET_SINK_SHUFFLE_PARTITIONED) {
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(*_t_tablet_sink_exprs,
                                                             _tablet_sink_expr_ctxs));
    }
    _writer.reset(new Writer());
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

    auto block_holder = vectorized::BroadcastPBlockHolder::create_shared();
    bool should_output = !block->empty() || eos;
    size_t data_size =
            _part_type == TPartitionType::TABLE_SINK_RANDOM_PARTITIONED ? block->bytes() : 0;
    if (is_broadcast()) {
        // For broadcast shuffling, we do accumulate a full data block in `local_state._serializer`.
        RETURN_IF_ERROR(local_state._serializer.next_serialized_block(block, &should_output, eos));
        if (should_output) {
            local_state._serializer.swap_data(block);
            if (!local_state.only_local_exchange) {
                RETURN_IF_ERROR(local_state._serializer.serialize_block(
                        block, block_holder->get_block(), local_state._rpc_channels_num));
            }
        }
        local_state._broadcast_pb_mem_limiter->acquire(*block_holder);
    }
    if (!should_output) {
        return Status::OK();
    }
    local_state._channel_selector->process_next_block(data_size);
    RETURN_IF_ERROR(_writer->write(&local_state, state, block, eos));

    {
        SCOPED_TIMER(local_state._test_timer1);
        bool moved = local_state._could_be_moved;
        auto& channel_ids = local_state._channel_selector->next_channel_ids();
        for (auto channel_id : channel_ids) {
            moved = moved && local_state.channels[channel_id]->is_local();
        }
        if (moved) {
            *block = block->clone_empty();
        }
        for (auto channel_id : channel_ids) {
            RETURN_IF_ERROR(_writer->send_to_channels(
                    &local_state, state, local_state.channels[channel_id].get(), channel_id, eos,
                    is_broadcast() ? block_holder : nullptr, local_state._test_timer2,
                    local_state._test_timer3, local_state._test_timer4, local_state._test_timer5));
        }
    }

    Status final_st = Status::OK();
    int channel_id = 0;
    if (eos) {
        DCHECK(local_state._serializer.get_block() == nullptr ||
               local_state._serializer.get_block()->empty())
                << " node id: " << node_id() << " remain rows"
                << local_state._serializer.get_block()->rows();
        local_state._serializer.reset_block();
        for (auto& channel : local_state.channels) {
            Status st = _writer->finish(local_state.exchanger(), state, channel.get(), channel_id,
                                        Status::OK());
            if (!st.ok() && final_st.ok()) {
                final_st = st;
            }
            channel_id++;
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
    if (!exec_status.ok()) {
        Status final_st = Status::OK();
        int channel_id = 0;
        for (auto& channel : channels) {
            Status st = _parent->cast<ExchangeSinkOperatorX>()._writer->finish(
                    exchanger(), state, channel.get(), channel_id, exec_status);
            if (!st.ok() && final_st.ok()) {
                final_st = st;
            }
            channel_id++;
        }
        RETURN_IF_ERROR(final_st);
    }
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
