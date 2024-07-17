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

#include <memory>
#include <random>

#include "common/status.h"
#include "exchange_sink_buffer.h"
#include "pipeline/dependency.h"
#include "pipeline/exec/operator.h"
#include "pipeline/exec/sort_source_operator.h"
#include "pipeline/local_exchange/local_exchange_sink_operator.h"
#include "vec/columns/column_const.h"
#include "vec/exprs/vexpr.h"

namespace doris::pipeline {

Status ExchangeSinkLocalState::serialize_block(vectorized::Block* src, PBlock* dest,
                                               int num_receivers) {
    return _parent->cast<ExchangeSinkOperatorX>().serialize_block(*this, src, dest, num_receivers);
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
    _blocks_sent_counter = ADD_COUNTER_WITH_LEVEL(_profile, "BlocksProduced", TUnit::UNIT, 1);
    _rows_sent_counter = ADD_COUNTER_WITH_LEVEL(_profile, "RowsProduced", TUnit::UNIT, 1);
    _overall_throughput = _profile->add_derived_counter(
            "OverallThroughput", TUnit::BYTES_PER_SECOND,
            std::bind<int64_t>(&RuntimeProfile::units_per_second, _bytes_sent_counter,
                               _profile->total_time_counter()),
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
            channel_shared_ptrs.emplace_back(
                    new vectorized::PipChannel(this, p._row_desc, p._dests[i].brpc_server,
                                               fragment_instance_id, p._dest_node_id));
            fragment_id_to_channel_index.emplace(fragment_instance_id.lo,
                                                 channel_shared_ptrs.size() - 1);
            channels.push_back(channel_shared_ptrs.back().get());
        } else {
            channel_shared_ptrs.emplace_back(
                    channel_shared_ptrs[fragment_id_to_channel_index[fragment_instance_id.lo]]);
        }
    }
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());

    // Make sure brpc stub is ready before execution.
    for (int i = 0; i < channels.size(); ++i) {
        RETURN_IF_ERROR(channels[i]->init_stub(state));
    }
    return Status::OK();
}

Status ExchangeSinkLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(Base::open(state));
    auto& p = _parent->cast<ExchangeSinkOperatorX>();
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());

    int local_size = 0;
    for (int i = 0; i < channels.size(); ++i) {
        RETURN_IF_ERROR(channels[i]->open(state));
        if (channels[i]->is_local()) {
            local_size++;
        }
    }
    if (_part_type == TPartitionType::UNPARTITIONED || _part_type == TPartitionType::RANDOM ||
        _part_type == TPartitionType::TABLE_SINK_RANDOM_PARTITIONED) {
        std::random_device rd;
        std::mt19937 g(rd());
        shuffle(channels.begin(), channels.end(), g);
    }
    only_local_exchange = local_size == channels.size();

    PUniqueId id;
    id.set_hi(_state->query_id().hi);
    id.set_lo(_state->query_id().lo);
    _sink_buffer = std::make_unique<ExchangeSinkBuffer>(id, p._dest_node_id, _sender_id,
                                                        _state->be_number(), state, this);

    register_channels(_sink_buffer.get());
    _queue_dependency = Dependency::create_shared(_parent->operator_id(), _parent->node_id(),
                                                  "ExchangeSinkQueueDependency", true);
    _sink_buffer->set_dependency(_queue_dependency, _finish_dependency);
    if ((_part_type == TPartitionType::UNPARTITIONED || channels.size() == 1) &&
        !only_local_exchange) {
        _broadcast_dependency = Dependency::create_shared(
                _parent->operator_id(), _parent->node_id(), "BroadcastDependency", true);
        _sink_buffer->set_broadcast_dependency(_broadcast_dependency);
        _broadcast_pb_mem_limiter =
                vectorized::BroadcastPBlockHolderMemLimiter::create_shared(_broadcast_dependency);
        _wait_broadcast_buffer_timer =
                ADD_CHILD_TIMER(_profile, "WaitForBroadcastBuffer", timer_name);
    } else if (local_size > 0) {
        size_t dep_id = 0;
        for (auto* channel : channels) {
            if (channel->is_local()) {
                if (auto dep = channel->get_local_channel_dependency()) {
                    _local_channels_dependency.push_back(dep);
                    DCHECK(_local_channels_dependency[dep_id] != nullptr);
                    _wait_channel_timer.push_back(_profile->add_nonzero_counter(
                            fmt::format("WaitForLocalExchangeBuffer{}", dep_id), TUnit ::TIME_NS,
                            timer_name, 1));
                    dep_id++;
                } else {
                    LOG(WARNING) << "local recvr is null: query id = "
                                 << print_id(state->query_id()) << " node id = " << p.node_id();
                }
            }
        }
    }
    if (_part_type == TPartitionType::HASH_PARTITIONED) {
        _partition_count = channels.size();
        _partitioner.reset(new vectorized::Crc32HashPartitioner<vectorized::ShuffleChannelIds>(
                channels.size()));
        RETURN_IF_ERROR(_partitioner->init(p._texprs));
        RETURN_IF_ERROR(_partitioner->prepare(state, p._row_desc));
        _profile->add_info_string("Partitioner",
                                  fmt::format("Crc32HashPartitioner({})", _partition_count));
    } else if (_part_type == TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED) {
        _partition_count = channel_shared_ptrs.size();
        _partitioner.reset(new vectorized::Crc32HashPartitioner<vectorized::ShuffleChannelIds>(
                channel_shared_ptrs.size()));
        RETURN_IF_ERROR(_partitioner->init(p._texprs));
        RETURN_IF_ERROR(_partitioner->prepare(state, p._row_desc));
        _profile->add_info_string("Partitioner",
                                  fmt::format("Crc32HashPartitioner({})", _partition_count));
    } else if (_part_type == TPartitionType::TABLET_SINK_SHUFFLE_PARTITIONED) {
        _partition_count = channels.size();
        _profile->add_info_string("Partitioner",
                                  fmt::format("Crc32HashPartitioner({})", _partition_count));
        _txn_id = p._tablet_sink_txn_id;
        _schema = std::make_shared<OlapTableSchemaParam>();
        RETURN_IF_ERROR(_schema->init(p._tablet_sink_schema));
        _vpartition = std::make_unique<VOlapTablePartitionParam>(_schema, p._tablet_sink_partition);
        RETURN_IF_ERROR(_vpartition->init());
        auto find_tablet_mode = vectorized::OlapTabletFinder::FindTabletMode::FIND_TABLET_EVERY_ROW;
        _tablet_finder =
                std::make_unique<vectorized::OlapTabletFinder>(_vpartition.get(), find_tablet_mode);
        _tablet_sink_tuple_desc = _state->desc_tbl().get_tuple_descriptor(p._tablet_sink_tuple_id);
        _tablet_sink_row_desc = p._pool->add(new RowDescriptor(_tablet_sink_tuple_desc, false));
        // if _part_type == TPartitionType::TABLET_SINK_SHUFFLE_PARTITIONED, we handle the processing of auto_increment column
        // on exchange node rather than on TabletWriter
        _block_convertor =
                std::make_unique<vectorized::OlapTableBlockConvertor>(_tablet_sink_tuple_desc);
        _block_convertor->init_autoinc_info(_schema->db_id(), _schema->table_id(),
                                            _state->batch_size());
        _location = p._pool->add(new OlapTableLocationParam(p._tablet_sink_location));
        _row_distribution.init(
                {.state = _state,
                 .block_convertor = _block_convertor.get(),
                 .tablet_finder = _tablet_finder.get(),
                 .vpartition = _vpartition.get(),
                 .add_partition_request_timer = _add_partition_request_timer,
                 .txn_id = _txn_id,
                 .pool = p._pool.get(),
                 .location = _location,
                 .vec_output_expr_ctxs = &_fake_expr_ctxs,
                 .schema = _schema,
                 .caller = (void*)this,
                 .create_partition_callback = &ExchangeSinkLocalState::empty_callback_function});
    } else if (_part_type == TPartitionType::TABLE_SINK_HASH_PARTITIONED) {
        _partition_count =
                channels.size() * config::table_sink_partition_write_max_partition_nums_per_writer;
        _partitioner.reset(new vectorized::Crc32HashPartitioner<vectorized::ShuffleChannelIds>(
                _partition_count));
        _partition_function.reset(new HashPartitionFunction(_partitioner.get()));

        scale_writer_partitioning_exchanger.reset(new vectorized::ScaleWriterPartitioningExchanger<
                                                  HashPartitionFunction>(
                channels.size(), *_partition_function, _partition_count, channels.size(), 1,
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
                                  state->task_num()));

        RETURN_IF_ERROR(_partitioner->init(p._texprs));
        RETURN_IF_ERROR(_partitioner->prepare(state, p._row_desc));
        _profile->add_info_string("Partitioner",
                                  fmt::format("Crc32HashPartitioner({})", _partition_count));
    }

    _finish_dependency->block();
    if (_part_type == TPartitionType::HASH_PARTITIONED ||
        _part_type == TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED ||
        _part_type == TPartitionType::TABLE_SINK_HASH_PARTITIONED) {
        RETURN_IF_ERROR(_partitioner->open(state));
    } else if (_part_type == TPartitionType::TABLET_SINK_SHUFFLE_PARTITIONED) {
        RETURN_IF_ERROR(_row_distribution.open(_tablet_sink_row_desc));
    }
    return Status::OK();
}

Status ExchangeSinkLocalState::_send_new_partition_batch() {
    if (_row_distribution.need_deal_batching()) { // maybe try_close more than 1 time
        RETURN_IF_ERROR(_row_distribution.automatic_create_partition());
        vectorized::Block tmp_block =
                _row_distribution._batching_block->to_block(); // Borrow out, for lval ref
        auto& p = _parent->cast<ExchangeSinkOperatorX>();
        // these order is only.
        //  1. clear batching stats(and flag goes true) so that we won't make a new batching process in dealing batched block.
        //  2. deal batched block
        //  3. now reuse the column of lval block. cuz write doesn't real adjust it. it generate a new block from that.
        _row_distribution.clear_batching_stats();
        RETURN_IF_ERROR(p.sink(_state, &tmp_block, false));
        // Recovery back
        _row_distribution._batching_block->set_mutable_columns(tmp_block.mutate_columns());
        _row_distribution._batching_block->clear_column_data();
        _row_distribution._deal_batched = false;
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
        const TDataStreamSink& sink, const std::vector<TPlanFragmentDestination>& destinations)
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
          _enable_local_merge_sort(state->enable_local_merge_sort()) {
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

Status ExchangeSinkOperatorX::sink(RuntimeState* state, vectorized::Block* block, bool eos) {
    auto& local_state = get_local_state(state);
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)block->rows());
    COUNTER_UPDATE(local_state.rows_sent_counter(), (int64_t)block->rows());
    SCOPED_TIMER(local_state.exec_time_counter());
    local_state._peak_memory_usage_counter->set(local_state._mem_tracker->peak_consumption());
    bool all_receiver_eof = true;
    for (auto* channel : local_state.channels) {
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
                for (auto* channel : local_state.channels) {
                    if (!channel->is_receiver_eof()) {
                        status = channel->send_local_block(block);
                        HANDLE_CHANNEL_STATUS(state, channel, status);
                    }
                }
            }
        } else {
            auto block_holder = vectorized::BroadcastPBlockHolder::create_shared();
            {
                SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
                bool serialized = false;
                RETURN_IF_ERROR(local_state._serializer.next_serialized_block(
                        block, block_holder->get_block(), local_state.channels.size(), &serialized,
                        eos));
                if (serialized) {
                    auto cur_block = local_state._serializer.get_block()->to_block();
                    if (!cur_block.empty()) {
                        RETURN_IF_ERROR(local_state._serializer.serialize_block(
                                &cur_block, block_holder->get_block(),
                                local_state.channels.size()));
                    } else {
                        block_holder->reset_block();
                    }

                    local_state._broadcast_pb_mem_limiter->acquire(*block_holder);

                    for (auto* channel : local_state.channels) {
                        if (!channel->is_receiver_eof()) {
                            Status status;
                            if (channel->is_local()) {
                                status = channel->send_local_block(&cur_block);
                            } else {
                                SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
                                status = channel->send_broadcast_block(block_holder, eos);
                            }
                            HANDLE_CHANNEL_STATUS(state, channel, status);
                        }
                    }
                    cur_block.clear_column_data();
                    local_state._serializer.get_block()->set_mutable_columns(
                            cur_block.mutate_columns());
                }
            }
        }
    } else if (_part_type == TPartitionType::RANDOM) {
        // 1. select channel
        vectorized::PipChannel* current_channel =
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
                auto status =
                        current_channel->send_remote_block(current_channel->ch_cur_pb_block(), eos);
                HANDLE_CHANNEL_STATUS(state, current_channel, status);
                current_channel->ch_roll_pb_block();
            }
        }
        local_state.current_channel_idx =
                (local_state.current_channel_idx + 1) % local_state.channels.size();
    } else if (_part_type == TPartitionType::HASH_PARTITIONED ||
               _part_type == TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED) {
        auto rows = block->rows();
        {
            SCOPED_TIMER(local_state._split_block_hash_compute_timer);
            RETURN_IF_ERROR(
                    local_state._partitioner->do_partitioning(state, block, _mem_tracker.get()));
        }
        if (_part_type == TPartitionType::HASH_PARTITIONED) {
            RETURN_IF_ERROR(channel_add_rows(
                    state, local_state.channels, local_state._partition_count,
                    local_state._partitioner->get_channel_ids().get<uint32_t>(), rows, block, eos));
        } else {
            RETURN_IF_ERROR(channel_add_rows(
                    state, local_state.channel_shared_ptrs, local_state._partition_count,
                    local_state._partitioner->get_channel_ids().get<uint32_t>(), rows, block, eos));
        }
    } else if (_part_type == TPartitionType::TABLET_SINK_SHUFFLE_PARTITIONED) {
        // check out of limit
        RETURN_IF_ERROR(local_state._send_new_partition_batch());
        std::shared_ptr<vectorized::Block> convert_block = std::make_shared<vectorized::Block>();
        const auto& num_channels = local_state._partition_count;
        std::vector<std::vector<uint32>> channel2rows;
        channel2rows.resize(num_channels);
        auto input_rows = block->rows();

        if (input_rows > 0) {
            bool has_filtered_rows = false;
            int64_t filtered_rows = 0;
            local_state._number_input_rows += input_rows;

            RETURN_IF_ERROR(local_state._row_distribution.generate_rows_distribution(
                    *block, convert_block, filtered_rows, has_filtered_rows,
                    local_state._row_part_tablet_ids, local_state._number_input_rows));

            const auto& row_ids = local_state._row_part_tablet_ids[0].row_ids;
            const auto& tablet_ids = local_state._row_part_tablet_ids[0].tablet_ids;
            for (int idx = 0; idx < row_ids.size(); ++idx) {
                const auto& row = row_ids[idx];
                const auto& tablet_id_hash =
                        HashUtil::zlib_crc_hash(&tablet_ids[idx], sizeof(int64), 0);
                channel2rows[tablet_id_hash % num_channels].emplace_back(row);
            }
        }

        if (eos) {
            local_state._row_distribution._deal_batched = true;
            RETURN_IF_ERROR(local_state._send_new_partition_batch());
        }
        RETURN_IF_ERROR(channel_add_rows_with_idx(state, local_state.channels, num_channels,
                                                  channel2rows, convert_block.get(), eos));
    } else if (_part_type == TPartitionType::TABLE_SINK_HASH_PARTITIONED) {
        {
            SCOPED_TIMER(local_state._split_block_hash_compute_timer);
            RETURN_IF_ERROR(
                    local_state._partitioner->do_partitioning(state, block, _mem_tracker.get()));
        }
        std::vector<std::vector<uint32>> assignments =
                local_state.scale_writer_partitioning_exchanger->accept(block);
        RETURN_IF_ERROR(channel_add_rows_with_idx(
                state, local_state.channels, local_state.channels.size(), assignments, block, eos));

    } else if (_part_type == TPartitionType::TABLE_SINK_RANDOM_PARTITIONED) {
        // Control the number of channels according to the flow, thereby controlling the number of table sink writers.
        // 1. select channel
        vectorized::PipChannel* current_channel =
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
                auto status =
                        current_channel->send_remote_block(current_channel->ch_cur_pb_block(), eos);
                HANDLE_CHANNEL_STATUS(state, current_channel, status);
                current_channel->ch_roll_pb_block();
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
        for (int i = 0; i < local_state.channels.size(); ++i) {
            Status st = local_state.channels[i]->close(state, Status::OK());
            if (!st.ok() && final_st.ok()) {
                final_st = st;
            }
        }
        local_state._sink_buffer->set_should_stop();
        return final_st;
    }
    return final_st;
}

Status ExchangeSinkOperatorX::serialize_block(ExchangeSinkLocalState& state, vectorized::Block* src,
                                              PBlock* dest, int num_receivers) {
    {
        SCOPED_TIMER(state.serialize_batch_timer());
        dest->Clear();
        size_t uncompressed_bytes = 0;
        size_t compressed_bytes = 0;
        RETURN_IF_ERROR(src->serialize(_state->be_exec_version(), dest, &uncompressed_bytes,
                                       &compressed_bytes, _compression_type,
                                       _transfer_large_data_by_brpc));
        COUNTER_UPDATE(state.bytes_sent_counter(), compressed_bytes * num_receivers);
        COUNTER_UPDATE(state.uncompressed_bytes_counter(), uncompressed_bytes * num_receivers);
        COUNTER_UPDATE(state.compress_timer(), src->get_compress_time());
    }

    return Status::OK();
}

void ExchangeSinkLocalState::register_channels(pipeline::ExchangeSinkBuffer* buffer) {
    for (auto channel : channels) {
        ((vectorized::PipChannel*)channel)->register_exchange_buffer(buffer);
    }
}

template <typename Channels, typename HashValueType>
Status ExchangeSinkOperatorX::channel_add_rows(RuntimeState* state, Channels& channels,
                                               int num_channels,
                                               const HashValueType* __restrict channel_ids,
                                               int rows, vectorized::Block* block, bool eos) {
    std::vector<std::vector<uint32_t>> channel2rows;
    channel2rows.resize(num_channels);
    for (uint32_t i = 0; i < rows; i++) {
        channel2rows[channel_ids[i]].emplace_back(i);
    }

    RETURN_IF_ERROR(
            channel_add_rows_with_idx(state, channels, num_channels, channel2rows, block, eos));
    return Status::OK();
}

template <typename Channels>
Status ExchangeSinkOperatorX::channel_add_rows_with_idx(
        RuntimeState* state, Channels& channels, int num_channels,
        std::vector<std::vector<uint32_t>>& channel2rows, vectorized::Block* block, bool eos) {
    Status status = Status::OK();
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

std::string ExchangeSinkLocalState::debug_string(int indentation_level) const {
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer, "{}", Base::debug_string(indentation_level));
    fmt::format_to(debug_string_buffer,
                   ", Sink Buffer: (_should_stop = {}, _busy_channels = {}, _is_finishing = {}), "
                   "_reach_limit: {}",
                   _sink_buffer->_should_stop.load(), _sink_buffer->_busy_channels.load(),
                   _sink_buffer->_is_finishing.load(), _reach_limit.load());
    return fmt::to_string(debug_string_buffer);
}

Status ExchangeSinkLocalState::close(RuntimeState* state, Status exec_status) {
    if (_closed) {
        return Status::OK();
    }
    if (_part_type == TPartitionType::TABLET_SINK_SHUFFLE_PARTITIONED &&
        _block_convertor != nullptr && _tablet_finder != nullptr) {
        _state->update_num_rows_load_filtered(_block_convertor->num_filtered_rows() +
                                              _tablet_finder->num_filtered_rows());
        _state->update_num_rows_load_unselected(
                _tablet_finder->num_immutable_partition_filtered_rows());
        // sink won't see those filtered rows, we should compensate here
        _state->set_num_rows_load_total(_state->num_rows_load_filtered() +
                                        _state->num_rows_load_unselected());
    }
    SCOPED_TIMER(exec_time_counter());
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
    if (_child_x && _enable_local_merge_sort) {
        // SORT_OPERATOR -> DATA_STREAM_SINK_OPERATOR
        // SORT_OPERATOR -> LOCAL_MERGE_SORT -> DATA_STREAM_SINK_OPERATOR
        if (auto sort_source = std::dynamic_pointer_cast<SortSourceOperatorX>(_child_x);
            sort_source && sort_source->use_local_merge()) {
            // Sort the data local
            return ExchangeType::LOCAL_MERGE_SORT;
        }
    }
    return DataSinkOperatorX<ExchangeSinkLocalState>::required_data_distribution();
}

} // namespace doris::pipeline
