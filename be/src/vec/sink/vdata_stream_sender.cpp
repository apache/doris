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

#include "vec/sink/vdata_stream_sender.h"

#include <fmt/format.h>
#include <fmt/ranges.h> // IWYU pragma: keep
#include <gen_cpp/DataSinks_types.h>
#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/data.pb.h>
#include <gen_cpp/internal_service.pb.h>
#include <opentelemetry/nostd/shared_ptr.h>
#include <stddef.h>

#include <algorithm>
#include <functional>
#include <map>
#include <random>

#include "common/object_pool.h"
#include "common/status.h"
#include "pipeline/exec/exchange_sink_operator.h"
#include "pipeline/exec/result_file_sink_operator.h"
#include "runtime/descriptors.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "runtime/types.h"
#include "util/proto_util.h"
#include "util/telemetry/telemetry.h"
#include "vec/columns/column_const.h"
#include "vec/common/sip_hash.h"
#include "vec/exprs/vexpr.h"
#include "vec/runtime/vdata_stream_mgr.h"
#include "vec/runtime/vdata_stream_recvr.h"

namespace doris::vectorized {

template <typename Parent>
Status Channel<Parent>::init(RuntimeState* state) {
    _be_number = state->be_number();

    if (_brpc_dest_addr.hostname.empty()) {
        LOG(WARNING) << "there is no brpc destination address's hostname"
                        ", maybe version is not compatible.";
        return Status::InternalError("no brpc destination");
    }

    // initialize brpc request
    _brpc_request.mutable_finst_id()->set_hi(_fragment_instance_id.hi);
    _brpc_request.mutable_finst_id()->set_lo(_fragment_instance_id.lo);
    _finst_id = _brpc_request.finst_id();

    _brpc_request.mutable_query_id()->set_hi(state->query_id().hi);
    _brpc_request.mutable_query_id()->set_lo(state->query_id().lo);
    _query_id = _brpc_request.query_id();

    _brpc_request.set_node_id(_dest_node_id);
    _brpc_request.set_sender_id(_parent->sender_id());
    _brpc_request.set_be_number(_be_number);

    _brpc_timeout_ms = std::min(3600, state->execution_timeout()) * 1000;

    if (_brpc_dest_addr.hostname == BackendOptions::get_localhost()) {
        _brpc_stub = state->exec_env()->brpc_internal_client_cache()->get_client(
                "127.0.0.1", _brpc_dest_addr.port);
    } else {
        _brpc_stub = state->exec_env()->brpc_internal_client_cache()->get_client(_brpc_dest_addr);
    }

    if (!_brpc_stub) {
        std::string msg = fmt::format("Get rpc stub failed, dest_addr={}:{}",
                                      _brpc_dest_addr.hostname, _brpc_dest_addr.port);
        LOG(WARNING) << msg;
        return Status::InternalError(msg);
    }

    if (state->query_options().__isset.enable_local_exchange) {
        _is_local &= state->query_options().enable_local_exchange;
    }

    if (_is_local) {
        _local_recvr = _parent->state()->exec_env()->vstream_mgr()->find_recvr(
                _fragment_instance_id, _dest_node_id);
    }

    _serializer.set_is_local(_is_local);

    // In bucket shuffle join will set fragment_instance_id (-1, -1)
    // to build a camouflaged empty channel. the ip and port is '0.0.0.0:0"
    // so the empty channel not need call function close_internal()
    _need_close = (_fragment_instance_id.hi != -1 && _fragment_instance_id.lo != -1);
    _state = state;
    return Status::OK();
}

template <typename Parent>
Status Channel<Parent>::send_current_block(bool eos, Status exec_status) {
    // FIXME: Now, local exchange will cause the performance problem is in a multi-threaded scenario
    // so this feature is turned off here by default. We need to re-examine this logic
    if (is_local()) {
        return send_local_block(exec_status, eos);
    }
    SCOPED_CONSUME_MEM_TRACKER(_parent->mem_tracker());
    if (eos) {
        RETURN_IF_ERROR(_serializer.serialize_block(_ch_cur_pb_block, 1));
    }
    RETURN_IF_ERROR(send_remote_block(_ch_cur_pb_block, eos, exec_status));
    ch_roll_pb_block();
    return Status::OK();
}

template <typename Parent>
Status Channel<Parent>::send_local_block(Status exec_status, bool eos) {
    if constexpr (!std::is_same_v<pipeline::ResultFileSinkLocalState, Parent>) {
        SCOPED_TIMER(_parent->local_send_timer());
    }
    Block block = _serializer.get_block()->to_block();
    _serializer.get_block()->set_muatable_columns(block.clone_empty_columns());
    if (_recvr_is_valid()) {
        if constexpr (!std::is_same_v<pipeline::ResultFileSinkLocalState, Parent>) {
            COUNTER_UPDATE(_parent->local_bytes_send_counter(), block.bytes());
            COUNTER_UPDATE(_parent->local_sent_rows(), block.rows());
            COUNTER_UPDATE(_parent->blocks_sent_counter(), 1);
        }

        _local_recvr->add_block(&block, _parent->sender_id(), true);
        if (eos) {
            _local_recvr->remove_sender(_parent->sender_id(), _be_number, exec_status);
        }
        return Status::OK();
    } else {
        _serializer.reset_block();
        return _receiver_status;
    }
}

template <typename Parent>
Status Channel<Parent>::send_local_block(Block* block) {
    if constexpr (!std::is_same_v<pipeline::ResultFileSinkLocalState, Parent>) {
        SCOPED_TIMER(_parent->local_send_timer());
    }
    if (_recvr_is_valid()) {
        if constexpr (!std::is_same_v<pipeline::ResultFileSinkLocalState, Parent>) {
            COUNTER_UPDATE(_parent->local_bytes_send_counter(), block->bytes());
            COUNTER_UPDATE(_parent->local_sent_rows(), block->rows());
            COUNTER_UPDATE(_parent->blocks_sent_counter(), 1);
        }
        _local_recvr->add_block(block, _parent->sender_id(), false);
        return Status::OK();
    } else {
        return _receiver_status;
    }
}

template <typename Parent>
Status Channel<Parent>::send_remote_block(PBlock* block, bool eos, Status exec_status) {
    if constexpr (!std::is_same_v<pipeline::ResultFileSinkLocalState, Parent>) {
        SCOPED_TIMER(_parent->brpc_send_timer());
        COUNTER_UPDATE(_parent->blocks_sent_counter(), 1);
    }

    if (_closure == nullptr) {
        _closure = new RefCountClosure<PTransmitDataResult>();
        _closure->ref();
    } else {
        RETURN_IF_ERROR(_wait_last_brpc());
        SCOPED_TRACK_MEMORY_TO_UNKNOWN();
        _closure->cntl.Reset();
    }
    VLOG_ROW << "Channel<Parent>::send_batch() instance_id=" << print_id(_fragment_instance_id)
             << " dest_node=" << _dest_node_id << " to_host=" << _brpc_dest_addr.hostname
             << " _packet_seq=" << _packet_seq << " row_desc=" << _row_desc.debug_string();
    if (_is_transfer_chain && (_send_query_statistics_with_every_batch || eos)) {
        auto statistic = _brpc_request.mutable_query_statistics();
        _parent->query_statistics()->to_pb(statistic);
    }

    _brpc_request.set_eos(eos);
    if (!exec_status.ok()) {
        exec_status.to_protobuf(_brpc_request.mutable_exec_status());
    }
    if (block != nullptr) {
        _brpc_request.set_allocated_block(block);
    }
    _brpc_request.set_packet_seq(_packet_seq++);

    _closure->ref();
    _closure->cntl.set_timeout_ms(_brpc_timeout_ms);
    if (config::exchange_sink_ignore_eovercrowded) {
        _closure->cntl.ignore_eovercrowded();
    }

    {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(ExecEnv::GetInstance()->orphan_mem_tracker());
        if (enable_http_send_block(_brpc_request)) {
            RETURN_IF_ERROR(transmit_block_http(_state->exec_env(), _closure, _brpc_request,
                                                _brpc_dest_addr));
        } else {
            transmit_block(*_brpc_stub, _closure, _brpc_request);
        }
    }

    if (block != nullptr) {
        static_cast<void>(_brpc_request.release_block());
    }
    return Status::OK();
}

template <typename Parent>
Status Channel<Parent>::add_rows(Block* block, const std::vector<uint32_t>& rows, bool eos) {
    if (_fragment_instance_id.lo == -1) {
        return Status::OK();
    }

    bool serialized = false;
    RETURN_IF_ERROR(
            _serializer.next_serialized_block(block, _ch_cur_pb_block, 1, &serialized, eos, &rows));
    if (serialized) {
        RETURN_IF_ERROR(send_current_block(false, Status::OK()));
    }

    return Status::OK();
}

template <typename Parent>
Status Channel<Parent>::close_wait(RuntimeState* state) {
    if (_need_close) {
        Status st = _wait_last_brpc();
        if (st.is<ErrorCode::END_OF_FILE>()) {
            st = Status::OK();
        } else if (!st.ok()) {
            state->log_error(st.to_string());
        }
        _need_close = false;
        return st;
    }
    _serializer.reset_block();
    return Status::OK();
}

template <typename Parent>
Status Channel<Parent>::close_internal(Status exec_status) {
    if (!_need_close) {
        return Status::OK();
    }
    VLOG_RPC << "Channel::close_internal() instance_id=" << print_id(_fragment_instance_id)
             << " dest_node=" << _dest_node_id << " #rows= "
             << ((_serializer.get_block() == nullptr) ? 0 : _serializer.get_block()->rows())
             << " receiver status: " << _receiver_status << ", exec_status: " << exec_status;
    if (is_receiver_eof()) {
        _serializer.reset_block();
        return Status::OK();
    }
    Status status;
    if (_serializer.get_block() != nullptr && _serializer.get_block()->rows() > 0) {
        status = send_current_block(true, exec_status);
    } else {
        SCOPED_CONSUME_MEM_TRACKER(_parent->mem_tracker());
        if (is_local()) {
            if (_recvr_is_valid()) {
                _local_recvr->remove_sender(_parent->sender_id(), _be_number, exec_status);
            }
        } else {
            status = send_remote_block((PBlock*)nullptr, true, exec_status);
        }
    }
    // Don't wait for the last packet to finish, left it to close_wait.
    if (status.is<ErrorCode::END_OF_FILE>()) {
        return Status::OK();
    } else {
        return status;
    }
}

template <typename Parent>
Status Channel<Parent>::close(RuntimeState* state, Status exec_status) {
    if (_closed) {
        return Status::OK();
    }
    _closed = true;

    Status st = close_internal(exec_status);
    if (!st.ok()) {
        state->log_error(st.to_string());
    }
    return st;
}

template <typename Parent>
void Channel<Parent>::ch_roll_pb_block() {
    _ch_cur_pb_block = (_ch_cur_pb_block == &_ch_pb_block1 ? &_ch_pb_block2 : &_ch_pb_block1);
}

VDataStreamSender::VDataStreamSender(RuntimeState* state, ObjectPool* pool, int sender_id,
                                     const RowDescriptor& row_desc, const TDataStreamSink& sink,
                                     const std::vector<TPlanFragmentDestination>& destinations,
                                     bool send_query_statistics_with_every_batch)
        : DataSink(row_desc),
          _sender_id(sender_id),
          _state(state),
          _pool(pool),
          _current_channel_idx(0),
          _part_type(sink.output_partition.type),
          _serialize_batch_timer(nullptr),
          _bytes_sent_counter(nullptr),
          _local_send_timer(nullptr),
          _split_block_hash_compute_timer(nullptr),
          _split_block_distribute_by_channel_timer(nullptr),
          _blocks_sent_counter(nullptr),
          _local_bytes_send_counter(nullptr),
          _dest_node_id(sink.dest_node_id),
          _transfer_large_data_by_brpc(config::transfer_large_data_by_brpc),
          _serializer(this) {
    DCHECK_GT(destinations.size(), 0);
    DCHECK(sink.output_partition.type == TPartitionType::UNPARTITIONED ||
           sink.output_partition.type == TPartitionType::HASH_PARTITIONED ||
           sink.output_partition.type == TPartitionType::RANDOM ||
           sink.output_partition.type == TPartitionType::RANGE_PARTITIONED ||
           sink.output_partition.type == TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED);

    std::map<int64_t, int64_t> fragment_id_to_channel_index;
    _enable_pipeline_exec = state->enable_pipeline_exec();

    for (int i = 0; i < destinations.size(); ++i) {
        // Select first dest as transfer chain.
        bool is_transfer_chain = (i == 0);
        const auto& fragment_instance_id = destinations[i].fragment_instance_id;
        if (fragment_id_to_channel_index.find(fragment_instance_id.lo) ==
            fragment_id_to_channel_index.end()) {
            if (_enable_pipeline_exec) {
                _channel_shared_ptrs.emplace_back(new PipChannel<VDataStreamSender>(
                        this, row_desc, destinations[i].brpc_server, fragment_instance_id,
                        sink.dest_node_id, is_transfer_chain,
                        send_query_statistics_with_every_batch));
            } else {
                _channel_shared_ptrs.emplace_back(
                        new Channel(this, row_desc, destinations[i].brpc_server,
                                    fragment_instance_id, sink.dest_node_id, is_transfer_chain,
                                    send_query_statistics_with_every_batch));
            }
            fragment_id_to_channel_index.emplace(fragment_instance_id.lo,
                                                 _channel_shared_ptrs.size() - 1);
            _channels.push_back(_channel_shared_ptrs.back().get());
        } else {
            _channel_shared_ptrs.emplace_back(
                    _channel_shared_ptrs[fragment_id_to_channel_index[fragment_instance_id.lo]]);
        }
    }
    _name = "VDataStreamSender";
    if (_enable_pipeline_exec) {
        _broadcast_pb_blocks.resize(config::num_broadcast_buffer);
        _broadcast_pb_block_idx = 0;
    } else {
        _cur_pb_block = &_pb_block1;
    }
}

VDataStreamSender::VDataStreamSender(RuntimeState* state, ObjectPool* pool, int sender_id,
                                     const RowDescriptor& row_desc, PlanNodeId dest_node_id,
                                     const std::vector<TPlanFragmentDestination>& destinations,
                                     bool send_query_statistics_with_every_batch)
        : DataSink(row_desc),
          _sender_id(sender_id),
          _state(state),
          _pool(pool),
          _current_channel_idx(0),
          _part_type(TPartitionType::UNPARTITIONED),
          _serialize_batch_timer(nullptr),
          _compress_timer(nullptr),
          _brpc_send_timer(nullptr),
          _brpc_wait_timer(nullptr),
          _bytes_sent_counter(nullptr),
          _local_send_timer(nullptr),
          _split_block_hash_compute_timer(nullptr),
          _split_block_distribute_by_channel_timer(nullptr),
          _blocks_sent_counter(nullptr),
          _local_bytes_send_counter(nullptr),
          _dest_node_id(dest_node_id),
          _serializer(this) {
    _cur_pb_block = &_pb_block1;
    _name = "VDataStreamSender";
    std::map<int64_t, int64_t> fragment_id_to_channel_index;
    for (int i = 0; i < destinations.size(); ++i) {
        const auto& fragment_instance_id = destinations[i].fragment_instance_id;
        if (fragment_id_to_channel_index.find(fragment_instance_id.lo) ==
            fragment_id_to_channel_index.end()) {
            _channel_shared_ptrs.emplace_back(
                    new Channel(this, row_desc, destinations[i].brpc_server, fragment_instance_id,
                                _dest_node_id, false, send_query_statistics_with_every_batch));
        }
        fragment_id_to_channel_index.emplace(fragment_instance_id.lo,
                                             _channel_shared_ptrs.size() - 1);
        _channels.push_back(_channel_shared_ptrs.back().get());
    }
}

VDataStreamSender::~VDataStreamSender() {
    _channel_shared_ptrs.clear();
}

Status VDataStreamSender::init(const TDataSink& tsink) {
    RETURN_IF_ERROR(DataSink::init(tsink));
    const TDataStreamSink& t_stream_sink = tsink.stream_sink;
    if (_part_type == TPartitionType::HASH_PARTITIONED) {
        _partition_count = _channels.size();
        _partitioner.reset(new HashPartitioner(_channels.size()));
        RETURN_IF_ERROR(_partitioner->init(t_stream_sink.output_partition.partition_exprs));
    } else if (_part_type == TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED) {
        _partition_count = _channel_shared_ptrs.size();
        _partitioner.reset(new BucketHashPartitioner(_channel_shared_ptrs.size()));
        RETURN_IF_ERROR(_partitioner->init(t_stream_sink.output_partition.partition_exprs));
    } else if (_part_type == TPartitionType::RANGE_PARTITIONED) {
        return Status::InternalError("TPartitionType::RANGE_PARTITIONED should not be used");
    } else {
        // UNPARTITIONED
    }
    return Status::OK();
}

Status VDataStreamSender::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::prepare(state));

    std::vector<std::string> instances;
    for (const auto& channel : _channels) {
        instances.emplace_back(channel->get_fragment_instance_id_str());
    }
    std::string title = fmt::format("VDataStreamSender (dst_id={}, dst_fragments=[{}])",
                                    _dest_node_id, instances);
    _profile = _pool->add(new RuntimeProfile(title));
    SCOPED_TIMER(_profile->total_time_counter());
    _mem_tracker = std::make_unique<MemTracker>("VDataStreamSender:" +
                                                print_id(state->fragment_instance_id()));
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());

    if (_part_type == TPartitionType::UNPARTITIONED || _part_type == TPartitionType::RANDOM) {
        std::random_device rd;
        std::mt19937 g(rd());
        shuffle(_channels.begin(), _channels.end(), g);
    } else if (_part_type == TPartitionType::HASH_PARTITIONED ||
               _part_type == TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED) {
        RETURN_IF_ERROR(_partitioner->prepare(state, _row_desc));
    }

    _bytes_sent_counter = ADD_COUNTER(profile(), "BytesSent", TUnit::BYTES);
    _uncompressed_bytes_counter = ADD_COUNTER(profile(), "UncompressedRowBatchSize", TUnit::BYTES);
    _local_sent_rows = ADD_COUNTER(profile(), "LocalSentRows", TUnit::UNIT);
    _serialize_batch_timer = ADD_TIMER(profile(), "SerializeBatchTime");
    _compress_timer = ADD_TIMER(profile(), "CompressTime");
    _brpc_send_timer = ADD_TIMER(profile(), "BrpcSendTime");
    _brpc_wait_timer = ADD_TIMER(profile(), "BrpcSendTime.Wait");
    _local_send_timer = ADD_TIMER(profile(), "LocalSendTime");
    _split_block_hash_compute_timer = ADD_TIMER(profile(), "SplitBlockHashComputeTime");
    _split_block_distribute_by_channel_timer =
            ADD_TIMER(profile(), "SplitBlockDistributeByChannelTime");
    _merge_block_timer = ADD_TIMER(profile(), "MergeBlockTime");
    _blocks_sent_counter = ADD_COUNTER_WITH_LEVEL(profile(), "BlocksSent", TUnit::UNIT, 1);
    _overall_throughput = profile()->add_derived_counter(
            "OverallThroughput", TUnit::BYTES_PER_SECOND,
            std::bind<int64_t>(&RuntimeProfile::units_per_second, _bytes_sent_counter,
                               profile()->total_time_counter()),
            "");
    _local_bytes_send_counter = ADD_COUNTER(profile(), "LocalBytesSent", TUnit::BYTES);
    _memory_usage_counter = ADD_LABEL_COUNTER(profile(), "MemoryUsage");
    _peak_memory_usage_counter =
            profile()->AddHighWaterMarkCounter("PeakMemoryUsage", TUnit::BYTES, "MemoryUsage");
    return Status::OK();
}

Status VDataStreamSender::open(RuntimeState* state) {
    DCHECK(state != nullptr);
    int local_size = 0;
    for (int i = 0; i < _channels.size(); ++i) {
        RETURN_IF_ERROR(_channels[i]->init(state));
        if (_channels[i]->is_local()) {
            local_size++;
        }
    }
    _only_local_exchange = local_size == _channels.size();
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
    if (_part_type == TPartitionType::HASH_PARTITIONED ||
        _part_type == TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED) {
        RETURN_IF_ERROR(_partitioner->open(state));
    }

    _compression_type = state->fragement_transmission_compression_type();
    return Status::OK();
}

template <typename ChannelPtrType>
void VDataStreamSender::_handle_eof_channel(RuntimeState* state, ChannelPtrType channel,
                                            Status st) {
    channel->set_receiver_eof(st);
    // Chanel will not send RPC to the downstream when eof, so close chanel by OK status.
    static_cast<void>(channel->close(state, Status::OK()));
}

Status VDataStreamSender::send(RuntimeState* state, Block* block, bool eos) {
    SCOPED_TIMER(_profile->total_time_counter());
    _peak_memory_usage_counter->set(_mem_tracker->peak_consumption());
    bool all_receiver_eof = true;
    for (auto channel : _channels) {
        if (!channel->is_receiver_eof()) {
            all_receiver_eof = false;
            break;
        }
    }
    if (all_receiver_eof) {
        return Status::EndOfFile("all data stream channels EOF");
    }

    if (_part_type == TPartitionType::UNPARTITIONED || _channels.size() == 1) {
        // 1. serialize depends on it is not local exchange
        // 2. send block
        // 3. rollover block
        if (_only_local_exchange) {
            if (!block->empty()) {
                Status status;
                for (auto channel : _channels) {
                    if (!channel->is_receiver_eof()) {
                        status = channel->send_local_block(block);
                        HANDLE_CHANNEL_STATUS(state, channel, status);
                    }
                }
            }
        } else if (_enable_pipeline_exec) {
            BroadcastPBlockHolder* block_holder = nullptr;
            RETURN_IF_ERROR(_get_next_available_buffer(&block_holder));
            {
                SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
                bool serialized = false;
                RETURN_IF_ERROR(_serializer.next_serialized_block(
                        block, block_holder->get_block(), _channels.size(), &serialized, eos));
                if (serialized) {
                    auto cur_block = _serializer.get_block()->to_block();
                    if (!cur_block.empty()) {
                        RETURN_IF_ERROR(_serializer.serialize_block(
                                &cur_block, block_holder->get_block(), _channels.size()));
                    } else {
                        block_holder->get_block()->Clear();
                    }
                    Status status;
                    for (auto channel : _channels) {
                        if (!channel->is_receiver_eof()) {
                            if (channel->is_local()) {
                                status = channel->send_local_block(&cur_block);
                            } else {
                                SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
                                status = channel->send_broadcast_block(block_holder, nullptr, eos);
                            }
                            HANDLE_CHANNEL_STATUS(state, channel, status);
                        }
                    }
                    cur_block.clear_column_data();
                    _serializer.get_block()->set_muatable_columns(cur_block.mutate_columns());
                }
            }
        } else {
            SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
            bool serialized = false;
            RETURN_IF_ERROR(_serializer.next_serialized_block(
                    block, _cur_pb_block, _channels.size(), &serialized, false));
            if (serialized) {
                auto cur_block = _serializer.get_block()->to_block();
                if (!cur_block.empty()) {
                    RETURN_IF_ERROR(_serializer.serialize_block(&cur_block, _cur_pb_block,
                                                                _channels.size()));
                }
                Status status;
                for (auto channel : _channels) {
                    if (!channel->is_receiver_eof()) {
                        if (channel->is_local()) {
                            status = channel->send_local_block(&cur_block);
                        } else {
                            SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
                            status = channel->send_remote_block(_cur_pb_block, false);
                        }
                        HANDLE_CHANNEL_STATUS(state, channel, status);
                    }
                }
                cur_block.clear_column_data();
                _serializer.get_block()->set_muatable_columns(cur_block.mutate_columns());
                _roll_pb_block();
            }
        }
    } else if (_part_type == TPartitionType::RANDOM) {
        // 1. select channel
        Channel<VDataStreamSender>* current_channel = _channels[_current_channel_idx];
        if (!current_channel->is_receiver_eof()) {
            // 2. serialize, send and rollover block
            if (current_channel->is_local()) {
                auto status = current_channel->send_local_block(block);
                HANDLE_CHANNEL_STATUS(state, current_channel, status);
            } else {
                SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
                RETURN_IF_ERROR(
                        _serializer.serialize_block(block, current_channel->ch_cur_pb_block()));
                auto status =
                        current_channel->send_remote_block(current_channel->ch_cur_pb_block(), eos);
                HANDLE_CHANNEL_STATUS(state, current_channel, status);
                current_channel->ch_roll_pb_block();
            }
        }
        _current_channel_idx = (_current_channel_idx + 1) % _channels.size();
    } else if (_part_type == TPartitionType::HASH_PARTITIONED ||
               _part_type == TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED) {
        auto rows = block->rows();
        SCOPED_TIMER(_split_block_hash_compute_timer);
        RETURN_IF_ERROR(_partitioner->do_partitioning(state, block, _mem_tracker.get()));
        if (_part_type == TPartitionType::HASH_PARTITIONED) {
            RETURN_IF_ERROR(channel_add_rows(state, _channels, _partition_count,
                                             (uint64_t*)_partitioner->get_hash_values(), rows,
                                             block, _enable_pipeline_exec ? eos : false));
        } else {
            RETURN_IF_ERROR(channel_add_rows(state, _channel_shared_ptrs, _partition_count,
                                             (uint32_t*)_partitioner->get_hash_values(), rows,
                                             block, _enable_pipeline_exec ? eos : false));
        }
    } else {
        // Range partition
        // 1. calculate range
        // 2. dispatch rows to channel
    }
    return Status::OK();
}

Status VDataStreamSender::try_close(RuntimeState* state, Status exec_status) {
    _serializer.reset_block();
    Status final_st = Status::OK();
    for (int i = 0; i < _channels.size(); ++i) {
        Status st = _channels[i]->close(state, exec_status);
        if (!st.ok() && final_st.ok()) {
            final_st = st;
        }
    }
    return final_st;
}

Status VDataStreamSender::close(RuntimeState* state, Status exec_status) {
    if (_closed) {
        return Status::OK();
    }

    Status final_st = Status::OK();
    if (!state->enable_pipeline_exec()) {
        {
            // send last block
            SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
            if (_serializer.get_block() && _serializer.get_block()->rows() > 0) {
                Block block = _serializer.get_block()->to_block();
                RETURN_IF_ERROR(
                        _serializer.serialize_block(&block, _cur_pb_block, _channels.size()));
                Status status;
                for (auto channel : _channels) {
                    if (!channel->is_receiver_eof()) {
                        if (channel->is_local()) {
                            status = channel->send_local_block(&block);
                        } else {
                            SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
                            status = channel->send_remote_block(_cur_pb_block, false);
                        }
                        HANDLE_CHANNEL_STATUS(state, channel, status);
                    }
                }
            }
        }
        for (int i = 0; i < _channels.size(); ++i) {
            Status st = _channels[i]->close(state, exec_status);
            if (!st.ok() && final_st.ok()) {
                final_st = st;
            }
        }
        // wait all channels to finish
        for (int i = 0; i < _channels.size(); ++i) {
            Status st = _channels[i]->close_wait(state);
            if (!st.ok() && final_st.ok()) {
                final_st = st;
            }
        }
    }

    if (_peak_memory_usage_counter) {
        _peak_memory_usage_counter->set(_mem_tracker->peak_consumption());
    }
    static_cast<void>(DataSink::close(state, exec_status));
    return final_st;
}

template <typename Parent>
BlockSerializer<Parent>::BlockSerializer(Parent* parent, bool is_local)
        : _parent(parent), _is_local(is_local), _batch_size(parent->state()->batch_size()) {}

template <typename Parent>
Status BlockSerializer<Parent>::next_serialized_block(Block* block, PBlock* dest, int num_receivers,
                                                      bool* serialized, bool eos,
                                                      const std::vector<uint32_t>* rows) {
    if (_mutable_block == nullptr) {
        SCOPED_CONSUME_MEM_TRACKER(_parent->mem_tracker());
        _mutable_block = MutableBlock::create_unique(block->clone_empty());
    }

    {
        SCOPED_CONSUME_MEM_TRACKER(_parent->mem_tracker());
        if (rows) {
            if (rows->size() > 0) {
                if constexpr (!std::is_same_v<pipeline::ResultFileSinkLocalState, Parent>) {
                    SCOPED_TIMER(_parent->split_block_distribute_by_channel_timer());
                }
                const uint32_t* begin = &(*rows)[0];
                _mutable_block->add_rows(block, begin, begin + rows->size());
            }
        } else if (!block->empty()) {
            if constexpr (!std::is_same_v<pipeline::ResultFileSinkLocalState, Parent>) {
                SCOPED_TIMER(_parent->merge_block_timer());
            }
            RETURN_IF_ERROR(_mutable_block->merge(*block));
        }
    }

    if (_mutable_block->rows() >= _batch_size || eos) {
        if (!_is_local) {
            RETURN_IF_ERROR(serialize_block(dest, num_receivers));
        }
        *serialized = true;
        return Status::OK();
    }
    *serialized = false;
    return Status::OK();
}

template <typename Parent>
Status BlockSerializer<Parent>::serialize_block(PBlock* dest, int num_receivers) {
    if (_mutable_block && _mutable_block->rows() > 0) {
        auto block = _mutable_block->to_block();
        RETURN_IF_ERROR(serialize_block(&block, dest, num_receivers));
        block.clear_column_data();
        _mutable_block->set_muatable_columns(block.mutate_columns());
    }

    return Status::OK();
}

template <typename Parent>
Status BlockSerializer<Parent>::serialize_block(const Block* src, PBlock* dest, int num_receivers) {
    if constexpr (!std::is_same_v<pipeline::ResultFileSinkLocalState, Parent>) {
        SCOPED_TIMER(_parent->_serialize_batch_timer);
        dest->Clear();
        size_t uncompressed_bytes = 0, compressed_bytes = 0;
        RETURN_IF_ERROR(src->serialize(
                _parent->_state->be_exec_version(), dest, &uncompressed_bytes, &compressed_bytes,
                _parent->compression_type(), _parent->transfer_large_data_by_brpc()));
        COUNTER_UPDATE(_parent->_bytes_sent_counter, compressed_bytes * num_receivers);
        COUNTER_UPDATE(_parent->_uncompressed_bytes_counter, uncompressed_bytes * num_receivers);
        COUNTER_UPDATE(_parent->_compress_timer, src->get_compress_time());
    }

    return Status::OK();
}

void VDataStreamSender::_roll_pb_block() {
    _cur_pb_block = (_cur_pb_block == &_pb_block1 ? &_pb_block2 : &_pb_block1);
}

Status VDataStreamSender::_get_next_available_buffer(BroadcastPBlockHolder** holder) {
    if (_broadcast_pb_block_idx >= _broadcast_pb_blocks.size()) {
        return Status::InternalError(
                "get_next_available_buffer meet invalid index, index={}, size={}",
                _broadcast_pb_block_idx, _broadcast_pb_blocks.size());
    }
    if (!_broadcast_pb_blocks[_broadcast_pb_block_idx].available()) {
        return Status::InternalError("broadcast_pb_blocks not available");
    }
    *holder = &_broadcast_pb_blocks[_broadcast_pb_block_idx];
    _broadcast_pb_block_idx++;
    return Status::OK();
}

void VDataStreamSender::registe_channels(pipeline::ExchangeSinkBuffer<VDataStreamSender>* buffer) {
    for (auto channel : _channels) {
        ((PipChannel<VDataStreamSender>*)channel)->registe(buffer);
    }
}

bool VDataStreamSender::channel_all_can_write() {
    if ((_part_type == TPartitionType::UNPARTITIONED || _channels.size() == 1) &&
        !_only_local_exchange) {
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
        for (auto channel : _channels) {
            if (!channel->can_write()) {
                return false;
            }
        }
        return true;
    }
}

template class Channel<pipeline::ExchangeSinkLocalState>;
template class Channel<VDataStreamSender>;
template class Channel<pipeline::ResultFileSinkLocalState>;
template class BlockSerializer<pipeline::ResultFileSinkLocalState>;
template class BlockSerializer<pipeline::ExchangeSinkLocalState>;
template class BlockSerializer<VDataStreamSender>;

} // namespace doris::vectorized
