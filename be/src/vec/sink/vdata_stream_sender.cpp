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

Status Channel::init(RuntimeState* state) {
    _be_number = state->be_number();

    if (_brpc_dest_addr.hostname.empty()) {
        LOG(WARNING) << "there is no brpc destination address's hostname"
                        ", maybe version is not compatible.";
        return Status::InternalError("no brpc destination");
    }

    // initialize brpc request
    _finst_id.set_hi(_fragment_instance_id.hi);
    _finst_id.set_lo(_fragment_instance_id.lo);
    _brpc_request.set_allocated_finst_id(&_finst_id);

    _query_id.set_hi(state->query_id().hi);
    _query_id.set_lo(state->query_id().lo);
    _brpc_request.set_allocated_query_id(&_query_id);

    _brpc_request.set_node_id(_dest_node_id);
    _brpc_request.set_sender_id(_parent->_sender_id);
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

    // In bucket shuffle join will set fragment_instance_id (-1, -1)
    // to build a camouflaged empty channel. the ip and port is '0.0.0.0:0"
    // so the empty channel not need call function close_internal()
    _need_close = (_fragment_instance_id.hi != -1 && _fragment_instance_id.lo != -1);
    _state = state;
    return Status::OK();
}

Status Channel::send_current_block(bool eos) {
    // FIXME: Now, local exchange will cause the performance problem is in a multi-threaded scenario
    // so this feature is turned off here by default. We need to re-examine this logic
    if (is_local()) {
        return send_local_block(eos);
    }
    SCOPED_CONSUME_MEM_TRACKER(_parent->_mem_tracker.get());
    auto block = _mutable_block->to_block();
    RETURN_IF_ERROR(_parent->serialize_block(&block, _ch_cur_pb_block));
    block.clear_column_data();
    _mutable_block->set_muatable_columns(block.mutate_columns());
    RETURN_IF_ERROR(send_block(_ch_cur_pb_block, eos));
    ch_roll_pb_block();
    return Status::OK();
}

Status Channel::send_local_block(bool eos) {
    SCOPED_TIMER(_parent->_local_send_timer);
    Block block = _mutable_block->to_block();
    _mutable_block->set_muatable_columns(block.clone_empty_columns());
    if (_recvr_is_valid()) {
        COUNTER_UPDATE(_parent->_local_bytes_send_counter, block.bytes());
        COUNTER_UPDATE(_parent->_local_sent_rows, block.rows());
        COUNTER_UPDATE(_parent->_blocks_sent_counter, 1);
        _local_recvr->add_block(&block, _parent->_sender_id, true);
        if (eos) {
            _local_recvr->remove_sender(_parent->_sender_id, _be_number,
                                        _parent->query_statisticsPtr());
        }
        return Status::OK();
    } else {
        _mutable_block.reset();
        return _receiver_status;
    }
}

Status Channel::send_local_block(Block* block) {
    SCOPED_TIMER(_parent->_local_send_timer);
    if (_recvr_is_valid()) {
        COUNTER_UPDATE(_parent->_local_bytes_send_counter, block->bytes());
        COUNTER_UPDATE(_parent->_local_sent_rows, block->rows());
        COUNTER_UPDATE(_parent->_blocks_sent_counter, 1);
        _local_recvr->add_block(block, _parent->_sender_id, false);
        return Status::OK();
    } else {
        return _receiver_status;
    }
}

Status Channel::send_block(PBlock* block, bool eos) {
    SCOPED_TIMER(_parent->_brpc_send_timer);
    COUNTER_UPDATE(_parent->_blocks_sent_counter, 1);
    if (_closure == nullptr) {
        _closure = new RefCountClosure<PTransmitDataResult>();
        _closure->ref();
    } else {
        RETURN_IF_ERROR(_wait_last_brpc());
        SCOPED_TRACK_MEMORY_TO_UNKNOWN();
        _closure->cntl.Reset();
    }
    VLOG_ROW << "Channel::send_batch() instance_id=" << _fragment_instance_id
             << " dest_node=" << _dest_node_id << " to_host=" << _brpc_dest_addr.hostname
             << " _packet_seq=" << _packet_seq << " row_desc=" << _row_desc.debug_string();
    if (_is_transfer_chain && (_send_query_statistics_with_every_batch || eos)) {
        auto statistic = _brpc_request.mutable_query_statistics();
        _parent->_query_statistics->to_pb(statistic);
    }

    _brpc_request.set_eos(eos);
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
            RETURN_IF_ERROR(transmit_block_http(_state, _closure, _brpc_request, _brpc_dest_addr));
        } else {
            transmit_block(*_brpc_stub, _closure, _brpc_request);
        }
    }

    if (block != nullptr) {
        static_cast<void>(_brpc_request.release_block());
    }
    return Status::OK();
}

Status Channel::add_rows(Block* block, const std::vector<int>& rows) {
    if (_fragment_instance_id.lo == -1) {
        return Status::OK();
    }

    if (_mutable_block == nullptr) {
        SCOPED_CONSUME_MEM_TRACKER(_parent->_mem_tracker.get());
        _mutable_block = MutableBlock::create_unique(block->clone_empty());
    }

    int row_wait_add = rows.size();
    int batch_size = _parent->state()->batch_size();
    const int* begin = &rows[0];

    while (row_wait_add > 0) {
        int row_add = 0;
        int max_add = batch_size - _mutable_block->rows();
        if (row_wait_add >= max_add) {
            row_add = max_add;
        } else {
            row_add = row_wait_add;
        }

        {
            SCOPED_CONSUME_MEM_TRACKER(_parent->_mem_tracker.get());
            SCOPED_TIMER(_parent->_split_block_distribute_by_channel_timer);
            _mutable_block->add_rows(block, begin, begin + row_add);
        }

        row_wait_add -= row_add;
        begin += row_add;

        if (row_add == max_add) {
            RETURN_IF_ERROR(send_current_block(false));
        }
    }

    return Status::OK();
}

Status Channel::close_wait(RuntimeState* state) {
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
    _mutable_block.reset();
    return Status::OK();
}

Status Channel::close_internal() {
    if (!_need_close) {
        return Status::OK();
    }
    VLOG_RPC << "Channel::close() instance_id=" << _fragment_instance_id
             << " dest_node=" << _dest_node_id
             << " #rows= " << ((_mutable_block == nullptr) ? 0 : _mutable_block->rows())
             << " receiver status: " << _receiver_status;
    if (is_receiver_eof()) {
        _mutable_block.reset();
        return Status::OK();
    }
    Status status;
    if (_mutable_block != nullptr && _mutable_block->rows() > 0) {
        status = send_current_block(true);
    } else {
        SCOPED_CONSUME_MEM_TRACKER(_parent->_mem_tracker.get());
        if (is_local()) {
            if (_recvr_is_valid()) {
                _local_recvr->remove_sender(_parent->_sender_id, _be_number,
                                            _parent->query_statisticsPtr());
            }
        } else {
            status = send_block((PBlock*)nullptr, true);
        }
    }
    // Don't wait for the last packet to finish, left it to close_wait.
    if (status.is<ErrorCode::END_OF_FILE>()) {
        return Status::OK();
    } else {
        return status;
    }
}

Status Channel::close(RuntimeState* state) {
    if (_closed) {
        return Status::OK();
    }
    _closed = true;

    Status st = close_internal();
    if (!st.ok()) {
        state->log_error(st.to_string());
    }
    return st;
}

void Channel::ch_roll_pb_block() {
    _ch_cur_pb_block = (_ch_cur_pb_block == &_ch_pb_block1 ? &_ch_pb_block2 : &_ch_pb_block1);
}

VDataStreamSender::VDataStreamSender(RuntimeState* state, ObjectPool* pool, int sender_id,
                                     const RowDescriptor& row_desc, const TDataStreamSink& sink,
                                     const std::vector<TPlanFragmentDestination>& destinations,
                                     int per_channel_buffer_size,
                                     bool send_query_statistics_with_every_batch)
        : _sender_id(sender_id),
          _pool(pool),
          _row_desc(row_desc),
          _current_channel_idx(0),
          _part_type(sink.output_partition.type),
          _ignore_not_found(sink.__isset.ignore_not_found ? sink.ignore_not_found : true),
          _profile(nullptr),
          _serialize_batch_timer(nullptr),
          _bytes_sent_counter(nullptr),
          _local_send_timer(nullptr),
          _split_block_hash_compute_timer(nullptr),
          _split_block_distribute_by_channel_timer(nullptr),
          _blocks_sent_counter(nullptr),
          _local_bytes_send_counter(nullptr),
          _dest_node_id(sink.dest_node_id),
          _transfer_large_data_by_brpc(config::transfer_large_data_by_brpc) {
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
                _channel_shared_ptrs.emplace_back(new PipChannel(
                        this, row_desc, destinations[i].brpc_server, fragment_instance_id,
                        sink.dest_node_id, per_channel_buffer_size, is_transfer_chain,
                        send_query_statistics_with_every_batch));
            } else {
                _channel_shared_ptrs.emplace_back(new Channel(
                        this, row_desc, destinations[i].brpc_server, fragment_instance_id,
                        sink.dest_node_id, per_channel_buffer_size, is_transfer_chain,
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

VDataStreamSender::VDataStreamSender(ObjectPool* pool, int sender_id, const RowDescriptor& row_desc,
                                     PlanNodeId dest_node_id,
                                     const std::vector<TPlanFragmentDestination>& destinations,
                                     int per_channel_buffer_size,
                                     bool send_query_statistics_with_every_batch)
        : _sender_id(sender_id),
          _pool(pool),
          _row_desc(row_desc),
          _current_channel_idx(0),
          _part_type(TPartitionType::UNPARTITIONED),
          _ignore_not_found(true),
          _profile(nullptr),
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
          _dest_node_id(dest_node_id) {
    _cur_pb_block = &_pb_block1;
    _name = "VDataStreamSender";
    std::map<int64_t, int64_t> fragment_id_to_channel_index;
    for (int i = 0; i < destinations.size(); ++i) {
        const auto& fragment_instance_id = destinations[i].fragment_instance_id;
        if (fragment_id_to_channel_index.find(fragment_instance_id.lo) ==
            fragment_id_to_channel_index.end()) {
            _channel_shared_ptrs.emplace_back(
                    new Channel(this, row_desc, destinations[i].brpc_server, fragment_instance_id,
                                _dest_node_id, per_channel_buffer_size, false,
                                send_query_statistics_with_every_batch));
        }
        fragment_id_to_channel_index.emplace(fragment_instance_id.lo,
                                             _channel_shared_ptrs.size() - 1);
        _channels.push_back(_channel_shared_ptrs.back().get());
    }
}

VDataStreamSender::VDataStreamSender(ObjectPool* pool, const RowDescriptor& row_desc,
                                     int per_channel_buffer_size,
                                     bool send_query_statistics_with_every_batch)
        : _sender_id(0),
          _pool(pool),
          _row_desc(row_desc),
          _current_channel_idx(0),
          _ignore_not_found(true),
          _profile(nullptr),
          _serialize_batch_timer(nullptr),
          _compress_timer(nullptr),
          _brpc_send_timer(nullptr),
          _brpc_wait_timer(nullptr),
          _bytes_sent_counter(nullptr),
          _local_send_timer(nullptr),
          _split_block_hash_compute_timer(nullptr),
          _split_block_distribute_by_channel_timer(nullptr),
          _blocks_sent_counter(nullptr),
          _peak_memory_usage_counter(nullptr),
          _local_bytes_send_counter(nullptr),
          _dest_node_id(0) {
    _cur_pb_block = &_pb_block1;
    _name = "VDataStreamSender";
}

VDataStreamSender::~VDataStreamSender() {
    _channel_shared_ptrs.clear();
}

Status VDataStreamSender::init(const TDataSink& tsink) {
    RETURN_IF_ERROR(DataSink::init(tsink));
    const TDataStreamSink& t_stream_sink = tsink.stream_sink;
    if (_part_type == TPartitionType::HASH_PARTITIONED ||
        _part_type == TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED) {
        RETURN_IF_ERROR(VExpr::create_expr_trees(t_stream_sink.output_partition.partition_exprs,
                                                 _partition_expr_ctxs));
    } else if (_part_type == TPartitionType::RANGE_PARTITIONED) {
        return Status::InternalError("TPartitionType::RANGE_PARTITIONED should not be used");
    } else {
        // UNPARTITIONED
    }
    return Status::OK();
}

Status VDataStreamSender::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::prepare(state));
    _state = state;

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
        if (_state->query_options().__isset.enable_new_shuffle_hash_method) {
            _new_shuffle_hash_method = _state->query_options().enable_new_shuffle_hash_method;
        }
        RETURN_IF_ERROR(VExpr::prepare(_partition_expr_ctxs, state, _row_desc));
    } else {
        RETURN_IF_ERROR(VExpr::prepare(_partition_expr_ctxs, state, _row_desc));
    }

    _bytes_sent_counter = ADD_COUNTER(profile(), "BytesSent", TUnit::BYTES);
    _uncompressed_bytes_counter = ADD_COUNTER(profile(), "UncompressedRowBatchSize", TUnit::BYTES);
    _ignore_rows = ADD_COUNTER(profile(), "IgnoreRows", TUnit::UNIT);
    _local_sent_rows = ADD_COUNTER(profile(), "LocalSentRows", TUnit::UNIT);
    _serialize_batch_timer = ADD_TIMER(profile(), "SerializeBatchTime");
    _compress_timer = ADD_TIMER(profile(), "CompressTime");
    _brpc_send_timer = ADD_TIMER(profile(), "BrpcSendTime");
    _brpc_wait_timer = ADD_TIMER(profile(), "BrpcSendTime.Wait");
    _local_send_timer = ADD_TIMER(profile(), "LocalSendTime");
    _split_block_hash_compute_timer = ADD_TIMER(profile(), "SplitBlockHashComputeTime");
    _split_block_distribute_by_channel_timer =
            ADD_TIMER(profile(), "SplitBlockDistributeByChannelTime");
    _blocks_sent_counter = ADD_COUNTER(profile(), "BlocksSent", TUnit::UNIT);
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
    RETURN_IF_ERROR(VExpr::open(_partition_expr_ctxs, state));

    _compression_type = state->fragement_transmission_compression_type();
    return Status::OK();
}

template <typename ChannelPtrType>
void VDataStreamSender::_handle_eof_channel(RuntimeState* state, ChannelPtrType channel,
                                            Status st) {
    channel->set_receiver_eof(st);
    channel->close(state);
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
            Status status;
            for (auto channel : _channels) {
                if (!channel->is_receiver_eof()) {
                    status = channel->send_local_block(block);
                    HANDLE_CHANNEL_STATUS(state, channel, status);
                }
            }
        } else if (_enable_pipeline_exec) {
            BroadcastPBlockHolder* block_holder = nullptr;
            RETURN_IF_ERROR(_get_next_available_buffer(&block_holder));
            {
                SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
                RETURN_IF_ERROR(
                        serialize_block(block, block_holder->get_block(), _channels.size()));
            }

            Status status;
            for (auto channel : _channels) {
                if (!channel->is_receiver_eof()) {
                    if (channel->is_local()) {
                        status = channel->send_local_block(block);
                    } else {
                        SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
                        status = channel->send_block(block_holder, eos);
                    }
                    HANDLE_CHANNEL_STATUS(state, channel, status);
                }
            }
        } else {
            {
                SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
                RETURN_IF_ERROR(serialize_block(block, _cur_pb_block, _channels.size()));
            }

            Status status;
            for (auto channel : _channels) {
                if (!channel->is_receiver_eof()) {
                    if (channel->is_local()) {
                        status = channel->send_local_block(block);
                    } else {
                        SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
                        status = channel->send_block(_cur_pb_block, eos);
                    }
                    HANDLE_CHANNEL_STATUS(state, channel, status);
                }
            }
            // rollover
            _roll_pb_block();
        }
    } else if (_part_type == TPartitionType::RANDOM) {
        // 1. select channel
        Channel* current_channel = _channels[_current_channel_idx];
        if (!current_channel->is_receiver_eof()) {
            // 2. serialize, send and rollover block
            if (current_channel->is_local()) {
                auto status = current_channel->send_local_block(block);
                HANDLE_CHANNEL_STATUS(state, current_channel, status);
            } else {
                SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
                RETURN_IF_ERROR(serialize_block(block, current_channel->ch_cur_pb_block()));
                auto status = current_channel->send_block(current_channel->ch_cur_pb_block(), eos);
                HANDLE_CHANNEL_STATUS(state, current_channel, status);
                current_channel->ch_roll_pb_block();
            }
        }
        _current_channel_idx = (_current_channel_idx + 1) % _channels.size();
    } else if (_part_type == TPartitionType::HASH_PARTITIONED ||
               _part_type == TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED) {
        // will only copy schema
        // we don't want send temp columns
        auto column_to_keep = block->columns();

        int result_size = _partition_expr_ctxs.size();
        int result[result_size];
        {
            SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
            RETURN_IF_ERROR(get_partition_column_result(block, result));
        }

        // vectorized calculate hash
        int rows = block->rows();
        auto element_size = _channels.size();
        std::vector<uint64_t> hash_vals(rows);
        auto* __restrict hashes = hash_vals.data();

        // TODO: after we support new shuffle hash method, should simple the code
        if (_part_type == TPartitionType::HASH_PARTITIONED) {
            if (!_new_shuffle_hash_method) {
                SCOPED_TIMER(_split_block_hash_compute_timer);
                // for each row, we have a siphash val
                std::vector<SipHash> siphashs(rows);
                // result[j] means column index, i means rows index
                for (int j = 0; j < result_size; ++j) {
                    // complex type most not implement get_data_at() method which column_const will call
                    unpack_if_const(block->get_by_position(result[j]).column)
                            .first->update_hashes_with_value(siphashs);
                }
                for (int i = 0; i < rows; i++) {
                    hashes[i] = siphashs[i].get64() % element_size;
                }
            } else {
                SCOPED_TIMER(_split_block_hash_compute_timer);
                // result[j] means column index, i means rows index, here to calculate the xxhash value
                for (int j = 0; j < result_size; ++j) {
                    // complex type most not implement get_data_at() method which column_const will call
                    unpack_if_const(block->get_by_position(result[j]).column)
                            .first->update_hashes_with_value(hashes);
                }

                for (int i = 0; i < rows; i++) {
                    hashes[i] = hashes[i] % element_size;
                }
            }

            {
                SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
                Block::erase_useless_column(block, column_to_keep);
            }

            RETURN_IF_ERROR(channel_add_rows(state, _channels, element_size, hashes, rows, block));
        } else {
            for (int j = 0; j < result_size; ++j) {
                // complex type most not implement get_data_at() method which column_const will call
                unpack_if_const(block->get_by_position(result[j]).column)
                        .first->update_crcs_with_value(
                                hash_vals, _partition_expr_ctxs[j]->root()->type().type);
            }
            element_size = _channel_shared_ptrs.size();
            for (int i = 0; i < rows; i++) {
                hashes[i] = hashes[i] % element_size;
            }

            {
                SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
                Block::erase_useless_column(block, column_to_keep);
            }
            RETURN_IF_ERROR(channel_add_rows(state, _channel_shared_ptrs, element_size, hashes,
                                             rows, block));
        }
    } else {
        // Range partition
        // 1. calculate range
        // 2. dispatch rows to channel
    }
    return Status::OK();
}

Status VDataStreamSender::try_close(RuntimeState* state, Status exec_status) {
    Status final_st = Status::OK();
    for (int i = 0; i < _channels.size(); ++i) {
        Status st = _channels[i]->close(state);
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
        for (int i = 0; i < _channels.size(); ++i) {
            Status st = _channels[i]->close(state);
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
    DataSink::close(state, exec_status);
    return final_st;
}

Status VDataStreamSender::serialize_block(Block* src, PBlock* dest, int num_receivers) {
    {
        SCOPED_TIMER(_serialize_batch_timer);
        dest->Clear();
        size_t uncompressed_bytes = 0, compressed_bytes = 0;
        RETURN_IF_ERROR(src->serialize(_state->be_exec_version(), dest, &uncompressed_bytes,
                                       &compressed_bytes, _compression_type,
                                       _transfer_large_data_by_brpc));
        COUNTER_UPDATE(_bytes_sent_counter, compressed_bytes * num_receivers);
        COUNTER_UPDATE(_uncompressed_bytes_counter, uncompressed_bytes * num_receivers);
        COUNTER_UPDATE(_compress_timer, src->get_compress_time());
    }

    return Status::OK();
}

void VDataStreamSender::_roll_pb_block() {
    _cur_pb_block = (_cur_pb_block == &_pb_block1 ? &_pb_block2 : &_pb_block1);
}

Status VDataStreamSender::_get_next_available_buffer(BroadcastPBlockHolder** holder) {
    DCHECK(_broadcast_pb_blocks[_broadcast_pb_block_idx].available());
    *holder = &_broadcast_pb_blocks[_broadcast_pb_block_idx];
    _broadcast_pb_block_idx++;
    return Status::OK();
}

void VDataStreamSender::registe_channels(pipeline::ExchangeSinkBuffer* buffer) {
    for (auto channel : _channels) {
        ((PipChannel*)channel)->registe(buffer);
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

} // namespace doris::vectorized
