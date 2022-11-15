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
#include <fmt/ranges.h>

#include <random>

#include "runtime/dpp_sink_internal.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "util/brpc_client_cache.h"
#include "util/proto_util.h"
#include "vec/common/sip_hash.h"
#include "vec/runtime/vdata_stream_mgr.h"
#include "vec/runtime/vdata_stream_recvr.h"
#include "vec/runtime/vpartition_info.h"

namespace doris::vectorized {

Status VDataStreamSender::Channel::init(RuntimeState* state) {
    _be_number = state->be_number();

    _capacity = std::max(1, _buffer_size / std::max(_row_desc.get_row_size(), 1));

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

    _brpc_timeout_ms = std::min(3600, state->query_options().query_timeout) * 1000;

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
        _enable_local_exchange = state->query_options().enable_local_exchange;
    }

    // In bucket shuffle join will set fragment_instance_id (-1, -1)
    // to build a camouflaged empty channel. the ip and port is '0.0.0.0:0"
    // so the empty channel not need call function close_internal()
    _need_close = (_fragment_instance_id.hi != -1 && _fragment_instance_id.lo != -1);
    _state = state;
    return Status::OK();
}

Status VDataStreamSender::Channel::send_current_block(bool eos) {
    // FIXME: Now, local exchange will cause the performance problem is in a multi-threaded scenario
    // so this feature is turned off here by default. We need to re-examine this logic
    if (_enable_local_exchange && is_local()) {
        return send_local_block(eos);
    }
    auto block = _mutable_block->to_block();
    RETURN_IF_ERROR(_parent->serialize_block(&block, _ch_cur_pb_block));
    block.clear_column_data();
    _mutable_block->set_muatable_columns(block.mutate_columns());
    RETURN_IF_ERROR(send_block(_ch_cur_pb_block, eos));
    ch_roll_pb_block();
    return Status::OK();
}

Status VDataStreamSender::Channel::send_local_block(bool eos) {
    std::shared_ptr<VDataStreamRecvr> recvr =
            _parent->state()->exec_env()->vstream_mgr()->find_recvr(_fragment_instance_id,
                                                                    _dest_node_id);
    Block block = _mutable_block->to_block();
    _mutable_block->set_muatable_columns(block.clone_empty_columns());
    if (recvr != nullptr) {
        COUNTER_UPDATE(_parent->_local_bytes_send_counter, block.bytes());
        COUNTER_UPDATE(_parent->_local_sent_rows, block.rows());
        recvr->add_block(&block, _parent->_sender_id, true);
        if (eos) {
            recvr->remove_sender(_parent->_sender_id, _be_number);
        }
    }
    return Status::OK();
}

Status VDataStreamSender::Channel::send_local_block(Block* block) {
    std::shared_ptr<VDataStreamRecvr> recvr =
            _parent->state()->exec_env()->vstream_mgr()->find_recvr(_fragment_instance_id,
                                                                    _dest_node_id);
    if (recvr != nullptr) {
        COUNTER_UPDATE(_parent->_local_bytes_send_counter, block->bytes());
        COUNTER_UPDATE(_parent->_local_sent_rows, block->rows());
        recvr->add_block(block, _parent->_sender_id, false);
    }
    return Status::OK();
}

Status VDataStreamSender::Channel::send_block(PBlock* block, bool eos) {
    SCOPED_TIMER(_parent->_brpc_send_timer);
    if (_closure == nullptr) {
        _closure = new RefCountClosure<PTransmitDataResult>();
        _closure->ref();
    } else {
        RETURN_IF_ERROR(_wait_last_brpc());
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(ExecEnv::GetInstance()->orphan_mem_tracker());
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

    if (_parent->_transfer_large_data_by_brpc && _brpc_request.has_block() &&
        _brpc_request.block().has_column_values() &&
        _brpc_request.ByteSizeLong() > MIN_HTTP_BRPC_SIZE) {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(ExecEnv::GetInstance()->orphan_mem_tracker());
        Status st = request_embed_attachment_contain_block<PTransmitDataParams,
                                                           RefCountClosure<PTransmitDataResult>>(
                &_brpc_request, _closure);
        RETURN_IF_ERROR(st);
        std::string brpc_url =
                fmt::format("http://{}:{}", _brpc_dest_addr.hostname, _brpc_dest_addr.port);
        std::shared_ptr<PBackendService_Stub> _brpc_http_stub =
                _state->exec_env()->brpc_internal_client_cache()->get_new_client_no_cache(brpc_url,
                                                                                          "http");
        _closure->cntl.http_request().uri() =
                brpc_url + "/PInternalServiceImpl/transmit_block_by_http";
        _closure->cntl.http_request().set_method(brpc::HTTP_METHOD_POST);
        _closure->cntl.http_request().set_content_type("application/json");
        _brpc_http_stub->transmit_block_by_http(&_closure->cntl, nullptr, &_closure->result,
                                                _closure);
    } else {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(ExecEnv::GetInstance()->orphan_mem_tracker());
        _closure->cntl.http_request().Clear();
        _brpc_stub->transmit_block(&_closure->cntl, &_brpc_request, &_closure->result, _closure);
    }
    if (block != nullptr) {
        _brpc_request.release_block();
    }
    return Status::OK();
}

Status VDataStreamSender::Channel::add_rows(Block* block, const std::vector<int>& rows) {
    if (_fragment_instance_id.lo == -1) {
        return Status::OK();
    }

    if (_mutable_block == nullptr) {
        _mutable_block.reset(new MutableBlock(block->clone_empty()));
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

        _mutable_block->add_rows(block, begin, begin + row_add);

        row_wait_add -= row_add;
        begin += row_add;

        if (row_add == max_add) {
            RETURN_IF_ERROR(send_current_block());
        }
    }

    return Status::OK();
}

Status VDataStreamSender::Channel::close_wait(RuntimeState* state) {
    if (_need_close) {
        Status st = _wait_last_brpc();
        if (!st.ok()) {
            state->log_error(st.get_error_msg());
        }
        _need_close = false;
        return st;
    }
    _mutable_block.reset();
    return Status::OK();
}

Status VDataStreamSender::Channel::close_internal() {
    if (!_need_close) {
        return Status::OK();
    }
    VLOG_RPC << "Channel::close() instance_id=" << _fragment_instance_id
             << " dest_node=" << _dest_node_id
             << " #rows= " << ((_mutable_block == nullptr) ? 0 : _mutable_block->rows());
    if (_mutable_block != nullptr && _mutable_block->rows() > 0) {
        RETURN_IF_ERROR(send_current_block(true));
    } else {
        RETURN_IF_ERROR(send_block(nullptr, true));
    }
    // Don't wait for the last packet to finish, left it to close_wait.
    return Status::OK();
}

Status VDataStreamSender::Channel::close(RuntimeState* state) {
    Status st = close_internal();
    if (!st.ok()) {
        state->log_error(st.get_error_msg());
    }
    return st;
}

void VDataStreamSender::Channel::ch_roll_pb_block() {
    _ch_cur_pb_block = (_ch_cur_pb_block == &_ch_pb_block1 ? &_ch_pb_block2 : &_ch_pb_block1);
}

VDataStreamSender::VDataStreamSender(ObjectPool* pool, int sender_id, const RowDescriptor& row_desc,
                                     const TDataStreamSink& sink,
                                     const std::vector<TPlanFragmentDestination>& destinations,
                                     int per_channel_buffer_size,
                                     bool send_query_statistics_with_every_batch)
        : _sender_id(sender_id),
          _pool(pool),
          _row_desc(row_desc),
          _current_channel_idx(0),
          _part_type(sink.output_partition.type),
          _ignore_not_found(sink.__isset.ignore_not_found ? sink.ignore_not_found : true),
          _cur_pb_block(&_pb_block1),
          _profile(nullptr),
          _serialize_batch_timer(nullptr),
          _bytes_sent_counter(nullptr),
          _local_bytes_send_counter(nullptr),
          _dest_node_id(sink.dest_node_id),
          _transfer_large_data_by_brpc(config::transfer_large_data_by_brpc) {
    DCHECK_GT(destinations.size(), 0);
    DCHECK(sink.output_partition.type == TPartitionType::UNPARTITIONED ||
           sink.output_partition.type == TPartitionType::HASH_PARTITIONED ||
           sink.output_partition.type == TPartitionType::RANDOM ||
           sink.output_partition.type == TPartitionType::RANGE_PARTITIONED ||
           sink.output_partition.type == TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED);
    //
    std::map<int64_t, int64_t> fragment_id_to_channel_index;

    for (int i = 0; i < destinations.size(); ++i) {
        // Select first dest as transfer chain.
        bool is_transfer_chain = (i == 0);
        const auto& fragment_instance_id = destinations[i].fragment_instance_id;
        if (fragment_id_to_channel_index.find(fragment_instance_id.lo) ==
            fragment_id_to_channel_index.end()) {
            _channel_shared_ptrs.emplace_back(
                    new Channel(this, row_desc, destinations[i].brpc_server, fragment_instance_id,
                                sink.dest_node_id, per_channel_buffer_size, is_transfer_chain,
                                send_query_statistics_with_every_batch));
            fragment_id_to_channel_index.emplace(fragment_instance_id.lo,
                                                 _channel_shared_ptrs.size() - 1);
            _channels.push_back(_channel_shared_ptrs.back().get());
        } else {
            _channel_shared_ptrs.emplace_back(
                    _channel_shared_ptrs[fragment_id_to_channel_index[fragment_instance_id.lo]]);
        }
    }
    _name = "VDataStreamSender";
}

VDataStreamSender::VDataStreamSender(ObjectPool* pool, int sender_id, const RowDescriptor& row_desc,
                                     const std::vector<TPlanFragmentDestination>& destinations,
                                     int per_channel_buffer_size,
                                     bool send_query_statistics_with_every_batch)
        : _sender_id(sender_id),
          _pool(pool),
          _row_desc(row_desc),
          _current_channel_idx(0),
          _ignore_not_found(true),
          _cur_pb_block(&_pb_block1),
          _profile(nullptr),
          _serialize_batch_timer(nullptr),
          _compress_timer(nullptr),
          _brpc_send_timer(nullptr),
          _brpc_wait_timer(nullptr),
          _bytes_sent_counter(nullptr),
          _local_bytes_send_counter(nullptr),
          _dest_node_id(0) {
    _name = "VDataStreamSender";
}

VDataStreamSender::VDataStreamSender(ObjectPool* pool, const RowDescriptor& row_desc,
                                     int per_channel_buffer_size,
                                     bool send_query_statistics_with_every_batch)
        : _sender_id(0),
          _pool(pool),
          _row_desc(row_desc),
          _current_channel_idx(0),
          _ignore_not_found(true),
          _cur_pb_block(&_pb_block1),
          _profile(nullptr),
          _serialize_batch_timer(nullptr),
          _compress_timer(nullptr),
          _brpc_send_timer(nullptr),
          _brpc_wait_timer(nullptr),
          _bytes_sent_counter(nullptr),
          _local_bytes_send_counter(nullptr),
          _dest_node_id(0) {
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
        RETURN_IF_ERROR(VExpr::create_expr_trees(
                _pool, t_stream_sink.output_partition.partition_exprs, &_partition_expr_ctxs));
    } else if (_part_type == TPartitionType::RANGE_PARTITIONED) {
        // Range partition
        // Partition Exprs
        RETURN_IF_ERROR(VExpr::create_expr_trees(
                _pool, t_stream_sink.output_partition.partition_exprs, &_partition_expr_ctxs));
        // Partition infos
        int num_parts = t_stream_sink.output_partition.partition_infos.size();
        if (num_parts == 0) {
            return Status::InternalError("Empty partition info.");
        }
        for (int i = 0; i < num_parts; ++i) {
            VPartitionInfo* info = _pool->add(new VPartitionInfo());
            RETURN_IF_ERROR(VPartitionInfo::from_thrift(
                    _pool, t_stream_sink.output_partition.partition_infos[i], info));
            _partition_infos.push_back(info);
        }
        // partitions should be in ascending order
        std::sort(_partition_infos.begin(), _partition_infos.end(),
                  [](const VPartitionInfo* v1, const VPartitionInfo* v2) {
                      return v1->range() < v2->range();
                  });
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
    _mem_tracker = std::make_unique<MemTracker>(
            "VDataStreamSender:" + print_id(state->fragment_instance_id()), _profile);
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
        for (auto iter : _partition_infos) {
            RETURN_IF_ERROR(iter->prepare(state, _row_desc));
        }
    }

    _bytes_sent_counter = ADD_COUNTER(profile(), "BytesSent", TUnit::BYTES);
    _uncompressed_bytes_counter = ADD_COUNTER(profile(), "UncompressedRowBatchSize", TUnit::BYTES);
    _ignore_rows = ADD_COUNTER(profile(), "IgnoreRows", TUnit::UNIT);
    _local_sent_rows = ADD_COUNTER(profile(), "LocalSentRows", TUnit::UNIT);
    _serialize_batch_timer = ADD_TIMER(profile(), "SerializeBatchTime");
    _compress_timer = ADD_TIMER(profile(), "CompressTime");
    _brpc_send_timer = ADD_TIMER(profile(), "BrpcSendTime");
    _brpc_wait_timer = ADD_TIMER(profile(), "BrpcSendTime.Wait");
    _overall_throughput = profile()->add_derived_counter(
            "OverallThroughput", TUnit::BYTES_PER_SECOND,
            std::bind<int64_t>(&RuntimeProfile::units_per_second, _bytes_sent_counter,
                               profile()->total_time_counter()),
            "");
    _local_bytes_send_counter = ADD_COUNTER(profile(), "LocalBytesSent", TUnit::BYTES);
    for (int i = 0; i < _channels.size(); ++i) {
        RETURN_IF_ERROR(_channels[i]->init(state));
    }
    return Status::OK();
}

Status VDataStreamSender::open(RuntimeState* state) {
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VDataStreamSender::open");
    DCHECK(state != nullptr);
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
    RETURN_IF_ERROR(VExpr::open(_partition_expr_ctxs, state));
    for (auto iter : _partition_infos) {
        RETURN_IF_ERROR(iter->open(state));
    }

    _compression_type = state->fragement_transmission_compression_type();
    return Status::OK();
}

Status VDataStreamSender::send(RuntimeState* state, RowBatch* batch) {
    return Status::NotSupported("Not Implemented VOlapScanNode Node::get_next scalar");
}

Status VDataStreamSender::send(RuntimeState* state, Block* block) {
    INIT_AND_SCOPE_SEND_SPAN(state->get_tracer(), _send_span, "VDataStreamSender::send")
    SCOPED_TIMER(_profile->total_time_counter());
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
    if (_part_type == TPartitionType::UNPARTITIONED || _channels.size() == 1) {
        // 1. serialize depends on it is not local exchange
        // 2. send block
        // 3. rollover block
        int local_size = 0;
        for (auto channel : _channels) {
            if (channel->is_local()) {
                local_size++;
            }
        }
        if (local_size == _channels.size()) {
            for (auto channel : _channels) {
                RETURN_IF_ERROR(channel->send_local_block(block));
            }
        } else {
            RETURN_IF_ERROR(serialize_block(block, _cur_pb_block, _channels.size()));
            for (auto channel : _channels) {
                if (channel->is_local()) {
                    RETURN_IF_ERROR(channel->send_local_block(block));
                } else {
                    RETURN_IF_ERROR(channel->send_block(_cur_pb_block));
                }
            }
            // rollover
            _roll_pb_block();
        }
    } else if (_part_type == TPartitionType::RANDOM) {
        // 1. select channel
        Channel* current_channel = _channels[_current_channel_idx];
        // 2. serialize, send and rollover block
        if (current_channel->is_local()) {
            RETURN_IF_ERROR(current_channel->send_local_block(block));
        } else {
            RETURN_IF_ERROR(serialize_block(block, current_channel->ch_cur_pb_block()));
            RETURN_IF_ERROR(current_channel->send_block(current_channel->ch_cur_pb_block()));
            current_channel->ch_roll_pb_block();
        }
        _current_channel_idx = (_current_channel_idx + 1) % _channels.size();
    } else if (_part_type == TPartitionType::HASH_PARTITIONED ||
               _part_type == TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED) {
        // will only copy schema
        // we don't want send temp columns
        auto column_to_keep = block->columns();

        int result_size = _partition_expr_ctxs.size();
        int result[result_size];
        RETURN_IF_ERROR(get_partition_column_result(block, result));

        // vectorized calculate hash
        int rows = block->rows();
        auto element_size = _channels.size();
        std::vector<uint64_t> hash_vals(rows);
        auto* __restrict hashes = hash_vals.data();

        // TODO: after we support new shuffle hash method, should simple the code
        if (_part_type == TPartitionType::HASH_PARTITIONED) {
            if (!_new_shuffle_hash_method) {
                // for each row, we have a siphash val
                std::vector<SipHash> siphashs(rows);
                // result[j] means column index, i means rows index
                for (int j = 0; j < result_size; ++j) {
                    block->get_by_position(result[j]).column->update_hashes_with_value(siphashs);
                }
                for (int i = 0; i < rows; i++) {
                    hashes[i] = siphashs[i].get64() % element_size;
                }
            } else {
                // result[j] means column index, i means rows index, here to calculate the xxhash value
                for (int j = 0; j < result_size; ++j) {
                    block->get_by_position(result[j]).column->update_hashes_with_value(hashes);
                }

                for (int i = 0; i < rows; i++) {
                    hashes[i] = hashes[i] % element_size;
                }
            }

            Block::erase_useless_column(block, column_to_keep);
            RETURN_IF_ERROR(channel_add_rows(_channels, element_size, hashes, rows, block));
        } else {
            for (int j = 0; j < result_size; ++j) {
                block->get_by_position(result[j]).column->update_crcs_with_value(
                        hash_vals, _partition_expr_ctxs[j]->root()->type().type);
            }
            element_size = _channel_shared_ptrs.size();
            for (int i = 0; i < rows; i++) {
                hashes[i] = hashes[i] % element_size;
            }

            Block::erase_useless_column(block, column_to_keep);
            RETURN_IF_ERROR(
                    channel_add_rows(_channel_shared_ptrs, element_size, hashes, rows, block));
        }
    } else {
        // Range partition
        // 1. calculate range
        // 2. dispatch rows to channel
    }
    return Status::OK();
}

Status VDataStreamSender::close(RuntimeState* state, Status exec_status) {
    if (_closed) {
        return Status::OK();
    }

    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VDataStreamSender::close");
    Status final_st = Status::OK();
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
    for (auto iter : _partition_infos) {
        iter->close(state);
    }
    VExpr::close(_partition_expr_ctxs, state);
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

} // namespace doris::vectorized
