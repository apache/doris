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

#include "runtime/client_cache.h"
#include "runtime/dpp_sink_internal.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
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
    _brpc_request.set_node_id(_dest_node_id);
    _brpc_request.set_sender_id(_parent->_sender_id);
    _brpc_request.set_be_number(_be_number);

    _brpc_timeout_ms = std::min(3600, state->query_options().query_timeout) * 1000;
    _brpc_stub = state->exec_env()->brpc_internal_client_cache()->get_client(_brpc_dest_addr);

    if (_brpc_dest_addr.hostname == BackendOptions::get_localhost()) {
        _brpc_stub = state->exec_env()->brpc_internal_client_cache()->get_client(
                "127.0.0.1", _brpc_dest_addr.port);
    } else {
        _brpc_stub = state->exec_env()->brpc_internal_client_cache()->get_client(_brpc_dest_addr);
    }

    // In bucket shuffle join will set fragment_instance_id (-1, -1)
    // to build a camouflaged empty channel. the ip and port is '0.0.0.0:0"
    // so the empty channel not need call function close_internal()
    _need_close = (_fragment_instance_id.hi != -1 && _fragment_instance_id.lo != -1);
    return Status::OK();
}

Status VDataStreamSender::Channel::send_current_block(bool eos) {
    // TODO: Now, local exchange will cause the performance problem is in a multi-threaded scenario
    //  so this feature is turned off here. We need to re-examine this logic
    //    if (is_local()) {
    //        return send_local_block(eos);
    //    }
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
    if (recvr != nullptr) {
        Block block = _mutable_block->to_block();
        COUNTER_UPDATE(_parent->_local_bytes_send_counter, block.bytes());
        recvr->add_block(&block, _parent->_sender_id, true);
        if (eos) {
            recvr->remove_sender(_parent->_sender_id, _be_number);
        }
    }
    _mutable_block->clear();
    return Status::OK();
}

Status VDataStreamSender::Channel::send_local_block(Block* block) {
    std::shared_ptr<VDataStreamRecvr> recvr =
            _parent->state()->exec_env()->vstream_mgr()->find_recvr(_fragment_instance_id,
                                                                    _dest_node_id);
    if (recvr != nullptr) {
        COUNTER_UPDATE(_parent->_local_bytes_send_counter, block->bytes());
        recvr->add_block(block, _parent->_sender_id, false);
    }
    return Status::OK();
}

Status VDataStreamSender::Channel::send_block(PBlock* block, bool eos) {
    if (_closure == nullptr) {
        _closure = new RefCountClosure<PTransmitDataResult>();
        _closure->ref();
    } else {
        RETURN_IF_ERROR(_wait_last_brpc());
        _closure->cntl.Reset();
    }
    VLOG_ROW << "Channel::send_batch() instance_id=" << _fragment_instance_id
             << " dest_node=" << _dest_node_id;
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

    if (_brpc_request.has_block()) {
        request_block_transfer_attachment<PTransmitDataParams,
            RefCountClosure<PTransmitDataResult>>(&_brpc_request, _parent->_column_values_buffer,
                    _closure);
    }

    _brpc_stub->transmit_block(&_closure->cntl, &_brpc_request, &_closure->result, _closure);
    if (block != nullptr) {
        _brpc_request.release_block();
    }
    return Status::OK();
}

Status VDataStreamSender::Channel::add_row(Block* block, int row) {
    if (_fragment_instance_id.lo == -1) {
        return Status::OK();
    }

    if (_mutable_block.get() == nullptr) {
        _mutable_block.reset(new MutableBlock(block->clone_empty()));
    }
    _mutable_block->add_row(block, row);

    if (_mutable_block->rows() == _parent->state()->batch_size()) {
        RETURN_IF_ERROR(send_current_block());
    }
    return Status::OK();
}

Status VDataStreamSender::Channel::add_rows(Block* block, const std::vector<int>& rows) {
    if (_fragment_instance_id.lo == -1) {
        return Status::OK();
    }

    if (_mutable_block.get() == nullptr) {
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
          _dest_node_id(sink.dest_node_id) {
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
    _profile = _pool->add(new RuntimeProfile(std::move(title)));
    SCOPED_TIMER(_profile->total_time_counter());
    _mem_tracker = MemTracker::create_tracker(
            -1, "VDataStreamSender:" + print_id(state->fragment_instance_id()),
            state->instance_mem_tracker(), MemTrackerLevel::VERBOSE, _profile);

    if (_part_type == TPartitionType::UNPARTITIONED || _part_type == TPartitionType::RANDOM) {
        std::random_device rd;
        std::mt19937 g(rd());
        shuffle(_channels.begin(), _channels.end(), g);
    } else if (_part_type == TPartitionType::HASH_PARTITIONED ||
               _part_type == TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED) {
        RETURN_IF_ERROR(VExpr::prepare(_partition_expr_ctxs, state, _row_desc, _expr_mem_tracker));
    } else {
        RETURN_IF_ERROR(VExpr::prepare(_partition_expr_ctxs, state, _row_desc, _expr_mem_tracker));
        for (auto iter : _partition_infos) {
            RETURN_IF_ERROR(iter->prepare(state, _row_desc, _expr_mem_tracker));
        }
    }

    _bytes_sent_counter = ADD_COUNTER(profile(), "BytesSent", TUnit::BYTES);
    _uncompressed_bytes_counter = ADD_COUNTER(profile(), "UncompressedRowBatchSize", TUnit::BYTES);
    _ignore_rows = ADD_COUNTER(profile(), "IgnoreRows", TUnit::UNIT);
    _serialize_batch_timer = ADD_TIMER(profile(), "SerializeBatchTime");
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
    DCHECK(state != nullptr);
    RETURN_IF_ERROR(VExpr::open(_partition_expr_ctxs, state));
    for (auto iter : _partition_infos) {
        RETURN_IF_ERROR(iter->open(state));
    }
    return Status::OK();
}

Status VDataStreamSender::send(RuntimeState* state, RowBatch* batch) {
    return Status::NotSupported("Not Implemented VOlapScanNode Node::get_next scalar");
}

Status VDataStreamSender::send(RuntimeState* state, Block* block) {
    SCOPED_TIMER(_profile->total_time_counter());
    if (_part_type == TPartitionType::UNPARTITIONED || _channels.size() == 1) {
        // 1. serialize depends on it is not local exchange
        // 2. send block
        // 3. rollover block
        int local_size = 0;
        for (auto channel : _channels) {
            if (channel->is_local()) local_size++;
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
    } else if (_part_type == TPartitionType::HASH_PARTITIONED) {
        int num_channels = _channels.size();
        // will only copy schema
        // we don't want send temp columns

        int result_size = _partition_expr_ctxs.size();
        int result[result_size];
        RETURN_IF_ERROR(get_partition_column_result(block, result));

        // vectorized calculate hash
        int rows = block->rows();
        // for each row, we have a siphash val
        std::vector<SipHash> siphashs(rows);
        // result[j] means column index, i means rows index
        for (int j = 0; j < result_size; ++j) {
            auto column = block->get_by_position(result[j]).column;
            for (int i = 0; i < rows; ++i) {
                column->update_hash_with_value(i, siphashs[i]);
            }
        }

        // channel2rows' subscript means channel id
        std::vector<vectorized::UInt64> hash_vals(rows);
        for (int i = 0; i < rows; i++) {
            hash_vals[i] = siphashs[i].get64();
        }

        RETURN_IF_ERROR(channel_add_rows(_channels, num_channels, hash_vals, rows, block));
    } else if (_part_type == TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED) {
        // 1. calculate hash
        // 2. dispatch rows to channel
        int num_channels = _channel_shared_ptrs.size();

        int result_size = _partition_expr_ctxs.size();
        int result[result_size];
        RETURN_IF_ERROR(get_partition_column_result(block, result));

        // vectorized calculate hash val
        int rows = block->rows();
        // for each row, we have a hash_val
        std::vector<size_t> hash_vals(rows);

        // result[j] means column index, i means rows index
        for (int j = 0; j < result_size; ++j) {
            auto& column = block->get_by_position(result[j]).column;
            for (int i = 0; i < rows; ++i) {
                auto val = column->get_data_at(i);
                if (val.data == nullptr) {
                    // nullptr is treat as 0 when hash
                    static const int INT_VALUE = 0;
                    static const TypeDescriptor INT_TYPE(TYPE_INT);
                    hash_vals[i] = RawValue::zlib_crc32(&INT_VALUE, INT_TYPE, hash_vals[i]);
                } else {
                    hash_vals[i] = RawValue::zlib_crc32(val.data, val.size,
                                                        _partition_expr_ctxs[j]->root()->type(),
                                                        hash_vals[i]);
                }
            }
        }

        RETURN_IF_ERROR(
                channel_add_rows(_channel_shared_ptrs, num_channels, hash_vals, rows, block));
    } else {
        // Range partition
        // 1. calculate range
        // 2. dispatch rows to channel
    }
    return Status::OK();
}

Status VDataStreamSender::close(RuntimeState* state, Status exec_status) {
    if (_closed) return Status::OK();
    _closed = true;

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
    return final_st;
}

Status VDataStreamSender::serialize_block(Block* src, PBlock* dest, int num_receivers) {
    {
        SCOPED_TIMER(_serialize_batch_timer);
        dest->Clear();
        size_t uncompressed_bytes = 0, compressed_bytes = 0;
        RETURN_IF_ERROR(src->serialize(dest, &uncompressed_bytes, &compressed_bytes, &_column_values_buffer));
        COUNTER_UPDATE(_bytes_sent_counter, compressed_bytes * num_receivers);
        COUNTER_UPDATE(_uncompressed_bytes_counter, uncompressed_bytes * num_receivers);
    }

    return Status::OK();
}

void VDataStreamSender::_roll_pb_block() {
    _cur_pb_block = (_cur_pb_block == &_pb_block1 ? &_pb_block2 : &_pb_block1);
}

} // namespace doris::vectorized
