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

#include "runtime/data_stream_sender.h"

#include <arpa/inet.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <algorithm>
#include <boost/shared_ptr.hpp>
#include <boost/thread/thread.hpp>
#include <iostream>

#include "common/config.h"
#include "common/logging.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "gen_cpp/BackendService.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/client_cache.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/data_stream_recvr.h"
#include "runtime/descriptors.h"
#include "runtime/dpp_sink_internal.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/raw_value.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple_row.h"
#include "service/backend_options.h"
#include "service/brpc.h"
#include "util/brpc_stub_cache.h"
#include "util/debug_util.h"
#include "util/defer_op.h"
#include "util/network_util.h"
#include "util/ref_count_closure.h"
#include "util/thrift_client.h"
#include "util/thrift_util.h"

namespace doris {

// A channel sends data asynchronously via calls to transmit_data
// to a single destination ipaddress/node.
// It has a fixed-capacity buffer and allows the caller either to add rows to
// that buffer individually (AddRow()), or circumvent the buffer altogether and send
// TRowBatches directly (SendBatch()). Either way, there can only be one in-flight RPC
// at any one time (ie, sending will block if the most recent rpc hasn't finished,
// which allows the receiver node to throttle the sender by withholding acks).
// *Not* thread-safe.
class DataStreamSender::Channel {
public:
    // Create channel to send data to particular ipaddress/port/query/node
    // combination. buffer_size is specified in bytes and a soft limit on
    // how much tuple data is getting accumulated before being sent; it only applies
    // when data is added via add_row() and not sent directly via send_batch().
    Channel(DataStreamSender* parent, const RowDescriptor& row_desc,
            const TNetworkAddress& brpc_dest, const TUniqueId& fragment_instance_id,
            PlanNodeId dest_node_id, int buffer_size, bool is_transfer_chain,
            bool send_query_statistics_with_every_batch)
            : _parent(parent),
              _buffer_size(buffer_size),
              _row_desc(row_desc),
              _fragment_instance_id(fragment_instance_id),
              _dest_node_id(dest_node_id),
              _num_data_bytes_sent(0),
              _packet_seq(0),
              _need_close(false),
              _be_number(0),
              _brpc_dest_addr(brpc_dest),
              _is_transfer_chain(is_transfer_chain),
              _send_query_statistics_with_every_batch(send_query_statistics_with_every_batch) {
        std::string localhost = BackendOptions::get_localhost();
        _is_local =
                _brpc_dest_addr.hostname == localhost && _brpc_dest_addr.port == config::brpc_port;
        if (_is_local) {
            LOG(INFO) << "will use local exechange, dest_node_id:" << _dest_node_id;
        }
    }

    virtual ~Channel() {
        if (_closure != nullptr && _closure->unref()) {
            delete _closure;
        }
        // release this before request desctruct
        _brpc_request.release_finst_id();
    }

    // Initialize channel.
    // Returns OK if successful, error indication otherwise.
    Status init(RuntimeState* state);

    // Copies a single row into this channel's output buffer and flushes buffer
    // if it reaches capacity.
    // Returns error status if any of the preceding rpcs failed, OK otherwise.
    Status add_row(TupleRow* row);

    // Asynchronously sends a row batch.
    // Returns the status of the most recently finished transmit_data
    // rpc (or OK if there wasn't one that hasn't been reported yet).
    // if batch is nullptr, send the eof packet
    Status send_batch(PRowBatch* batch, bool eos = false);

    Status send_local_batch(bool eos);

    Status send_local_batch(RowBatch* batch, bool use_move);

    // Flush buffered rows and close channel. This function don't wait the response
    // of close operation, client should call close_wait() to finish channel's close.
    // We split one close operation into two phases in order to make multiple channels
    // can run parallel.
    Status close(RuntimeState* state);

    // Get close wait's response, to finish channel close operation.
    Status close_wait(RuntimeState* state);

    int64_t num_data_bytes_sent() const { return _num_data_bytes_sent; }

    PRowBatch* pb_batch() { return &_pb_batch; }

    std::string get_fragment_instance_id_str() {
        UniqueId uid(_fragment_instance_id);
        return uid.to_string();
    }

    TUniqueId get_fragment_instance_id() { return _fragment_instance_id; }

    bool is_local() { return _is_local; }

private:
    inline Status _wait_last_brpc() {
        if (_closure == nullptr) return Status::OK();
        auto cntl = &_closure->cntl;
        brpc::Join(cntl->call_id());
        if (cntl->Failed()) {
            std::stringstream ss;
            ss << "failed to send brpc batch, error=" << berror(cntl->ErrorCode())
               << ", error_text=" << cntl->ErrorText()
               << ", client: " << BackendOptions::get_localhost();
            LOG(WARNING) << ss.str();
            return Status::ThriftRpcError(ss.str());
        }
        return Status::OK();
    }

private:
    // Serialize _batch into _thrift_batch and send via send_batch().
    // Returns send_batch() status.
    Status send_current_batch(bool eos = false);
    Status close_internal();

    DataStreamSender* _parent;
    int _buffer_size;

    const RowDescriptor& _row_desc;
    TUniqueId _fragment_instance_id;
    PlanNodeId _dest_node_id;

    // the number of TRowBatch.data bytes sent successfully
    int64_t _num_data_bytes_sent;
    int64_t _packet_seq;

    // we're accumulating rows into this batch
    boost::scoped_ptr<RowBatch> _batch;

    bool _need_close;
    int _be_number;

    TNetworkAddress _brpc_dest_addr;

    // TODO(zc): initused for brpc
    PUniqueId _finst_id;
    PRowBatch _pb_batch;
    PTransmitDataParams _brpc_request;
    PBackendService_Stub* _brpc_stub = nullptr;
    RefCountClosure<PTransmitDataResult>* _closure = nullptr;
    int32_t _brpc_timeout_ms = 500;
    // whether the dest can be treated as query statistics transfer chain.
    bool _is_transfer_chain;
    bool _send_query_statistics_with_every_batch;
    bool _is_local;
};

Status DataStreamSender::Channel::init(RuntimeState* state) {
    _be_number = state->be_number();

    // TODO: figure out how to size _batch
    int capacity = std::max(1, _buffer_size / std::max(_row_desc.get_row_size(), 1));
    _batch.reset(new RowBatch(_row_desc, capacity, _parent->_mem_tracker.get()));

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
    if (_brpc_dest_addr.hostname == BackendOptions::get_localhost()) {
        _brpc_stub =
                state->exec_env()->brpc_stub_cache()->get_stub("127.0.0.1", _brpc_dest_addr.port);
    } else {
        _brpc_stub = state->exec_env()->brpc_stub_cache()->get_stub(_brpc_dest_addr);
    }

    // In bucket shuffle join will set fragment_instance_id (-1, -1)
    // to build a camouflaged empty channel. the ip and port is '0.0.0.0:0"
    // so the empty channel not need call function close_internal()
    _need_close = (_fragment_instance_id.hi != -1 && _fragment_instance_id.lo != -1);
    return Status::OK();
}

Status DataStreamSender::Channel::send_batch(PRowBatch* batch, bool eos) {
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
    if (batch != nullptr) {
        _brpc_request.set_allocated_row_batch(batch);
    }
    _brpc_request.set_packet_seq(_packet_seq++);

    _closure->ref();
    _closure->cntl.set_timeout_ms(_brpc_timeout_ms);
    _brpc_stub->transmit_data(&_closure->cntl, &_brpc_request, &_closure->result, _closure);
    if (batch != nullptr) {
        _brpc_request.release_row_batch();
    }
    return Status::OK();
}

Status DataStreamSender::Channel::add_row(TupleRow* row) {
    if (_fragment_instance_id.lo == -1) {
        return Status::OK();
    }
    int row_num = _batch->add_row();

    if (row_num == RowBatch::INVALID_ROW_INDEX) {
        // _batch is full, let's send it; but first wait for an ongoing
        // transmission to finish before modifying _thrift_batch
        RETURN_IF_ERROR(send_current_batch());
        row_num = _batch->add_row();
        DCHECK_NE(row_num, RowBatch::INVALID_ROW_INDEX);
    }

    TupleRow* dest = _batch->get_row(row_num);
    _batch->copy_row(row, dest);
    const std::vector<TupleDescriptor*>& descs = _row_desc.tuple_descriptors();

    for (int i = 0; i < descs.size(); ++i) {
        if (UNLIKELY(row->get_tuple(i) == NULL)) {
            dest->set_tuple(i, NULL);
        } else {
            dest->set_tuple(i, row->get_tuple(i)->deep_copy(*descs[i], _batch->tuple_data_pool()));
        }
    }

    _batch->commit_last_row();
    return Status::OK();
}

Status DataStreamSender::Channel::send_current_batch(bool eos) {
    if (is_local()) {
        return send_local_batch(eos);
    }
    {
        SCOPED_TIMER(_parent->_serialize_batch_timer);
        int uncompressed_bytes = _batch->serialize(&_pb_batch);
        COUNTER_UPDATE(_parent->_bytes_sent_counter, RowBatch::get_batch_size(_pb_batch));
        COUNTER_UPDATE(_parent->_uncompressed_bytes_counter, uncompressed_bytes);
    }
    _batch->reset();
    RETURN_IF_ERROR(send_batch(&_pb_batch, eos));
    return Status::OK();
}

Status DataStreamSender::Channel::send_local_batch(bool eos) {
    boost::shared_ptr<DataStreamRecvr> recvr =
            _parent->state()->exec_env()->stream_mgr()->find_recvr(_fragment_instance_id,
                                                                   _dest_node_id);
    if (recvr != nullptr) {
        recvr->add_batch(_batch.get(), _parent->_sender_id, true);
        if (eos) {
            recvr->remove_sender(_parent->_sender_id, _be_number);
        }
    }
    _batch->reset();
    return Status::OK();
}

Status DataStreamSender::Channel::send_local_batch(RowBatch* batch, bool use_move) {
    boost::shared_ptr<DataStreamRecvr> recvr =
            _parent->state()->exec_env()->stream_mgr()->find_recvr(_fragment_instance_id,
                                                                   _dest_node_id);
    if (recvr != nullptr) {
        recvr->add_batch(batch, _parent->_sender_id, use_move);
    }
    return Status::OK();
}

Status DataStreamSender::Channel::close_internal() {
    if (!_need_close) {
        return Status::OK();
    }
    VLOG_RPC << "Channel::close() instance_id=" << _fragment_instance_id
             << " dest_node=" << _dest_node_id
             << " #rows= " << ((_batch == nullptr) ? 0 : _batch->num_rows());
    if (_batch != NULL && _batch->num_rows() > 0) {
        RETURN_IF_ERROR(send_current_batch(true));
    } else {
        RETURN_IF_ERROR(send_batch(nullptr, true));
    }
    // Don't wait for the last packet to finish, left it to close_wait.
    return Status::OK();
}

Status DataStreamSender::Channel::close(RuntimeState* state) {
    Status st = close_internal();
    if (!st.ok()) {
        state->log_error(st.get_error_msg());
    }
    return st;
}

Status DataStreamSender::Channel::close_wait(RuntimeState* state) {
    if (_need_close) {
        Status st = _wait_last_brpc();
        if (!st.ok()) {
            state->log_error(st.get_error_msg());
        }
        _need_close = false;
        return st;
    }
    _batch.reset();
    return Status::OK();
}

DataStreamSender::DataStreamSender(ObjectPool* pool, int sender_id, const RowDescriptor& row_desc,
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
          _current_pb_batch(&_pb_batch1),
          _profile(NULL),
          _serialize_batch_timer(NULL),
          _bytes_sent_counter(NULL),
          _dest_node_id(sink.dest_node_id) {
    DCHECK_GT(destinations.size(), 0);
    DCHECK(sink.output_partition.type == TPartitionType::UNPARTITIONED ||
           sink.output_partition.type == TPartitionType::HASH_PARTITIONED ||
           sink.output_partition.type == TPartitionType::RANDOM ||
           sink.output_partition.type == TPartitionType::RANGE_PARTITIONED ||
           sink.output_partition.type == TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED);
    // TODO: use something like google3's linked_ptr here (scoped_ptr isn't copyable

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
            fragment_id_to_channel_index.insert(
                    {fragment_instance_id.lo, _channel_shared_ptrs.size() - 1});
            _channels.push_back(_channel_shared_ptrs.back().get());
        } else {
            _channel_shared_ptrs.emplace_back(
                    _channel_shared_ptrs[fragment_id_to_channel_index[fragment_instance_id.lo]]);
        }
    }
    _name = "DataStreamSender";
}

// We use the ParttitionRange to compare here. It should not be a member function of PartitionInfo
// class becaurce there are some other member in it.
// TODO: move this to dpp_sink
static bool compare_part_use_range(const PartitionInfo* v1, const PartitionInfo* v2) {
    return v1->range() < v2->range();
}

Status DataStreamSender::init(const TDataSink& tsink) {
    RETURN_IF_ERROR(DataSink::init(tsink));
    const TDataStreamSink& t_stream_sink = tsink.stream_sink;
    if (_part_type == TPartitionType::HASH_PARTITIONED ||
        _part_type == TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED) {
        RETURN_IF_ERROR(Expr::create_expr_trees(
                _pool, t_stream_sink.output_partition.partition_exprs, &_partition_expr_ctxs));
    } else if (_part_type == TPartitionType::RANGE_PARTITIONED) {
        // Range partition
        // Partition Exprs
        RETURN_IF_ERROR(Expr::create_expr_trees(
                _pool, t_stream_sink.output_partition.partition_exprs, &_partition_expr_ctxs));
        // Partition infos
        int num_parts = t_stream_sink.output_partition.partition_infos.size();
        if (num_parts == 0) {
            return Status::InternalError("Empty partition info.");
        }
        for (int i = 0; i < num_parts; ++i) {
            PartitionInfo* info = _pool->add(new PartitionInfo());
            RETURN_IF_ERROR(PartitionInfo::from_thrift(
                    _pool, t_stream_sink.output_partition.partition_infos[i], info));
            _partition_infos.push_back(info);
        }
        // partitions should be in ascending order
        std::sort(_partition_infos.begin(), _partition_infos.end(), compare_part_use_range);
    } else {
    }

    return Status::OK();
}

Status DataStreamSender::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::prepare(state));
    _state = state;
    std::string instances;
    for (const auto& channel : _channels) {
        if (instances.empty()) {
            instances = channel->get_fragment_instance_id_str();
        } else {
            instances += ", ";
            instances += channel->get_fragment_instance_id_str();
        }
    }
    std::stringstream title;
    title << "DataStreamSender (dst_id=" << _dest_node_id << ", dst_fragments=[" << instances
          << "])";
    _profile = _pool->add(new RuntimeProfile(title.str()));
    SCOPED_TIMER(_profile->total_time_counter());
    _mem_tracker = MemTracker::CreateTracker(
            _profile, -1, "DataStreamSender:" + print_id(state->fragment_instance_id()),
            state->instance_mem_tracker());

    if (_part_type == TPartitionType::UNPARTITIONED || _part_type == TPartitionType::RANDOM) {
        // Randomize the order we open/transmit to channels to avoid thundering herd problems.
        srand(reinterpret_cast<uint64_t>(this));
        random_shuffle(_channels.begin(), _channels.end());
    } else if (_part_type == TPartitionType::HASH_PARTITIONED ||
               _part_type == TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED) {
        RETURN_IF_ERROR(Expr::prepare(_partition_expr_ctxs, state, _row_desc, _expr_mem_tracker));
    } else {
        RETURN_IF_ERROR(Expr::prepare(_partition_expr_ctxs, state, _row_desc, _expr_mem_tracker));
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
    for (int i = 0; i < _channels.size(); ++i) {
        RETURN_IF_ERROR(_channels[i]->init(state));
    }

    return Status::OK();
}

DataStreamSender::~DataStreamSender() {
    // TODO: check that sender was either already closed() or there was an error
    // on some channel
    _channel_shared_ptrs.clear();
}

Status DataStreamSender::open(RuntimeState* state) {
    DCHECK(state != NULL);
    RETURN_IF_ERROR(Expr::open(_partition_expr_ctxs, state));
    for (auto iter : _partition_infos) {
        RETURN_IF_ERROR(iter->open(state));
    }
    return Status::OK();
}

Status DataStreamSender::send(RuntimeState* state, RowBatch* batch) {
    SCOPED_TIMER(_profile->total_time_counter());

    // Unpartition or _channel size
    if (_part_type == TPartitionType::UNPARTITIONED || _channels.size() == 1) {
        int local_size = 0;
        for (auto channel : _channels) {
            if (channel->is_local()) {
                local_size++;
            }
        }
        if (local_size == _channels.size()) {
            // we don't have to serialize
            for (auto channel : _channels) {
                RETURN_IF_ERROR(channel->send_local_batch(batch, false));
            }
        } else {
            RETURN_IF_ERROR(serialize_batch(batch, _current_pb_batch, _channels.size()));
            for (auto channel : _channels) {
                if (channel->is_local()) {
                    RETURN_IF_ERROR(channel->send_local_batch(batch, false));
                } else {
                    RETURN_IF_ERROR(channel->send_batch(_current_pb_batch));
                }
            }
            _current_pb_batch = (_current_pb_batch == &_pb_batch1 ? &_pb_batch2 : &_pb_batch1);
        }
    } else if (_part_type == TPartitionType::RANDOM) {
        // Round-robin batches among channels. Wait for the current channel to finish its
        // rpc before overwriting its batch.
        Channel* current_channel = _channels[_current_channel_idx];
        if (current_channel->is_local()) {
            RETURN_IF_ERROR(current_channel->send_local_batch(batch, false));
        } else {
            RETURN_IF_ERROR(serialize_batch(batch, current_channel->pb_batch()));
            RETURN_IF_ERROR(current_channel->send_batch(current_channel->pb_batch()));
        }
        _current_channel_idx = (_current_channel_idx + 1) % _channels.size();
    } else if (_part_type == TPartitionType::HASH_PARTITIONED) {
        // hash-partition batch's rows across channels
        int num_channels = _channels.size();

        for (int i = 0; i < batch->num_rows(); ++i) {
            TupleRow* row = batch->get_row(i);
            size_t hash_val = 0;

            for (auto ctx : _partition_expr_ctxs) {
                void* partition_val = ctx->get_value(row);
                // We can't use the crc hash function here because it does not result
                // in uncorrelated hashes with different seeds.  Instead we must use
                // fvn hash.
                // TODO: fix crc hash/GetHashValue()
                hash_val =
                        RawValue::get_hash_value_fvn(partition_val, ctx->root()->type(), hash_val);
            }
            auto target_channel_id = hash_val % num_channels;
            RETURN_IF_ERROR(_channels[target_channel_id]->add_row(row));
        }
    } else if (_part_type == TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED) {
        // hash-partition batch's rows across channels
        int num_channels = _channel_shared_ptrs.size();

        for (int i = 0; i < batch->num_rows(); ++i) {
            TupleRow* row = batch->get_row(i);
            size_t hash_val = 0;

            for (auto ctx : _partition_expr_ctxs) {
                void* partition_val = ctx->get_value(row);
                // We must use the crc hash function to make sure the hash val equal
                // to left table data distribute hash val
                hash_val = RawValue::zlib_crc32(partition_val, ctx->root()->type(), hash_val);
            }
            auto target_channel_id = hash_val % num_channels;
            RETURN_IF_ERROR(_channel_shared_ptrs[target_channel_id]->add_row(row));
        }
    } else {
        // Range partition
        int num_channels = _channels.size();
        int ignore_rows = 0;
        for (int i = 0; i < batch->num_rows(); ++i) {
            TupleRow* row = batch->get_row(i);
            size_t hash_val = 0;
            bool ignore = false;
            RETURN_IF_ERROR(compute_range_part_code(state, row, &hash_val, &ignore));
            if (ignore) {
                // skip this row
                ignore_rows++;
                continue;
            }
            RETURN_IF_ERROR(_channels[hash_val % num_channels]->add_row(row));
        }
        COUNTER_UPDATE(_ignore_rows, ignore_rows);
    }

    return Status::OK();
}

int DataStreamSender::binary_find_partition(const PartRangeKey& key) const {
    int low = 0;
    int high = _partition_infos.size() - 1;

    VLOG_ROW << "range key: " << key.debug_string() << std::endl;
    while (low <= high) {
        int mid = low + (high - low) / 2;
        int cmp = _partition_infos[mid]->range().compare_key(key);
        if (cmp == 0) {
            return mid;
        } else if (cmp < 0) { // current < partition[mid]
            low = mid + 1;
        } else {
            high = mid - 1;
        }
    }

    return -1;
}

Status DataStreamSender::find_partition(RuntimeState* state, TupleRow* row, PartitionInfo** info,
                                        bool* ignore) {
    if (_partition_expr_ctxs.size() == 0) {
        *info = _partition_infos[0];
        return Status::OK();
    } else {
        *ignore = false;
        // use binary search to get the right partition.
        ExprContext* ctx = _partition_expr_ctxs[0];
        void* partition_val = ctx->get_value(row);
        // construct a PartRangeKey
        PartRangeKey tmpPartKey;
        if (NULL != partition_val) {
            RETURN_IF_ERROR(
                    PartRangeKey::from_value(ctx->root()->type().type, partition_val, &tmpPartKey));
        } else {
            tmpPartKey = PartRangeKey::neg_infinite();
        }

        int part_index = binary_find_partition(tmpPartKey);
        if (part_index < 0) {
            if (_ignore_not_found) {
                // TODO(zc): add counter to compute its
                std::stringstream error_log;
                error_log << "there is no corresponding partition for this key: ";
                ctx->print_value(row, &error_log);
                LOG(INFO) << error_log.str();
                *ignore = true;
                return Status::OK();
            } else {
                std::stringstream error_log;
                error_log << "there is no corresponding partition for this key: ";
                ctx->print_value(row, &error_log);
                return Status::InternalError(error_log.str());
            }
        }
        *info = _partition_infos[part_index];
    }
    return Status::OK();
}

Status DataStreamSender::process_distribute(RuntimeState* state, TupleRow* row,
                                            const PartitionInfo* part, size_t* code) {
    uint32_t hash_val = 0;
    for (auto& ctx : part->distributed_expr_ctxs()) {
        void* partition_val = ctx->get_value(row);
        if (partition_val != NULL) {
            hash_val = RawValue::zlib_crc32(partition_val, ctx->root()->type(), hash_val);
        } else {
            //NULL is treat as 0 when hash
            static const int INT_VALUE = 0;
            static const TypeDescriptor INT_TYPE(TYPE_INT);
            hash_val = RawValue::zlib_crc32(&INT_VALUE, INT_TYPE, hash_val);
        }
    }
    hash_val %= part->distributed_bucket();

    int64_t part_id = part->id();
    *code = RawValue::get_hash_value_fvn(&part_id, TypeDescriptor(TYPE_BIGINT), hash_val);

    return Status::OK();
}

Status DataStreamSender::compute_range_part_code(RuntimeState* state, TupleRow* row,
                                                 size_t* hash_value, bool* ignore) {
    // process partition
    PartitionInfo* part = nullptr;
    RETURN_IF_ERROR(find_partition(state, row, &part, ignore));
    if (*ignore) {
        return Status::OK();
    }
    // process distribute
    RETURN_IF_ERROR(process_distribute(state, row, part, hash_value));
    return Status::OK();
}

Status DataStreamSender::close(RuntimeState* state, Status exec_status) {
    // TODO: only close channels that didn't have any errors
    // make all channels close parallel
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
    Expr::close(_partition_expr_ctxs, state);

    return final_st;
}

template <typename T>
Status DataStreamSender::serialize_batch(RowBatch* src, T* dest, int num_receivers) {
    VLOG_ROW << "serializing " << src->num_rows() << " rows";
    {
        // TODO(zc)
        // SCOPED_TIMER(_profile->total_time_counter());
        SCOPED_TIMER(_serialize_batch_timer);
        // TODO(zc)
        // RETURN_IF_ERROR(src->serialize(dest));
        int uncompressed_bytes = src->serialize(dest);
        int bytes = RowBatch::get_batch_size(*dest);
        // TODO(zc)
        // int uncompressed_bytes = bytes - dest->tuple_data.size() + dest->uncompressed_size;
        // The size output_batch would be if we didn't compress tuple_data (will be equal to
        // actual batch size if tuple_data isn't compressed)
        COUNTER_UPDATE(_bytes_sent_counter, bytes * num_receivers);
        COUNTER_UPDATE(_uncompressed_bytes_counter, uncompressed_bytes * num_receivers);
    }

    return Status::OK();
}

int64_t DataStreamSender::get_num_data_bytes_sent() const {
    // TODO: do we need synchronization here or are reads & writes to 8-byte ints
    // atomic?
    int64_t result = 0;

    for (int i = 0; i < _channels.size(); ++i) {
        result += _channels[i]->num_data_bytes_sent();
    }

    return result;
}

} // namespace doris
