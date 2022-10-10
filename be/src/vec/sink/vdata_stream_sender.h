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

#pragma once

#include "common/global_types.h"
#include "exec/data_sink.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/data.pb.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/descriptors.h"
#include "service/backend_options.h"
#include "util/ref_count_closure.h"
#include "util/uid_util.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
class ObjectPool;
class RowBatch;
class RuntimeState;
class RuntimeProfile;
class BufferControlBlock;
class ExprContext;
class MemTracker;
class PartRangeKey;

namespace vectorized {
class VExprContext;
class VPartitionInfo;

class VDataStreamSender : public DataSink {
public:
    VDataStreamSender(ObjectPool* pool, int sender_id, const RowDescriptor& row_desc,
                      const TDataStreamSink& sink,
                      const std::vector<TPlanFragmentDestination>& destinations,
                      int per_channel_buffer_size, bool send_query_statistics_with_every_batch);

    VDataStreamSender(ObjectPool* pool, int sender_id, const RowDescriptor& row_desc,
                      const std::vector<TPlanFragmentDestination>& destinations,
                      int per_channel_buffer_size, bool send_query_statistics_with_every_batch);

    VDataStreamSender(ObjectPool* pool, const RowDescriptor& row_desc, int per_channel_buffer_size,
                      bool send_query_statistics_with_every_batch);

    ~VDataStreamSender() override;

    Status init(const TDataSink& thrift_sink) override;

    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;

    Status send(RuntimeState* state, RowBatch* batch) override;
    Status send(RuntimeState* state, Block* block) override;

    Status close(RuntimeState* state, Status exec_status) override;
    RuntimeProfile* profile() override { return _profile; }

    RuntimeState* state() { return _state; }

    Status serialize_block(Block* src, PBlock* dest, int num_receivers = 1);

protected:
    void _roll_pb_block();
    class Channel;

    Status get_partition_column_result(Block* block, int* result) const {
        int counter = 0;
        for (auto ctx : _partition_expr_ctxs) {
            RETURN_IF_ERROR(ctx->execute(block, &result[counter++]));
        }
        return Status::OK();
    }

    template <typename Channels>
    Status channel_add_rows(Channels& channels, int num_channels, const uint64_t* channel_ids,
                            int rows, Block* block);

    struct hash_128 {
        uint64_t high;
        uint64_t low;
    };

    using hash_128_t = hash_128;

    Status handle_unpartitioned(Block* block);

    // Sender instance id, unique within a fragment.
    int _sender_id;

    RuntimeState* _state;
    ObjectPool* _pool;
    const RowDescriptor& _row_desc;

    int _current_channel_idx; // index of current channel to send to if _random == true

    TPartitionType::type _part_type;
    bool _ignore_not_found;

    // serialized batches for broadcasting; we need two so we can write
    // one while the other one is still being sent
    PBlock _pb_block1;
    PBlock _pb_block2;
    PBlock* _cur_pb_block = nullptr;

    // compute per-row partition values
    std::vector<VExprContext*> _partition_expr_ctxs;

    std::vector<Channel*> _channels;
    std::vector<std::shared_ptr<Channel>> _channel_shared_ptrs;

    // map from range value to partition_id
    // sorted in ascending orderi by range for binary search
    std::vector<VPartitionInfo*> _partition_infos;

    RuntimeProfile* _profile; // Allocated from _pool
    RuntimeProfile::Counter* _serialize_batch_timer;
    RuntimeProfile::Counter* _compress_timer;
    RuntimeProfile::Counter* _brpc_send_timer;
    RuntimeProfile::Counter* _brpc_wait_timer;
    RuntimeProfile::Counter* _bytes_sent_counter;
    RuntimeProfile::Counter* _uncompressed_bytes_counter;
    RuntimeProfile::Counter* _ignore_rows;
    RuntimeProfile::Counter* _local_sent_rows;

    std::unique_ptr<MemTracker> _mem_tracker;

    // Throughput per total time spent in sender
    RuntimeProfile::Counter* _overall_throughput;
    // Used to counter send bytes under local data exchange
    RuntimeProfile::Counter* _local_bytes_send_counter;
    // Identifier of the destination plan node.
    PlanNodeId _dest_node_id;

    // User can change this config at runtime, avoid it being modified during query or loading process.
    bool _transfer_large_data_by_brpc = false;

    segment_v2::CompressionTypePB _compression_type;

    bool _new_shuffle_hash_method = false;
};

// TODO: support local exechange

class VDataStreamSender::Channel {
public:
    // Create channel to send data to particular ipaddress/port/query/node
    // combination. buffer_size is specified in bytes and a soft limit on
    // how much tuple data is getting accumulated before being sent; it only applies
    // when data is added via add_row() and not sent directly via send_batch().
    Channel(VDataStreamSender* parent, const RowDescriptor& row_desc,
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
              _brpc_dest_addr(brpc_dest),
              _is_transfer_chain(is_transfer_chain),
              _send_query_statistics_with_every_batch(send_query_statistics_with_every_batch),
              _ch_cur_pb_block(&_ch_pb_block1) {
        std::string localhost = BackendOptions::get_localhost();
        _is_local = (_brpc_dest_addr.hostname == localhost) &&
                    (_brpc_dest_addr.port == config::brpc_port);
        if (_is_local) {
            VLOG_NOTICE << "will use local Exchange, dest_node_id is : " << _dest_node_id;
        }
    }

    virtual ~Channel() {
        if (_closure != nullptr && _closure->unref()) {
            delete _closure;
        }
        // release this before request desctruct
        _brpc_request.release_finst_id();
        _brpc_request.release_query_id();
    }

    // Initialize channel.
    // Returns OK if successful, error indication otherwise.
    Status init(RuntimeState* state);

    // Copies a single row into this channel's output buffer and flushes buffer
    // if it reaches capacity.
    // Returns error status if any of the preceding rpcs failed, OK otherwise.
    //Status add_row(TupleRow* row);

    // Asynchronously sends a row batch.
    // Returns the status of the most recently finished transmit_data
    // rpc (or OK if there wasn't one that hasn't been reported yet).
    // if batch is nullptr, send the eof packet
    Status send_block(PBlock* block, bool eos = false);

    Status add_rows(Block* block, const std::vector<int>& row);

    Status send_current_block(bool eos = false);

    Status send_local_block(bool eos = false);

    Status send_local_block(Block* block);
    // Flush buffered rows and close channel. This function don't wait the response
    // of close operation, client should call close_wait() to finish channel's close.
    // We split one close operation into two phases in order to make multiple channels
    // can run parallel.
    Status close(RuntimeState* state);

    // Get close wait's response, to finish channel close operation.
    Status close_wait(RuntimeState* state);

    int64_t num_data_bytes_sent() const { return _num_data_bytes_sent; }

    PBlock* ch_cur_pb_block() { return _ch_cur_pb_block; }

    std::string get_fragment_instance_id_str() {
        UniqueId uid(_fragment_instance_id);
        return uid.to_string();
    }

    TUniqueId get_fragment_instance_id() const { return _fragment_instance_id; }

    bool is_local() const { return _is_local; }

    void ch_roll_pb_block();

private:
    Status _wait_last_brpc() {
        SCOPED_TIMER(_parent->_brpc_wait_timer);
        if (_closure == nullptr) {
            return Status::OK();
        }
        auto cntl = &_closure->cntl;
        auto call_id = _closure->cntl.call_id();
        brpc::Join(call_id);
        if (cntl->Failed()) {
            std::string err = fmt::format(
                    "failed to send brpc batch, error={}, error_text={}, client: {}, "
                    "latency = {}",
                    berror(cntl->ErrorCode()), cntl->ErrorText(), BackendOptions::get_localhost(),
                    cntl->latency_us());
            LOG(WARNING) << err;
            return Status::RpcError(err);
        }
        return Status::OK();
    }

private:
    // Serialize _batch into _thrift_batch and send via send_batch().
    // Returns send_batch() status.
    Status send_current_batch(bool eos = false);
    Status close_internal();

    VDataStreamSender* _parent;
    int _buffer_size;

    const RowDescriptor& _row_desc;
    TUniqueId _fragment_instance_id;
    PlanNodeId _dest_node_id;

    // the number of RowBatch.data bytes sent successfully
    int64_t _num_data_bytes_sent;
    int64_t _packet_seq;

    // we're accumulating rows into this batch
    std::unique_ptr<MutableBlock> _mutable_block;

    bool _need_close;
    int _be_number;

    TNetworkAddress _brpc_dest_addr;

    PUniqueId _finst_id;
    PUniqueId _query_id;
    PBlock _pb_block;
    PTransmitDataParams _brpc_request;
    std::shared_ptr<PBackendService_Stub> _brpc_stub = nullptr;
    RefCountClosure<PTransmitDataResult>* _closure = nullptr;
    int32_t _brpc_timeout_ms = 500;
    // whether the dest can be treated as query statistics transfer chain.
    bool _is_transfer_chain;
    bool _send_query_statistics_with_every_batch;
    RuntimeState* _state;

    size_t _capacity;
    bool _is_local;

    // serialized blocks for broadcasting; we need two so we can write
    // one while the other one is still being sent.
    // Which is for same reason as `_cur_pb_block`, `_pb_block1` and `_pb_block2`
    // in VDataStreamSender.
    PBlock* _ch_cur_pb_block = nullptr;
    PBlock _ch_pb_block1;
    PBlock _ch_pb_block2;

    bool _enable_local_exchange = false;
};

template <typename Channels>
Status VDataStreamSender::channel_add_rows(Channels& channels, int num_channels,
                                           const uint64_t* __restrict channel_ids, int rows,
                                           Block* block) {
    std::vector<int> channel2rows[num_channels];

    for (int i = 0; i < rows; i++) {
        channel2rows[channel_ids[i]].emplace_back(i);
    }

    for (int i = 0; i < num_channels; ++i) {
        if (!channel2rows[i].empty()) {
            RETURN_IF_ERROR(channels[i]->add_rows(block, channel2rows[i]));
        }
    }

    return Status::OK();
}

} // namespace vectorized
} // namespace doris
