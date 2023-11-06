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

#include <brpc/controller.h>
#include <butil/errno.h>
#include <fmt/format.h>
#include <gen_cpp/Partitions_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/data.pb.h>
#include <gen_cpp/internal_service.pb.h>
#include <gen_cpp/types.pb.h>
#include <stdint.h>

#include <atomic>
#include <cstddef>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/global_types.h"
#include "common/logging.h"
#include "common/status.h"
#include "exec/data_sink.h"
#include "pipeline/exec/exchange_sink_buffer.h"
#include "service/backend_options.h"
#include "util/ref_count_closure.h"
#include "util/runtime_profile.h"
#include "util/uid_util.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/runtime/partitioner.h"
#include "vec/runtime/vdata_stream_recvr.h"

namespace doris {
class ObjectPool;
class RuntimeState;
class MemTracker;
class RowDescriptor;
class TDataSink;
class TDataStreamSink;
class TPlanFragmentDestination;

namespace segment_v2 {
enum CompressionTypePB : int;
} // namespace segment_v2

namespace pipeline {
class ExchangeSinkOperator;
class ExchangeSinkOperatorX;
class LocalExchangeChannelDependency;
} // namespace pipeline

namespace vectorized {
template <typename>
class Channel;
class VDataStreamSender;

template <typename Parent>
class BlockSerializer {
public:
    BlockSerializer(Parent* parent, bool is_local = true);
    Status next_serialized_block(Block* src, PBlock* dest, int num_receivers, bool* serialized,
                                 bool eos, const std::vector<int>* rows = nullptr);
    Status serialize_block(PBlock* dest, int num_receivers = 1);
    Status serialize_block(const Block* src, PBlock* dest, int num_receivers = 1);

    MutableBlock* get_block() const { return _mutable_block.get(); }

    void reset_block() { _mutable_block.reset(); }

    void set_is_local(bool is_local) { _is_local = is_local; }

private:
    Parent* _parent;
    std::unique_ptr<MutableBlock> _mutable_block;

    bool _is_local;
    const int _batch_size;
};

struct ShuffleChannelIds {
    template <typename HashValueType>
    HashValueType operator()(HashValueType l, size_t r) {
        return l % r;
    }
};

class VDataStreamSender : public DataSink {
public:
    friend class pipeline::ExchangeSinkOperator;
    VDataStreamSender(RuntimeState* state, ObjectPool* pool, int sender_id,
                      const RowDescriptor& row_desc, const TDataStreamSink& sink,
                      const std::vector<TPlanFragmentDestination>& destinations,
                      bool send_query_statistics_with_every_batch);

    VDataStreamSender(RuntimeState* state, ObjectPool* pool, int sender_id,
                      const RowDescriptor& row_desc, PlanNodeId dest_node_id,
                      const std::vector<TPlanFragmentDestination>& destinations,
                      bool send_query_statistics_with_every_batch);

    ~VDataStreamSender() override;

    Status init(const TDataSink& thrift_sink) override;

    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;

    Status send(RuntimeState* state, Block* block, bool eos = false) override;
    Status try_close(RuntimeState* state, Status exec_status) override;
    Status close(RuntimeState* state, Status exec_status) override;

    RuntimeState* state() { return _state; }

    void registe_channels(pipeline::ExchangeSinkBuffer<VDataStreamSender>* buffer);

    bool channel_all_can_write();

    int sender_id() const { return _sender_id; }

    RuntimeProfile::Counter* brpc_wait_timer() { return _brpc_wait_timer; }
    RuntimeProfile::Counter* blocks_sent_counter() { return _blocks_sent_counter; }
    RuntimeProfile::Counter* local_send_timer() { return _local_send_timer; }
    RuntimeProfile::Counter* local_bytes_send_counter() { return _local_bytes_send_counter; }
    RuntimeProfile::Counter* local_sent_rows() { return _local_sent_rows; }
    RuntimeProfile::Counter* brpc_send_timer() { return _brpc_send_timer; }
    RuntimeProfile::Counter* split_block_distribute_by_channel_timer() {
        return _split_block_distribute_by_channel_timer;
    }
    MemTracker* mem_tracker() { return _mem_tracker.get(); }
    QueryStatistics* query_statistics() { return _query_statistics.get(); }
    QueryStatisticsPtr query_statisticsPtr() { return _query_statistics; }
    bool transfer_large_data_by_brpc() { return _transfer_large_data_by_brpc; }
    RuntimeProfile::Counter* merge_block_timer() { return _merge_block_timer; }
    segment_v2::CompressionTypePB& compression_type() { return _compression_type; }

protected:
    friend class BlockSerializer<VDataStreamSender>;
    friend class Channel<VDataStreamSender>;
    friend class PipChannel<VDataStreamSender>;
    friend class pipeline::ExchangeSinkBuffer<VDataStreamSender>;

    void _roll_pb_block();
    Status _get_next_available_buffer(BroadcastPBlockHolder** holder);

    template <typename Channels, typename HashValueType>
    Status channel_add_rows(RuntimeState* state, Channels& channels, int num_channels,
                            const HashValueType* __restrict channel_ids, int rows, Block* block,
                            bool eos);

    template <typename ChannelPtrType>
    void _handle_eof_channel(RuntimeState* state, ChannelPtrType channel, Status st);

    // Sender instance id, unique within a fragment.
    int _sender_id;

    RuntimeState* _state;
    ObjectPool* _pool;

    int _current_channel_idx; // index of current channel to send to if _random == true

    TPartitionType::type _part_type;

    // serialized batches for broadcasting; we need two so we can write
    // one while the other one is still being sent
    PBlock _pb_block1;
    PBlock _pb_block2;
    PBlock* _cur_pb_block = nullptr;

    // used by pipeline engine
    std::vector<BroadcastPBlockHolder> _broadcast_pb_blocks;
    int _broadcast_pb_block_idx;

    std::unique_ptr<PartitionerBase> _partitioner;
    size_t _partition_count;

    std::vector<Channel<VDataStreamSender>*> _channels;
    std::vector<std::shared_ptr<Channel<VDataStreamSender>>> _channel_shared_ptrs;

    RuntimeProfile::Counter* _serialize_batch_timer;
    RuntimeProfile::Counter* _compress_timer;
    RuntimeProfile::Counter* _brpc_send_timer;
    RuntimeProfile::Counter* _brpc_wait_timer;
    RuntimeProfile::Counter* _bytes_sent_counter;
    RuntimeProfile::Counter* _uncompressed_bytes_counter;
    RuntimeProfile::Counter* _local_sent_rows;
    RuntimeProfile::Counter* _local_send_timer;
    RuntimeProfile::Counter* _split_block_hash_compute_timer;
    RuntimeProfile::Counter* _split_block_distribute_by_channel_timer;
    RuntimeProfile::Counter* _blocks_sent_counter;
    RuntimeProfile::Counter* _merge_block_timer;
    RuntimeProfile::Counter* _memory_usage_counter;
    RuntimeProfile::Counter* _peak_memory_usage_counter;

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

    bool _only_local_exchange = false;
    bool _enable_pipeline_exec = false;

    BlockSerializer<VDataStreamSender> _serializer;
};

template <typename Parent = VDataStreamSender>
class Channel {
public:
    friend class VDataStreamSender;
    friend class pipeline::ExchangeSinkBuffer<Parent>;
    // Create channel to send data to particular ipaddress/port/query/node
    // combination. buffer_size is specified in bytes and a soft limit on
    // how much tuple data is getting accumulated before being sent; it only applies
    // when data is added via add_row() and not sent directly via send_batch().
    Channel(Parent* parent, const RowDescriptor& row_desc, const TNetworkAddress& brpc_dest,
            const TUniqueId& fragment_instance_id, PlanNodeId dest_node_id, bool is_transfer_chain,
            bool send_query_statistics_with_every_batch)
            : _parent(parent),
              _row_desc(row_desc),
              _fragment_instance_id(fragment_instance_id),
              _dest_node_id(dest_node_id),
              _num_data_bytes_sent(0),
              _packet_seq(0),
              _need_close(false),
              _closed(false),
              _brpc_dest_addr(brpc_dest),
              _is_transfer_chain(is_transfer_chain),
              _send_query_statistics_with_every_batch(send_query_statistics_with_every_batch),
              _is_local((_brpc_dest_addr.hostname == BackendOptions::get_localhost()) &&
                        (_brpc_dest_addr.port == config::brpc_port)),
              _serializer(_parent, _is_local) {
        if (_is_local) {
            VLOG_NOTICE << "will use local Exchange, dest_node_id is : " << _dest_node_id;
        }
        _ch_cur_pb_block = &_ch_pb_block1;
    }

    virtual ~Channel() {
        if (_closure != nullptr && _closure->unref()) {
            delete _closure;
        }
    }

    // Initialize channel.
    // Returns OK if successful, error indication otherwise.
    Status init(RuntimeState* state);

    // Asynchronously sends a row batch.
    // Returns the status of the most recently finished transmit_data
    // rpc (or OK if there wasn't one that hasn't been reported yet).
    // if batch is nullptr, send the eof packet
    virtual Status send_remote_block(PBlock* block, bool eos = false,
                                     Status exec_status = Status::OK());

    virtual Status send_broadcast_block(BroadcastPBlockHolder* block, [[maybe_unused]] bool* sent,
                                        bool eos = false) {
        return Status::InternalError("Send BroadcastPBlockHolder is not allowed!");
    }

    virtual Status add_rows(Block* block, const std::vector<int>& row, bool eos);

    virtual Status send_current_block(bool eos, Status exec_status);

    Status send_local_block(Status exec_status, bool eos = false);

    Status send_local_block(Block* block);
    // Flush buffered rows and close channel. This function don't wait the response
    // of close operation, client should call close_wait() to finish channel's close.
    // We split one close operation into two phases in order to make multiple channels
    // can run parallel.
    Status close(RuntimeState* state, Status exec_status);

    // Get close wait's response, to finish channel close operation.
    Status close_wait(RuntimeState* state);

    int64_t num_data_bytes_sent() const { return _num_data_bytes_sent; }

    PBlock* ch_cur_pb_block() { return _ch_cur_pb_block; }

    std::string get_fragment_instance_id_str() {
        UniqueId uid(_fragment_instance_id);
        return uid.to_string();
    }

    bool is_local() const { return _is_local; }

    VDataStreamRecvr* local_recvr() {
        DCHECK(_is_local && _local_recvr != nullptr);
        return _local_recvr.get();
    }

    virtual void ch_roll_pb_block();

    bool can_write() {
        if (!is_local()) {
            return true;
        }

        // if local recvr queue mem over the exchange node mem limit, we must ensure each queue
        // has one block to do merge sort in exchange node to prevent the logic dead lock
        return !_local_recvr || _local_recvr->is_closed() || !_local_recvr->exceeds_limit(0) ||
               _local_recvr->sender_queue_empty(_parent->sender_id());
    }

    bool is_receiver_eof() const { return _receiver_status.is<ErrorCode::END_OF_FILE>(); }

    void set_receiver_eof(Status st) { _receiver_status = st; }

protected:
    bool _recvr_is_valid() {
        if (_local_recvr && !_local_recvr->is_closed()) {
            return true;
        }
        _receiver_status = Status::EndOfFile("local data stream receiver closed");
        return false;
    }

    Status _wait_last_brpc() {
        SCOPED_TIMER(_parent->brpc_wait_timer());
        if (_closure == nullptr) {
            return Status::OK();
        }
        auto cntl = &_closure->cntl;
        auto call_id = _closure->cntl.call_id();
        brpc::Join(call_id);
        _receiver_status = Status::create(_closure->result.status());
        if (cntl->Failed()) {
            std::string err = fmt::format(
                    "failed to send brpc batch, error={}, error_text={}, client: {}, "
                    "latency = {}",
                    berror(cntl->ErrorCode()), cntl->ErrorText(), BackendOptions::get_localhost(),
                    cntl->latency_us());
            LOG(WARNING) << err;
            return Status::RpcError(err);
        }
        return _receiver_status;
    }

    // Serialize _batch into _thrift_batch and send via send_batch().
    // Returns send_batch() status.
    Status send_current_batch(bool eos = false);
    Status close_internal(Status exec_status);

    Parent* _parent;

    const RowDescriptor& _row_desc;
    TUniqueId _fragment_instance_id;
    PlanNodeId _dest_node_id;

    // the number of RowBatch.data bytes sent successfully
    int64_t _num_data_bytes_sent;
    int64_t _packet_seq;

    bool _need_close;
    bool _closed;
    int _be_number;

    TNetworkAddress _brpc_dest_addr;

    PUniqueId _finst_id;
    PUniqueId _query_id;
    PBlock _pb_block;
    PTransmitDataParams _brpc_request;
    std::shared_ptr<PBackendService_Stub> _brpc_stub = nullptr;
    RefCountClosure<PTransmitDataResult>* _closure = nullptr;
    Status _receiver_status;
    int32_t _brpc_timeout_ms = 500;
    // whether the dest can be treated as query statistics transfer chain.
    bool _is_transfer_chain;
    bool _send_query_statistics_with_every_batch;
    RuntimeState* _state;

    bool _is_local;
    std::shared_ptr<VDataStreamRecvr> _local_recvr;
    // serialized blocks for broadcasting; we need two so we can write
    // one while the other one is still being sent.
    // Which is for same reason as `_cur_pb_block`, `_pb_block1` and `_pb_block2`
    // in VDataStreamSender.
    PBlock* _ch_cur_pb_block = nullptr;
    PBlock _ch_pb_block1;
    PBlock _ch_pb_block2;

    BlockSerializer<Parent> _serializer;
};

#define HANDLE_CHANNEL_STATUS(state, channel, status)    \
    do {                                                 \
        if (status.is<ErrorCode::END_OF_FILE>()) {       \
            _handle_eof_channel(state, channel, status); \
        } else {                                         \
            RETURN_IF_ERROR(status);                     \
        }                                                \
    } while (0)

template <typename Channels, typename HashValueType>
Status VDataStreamSender::channel_add_rows(RuntimeState* state, Channels& channels,
                                           int num_channels,
                                           const HashValueType* __restrict channel_ids, int rows,
                                           Block* block, bool eos) {
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

template <typename Parent = VDataStreamSender>
class PipChannel final : public Channel<Parent> {
public:
    PipChannel(Parent* parent, const RowDescriptor& row_desc, const TNetworkAddress& brpc_dest,
               const TUniqueId& fragment_instance_id, PlanNodeId dest_node_id,
               bool is_transfer_chain, bool send_query_statistics_with_every_batch)
            : Channel<Parent>(parent, row_desc, brpc_dest, fragment_instance_id, dest_node_id,
                              is_transfer_chain, send_query_statistics_with_every_batch) {
        ch_roll_pb_block();
    }

    ~PipChannel() override {
        if (Channel<Parent>::_ch_cur_pb_block) {
            delete Channel<Parent>::_ch_cur_pb_block;
        }
    }

    void ch_roll_pb_block() override {
        // We have two choices here.
        // 1. Use a PBlock pool and fetch an available PBlock if we need one. In this way, we can
        //    reuse the memory, but we have to use a lock to synchronize.
        // 2. Create a new PBlock every time. In this way we don't need a lock but have to allocate
        //    new memory.
        // Now we use the second way.
        Channel<Parent>::_ch_cur_pb_block = new PBlock();
    }

    // Asynchronously sends a block
    // Returns the status of the most recently finished transmit_data
    // rpc (or OK if there wasn't one that hasn't been reported yet).
    // if batch is nullptr, send the eof packet
    Status send_remote_block(PBlock* block, bool eos = false,
                             Status exec_status = Status::OK()) override {
        COUNTER_UPDATE(Channel<Parent>::_parent->blocks_sent_counter(), 1);
        std::unique_ptr<PBlock> pblock_ptr;
        pblock_ptr.reset(block);

        if (eos) {
            if (_eos_send) {
                return Status::OK();
            } else {
                _eos_send = true;
            }
        }
        if (eos || block->column_metas_size()) {
            RETURN_IF_ERROR(_buffer->add_block({this, std::move(pblock_ptr), eos, exec_status}));
        }
        return Status::OK();
    }

    Status send_broadcast_block(BroadcastPBlockHolder* block, [[maybe_unused]] bool* sent,
                                bool eos = false) override {
        COUNTER_UPDATE(Channel<Parent>::_parent->blocks_sent_counter(), 1);
        if (eos) {
            if (_eos_send) {
                return Status::OK();
            }
            _eos_send = true;
        }
        if (eos || block->get_block()->column_metas_size()) {
            RETURN_IF_ERROR(_buffer->add_block({this, block, eos}, sent));
        }
        return Status::OK();
    }

    Status add_rows(Block* block, const std::vector<int>& rows, bool eos) override {
        if (Channel<Parent>::_fragment_instance_id.lo == -1) {
            return Status::OK();
        }

        bool serialized = false;
        _pblock = std::make_unique<PBlock>();
        RETURN_IF_ERROR(Channel<Parent>::_serializer.next_serialized_block(
                block, _pblock.get(), 1, &serialized, eos, &rows));
        if (serialized) {
            Status exec_status = Status::OK();
            RETURN_IF_ERROR(send_current_block(eos, exec_status));
        }

        return Status::OK();
    }

    // send _mutable_block
    Status send_current_block(bool eos, Status exec_status) override {
        if (Channel<Parent>::is_local()) {
            return Channel<Parent>::send_local_block(exec_status, eos);
        }
        SCOPED_CONSUME_MEM_TRACKER(Channel<Parent>::_parent->mem_tracker());
        RETURN_IF_ERROR(send_remote_block(_pblock.release(), eos, exec_status));
        return Status::OK();
    }

    void registe(pipeline::ExchangeSinkBuffer<Parent>* buffer) {
        _buffer = buffer;
        _buffer->register_sink(Channel<Parent>::_fragment_instance_id);
    }

    pipeline::SelfDeleteClosure<PTransmitDataResult>* get_closure(
            InstanceLoId id, bool eos, vectorized::BroadcastPBlockHolder* data) {
        if (!_closure) {
            _closure.reset(new pipeline::SelfDeleteClosure<PTransmitDataResult>());
        } else {
            _closure->cntl.Reset();
        }
        _closure->init(id, eos, data);
        return _closure.get();
    }

    void set_dependency(std::shared_ptr<pipeline::LocalExchangeChannelDependency> dependency) {
        if (!Channel<Parent>::_local_recvr) {
            throw Exception(ErrorCode::INTERNAL_ERROR, "_local_recvr is null");
        }
        Channel<Parent>::_local_recvr->set_dependency(Channel<Parent>::_parent->sender_id(),
                                                      dependency);
    }

private:
    friend class VDataStreamSender;

    pipeline::ExchangeSinkBuffer<Parent>* _buffer = nullptr;
    bool _eos_send = false;
    std::unique_ptr<pipeline::SelfDeleteClosure<PTransmitDataResult>> _closure = nullptr;
    std::unique_ptr<PBlock> _pblock;
};

} // namespace vectorized
} // namespace doris
