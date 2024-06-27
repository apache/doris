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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/global_types.h"
#include "common/logging.h"
#include "common/status.h"
#include "exec/tablet_info.h"
#include "pipeline/exec/exchange_sink_buffer.h"
#include "service/backend_options.h"
#include "util/ref_count_closure.h"
#include "util/runtime_profile.h"
#include "util/uid_util.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/runtime/partitioner.h"
#include "vec/runtime/vdata_stream_recvr.h"
#include "vec/sink/vrow_distribution.h"
#include "vec/sink/vtablet_finder.h"

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
class ExchangeSinkOperatorX;
class Dependency;
class ExchangeSinkLocalState;
} // namespace pipeline

namespace vectorized {
template <typename>
class Channel;

template <typename Parent>
class BlockSerializer {
public:
    BlockSerializer(Parent* parent, bool is_local = true);
    Status next_serialized_block(Block* src, PBlock* dest, int num_receivers, bool* serialized,
                                 bool eos, const std::vector<uint32_t>* rows = nullptr);
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

template <typename Parent>
class Channel {
public:
    friend class pipeline::ExchangeSinkBuffer;
    // Create channel to send data to particular ipaddress/port/query/node
    // combination. buffer_size is specified in bytes and a soft limit on
    // how much tuple data is getting accumulated before being sent; it only applies
    // when data is added via add_row() and not sent directly via send_batch().
    Channel(Parent* parent, const RowDescriptor& row_desc, TNetworkAddress brpc_dest,
            TUniqueId fragment_instance_id, PlanNodeId dest_node_id)
            : _parent(parent),
              _row_desc(row_desc),
              _fragment_instance_id(std::move(fragment_instance_id)),
              _dest_node_id(dest_node_id),
              _need_close(false),
              _closed(false),
              _brpc_dest_addr(std::move(brpc_dest)),
              _is_local((_brpc_dest_addr.hostname == BackendOptions::get_localhost()) &&
                        (_brpc_dest_addr.port == config::brpc_port)),
              _serializer(_parent, _is_local) {
        if (_is_local) {
            VLOG_NOTICE << "will use local Exchange, dest_node_id is : " << _dest_node_id;
        }
        _ch_cur_pb_block = &_ch_pb_block1;
    }

    virtual ~Channel() = default;

    // Initialize channel.
    // Returns OK if successful, error indication otherwise.
    Status init_stub(RuntimeState* state);
    Status open(RuntimeState* state);

    // Asynchronously sends a row batch.
    // Returns the status of the most recently finished transmit_data
    // rpc (or OK if there wasn't one that hasn't been reported yet).
    // if batch is nullptr, send the eof packet
    virtual Status send_remote_block(PBlock* block, bool eos = false,
                                     Status exec_status = Status::OK());

    virtual Status send_broadcast_block(std::shared_ptr<BroadcastPBlockHolder>& block,
                                        bool eos = false) {
        return Status::InternalError("Send BroadcastPBlockHolder is not allowed!");
    }

    virtual Status add_rows(Block* block, const std::vector<uint32_t>& row, bool eos);

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

    virtual void ch_roll_pb_block();

    bool is_receiver_eof() const { return _receiver_status.is<ErrorCode::END_OF_FILE>(); }

    void set_receiver_eof(Status st) { _receiver_status = st; }

protected:
    bool _recvr_is_valid() {
        if (_local_recvr && !_local_recvr->is_closed()) {
            return true;
        }
        _receiver_status = Status::OK(); // local data stream receiver closed
        return false;
    }

    Status _wait_last_brpc() {
        SCOPED_TIMER(_parent->brpc_wait_timer());
        if (_send_remote_block_callback == nullptr) {
            return Status::OK();
        }
        _send_remote_block_callback->join();
        if (_send_remote_block_callback->cntl_->Failed()) {
            std::string err = fmt::format(
                    "failed to send brpc batch, error={}, error_text={}, client: {}, "
                    "latency = {}",
                    berror(_send_remote_block_callback->cntl_->ErrorCode()),
                    _send_remote_block_callback->cntl_->ErrorText(),
                    BackendOptions::get_localhost(),
                    _send_remote_block_callback->cntl_->latency_us());
            LOG(WARNING) << err;
            return Status::RpcError(err);
        }
        _receiver_status = Status::create(_send_remote_block_callback->response_->status());
        return _receiver_status;
    }

    Status close_internal(Status exec_status);

    Parent* _parent = nullptr;

    const RowDescriptor& _row_desc;
    const TUniqueId _fragment_instance_id;
    PlanNodeId _dest_node_id;

    // the number of RowBatch.data bytes sent successfully
    int64_t _num_data_bytes_sent {};
    int64_t _packet_seq {};

    bool _need_close;
    bool _closed;
    int _be_number;

    TNetworkAddress _brpc_dest_addr;

    PUniqueId _finst_id;
    PUniqueId _query_id;
    PBlock _pb_block;
    std::shared_ptr<PTransmitDataParams> _brpc_request = nullptr;
    std::shared_ptr<PBackendService_Stub> _brpc_stub = nullptr;
    std::shared_ptr<DummyBrpcCallback<PTransmitDataResult>> _send_remote_block_callback;
    Status _receiver_status;
    int32_t _brpc_timeout_ms = 500;
    RuntimeState* _state = nullptr;

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

class PipChannel final : public Channel<pipeline::ExchangeSinkLocalState> {
public:
    PipChannel(pipeline::ExchangeSinkLocalState* parent, const RowDescriptor& row_desc,
               const TNetworkAddress& brpc_dest, const TUniqueId& fragment_instance_id,
               PlanNodeId dest_node_id)
            : Channel<pipeline::ExchangeSinkLocalState>(parent, row_desc, brpc_dest,
                                                        fragment_instance_id, dest_node_id) {
        ch_roll_pb_block();
    }

    ~PipChannel() override { delete Channel<pipeline::ExchangeSinkLocalState>::_ch_cur_pb_block; }

    void ch_roll_pb_block() override {
        // We have two choices here.
        // 1. Use a PBlock pool and fetch an available PBlock if we need one. In this way, we can
        //    reuse the memory, but we have to use a lock to synchronize.
        // 2. Create a new PBlock every time. In this way we don't need a lock but have to allocate
        //    new memory.
        // Now we use the second way.
        Channel<pipeline::ExchangeSinkLocalState>::_ch_cur_pb_block = new PBlock();
    }

    // Asynchronously sends a block
    // Returns the status of the most recently finished transmit_data
    // rpc (or OK if there wasn't one that hasn't been reported yet).
    // if batch is nullptr, send the eof packet
    Status send_remote_block(PBlock* block, bool eos = false,
                             Status exec_status = Status::OK()) override;

    Status send_broadcast_block(std::shared_ptr<BroadcastPBlockHolder>& block,
                                bool eos = false) override;

    Status add_rows(Block* block, const std::vector<uint32_t>& rows, bool eos) override {
        if (Channel<pipeline::ExchangeSinkLocalState>::_fragment_instance_id.lo == -1) {
            return Status::OK();
        }

        bool serialized = false;
        _pblock = std::make_unique<PBlock>();
        RETURN_IF_ERROR(
                Channel<pipeline::ExchangeSinkLocalState>::_serializer.next_serialized_block(
                        block, _pblock.get(), 1, &serialized, eos, &rows));
        if (serialized) {
            Status exec_status = Status::OK();
            RETURN_IF_ERROR(send_current_block(eos, exec_status));
        }

        return Status::OK();
    }

    // send _mutable_block
    Status send_current_block(bool eos, Status exec_status) override;

    void register_exchange_buffer(pipeline::ExchangeSinkBuffer* buffer) {
        _buffer = buffer;
        _buffer->register_sink(Channel<pipeline::ExchangeSinkLocalState>::_fragment_instance_id);
    }

    std::shared_ptr<pipeline::ExchangeSendCallback<PTransmitDataResult>> get_send_callback(
            InstanceLoId id, bool eos) {
        if (!_send_callback) {
            _send_callback = pipeline::ExchangeSendCallback<PTransmitDataResult>::create_shared();
        } else {
            _send_callback->cntl_->Reset();
        }
        _send_callback->init(id, eos);
        return _send_callback;
    }

    std::shared_ptr<pipeline::Dependency> get_local_channel_dependency();

private:
    pipeline::ExchangeSinkBuffer* _buffer = nullptr;
    bool _eos_send = false;
    std::shared_ptr<pipeline::ExchangeSendCallback<PTransmitDataResult>> _send_callback;
    std::unique_ptr<PBlock> _pblock;
};

} // namespace vectorized
} // namespace doris
