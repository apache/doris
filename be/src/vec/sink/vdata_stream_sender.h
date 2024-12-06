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
#include "common/compile_check_begin.h"
class ObjectPool;
class RuntimeState;
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

class BlockSerializer {
public:
    BlockSerializer(pipeline::ExchangeSinkLocalState* parent, bool is_local = true);
#ifdef BE_TEST
    BlockSerializer() : _batch_size(0) {};
#endif
    Status next_serialized_block(Block* src, PBlock* dest, size_t num_receivers, bool* serialized,
                                 bool eos, const std::vector<uint32_t>* rows = nullptr);
    Status serialize_block(PBlock* dest, size_t num_receivers = 1);
    Status serialize_block(const Block* src, PBlock* dest, size_t num_receivers = 1);

    MutableBlock* get_block() const { return _mutable_block.get(); }

    void reset_block() { _mutable_block.reset(); }

    void set_is_local(bool is_local) { _is_local = is_local; }
    bool is_local() const { return _is_local; }

private:
    pipeline::ExchangeSinkLocalState* _parent;
    std::unique_ptr<MutableBlock> _mutable_block;

    bool _is_local;
    const int _batch_size;
};

class Channel {
public:
    friend class pipeline::ExchangeSinkBuffer;
    // Create channel to send data to particular ipaddress/port/query/node
    // combination. buffer_size is specified in bytes and a soft limit on
    // how much tuple data is getting accumulated before being sent; it only applies
    // when data is added via add_row() and not sent directly via send_batch().
    Channel(pipeline::ExchangeSinkLocalState* parent, TNetworkAddress brpc_dest,
            TUniqueId fragment_instance_id, PlanNodeId dest_node_id)
            : _parent(parent),
              _fragment_instance_id(std::move(fragment_instance_id)),
              _dest_node_id(dest_node_id),
              _brpc_dest_addr(std::move(brpc_dest)),
              _is_local((_brpc_dest_addr.hostname == BackendOptions::get_localhost()) &&
                        (_brpc_dest_addr.port == config::brpc_port)),
              _serializer(_parent, _is_local) {}

    virtual ~Channel() = default;

    // Initialize channel.
    // Returns OK if successful, error indication otherwise.
    Status init(RuntimeState* state);
    Status open(RuntimeState* state);

    Status send_local_block(Block* block, bool eos, bool can_be_moved);
    // Flush buffered rows and close channel. This function don't wait the response
    // of close operation, client should call close_wait() to finish channel's close.
    // We split one close operation into two phases in order to make multiple channels
    // can run parallel.
    Status close(RuntimeState* state);

    std::string get_fragment_instance_id_str() {
        UniqueId uid(_fragment_instance_id);
        return uid.to_string();
    }

    bool is_local() const { return _is_local; }

    bool is_receiver_eof() const { return _receiver_status.is<ErrorCode::END_OF_FILE>(); }

    void set_receiver_eof(Status st) { _receiver_status = st; }

    int64_t mem_usage() const;

    // Asynchronously sends a block
    // Returns the status of the most recently finished transmit_data
    // rpc (or OK if there wasn't one that hasn't been reported yet).
    // if batch is nullptr, send the eof packet
    Status send_remote_block(std::unique_ptr<PBlock>&& block, bool eos = false);
    Status send_broadcast_block(std::shared_ptr<BroadcastPBlockHolder>& block, bool eos = false);

    Status add_rows(Block* block, const std::vector<uint32_t>& rows, bool eos) {
        if (_fragment_instance_id.lo == -1) {
            return Status::OK();
        }

        bool serialized = false;
        if (_pblock == nullptr) {
            _pblock = std::make_unique<PBlock>();
        }
        RETURN_IF_ERROR(_serializer.next_serialized_block(block, _pblock.get(), 1, &serialized, eos,
                                                          &rows));
        if (serialized) {
            RETURN_IF_ERROR(_send_current_block(eos));
        }

        return Status::OK();
    }

    void set_exchange_buffer(pipeline::ExchangeSinkBuffer* buffer) { _buffer = buffer; }

    InstanceLoId dest_ins_id() const { return _fragment_instance_id.lo; }

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

protected:
    Status _send_local_block(bool eos);
    Status _send_current_block(bool eos);

    Status _recvr_status() const {
        if (_local_recvr && !_local_recvr->is_closed()) {
            return Status::OK();
        }
        return Status::EndOfFile(
                "local data stream receiver closed"); // local data stream receiver closed
    }

    pipeline::ExchangeSinkLocalState* _parent = nullptr;

    const TUniqueId _fragment_instance_id;
    PlanNodeId _dest_node_id;
    bool _closed {false};
    bool _need_close {false};
    int _be_number;

    TNetworkAddress _brpc_dest_addr;

    PBlock _pb_block;
    std::shared_ptr<PBackendService_Stub> _brpc_stub = nullptr;
    Status _receiver_status;
    int32_t _brpc_timeout_ms = 500;
    RuntimeState* _state = nullptr;

    bool _is_local;
    std::shared_ptr<VDataStreamRecvr> _local_recvr;

    BlockSerializer _serializer;

    pipeline::ExchangeSinkBuffer* _buffer = nullptr;
    bool _eos_send = false;
    std::shared_ptr<pipeline::ExchangeSendCallback<PTransmitDataResult>> _send_callback;
    std::unique_ptr<PBlock> _pblock;
};

#define HANDLE_CHANNEL_STATUS(state, channel, status)    \
    do {                                                 \
        if (status.is<ErrorCode::END_OF_FILE>()) {       \
            _handle_eof_channel(state, channel, status); \
        } else {                                         \
            RETURN_IF_ERROR(status);                     \
        }                                                \
    } while (0)

} // namespace vectorized
} // namespace doris

#include "common/compile_check_end.h"
