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
#include <gen_cpp/Types_types.h>
#include <gen_cpp/data.pb.h>
#include <gen_cpp/internal_service.pb.h>
#include <glog/logging.h>
#include <stddef.h>

#include <algorithm>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <random>

#include "common/object_pool.h"
#include "common/status.h"
#include "exec/tablet_info.h"
#include "pipeline/exec/exchange_sink_operator.h"
#include "pipeline/exec/result_file_sink_operator.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "runtime/types.h"
#include "util/proto_util.h"
#include "vec/columns/column_const.h"
#include "vec/common/sip_hash.h"
#include "vec/exprs/vexpr.h"
#include "vec/runtime/vdata_stream_mgr.h"
#include "vec/runtime/vdata_stream_recvr.h"
#include "vec/sink/vrow_distribution.h"
#include "vec/sink/writer/vtablet_writer_v2.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

Status Channel::init(RuntimeState* state) {
    // only enable_local_exchange() is true and the destination address is localhost, then the channel is local
    _is_local &= state->enable_local_exchange();

    if (_is_local) {
        return Status::OK();
    }

    RETURN_IF_ERROR(_init_brpc_stub(state));

    return Status::OK();
}

Status Channel::_init_brpc_stub(RuntimeState* state) {
    if (_brpc_dest_addr.hostname.empty()) {
        LOG(WARNING) << "there is no brpc destination address's hostname"
                        ", maybe version is not compatible.";
        return Status::InternalError("no brpc destination");
    }

    auto network_address = _brpc_dest_addr;
    if (_brpc_dest_addr.hostname == BackendOptions::get_localhost()) {
        _brpc_stub = state->exec_env()->brpc_internal_client_cache()->get_client(
                "127.0.0.1", _brpc_dest_addr.port);
        network_address.hostname = "127.0.0.1";
    } else {
        _brpc_stub = state->exec_env()->brpc_internal_client_cache()->get_client(_brpc_dest_addr);
    }

    if (!_brpc_stub) {
        std::string msg = fmt::format("Get rpc stub failed, dest_addr={}:{}",
                                      _brpc_dest_addr.hostname, _brpc_dest_addr.port);
        LOG(WARNING) << msg;
        return Status::InternalError(msg);
    }

    if (config::enable_brpc_connection_check) {
        state->get_query_ctx()->add_using_brpc_stub(_brpc_dest_addr, _brpc_stub);
    }
    return Status::OK();
}

Status Channel::open(RuntimeState* state) {
    if (_is_local) {
        RETURN_IF_ERROR(_find_local_recvr(state));
    }
    _be_number = state->be_number();
    _brpc_timeout_ms = get_execution_rpc_timeout_ms(state->execution_timeout());
    _serializer.set_is_local(_is_local);

    // In bucket shuffle join will set fragment_instance_id (-1, -1)
    // to build a camouflaged empty channel. the ip and port is '0.0.0.0:0"
    // so the empty channel not need call function close_internal()
    _need_close = (_fragment_instance_id.hi != -1 && _fragment_instance_id.lo != -1);

    return Status::OK();
}

Status Channel::_find_local_recvr(RuntimeState* state) {
    auto st = _parent->state()->exec_env()->vstream_mgr()->find_recvr(_fragment_instance_id,
                                                                      _dest_node_id, &_local_recvr);
    if (!st.ok()) {
        // If could not find local receiver, then it means the channel is EOF.
        // Maybe downstream task is finished already.
        //if (_receiver_status.ok()) {
        //    _receiver_status = Status::EndOfFile("local data stream receiver is deconstructed");
        //}
        LOG(INFO) << "Query: " << print_id(state->query_id())
                  << " recvr is not found, maybe downstream task is finished. error st is: "
                  << st.to_string();
    }
    return Status::OK();
}
Status Channel::add_rows(Block* block, const uint32_t* data, const uint32_t offset,
                         const uint32_t size, bool eos) {
    if (_fragment_instance_id.lo == -1) {
        return Status::OK();
    }

    bool serialized = false;
    if (_pblock == nullptr) {
        _pblock = std::make_unique<PBlock>();
    }
    int64_t old_channel_mem_usage = mem_usage();
    Defer update_mem([&]() {
        COUNTER_UPDATE(_parent->memory_used_counter(), mem_usage() - old_channel_mem_usage);
    });
    RETURN_IF_ERROR(_serializer.next_serialized_block(block, _pblock.get(), 1, &serialized, eos,
                                                      data, offset, size));
    if (serialized) {
        RETURN_IF_ERROR(_send_current_block(eos));
    }

    return Status::OK();
}

std::shared_ptr<pipeline::Dependency> Channel::get_local_channel_dependency() {
    if (!_local_recvr) {
        return nullptr;
    }
    return _local_recvr->get_local_channel_dependency(_parent->sender_id());
}

int64_t Channel::mem_usage() const {
    return _serializer.mem_usage();
}

Status Channel::send_remote_block(std::unique_ptr<PBlock>&& block, bool eos) {
    COUNTER_UPDATE(_parent->blocks_sent_counter(), 1);

    if (eos) {
        if (_eos_send) {
            return Status::OK();
        } else {
            _eos_send = true;
        }
    }
    if (eos || block->column_metas_size()) {
        RETURN_IF_ERROR(_buffer->add_block(this, {std::move(block), eos}));
    }
    return Status::OK();
}

Status Channel::send_broadcast_block(std::shared_ptr<BroadcastPBlockHolder>& block, bool eos) {
    COUNTER_UPDATE(_parent->blocks_sent_counter(), 1);
    if (eos) {
        if (_eos_send) {
            return Status::OK();
        }
        _eos_send = true;
    }
    if (eos || block->get_block()->column_metas_size()) {
        RETURN_IF_ERROR(_buffer->add_block(this, {block, eos}));
    }
    return Status::OK();
}

Status Channel::_send_current_block(bool eos) {
    if (is_local()) {
        return _send_local_block(eos);
    }
    // here _pblock maybe nullptr , but we must send the eos to the receiver
    return send_remote_block(std::move(_pblock), eos);
}

Status Channel::_send_local_block(bool eos) {
    Block block;
    if (_serializer.get_block() != nullptr) {
        block = _serializer.get_block()->to_block();
        _serializer.get_block()->set_mutable_columns(block.clone_empty_columns());
    }

    if (!block.empty() || eos) {
        RETURN_IF_ERROR(send_local_block(&block, eos, true));
    }
    return Status::OK();
}

Status Channel::send_local_block(Block* block, bool eos, bool can_be_moved) {
    SCOPED_TIMER(_parent->local_send_timer());

    if (eos) {
        if (_eos_send) {
            return Status::OK();
        } else {
            _eos_send = true;
        }
    }

    if (is_receiver_eof()) {
        return _receiver_status;
    }
    auto receiver_status = _recvr_status();
    // _local_recvr depdend on pipeline::ExchangeLocalState* _parent to do some memory counter settings
    // but it only owns a raw pointer, so that the ExchangeLocalState object may be deconstructed.
    // Lock the fragment context to ensure the runtime state and other objects are not deconstructed
    TaskExecutionContextSPtr ctx_lock = nullptr;
    if (receiver_status.ok() && _local_recvr != nullptr) {
        ctx_lock = _local_recvr->task_exec_ctx();
        // Do not return internal error, because when query finished, the downstream node
        // may finish before upstream node. And the object maybe deconstructed. If return error
        // then the upstream node may report error status to FE, the query is failed.
        if (ctx_lock == nullptr) {
            receiver_status = Status::EndOfFile("local data stream receiver is deconstructed");
        }
    }
    if (receiver_status.ok()) {
        COUNTER_UPDATE(_parent->local_bytes_send_counter(), block->bytes());
        COUNTER_UPDATE(_parent->local_sent_rows(), block->rows());
        COUNTER_UPDATE(_parent->blocks_sent_counter(), 1);

        const auto sender_id = _parent->sender_id();
        if (!block->empty()) [[likely]] {
            _local_recvr->add_block(block, sender_id, can_be_moved);
        }

        if (eos) [[unlikely]] {
            _local_recvr->remove_sender(sender_id, _be_number, Status::OK());
            _parent->on_channel_finished(_fragment_instance_id.lo);
        }
        return Status::OK();
    } else {
        _receiver_status = std::move(receiver_status);
        _parent->on_channel_finished(_fragment_instance_id.lo);
        return _receiver_status;
    }
}

Status Channel::close(RuntimeState* state) {
    if (_closed) {
        return Status::OK();
    }
    _closed = true;

    if (!_need_close) {
        return Status::OK();
    }

    if (is_receiver_eof()) {
        _serializer.reset_block();
        return Status::OK();
    } else {
        return _send_current_block(true);
    }
}

BlockSerializer::BlockSerializer(pipeline::ExchangeSinkLocalState* parent, bool is_local)
        : _parent(parent), _is_local(is_local), _batch_size(parent->state()->batch_size()) {}

Status BlockSerializer::next_serialized_block(Block* block, PBlock* dest, size_t num_receivers,
                                              bool* serialized, bool eos, const uint32_t* data,
                                              const uint32_t offset, const uint32_t size) {
    if (_mutable_block == nullptr) {
        _mutable_block = MutableBlock::create_unique(block->clone_empty());
    }

    {
        SCOPED_TIMER(_parent->merge_block_timer());
        if (data) {
            if (size > 0) {
                RETURN_IF_ERROR(
                        _mutable_block->add_rows(block, data + offset, data + offset + size));
            }
        } else if (!block->empty()) {
            RETURN_IF_ERROR(_mutable_block->merge(*block));
        }
    }

    if (_mutable_block->rows() >= _batch_size || eos ||
        (_mutable_block->rows() > 0 && _mutable_block->allocated_bytes() > _buffer_mem_limit)) {
        if (!_is_local) {
            RETURN_IF_ERROR(_serialize_block(dest, num_receivers));
        }
        *serialized = true;
    } else {
        *serialized = false;
    }
    return Status::OK();
}

Status BlockSerializer::_serialize_block(PBlock* dest, size_t num_receivers) {
    if (_mutable_block && _mutable_block->rows() > 0) {
        auto block = _mutable_block->to_block();
        RETURN_IF_ERROR(serialize_block(&block, dest, num_receivers));
        if (_parent->state()->low_memory_mode()) {
            reset_block();
        } else {
            block.clear_column_data();
            _mutable_block->set_mutable_columns(block.mutate_columns());
        }
    }

    return Status::OK();
}

Status BlockSerializer::serialize_block(const Block* src, PBlock* dest, size_t num_receivers) {
    SCOPED_TIMER(_parent->_serialize_batch_timer);
    dest->Clear();
    size_t uncompressed_bytes = 0, compressed_bytes = 0;
    RETURN_IF_ERROR(src->serialize(_parent->_state->be_exec_version(), dest, &uncompressed_bytes,
                                   &compressed_bytes, _parent->compression_type(),
                                   _parent->transfer_large_data_by_brpc()));
    COUNTER_UPDATE(_parent->_bytes_sent_counter, compressed_bytes * num_receivers);
    COUNTER_UPDATE(_parent->_uncompressed_bytes_counter, uncompressed_bytes * num_receivers);
    COUNTER_UPDATE(_parent->_compress_timer, src->get_compress_time());
#ifndef BE_TEST
    _parent->state()->get_query_ctx()->resource_ctx()->io_context()->update_shuffle_send_bytes(
            compressed_bytes * num_receivers);
    _parent->state()->get_query_ctx()->resource_ctx()->io_context()->update_shuffle_send_rows(
            src->rows() * num_receivers);
#endif

    return Status::OK();
}

} // namespace doris::vectorized
