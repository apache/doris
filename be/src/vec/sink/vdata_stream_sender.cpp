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
#include "runtime/memory/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "runtime/types.h"
#include "util/proto_util.h"
#include "vec/columns/column_const.h"
#include "vec/columns/columns_number.h"
#include "vec/common/sip_hash.h"
#include "vec/exprs/vexpr.h"
#include "vec/runtime/vdata_stream_mgr.h"
#include "vec/runtime/vdata_stream_recvr.h"
#include "vec/sink/vrow_distribution.h"
#include "vec/sink/writer/vtablet_writer_v2.h"

namespace doris::vectorized {

template <typename Parent>
Status Channel<Parent>::init_stub(RuntimeState* state) {
    if (_brpc_dest_addr.hostname.empty()) {
        LOG(WARNING) << "there is no brpc destination address's hostname"
                        ", maybe version is not compatible.";
        return Status::InternalError("no brpc destination");
    }
    if (state->query_options().__isset.enable_local_exchange) {
        _is_local &= state->query_options().enable_local_exchange;
    }
    if (_is_local) {
        auto st = _parent->state()->exec_env()->vstream_mgr()->find_recvr(
                _fragment_instance_id, _dest_node_id, &_local_recvr);
        if (!st.ok()) {
            // Recvr not found. Maybe downstream task is finished already.
            LOG(INFO) << "Recvr is not found : " << st.to_string();
        }
        return Status::OK();
    }
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
    return Status::OK();
}

template <typename Parent>
Status Channel<Parent>::open(RuntimeState* state) {
    _be_number = state->be_number();
    _brpc_request = std::make_shared<PTransmitDataParams>();
    // initialize brpc request
    _brpc_request->mutable_finst_id()->set_hi(_fragment_instance_id.hi);
    _brpc_request->mutable_finst_id()->set_lo(_fragment_instance_id.lo);
    _finst_id = _brpc_request->finst_id();

    _brpc_request->mutable_query_id()->set_hi(state->query_id().hi);
    _brpc_request->mutable_query_id()->set_lo(state->query_id().lo);
    _query_id = _brpc_request->query_id();

    _brpc_request->set_node_id(_dest_node_id);
    _brpc_request->set_sender_id(_parent->sender_id());
    _brpc_request->set_be_number(_be_number);

    _brpc_timeout_ms = std::min(3600, state->execution_timeout()) * 1000;

    _serializer.set_is_local(_is_local);

    // In bucket shuffle join will set fragment_instance_id (-1, -1)
    // to build a camouflaged empty channel. the ip and port is '0.0.0.0:0"
    // so the empty channel not need call function close_internal()
    _need_close = (_fragment_instance_id.hi != -1 && _fragment_instance_id.lo != -1);
    _state = state;
    return Status::OK();
}

std::shared_ptr<pipeline::Dependency> PipChannel::get_local_channel_dependency() {
    if (!Channel<pipeline::ExchangeSinkLocalState>::_local_recvr) {
        return nullptr;
    }
    return Channel<pipeline::ExchangeSinkLocalState>::_local_recvr->get_local_channel_dependency(
            Channel<pipeline::ExchangeSinkLocalState>::_parent->sender_id());
}

Status PipChannel::send_remote_block(PBlock* block, bool eos, Status exec_status) {
    COUNTER_UPDATE(Channel<pipeline::ExchangeSinkLocalState>::_parent->blocks_sent_counter(), 1);
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

Status PipChannel::send_broadcast_block(std::shared_ptr<BroadcastPBlockHolder>& block, bool eos) {
    COUNTER_UPDATE(Channel<pipeline::ExchangeSinkLocalState>::_parent->blocks_sent_counter(), 1);
    if (eos) {
        if (_eos_send) {
            return Status::OK();
        }
        _eos_send = true;
    }
    if (eos || block->get_block()->column_metas_size()) {
        RETURN_IF_ERROR(_buffer->add_block({this, block, eos}));
    }
    return Status::OK();
}

Status PipChannel::send_current_block(bool eos, Status exec_status) {
    if (Channel<pipeline::ExchangeSinkLocalState>::is_local()) {
        return Channel<pipeline::ExchangeSinkLocalState>::send_local_block(exec_status, eos);
    }
    SCOPED_CONSUME_MEM_TRACKER(Channel<pipeline::ExchangeSinkLocalState>::_parent->mem_tracker());
    RETURN_IF_ERROR(send_remote_block(_pblock.release(), eos, exec_status));
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
    SCOPED_TIMER(_parent->local_send_timer());
    Block block = _serializer.get_block()->to_block();
    _serializer.get_block()->set_mutable_columns(block.clone_empty_columns());
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
    SCOPED_TIMER(_parent->local_send_timer());
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
        COUNTER_UPDATE(_parent->blocks_sent_counter(), 1);
    }
    SCOPED_TIMER(_parent->brpc_send_timer());

    if (_send_remote_block_callback == nullptr) {
        _send_remote_block_callback = DummyBrpcCallback<PTransmitDataResult>::create_shared();
    } else {
        RETURN_IF_ERROR(_wait_last_brpc());
        _send_remote_block_callback->cntl_->Reset();
    }
    VLOG_ROW << "Channel<Parent>::send_batch() instance_id=" << print_id(_fragment_instance_id)
             << " dest_node=" << _dest_node_id << " to_host=" << _brpc_dest_addr.hostname
             << " _packet_seq=" << _packet_seq << " row_desc=" << _row_desc.debug_string();

    _brpc_request->set_eos(eos);
    if (!exec_status.ok()) {
        exec_status.to_protobuf(_brpc_request->mutable_exec_status());
    }
    if (block != nullptr && !block->column_metas().empty()) {
        _brpc_request->set_allocated_block(block);
    }
    _brpc_request->set_packet_seq(_packet_seq++);

    _send_remote_block_callback->cntl_->set_timeout_ms(_brpc_timeout_ms);
    if (config::exchange_sink_ignore_eovercrowded) {
        _send_remote_block_callback->cntl_->ignore_eovercrowded();
    }

    {
        auto send_remote_block_closure =
                AutoReleaseClosure<PTransmitDataParams, DummyBrpcCallback<PTransmitDataResult>>::
                        create_unique(_brpc_request, _send_remote_block_callback);
        if (enable_http_send_block(*_brpc_request)) {
            RETURN_IF_ERROR(transmit_block_httpv2(
                    _state->exec_env(), std::move(send_remote_block_closure), _brpc_dest_addr));
        } else {
            transmit_blockv2(*_brpc_stub, std::move(send_remote_block_closure));
        }
    }

    if (block != nullptr) {
        static_cast<void>(_brpc_request->release_block());
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
            // Non pipeline engine will send an empty eos block
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
            if (!rows->empty()) {
                SCOPED_TIMER(_parent->split_block_distribute_by_channel_timer());
                const auto* begin = rows->data();
                RETURN_IF_ERROR(_mutable_block->add_rows(block, begin, begin + rows->size()));
            }
        } else if (!block->empty()) {
            SCOPED_TIMER(_parent->merge_block_timer());
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
        _mutable_block->set_mutable_columns(block.mutate_columns());
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
        _parent->get_query_statistics_ptr()->add_shuffle_send_bytes(compressed_bytes *
                                                                    num_receivers);
        _parent->get_query_statistics_ptr()->add_shuffle_send_rows(src->rows() * num_receivers);
    }

    return Status::OK();
}

template class Channel<pipeline::ExchangeSinkLocalState>;
template class Channel<pipeline::ResultFileSinkLocalState>;
template class BlockSerializer<pipeline::ResultFileSinkLocalState>;
template class BlockSerializer<pipeline::ExchangeSinkLocalState>;

} // namespace doris::vectorized
