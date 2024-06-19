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

#include "exchange_sink_buffer.h"

#include <brpc/controller.h>
#include <butil/errno.h>
#include <butil/iobuf_inl.h>
#include <fmt/format.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>
#include <google/protobuf/stubs/callback.h>
#include <stddef.h>

#include <atomic>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <ostream>
#include <utility>

#include "common/status.h"
#include "pipeline/exec/exchange_sink_operator.h"
#include "pipeline/pipeline_fragment_context.h"
#include "runtime/exec_env.h"
#include "runtime/thread_context.h"
#include "service/backend_options.h"
#include "util/proto_util.h"
#include "util/time.h"
#include "vec/sink/vdata_stream_sender.h"

namespace doris {

namespace vectorized {
BroadcastPBlockHolder::~BroadcastPBlockHolder() {
    // lock the parent queue, if the queue could lock success, then return the block
    // to the queue, to reuse the block
    std::shared_ptr<BroadcastPBlockHolderMemLimiter> limiter = _parent_creator.lock();
    if (limiter != nullptr) {
        limiter->release(*this);
    }
    // If the queue already deconstruted, then release pblock automatically since it
    // is a unique ptr.
}

void BroadcastPBlockHolderMemLimiter::acquire(BroadcastPBlockHolder& holder) {
    std::unique_lock l(_holders_lock);
    DCHECK(_broadcast_dependency != nullptr);
    holder.set_parent_creator(shared_from_this());
    auto size = holder._pblock->column_values().size();
    _total_queue_buffer_size += size;
    _total_queue_blocks_count++;
    if (_total_queue_buffer_size >= config::exchg_node_buffer_size_bytes ||
        _total_queue_blocks_count >= config::num_broadcast_buffer) {
        _broadcast_dependency->block();
    }
}

void BroadcastPBlockHolderMemLimiter::release(const BroadcastPBlockHolder& holder) {
    std::unique_lock l(_holders_lock);
    DCHECK(_broadcast_dependency != nullptr);
    auto size = holder._pblock->column_values().size();
    _total_queue_buffer_size -= size;
    _total_queue_blocks_count--;
    if (_total_queue_buffer_size <= 0) {
        _broadcast_dependency->set_ready();
    }
}

} // namespace vectorized

namespace pipeline {

ExchangeSinkBuffer::ExchangeSinkBuffer(PUniqueId query_id, PlanNodeId dest_node_id, int send_id,
                                       int be_number, RuntimeState* state,
                                       ExchangeSinkLocalState* parent)
        : HasTaskExecutionCtx(state),
          _queue_capacity(0),
          _is_finishing(false),
          _query_id(query_id),
          _dest_node_id(dest_node_id),
          _sender_id(send_id),
          _be_number(be_number),
          _state(state),
          _context(state->get_query_ctx()),
          _parent(parent) {}

void ExchangeSinkBuffer::close() {
    // Could not clear the queue here, because there maybe a running rpc want to
    // get a request from the queue, and clear method will release the request
    // and it will core.
    //_instance_to_broadcast_package_queue.clear();
    //_instance_to_package_queue.clear();
    //_instance_to_request.clear();
}

void ExchangeSinkBuffer::_set_ready_to_finish(bool all_done) {
    if (_finish_dependency && _should_stop && all_done) {
        _finish_dependency->set_ready();
    }
}

void ExchangeSinkBuffer::register_sink(TUniqueId fragment_instance_id) {
    if (_is_finishing) {
        return;
    }
    auto low_id = fragment_instance_id.lo;
    if (_instance_to_package_queue_mutex.count(low_id)) {
        return;
    }
    _instance_to_package_queue_mutex[low_id] = std::make_unique<std::mutex>();
    _instance_to_seq[low_id] = 0;
    _instance_to_package_queue[low_id] = std::queue<TransmitInfo, std::list<TransmitInfo>>();
    _instance_to_broadcast_package_queue[low_id] =
            std::queue<BroadcastTransmitInfo, std::list<BroadcastTransmitInfo>>();
    _queue_capacity =
            config::exchg_buffer_queue_capacity_factor * _instance_to_package_queue.size();
    PUniqueId finst_id;
    finst_id.set_hi(fragment_instance_id.hi);
    finst_id.set_lo(fragment_instance_id.lo);
    _rpc_channel_is_idle[low_id] = true;
    _instance_to_rpc_ctx[low_id] = {};
    _instance_to_receiver_eof[low_id] = false;
    _instance_to_rpc_time[low_id] = 0;
    _construct_request(low_id, finst_id);
}

Status ExchangeSinkBuffer::add_block(TransmitInfo&& request) {
    if (_is_finishing) {
        return Status::OK();
    }
    auto ins_id = request.channel->_fragment_instance_id.lo;
    if (_is_receiver_eof(ins_id)) {
        return Status::EndOfFile("receiver eof");
    }
    bool send_now = false;
    {
        std::unique_lock<std::mutex> lock(*_instance_to_package_queue_mutex[ins_id]);
        // Do not have in process rpc, directly send
        if (_rpc_channel_is_idle[ins_id]) {
            send_now = true;
            _rpc_channel_is_idle[ins_id] = false;
            _busy_channels++;
        }
        if (request.block) {
            RETURN_IF_ERROR(
                    BeExecVersionManager::check_be_exec_version(request.block->be_exec_version()));
        }
        _instance_to_package_queue[ins_id].emplace(std::move(request));
        _total_queue_size++;
        if (_queue_dependency && _total_queue_size > _queue_capacity) {
            _queue_dependency->block();
        }
    }
    if (send_now) {
        RETURN_IF_ERROR(_send_rpc(ins_id));
    }

    return Status::OK();
}

Status ExchangeSinkBuffer::add_block(BroadcastTransmitInfo&& request) {
    if (_is_finishing) {
        return Status::OK();
    }
    auto ins_id = request.channel->_fragment_instance_id.lo;
    if (_is_receiver_eof(ins_id)) {
        return Status::EndOfFile("receiver eof");
    }
    bool send_now = false;
    {
        std::unique_lock<std::mutex> lock(*_instance_to_package_queue_mutex[ins_id]);
        // Do not have in process rpc, directly send
        if (_rpc_channel_is_idle[ins_id]) {
            send_now = true;
            _rpc_channel_is_idle[ins_id] = false;
            _busy_channels++;
        }
        if (request.block_holder->get_block()) {
            RETURN_IF_ERROR(BeExecVersionManager::check_be_exec_version(
                    request.block_holder->get_block()->be_exec_version()));
        }
        _instance_to_broadcast_package_queue[ins_id].emplace(request);
    }
    if (send_now) {
        RETURN_IF_ERROR(_send_rpc(ins_id));
    }

    return Status::OK();
}

Status ExchangeSinkBuffer::_send_rpc(InstanceLoId id) {
    std::unique_lock<std::mutex> lock(*_instance_to_package_queue_mutex[id]);

    DCHECK(_rpc_channel_is_idle[id] == false);

    std::queue<TransmitInfo, std::list<TransmitInfo>>& q = _instance_to_package_queue[id];
    std::queue<BroadcastTransmitInfo, std::list<BroadcastTransmitInfo>>& broadcast_q =
            _instance_to_broadcast_package_queue[id];

    if (_is_finishing) {
        _turn_off_channel(id);
        return Status::OK();
    }

    if (!q.empty()) {
        // If we have data to shuffle which is not broadcasted
        auto& request = q.front();
        auto& brpc_request = _instance_to_request[id];
        brpc_request->set_eos(request.eos);
        brpc_request->set_packet_seq(_instance_to_seq[id]++);
        if (request.block && !request.block->column_metas().empty()) {
            brpc_request->set_allocated_block(request.block.get());
        }
        if (!request.exec_status.ok()) {
            request.exec_status.to_protobuf(brpc_request->mutable_exec_status());
        }
        auto send_callback = request.channel->get_send_callback(id, request.eos);

        _instance_to_rpc_ctx[id]._send_callback = send_callback;
        _instance_to_rpc_ctx[id].is_cancelled = false;

        send_callback->cntl_->set_timeout_ms(request.channel->_brpc_timeout_ms);
        if (config::exchange_sink_ignore_eovercrowded) {
            send_callback->cntl_->ignore_eovercrowded();
        }
        send_callback->addFailedHandler([&, weak_task_ctx = weak_task_exec_ctx()](
                                                const InstanceLoId& id, const std::string& err) {
            auto task_lock = weak_task_ctx.lock();
            if (task_lock == nullptr) {
                // This means ExchangeSinkBuffer Ojbect already destroyed, not need run failed any more.
                return;
            }
            // attach task for memory tracker and query id when core
            SCOPED_ATTACH_TASK(_state);
            _failed(id, err);
        });
        send_callback->start_rpc_time = GetCurrentTimeNanos();
        send_callback->addSuccessHandler([&, weak_task_ctx = weak_task_exec_ctx()](
                                                 const InstanceLoId& id, const bool& eos,
                                                 const PTransmitDataResult& result,
                                                 const int64_t& start_rpc_time) {
            auto task_lock = weak_task_ctx.lock();
            if (task_lock == nullptr) {
                // This means ExchangeSinkBuffer Ojbect already destroyed, not need run failed any more.
                return;
            }
            // attach task for memory tracker and query id when core
            SCOPED_ATTACH_TASK(_state);
            set_rpc_time(id, start_rpc_time, result.receive_time());
            Status s(Status::create(result.status()));
            if (s.is<ErrorCode::END_OF_FILE>()) {
                _set_receiver_eof(id);
            } else if (!s.ok()) {
                _failed(id,
                        fmt::format("exchange req success but status isn't ok: {}", s.to_string()));
            } else if (eos) {
                _ended(id);
            } else {
                static_cast<void>(_send_rpc(id));
            }
        });
        {
            auto send_remote_block_closure =
                    AutoReleaseClosure<PTransmitDataParams,
                                       pipeline::ExchangeSendCallback<PTransmitDataResult>>::
                            create_unique(brpc_request, send_callback);
            if (enable_http_send_block(*brpc_request)) {
                RETURN_IF_ERROR(transmit_block_httpv2(_context->exec_env(),
                                                      std::move(send_remote_block_closure),
                                                      request.channel->_brpc_dest_addr));
            } else {
                transmit_blockv2(*request.channel->_brpc_stub,
                                 std::move(send_remote_block_closure));
            }
        }
        if (request.block) {
            static_cast<void>(brpc_request->release_block());
        }
        q.pop();
        _total_queue_size--;
        if (_queue_dependency && _total_queue_size <= _queue_capacity) {
            _queue_dependency->set_ready();
        }
    } else if (!broadcast_q.empty()) {
        // If we have data to shuffle which is broadcasted
        auto& request = broadcast_q.front();
        auto& brpc_request = _instance_to_request[id];
        brpc_request->set_eos(request.eos);
        brpc_request->set_packet_seq(_instance_to_seq[id]++);
        if (request.block_holder->get_block() &&
            !request.block_holder->get_block()->column_metas().empty()) {
            brpc_request->set_allocated_block(request.block_holder->get_block());
        }
        auto send_callback = request.channel->get_send_callback(id, request.eos);

        ExchangeRpcContext rpc_ctx;
        rpc_ctx._send_callback = send_callback;
        rpc_ctx.is_cancelled = false;
        _instance_to_rpc_ctx[id] = rpc_ctx;

        send_callback->cntl_->set_timeout_ms(request.channel->_brpc_timeout_ms);
        if (config::exchange_sink_ignore_eovercrowded) {
            send_callback->cntl_->ignore_eovercrowded();
        }
        send_callback->addFailedHandler([&, weak_task_ctx = weak_task_exec_ctx()](
                                                const InstanceLoId& id, const std::string& err) {
            auto task_lock = weak_task_ctx.lock();
            if (task_lock == nullptr) {
                // This means ExchangeSinkBuffer Ojbect already destroyed, not need run failed any more.
                return;
            }
            // attach task for memory tracker and query id when core
            SCOPED_ATTACH_TASK(_state);
            _failed(id, err);
        });
        send_callback->start_rpc_time = GetCurrentTimeNanos();
        send_callback->addSuccessHandler([&, weak_task_ctx = weak_task_exec_ctx()](
                                                 const InstanceLoId& id, const bool& eos,
                                                 const PTransmitDataResult& result,
                                                 const int64_t& start_rpc_time) {
            auto task_lock = weak_task_ctx.lock();
            if (task_lock == nullptr) {
                // This means ExchangeSinkBuffer Ojbect already destroyed, not need run failed any more.
                return;
            }
            // attach task for memory tracker and query id when core
            SCOPED_ATTACH_TASK(_state);
            set_rpc_time(id, start_rpc_time, result.receive_time());
            Status s(Status::create(result.status()));
            if (s.is<ErrorCode::END_OF_FILE>()) {
                _set_receiver_eof(id);
            } else if (!s.ok()) {
                _failed(id,
                        fmt::format("exchange req success but status isn't ok: {}", s.to_string()));
            } else if (eos) {
                _ended(id);
            } else {
                static_cast<void>(_send_rpc(id));
            }
        });
        {
            auto send_remote_block_closure =
                    AutoReleaseClosure<PTransmitDataParams,
                                       pipeline::ExchangeSendCallback<PTransmitDataResult>>::
                            create_unique(brpc_request, send_callback);
            if (enable_http_send_block(*brpc_request)) {
                RETURN_IF_ERROR(transmit_block_httpv2(_context->exec_env(),
                                                      std::move(send_remote_block_closure),
                                                      request.channel->_brpc_dest_addr));
            } else {
                transmit_blockv2(*request.channel->_brpc_stub,
                                 std::move(send_remote_block_closure));
            }
        }
        if (request.block_holder->get_block()) {
            static_cast<void>(brpc_request->release_block());
        }
        broadcast_q.pop();
    } else {
        _turn_off_channel(id);
    }

    return Status::OK();
}

void ExchangeSinkBuffer::_construct_request(InstanceLoId id, PUniqueId finst_id) {
    _instance_to_request[id] = std::make_shared<PTransmitDataParams>();
    _instance_to_request[id]->mutable_finst_id()->CopyFrom(finst_id);
    _instance_to_request[id]->mutable_query_id()->CopyFrom(_query_id);

    _instance_to_request[id]->set_node_id(_dest_node_id);
    _instance_to_request[id]->set_sender_id(_sender_id);
    _instance_to_request[id]->set_be_number(_be_number);
}

void ExchangeSinkBuffer::_ended(InstanceLoId id) {
    if (!_instance_to_package_queue_mutex.template contains(id)) {
        std::stringstream ss;
        ss << "failed find the instance id:" << id
           << " now mutex map size:" << _instance_to_package_queue_mutex.size();
        for (const auto& p : _instance_to_package_queue_mutex) {
            ss << " key:" << p.first << " value:" << p.second << "\n";
        }
        LOG(INFO) << ss.str();

        LOG(FATAL) << "not find the instance id";
        __builtin_unreachable();
    } else {
        std::unique_lock<std::mutex> lock(*_instance_to_package_queue_mutex[id]);
        _turn_off_channel(id);
    }
}

void ExchangeSinkBuffer::_failed(InstanceLoId id, const std::string& err) {
    _is_finishing = true;
    _context->cancel(Status::Cancelled(err));
    std::unique_lock<std::mutex> lock(*_instance_to_package_queue_mutex[id]);
    _turn_off_channel(id, true);
}

void ExchangeSinkBuffer::_set_receiver_eof(InstanceLoId id) {
    std::unique_lock<std::mutex> lock(*_instance_to_package_queue_mutex[id]);
    _instance_to_receiver_eof[id] = true;
    _turn_off_channel(id, true);
    std::queue<BroadcastTransmitInfo, std::list<BroadcastTransmitInfo>> empty;
    swap(empty, _instance_to_broadcast_package_queue[id]);
}

bool ExchangeSinkBuffer::_is_receiver_eof(InstanceLoId id) {
    std::unique_lock<std::mutex> lock(*_instance_to_package_queue_mutex[id]);
    return _instance_to_receiver_eof[id];
}

void ExchangeSinkBuffer::_turn_off_channel(InstanceLoId id, bool cleanup) {
    if (!_rpc_channel_is_idle[id]) {
        _rpc_channel_is_idle[id] = true;
        auto all_done = _busy_channels.fetch_sub(1) == 1;
        _set_ready_to_finish(all_done);
        if (cleanup && all_done) {
            auto weak_task_ctx = weak_task_exec_ctx();
            if (auto pip_ctx = weak_task_ctx.lock()) {
                _parent->set_reach_limit();
            }
        }
    }
}

void ExchangeSinkBuffer::get_max_min_rpc_time(int64_t* max_time, int64_t* min_time) {
    int64_t local_max_time = 0;
    int64_t local_min_time = INT64_MAX;
    for (auto& [id, time] : _instance_to_rpc_time) {
        if (time != 0) {
            local_max_time = std::max(local_max_time, time);
            local_min_time = std::min(local_min_time, time);
        }
    }
    *max_time = local_max_time;
    *min_time = local_min_time == INT64_MAX ? 0 : local_min_time;
}

int64_t ExchangeSinkBuffer::get_sum_rpc_time() {
    int64_t sum_time = 0;
    for (auto& [id, time] : _instance_to_rpc_time) {
        sum_time += time;
    }
    return sum_time;
}

void ExchangeSinkBuffer::set_rpc_time(InstanceLoId id, int64_t start_rpc_time,
                                      int64_t receive_rpc_time) {
    _rpc_count++;
    int64_t rpc_spend_time = receive_rpc_time - start_rpc_time;
    DCHECK(_instance_to_rpc_time.find(id) != _instance_to_rpc_time.end());
    if (rpc_spend_time > 0) {
        _instance_to_rpc_time[id] += rpc_spend_time;
    }
}

void ExchangeSinkBuffer::update_profile(RuntimeProfile* profile) {
    auto* _max_rpc_timer = ADD_TIMER(profile, "RpcMaxTime");
    auto* _min_rpc_timer = ADD_TIMER(profile, "RpcMinTime");
    auto* _sum_rpc_timer = ADD_TIMER(profile, "RpcSumTime");
    auto* _count_rpc = ADD_COUNTER(profile, "RpcCount", TUnit::UNIT);
    auto* _avg_rpc_timer = ADD_TIMER(profile, "RpcAvgTime");

    int64_t max_rpc_time = 0, min_rpc_time = 0;
    get_max_min_rpc_time(&max_rpc_time, &min_rpc_time);
    _max_rpc_timer->set(max_rpc_time);
    _min_rpc_timer->set(min_rpc_time);

    _count_rpc->set(_rpc_count);
    int64_t sum_time = get_sum_rpc_time();
    _sum_rpc_timer->set(sum_time);
    _avg_rpc_timer->set(sum_time / std::max(static_cast<int64_t>(1), _rpc_count.load()));
}

} // namespace pipeline
} // namespace doris
