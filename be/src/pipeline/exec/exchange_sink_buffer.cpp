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

void BroadcastPBlockHolder::unref() noexcept {
    DCHECK_GT(_ref_count._value, 0);
    auto old_value = _ref_count._value.fetch_sub(1);
    if (_dep && old_value == 1) {
        _dep->return_available_block();
    }
}

} // namespace vectorized

namespace pipeline {

template <typename Parent>
ExchangeSinkBuffer<Parent>::ExchangeSinkBuffer(PUniqueId query_id, PlanNodeId dest_node_id,
                                               int send_id, int be_number, QueryContext* context)
        : _is_finishing(false),
          _query_id(query_id),
          _dest_node_id(dest_node_id),
          _sender_id(send_id),
          _be_number(be_number),
          _context(context) {}

template <typename Parent>
ExchangeSinkBuffer<Parent>::~ExchangeSinkBuffer() = default;

template <typename Parent>
void ExchangeSinkBuffer<Parent>::close() {
    _instance_to_broadcast_package_queue.clear();
    _instance_to_package_queue.clear();
    _instance_to_request.clear();
}

template <typename Parent>
bool ExchangeSinkBuffer<Parent>::can_write() const {
    size_t max_package_size = 64 * _instance_to_package_queue.size();
    size_t total_package_size = 0;
    for (auto& [_, q] : _instance_to_package_queue) {
        total_package_size += q.size();
    }
    return total_package_size <= max_package_size;
}

template <typename Parent>
bool ExchangeSinkBuffer<Parent>::is_pending_finish() {
    //note(wb) angly implementation here, because operator couples the scheduling logic
    // graceful implementation maybe as follows:
    // 1 make ExchangeSinkBuffer support try close which calls brpc::StartCancel
    // 2 make BlockScheduler calls tryclose when query is cancel
    DCHECK(_context != nullptr);
    bool need_cancel = _context->is_cancelled();

    for (auto& pair : _instance_to_package_queue_mutex) {
        std::unique_lock<std::mutex> lock(*(pair.second));
        auto& id = pair.first;
        if (!_rpc_channel_is_idle.at(id)) {
            // when pending finish, we need check whether current query is cancelled
            if (need_cancel && _instance_to_rpc_ctx.find(id) != _instance_to_rpc_ctx.end()) {
                auto& rpc_ctx = _instance_to_rpc_ctx[id];
                if (!rpc_ctx.is_cancelled) {
                    brpc::StartCancel(rpc_ctx._closure->cntl.call_id());
                    rpc_ctx.is_cancelled = true;
                }
            }
            return true;
        }
    }
    return false;
}

template <typename Parent>
void ExchangeSinkBuffer<Parent>::register_sink(TUniqueId fragment_instance_id) {
    if (_is_finishing) {
        return;
    }
    auto low_id = fragment_instance_id.lo;
    if (_instance_to_package_queue_mutex.count(low_id)) {
        return;
    }
    _instance_to_package_queue_mutex[low_id] = std::make_unique<std::mutex>();
    _instance_to_seq[low_id] = 0;
    _instance_to_package_queue[low_id] =
            std::queue<TransmitInfo<Parent>, std::list<TransmitInfo<Parent>>>();
    _instance_to_broadcast_package_queue[low_id] =
            std::queue<BroadcastTransmitInfo<Parent>, std::list<BroadcastTransmitInfo<Parent>>>();
    _queue_capacity = QUEUE_CAPACITY_FACTOR * _instance_to_package_queue.size();
    PUniqueId finst_id;
    finst_id.set_hi(fragment_instance_id.hi);
    finst_id.set_lo(fragment_instance_id.lo);
    _rpc_channel_is_idle[low_id] = true;
    _instance_to_rpc_ctx[low_id] = {};
    _instance_to_receiver_eof[low_id] = false;
    _instance_to_rpc_time[low_id] = 0;
    _construct_request(low_id, finst_id);
}

template <typename Parent>
Status ExchangeSinkBuffer<Parent>::add_block(TransmitInfo<Parent>&& request) {
    if (_is_finishing) {
        return Status::OK();
    }
    TUniqueId ins_id = request.channel->_fragment_instance_id;
    bool send_now = false;
    {
        std::unique_lock<std::mutex> lock(*_instance_to_package_queue_mutex[ins_id.lo]);
        // Do not have in process rpc, directly send
        if (_rpc_channel_is_idle[ins_id.lo]) {
            send_now = true;
            _rpc_channel_is_idle[ins_id.lo] = false;
            _busy_channels++;
            if (_finish_dependency) {
                _finish_dependency->block_finishing();
            }
        }
        _instance_to_package_queue[ins_id.lo].emplace(std::move(request));
        _total_queue_size++;
        if (_queue_dependency && _total_queue_size > _queue_capacity) {
            _queue_dependency->block_writing();
        }
    }
    if (send_now) {
        RETURN_IF_ERROR(_send_rpc(ins_id.lo));
    }

    return Status::OK();
}

template <typename Parent>
Status ExchangeSinkBuffer<Parent>::add_block(BroadcastTransmitInfo<Parent>&& request,
                                             [[maybe_unused]] bool* sent) {
    if (_is_finishing) {
        return Status::OK();
    }
    TUniqueId ins_id = request.channel->_fragment_instance_id;
    if (_is_receiver_eof(ins_id.lo)) {
        return Status::EndOfFile("receiver eof");
    }
    bool send_now = false;
    if (sent) {
        *sent = true;
    }
    request.block_holder->ref();
    {
        std::unique_lock<std::mutex> lock(*_instance_to_package_queue_mutex[ins_id.lo]);
        // Do not have in process rpc, directly send
        if (_rpc_channel_is_idle[ins_id.lo]) {
            send_now = true;
            _rpc_channel_is_idle[ins_id.lo] = false;
            _busy_channels++;
            if (_finish_dependency) {
                _finish_dependency->block_finishing();
            }
        }
        _instance_to_broadcast_package_queue[ins_id.lo].emplace(request);
    }
    if (send_now) {
        RETURN_IF_ERROR(_send_rpc(ins_id.lo));
    }

    return Status::OK();
}

template <typename Parent>
Status ExchangeSinkBuffer<Parent>::_send_rpc(InstanceLoId id) {
    std::unique_lock<std::mutex> lock(*_instance_to_package_queue_mutex[id]);

    DCHECK(_rpc_channel_is_idle[id] == false);

    std::queue<TransmitInfo<Parent>, std::list<TransmitInfo<Parent>>>& q =
            _instance_to_package_queue[id];
    std::queue<BroadcastTransmitInfo<Parent>, std::list<BroadcastTransmitInfo<Parent>>>&
            broadcast_q = _instance_to_broadcast_package_queue[id];

    if (_is_finishing) {
        _rpc_channel_is_idle[id] = true;
        _busy_channels--;
        if (_finish_dependency && _busy_channels == 0) {
            _finish_dependency->set_ready_to_finish();
        }
        return Status::OK();
    }

    if (!q.empty()) {
        // If we have data to shuffle which is not broadcasted
        auto& request = q.front();
        auto& brpc_request = _instance_to_request[id];
        brpc_request->set_eos(request.eos);
        brpc_request->set_packet_seq(_instance_to_seq[id]++);
        if (_statistics && _statistics->collected()) {
            auto statistic = brpc_request->mutable_query_statistics();
            _statistics->to_pb(statistic);
        }
        if (request.block) {
            brpc_request->set_allocated_block(request.block.get());
        }
        if (!request.exec_status.ok()) {
            request.exec_status.to_protobuf(brpc_request->mutable_exec_status());
        }
        auto* closure = request.channel->get_closure(id, request.eos, nullptr);

        _instance_to_rpc_ctx[id]._closure = closure;
        _instance_to_rpc_ctx[id].is_cancelled = false;

        closure->cntl.set_timeout_ms(request.channel->_brpc_timeout_ms);
        if (config::exchange_sink_ignore_eovercrowded) {
            closure->cntl.ignore_eovercrowded();
        }
        closure->addFailedHandler(
                [&](const InstanceLoId& id, const std::string& err) { _failed(id, err); });
        closure->start_rpc_time = GetCurrentTimeNanos();
        closure->addSuccessHandler([&](const InstanceLoId& id, const bool& eos,
                                       const PTransmitDataResult& result,
                                       const int64_t& start_rpc_time) {
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
            SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(ExecEnv::GetInstance()->orphan_mem_tracker());
            if (enable_http_send_block(*brpc_request)) {
                RETURN_IF_ERROR(transmit_block_http(_context->exec_env(), closure, *brpc_request,
                                                    request.channel->_brpc_dest_addr));
            } else {
                transmit_block(*request.channel->_brpc_stub, closure, *brpc_request);
            }
        }
        if (request.block) {
            static_cast<void>(brpc_request->release_block());
        }
        q.pop();
        _total_queue_size--;
        if (_queue_dependency && _total_queue_size <= _queue_capacity) {
            _queue_dependency->set_ready_for_write();
        }
    } else if (!broadcast_q.empty()) {
        // If we have data to shuffle which is broadcasted
        auto& request = broadcast_q.front();
        auto& brpc_request = _instance_to_request[id];
        brpc_request->set_eos(request.eos);
        brpc_request->set_packet_seq(_instance_to_seq[id]++);
        if (request.block_holder->get_block()) {
            brpc_request->set_allocated_block(request.block_holder->get_block());
        }
        if (_statistics && _statistics->collected()) {
            auto statistic = brpc_request->mutable_query_statistics();
            _statistics->to_pb(statistic);
        }
        auto* closure = request.channel->get_closure(id, request.eos, request.block_holder);

        ExchangeRpcContext rpc_ctx;
        rpc_ctx._closure = closure;
        rpc_ctx.is_cancelled = false;
        _instance_to_rpc_ctx[id] = rpc_ctx;

        closure->cntl.set_timeout_ms(request.channel->_brpc_timeout_ms);
        if (config::exchange_sink_ignore_eovercrowded) {
            closure->cntl.ignore_eovercrowded();
        }
        closure->addFailedHandler(
                [&](const InstanceLoId& id, const std::string& err) { _failed(id, err); });
        closure->start_rpc_time = GetCurrentTimeNanos();
        closure->addSuccessHandler([&](const InstanceLoId& id, const bool& eos,
                                       const PTransmitDataResult& result,
                                       const int64_t& start_rpc_time) {
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
            SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(ExecEnv::GetInstance()->orphan_mem_tracker());
            if (enable_http_send_block(*brpc_request)) {
                RETURN_IF_ERROR(transmit_block_http(_context->exec_env(), closure, *brpc_request,
                                                    request.channel->_brpc_dest_addr));
            } else {
                transmit_block(*request.channel->_brpc_stub, closure, *brpc_request);
            }
        }
        if (request.block_holder->get_block()) {
            static_cast<void>(brpc_request->release_block());
        }
        broadcast_q.pop();
    } else {
        _rpc_channel_is_idle[id] = true;
        _busy_channels--;
        if (_finish_dependency && _busy_channels == 0) {
            _finish_dependency->set_ready_to_finish();
        }
    }

    return Status::OK();
}

template <typename Parent>
void ExchangeSinkBuffer<Parent>::_construct_request(InstanceLoId id, PUniqueId finst_id) {
    _instance_to_request[id] = std::make_unique<PTransmitDataParams>();
    _instance_to_request[id]->mutable_finst_id()->CopyFrom(finst_id);
    _instance_to_request[id]->mutable_query_id()->CopyFrom(_query_id);

    _instance_to_request[id]->set_node_id(_dest_node_id);
    _instance_to_request[id]->set_sender_id(_sender_id);
    _instance_to_request[id]->set_be_number(_be_number);
}

template <typename Parent>
void ExchangeSinkBuffer<Parent>::_ended(InstanceLoId id) {
    std::unique_lock<std::mutex> lock(*_instance_to_package_queue_mutex[id]);
    if (!_rpc_channel_is_idle[id]) {
        _busy_channels--;
        if (_finish_dependency && _busy_channels == 0) {
            _finish_dependency->set_ready_to_finish();
        }
    }
    _rpc_channel_is_idle[id] = true;
}

template <typename Parent>
void ExchangeSinkBuffer<Parent>::_failed(InstanceLoId id, const std::string& err) {
    _is_finishing = true;
    _context->cancel(true, err, Status::Cancelled(err));
    _ended(id);
}

template <typename Parent>
void ExchangeSinkBuffer<Parent>::_set_receiver_eof(InstanceLoId id) {
    std::unique_lock<std::mutex> lock(*_instance_to_package_queue_mutex[id]);
    _instance_to_receiver_eof[id] = true;
    if (!_rpc_channel_is_idle[id]) {
        _busy_channels--;
        if (_finish_dependency && _busy_channels == 0) {
            _finish_dependency->set_ready_to_finish();
        }
    }
    _rpc_channel_is_idle[id] = true;
}

template <typename Parent>
bool ExchangeSinkBuffer<Parent>::_is_receiver_eof(InstanceLoId id) {
    std::unique_lock<std::mutex> lock(*_instance_to_package_queue_mutex[id]);
    return _instance_to_receiver_eof[id];
}

template <typename Parent>
void ExchangeSinkBuffer<Parent>::get_max_min_rpc_time(int64_t* max_time, int64_t* min_time) {
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

template <typename Parent>
int64_t ExchangeSinkBuffer<Parent>::get_sum_rpc_time() {
    int64_t sum_time = 0;
    for (auto& [id, time] : _instance_to_rpc_time) {
        sum_time += time;
    }
    return sum_time;
}

template <typename Parent>
void ExchangeSinkBuffer<Parent>::set_rpc_time(InstanceLoId id, int64_t start_rpc_time,
                                              int64_t receive_rpc_time) {
    _rpc_count++;
    int64_t rpc_spend_time = receive_rpc_time - start_rpc_time;
    DCHECK(_instance_to_rpc_time.find(id) != _instance_to_rpc_time.end());
    if (rpc_spend_time > 0) {
        _instance_to_rpc_time[id] += rpc_spend_time;
    }
}

template <typename Parent>
void ExchangeSinkBuffer<Parent>::update_profile(RuntimeProfile* profile) {
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

template class ExchangeSinkBuffer<vectorized::VDataStreamSender>;
template class ExchangeSinkBuffer<pipeline::ExchangeSinkLocalState>;

} // namespace pipeline
} // namespace doris
