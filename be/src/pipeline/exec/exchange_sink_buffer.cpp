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
#include <gen_cpp/types.pb.h>
#include <glog/logging.h>
#include <google/protobuf/stubs/callback.h>
#include <pdqsort.h>
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
#include "util/defer_op.h"
#include "util/proto_util.h"
#include "util/time.h"
#include "vec/sink/vdata_stream_sender.h"

namespace doris {
#include "common/compile_check_begin.h"

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
    if (_total_queue_buffer_size >= _total_queue_buffer_size_limit ||
        _total_queue_blocks_count >= _total_queue_blocks_count_limit) {
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
ExchangeSinkBuffer::ExchangeSinkBuffer(PUniqueId query_id, PlanNodeId dest_node_id,
                                       PlanNodeId node_id, RuntimeState* state,
                                       const std::vector<InstanceLoId>& sender_ins_ids)
        : HasTaskExecutionCtx(state),
          _queue_capacity(0),
          _is_failed(false),
          _query_id(std::move(query_id)),
          _dest_node_id(dest_node_id),
          _node_id(node_id),
          _state(state),
          _context(state->get_query_ctx()),
          _exchange_sink_num(sender_ins_ids.size()) {}

void ExchangeSinkBuffer::close() {
    // Could not clear the queue here, because there maybe a running rpc want to
    // get a request from the queue, and clear method will release the request
    // and it will core.
    //_instance_to_broadcast_package_queue.clear();
    //_instance_to_package_queue.clear();
    //_instance_to_request.clear();
}

void ExchangeSinkBuffer::construct_request(TUniqueId fragment_instance_id) {
    if (_is_failed) {
        return;
    }
    auto low_id = fragment_instance_id.lo;
    if (_rpc_instances.contains(low_id)) {
        return;
    }

    // Initialize the instance data
    auto instance_data = std::make_unique<RpcInstance>(low_id);
    instance_data->mutex = std::make_unique<std::mutex>();
    instance_data->seq = 0;
    instance_data->package_queue = std::queue<TransmitInfo, std::list<TransmitInfo>>();
    instance_data->broadcast_package_queue =
            std::queue<BroadcastTransmitInfo, std::list<BroadcastTransmitInfo>>();
    _queue_capacity = config::exchg_buffer_queue_capacity_factor * _rpc_instances.size();

    PUniqueId finst_id;
    finst_id.set_hi(fragment_instance_id.hi);
    finst_id.set_lo(fragment_instance_id.lo);

    instance_data->rpc_channel_is_idle = true;
    instance_data->rpc_channel_is_turn_off = false;

    // Initialize request
    instance_data->request = std::make_shared<PTransmitDataParams>();
    instance_data->request->mutable_finst_id()->CopyFrom(finst_id);
    instance_data->request->mutable_query_id()->CopyFrom(_query_id);
    instance_data->request->set_node_id(_dest_node_id);
    instance_data->running_sink_count = _exchange_sink_num;

    _rpc_instances[low_id] = std::move(instance_data);
}

Status ExchangeSinkBuffer::add_block(TransmitInfo&& request) {
    if (_is_failed) {
        return Status::OK();
    }
    auto ins_id = request.channel->dest_ins_id();
    if (!_rpc_instances.contains(ins_id)) {
        return Status::InternalError("fragment_instance_id {} not do register_sink",
                                     print_id(request.channel->_fragment_instance_id));
    }
    auto& instance_data = *_rpc_instances[ins_id];
    if (instance_data.rpc_channel_is_turn_off) {
        return Status::EndOfFile("receiver eof");
    }
    bool send_now = false;
    {
        std::unique_lock<std::mutex> lock(*instance_data.mutex);
        // Do not have in process rpc, directly send
        if (instance_data.rpc_channel_is_idle) {
            send_now = true;
            instance_data.rpc_channel_is_idle = false;
        }
        if (request.block) {
            RETURN_IF_ERROR(
                    BeExecVersionManager::check_be_exec_version(request.block->be_exec_version()));
            COUNTER_UPDATE(request.channel->_parent->memory_used_counter(),
                           request.block->ByteSizeLong());
        }
        instance_data.package_queue.emplace(std::move(request));
        _total_queue_size++;
        if (_total_queue_size > _queue_capacity) {
            for (auto& dep : _queue_deps) {
                dep->block();
            }
        }
    }
    if (send_now) {
        RETURN_IF_ERROR(_send_rpc(instance_data));
    }

    return Status::OK();
}

Status ExchangeSinkBuffer::add_block(BroadcastTransmitInfo&& request) {
    if (_is_failed) {
        return Status::OK();
    }
    auto ins_id = request.channel->dest_ins_id();
    if (!_rpc_instances.contains(ins_id)) {
        return Status::InternalError("fragment_instance_id {} not do register_sink",
                                     print_id(request.channel->_fragment_instance_id));
    }
    auto& instance_data = *_rpc_instances[ins_id];
    if (instance_data.rpc_channel_is_turn_off) {
        return Status::EndOfFile("receiver eof");
    }
    bool send_now = false;
    {
        std::unique_lock<std::mutex> lock(*instance_data.mutex);
        // Do not have in process rpc, directly send
        if (instance_data.rpc_channel_is_idle) {
            send_now = true;
            instance_data.rpc_channel_is_idle = false;
        }
        if (request.block_holder->get_block()) {
            RETURN_IF_ERROR(BeExecVersionManager::check_be_exec_version(
                    request.block_holder->get_block()->be_exec_version()));
        }
        instance_data.broadcast_package_queue.emplace(request);
    }
    if (send_now) {
        RETURN_IF_ERROR(_send_rpc(instance_data));
    }

    return Status::OK();
}

Status ExchangeSinkBuffer::_send_rpc(RpcInstance& instance_data) {
    std::unique_lock<std::mutex> lock(*(instance_data.mutex));

    std::queue<TransmitInfo, std::list<TransmitInfo>>& q = instance_data.package_queue;
    std::queue<BroadcastTransmitInfo, std::list<BroadcastTransmitInfo>>& broadcast_q =
            instance_data.broadcast_package_queue;

    if (_is_failed) {
        _turn_off_channel(instance_data, lock);
        return Status::OK();
    }
    if (instance_data.rpc_channel_is_turn_off) {
        return Status::OK();
    }

    if (!q.empty()) {
        // If we have data to shuffle which is not broadcasted
        auto& request = q.front();
        auto& brpc_request = instance_data.request;
        brpc_request->set_eos(request.eos);
        brpc_request->set_packet_seq(instance_data.seq++);
        brpc_request->set_sender_id(request.channel->_parent->sender_id());
        brpc_request->set_be_number(request.channel->_parent->be_number());
        if (request.block && !request.block->column_metas().empty()) {
            brpc_request->set_allocated_block(request.block.get());
        }
        auto send_callback = request.channel->get_send_callback(&instance_data, request.eos);

        send_callback->cntl_->set_timeout_ms(request.channel->_brpc_timeout_ms);
        if (config::execution_ignore_eovercrowded) {
            send_callback->cntl_->ignore_eovercrowded();
        }
        send_callback->addFailedHandler([&, weak_task_ctx = weak_task_exec_ctx()](
                                                RpcInstance* ins, const std::string& err) {
            auto task_lock = weak_task_ctx.lock();
            if (task_lock == nullptr) {
                // This means ExchangeSinkBuffer Ojbect already destroyed, not need run failed any more.
                return;
            }
            // attach task for memory tracker and query id when core
            SCOPED_ATTACH_TASK(_state);
            _failed(ins->id, err);
        });
        send_callback->start_rpc_time = GetCurrentTimeNanos();
        send_callback->addSuccessHandler([&, weak_task_ctx = weak_task_exec_ctx()](
                                                 RpcInstance* ins_ptr, const bool& eos,
                                                 const PTransmitDataResult& result,
                                                 const int64_t& start_rpc_time) {
            auto task_lock = weak_task_ctx.lock();
            if (task_lock == nullptr) {
                // This means ExchangeSinkBuffer Ojbect already destroyed, not need run failed any more.
                return;
            }
            // attach task for memory tracker and query id when core
            SCOPED_ATTACH_TASK(_state);

            auto& ins = *ins_ptr;
            auto end_rpc_time = GetCurrentTimeNanos();
            update_rpc_time(ins, start_rpc_time, end_rpc_time);

            Status s(Status::create(result.status()));
            if (s.is<ErrorCode::END_OF_FILE>()) {
                _set_receiver_eof(ins);
            } else if (!s.ok()) {
                _failed(ins.id,
                        fmt::format("exchange req success but status isn't ok: {}", s.to_string()));
                return;
            } else if (eos) {
                _ended(ins);
            }
            // The eos here only indicates that the current exchange sink has reached eos.
            // However, the queue still contains data from other exchange sinks, so RPCs need to continue being sent.
            s = _send_rpc(ins);
            if (!s) {
                _failed(ins.id,
                        fmt::format("exchange req success but status isn't ok: {}", s.to_string()));
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
            COUNTER_UPDATE(request.channel->_parent->memory_used_counter(),
                           -request.block->ByteSizeLong());
            static_cast<void>(brpc_request->release_block());
        }
        q.pop();
        _total_queue_size--;
        if (_total_queue_size <= _queue_capacity) {
            for (auto& dep : _queue_deps) {
                dep->set_ready();
            }
        }
    } else if (!broadcast_q.empty()) {
        // If we have data to shuffle which is broadcasted
        auto& request = broadcast_q.front();
        auto& brpc_request = instance_data.request;
        brpc_request->set_eos(request.eos);
        brpc_request->set_packet_seq(instance_data.seq++);
        brpc_request->set_sender_id(request.channel->_parent->sender_id());
        brpc_request->set_be_number(request.channel->_parent->be_number());
        if (request.block_holder->get_block() &&
            !request.block_holder->get_block()->column_metas().empty()) {
            brpc_request->set_allocated_block(request.block_holder->get_block());
        }
        auto send_callback = request.channel->get_send_callback(&instance_data, request.eos);
        send_callback->cntl_->set_timeout_ms(request.channel->_brpc_timeout_ms);
        if (config::execution_ignore_eovercrowded) {
            send_callback->cntl_->ignore_eovercrowded();
        }
        send_callback->addFailedHandler([&, weak_task_ctx = weak_task_exec_ctx()](
                                                RpcInstance* ins, const std::string& err) {
            auto task_lock = weak_task_ctx.lock();
            if (task_lock == nullptr) {
                // This means ExchangeSinkBuffer Ojbect already destroyed, not need run failed any more.
                return;
            }
            // attach task for memory tracker and query id when core
            SCOPED_ATTACH_TASK(_state);
            _failed(ins->id, err);
        });
        send_callback->start_rpc_time = GetCurrentTimeNanos();
        send_callback->addSuccessHandler([&, weak_task_ctx = weak_task_exec_ctx()](
                                                 RpcInstance* ins_ptr, const bool& eos,
                                                 const PTransmitDataResult& result,
                                                 const int64_t& start_rpc_time) {
            auto task_lock = weak_task_ctx.lock();
            if (task_lock == nullptr) {
                // This means ExchangeSinkBuffer Ojbect already destroyed, not need run failed any more.
                return;
            }
            // attach task for memory tracker and query id when core
            SCOPED_ATTACH_TASK(_state);
            auto& ins = *ins_ptr;
            auto end_rpc_time = GetCurrentTimeNanos();
            update_rpc_time(ins, start_rpc_time, end_rpc_time);

            Status s(Status::create(result.status()));
            if (s.is<ErrorCode::END_OF_FILE>()) {
                _set_receiver_eof(ins);
            } else if (!s.ok()) {
                _failed(ins.id,
                        fmt::format("exchange req success but status isn't ok: {}", s.to_string()));
                return;
            } else if (eos) {
                _ended(ins);
            }

            // The eos here only indicates that the current exchange sink has reached eos.
            // However, the queue still contains data from other exchange sinks, so RPCs need to continue being sent.
            s = _send_rpc(ins);
            if (!s) {
                _failed(ins.id,
                        fmt::format("exchange req success but status isn't ok: {}", s.to_string()));
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
        instance_data.rpc_channel_is_idle = true;
    }

    return Status::OK();
}

void ExchangeSinkBuffer::_ended(RpcInstance& ins) {
    std::unique_lock<std::mutex> lock(*ins.mutex);
    ins.running_sink_count--;
    if (ins.running_sink_count == 0) {
        _turn_off_channel(ins, lock);
    }
}

void ExchangeSinkBuffer::_failed(InstanceLoId id, const std::string& err) {
    _is_failed = true;
    LOG(INFO) << "send rpc failed, instance id: " << id << ", _dest_node_id: " << _dest_node_id
              << ", node id: " << _node_id << ", err: " << err;
    _context->cancel(Status::Cancelled(err));
}

void ExchangeSinkBuffer::_set_receiver_eof(RpcInstance& ins) {
    std::unique_lock<std::mutex> lock(*ins.mutex);
    // When the receiving side reaches eof, it means the receiver has finished early.
    // The remaining data in the current rpc_channel does not need to be sent,
    // and the rpc_channel should be turned off immediately.
    Defer turn_off([&]() { _turn_off_channel(ins, lock); });

    std::queue<BroadcastTransmitInfo, std::list<BroadcastTransmitInfo>>& broadcast_q =
            ins.broadcast_package_queue;
    for (; !broadcast_q.empty(); broadcast_q.pop()) {
        if (broadcast_q.front().block_holder->get_block()) {
            COUNTER_UPDATE(broadcast_q.front().channel->_parent->memory_used_counter(),
                           -broadcast_q.front().block_holder->get_block()->ByteSizeLong());
        }
    }
    {
        std::queue<BroadcastTransmitInfo, std::list<BroadcastTransmitInfo>> empty;
        swap(empty, broadcast_q);
    }

    std::queue<TransmitInfo, std::list<TransmitInfo>>& q = ins.package_queue;
    for (; !q.empty(); q.pop()) {
        // Must update _total_queue_size here, otherwise if _total_queue_size > _queue_capacity at EOF,
        // ExchangeSinkQueueDependency will be blocked and pipeline will be deadlocked
        _total_queue_size--;
        if (q.front().block) {
            COUNTER_UPDATE(q.front().channel->_parent->memory_used_counter(),
                           -q.front().block->ByteSizeLong());
        }
    }

    // Try to wake up pipeline after clearing the queue
    if (_total_queue_size <= _queue_capacity) {
        for (auto& dep : _queue_deps) {
            dep->set_ready();
        }
    }

    {
        std::queue<TransmitInfo, std::list<TransmitInfo>> empty;
        swap(empty, q);
    }
}

// The unused parameter `with_lock` is to ensure that the function is called when the lock is held.
void ExchangeSinkBuffer::_turn_off_channel(RpcInstance& ins,
                                           std::unique_lock<std::mutex>& /*with_lock*/) {
    if (!ins.rpc_channel_is_idle) {
        ins.rpc_channel_is_idle = true;
    }
    // Ensure that each RPC is turned off only once.
    if (ins.rpc_channel_is_turn_off) {
        return;
    }
    ins.rpc_channel_is_turn_off = true;
    auto weak_task_ctx = weak_task_exec_ctx();
    if (auto pip_ctx = weak_task_ctx.lock()) {
        for (auto& parent : _parents) {
            parent->on_channel_finished(ins.id);
        }
    }
}

void ExchangeSinkBuffer::get_max_min_rpc_time(int64_t* max_time, int64_t* min_time) {
    int64_t local_max_time = 0;
    int64_t local_min_time = INT64_MAX;
    for (auto& [_, ins] : _rpc_instances) {
        if (ins->stats.sum_time != 0) {
            local_max_time = std::max(local_max_time, ins->stats.sum_time);
            local_min_time = std::min(local_min_time, ins->stats.sum_time);
        }
    }
    *max_time = local_max_time;
    *min_time = local_min_time == INT64_MAX ? 0 : local_min_time;
}

int64_t ExchangeSinkBuffer::get_sum_rpc_time() {
    int64_t sum_time = 0;
    for (auto& [_, ins] : _rpc_instances) {
        sum_time += ins->stats.sum_time;
    }
    return sum_time;
}

void ExchangeSinkBuffer::update_rpc_time(RpcInstance& ins, int64_t start_rpc_time,
                                         int64_t receive_rpc_time) {
    _rpc_count++;
    int64_t rpc_spend_time = receive_rpc_time - start_rpc_time;
    if (rpc_spend_time > 0) {
        auto& stats = ins.stats;
        ++stats.rpc_count;
        stats.sum_time += rpc_spend_time;
        stats.max_time = std::max(stats.max_time, rpc_spend_time);
        stats.min_time = std::min(stats.min_time, rpc_spend_time);
    }
}

void ExchangeSinkBuffer::update_profile(RuntimeProfile* profile) {
    auto* _max_rpc_timer = ADD_TIMER_WITH_LEVEL(profile, "RpcMaxTime", 1);
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

    auto max_count = _state->rpc_verbose_profile_max_instance_count();
    // This counter will lead to performance degradation.
    // So only collect this information when the profile level is greater than 3.
    if (_state->profile_level() > 3 && max_count > 0) {
        std::vector<std::pair<InstanceLoId, RpcInstanceStatistics>> tmp_rpc_stats_vec;
        for (const auto& [id, ins] : _rpc_instances) {
            tmp_rpc_stats_vec.emplace_back(id, ins->stats);
        }
        pdqsort(tmp_rpc_stats_vec.begin(), tmp_rpc_stats_vec.end(),
                [](const auto& a, const auto& b) { return a.second.max_time > b.second.max_time; });
        auto count = std::min((size_t)max_count, tmp_rpc_stats_vec.size());
        int i = 0;
        auto* detail_profile = profile->create_child("RpcInstanceDetails", true, true);
        for (const auto& [id, stats] : tmp_rpc_stats_vec) {
            if (0 == stats.rpc_count) {
                continue;
            }
            std::stringstream out;
            out << "Instance " << std::hex << id;
            auto stats_str = fmt::format(
                    "Count: {}, MaxTime: {}, MinTime: {}, AvgTime: {}, SumTime: {}",
                    stats.rpc_count, PrettyPrinter::print(stats.max_time, TUnit::TIME_NS),
                    PrettyPrinter::print(stats.min_time, TUnit::TIME_NS),
                    PrettyPrinter::print(
                            stats.sum_time / std::max(static_cast<int64_t>(1), stats.rpc_count),
                            TUnit::TIME_NS),
                    PrettyPrinter::print(stats.sum_time, TUnit::TIME_NS));
            detail_profile->add_info_string(out.str(), stats_str);
            if (++i == count) {
                break;
            }
        }
    }
}

std::string ExchangeSinkBuffer::debug_each_instance_queue_size() {
    fmt::memory_buffer debug_string_buffer;
    for (auto& [id, instance_data] : _rpc_instances) {
        std::unique_lock<std::mutex> lock(*instance_data->mutex);
        fmt::format_to(debug_string_buffer, "Instance: {}, queue size: {}\n", id,
                       instance_data->package_queue.size());
    }
    return fmt::to_string(debug_string_buffer);
}

} // namespace pipeline
#include "common/compile_check_end.h"
} // namespace doris
