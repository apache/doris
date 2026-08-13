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

#include "load/channel/load_channel_mgr.h"

#include <fmt/format.h>
#include <gen_cpp/internal_service.pb.h>

#include <algorithm>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <ctime>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/logging.h"
#include "common/metrics/doris_metrics.h"
#include "common/metrics/metrics.h"
#include "load/channel/load_channel.h"
#include "runtime/exec_env.h"
#include "util/debug_points.h"
#include "util/thread.h"

namespace doris {

#ifndef BE_TEST
constexpr uint32_t START_BG_INTERVAL = 60;
#else
constexpr uint32_t START_BG_INTERVAL = 1;
#endif

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(load_channel_count, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_5ARG(load_channel_mem_consumption, MetricUnit::BYTES, "",
                                   mem_consumption, Labels({{"type", "load"}}));

static int64_t calc_channel_timeout_s(int64_t timeout_in_req_s) {
    int64_t load_channel_timeout_s = config::streaming_load_rpc_max_alive_time_sec;
    if (timeout_in_req_s > 0) {
        load_channel_timeout_s = std::max<int64_t>(load_channel_timeout_s, timeout_in_req_s);
    }
    return load_channel_timeout_s;
}

LoadChannelMgr::LoadChannelMgr() : _stop_background_threads_latch(1) {
    REGISTER_HOOK_METRIC(load_channel_count, [this]() {
        // std::lock_guard<std::mutex> l(_lock);
        return _load_channels.size();
    });
}

void LoadChannelMgr::stop() {
    DEREGISTER_HOOK_METRIC(load_channel_count);
    DEREGISTER_HOOK_METRIC(load_channel_mem_consumption);
    _stop_background_threads_latch.count_down();
    if (_load_channels_clean_thread) {
        _load_channels_clean_thread->join();
    }
}

Status LoadChannelMgr::init(int64_t process_mem_limit) {
    _load_state_channels = std::make_unique<LoadStateChannelCache>(1024);
    _final_tablet_result_cache = std::make_unique<FinalTabletResultCache>();
    RETURN_IF_ERROR(_start_bg_worker());
    return Status::OK();
}

Status LoadChannelMgr::open(const PTabletWriterOpenRequest& params) {
    UniqueId load_id(params.id());
    std::shared_ptr<LoadChannel> channel;
    {
        std::lock_guard<std::mutex> l(_lock);
        auto it = _load_channels.find(load_id);
        if (it != _load_channels.end()) {
            channel = it->second;
        } else {
            // create a new load channel
            int64_t timeout_in_req_s =
                    params.has_load_channel_timeout_s() ? params.load_channel_timeout_s() : -1;
            int64_t channel_timeout_s = calc_channel_timeout_s(timeout_in_req_s);
            bool is_high_priority = (params.has_is_high_priority() && params.is_high_priority());

            int64_t wg_id = -1;
            if (params.has_workload_group_id()) {
                wg_id = params.workload_group_id();
            }
            channel.reset(new LoadChannel(load_id, channel_timeout_s, is_high_priority,
                                          params.sender_ip(), params.backend_id(),
                                          params.enable_profile(), wg_id));
            _load_channels.insert({load_id, channel});
        }
    }

    RETURN_IF_ERROR(channel->open(params));

    return Status::OK();
}

Status LoadChannelMgr::_get_load_channel(std::shared_ptr<LoadChannel>& channel, bool& is_eof,
                                         const UniqueId& load_id,
                                         const PTabletWriterAddBlockRequest& request,
                                         PTabletWriterAddBlockResult* response) {
    is_eof = false;
    std::lock_guard<std::mutex> l(_lock);
    auto it = _load_channels.find(load_id);
    if (it == _load_channels.end()) {
        Cache::Handle* handle = _load_state_channels->lookup(load_id.to_string());
        if (handle != nullptr) {
            // load is cancelled
            if (auto* value = _load_state_channels->value(handle); value != nullptr) {
                const auto* cache_value = reinterpret_cast<CacheValue*>(value);
                const auto& cancel_reason = cache_value->_cancel_reason;
                _load_state_channels->release(handle);
                if (!cancel_reason.empty()) {
                    LOG(INFO) << fmt::format(
                            "The channel has been cancelled, load_id = {}, error = {}",
                            print_id(load_id), cancel_reason);
                    return Status::Cancelled(cancel_reason);
                }
            } else {
                // load is success, success only when eos be true
                _load_state_channels->release(handle);
                if (request.has_eos() && request.eos()) {
                    if (request.need_final_tablet_result()) {
                        auto* result_handle =
                                _final_tablet_result_cache->lookup(load_id.to_string());
                        if (result_handle == nullptr) {
                            LOG(WARNING) << "Final tablet result is unavailable for retried load "
                                         << load_id << ", sender_id=" << request.sender_id();
                            is_eof = true;
                            return Status::OK();
                        }
                        const auto* result_cache_value =
                                reinterpret_cast<FinalTabletResultCache::CacheValue*>(
                                        _final_tablet_result_cache->value(result_handle));
                        const auto result = result_cache_value->_results.find(request.index_id());
                        if (result == result_cache_value->_results.end()) {
                            _final_tablet_result_cache->release(result_handle);
                            LOG(WARNING) << "Final tablet result is unavailable for retried load "
                                         << load_id << ", index_id=" << request.index_id()
                                         << ", sender_id=" << request.sender_id();
                            is_eof = true;
                            return Status::OK();
                        }
                        response->CopyFrom(result->second.result);
                        response->set_final_tablet_result_fanout(request.sender_id() !=
                                                                 result->second.owner_sender_id);
                        _final_tablet_result_cache->release(result_handle);
                    }
                    is_eof = true;
                    return Status::OK();
                }
            }
        }

        return Status::InternalError<false>(
                "Fail to add batch in load channel: unknown load_id={}. "
                "This may be due to a BE restart. Please retry the load.",
                load_id.to_string());
    }
    channel = it->second;
    if (request.eos() && request.need_final_tablet_result()) {
        channel->_reserve_final_tablet_result(request.index_id());
    }
    return Status::OK();
}

Status LoadChannelMgr::add_batch(const PTabletWriterAddBlockRequest& request,
                                 PTabletWriterAddBlockResult* response,
                                 google::protobuf::Closure** done) {
    UniqueId load_id(request.id());
    // 1. get load channel
    std::shared_ptr<LoadChannel> channel;
    bool is_eof;
    auto status = _get_load_channel(channel, is_eof, load_id, request, response);
    if (!status.ok() || is_eof) {
        return status;
    }
    SCOPED_TIMER(channel->get_mgr_add_batch_timer());

    if (!channel->is_high_priority()) {
        // 2. check if mem consumption exceed limit
        // If this is a high priority load task, do not handle this.
        // because this may block for a while, which may lead to rpc timeout.
        SCOPED_TIMER(channel->get_handle_mem_limit_timer());
        ExecEnv::GetInstance()->memtable_memory_limiter()->handle_memtable_flush(
                [channel]() { return channel->is_cancelled(); }, channel->workload_group().get());
        if (channel->is_cancelled()) {
            return Status::Cancelled("LoadChannel has been cancelled: {}.", load_id.to_string());
        }
    }

    // 3. add batch to load channel
    // batch may not exist in request(eg: eos request without batch),
    // this case will be handled in load channel's add batch method.
    Status st = channel->add_batch(request, response, done);
    if (UNLIKELY(!st.ok())) {
        RETURN_IF_ERROR(channel->cancel(st));
        return st;
    }

    // 4. handle finish
    if (channel->is_finished()) {
        _finish_load_channel(load_id, channel);
    }
    return Status::OK();
}

void LoadChannelMgr::_finish_load_channel(const UniqueId load_id,
                                          const std::shared_ptr<LoadChannel>& channel) {
    VLOG_NOTICE << "removing load channel " << load_id << " because it's finished";
    const std::string cache_key = load_id.to_string();
    bool need_final_tablet_result = false;
    {
        std::lock_guard<std::mutex> l(_lock);
        const auto channel_it = _load_channels.find(load_id);
        if (channel_it == _load_channels.end() || channel_it->second != channel) {
            return;
        }
        need_final_tablet_result = channel->need_final_tablet_result();
        if (!need_final_tablet_result) {
            _load_channels.erase(channel_it);
        }
        // Cross-AZ retains the active channel for the lock-free copy, but publishes success now
        // so cancel cannot remove the only retry tombstone during that window.
        auto* handle = _load_state_channels->insert(cache_key, nullptr, 1, 1);
        _load_state_channels->release(handle);
    }
    if (!need_final_tablet_result) {
        VLOG_CRITICAL << "removed load channel " << load_id;
        return;
    }

    std::unique_ptr<FinalTabletResultCache::CacheValue> cache_value;
    DBUG_EXECUTE_IF("LoadChannelMgr.finish.before_copy", DBUG_RUN_CALLBACK());
    size_t cache_size = 0;
    std::unordered_map<int64_t, LoadChannel::FinalTabletResult> final_tablet_results;
    bool oversized = false;
    size_t result_bytes = 0;
    const size_t cache_overhead =
            sizeof(FinalTabletResultCache::CacheValue) + sizeof(LRUHandle) - 1 + cache_key.size();
    const bool ready = channel->copy_final_tablet_results(
            &final_tablet_results, FinalTabletResultCache::MAX_BYTES - cache_overhead, &oversized,
            &result_bytes);
    if (ready && !oversized && !final_tablet_results.empty()) {
        cache_value = std::make_unique<FinalTabletResultCache::CacheValue>();
        cache_value->_results = std::move(final_tablet_results);
        cache_size = sizeof(FinalTabletResultCache::CacheValue) + result_bytes;
    } else if (!ready || oversized) {
        LOG(WARNING) << "Skip caching final tablet result for load " << load_id
                     << ", ready=" << ready << ", oversized=" << oversized;
    }

    {
        std::lock_guard<std::mutex> l(_lock);
        const auto channel_it = _load_channels.find(load_id);
        if (channel_it == _load_channels.end() || channel_it->second != channel) {
            return;
        }
        _load_channels.erase(channel_it);
        _final_tablet_result_cache->erase(cache_key);
        if (cache_value != nullptr) {
            auto* result_handle = _final_tablet_result_cache->insert(cache_key, cache_value.get(),
                                                                     cache_size, cache_size);
            cache_value.release();
            _final_tablet_result_cache->release(result_handle);
        }
        auto* handle = _load_state_channels->insert(cache_key, nullptr, 1, 1);
        _load_state_channels->release(handle);
    }
    VLOG_CRITICAL << "removed load channel " << load_id;
}

Status LoadChannelMgr::cancel(const PTabletWriterCancelRequest& params) {
    UniqueId load_id(params.id());
    std::shared_ptr<LoadChannel> cancelled_channel;
    {
        std::lock_guard<std::mutex> l(_lock);
        if (_load_channels.contains(load_id)) {
            cancelled_channel = _load_channels[load_id];
            _load_channels.erase(load_id);
        }
        // We just need to record the first cancel msg
        auto* existing_handle = _load_state_channels->lookup(load_id.to_string());
        if (existing_handle == nullptr) {
            if (params.has_cancel_reason() && !params.cancel_reason().empty()) {
                std::unique_ptr<CacheValue> cancel_reason_ptr = std::make_unique<CacheValue>();
                cancel_reason_ptr->_cancel_reason = params.cancel_reason();
                size_t cache_capacity =
                        cancel_reason_ptr->_cancel_reason.capacity() + sizeof(CacheValue);
                auto* handle = _load_state_channels->insert(
                        load_id.to_string(), cancel_reason_ptr.get(), 1, cache_capacity);
                cancel_reason_ptr.release();
                _load_state_channels->release(handle);
                LOG(INFO) << fmt::format("load_id = {}, record_error reason = {}",
                                         print_id(load_id), params.cancel_reason());
            }
        } else {
            _load_state_channels->release(existing_handle);
        }
    }

    if (cancelled_channel != nullptr) {
        const Status reason = params.has_cancel_reason()
                                      ? Status::Cancelled(params.cancel_reason())
                                      : Status::Cancelled("Load channel cancelled");
        RETURN_IF_ERROR(cancelled_channel->cancel(reason));
        LOG(INFO) << "load channel has been cancelled: " << load_id;
    }

    return Status::OK();
}

Status LoadChannelMgr::_start_bg_worker() {
    RETURN_IF_ERROR(Thread::create(
            "LoadChannelMgr", "cancel_timeout_load_channels",
            [this]() {
                while (!_stop_background_threads_latch.wait_for(
                        std::chrono::seconds(START_BG_INTERVAL))) {
                    static_cast<void>(_start_load_channels_clean());
                }
            },
            &_load_channels_clean_thread));

    return Status::OK();
}

Status LoadChannelMgr::_start_load_channels_clean() {
    std::vector<std::shared_ptr<LoadChannel>> need_delete_channels;
    LOG(INFO) << "cleaning timed out load channels";
    time_t now = time(nullptr);
    {
        std::vector<UniqueId> need_delete_channel_ids;
        std::lock_guard<std::mutex> l(_lock);
        int i = 0;
        for (auto& kv : _load_channels) {
            VLOG_CRITICAL << "load channel[" << i++ << "]: " << *(kv.second);
            time_t last_updated_time = kv.second->last_updated_time();
            if (difftime(now, last_updated_time) >= kv.second->timeout()) {
                need_delete_channel_ids.emplace_back(kv.first);
                need_delete_channels.emplace_back(kv.second);
            }
        }

        for (auto& key : need_delete_channel_ids) {
            _load_channels.erase(key);
            LOG(INFO) << "erase timeout load channel: " << key;
        }
    }

    // we must cancel these load channels before destroying them.
    // otherwise some object may be invalid before trying to visit it.
    // eg: MemTracker in load channel
    for (auto& channel : need_delete_channels) {
        RETURN_IF_ERROR(channel->cancel(Status::TimedOut("Load channel timed out")));
        LOG(INFO) << "load channel has been safely deleted: " << channel->load_id()
                  << ", timeout(s): " << channel->timeout();
    }

    return Status::OK();
}
} // namespace doris
