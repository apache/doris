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

#include "runtime/load_channel_mgr.h"

#include "gutil/strings/substitute.h"
#include "runtime/load_channel.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/thread_context.h"
#include "service/backend_options.h"
#include "util/doris_metrics.h"
#include "util/stopwatch.hpp"

namespace doris {

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(load_channel_count, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_5ARG(load_channel_mem_consumption, MetricUnit::BYTES, "",
                                   mem_consumption, Labels({{"type", "load"}}));

// Calculate the total memory limit of all load tasks on this BE
static int64_t calc_process_max_load_memory(int64_t process_mem_limit) {
    if (process_mem_limit == -1) {
        // no limit
        return -1;
    }
    int32_t max_load_memory_percent = config::load_process_max_memory_limit_percent;
    int64_t max_load_memory_bytes = process_mem_limit * max_load_memory_percent / 100;
    return std::min<int64_t>(max_load_memory_bytes, config::load_process_max_memory_limit_bytes);
}

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

LoadChannelMgr::~LoadChannelMgr() {
    DEREGISTER_HOOK_METRIC(load_channel_count);
    DEREGISTER_HOOK_METRIC(load_channel_mem_consumption);
    _stop_background_threads_latch.count_down();
    if (_load_channels_clean_thread) {
        _load_channels_clean_thread->join();
    }
    delete _last_success_channel;
}

Status LoadChannelMgr::init(int64_t process_mem_limit) {
    _load_hard_mem_limit = calc_process_max_load_memory(process_mem_limit);
    _load_soft_mem_limit = _load_hard_mem_limit * config::load_process_soft_mem_limit_percent / 100;
    _mem_tracker = std::make_unique<MemTracker>("LoadChannelMgr");
    _mem_tracker_set = std::make_unique<MemTrackerLimiter>(MemTrackerLimiter::Type::LOAD,
                                                           "LoadChannelMgrTrackerSet");
    REGISTER_HOOK_METRIC(load_channel_mem_consumption,
                         [this]() { return _mem_tracker->consumption(); });
    _last_success_channel = new_lru_cache("LastestSuccessChannelCache", 1024);
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

            // Use the same mem limit as LoadChannelMgr for a single load channel
#ifndef BE_TEST
            auto channel_mem_tracker = std::make_unique<MemTracker>(
                    fmt::format("LoadChannel#senderIp={}#loadID={}", params.sender_ip(),
                                load_id.to_string()),
                    nullptr, ExecEnv::GetInstance()->load_channel_mgr()->mem_tracker_set());
#else
            auto channel_mem_tracker = std::make_unique<MemTracker>(fmt::format(
                    "LoadChannel#senderIp={}#loadID={}", params.sender_ip(), load_id.to_string()));
#endif
            channel.reset(new LoadChannel(load_id, std::move(channel_mem_tracker),
                                          channel_timeout_s, is_high_priority, params.sender_ip(),
                                          params.is_vectorized()));
            _load_channels.insert({load_id, channel});
        }
    }

    RETURN_IF_ERROR(channel->open(params));
    return Status::OK();
}

static void dummy_deleter(const CacheKey& key, void* value) {}

void LoadChannelMgr::_finish_load_channel(const UniqueId load_id) {
    VLOG_NOTICE << "removing load channel " << load_id << " because it's finished";
    {
        std::lock_guard<std::mutex> l(_lock);
        _load_channels.erase(load_id);
        auto handle = _last_success_channel->insert(load_id.to_string(), nullptr, 1, dummy_deleter);
        _last_success_channel->release(handle);
    }
    VLOG_CRITICAL << "removed load channel " << load_id;
}

Status LoadChannelMgr::cancel(const PTabletWriterCancelRequest& params) {
    UniqueId load_id(params.id());
    std::shared_ptr<LoadChannel> cancelled_channel;
    {
        std::lock_guard<std::mutex> l(_lock);
        if (_load_channels.find(load_id) != _load_channels.end()) {
            cancelled_channel = _load_channels[load_id];
            _load_channels.erase(load_id);
        }
    }

    if (cancelled_channel != nullptr) {
        cancelled_channel->cancel();
        LOG(INFO) << "load channel has been cancelled: " << load_id;
    }

    return Status::OK();
}

Status LoadChannelMgr::_start_bg_worker() {
    RETURN_IF_ERROR(Thread::create(
            "LoadChannelMgr", "cancel_timeout_load_channels",
            [this]() {
#ifdef GOOGLE_PROFILER
                ProfilerRegisterThread();
#endif
#ifndef BE_TEST
                uint32_t interval = 60;
#else
                uint32_t interval = 1;
#endif
                while (!_stop_background_threads_latch.wait_for(std::chrono::seconds(interval))) {
                    _start_load_channels_clean();
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
        VLOG_CRITICAL << "there are " << _load_channels.size() << " running load channels";
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
        channel->cancel();
        LOG(INFO) << "load channel has been safely deleted: " << channel->load_id()
                  << ", timeout(s): " << channel->timeout();
    }

    // this log print every 1 min, so that we could observe the mem consumption of load process
    // on this Backend
    LOG(INFO) << "load mem consumption(bytes). limit: " << _load_hard_mem_limit
              << ", current: " << _mem_tracker->consumption()
              << ", peak: " << _mem_tracker->peak_consumption();

    return Status::OK();
}

} // namespace doris
