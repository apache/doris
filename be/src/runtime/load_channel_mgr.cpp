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

#include <fmt/format.h>
#include <gen_cpp/internal_service.pb.h>

#include <algorithm>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <ctime>
#include <functional>
#include <map>
#include <memory>
#include <ostream>
#include <queue>
#include <string>
#include <tuple>
#include <vector>

#include "common/config.h"
#include "common/logging.h"
#include "runtime/exec_env.h"
#include "runtime/load_channel.h"
#include "runtime/memory/mem_tracker.h"
#include "util/doris_metrics.h"
#include "util/mem_info.h"
#include "util/metrics.h"
#include "util/perf_counters.h"
#include "util/pretty_printer.h"
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

LoadChannelMgr::~LoadChannelMgr() {
    delete _last_success_channel;
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

            channel.reset(new LoadChannel(load_id, channel_timeout_s, is_high_priority,
                                          params.sender_ip(), params.backend_id(),
                                          params.enable_profile()));
            _load_channels.insert({load_id, channel});
        }
    }

    RETURN_IF_ERROR(channel->open(params));

    return Status::OK();
}

static void dummy_deleter(const CacheKey& key, void* value) {}

Status LoadChannelMgr::_get_load_channel(std::shared_ptr<LoadChannel>& channel, bool& is_eof,
                                         const UniqueId& load_id,
                                         const PTabletWriterAddBlockRequest& request) {
    is_eof = false;
    std::lock_guard<std::mutex> l(_lock);
    auto it = _load_channels.find(load_id);
    if (it == _load_channels.end()) {
        auto handle = _last_success_channel->lookup(load_id.to_string());
        // success only when eos be true
        if (handle != nullptr) {
            _last_success_channel->release(handle);
            if (request.has_eos() && request.eos()) {
                is_eof = true;
                return Status::OK();
            }
        }
        return Status::InternalError("fail to add batch in load channel. unknown load_id={}",
                                     load_id.to_string());
    }
    channel = it->second;
    return Status::OK();
}

Status LoadChannelMgr::add_batch(const PTabletWriterAddBlockRequest& request,
                                 PTabletWriterAddBlockResult* response) {
    UniqueId load_id(request.id());
    // 1. get load channel
    std::shared_ptr<LoadChannel> channel;
    bool is_eof;
    auto status = _get_load_channel(channel, is_eof, load_id, request);
    if (!status.ok() || is_eof) {
        return status;
    }
    SCOPED_TIMER(channel->get_mgr_add_batch_timer());

    if (!channel->is_high_priority()) {
        // 2. check if mem consumption exceed limit
        // If this is a high priority load task, do not handle this.
        // because this may block for a while, which may lead to rpc timeout.
        SCOPED_TIMER(channel->get_handle_mem_limit_timer());
        ExecEnv::GetInstance()->memtable_memory_limiter()->handle_memtable_flush();
    }

    // 3. add batch to load channel
    // batch may not exist in request(eg: eos request without batch),
    // this case will be handled in load channel's add batch method.
    Status st = channel->add_batch(request, response);
    if (UNLIKELY(!st.ok())) {
        static_cast<void>(channel->cancel());
        return st;
    }

    // 4. handle finish
    if (channel->is_finished()) {
        _finish_load_channel(load_id);
    }
    return Status::OK();
}

void LoadChannelMgr::_finish_load_channel(const UniqueId load_id) {
    VLOG_NOTICE << "removing load channel " << load_id << " because it's finished";
    {
        std::lock_guard<std::mutex> l(_lock);
        if (_load_channels.find(load_id) != _load_channels.end()) {
            _load_channels.erase(load_id);
        }
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
        static_cast<void>(cancelled_channel->cancel());
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
        static_cast<void>(channel->cancel());
        LOG(INFO) << "load channel has been safely deleted: " << channel->load_id()
                  << ", timeout(s): " << channel->timeout();
    }

    return Status::OK();
}
} // namespace doris
