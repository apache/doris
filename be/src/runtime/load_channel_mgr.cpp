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

#include "runtime/load_channel.h"
#include "runtime/mem_tracker.h"
#include "service/backend_options.h"
#include "util/stopwatch.hpp"
#include "olap/lru_cache.h"

namespace doris {

LoadChannelMgr::LoadChannelMgr():_is_stopped(false) {
    _lastest_success_channel = new_lru_cache(1024);
}

Status LoadChannelMgr::init(int64_t process_mem_limit) {
    int64_t load_mem_limit = _calc_total_mem_limit(process_mem_limit);
    _mem_tracker.reset(new MemTracker(load_mem_limit, "load channel mgr"));
    RETURN_IF_ERROR(_start_bg_worker());
    return Status::OK();
}

int64_t LoadChannelMgr::_calc_total_mem_limit(int64_t process_mem_limit) {
    if (process_mem_limit == -1) {
        // no limit
        return -1;
    }
    int64_t load_mem_limit = process_mem_limit * (config::load_process_max_memory_limit_percent / 100.0);
    return std::min<int64_t>(load_mem_limit, config::load_process_max_memory_limit_bytes);
}

LoadChannelMgr::~LoadChannelMgr() {
    _is_stopped.store(true);
    if (_load_channels_clean_thread.joinable()) {
        _load_channels_clean_thread.join();
    }
    delete _lastest_success_channel;
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
            int64_t load_mem_limit = _calc_load_mem_limit(params.has_load_mem_limit() ? params.load_mem_limit() : -1);
            int64_t load_channel_timeout_s = _get_load_channel_timeout(params.has_load_channel_timeout_s() ? params.load_channel_timeout_s() : -1);
            channel.reset(new LoadChannel(load_id, load_mem_limit, _mem_tracker.get(), load_channel_timeout_s));
            _load_channels.insert({load_id, channel});
        }
    }

    RETURN_IF_ERROR(channel->open(params));
    return Status::OK();
}

int64_t LoadChannelMgr::_calc_load_mem_limit(int64_t mem_limit) {
    // default mem limit is used to be compatible with old request.
    // new request should be set load_mem_limit.
    const int64_t default_load_mem_limit = 2 * 1024 * 1024 * 1024L; // 2GB
    int64_t load_mem_limit = default_load_mem_limit;
    if (mem_limit != -1) {
        // mem limit of a certain load should between config::write_buffer_size and config::load_process_memory_limit_bytes
        load_mem_limit = std::max<int64_t>(mem_limit, config::write_buffer_size);
        load_mem_limit = std::min<int64_t>(_mem_tracker->limit(), load_mem_limit);
    }
    return load_mem_limit;
}

int64_t LoadChannelMgr::_get_load_channel_timeout(int64_t timeout_s) {
    int64_t load_channel_timeout_s = config::streaming_load_rpc_max_alive_time_sec;
    if (timeout_s > 0) {
        load_channel_timeout_s = std::max<int64_t>(load_channel_timeout_s, timeout_s);
    }
    return load_channel_timeout_s;
}

static void dummy_deleter(const CacheKey& key, void* value) {
}

Status LoadChannelMgr::add_batch(
        const PTabletWriterAddBatchRequest& request,
        google::protobuf::RepeatedPtrField<PTabletInfo>* tablet_vec,
        int64_t* wait_lock_time_ns) {

    UniqueId load_id(request.id());
    // 1. get load channel
    std::shared_ptr<LoadChannel> channel;
    {
        std::lock_guard<std::mutex> l(_lock);
        auto it = _load_channels.find(load_id);
        if (it == _load_channels.end()) {
            auto handle = _lastest_success_channel->lookup(load_id.to_string());
            // success only when eos be true
            if (handle != nullptr) {
                _lastest_success_channel->release(handle);
                if (request.has_eos() && request.eos()) {
                    return Status::OK();
                }
            }
            std::stringstream ss;
            ss << "load channel manager add batch with unknown load id: " << load_id;
            return Status::InternalError(ss.str());
        }
        channel = it->second;
    }

    // 2. check if mem consumption exceed limit
    _handle_mem_exceed_limit(); 

    // 3. add batch to load channel
    // batch may not exist in request(eg: eos request without batch),
    // this case will be handled in load channel's add batch method.
    RETURN_IF_ERROR(channel->add_batch(request, tablet_vec));

    // 4. handle finish
    if (channel->is_finished()) {
        LOG(INFO) << "removing load channel " << load_id << " because it's finished";
        {
            std::lock_guard<std::mutex> l(_lock);
            _load_channels.erase(load_id);
            auto handle = _lastest_success_channel->insert(
                load_id.to_string(), nullptr, 1, dummy_deleter);
            _lastest_success_channel->release(handle);
        }
        VLOG(1) << "removed load channel " << load_id;
    }
    return Status::OK();
}

void LoadChannelMgr::_handle_mem_exceed_limit() {
    // lock so that only one thread can check mem limit
    std::lock_guard<std::mutex> l(_lock);
    if (!_mem_tracker->limit_exceeded()) {
        return;
    }
    
    int64_t max_consume = 0;
    std::shared_ptr<LoadChannel> channel;
    for (auto& kv : _load_channels) {
        if (kv.second->mem_consumption() > max_consume) {
            max_consume = kv.second->mem_consumption();
            channel = kv.second;
        }
    }
    if (max_consume == 0) {
        // should not happen, add log to observe
        LOG(WARNING) << "failed to find suitable load channel when total load mem limit execeed";
        return;
    }
    DCHECK(channel.get() != nullptr);

    // force reduce mem limit of the selected channel
    LOG(INFO) << "reducing memory of " << *channel
              << " because total load mem consumption " << _mem_tracker->consumption()
              << " has exceeded limit " << _mem_tracker->limit();
    channel->handle_mem_exceed_limit(true);
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
    
    if (cancelled_channel.get() != nullptr) {
        cancelled_channel->cancel();
        LOG(INFO) << "load channel has been cancelled: " << load_id;
    }

    return Status::OK();
}

Status LoadChannelMgr::_start_bg_worker() {
    _load_channels_clean_thread = std::thread(
        [this] {
#ifdef GOOGLE_PROFILER
            ProfilerRegisterThread();
#endif

#ifndef BE_TEST
            uint32_t interval = 60;
#else
            uint32_t interval = 1;
#endif
            while (!_is_stopped.load()) {
                _start_load_channels_clean();
                sleep(interval);
            }
        });
    return Status::OK();
}

Status LoadChannelMgr::_start_load_channels_clean() {
    std::vector<std::shared_ptr<LoadChannel>> need_delete_channels;
    LOG(INFO) << "start cleaning load channels that have timed out";
    time_t now = time(nullptr);
    {
        std::vector<UniqueId> need_delete_channel_ids;
        std::lock_guard<std::mutex> l(_lock);
        VLOG(1) << "there are " << _load_channels.size() << " running load channels";
        int i = 0;
        for (auto& kv : _load_channels) {
            VLOG(1) << "load channel[" << i++ << "]: " << *(kv.second);
            time_t last_updated_time = kv.second->last_updated_time();
            if (difftime(now, last_updated_time) >= kv.second->timeout()) {
                need_delete_channel_ids.emplace_back(kv.first);
                need_delete_channels.emplace_back(kv.second);
            }
        }

        for(auto& key: need_delete_channel_ids) {
            _load_channels.erase(key);
            LOG(INFO) << "erase timeout load channel: " << key;
        }
    }

    // we must cancel these load channels before destroying them.
    // or some object may be invalid before trying to visit it.
    // eg: MemTracker in load channel
    for (auto& channel : need_delete_channels) {
        channel->cancel();
        LOG(INFO) << "load channel has been safely deleted: " << channel->load_id()
                  << ", timeout(s): " << channel->timeout();
    }

    // this log print every 1 min, so that we could observe the mem consumption of load process
    // on this Backend
    LOG(INFO) << "load mem consumption(bytes). limit: " << _mem_tracker->limit()
            << ", current: " << _mem_tracker->consumption()
            << ", peak: " << _mem_tracker->peak_consumption();

    return Status::OK();
}

}
