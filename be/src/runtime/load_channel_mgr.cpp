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

#include <cstdint>
#include <unordered_map>
#include <utility>

#include "common/object_pool.h"
#include "exec/tablet_info.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/row_batch.h"
#include "runtime/tuple_row.h"
#include "service/backend_options.h"
#include "util/stopwatch.hpp"
#include "olap/delta_writer.h"
#include "olap/lru_cache.h"

namespace doris {

LoadChannelMgr::LoadChannelMgr(ExecEnv* exec_env) :_exec_env(exec_env) {
    _load_channels.init(2011);
    _lastest_success_channel = new_lru_cache(1024);
}

LoadChannelMgr::~LoadChannelMgr() {
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
            // create a new 
            channel.reset(new LoadChannel(load_id));
            _load_channels.insert(load_id, channel);
        }
    }

    RETURN_IF_ERROR(channel->open(params));
    return Status::OK();
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
            if (handle != nullptr && request.has_eos() && request.eos()) {
                _lastest_success_channel->release(handle);
                return Status::OK();
            }
            std::stringstream ss;
            ss << "TabletWriter add batch with unknown load id: " << load_id;
            return Status::InternalError(ss.str());
        }
        channel = it->second;
    }

    // 2. add batch to load channel
    if (request.has_row_batch()) {
        RETURN_IF_ERROR(channel->add_batch(request, tablet_vec));
    }

    // 3. handle finish
    if (channel->is_finished()) {
        std::lock_guard<std::mutex> l(_lock);
        _load_channels.erase(load_id);
        auto handle = _lastest_success_channel->insert(
                load_id.to_string(), nullptr, 1, dummy_deleter);
        _lastest_success_channel->release(handle);
    }
    return Status::OK();
}

Status LoadChannelMgr::cancel(const PTabletWriterCancelRequest& params) {
    UniqueId load_id(request.id());
    {
        std::lock_guard<std::mutex> l(_lock);
        _load_channels.erase(load_id);
    }
    return Status::OK();
}

Status LoadChannelMgr::start_bg_worker() {
    _tablets_channel_clean_thread = std::thread(
        [this] {
            #ifdef GOOGLE_PROFILER
                ProfilerRegisterThread();
            #endif

            uint32_t interval = 60;
            while (true) {
                _start_load_channels_clean();
                sleep(interval);
            }
        });
    _tablets_channel_clean_thread.detach();
    return Status::OK();
}

Status LoadChannelMgr::_start_load_channels_clean() {
    const int32_t max_alive_time = config::streaming_load_rpc_max_alive_time_sec;
    time_t now = time(nullptr);
    {
        std::lock_guard<std::mutex> l(_lock);
        std::vector<UniqueId> need_delete_channel_ids;

        for (auto& kv : _load_channels) {
            time_t last_updated_time = kv.second->last_updated_time();
            if (difftime(now, last_updated_time) >= max_alive_time) {
                need_delete_channel_ids.emplace_back(kv.first);
            }
        }

        for(auto& key: need_delete_channel_ids) {
            _load_channels.erase(key);
            LOG(INFO) << "erase timeout load channel: " << key;
        }
    }
    return Status::OK();
}

}
