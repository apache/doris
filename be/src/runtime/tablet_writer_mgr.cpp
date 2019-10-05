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

#include "runtime/tablet_writer_mgr.h"

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

TabletWriterMgr::TabletWriterMgr(ExecEnv* exec_env) :_exec_env(exec_env) {
    _tablets_channels.init(2011);
    _lastest_success_channel = new_lru_cache(1024);
}

TabletWriterMgr::~TabletWriterMgr() {
    delete _lastest_success_channel;
}

Status TabletWriterMgr::open(const PTabletWriterOpenRequest& params) {
    TabletsChannelKey key(params.id(), params.index_id());
    std::shared_ptr<TabletsChannel> channel;
    {
        std::lock_guard<std::mutex> l(_lock);
        auto val = _tablets_channels.seek(key);
        if (val != nullptr) {
            channel = *val;
        } else {
            // create a new 
            channel.reset(new TabletsChannel(key));
            _tablets_channels.insert(key, channel);
        }
    }
    RETURN_IF_ERROR(channel->open(params));
    return Status::OK();
}

static void dummy_deleter(const CacheKey& key, void* value) {
}

Status TabletWriterMgr::add_batch(
        const PTabletWriterAddBatchRequest& request,
        google::protobuf::RepeatedPtrField<PTabletInfo>* tablet_vec,
        int64_t* wait_lock_time_ns) {
    TabletsChannelKey key(request.id(), request.index_id());
    std::shared_ptr<TabletsChannel> channel;
    {
        MonotonicStopWatch timer;
        timer.start();
        std::lock_guard<std::mutex> l(_lock);
        *wait_lock_time_ns += timer.elapsed_time();
        auto value = _tablets_channels.seek(key);
        if (value == nullptr) {
            auto handle = _lastest_success_channel->lookup(key.to_string());
            // success only when eos be true
            if (handle != nullptr && request.has_eos() && request.eos()) {
                _lastest_success_channel->release(handle);
                return Status::OK();
            }
            std::stringstream ss;
            ss << "TabletWriter add batch with unknown id, key=" << key;
            return Status::InternalError(ss.str());
        }
        channel = *value;
    }
    if (request.has_row_batch()) {
        RETURN_IF_ERROR(channel->add_batch(request));
    }
    Status st;
    if (request.has_eos() && request.eos()) {
        bool finished = false;
        st = channel->close(request.sender_id(), &finished, request.partition_ids(), tablet_vec);
        if (!st.ok()) {
            LOG(WARNING) << "channle close failed, key=" << key
                << ", sender_id=" << request.sender_id()
                << ", err_msg=" << st.get_error_msg();
        }
        if (finished) {
            MonotonicStopWatch timer;
            timer.start();
            std::lock_guard<std::mutex> l(_lock);
            *wait_lock_time_ns += timer.elapsed_time();
            _tablets_channels.erase(key);
            if (st.ok()) {
                auto handle = _lastest_success_channel->insert(
                    key.to_string(), nullptr, 1, dummy_deleter);
                _lastest_success_channel->release(handle);
            }
        }
    }
    return st;
}

Status TabletWriterMgr::cancel(const PTabletWriterCancelRequest& params) {
    TabletsChannelKey key(params.id(), params.index_id());
    {
        std::lock_guard<std::mutex> l(_lock);
        _tablets_channels.erase(key);
    }
    return Status::OK();
}

Status TabletWriterMgr::start_bg_worker() {
    _tablets_channel_clean_thread = std::thread(
        [this] {
            #ifdef GOOGLE_PROFILER
                ProfilerRegisterThread();
            #endif

            uint32_t interval = 60;
            while (true) {
                _start_tablets_channel_clean();
                sleep(interval);
            }
        });
    _tablets_channel_clean_thread.detach();
    return Status::OK();
}

Status TabletWriterMgr::_start_tablets_channel_clean() {
    const int32_t max_alive_time = config::streaming_load_rpc_max_alive_time_sec;
    time_t now = time(nullptr);
    {
        std::lock_guard<std::mutex> l(_lock);
        std::vector<TabletsChannelKey> need_delete_keys;

        for (auto& kv : _tablets_channels) {
            time_t last_updated_time = kv.second->last_updated_time();
            if (difftime(now, last_updated_time) >= max_alive_time) {
                need_delete_keys.emplace_back(kv.first);
            }
        }

        for(auto& key: need_delete_keys) {
            _tablets_channels.erase(key);
            LOG(INFO) << "erase timeout tablets channel: " << key;
        }
    }
    return Status::OK();
}

}
