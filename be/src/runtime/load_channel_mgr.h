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

#pragma once

#include <ctime>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>

#include "common/status.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "gutil/ref_counted.h"
#include "olap/lru_cache.h"
#include "runtime/load_channel.h"
#include "runtime/tablets_channel.h"
#include "runtime/thread_context.h"
#include "util/countdown_latch.h"
#include "util/thread.h"
#include "util/uid_util.h"

namespace doris {

class Cache;

// LoadChannelMgr -> LoadChannel -> TabletsChannel -> DeltaWriter
// All dispatched load data for this backend is routed from this class
class LoadChannelMgr {
public:
    LoadChannelMgr();
    ~LoadChannelMgr();

    Status init(int64_t process_mem_limit);

    // open a new load channel if not exist
    Status open(const PTabletWriterOpenRequest& request);

    template <typename TabletWriterAddRequest, typename TabletWriterAddResult>
    Status add_batch(const TabletWriterAddRequest& request, TabletWriterAddResult* response);

    // cancel all tablet stream for 'load_id' load
    Status cancel(const PTabletWriterCancelRequest& request);

private:
    template <typename Request>
    Status _get_load_channel(std::shared_ptr<LoadChannel>& channel, bool& is_eof,
                             const UniqueId& load_id, const Request& request);

    void _finish_load_channel(UniqueId load_id);
    // check if the total load mem consumption exceeds limit.
    // If yes, it will pick a load channel to try to reduce memory consumption.
    template <typename TabletWriterAddResult>
    Status _handle_mem_exceed_limit(TabletWriterAddResult* response);
    void _pending_if_hard_limit_exceeded();

    Status _start_bg_worker();

protected:
    // lock protect the load channel map
    std::mutex _lock;
    // load id -> load channel
    std::unordered_map<UniqueId, std::shared_ptr<LoadChannel>> _load_channels;
    std::shared_ptr<LoadChannel> _reduce_memory_channel = nullptr;
    Cache* _last_success_channel = nullptr;

    // check the total load channel mem consumption of this Backend
    std::shared_ptr<MemTrackerLimiter> _mem_tracker;
    int64_t _load_process_soft_limit = -1;

    // If hard limit reached, one thread will trigger load channel flush,
    // other threads should wait on the condition variable.
    bool _hard_limit_reached = false;
    std::condition_variable _hard_limit_cond;

    CountDownLatch _stop_background_threads_latch;
    // thread to clean timeout load channels
    scoped_refptr<Thread> _load_channels_clean_thread;
    Status _start_load_channels_clean();
};

template <typename Request>
Status LoadChannelMgr::_get_load_channel(std::shared_ptr<LoadChannel>& channel, bool& is_eof,
                                         const UniqueId& load_id, const Request& request) {
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

template <typename TabletWriterAddRequest, typename TabletWriterAddResult>
Status LoadChannelMgr::add_batch(const TabletWriterAddRequest& request,
                                 TabletWriterAddResult* response) {
    UniqueId load_id(request.id());
    // 1. get load channel
    std::shared_ptr<LoadChannel> channel;
    bool is_eof;
    auto status = _get_load_channel(channel, is_eof, load_id, request);
    if (!status.ok() || is_eof) {
        return status;
    }

    if (!channel->is_high_priority()) {
        // 2. check if mem consumption exceed limit
        // If this is a high priority load task, do not handle this.
        // because this may block for a while, which may lead to rpc timeout.
        RETURN_IF_ERROR(_handle_mem_exceed_limit(response));
    }

    // 3. add batch to load channel
    // batch may not exist in request(eg: eos request without batch),
    // this case will be handled in load channel's add batch method.
    RETURN_IF_ERROR(channel->add_batch(request, response));

    // 4. handle finish
    if (channel->is_finished()) {
        _finish_load_channel(load_id);
    }
    return Status::OK();
}

template <typename TabletWriterAddResult>
Status LoadChannelMgr::_handle_mem_exceed_limit(TabletWriterAddResult* response) {
    _pending_if_hard_limit_exceeded();
    // Check limit and pick load channel to reduce memory.
    std::shared_ptr<LoadChannel> channel;
    {
        std::lock_guard<std::mutex> l(_lock);
        // Check the soft limit.
        if (_mem_tracker->consumption() < _load_process_soft_limit) {
            return Status::OK();
        }

        // Some other thread is flushing data, and not reached hard limit now,
        // we don't need to handle mem limit in current thread.
        if (_reduce_memory_channel != nullptr && !_mem_tracker->limit_exceeded()) {
            return Status::OK();
        }

        // We need to pick a LoadChannel to reduce memory usage.
        // If `_reduce_memory_channel` is not null, it means the hard limit is
        // exceed now, we still need to pick a load channel again. Because
        // `_reduce_memory_channel` might not be the largest consumer now.
        int64_t max_consume = 0;
        for (auto& kv : _load_channels) {
            if (kv.second->is_high_priority()) {
                // do not select high priority channel to reduce memory
                // to avoid blocking them.
                continue;
            }
            if (kv.second->mem_consumption() > max_consume) {
                max_consume = kv.second->mem_consumption();
                channel = kv.second;
            }
        }
        if (max_consume == 0) {
            // should not happen, add log to observe
            LOG(WARNING) << "failed to find suitable load channel when total load mem limit exceed";
            return Status::OK();
        }
        DCHECK(channel.get() != nullptr);
        _reduce_memory_channel = channel;

        std::ostringstream oss;
        oss << "reducing memory of " << *channel << " because total load mem consumption "
            << _mem_tracker->consumption() << " has exceeded";
        if (_mem_tracker->limit_exceeded()) {
            _hard_limit_reached = true;
            oss << " hard limit: " << _mem_tracker->limit();
        } else {
            oss << " soft limit: " << _load_process_soft_limit;
        }
        LOG(INFO) << oss.str();
    }

    // No matter soft limit or hard limit reached, only 1 thread will wait here,
    // if hard limit reached, other threads will pending at the beginning of this
    // method.
    Status st = _reduce_memory_channel->handle_mem_exceed_limit(response);

    {
        std::lock_guard<std::mutex> l(_lock);
        if (_hard_limit_reached) {
            _hard_limit_reached = false;
            _hard_limit_cond.notify_all();
        }
        if (_reduce_memory_channel == channel) {
            _reduce_memory_channel = nullptr;
        }
    }
    return st;
}

} // namespace doris
