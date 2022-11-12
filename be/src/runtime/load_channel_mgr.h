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
#include "gutil/walltime.h"
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

    void refresh_mem_tracker() {
        int64_t mem_usage = 0;
        std::lock_guard<std::mutex> l(_lock);
        for (auto& kv : _load_channels) {
            mem_usage += kv.second->mem_consumption();
        }
        _mem_tracker->set_consumption(mem_usage);
    }
    MemTrackerLimiter* mem_tracker_set() { return _mem_tracker_set.get(); }

private:
    template <typename Request>
    Status _get_load_channel(std::shared_ptr<LoadChannel>& channel, bool& is_eof,
                             const UniqueId& load_id, const Request& request);

    void _finish_load_channel(UniqueId load_id);
    // check if the total load mem consumption exceeds limit.
    // If yes, it will pick a load channel to try to reduce memory consumption.
    template <typename TabletWriterAddResult>
    Status _handle_mem_exceed_limit(TabletWriterAddResult* response);

    Status _start_bg_worker();

protected:
    // lock protect the load channel map
    std::mutex _lock;
    // load id -> load channel
    std::unordered_map<UniqueId, std::shared_ptr<LoadChannel>> _load_channels;
    Cache* _last_success_channel = nullptr;

    // check the total load channel mem consumption of this Backend
    std::unique_ptr<MemTracker> _mem_tracker;
    // Associate load channel tracker and memtable tracker, avoid default association to Orphan tracker.
    std::unique_ptr<MemTrackerLimiter> _mem_tracker_set;
    int64_t _load_hard_mem_limit = -1;
    int64_t _load_soft_mem_limit = -1;
    // By default, we try to reduce memory on the load channel with largest mem consumption,
    // but if there are lots of small load channel, even the largest one consumes very
    // small memory, in this case we need to pick multiple load channels to reduce memory
    // more effectively.
    // `_load_channel_min_mem_to_reduce` is used to determine whether the largest load channel's
    // memory consumption is big enough.
    int64_t _load_channel_min_mem_to_reduce = -1;
    bool _soft_reduce_mem_in_progress = false;

    // If hard limit reached, one thread will trigger load channel flush,
    // other threads should wait on the condition variable.
    bool _should_wait_flush = false;
    std::condition_variable _wait_flush_cond;

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
    // Check the soft limit.
    DCHECK(_load_soft_mem_limit > 0);
    int64_t process_mem_limit = MemInfo::mem_limit() * config::soft_mem_limit_frac;
    if (_mem_tracker->consumption() < _load_soft_mem_limit &&
        MemInfo::proc_mem_no_allocator_cache() < process_mem_limit) {
        return Status::OK();
    }
    // Indicate whether current thread is reducing mem on hard limit.
    bool reducing_mem_on_hard_limit = false;
    std::vector<std::shared_ptr<LoadChannel>> channels_to_reduce_mem;
    {
        std::unique_lock<std::mutex> l(_lock);
        while (_should_wait_flush) {
            LOG(INFO) << "Reached the load hard limit " << _load_hard_mem_limit
                      << ", waiting for flush";
            _wait_flush_cond.wait(l);
        }
        bool hard_limit_reached = _mem_tracker->consumption() >= _load_hard_mem_limit ||
                                  MemInfo::proc_mem_no_allocator_cache() >= process_mem_limit;
        // Some other thread is flushing data, and not reached hard limit now,
        // we don't need to handle mem limit in current thread.
        if (_soft_reduce_mem_in_progress && !hard_limit_reached) {
            return Status::OK();
        }

        // Pick LoadChannels to reduce memory usage, if some other thread is reducing memory
        // due to soft limit, and we reached hard limit now, current thread may pick some
        // duplicate channels and trigger duplicate reducing memory process.
        // But the load channel's reduce memory process is thread safe, only 1 thread can
        // reduce memory at the same time, other threads will wait on a condition variable,
        // after the reduce-memory work finished, all threads will return.
        using ChannelMemPair = std::pair<std::shared_ptr<LoadChannel>, int64_t>;
        std::vector<ChannelMemPair> candidate_channels;
        int64_t total_consume = 0;
        for (auto& kv : _load_channels) {
            if (kv.second->is_high_priority()) {
                // do not select high priority channel to reduce memory
                // to avoid blocking them.
                continue;
            }
            int64_t mem = kv.second->mem_consumption();
            // save the mem consumption, since the calculation might be expensive.
            candidate_channels.push_back(std::make_pair(kv.second, mem));
            total_consume += mem;
        }

        if (candidate_channels.empty()) {
            // should not happen, add log to observe
            LOG(WARNING) << "All load channels are high priority, failed to find suitable"
                         << "channels to reduce memory when total load mem limit exceed";
            return Status::OK();
        }

        // sort all load channels, try to find the largest one.
        std::sort(candidate_channels.begin(), candidate_channels.end(),
                  [](const ChannelMemPair& lhs, const ChannelMemPair& rhs) {
                      return lhs.second > rhs.second;
                  });

        int64_t mem_consumption_in_picked_channel = 0;
        auto largest_channel = *candidate_channels.begin();
        // If some load-channel is big enough, we can reduce it only, try our best to avoid
        // reducing small load channels.
        if (_load_channel_min_mem_to_reduce > 0 &&
            largest_channel.second > _load_channel_min_mem_to_reduce) {
            // Pick 1 load channel to reduce memory.
            channels_to_reduce_mem.push_back(largest_channel.first);
            mem_consumption_in_picked_channel = largest_channel.second;
        } else {
            // Pick multiple channels to reduce memory.
            int64_t mem_to_flushed = total_consume / 3;
            for (auto ch : candidate_channels) {
                channels_to_reduce_mem.push_back(ch.first);
                mem_consumption_in_picked_channel += ch.second;
                if (mem_consumption_in_picked_channel >= mem_to_flushed) {
                    break;
                }
            }
        }

        std::ostringstream oss;
        if (MemInfo::proc_mem_no_allocator_cache() < process_mem_limit) {
            oss << "reducing memory of " << channels_to_reduce_mem.size()
                << " load channels (total mem consumption: " << mem_consumption_in_picked_channel
                << " bytes), because total load mem consumption "
                << PrettyPrinter::print(_mem_tracker->consumption(), TUnit::BYTES)
                << " has exceeded";
            if (_mem_tracker->consumption() > _load_hard_mem_limit) {
                _should_wait_flush = true;
                reducing_mem_on_hard_limit = true;
                oss << " hard limit: " << PrettyPrinter::print(_load_hard_mem_limit, TUnit::BYTES);
            } else {
                _soft_reduce_mem_in_progress = true;
                oss << " soft limit: " << PrettyPrinter::print(_load_soft_mem_limit, TUnit::BYTES);
            }
        } else {
            _should_wait_flush = true;
            reducing_mem_on_hard_limit = true;
            oss << "reducing memory of " << channels_to_reduce_mem.size()
                << " load channels (total mem consumption: " << mem_consumption_in_picked_channel
                << " bytes), because " << PerfCounters::get_vm_rss_str() << " has exceeded limit "
                << PrettyPrinter::print(process_mem_limit, TUnit::BYTES)
                << " , tc/jemalloc allocator cache " << MemInfo::allocator_cache_mem_str();
        }
        LOG(INFO) << oss.str();
    }

    Status st = Status::OK();
    for (auto ch : channels_to_reduce_mem) {
        uint64_t begin = GetCurrentTimeMicros();
        int64_t mem_usage = ch->mem_consumption();
        st = ch->handle_mem_exceed_limit(response);
        LOG(INFO) << "reduced memory of " << *ch << ", cost "
                  << (GetCurrentTimeMicros() - begin) / 1000
                  << " ms, released memory: " << mem_usage - ch->mem_consumption() << " bytes";
    }

    {
        std::lock_guard<std::mutex> l(_lock);
        // If a thread have finished the memtable flush for soft limit, and now
        // the hard limit is already reached, it should not update these variables.
        if (reducing_mem_on_hard_limit && _should_wait_flush) {
            _should_wait_flush = false;
            _wait_flush_cond.notify_all();
        }
        if (_soft_reduce_mem_in_progress) {
            _soft_reduce_mem_in_progress = false;
        }
    }
    return st;
}

} // namespace doris
