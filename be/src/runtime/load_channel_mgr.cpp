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

#include <functional>
#include <map>
#include <memory>
#include <queue>
#include <tuple>
#include <vector>

#include "runtime/load_channel.h"
#include "runtime/memory/mem_tracker.h"
#include "util/doris_metrics.h"
#include "util/stopwatch.hpp"
#include "util/time.h"

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
                                          channel_timeout_s, is_high_priority, params.sender_ip()));
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
              << ", peak: " << _mem_tracker->peak_consumption()
              << ", total running load channels: " << _load_channels.size();

    return Status::OK();
}

void LoadChannelMgr::_handle_mem_exceed_limit() {
    // Check the soft limit.
    DCHECK(_load_soft_mem_limit > 0);
    int64_t process_mem_limit = MemInfo::soft_mem_limit();
    int64_t proc_mem_no_allocator_cache = MemInfo::proc_mem_no_allocator_cache();
    if (_mem_tracker->consumption() < _load_soft_mem_limit &&
        proc_mem_no_allocator_cache < process_mem_limit) {
        return;
    }
    // Indicate whether current thread is reducing mem on hard limit.
    bool reducing_mem_on_hard_limit = false;
    // tuple<LoadChannel, index_id, tablet_id, mem_size>
    std::vector<std::tuple<std::shared_ptr<LoadChannel>, int64_t, int64_t, int64_t>>
            writers_to_reduce_mem;
    {
        std::unique_lock<std::mutex> l(_lock);
        while (_should_wait_flush) {
            LOG(INFO) << "Reached the load hard limit " << _load_hard_mem_limit
                      << ", waiting for flush";
            _wait_flush_cond.wait(l);
        }
        bool hard_limit_reached = _mem_tracker->consumption() >= _load_hard_mem_limit ||
                                  proc_mem_no_allocator_cache >= process_mem_limit;
        // Some other thread is flushing data, and not reached hard limit now,
        // we don't need to handle mem limit in current thread.
        if (_soft_reduce_mem_in_progress && !hard_limit_reached) {
            return;
        }

        // tuple<LoadChannel, index_id, multimap<mem size, tablet_id>>
        using WritersMem = std::tuple<std::shared_ptr<LoadChannel>, int64_t,
                                      std::multimap<int64_t, int64_t, std::greater<int64_t>>>;
        std::vector<WritersMem> all_writers_mem;

        // tuple<current iterator in multimap, end iterator in multimap, pos in all_writers_mem>
        using WriterMemItem =
                std::tuple<std::multimap<int64_t, int64_t, std::greater<int64_t>>::iterator,
                           std::multimap<int64_t, int64_t, std::greater<int64_t>>::iterator,
                           size_t>;
        auto cmp = [](WriterMemItem& lhs, WriterMemItem& rhs) {
            return std::get<0>(lhs)->first < std::get<0>(rhs)->first;
        };
        std::priority_queue<WriterMemItem, std::vector<WriterMemItem>, decltype(cmp)>
                tablets_mem_heap(cmp);

        for (auto& kv : _load_channels) {
            if (kv.second->is_high_priority()) {
                // do not select high priority channel to reduce memory
                // to avoid blocking them.
                continue;
            }
            std::vector<std::pair<int64_t, std::multimap<int64_t, int64_t, std::greater<int64_t>>>>
                    writers_mem_snap;
            kv.second->get_writers_mem_consumption_snapshot(&writers_mem_snap);
            for (auto item : writers_mem_snap) {
                // multimap is empty
                if (item.second.empty()) {
                    continue;
                }
                all_writers_mem.emplace_back(kv.second, item.first, std::move(item.second));
                size_t pos = all_writers_mem.size() - 1;
                tablets_mem_heap.emplace(std::get<2>(all_writers_mem[pos]).begin(),
                                         std::get<2>(all_writers_mem[pos]).end(), pos);
            }
        }

        // reduce 1/10 memory every time
        int64_t mem_to_flushed = _mem_tracker->consumption() / 10;
        int64_t mem_consumption_in_picked_writer = 0;
        while (!tablets_mem_heap.empty()) {
            WriterMemItem tablet_mem_item = tablets_mem_heap.top();
            size_t pos = std::get<2>(tablet_mem_item);
            auto load_channel = std::get<0>(all_writers_mem[pos]);
            int64_t index_id = std::get<1>(all_writers_mem[pos]);
            int64_t tablet_id = std::get<0>(tablet_mem_item)->second;
            int64_t mem_size = std::get<0>(tablet_mem_item)->first;
            writers_to_reduce_mem.emplace_back(load_channel, index_id, tablet_id, mem_size);
            load_channel->flush_memtable_async(index_id, tablet_id);
            mem_consumption_in_picked_writer += std::get<0>(tablet_mem_item)->first;
            if (mem_consumption_in_picked_writer > mem_to_flushed) {
                break;
            }
            tablets_mem_heap.pop();
            if (std::get<0>(tablet_mem_item)++ != std::get<1>(tablet_mem_item)) {
                tablets_mem_heap.push(tablet_mem_item);
            }
        }

        if (writers_to_reduce_mem.empty()) {
            // should not happen, add log to observe
            LOG(WARNING) << "failed to find suitable writers to reduce memory"
                         << " when total load mem limit exceed";
            return;
        }

        std::ostringstream oss;
        oss << "reducing memory of " << writers_to_reduce_mem.size()
            << " delta writers (total mem: "
            << PrettyPrinter::print_bytes(mem_consumption_in_picked_writer) << ", max mem: "
            << PrettyPrinter::print_bytes(std::get<3>(writers_to_reduce_mem.front()))
            << ", min mem:" << PrettyPrinter::print_bytes(std::get<3>(writers_to_reduce_mem.back()))
            << "), ";
        if (proc_mem_no_allocator_cache < process_mem_limit) {
            oss << "because total load mem consumption "
                << PrettyPrinter::print_bytes(_mem_tracker->consumption()) << " has exceeded";
            if (_mem_tracker->consumption() > _load_hard_mem_limit) {
                _should_wait_flush = true;
                reducing_mem_on_hard_limit = true;
                oss << " hard limit: " << PrettyPrinter::print_bytes(_load_hard_mem_limit);
            } else {
                _soft_reduce_mem_in_progress = true;
                oss << " soft limit: " << PrettyPrinter::print_bytes(_load_soft_mem_limit);
            }
        } else {
            _should_wait_flush = true;
            reducing_mem_on_hard_limit = true;
            oss << "because proc_mem_no_allocator_cache consumption "
                << PrettyPrinter::print_bytes(proc_mem_no_allocator_cache)
                << ", has exceeded process limit " << PrettyPrinter::print_bytes(process_mem_limit)
                << ", total load mem consumption: "
                << PrettyPrinter::print_bytes(_mem_tracker->consumption())
                << ", vm_rss: " << PerfCounters::get_vm_rss_str()
                << ", tc/jemalloc allocator cache: " << MemInfo::allocator_cache_mem_str();
        }
        LOG(INFO) << oss.str();
    }

    // wait all writers flush without lock
    for (auto item : writers_to_reduce_mem) {
        VLOG_NOTICE << "reducing memory, wait flush load_id: " << std::get<0>(item)->load_id()
                    << ", index_id: " << std::get<1>(item) << ", tablet_id: " << std::get<2>(item)
                    << ", mem_size: " << PrettyPrinter::print_bytes(std::get<3>(item));
        std::get<0>(item)->wait_flush(std::get<1>(item), std::get<2>(item));
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
        // refresh mem tacker to avoid duplicate reduce
        _refresh_mem_tracker_without_lock();
    }
}

} // namespace doris
