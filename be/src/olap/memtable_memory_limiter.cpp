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

#include "olap/memtable_memory_limiter.h"

#include "common/config.h"
#include "olap/memtable_writer.h"
#include "util/doris_metrics.h"
#include "util/mem_info.h"
#include "util/metrics.h"

namespace doris {
DEFINE_GAUGE_METRIC_PROTOTYPE_5ARG(memtable_memory_limiter_mem_consumption, MetricUnit::BYTES, "",
                                   memtable_memory_limiter_mem_consumption,
                                   Labels({{"type", "load"}}));

// Calculate the total memory limit of all load tasks on this BE
static int64_t calc_process_max_load_memory(int64_t process_mem_limit) {
    if (process_mem_limit == -1) {
        // no limit
        return -1;
    }
    int32_t max_load_memory_percent = config::load_process_max_memory_limit_percent;
    return process_mem_limit * max_load_memory_percent / 100;
}

MemTableMemoryLimiter::MemTableMemoryLimiter() {}

MemTableMemoryLimiter::~MemTableMemoryLimiter() {
    DEREGISTER_HOOK_METRIC(memtable_memory_limiter_mem_consumption);
}

Status MemTableMemoryLimiter::init(int64_t process_mem_limit) {
    _load_hard_mem_limit = calc_process_max_load_memory(process_mem_limit);
    _load_soft_mem_limit = _load_hard_mem_limit * config::load_process_soft_mem_limit_percent / 100;
    _mem_tracker = std::make_unique<MemTrackerLimiter>(MemTrackerLimiter::Type::LOAD,
                                                       "MemTableMemoryLimiter");
    REGISTER_HOOK_METRIC(memtable_memory_limiter_mem_consumption,
                         [this]() { return _mem_tracker->consumption(); });
    return Status::OK();
}

void MemTableMemoryLimiter::register_writer(std::weak_ptr<MemTableWriter> writer) {
    std::lock_guard<std::mutex> l(_lock);
    _writers.push_back(writer);
}

void MemTableMemoryLimiter::handle_memtable_flush() {
    // Check the soft limit.
    DCHECK(_load_soft_mem_limit > 0);
    // Record current memory status.
    int64_t process_soft_mem_limit = MemInfo::soft_mem_limit();
    int64_t proc_mem_no_allocator_cache = MemInfo::proc_mem_no_allocator_cache();
#ifndef BE_TEST
    // If process memory is almost full but data load don't consume more than 5% (50% * 10%) of
    // total memory, we don't need to flush memtable.
    bool reduce_on_process_soft_mem_limit =
            proc_mem_no_allocator_cache >= process_soft_mem_limit &&
            _mem_tracker->consumption() >= _load_hard_mem_limit / 10;
    if (_mem_tracker->consumption() < _load_soft_mem_limit && !reduce_on_process_soft_mem_limit) {
        return;
    }
#endif
    // Indicate whether current thread is reducing mem on hard limit.
    bool reducing_mem_on_hard_limit = false;
    Status st;
    std::vector<WriterMemItem> writers_to_reduce_mem;
    {
        MonotonicStopWatch timer;
        timer.start();
        std::unique_lock<std::mutex> l(_lock);
        while (_should_wait_flush) {
            _wait_flush_cond.wait(l);
        }
        LOG(INFO) << "Reached the one tenth of load hard limit " << _load_hard_mem_limit / 10
                  << "and process remaining allocator cache " << proc_mem_no_allocator_cache
                  << "reached process soft memory limit " << process_soft_mem_limit
                  << ", waited for flush, time_ns:" << timer.elapsed_time();
#ifndef BE_TEST
        bool hard_limit_reached = _mem_tracker->consumption() >= _load_hard_mem_limit ||
                                  proc_mem_no_allocator_cache >= process_soft_mem_limit;
        // Some other thread is flushing data, and not reached hard limit now,
        // we don't need to handle mem limit in current thread.
        if (_soft_reduce_mem_in_progress && !hard_limit_reached) {
            return;
        }
#endif

        auto cmp = [](WriterMemItem& lhs, WriterMemItem& rhs) {
            return lhs.mem_size < rhs.mem_size;
        };
        std::priority_queue<WriterMemItem, std::vector<WriterMemItem>, decltype(cmp)> mem_heap(cmp);

        for (auto it = _writers.begin(); it != _writers.end();) {
            if (auto writer = it->lock()) {
                int64_t active_memtable_mem = writer->active_memtable_mem_consumption();
                mem_heap.emplace(writer, active_memtable_mem);
                ++it;
            } else {
                *it = std::move(_writers.back());
                _writers.pop_back();
            }
        }
        int64_t mem_to_flushed = _mem_tracker->consumption() / 10;
        int64_t mem_consumption_in_picked_writer = 0;
        while (!mem_heap.empty()) {
            WriterMemItem mem_item = mem_heap.top();
            mem_heap.pop();
            auto writer = mem_item.writer.lock();
            if (!writer) {
                continue;
            }
            int64_t mem_size = mem_item.mem_size;
            writers_to_reduce_mem.emplace_back(writer, mem_size);
            st = writer->flush_async();
            if (!st.ok()) {
                auto err_msg = fmt::format(
                        "tablet writer failed to reduce mem consumption by flushing memtable, "
                        "tablet_id={}, err={}",
                        writer->tablet_id(), st.to_string());
                LOG(WARNING) << err_msg;
                static_cast<void>(writer->cancel_with_status(st));
            }
            mem_consumption_in_picked_writer += mem_size;
            if (mem_consumption_in_picked_writer > mem_to_flushed) {
                break;
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
            << " memtable writers (total mem: "
            << PrettyPrinter::print_bytes(mem_consumption_in_picked_writer)
            << ", max mem: " << PrettyPrinter::print_bytes(writers_to_reduce_mem.front().mem_size)
            << ", min mem:" << PrettyPrinter::print_bytes(writers_to_reduce_mem.back().mem_size)
            << "), ";
        if (proc_mem_no_allocator_cache < process_soft_mem_limit) {
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
                << ", has exceeded process soft limit "
                << PrettyPrinter::print_bytes(process_soft_mem_limit)
                << ", total load mem consumption: "
                << PrettyPrinter::print_bytes(_mem_tracker->consumption())
                << ", vm_rss: " << PerfCounters::get_vm_rss_str();
        }
        LOG(INFO) << oss.str();
    }

    // wait all writers flush without lock
    for (auto item : writers_to_reduce_mem) {
        VLOG_NOTICE << "reducing memory, wait flush mem_size: "
                    << PrettyPrinter::print_bytes(item.mem_size);
        auto writer = item.writer.lock();
        if (!writer) {
            continue;
        }
        st = writer->wait_flush();
        if (!st.ok()) {
            auto err_msg = fmt::format(
                    "tablet writer failed to reduce mem consumption by flushing memtable, "
                    "tablet_id={}, err={}",
                    writer->tablet_id(), st.to_string());
            LOG(WARNING) << err_msg;
            static_cast<void>(writer->cancel_with_status(st));
        }
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

void MemTableMemoryLimiter::_refresh_mem_tracker_without_lock() {
    _mem_usage = 0;
    for (auto it = _writers.begin(); it != _writers.end();) {
        if (auto writer = it->lock()) {
            _mem_usage += writer->mem_consumption(MemType::ALL);
            ++it;
        } else {
            *it = std::move(_writers.back());
            _writers.pop_back();
        }
    }
    VLOG_DEBUG << "refreshed mem_tracker, num writers: " << _writers.size();
    THREAD_MEM_TRACKER_TRANSFER_TO(_mem_usage - _mem_tracker->consumption(), _mem_tracker.get());
}

} // namespace doris