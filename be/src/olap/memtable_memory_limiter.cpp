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

bool MemTableMemoryLimiter::_soft_limit_reached() {
    return _mem_tracker->consumption() >= _load_soft_mem_limit ||
           (MemInfo::proc_mem_no_allocator_cache() >= MemInfo::soft_mem_limit() &&
            _mem_tracker->consumption() >= _load_hard_mem_limit / 10);
}

bool MemTableMemoryLimiter::_hard_limit_reached() {
    return _mem_tracker->consumption() >= _load_hard_mem_limit ||
           MemInfo::proc_mem_no_allocator_cache() >= MemInfo::soft_mem_limit();
}

void MemTableMemoryLimiter::handle_memtable_flush() {
    // Check the soft limit.
    DCHECK(_load_soft_mem_limit > 0);
    if (!_soft_limit_reached()) {
        return;
    }
    {
        MonotonicStopWatch timer;
        timer.start();
        std::unique_lock<std::mutex> l(_lock);
        while (_soft_limit_reached()) {
            LOG(INFO) << "reached memtable memory soft limit"
                      << " (active: " << PrettyPrinter::print_bytes(_active_mem_usage)
                      << ", write: " << PrettyPrinter::print_bytes(_write_mem_usage)
                      << ", flush: " << PrettyPrinter::print_bytes(_flush_mem_usage) << ")";
            if (_active_mem_usage >= _load_soft_mem_limit ||
                (_active_mem_usage > _load_hard_mem_limit / 10 && _hard_limit_reached())) {
                _flush_active_memtables();
            }
            _soft_limit_end_cond.wait(l);
        }
        timer.stop();
        int64_t time_ms = timer.elapsed_time() / 1000 / 1000;
        LOG(INFO) << "waited " << time_ms << " ms for memtable memory limit";
    }
}

void MemTableMemoryLimiter::_flush_active_memtables() {
    auto cmp = [](WriterMemItem& lhs, WriterMemItem& rhs) { return lhs.mem_size < rhs.mem_size; };
    std::priority_queue<WriterMemItem, std::vector<WriterMemItem>, decltype(cmp)> mem_heap(cmp);

    _active_mem_usage = 0;
    for (auto it = _writers.begin(); it != _writers.end();) {
        if (auto writer = it->lock()) {
            int64_t active_memtable_mem = writer->active_memtable_mem_consumption();
            _active_mem_usage += active_memtable_mem;
            mem_heap.emplace(writer, active_memtable_mem);
            ++it;
        } else {
            *it = std::move(_writers.back());
            _writers.pop_back();
        }
    }
    int64_t mem_to_flush = _hard_limit_reached()
                                   ? _active_mem_usage - _load_hard_mem_limit / 11
                                   : _active_mem_usage - _load_soft_mem_limit / 5 * 4;
    int64_t mem_flushed = 0;
    int64_t num_flushed = 0;
    while (!mem_heap.empty()) {
        WriterMemItem mem_item = mem_heap.top();
        mem_heap.pop();
        num_flushed++;
        mem_flushed += mem_item.mem_size;
        auto writer = mem_item.writer.lock();
        if (!writer) {
            continue;
        }
        int64_t active_mem = writer->active_memtable_mem_consumption();
        // if the memtable writer just got flushed, don't flush it again
        if (active_mem < mem_item.mem_size / 2) {
            mem_flushed -= active_mem;
            continue;
        }
        Status st = writer->flush_async();
        if (!st.ok()) {
            auto err_msg = fmt::format(
                    "tablet writer failed to reduce mem consumption by flushing memtable, "
                    "tablet_id={}, err={}",
                    writer->tablet_id(), st.to_string());
            LOG(WARNING) << err_msg;
            static_cast<void>(writer->cancel_with_status(st));
        }
        if (mem_flushed >= mem_to_flush) {
            break;
        }
    }
    LOG(INFO) << "flushed " << num_flushed
              << " memtable writers (total mem: " << PrettyPrinter::print_bytes(mem_flushed) << ")";
}

void MemTableMemoryLimiter::refresh_mem_tracker() {
    std::lock_guard<std::mutex> l(_lock);
    _flush_mem_usage = 0;
    _write_mem_usage = 0;
    _active_mem_usage = 0;
    for (auto it = _writers.begin(); it != _writers.end();) {
        if (auto writer = it->lock()) {
            _active_mem_usage += writer->active_memtable_mem_consumption();
            _flush_mem_usage += writer->mem_consumption(MemType::FLUSH);
            _write_mem_usage += writer->mem_consumption(MemType::WRITE);
            ++it;
        } else {
            *it = std::move(_writers.back());
            _writers.pop_back();
        }
    }
    _mem_usage = _flush_mem_usage + _write_mem_usage;
    VLOG_DEBUG << "refreshed mem_tracker, num writers: " << _writers.size();
    THREAD_MEM_TRACKER_TRANSFER_TO(_mem_usage - _mem_tracker->consumption(), _mem_tracker.get());
    if (_soft_limit_reached()) {
        LOG(INFO) << "reached " << (_hard_limit_reached() ? "hard" : "soft") << " limit"
                  << ", process mem: " << PerfCounters::get_vm_rss_str()
                  << " (without allocator cache: "
                  << PrettyPrinter::print_bytes(MemInfo::proc_mem_no_allocator_cache())
                  << "), load mem: " << PrettyPrinter::print_bytes(_mem_tracker->consumption())
                  << ", memtable writers num: " << _writers.size()
                  << " (active: " << PrettyPrinter::print_bytes(_active_mem_usage)
                  << ", write: " << PrettyPrinter::print_bytes(_write_mem_usage)
                  << ", flush: " << PrettyPrinter::print_bytes(_flush_mem_usage) << ")";
    } else {
        _soft_limit_end_cond.notify_all();
    }
}

} // namespace doris