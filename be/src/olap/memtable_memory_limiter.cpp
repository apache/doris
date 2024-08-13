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

#include <bvar/bvar.h>

#include "common/config.h"
#include "olap/memtable_writer.h"
#include "util/doris_metrics.h"
#include "util/mem_info.h"
#include "util/metrics.h"

namespace doris {
DEFINE_GAUGE_METRIC_PROTOTYPE_5ARG(memtable_memory_limiter_mem_consumption, MetricUnit::BYTES, "",
                                   memtable_memory_limiter_mem_consumption,
                                   Labels({{"type", "load"}}));

bvar::LatencyRecorder g_memtable_memory_limit_latency_ms("mm_limiter_limit_time_ms");
bvar::Adder<int> g_memtable_memory_limit_waiting_threads("mm_limiter_waiting_threads");
bvar::Status<int64_t> g_memtable_active_memory("mm_limiter_mem_active", 0);
bvar::Status<int64_t> g_memtable_write_memory("mm_limiter_mem_write", 0);
bvar::Status<int64_t> g_memtable_flush_memory("mm_limiter_mem_flush", 0);
bvar::Status<int64_t> g_memtable_load_memory("mm_limiter_mem_load", 0);
bvar::Status<int64_t> g_load_hard_mem_limit("mm_limiter_limit_hard", 0);
bvar::Status<int64_t> g_load_soft_mem_limit("mm_limiter_limit_soft", 0);

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
    _load_safe_mem_permit =
            _load_hard_mem_limit * config::load_process_safe_mem_permit_percent / 100;
    g_load_hard_mem_limit.set_value(_load_hard_mem_limit);
    g_load_soft_mem_limit.set_value(_load_soft_mem_limit);
    _memtable_tracker_set =
            MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::LOAD, "MemTableTrackerSet");
    _mem_tracker = std::make_unique<MemTracker>("AllMemTableMemory",
                                                ExecEnv::GetInstance()->details_mem_tracker_set());
    REGISTER_HOOK_METRIC(memtable_memory_limiter_mem_consumption,
                         [this]() { return _mem_tracker->consumption(); });
    _log_timer.start();
    return Status::OK();
}

void MemTableMemoryLimiter::register_writer(std::weak_ptr<MemTableWriter> writer) {
    std::lock_guard<std::mutex> l(_lock);
    _writers.push_back(writer);
}

bool MemTableMemoryLimiter::_sys_avail_mem_less_than_warning_water_mark() {
    // reserve a small amount of memory so we do not trigger MinorGC
    return doris::GlobalMemoryArbitrator::sys_mem_available() <
           doris::MemInfo::sys_mem_available_warning_water_mark() +
                   config::memtable_limiter_reserved_memory_bytes;
}

bool MemTableMemoryLimiter::_process_used_mem_more_than_soft_mem_limit() {
    // reserve a small amount of memory so we do not trigger MinorGC
    return GlobalMemoryArbitrator::process_memory_usage() >
           MemInfo::soft_mem_limit() - config::memtable_limiter_reserved_memory_bytes;
}

bool MemTableMemoryLimiter::_soft_limit_reached() {
    return _mem_tracker->consumption() >= _load_soft_mem_limit || _hard_limit_reached();
}

bool MemTableMemoryLimiter::_hard_limit_reached() {
    return _mem_tracker->consumption() >= _load_hard_mem_limit ||
           _sys_avail_mem_less_than_warning_water_mark() ||
           _process_used_mem_more_than_soft_mem_limit();
}

bool MemTableMemoryLimiter::_load_usage_low() {
    return _mem_tracker->consumption() <= _load_safe_mem_permit;
}

void MemTableMemoryLimiter::handle_memtable_flush() {
    // Check the soft limit.
    DCHECK(_load_soft_mem_limit > 0);
    if (!_soft_limit_reached() || _load_usage_low()) {
        return;
    }
    MonotonicStopWatch timer;
    timer.start();
    std::unique_lock<std::mutex> l(_lock);
    g_memtable_memory_limit_waiting_threads << 1;
    while (_hard_limit_reached()) {
        LOG(INFO) << "reached memtable memory hard limit"
                  << " (active: " << PrettyPrinter::print_bytes(_active_mem_usage)
                  << ", write: " << PrettyPrinter::print_bytes(_write_mem_usage)
                  << ", flush: " << PrettyPrinter::print_bytes(_flush_mem_usage) << ")";
        if (_active_mem_usage >=
            _write_mem_usage * config::memtable_hard_limit_active_percent / 100) {
            _flush_active_memtables(_write_mem_usage / 20);
        }
        if (!_hard_limit_reached()) {
            break;
        }
        auto st = _hard_limit_end_cond.wait_for(l, std::chrono::milliseconds(1000));
        if (st == std::cv_status::timeout) {
            LOG(INFO) << "timeout when waiting for memory hard limit end, try again";
        }
    }
    g_memtable_memory_limit_waiting_threads << -1;
    if (_soft_limit_reached()) {
        LOG(INFO) << "reached memtable memory soft limit"
                  << " (active: " << PrettyPrinter::print_bytes(_active_mem_usage)
                  << ", write: " << PrettyPrinter::print_bytes(_write_mem_usage)
                  << ", flush: " << PrettyPrinter::print_bytes(_flush_mem_usage) << ")";
        if (_active_mem_usage >=
            _write_mem_usage * config::memtable_soft_limit_active_percent / 100) {
            _flush_active_memtables(_write_mem_usage / 20);
        }
    }
    timer.stop();
    int64_t time_ms = timer.elapsed_time() / 1000 / 1000;
    g_memtable_memory_limit_latency_ms << time_ms;
    LOG(INFO) << "waited " << time_ms << " ms for memtable memory limit";
}

void MemTableMemoryLimiter::_flush_active_memtables(int64_t need_flush) {
    if (need_flush <= 0) {
        return;
    }

    _refresh_mem_tracker();
    if (_active_writers.size() == 0) {
        return;
    }
    int64_t mem_flushed = 0;
    int64_t num_flushed = 0;
    int64_t avg_mem = _active_mem_usage / _active_writers.size();
    for (auto writer : _active_writers) {
        int64_t mem = _flush_memtable(writer, avg_mem);
        mem_flushed += mem;
        num_flushed += (mem > 0);
        if (mem_flushed >= need_flush) {
            break;
        }
    }
    LOG(INFO) << "flushed " << num_flushed << " out of " << _active_writers.size()
              << " active writers, flushed size: " << PrettyPrinter::print_bytes(mem_flushed);
}

int64_t MemTableMemoryLimiter::_flush_memtable(std::weak_ptr<MemTableWriter> writer_to_flush,
                                               int64_t threshold) {
    auto writer = writer_to_flush.lock();
    if (!writer) {
        return 0;
    }
    auto mem_usage = writer->active_memtable_mem_consumption();
    // if the memtable writer just got flushed, don't flush it again
    if (mem_usage < threshold) {
        VLOG_DEBUG << "flushing active memtables, active mem usage "
                   << PrettyPrinter::print_bytes(mem_usage) << " is less than "
                   << PrettyPrinter::print_bytes(threshold) << ", skipping";
        return 0;
    }
    VLOG_DEBUG << "flushing active memtables, active mem usage "
               << PrettyPrinter::print_bytes(mem_usage);
    Status st = writer->flush_async();
    if (!st.ok()) {
        auto err_msg = fmt::format(
                "tablet writer failed to reduce mem consumption by flushing memtable, "
                "tablet_id={}, err={}",
                writer->tablet_id(), st.to_string());
        LOG(WARNING) << err_msg;
        static_cast<void>(writer->cancel_with_status(st));
        return 0;
    }
    return mem_usage;
}

void MemTableMemoryLimiter::refresh_mem_tracker() {
    std::lock_guard<std::mutex> l(_lock);
    _refresh_mem_tracker();
    std::stringstream ss;
    Limit limit = Limit::NONE;
    if (_soft_limit_reached()) {
        limit = _hard_limit_reached() ? Limit::HARD : Limit::SOFT;
        ss << "reached " << (limit == Limit::HARD ? "hard" : "soft") << " limit";
    } else if (_last_limit == Limit::NONE) {
        return;
    } else {
        ss << "ended " << (_last_limit == Limit::HARD ? "hard" : "soft") << " limit";
    }

    if (_last_limit == limit && _log_timer.elapsed_time() < LOG_INTERVAL) {
        return;
    }

    _last_limit = limit;
    _log_timer.reset();
    // if not exist load task, this log should not be printed.
    if (_mem_usage != 0) {
        LOG(INFO) << fmt::format(
                "{}, {}, load mem: {}, memtable writers num: {} (active: {}, write: {}, flush: {})",
                ss.str(), GlobalMemoryArbitrator::process_memory_used_details_str(),
                PrettyPrinter::print_bytes(_mem_tracker->consumption()), _writers.size(),
                PrettyPrinter::print_bytes(_active_mem_usage),
                PrettyPrinter::print_bytes(_write_mem_usage),
                PrettyPrinter::print_bytes(_flush_mem_usage));
    }
}

void MemTableMemoryLimiter::_refresh_mem_tracker() {
    _flush_mem_usage = 0;
    _write_mem_usage = 0;
    _active_mem_usage = 0;
    _active_writers.clear();
    for (auto it = _writers.begin(); it != _writers.end();) {
        if (auto writer = it->lock()) {
            auto active_usage = writer->active_memtable_mem_consumption();
            _active_mem_usage += active_usage;
            if (active_usage > 0) {
                _active_writers.push_back(writer);
            }
            _flush_mem_usage += writer->mem_consumption(MemType::FLUSH);
            _write_mem_usage += writer->mem_consumption(MemType::WRITE);
            ++it;
        } else {
            *it = std::move(_writers.back());
            _writers.pop_back();
        }
    }
    _mem_usage = _flush_mem_usage + _write_mem_usage;
    g_memtable_active_memory.set_value(_active_mem_usage);
    g_memtable_write_memory.set_value(_write_mem_usage);
    g_memtable_flush_memory.set_value(_flush_mem_usage);
    g_memtable_load_memory.set_value(_mem_usage);
    VLOG_DEBUG << "refreshed mem_tracker, num writers: " << _writers.size();
    _mem_tracker->set_consumption(_mem_usage);
    if (!_hard_limit_reached()) {
        _hard_limit_end_cond.notify_all();
    }
}

} // namespace doris
