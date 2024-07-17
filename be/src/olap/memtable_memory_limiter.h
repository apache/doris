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

#include <stdint.h>

#include "common/status.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "util/countdown_latch.h"
#include "util/stopwatch.hpp"

namespace doris {
class MemTableWriter;
struct WriterMemItem {
    std::weak_ptr<MemTableWriter> writer;
    int64_t mem_size;
};
class MemTableMemoryLimiter {
public:
    MemTableMemoryLimiter();
    ~MemTableMemoryLimiter();

    Status init(int64_t process_mem_limit);

    // check if the total mem consumption exceeds limit.
    // If yes, it will flush memtable to try to reduce memory consumption.
    void handle_memtable_flush();

    void register_writer(std::weak_ptr<MemTableWriter> writer);

    void refresh_mem_tracker();

    MemTrackerLimiter* memtable_tracker_set() { return _memtable_tracker_set.get(); }
    MemTracker* mem_tracker() { return _mem_tracker.get(); }

    int64_t mem_usage() const { return _mem_usage; }

private:
    static inline bool _sys_avail_mem_less_than_warning_water_mark();
    static inline bool _process_used_mem_more_than_soft_mem_limit();

    bool _soft_limit_reached();
    bool _hard_limit_reached();
    bool _load_usage_low();
    void _flush_active_memtables(int64_t need_flush);
    int64_t _flush_memtable(std::weak_ptr<MemTableWriter> writer_to_flush, int64_t threshold);
    void _refresh_mem_tracker();

    std::mutex _lock;
    std::condition_variable _hard_limit_end_cond;
    int64_t _mem_usage = 0;
    int64_t _flush_mem_usage = 0;
    int64_t _write_mem_usage = 0;
    int64_t _active_mem_usage = 0;

    // mem tracker collection of all mem tables.
    std::shared_ptr<MemTrackerLimiter> _memtable_tracker_set;
    // sum of all mem table memory.
    std::unique_ptr<MemTracker> _mem_tracker;
    int64_t _load_hard_mem_limit = -1;
    int64_t _load_soft_mem_limit = -1;
    int64_t _load_safe_mem_permit = -1;

    enum Limit { NONE, SOFT, HARD } _last_limit = Limit::NONE;
    MonotonicStopWatch _log_timer;
    static const int64_t LOG_INTERVAL = 1 * 1000 * 1000 * 1000; // 1s

    std::vector<std::weak_ptr<MemTableWriter>> _writers;
    std::vector<std::weak_ptr<MemTableWriter>> _active_writers;
};
} // namespace doris
