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

    void refresh_mem_tracker() {
        std::lock_guard<std::mutex> l(_lock);
        _refresh_mem_tracker_without_lock();
    }

    MemTrackerLimiter* mem_tracker() { return _mem_tracker.get(); }

    int64_t mem_usage() const { return _mem_usage; }

private:
    void _refresh_mem_tracker_without_lock();

    std::mutex _lock;
    // If hard limit reached, one thread will trigger load channel flush,
    // other threads should wait on the condition variable.
    bool _should_wait_flush = false;
    std::condition_variable _wait_flush_cond;
    int64_t _mem_usage = 0;

    std::unique_ptr<MemTrackerLimiter> _mem_tracker;
    int64_t _load_hard_mem_limit = -1;
    int64_t _load_soft_mem_limit = -1;
    bool _soft_reduce_mem_in_progress = false;

    std::vector<std::weak_ptr<MemTableWriter>> _writers;
};
} // namespace doris