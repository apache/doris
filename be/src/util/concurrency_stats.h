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

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace doris {

// A thread-safe counter for tracking concurrent operations
// Uses atomic variable for high-performance concurrent access
class ConcurrencyCounter {
public:
    explicit ConcurrencyCounter(std::string name) : _name(std::move(name)), _count(0) {}

    // Increment the counter
    void increment() { _count.fetch_add(1, std::memory_order_relaxed); }

    // Decrement the counter
    void decrement() { _count.fetch_sub(1, std::memory_order_relaxed); }

    // Get current value
    int64_t value() const { return _count.load(std::memory_order_relaxed); }

    const std::string& name() const { return _name; }

    // RAII helper for automatic increment/decrement
    class Guard {
    public:
        explicit Guard(ConcurrencyCounter* counter) : _counter(counter) {
            if (_counter) {
                _counter->increment();
            }
        }

        ~Guard() {
            if (_counter) {
                _counter->decrement();
            }
        }

        Guard(const Guard&) = delete;
        Guard& operator=(const Guard&) = delete;

    private:
        ConcurrencyCounter* _counter;
    };

private:
    std::string _name;
    std::atomic<int64_t> _count;
};

// Singleton manager for all concurrency counters
// All counters are defined here in order
class ConcurrencyStatsManager {
public:
    static ConcurrencyStatsManager& instance();

    // Start the background thread for periodic logging
    void start();

    // Stop the background thread
    void stop();

    // Manually dump all counters to log
    void dump_to_log();

    // Access to individual counters (defined in order of read path from top to bottom)
    ConcurrencyCounter* vscanner_get_block;
    ConcurrencyCounter* segment_iterator_next_batch;
    ConcurrencyCounter* column_reader_read_page;
    ConcurrencyCounter* page_io_decompress;
    ConcurrencyCounter* page_io_pre_decode;
    ConcurrencyCounter* page_io_insert_page_cache;
    ConcurrencyCounter* cached_remote_reader_read_at;
    ConcurrencyCounter* cached_remote_reader_get_or_set;
    ConcurrencyCounter* cached_remote_reader_get_or_set_wait_lock;
    ConcurrencyCounter* cached_remote_reader_write_back;
    ConcurrencyCounter* cached_remote_reader_blocking;
    ConcurrencyCounter* cached_remote_reader_local_read;
    ConcurrencyCounter* s3_file_reader_read;

private:
    ConcurrencyStatsManager();
    ~ConcurrencyStatsManager();

    ConcurrencyStatsManager(const ConcurrencyStatsManager&) = delete;
    ConcurrencyStatsManager& operator=(const ConcurrencyStatsManager&) = delete;

    void _dump_thread_func();

    // All counters in the order they should be printed
    std::vector<ConcurrencyCounter*> _counters;

    std::atomic<bool> _running;
    std::unique_ptr<std::thread> _dump_thread;
};

// Macro for scoped counting
#define SCOPED_CONCURRENCY_COUNT_IMPL(counter_ptr, unique_id) \
    doris::ConcurrencyCounter::Guard _concurrency_guard_##unique_id(counter_ptr)

#define SCOPED_CONCURRENCY_COUNT_HELPER(counter_ptr, line) \
    SCOPED_CONCURRENCY_COUNT_IMPL(counter_ptr, line)

#define SCOPED_CONCURRENCY_COUNT(counter_ptr) SCOPED_CONCURRENCY_COUNT_HELPER(counter_ptr, __LINE__)

} // namespace doris
