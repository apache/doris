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

#include "util/concurrency_stats.h"

#include <chrono>
#include <sstream>

#include "common/config.h"
#include "common/logging.h"
#include "util/thread.h"

namespace doris {
ConcurrencyStatsManager::ConcurrencyStatsManager() : _running(false) {
    // Initialize all counters in the order of read path (top to bottom)
    vscanner_get_block = new ConcurrencyCounter("vscanner");
    segment_iterator_next_batch = new ConcurrencyCounter("segment_iterator");
    column_reader_read_page = new ConcurrencyCounter("column_reader");
    page_io_decompress = new ConcurrencyCounter("page_io.decompress");
    page_io_pre_decode = new ConcurrencyCounter("page_io.pre_decode");
    page_io_insert_page_cache = new ConcurrencyCounter("page_io.insert_page_cache");
    cached_remote_reader_read_at = new ConcurrencyCounter("file_cache.read_at");
    cached_remote_reader_get_or_set = new ConcurrencyCounter("file_cache.get_or_set");
    cached_remote_reader_get_or_set_wait_lock =
            new ConcurrencyCounter("file_cache.get_or_set_wait_lock");
    cached_remote_reader_write_back = new ConcurrencyCounter("file_cache.write_back");
    cached_remote_reader_blocking = new ConcurrencyCounter("file_cache.blocking");
    cached_remote_reader_local_read = new ConcurrencyCounter("file_cache.local_read");
    s3_file_reader_read = new ConcurrencyCounter("s3.read");

    // Add to vector in the order they should be printed
    _counters.push_back(vscanner_get_block);
    _counters.push_back(segment_iterator_next_batch);
    _counters.push_back(column_reader_read_page);
    _counters.push_back(page_io_decompress);
    _counters.push_back(page_io_pre_decode);
    _counters.push_back(page_io_insert_page_cache);
    _counters.push_back(cached_remote_reader_read_at);
    _counters.push_back(cached_remote_reader_get_or_set);
    _counters.push_back(cached_remote_reader_get_or_set_wait_lock);
    _counters.push_back(cached_remote_reader_write_back);
    _counters.push_back(cached_remote_reader_blocking);
    _counters.push_back(cached_remote_reader_local_read);
    _counters.push_back(s3_file_reader_read);
}

ConcurrencyStatsManager::~ConcurrencyStatsManager() {
    stop();

    // Clean up counters
    for (auto* counter : _counters) {
        delete counter;
    }
    _counters.clear();
}

ConcurrencyStatsManager& ConcurrencyStatsManager::instance() {
    static ConcurrencyStatsManager instance;
    return instance;
}

void ConcurrencyStatsManager::start() {
    if (_running.exchange(true)) {
        return; // Already running
    }

    _dump_thread = std::make_unique<std::thread>([this]() { _dump_thread_func(); });
}

void ConcurrencyStatsManager::stop() {
    if (!_running.exchange(false)) {
        return; // Not running
    }

    if (_dump_thread && _dump_thread->joinable()) {
        _dump_thread->join();
    }
    _dump_thread.reset();
}

void ConcurrencyStatsManager::dump_to_log() {
    if (_counters.empty()) {
        return;
    }

    // Build single line output: CONCURRENCY_STATS name1=value1 name2=value2 ...
    std::stringstream ss;
    ss << "CONCURRENCY_STATS";

    for (const auto* counter : _counters) {
        int64_t value = counter->value();
        ss << " " << counter->name() << "=" << value;
    }

    LOG(INFO) << ss.str();
}

void ConcurrencyStatsManager::_dump_thread_func() {
    Thread::set_self_name("ConcurrencyStatsManager_dump_thread");
    while (_running.load(std::memory_order_relaxed)) {
        // Check if dumping is enabled
        if (config::enable_concurrency_stats_dump) {
            dump_to_log();
        }

        // Sleep for the configured interval
        int32_t interval_ms = config::concurrency_stats_dump_interval_ms;
        if (interval_ms <= 0) {
            interval_ms = 100; // Default to 100ms if invalid
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
    }
}

} // namespace doris
