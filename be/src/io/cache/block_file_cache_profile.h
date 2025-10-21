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

#include <gen_cpp/Metrics_types.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "io/io_common.h"
#include "olap/olap_common.h"
#include "util/doris_metrics.h"
#include "util/metrics.h"
#include "util/runtime_profile.h"

namespace doris {
namespace io {

struct AtomicStatistics {
    std::atomic<int64_t> num_io_bytes_read_from_cache = 0;
    std::atomic<int64_t> num_io_bytes_read_from_remote = 0;
};
class FileCacheMetrics {
public:
    static FileCacheMetrics& instance() {
        static FileCacheMetrics s_metrics;
        return s_metrics;
    }

    FileCacheMetrics() {
        FileCacheStatistics stats;
        update(&stats);
    }

    void update(FileCacheStatistics* stats);

private:
    std::shared_ptr<AtomicStatistics> report();
    void register_entity();
    void update_metrics_callback();

private:
    std::mutex _mtx;
    // use shared_ptr for concurrent
    std::shared_ptr<AtomicStatistics> _statistics;
};

struct FileCacheProfileReporter {
    RuntimeProfile::Counter* num_local_io_total = nullptr;
    RuntimeProfile::Counter* num_remote_io_total = nullptr;
    RuntimeProfile::Counter* local_io_timer = nullptr;
    RuntimeProfile::Counter* bytes_scanned_from_cache = nullptr;
    RuntimeProfile::Counter* bytes_scanned_from_remote = nullptr;
    RuntimeProfile::Counter* remote_io_timer = nullptr;
    RuntimeProfile::Counter* write_cache_io_timer = nullptr;
    RuntimeProfile::Counter* bytes_write_into_cache = nullptr;
    RuntimeProfile::Counter* num_skip_cache_io_total = nullptr;
    RuntimeProfile::Counter* read_cache_file_directly_timer = nullptr;
    RuntimeProfile::Counter* cache_get_or_set_timer = nullptr;
    RuntimeProfile::Counter* lock_wait_timer = nullptr;
    RuntimeProfile::Counter* get_timer = nullptr;
    RuntimeProfile::Counter* set_timer = nullptr;

    RuntimeProfile::Counter* inverted_index_num_local_io_total = nullptr;
    RuntimeProfile::Counter* inverted_index_num_remote_io_total = nullptr;
    RuntimeProfile::Counter* inverted_index_bytes_scanned_from_cache = nullptr;
    RuntimeProfile::Counter* inverted_index_bytes_scanned_from_remote = nullptr;
    RuntimeProfile::Counter* inverted_index_local_io_timer = nullptr;
    RuntimeProfile::Counter* inverted_index_remote_io_timer = nullptr;
    RuntimeProfile::Counter* inverted_index_io_timer = nullptr;

    FileCacheProfileReporter(RuntimeProfile* profile);
    void update(const FileCacheStatistics* statistics) const;
};

} // namespace io
} // namespace doris