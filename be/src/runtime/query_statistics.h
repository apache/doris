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

#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <stdint.h>

#include <map>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>

#include "util/spinlock.h"

namespace doris {

class PNodeStatistics;
class PQueryStatistics;

// This is responsible for collecting query statistics, usually it consists of
// two parts, one is current fragment or plan's statistics, the other is sub fragment
// or plan's statistics and QueryStatisticsRecvr is responsible for collecting it.
class QueryStatistics {
public:
    QueryStatistics()
            : scan_rows(0),
              scan_bytes(0),
              cpu_nanos(0),
              returned_rows(0),
              max_peak_memory_bytes(0),
              current_used_memory_bytes(0),
              shuffle_send_bytes(0),
              shuffle_send_rows(0) {}
    virtual ~QueryStatistics();

    void merge(const QueryStatistics& other);

    void add_scan_rows(int64_t delta_scan_rows) { scan_rows += delta_scan_rows; }

    void add_scan_bytes(int64_t delta_scan_bytes) { scan_bytes += delta_scan_bytes; }

    void add_cpu_nanos(int64_t delta_cpu_time) { cpu_nanos += delta_cpu_time; }

    void add_shuffle_send_bytes(int64_t delta_bytes) { shuffle_send_bytes += delta_bytes; }

    void add_shuffle_send_rows(int64_t delta_rows) { shuffle_send_rows += delta_rows; }

    void add_scan_bytes_from_local_storage(int64_t scan_bytes_from_local_storage) {
        _scan_bytes_from_local_storage += scan_bytes_from_local_storage;
    }

    void add_scan_bytes_from_remote_storage(int64_t scan_bytes_from_remote_storage) {
        _scan_bytes_from_remote_storage += scan_bytes_from_remote_storage;
    }

    void add_returned_rows(int64_t num_rows) { returned_rows += num_rows; }

    void set_max_peak_memory_bytes(int64_t max_peak_memory_bytes) {
        this->max_peak_memory_bytes = max_peak_memory_bytes;
    }

    void set_current_used_memory_bytes(int64_t current_used_memory) {
        current_used_memory_bytes = current_used_memory;
    }

    void to_pb(PQueryStatistics* statistics);
    void to_thrift(TQueryStatistics* statistics) const;
    void from_pb(const PQueryStatistics& statistics);
    bool collected() const { return _collected; }

    int64_t get_scan_rows() { return scan_rows; }
    int64_t get_scan_bytes() { return scan_bytes; }
    int64_t get_current_used_memory_bytes() { return current_used_memory_bytes; }

private:
    std::atomic<int64_t> scan_rows;
    std::atomic<int64_t> scan_bytes;
    std::atomic<int64_t> cpu_nanos;
    std::atomic<int64_t> _scan_bytes_from_local_storage;
    std::atomic<int64_t> _scan_bytes_from_remote_storage;
    // number rows returned by query.
    // only set once by result sink when closing.
    std::atomic<int64_t> returned_rows;
    // Maximum memory peak for all backends.
    // only set once by result sink when closing.
    std::atomic<int64_t> max_peak_memory_bytes;
    bool _collected = false;
    std::atomic<int64_t> current_used_memory_bytes;

    std::atomic<int64_t> shuffle_send_bytes;
    std::atomic<int64_t> shuffle_send_rows;
};
using QueryStatisticsPtr = std::shared_ptr<QueryStatistics>;
// It is used for collecting sub plan query statistics in DataStreamRecvr.

} // namespace doris
