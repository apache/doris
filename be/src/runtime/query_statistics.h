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

class QueryStatisticsRecvr;
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

    void add_scan_rows(int64_t delta_scan_rows) {
        this->scan_rows.fetch_add(delta_scan_rows, std::memory_order_relaxed);
    }

    void add_scan_bytes(int64_t delta_scan_bytes) {
        this->scan_bytes.fetch_add(delta_scan_bytes, std::memory_order_relaxed);
    }

    void add_cpu_nanos(int64_t delta_cpu_time) {
        this->cpu_nanos.fetch_add(delta_cpu_time, std::memory_order_relaxed);
    }

    void add_shuffle_send_bytes(int64_t delta_bytes) {
        this->shuffle_send_bytes.fetch_add(delta_bytes, std::memory_order_relaxed);
    }

    void add_shuffle_send_rows(int64_t delta_rows) {
        this->shuffle_send_rows.fetch_add(delta_rows, std::memory_order_relaxed);
    }

    void add_scan_bytes_from_local_storage(int64_t scan_bytes_from_local_storage) {
        this->_scan_bytes_from_local_storage += scan_bytes_from_local_storage;
    }

    void add_scan_bytes_from_remote_storage(int64_t scan_bytes_from_remote_storage) {
        this->_scan_bytes_from_remote_storage += scan_bytes_from_remote_storage;
    }

    void add_returned_rows(int64_t num_rows) {
        this->returned_rows.fetch_add(num_rows, std::memory_order_relaxed);
    }

    void set_max_peak_memory_bytes(int64_t max_peak_memory_bytes) {
        this->max_peak_memory_bytes.store(max_peak_memory_bytes, std::memory_order_relaxed);
    }

    void set_current_used_memory_bytes(int64_t current_used_memory) {
        this->current_used_memory_bytes.store(current_used_memory, std::memory_order_relaxed);
    }

    void merge(QueryStatisticsRecvr* recvr);

    void merge(QueryStatisticsRecvr* recvr, int sender_id);

    void clearNodeStatistics();

    void clear() {
        scan_rows.store(0, std::memory_order_relaxed);
        scan_bytes.store(0, std::memory_order_relaxed);
        cpu_nanos.store(0, std::memory_order_relaxed);
        shuffle_send_bytes.store(0, std::memory_order_relaxed);
        shuffle_send_rows.store(0, std::memory_order_relaxed);
        _scan_bytes_from_local_storage.store(0);
        _scan_bytes_from_remote_storage.store(0);

        returned_rows.store(0, std::memory_order_relaxed);
        max_peak_memory_bytes.store(0, std::memory_order_relaxed);
        clearNodeStatistics();
        //clear() is used before collection, so calling "clear" is equivalent to being collected.
        set_collected();
    }

    void to_pb(PQueryStatistics* statistics);
    void to_thrift(TQueryStatistics* statistics) const;
    void from_pb(const PQueryStatistics& statistics);
    bool collected() const { return _collected; }
    void set_collected() { _collected = true; }

    int64_t get_scan_rows() { return scan_rows.load(std::memory_order_relaxed); }
    int64_t get_scan_bytes() { return scan_bytes.load(std::memory_order_relaxed); }
    int64_t get_current_used_memory_bytes() {
        return current_used_memory_bytes.load(std::memory_order_relaxed);
    }

private:
    friend class QueryStatisticsRecvr;
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
class QueryStatisticsRecvr {
public:
    ~QueryStatisticsRecvr() = default;

    // Transmitted via RPC, incurring serialization overhead.
    void insert(const PQueryStatistics& statistics, int sender_id);

    // using local_exchange for transmission, only need to hold a shared pointer.
    void insert(QueryStatisticsPtr statistics, int sender_id);

    QueryStatisticsPtr find(int sender_id);

private:
    friend class QueryStatistics;

    void merge(QueryStatistics* statistics) {
        std::lock_guard<std::mutex> l(_lock);
        for (auto& pair : _query_statistics) {
            statistics->merge(*(pair.second));
        }
    }

    std::map<int, QueryStatisticsPtr> _query_statistics;
    std::mutex _lock;
};

} // namespace doris
