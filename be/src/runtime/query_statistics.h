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

class NodeStatistics {
public:
    NodeStatistics() : peak_memory_bytes(0) {}

    void set_peak_memory(int64_t peak_memory) {
        this->peak_memory_bytes = std::max(this->peak_memory_bytes, peak_memory);
    }

    void merge(const NodeStatistics& other);

    void to_pb(PNodeStatistics* node_statistics);

    void from_pb(const PNodeStatistics& node_statistics);

private:
    friend class QueryStatistics;
    int64_t peak_memory_bytes;
};

// This is responsible for collecting query statistics, usually it consists of
// two parts, one is current fragment or plan's statistics, the other is sub fragment
// or plan's statistics and QueryStatisticsRecvr is responsible for collecting it.
class QueryStatistics {
public:
    QueryStatistics()
            : scan_rows(0), scan_bytes(0), cpu_ms(0), returned_rows(0), max_peak_memory_bytes(0) {}
    ~QueryStatistics();

    void merge(const QueryStatistics& other);

    void add_scan_rows(int64_t scan_rows) { this->scan_rows += scan_rows; }

    void add_scan_bytes(int64_t scan_bytes) { this->scan_bytes += scan_bytes; }

    void add_cpu_ms(int64_t cpu_ms) { this->cpu_ms += cpu_ms; }

    NodeStatistics* add_nodes_statistics(int64_t node_id) {
        NodeStatistics* nodeStatistics = nullptr;
        auto iter = _nodes_statistics_map.find(node_id);
        if (iter == _nodes_statistics_map.end()) {
            nodeStatistics = new NodeStatistics;
            _nodes_statistics_map[node_id] = nodeStatistics;
        } else {
            nodeStatistics = iter->second;
        }
        return nodeStatistics;
    }

    void set_returned_rows(int64_t num_rows) { this->returned_rows = num_rows; }

    void set_max_peak_memory_bytes(int64_t max_peak_memory_bytes) {
        this->max_peak_memory_bytes = max_peak_memory_bytes;
    }

    void merge(QueryStatisticsRecvr* recvr);

    void merge(QueryStatisticsRecvr* recvr, int sender_id);
    // Get the maximum value from the peak memory collected by all node statistics
    int64_t calculate_max_peak_memory_bytes();

    void clearNodeStatistics();

    void clear() {
        scan_rows = 0;
        scan_bytes = 0;
        cpu_ms = 0;
        returned_rows = 0;
        max_peak_memory_bytes = 0;
        clearNodeStatistics();
        //clear() is used before collection, so calling "clear" is equivalent to being collected.
        set_collected();
    }

    void to_pb(PQueryStatistics* statistics);

    void from_pb(const PQueryStatistics& statistics);
    bool collected() const { return _collected; }
    void set_collected() { _collected = true; }

private:
    friend class QueryStatisticsRecvr;
    int64_t scan_rows;
    int64_t scan_bytes;
    int64_t cpu_ms;
    // number rows returned by query.
    // only set once by result sink when closing.
    int64_t returned_rows;
    // Maximum memory peak for all backends.
    // only set once by result sink when closing.
    int64_t max_peak_memory_bytes;
    // The statistics of the query on each backend.
    using NodeStatisticsMap = std::unordered_map<int64_t, NodeStatistics*>;
    NodeStatisticsMap _nodes_statistics_map;
    bool _collected = false;
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

private:
    friend class QueryStatistics;

    void merge(QueryStatistics* statistics) {
        std::lock_guard<SpinLock> l(_lock);
        for (auto& pair : _query_statistics) {
            if (pair.second) {
                statistics->merge(*(pair.second));
            }
        }
    }

    std::map<int, QueryStatisticsPtr> _query_statistics;
    SpinLock _lock;
};

} // namespace doris
