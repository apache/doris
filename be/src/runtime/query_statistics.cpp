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

#include "runtime/query_statistics.h"

#include <gen_cpp/data.pb.h>
#include <glog/logging.h>

#include <memory>

#include "util/time.h"

namespace doris {

void NodeStatistics::merge(const NodeStatistics& other) {
    peak_memory_bytes = std::max(other.peak_memory_bytes, peak_memory_bytes);
}

void NodeStatistics::to_pb(PNodeStatistics* node_statistics) {
    DCHECK(node_statistics != nullptr);
    node_statistics->set_peak_memory_bytes(peak_memory_bytes);
}

void NodeStatistics::from_pb(const PNodeStatistics& node_statistics) {
    peak_memory_bytes = node_statistics.peak_memory_bytes();
}

void QueryStatistics::merge(const QueryStatistics& other) {
    scan_rows += other.scan_rows;
    scan_bytes += other.scan_bytes;
    int64_t other_cpu_time = other.cpu_nanos.load(std::memory_order_relaxed);
    cpu_nanos += other_cpu_time;

    int64_t other_peak_mem = other.max_peak_memory_bytes.load(std::memory_order_relaxed);
    if (other_peak_mem > this->max_peak_memory_bytes) {
        this->max_peak_memory_bytes = other_peak_mem;
    }

    int64_t other_memory_used = other.current_used_memory_bytes.load(std::memory_order_relaxed);
    if (other_memory_used > 0) {
        this->current_used_memory_bytes = other_memory_used;
    }

    for (auto& other_node_statistics : other._nodes_statistics_map) {
        int64_t node_id = other_node_statistics.first;
        auto node_statistics = add_nodes_statistics(node_id);
        node_statistics->merge(*other_node_statistics.second);
    }
}

void QueryStatistics::to_pb(PQueryStatistics* statistics) {
    DCHECK(statistics != nullptr);
    statistics->set_scan_rows(scan_rows);
    statistics->set_scan_bytes(scan_bytes);
    statistics->set_cpu_ms(cpu_nanos / NANOS_PER_MILLIS);
    statistics->set_returned_rows(returned_rows);
    statistics->set_max_peak_memory_bytes(max_peak_memory_bytes);
    for (auto iter = _nodes_statistics_map.begin(); iter != _nodes_statistics_map.end(); ++iter) {
        auto node_statistics = statistics->add_nodes_statistics();
        node_statistics->set_node_id(iter->first);
        iter->second->to_pb(node_statistics);
    }
}

void QueryStatistics::to_thrift(TQueryStatistics* statistics) const {
    DCHECK(statistics != nullptr);
    statistics->__set_scan_bytes(scan_bytes.load(std::memory_order_relaxed));
    statistics->__set_scan_rows(scan_rows.load(std::memory_order_relaxed));
    statistics->__set_cpu_ms(cpu_nanos.load(std::memory_order_relaxed) / NANOS_PER_MILLIS);
    statistics->__set_returned_rows(returned_rows);
    statistics->__set_max_peak_memory_bytes(max_peak_memory_bytes.load(std::memory_order_relaxed));
    statistics->__set_current_used_memory_bytes(
            current_used_memory_bytes.load(std::memory_order_relaxed));
}

void QueryStatistics::from_pb(const PQueryStatistics& statistics) {
    scan_rows = statistics.scan_rows();
    scan_bytes = statistics.scan_bytes();
    cpu_nanos = statistics.cpu_ms() * NANOS_PER_MILLIS;
    for (auto& p_node_statistics : statistics.nodes_statistics()) {
        int64_t node_id = p_node_statistics.node_id();
        auto node_statistics = add_nodes_statistics(node_id);
        node_statistics->from_pb(p_node_statistics);
    }
}

int64_t QueryStatistics::calculate_max_peak_memory_bytes() {
    int64_t max_peak_memory = 0;
    for (auto iter = _nodes_statistics_map.begin(); iter != _nodes_statistics_map.end(); ++iter) {
        if (max_peak_memory < iter->second->peak_memory_bytes) {
            max_peak_memory = iter->second->peak_memory_bytes;
        }
    }
    return max_peak_memory;
}

void QueryStatistics::merge(QueryStatisticsRecvr* recvr) {
    recvr->merge(this);
}

void QueryStatistics::merge(QueryStatisticsRecvr* recvr, int sender_id) {
    DCHECK(recvr != nullptr);
    auto QueryStatisticsptr = recvr->find(sender_id);
    if (QueryStatisticsptr) {
        merge(*QueryStatisticsptr);
    }
}

void QueryStatistics::clearNodeStatistics() {
    for (auto& pair : _nodes_statistics_map) {
        delete pair.second;
    }
    _nodes_statistics_map.clear();
}

QueryStatistics::~QueryStatistics() {
    clearNodeStatistics();
}

void QueryStatisticsRecvr::insert(const PQueryStatistics& statistics, int sender_id) {
    std::lock_guard<std::mutex> l(_lock);
    if (!_query_statistics.contains(sender_id)) {
        _query_statistics[sender_id] = std::make_shared<QueryStatistics>();
    }
    _query_statistics[sender_id]->from_pb(statistics);
}

void QueryStatisticsRecvr::insert(QueryStatisticsPtr statistics, int sender_id) {
    if (!statistics->collected()) return;
    if (_query_statistics.contains(sender_id)) return;
    std::lock_guard<std::mutex> l(_lock);
    _query_statistics[sender_id] = statistics;
}

QueryStatisticsPtr QueryStatisticsRecvr::find(int sender_id) {
    std::lock_guard<std::mutex> l(_lock);
    auto it = _query_statistics.find(sender_id);
    if (it != _query_statistics.end()) {
        return it->second;
    }
    return nullptr;
}

} // namespace doris
