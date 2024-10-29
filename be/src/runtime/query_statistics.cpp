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

void QueryStatistics::merge(const QueryStatistics& other) {
    scan_rows += other.scan_rows;
    scan_bytes += other.scan_bytes;
    cpu_nanos += other.cpu_nanos;
    shuffle_send_bytes += other.shuffle_send_bytes;
    shuffle_send_rows += other.shuffle_send_rows;
    _scan_bytes_from_local_storage += other._scan_bytes_from_local_storage;
    _scan_bytes_from_remote_storage += other._scan_bytes_from_remote_storage;

    int64_t other_peak_mem = other.max_peak_memory_bytes;
    if (other_peak_mem > this->max_peak_memory_bytes) {
        this->max_peak_memory_bytes = other_peak_mem;
    }

    int64_t other_memory_used = other.current_used_memory_bytes;
    if (other_memory_used > 0) {
        this->current_used_memory_bytes = other_memory_used;
    }
    for (const auto& [node_id, exec_stats_item] : other._exec_stats_items) {
        update_exec_stats_item(node_id, exec_stats_item->push_rows, exec_stats_item->pull_rows,
                                exec_stats_item->pred_filter_rows, exec_stats_item->index_filter_rows,
                                exec_stats_item->rf_filter_rows);
    }
}

void QueryStatistics::update_exec_stats_item(uint32_t node_id, int64_t push, int64_t pull, int64_t pred_filter,
                                             int64_t index_filter, int64_t rf_filter) {
    auto iter = _exec_stats_items.find(node_id);
    if (iter == _exec_stats_items.end()) {
        _exec_stats_items.insert(
                {node_id, std::make_shared<NodeExecStats>(push, pull, pred_filter, index_filter, rf_filter)});
    } else {
        iter->second->push_rows += push;
        iter->second->pull_rows += pull;
        iter->second->pred_filter_rows += pred_filter;
        iter->second->index_filter_rows += index_filter;
        iter->second->rf_filter_rows += rf_filter;
    }
}

void QueryStatistics::add_exec_stats_item(uint32_t node_id, int64_t push, int64_t pull, int64_t pred_filter,
                                          int64_t index_filter, int64_t rf_filter) {
    update_exec_stats_item(node_id, push, pull, pred_filter, index_filter, rf_filter);
}

void QueryStatistics::to_pb(PQueryStatistics* statistics) {
    DCHECK(statistics != nullptr);
    statistics->set_scan_rows(scan_rows);
    statistics->set_scan_bytes(scan_bytes);
    statistics->set_cpu_ms(cpu_nanos / NANOS_PER_MILLIS);
    statistics->set_returned_rows(returned_rows);
    statistics->set_max_peak_memory_bytes(max_peak_memory_bytes);
    statistics->set_scan_bytes_from_remote_storage(_scan_bytes_from_remote_storage);
    statistics->set_scan_bytes_from_local_storage(_scan_bytes_from_local_storage);

    for (const auto& [node_id, exec_stats_item] : _exec_stats_items) {
        auto new_exec_stats_item = statistics->add_node_exec_stats_items();
        new_exec_stats_item->set_node_id(node_id);
        new_exec_stats_item->set_push_rows(exec_stats_item->push_rows);
        new_exec_stats_item->set_pull_rows(exec_stats_item->pull_rows);
        new_exec_stats_item->set_index_filter_rows(exec_stats_item->index_filter_rows);
        new_exec_stats_item->set_rf_filter_rows(exec_stats_item->rf_filter_rows);
        new_exec_stats_item->set_pred_filter_rows(exec_stats_item->pred_filter_rows);
    }
}

void QueryStatistics::to_thrift(TQueryStatistics* statistics) const {
    DCHECK(statistics != nullptr);
    statistics->__set_scan_bytes(scan_bytes);
    statistics->__set_scan_rows(scan_rows);
    statistics->__set_cpu_ms(cpu_nanos / NANOS_PER_MILLIS);
    statistics->__set_returned_rows(returned_rows);
    statistics->__set_max_peak_memory_bytes(max_peak_memory_bytes);
    statistics->__set_current_used_memory_bytes(current_used_memory_bytes);
    statistics->__set_shuffle_send_bytes(shuffle_send_bytes);
    statistics->__set_shuffle_send_rows(shuffle_send_rows);
    statistics->__set_scan_bytes_from_remote_storage(_scan_bytes_from_remote_storage);
    statistics->__set_scan_bytes_from_local_storage(_scan_bytes_from_local_storage);

    std::vector<TNodeExecStatsItemPB> pbList;
    for (const auto& [node_id, exec_stats_item] : _exec_stats_items) {
        TNodeExecStatsItemPB *pb = new TNodeExecStatsItemPB();
        pb->__set_node_id(node_id);
        pb->__set_push_rows(exec_stats_item->push_rows);
        pb->__set_pull_rows(exec_stats_item->pull_rows);
        pb->__set_index_filter_rows(exec_stats_item->index_filter_rows);
        pb->__set_rf_filter_rows(exec_stats_item->rf_filter_rows);
        pb->__set_pred_filter_rows(exec_stats_item->pred_filter_rows);
        pbList.push_back(*pb);
    }
    statistics->__set_node_exec_stats_items(pbList);
}

void QueryStatistics::from_pb(const PQueryStatistics& statistics) {
    scan_rows = statistics.scan_rows();
    scan_bytes = statistics.scan_bytes();
    cpu_nanos = statistics.cpu_ms() * NANOS_PER_MILLIS;
    _scan_bytes_from_local_storage = statistics.scan_bytes_from_local_storage();
    _scan_bytes_from_remote_storage = statistics.scan_bytes_from_remote_storage();
    for (int i = 0; i < statistics.node_exec_stats_items_size(); ++i) {
        const auto& exec_stats_item = statistics.node_exec_stats_items(i);
        update_exec_stats_item(exec_stats_item.node_id(), exec_stats_item.push_rows(), exec_stats_item.pull_rows(),
                                exec_stats_item.pred_filter_rows(), exec_stats_item.index_filter_rows(),
                                exec_stats_item.rf_filter_rows());
    }
}

QueryStatistics::~QueryStatistics() {
    _exec_stats_items.clear();
}

} // namespace doris
