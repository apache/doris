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

#include "io/cache/block/block_file_cache_profile.h"

#include <functional>
#include <memory>
#include <string>

namespace doris {
namespace io {

DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(num_io_bytes_read_total, MetricUnit::OPERATIONS);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(num_io_bytes_read_from_cache, MetricUnit::OPERATIONS);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(num_io_bytes_read_from_remote, MetricUnit::OPERATIONS);

std::shared_ptr<AtomicStatistics> FileCacheProfile::report(int64_t table_id, int64_t partition_id) {
    std::shared_ptr<AtomicStatistics> stats = std::make_shared<AtomicStatistics>();
    if (_profile.count(table_id) == 1 && _profile[table_id].count(partition_id) == 1) {
        std::lock_guard lock(_mtx);
        stats->num_io_bytes_read_from_cache +=
                (_profile[table_id][partition_id])->num_io_bytes_read_from_cache;
        stats->num_io_bytes_read_from_cache +=
                _profile[table_id][partition_id]->num_io_bytes_read_from_cache;
    }
    return stats;
}

std::shared_ptr<AtomicStatistics> FileCacheProfile::report(int64_t table_id) {
    std::shared_ptr<AtomicStatistics> stats = std::make_shared<AtomicStatistics>();
    if (_profile.count(table_id) == 1) {
        std::lock_guard lock(_mtx);
        auto& partition_map = _profile[table_id];
        for (auto& [_, partition_stats] : partition_map) {
            stats->num_io_bytes_read_from_cache += partition_stats->num_io_bytes_read_from_cache;
            stats->num_io_bytes_read_from_remote += partition_stats->num_io_bytes_read_from_remote;
        }
    }
    return stats;
}

void FileCacheProfile::update(int64_t table_id, int64_t partition_id, OlapReaderStatistics* stats) {
    if (!s_enable_profile.load(std::memory_order_acquire)) {
        return;
    }
    std::shared_ptr<AtomicStatistics> count;
    std::shared_ptr<FileCacheMetric> partition_metric;
    std::shared_ptr<FileCacheMetric> table_metric;
    {
        std::lock_guard lock(_mtx);
        if (_profile.count(table_id) < 1 || _profile[table_id].count(partition_id) < 1) {
            _profile[table_id][partition_id] = std::make_shared<AtomicStatistics>();
            partition_metric = std::make_shared<FileCacheMetric>(table_id, partition_id, this);
            _partition_metrics[table_id][partition_id] = partition_metric;
            if (_table_metrics.count(table_id) < 1) {
                table_metric = std::make_shared<FileCacheMetric>(table_id, this);
                _table_metrics[table_id] = table_metric;
            }
        }
        count = _profile[table_id][partition_id];
    }
    if (partition_metric) {
        partition_metric->register_entity();
    }
    if (table_metric) {
        table_metric->register_entity();
    }
    count->num_io_bytes_read_from_cache += stats->file_cache_stats.bytes_read_from_local;
    count->num_io_bytes_read_from_remote += stats->file_cache_stats.bytes_read_from_remote;
}

void FileCacheProfile::deregister_metric(int64_t table_id, int64_t partition_id) {
    if (!s_enable_profile.load(std::memory_order_acquire)) {
        return;
    }
    std::shared_ptr<FileCacheMetric> partition_metric;
    std::shared_ptr<FileCacheMetric> table_metric;
    {
        std::lock_guard lock(_mtx);
        partition_metric = _partition_metrics[table_id][partition_id];
        _partition_metrics[table_id].erase(partition_id);
        if (_partition_metrics[table_id].empty()) {
            _partition_metrics.erase(table_id);
            table_metric = _table_metrics[table_id];
            _table_metrics.erase(table_id);
        }
        _profile[table_id].erase(partition_id);
        if (_profile[table_id].empty()) {
            _profile.erase(table_id);
        }
    }
    partition_metric->deregister_entity();
    if (table_metric) {
        table_metric->deregister_entity();
    }
}

void FileCacheMetric::register_entity() {
    std::string table_id_str = std::to_string(table_id);
    std::string partition_id_str = partition_id != -1 ? std::to_string(partition_id) : "total";
    entity = DorisMetrics::instance()->metric_registry()->register_entity(
            std::string("cloud_file_cache"),
            {{"table_id", table_id_str}, {"partition_id", partition_id_str}});
    INT_COUNTER_METRIC_REGISTER(entity, num_io_bytes_read_total);
    INT_COUNTER_METRIC_REGISTER(entity, num_io_bytes_read_from_cache);
    INT_COUNTER_METRIC_REGISTER(entity, num_io_bytes_read_from_remote);
    entity->register_hook("cloud_file_cache",
                          std::bind(&FileCacheMetric::update_table_metrics, this));
}

void FileCacheMetric::update_table_metrics() const {
    auto stats = profile->report(table_id);
    num_io_bytes_read_from_cache->set_value(stats->num_io_bytes_read_from_cache);
    num_io_bytes_read_from_remote->set_value(stats->num_io_bytes_read_from_remote);
    num_io_bytes_read_total->set_value(stats->num_io_bytes_read_from_cache +
                                       stats->num_io_bytes_read_from_remote);
}

void FileCacheMetric::update_partition_metrics() const {
    auto stats = profile->report(table_id, partition_id);
    num_io_bytes_read_from_cache->set_value(stats->num_io_bytes_read_from_cache);
    num_io_bytes_read_from_remote->set_value(stats->num_io_bytes_read_from_remote);
    num_io_bytes_read_total->set_value(stats->num_io_bytes_read_from_cache +
                                       stats->num_io_bytes_read_from_remote);
}

} // namespace io
} // namespace doris
