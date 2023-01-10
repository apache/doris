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

#include <memory>

#include "http/http_common.h"

namespace doris {
namespace io {

DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(file_cache_num_io_total, MetricUnit::OPERATIONS);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(file_cache_num_io_hit_cache, MetricUnit::OPERATIONS);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(file_cache_num_io_bytes_read_total, MetricUnit::OPERATIONS);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(file_cache_num_io_bytes_read_from_file_cache,
                                     MetricUnit::OPERATIONS);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(file_cache_num_io_bytes_read_from_write_cache,
                                     MetricUnit::OPERATIONS);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(file_cache_num_io_written_in_file_cache,
                                     MetricUnit::OPERATIONS);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(file_cache_num_io_bytes_written_in_file_cache,
                                     MetricUnit::OPERATIONS);

FileCacheStatistics FileCacheProfile::report(int64_t table_id, int64_t partition_id) {
    FileCacheStatistics stats;
    if (_profile.count(table_id) == 1 && _profile[table_id].count(partition_id) == 1) {
        std::shared_ptr<AtomicStatistics> count;
        {
            std::lock_guard lock(_mtx);
            count = _profile[table_id][partition_id];
        }
        stats.num_io_total = count->num_io_total.load(std::memory_order_relaxed);
        stats.num_io_hit_cache = count->num_io_hit_cache.load(std::memory_order_relaxed);
        stats.num_io_bytes_read_total =
                count->num_io_bytes_read_total.load(std::memory_order_relaxed);
        stats.num_io_bytes_read_from_file_cache =
                count->num_io_bytes_read_from_file_cache.load(std::memory_order_relaxed);
        stats.num_io_bytes_read_from_write_cache =
                count->num_io_bytes_read_from_write_cache.load(std::memory_order_relaxed);
        stats.num_io_written_in_file_cache =
                count->num_io_written_in_file_cache.load(std::memory_order_relaxed);
        stats.num_io_bytes_written_in_file_cache =
                count->num_io_bytes_written_in_file_cache.load(std::memory_order_relaxed);
    }
    return stats;
}

FileCacheStatistics FileCacheProfile::report(int64_t table_id) {
    FileCacheStatistics stats;
    if (_profile.count(table_id) == 1) {
        std::lock_guard lock(_mtx);
        auto& partition_map = _profile[table_id];
        for (auto& [partition_id, atomic_stats] : partition_map) {
            stats.num_io_total += atomic_stats->num_io_total.load(std::memory_order_relaxed);
            stats.num_io_hit_cache +=
                    atomic_stats->num_io_hit_cache.load(std::memory_order_relaxed);
            stats.num_io_bytes_read_total +=
                    atomic_stats->num_io_bytes_read_total.load(std::memory_order_relaxed);
            stats.num_io_bytes_read_from_file_cache +=
                    atomic_stats->num_io_bytes_read_from_file_cache.load(std::memory_order_relaxed);
            stats.num_io_bytes_read_from_write_cache +=
                    atomic_stats->num_io_bytes_read_from_write_cache.load(
                            std::memory_order_relaxed);
            stats.num_io_written_in_file_cache +=
                    atomic_stats->num_io_written_in_file_cache.load(std::memory_order_relaxed);
            stats.num_io_bytes_written_in_file_cache +=
                    atomic_stats->num_io_bytes_written_in_file_cache.load(
                            std::memory_order_relaxed);
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
    if (partition_metric) [[unlikely]] {
        partition_metric->register_entity();
    }
    if (table_metric) [[unlikely]] {
        table_metric->register_entity();
    }
    count->num_io_total.fetch_add(stats->file_cache_stats.num_io_total, std::memory_order_relaxed);
    count->num_io_hit_cache.fetch_add(stats->file_cache_stats.num_io_hit_cache,
                                      std::memory_order_relaxed);
    count->num_io_bytes_read_total.fetch_add(stats->file_cache_stats.num_io_bytes_read_total,
                                             std::memory_order_relaxed);
    count->num_io_bytes_read_from_file_cache.fetch_add(
            stats->file_cache_stats.num_io_bytes_read_from_file_cache, std::memory_order_relaxed);
    count->num_io_bytes_read_from_write_cache.fetch_add(
            stats->file_cache_stats.num_io_bytes_read_from_write_cache, std::memory_order_relaxed);
    count->num_io_written_in_file_cache.fetch_add(
            stats->file_cache_stats.num_io_written_in_file_cache, std::memory_order_relaxed);
    count->num_io_bytes_written_in_file_cache.fetch_add(
            stats->file_cache_stats.num_io_bytes_written_in_file_cache, std::memory_order_relaxed);
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
    std::string name = "table_" + std::to_string(table_id);
    if (partition_id != -1) {
        name += "_partition_" + std::to_string(partition_id);
    }
    entity = DorisMetrics::instance()->metric_registry()->register_entity(
            std::string("cloud_file_cache"), {{"name", name}});
    INT_ATOMIC_COUNTER_METRIC_REGISTER(entity, file_cache_num_io_total);
    INT_ATOMIC_COUNTER_METRIC_REGISTER(entity, file_cache_num_io_hit_cache);
    INT_ATOMIC_COUNTER_METRIC_REGISTER(entity, file_cache_num_io_bytes_read_total);
    INT_ATOMIC_COUNTER_METRIC_REGISTER(entity, file_cache_num_io_bytes_read_from_file_cache);
    INT_ATOMIC_COUNTER_METRIC_REGISTER(entity, file_cache_num_io_bytes_read_from_write_cache);
    INT_ATOMIC_COUNTER_METRIC_REGISTER(entity, file_cache_num_io_written_in_file_cache);
    INT_ATOMIC_COUNTER_METRIC_REGISTER(entity, file_cache_num_io_bytes_written_in_file_cache);
    entity->register_hook(name, std::bind(&FileCacheMetric::update_table_metrics, this));
}

void FileCacheMetric::update_table_metrics() const {
    FileCacheStatistics stats = profile->report(table_id);
    file_cache_num_io_total->set_value(stats.num_io_total);
    file_cache_num_io_hit_cache->set_value(stats.num_io_hit_cache);
    file_cache_num_io_bytes_read_total->set_value(stats.num_io_bytes_read_total);
    file_cache_num_io_bytes_read_from_file_cache->set_value(
            stats.num_io_bytes_read_from_file_cache);
    file_cache_num_io_bytes_read_from_write_cache->set_value(
            stats.num_io_bytes_read_from_write_cache);
    file_cache_num_io_written_in_file_cache->set_value(stats.num_io_written_in_file_cache);
    file_cache_num_io_bytes_written_in_file_cache->set_value(
            stats.num_io_bytes_written_in_file_cache);
}

void FileCacheMetric::update_partition_metrics() const {
    FileCacheStatistics stats = profile->report(table_id, partition_id);
    file_cache_num_io_total->set_value(stats.num_io_total);
    file_cache_num_io_hit_cache->set_value(stats.num_io_hit_cache);
    file_cache_num_io_bytes_read_total->set_value(stats.num_io_bytes_read_total);
    file_cache_num_io_bytes_read_from_file_cache->set_value(
            stats.num_io_bytes_read_from_file_cache);
    file_cache_num_io_bytes_read_from_write_cache->set_value(
            stats.num_io_bytes_read_from_write_cache);
    file_cache_num_io_written_in_file_cache->set_value(stats.num_io_written_in_file_cache);
    file_cache_num_io_bytes_written_in_file_cache->set_value(
            stats.num_io_bytes_written_in_file_cache);
}

} // namespace io
} // namespace doris
