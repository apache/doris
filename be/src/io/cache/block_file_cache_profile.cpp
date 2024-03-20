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

#include "io/cache/block_file_cache_profile.h"

#include <functional>
#include <memory>
#include <string>

namespace doris::io {

DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(num_io_bytes_read_total, MetricUnit::OPERATIONS);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(num_io_bytes_read_from_cache, MetricUnit::OPERATIONS);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(num_io_bytes_read_from_remote, MetricUnit::OPERATIONS);

std::shared_ptr<AtomicStatistics> FileCacheProfile::report(int64_t table_id) {
    std::shared_ptr<AtomicStatistics> stats = std::make_shared<AtomicStatistics>();
    std::lock_guard lock(_mtx);
    auto& table_stats = _profile[table_id];
    stats->num_io_bytes_read_from_cache += table_stats->num_io_bytes_read_from_cache;
    stats->num_io_bytes_read_from_remote += table_stats->num_io_bytes_read_from_remote;
    return stats;
}

void FileCacheProfile::update(int64_t table_id, FileCacheStatistics* stats) {
    std::shared_ptr<AtomicStatistics> count;
    std::shared_ptr<FileCacheMetric> table_metric;
    {
        std::lock_guard lock(_mtx);
        if (!_profile.contains(table_id)) {
            _profile[table_id] = std::make_shared<AtomicStatistics>();
            table_metric = std::make_shared<FileCacheMetric>(table_id, this);
            _table_metrics[table_id] = table_metric;
        }
        count = _profile[table_id];
    }
    if (table_metric) [[unlikely]] {
        table_metric->register_entity();
    }
    count->num_io_bytes_read_from_cache += stats->bytes_read_from_local;
    count->num_io_bytes_read_from_remote += stats->bytes_read_from_remote;
}

void FileCacheMetric::register_entity() {
    std::string table_id_str = std::to_string(table_id);
    entity = DorisMetrics::instance()->metric_registry()->register_entity(
            std::string("cloud_file_cache"), {{"table_id", table_id_str}});
    INT_ATOMIC_COUNTER_METRIC_REGISTER(entity, num_io_bytes_read_total);
    INT_ATOMIC_COUNTER_METRIC_REGISTER(entity, num_io_bytes_read_from_cache);
    INT_ATOMIC_COUNTER_METRIC_REGISTER(entity, num_io_bytes_read_from_remote);
    entity->register_hook("cloud_file_cache", [this]() { update_table_metrics(); });
}

void FileCacheMetric::update_table_metrics() const {
    auto stats = profile->report(table_id);
    num_io_bytes_read_from_cache->set_value(stats->num_io_bytes_read_from_cache);
    num_io_bytes_read_from_remote->set_value(stats->num_io_bytes_read_from_remote);
    num_io_bytes_read_total->set_value(stats->num_io_bytes_read_from_cache +
                                       stats->num_io_bytes_read_from_remote);
}

} // namespace doris::io
