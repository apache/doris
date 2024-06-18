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

#include "util/doris_metrics.h"

namespace doris::io {

std::shared_ptr<AtomicStatistics> FileCacheProfile::report() {
    std::shared_ptr<AtomicStatistics> stats = std::make_shared<AtomicStatistics>();
    std::lock_guard lock(_mtx);
    stats->num_io_bytes_read_from_cache += _profile->num_io_bytes_read_from_cache;
    stats->num_io_bytes_read_from_remote += _profile->num_io_bytes_read_from_remote;
    return stats;
}

void FileCacheProfile::update(FileCacheStatistics* stats) {
    {
        std::lock_guard lock(_mtx);
        if (!_profile) {
            _profile = std::make_shared<AtomicStatistics>();
            _file_cache_metric = std::make_shared<FileCacheMetric>(this);
            _file_cache_metric->register_entity();
        }
    }
    _profile->num_io_bytes_read_from_cache += stats->bytes_read_from_local;
    _profile->num_io_bytes_read_from_remote += stats->bytes_read_from_remote;
}

void FileCacheMetric::register_entity() {
    DorisMetrics::instance()->server_entity()->register_hook("block_file_cache",
                                                             [this]() { update_table_metrics(); });
}

void FileCacheMetric::update_table_metrics() const {
    auto stats = profile->report();
    DorisMetrics::instance()->num_io_bytes_read_from_cache->set_value(
            stats->num_io_bytes_read_from_cache);
    DorisMetrics::instance()->num_io_bytes_read_from_remote->set_value(
            stats->num_io_bytes_read_from_remote);
    DorisMetrics::instance()->num_io_bytes_read_total->set_value(
            stats->num_io_bytes_read_from_cache + stats->num_io_bytes_read_from_remote);
}

} // namespace doris::io
