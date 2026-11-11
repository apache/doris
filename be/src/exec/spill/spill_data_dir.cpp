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

#include "exec/spill/spill_data_dir.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include <utility>

#include "common/logging.h"
#include "common/metrics/doris_metrics.h"
#include "common/metrics/metrics.h"
#include "util/pretty_printer.h"

namespace doris {

DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(spill_file_created_total, MetricUnit::NOUNIT);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(spill_file_deleted_total, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(spill_disk_capacity, MetricUnit::BYTES);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(spill_disk_limit, MetricUnit::BYTES);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(spill_disk_avail_capacity, MetricUnit::BYTES);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(spill_disk_data_size, MetricUnit::BYTES);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(spill_disk_inode_total, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(spill_disk_inode_available, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(spill_disk_has_spill_data, MetricUnit::BYTES);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(spill_disk_has_spill_gc_data, MetricUnit::BYTES);

SpillDataDir::SpillDataDir(std::string path, std::string spill_root, const std::string& metric_path,
                           int64_t capacity_bytes, TStorageMedium::type storage_medium)
        : _path(std::move(path)),
          _spill_root(std::move(spill_root)),
          _disk_capacity_bytes(capacity_bytes),
          _storage_medium(storage_medium) {
    spill_data_dir_metric_entity = DorisMetrics::instance()->metric_registry()->register_entity(
            std::string("spill_data_dir.") + _path, {{"path", metric_path}});
    INT_COUNTER_METRIC_REGISTER(spill_data_dir_metric_entity, spill_file_created_total);
    INT_COUNTER_METRIC_REGISTER(spill_data_dir_metric_entity, spill_file_deleted_total);
    INT_GAUGE_METRIC_REGISTER(spill_data_dir_metric_entity, spill_disk_capacity);
    INT_GAUGE_METRIC_REGISTER(spill_data_dir_metric_entity, spill_disk_limit);
    INT_GAUGE_METRIC_REGISTER(spill_data_dir_metric_entity, spill_disk_avail_capacity);
    INT_GAUGE_METRIC_REGISTER(spill_data_dir_metric_entity, spill_disk_data_size);
    INT_GAUGE_METRIC_REGISTER(spill_data_dir_metric_entity, spill_disk_inode_total);
    INT_GAUGE_METRIC_REGISTER(spill_data_dir_metric_entity, spill_disk_inode_available);
    INT_GAUGE_METRIC_REGISTER(spill_data_dir_metric_entity, spill_disk_has_spill_data);
    INT_GAUGE_METRIC_REGISTER(spill_data_dir_metric_entity, spill_disk_has_spill_gc_data);
}

SpillDataDir::~SpillDataDir() = default;

void SpillDataDir::record_spill_file_created() {
    spill_file_created_total->increment(1);
}

void SpillDataDir::record_spill_file_deleted(size_t count) {
    spill_file_deleted_total->increment(static_cast<int64_t>(count));
}

std::string SpillDataDir::get_spill_data_path(const std::string& query_id) const {
    DCHECK(ready()) << "spill store is not ready: " << _path;
    if (query_id.empty()) {
        return _spill_root;
    }
    return fmt::format("{}/{}", _spill_root, query_id);
}

bool SpillDataDir::reach_capacity_limit(int64_t incoming_data_size) {
    std::lock_guard<std::mutex> l(_mutex);
    return _reach_limit_unlocked(incoming_data_size, l);
}

Status SpillDataDir::try_reserve(int64_t bytes) {
    std::lock_guard<std::mutex> l(_mutex);
    if (_reach_limit_unlocked(bytes, l)) {
        return Status::Error<ErrorCode::DISK_REACH_CAPACITY_LIMIT>(
                "spill data total size exceed limit, path: {}, size limit: {}, spill data "
                "size: {}",
                _path, PrettyPrinter::print_bytes(_spill_data_limit_bytes),
                PrettyPrinter::print_bytes(_spill_data_bytes));
    }
    _spill_data_bytes += bytes;
    spill_disk_data_size->set_value(_spill_data_bytes);
    return Status::OK();
}

void SpillDataDir::reserve(int64_t bytes) {
    std::lock_guard<std::mutex> l(_mutex);
    _spill_data_bytes += bytes;
    spill_disk_data_size->set_value(_spill_data_bytes);
}

void SpillDataDir::release(int64_t bytes) {
    std::lock_guard<std::mutex> lock(_mutex);
    DCHECK_GE(bytes, 0);
    DCHECK_GE(_spill_data_bytes, bytes);
    _spill_data_bytes -= bytes;
    spill_disk_data_size->set_value(_spill_data_bytes);
}

int64_t SpillDataDir::get_spill_data_bytes() {
    std::lock_guard<std::mutex> l(_mutex);
    return _spill_data_bytes;
}

int64_t SpillDataDir::get_spill_data_limit() {
    std::lock_guard<std::mutex> l(_mutex);
    return _spill_data_limit_bytes;
}

double SpillDataDir::_inode_usage(size_t total, size_t available) {
    return total == 0 ? 0 : (double)(total - available) / (double)total;
}

std::string SpillDataDir::_inode_debug_string(size_t total, size_t available) {
    if (total == 0) {
        return "inodes: unknown";
    }
    return fmt::format("inodes: total: {}, used: {}, available: {}, used pct: {:.2f}%", total,
                       total - available, available, _inode_usage(total, available) * 100);
}

std::string SpillDataDir::debug_string() {
    std::lock_guard<std::mutex> l(_mutex);
    return fmt::format(
            "path: {}, capacity: {}, limit: {}, used: {}, available: {}, {}", _path,
            PrettyPrinter::print_bytes(_disk_capacity_bytes),
            PrettyPrinter::print_bytes(_spill_data_limit_bytes),
            PrettyPrinter::print_bytes(_spill_data_bytes),
            PrettyPrinter::print_bytes(_available_bytes),
            _inode_debug_string(static_cast<size_t>(spill_disk_inode_total->value()),
                                static_cast<size_t>(spill_disk_inode_available->value())));
}

} // namespace doris
