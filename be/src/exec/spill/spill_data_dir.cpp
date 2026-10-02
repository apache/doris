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

#include <algorithm>
#include <filesystem>
#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "common/metrics/doris_metrics.h"
#include "common/metrics/metrics.h"
#include "io/fs/local_file_system.h"
#include "storage/olap_define.h"
#include "util/parse_util.h"
#include "util/pretty_printer.h"

namespace doris {

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(spill_disk_capacity, MetricUnit::BYTES);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(spill_disk_limit, MetricUnit::BYTES);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(spill_disk_avail_capacity, MetricUnit::BYTES);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(spill_disk_data_size, MetricUnit::BYTES);
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
    INT_GAUGE_METRIC_REGISTER(spill_data_dir_metric_entity, spill_disk_capacity);
    INT_GAUGE_METRIC_REGISTER(spill_data_dir_metric_entity, spill_disk_limit);
    INT_GAUGE_METRIC_REGISTER(spill_data_dir_metric_entity, spill_disk_avail_capacity);
    INT_GAUGE_METRIC_REGISTER(spill_data_dir_metric_entity, spill_disk_data_size);
    INT_GAUGE_METRIC_REGISTER(spill_data_dir_metric_entity, spill_disk_has_spill_data);
    INT_GAUGE_METRIC_REGISTER(spill_data_dir_metric_entity, spill_disk_has_spill_gc_data);
}

SpillDataDir::~SpillDataDir() = default;

std::string SpillDataDir::get_spill_data_path(const std::string& query_id) const {
    DCHECK(ready()) << "spill store is not ready: " << _path;
    if (query_id.empty()) {
        return _spill_root;
    }
    return fmt::format("{}/{}", _spill_root, query_id);
}

bool SpillDataDir::reach_capacity_limit(int64_t incoming_data_size) {
    std::lock_guard<std::mutex> l(_mutex);
    return _reach_limit_unlocked(incoming_data_size);
}

Status SpillDataDir::try_reserve(int64_t bytes, bool force) {
    std::lock_guard<std::mutex> l(_mutex);
    if (!force && _reach_limit_unlocked(bytes)) {
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

void SpillDataDir::update_spill_data_usage(int64_t incoming_data_size) {
    std::lock_guard<std::mutex> l(_mutex);
    _spill_data_bytes += incoming_data_size;
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

std::string SpillDataDir::debug_string() {
    return fmt::format(
            "path: {}, capacity: {}, limit: {}, used: {}, available: "
            "{}",
            _path, PrettyPrinter::print_bytes(_disk_capacity_bytes),
            PrettyPrinter::print_bytes(_spill_data_limit_bytes),
            PrettyPrinter::print_bytes(_spill_data_bytes),
            PrettyPrinter::print_bytes(_available_bytes));
}

// ── LocalSpillDataDir ──

static bool is_directory_empty(const std::filesystem::path& dir) {
    // Spill cleanup may delete the directory while the iterator is constructed or advanced. Treat
    // that race as empty for these presence metrics.
    try {
        return std::filesystem::is_directory(dir) &&
               std::filesystem::directory_iterator(dir) ==
                       std::filesystem::end(std::filesystem::directory_iterator {});
    } catch (const std::filesystem::filesystem_error&) {
        return true;
    }
}

LocalSpillDataDir::LocalSpillDataDir(std::string path, int64_t capacity_bytes,
                                     TStorageMedium::type storage_medium)
        : SpillDataDir(path, fmt::format("{}/{}", path, SPILL_DIR_PREFIX),
                       fmt::format("{}/{}", path, SPILL_DIR_PREFIX), capacity_bytes,
                       storage_medium) {}

Status LocalSpillDataDir::init() {
    bool exists = false;
    RETURN_IF_ERROR(io::global_local_filesystem()->exists(_path, &exists));
    if (!exists) {
        RETURN_NOT_OK_STATUS_WITH_WARN(Status::IOError("opendir failed, path={}", _path),
                                       "check file exist failed");
    }
    RETURN_IF_ERROR(update_capacity());
    LOG(INFO) << fmt::format(
            "spill storage path: {}, capacity: {}, limit: {}, available: "
            "{}",
            _path, PrettyPrinter::print_bytes(_disk_capacity_bytes),
            PrettyPrinter::print_bytes(_spill_data_limit_bytes),
            PrettyPrinter::print_bytes(_available_bytes));
    return Status::OK();
}

io::FileSystemSPtr LocalSpillDataDir::fs() const {
    return io::global_local_filesystem();
}

std::string LocalSpillDataDir::get_spill_data_gc_path(const std::string& sub_dir_name) const {
    auto dir = fmt::format("{}/{}", _path, SPILL_GC_DIR_PREFIX);
    if (!sub_dir_name.empty()) {
        dir = fmt::format("{}/{}", dir, sub_dir_name);
    }
    return dir;
}

Status LocalSpillDataDir::update_capacity() {
    std::lock_guard<std::mutex> l(_mutex);
    RETURN_IF_ERROR(io::global_local_filesystem()->get_space_info(_path, &_disk_capacity_bytes,
                                                                  &_available_bytes));
    spill_disk_capacity->set_value(_disk_capacity_bytes);
    spill_disk_avail_capacity->set_value(_available_bytes);
    auto disk_use_max_bytes =
            (int64_t)(_disk_capacity_bytes * config::storage_flood_stage_usage_percent / 100);
    bool is_percent = true;
    _spill_data_limit_bytes = ParseUtil::parse_mem_spec(config::spill_storage_limit, -1,
                                                        _disk_capacity_bytes, &is_percent);
    if (_spill_data_limit_bytes <= 0) {
        spill_disk_limit->set_value(_spill_data_limit_bytes);
        auto err_msg = fmt::format("Failed to parse spill storage limit from '{}'",
                                   config::spill_storage_limit);
        LOG(WARNING) << err_msg;
        return Status::InvalidArgument(err_msg);
    }
    if (is_percent) {
        _spill_data_limit_bytes = (int64_t)(_spill_data_limit_bytes *
                                            config::storage_flood_stage_usage_percent / 100);
    }
    _spill_data_limit_bytes = std::min(_spill_data_limit_bytes, disk_use_max_bytes);
    spill_disk_limit->set_value(_spill_data_limit_bytes);

    std::string spill_root_dir = get_spill_data_path();
    std::string spill_gc_root_dir = get_spill_data_gc_path();
    spill_disk_has_spill_data->set_value(is_directory_empty(spill_root_dir) ? 0 : 1);
    spill_disk_has_spill_gc_data->set_value(is_directory_empty(spill_gc_root_dir) ? 0 : 1);

    return Status::OK();
}

bool LocalSpillDataDir::_reach_disk_capacity_limit(int64_t incoming_data_size) {
    double used_pct = get_disk_usage(incoming_data_size);
    int64_t left_bytes = _available_bytes - incoming_data_size;
    if (used_pct >= config::storage_flood_stage_usage_percent / 100.0 &&
        left_bytes <= config::storage_flood_stage_left_capacity_bytes) {
        LOG(WARNING) << "reach capacity limit. used pct: " << used_pct
                     << ", left bytes: " << left_bytes << ", path: " << _path;
        return true;
    }
    return false;
}

bool LocalSpillDataDir::_reach_limit_unlocked(int64_t incoming_data_size) {
    if (_reach_disk_capacity_limit(incoming_data_size)) {
        return true;
    }
    if (_spill_data_bytes + incoming_data_size > _spill_data_limit_bytes) {
        LOG_EVERY_T(WARNING, 1) << fmt::format(
                "spill data reach limit, path: {}, capacity: {}, limit: {}, used: {}, "
                "available: "
                "{}, "
                "incoming "
                "bytes: {}",
                _path, PrettyPrinter::print_bytes(_disk_capacity_bytes),
                PrettyPrinter::print_bytes(_spill_data_limit_bytes),
                PrettyPrinter::print_bytes(_spill_data_bytes),
                PrettyPrinter::print_bytes(_available_bytes),
                PrettyPrinter::print_bytes(incoming_data_size));
        return true;
    }
    return false;
}

} // namespace doris
