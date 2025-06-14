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

#include "exec/schema_scanner/materialized_schema_table_dir.h"

#include <memory>

#include "io/fs/file_system.h"
#include "io/fs/local_file_system.h"
#include "util/parse_util.h"

namespace doris {
#include "common/compile_check_begin.h"

MaterializedSchemaTableDir::MaterializedSchemaTableDir(std::string path, int64_t capacity_bytes,
                                                       TStorageMedium::type storage_medium)
        : path_(std::move(path)),
          disk_capacity_bytes_(capacity_bytes),
          storage_medium_(storage_medium) {}

Status MaterializedSchemaTableDir::init() {
    bool exists = false;
    RETURN_IF_ERROR(io::global_local_filesystem()->exists(path_, &exists));
    if (!exists) {
        RETURN_NOT_OK_STATUS_WITH_WARN(Status::IOError("opendir failed, path={}", path_),
                                       "check file exist failed");
    }
    RETURN_IF_ERROR(update_capacity());
    LOG(INFO) << fmt::format(
            "materialized schema table storage path: {}, capacity: {}, limit: {}, available: "
            "{}",
            path_, PrettyPrinter::print_bytes(disk_capacity_bytes_),
            PrettyPrinter::print_bytes(disk_limit_bytes_),
            PrettyPrinter::print_bytes(available_bytes_));
    return Status::OK();
}

std::string MaterializedSchemaTableDir::get_materialized_path(const std::string& name) const {
    auto dir = fmt::format("{}/{}", path_, MATERIALIZED_SCHEMA_TABLE_DIR_PREFIX);
    if (!name.empty()) {
        dir = fmt::format("{}/{}", dir, name);
    }
    return dir;
}

Status MaterializedSchemaTableDir::update_capacity() {
    std::lock_guard<std::mutex> l(mutex_);
    RETURN_IF_ERROR(io::global_local_filesystem()->get_space_info(path_, &disk_capacity_bytes_,
                                                                  &available_bytes_));
    auto disk_use_max_bytes =
            (int64_t)(disk_capacity_bytes_ * config::storage_flood_stage_usage_percent / 100);
    bool is_percent = true;
    disk_limit_bytes_ = ParseUtil::parse_mem_spec(config::materialized_schema_table_storage_limit,
                                                  -1, disk_capacity_bytes_, &is_percent);
    if (disk_limit_bytes_ <= 0) {
        auto err_msg =
                fmt::format("Failed to parse materialized_schema_table_storage_limit from '{}'",
                            config::materialized_schema_table_storage_limit);
        LOG(WARNING) << err_msg;
        return Status::InvalidArgument(err_msg);
    }
    if (disk_limit_bytes_ > disk_use_max_bytes) {
        disk_limit_bytes_ = disk_use_max_bytes;
    }

    return Status::OK();
}

bool MaterializedSchemaTableDir::reach_disk_capacity_limit(int64_t incoming_data_size) {
    double used_pct = get_disk_usage(incoming_data_size);
    int64_t left_bytes = available_bytes_ - incoming_data_size;
    if (used_pct >= config::storage_flood_stage_usage_percent / 100.0 &&
        left_bytes <= config::storage_flood_stage_left_capacity_bytes) {
        LOG(WARNING) << "reach capacity limit. used pct: " << used_pct
                     << ", left bytes: " << left_bytes << ", path: " << path_;
        return true;
    }
    return false;
}

bool MaterializedSchemaTableDir::reach_capacity_limit(int64_t incoming_data_size) {
    std::lock_guard<std::mutex> l(mutex_);
    if (reach_disk_capacity_limit(incoming_data_size)) {
        return true;
    }
    if (disk_used_bytes_ + incoming_data_size > disk_limit_bytes_) {
        LOG_EVERY_T(WARNING, 1) << fmt::format(
                "MaterializedSchemaTableDir reach limit, path: {}, capacity: {}, limit: {}, used: "
                "{}, available: "
                "{}, incoming bytes: {}",
                path_, PrettyPrinter::print_bytes(disk_capacity_bytes_),
                PrettyPrinter::print_bytes(disk_limit_bytes_),
                PrettyPrinter::print_bytes(disk_used_bytes_),
                PrettyPrinter::print_bytes(available_bytes_),
                PrettyPrinter::print_bytes(incoming_data_size));
        return true;
    }
    return false;
}

std::string MaterializedSchemaTableDir::debug_string() {
    return fmt::format(
            "path: {}, capacity: {}, limit: {}, used: {}, available: "
            "{}",
            path_, PrettyPrinter::print_bytes(disk_capacity_bytes_),
            PrettyPrinter::print_bytes(disk_limit_bytes_),
            PrettyPrinter::print_bytes(disk_used_bytes_),
            PrettyPrinter::print_bytes(available_bytes_));
}

} // namespace doris
