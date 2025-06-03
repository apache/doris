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

#include "exec/schema_scanner/materialized_schema_table_mgr.h"
#include "util/parse_util.h"

namespace doris {
#include "common/compile_check_begin.h"

MaterializedSchemaTableDir::MaterializedSchemaTableDir(std::string path, int64_t capacity_bytes,
    TStorageMedium::type storage_medium)
    : _path(std::move(path)),
    _disk_capacity_bytes(capacity_bytes),
    _storage_medium(storage_medium) {
}

Status MaterializedSchemaTableDir::init() {
    bool exists = false;
    RETURN_IF_ERROR(io::global_local_filesystem()->exists(_path, &exists));
    if (!exists) {
        RETURN_NOT_OK_STATUS_WITH_WARN(Status::IOError("opendir failed, path={}", _path),
                                       "check file exist failed");
    }
    RETURN_IF_ERROR(update_capacity());
    LOG(INFO) << fmt::format(
            "materialized schema table storage path: {}, capacity: {}, limit: {}, available: "
            "{}",
            _path, PrettyPrinter::print_bytes(_disk_capacity_bytes),
            PrettyPrinter::print_bytes(_disk_limit_bytes),
            PrettyPrinter::print_bytes(_available_bytes));
    return Status::OK();
}

std::string MaterializedSchemaTableDir::get_materialized_path(const std::string& name) const {
    auto dir = fmt::format("{}/{}", _path, MATERIALIZED_SCHEMA_TABLE_DIR_PREFIX);
    if (!name.empty()) {
        dir = fmt::format("{}/{}", dir, name);
    }
    return dir;
}

Status MaterializedSchemaTableDir::update_capacity() {
    std::lock_guard<std::mutex> l(_mutex);
    RETURN_IF_ERROR(io::global_local_filesystem()->get_space_info(_path, &_disk_capacity_bytes,
                                                                  &_available_bytes));
    auto disk_use_max_bytes =
            (int64_t)(_disk_capacity_bytes * config::storage_flood_stage_usage_percent / 100);
    bool is_percent = true;
    _disk_limit_bytes = ParseUtil::parse_mem_spec(config::materialized_schema_table_storage_limit, -1,
                                                        _disk_capacity_bytes, &is_percent);
    if (_disk_limit_bytes <= 0) {
        auto err_msg = fmt::format("Failed to parse materialized_schema_table_storage_limit from '{}'",
                                   config::materialized_schema_table_storage_limit);
        LOG(WARNING) << err_msg;
        return Status::InvalidArgument(err_msg);
    }
    if (_disk_limit_bytes > disk_use_max_bytes) {
        _disk_limit_bytes = disk_use_max_bytes;
    }

    return Status::OK();
}

bool MaterializedSchemaTableDir::_reach_disk_capacity_limit(int64_t incoming_data_size) {
    double used_pct = _get_disk_usage(incoming_data_size);
    int64_t left_bytes = _available_bytes - incoming_data_size;
    if (used_pct >= config::storage_flood_stage_usage_percent / 100.0 &&
        left_bytes <= config::storage_flood_stage_left_capacity_bytes) {
        LOG(WARNING) << "reach capacity limit. used pct: " << used_pct
                     << ", left bytes: " << left_bytes << ", path: " << _path;
        return true;
    }
    return false;
}

bool MaterializedSchemaTableDir::reach_capacity_limit(int64_t incoming_data_size) {
    std::lock_guard<std::mutex> l(_mutex);
    if (_reach_disk_capacity_limit(incoming_data_size)) {
        return true;
    }
    if (_disk_used_bytes + incoming_data_size > _disk_limit_bytes) {
        LOG_EVERY_T(WARNING, 1) << fmt::format(
                "MaterializedSchemaTableDir reach limit, path: {}, capacity: {}, limit: {}, used: {}, available: "
                "{}, incoming bytes: {}",
                _path, PrettyPrinter::print_bytes(_disk_capacity_bytes),
                PrettyPrinter::print_bytes(_disk_limit_bytes),
                PrettyPrinter::print_bytes(_disk_used_bytes),
                PrettyPrinter::print_bytes(_available_bytes),
                PrettyPrinter::print_bytes(incoming_data_size));
        return true;
    }
    return false;
}

MaterializedSchemaTableMgr::MaterializedSchemaTableMgr(std::unordered_map<std::string, std::unique_ptr<MaterializedSchemaTableDir>>&& materialized_schema_table_store_map) : materialized_schema_table_store_map_(std::move(materialized_schema_table_store_map)) {
    // materialized_schema_tables_[TSchemaTableType::SCH_BACKEND_ACTIVE_TASKS] =
    //         MaterializedSchemaTable(TSchemaTableType::SCH_BACKEND_ACTIVE_TASKS,
    //                                 std::make_unique<vectorized::SpillDataDir>(
    //                                     "BackendActiveTasks", spill_path.capacity_bytes,
    //                                     spill_path.storage_medium),
    //                                 config::schema_table_spill_ttl_s);

    // materialized_schema_tables_[TSchemaTableType::SCH_PROCESSLIST] =
    //         MaterializedSchemaTable(TSchemaTableType::SCH_PROCESSLIST,
    //                                 ExecEnv::GetInstance()->get_spill_data_dir(),
    //                                 config::schema_table_spill_ttl_s);
}

Status MaterializedSchemaTableMgr::init() {
    LOG(INFO) << "init MaterializedSchemaTableMgr";
    for (const auto& store : materialized_schema_table_store_map_) {
        RETURN_IF_ERROR(store.second->init());
    }

    for (const auto& [path, store] : materialized_schema_table_store_map_) {
        auto root_dir = store->get_materialized_path();
        bool exists = true;
        RETURN_IF_ERROR(io::global_local_filesystem()->exists(root_dir, &exists));
        if (!exists) {
            RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(root_dir));
        }
    }
    return Status::OK();
}

} // namespace doris
