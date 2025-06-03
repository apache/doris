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

#pragma once

#include <string>

#include "exec/schema_scanner/materialized_schema_table.h"
#include "vec/core/block.h"

namespace doris {
#include "common/compile_check_begin.h"

class MaterializedSchemaTableDir {
public:
    MaterializedSchemaTableDir(std::string path, int64_t capacity_bytes,
                    TStorageMedium::type storage_medium = TStorageMedium::HDD);

    Status init();

    const std::string& path() const { return _path; }

    std::string get_materialized_path(const std::string& name = "") const;

    TStorageMedium::type storage_medium() const { return _storage_medium; }

    // check if the capacity reach the limit after adding the incoming data
    // return true if limit reached, otherwise, return false.
    bool reach_capacity_limit(int64_t incoming_data_size);

    Status update_capacity();

    void update_usage_bytes(int64_t incoming_data_size) {
        std::lock_guard<std::mutex> l(_mutex);
        _disk_used_bytes += incoming_data_size;
    }

    int64_t get_usage_bytes() {
        std::lock_guard<std::mutex> l(_mutex);
        return _disk_used_bytes;
    }

    int64_t get_disk_limit_bytes() {
        std::lock_guard<std::mutex> l(_mutex);
        return _disk_limit_bytes;
    }

    std::string debug_string();

private:
    bool _reach_disk_capacity_limit(int64_t incoming_data_size);
    double _get_disk_usage(int64_t incoming_data_size) const {
        return _disk_capacity_bytes == 0
                        ? 0
                        : (double)(_disk_capacity_bytes - _available_bytes + incoming_data_size) /
                                    (double)_disk_capacity_bytes;
    }

    friend class MaterializedSchemaTableMgr;
    std::string _path;

    // protect _disk_capacity_bytes, _available_bytes, _disk_limit_bytes, _disk_used_bytes
    std::mutex _mutex;
    // the actual capacity of the disk of this data dir
    size_t _disk_capacity_bytes;
    int64_t _disk_limit_bytes = 0;
    // the actual available capacity of the disk of this data dir
    size_t _available_bytes = 0;
    int64_t _disk_used_bytes = 0;
    TStorageMedium::type _storage_medium;
};

class MaterializedSchemaTableMgr {
public:
    static MaterializedSchemaTableMgr* instance() {
        return ExecEnv::GetInstance()->materialized_schema_table_mgr();
    }

    static std::string schema_table_spill_path(TSchemaTableType::type type) {
        switch (type) {
        case TSchemaTableType::SCH_BACKEND_ACTIVE_TASKS:
            return "BackendActiveTasks";
        case TSchemaTableType::SCH_PROCESSLIST:
            return "ProcessList";
        default:
            throw Exception(Status::FatalError("not match type of schema table_ spill :{}",
                                               static_cast<int>(type)));
        }
        throw Exception(Status::FatalError("__builtin_unreachable"));
    }

    ~MaterializedSchemaTableMgr() = default;
    MaterializedSchemaTableMgr(std::unordered_map<std::string, std::unique_ptr<MaterializedSchemaTableDir>>&& materialized_schema_table_store_map);

    Status init();

    void put_block(TSchemaTableType::type type, const std::shared_ptr<vectorized::Block>& block) {
        // 写入 uint64_t timestamp, 
    }

    Status get_reader(TSchemaTableType::type type) {

    }

private:
    std::unordered_map<TSchemaTableType::type, MaterializedSchemaTable> materialized_schema_tables_;
    std::unordered_map<std::string, std::unique_ptr<MaterializedSchemaTableDir>> materialized_schema_table_store_map_;
};

#include "common/compile_check_end.h"
} // namespace doris
