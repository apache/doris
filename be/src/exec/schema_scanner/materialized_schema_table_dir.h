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

#include "common/status.h"
#include "vec/core/block.h"

namespace doris {
#include "common/compile_check_begin.h"

class MaterializedSchemaTableDir {
public:
    MaterializedSchemaTableDir(std::string path, int64_t capacity_bytes,
                               TStorageMedium::type storage_medium = TStorageMedium::HDD);

    Status init();

    const std::string& path() const { return path_; }

    std::string get_materialized_path(const std::string& name = "") const;

    TStorageMedium::type storage_medium() const { return storage_medium_; }

    // check if the capacity reach the limit after adding the incoming data
    // return true if limit reached, otherwise, return false.
    bool reach_capacity_limit(int64_t incoming_data_size);

    Status update_capacity();

    void update_usage_bytes(int64_t incoming_data_size) {
        std::lock_guard<std::mutex> l(mutex_);
        disk_used_bytes_ += incoming_data_size;
    }

    int64_t get_usage_bytes() {
        std::lock_guard<std::mutex> l(mutex_);
        return disk_used_bytes_;
    }

    int64_t get_disk_limit_bytes() {
        std::lock_guard<std::mutex> l(mutex_);
        return disk_limit_bytes_;
    }

    std::string debug_string();

    bool reach_disk_capacity_limit(int64_t incoming_data_size);
    double get_disk_usage(int64_t incoming_data_size) const {
        return disk_capacity_bytes_ == 0
                       ? 0
                       : (double)(disk_capacity_bytes_ - available_bytes_ + incoming_data_size) /
                                 (double)disk_capacity_bytes_;
    }

private:
    std::string path_;

    // protect disk_capacity_bytes_, available_bytes_, disk_limit_bytes_, disk_used_bytes_
    std::mutex mutex_;
    // the actual capacity of the disk of this data dir
    size_t disk_capacity_bytes_;
    int64_t disk_limit_bytes_ = 0;
    // the actual available capacity of the disk of this data dir
    size_t available_bytes_ = 0;
    int64_t disk_used_bytes_ = 0;
    TStorageMedium::type storage_medium_;
};

#include "common/compile_check_end.h"
} // namespace doris
