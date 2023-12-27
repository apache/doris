
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

#include <cstddef>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/factory_creator.h"
#include "common/status.h"

namespace doris {
class WalDirInfo {
    ENABLE_FACTORY_CREATOR(WalDirInfo);

public:
    WalDirInfo(std::string wal_dir, size_t limit, size_t used, size_t pre_allocated)
            : _wal_dir(std::move(wal_dir)),
              _limit(limit),
              _used(used),
              _pre_allocated(pre_allocated) {}
    std::string get_wal_dir();
    size_t get_limit();
    size_t get_used();
    size_t get_pre_allocated();
    Status set_limit(size_t limit);
    Status set_used(size_t used);
    Status set_pre_allocated(size_t pre_allocated, bool is_add_pre_allocated);
    size_t available();
    Status update_wal_dir_limit(size_t limit = -1);
    Status update_wal_dir_used(size_t used = -1);
    Status update_wal_dir_pre_allocated(size_t pre_allocated, bool is_add_pre_allocated = true);

private:
    std::string _wal_dir;
    size_t _limit;
    size_t _used;
    size_t _pre_allocated;
    std::shared_mutex _lock;
};

class WalDirsInfo {
    ENABLE_FACTORY_CREATOR(WalDirsInfo);

public:
    WalDirsInfo() = default;
    ~WalDirsInfo() = default;
    Status add(const std::string& wal_dir, size_t limit, size_t used, size_t pre_allocated);
    Status clear();
    std::string get_available_random_wal_dir();
    size_t get_max_available_size();
    Status update_wal_dir_limit(std::string wal_dir, size_t limit = -1);
    Status update_all_wal_dir_limit();
    Status update_wal_dir_used(std::string wal_dir, size_t used = -1);
    Status update_all_wal_dir_used();
    Status update_wal_dir_pre_allocated(std::string wal_dir, size_t pre_allocated,
                                        bool is_add_pre_allocated);
    Status get_wal_dir_available_size(const std::string& wal_dir, size_t* available_bytes);

private:
    std::vector<std::shared_ptr<WalDirInfo>> _wal_dirs_info_vec;
    std::shared_mutex _lock;
};

} // namespace doris