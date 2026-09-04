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

#include <cstdint>
#include <mutex>
#include <string>

#include "common/status.h"

struct LanceDataset;
struct LanceSession;

namespace doris::format::lance {

// Owns the single Lance session shared by all queries in one BE process. The session always owns
// Lance's metadata/index caches and optionally installs the Foyer data-file cache. Readers only
// open datasets through this class and do not depend on the selected data-cache implementation.
class LanceSessionManager final {
public:
    struct Config {
        int64_t lance_index_cache_size_bytes = 0;
        int64_t lance_metadata_cache_size_bytes = 0;
        bool enable_lance_data_cache = false;
        std::string lance_data_cache_path;
        int64_t lance_data_cache_disk_capacity_bytes = 0;
        int64_t lance_data_cache_read_block_size_bytes = 0;
    };

    static LanceSessionManager& instance();

    // The explicit configuration constructor keeps the process-global config out of focused
    // manager tests. Production readers use instance().
    explicit LanceSessionManager(Config config);
    ~LanceSessionManager();

    LanceSessionManager(const LanceSessionManager&) = delete;
    LanceSessionManager& operator=(const LanceSessionManager&) = delete;

    Status open_dataset(const char* uri, const char* const* storage_options, uint64_t version,
                        LanceDataset** dataset);

private:
    Status _initialize();

    Config _config;
    std::once_flag _initialize_once;
    LanceSession* _session = nullptr;
    Status _initialize_status = Status::OK();
};

} // namespace doris::format::lance
