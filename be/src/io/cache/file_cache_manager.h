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

#include <memory>

#include "common/status.h"

namespace doris {
namespace io {

class FileCacheManager {
public:
    FileCacheManager() = default;
    ~FileCacheManager() = default;

    static FileCacheManager* instance() {
        return _file_cache_manager;
    }

    void add_file_cache(const std::string& cache_path, FileCache* file_cache) { }

    void remove_file_cache(const std::string& cache_path) { }

    void clean_timeout_caches() {}
private:
    static FileCacheManager* _file_cache_manager;

    std::shared_mutex _cache_map_mtx;
    // cache_path -> FileCache
    std::map<std::string, FileCache*> _file_cache_map;
};


} // namespace io
} // namespace doris
