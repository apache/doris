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

#include "common/config.h"
#include "common/status.h"
#include "io/cache/file_cache.h"

namespace doris {
namespace io {

class FileCacheManager {
public:
    FileCacheManager() = default;
    ~FileCacheManager() = default;

    static FileCacheManager* instance();

    void add_file_cache(const Path& cache_path, FileCachePtr file_cache);

    void remove_file_cache(const Path& cache_path);

    void clean_timeout_caches();

    FileCachePtr new_file_cache(const Path& cache_dir, int64_t alive_time_sec,
                                io::FileReaderSPtr remote_file_reader,
                                const std::string& file_cache_type);

private:
    std::shared_mutex _cache_map_lock;
    // cache_path -> FileCache
    std::map<std::string, FileCachePtr> _file_cache_map;
};

} // namespace io
} // namespace doris
