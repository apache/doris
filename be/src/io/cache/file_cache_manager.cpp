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

#include "io/cache/file_cache_manager.h"

#include "io/cache/sub_file_cache.h"
#include "io/cache/whole_file_cache.h"

namespace doris {
namespace io {

void FileCacheManager::add_file_cache(const Path& cache_path, FileCachePtr file_cache) {
    std::lock_guard<std::shared_mutex> wrlock(_cache_map_lock);
    _file_cache_map.emplace(cache_path.native(), file_cache);
}

void FileCacheManager::remove_file_cache(const Path& cache_path) {
    std::lock_guard<std::shared_mutex> wrlock(_cache_map_lock);
    _file_cache_map.erase(cache_path.native());
}

void FileCacheManager::clean_timeout_caches() {
    std::shared_lock<std::shared_mutex> rdlock(_cache_map_lock);
    for (std::map<std::string, FileCachePtr>::const_iterator iter = _file_cache_map.cbegin();
         iter != _file_cache_map.cend(); ++iter) {
        iter->second->clean_timeout_cache();
    }
}

FileCachePtr FileCacheManager::new_file_cache(const Path& cache_dir, int64_t alive_time_sec,
                                              io::FileReaderSPtr remote_file_reader,
                                              const std::string& file_cache_type) {
    if (file_cache_type == "whole_file_cache") {
        return std::make_unique<WholeFileCache>(cache_dir, alive_time_sec, remote_file_reader);
    } else if (file_cache_type == "sub_file_cache") {
        return std::make_unique<SubFileCache>(cache_dir, alive_time_sec, remote_file_reader);
    } else {
        return nullptr;
    }
}

FileCacheManager* FileCacheManager::instance() {
    static FileCacheManager cache_manager;
    return &cache_manager;
}

} // namespace io
} // namespace doris
