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

#include <list>
#include <memory>
#include <queue>

#include "common/config.h"
#include "common/status.h"
#include "io/cache/file_cache.h"

namespace doris {
namespace io {

class GCContextPerDisk {
public:
    GCContextPerDisk() : _conf_max_size(0), _used_size(0) {}
    void init(const std::string& path, int64_t max_size);
    bool try_add_file_cache(FileCachePtr cache, int64_t file_size);
    void get_gc_file_caches(std::list<FileCachePtr>&);

private:
    std::string _disk_path;
    int64_t _conf_max_size;
    int64_t _used_size;
    std::priority_queue<FileCachePtr, std::vector<FileCachePtr>, FileCacheLRUComparator> _lru_queue;
};

class FileCacheManager {
public:
    FileCacheManager() = default;
    ~FileCacheManager() = default;

    static FileCacheManager* instance();

    void add_file_cache(const std::string& cache_path, FileCachePtr file_cache);

    void remove_file_cache(const std::string& cache_path);

    void gc_file_caches();

    FileCachePtr new_file_cache(const std::string& cache_dir, int64_t alive_time_sec,
                                io::FileReaderSPtr remote_file_reader,
                                const std::string& file_cache_type);

    bool exist(const std::string& cache_path);

private:
    void _gc_unused_file_caches(std::list<FileCachePtr>& result);
    void _add_file_cache_for_gc_by_disk(std::vector<GCContextPerDisk>& contexts,
                                        FileCachePtr file_cache);

private:
    std::shared_mutex _cache_map_lock;
    // cache_path -> FileCache
    std::map<std::string, FileCachePtr> _file_cache_map;
};

} // namespace io
} // namespace doris
