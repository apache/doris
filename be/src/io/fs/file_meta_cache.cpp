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

#include "io/fs/file_meta_cache.h"

#include <algorithm>

#include "util/parse_util.h"

namespace doris {

Status FileMetaCache::create(const std::string& memory_limit, int64_t legacy_capacity,
                             int64_t process_memory_limit, int64_t physical_memory,
                             std::unique_ptr<FileMetaCache>* cache) {
    if (memory_limit.empty()) {
        *cache = std::make_unique<FileMetaCache>(std::max<int64_t>(0, legacy_capacity));
        return Status::OK();
    }
    bool is_percent = false;
    int64_t capacity = ParseUtil::parse_mem_spec(memory_limit, process_memory_limit,
                                                 physical_memory, &is_percent);
    if (capacity < 0 || capacity > process_memory_limit) {
        return Status::InvalidArgument(
                "external_file_meta_cache_memory_limit '{}' must be between 0 and the BE "
                "memory limit ({})",
                memory_limit, process_memory_limit);
    }
    // Existing deployments may explicitly disable the cache with the old count setting.
    if (legacy_capacity <= 0) {
        capacity = 0;
    }
    *cache = std::make_unique<FileMetaCache>(capacity, LRUCacheType::SIZE);
    return Status::OK();
}

std::string FileMetaCache::get_key(const std::string file_name, int64_t modification_time,
                                   int64_t file_size) {
    std::string meta_cache_key;
    meta_cache_key.resize(file_name.size() + sizeof(int64_t));

    memcpy(meta_cache_key.data(), file_name.data(), file_name.size());
    if (modification_time != 0) {
        memcpy(meta_cache_key.data() + file_name.size(), &modification_time, sizeof(int64_t));
    } else {
        memcpy(meta_cache_key.data() + file_name.size(), &file_size, sizeof(int64_t));
    }
    return meta_cache_key;
}

std::string FileMetaCache::get_key(io::FileReaderSPtr file_reader,
                                   const io::FileDescription& _file_description) {
    return FileMetaCache::get_key(
            file_reader->path().native(), _file_description.mtime,
            _file_description.file_size == -1 ? file_reader->size() : _file_description.file_size);
}

} // namespace doris
