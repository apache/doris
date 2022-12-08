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
#include <queue>
#include <shared_mutex>
#include <string>

#include "common/status.h"
#include "io/fs/file_reader.h"
#include "io/fs/path.h"

namespace doris {
namespace io {

const std::string CACHE_DONE_FILE_SUFFIX = "_DONE";

class FileCache : public FileReader {
public:
    FileCache() : _cache_file_size(0) {}
    ~FileCache() override = default;

    DISALLOW_COPY_AND_ASSIGN(FileCache);

    virtual const Path& cache_dir() const = 0;

    size_t cache_file_size() const { return _cache_file_size; }

    virtual io::FileReaderSPtr remote_file_reader() const = 0;

    virtual Status clean_timeout_cache() = 0;

    virtual Status clean_all_cache() = 0;

    virtual Status clean_one_cache(size_t* cleaned_size) = 0;

    bool is_gc_finish() { return _gc_lru_queue.empty(); }

    virtual bool is_dummy_file_cache() { return false; }

    Status download_cache_to_local(const Path& cache_file, const Path& cache_done_file,
                                   io::FileReaderSPtr remote_file_reader, size_t req_size,
                                   size_t offset = 0);

    int64_t get_oldest_match_time() const {
        return _gc_lru_queue.empty() ? 0 : _gc_lru_queue.top().last_match_time;
    };

protected:
    Status _remove_file(const Path& cache_file, const Path& cache_done_file, size_t* cleaned_size);

    struct CacheFileInfo {
        Path file;
        size_t offset;
        int64_t last_match_time;
    };

    struct CacheFileLRUComparator {
        bool operator()(const CacheFileInfo& lhs, const CacheFileInfo& rhs) const {
            return lhs.last_match_time > rhs.last_match_time;
        }
    };

    size_t _cache_file_size;

    using GcQueue =
            std::priority_queue<CacheFileInfo, std::vector<CacheFileInfo>, CacheFileLRUComparator>;
    GcQueue _gc_lru_queue;
};

using FileCachePtr = std::shared_ptr<FileCache>;

struct FileCacheLRUComparator {
    bool operator()(const FileCachePtr& lhs, const FileCachePtr& rhs) const {
        return lhs->get_oldest_match_time() > rhs->get_oldest_match_time();
    }
};

} // namespace io
} // namespace doris
