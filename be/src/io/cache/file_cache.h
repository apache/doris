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
#include <shared_mutex>

#include "common/status.h"
#include "io/fs/file_reader.h"
#include "io/fs/path.h"

namespace doris {
namespace io {

const std::string CACHE_DONE_FILE_SUFFIX = "_DONE";

class FileCache : public FileReader {
public:
    FileCache() : _last_match_time(time(nullptr)), _cache_file_size(0) {}
    virtual ~FileCache() = default;

    DISALLOW_COPY_AND_ASSIGN(FileCache);

    virtual const Path& cache_dir() const = 0;

    size_t cache_file_size() const { return _cache_file_size; }

    virtual io::FileReaderSPtr remote_file_reader() const = 0;

    virtual Status clean_timeout_cache() = 0;

    virtual Status clean_all_cache() = 0;

    virtual bool is_dummy_file_cache() { return false; }

    Status download_cache_to_local(const Path& cache_file, const Path& cache_done_file,
                                   io::FileReaderSPtr remote_file_reader, size_t req_size,
                                   size_t offset = 0);

    void update_last_match_time() { _last_match_time = time(nullptr); }
    int64_t get_last_match_time() const { return _last_match_time; }

protected:
    int64_t _last_match_time;
    size_t _cache_file_size;
};

using FileCachePtr = std::shared_ptr<FileCache>;

struct FileCacheLRUComparator {
    bool operator()(const FileCachePtr& lhs, const FileCachePtr& rhs) const {
        return lhs->get_last_match_time() > rhs->get_last_match_time();
    }
};

} // namespace io
} // namespace doris
