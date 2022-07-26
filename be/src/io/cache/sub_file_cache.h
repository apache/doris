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
#include "io/cache/file_cache.h"
#include "io/fs/path.h"

namespace doris {
namespace io {

class SubFileCache final : public FileCache {
public:
    SubFileCache(const Path& cache_file_path, int64_t alive_time_sec);
    ~SubFileCache() override;

    Status read_at(size_t offset, Slice result, size_t* bytes_read) override;

    const Path& cache_file_path() const override {
        return _cache_file_path;
    }

    size_t cache_file_size() const override {
        return _cache_file_size;
    }

    Status clean_timeout_cache() override;

    Status clean_all_cache() override;
private:
    Path _cache_file_path;
    size_t _cache_file_size;
    int64_t _alive_time_sec;

    std::shared_mutex _cache_map_mtx;
    std::map<std::string, int64_t> _last_match_times;
    std::map<std::string, std::shared_ptr<LocalFileReader>> _cache_file_readers;
};

} // namespace io
} // namespace doris
