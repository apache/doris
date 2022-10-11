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

#include <future>
#include <memory>

#include "common/status.h"
#include "io/cache/file_cache.h"
#include "io/fs/path.h"

namespace doris {
namespace io {

// Only used for GC
class DummyFileCache final : public FileCache {
public:
    DummyFileCache(const Path& cache_dir, int64_t alive_time_sec);

    ~DummyFileCache() override;

    Status close() override { return Status::OK(); }

    Status read_at(size_t offset, Slice result, size_t* bytes_read) override {
        return Status::NotSupported("dummy file cache only used for GC");
    }

    const Path& path() const override { return _cache_dir; }

    size_t size() const override { return 0; }

    bool closed() const override { return true; }

    const Path& cache_dir() const override { return _cache_dir; }

    io::FileReaderSPtr remote_file_reader() const override { return nullptr; }

    Status clean_timeout_cache() override;

    Status clean_all_cache() override;

    Status load_and_clean();

    bool is_dummy_file_cache() override { return true; }

private:
    Status _clean_unfinished_cache();
    void _update_last_mtime(const Path& done_file);
    void _add_file_cache(const Path& data_file);
    void _load();
    Status _clean_cache_internal();

private:
    Path _cache_dir;
    int64_t _alive_time_sec;

    std::map<Path, int64_t> _file_sizes;
    std::list<Path> _unfinished_files;
};

} // namespace io
} // namespace doris
