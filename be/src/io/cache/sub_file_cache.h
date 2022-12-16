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

class SubFileCache final : public FileCache {
public:
    SubFileCache(const Path& cache_dir, int64_t alive_time_sec,
                 io::FileReaderSPtr remote_file_reader);
    ~SubFileCache() override;

    Status close() override { return _remote_file_reader->close(); }

    Status read_at(size_t offset, Slice result, const IOContext& io_ctx,
                   size_t* bytes_read) override;

    const Path& path() const override { return _remote_file_reader->path(); }

    size_t size() const override { return _remote_file_reader->size(); }

    bool closed() const override { return _remote_file_reader->closed(); }

    const Path& cache_dir() const override { return _cache_dir; }

    io::FileReaderSPtr remote_file_reader() const override { return _remote_file_reader; }

    Status clean_timeout_cache() override;

    Status clean_all_cache() override;

    Status clean_one_cache(size_t* cleaned_size) override;

    int64_t get_oldest_match_time() const override {
        return _gc_lru_queue.empty() ? 0 : _gc_lru_queue.top().last_match_time;
    };

    bool is_gc_finish() const override { return _gc_lru_queue.empty(); }

private:
    Status _generate_cache_reader(size_t offset, size_t req_size);

    Status _clean_cache_internal(size_t offset, size_t* cleaned_size);

    Status _get_need_cache_offsets(size_t offset, size_t req_size,
                                   std::vector<size_t>* cache_offsets);

    size_t _calc_cache_file_size();

    std::pair<Path, Path> _cache_path(size_t offset);

private:
    struct SubFileInfo {
        size_t offset;
        int64_t last_match_time;
    };
    using SubGcQueue = std::priority_queue<SubFileInfo, std::vector<SubFileInfo>,
                                           SubFileLRUComparator<SubFileInfo>>;
    // used by gc thread, and currently has no lock protection
    SubGcQueue _gc_lru_queue;

    Path _cache_dir;
    int64_t _alive_time_sec;
    io::FileReaderSPtr _remote_file_reader;

    std::shared_mutex _cache_map_lock;
    // offset_begin -> last_match_time
    std::map<size_t, int64_t> _last_match_times;
    // offset_begin -> local file reader
    std::map<size_t, io::FileReaderSPtr> _cache_file_readers;
};

} // namespace io
} // namespace doris
