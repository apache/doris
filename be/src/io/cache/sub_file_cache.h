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

#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <map>
#include <memory>
#include <queue>
#include <shared_mutex>
#include <utility>
#include <vector>

#include "common/status.h"
#include "io/cache/file_cache.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/file_system.h"
#include "io/fs/path.h"
#include "util/slice.h"

namespace doris {
namespace io {
class IOContext;

class SubFileCache final : public FileCache {
public:
    SubFileCache(const Path& cache_dir, int64_t alive_time_sec,
                 io::FileReaderSPtr remote_file_reader);
    ~SubFileCache() override;

    Status close() override { return _remote_file_reader->close(); }

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
    }

    bool is_gc_finish() const override { return _gc_lru_queue.empty(); }

    FileSystemSPtr fs() const override { return _remote_file_reader->fs(); }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const IOContext* io_ctx) override;

private:
    Status _generate_cache_reader(size_t offset, size_t req_size);

    Status _clean_cache_internal(size_t offset, size_t* cleaned_size);

    Status _get_need_cache_offsets(size_t offset, size_t req_size,
                                   std::vector<size_t>* cache_offsets);

    std::pair<Path, Path> _cache_path(size_t offset);

    Status _init();

    Status _get_all_sub_file_size(std::map<int64_t, int64_t>* expect_file_size_map);

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

    bool _is_inited = false;
};

} // namespace io
} // namespace doris
