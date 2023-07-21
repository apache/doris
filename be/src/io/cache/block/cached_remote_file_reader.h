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

#include <memory>
#include <string>
#include <utility>

#include "common/status.h"
#include "io/cache/block/block_file_cache.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/file_system.h"
#include "io/fs/path.h"
#include "util/slice.h"

namespace doris {
namespace io {
class IOContext;
struct FileCacheStatistics;

class CachedRemoteFileReader final : public FileReader {
public:
    CachedRemoteFileReader(FileReaderSPtr remote_file_reader, const std::string& cache_path,
                           const long modification_time);

    CachedRemoteFileReader(FileReaderSPtr remote_file_reader, const std::string& cache_base_path,
                           const std::string& cache_path, const long modification_time);

    ~CachedRemoteFileReader() override;

    Status close() override;

    const Path& path() const override { return _remote_file_reader->path(); }

    size_t size() const override { return _remote_file_reader->size(); }

    bool closed() const override { return _remote_file_reader->closed(); }

    FileSystemSPtr fs() const override { return _remote_file_reader->fs(); }

    FileReader* get_remote_reader() { return _remote_file_reader.get(); }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const IOContext* io_ctx) override;

private:
    std::pair<size_t, size_t> _align_size(size_t offset, size_t size) const;

    FileReaderSPtr _remote_file_reader;
    IFileCache::Key _cache_key;
    CloudFileCachePtr _cache;

    struct ReadStatistics {
        bool hit_cache = true;
        bool skip_cache = false;
        int64_t bytes_read = 0;
        int64_t bytes_write_into_file_cache = 0;
        int64_t remote_read_timer = 0;
        int64_t local_read_timer = 0;
        int64_t local_write_timer = 0;
    };
    void _update_state(const ReadStatistics& stats, FileCacheStatistics* state) const;
};

} // namespace io
} // namespace doris