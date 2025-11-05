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

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <shared_mutex>
#include <utility>
#include <vector>

#include "common/status.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/file_block.h"
#include "io/cache/file_cache_common.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/path.h"
#include "util/slice.h"

namespace doris::io {
struct IOContext;
struct FileCacheStatistics;

class CachedRemoteFileReader final : public FileReader,
                                      public std::enable_shared_from_this<CachedRemoteFileReader> {
public:
    CachedRemoteFileReader(FileReaderSPtr remote_file_reader, const FileReaderOptions& opts);

    ~CachedRemoteFileReader() override;

    Status close() override;

    const Path& path() const override { return _remote_file_reader->path(); }

    size_t size() const override { return _remote_file_reader->size(); }

    bool closed() const override { return _remote_file_reader->closed(); }

    FileReader* get_remote_reader() { return _remote_file_reader.get(); }

    static std::pair<size_t, size_t> s_align_size(size_t offset, size_t size, size_t length);
    
    // Asynchronously prefetch a range of data into cache.
    // This is a fire-and-forget operation that returns immediately (non-blocking).
    // The actual I/O happens asynchronously in the I/O threadpool.
    Status prefetch(size_t offset, size_t size, const IOContext* io_ctx = nullptr);
    
    // Asynchronously prefetch multiple ranges of data into cache (batch version).
    Status prefetch_batch(const std::vector<std::pair<size_t, size_t>>& ranges,
                          const IOContext* io_ctx = nullptr);

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const IOContext* io_ctx) override;

private:
    void _insert_file_reader(FileBlockSPtr file_block);
    
    // Helper to submit a single prefetch task to the I/O threadpool
    Status _submit_prefetch_task(size_t offset, size_t size, IOContext io_ctx);
    
    bool _is_doris_table;
    FileReaderSPtr _remote_file_reader;
    UInt128Wrapper _cache_hash;
    BlockFileCache* _cache;
    std::shared_mutex _mtx;
    std::map<size_t, FileBlockSPtr> _cache_file_readers;

    void _update_stats(const ReadStatistics& stats, FileCacheStatistics* state,
                       bool is_inverted_index) const;

    std::atomic<int> _parallel_ref {0};
    std::mutex _parallel_mtx;
    std::condition_variable _parallel_cv;
};

} // namespace doris::io
