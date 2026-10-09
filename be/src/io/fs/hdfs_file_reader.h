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

#include <atomic>
#include <memory>
#include <string>

#include "common/status.h"
#include "io/fs/file_handle_cache.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_system.h"
#include "io/fs/hdfs.h"
#include "io/fs/hdfs_file_system.h"
#include "io/fs/path.h"
#include "util/slice.h"

namespace doris::io {
struct IOContext;

class HdfsFileReader final : public FileReader {
public:
    // Accepted path format:
    // - fs_name/path_to_file
    // - /path_to_file
    // TODO(plat1ko): Support related path for cloud mode
    static Result<FileReaderSPtr> create(Path path, const hdfsFS& fs, std::string fs_name,
                                         const FileReaderOptions& opts);

    HdfsFileReader(Path path, std::string fs_name, FileHandleCache::Accessor accessor,
                   int64_t mtime = 0);

    ~HdfsFileReader() override;

    Status close() override;

    const Path& path() const override { return _path; }

    size_t size() const override { return _handle->file_size(); }

    bool closed() const override { return _closed.load(std::memory_order_acquire); }

    int64_t mtime() const override { return _mtime; }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const IOContext* io_ctx) override;

    Status do_read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                           const IOContext* io_ctx);

private:
    Path _path;
    std::string _fs_name;
    FileHandleCache::Accessor _accessor;
    CachedHdfsFileHandle* _handle = nullptr; // owned by _cached_file_handle
    std::atomic<bool> _closed = false;
    int64_t _mtime;
};
} // namespace doris::io
