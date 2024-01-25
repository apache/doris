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

#include <butil/macros.h>
#include <stddef.h>

#include <memory>

#include "common/status.h"
#include "io/fs/path.h"
#include "util/slice.h"

namespace doris {

namespace io {

class FileSystem;
struct IOContext;

enum class FileCachePolicy : uint8_t {
    NO_CACHE,
    FILE_BLOCK_CACHE,
};

inline FileCachePolicy cache_type_from_string(std::string_view type) {
    if (type == "file_block_cache") {
        return FileCachePolicy::FILE_BLOCK_CACHE;
    } else {
        return FileCachePolicy::NO_CACHE;
    }
}

// Only affects remote file readers
struct FileReaderOptions {
    FileCachePolicy cache_type {FileCachePolicy::NO_CACHE};
    bool is_doris_table = false;
    std::string cache_base_path;
    // Length of the file in bytes, -1 means unset.
    // If the file length is not set, the file length will be fetched from the file system.
    int64_t file_size = -1;
    // Use modification time to determine whether the file is changed
    int64_t mtime = 0;

    static const FileReaderOptions DEFAULT;
};

inline const FileReaderOptions FileReaderOptions::DEFAULT;

class FileReader {
public:
    FileReader() = default;
    virtual ~FileReader() = default;

    DISALLOW_COPY_AND_ASSIGN(FileReader);

    /// If io_ctx is not null,
    /// the caller must ensure that the IOContext exists during the left cycle of read_at()
    Status read_at(size_t offset, Slice result, size_t* bytes_read,
                   const IOContext* io_ctx = nullptr);

    virtual Status close() = 0;

    virtual const Path& path() const = 0;

    virtual size_t size() const = 0;

    virtual bool closed() const = 0;

    virtual std::shared_ptr<FileSystem> fs() const = 0;

protected:
    virtual Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                                const IOContext* io_ctx) = 0;
};

using FileReaderSPtr = std::shared_ptr<FileReader>;

} // namespace io
} // namespace doris
