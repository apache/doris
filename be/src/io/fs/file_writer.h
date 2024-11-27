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
#include "gutil/macros.h"
#include "io/fs/path.h"
#include "util/debug_points.h"
#include "util/slice.h"

namespace doris {
namespace io {
class FileSystem;

// Only affects remote file writers
struct FileWriterOptions {
    // S3 committer will start multipart uploading all files on BE side,
    // and then complete multipart upload these files on FE side.
    // If you do not complete multi parts of a file, the file will not be visible.
    // So in this way, the atomicity of a single file can be guaranteed. But it still cannot
    // guarantee the atomicity of multiple files.
    // Because hive committers have best-effort semantics,
    // this shortens the inconsistent time window.
    bool used_by_s3_committer = false;
    bool write_file_cache = false;
    bool is_cold_data = false;
    bool sync_file_data = true;        // Whether flush data into storage system
    int64_t file_cache_expiration = 0; // Absolute time
    // Whether to create empty file if no content
    bool create_empty_file = true;
};

class FileWriter {
public:
    // FIXME(plat1ko): FileWriter should be interface
    FileWriter(Path&& path, std::shared_ptr<FileSystem> fs) : _path(std::move(path)), _fs(fs) {}
    FileWriter() = default;
    virtual ~FileWriter() = default;

    DISALLOW_COPY_AND_ASSIGN(FileWriter);

    // Open the file for writing.
    virtual Status open() { return Status::OK(); }

    // Normal close. Wait for all data to persist before returning.
    virtual Status close() = 0;

    Status append(const Slice& data) { return appendv(&data, 1); }

    virtual Status appendv(const Slice* data, size_t data_cnt) = 0;

    // Call this method when there is no more data to write.
    // FIXME(cyx): Does not seem to be an appropriate interface for file system?
    virtual Status finalize() = 0;

    const Path& path() const { return _path; }

    size_t bytes_appended() const { return _bytes_appended; }

    std::shared_ptr<FileSystem> fs() const { return _fs; }

    bool is_closed() { return _closed; }

protected:
    Path _path;
    size_t _bytes_appended = 0;
    std::shared_ptr<FileSystem> _fs;
    bool _closed = false;
    bool _opened = false;
    bool _create_empty_file = true;
};

using FileWriterPtr = std::unique_ptr<FileWriter>;

} // namespace io
} // namespace doris
