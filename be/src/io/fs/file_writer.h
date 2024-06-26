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
#include "gutil/macros.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/path.h"
#include "util/slice.h"

namespace doris::io {
class FileSystem;
struct FileCacheAllocatorBuilder;

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
    bool sync_file_data = true;         // Whether flush data into storage system
    uint64_t file_cache_expiration = 0; // Absolute time
};

struct AsyncCloseStatusPack {
    std::promise<Status> promise;
    std::future<Status> future;
};

class FileWriter {
public:
    enum class State : uint8_t {
        OPENED = 0,
        ASYNC_CLOSING,
        CLOSED,
    };
    FileWriter() = default;
    virtual ~FileWriter() = default;

    FileWriter(const FileWriter&) = delete;
    const FileWriter& operator=(const FileWriter&) = delete;

    // Normal close. Wait for all data to persist before returning.
    // If there is no data appended, an empty file will be persisted.
    virtual Status close(bool non_block = false) = 0;

    Status append(const Slice& data) { return appendv(&data, 1); }

    virtual Status appendv(const Slice* data, size_t data_cnt) = 0;

    virtual const Path& path() const = 0;

    virtual size_t bytes_appended() const = 0;

    virtual State state() const = 0;

    virtual FileCacheAllocatorBuilder* cache_builder() const = 0;
};

} // namespace doris::io
