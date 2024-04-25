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
#include "io/fs/file_reader.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/path.h"
#include "util/slice.h"

namespace doris::io {
class FileSystem;
struct FileCacheAllocatorBuilder;

// Only affects remote file writers
struct FileWriterOptions {
    bool write_file_cache = false;
    bool is_cold_data = false;
    bool sync_file_data = true;         // Whether flush data into storage system
    uint64_t file_cache_expiration = 0; // Absolute time
};

class FileWriter {
public:
    FileWriter() = default;
    virtual ~FileWriter() = default;

    FileWriter(const FileWriter&) = delete;
    const FileWriter& operator=(const FileWriter&) = delete;

    // Normal close. Wait for all data to persist before returning.
    // If there is no data appended, an empty file will be persisted.
    virtual Status close() = 0;

    Status append(const Slice& data) { return appendv(&data, 1); }

    virtual Status appendv(const Slice* data, size_t data_cnt) = 0;

    // Call this method when there is no more data to write.
    // FIXME(cyx): Does not seem to be an appropriate interface for file system?
    virtual Status finalize() = 0;

    virtual const Path& path() const = 0;

    virtual size_t bytes_appended() const = 0;

    virtual bool closed() const = 0;

    virtual FileCacheAllocatorBuilder* cache_builder() const = 0;
};

} // namespace doris::io
