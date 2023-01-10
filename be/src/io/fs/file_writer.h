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
#include "util/slice.h"

namespace doris {
namespace io {
class FileSystem;

class FileWriter {
public:
    FileWriter(Path&& path) : _path(std::move(path)) {}
    virtual ~FileWriter() = default;

    DISALLOW_COPY_AND_ASSIGN(FileWriter);

    // Normal close. Wait for all data to persist before returning.
    virtual Status close() = 0;

    // Abnormal close and remove this file.
    virtual Status abort() = 0;

    virtual Status append(const Slice& data) = 0;

    virtual Status appendv(const Slice* data, size_t data_cnt) = 0;

    virtual Status write_at(size_t offset, const Slice& data) = 0;

    // Call this method when there is no more data to write.
    // FIXME(cyx): Does not seem to be an appropriate interface for file system?
    virtual Status finalize() = 0;

    virtual size_t bytes_appended() const = 0;

    virtual FileSystem* fs() const = 0;

    const Path& path() const { return _path; }

protected:
    Path _path;
};

using FileWriterPtr = std::unique_ptr<FileWriter>;

} // namespace io
} // namespace doris
