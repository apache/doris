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

struct IOContext;

namespace io {

class FileReader {
public:
    FileReader() = default;
    virtual ~FileReader() = default;

    DISALLOW_COPY_AND_ASSIGN(FileReader);

    virtual Status close() = 0;

    virtual Status read_at(size_t offset, Slice result, const IOContext& io_ctx,
                           size_t* bytes_read) = 0;

    virtual const Path& path() const = 0;

    virtual size_t size() const = 0;

    virtual bool closed() const = 0;
};

using FileReaderSPtr = std::shared_ptr<FileReader>;

} // namespace io
} // namespace doris
