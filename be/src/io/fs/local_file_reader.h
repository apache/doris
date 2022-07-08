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

#include "io/fs/file_reader.h"
#include "io/fs/path.h"
#include "util/file_cache.h"

namespace doris {
namespace io {

class LocalFileReader final : public FileReader {
public:
    LocalFileReader(Path path, size_t file_size,
                    std::shared_ptr<OpenedFileHandle<int>> file_handle);

    ~LocalFileReader() override;

    Status close() override;

    Status read_at(size_t offset, Slice result, size_t* bytes_read) override;

    const Path& path() const override { return _path; }

    size_t size() const override { return _file_size; }

private:
    std::shared_ptr<OpenedFileHandle<int>> _file_handle;
    int _fd; // ref
    Path _path;
    size_t _file_size;

    std::atomic_bool _closed;
};

} // namespace io
} // namespace doris
