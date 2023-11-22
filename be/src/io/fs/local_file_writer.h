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

#include "common/status.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/path.h"
#include "util/slice.h"

namespace doris {
namespace io {

class LocalFileWriter final : public FileWriter {
public:
    LocalFileWriter(Path path, int fd, FileSystemSPtr fs, bool sync_data = true);
    LocalFileWriter(Path path, int fd);
    ~LocalFileWriter() override;

    Status close() override;
    Status abort() override;
    Status appendv(const Slice* data, size_t data_cnt) override;
    Status write_at(size_t offset, const Slice& data) override;
    Status finalize() override;

private:
    Status _close(bool sync);

private:
    int _fd; // owned
    bool _dirty = false;
    const bool _sync_data;
};

} // namespace io
} // namespace doris
