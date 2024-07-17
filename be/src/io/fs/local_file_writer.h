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
#include "io/fs/file_writer.h"
#include "io/fs/path.h"
#include "util/slice.h"

namespace doris::io {
struct FileCacheAllocatorBuilder;
class LocalFileWriter final : public FileWriter {
public:
    LocalFileWriter(Path path, int fd, bool sync_data = true);
    ~LocalFileWriter() override;

    Status appendv(const Slice* data, size_t data_cnt) override;
    const Path& path() const override { return _path; }
    size_t bytes_appended() const override;
    State state() const override { return _state; }

    FileCacheAllocatorBuilder* cache_builder() const override { return nullptr; }

    Status close(bool non_block = false) override;

private:
    Status _finalize();
    void _abort();
    Status _close(bool sync);

    Path _path;
    int _fd; // owned
    bool _dirty = false;
    const bool _sync_data = true;
    size_t _bytes_appended = 0;
    State _state {State::OPENED};
};

} // namespace doris::io
