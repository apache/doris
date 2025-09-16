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

#include <cstdint>
#include <memory>

#include "arrow/io/interfaces.h"
#include "common/factory_creator.h"
#include "io/fs/file_reader.h"

namespace doris {
namespace io {
class FileSystem;
struct IOContext;
} // namespace io

namespace vectorized {
#include "common/compile_check_begin.h"
class ArrowPipInputStream : public arrow::io::InputStream {
    ENABLE_FACTORY_CREATOR(ArrowPipInputStream);

public:
    ArrowPipInputStream(io::FileReaderSPtr file_reader);
    ~ArrowPipInputStream() override = default;

    arrow::Status Close() override;
    bool closed() const override;

    arrow::Result<int64_t> Tell() const override;

    arrow::Result<int64_t> Read(int64_t nbytes, void* out) override;

    arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override;

    Status HasNext(bool* get);

private:
    io::FileReaderSPtr _file_reader;
    int64_t _pos;
    bool _begin;
    // The read buf is very small, so use stack memory directly.
    uint8_t _read_buf[4];
};

} // namespace vectorized
#include "common/compile_check_end.h"
} // namespace doris
