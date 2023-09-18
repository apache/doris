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

#include <stddef.h>
#include <stdint.h>

#include <memory>

#include "common/status.h"
#include "exec/line_reader.h"
#include "io/fs/file_reader_writer_fwd.h"

namespace doris {
class PDataRow;

namespace io {
struct IOContext;
} // namespace io

// only used for FORMAT_PROTO type, which used for insert
// transaction(begin/insert into/commit)
class NewPlainBinaryLineReader : public LineReader {
    ENABLE_FACTORY_CREATOR(NewPlainBinaryLineReader);

public:
    NewPlainBinaryLineReader(io::FileReaderSPtr file_reader);

    ~NewPlainBinaryLineReader() override;

    Status read_line(const uint8_t** ptr, size_t* size, bool* eof,
                     const io::IOContext* io_ctx) override;

    void close() override;

private:
    io::FileReaderSPtr _file_reader;

    std::unique_ptr<uint8_t[]> _file_buf;
    std::unique_ptr<PDataRow> _cur_row;
};

} // namespace doris
