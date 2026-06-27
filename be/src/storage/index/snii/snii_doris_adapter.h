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
#include <vector>

#include "common/status.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_writer.h"
#include "io/io_common.h"
#include "snii/common/status.h"
#include "snii/io/file_reader.h"
#include "snii/io/file_writer.h"
#include "util/slice.h"

namespace doris::segment_v2::snii_doris {

Status to_doris_status(const ::snii::Status& status);
::snii::Status to_snii_status(const Status& status);

class DorisSniiFileWriter final : public ::snii::io::FileWriter {
public:
    explicit DorisSniiFileWriter(io::FileWriter* writer) : _writer(writer) {}

    ::snii::Status append(::snii::Slice data) override;
    ::snii::Status finalize() override;
    uint64_t bytes_written() const override;

private:
    io::FileWriter* _writer = nullptr;
};

class DorisSniiFileReader final : public ::snii::io::FileReader {
public:
    class ScopedIOContext {
    public:
        explicit ScopedIOContext(const io::IOContext* io_ctx);
        ~ScopedIOContext();

        ScopedIOContext(const ScopedIOContext&) = delete;
        ScopedIOContext& operator=(const ScopedIOContext&) = delete;

    private:
        const io::IOContext* _previous = nullptr;
        io::IOContext _io_ctx;
    };

    explicit DorisSniiFileReader(io::FileReaderSPtr reader, const io::IOContext* io_ctx = nullptr);

    ::snii::Status read_at(uint64_t offset, size_t len, std::vector<uint8_t>* const out) override;
    ::snii::Status read_batch(const std::vector<::snii::io::Range>& ranges,
                              std::vector<std::vector<uint8_t>>* const outs) override;
    uint64_t size() const override;

private:
    static io::IOContext _make_index_io_context(const io::IOContext* io_ctx);
    ::snii::Status _check_read_range(uint64_t offset, size_t len) const;
    ::snii::Status _read_at(uint64_t offset, size_t len, std::vector<uint8_t>* const out) const;
    const io::IOContext* _current_io_ctx() const;
    void _record_read_stats(int64_t request_bytes, int64_t read_bytes, int64_t range_read_count,
                            int64_t serial_read_rounds) const;

    io::FileReaderSPtr _reader;
    io::IOContext _default_io_ctx;
    static thread_local const io::IOContext* _scoped_io_ctx;
};

} // namespace doris::segment_v2::snii_doris
