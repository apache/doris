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
#include <string>
#include <vector>

#include "common/status.h"
#include "snii/common/slice.h"
#include "snii/io/file_reader.h"
#include "snii/io/file_writer.h"

namespace snii::io {

// Local-filesystem FileReader. Uses pread for positional, thread-safe reads
// (so concurrent batch fetches do not contend on a shared file offset).
class LocalFileReader : public FileReader {
public:
    LocalFileReader() = default;
    ~LocalFileReader() override;

    LocalFileReader(const LocalFileReader&) = delete;
    LocalFileReader& operator=(const LocalFileReader&) = delete;

    doris::Status open(const std::string& path);
    doris::Status read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) override;
    uint64_t size() const override { return size_; }

private:
    int fd_ = -1;
    uint64_t size_ = 0;
};

// Local-filesystem append-only FileWriter. Appends accumulate in a fixed
// userspace buffer and are flushed to the fd in large chunks, collapsing the
// many tiny per-append ::write() syscalls of the build path (e.g. ~53k writes
// averaging ~683 B each) into a handful of big writes. The produced file is
// byte-identical to the unbuffered path; only the syscall count drops.
class LocalFileWriter : public FileWriter {
public:
    LocalFileWriter() = default;
    ~LocalFileWriter() override;

    LocalFileWriter(const LocalFileWriter&) = delete;
    LocalFileWriter& operator=(const LocalFileWriter&) = delete;

    doris::Status open(const std::string& path);
    doris::Status append(Slice data) override;
    doris::Status finalize() override;
    uint64_t bytes_written() const override { return bytes_written_; }

private:
    // Userspace write buffer size. 256 KiB amortizes the write() syscall cost over
    // many appends while keeping transient RAM negligible vs the index sections.
    static constexpr size_t kBufCapacity = 256u * 1024;

    // Flushes the userspace buffer to the fd with a robust partial-write loop.
    doris::Status flush_buffer();
    // Writes a raw byte span straight to the fd (used for spans larger than the
    // buffer, bypassing a needless copy).
    doris::Status write_all(const uint8_t* data, size_t len);

    int fd_ = -1;
    uint64_t bytes_written_ = 0;
    std::vector<uint8_t> buf_;
};

} // namespace snii::io
