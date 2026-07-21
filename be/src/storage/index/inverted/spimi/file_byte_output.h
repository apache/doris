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
#include <vector>

#include "common/status.h"
#include "io/fs/file_writer.h"
#include "storage/index/inverted/spimi/byte_output.h"

namespace doris::segment_v2::inverted_index::spimi {

// ByteOutput backed by an io::FileWriter. Buffers writes in memory and
// flushes in 64 KB chunks (matching CLucene's IndexOutput buffer size) so
// the integration layer can write directly into Doris segment file writers
// without round-tripping through std::vector first.
//
// The ByteOutput interface does not return Status from each call (it
// mirrors CLucene's signature), so write errors are captured in `_status`
// and made available via `status()` / `Finish()`. Once an error is seen,
// subsequent WriteByte / WriteBytes calls are no-ops (the test harness can
// then assert on `status()` rather than chasing a partial file). Callers
// MUST invoke `Finish()` before checking the file's contents — it flushes
// the buffer and returns the accumulated status.
//
// Ownership: this class does not own the FileWriter; the caller is
// responsible for keeping it alive for the lifetime of the FileByteOutput
// and for calling FileWriter::close() after Finish().
class FileByteOutput final : public ByteOutput {
public:
    static constexpr size_t kDefaultBufferSize = 64 * 1024;

    FileByteOutput(io::FileWriter* file_writer, size_t buffer_size = kDefaultBufferSize);

    ~FileByteOutput() override = default;

    void WriteByte(uint8_t b) override;
    void WriteBytes(const uint8_t* b, size_t len) override;
    int64_t FilePointer() const override { return _file_pointer; }

    // Flushes any buffered bytes to the underlying FileWriter and returns the
    // first non-OK status seen during the writer's lifetime (or OK if none).
    // After Finish(), the FileWriter holds the complete byte stream; callers
    // may then close the file or hand it to other writers.
    Status Finish();

    // First non-OK status observed; useful for tests to assert mid-stream
    // errors without waiting for Finish().
    const Status& status() const { return _status; }

private:
    void FlushBuffer();

    io::FileWriter* _file_writer;
    std::vector<uint8_t> _buffer;
    size_t _buffer_pos = 0;
    int64_t _file_pointer = 0;
    Status _status = Status::OK();
};

} // namespace doris::segment_v2::inverted_index::spimi
