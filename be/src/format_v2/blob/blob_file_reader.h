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
#include <optional>

#include "common/status.h"
#include "core/pod_array.h"
#include "io/fs/file_reader.h"

namespace doris::format {

// Physical Paimon managed-BLOB format, version 1. Row numbers are local to this file;
// table row ids, field ids, projection, and SQL FILE values belong to the caller.
class BlobFileReader {
public:
    struct ByteRange {
        // Absolute byte offset of the payload in this .blob file, excluding record framing.
        int64_t offset;
        // Payload bytes only; zero is a valid non-NULL empty value.
        int64_t size;
    };

    // Read and decode the footer/index only. Data access requires a successful open().
    Status open(io::FileReaderSPtr reader, int64_t expected_rows, const io::IOContext* io_ctx);
    // row is zero-based within this .blob file. SQL NULL returns nullopt, while an empty
    // non-NULL value returns a ByteRange with size 0. Descriptor reads need only this index.
    Status get_range(int64_t row, std::optional<ByteRange>* range) const;
    // Validate record framing and CRC, then copy the payload into the caller's tracked buffer.
    // range must be an unmodified non-NULL range returned by get_range() after this reader's
    // most recent successful open().
    // The buffer capacity is reused across calls; descriptor mode never calls this method.
    Status read_payload(const ByteRange& range, PODArray<char>* bytes,
                        const io::IOContext* io_ctx) const;

private:
    Status _read_exact(int64_t offset, char* data, size_t size, const io::IOContext* io_ctx) const;

    io::FileReaderSPtr _reader;
    // Decoded index retained for this file, using tracked memory like the temporary buffers.
    // Entries follow file row order; a negative offset represents SQL NULL.
    PODArray<ByteRange> _ranges;
};

} // namespace doris::format
