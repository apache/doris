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
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "common/status.h"

namespace doris::io {
struct IOContext;
} // namespace doris::io

namespace doris::segment_v2 {

// Where an ANN index keeps its bytes: a few named files, each written once from the front and then
// only read. faiss needs no more of a store than that. It serializes an index as one stream
// (ann.faiss), and IVF_ON_DISK adds a second file whose lists it reads at random offsets while it
// searches (ann.ivfdata). This header is all the ANN code sees of storage; the index file formats
// implement it (V1 and V2 in inverted/clucene/lucene_ann_store.h).

// One file of an index, open for reading.
class AnnIndexInput {
public:
    virtual ~AnnIndexInput() = default;
    virtual uint64_t length() const = 0;
    // Reads exactly `size` bytes, and fails when offset + size passes length(). Not thread-safe: an
    // implementation may hold a cursor and a read-ahead buffer, and it coalesces small reads.
    //
    // The callers depend on that coalescing and do none of their own. faiss::read_index reads an
    // index in thousands of reads of a few bytes each (a fourcc, a dimension, a count), and an
    // IVF_ON_DISK search reads a list's ids right after its codes. With a 4 KiB buffer, a hundred
    // 8-byte header fields cost one read of the file instead of a hundred, and a list of 30 vectors
    // of 32 floats (3840 bytes of codes, then 240 of ids) costs one read instead of two.
    //
    // io_ctx belongs to the caller and to this call: the file cache charges the read to it, and the
    // input keeps no pointer to it afterwards, because a search's context ends with its query while
    // the input lives as long as the loaded index.
    virtual Status read_at(uint64_t offset, void* out, size_t size,
                           const io::IOContext* io_ctx) = 0;
};

// One file of an index, open for writing from the front.
class AnnIndexOutput {
public:
    virtual ~AnnIndexOutput() = default;
    virtual Status append(const void* data, size_t size) = 0;
    // Idempotent: calls after the first do nothing and succeed. An output destroyed while still
    // open closes itself and logs a failure instead of reporting it; that is the cleanup after a
    // failed serialization, whose own error has already been returned.
    virtual Status close() = 0;
};

// The read side: the named files of one index. An input may use what its source holds (an open
// file, a decoded file table), so the source must outlive every input it opened.
class AnnIndexSource {
public:
    virtual ~AnnIndexSource() = default;
    virtual Result<std::unique_ptr<AnnIndexInput>> open_input(std::string_view name) = 0;
    virtual std::string debug_string() const = 0;
};

// The write side: the named files of one index.
class AnnIndexSink {
public:
    virtual ~AnnIndexSink() = default;
    virtual Result<std::unique_ptr<AnnIndexOutput>> create_output(std::string_view name) = 0;
    virtual std::string debug_string() const = 0;
};

} // namespace doris::segment_v2
