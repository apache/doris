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
#include <cstring>
#include <span>
#include <vector>

#include "common/check.h"
#include "common/status.h"
#include "storage/index/query/spi/io_metrics.h"

namespace doris::index_query {

// One logical read request (offset, length).
struct IoRange {
    uint64_t offset = 0;
    size_t len = 0;
};

// Owns physical read buffers and exposes their bytes in request order.
struct IoReadResult {
    IoReadResult() = default;
    IoReadResult(const IoReadResult&) = delete;
    IoReadResult& operator=(const IoReadResult&) = delete;
    IoReadResult(IoReadResult&&) = default;
    IoReadResult& operator=(IoReadResult&&) = default;

    void clear() {
        views.clear();
        buffers.clear();
    }

    std::vector<std::vector<uint8_t>> buffers;
    std::vector<std::span<const uint8_t>> views;
};

// Provides exact byte reads independently of the index format.
class IoReader {
public:
    virtual ~IoReader() = default;

    // Reads exactly len bytes starting at offset into *out (which is resized to
    // len). Reading past EOF is an error (Corruption/IoError).
    virtual Status read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) = 0;

    // Fills caller-owned memory. Override this to avoid the default temporary buffer.
    virtual Status read_into(uint64_t offset, uint8_t* out, size_t out_len) {
        if (out_len == 0) {
            return Status::OK();
        }
        if (out == nullptr) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "read_into: null output buffer");
        }
        std::vector<uint8_t> scratch;
        RETURN_IF_ERROR(read_at(offset, out_len, &scratch));
        // read_at must fill the requested length on success.
        DORIS_CHECK_EQ(scratch.size(), out_len);
        std::memcpy(out, scratch.data(), out_len);
        return Status::OK();
    }

    // Implementations may read ranges concurrently; the default reads them sequentially.
    virtual Status read_batch(const std::vector<IoRange>& ranges, IoReadResult* outs) {
        outs->clear();
        outs->buffers.resize(ranges.size());
        outs->views.resize(ranges.size());
        for (size_t i = 0; i < ranges.size(); ++i) {
            RETURN_IF_ERROR(read_at(ranges[i].offset, ranges[i].len, &outs->buffers[i]));
            outs->views[i] = outs->buffers[i];
        }
        return Status::OK();
    }

    // Total size of the underlying object in bytes.
    virtual uint64_t size() const = 0;

    // Optional live metrics. Readers that do not account I/O return nullptr.
    virtual const IoMetrics* io_metrics() const { return nullptr; }
};

} // namespace doris::index_query
