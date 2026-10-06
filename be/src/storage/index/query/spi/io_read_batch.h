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
#include <span>
#include <vector>

#include "common/status.h"
#include "storage/index/query/spi/io_reader.h"

namespace doris::index_query {

// Coalesces ranges for one reader and owns their fetched buffers.
// Registration performs no I/O; returned byte views borrow the fetched buffers.
class IoReadBatch {
public:
    // coalesce_gap: requests separated by a gap <= this many bytes are merged into
    // one physical read (reads a few extra bytes to save a request). 0 merges only
    // overlapping/adjacent ranges.
    explicit IoReadBatch(IoReader* reader, uint64_t coalesce_gap = 0);

    // Registers a desired range; returns a handle usable with get() after fetch().
    size_t add(uint64_t offset, uint64_t len);
    // Adds a range only if the coalesced batch fits both read limits. A rejected
    // range leaves the batch and handle unchanged; no I/O is performed.
    Status try_add(uint64_t offset, uint64_t len, uint64_t max_bytes, size_t max_ranges,
                   bool* accepted, size_t* handle);

    IoReadBatch(const IoReadBatch&) = delete;
    IoReadBatch& operator=(const IoReadBatch&) = delete;

    // Coalesces and issues one batched read; fills internal buffers.
    Status fetch();

    // Bytes for handle h, valid after a successful fetch until the next fetch or clear.
    std::span<const uint8_t> get(size_t h) const;

    IoReader* reader() const { return reader_; }
    // The bytes the last fetch read, held until the next fetch or clear.
    uint64_t fetched_bytes() const;
    size_t pending() const { return reqs_.size(); }
    void clear();

private:
    struct Req {
        uint64_t offset;
        uint64_t len;
        size_t len_size = 0;   // validated size_t length after successful fetch()
        size_t read_index = 0; // index into fetched views after fetch
        size_t sub_offset = 0; // byte offset of this req within its physical read
    };

    Status refresh_bounded_ranges();

    IoReader* reader_;
    uint64_t coalesce_gap_;
    std::vector<Req> reqs_;
    IoReadResult fetched_;
    // Built only for bounded registration; the ordinary add/fetch path stays lazy.
    std::vector<IoRange> bounded_ranges_;
    size_t bounded_requests_ = 0;
    uint64_t bounded_bytes_ = 0;
};

} // namespace doris::index_query
