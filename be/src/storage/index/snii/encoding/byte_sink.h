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

#include "storage/index/snii/common/slice.h"

namespace doris::snii {

// append-only write cursor: all section serialization goes through this; manual byte assembly is forbidden.
// All multi-byte fixed-width fields are little-endian.
class ByteSink {
public:
    void put_u8(uint8_t v) { buf_.push_back(v); }
    void put_fixed16(uint16_t v);
    void put_fixed32(uint32_t v);
    void put_fixed64(uint64_t v);
    void put_varint32(uint32_t v);
    void put_varint64(uint64_t v);
    void put_zigzag(int64_t v);
    void put_bytes(Slice s);

    size_t size() const { return buf_.size(); }
    const std::vector<uint8_t>& buffer() const { return buf_; }
    Slice view() const { return Slice(buf_); }

    // Reserves capacity for `additional` MORE bytes on top of the current size(),
    // so a caller about to append a known-length run pays at most one reallocation
    // instead of the geometric-growth reallocs a byte-at-a-time put_u8 loop would
    // trigger. The argument is RELATIVE (absolute target = size() + additional): a
    // sink REUSED across encodes (e.g. the shared PFOR-run `out`) keeps accumulating
    // correctly rather than no-op'ing on a repeated per-run reserve of the same
    // value. Affects only the backing buffer's capacity -- the emitted bytes are
    // unchanged.
    void reserve(size_t additional) { buf_.reserve(buf_.size() + additional); }

    // Resets the cursor to empty while RETAINING the backing capacity, so a sink can
    // be reused across many small encodes (e.g. per-window region/prx scratch in the
    // windowed posting builder) without re-allocating each time -- this avoids the
    // cumulative small-allocation churn that fragments the heap arena and inflates
    // peak RSS during the merge of a high-df term split into thousands of windows.
    void clear() { buf_.clear(); }

    // Moves the backing buffer OUT to the caller (the sink is left empty), so an encoded
    // section can be handed off without the copy (+ copy-induced capacity slack) that
    // reading buffer() and copy-assigning would incur. Use only when the sink is not
    // reused afterward (a stack-local about to die, or one that is clear()'d next).
    std::vector<uint8_t> take() { return std::move(buf_); }

private:
    std::vector<uint8_t> buf_;
};

} // namespace doris::snii
