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

#include "common/status.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"

// Forward-declare the CRoaring C++ bitmap so this header stays free of the
// (large) roaring include; the concrete type is only needed in the .cpp.
namespace roaring {
class Roaring;
} // namespace roaring

namespace doris::snii::format {

// SectionFramer type byte for the null-bitmap POD. There is no dedicated
// SectionType enum value yet, so we use a documented literal (0x20) outside the
// currently allocated enum range (1..9) to avoid colliding with existing types.
inline constexpr uint8_t kNullBitmapSectionType = 0x20;

// NullBitmap POD: per logical index, a Roaring bitmap of null docids (docs whose
// value is NULL / not indexed). It decouples per-doc NULL information from the
// per-term dictionary / postings so NULL handling can pull only this side POD.
//
// On-disk layout (the whole section is framed by SectionFramer, which adds a
// type + varint64 len + payload + fixed32 crc32c envelope):
//   framer payload = [varint64 doc_count][varint64 roaring_size][roaring_bytes]
// roaring_bytes is the portable CRoaring serialization (Roaring::write).
class NullBitmapWriter {
public:
    NullBitmapWriter();
    ~NullBitmapWriter();

    NullBitmapWriter(const NullBitmapWriter&) = delete;
    NullBitmapWriter& operator=(const NullBitmapWriter&) = delete;

    // Marks docid as NULL (adding the same docid twice is idempotent).
    void add_null(uint32_t docid);

    // Number of distinct null docids accumulated so far.
    uint32_t null_count() const;

    // Serializes [doc_count][roaring_size][roaring_bytes] framed by SectionFramer
    // and appends it to sink (does not clear sink). doc_count is the total number
    // of docs in the logical index (recorded so the reader can round-trip it).
    void finish(uint32_t doc_count, ByteSink* sink) const;

private:
    std::unique_ptr<roaring::Roaring> bitmap_;
};

// Read-only view: on open, SectionFramer verifies the CRC and truncation; this
// class then guards roaring_size against the remaining payload bytes before
// deserializing the Roaring bitmap (anti-DoS), so a corrupt size cannot trigger
// an oversized allocation/read. is_null() is then an O(1) membership test.
class NullBitmapReader {
public:
    NullBitmapReader();
    ~NullBitmapReader();

    NullBitmapReader(const NullBitmapReader&) = delete;
    NullBitmapReader& operator=(const NullBitmapReader&) = delete;
    NullBitmapReader(NullBitmapReader&&) noexcept;
    NullBitmapReader& operator=(NullBitmapReader&&) noexcept;

    // Parses the entire section (framer envelope + payload). Returns Corruption on
    // CRC mismatch, truncation, doc_count overflow, or an oversized roaring_size.
    static Status open(Slice framed, NullBitmapReader* out);

    // True iff docid was marked NULL. docids outside the null set (including those
    // >= doc_count) return false.
    bool is_null(uint32_t docid) const;

    // Number of distinct null docids in the bitmap.
    uint32_t null_count() const;

    // Copies the decoded bitmap into the caller-owned Roaring object.
    void copy_to(roaring::Roaring* out) const;

    // Total doc count of the logical index, as recorded by the writer.
    uint32_t doc_count() const { return doc_count_; }

private:
    std::unique_ptr<roaring::Roaring> bitmap_;
    uint32_t doc_count_ = 0;
};

} // namespace doris::snii::format
