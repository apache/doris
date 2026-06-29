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

#include "snii/common/slice.h"
#include "common/status.h"
#include "snii/encoding/byte_sink.h"

namespace snii::format {

// BlockRef.flags bit definitions.
namespace block_ref_flags {
// bit0: the on-disk block bytes are zstd(uncompressed_block). When set, the
// directory also stores uncomp_len, and the reader zstd-decompresses the fetched
// [offset, offset+length) range to uncomp_len before parsing the dict block. The
// block-level crc32c (and BlockRef.checksum) cover the UNCOMPRESSED bytes, so a
// zstd block shrinks the bytes fetched from S3 while keeping the same integrity
// guarantees after decompression in RAM.
inline constexpr uint8_t kZstd = 1u << 0;
} // namespace block_ref_flags

// Physical location and checksum info for a single DICT block. Aligned with SampledTermIndex by ordinal:
// SampledTermIndex[i]'s first_term corresponds to DictBlockDirectory[i] (see design spec
// "sampled dict index"). The read path issues a single range read over [offset, offset+length).
struct BlockRef {
    uint64_t offset = 0;     // absolute byte offset of the block within the container
    uint64_t length = 0;     // ON-DISK byte length of the block (compressed when kZstd)
    uint32_t n_entries = 0;  // number of DictEntry records within this block
    uint8_t flags = 0;       // block-level flags (block_ref_flags::*)
    uint32_t checksum = 0;   // crc32c of the block's UNCOMPRESSED content (verified after read)
    uint64_t uncomp_len = 0; // uncompressed block byte length (stored only when kZstd set)
};

// DICT block directory: block ordinal → physical location mapping.
//
// on-disk layout (framed by SectionFramer with a unified type+len+crc32c wrapper):
//   [u8 type=kDictBlockDirectory][varint64 payload_len][payload][fixed32 crc32c]
//   payload = varint32 n_blocks
//             then n_blocks × block_ref{
//               varint64 offset, varint64 length, varint32 n_entries,
//               u8 flags, fixed32 checksum }
// Section-level crc detects truncation/corruption; block_ref.checksum is the per-block crc.
class DictBlockDirectoryBuilder {
public:
    void add(const BlockRef& ref) { refs_.push_back(ref); }

    // Encodes as a kDictBlockDirectory framed section (with embedded crc32c) and appends to sink.
    void finish(ByteSink* sink) const;

private:
    std::vector<BlockRef> refs_;
};

// Reads and verifies a kDictBlockDirectory framed section; provides ordinal → BlockRef lookup.
// After parsing, all block_refs reside in the reader (entering the searcher cache along with meta).
class DictBlockDirectoryReader {
public:
    // Verifies the section crc and deserializes all block_refs.
    // crc mismatch / truncation / trailing bytes → kCorruption; wrong section type → kInvalidArgument.
    static doris::Status open(Slice section, DictBlockDirectoryReader* out);

    uint32_t n_blocks() const { return static_cast<uint32_t>(refs_.size()); }

    // Returns the ordinal-th block_ref; ordinal >= n_blocks → kNotFound.
    doris::Status get(uint32_t ordinal, BlockRef* out) const;

private:
    std::vector<BlockRef> refs_;
};

} // namespace snii::format
