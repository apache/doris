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

#include "common/status.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"

namespace doris::snii::format {

// Fixed-size entry written at the very end of a segment's .idx file. It lets a
// reader locate the tail meta region with a single read of the trailing
// tail_pointer_size() bytes (see design spec "fixed tail pointer").
//
// On-disk layout (all multi-byte fields little-endian, FIXED total size so the
// reader can read exactly the last tail_pointer_size() bytes):
//   [u32 magic = kTailMagic]
//   [u16 format_version = kFormatVersion]
//   [u64 meta_region_offset]
//   [u64 meta_region_length]
//   [u64 hot_off]                  (offset of the hot region [hot_off, EOF);
//                                   0 if absent)
//   [u32 meta_region_checksum]
//   [u32 bootstrap_header_checksum]
//   [u8  tail_pointer_size]        (== tail_pointer_size())
//   [u32 tail_checksum]            (crc32c over all preceding tail-pointer bytes)
//
// The fixed layout deliberately does NOT use the SectionFramer (which is
// variable-length): a footer needs a constant trailing size the reader knows up
// front.
struct TailPointer {
    uint64_t meta_region_offset = 0;
    uint64_t meta_region_length = 0;
    uint64_t hot_off = 0;
    uint32_t meta_region_checksum = 0;
    uint32_t bootstrap_header_checksum = 0;
};

// Constant on-disk size of the tail pointer, so the reader knows how many
// trailing bytes to read.
size_t tail_pointer_size();

// Appends the fixed-layout tail-pointer bytes (magic / version / fields / size /
// tail_checksum) to sink. Returns Internal if the encoded size would not fit the
// fixed-size contract (a programming error, never expected at runtime).
Status encode_tail_pointer(const TailPointer& tp, ByteSink* sink);

// Parses the trailing tail-pointer bytes. last_bytes must be exactly
// tail_pointer_size() bytes long. Verifies magic and tail_checksum, then fills
// out with the parsed fields. Wrong magic / checksum mismatch / wrong length ->
// Corruption.
Status decode_tail_pointer(Slice last_bytes, TailPointer* out);

} // namespace doris::snii::format
