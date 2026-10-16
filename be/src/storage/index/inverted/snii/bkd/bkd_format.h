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
#include <string_view>

// On-disk contract constants for the SNII-native one-dimensional BKD numeric
// index. This header is CONSTANTS ONLY -- no logic, no includes beyond the
// standard library -- so both the write side and the read side can depend on it
// without pulling each other in (in the third-party implementation this replaces,
// the writer TU transitively included the whole read side through its shared
// doc-id codec header).
//
// Once published these values are format semantics: changing any of them
// requires bumping kFormatVersion and defining a compatibility policy. The
// format deliberately does NOT interoperate with the third-party BKD bytes;
// V1/V2/V3 tables keep using the old implementation and SNII tables use this one.
//
// Structural integers use the SNII encoding vocabulary (little-endian
// put_fixed*, LEB128 put_varint*, see snii/encoding/byte_sink.h). Point VALUES
// are exempt: they stay unsigned big-endian sortable bytes as produced by
// KeyCoder::full_encode_ascending, because every BKD comparison is a byte-wise
// unsigned compare from offset 0 (INV-1). The two are fully decoupled -- values
// only ever travel through put_bytes / memcpy, never through an integer codec.
namespace doris::snii::bkd {

// ---- bkd_index header magic / version ----
// The big-endian reading of the ASCII bytes "BKD1" ('B'=0x42 'K'=0x4B 'D'=0x44
// '1'=0x31). Written with ByteSink::put_fixed32, i.e. little-endian on disk;
// what is pinned is this numeric value, which is all a reader compares. (Note
// this differs from format::kContainerMagic, whose constant is spelled as the
// little-endian reading of "SNII" -- the two are independent magics and neither
// convention constrains the other.)
inline constexpr uint32_t kBkdIndexMagic = 0x424B4431U;

// Version written by this binary.
inline constexpr uint32_t kFormatVersion = 1;
// Highest version this binary can read. A file above it is a CAPABILITY
// boundary, not corruption: readers must return INVERTED_INDEX_NOT_SUPPORTED so
// the caller falls back to "index unavailable" rather than reporting a damaged
// segment.
inline constexpr uint32_t kSupportedVersion = 1;

// ---- SectionFramer type byte for the whole bkd_index payload ----
// The framer type byte is one flat namespace across the container, and there is
// no shared SectionType enum for blob logical indexes, so -- exactly as
// format::kNullBitmapSectionType (0x20) does -- this is a documented literal
// picked outside the ranges already taken by the inverted-index sections
// (format::SectionType, currently 1..14) and the null-bitmap POD (0x20).
// Framing the payload is what gives bkd_index its checksum; no section here
// hand-rolls a crc.
inline constexpr uint8_t kBkdIndexSectionType = 0x30;

// ---- Leaf block value encoding (bkd_data, one byte per leaf) ----
// A closed, disjoint, exhaustive 3-value enum, replacing the old
// `-1 / -2 / sorted_dim` encoding in which -1 conflated "all values equal" with
// "the common prefix covers the whole value", and sorted_dim burned a byte to
// carry a value that is always 0 in one dimension.
enum class LeafValueMode : uint8_t {
    // Every point in the leaf has the same value; the common prefix IS the
    // value and no suffix data follows.
    kAllEqual = 0,
    // Run-length: run_count varint32, then run_count pairs of
    // { suffix bytes[S], run_len varint32 }.
    kRle = 1,
    // point_count fixed-width suffixes of S = bytes_per_dim - common_prefix_len
    // bytes each.
    kRaw = 2,
};
// Largest legal value_mode byte. A leaf decoder reads an untrusted byte from
// disk and must reject anything above this as INVERTED_INDEX_FILE_CORRUPTED.
inline constexpr LeafValueMode kMaxLeafValueMode = LeafValueMode::kRaw;

// ---- bkd_index header `flags` bits ----
namespace index_flags {
// The build spilled at least one run to disk instead of finishing entirely in
// the resident buffer. DIAGNOSTIC ONLY: the produced bytes are identical either
// way, so no read path may branch on it.
inline constexpr uint32_t kBuiltWithSpill = 1U << 0;
// bit1-31 reserved.
} // namespace index_flags

// ---- Blob sub-file names in the SNII named-file table ----
// Two files, not the old implementation's three: bkd_meta is folded into the
// bkd_index header, where it gains a magic, a version and a framer checksum.
// bkd_index is HOT (read in full at open and kept resident); bkd_data is COLD
// (one positioned read per touched leaf).
inline constexpr std::string_view kBkdIndexFileName = "bkd_index";
inline constexpr std::string_view kBkdDataFileName = "bkd_data";

// ---- Build-time parameters (NOT format semantics) ----
// A reader derives everything it needs from the header, so these may be retuned
// against real measurements without a version bump.

// Width of the doc_id tail of a build-time point record
// ([value: bytes_per_dim][doc_id: 4 bytes BIG-endian]). Big-endian is
// deliberate: a memcmp of the whole record then equals lexicographic
// (value, doc_id) order, so the sorter and the merger never split the record
// into fields and the sort key is (value, doc_id) by construction -- which is
// also what makes docids ascending inside a single-value run.
inline constexpr uint32_t kPointDocIdBytes = 4;

// Points a leaf holds unless the caller overrides it.
//
// 128, not the 1024 inherited from the third-party implementation this
// replaces. Measured at 2M BIGINT points (the BKD comparison benchmark under
// be/test/.../bench, 2026-08-02): 1024 was never calibrated, and is the worst
// reasonable choice for selective queries -- a boundary leaf is scanned in full
// however few values match, so narrow-range cost rises monotonically with leaf
// size (12x from 128 to 4096). It also costs index size: 128 emits 8.73 MB
// against 1024's 9.34 MB, cutting the size regression against the baseline
// implementation from +15.5% to +7.9%.
//
// The size curve is NOT monotonic -- 256 is the largest of the six values
// swept -- because below roughly 256 the per-leaf prefix compression gains
// more than the larger leaf directory and split array cost.
//
// The trade is wide ranges: 136 ms at 128 against 111 ms at 1024, +23%. That
// is the deliberate call -- selective predicates and index size are the common
// case, and a range wide enough to feel the difference is already reading a
// million rows.
inline constexpr uint32_t kDefaultPointsPerLeaf = 128;

// Hard ceiling on the recorded points_per_leaf, enforced at open. This is not a
// tuning knob: points_per_leaf is the ONLY quantity that bounds a leaf's count,
// and a leaf's count is what sizes the doc id vector during leaf decode. Left
// unbounded, a self-consistent but hostile bkd_index (inflate point_count and
// the leaf counts together and the sum identity still holds) would drive an
// arbitrarily large allocation from a ~25-byte leaf block, and the resulting
// bad_alloc would escape a module that has no catch anywhere -- turning a
// recoverable downgrade into a node crash, which is exactly what design 8
// exists to prevent. 1 Mi points caps one leaf's doc id vector at 4 MiB.
inline constexpr uint32_t kMaxPointsPerLeaf = 1U << 20;

// Resident point-buffer ceiling before a run is sorted and spilled. Bounds
// build-time RSS independently of the segment's row count.
inline constexpr uint64_t kDefaultBuildBufferBytes = 256ULL << 20;

// Smallest read window a merge cursor is given, in records. It sets the merge's
// FAN-IN: a pass folds at most build_buffer_bytes / (record_size x this) runs at
// once, and a build with more runs than that folds them in groups over several
// passes rather than opening every run at once.
//
// Without a fan-in cap the "one record per cursor" floor makes a single merge
// hold run_count records, i.e. total_points / max_points -- a footprint that
// GROWS with the input, which is exactly what the build_buffer_bytes ceiling
// exists to prevent. 32 records is small enough that a real 256 MiB buffer
// still folds ~700k runs in one pass (so multi-pass never triggers in
// practice), and large enough that a cursor read is not one record per syscall.
inline constexpr uint32_t kMinMergeCursorRecords = 32;

} // namespace doris::snii::bkd
