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

#include "common/check.h"
#include "common/status.h"
#include "storage/index/snii/bkd/bkd_types.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"

// bkd_index -- the HOT sub-file of the SNII-native BKD index (design 5.1).
//
// One SectionFramer section (type kBkdIndexSectionType), so the checksum and the
// length envelope come from the framer and nothing here hand-rolls a crc. Payload:
//
//   --- header ---
//   magic            fixed32   kBkdIndexMagic
//   format_version   varint32
//   flags            varint32
//   bytes_per_dim    varint32
//   field_type       varint32
//   point_count      varint64
//   doc_count        varint32
//   leaf_count       varint32
//   points_per_leaf  varint32
//   --- present only when leaf_count > 0 ---
//   min_value        bytes[bytes_per_dim]
//   max_value        bytes[bytes_per_dim]
//   split_values     bytes[(leaf_count - 1) * bytes_per_dim]   ascending, fixed width
//   leaf_offsets     delta-varint64[leaf_count]                strictly increasing
//   leaf_counts      varint32[leaf_count]
//
// leaf_count == 0 is the EMPTY index (design 5.3): header only, and bkd_data has
// length 0. It is a legal state, never corruption -- unlike the old
// implementation's implicit `indexFP == 0` sentinel over an unchecked bkd_meta.
//
// There is no internal node tree. In one dimension an inner node only routes a
// value to a leaf, which is exactly what an ordered array of split values does:
// leaf i covers [split_value(i - 1), split_value(i)). The old recursive packed
// tree existed to carry a split DIMENSION and an FP delta per level; with
// multi-dimensional support out of scope it collapses to a binary-searchable
// fixed-width array (design 5.1).
namespace doris::snii::bkd {

// Serializes the bkd_index payload and APPENDS the framed section to `sink`
// (`sink` is not cleared).
//
// Every argument is a BUILD-TIME INVARIANT -- the builder produced all of it in
// this same run -- so violations are programming errors and trip DORIS_CHECK
// rather than returning a Status (design 8). Untrusted bytes only ever enter
// through BkdIndexBlockReader::open.
//
//   min_value / max_value : exactly bytes_per_dim bytes, empty iff leaf_count == 0
//   split_values          : (leaf_count - 1) * bytes_per_dim bytes, non-decreasing
//   leaves                : leaf_count entries, offsets strictly increasing,
//                           counts summing to header.point_count
void encode_bkd_index_block(const BkdIndexHeader& header, Slice min_value, Slice max_value,
                            Slice split_values, std::span<const LeafRef> leaves, ByteSink* sink);

// Decoded bkd_index. Immutable once open() returns, owns its arrays, and holds no
// cursor -- so one instance can serve concurrent queries with no locking and no
// per-query copy (design 9), unlike the packed index the old reader deep-copied on
// every query.
//
// open() runs the ENTIRE structural validation up front (design 8.2): after it
// succeeds the invariants hold by construction and the query hot path may index
// the arrays without re-checking. Disk bytes are NOT invariants, so every one of
// those checks is a Status, never a DORIS_CHECK -- asserting on them would turn a
// recoverable index downgrade into a node crash.
class BkdIndexBlockReader {
public:
    BkdIndexBlockReader() = default;

    // Parses and fully validates a framed bkd_index section.
    //
    // `data_length` is the byte length of the companion bkd_data sub-file; leaf
    // offsets are bounded against it HERE so leaf reads later need no bound check.
    //
    // format_version above kSupportedVersion -> INVERTED_INDEX_NOT_SUPPORTED: a
    // capability boundary, so the caller reports "index unavailable" instead of a
    // damaged segment. Every other rejection (bad magic, unknown field_type,
    // bytes_per_dim disagreeing with field_type, array lengths disagreeing with
    // leaf_count, unordered split values, non-increasing or out-of-range leaf
    // offsets, leaf counts not summing to point_count, trailing bytes, crc
    // mismatch, truncation) -> INVERTED_INDEX_FILE_CORRUPTED.
    static Status open(Slice framed, uint64_t data_length, BkdIndexBlockReader* out);

    const BkdIndexHeader& header() const { return header_; }
    uint32_t leaf_count() const { return header_.leaf_count; }
    // The empty index (design 5.3). Callers must branch on this before asking for
    // bounds, split values or leaves -- an empty index has none.
    bool empty() const { return header_.leaf_count == 0; }

    // Smallest / largest indexed value, as unsigned big-endian sortable bytes.
    // Once per query (the global-bounds fast reject), hence DORIS_CHECK.
    Slice min_value() const {
        DORIS_CHECK(!empty());
        return Slice(bounds_.data(), header_.bytes_per_dim);
    }
    Slice max_value() const {
        DORIS_CHECK(!empty());
        return Slice(bounds_.data() + header_.bytes_per_dim, header_.bytes_per_dim);
    }

    // Boundary between leaf i and leaf i + 1: leaf i + 1 covers
    // [split_value(i), split_value(i + 1)). i < leaf_count - 1. Read inside the
    // binary search, hence DCHECK.
    Slice split_value(uint32_t i) const {
        DCHECK_LT(static_cast<uint64_t>(i) + 1, header_.leaf_count);
        return Slice(split_values_.data() + static_cast<size_t>(i) * header_.bytes_per_dim,
                     header_.bytes_per_dim);
    }

    // The whole fixed-width split array, for a search that walks it directly
    // instead of going through split_value(). Empty when leaf_count <= 1.
    Slice split_values() const { return Slice(split_values_); }

    LeafRef leaf(uint32_t i) const {
        DCHECK_LT(i, header_.leaf_count);
        return leaves_[i];
    }

    // Resident heap held beyond sizeof(*this). This is the REAL cost -- the arrays
    // are decoded once and never re-materialized -- so a searcher-cache charge
    // built on it does not under-count the way the old ram_bytes_used() did by
    // omitting the packed index entirely.
    size_t heap_bytes() const;

private:
    Status decode_payload(Slice payload, uint64_t data_length);

    BkdIndexHeader header_;
    // min_value followed by max_value, one allocation. Empty for an empty index.
    std::vector<uint8_t> bounds_;
    // (leaf_count - 1) * bytes_per_dim bytes, non-decreasing, fixed width.
    std::vector<uint8_t> split_values_;
    std::vector<LeafRef> leaves_;
};

} // namespace doris::snii::bkd
