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

#include "common/status.h"
#include "storage/index/snii/bkd/bkd_format.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"

// bkd_data contains self-contained leaf blocks in directory order:
//
//   point_count         varint32
//   value_mode          u8
//   common_prefix_len   varint32
//   common_prefix       bytes[common_prefix_len]
//   value_area          kAllEqual: empty; kRle: runs; kRaw: point suffixes
//   docid_block         PFOR block of point_count codes
//   docid_block_offset  varint32
//   offset_length       u8
//
// Doc ID deltas restart for each equal-value run; kRaw stores absolute IDs. The tail offset lets fully covered leaves read doc IDs without decoding value bytes.
namespace doris::snii::bkd {

// ---------------------------------------------------------------------------
// Encode
// ---------------------------------------------------------------------------

// Appends one leaf from sorted [value][big-endian doc ID] records to sink. Arguments are builder invariants; duplicate records are valid.
void encode_leaf_block(Slice records, uint32_t bytes_per_dim, ByteSink* sink);

// ---------------------------------------------------------------------------
// Decode
// ---------------------------------------------------------------------------

// One maximal run of equal values inside a decoded leaf. kRle stores runs
// explicitly; kAllEqual is one run over the whole leaf; kRaw reports one run per
// point (its values are nearly all distinct, and merging the occasional pair would
// cost a memcmp per point on the boundary-leaf path to save nothing).
struct LeafValueRun {
    // The run's value is common_prefix ++ suffix. A VIEW into the block bytes
    // passed to the decoder -- it does not own them. Empty exactly when the common
    // prefix already covers the whole value (kAllEqual).
    Slice suffix;
    // Index of the run's first point in the leaf. doc_ids[first_point,
    // first_point + count) belong to this run and are non-decreasing.
    uint32_t first_point = 0;
    uint32_t count = 0;
};

// A decoded leaf. Reused across leaves by the query path: the vectors keep their
// capacity, so a scan over many leaves does not re-allocate per leaf.
//
// The Slices are VIEWS into the block handed to decode_leaf_block and are only
// valid while those bytes are.
struct DecodedLeafBlock {
    // Set LAST, so a failed decode leaves it 0 and the partially filled arrays
    // below are unusable scratch rather than plausible-looking data.
    uint32_t point_count = 0;
    LeafValueMode value_mode = LeafValueMode::kAllEqual;
    // common_prefix_len bytes shared by every value in the leaf.
    Slice common_prefix;
    // bytes_per_dim - common_prefix.size(); the width of every suffix in `runs`.
    uint32_t suffix_width = 0;
    // Ascending by value, covering [0, point_count) with no gap and no overlap.
    std::vector<LeafValueRun> runs;
    // point_count doc ids in point order (i.e. in (value, doc_id) order).
    std::vector<uint32_t> doc_ids;

    void clear() {
        point_count = 0;
        value_mode = LeafValueMode::kAllEqual;
        common_prefix = Slice();
        suffix_width = 0;
        runs.clear();
        doc_ids.clear();
    }
};

// Decodes values and doc IDs from one boundary leaf. Checks disk bytes and the validated directory's expected_point_count before allocation, returning corruption on malformed data.
Status decode_leaf_block(Slice block, uint32_t bytes_per_dim, uint32_t expected_point_count,
                         DecodedLeafBlock* out);

// Decodes doc IDs from a fully covered leaf using the tail offset. Validates the offset and returns the same IDs as decode_leaf_block.
Status decode_leaf_doc_ids(Slice block, uint32_t bytes_per_dim, uint32_t expected_point_count,
                           std::vector<uint32_t>* doc_ids);

} // namespace doris::snii::bkd
