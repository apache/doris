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

// bkd_data leaf blocks -- the COLD sub-file of the SNII-native BKD index
// (design 5.2). One block per leaf, self-contained, concatenated in leaf order;
// the bkd_index leaf directory says where each one starts and how many points it
// holds.
//
//   point_count         varint32   points in this leaf; must equal the count the
//                                  leaf directory records, else the block is corrupt
//   value_mode          u8         LeafValueMode
//   common_prefix_len   varint32   0 .. bytes_per_dim
//   common_prefix       bytes[common_prefix_len]
//   --- value area, with S = bytes_per_dim - common_prefix_len ---
//   kAllEqual : nothing. common_prefix_len == bytes_per_dim, i.e. the prefix IS
//               the one value every point in the leaf carries.
//   kRle      : run_count varint32, then run_count x { suffix bytes[S], run_len varint32 }
//   kRaw      : point_count x suffix bytes[S]
//   --- doc ids ---
//   docid_block         one PFOR block (encoding/pfor.h) of point_count uint32 codes
//   docid_block_offset  varint32   block-relative offset of the docid block
//   offset_length       u8         byte length of the varint immediately above
//
// The doc id CODES exploit a property the old implementation had but never used:
// the build-time sort key is (value, doc_id) (see kPointDocIdBytes), so doc ids
// ascend inside every run of equal values. Hence
//   kAllEqual -> one ascending delta run over the whole leaf (code 0 is absolute)
//   kRle      -> deltas restarting at each run (each run's first code is absolute)
//   kRaw      -> the doc ids themselves; values are nearly all distinct there, so
//                runs are length 1 and a delta would only add work
// All three are ONE pfor block of point_count codes, so there is no equivalent of
// the old five-way bpv dispatch (one branch of which was write-never/read-only).
//
// WHY docid_block_offset SITS AT THE TAIL (design 7.2): a whole-leaf hit wants the
// doc ids without paying for the value area, and the value area comes first
// because the only readers that decode values are the at most two boundary leaves
// of a range, while whole-leaf hits can number in the thousands. The trailing
// offset_length byte is what makes the varint reachable from the end: a bare
// LEB128 varint cannot be scanned backwards (the last byte of the doc id block may
// itself have bit 7 set, so the varint's start is ambiguous). Note that a reader
// still has to parse the head for value_mode and point_count, and that a kRle leaf
// additionally has to walk the run lengths its doc id deltas restart on -- the
// offset removes the value BYTES from the whole-leaf path, not the head.
//
// ENCODE AND DECODE ARE TWO INDEPENDENT FREE FUNCTIONS sharing nothing but the
// constants in bkd_format.h (design 4). The old docids_writer declared both
// directions on one class, which is how the writer TU ended up transitively
// including the entire read side.
namespace doris::snii::bkd {

// ---------------------------------------------------------------------------
// Encode
// ---------------------------------------------------------------------------

// Encodes one leaf and APPENDS the block to `sink` (`sink` is not cleared).
//
// `records` is the build-time point array (design 6.2): point_count fixed-width
// records of [value: bytes_per_dim][doc_id: kPointDocIdBytes big-endian], already
// sorted by the memcmp of the whole record, which is exactly (value, doc_id)
// order. This is the builder's own buffer, so no PointRef array is materialized
// per leaf.
//
// Every argument is a BUILD-TIME INVARIANT -- the builder produced all of it in
// this same run -- so violations trip DORIS_CHECK (DCHECK for the per-point ones)
// rather than returning a Status (design 8). Untrusted bytes only ever enter
// through the decode functions below.
//
//   sink        : non-null
//   bytes_per_dim: > 0, and records.size() a whole multiple of the record size
//   point_count : >= 1 (the builder never emits an empty leaf)
//   ordering    : records non-decreasing under memcmp. Equal ADJACENT records are
//                 legal: an array column may repeat one value inside one row, and
//                 that pair encodes as a zero doc id delta.
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

// Decodes one leaf block: values as prefix + runs of suffixes, plus every doc id.
// This is the boundary-leaf path of a range query, the only one that looks at
// values at all.
//
// `block` must be exactly the leaf's bytes, which the reader knows from the leaf
// directory (leaf i + 1's offset, or the bkd_data length for the last leaf).
// `bytes_per_dim` and `expected_point_count` come from the already-validated
// bkd_index: the count both implements design 5.2's "a point_count disagreeing
// with the directory is corruption" rule and bounds the decode allocation -- a
// leaf whose values are all equal spends zero bytes per point, so without that
// bound a damaged point_count could drive a multi-gigabyte resize.
//
// Leaf blocks are read lazily and are NOT covered by the open-time validation, so
// every field here is checked as it is decoded (design 8.3). Disk bytes are not
// invariants: every rejection is Status INVERTED_INDEX_FILE_CORRUPTED, never a
// DORIS_CHECK, so a damaged leaf downgrades the query instead of killing the node.
Status decode_leaf_block(Slice block, uint32_t bytes_per_dim, uint32_t expected_point_count,
                         DecodedLeafBlock* out);

// Decodes ONLY the doc ids, stepping over the value area via the tail
// docid_block_offset. This is the whole-leaf-hit path (design 7.2): every leaf
// strictly between the two boundary leaves of a range contributes all of its doc
// ids and none of its values.
//
// Same arguments, same corruption contract, and the same doc ids as
// decode_leaf_block would produce for the same block -- including the validation
// that the tail offset agrees with where the value area actually ends, so a
// damaged offset cannot silently reinterpret value bytes as doc ids.
Status decode_leaf_doc_ids(Slice block, uint32_t bytes_per_dim, uint32_t expected_point_count,
                           std::vector<uint32_t>* doc_ids);

} // namespace doris::snii::bkd
