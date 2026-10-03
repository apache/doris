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
#include <memory>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/bkd/bkd_index_block.h"
#include "storage/index/snii/bkd/bkd_types.h"
#include "storage/index/snii/bkd/leaf_codec.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/io/file_reader.h"

// Forward-declare the CRoaring C++ bitmap so this header stays free of the
// (large) roaring include, exactly as format/null_bitmap.h does.
namespace roaring {
class Roaring;
} // namespace roaring

// Reads BKD sections using the decoded index and leaf codec.
namespace doris::snii::bkd {

// Caller-owned buffers reused across queries. A new scratch is also valid for any query.
struct BkdQueryScratch {
    // Raw bytes of the leaf currently being examined, as read from bkd_data.
    std::vector<uint8_t> leaf_bytes;
    // The boundary-leaf decode (values + doc ids). Keeps its capacity across
    // leaves.
    DecodedLeafBlock decoded;
    // The whole-leaf-hit decode (doc ids only).
    std::vector<uint32_t> doc_ids;
    // Doc ids gathered across consecutive whole-leaf hits, SORTED before they
    // reach the bitmap. Leaves are ordered by value, so their doc ids arrive in
    // arbitrary order, and roaring inserts sorted input far more cheaply.
    std::vector<uint32_t> gathered;
    // Ping-pong buffer for the radix sort of `gathered`.
    std::vector<uint32_t> radix_scratch;
    // lookup_many's probe values, ordered and deduplicated internally.
    std::vector<Slice> probes;
};

// Holds a validated, immutable bkd_index and a borrowed FileReader for bkd_data. The file must outlive this reader; queries use caller-owned scratch for concurrent access.
class BkdReader {
public:
    // Validates section extents and the full bkd_index before publishing the reader. Unsupported versions return INVERTED_INDEX_NOT_SUPPORTED; malformed data returns INVERTED_INDEX_FILE_CORRUPTED, leaving out unchanged.
    static Status open(io::FileReader* file, const BkdSections& sections,
                       std::unique_ptr<BkdReader>* out);

    ~BkdReader() = default;

    BkdReader(const BkdReader&) = delete;
    BkdReader& operator=(const BkdReader&) = delete;

    // Finds matching doc IDs in one pass; an empty bound is unbounded. Non-empty bounds must be KeyCoder-encoded for the index field type; hits is cleared, and scratch can be reused across calls.
    Status range(Slice lower, bool lower_inclusive, Slice upper, bool upper_inclusive,
                 roaring::Roaring* hits) const;
    Status range(Slice lower, bool lower_inclusive, Slice upper, bool upper_inclusive,
                 roaring::Roaring* hits, BkdQueryScratch* scratch) const;

    // Sorts and deduplicates values, then reads each matching leaf once. Clears hits before collecting the union of matching doc IDs.
    Status lookup_many(const std::vector<Slice>& values, roaring::Roaring* hits) const;
    Status lookup_many(const std::vector<Slice>& values, roaring::Roaring* hits,
                       BkdQueryScratch* scratch) const;

    // Estimates point count from the resident directory without leaf reads. Whole leaves count exactly; each partial boundary leaf contributes half its count, and an impossible range returns zero.
    Status estimate_cardinality(Slice lower, bool lower_inclusive, Slice upper,
                                bool upper_inclusive, uint64_t* out) const;

    // Everything the validated bkd_index header records, including the
    // field_type a caller resolves its KeyCoder from.
    // The file this reader was opened against. Callers that resolved an extent
    // from the SAME container (a blob index's null-bitmap sub-file) must read
    // through THIS reader, not through whatever IndexFileReader they happen to
    // hold: a searcher-cache hit can outlive the IndexFileReader that opened it,
    // and the caller's own may never have been init()-ed.
    io::FileReader* reader() const { return file_; }

    const BkdIndexHeader& header() const { return block_.header(); }

    uint64_t point_count() const { return block_.header().point_count; }
    uint32_t doc_count() const { return block_.header().doc_count; }
    uint32_t leaf_count() const { return block_.leaf_count(); }
    // An empty index has no bounds.
    bool empty() const { return block_.empty(); }

    // Smallest / largest indexed value as sortable bytes. DORIS_CHECKs !empty().
    Slice min_value() const { return block_.min_value(); }
    Slice max_value() const { return block_.max_value(); }

    // Includes this object and its decoded index arrays.
    size_t memory_usage() const { return sizeof(*this) + block_.heap_bytes(); }

private:
    BkdReader(io::FileReader* file, const BkdSections& sections);

    // Reads the leaf extent established by the validated directory.
    Status read_leaf(uint32_t index, std::vector<uint8_t>* buffer) const;

    // Decodes and filters a boundary leaf, which may contain both matching and non-matching values.
    Status scan_boundary_leaf(uint32_t index, Slice lower, bool lower_inclusive, Slice upper,
                              bool upper_inclusive, roaring::Roaring* hits,
                              BkdQueryScratch* scratch) const;

    io::FileReader* const file_;
    const BkdSections sections_;
    // The whole hot sub-file, decoded once. Immutable, owns its arrays, holds no
    // cursor.
    BkdIndexBlockReader block_;
};

} // namespace doris::snii::bkd
