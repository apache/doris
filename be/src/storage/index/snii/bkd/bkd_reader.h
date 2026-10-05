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

// Read side of the SNII-native BKD index (design 7). This header includes only
// the DECODE half of the shared leaf codec plus the already-decoded bkd_index;
// nothing from bkd_builder.h reaches it (design 4).
namespace doris::snii::bkd {

// Per-query working buffers, owned by the CALLER.
//
// Design 9 targets zero per-query heap allocation: a caller that runs many
// queries keeps one of these alive and every leaf read, every decoded leaf and
// every doc id array lands in buffers that are already the right size. The
// reader itself stays immutable and stateless, which is what makes it shareable
// across concurrent queries with no locking -- the state has to live somewhere,
// and this is that somewhere.
//
// A fresh scratch is always valid; reuse is an optimization, never a
// correctness requirement, and nothing carried over from a previous query can
// affect the next one's answer.
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

// An opened BKD index: the whole hot bkd_index resident and validated, plus the
// FileReader the cold bkd_data leaves are read from.
//
// IMMUTABLE AFTER open(). Every query method is const and keeps all of its state
// on the stack or in the caller's BkdQueryScratch, so one instance serves
// concurrent queries with no locking, no clone() and no per-query copy of
// anything (design 9). The old reader needed clone() because IndexInput carries a
// cursor, deep-copied its packed index on every query, and wrote a shared field
// from query threads; positioned reads plus an immutable directory remove all
// three by construction.
//
// Lifetime: `file` is borrowed and must outlive the reader. There is no
// reference counting and no Directory -- the SNII segment reader owns the file
// and strictly outlives every index opened over it (D2).
class BkdReader {
public:
    // Reads bkd_index in full, validates it (BkdIndexBlockReader::open runs the
    // entire structural check, design 8.2), and publishes the reader.
    //
    // `sections` comes from the container's named-file table, i.e. from disk, so
    // an extent that does not fit the file is damage to REPORT, not an invariant
    // to assert -- and it is rejected before its length is handed to a read.
    //
    // A bkd_index above kSupportedVersion comes back as
    // INVERTED_INDEX_NOT_SUPPORTED; every other rejection is
    // INVERTED_INDEX_FILE_CORRUPTED. `*out` is left untouched on failure.
    //
    // An index with zero points opens successfully (design 5.3): emptiness is a
    // legal state that answers queries with an empty bitmap, NOT an error the
    // adapter has to translate.
    static Status open(io::FileReader* file, const BkdSections& sections,
                       std::unique_ptr<BkdReader>* out);

    ~BkdReader() = default;

    BkdReader(const BkdReader&) = delete;
    BkdReader& operator=(const BkdReader&) = delete;

    // The single range primitive (design 7.1). An EMPTY `lower` / `upper` Slice
    // means that side is unbounded, so <, <=, >, >= and BETWEEN are all one call
    // and one pass. A non-empty bound must be exactly bytes_per_dim unsigned
    // big-endian sortable bytes produced by the KeyCoder of header().field_type
    // -- the index's OWN type, not the query's, or every comparison would run
    // against a different byte order (INV-1). A wrong length is a caller bug and
    // trips DORIS_CHECK.
    //
    // `hits` is CLEARED and then filled with exactly the matching doc ids;
    // whatever it held before is discarded.
    //
    // An interval that is empty on its face (lower above upper, or an open
    // interval over a single value) is answered with an empty bitmap and no I/O
    // at all -- it is a legal query, not an error.
    //
    // The overload without a scratch allocates one on the stack for the duration
    // of the call; pass one explicitly to reuse its buffers across queries.
    Status range(Slice lower, bool lower_inclusive, Slice upper, bool upper_inclusive,
                 roaring::Roaring* hits) const;
    Status range(Slice lower, bool lower_inclusive, Slice upper, bool upper_inclusive,
                 roaring::Roaring* hits, BkdQueryScratch* scratch) const;

    // Multi-value lookup in ONE pass (design 7.3). `values` may arrive in ANY
    // order and may contain duplicates: they are sorted and deduplicated here.
    // They were once a caller invariant, but the caller this serves --
    // InListPredicateBase over a hash-backed HybridSet -- cannot supply order
    // cheaply, and the cost of sorting N probes is nothing against the leaf
    // reads they cause.
    //
    // Equivalent to the union of range(v, true, v, true) over every value, but a
    // leaf that several values land in is READ ONCE. That is the whole reason
    // this exists: `IN (v1..vN)` currently runs N full traversals, one per value,
    // because InListPredicateBase loops over its own value set (design 15 Q1b).
    //
    // `hits` is CLEARED first, exactly as range() does.
    Status lookup_many(const std::vector<Slice>& values, roaring::Roaring* hits) const;
    Status lookup_many(const std::vector<Slice>& values, roaring::Roaring* hits,
                       BkdQueryScratch* scratch) const;

    // How many POINTS the interval is expected to hold, from the resident leaf
    // directory alone -- no leaf is read (design 7.4).
    //
    // Only the two boundary leaves are guessed at, each at half its recorded
    // count, so the error never exceeds points_per_leaf and an interval that
    // covers whole leaves only is EXACT. The old implementation returned
    // max_points_in_leaf x subtree_leaves for an inside node, i.e. it assumed
    // every leaf was full, and over-counted a sparse tail by multiples -- which
    // matters because this number is what inverted_index_skip_threshold's bypass
    // decision is made on.
    //
    // Bound semantics are range()'s: an empty Slice is an unbounded side, and an
    // interval that cannot match anything estimates 0.
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
    // The empty index (design 5.3). Callers must branch on this before asking for
    // bounds -- an empty index has none.
    bool empty() const { return block_.empty(); }

    // Smallest / largest indexed value as sortable bytes. DORIS_CHECKs !empty().
    Slice min_value() const { return block_.min_value(); }
    Slice max_value() const { return block_.max_value(); }

    // Real resident cost: this object plus the decoded leaf directory and split
    // array it owns. There is no hidden per-query allocation for it to omit, so
    // unlike the old ram_bytes_used() -- which left out the packed index that was
    // then deep-copied per query -- this is the whole story.
    size_t memory_usage() const { return sizeof(*this) + block_.heap_bytes(); }

private:
    BkdReader(io::FileReader* file, const BkdSections& sections);

    // Reads leaf `index` into `buffer`. The block's extent comes from the leaf
    // directory: the next leaf's offset, or the bkd_data length for the last
    // leaf. Both the strict ordering and the bound against data_length were
    // established at open, so nothing is re-checked here (design 8.2).
    Status read_leaf(uint32_t index, std::vector<uint8_t>* buffer) const;

    // A leaf that may hold both matching and non-matching values: decode the
    // values and filter run by run. At most two leaves per range see this
    // (design 7.2). An empty bound means that side needs no test, which is how
    // the middle-of-a-range boundary leaves skip half the work.
    Status scan_boundary_leaf(uint32_t index, Slice lower, bool lower_inclusive, Slice upper,
                              bool upper_inclusive, roaring::Roaring* hits,
                              BkdQueryScratch* scratch) const;

    // A leaf entirely inside the range: take its doc ids and never look at a
    // value. The leaf's trailing docid_block_offset is what makes this skip the
    // value bytes outright (design 7.2) -- and this is the path with thousands of
    // leaves on it, which is why the layout optimizes it over the boundary one.

    io::FileReader* const file_;
    const BkdSections sections_;
    // The whole hot sub-file, decoded once. Immutable, owns its arrays, holds no
    // cursor.
    BkdIndexBlockReader block_;
};

} // namespace doris::snii::bkd
