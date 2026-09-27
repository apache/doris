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

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"

namespace doris::snii::format {

// Norms of one logical index: the 1-byte encoded document length BM25 length normalization
// reads per docid (SniiStatsProvider::encoded_norm). A document carries a norm when it is not
// NULL, or when it is NULL but still produced tokens (a nullable ARRAY row may keep its nested
// payload under the NULL flag; its tokens sit in the postings exactly as for a non-NULL row).
// Postings only reference documents that carry a norm.
//
// Two section layouts exist. Both are framed by SectionFramer:
//   framer envelope = [u8 type][varint64 payload_len][payload][fixed32 crc32c]
//
// DENSE (type SectionType::kNormsPod = 14), one byte per document:
//   payload = [varint64 doc_count][doc_count bytes: encoded_norm[docid]]
//   A document without a norm stores kEmptyDocumentNorm. This is the only layout readers before
//   the sparse layout understand, and the writer keeps it for every logical index without NULL
//   rows, so those sections stay byte-identical to the earlier format.
//
// SPARSE (type SectionType::kNormsSparse = 15), bytes only for documents with a norm:
//   payload = [varint64 doc_count]
//             [varint64 present_count]         documents that carry a norm
//             [u8 bytes_per_norm]              0 (all norms equal) or 1
//             [varint64 block_count]
//             [block_count x 12-byte block header]
//             [varint64 block_data_len]
//             [block_data_len bytes: block payloads, in header order, without gaps]
//             bytes_per_norm == 0: [u8 constant norm] (written as 0 when present_count == 0)
//             bytes_per_norm == 1: [present_count bytes: norms in ascending docid order]
//   The present docids are split into blocks of 65536 docids (block key = docid >> 16); only
//   blocks with at least one present docid are stored, in ascending key order. A block header is
//     [fixed16 key][u8 kind][u8 reserved = 0][fixed32 rank_base][fixed32 payload_offset]
//   rank_base is the number of present docids in the preceding blocks (the index of the block's
//   first norm), payload_offset the offset of the block payload inside the block data. A block's
//   cardinality is the next header's rank_base (present_count for the last block) minus its own.
//   With span = min(65536, doc_count - (key << 16)) and low = docid & 0xFFFF, the kinds are
//     0 ALL:    every docid of the block is present; no payload; cardinality == span.
//     1 ARRAY:  [cardinality x fixed16 low], strictly ascending.
//     2 BITSET: [1024 x fixed64 words][128 x fixed16 rank]; bit (low & 63) of word (low >> 6) is
//               set for a present low, no bit at or past span is set, and rank[g] is the number
//               of set bits in words [0, 8 * g).
//     3 RUNS:   [fixed16 run_count][run_count x (fixed16 first_low, fixed16 last_low,
//               fixed16 rank_before)], runs ascending and separated by at least one absent low;
//               rank_before is the number of present lows in the preceding runs.
//   The writer picks, per block, ALL when nothing is absent and otherwise the smallest of ARRAY,
//   RUNS and BITSET (ties in that order). A lookup costs a binary search over the block headers
//   plus O(1) (ALL, BITSET) or a binary search inside the block (ARRAY, RUNS), and reads only the
//   section bytes, so the reader needs no memory beyond the section itself.
//
// The writer picks the layout when the logical index is finished: DENSE when no document lacks a
// norm or the BE config enable_snii_sparse_norms is off, otherwise the smaller of SPARSE and DENSE
// (DENSE on a tie). Readers accept both layouts whatever the config says.

// Norm stored by the dense layout for a document without a norm. Equals query::encode_norm(0),
// which is what every writer stored for such rows before the sparse layout existed.
inline constexpr uint8_t kEmptyDocumentNorm = 1;

// Accumulates dense norms one document at a time (docid is the append order).
class NormsPodWriter {
public:
    // Appends the encoded_norm for the next docid (docid is implicit, assigned in append order starting from 0).
    void add(uint8_t encoded_norm) { norms_.push_back(encoded_norm); }

    // Number of docs accumulated so far (i.e., the next docid to be assigned).
    size_t count() const { return norms_.size(); }

    // Writes the DENSE section [doc_count][bytes] framed by SectionFramer into sink (appends; does
    // not clear sink).
    void finish(ByteSink* sink) const;
    // Zero-copy source overload: one norm per document.
    static void finish(std::span<const uint8_t> norms, ByteSink* sink);

private:
    std::vector<uint8_t> norms_;
};

// Norms of one logical index as handed to the section writer.
struct NormsSectionInput {
    uint32_t doc_count = 0;
    // Ascending NULL docids.
    std::span<const uint32_t> null_docids;
    // Ascending subset of null_docids whose documents still carry a norm.
    std::span<const uint32_t> null_docids_with_norms;
    // Encoded norms of the documents that carry one (every docid outside null_docids plus
    // null_docids_with_norms), in ascending docid order.
    std::span<const uint8_t> norms;
};

enum class NormsLayout : uint8_t { kDense, kSparse };

struct NormsSectionPlan {
    NormsLayout layout = NormsLayout::kDense;
    // SPARSE only.
    uint8_t bytes_per_norm = 1;
    uint8_t constant_norm = 0;
    size_t payload_bytes = 0;
    size_t framed_bytes = 0;
};

// Validates the input shape (sorted docids inside the document domain, null_docids_with_norms a
// subset of null_docids, one norm per document that carries one) and picks the layout described
// above. force_dense selects DENSE regardless of size (LogicalIndexWriter::finalize_build passes
// !config::enable_snii_sparse_norms).
Status plan_norms_section(const NormsSectionInput& in, bool force_dense, NormsSectionPlan* out);

// Appends the planned section to sink: exactly plan.framed_bytes bytes. The input must be the
// one the plan was computed from.
void write_norms_section(const NormsSectionInput& in, const NormsSectionPlan& plan, ByteSink* sink);

// Framed length of the DENSE section for doc_count documents. A valid norms section of either
// layout is never longer (the writer only chooses SPARSE when it is shorter).
uint64_t dense_norms_section_bytes(uint64_t doc_count);

// Read-only view over a DENSE or SPARSE section. open() verifies the framer CRC and the complete
// layout; afterwards lookups only read the borrowed section bytes, so copies are cheap and the
// caller must keep the bytes alive.
class NormsPodReader {
public:
    NormsPodReader() = default;

    // Parses the entire section (including the framer envelope). Returns Corruption on CRC
    // mismatch, truncation, an unknown section type, or any layout inconsistency.
    static Status open(Slice framed, NormsPodReader* out);

    uint32_t doc_count() const { return doc_count_; }
    bool is_sparse() const { return layout_ == NormsLayout::kSparse; }
    // Documents that carry a norm in a SPARSE section; doc_count() for a DENSE section, which
    // stores a byte for every document.
    uint32_t present_count() const { return present_count_; }

    // Precondition (hard contract): docid < doc_count() and docid carries a norm (it comes from a
    // posting decoded internally by SNII). Both layouts only assert it in debug builds; use
    // try_encoded_norm when the docid is untrusted.
    uint8_t encoded_norm(uint32_t docid) const {
        assert(docid < doc_count_);
        if (layout_ == NormsLayout::kDense) {
            return norms_[docid];
        }
        return sparse_encoded_norm(docid);
    }

    // Checked access: InvalidArgument for a docid out of range, Corruption for a docid without a
    // norm in a SPARSE section; never reads out-of-range memory.
    Status try_encoded_norm(uint32_t docid, uint8_t* out) const {
        if (docid >= doc_count_) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("norms: docid out of range");
        }
        if (layout_ == NormsLayout::kDense) {
            *out = norms_[docid];
            return Status::OK();
        }
        return try_sparse_encoded_norm(docid, out);
    }

private:
    static Status open_sparse(Slice payload, NormsPodReader* out);
    // Index of docid among the present docids; false when docid carries no norm.
    bool sparse_rank(uint32_t docid, uint32_t* rank) const;
    uint8_t sparse_encoded_norm(uint32_t docid) const;
    Status try_sparse_encoded_norm(uint32_t docid, uint8_t* out) const;

    NormsLayout layout_ = NormsLayout::kDense;
    uint32_t doc_count_ = 0;
    uint32_t present_count_ = 0;
    // DENSE: doc_count_ bytes. SPARSE: present_count_ bytes when bytes_per_norm_ == 1.
    const uint8_t* norms_ = nullptr;
    // SPARSE only.
    uint8_t bytes_per_norm_ = 0;
    uint8_t constant_norm_ = 0;
    uint32_t block_count_ = 0;
    const uint8_t* block_headers_ = nullptr;
    const uint8_t* block_data_ = nullptr;
};

} // namespace doris::snii::format
