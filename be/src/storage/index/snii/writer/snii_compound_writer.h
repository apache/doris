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
#include <vector>

#include "common/status.h"
#include "storage/index/snii/io/file_writer.h"
#include "storage/index/snii/writer/logical_index_writer.h"

// SniiCompoundWriter -- orchestrates a single-segment SNII container for one or
// more logical indexes, written front-to-back through an append-only
// io::FileWriter (no seek-back). It resolves all back-references by writing the
// metadata groups, raw directory, and fixed tail pointer LAST.
//
// CONTAINER LAYOUT PRODUCED (this is the on-disk contract the reader matches):
//   [bootstrap_header]                          (kBootstrapHeaderSize bytes)
//   for each logical index, in add order:
//     [posting region]       interleaved [prx][frq] per pod_ref term, term order
//                            (prx span empty when !has_prx)
//     [DICT blocks region]   concatenated DICT blocks, split by
//                            target_dict_block_bytes
//   for each logical index, in add order:
//     [norms POD]            NormsPodWriter::finish (scoring only; else absent)
//     [null bitmap POD]      NullBitmapWriter::finish (when nulls exist)
//   for each logical index, in add order:
//     [Core metadata][SampledTermIndex blob][DICT block directory blob]
//   [metadata directory]     raw SniiMetadataDirectoryPB bytes
//   [tail_pointer]           encode_tail_pointer at EOF
//
// (The posting region is streamed BEFORE the DICT region per index: postings are
// the large append-only term-ordered stream; the DICT region is the compact
// compressed trailer.)
//
// OFFSET CONVENTIONS (ABSOLUTE file offsets unless stated otherwise):
//   - SectionRefs in each Core metadata record ABSOLUTE file offset+length of
//     that index's posting, DICT, norms, null-bitmap, and BSBF regions. Absent
//     regions are (0,0); a present-but-empty posting region (all-INLINE index)
//     is (off, 0).
//   - DictBlockDirectory entries record each DICT block's ABSOLUTE file offset +
//     length.
//   - A windowed/slim pod_ref entry's absolute .frq offset =
//       section_refs.posting_region.offset + frq_base + frq_off_delta
//     where frq_base is the posting-region-relative running offset captured at the
//     block's open (see logical_index_writer.h). prx follows the identical rule
//     against the SAME region (prx_base == frq_base).
//   - tail_pointer.directory_offset/length point at the raw metadata directory.
namespace doris::snii::writer {

class SniiCompoundWriter;

// T2.2 (compaction index merge fast path): handle for ONE streamed logical-index
// session inside a SniiCompoundWriter, obtained from begin_streamed_index(). The
// caller pushes lexicographically sorted terms (the k-way merge output) and seals
// the index with finish(), which lays the [posting][dict] regions out exactly like
// add_logical_index. The handle is owned by the compound writer and stays valid
// for the writer's lifetime; it must not outlive it.
//
// CRASH SAFETY (invariant 6): a session that was begun but never successfully
// finished keeps the container permanently unsealable -- its posting bytes are
// already in the file, so SniiCompoundWriter::finish() fails loudly instead of
// writing a tail that silently omits the half-fed index.
class SniiStreamedIndexSession {
public:
    SniiStreamedIndexSession(const SniiStreamedIndexSession&) = delete;
    SniiStreamedIndexSession& operator=(const SniiStreamedIndexSession&) = delete;
    SniiStreamedIndexSession(SniiStreamedIndexSession&&) = delete;
    SniiStreamedIndexSession& operator=(SniiStreamedIndexSession&&) = delete;

    // Terms must arrive in strictly increasing lexicographic order with
    // ascending-docid postings. Unlike the standalone LogicalIndexWriter API,
    // every rejection is terminal here because posting bytes may already have
    // entered the compound output; all later calls return the first error.
    Status push_term(StreamedTermPostings&& tp);
    // Binds the semantic (plain-token) count after the merge's single postings
    // pass. Complete scoring metadata requires exactly one call, including for
    // an empty destination whose count is zero.
    Status set_semantic_token_count(uint64_t token_count);
    // Seals this index: flushes the trailing DICT block, streams the DICT region
    // right after the posting region and records the placements. A failed finish
    // leaves the session unfinished (and the container unsealable) -- there is
    // no retry, the whole compaction round must fail and be redone.
    Status finish();
    // Makes the owning compound permanently unsealable. Merge plans call this
    // on every destination when any source or sibling destination fails, because
    // a successfully written prefix is not a complete logical index.
    void abort(const Status& cause);
    bool finished() const { return finished_; }

private:
    friend class SniiCompoundWriter;
    SniiStreamedIndexSession(SniiCompoundWriter* owner, SniiIndexInput in,
                             TrackedNullDocids null_docids, TrackedEncodedNorms encoded_norms);
    static SniiIndexInput attach_encoded_norms(SniiIndexInput in,
                                               TrackedEncodedNorms* encoded_norms,
                                               uint64_t reserved_bytes);

    SniiCompoundWriter* owner_;
    // The reservation precedes input_ so input_.encoded_norms is destroyed
    // before its charge is released.
    MemoryReporter::Reservation encoded_norms_reservation_;
    // Owns the input: LogicalIndexWriter keeps references into it (terms /
    // encoded_norms), so it must live exactly as long as the writer.
    SniiIndexInput input_;
    std::unique_ptr<LogicalIndexWriter> writer_;
    uint64_t post_off_ = 0;
    bool semantic_token_count_required_ = false;
    bool semantic_token_count_set_ = false;
    bool finished_ = false;
};

class SniiCompoundWriter {
public:
    explicit SniiCompoundWriter(io::FileWriter* out);
    SniiCompoundWriter(const SniiCompoundWriter&) = delete;
    SniiCompoundWriter& operator=(const SniiCompoundWriter&) = delete;
    SniiCompoundWriter(SniiCompoundWriter&&) = delete;
    SniiCompoundWriter& operator=(SniiCompoundWriter&&) = delete;

    // Buffers one logical index: builds its section bytes and meta sub-sections.
    // The actual file writing happens in finish() (single front-to-back pass).
    Status add_logical_index(const SniiIndexInput& in);

    // T2.2: begins a STREAMED logical-index session (the compaction merge fast
    // path) -- the caller pushes pre-merged terms through *session instead of
    // handing the writer a term source, so `in` must carry NO term_source and NO
    // materialized terms. Only ONE session may be active at a time (its posting
    // region streams straight into the container output, so a concurrent
    // add_logical_index or second session would interleave bytes); both are
    // rejected while a session is unfinished, as is finish(). The returned
    // handle is owned by this writer and valid for its lifetime.
    Status begin_streamed_index(SniiIndexInput in, SniiStreamedIndexSession** session);
    Status begin_streamed_index(SniiIndexInput in, TrackedNullDocids null_docids,
                                SniiStreamedIndexSession** session);
    Status begin_streamed_index(SniiIndexInput in, TrackedNullDocids null_docids,
                                TrackedEncodedNorms encoded_norms,
                                SniiStreamedIndexSession** session);

    // Writes bootstrap header + all index sections + adjacent metadata groups +
    // raw directory + tail pointer, then finalizes the underlying writer.
    Status finish();

private:
    // Absolute placement of one index's sections, resolved during finish().
    struct Placement {
        uint64_t dict_off = 0;
        uint64_t dict_len = 0;
        uint64_t post_off = 0; // interleaved [prx][frq] posting region (was frq + prx)
        uint64_t post_len = 0;
        uint64_t norms_off = 0;
        uint64_t norms_len = 0;
        uint64_t null_off = 0;
        uint64_t null_len = 0;
        uint64_t bsbf_off = 0;
        uint64_t bsbf_len = 0;
    };

    friend class SniiStreamedIndexSession;

    Status ensure_bootstrap();
    Status write_bootstrap();
    Status write_norms();
    Status write_tail();
    Status append(const std::vector<uint8_t>& bytes);
    Status poison(Status status);
    // Seals one streamed session: records its placements and adopts its
    // LogicalIndexWriter into indexes_ (only on FULL success -- see the
    // crash-safety note on SniiStreamedIndexSession).
    Status finish_streamed_index(SniiStreamedIndexSession* session);
    // An unfinished streamed session (begun but not successfully sealed):
    // blocks add_logical_index, another begin_streamed_index and finish().
    bool has_active_session() const { return !sessions_.empty() && !sessions_.back()->finished(); }

    io::FileWriter* out_;
    std::vector<std::unique_ptr<LogicalIndexWriter>> indexes_;
    // Streamed sessions in begin order (at most the last one is unfinished).
    // Owned here so the raw handles returned to callers stay valid for the
    // writer's lifetime; a finished session is inert (its writer moved out).
    std::vector<std::unique_ptr<SniiStreamedIndexSession>> sessions_;
    // Per-index placement; post_off/post_len are filled as each index's posting region
    // streams in during add_logical_index, the rest during finish(). The absolute write
    // offset is out_->bytes_written() (the single source of truth -- no separate cursor).
    std::vector<Placement> placements_;
    bool bootstrap_written_ = false;
    bool finished_ = false;
    Status failed_ = Status::OK();
};

} // namespace doris::snii::writer
