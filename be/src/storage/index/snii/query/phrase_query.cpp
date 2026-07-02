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

#include "storage/index/snii/query/phrase_query.h"

#include <algorithm>
#include <cstdint>
#include <iterator>
#include <limits>
#include <memory>
#include <numeric>
#include <utility>
#include <vector>

#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/format/frq_pod.h"
#include "storage/index/snii/format/frq_prelude.h"
#include "storage/index/snii/format/phrase_bigram.h"
#include "storage/index/snii/format/prx_pod.h"
#include "storage/index/snii/io/batch_range_fetcher.h"
#include "storage/index/snii/query/internal/docid_conjunction.h"
#include "storage/index/snii/query/internal/docid_posting_reader.h"
#include "storage/index/snii/query/internal/docid_set_ops.h"
#include "storage/index/snii/query/internal/position_math.h"
#include "storage/index/snii/query/internal/query_test_counters.h"
#include "storage/index/snii/query/prefix_query.h"
#include "storage/index/snii/query/term_query.h"
#include "storage/index/snii/reader/windowed_posting.h"

// phrase_query implements MATCH_PHRASE with WINDOW (sub-block) SKIPPING for
// high-df windowed terms (design spec section 6.2):
//   1. Resolve every term; reject if any is absent.
//   2. Batch-read each windowed term's prelude + each slim/inline term's full
//      docid posting in one round; open the two-level prelude readers.
//   3. Pick the DRIVER = smallest-df term; materialize it fully -> the initial
//      candidate docid set.
//   4. For every other term in ascending-df order, narrow the candidate set:
//        - slim/inline: intersect with its (already decoded) full posting.
//        - windowed:    locate_window() the CURRENT candidates -> the SET of
//                       windows covering them; batch-fetch ONLY those windows'
//                       .frq docid regions; keep candidates present in some
//                       covering window. A high-df term thus reads
//                       O(candidates) windows instead of its whole O(df)
//                       posting.
//   5. Fetch PRX only for retained chunks and run the positional phrase check
//      (term[0]@p, term[1]@p+1, ...) on the survivors.
// The result is identical to a full-read intersection; only the bytes read for
// high-df windowed terms shrink.
namespace doris::snii::query {

using query::internal::DocidChunk;
using query::internal::DocidSource;
using query::internal::ResolvedQueryTerm;
using query::internal::TermPlan;
using reader::LogicalIndexReader;

namespace {

struct ExpectedTailPositions {
    uint32_t docid = 0;
    size_t positions_begin = 0;
    size_t positions_end = 0;
};

struct ExpectedTailPositionSet {
    std::vector<ExpectedTailPositions> docs;
    std::vector<uint32_t> positions;

    void clear() {
        docs.clear();
        positions.clear();
    }

    void reserve_docs(size_t count) {
        docs.reserve(count);
        positions.reserve(count);
    }
};

// One decoded chunk of a term's posting: a windowed term's covering window, or
// a slim/inline term's single posting. `docids` is decoded in the conjunction
// phase (and reused by the streaming cursor -- the dd region is decoded exactly
// once); `prx` is the on-disk positions bytes, decoded lazily by the cursor
// (once per chunk) during phrase verification.
struct PosChunk {
    std::vector<uint32_t> docids; // ascending, absolute
    // Empty means the chunk keeps every PRX doc in on-disk order. Non-empty means
    // `docids[i]` corresponds to on-disk local document ordinal
    // `prx_doc_ordinals[i]`, allowing PRX decode to skip positions for docs that
    // were removed by the docid-only conjunction.
    std::vector<uint32_t> prx_doc_ordinals;
    uint32_t prx_doc_count = 0;
    Slice prx; // .prx window bytes (reference fetcher/round1/entry)
    bool windowed = false;
    uint32_t window = 0;
};

// A term's retained posting as an ordered list of chunks (windowed: covering
// windows in docid order; slim/inline: one). The referenced prx bytes live in
// `round1` / the per-term fetchers kept alive in phrase_query::owners for the
// whole query, so the cursor can decode positions during verification.
struct PosSource {
    std::vector<PosChunk> chunks;
};

struct PhraseExecutionState {
    std::vector<PosSource> srcs;
    std::vector<std::unique_ptr<io::BatchRangeFetcher>> owners;
    std::vector<uint32_t> candidates;
};

struct PhraseTermMapping {
    std::vector<std::string> unique_terms;
    std::vector<size_t> phrase_plan_index;
};

PhraseTermMapping BuildPhraseTermMapping(const std::vector<std::string>& terms) {
    PhraseTermMapping mapping;
    mapping.phrase_plan_index.reserve(terms.size());
    for (const std::string& term : terms) {
        auto it = std::ranges::find(mapping.unique_terms, term);
        if (it == mapping.unique_terms.end()) {
            mapping.phrase_plan_index.push_back(mapping.unique_terms.size());
            mapping.unique_terms.push_back(term);
            continue;
        }
        mapping.phrase_plan_index.push_back(static_cast<size_t>(it - mapping.unique_terms.begin()));
    }
    return mapping;
}

Status phrase_bigram_enabled(const LogicalIndexReader& idx, bool* enabled) {
    ResolvedQueryTerm sentinel;
    return internal::resolve_query_term(idx, format::make_phrase_bigram_sentinel_term(), &sentinel,
                                        enabled);
}

Status TryTwoTermPhraseBigram(const LogicalIndexReader& idx, const std::vector<std::string>& terms,
                              std::vector<uint32_t>* const docids, bool* handled) {
    *handled = false;
    if (terms.size() != 2) {
        return Status::OK();
    }
    if (!format::is_phrase_bigram_indexable_term(terms[0]) ||
        !format::is_phrase_bigram_indexable_term(terms[1])) {
        return Status::OK();
    }

    ResolvedQueryTerm resolved;
    bool found = false;
    RETURN_IF_ERROR(internal::resolve_query_term(
            idx, format::make_phrase_bigram_term(terms[0], terms[1]), &resolved, &found));
    if (found) {
        // Docid membership IS the phrase answer (the pair was adjacent when the
        // bigram token was emitted); positions are never read here, which is what
        // lets the writer store bigram postings docs-only (G01 part B).
        SNII_QUERY_COUNT(bigram_hits);
        *handled = true;
        return internal::read_docid_posting(idx, resolved.entry, resolved.frq_base,
                                            resolved.prx_base, docids);
    }

    // Bigram dict MISS. When THIS segment's meta declares bigram df-pruning
    // (G01), the miss is ambiguous -- the pair may have been pruned (df below the
    // recorded threshold) rather than absent -- so fall back to the generic
    // positions-verification phrase path (*handled stays false). The fallback is
    // the full-fidelity implementation, and a pruned pair is low-df by
    // definition, so its candidate set is small. Segments WITHOUT the meta field
    // keep the legacy contract: the writer materialized EVERY adjacent pair, so
    // miss == no adjacency == empty result.
    if (idx.bigram_prune_min_df() > 0) {
        SNII_QUERY_COUNT(bigram_fallbacks);
        return Status::OK();
    }

    bool enabled = false;
    RETURN_IF_ERROR(phrase_bigram_enabled(idx, &enabled));
    if (!enabled) {
        return Status::OK();
    }
    docids->clear();
    *handled = true;
    return Status::OK();
}

Status append_prx_doc_ordinal(size_t ordinal, std::vector<uint32_t>* out) {
    if (ordinal > std::numeric_limits<uint32_t>::max()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "phrase_query: prx doc ordinal exceeds u32");
    }
    out->push_back(static_cast<uint32_t>(ordinal));
    return Status::OK();
}

Status append_selected_ordinal(size_t doc_index, const std::vector<uint32_t>& prx_doc_ordinals,
                               std::vector<uint32_t>* selected_ordinals) {
    if (!prx_doc_ordinals.empty()) {
        selected_ordinals->push_back(prx_doc_ordinals[doc_index]);
        return Status::OK();
    }
    return append_prx_doc_ordinal(doc_index, selected_ordinals);
}

Status append_selected_doc(size_t doc_index, uint32_t docid,
                           const std::vector<uint32_t>& prx_doc_ordinals,
                           std::vector<uint32_t>* selected_docids,
                           std::vector<uint32_t>* selected_ordinals) {
    selected_docids->push_back(docid);
    return append_selected_ordinal(doc_index, prx_doc_ordinals, selected_ordinals);
}

Status materialize_selected_prefix(size_t count, size_t capacity,
                                   const std::vector<uint32_t>& docids,
                                   const std::vector<uint32_t>& prx_doc_ordinals,
                                   std::vector<uint32_t>* selected_docids,
                                   std::vector<uint32_t>* selected_ordinals) {
    selected_docids->reserve(capacity);
    selected_ordinals->reserve(capacity);
    selected_docids->insert(selected_docids->end(), docids.begin(), docids.begin() + count);
    for (size_t i = 0; i < count; ++i) {
        RETURN_IF_ERROR(append_selected_ordinal(i, prx_doc_ordinals, selected_ordinals));
    }
    return Status::OK();
}

Status materialize_selected_prefix_if_needed(bool* selected_all, size_t count, size_t capacity,
                                             const std::vector<uint32_t>& docids,
                                             const std::vector<uint32_t>& prx_doc_ordinals,
                                             std::vector<uint32_t>* selected_docids,
                                             std::vector<uint32_t>* selected_ordinals) {
    if (!*selected_all) {
        return Status::OK();
    }
    *selected_all = false;
    return materialize_selected_prefix(count, capacity, docids, prx_doc_ordinals, selected_docids,
                                       selected_ordinals);
}

Status SelectCandidateDocsForPrx(std::vector<uint32_t>* docids,
                                 std::vector<uint32_t>* prx_doc_ordinals, uint32_t prx_doc_count,
                                 const std::vector<uint32_t>& candidates, PosChunk* chunk) {
    chunk->docids.clear();
    chunk->prx_doc_ordinals.clear();
    if (prx_doc_count == 0 && docids->size() > std::numeric_limits<uint32_t>::max()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "phrase_query: prx doc count exceeds u32");
    }
    chunk->prx_doc_count =
            prx_doc_count == 0 ? static_cast<uint32_t>(docids->size()) : prx_doc_count;
    if (docids->empty() || candidates.empty()) {
        return Status::OK();
    }
    if (!prx_doc_ordinals->empty() && prx_doc_ordinals->size() != docids->size()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "phrase_query: prx ordinal/docid count mismatch");
    }

    std::vector<uint32_t> selected_docids;
    std::vector<uint32_t> selected_ordinals;
    bool selected_all = true;
    const size_t selected_capacity = std::min(docids->size(), candidates.size());

    auto candidate_it = std::ranges::lower_bound(candidates, docids->front());
    size_t candidate_index = static_cast<size_t>(candidate_it - candidates.begin());
    for (size_t doc_index = 0; doc_index < docids->size(); ++doc_index) {
        const uint32_t docid = (*docids)[doc_index];
        while (candidate_index < candidates.size() && candidates[candidate_index] < docid) {
            ++candidate_index;
        }
        if (candidate_index == candidates.size()) {
            RETURN_IF_ERROR(materialize_selected_prefix_if_needed(
                    &selected_all, doc_index, selected_capacity, *docids, *prx_doc_ordinals,
                    &selected_docids, &selected_ordinals));
            break;
        }
        if (candidates[candidate_index] != docid) {
            RETURN_IF_ERROR(materialize_selected_prefix_if_needed(
                    &selected_all, doc_index, selected_capacity, *docids, *prx_doc_ordinals,
                    &selected_docids, &selected_ordinals));
            continue;
        }

        if (!selected_all) {
            RETURN_IF_ERROR(append_selected_doc(doc_index, docid, *prx_doc_ordinals,
                                                &selected_docids, &selected_ordinals));
        }
        ++candidate_index;
    }

    if (selected_all) {
        chunk->docids = std::move(*docids);
        chunk->prx_doc_ordinals = std::move(*prx_doc_ordinals);
        docids->clear();
        prx_doc_ordinals->clear();
        return Status::OK();
    }
    if (selected_docids.empty()) {
        return Status::OK();
    }
    chunk->docids = std::move(selected_docids);
    chunk->prx_doc_ordinals = std::move(selected_ordinals);
    return Status::OK();
}

// PRX byte ranges for every candidate-bearing chunk across all phrase terms are
// added to one shared BatchRangeFetcher and fetched in a single batched round
// (T02). Pass 1 records, for each chunk that needs on-disk PRX bytes, where to
// write the fetched slice back: which plan's PosSource, which chunk within it,
// and the fetcher handle.
struct PrxRangeAssignment {
    size_t plan_index;
    size_t chunk_index;
    size_t handle;
};

void record_prx_assignment(std::vector<PrxRangeAssignment>* assignments, size_t plan_index,
                           size_t chunk_index, size_t handle) {
    assignments->push_back(PrxRangeAssignment {
            .plan_index = plan_index, .chunk_index = chunk_index, .handle = handle});
}

Status BuildFlatPositionSource(const LogicalIndexReader& idx, const io::BatchRangeFetcher& round1,
                               DocidSource* doc_source, const TermPlan& p,
                               const std::vector<uint32_t>& candidates, size_t plan_index,
                               io::BatchRangeFetcher* prx_fetcher,
                               std::vector<PrxRangeAssignment>* assignments, PosSource* src) {
    PosChunk chunk;
    std::vector<uint32_t> docids;
    std::vector<uint32_t> prx_doc_ordinals;
    const bool docids_are_final_candidates =
            doc_source->docids_are_final_candidates && !doc_source->chunks.empty();
    if (!doc_source->chunks.empty()) {
        DocidChunk& doc_chunk = doc_source->chunks.front();
        docids = std::move(doc_chunk.docids);
        prx_doc_ordinals = std::move(doc_chunk.prx_doc_ordinals);
        chunk.prx_doc_count = doc_chunk.prx_doc_count;
    }
    // pod_ref PRX bytes are read from the shared fetcher (one batched round for the
    // whole phrase); inline PRX bytes already live in the dict entry. The pod_ref
    // range is added unconditionally to keep the bytes read identical to the prior
    // per-term fetch(); the handle is only recorded as an assignment when the chunk
    // is kept (an empty chunk reads the same bytes but needs no backfill).
    bool has_prx_handle = false;
    size_t prx_handle = 0;
    if (p.pod_ref) {
        uint64_t poff = 0;
        uint64_t plen = 0;
        RETURN_IF_ERROR(idx.resolve_prx_window(p.entry, p.prx_base, &poff, &plen));
        prx_handle = prx_fetcher->add(poff, plen);
        has_prx_handle = true;
    } else {
        chunk.prx = Slice(p.entry.prx_bytes);
    }
    if (docids.empty()) {
        Slice dd;
        if (p.pod_ref) {
            dd = round1.get(p.frq_handle);
        } else {
            RETURN_IF_ERROR(internal::inline_dd_region(p.entry, &dd));
        }
        RETURN_IF_ERROR(format::decode_dd_region(dd, p.entry.dd_meta,
                                                 /*win_base=*/0, &docids));
        if (docids.size() > std::numeric_limits<uint32_t>::max()) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "phrase_query: prx doc count exceeds u32");
        }
        chunk.prx_doc_count = static_cast<uint32_t>(docids.size());
    }
    if (docids_are_final_candidates) {
        chunk.docids = std::move(docids);
        chunk.prx_doc_ordinals = std::move(prx_doc_ordinals);
        if (!chunk.docids.empty()) {
            if (has_prx_handle) {
                record_prx_assignment(assignments, plan_index, src->chunks.size(), prx_handle);
            }
            src->chunks.push_back(std::move(chunk));
        }
        return Status::OK();
    }
    RETURN_IF_ERROR(SelectCandidateDocsForPrx(&docids, &prx_doc_ordinals, chunk.prx_doc_count,
                                              candidates, &chunk));
    if (!chunk.docids.empty()) {
        if (has_prx_handle) {
            record_prx_assignment(assignments, plan_index, src->chunks.size(), prx_handle);
        }
        src->chunks.push_back(std::move(chunk));
    }
    return Status::OK();
}

bool ChunkMayContainCandidate(const DocidChunk& chunk, const std::vector<uint32_t>& candidates) {
    if (chunk.docids.empty() || candidates.empty()) {
        return false;
    }
    const auto it = std::ranges::lower_bound(candidates, chunk.docids.front());
    return it != candidates.end() && *it <= chunk.docids.back();
}

Status DecodeWindowedPositionSource(const LogicalIndexReader& idx, const TermPlan& p,
                                    DocidSource* doc_source,
                                    const std::vector<uint32_t>& candidates, size_t plan_index,
                                    io::BatchRangeFetcher* prx_fetcher,
                                    std::vector<PrxRangeAssignment>* assignments, PosSource* src) {
    for (size_t i = 0; i < doc_source->chunks.size(); ++i) {
        DocidChunk& doc_chunk = doc_source->chunks[i];
        if (!doc_source->docids_are_final_candidates &&
            !ChunkMayContainCandidate(doc_chunk, candidates)) {
            continue;
        }
        if (!doc_chunk.windowed) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "phrase_query: expected windowed doc chunk");
        }
        PosChunk chunk;
        if (doc_source->docids_are_final_candidates) {
            chunk.docids = std::move(doc_chunk.docids);
            chunk.prx_doc_ordinals = std::move(doc_chunk.prx_doc_ordinals);
            chunk.prx_doc_count = doc_chunk.prx_doc_count;
        } else {
            RETURN_IF_ERROR(SelectCandidateDocsForPrx(&doc_chunk.docids,
                                                      &doc_chunk.prx_doc_ordinals,
                                                      doc_chunk.prx_doc_count, candidates, &chunk));
        }
        if (chunk.docids.empty()) {
            continue;
        }

        reader::WindowAbsRange range;
        RETURN_IF_ERROR(reader::windowed_window_range(
                idx, p.entry, p.frq_base, p.prx_base, p.prelude, doc_chunk.window,
                /*want_positions=*/true, /*want_freq=*/false, &range));
        chunk.windowed = true;
        chunk.window = doc_chunk.window;
        const size_t prx_handle = prx_fetcher->add(range.prx_off, range.prx_len);
        record_prx_assignment(assignments, plan_index, src->chunks.size(), prx_handle);
        src->chunks.push_back(std::move(chunk));
    }
    return Status::OK();
}

Status BuildPositionSourcesForCandidates(
        const LogicalIndexReader& idx, const io::BatchRangeFetcher& round1,
        const std::vector<TermPlan>& plans, std::vector<DocidSource>* doc_sources,
        const std::vector<uint32_t>& candidates,
        std::vector<std::unique_ptr<io::BatchRangeFetcher>>* owners, std::vector<PosSource>* srcs) {
    srcs->assign(plans.size(), PosSource {});
    // All phrase terms share one PRX fetcher: pass 1 adds every candidate-bearing
    // chunk's PRX range and records a backfill assignment; a single fetch() then
    // issues one batched read (one serial round on a remote reader); pass 2 fills
    // in each chunk's PRX slice. This collapses the prior per-term fetch() -- O(n)
    // serial remote rounds for an n-term phrase -- into one.
    auto prx_fetcher =
            std::make_unique<io::BatchRangeFetcher>(idx.reader(), reader::kSameTermCoalesceGap);
    std::vector<PrxRangeAssignment> assignments;
    for (size_t i = 0; i < plans.size(); ++i) {
        const TermPlan& p = plans[i];
        if (p.windowed) {
            RETURN_IF_ERROR(DecodeWindowedPositionSource(idx, p, &(*doc_sources)[i], candidates, i,
                                                         prx_fetcher.get(), &assignments,
                                                         &(*srcs)[i]));
            continue;
        }
        RETURN_IF_ERROR(BuildFlatPositionSource(idx, round1, &(*doc_sources)[i], p, candidates, i,
                                                prx_fetcher.get(), &assignments, &(*srcs)[i]));
    }
    if (prx_fetcher->pending() > 0) {
        RETURN_IF_ERROR(prx_fetcher->fetch());
    }
    for (const PrxRangeAssignment& a : assignments) {
        (*srcs)[a.plan_index].chunks[a.chunk_index].prx = prx_fetcher->get(a.handle);
    }
    // Keep the fetcher alive only when some chunk slice references its buffers.
    if (!assignments.empty()) {
        owners->push_back(std::move(prx_fetcher));
    }
    return Status::OK();
}

class PosChunkDecoder {
public:
    void reset() {
        chunk_ = nullptr;
        offsets_by_prx_ordinal_ = false;
    }

    Status decode(const PosChunk& chunk) {
        chunk_ = &chunk;
        ByteSource ps(chunk.prx);
        offsets_by_prx_ordinal_ = false;
        if (chunk.prx_doc_ordinals.empty()) {
            RETURN_IF_ERROR(format::read_prx_window_csr(&ps, &pflat_, &poff_));
        } else if (should_decode_full_prx_window(chunk)) {
            RETURN_IF_ERROR(format::read_prx_window_csr(&ps, &pflat_, &poff_));
            offsets_by_prx_ordinal_ = true;
        } else {
            RETURN_IF_ERROR(format::read_prx_window_csr_selective(&ps, chunk.prx_doc_ordinals,
                                                                  &pflat_, &poff_));
        }
        if (offsets_by_prx_ordinal_) {
            if (poff_.size() != static_cast<size_t>(chunk.prx_doc_count) + 1) {
                return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                        "phrase_query: full prx doc-count mismatch");
            }
        } else if (poff_.size() != chunk.docids.size() + 1) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "phrase_query: selected prx/doc-count mismatch");
        }
        if (poff_.back() > pflat_.size()) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "phrase_query: prx final offset out of range");
        }
        return Status::OK();
    }

    Status positions(size_t doc_index, std::pair<const uint32_t*, const uint32_t*>* out) const {
        if (chunk_ == nullptr || doc_index >= chunk_->docids.size()) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "phrase_query: decoded chunk doc index out of range");
        }
        const size_t pos_index =
                offsets_by_prx_ordinal_ ? chunk_->prx_doc_ordinals[doc_index] : doc_index;
        if (pos_index + 1 >= poff_.size()) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "phrase_query: prx ordinal offset out of range");
        }
        const uint32_t begin = poff_[pos_index];
        const uint32_t end = poff_[pos_index + 1];
        if (begin == end) {
            *out = {nullptr, nullptr};
            return Status::OK();
        }
        if (end > pflat_.size()) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "phrase_query: prx offset out of range");
        }
        *out = {pflat_.data() + begin, pflat_.data() + end};
        return Status::OK();
    }

    inline __attribute__((always_inline)) std::pair<const uint32_t*, const uint32_t*>
    positions_unchecked(size_t doc_index) const {
        const size_t pos_index =
                offsets_by_prx_ordinal_ ? chunk_->prx_doc_ordinals[doc_index] : doc_index;
        const uint32_t begin = poff_[pos_index];
        const uint32_t end = poff_[pos_index + 1];
        if (begin == end) {
            return {nullptr, nullptr};
        }
        return {pflat_.data() + begin, pflat_.data() + end};
    }

private:
    static bool should_decode_full_prx_window(const PosChunk& chunk) {
        return chunk.prx_doc_count != 0 &&
               static_cast<uint64_t>(chunk.prx_doc_ordinals.size()) * 2 >= chunk.prx_doc_count;
    }

    const PosChunk* chunk_ = nullptr;
    bool offsets_by_prx_ordinal_ = false;
    std::vector<uint32_t> pflat_;
    std::vector<uint32_t> poff_;
};

// Streaming position cursor over one term's retained chunks. It advances ONLY
// forward (callers seek ascending candidate docids), decodes each chunk's
// docids once (reused from the conjunction phase) and each chunk's positions at
// most once (lazily, into a flat CSR whose capacity is retained across chunks).
// No per-doc allocation, no per-candidate docid binary search: positions are
// addressed by the doc's local index within its chunk. This is the read-side
// dual of the windowed posting layout -- the S3-native batch fetch already
// pulled every needed chunk into memory; the cursor is pure in-memory column
// iteration.
class PostingCursor {
public:
    void init(const PosSource* src) {
        src_ = src;
        ci_ = 0;
        li_ = 0;
        decoded_pos_chunk_ = kNoChunk;
        decoder_.reset();
    }

    // Positions the cursor at `target` (guaranteed present: candidates are the
    // intersection of exactly these chunks' docids). Monotonic forward advance.
    Status seek(uint32_t target) {
        while (ci_ < src_->chunks.size() &&
               (src_->chunks[ci_].docids.empty() || src_->chunks[ci_].docids.back() < target)) {
            ++ci_;
            li_ = 0;
        }
        if (ci_ >= src_->chunks.size()) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "phrase_query: cursor exhausted before target docid");
        }
        const std::vector<uint32_t>& d = src_->chunks[ci_].docids;
        while (li_ < d.size() && d[li_] < target) {
            ++li_;
        }
        if (li_ >= d.size() || d[li_] != target) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "phrase_query: candidate missing from posting chunk");
        }
        return Status::OK();
    }

    // [begin,end) of the current doc's positions, decoding the current chunk's
    // .prx exactly once (cached). Must follow a seek that landed on a real doc.
    Status positions(std::pair<const uint32_t*, const uint32_t*>* out) {
        if (ci_ >= src_->chunks.size() || li_ >= src_->chunks[ci_].docids.size()) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "phrase_query: cursor positions out of range");
        }
        if (decoded_pos_chunk_ != ci_) {
            RETURN_IF_ERROR(decoder_.decode(src_->chunks[ci_]));
            decoded_pos_chunk_ = ci_;
        }
        return decoder_.positions(li_, out);
    }

    Status next(uint32_t* docid, std::pair<const uint32_t*, const uint32_t*>* out) {
        while (ci_ < src_->chunks.size() &&
               (src_->chunks[ci_].docids.empty() || li_ >= src_->chunks[ci_].docids.size())) {
            ++ci_;
            li_ = 0;
        }
        if (ci_ >= src_->chunks.size()) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "phrase_query: cursor exhausted before next docid");
        }
        *docid = src_->chunks[ci_].docids[li_];
        RETURN_IF_ERROR(positions(out));
        ++li_;
        return Status::OK();
    }

private:
    static constexpr size_t kNoChunk = static_cast<size_t>(-1);

    const PosSource* src_ = nullptr;
    size_t ci_ = 0;                       // current chunk
    size_t li_ = 0;                       // current local doc index within the chunk
    size_t decoded_pos_chunk_ = kNoChunk; // which chunk decoder_ currently holds
    PosChunkDecoder decoder_;
};

class PhrasePositionLoader {
public:
    PhrasePositionLoader(size_t plan_count, std::vector<PosSource>& srcs)
            : cursors_(plan_count), plan_spans_(plan_count), loaded_epoch_(plan_count, 0) {
        for (size_t i = 0; i < plan_count; ++i) {
            cursors_[i].init(&srcs[i]);
        }
    }

    void begin_doc(uint32_t docid) {
        docid_ = docid;
        ++epoch_;
        if (epoch_ == 0) {
            std::ranges::fill(loaded_epoch_, 0);
            epoch_ = 1;
        }
    }

    Status positions_for_phrase_pos(const std::vector<size_t>& phrase_plan_index, size_t phrase_pos,
                                    std::pair<const uint32_t*, const uint32_t*>* out) {
        const size_t plan_index = phrase_plan_index[phrase_pos];
        if (loaded_epoch_[plan_index] != epoch_) {
            RETURN_IF_ERROR(cursors_[plan_index].seek(docid_));
            RETURN_IF_ERROR(cursors_[plan_index].positions(&plan_spans_[plan_index]));
            loaded_epoch_[plan_index] = epoch_;
        }
        *out = plan_spans_[plan_index];
        return Status::OK();
    }

private:
    std::vector<PostingCursor> cursors_;
    std::vector<std::pair<const uint32_t*, const uint32_t*>> plan_spans_;
    std::vector<uint32_t> loaded_epoch_;
    uint32_t docid_ = 0;
    uint32_t epoch_ = 0;
};

bool ContainsTwoTermPhrase(std::pair<const uint32_t*, const uint32_t*> left_span,
                           std::pair<const uint32_t*, const uint32_t*> right_span,
                           uint32_t right_delta) {
    const uint32_t* left = left_span.first;
    const uint32_t* right = right_span.first;
    if (left == left_span.second || right == right_span.second) {
        return false;
    }
    const uint32_t max_start = std::numeric_limits<uint32_t>::max() - right_delta;
    if (left + 1 == left_span.second && right + 1 == right_span.second) {
        return *left <= max_start && *right == *left + right_delta;
    }
    while (left != left_span.second && right != right_span.second) {
        if (*left > max_start) {
            return false;
        }
        const uint32_t want = *left + right_delta;
        while (right != right_span.second && *right < want) {
            ++right;
        }
        if (right == right_span.second) {
            return false;
        }
        if (*right == want) {
            return true;
        }
        ++left;
    }
    return false;
}

size_t SelectPhraseVerificationPair(const std::vector<TermPlan>& plans,
                                    const std::vector<size_t>& phrase_plan_index) {
    size_t best_left = 0;
    uint64_t best_score = std::numeric_limits<uint64_t>::max();
    for (size_t left = 0; left + 1 < phrase_plan_index.size(); ++left) {
        const uint64_t score = static_cast<uint64_t>(plans[phrase_plan_index[left]].df) +
                               plans[phrase_plan_index[left + 1]].df;
        if (score < best_score) {
            best_score = score;
            best_left = left;
        }
    }
    return best_left;
}

void CollectTwoTermPhraseStarts(std::pair<const uint32_t*, const uint32_t*> left_span,
                                std::pair<const uint32_t*, const uint32_t*> right_span,
                                uint32_t right_delta, uint32_t left_offset,
                                std::vector<uint32_t>& starts) {
    starts.clear();
    const uint32_t* left = left_span.first;
    const uint32_t* right = right_span.first;
    const uint32_t max_left = std::numeric_limits<uint32_t>::max() - right_delta;
    while (left != left_span.second && right != right_span.second) {
        if (*left > max_left) {
            return;
        }
        const uint32_t want = *left + right_delta;
        while (right != right_span.second && *right < want) {
            ++right;
        }
        if (right == right_span.second) {
            return;
        }
        if (*right == want && *left >= left_offset) {
            starts.push_back(*left - left_offset);
        }
        ++left;
    }
}

Status EmitTwoTermPhraseStreaming(const std::vector<size_t>& phrase_plan_index,
                                  const std::vector<uint32_t>& position_offsets,
                                  std::vector<PosSource>& srcs,
                                  const std::vector<uint32_t>& candidates,
                                  std::vector<uint32_t>* docids) {
    const size_t left_plan = phrase_plan_index[0];
    const size_t right_plan = phrase_plan_index[1];
    const uint32_t right_delta = position_offsets[1] - position_offsets[0];

    if (left_plan == right_plan) {
        PostingCursor cursor;
        cursor.init(&srcs[left_plan]);
        for (uint32_t expected_docid : candidates) {
            uint32_t docid = 0;
            std::pair<const uint32_t*, const uint32_t*> span;
            RETURN_IF_ERROR(cursor.next(&docid, &span));
            if (docid != expected_docid) {
                return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                        "phrase_query: repeated-term cursor/docid mismatch");
            }
            if (ContainsTwoTermPhrase(span, span, right_delta)) {
                docids->push_back(docid);
            }
        }
        return Status::OK();
    }

    PostingCursor left_cursor;
    PostingCursor right_cursor;
    left_cursor.init(&srcs[left_plan]);
    right_cursor.init(&srcs[right_plan]);
    for (uint32_t expected_docid : candidates) {
        uint32_t left_docid = 0;
        uint32_t right_docid = 0;
        std::pair<const uint32_t*, const uint32_t*> left_span;
        std::pair<const uint32_t*, const uint32_t*> right_span;
        RETURN_IF_ERROR(left_cursor.next(&left_docid, &left_span));
        RETURN_IF_ERROR(right_cursor.next(&right_docid, &right_span));
        if (left_docid != expected_docid || right_docid != expected_docid) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "phrase_query: two-term cursor/docid mismatch");
        }
        if (ContainsTwoTermPhrase(left_span, right_span, right_delta)) {
            docids->push_back(expected_docid);
        }
    }
    return Status::OK();
}

void EmitTwoTermPhraseChunkPair(const PosChunk& left, const PosChunk& right,
                                const PosChunkDecoder& left_decoder,
                                const PosChunkDecoder& right_decoder, uint32_t right_delta,
                                std::vector<uint32_t>& docids) {
    size_t li = static_cast<size_t>(std::ranges::lower_bound(left.docids, right.docids.front()) -
                                    left.docids.begin());
    size_t ri = static_cast<size_t>(std::ranges::lower_bound(right.docids, left.docids.front()) -
                                    right.docids.begin());
    while (li < left.docids.size() && ri < right.docids.size()) {
        const uint32_t left_docid = left.docids[li];
        const uint32_t right_docid = right.docids[ri];
        if (left_docid < right_docid) {
            ++li;
            continue;
        }
        if (right_docid < left_docid) {
            ++ri;
            continue;
        }

        const std::pair<const uint32_t*, const uint32_t*> left_span =
                left_decoder.positions_unchecked(li);
        const std::pair<const uint32_t*, const uint32_t*> right_span =
                right_decoder.positions_unchecked(ri);
        if (ContainsTwoTermPhrase(left_span, right_span, right_delta)) {
            docids.push_back(left_docid);
        }
        ++li;
        ++ri;
    }
}

Status EmitTwoTermPhraseChunkMerge(const std::vector<size_t>& phrase_plan_index,
                                   const std::vector<uint32_t>& position_offsets,
                                   std::vector<PosSource>& srcs,
                                   std::vector<uint32_t>* const docids) {
    const size_t left_plan = phrase_plan_index[0];
    const size_t right_plan = phrase_plan_index[1];
    const uint32_t right_delta = position_offsets[1] - position_offsets[0];
    const PosSource& left_src = srcs[left_plan];
    const PosSource& right_src = srcs[right_plan];

    PosChunkDecoder left_decoder;
    PosChunkDecoder right_decoder;
    auto decoded_left_chunk = static_cast<size_t>(-1);
    auto decoded_right_chunk = static_cast<size_t>(-1);
    size_t left_chunk = 0;
    size_t right_chunk = 0;
    while (left_chunk < left_src.chunks.size() && right_chunk < right_src.chunks.size()) {
        const PosChunk& left = left_src.chunks[left_chunk];
        const PosChunk& right = right_src.chunks[right_chunk];
        if (left.docids.empty()) {
            ++left_chunk;
            continue;
        }
        if (right.docids.empty()) {
            ++right_chunk;
            continue;
        }
        if (left.docids.back() < right.docids.front()) {
            ++left_chunk;
            continue;
        }
        if (right.docids.back() < left.docids.front()) {
            ++right_chunk;
            continue;
        }

        if (decoded_left_chunk != left_chunk) {
            RETURN_IF_ERROR(left_decoder.decode(left));
            decoded_left_chunk = left_chunk;
        }
        if (decoded_right_chunk != right_chunk) {
            RETURN_IF_ERROR(right_decoder.decode(right));
            decoded_right_chunk = right_chunk;
        }

        EmitTwoTermPhraseChunkPair(left, right, left_decoder, right_decoder, right_delta, *docids);

        const uint32_t left_last = left.docids.back();
        const uint32_t right_last = right.docids.back();
        if (left_last <= right_last) {
            ++left_chunk;
        }
        if (right_last <= left_last) {
            ++right_chunk;
        }
    }
    return Status::OK();
}

bool PhraseStartMatchesAllTerms(
        uint32_t start, size_t phrase_len, size_t pair_left, size_t pair_right,
        const std::vector<uint32_t>& position_offsets,
        const std::vector<std::pair<const uint32_t*, const uint32_t*>>& span) {
    for (size_t t = 0; t < phrase_len; ++t) {
        if (t == pair_left || t == pair_right) {
            continue;
        }
        uint32_t want = 0;
        if (!internal::add_position_offset(start, position_offsets[t], &want)) {
            return false;
        }
        if (!std::binary_search(span[t].first, span[t].second, want)) {
            return false;
        }
    }
    return true;
}

Status EmitSingleTermPhraseStreaming(const std::vector<size_t>& phrase_plan_index,
                                     std::vector<PosSource>& srcs,
                                     const std::vector<uint32_t>& candidates,
                                     std::vector<uint32_t>* docids) {
    PhrasePositionLoader loader(srcs.size(), srcs);
    for (uint32_t d : candidates) {
        loader.begin_doc(d);
        std::pair<const uint32_t*, const uint32_t*> single_span;
        RETURN_IF_ERROR(loader.positions_for_phrase_pos(phrase_plan_index, 0, &single_span));
        if (single_span.first != single_span.second) {
            docids->push_back(d);
        }
    }
    return Status::OK();
}

Status EmitMultiTermPhraseStreaming(const std::vector<TermPlan>& plans,
                                    const std::vector<size_t>& phrase_plan_index,
                                    const std::vector<uint32_t>& position_offsets,
                                    std::vector<PosSource>& srcs,
                                    const std::vector<uint32_t>& candidates,
                                    std::vector<uint32_t>* docids) {
    const size_t phrase_len = phrase_plan_index.size();
    PhrasePositionLoader loader(plans.size(), srcs);
    std::vector<std::pair<const uint32_t*, const uint32_t*>> span(phrase_len);
    std::vector<uint32_t> starts;
    const size_t pair_left = SelectPhraseVerificationPair(plans, phrase_plan_index);
    const size_t pair_right = pair_left + 1;
    for (uint32_t d : candidates) {
        loader.begin_doc(d);
        std::pair<const uint32_t*, const uint32_t*> left_span;
        std::pair<const uint32_t*, const uint32_t*> right_span;
        RETURN_IF_ERROR(loader.positions_for_phrase_pos(phrase_plan_index, pair_left, &left_span));
        RETURN_IF_ERROR(
                loader.positions_for_phrase_pos(phrase_plan_index, pair_right, &right_span));

        CollectTwoTermPhraseStarts(left_span, right_span,
                                   position_offsets[pair_right] - position_offsets[pair_left],
                                   position_offsets[pair_left], starts);
        if (starts.empty()) {
            continue;
        }

        span[pair_left] = left_span;
        span[pair_right] = right_span;
        for (size_t pp = 0; pp < phrase_len; ++pp) {
            if (pp == pair_left || pp == pair_right) {
                continue;
            }
            RETURN_IF_ERROR(loader.positions_for_phrase_pos(phrase_plan_index, pp, &span[pp]));
        }

        for (uint32_t start : starts) {
            if (PhraseStartMatchesAllTerms(start, phrase_len, pair_left, pair_right,
                                           position_offsets, span)) {
                docids->push_back(d);
                break;
            }
        }
    }
    return Status::OK();
}

// Single streaming pass over the candidates: for each (ascending) candidate,
// gather positions lazily, and test the consecutive-phrase predicate
// (term[0]@p, term[1]@p+1, ...). Multi-term phrases first test the cheapest
// adjacent pair by df before decoding the remaining terms for that document.
// Cursors decode each retained chunk at most once and address positions by
// local index -- no per-candidate docid binary search, no full-candidate
// position materialization. Candidates are ascending so the emitted docids are
// already sorted.
Status EmitPhraseStreaming(const std::vector<TermPlan>& plans,
                           const std::vector<size_t>& phrase_plan_index,
                           const std::vector<uint32_t>& position_offsets,
                           std::vector<PosSource>& srcs, const std::vector<uint32_t>& candidates,
                           std::vector<uint32_t>* docids) {
    const size_t phrase_len = phrase_plan_index.size();
    if (phrase_len == 1) {
        return EmitSingleTermPhraseStreaming(phrase_plan_index, srcs, candidates, docids);
    }
    if (phrase_len == 2) {
        if (phrase_plan_index[0] != phrase_plan_index[1]) {
            return EmitTwoTermPhraseChunkMerge(phrase_plan_index, position_offsets, srcs, docids);
        }
        return EmitTwoTermPhraseStreaming(phrase_plan_index, position_offsets, srcs, candidates,
                                          docids);
    }
    return EmitMultiTermPhraseStreaming(plans, phrase_plan_index, position_offsets, srcs,
                                        candidates, docids);
}

Status BuildPhraseExecutionState(const LogicalIndexReader& idx, io::BatchRangeFetcher* round1,
                                 std::vector<TermPlan>* plans, PhraseExecutionState* state) {
    if (round1->pending() > 0) {
        RETURN_IF_ERROR(round1->fetch());
    }
    RETURN_IF_ERROR(internal::open_preludes(*round1, plans,
                                            /*need_positions=*/true));

    state->owners.clear();
    state->candidates.clear();
    std::vector<DocidSource> doc_sources;
    RETURN_IF_ERROR(internal::build_docid_only_conjunction(idx, *round1, *plans, &state->candidates,
                                                           &doc_sources));
    if (state->candidates.empty()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(BuildPositionSourcesForCandidates(
            idx, *round1, *plans, &doc_sources, state->candidates, &state->owners, &state->srcs));
    return Status::OK();
}

Status ExecutePhrasePlans(const LogicalIndexReader& idx, io::BatchRangeFetcher* round1,
                          std::vector<TermPlan>* plans,
                          const std::vector<size_t>& phrase_plan_index,
                          std::vector<uint32_t>* docids) {
    PhraseExecutionState state;
    RETURN_IF_ERROR(BuildPhraseExecutionState(idx, round1, plans, &state));
    if (state.candidates.empty()) {
        return Status::OK();
    }

    std::vector<uint32_t> position_offsets;
    if (!internal::build_position_offsets(phrase_plan_index.size(), &position_offsets)) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "phrase_query: phrase length exceeds doc position range");
    }
    return EmitPhraseStreaming(*plans, phrase_plan_index, position_offsets, state.srcs,
                               state.candidates, docids);
}

Status ExecuteResolvedPhraseTerms(const LogicalIndexReader& idx,
                                  const std::vector<ResolvedQueryTerm>& terms,
                                  std::vector<uint32_t>* docids) {
    io::BatchRangeFetcher round1(idx.reader());
    std::vector<TermPlan> plans;
    RETURN_IF_ERROR(internal::plan_resolved_terms(idx, terms, &round1, &plans,
                                                  /*need_positions=*/false));
    std::vector<size_t> phrase_plan_index(terms.size());
    std::iota(phrase_plan_index.begin(), phrase_plan_index.end(), 0);
    return ExecutePhrasePlans(idx, &round1, &plans, phrase_plan_index, docids);
}

Status CollectExpectedTailPositions(const std::vector<TermPlan>& plans,
                                    const std::vector<uint32_t>& position_offsets,
                                    std::vector<PosSource>& srcs,
                                    const std::vector<uint32_t>& candidates,
                                    ExpectedTailPositionSet* out) {
    const size_t n = plans.size();
    std::vector<PostingCursor> cur(n);
    for (size_t i = 0; i < n; ++i) {
        cur[i].init(&srcs[i]);
    }

    std::vector<PostingCursor*> ordered(n);
    for (size_t i = 0; i < n; ++i) {
        ordered[plans[i].order] = &cur[i];
    }

    std::vector<std::pair<const uint32_t*, const uint32_t*>> span(n);
    for (uint32_t d : candidates) {
        for (size_t i = 0; i < n; ++i) {
            RETURN_IF_ERROR(cur[i].seek(d));
        }
        for (size_t pp = 0; pp < n; ++pp) {
            RETURN_IF_ERROR(ordered[pp]->positions(&span[pp]));
        }

        // Anchor the outer enumeration on the SPARSEST exact term (smallest
        // per-doc position span), not the hardcoded phrase-position-0 term. The
        // set of valid phrase starts is anchor-independent -- each valid start
        // maps 1:1 to exactly one anchor position (anchor_pos = start +
        // offset[anchor]) -- so enumerating the shortest span and binary-searching
        // the others yields the identical result set with the fewest outer
        // iterations. A leading high-frequency exact term no longer forces
        // O(|span[0]|) work per candidate doc.
        size_t anchor = 0;
        auto best = static_cast<size_t>(span[0].second - span[0].first);
        for (size_t t = 1; t < n; ++t) {
            const auto sz = static_cast<size_t>(span[t].second - span[t].first);
            if (sz < best) {
                best = sz;
                anchor = t;
            }
        }
        const uint32_t anchor_off = position_offsets[anchor];
        SNII_QUERY_ADD(anchor_iterations, best);

        const size_t expected_begin = out->positions.size();
        for (const uint32_t* p = span[anchor].first; p != span[anchor].second; ++p) {
            const uint32_t anchor_pos = *p;
            // Underflow guard: a general anchor (offset > 0) can sit at a position
            // smaller than its offset, which would wrap `start`. Such a position
            // admits no valid phrase start and is skipped. (The old span[0] anchor
            // had offset 0 and could never underflow.)
            if (anchor_pos < anchor_off) {
                continue;
            }
            const uint32_t start = anchor_pos - anchor_off;
            bool ok = true;
            for (size_t t = 0; t < n; ++t) {
                if (t == anchor) {
                    continue; // the anchor term's position is satisfied by construction
                }
                uint32_t want = 0;
                if (!internal::add_position_offset(start, position_offsets[t], &want)) {
                    ok = false;
                    break;
                }
                if (!std::binary_search(span[t].first, span[t].second, want)) {
                    ok = false;
                    break;
                }
            }
            uint32_t tail_pos = 0;
            if (ok && internal::add_position_offset(start, position_offsets[n], &tail_pos)) {
                out->positions.push_back(tail_pos);
            }
        }
        const size_t expected_end = out->positions.size();
        if (expected_end != expected_begin) {
            out->docs.push_back({d, expected_begin, expected_end});
        }
    }
    return Status::OK();
}

Status CollectSingleTermExpectedTailPositions(std::vector<PosSource>& srcs,
                                              const std::vector<uint32_t>& candidates,
                                              uint32_t tail_offset, ExpectedTailPositionSet* out) {
    PostingCursor cursor;
    cursor.init(srcs.data());
    out->reserve_docs(out->docs.size() + candidates.size());

    for (uint32_t d : candidates) {
        RETURN_IF_ERROR(cursor.seek(d));
        std::pair<const uint32_t*, const uint32_t*> span;
        RETURN_IF_ERROR(cursor.positions(&span));

        const size_t expected_begin = out->positions.size();
        for (const uint32_t* p = span.first; p != span.second; ++p) {
            uint32_t tail_pos = 0;
            if (internal::add_position_offset(*p, tail_offset, &tail_pos)) {
                out->positions.push_back(tail_pos);
            }
        }
        const size_t expected_end = out->positions.size();
        if (expected_end != expected_begin) {
            out->docs.push_back({d, expected_begin, expected_end});
        }
    }
    return Status::OK();
}

Status CollectExpectedTailPositions(const LogicalIndexReader& idx,
                                    const std::vector<ResolvedQueryTerm>& exact_terms,
                                    ExpectedTailPositionSet* out) {
    out->clear();
    io::BatchRangeFetcher round1(idx.reader());
    std::vector<TermPlan> plans;
    RETURN_IF_ERROR(internal::plan_resolved_terms(idx, exact_terms, &round1, &plans,
                                                  /*need_positions=*/false));

    PhraseExecutionState state;
    RETURN_IF_ERROR(BuildPhraseExecutionState(idx, &round1, &plans, &state));
    if (state.candidates.empty()) {
        return Status::OK();
    }
    out->reserve_docs(state.candidates.size());
    std::vector<uint32_t> position_offsets;
    if (!internal::build_position_offsets(plans.size() + 1, &position_offsets)) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "phrase_prefix_query: phrase length exceeds doc position range");
    }
    if (plans.size() == 1) {
        return CollectSingleTermExpectedTailPositions(state.srcs, state.candidates,
                                                      position_offsets[1], out);
    }
    return CollectExpectedTailPositions(plans, position_offsets, state.srcs, state.candidates, out);
}

bool contains_any_position(const ExpectedTailPositionSet& expected,
                           const ExpectedTailPositions& wanted,
                           std::pair<const uint32_t*, const uint32_t*> actual) {
    for (size_t i = wanted.positions_begin; i < wanted.positions_end; ++i) {
        if (std::binary_search(actual.first, actual.second, expected.positions[i])) {
            return true;
        }
    }
    return false;
}

// `expected_docids` is the ascending docid projection of `expected.docs`. It is
// invariant across every tail expansion (it depends only on the const `expected`
// set, never on `tail`), so the caller builds it ONCE and passes it in by
// const-ref rather than rebuilding it per tail hit inside this function.
Status CollectTailMatchesAtExpectedPositions(const LogicalIndexReader& idx,
                                             const ResolvedQueryTerm& tail,
                                             const ExpectedTailPositionSet& expected,
                                             const std::vector<uint32_t>& expected_docids,
                                             std::vector<uint32_t>* out) {
    if (expected.docs.empty()) {
        return Status::OK();
    }

    io::BatchRangeFetcher round1(idx.reader());
    std::vector<TermPlan> plans;
    RETURN_IF_ERROR(internal::plan_resolved_terms(idx, {tail}, &round1, &plans,
                                                  /*need_positions=*/false));

    if (round1.pending() > 0) {
        RETURN_IF_ERROR(round1.fetch());
    }
    RETURN_IF_ERROR(internal::open_preludes(round1, &plans,
                                            /*need_positions=*/true));

    std::vector<uint32_t> tail_candidates;
    std::vector<DocidSource> doc_sources;
    RETURN_IF_ERROR(internal::filter_docids_by_conjunction(idx, round1, plans, expected_docids,
                                                           &tail_candidates, &doc_sources));
    if (tail_candidates.empty()) {
        return Status::OK();
    }

    std::vector<std::unique_ptr<io::BatchRangeFetcher>> owners;
    std::vector<PosSource> srcs;
    RETURN_IF_ERROR(BuildPositionSourcesForCandidates(idx, round1, plans, &doc_sources,
                                                      tail_candidates, &owners, &srcs));

    PostingCursor cursor;
    cursor.init(srcs.data());
    size_t ei = 0;
    size_t ti = 0;
    while (ei < expected.docs.size() && ti < tail_candidates.size()) {
        const uint32_t want_doc = expected.docs[ei].docid;
        const uint32_t tail_doc = tail_candidates[ti];
        if (want_doc < tail_doc) {
            ++ei;
            continue;
        }
        if (tail_doc < want_doc) {
            ++ti;
            continue;
        }

        RETURN_IF_ERROR(cursor.seek(want_doc));
        std::pair<const uint32_t*, const uint32_t*> actual;
        RETURN_IF_ERROR(cursor.positions(&actual));
        if (contains_any_position(expected, expected.docs[ei], actual)) {
            out->push_back(want_doc);
        }
        ++ei;
        ++ti;
    }
    return Status::OK();
}

} // namespace

Status phrase_query(const LogicalIndexReader& idx, const std::vector<std::string>& terms,
                    std::vector<uint32_t>* const docids) {
    if (docids == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("phrase_query: null out");
    }
    docids->clear();
    if (terms.empty()) {
        return Status::OK();
    }
    if (terms.size() == 1) {
        return term_query(idx, terms.front(), docids);
    }
    if (!idx.has_positions()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED, false>(
                "phrase_query: index has no positions");
    }
    bool handled_by_bigram = false;
    RETURN_IF_ERROR(TryTwoTermPhraseBigram(idx, terms, docids, &handled_by_bigram));
    if (handled_by_bigram) {
        return Status::OK();
    }

    // Round 1: preludes (windowed) + docid postings (slim/inline) batched
    // together. Positions are fetched after the docid-only conjunction has
    // produced final candidates, so phrase verification does not read PRX for
    // windows later removed by the docid intersection.
    io::BatchRangeFetcher round1(idx.reader());
    const PhraseTermMapping mapping = BuildPhraseTermMapping(terms);
    std::vector<TermPlan> plans;
    bool all_present = false;
    RETURN_IF_ERROR(internal::plan_terms(idx, mapping.unique_terms, &round1, &plans, &all_present,
                                         /*need_positions=*/false));
    if (!all_present) {
        return Status::OK();
    }
    return ExecutePhrasePlans(idx, &round1, &plans, mapping.phrase_plan_index, docids);
}

Status phrase_query(const LogicalIndexReader& idx, const std::vector<std::string>& terms,
                    std::vector<uint32_t>* const docids, QueryProfile* profile) {
    QueryProfileScope profile_scope(idx.reader(), profile);
    return phrase_query(idx, terms, docids);
}

Status phrase_prefix_query(const LogicalIndexReader& idx, const std::vector<std::string>& terms,
                           std::vector<uint32_t>* const docids, int32_t max_expansions) {
    if (docids == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("phrase_prefix_query: null out");
    }
    docids->clear();
    if (terms.empty()) {
        return Status::OK();
    }
    if (terms.size() == 1) {
        return prefix_query(idx, terms.front(), docids, max_expansions);
    }
    if (!idx.has_positions()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED, false>(
                "phrase_prefix_query: index has no positions");
    }
    std::vector<ResolvedQueryTerm> exact_terms;
    exact_terms.reserve(terms.size() - 1);
    for (size_t i = 0; i + 1 < terms.size(); ++i) {
        ResolvedQueryTerm resolved;
        bool found = false;
        RETURN_IF_ERROR(internal::resolve_query_term(idx, terms[i], &resolved, &found));
        if (!found) {
            return Status::OK();
        }
        exact_terms.push_back(std::move(resolved));
    }

    // Expand the tail prefix over REAL terms only: hidden phrase-bigram dict
    // terms (and the sentinel) are rejected DURING enumeration so they never
    // consume a max_expansions slot. The previous prefix_terms(max_expansions) +
    // erase_if ordering counted hidden terms against the expansion budget FIRST
    // and dropped them AFTER, so every hidden term inside the prefix range
    // silently displaced one real tail past the truncated window -- and because
    // G01 df-pruning changes how many hidden bigram terms a segment
    // materializes, the surviving tail set (and thus the result) depended on
    // the segment's bigram layout instead of the query. Bigram postings are
    // NEVER consulted as tail evidence here, on pruned or legacy segments
    // alike: every expanded tail below is verified against UNIGRAM postings and
    // positions (single-tail via the generic streaming phrase path, multi-tail
    // via CollectTailMatchesAtExpectedPositions), which G01's diet leaves
    // untouched.
    std::vector<LogicalIndexReader::PrefixHit> tail_hits;
    RETURN_IF_ERROR(idx.visit_prefix_terms(terms.back(), [&](LogicalIndexReader::PrefixHit&& hit,
                                                             bool* stop) {
        if (format::is_phrase_bigram_term(hit.term)) {
            return Status::OK(); // hidden term: never a tail, never a slot
        }
        tail_hits.push_back(std::move(hit));
        *stop = max_expansions > 0 && tail_hits.size() >= static_cast<size_t>(max_expansions);
        return Status::OK();
    }));
    if (tail_hits.empty()) {
        return Status::OK();
    }
    if (tail_hits.size() == 1) {
        std::vector<ResolvedQueryTerm> resolved_terms = exact_terms;
        resolved_terms.push_back(ResolvedQueryTerm {.entry = std::move(tail_hits.front().entry),
                                                    .frq_base = tail_hits.front().frq_base,
                                                    .prx_base = tail_hits.front().prx_base});
        return ExecuteResolvedPhraseTerms(idx, resolved_terms, docids);
    }

    ExpectedTailPositionSet expected;
    RETURN_IF_ERROR(CollectExpectedTailPositions(idx, exact_terms, &expected));
    if (expected.docs.empty()) {
        return Status::OK();
    }

    // Hoist the expected-docid projection out of the per-tail loop: it depends only
    // on the (const) expected set, is byte-identical for every tail expansion, and
    // is ascending because expected.docs derives from ascending candidates -- which
    // satisfies filter_docids_by_conjunction's sorted-input contract.
    std::vector<uint32_t> expected_docids;
    expected_docids.reserve(expected.docs.size());
    for (const ExpectedTailPositions& doc : expected.docs) {
        expected_docids.push_back(doc.docid);
    }
    SNII_QUERY_COUNT(expected_docids_build);

    std::vector<uint32_t> acc;
    for (LogicalIndexReader::PrefixHit& hit : tail_hits) {
        ResolvedQueryTerm tail {
                .entry = std::move(hit.entry), .frq_base = hit.frq_base, .prx_base = hit.prx_base};
        std::vector<uint32_t> tail_docs;
        RETURN_IF_ERROR(CollectTailMatchesAtExpectedPositions(idx, tail, expected, expected_docids,
                                                              &tail_docs));
        internal::union_sorted_into(&acc, tail_docs);
    }
    *docids = std::move(acc);
    return Status::OK();
}

Status phrase_prefix_query(const LogicalIndexReader& idx, const std::vector<std::string>& terms,
                           std::vector<uint32_t>* const docids, QueryProfile* profile,
                           int32_t max_expansions) {
    QueryProfileScope profile_scope(idx.reader(), profile);
    return phrase_prefix_query(idx, terms, docids, max_expansions);
}

} // namespace doris::snii::query
