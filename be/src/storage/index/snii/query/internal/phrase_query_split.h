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

#include <algorithm>
#include <atomic>
#include <bit>
#include <chrono>
#include <cstdint>
#include <iterator>
#include <limits>
#include <memory>
#include <span>
#include <utility>
#include <vector>

#include "common/check.h"
#include "storage/index/inverted/common_grams/common_grams_key_codec.h"
#include "storage/index/inverted/common_grams/common_grams_query_cost.h"
#include "storage/index/inverted/common_grams/common_grams_segment_metadata.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/format/frq_pod.h"
#include "storage/index/snii/format/frq_prelude.h"
#include "storage/index/snii/format/prx_pod.h"
#include "storage/index/snii/io/batch_range_fetcher.h"
#include "storage/index/snii/query/internal/docid_conjunction.h"
#include "storage/index/snii/query/internal/docid_posting_reader.h"
#include "storage/index/snii/query/internal/docid_set_ops.h"
#include "storage/index/snii/query/internal/docid_union.h"
#include "storage/index/snii/query/internal/plain_term_routing.h"
#include "storage/index/snii/query/internal/position_math.h"
#include "storage/index/snii/query/internal/query_test_counters.h"
#include "storage/index/snii/query/internal/resolved_phrase_plan.h"
#include "storage/index/snii/query/internal/term_expansion.h"
#include "storage/index/snii/query/phrase_prx_validation.h"
#include "storage/index/snii/query/phrase_query.h"
#include "storage/index/snii/query/phrase_verify_timer.h"
#include "storage/index/snii/query/prefix_query.h"
#include "storage/index/snii/query/term_query.h"
#include "storage/index/snii/reader/windowed_posting.h"
#include "util/debug_points.h"

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
//
// Internal to the phrase-query implementation, which spans phrase_plan.cpp,
// phrase_position_source.cpp, phrase_emit.cpp, phrase_prefix_exec.cpp,
// phrase_planned_query.cpp and phrase_query.cpp. This header carries the types and
// functions those translation units share; nothing outside query/ may include it.
namespace doris::snii::query::phrase_impl {

struct PosSource;

using query::internal::DocidChunk;
using query::internal::DocidSource;
using query::internal::ResolvedQueryTerm;
using query::internal::TermPlan;
using reader::LogicalIndexReader;

bool apply_common_grams_plan_debug_override(bool cost_prefers_gram,
                                            CommonGramsPlanDebugOverride debug_override);

bool should_use_streaming_exact_phrase(const std::vector<TermPlan>& plans,
                                       const std::vector<PosSource>& sources,
                                       std::span<const size_t> phrase_plan_index,
                                       size_t candidate_count, bool needs_frequency,
                                       const PhraseQueryOptions& options,
                                       internal::ExactPhrasePositionAccess position_access);

class CommonGramsPlanningTimer {
public:
    explicit CommonGramsPlanningTimer(format::PhraseQueryExecutionStats* stats) : stats_(stats) {
        if (stats_ != nullptr) {
            start_ = std::chrono::steady_clock::now();
        }
    }

    ~CommonGramsPlanningTimer() { finish(); }

    CommonGramsPlanningTimer(const CommonGramsPlanningTimer&) = delete;
    CommonGramsPlanningTimer& operator=(const CommonGramsPlanningTimer&) = delete;

    void finish() {
        if (finished_) {
            return;
        }
        finished_ = true;
        if (stats_ == nullptr) {
            return;
        }
        const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                     std::chrono::steady_clock::now() - start_)
                                     .count();
        stats_->common_grams_planning_ns += static_cast<uint64_t>(std::max<int64_t>(1, elapsed));
    }

private:
    format::PhraseQueryExecutionStats* stats_ = nullptr;
    std::chrono::steady_clock::time_point start_;
    bool finished_ = false;
};

size_t position_span_size(std::pair<const uint32_t*, const uint32_t*> span);

bool should_use_monotonic_position_scan(std::pair<const uint32_t*, const uint32_t*> anchor_span,
                                        size_t checked_span_size, uint32_t anchor_offset,
                                        uint32_t checked_offset);

struct ExpectedTailPositions {
    uint32_t docid = 0;
    uint32_t phrase_frequency = 0;
    size_t positions_begin = 0;
    size_t positions_end = 0;
};

static_assert(sizeof(ExpectedTailPositions) == 3 * sizeof(uint64_t));

struct ExpectedTailPositionSet {
    std::vector<ExpectedTailPositions> docs;
    std::vector<uint32_t> positions;
    std::vector<uint8_t> position_matched;
    size_t matched_count = 0;

    void clear() {
        docs.clear();
        positions.clear();
        position_matched.clear();
        matched_count = 0;
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
    format::PrxDecodeContext* observer_context = nullptr;
    uint64_t logical_position_work = 0;
    uint64_t logical_position_docs = 0;
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

struct PhysicalPhrasePlan {
    std::vector<std::string> unique_terms;
    std::vector<size_t> phrase_plan_index;
    std::vector<uint32_t> position_offsets;
    std::vector<uint8_t> common_gram_clauses;
};

bool has_common_grams_capability(
        const LogicalIndexReader& idx,
        const segment_v2::inverted_index::CommonGramsQueryIdentity* query_identity);

bool entry_has_positions(const format::DictEntry& entry);

Status build_physical_phrase_plan_prefix(const LogicalIndexReader& idx,
                                         const segment_v2::InvertedIndexQueryInfo& query_info,
                                         size_t clause_count, bool allow_common_grams,
                                         PhysicalPhrasePlan* plan, bool* all_representable);

Status build_physical_phrase_plan(const LogicalIndexReader& idx,
                                  const segment_v2::InvertedIndexQueryInfo& query_info,
                                  bool allow_common_grams, PhysicalPhrasePlan* plan,
                                  bool* all_representable);

size_t resolved_batch_index(const std::vector<std::string>& batch_terms, std::string_view term);

bool all_plan_terms_present(const PhysicalPhrasePlan& plan,
                            const std::vector<std::string>& batch_terms,
                            const std::vector<uint8_t>& found);

uint64_t plan_visible_posting_bytes(const format::DictEntry& entry, bool need_positions);

segment_v2::inverted_index::CommonGramsPlanRawCost phrase_plan_raw_cost(
        const PhysicalPhrasePlan& plan, const std::vector<std::string>& batch_terms,
        const std::vector<ResolvedQueryTerm>& resolved, const std::vector<uint8_t>& found,
        bool need_positions);

segment_v2::inverted_index::CommonGramsPlanRawCost alternative_clause_raw_cost(
        const std::vector<ResolvedQueryTerm>& terms, bool need_positions);

void append_alternative_clause_cost(
        const segment_v2::inverted_index::CommonGramsPlanRawCost& clause,
        segment_v2::inverted_index::CommonGramsPlanRawCost* plan);

segment_v2::inverted_index::CommonGramsPlanRawCost hybrid_verification_raw_cost(
        const segment_v2::inverted_index::CommonGramsPlanRawCost& prefilter_cost,
        const segment_v2::inverted_index::CommonGramsPlanRawCost& verification_cost);

internal::ResolvedPhrasePlan materialize_resolved_phrase_plan(
        const PhysicalPhrasePlan& plan, const std::vector<std::string>& batch_terms,
        std::vector<ResolvedQueryTerm>* resolved);

internal::ResolvedPhrasePlan copy_resolved_phrase_plan(
        const PhysicalPhrasePlan& plan, const std::vector<std::string>& batch_terms,
        const std::vector<ResolvedQueryTerm>& resolved);

bool physical_phrase_plan_has_docs_only_term(const PhysicalPhrasePlan& plan,
                                             const std::vector<std::string>& batch_terms,
                                             const std::vector<ResolvedQueryTerm>& resolved);

void append_physical_phrase_clause(const PhysicalPhrasePlan& source, size_t clause,
                                   uint32_t position_offset, PhysicalPhrasePlan* target);

struct HybridPositionedCover {
    PhysicalPhrasePlan candidate_prefilter;
    PhysicalPhrasePlan verification;
};

struct HybridExactPlanArtifact {
    std::optional<HybridPositionedCover> positioned_cover;
};

HybridExactPlanArtifact build_hybrid_exact_plan_artifact(
        const PhysicalPhrasePlan& plain_plan, const PhysicalPhrasePlan& gram_plan,
        const std::vector<std::string>& batch_terms,
        const std::vector<ResolvedQueryTerm>& resolved);

struct ResolvedMappedTail {
    size_t batch_index = 0;
    uint32_t expansion_ordinal = 0;
};

struct HybridPrefixMappedTails {
    std::vector<size_t> positioned_indices;
    std::vector<size_t> docs_only_indices;
    std::vector<uint32_t> docs_only_ordinals;
};

struct HybridPrefixPlanArtifact {
    HybridPositionedCover plain_tail_cover;
    HybridPrefixMappedTails mapped_tail_split;
    std::optional<PhysicalPhrasePlan> positioned_tail_verification;
    uint32_t plain_tail_position_offset = 0;
    bool maps_tail_to_gram = false;
};

std::optional<HybridPrefixPlanArtifact> try_build_hybrid_prefix_plan_artifact(
        const PhysicalPhrasePlan& plain_leading, const PhysicalPhrasePlan& gram_leading,
        const std::vector<std::string>& batch_terms, const std::vector<ResolvedQueryTerm>& resolved,
        const std::vector<ResolvedMappedTail>& mapped_tails, bool maps_tail_to_gram);

struct HybridPrefixCandidateSet {
    bool active = false;
    std::vector<uint32_t> docs;
};

Status build_hybrid_leading_candidates(const LogicalIndexReader& idx,
                                       const PhysicalPhrasePlan& candidate_prefilter,
                                       const std::vector<std::string>& batch_terms,
                                       const std::vector<ResolvedQueryTerm>& resolved,
                                       HybridPrefixCandidateSet* candidates);

Status build_hybrid_docs_only_tail_candidates(const LogicalIndexReader& idx,
                                              const std::vector<ResolvedQueryTerm>& resolved,
                                              const std::vector<size_t>& gram_tail_indices,
                                              const HybridPrefixCandidateSet& leading_candidates,
                                              std::vector<uint32_t>* candidates);

Status execute_hybrid_exact_phrase_plan(
        const LogicalIndexReader& idx, const PhysicalPhrasePlan& gram_plan,
        const std::vector<std::string>& batch_terms, const HybridExactPlanArtifact& artifact,
        std::vector<ResolvedQueryTerm>* resolved, std::vector<uint32_t>* docids,
        format::PrxDecodeContext* decode_context, bool* candidate_intersection_empty = nullptr);

void append_resolved_phrase_clause(ResolvedQueryTerm term, uint32_t position_offset,
                                   internal::ResolvedPhrasePlan* plan);

internal::ResolvedPhrasePlan build_resolved_phrase_plan(
        std::vector<ResolvedQueryTerm> resolved_terms);

Status resolve_and_execute_physical_phrase_plan(const LogicalIndexReader& idx,
                                                const PhysicalPhrasePlan& plan,
                                                std::vector<uint32_t>* docids,
                                                format::PrxDecodeContext* decode_context,
                                                CommonGramsPlanningTimer& planning_timer);

Status planned_exact_phrase_query_impl(
        const LogicalIndexReader& idx, const segment_v2::InvertedIndexQueryInfo& plain_query_info,
        const segment_v2::InvertedIndexQueryInfo& gram_query_info,
        const segment_v2::inverted_index::CommonGramsQueryIdentity* common_grams_identity,
        std::vector<uint32_t>* docids, format::PrxDecodeContext* decode_context,
        ExactPhrasePlanKind* selected_plan,
        segment_v2::inverted_index::CommonGramsPlanCostModel cost_model,
        CommonGramsPlanDebugOverride debug_override);

PhraseTermMapping build_phrase_term_mapping(const std::vector<std::string>& terms);

Status build_position_sources_for_candidates(
        const LogicalIndexReader& idx, const io::BatchRangeFetcher& round1,
        const std::vector<TermPlan>& plans, std::vector<DocidSource>* doc_sources,
        const std::vector<uint32_t>& candidates,
        std::vector<std::unique_ptr<io::BatchRangeFetcher>>* owners, std::vector<PosSource>* srcs,
        format::PrxDecodeContext* observer_context);

class PosChunkDecoder {
public:
    explicit PosChunkDecoder(format::PrxDecodeContext* observer_context = nullptr)
            : observer_context_(observer_context) {}

    void set_decode_state(format::PrxDecodeContext* observer_context) {
        observer_context_ = observer_context;
    }

    void reset() {
        chunk_ = nullptr;
        offsets_by_prx_ordinal_ = false;
    }

    Status decode(const PosChunk& chunk) {
        chunk_ = &chunk;
        ByteSource ps(chunk.prx);
        const bool selected_all = chunk.prx_doc_ordinals.empty();
        const bool decode_full = selected_all || should_decode_full_prx_window(chunk);
        offsets_by_prx_ordinal_ = decode_full && !selected_all;
        return internal::decode_and_validate_prx_frame(
                &ps, chunk.prx_doc_ordinals, decode_full, selected_all, chunk.prx_doc_count,
                chunk.docids.size(), &pflat_, &poff_, observer_context_);
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
    format::PrxDecodeContext* observer_context_ = nullptr;
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
        decoder_.set_decode_state(src->observer_context);
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

enum class PhraseCandidateMetric : uint8_t {
    kExact,
    kPrefixLeading,
};

Status build_phrase_execution_state(const LogicalIndexReader& idx, io::BatchRangeFetcher* round1,
                                    std::vector<TermPlan>* plans, PhraseExecutionState* state,
                                    const std::vector<uint32_t>* candidate_prefilter,
                                    format::PrxDecodeContext* observer_context,
                                    PhraseCandidateMetric candidate_metric);

Status execute_phrase_plans(const LogicalIndexReader& idx, io::BatchRangeFetcher* round1,
                            std::vector<TermPlan>* plans,
                            const std::vector<size_t>& phrase_plan_index,
                            std::vector<uint32_t>* docids,
                            format::PrxDecodeContext* observer_context,
                            std::vector<PhraseMatch>* matches, const PhraseQueryOptions& options);

Status execute_resolved_phrase_prefix_terms(
        const LogicalIndexReader& idx, internal::ResolvedPhrasePlan exact_plan,
        std::vector<ResolvedQueryTerm> tail_terms, uint32_t tail_position_offset,
        std::vector<uint32_t>* docids, format::PrxDecodeContext* decode_context,
        std::vector<PhraseMatch>* matches = nullptr,
        const std::vector<uint32_t>* candidate_prefilter = nullptr);

Status execute_hybrid_phrase_prefix_plan(
        const LogicalIndexReader& idx, const HybridPrefixPlanArtifact& artifact,
        const std::vector<std::string>& batch_terms, const std::vector<ResolvedQueryTerm>& resolved,
        const std::vector<ResolvedQueryTerm>& plain_tail_terms, std::vector<uint32_t>* docids,
        format::PrxDecodeContext* decode_context, CommonGramsPlanningTimer& planning_timer,
        bool* candidate_intersection_empty);

struct HybridPrefixCostEstimate {
    segment_v2::inverted_index::CommonGramsPlanRawCost raw_cost;
    uint64_t estimated_cost = 0;
};

HybridPrefixCostEstimate estimate_hybrid_prefix_plan_cost(
        const HybridPrefixPlanArtifact& artifact, const std::vector<std::string>& batch_terms,
        const std::vector<ResolvedQueryTerm>& resolved, const std::vector<uint8_t>& found,
        const std::vector<ResolvedQueryTerm>& plain_tail_terms, uint32_t position_verify_factor);

Status phrase_query_impl(const LogicalIndexReader& idx, const std::vector<std::string>& terms,
                         std::vector<uint32_t>* const docids,
                         format::PrxDecodeContext* decode_context,
                         std::vector<PhraseMatch>* matches, const PhraseQueryOptions& options);

Status phrase_prefix_query_impl(const LogicalIndexReader& idx,
                                const std::vector<std::string>& terms,
                                std::vector<uint32_t>* const docids, int32_t max_expansions,
                                format::PrxDecodeContext* decode_context,
                                CommonGramsPlanningTimer* planning_timer,
                                std::vector<PhraseMatch>* matches = nullptr);

Status planned_phrase_prefix_query_impl(
        const LogicalIndexReader& idx, const segment_v2::InvertedIndexQueryInfo& plain_query_info,
        const segment_v2::InvertedIndexQueryInfo& gram_query_info,
        const segment_v2::inverted_index::CommonGramsQueryIdentity* common_grams_identity,
        std::vector<uint32_t>* docids, int32_t max_expansions,
        format::PrxDecodeContext* decode_context, PhrasePrefixPlanKind* selected_plan,
        segment_v2::inverted_index::CommonGramsPlanCostModel cost_model,
        CommonGramsPlanDebugOverride debug_override);

template <typename Index>
segment_v2::inverted_index::CommonGramsPlanRawCost alternative_clause_raw_cost(
        const std::vector<ResolvedQueryTerm>& terms, const std::vector<Index>& indices,
        bool need_positions) {
    segment_v2::inverted_index::CommonGramsPlanRawCost cost;
    unsigned __int128 posting_bytes = 0;
    unsigned __int128 candidate_df = 0;
    for (size_t index : indices) {
        DORIS_CHECK_LT(index, terms.size());
        posting_bytes += plan_visible_posting_bytes(terms[index].entry, need_positions);
        candidate_df += terms[index].entry.df;
    }
    cost.posting_bytes_or_df_sum = posting_bytes > std::numeric_limits<uint64_t>::max()
                                           ? std::numeric_limits<uint64_t>::max()
                                           : static_cast<uint64_t>(posting_bytes);
    cost.estimated_candidate_df = candidate_df > std::numeric_limits<uint64_t>::max()
                                          ? std::numeric_limits<uint64_t>::max()
                                          : static_cast<uint64_t>(candidate_df);
    cost.clause_count = 1;
    return cost;
}

} // namespace doris::snii::query::phrase_impl

#ifdef BE_TEST
namespace doris::snii::query::internal::testing {

uint64_t streaming_exact_phrase_execution_count();
void reset_streaming_exact_phrase_execution_count();
void note_streaming_exact_phrase_execution();

} // namespace doris::snii::query::internal::testing
#endif
