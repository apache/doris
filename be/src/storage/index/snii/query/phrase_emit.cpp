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

#include <algorithm>
#include <atomic>
#include <bit>
#include <chrono>
#include <cstdint>
#include <iterator>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "common/check.h"
#include "roaring/roaring.hh"
#include "storage/index/query/docid_set_ops.h"
#include "storage/index/query/phrase/exact_phrase_matcher.h"
#include "storage/index/query/phrase/exact_phrase_stream_matcher.h"
#include "storage/index/query/phrase/phrase_verifier.h"
#include "storage/index/query/phrase/position_math.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/format/frq_pod.h"
#include "storage/index/snii/format/frq_prelude.h"
#include "storage/index/snii/format/prx_pod.h"
#include "storage/index/snii/format/prx_position_iterator.h"
#include "storage/index/snii/io/batch_range_fetcher.h"
#include "storage/index/snii/query/internal/docid_conjunction.h"
#include "storage/index/snii/query/internal/docid_posting_reader.h"
#include "storage/index/snii/query/internal/docid_union.h"
#include "storage/index/snii/query/internal/phrase_query_split.h"
#include "storage/index/snii/query/internal/plain_term_routing.h"
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

namespace doris::snii::query::phrase_impl {

using query::internal::DocidChunk;
using query::internal::DocidSource;
using query::internal::ResolvedQueryTerm;
using query::internal::TermPlan;
using reader::LogicalIndexReader;
using internal::PhraseVerifyTimer;

bool should_use_streaming_exact_phrase(const std::vector<TermPlan>& plans,
                                       const std::vector<PosSource>& sources,
                                       std::span<const size_t> phrase_plan_index,
                                       size_t candidate_count, bool needs_frequency,
                                       const PhraseQueryOptions& options,
                                       internal::ExactPhrasePositionAccess position_access) {
    constexpr uint64_t kMinMaximumPositionWork = 8;
    constexpr uint64_t kMinEstimatedPositionWork = 512;
    if (position_access == internal::ExactPhrasePositionAccess::kMaterializedOnly ||
        options.slop != 0 || needs_frequency) {
        return false;
    }
    DORIS_CHECK_EQ(plans.size(), sources.size());

    unsigned __int128 sum_position_work = 0;
    uint64_t max_position_work = 0;
    for (size_t clause = 0; clause < phrase_plan_index.size(); ++clause) {
        const size_t plan_index = phrase_plan_index[clause];
        DORIS_CHECK_LT(plan_index, plans.size());
        for (size_t preceding = 0; preceding < clause; ++preceding) {
            if (phrase_plan_index[preceding] == plan_index) {
                return false;
            }
        }
        const TermPlan& plan = plans[plan_index];
        DORIS_CHECK_NE(plan.df, 0);
        // Dictionary entries lack term frequencies; estimate position work from decoded PRX frames.
        DORIS_CHECK_NE(sources[plan_index].logical_position_docs, 0);
        const uint64_t position_work = sources[plan_index].logical_position_work /
                                       sources[plan_index].logical_position_docs;
        sum_position_work += position_work;
        max_position_work = std::max(max_position_work, position_work);
    }
    if (max_position_work < kMinMaximumPositionWork) {
        return false;
    }

    constexpr unsigned __int128 kMaxU128 = ~static_cast<unsigned __int128>(0);
    const auto candidates = static_cast<unsigned __int128>(candidate_count);
    const unsigned __int128 raw_estimate =
            candidates > kMaxU128 / sum_position_work ? kMaxU128 : candidates * sum_position_work;
    const uint64_t estimated_position_work = raw_estimate > std::numeric_limits<uint64_t>::max()
                                                     ? std::numeric_limits<uint64_t>::max()
                                                     : static_cast<uint64_t>(raw_estimate);
    return estimated_position_work >= kMinEstimatedPositionWork;
}

namespace {
class StreamingPostingCursor {
public:
    void init(const PosSource* source) {
        DORIS_CHECK(source != nullptr);
        source_ = source;
        chunk_index_ = 0;
        local_doc_index_ = 0;
        active_frame_ = kNoChunk;
        local_query_stats_ = {};
        if (source_->observer_context != nullptr) {
            iterator_context_ = *source_->observer_context;
            iterator_context_.query_stats = source_->observer_context->query_stats == nullptr
                                                    ? nullptr
                                                    : &local_query_stats_;
        }
    }

    Status seek(uint32_t docid) {
        while (chunk_index_ < source_->chunks.size() &&
               (source_->chunks[chunk_index_].docids.empty() ||
                source_->chunks[chunk_index_].docids.back() < docid)) {
            RETURN_IF_ERROR(finish_active_frame());
            ++chunk_index_;
            local_doc_index_ = 0;
        }
        if (chunk_index_ >= source_->chunks.size()) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "phrase_query: streaming cursor exhausted before target docid");
        }

        const PosChunk& chunk = source_->chunks[chunk_index_];
        while (local_doc_index_ < chunk.docids.size() && chunk.docids[local_doc_index_] < docid) {
            ++local_doc_index_;
        }
        if (local_doc_index_ >= chunk.docids.size() || chunk.docids[local_doc_index_] != docid) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "phrase_query: candidate missing from streaming posting chunk");
        }

        if (active_frame_ != chunk_index_) {
            RETURN_IF_ERROR(finish_active_frame());
            RETURN_IF_ERROR(positions_.reset(
                    chunk.prx, chunk.prx_doc_count, chunk.prx_doc_ordinals,
                    source_->observer_context == nullptr ? nullptr : &iterator_context_));
            active_frame_ = chunk_index_;
        }
        DORIS_CHECK(chunk.prx_doc_ordinals.empty() ||
                    chunk.prx_doc_ordinals.size() == chunk.docids.size());
        DORIS_CHECK(!chunk.prx_doc_ordinals.empty() ||
                    local_doc_index_ <= std::numeric_limits<uint32_t>::max());
        const uint32_t prx_doc_ordinal = chunk.prx_doc_ordinals.empty()
                                                 ? static_cast<uint32_t>(local_doc_index_)
                                                 : chunk.prx_doc_ordinals[local_doc_index_];
        return positions_.seek(prx_doc_ordinal);
    }

    // `available` is an output parameter required by the exact matcher cursor contract.
    Status next_position(uint32_t* position,
                         bool* available) { // NOLINT(readability-non-const-parameter)
        return positions_.next_position(position, available);
    }

    Status finish_doc() {
        RETURN_IF_ERROR(positions_.finish_doc());
        ++local_doc_index_;
        return Status::OK();
    }

    Status finish() {
        RETURN_IF_ERROR(finish_active_frame());
        while (chunk_index_ < source_->chunks.size() &&
               local_doc_index_ == source_->chunks[chunk_index_].docids.size()) {
            ++chunk_index_;
            local_doc_index_ = 0;
        }
        if (chunk_index_ != source_->chunks.size()) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "phrase_query: streaming cursor has unconsumed candidate docs");
        }
        return Status::OK();
    }

    void add_query_stats(format::PhraseQueryExecutionStats* stats) const {
        stats->prx_streaming_frames += local_query_stats_.prx_streaming_frames;
    }

private:
    Status finish_active_frame() {
        if (active_frame_ == kNoChunk) {
            return Status::OK();
        }
        RETURN_IF_ERROR(positions_.finish_frame());
        active_frame_ = kNoChunk;
        return Status::OK();
    }

    static constexpr size_t kNoChunk = static_cast<size_t>(-1);

    format::PrxPositionIterator positions_;
    format::PrxDecodeContext iterator_context_;
    format::PhraseQueryExecutionStats local_query_stats_;
    const PosSource* source_ = nullptr;
    size_t chunk_index_ = 0;
    size_t local_doc_index_ = 0;
    size_t active_frame_ = kNoChunk;
};

class PhraseMatchCollector {
public:
    PhraseMatchCollector(std::vector<uint32_t>* docids, std::vector<PhraseMatch>* matches)
            : docids_(docids), matches_(matches) {
        DCHECK(docids_ != nullptr || matches_ != nullptr);
    }

    bool needs_frequency() const { return matches_ != nullptr; }

    void emit(uint32_t docid, float frequency) {
        DCHECK_GT(frequency, 0.0F);
        if (docids_ != nullptr) {
            docids_->push_back(docid);
        }
        if (matches_ != nullptr) {
            matches_->push_back({.docid = docid, .frequency = frequency});
        }
    }

private:
    std::vector<uint32_t>* docids_;
    std::vector<PhraseMatch>* matches_;
};

void emit_two_term_phrase_chunk_pair(const PosChunk& left, const PosChunk& right,
                                     const PosChunkDecoder& left_decoder,
                                     const PosChunkDecoder& right_decoder, uint32_t right_delta,
                                     PhraseMatchCollector* collector) {
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
        const uint32_t frequency =
                collector->needs_frequency()
                        ? index_query::count_two_term_phrase(left_span, right_span, right_delta)
                        : index_query::contains_two_term_phrase(left_span, right_span, right_delta);
        if (frequency != 0) {
            collector->emit(left_docid, frequency);
        }
        ++li;
        ++ri;
    }
}

Status emit_two_term_phrase_chunk_merge(const std::vector<size_t>& phrase_plan_index,
                                        const std::vector<uint32_t>& position_offsets,
                                        std::vector<PosSource>& srcs,
                                        PhraseMatchCollector* collector) {
    const size_t left_plan = phrase_plan_index[0];
    const size_t right_plan = phrase_plan_index[1];
    const uint32_t right_delta = position_offsets[1] - position_offsets[0];
    const PosSource& left_src = srcs[left_plan];
    const PosSource& right_src = srcs[right_plan];

    PosChunkDecoder left_decoder(left_src.observer_context);
    PosChunkDecoder right_decoder(right_src.observer_context);
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

        emit_two_term_phrase_chunk_pair(left, right, left_decoder, right_decoder, right_delta,
                                        collector);

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

// One pass over the ascending candidates. Two distinct terms of an exact phrase merge their
// decoded chunks directly; every other phrase goes through the common verifier, whose loader
// seeks a term's cursor to the candidate and decodes each retained chunk at most once.
Status emit_phrase_streaming(const std::vector<TermPlan>& plans,
                             const std::vector<size_t>& phrase_plan_index,
                             const std::vector<uint32_t>& position_offsets,
                             std::vector<PosSource>& srcs, const std::vector<uint32_t>& candidates,
                             PhraseMatchCollector* collector, const PhraseQueryOptions& options) {
    if (options.slop == 0 && phrase_plan_index.size() == 2 &&
        phrase_plan_index[0] != phrase_plan_index[1]) {
        return emit_two_term_phrase_chunk_merge(phrase_plan_index, position_offsets, srcs,
                                                collector);
    }
    std::vector<uint64_t> costs;
    costs.reserve(phrase_plan_index.size());
    for (size_t plan : phrase_plan_index) {
        costs.push_back(plans[plan].df);
    }
    index_query::PhraseVerifier verifier(phrase_plan_index, position_offsets, costs, options.slop,
                                         options.ordered);
    std::vector<PostingCursor> cursors(srcs.size());
    for (size_t plan = 0; plan < srcs.size(); ++plan) {
        cursors[plan].init(&srcs[plan]);
    }
    for (uint32_t docid : candidates) {
        const auto load = [&](size_t plan, index_query::PhrasePositionSpan* span) {
            SNII_QUERY_COUNT(phrase_position_loads);
            RETURN_IF_ERROR(cursors[plan].seek(docid));
            return cursors[plan].positions(span);
        };
        float frequency = 0.0F;
        RETURN_IF_ERROR(verifier.verify(load, collector->needs_frequency(), &frequency));
        if (frequency > 0.0F) {
            collector->emit(docid, frequency);
        }
    }
    return Status::OK();
}

Status emit_exact_phrase_streaming_positions(const std::vector<size_t>& phrase_plan_index,
                                             const std::vector<uint32_t>& position_offsets,
                                             std::vector<PosSource>& srcs,
                                             const std::vector<uint32_t>& candidates,
                                             PhraseMatchCollector* collector,
                                             format::PhraseQueryExecutionStats* query_stats) {
#ifdef BE_TEST
    internal::testing::note_streaming_exact_phrase_execution();
#endif
    std::vector<StreamingPostingCursor> cursors(srcs.size());
    index_query::validate_exact_phrase_stream_inputs(
            std::span(cursors), std::span(phrase_plan_index), std::span(position_offsets));
    for (size_t plan_index : phrase_plan_index) {
        cursors[plan_index].init(&srcs[plan_index]);
    }
    for (uint32_t docid : candidates) {
        bool matched = false;
        RETURN_IF_ERROR(index_query::match_exact_phrase_document(
                std::span(cursors), std::span(phrase_plan_index), std::span(position_offsets),
                docid, &matched));
        if (matched) {
            collector->emit(docid, 1);
        }
    }

    Status first_error;
    for (size_t plan_index : phrase_plan_index) {
        const Status status = cursors[plan_index].finish();
        if (!status.ok() && first_error.ok()) {
            first_error = status;
        }
    }
    if (!first_error.ok()) {
        return first_error;
    }
    for (size_t plan_index : phrase_plan_index) {
        cursors[plan_index].add_query_stats(query_stats);
    }
    return Status::OK();
}

// Candidate restrictions: a prefilter bounds the leading-term conjunction so
// only its docs get their positions read; a filter drops non-candidates from the
// conjunction result. Docs outside the restriction cannot contribute (the caller
// guarantees the final answer is a subset), so both are result-preserving.
// Phrase-prefix prefilters with the docs that also carry some tail expansion.

constexpr uint64_t kCandidateFilterDfRatio = 8;

uint32_t min_plan_df(const std::vector<TermPlan>& plans) {
    uint32_t min_df = std::numeric_limits<uint32_t>::max();
    for (const TermPlan& plan : plans) {
        min_df = std::min(min_df, plan.df);
    }
    return min_df;
}

} // namespace
CandidateRestriction restrict_to_candidates(const roaring::Roaring* candidates, uint32_t min_df,
                                            std::vector<uint32_t>* storage) {
    if (candidates == nullptr) {
        return {};
    }
    if (candidates->cardinality() > static_cast<uint64_t>(min_df) * kCandidateFilterDfRatio) {
        return {.filter = candidates};
    }
    storage->resize(candidates->cardinality());
    candidates->toUint32Array(storage->data());
    return {.prefilter = storage};
}

void retain_candidates(const roaring::Roaring& candidates, std::vector<uint32_t>* docids) {
    roaring::BulkContext context;
    std::erase_if(*docids,
                  [&](uint32_t docid) { return !candidates.containsBulk(context, docid); });
}

Status build_phrase_execution_state(const LogicalIndexReader& idx, io::BatchRangeFetcher* round1,
                                    std::vector<TermPlan>* plans, PhraseExecutionState* state,
                                    CandidateRestriction candidates,
                                    format::PrxDecodeContext* observer_context,
                                    PhraseCandidateMetric candidate_metric) {
    if (round1->pending() > 0) {
        RETURN_IF_ERROR(round1->fetch());
    }
    RETURN_IF_ERROR(internal::open_preludes(*round1, plans,
                                            /*need_positions=*/true));

    state->owners.clear();
    state->candidates.clear();
    std::vector<DocidSource> doc_sources;
    DORIS_CHECK(candidates.prefilter == nullptr || candidates.filter == nullptr);
    if (candidates.prefilter != nullptr) {
        if (candidates.prefilter->empty()) {
            return Status::OK();
        }
        RETURN_IF_ERROR(internal::filter_docids_by_conjunction(
                idx, *round1, *plans, *candidates.prefilter, &state->candidates, &doc_sources));
    } else {
        RETURN_IF_ERROR(internal::build_docid_only_conjunction(idx, *round1, *plans,
                                                               &state->candidates, &doc_sources));
    }
    if (candidates.filter != nullptr) {
        // The filter dropped docs these chunks still hold, so no source holds the final
        // candidates.
        retain_candidates(*candidates.filter, &state->candidates);
        for (DocidSource& source : doc_sources) {
            source.docids_are_final_candidates = false;
        }
    }
    if (observer_context != nullptr && observer_context->query_stats != nullptr) {
        if (candidate_metric == PhraseCandidateMetric::kExact) {
            observer_context->query_stats->exact_candidate_docs += state->candidates.size();
            observer_context->query_stats->exact_candidate_visits += state->candidates.size();
        } else {
            observer_context->query_stats->prefix_leading_candidate_docs +=
                    state->candidates.size();
        }
    }
    if (state->candidates.empty()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(build_position_sources_for_candidates(idx, *round1, *plans, &doc_sources,
                                                          state->candidates, &state->owners,
                                                          &state->srcs, observer_context));
    return Status::OK();
}

namespace {
Status execute_phrase_plans_at_offsets(
        const LogicalIndexReader& idx, io::BatchRangeFetcher* round1, std::vector<TermPlan>* plans,
        const std::vector<size_t>& phrase_plan_index, const std::vector<uint32_t>& position_offsets,
        std::vector<uint32_t>* docids, format::PrxDecodeContext* observer_context,
        std::vector<PhraseMatch>* matches, const PhraseQueryOptions& options,
        CandidateRestriction candidates, internal::ExactPhrasePositionAccess position_access) {
    DCHECK_EQ(phrase_plan_index.size(), position_offsets.size());
    PhraseExecutionState state;
    RETURN_IF_ERROR(build_phrase_execution_state(idx, round1, plans, &state, candidates,
                                                 observer_context, PhraseCandidateMetric::kExact));
    if (state.candidates.empty()) {
        return Status::OK();
    }

    const bool use_streaming = should_use_streaming_exact_phrase(
            *plans, state.srcs, phrase_plan_index, state.candidates.size(), matches != nullptr,
            options, position_access);
    PhraseVerifyTimer verify_timer(observer_context);
    format::PhraseQueryExecutionStats streaming_stats;
    if (use_streaming) {
        DCHECK(docids != nullptr);
        std::vector<uint32_t> staged_docids = std::move(*docids);
        docids->clear();
        PhraseMatchCollector collector(&staged_docids, nullptr);
        RETURN_IF_ERROR(emit_exact_phrase_streaming_positions(phrase_plan_index, position_offsets,
                                                              state.srcs, state.candidates,
                                                              &collector, &streaming_stats));
        *docids = std::move(staged_docids);
    } else {
        PhraseMatchCollector collector(docids, matches);
        RETURN_IF_ERROR(emit_phrase_streaming(*plans, phrase_plan_index, position_offsets,
                                              state.srcs, state.candidates, &collector, options));
    }
    verify_timer.commit_success();
    if (observer_context != nullptr && observer_context->query_stats != nullptr) {
        observer_context->query_stats->prx_streaming_frames += streaming_stats.prx_streaming_frames;
    }
    return Status::OK();
}

} // namespace
Status execute_phrase_plans(const LogicalIndexReader& idx, io::BatchRangeFetcher* round1,
                            std::vector<TermPlan>* plans,
                            const std::vector<size_t>& phrase_plan_index,
                            std::vector<uint32_t>* docids,
                            format::PrxDecodeContext* observer_context,
                            std::vector<PhraseMatch>* matches, const PhraseQueryOptions& options) {
    std::vector<uint32_t> position_offsets;
    if (!index_query::build_position_offsets(phrase_plan_index.size(), &position_offsets)) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "phrase_query: phrase length exceeds doc position range");
    }
    std::vector<uint32_t> candidate_docids;
    return execute_phrase_plans_at_offsets(
            idx, round1, plans, phrase_plan_index, position_offsets, docids, observer_context,
            matches, options,
            restrict_to_candidates(options.candidates, min_plan_df(*plans), &candidate_docids),
            internal::ExactPhrasePositionAccess::kAuto);
}

} // namespace doris::snii::query::phrase_impl

namespace doris::snii::query {

using namespace phrase_impl; // NOLINT(google-build-using-namespace): module-internal impl namespace

Status internal::execute_resolved_phrase_plan(const LogicalIndexReader& idx,
                                              internal::ResolvedPhrasePlan&& plan,
                                              std::vector<uint32_t>* docids,
                                              format::PrxDecodeContext* observer_context,
                                              std::vector<PhraseMatch>* matches,
                                              const roaring::Roaring* candidates,
                                              internal::ExactPhrasePositionAccess position_access) {
    if (docids == nullptr && matches == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("resolved_phrase_plan: null out");
    }
    if (docids != nullptr) {
        docids->clear();
    }
    if (matches != nullptr) {
        matches->clear();
    }
    DORIS_CHECK(plan.is_valid());
    if (plan.phrase_plan_index.empty()) {
        return Status::OK();
    }

    if (plan.phrase_plan_index.size() == 1) {
        DORIS_CHECK(matches == nullptr);
        const internal::ResolvedQueryTerm& term = plan.unique_terms[plan.phrase_plan_index.front()];
        RETURN_IF_ERROR(internal::read_docid_posting(idx, term.entry, term.frq_base, term.prx_base,
                                                     docids));
        if (candidates != nullptr) {
            retain_candidates(*candidates, docids);
        }
        return Status::OK();
    }

    io::BatchRangeFetcher round1(idx.reader());
    std::vector<TermPlan> plans;
    RETURN_IF_ERROR(internal::plan_resolved_terms(idx, std::move(plan.unique_terms), &round1,
                                                  &plans,
                                                  /*need_positions=*/false));
    std::vector<uint32_t> candidate_docids;
    return execute_phrase_plans_at_offsets(
            idx, &round1, &plans, plan.phrase_plan_index, plan.position_offsets, docids,
            observer_context, matches, {},
            restrict_to_candidates(candidates, min_plan_df(plans), &candidate_docids),
            position_access);
}

} // namespace doris::snii::query

#ifdef BE_TEST
namespace doris::snii::query::internal::testing {
namespace {

std::atomic<uint64_t>& streaming_exact_phrase_execution_atomic() {
    static std::atomic<uint64_t> counter {0};
    return counter;
}

} // namespace

uint64_t streaming_exact_phrase_execution_count() {
    return streaming_exact_phrase_execution_atomic().load(std::memory_order_relaxed);
}

void reset_streaming_exact_phrase_execution_count() {
    streaming_exact_phrase_execution_atomic().store(0, std::memory_order_relaxed);
}

void note_streaming_exact_phrase_execution() {
    streaming_exact_phrase_execution_atomic().fetch_add(1, std::memory_order_relaxed);
}

} // namespace doris::snii::query::internal::testing
#endif
