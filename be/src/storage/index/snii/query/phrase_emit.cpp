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
#include "storage/index/snii/query/internal/phrase_query_split.h"
#include "storage/index/snii/query/internal/plain_term_routing.h"
#include "storage/index/snii/query/internal/position_math.h"
#include "storage/index/snii/query/internal/query_test_counters.h"
#include "storage/index/snii/query/internal/resolved_phrase_plan.h"
#include "storage/index/snii/query/internal/sloppy_phrase_matcher.h"
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

namespace {
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
            SNII_QUERY_COUNT(phrase_position_epoch_cache_misses);
        } else {
            SNII_QUERY_COUNT(phrase_position_epoch_cache_hits);
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

class PhraseMatchCollector {
public:
    PhraseMatchCollector(std::vector<uint32_t>* docids, std::vector<PhraseMatch>* matches)
            : docids_(docids), matches_(matches) {
        DCHECK(docids_ != nullptr || matches_ != nullptr);
    }

    bool needs_frequency() const { return matches_ != nullptr; }

    void emit(uint32_t docid, uint32_t frequency) {
        DCHECK_GT(frequency, 0);
        if (docids_ != nullptr) {
            docids_->push_back(docid);
        }
        if (matches_ != nullptr) {
            matches_->push_back({.docid = docid, .frequency = static_cast<float>(frequency)});
        }
    }

    void emit_sloppy(uint32_t docid, float frequency) {
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

bool contains_two_term_phrase(std::pair<const uint32_t*, const uint32_t*> left_span,
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

size_t select_phrase_verification_pair(const std::vector<TermPlan>& plans,
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

class TwoTermPhraseStartCursor {
public:
    TwoTermPhraseStartCursor(std::pair<const uint32_t*, const uint32_t*> left_span,
                             std::pair<const uint32_t*, const uint32_t*> right_span,
                             uint32_t right_delta, uint32_t left_offset)
            : left_(left_span.first),
              left_end_(left_span.second),
              right_(right_span.first),
              right_end_(right_span.second),
              right_delta_(right_delta),
              left_offset_(left_offset),
              max_left_(std::numeric_limits<uint32_t>::max() - right_delta) {}

    bool next(uint32_t* start) {
        DCHECK(start != nullptr);
        while (left_ != left_end_ && right_ != right_end_) {
            if (*left_ > max_left_) {
                return false;
            }
            const uint32_t want = *left_ + right_delta_;
            while (right_ != right_end_ && *right_ < want) {
                ++right_;
            }
            if (right_ == right_end_) {
                return false;
            }
            const uint32_t left_position = *left_++;
            if (*right_ == want && left_position >= left_offset_) {
                *start = left_position - left_offset_;
                return true;
            }
        }
        return false;
    }

private:
    const uint32_t* left_;
    const uint32_t* left_end_;
    const uint32_t* right_;
    const uint32_t* right_end_;
    uint32_t right_delta_;
    uint32_t left_offset_;
    uint32_t max_left_;
};

uint32_t count_two_term_phrase(std::pair<const uint32_t*, const uint32_t*> left_span,
                               std::pair<const uint32_t*, const uint32_t*> right_span,
                               uint32_t right_delta) {
    TwoTermPhraseStartCursor starts(left_span, right_span, right_delta, /*left_offset=*/0);
    uint32_t frequency = 0;
    uint32_t start = 0;
    while (starts.next(&start)) {
        DCHECK_NE(frequency, std::numeric_limits<uint32_t>::max());
        ++frequency;
    }
    return frequency;
}

Status emit_two_term_phrase_streaming(const std::vector<size_t>& phrase_plan_index,
                                      const std::vector<uint32_t>& position_offsets,
                                      std::vector<PosSource>& srcs,
                                      const std::vector<uint32_t>& candidates,
                                      PhraseMatchCollector* collector) {
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
            const uint32_t frequency = collector->needs_frequency()
                                               ? count_two_term_phrase(span, span, right_delta)
                                               : contains_two_term_phrase(span, span, right_delta);
            if (frequency != 0) {
                collector->emit(docid, frequency);
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
        const uint32_t frequency =
                collector->needs_frequency()
                        ? count_two_term_phrase(left_span, right_span, right_delta)
                        : contains_two_term_phrase(left_span, right_span, right_delta);
        if (frequency != 0) {
            collector->emit(expected_docid, frequency);
        }
    }
    return Status::OK();
}

Status emit_sloppy_phrase_streaming(const std::vector<size_t>& phrase_plan_index,
                                    const std::vector<uint32_t>& position_offsets,
                                    std::vector<PosSource>& srcs,
                                    const std::vector<uint32_t>& candidates,
                                    const PhraseQueryOptions& options,
                                    PhraseMatchCollector* collector) {
    PhrasePositionLoader loader(srcs.size(), srcs);
    std::vector<internal::PhrasePositionSpan> spans(phrase_plan_index.size());
    internal::SloppyPhraseMatcher matcher(phrase_plan_index, position_offsets, options.slop,
                                          options.ordered);
    for (uint32_t docid : candidates) {
        loader.begin_doc(docid);
        for (size_t i = 0; i < phrase_plan_index.size(); ++i) {
            RETURN_IF_ERROR(loader.positions_for_phrase_pos(phrase_plan_index, i, &spans[i]));
        }
        const float frequency = matcher.match(spans, collector->needs_frequency());
        if (frequency > 0.0F) {
            collector->emit_sloppy(docid, frequency);
        }
    }
    return Status::OK();
}

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
                        ? count_two_term_phrase(left_span, right_span, right_delta)
                        : contains_two_term_phrase(left_span, right_span, right_delta);
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

bool phrase_start_matches_all_terms(
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

Status emit_single_term_phrase_streaming(const std::vector<size_t>& phrase_plan_index,
                                         std::vector<PosSource>& srcs,
                                         const std::vector<uint32_t>& candidates,
                                         PhraseMatchCollector* collector) {
    PhrasePositionLoader loader(srcs.size(), srcs);
    for (uint32_t d : candidates) {
        loader.begin_doc(d);
        std::pair<const uint32_t*, const uint32_t*> single_span;
        RETURN_IF_ERROR(loader.positions_for_phrase_pos(phrase_plan_index, 0, &single_span));
        if (single_span.first != single_span.second) {
            const auto span_size = static_cast<size_t>(single_span.second - single_span.first);
            DCHECK_LE(span_size, std::numeric_limits<uint32_t>::max());
            collector->emit(d, collector->needs_frequency() ? static_cast<uint32_t>(span_size) : 1);
        }
    }
    return Status::OK();
}

Status emit_multi_term_phrase_streaming(const std::vector<TermPlan>& plans,
                                        const std::vector<size_t>& phrase_plan_index,
                                        const std::vector<uint32_t>& position_offsets,
                                        std::vector<PosSource>& srcs,
                                        const std::vector<uint32_t>& candidates,
                                        PhraseMatchCollector* collector) {
    const size_t phrase_len = phrase_plan_index.size();
    PhrasePositionLoader loader(plans.size(), srcs);
    std::vector<std::pair<const uint32_t*, const uint32_t*>> span(phrase_len);
    const size_t pair_left = select_phrase_verification_pair(plans, phrase_plan_index);
    const size_t pair_right = pair_left + 1;
    for (uint32_t d : candidates) {
        loader.begin_doc(d);
        std::pair<const uint32_t*, const uint32_t*> left_span;
        std::pair<const uint32_t*, const uint32_t*> right_span;
        RETURN_IF_ERROR(loader.positions_for_phrase_pos(phrase_plan_index, pair_left, &left_span));
        RETURN_IF_ERROR(
                loader.positions_for_phrase_pos(phrase_plan_index, pair_right, &right_span));

        // `starts` retains raw pointers into the selected pair while the remaining
        // clause spans are loaded below. Every unique plan owns an independent
        // PostingCursor/PosChunkDecoder in PhrasePositionLoader; repeated phrase
        // positions map back to one plan and reuse that plan's epoch-cached span.
        TwoTermPhraseStartCursor starts(left_span, right_span,
                                        position_offsets[pair_right] - position_offsets[pair_left],
                                        position_offsets[pair_left]);
        uint32_t start = 0;
        if (!starts.next(&start)) {
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

        uint32_t frequency = 0;
        bool has_previous_start = false;
        uint32_t previous_start = 0;
        const uint32_t* first_clause_position = span[0].first;
        while (true) {
            if (!collector->needs_frequency()) {
                if (phrase_start_matches_all_terms(start, phrase_len, pair_left, pair_right,
                                                   position_offsets, span)) {
                    collector->emit(d, 1);
                    break;
                }
            } else if (!has_previous_start || start != previous_start) {
                has_previous_start = true;
                previous_start = start;
                if (phrase_start_matches_all_terms(start, phrase_len, pair_left, pair_right,
                                                   position_offsets, span)) {
                    uint32_t first_clause_want = 0;
                    const bool representable = internal::add_position_offset(
                            start, position_offsets[0], &first_clause_want);
                    DCHECK(representable);
                    while (first_clause_position != span[0].second &&
                           *first_clause_position < first_clause_want) {
                        ++first_clause_position;
                    }
                    const uint32_t* run_end = first_clause_position;
                    while (run_end != span[0].second && *run_end == first_clause_want) {
                        ++run_end;
                    }
                    const auto multiplicity =
                            static_cast<uint32_t>(run_end - first_clause_position);
                    DCHECK_NE(multiplicity, 0);
                    DCHECK_LE(frequency, std::numeric_limits<uint32_t>::max() - multiplicity);
                    frequency += multiplicity;
                    first_clause_position = run_end;
                }
            }
            if (!starts.next(&start)) {
                break;
            }
        }
        if (frequency != 0) {
            collector->emit(d, frequency);
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

Status emit_phrase_streaming(const std::vector<TermPlan>& plans,
                             const std::vector<size_t>& phrase_plan_index,
                             const std::vector<uint32_t>& position_offsets,
                             std::vector<PosSource>& srcs, const std::vector<uint32_t>& candidates,
                             PhraseMatchCollector* collector, const PhraseQueryOptions& options) {
    const size_t phrase_len = phrase_plan_index.size();
    if (options.slop != 0) {
        return emit_sloppy_phrase_streaming(phrase_plan_index, position_offsets, srcs, candidates,
                                            options, collector);
    }
    if (phrase_len == 1) {
        return emit_single_term_phrase_streaming(phrase_plan_index, srcs, candidates, collector);
    }
    if (phrase_len == 2) {
        if (phrase_plan_index[0] != phrase_plan_index[1]) {
            return emit_two_term_phrase_chunk_merge(phrase_plan_index, position_offsets, srcs,
                                                    collector);
        }
        return emit_two_term_phrase_streaming(phrase_plan_index, position_offsets, srcs, candidates,
                                              collector);
    }
    return emit_multi_term_phrase_streaming(plans, phrase_plan_index, position_offsets, srcs,
                                            candidates, collector);
}

// candidate_prefilter (optional): an ascending docid set the phrase must ALSO
// lie in. When provided, the leading-term conjunction is intersected with it so
// only docs in the prefilter get their positions read. Docs outside the
// prefilter cannot contribute (the caller guarantees the final answer is a
// subset), so this is result-preserving while cutting the position decode --
// used by phrase-prefix to restrict the huge leading-phrase candidate set to
// the docs that also carry some tail expansion.

} // namespace
Status build_phrase_execution_state(const LogicalIndexReader& idx, io::BatchRangeFetcher* round1,
                                    std::vector<TermPlan>* plans, PhraseExecutionState* state,
                                    const std::vector<uint32_t>* candidate_prefilter,
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
    if (candidate_prefilter != nullptr) {
        if (candidate_prefilter->empty()) {
            return Status::OK();
        }
        RETURN_IF_ERROR(internal::filter_docids_by_conjunction(
                idx, *round1, *plans, *candidate_prefilter, &state->candidates, &doc_sources));
    } else {
        RETURN_IF_ERROR(internal::build_docid_only_conjunction(idx, *round1, *plans,
                                                               &state->candidates, &doc_sources));
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
        const std::vector<uint32_t>* candidate_prefilter = nullptr) {
    DCHECK_EQ(phrase_plan_index.size(), position_offsets.size());
    PhraseExecutionState state;
    RETURN_IF_ERROR(build_phrase_execution_state(idx, round1, plans, &state, candidate_prefilter,
                                                 observer_context, PhraseCandidateMetric::kExact));
    if (state.candidates.empty()) {
        return Status::OK();
    }

    PhraseVerifyTimer verify_timer(observer_context);
    PhraseMatchCollector collector(docids, matches);
    RETURN_IF_ERROR(emit_phrase_streaming(*plans, phrase_plan_index, position_offsets, state.srcs,
                                          state.candidates, &collector, options));
    verify_timer.commit_success();
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
    if (!internal::build_position_offsets(phrase_plan_index.size(), &position_offsets)) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "phrase_query: phrase length exceeds doc position range");
    }
    return execute_phrase_plans_at_offsets(idx, round1, plans, phrase_plan_index, position_offsets,
                                           docids, observer_context, matches, options);
}

} // namespace doris::snii::query::phrase_impl

namespace doris::snii::query {

using namespace phrase_impl; // NOLINT(google-build-using-namespace): module-internal impl namespace

Status internal::execute_resolved_phrase_plan(const LogicalIndexReader& idx,
                                              internal::ResolvedPhrasePlan&& plan,
                                              std::vector<uint32_t>* docids,
                                              format::PrxDecodeContext* observer_context,
                                              std::vector<PhraseMatch>* matches,
                                              const std::vector<uint32_t>* candidate_prefilter) {
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
        if (candidate_prefilter != nullptr) {
            *docids = internal::intersect_sorted(*docids, *candidate_prefilter);
        }
        return Status::OK();
    }

    io::BatchRangeFetcher round1(idx.reader());
    std::vector<TermPlan> plans;
    RETURN_IF_ERROR(internal::plan_resolved_terms(idx, std::move(plan.unique_terms), &round1,
                                                  &plans,
                                                  /*need_positions=*/false));
    return execute_phrase_plans_at_offsets(idx, &round1, &plans, plan.phrase_plan_index,
                                           plan.position_offsets, docids, observer_context, matches,
                                           {}, candidate_prefilter);
}

} // namespace doris::snii::query
