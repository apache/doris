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
Status collect_expected_tail_positions(const std::vector<TermPlan>& plans,
                                       const std::vector<size_t>& phrase_plan_index,
                                       const std::vector<uint32_t>& position_offsets,
                                       std::vector<PosSource>& srcs,
                                       const std::vector<uint32_t>& candidates,
                                       ExpectedTailPositionSet* out,
                                       bool preserve_first_clause_multiplicity) {
    const size_t n = phrase_plan_index.size();
    DCHECK(n > 1);
    DCHECK_EQ(plans.size(), srcs.size());
    std::vector<PostingCursor> cur(plans.size());
    for (size_t i = 0; i < plans.size(); ++i) {
        cur[i].init(&srcs[i]);
    }

    std::vector<std::pair<const uint32_t*, const uint32_t*>> unique_span(plans.size());
    std::vector<std::pair<const uint32_t*, const uint32_t*>> span(n);
    for (uint32_t d : candidates) {
        for (size_t i = 0; i < plans.size(); ++i) {
            RETURN_IF_ERROR(cur[i].seek(d));
            RETURN_IF_ERROR(cur[i].positions(&unique_span[plans[i].order]));
        }
        for (size_t i = 0; i < n; ++i) {
            DCHECK_LT(phrase_plan_index[i], unique_span.size());
            span[i] = unique_span[phrase_plan_index[i]];
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
        auto best = position_span_size(span[0]);
        if (!preserve_first_clause_multiplicity) {
            for (size_t t = 1; t < n; ++t) {
                const auto sz = position_span_size(span[t]);
                if (sz < best) {
                    best = sz;
                    anchor = t;
                }
            }
        }
        const uint32_t anchor_off = position_offsets[anchor];
        SNII_QUERY_ADD(anchor_iterations, best);

        // Only the first non-anchor term is probed for every viable anchor.
        // Later terms can be skipped after an earlier mismatch, so using the
        // total anchor count to choose a forward scan for them could turn one
        // binary lookup into a full-span walk. The first term uses a forward
        // scan only when all anchors are valid and its conservative comparison
        // upper bound is at most half that of repeated binary search.
        const size_t first_checked_term = anchor == 0 ? 1 : 0;
        size_t monotonic_position_scan_term = n;
        if (should_use_monotonic_position_scan(span[anchor],
                                               position_span_size(span[first_checked_term]),
                                               anchor_off, position_offsets[first_checked_term])) {
            monotonic_position_scan_term = first_checked_term;
            SNII_QUERY_COUNT(monotonic_position_scans);
        }

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
                if (t == monotonic_position_scan_term) {
                    while (span[t].first != span[t].second && *span[t].first < want) {
                        ++span[t].first;
                    }
                    if (span[t].first == span[t].second || *span[t].first != want) {
                        ok = false;
                        break;
                    }
                } else if (!std::binary_search(span[t].first, span[t].second, want)) {
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
            out->docs.push_back(ExpectedTailPositions {
                    .docid = d, .positions_begin = expected_begin, .positions_end = expected_end});
        }
    }
    return Status::OK();
}

Status collect_single_term_expected_tail_positions(std::vector<PosSource>& srcs,
                                                   const std::vector<uint32_t>& candidates,
                                                   uint32_t tail_offset,
                                                   ExpectedTailPositionSet* out) {
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
            out->docs.push_back(ExpectedTailPositions {
                    .docid = d, .positions_begin = expected_begin, .positions_end = expected_end});
        }
    }
    return Status::OK();
}

Status collect_expected_tail_positions(const LogicalIndexReader& idx,
                                       internal::ResolvedPhrasePlan exact_plan,
                                       uint32_t tail_position_offset, ExpectedTailPositionSet* out,
                                       const std::vector<uint32_t>* candidate_prefilter,
                                       format::PrxDecodeContext* observer_context,
                                       bool preserve_first_clause_multiplicity) {
    out->clear();
    io::BatchRangeFetcher round1(idx.reader());
    std::vector<TermPlan> plans;
    RETURN_IF_ERROR(internal::plan_resolved_terms(idx, std::move(exact_plan.unique_terms), &round1,
                                                  &plans,
                                                  /*need_positions=*/false));

    PhraseExecutionState state;
    RETURN_IF_ERROR(build_phrase_execution_state(idx, &round1, &plans, &state, candidate_prefilter,
                                                 observer_context,
                                                 PhraseCandidateMetric::kPrefixLeading));
    if (state.candidates.empty()) {
        return Status::OK();
    }
    out->reserve_docs(state.candidates.size());
    DORIS_CHECK(!exact_plan.position_offsets.empty());
    DORIS_CHECK_GT(tail_position_offset, exact_plan.position_offsets.back());
    std::vector<uint32_t> position_offsets = std::move(exact_plan.position_offsets);
    position_offsets.push_back(tail_position_offset);
    PhraseVerifyTimer verify_timer(observer_context);
    if (exact_plan.phrase_plan_index.size() == 1) {
        DORIS_CHECK_LT(position_offsets[0], position_offsets[1]);
        RETURN_IF_ERROR(collect_single_term_expected_tail_positions(
                state.srcs, state.candidates, position_offsets[1] - position_offsets[0], out));
    } else {
        RETURN_IF_ERROR(collect_expected_tail_positions(
                plans, exact_plan.phrase_plan_index, position_offsets, state.srcs, state.candidates,
                out, preserve_first_clause_multiplicity));
    }
    verify_timer.commit_success();
    return Status::OK();
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

uint32_t mark_matching_positions(ExpectedTailPositionSet* expected,
                                 const ExpectedTailPositions& wanted,
                                 std::pair<const uint32_t*, const uint32_t*> actual) {
    DCHECK_EQ(expected->position_matched.size(), expected->positions.size());
    size_t expected_index = wanted.positions_begin;
    const uint32_t* actual_position = actual.first;
    uint32_t added = 0;
    while (expected_index < wanted.positions_end && actual_position != actual.second) {
        const uint32_t expected_position = expected->positions[expected_index];
        if (expected_position < *actual_position) {
            ++expected_index;
            continue;
        }
        if (*actual_position < expected_position) {
            ++actual_position;
            continue;
        }
        const size_t expected_run_end = static_cast<size_t>(
                std::upper_bound(expected->positions.begin() + expected_index,
                                 expected->positions.begin() + wanted.positions_end,
                                 expected_position) -
                expected->positions.begin());
        while (expected_index < expected_run_end) {
            if (expected->position_matched[expected_index] == 0) {
                expected->position_matched[expected_index] = 1;
                DCHECK_NE(added, std::numeric_limits<uint32_t>::max());
                ++added;
            }
            ++expected_index;
        }
        actual_position = std::upper_bound(actual_position, actual.second, expected_position);
    }
    return added;
}

// Upper bound on prefix expansions whose position cursors are held resident at
// once. The old per-tail loop verified a single expansion at a time (one
// PosSource + one PRX buffer live); the merged sweep below holds up to this many
// tail PosSources + cursors + PRX buffers simultaneously so it can read every
// tail's docid/prx bytes in ONE batched round and verify them in a single
// forward pass. `max_expansions` may be unbounded (<= 0), so this hard cap keeps
// resident memory bounded independent of the query: expansions beyond the cap
// are processed as additional capped groups (each a fresh single fetch) whose
// matched-doc flags are accumulated. The cap is tightened because each cursor
// here also holds decoded PRX rather than plain docids.
constexpr size_t kMaxTailMergeBatch = 32;

// Phrase-prefix only reads the residual tails' docid union to prefilter the
// leading-phrase candidate set when the smallest leading term's df reaches this
// -- i.e. when the leading candidate set is large enough that decoding all its
// positions dwarfs an extra docid-only union read. Scale the threshold with the
// segment so a fixed absolute gate does not disable the prefilter after rowset
// segmentation, while retaining the original 1<<16 cap for large segments.
constexpr uint32_t kMinPrefixLeadingPrefilterMinDf = 256;
constexpr uint32_t kMaxPrefixLeadingPrefilterMinDf = 1u << 16;
constexpr uint32_t kPrefixLeadingPrefilterDocFraction = 8;
// The tail union is decoded once for filtering and again for verification.
// Require enough leading PRX work to cover both reads and union overhead.
constexpr uint32_t kPrefixLeadingToTailDfRatio = 8;

uint32_t prefix_leading_prefilter_min_df(const LogicalIndexReader& idx,
                                         bool allow_segment_relative_gate) {
    if (!allow_segment_relative_gate) {
        return kMaxPrefixLeadingPrefilterMinDf;
    }
    const uint64_t segment_relative =
            idx.stats().indexed_doc_count / kPrefixLeadingPrefilterDocFraction;
    return static_cast<uint32_t>(std::clamp<uint64_t>(
            segment_relative, kMinPrefixLeadingPrefilterMinDf, kMaxPrefixLeadingPrefilterMinDf));
}

// Merged multi-tail verification for ONE resident-capped group of prefix
// expansions (`tails`, already truncated by max_expansions upstream). This
// replaces the per-tail verify-then-union loop: instead of re-planning + TWO
// remote rounds (docid, then prx) + a separate doc-walk PER tail and unioning N
// result lists, it plans every tail into ONE shared round1 fetch, intersects
// each tail with `expected_docids` in memory (no I/O), builds every surviving
// tail's position source in ONE batched PRX round, then sweeps the group's tail
// cursors over the ascending `expected` docs a SINGLE time -- marking a doc as
// soon as ANY tail has a position adjacent to a leading match.
//
// The marked set is byte-identical to the per-tail path's
//   UNION_{tail in group} { d : d in tail INTERSECT expected AND
//                               contains_any_position(expected, doc_d, pos_tail(d)) }
// because each tail's PosSource is still built from its OWN final-candidate
// docids (the shared-candidate argument is ignored for final-candidate sources),
// so pos_tail(d) and the per-doc position test are unchanged. Only the I/O rounds
// (2N -> 2) and the N separate unions (-> in-place flags) collapse. Bigram
// postings are NEVER consulted: every tail is verified against its unigram
// positions here.

Status collect_merged_tail_matches(const LogicalIndexReader& idx,
                                   std::vector<ResolvedQueryTerm> tails,
                                   ExpectedTailPositionSet* expected,
                                   const std::vector<uint32_t>& expected_docids, bool final_group,
                                   std::vector<uint32_t>* final_matches,
                                   format::PrxDecodeContext* observer_context,
                                   std::vector<PhraseMatch>* frequency_matches) {
    DCHECK(expected != nullptr);
    DCHECK(final_matches != nullptr || frequency_matches != nullptr);
    const size_t n = tails.size();
    if (n == 0 || expected->docs.empty()) {
        return Status::OK();
    }

    // Plan every tail into ONE fetcher so their docid postings + windowed
    // preludes are read in a single batched round (the per-tail path issued one
    // round per tail). Each tail keeps its own single-term plan vector so the
    // conjunction filter below consumes it directly, without slicing a shared
    // plan vector (whose prelude readers own decoded directory buffers).
    io::BatchRangeFetcher round1(idx.reader());
    std::vector<std::vector<TermPlan>> tail_plans(n);
    for (size_t i = 0; i < n; ++i) {
        std::vector<ResolvedQueryTerm> one;
        one.push_back(std::move(tails[i]));
        RETURN_IF_ERROR(internal::plan_resolved_terms(idx, std::move(one), &round1, &tail_plans[i],
                                                      /*need_positions=*/false));
    }
    if (round1.pending() > 0) {
        RETURN_IF_ERROR(round1.fetch());
    }
    for (size_t i = 0; i < n; ++i) {
        RETURN_IF_ERROR(internal::open_preludes(round1, &tail_plans[i],
                                                /*need_positions=*/true));
    }

    // Per-tail candidate docids (tail posting INTERSECT expected) and the aligned
    // final-candidate doc sources feeding the batched position builder. The
    // conjunction reads only already-fetched round1 bytes; a single-plan filter
    // marks its one source docids_are_final_candidates, so the position builder
    // materializes each tail's PosSource directly over its own candidate docs.
    // Tails whose intersection is empty are dropped here (exactly as the old
    // per-tail early return did), so no dead tail decodes its full posting.
    std::vector<std::vector<uint32_t>> tail_candidates(n);
    std::vector<TermPlan> active_plans;
    std::vector<DocidSource> active_sources;
    std::vector<size_t> active_index; // active slot -> tail index (into tail_candidates)
    for (size_t i = 0; i < n; ++i) {
        std::vector<DocidSource> tail_source;
        RETURN_IF_ERROR(internal::filter_docids_by_conjunction(
                idx, round1, tail_plans[i], expected_docids, &tail_candidates[i], &tail_source));
        if (tail_candidates[i].empty()) {
            continue; // this expansion has no doc in the expected set: nothing to verify
        }
        active_plans.push_back(std::move(tail_plans[i].front()));
        active_sources.push_back(tail_source.empty() ? DocidSource {}
                                                     : std::move(tail_source.front()));
        active_index.push_back(i);
    }
    if (active_plans.empty() && (!final_group || expected->matched_count == 0)) {
        return Status::OK();
    }
    // An empty final group must still sweep expected->docs and emit matches that
    // earlier resident groups recorded in-place.

    // ONE batched PRX round for every retained chunk across all surviving tails
    // (vs one round per tail before). `candidates` is intentionally empty: every
    // source is a final-candidate source, so the builder addresses positions by
    // the source's own docids and never consults the shared candidate list.
    std::vector<std::unique_ptr<io::BatchRangeFetcher>> owners;
    std::vector<PosSource> srcs;
    const std::vector<uint32_t> no_shared_candidates;
    if (!active_plans.empty()) {
        RETURN_IF_ERROR(build_position_sources_for_candidates(idx, round1, active_plans,
                                                              &active_sources, no_shared_candidates,
                                                              &owners, &srcs, observer_context));
    }

    // Single forward sweep over the ascending expected docs. For each doc probe
    // only the tails that actually posted it (per-tail ascending cursor over
    // tail_candidates), decode positions once, and emit the doc the instant one
    // tail's positions land adjacent to a leading match. Cursors advance strictly
    // forward because expected.docs is strictly ascending and each cursor is
    // sought at most once per doc.
    std::vector<PostingCursor> cursors(active_plans.size());
    for (size_t a = 0; a < active_plans.size(); ++a) {
        cursors[a].init(&srcs[a]);
    }
    std::vector<size_t> tail_pos(active_plans.size(), 0);
    PhraseVerifyTimer verify_timer(observer_context);
    DCHECK_EQ(expected->docs.size(), expected_docids.size());
    for (size_t expected_index = 0; expected_index < expected->docs.size(); ++expected_index) {
        ExpectedTailPositions& doc = expected->docs[expected_index];
        DCHECK_EQ(doc.docid, expected_docids[expected_index]);
        SNII_QUERY_COUNT(prefix_expected_doc_visits);
        if (observer_context != nullptr && observer_context->query_stats != nullptr) {
            ++observer_context->query_stats->prefix_tail_candidate_visits;
        }
        const uint32_t d = doc.docid;
        if (frequency_matches != nullptr || doc.phrase_frequency == 0) {
            bool matched = false;
            uint32_t added_frequency = 0;
            for (size_t a = 0;
                 a < active_plans.size() && (frequency_matches != nullptr || !matched); ++a) {
                std::vector<uint32_t>& cand = tail_candidates[active_index[a]];
                size_t& ti = tail_pos[a];
                while (ti < cand.size() && cand[ti] < d) {
                    ++ti;
                }
                if (ti >= cand.size() || cand[ti] != d) {
                    continue; // this expansion has no posting at d
                }
                RETURN_IF_ERROR(cursors[a].seek(d));
                std::pair<const uint32_t*, const uint32_t*> actual;
                RETURN_IF_ERROR(cursors[a].positions(&actual));
                if (frequency_matches == nullptr && contains_any_position(*expected, doc, actual)) {
                    matched = true;
                } else if (frequency_matches != nullptr) {
                    const uint32_t added = mark_matching_positions(expected, doc, actual);
                    DCHECK_LE(added_frequency, std::numeric_limits<uint32_t>::max() - added);
                    added_frequency += added;
                }
            }
            if (matched || added_frequency != 0) {
                if (doc.phrase_frequency == 0) {
                    ++expected->matched_count;
                }
                if (frequency_matches == nullptr) {
                    doc.phrase_frequency = 1;
                } else {
                    DCHECK_LE(doc.phrase_frequency,
                              std::numeric_limits<uint32_t>::max() - added_frequency);
                    doc.phrase_frequency += added_frequency;
                }
            }
        }
        if (final_group && doc.phrase_frequency != 0) {
            if (final_matches != nullptr) {
                final_matches->push_back(d);
            }
            if (frequency_matches != nullptr) {
                frequency_matches->push_back(PhraseMatch {
                        .docid = d, .frequency = static_cast<float>(doc.phrase_frequency)});
            }
        }
    }
    verify_timer.commit_success();
    return Status::OK();
}

} // namespace
Status execute_resolved_phrase_prefix_terms(
        const LogicalIndexReader& idx, internal::ResolvedPhrasePlan exact_plan,
        std::vector<ResolvedQueryTerm> tail_terms, uint32_t tail_position_offset,
        std::vector<uint32_t>* docids, format::PrxDecodeContext* decode_context,
        std::vector<PhraseMatch>* matches, const std::vector<uint32_t>* candidate_prefilter) {
    DORIS_CHECK(docids != nullptr || matches != nullptr);
    if (tail_terms.empty()) {
        return Status::OK();
    }
    if (exact_plan.phrase_plan_index.empty()) {
        DORIS_CHECK(matches == nullptr);
        DORIS_CHECK_EQ(tail_position_offset, 0U);
        if (tail_terms.size() == 1) {
            const auto& tail = tail_terms.front();
            RETURN_IF_ERROR(internal::read_docid_posting(idx, tail.entry, tail.frq_base,
                                                         tail.prx_base, docids));
            if (candidate_prefilter != nullptr) {
                *docids = internal::intersect_sorted(*docids, *candidate_prefilter);
            }
            return Status::OK();
        }
        std::vector<internal::ResolvedDocidPosting> tail_postings;
        tail_postings.reserve(tail_terms.size());
        for (const auto& tail : tail_terms) {
            tail_postings.push_back({tail.entry, tail.frq_base, tail.prx_base});
        }
        RETURN_IF_ERROR(internal::build_docid_union(idx, tail_postings, docids));
        if (candidate_prefilter != nullptr) {
            *docids = internal::intersect_sorted(*docids, *candidate_prefilter);
        }
        return Status::OK();
    }
    if (!idx.has_positions()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED, false>(
                "phrase_prefix_query: index has no positions");
    }
    DORIS_CHECK(!exact_plan.position_offsets.empty());
    DORIS_CHECK_GT(tail_position_offset, exact_plan.position_offsets.back());
    if (tail_terms.size() == 1) {
        append_resolved_phrase_clause(std::move(tail_terms.front()), tail_position_offset,
                                      &exact_plan);
        return internal::execute_resolved_phrase_plan(
                idx, std::move(exact_plan), docids, decode_context, matches, candidate_prefilter,
                internal::ExactPhrasePositionAccess::kMaterializedOnly);
    }

    uint32_t min_lead_df = std::numeric_limits<uint32_t>::max();
    for (const ResolvedQueryTerm& term : exact_plan.unique_terms) {
        min_lead_df = std::min(min_lead_df, term.entry.df);
    }
    uint64_t tail_df_sum = 0;
    for (const ResolvedQueryTerm& tail : tail_terms) {
        tail_df_sum += tail.entry.df;
    }
    const std::vector<uint32_t>* prefilter = candidate_prefilter;
    std::vector<uint32_t> tail_union;
    std::vector<uint32_t> combined_prefilter;
    const bool allow_segment_relative_prefilter =
            candidate_prefilter == nullptr && exact_plan.phrase_plan_index.size() == 1;
    const uint32_t leading_prefilter_min_df =
            prefix_leading_prefilter_min_df(idx, allow_segment_relative_prefilter);
    if (min_lead_df >= leading_prefilter_min_df &&
        tail_df_sum <= static_cast<uint64_t>(min_lead_df) / kPrefixLeadingToTailDfRatio) {
        std::vector<internal::ResolvedDocidPosting> tail_postings;
        tail_postings.reserve(tail_terms.size());
        for (const ResolvedQueryTerm& tail : tail_terms) {
            tail_postings.push_back({tail.entry, tail.frq_base, tail.prx_base});
        }
        RETURN_IF_ERROR(internal::build_docid_union(idx, tail_postings, &tail_union));
        if (tail_union.empty()) {
            return Status::OK();
        }
        if (candidate_prefilter == nullptr) {
            prefilter = &tail_union;
        } else {
            combined_prefilter = internal::intersect_sorted(*candidate_prefilter, tail_union);
            if (combined_prefilter.empty()) {
                return Status::OK();
            }
            prefilter = &combined_prefilter;
        }
    }

    ExpectedTailPositionSet expected;
    RETURN_IF_ERROR(collect_expected_tail_positions(idx, std::move(exact_plan),
                                                    tail_position_offset, &expected, prefilter,
                                                    decode_context, matches != nullptr));
    if (expected.docs.empty()) {
        return Status::OK();
    }
    if (matches != nullptr) {
        expected.position_matched.assign(expected.positions.size(), 0);
    }

    std::vector<uint32_t> expected_docids;
    expected_docids.reserve(expected.docs.size());
    for (const ExpectedTailPositions& doc : expected.docs) {
        expected_docids.push_back(doc.docid);
    }
    SNII_QUERY_COUNT(expected_docids_build);

    std::vector<uint32_t> final_matches;
    // Keep the expected-doc set stable across groups. Compacting matched docs
    // speculates that later tail groups intersect it; an empty intersection
    // skips the sweep entirely, so the compaction work cannot be repaid safely.
    for (size_t start = 0; start < tail_terms.size(); start += kMaxTailMergeBatch) {
        const size_t end = std::min(start + kMaxTailMergeBatch, tail_terms.size());
        const bool final_group = end == tail_terms.size();
        std::vector<ResolvedQueryTerm> group;
        group.reserve(end - start);
        for (size_t i = start; i < end; ++i) {
            group.push_back(std::move(tail_terms[i]));
        }
        RETURN_IF_ERROR(collect_merged_tail_matches(
                idx, std::move(group), &expected, expected_docids, final_group,
                docids == nullptr ? nullptr : &final_matches, decode_context, matches));
        if (matches == nullptr && !final_group && expected.matched_count == expected.docs.size()) {
            final_matches.reserve(expected.docs.size());
            for (const ExpectedTailPositions& doc : expected.docs) {
                DCHECK_NE(doc.phrase_frequency, 0);
                final_matches.push_back(doc.docid);
            }
            break;
        }
    }
    if (docids != nullptr) {
        *docids = std::move(final_matches);
    }
    return Status::OK();
}

namespace {
template <typename Index>
std::vector<ResolvedQueryTerm> copy_resolved_terms(const std::vector<ResolvedQueryTerm>& resolved,
                                                   const std::vector<Index>& indices) {
    std::vector<ResolvedQueryTerm> result;
    result.reserve(indices.size());
    for (size_t index : indices) {
        DORIS_CHECK_LT(index, resolved.size());
        result.push_back(resolved[index]);
    }
    return result;
}

} // namespace
Status execute_hybrid_phrase_prefix_plan(
        const LogicalIndexReader& idx, const HybridPrefixPlanArtifact& artifact,
        const std::vector<std::string>& batch_terms, const std::vector<ResolvedQueryTerm>& resolved,
        const std::vector<ResolvedQueryTerm>& plain_tail_terms, std::vector<uint32_t>* docids,
        format::PrxDecodeContext* decode_context, CommonGramsPlanningTimer& planning_timer,
        bool* candidate_intersection_empty) {
    DORIS_CHECK(idx.common_grams_posting_policy() == format::CommonGramsPostingPolicy::kHybridV1);
    DORIS_CHECK(docids != nullptr);
    DORIS_CHECK(candidate_intersection_empty != nullptr);
    docids->clear();
    *candidate_intersection_empty = false;

    const HybridPositionedCover& plain_tail_cover = artifact.plain_tail_cover;
    const uint32_t plain_tail_position_offset = artifact.plain_tail_position_offset;
    HybridPrefixCandidateSet leading_candidates;
    RETURN_IF_ERROR(build_hybrid_leading_candidates(idx, plain_tail_cover.candidate_prefilter,
                                                    batch_terms, resolved, &leading_candidates));
    if (leading_candidates.active && leading_candidates.docs.empty()) {
        planning_timer.finish();
        *candidate_intersection_empty = true;
        return Status::OK();
    }

    if (!artifact.maps_tail_to_gram) {
        DORIS_CHECK(leading_candidates.active);
        DORIS_CHECK(artifact.mapped_tail_split.positioned_indices.empty());
        DORIS_CHECK(artifact.mapped_tail_split.docs_only_indices.empty());
        planning_timer.finish();
        RETURN_IF_ERROR(execute_resolved_phrase_prefix_terms(
                idx,
                copy_resolved_phrase_plan(plain_tail_cover.verification, batch_terms, resolved),
                plain_tail_terms, plain_tail_position_offset, docids, decode_context, nullptr,
                &leading_candidates.docs));
        *candidate_intersection_empty = docids->empty();
        return Status::OK();
    }

    const HybridPrefixMappedTails& split = artifact.mapped_tail_split;
    DORIS_CHECK(!split.positioned_indices.empty() || !split.docs_only_indices.empty());
    DORIS_CHECK(leading_candidates.active || !split.docs_only_indices.empty());
    std::vector<uint32_t> docs_only_tail_candidates;
    if (!split.docs_only_indices.empty()) {
        RETURN_IF_ERROR(build_hybrid_docs_only_tail_candidates(
                idx, resolved, split.docs_only_indices, leading_candidates,
                &docs_only_tail_candidates));
    }
    planning_timer.finish();

    if (!split.positioned_indices.empty()) {
        DORIS_CHECK(artifact.positioned_tail_verification.has_value());
        std::vector<uint32_t> positioned_docs;
        RETURN_IF_ERROR(execute_resolved_phrase_prefix_terms(
                idx,
                copy_resolved_phrase_plan(*artifact.positioned_tail_verification, batch_terms,
                                          resolved),
                copy_resolved_terms(resolved, split.positioned_indices),
                plain_tail_position_offset - 1, &positioned_docs, decode_context, nullptr,
                leading_candidates.active ? &leading_candidates.docs : nullptr));
        internal::union_sorted_into(docids, positioned_docs);
    }

    if (!split.docs_only_indices.empty() && !docs_only_tail_candidates.empty()) {
        std::vector<uint32_t> docs_only_docs;
        RETURN_IF_ERROR(execute_resolved_phrase_prefix_terms(
                idx,
                copy_resolved_phrase_plan(plain_tail_cover.verification, batch_terms, resolved),
                copy_resolved_terms(plain_tail_terms, split.docs_only_ordinals),
                plain_tail_position_offset, &docs_only_docs, decode_context, nullptr,
                &docs_only_tail_candidates));
        internal::union_sorted_into(docids, docs_only_docs);
    }
    *candidate_intersection_empty = docids->empty();
    return Status::OK();
}

} // namespace doris::snii::query::phrase_impl
