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

bool apply_common_grams_plan_debug_override(bool cost_prefers_gram,
                                            CommonGramsPlanDebugOverride debug_override) {
    switch (debug_override) {
    case CommonGramsPlanDebugOverride::kNone:
        return cost_prefers_gram;
    case CommonGramsPlanDebugOverride::kForcePlain:
        return false;
    case CommonGramsPlanDebugOverride::kForceCommonGrams:
        return true;
    }
    DORIS_CHECK(false);
    return cost_prefers_gram;
}

size_t position_span_size(std::pair<const uint32_t*, const uint32_t*> span) {
    if (span.first == span.second) {
        return 0;
    }
    DCHECK(span.first != nullptr);
    DCHECK(span.second != nullptr);
    return static_cast<size_t>(span.second - span.first);
}

bool should_use_monotonic_position_scan(std::pair<const uint32_t*, const uint32_t*> anchor_span,
                                        size_t checked_span_size, uint32_t anchor_offset,
                                        uint32_t checked_offset) {
    const uint64_t anchor_count = position_span_size(anchor_span);
    const uint64_t binary_search_upper_bound =
            anchor_count * (static_cast<uint64_t>(std::bit_width(checked_span_size)) + 1);
    const uint64_t monotonic_scan_upper_bound = checked_span_size + 2 * anchor_count + 2;

    // Require a 2x comparison margin before paying even the O(1) validity
    // checks and adding the scan-path branches. This keeps low-TF spans on the
    // simpler binary-search path while retaining the high-TF dense case.
    if (2 * monotonic_scan_upper_bound > binary_search_upper_bound) {
        return false;
    }

    // Scanning is considered only when every anchor yields a representable
    // phrase start and checked-term position. Endpoint checks are sufficient
    // because anchor positions are sorted; invalid boundary shapes stay on the
    // existing per-anchor path without extra binary searches in this gate.
    if (*anchor_span.first < anchor_offset) {
        return false;
    }
    if (checked_offset <= anchor_offset) {
        return true;
    }
    const uint32_t offset_delta = checked_offset - anchor_offset;
    return anchor_span.second[-1] <= std::numeric_limits<uint32_t>::max() - offset_delta;
}

bool has_common_grams_capability(
        const LogicalIndexReader& idx,
        const segment_v2::inverted_index::CommonGramsQueryIdentity* query_identity) {
    const auto* metadata = idx.common_grams_metadata();
    if (metadata == nullptr || query_identity == nullptr) {
        return false;
    }
    if (idx.common_grams_posting_policy() == format::CommonGramsPostingPolicy::kHybridV1) {
        return segment_v2::inverted_index::is_common_grams_query_compatible(
                *metadata, *query_identity,
                segment_v2::inverted_index::CommonGramsCoverage::kMixed);
    }
    return segment_v2::inverted_index::is_common_grams_query_compatible(*metadata, *query_identity);
}

bool entry_has_positions(const format::DictEntry& entry) {
    return entry.kind == format::DictEntryKind::kInline ? !entry.prx_bytes.empty()
                                                        : entry.prx_len != 0;
}

Status build_physical_phrase_plan_prefix(const LogicalIndexReader& idx,
                                         const segment_v2::InvertedIndexQueryInfo& query_info,
                                         size_t clause_count, bool allow_common_grams,
                                         PhysicalPhrasePlan* plan, bool* all_representable) {
    plan->unique_terms.clear();
    plan->phrase_plan_index.clear();
    plan->position_offsets.clear();
    plan->common_gram_clauses.clear();
    *all_representable = true;
    DORIS_CHECK_LE(clause_count, query_info.term_infos.size());
    if (clause_count == 0) {
        return Status::OK();
    }

    const int32_t first_position = query_info.term_infos.front().position;
    DORIS_CHECK_LE(clause_count, static_cast<size_t>(std::numeric_limits<uint32_t>::max()));
    plan->phrase_plan_index.reserve(clause_count);
    plan->position_offsets.reserve(clause_count);
    plan->common_gram_clauses.reserve(clause_count);
    for (size_t i = 0; i < clause_count; ++i) {
        const segment_v2::TermInfo& term_info = query_info.term_infos[i];
        DORIS_CHECK(term_info.is_single_term());
        DORIS_CHECK_EQ(static_cast<int64_t>(term_info.position),
                       static_cast<int64_t>(first_position) + static_cast<int64_t>(i));

        std::string physical_term;
        if (term_info.key_kind == segment_v2::TermKeyKind::kCommonGram) {
            DORIS_CHECK(allow_common_grams);
            DORIS_CHECK(std::string_view(term_info.get_single_term())
                                .starts_with(segment_v2::inverted_index::CG_V1_MARKER));
            physical_term = term_info.get_single_term();
        } else {
            bool representable = false;
            RETURN_IF_ERROR(internal::route_plain_query_term(idx, term_info.get_single_term(),
                                                             &physical_term, &representable));
            if (!representable) {
                *all_representable = false;
                return Status::OK();
            }
        }

        auto unique = std::ranges::find(plan->unique_terms, physical_term);
        if (unique == plan->unique_terms.end()) {
            plan->phrase_plan_index.push_back(plan->unique_terms.size());
            plan->unique_terms.push_back(std::move(physical_term));
        } else {
            plan->phrase_plan_index.push_back(
                    static_cast<size_t>(unique - plan->unique_terms.begin()));
        }
        plan->position_offsets.push_back(static_cast<uint32_t>(i));
        plan->common_gram_clauses.push_back(
                static_cast<uint8_t>(term_info.key_kind == segment_v2::TermKeyKind::kCommonGram));
    }
    return Status::OK();
}

Status build_physical_phrase_plan(const LogicalIndexReader& idx,
                                  const segment_v2::InvertedIndexQueryInfo& query_info,
                                  bool allow_common_grams, PhysicalPhrasePlan* plan,
                                  bool* all_representable) {
    return build_physical_phrase_plan_prefix(idx, query_info, query_info.term_infos.size(),
                                             allow_common_grams, plan, all_representable);
}

size_t resolved_batch_index(const std::vector<std::string>& batch_terms, std::string_view term) {
    const auto it = std::ranges::lower_bound(batch_terms, term);
    DORIS_CHECK(it != batch_terms.end());
    DORIS_CHECK_EQ(*it, term);
    return static_cast<size_t>(it - batch_terms.begin());
}

bool all_plan_terms_present(const PhysicalPhrasePlan& plan,
                            const std::vector<std::string>& batch_terms,
                            const std::vector<uint8_t>& found) {
    for (const std::string& term : plan.unique_terms) {
        if (found[resolved_batch_index(batch_terms, term)] == 0) {
            return false;
        }
    }
    return true;
}

internal::ResolvedPhrasePlan materialize_resolved_phrase_plan(
        const PhysicalPhrasePlan& plan, const std::vector<std::string>& batch_terms,
        std::vector<ResolvedQueryTerm>* resolved) {
    internal::ResolvedPhrasePlan result;
    result.phrase_plan_index = plan.phrase_plan_index;
    result.position_offsets = plan.position_offsets;
    result.unique_terms.reserve(plan.unique_terms.size());
    for (const std::string& term : plan.unique_terms) {
        result.unique_terms.push_back(
                std::move((*resolved)[resolved_batch_index(batch_terms, term)]));
    }
    return result;
}

internal::ResolvedPhrasePlan copy_resolved_phrase_plan(
        const PhysicalPhrasePlan& plan, const std::vector<std::string>& batch_terms,
        const std::vector<ResolvedQueryTerm>& resolved) {
    internal::ResolvedPhrasePlan result;
    result.phrase_plan_index = plan.phrase_plan_index;
    result.position_offsets = plan.position_offsets;
    result.unique_terms.reserve(plan.unique_terms.size());
    for (const std::string& term : plan.unique_terms) {
        result.unique_terms.push_back(resolved[resolved_batch_index(batch_terms, term)]);
    }
    return result;
}

namespace {
PhysicalPhrasePlan build_hybrid_positioned_verification(
        const PhysicalPhrasePlan& plain_plan, const PhysicalPhrasePlan& gram_plan,
        const std::vector<std::string>& batch_terms, const std::vector<ResolvedQueryTerm>& resolved,
        bool tail_covers_last_plain_clause, PhysicalPhrasePlan* candidate_prefilter) {
    const size_t original_clause_count = plain_plan.phrase_plan_index.size();
    DORIS_CHECK_GT(original_clause_count, 0);
    DORIS_CHECK_EQ(plain_plan.position_offsets.size(), original_clause_count);
    DORIS_CHECK_EQ(plain_plan.common_gram_clauses.size(), original_clause_count);
    DORIS_CHECK_EQ(gram_plan.position_offsets.size(), gram_plan.phrase_plan_index.size());
    DORIS_CHECK_EQ(gram_plan.common_gram_clauses.size(), gram_plan.phrase_plan_index.size());

    PhysicalPhrasePlan verification;
    std::vector<size_t> positioned_gram_at(original_clause_count,
                                           gram_plan.phrase_plan_index.size());
    for (size_t clause = 0; clause < gram_plan.phrase_plan_index.size(); ++clause) {
        if (gram_plan.common_gram_clauses[clause] == 0) {
            continue;
        }
        const size_t gram_term = gram_plan.phrase_plan_index[clause];
        DORIS_CHECK_LT(gram_term, gram_plan.unique_terms.size());
        const size_t batch_index =
                resolved_batch_index(batch_terms, gram_plan.unique_terms[gram_term]);
        if (!entry_has_positions(resolved[batch_index].entry)) {
            if (candidate_prefilter != nullptr) {
                append_physical_phrase_clause(gram_plan, clause, gram_plan.position_offsets[clause],
                                              candidate_prefilter);
            }
            continue;
        }

        const size_t original_offset = gram_plan.position_offsets[clause];
        DORIS_CHECK_LT(original_offset + 1, original_clause_count);
        DORIS_CHECK_EQ(positioned_gram_at[original_offset], gram_plan.phrase_plan_index.size());
        positioned_gram_at[original_offset] = clause;
    }
    // Start from every positioned gram edge, then remove an edge only when both
    // endpoint tokens remain covered by another positioned edge. The left-to-right
    // pass produces a minimum-clause edge cover while retaining grams in preference
    // to adding their two plain components during the pass below.
    std::vector<uint8_t> positioned_coverage(original_clause_count, 0);
    if (tail_covers_last_plain_clause) {
        ++positioned_coverage.back();
    }
    for (size_t original_offset = 0; original_offset < original_clause_count; ++original_offset) {
        if (positioned_gram_at[original_offset] == gram_plan.phrase_plan_index.size()) {
            continue;
        }
        ++positioned_coverage[original_offset];
        ++positioned_coverage[original_offset + 1];
    }
    for (size_t original_offset = 0; original_offset < original_clause_count; ++original_offset) {
        if (positioned_gram_at[original_offset] == gram_plan.phrase_plan_index.size() ||
            positioned_coverage[original_offset] <= 1 ||
            positioned_coverage[original_offset + 1] <= 1) {
            continue;
        }
        positioned_gram_at[original_offset] = gram_plan.phrase_plan_index.size();
        --positioned_coverage[original_offset];
        --positioned_coverage[original_offset + 1];
    }

    for (size_t original_offset = 0; original_offset < original_clause_count; ++original_offset) {
        const size_t gram_clause = positioned_gram_at[original_offset];
        if (gram_clause != gram_plan.phrase_plan_index.size()) {
            append_physical_phrase_clause(gram_plan, gram_clause,
                                          static_cast<uint32_t>(original_offset), &verification);
            continue;
        }
        if (positioned_coverage[original_offset] == 0) {
            DORIS_CHECK_EQ(plain_plan.position_offsets[original_offset], original_offset);
            append_physical_phrase_clause(plain_plan, original_offset,
                                          static_cast<uint32_t>(original_offset), &verification);
        }
    }
    DORIS_CHECK(!verification.phrase_plan_index.empty() ||
                (tail_covers_last_plain_clause && original_clause_count == 1));
    return verification;
}

HybridPositionedCover build_hybrid_positioned_cover(const PhysicalPhrasePlan& plain_plan,
                                                    const PhysicalPhrasePlan& gram_plan,
                                                    const std::vector<std::string>& batch_terms,
                                                    const std::vector<ResolvedQueryTerm>& resolved,
                                                    bool tail_covers_last_plain_clause) {
    HybridPositionedCover result;
    result.verification = build_hybrid_positioned_verification(
            plain_plan, gram_plan, batch_terms, resolved, tail_covers_last_plain_clause,
            &result.candidate_prefilter);
    return result;
}

} // namespace
HybridExactPlanArtifact build_hybrid_exact_plan_artifact(
        const PhysicalPhrasePlan& plain_plan, const PhysicalPhrasePlan& gram_plan,
        const std::vector<std::string>& batch_terms,
        const std::vector<ResolvedQueryTerm>& resolved) {
    HybridExactPlanArtifact artifact;
    if (gram_plan.phrase_plan_index.size() > 1 &&
        physical_phrase_plan_has_docs_only_term(gram_plan, batch_terms, resolved)) {
        artifact.positioned_cover.emplace(
                build_hybrid_positioned_cover(plain_plan, gram_plan, batch_terms, resolved,
                                              /*tail_covers_last_plain_clause=*/false));
        DORIS_CHECK(!artifact.positioned_cover->candidate_prefilter.phrase_plan_index.empty());
    }
    return artifact;
}

namespace {
Status build_physical_phrase_plan_candidates(const LogicalIndexReader& idx,
                                             const PhysicalPhrasePlan& plan,
                                             const std::vector<std::string>& batch_terms,
                                             const std::vector<ResolvedQueryTerm>& resolved,
                                             std::vector<uint32_t>* candidates) {
    std::vector<ResolvedQueryTerm> candidate_terms;
    candidate_terms.reserve(plan.unique_terms.size());
    for (const std::string& term : plan.unique_terms) {
        candidate_terms.push_back(resolved[resolved_batch_index(batch_terms, term)]);
    }

    io::BatchRangeFetcher round1(idx.reader());
    std::vector<TermPlan> plans;
    RETURN_IF_ERROR(internal::plan_resolved_terms(idx, std::move(candidate_terms), &round1, &plans,
                                                  /*need_positions=*/false));
    if (round1.pending() > 0) {
        RETURN_IF_ERROR(round1.fetch());
    }
    RETURN_IF_ERROR(internal::open_preludes(round1, &plans, /*need_positions=*/false));
    return internal::build_docid_only_conjunction(idx, round1, plans, candidates);
}

HybridPrefixMappedTails split_hybrid_prefix_mapped_tails(
        const std::vector<ResolvedQueryTerm>& resolved,
        const std::vector<ResolvedMappedTail>& mapped_tails) {
    HybridPrefixMappedTails result;
    for (const ResolvedMappedTail& tail : mapped_tails) {
        DORIS_CHECK_LT(tail.batch_index, resolved.size());
        if (entry_has_positions(resolved[tail.batch_index].entry)) {
            result.positioned_indices.push_back(tail.batch_index);
        } else {
            result.docs_only_indices.push_back(tail.batch_index);
            result.docs_only_ordinals.push_back(tail.expansion_ordinal);
        }
    }
    return result;
}

} // namespace
std::optional<HybridPrefixPlanArtifact> try_build_hybrid_prefix_plan_artifact(
        const PhysicalPhrasePlan& plain_leading, const PhysicalPhrasePlan& gram_leading,
        const std::vector<std::string>& batch_terms, const std::vector<ResolvedQueryTerm>& resolved,
        const std::vector<ResolvedMappedTail>& mapped_tails, bool maps_tail_to_gram) {
    const bool requires_plain_verification =
            physical_phrase_plan_has_docs_only_term(gram_leading, batch_terms, resolved) ||
            std::ranges::any_of(mapped_tails, [&](const ResolvedMappedTail& tail) {
                DORIS_CHECK_LT(tail.batch_index, resolved.size());
                return !entry_has_positions(resolved[tail.batch_index].entry);
            });
    if (!requires_plain_verification) {
        return std::nullopt;
    }

    DORIS_CHECK(!plain_leading.phrase_plan_index.empty());
    DORIS_CHECK_LE(plain_leading.phrase_plan_index.size(),
                   static_cast<size_t>(std::numeric_limits<uint32_t>::max()));
    HybridPrefixPlanArtifact artifact;
    artifact.plain_tail_cover =
            build_hybrid_positioned_cover(plain_leading, gram_leading, batch_terms, resolved,
                                          /*tail_covers_last_plain_clause=*/false);
    artifact.plain_tail_position_offset =
            static_cast<uint32_t>(plain_leading.phrase_plan_index.size());
    artifact.maps_tail_to_gram = maps_tail_to_gram;
    if (!maps_tail_to_gram) {
        DORIS_CHECK(mapped_tails.empty());
        return artifact;
    }

    DORIS_CHECK(!mapped_tails.empty());
    artifact.mapped_tail_split = split_hybrid_prefix_mapped_tails(resolved, mapped_tails);
    if (!artifact.mapped_tail_split.positioned_indices.empty()) {
        artifact.positioned_tail_verification.emplace(build_hybrid_positioned_verification(
                plain_leading, gram_leading, batch_terms, resolved,
                /*tail_covers_last_plain_clause=*/true,
                /*candidate_prefilter=*/nullptr));
    }
    return artifact;
}

Status build_hybrid_leading_candidates(const LogicalIndexReader& idx,
                                       const PhysicalPhrasePlan& candidate_prefilter,
                                       const std::vector<std::string>& batch_terms,
                                       const std::vector<ResolvedQueryTerm>& resolved,
                                       HybridPrefixCandidateSet* candidates) {
    candidates->active = !candidate_prefilter.phrase_plan_index.empty();
    candidates->docs.clear();
    if (!candidates->active) {
        return Status::OK();
    }
    return build_physical_phrase_plan_candidates(idx, candidate_prefilter, batch_terms, resolved,
                                                 &candidates->docs);
}

namespace {
Status build_tail_candidates_within_leading(const LogicalIndexReader& idx,
                                            const std::vector<ResolvedQueryTerm>& resolved,
                                            const std::vector<size_t>& tail_indices,
                                            const std::vector<uint32_t>& leading_candidates,
                                            std::vector<uint32_t>* candidates) {
    std::vector<ResolvedQueryTerm> tail_terms;
    tail_terms.reserve(tail_indices.size());
    for (size_t index : tail_indices) {
        DORIS_CHECK_LT(index, resolved.size());
        tail_terms.push_back(resolved[index]);
    }

    io::BatchRangeFetcher round1(idx.reader());
    std::vector<TermPlan> tail_plans;
    RETURN_IF_ERROR(internal::plan_resolved_terms(idx, std::move(tail_terms), &round1, &tail_plans,
                                                  /*need_positions=*/false));
    if (round1.pending() > 0) {
        RETURN_IF_ERROR(round1.fetch());
    }
    RETURN_IF_ERROR(internal::open_preludes(round1, &tail_plans, /*need_positions=*/false));

    candidates->clear();
    std::vector<TermPlan> one_tail_plan;
    one_tail_plan.reserve(1);
    for (auto& tail_plan : tail_plans) {
        one_tail_plan.clear();
        one_tail_plan.push_back(std::move(tail_plan));
        std::vector<uint32_t> tail_matches;
        RETURN_IF_ERROR(internal::filter_docids_by_conjunction(
                idx, round1, one_tail_plan, leading_candidates, &tail_matches, nullptr));
        internal::union_sorted_into(candidates, tail_matches);
        if (candidates->size() == leading_candidates.size()) {
            break;
        }
    }
    return Status::OK();
}

} // namespace
Status build_hybrid_docs_only_tail_candidates(const LogicalIndexReader& idx,
                                              const std::vector<ResolvedQueryTerm>& resolved,
                                              const std::vector<size_t>& gram_tail_indices,
                                              const HybridPrefixCandidateSet& leading_candidates,
                                              std::vector<uint32_t>* candidates) {
    DORIS_CHECK(!gram_tail_indices.empty());
    unsigned __int128 tail_df_sum = 0;
    for (size_t index : gram_tail_indices) {
        DORIS_CHECK_LT(index, resolved.size());
        tail_df_sum += resolved[index].entry.df;
    }
    if (leading_candidates.active &&
        static_cast<unsigned __int128>(leading_candidates.docs.size()) <= tail_df_sum) {
        return build_tail_candidates_within_leading(idx, resolved, gram_tail_indices,
                                                    leading_candidates.docs, candidates);
    }

    std::vector<internal::ResolvedDocidPosting> tail_postings;
    tail_postings.reserve(gram_tail_indices.size());
    for (size_t index : gram_tail_indices) {
        const auto& tail = resolved[index];
        tail_postings.push_back({tail.entry, tail.frq_base, tail.prx_base});
    }
    std::vector<uint32_t> tail_candidates;
    RETURN_IF_ERROR(internal::build_docid_union(idx, tail_postings, &tail_candidates));
    *candidates = leading_candidates.active
                          ? internal::intersect_sorted(leading_candidates.docs, tail_candidates)
                          : std::move(tail_candidates);
    return Status::OK();
}

Status execute_hybrid_exact_phrase_plan(
        const LogicalIndexReader& idx, const PhysicalPhrasePlan& gram_plan,
        const std::vector<std::string>& batch_terms, const HybridExactPlanArtifact& artifact,
        std::vector<ResolvedQueryTerm>* resolved, std::vector<uint32_t>* docids,
        format::PrxDecodeContext* decode_context, bool* candidate_intersection_empty) {
    DORIS_CHECK(idx.common_grams_posting_policy() == format::CommonGramsPostingPolicy::kHybridV1);
    if (candidate_intersection_empty != nullptr) {
        *candidate_intersection_empty = false;
    }
    if (!artifact.positioned_cover.has_value()) {
        auto resolved_gram = materialize_resolved_phrase_plan(gram_plan, batch_terms, resolved);
        return internal::execute_resolved_phrase_plan(idx, std::move(resolved_gram), docids,
                                                      decode_context);
    }

    const HybridPositionedCover& hybrid_plan = *artifact.positioned_cover;
    std::vector<uint32_t> gram_candidates;
    RETURN_IF_ERROR(build_physical_phrase_plan_candidates(
            idx, hybrid_plan.candidate_prefilter, batch_terms, *resolved, &gram_candidates));
    if (gram_candidates.empty()) {
        if (candidate_intersection_empty != nullptr) {
            *candidate_intersection_empty = true;
        }
        return Status::OK();
    }
    auto resolved_verification =
            materialize_resolved_phrase_plan(hybrid_plan.verification, batch_terms, resolved);
    return internal::execute_resolved_phrase_plan(idx, std::move(resolved_verification), docids,
                                                  decode_context, nullptr, &gram_candidates);
}

void append_resolved_phrase_clause(ResolvedQueryTerm term, uint32_t position_offset,
                                   internal::ResolvedPhrasePlan* plan) {
    const auto unique = std::ranges::find(plan->unique_terms, term.entry.term,
                                          [](const ResolvedQueryTerm& candidate) {
                                              return std::string_view(candidate.entry.term);
                                          });
    if (unique == plan->unique_terms.end()) {
        plan->phrase_plan_index.push_back(plan->unique_terms.size());
        plan->unique_terms.push_back(std::move(term));
    } else {
        plan->phrase_plan_index.push_back(static_cast<size_t>(unique - plan->unique_terms.begin()));
    }
    plan->position_offsets.push_back(position_offset);
}

internal::ResolvedPhrasePlan build_resolved_phrase_plan(
        std::vector<ResolvedQueryTerm> resolved_terms) {
    internal::ResolvedPhrasePlan plan;
    plan.unique_terms.reserve(resolved_terms.size());
    plan.phrase_plan_index.reserve(resolved_terms.size());
    plan.position_offsets.reserve(resolved_terms.size());
    for (size_t i = 0; i < resolved_terms.size(); ++i) {
        DORIS_CHECK_LE(i, static_cast<size_t>(std::numeric_limits<uint32_t>::max()));
        append_resolved_phrase_clause(std::move(resolved_terms[i]), static_cast<uint32_t>(i),
                                      &plan);
    }
    return plan;
}

Status resolve_and_execute_physical_phrase_plan(const LogicalIndexReader& idx,
                                                const PhysicalPhrasePlan& plan,
                                                std::vector<uint32_t>* docids,
                                                format::PrxDecodeContext* decode_context,
                                                CommonGramsPlanningTimer& planning_timer) {
    std::vector<std::string> batch_terms = plan.unique_terms;
    std::ranges::sort(batch_terms);
    std::vector<ResolvedQueryTerm> resolved;
    std::vector<uint8_t> found;
    RETURN_IF_ERROR(internal::resolve_query_terms_batch(idx, batch_terms, &resolved, &found));
    if (!all_plan_terms_present(plan, batch_terms, found)) {
        return Status::OK();
    }
    auto resolved_plan = materialize_resolved_phrase_plan(plan, batch_terms, &resolved);
    planning_timer.finish();
    return internal::execute_resolved_phrase_plan(idx, std::move(resolved_plan), docids,
                                                  decode_context);
}

} // namespace doris::snii::query::phrase_impl
