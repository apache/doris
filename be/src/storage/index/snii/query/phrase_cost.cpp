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

uint64_t plan_visible_posting_bytes(const format::DictEntry& entry, bool need_positions) {
    unsigned __int128 bytes = entry.kind == format::DictEntryKind::kInline
                                      ? entry.inline_dd_disk_len
                                      : entry.frq_docs_len;
    if (need_positions) {
        bytes += entry.kind == format::DictEntryKind::kInline ? entry.prx_bytes.size()
                                                              : entry.prx_len;
    }
    return bytes > std::numeric_limits<uint64_t>::max() ? std::numeric_limits<uint64_t>::max()
                                                        : static_cast<uint64_t>(bytes);
}

segment_v2::inverted_index::CommonGramsPlanRawCost phrase_plan_raw_cost(
        const PhysicalPhrasePlan& plan, const std::vector<std::string>& batch_terms,
        const std::vector<ResolvedQueryTerm>& resolved, const std::vector<uint8_t>& found,
        bool need_positions) {
    segment_v2::inverted_index::CommonGramsPlanRawCost cost;
    unsigned __int128 posting_bytes = 0;
    for (const std::string& term : plan.unique_terms) {
        const size_t batch_index = resolved_batch_index(batch_terms, term);
        if (found[batch_index] != 0) {
            posting_bytes +=
                    plan_visible_posting_bytes(resolved[batch_index].entry, need_positions);
        }
    }
    cost.posting_bytes_or_df_sum = posting_bytes > std::numeric_limits<uint64_t>::max()
                                           ? std::numeric_limits<uint64_t>::max()
                                           : static_cast<uint64_t>(posting_bytes);
    cost.estimated_candidate_df = std::numeric_limits<uint64_t>::max();
    for (size_t plan_index : plan.phrase_plan_index) {
        const size_t batch_index = resolved_batch_index(batch_terms, plan.unique_terms[plan_index]);
        if (found[batch_index] != 0) {
            cost.estimated_candidate_df =
                    std::min<uint64_t>(cost.estimated_candidate_df, resolved[batch_index].entry.df);
        }
    }
    cost.clause_count = static_cast<uint32_t>(plan.phrase_plan_index.size());
    if (cost.clause_count == 0) {
        cost.estimated_candidate_df = 0;
    }
    return cost;
}

segment_v2::inverted_index::CommonGramsPlanRawCost alternative_clause_raw_cost(
        const std::vector<ResolvedQueryTerm>& terms, bool need_positions) {
    segment_v2::inverted_index::CommonGramsPlanRawCost cost;
    unsigned __int128 posting_bytes = 0;
    unsigned __int128 candidate_df = 0;
    for (const auto& term : terms) {
        posting_bytes += plan_visible_posting_bytes(term.entry, need_positions);
        candidate_df += term.entry.df;
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

void append_alternative_clause_cost(
        const segment_v2::inverted_index::CommonGramsPlanRawCost& clause,
        segment_v2::inverted_index::CommonGramsPlanRawCost* plan) {
    const unsigned __int128 posting_bytes =
            static_cast<unsigned __int128>(plan->posting_bytes_or_df_sum) +
            clause.posting_bytes_or_df_sum;
    plan->posting_bytes_or_df_sum = posting_bytes > std::numeric_limits<uint64_t>::max()
                                            ? std::numeric_limits<uint64_t>::max()
                                            : static_cast<uint64_t>(posting_bytes);
    plan->estimated_candidate_df =
            plan->clause_count == 0
                    ? clause.estimated_candidate_df
                    : std::min(plan->estimated_candidate_df, clause.estimated_candidate_df);
    ++plan->clause_count;
}

segment_v2::inverted_index::CommonGramsPlanRawCost hybrid_verification_raw_cost(
        const segment_v2::inverted_index::CommonGramsPlanRawCost& prefilter_cost,
        const segment_v2::inverted_index::CommonGramsPlanRawCost& verification_cost) {
    auto cost = prefilter_cost;
    const unsigned __int128 posting_bytes =
            static_cast<unsigned __int128>(cost.posting_bytes_or_df_sum) +
            verification_cost.posting_bytes_or_df_sum;
    cost.posting_bytes_or_df_sum = posting_bytes > std::numeric_limits<uint64_t>::max()
                                           ? std::numeric_limits<uint64_t>::max()
                                           : static_cast<uint64_t>(posting_bytes);
    cost.estimated_candidate_df =
            std::min(cost.estimated_candidate_df, verification_cost.estimated_candidate_df);
    cost.clause_count = verification_cost.clause_count;
    return cost;
}

bool physical_phrase_plan_has_docs_only_term(const PhysicalPhrasePlan& plan,
                                             const std::vector<std::string>& batch_terms,
                                             const std::vector<ResolvedQueryTerm>& resolved) {
    for (const std::string& term : plan.unique_terms) {
        if (!entry_has_positions(resolved[resolved_batch_index(batch_terms, term)].entry)) {
            return true;
        }
    }
    return false;
}

void append_physical_phrase_clause(const PhysicalPhrasePlan& source, size_t clause,
                                   uint32_t position_offset, PhysicalPhrasePlan* target) {
    DORIS_CHECK_LT(clause, source.phrase_plan_index.size());
    DORIS_CHECK_EQ(source.phrase_plan_index.size(), source.position_offsets.size());
    DORIS_CHECK_EQ(source.phrase_plan_index.size(), source.common_gram_clauses.size());
    const size_t source_term = source.phrase_plan_index[clause];
    DORIS_CHECK_LT(source_term, source.unique_terms.size());
    const std::string& physical_term = source.unique_terms[source_term];

    const auto unique = std::ranges::find(target->unique_terms, physical_term);
    if (unique == target->unique_terms.end()) {
        target->phrase_plan_index.push_back(target->unique_terms.size());
        target->unique_terms.push_back(physical_term);
    } else {
        target->phrase_plan_index.push_back(
                static_cast<size_t>(unique - target->unique_terms.begin()));
    }
    target->position_offsets.push_back(position_offset);
    target->common_gram_clauses.push_back(source.common_gram_clauses[clause]);
}

HybridPrefixCostEstimate estimate_hybrid_prefix_plan_cost(
        const HybridPrefixPlanArtifact& artifact, const std::vector<std::string>& batch_terms,
        const std::vector<ResolvedQueryTerm>& resolved, const std::vector<uint8_t>& found,
        const std::vector<ResolvedQueryTerm>& plain_tail_terms, uint32_t position_verify_factor) {
    const HybridPositionedCover& plain_tail_cover = artifact.plain_tail_cover;
    const bool has_leading_prefilter =
            !plain_tail_cover.candidate_prefilter.phrase_plan_index.empty();
    const auto leading_prefilter_cost =
            phrase_plan_raw_cost(plain_tail_cover.candidate_prefilter, batch_terms, resolved, found,
                                 /*need_positions=*/false);
    unsigned __int128 posting_bytes = leading_prefilter_cost.posting_bytes_or_df_sum;
    unsigned __int128 candidate_df_sum = 0;
    unsigned __int128 position_verify_work = 0;
    uint32_t max_clause_count = 0;

    const auto append_branch = [&](const PhysicalPhrasePlan& verification,
                                   const auto& tail_verification_cost,
                                   const auto* candidate_filter_cost) {
        auto verification_cost = phrase_plan_raw_cost(verification, batch_terms, resolved, found,
                                                      /*need_positions=*/true);
        append_alternative_clause_cost(tail_verification_cost, &verification_cost);
        posting_bytes += verification_cost.posting_bytes_or_df_sum;
        uint64_t candidate_df = verification_cost.estimated_candidate_df;
        if (has_leading_prefilter) {
            candidate_df = std::min(candidate_df, leading_prefilter_cost.estimated_candidate_df);
        }
        if (candidate_filter_cost != nullptr) {
            posting_bytes += candidate_filter_cost->posting_bytes_or_df_sum;
            candidate_df = std::min(candidate_df, candidate_filter_cost->estimated_candidate_df);
        }
        candidate_df_sum += candidate_df;
        position_verify_work +=
                static_cast<unsigned __int128>(candidate_df) * verification_cost.clause_count;
        max_clause_count = std::max(max_clause_count, verification_cost.clause_count);
    };

    if (!artifact.maps_tail_to_gram) {
        DORIS_CHECK(has_leading_prefilter);
        DORIS_CHECK(artifact.mapped_tail_split.positioned_indices.empty());
        DORIS_CHECK(artifact.mapped_tail_split.docs_only_indices.empty());
        append_branch(
                plain_tail_cover.verification,
                alternative_clause_raw_cost(plain_tail_terms, /*need_positions=*/true),
                static_cast<const segment_v2::inverted_index::CommonGramsPlanRawCost*>(nullptr));
    } else {
        const HybridPrefixMappedTails& split = artifact.mapped_tail_split;
        DORIS_CHECK(!split.positioned_indices.empty() || !split.docs_only_indices.empty());
        DORIS_CHECK(has_leading_prefilter || !split.docs_only_indices.empty());
        if (!split.positioned_indices.empty()) {
            DORIS_CHECK(artifact.positioned_tail_verification.has_value());
            append_branch(*artifact.positioned_tail_verification,
                          alternative_clause_raw_cost(resolved, split.positioned_indices,
                                                      /*need_positions=*/true),
                          static_cast<const segment_v2::inverted_index::CommonGramsPlanRawCost*>(
                                  nullptr));
        }
        if (!split.docs_only_indices.empty()) {
            const auto docs_only_filter_cost =
                    alternative_clause_raw_cost(resolved, split.docs_only_indices,
                                                /*need_positions=*/false);
            append_branch(plain_tail_cover.verification,
                          alternative_clause_raw_cost(plain_tail_terms, split.docs_only_ordinals,
                                                      /*need_positions=*/true),
                          &docs_only_filter_cost);
        }
    }

    HybridPrefixCostEstimate result;
    result.raw_cost.posting_bytes_or_df_sum = posting_bytes > std::numeric_limits<uint64_t>::max()
                                                      ? std::numeric_limits<uint64_t>::max()
                                                      : static_cast<uint64_t>(posting_bytes);
    result.raw_cost.estimated_candidate_df = candidate_df_sum > std::numeric_limits<uint64_t>::max()
                                                     ? std::numeric_limits<uint64_t>::max()
                                                     : static_cast<uint64_t>(candidate_df_sum);
    result.raw_cost.clause_count = max_clause_count;
    const unsigned __int128 estimated_cost =
            posting_bytes + position_verify_work * position_verify_factor;
    result.estimated_cost = estimated_cost > std::numeric_limits<uint64_t>::max()
                                    ? std::numeric_limits<uint64_t>::max()
                                    : static_cast<uint64_t>(estimated_cost);
    return result;
}

} // namespace doris::snii::query::phrase_impl
