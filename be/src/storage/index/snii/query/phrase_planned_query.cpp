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

Status planned_exact_phrase_query_impl(
        const LogicalIndexReader& idx, const segment_v2::InvertedIndexQueryInfo& plain_query_info,
        const segment_v2::InvertedIndexQueryInfo& gram_query_info,
        const segment_v2::inverted_index::CommonGramsQueryIdentity* common_grams_identity,
        std::vector<uint32_t>* docids, format::PrxDecodeContext* decode_context,
        ExactPhrasePlanKind* selected_plan,
        segment_v2::inverted_index::CommonGramsPlanCostModel cost_model,
        CommonGramsPlanDebugOverride debug_override) {
    if (docids == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "planned_exact_phrase_query: null out");
    }
    docids->clear();
    DORIS_CHECK(!plain_query_info.has_common_gram());
    auto* query_stats = decode_context == nullptr ? nullptr : decode_context->query_stats;
    CommonGramsPlanningTimer planning_timer(query_stats);
    if (query_stats != nullptr) {
        ++query_stats->common_grams_candidate_queries;
    }

    PhysicalPhrasePlan plain_plan;
    bool plain_representable = false;
    RETURN_IF_ERROR(build_physical_phrase_plan(idx, plain_query_info,
                                               /*allow_common_grams=*/false, &plain_plan,
                                               &plain_representable));
    if (!plain_representable || plain_plan.phrase_plan_index.empty()) {
        if (query_stats != nullptr) {
            ++query_stats->common_grams_plain_plans;
        }
        if (selected_plan != nullptr) {
            *selected_plan = ExactPhrasePlanKind::kPlain;
        }
        return Status::OK();
    }

    const bool index_has_common_grams = has_common_grams_capability(idx, common_grams_identity);
    const bool gram_capable = gram_query_info.has_common_gram() && index_has_common_grams;
    if (!gram_capable) {
        if (query_stats != nullptr) {
            ++query_stats->common_grams_plain_plans;
            if (gram_query_info.has_common_gram()) {
                ++query_stats->common_grams_fallback_incompatible;
            } else {
                ++query_stats->common_grams_fallback_no_gram;
            }
        }
        if (selected_plan != nullptr) {
            *selected_plan = ExactPhrasePlanKind::kPlain;
        }
        return resolve_and_execute_physical_phrase_plan(idx, plain_plan, docids, decode_context,
                                                        planning_timer);
    }

    PhysicalPhrasePlan gram_plan;
    bool gram_representable = false;
    RETURN_IF_ERROR(build_physical_phrase_plan(idx, gram_query_info,
                                               /*allow_common_grams=*/true, &gram_plan,
                                               &gram_representable));
    DORIS_CHECK(gram_representable);
    DORIS_CHECK(!gram_plan.phrase_plan_index.empty());

    std::vector<std::string> batch_terms = plain_plan.unique_terms;
    batch_terms.insert(batch_terms.end(), gram_plan.unique_terms.begin(),
                       gram_plan.unique_terms.end());
    std::ranges::sort(batch_terms);
    batch_terms.erase(std::unique(batch_terms.begin(), batch_terms.end()), batch_terms.end());

    std::vector<ResolvedQueryTerm> resolved;
    std::vector<uint8_t> found;
    RETURN_IF_ERROR(internal::resolve_query_terms_batch(idx, batch_terms, &resolved, &found));
    const bool plain_present = all_plan_terms_present(plain_plan, batch_terms, found);
    const bool gram_present = all_plan_terms_present(gram_plan, batch_terms, found);
    if (!plain_present || !gram_present) {
        if (query_stats != nullptr) {
            if (plain_present) {
                ++query_stats->common_grams_gram_plans;
                ++query_stats->common_grams_authoritative_empty;
            } else {
                ++query_stats->common_grams_plain_plans;
                ++query_stats->common_grams_authoritative_empty;
            }
        }
        if (selected_plan != nullptr) {
            *selected_plan =
                    plain_present ? ExactPhrasePlanKind::kCommonGrams : ExactPhrasePlanKind::kPlain;
        }
        return Status::OK();
    }

    std::optional<HybridExactPlanArtifact> hybrid_artifact;
    if (idx.common_grams_posting_policy() == format::CommonGramsPostingPolicy::kHybridV1) {
        hybrid_artifact.emplace(
                build_hybrid_exact_plan_artifact(plain_plan, gram_plan, batch_terms, resolved));
    }
    const bool plain_needs_positions = plain_plan.phrase_plan_index.size() > 1;
    const bool hybrid_gram_verification =
            hybrid_artifact.has_value() && hybrid_artifact->positioned_cover.has_value();
    const bool gram_needs_positions =
            gram_plan.phrase_plan_index.size() > 1 && !hybrid_gram_verification;
    const auto plain_raw_cost =
            phrase_plan_raw_cost(plain_plan, batch_terms, resolved, found, plain_needs_positions);
    segment_v2::inverted_index::CommonGramsPlanRawCost gram_raw_cost;
    if (hybrid_gram_verification) {
        const HybridPositionedCover& hybrid_plan = *hybrid_artifact->positioned_cover;
        const auto gram_prefilter_raw_cost =
                phrase_plan_raw_cost(hybrid_plan.candidate_prefilter, batch_terms, resolved, found,
                                     /*need_positions=*/false);
        const auto gram_verification_raw_cost =
                phrase_plan_raw_cost(hybrid_plan.verification, batch_terms, resolved, found,
                                     /*need_positions=*/true);
        gram_raw_cost =
                hybrid_verification_raw_cost(gram_prefilter_raw_cost, gram_verification_raw_cost);
    } else {
        gram_raw_cost =
                phrase_plan_raw_cost(gram_plan, batch_terms, resolved, found, gram_needs_positions);
    }
    const uint64_t plain_cost = segment_v2::inverted_index::estimate_common_grams_plan_cost(
            plain_raw_cost, plain_needs_positions ? cost_model.position_verify_factor : 0);
    const uint64_t gram_cost = segment_v2::inverted_index::estimate_common_grams_plan_cost(
            gram_raw_cost, (gram_needs_positions || hybrid_gram_verification)
                                   ? cost_model.position_verify_factor
                                   : 0);
    const bool cost_prefers_gram = segment_v2::inverted_index::common_grams_plan_cost_wins(
            plain_cost, gram_cost, cost_model.common_grams_cost_ratio_percent);
    const bool use_gram = apply_common_grams_plan_debug_override(cost_prefers_gram, debug_override);
    if (query_stats != nullptr) {
        query_stats->common_grams_plain_posting_bytes += plain_raw_cost.posting_bytes_or_df_sum;
        query_stats->common_grams_gram_posting_bytes += gram_raw_cost.posting_bytes_or_df_sum;
        query_stats->common_grams_plain_estimated_candidate_df +=
                plain_raw_cost.estimated_candidate_df;
        query_stats->common_grams_gram_estimated_candidate_df +=
                gram_raw_cost.estimated_candidate_df;
        query_stats->common_grams_plain_estimated_cost += plain_cost;
        query_stats->common_grams_gram_estimated_cost += gram_cost;
    }
    const ExactPhrasePlanKind chosen_kind =
            use_gram ? ExactPhrasePlanKind::kCommonGrams : ExactPhrasePlanKind::kPlain;
    if (query_stats != nullptr) {
        if (use_gram) {
            ++query_stats->common_grams_gram_plans;
        } else {
            ++query_stats->common_grams_plain_plans;
            if (debug_override == CommonGramsPlanDebugOverride::kNone) {
                ++query_stats->common_grams_fallback_cost;
            }
        }
    }
    if (selected_plan != nullptr) {
        *selected_plan = chosen_kind;
    }
    planning_timer.finish();
    if (use_gram &&
        idx.common_grams_posting_policy() == format::CommonGramsPostingPolicy::kHybridV1) {
        DORIS_CHECK(hybrid_artifact.has_value());
        bool candidate_intersection_empty = false;
        RETURN_IF_ERROR(execute_hybrid_exact_phrase_plan(
                idx, gram_plan, batch_terms, *hybrid_artifact, &resolved, docids, decode_context,
                &candidate_intersection_empty));
        if (candidate_intersection_empty) {
            if (query_stats != nullptr) {
                ++query_stats->common_grams_authoritative_empty;
            }
        }
        return Status::OK();
    }
    auto resolved_plan = materialize_resolved_phrase_plan(use_gram ? gram_plan : plain_plan,
                                                          batch_terms, &resolved);
    return internal::execute_resolved_phrase_plan(idx, std::move(resolved_plan), docids,
                                                  decode_context);
}

Status phrase_query_impl(const LogicalIndexReader& idx, const std::vector<std::string>& terms,
                         std::vector<uint32_t>* const docids,
                         format::PrxDecodeContext* decode_context,
                         std::vector<PhraseMatch>* matches, const PhraseQueryOptions& options) {
    if (docids == nullptr && matches == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("phrase_query: null out");
    }
    if (docids != nullptr) {
        docids->clear();
    }
    if (matches != nullptr) {
        matches->clear();
    }
    if (terms.empty()) {
        return Status::OK();
    }
    if (terms.size() == 1) {
        DORIS_CHECK(matches == nullptr);
        return term_query(idx, terms.front(), docids);
    }
    if (!idx.has_positions()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED, false>(
                "phrase_query: index has no positions");
    }
    io::BatchRangeFetcher round1(idx.reader());
    const PhraseTermMapping mapping = build_phrase_term_mapping(terms);
    std::vector<TermPlan> plans;
    bool all_present = false;
    RETURN_IF_ERROR(internal::plan_terms(idx, mapping.unique_terms, &round1, &plans, &all_present,
                                         /*need_positions=*/false));
    if (!all_present) {
        return Status::OK();
    }
    return execute_phrase_plans(idx, &round1, &plans, mapping.phrase_plan_index, docids,
                                decode_context, matches, options);
}

Status phrase_prefix_query_impl(const LogicalIndexReader& idx,
                                const std::vector<std::string>& terms,
                                std::vector<uint32_t>* const docids, int32_t max_expansions,
                                format::PrxDecodeContext* decode_context,
                                CommonGramsPlanningTimer* planning_timer,
                                std::vector<PhraseMatch>* matches) {
    if (docids == nullptr && matches == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("phrase_prefix_query: null out");
    }
    if (docids != nullptr) {
        docids->clear();
    }
    if (matches != nullptr) {
        matches->clear();
    }
    if (terms.empty()) {
        return Status::OK();
    }
    if (terms.size() == 1) {
        DORIS_CHECK(matches == nullptr);
        if (planning_timer != nullptr) {
            planning_timer->finish();
        }
        return prefix_query(idx, terms.front(), docids, max_expansions);
    }
    std::vector<ResolvedQueryTerm> exact_terms;
    exact_terms.reserve(terms.size() - 1);
    std::string physical_term_scratch;
    for (size_t i = 0; i + 1 < terms.size(); ++i) {
        std::string_view physical_term;
        bool representable = false;
        RETURN_IF_ERROR(internal::route_plain_query_term_view(idx, terms[i], &physical_term_scratch,
                                                              &physical_term, &representable));
        if (!representable) {
            return Status::OK();
        }
        ResolvedQueryTerm resolved;
        bool found = false;
        RETURN_IF_ERROR(internal::resolve_query_term(idx, physical_term, &resolved, &found));
        if (!found) {
            return Status::OK();
        }
        exact_terms.push_back(std::move(resolved));
    }

    // Expand the tail in the logical plain namespace. The visitor range-seeks
    // past typed internal namespaces before counting max_expansions and decodes
    // escaped physical keys before applying the logical prefix.
    std::vector<LogicalIndexReader::PrefixHit> tail_hits;
    RETURN_IF_ERROR(internal::visit_expanded_plain_terms(
            idx, terms.back(), [](std::string_view) { return true; },
            [&](LogicalIndexReader::PrefixHit&& hit, bool*) {
                tail_hits.push_back(std::move(hit));
                return Status::OK();
            },
            max_expansions));
    if (tail_hits.empty()) {
        return Status::OK();
    }
    std::vector<ResolvedQueryTerm> tail_terms;
    tail_terms.reserve(tail_hits.size());
    for (auto& hit : tail_hits) {
        tail_terms.push_back(ResolvedQueryTerm {
                .entry = std::move(hit.entry), .frq_base = hit.frq_base, .prx_base = hit.prx_base});
    }
    auto exact_plan = build_resolved_phrase_plan(std::move(exact_terms));
    if (planning_timer != nullptr) {
        planning_timer->finish();
    }
    DORIS_CHECK_LE(terms.size() - 1, static_cast<size_t>(std::numeric_limits<uint32_t>::max()));
    return execute_resolved_phrase_prefix_terms(idx, std::move(exact_plan), std::move(tail_terms),
                                                static_cast<uint32_t>(terms.size() - 1), docids,
                                                decode_context, matches);
}

Status planned_phrase_prefix_query_impl(
        const LogicalIndexReader& idx, const segment_v2::InvertedIndexQueryInfo& plain_query_info,
        const segment_v2::InvertedIndexQueryInfo& gram_query_info,
        const segment_v2::inverted_index::CommonGramsQueryIdentity* common_grams_identity,
        std::vector<uint32_t>* docids, int32_t max_expansions,
        format::PrxDecodeContext* decode_context, PhrasePrefixPlanKind* selected_plan,
        segment_v2::inverted_index::CommonGramsPlanCostModel cost_model,
        CommonGramsPlanDebugOverride debug_override) {
    if (docids == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "planned_phrase_prefix_query: null out");
    }
    docids->clear();
    if (selected_plan != nullptr) {
        *selected_plan = PhrasePrefixPlanKind::kPlain;
    }
    DORIS_CHECK(!plain_query_info.has_common_gram());
    auto* query_stats = decode_context == nullptr ? nullptr : decode_context->query_stats;
    CommonGramsPlanningTimer planning_timer(query_stats);
    if (query_stats != nullptr) {
        ++query_stats->common_grams_candidate_queries;
    }
    if (plain_query_info.term_infos.empty()) {
        if (query_stats != nullptr) {
            ++query_stats->common_grams_plain_plans;
        }
        return Status::OK();
    }
    for (const auto& term_info : plain_query_info.term_infos) {
        DORIS_CHECK(term_info.is_single_term());
    }

    PhysicalPhrasePlan plain_leading;
    bool plain_representable = false;
    RETURN_IF_ERROR(build_physical_phrase_plan_prefix(
            idx, plain_query_info, plain_query_info.term_infos.size() - 1,
            /*allow_common_grams=*/false, &plain_leading, &plain_representable));
    if (!plain_representable) {
        if (query_stats != nullptr) {
            ++query_stats->common_grams_plain_plans;
        }
        return Status::OK();
    }

    const auto execute_plain = [&]() {
        std::vector<std::string> terms;
        terms.reserve(plain_query_info.term_infos.size());
        for (const auto& term_info : plain_query_info.term_infos) {
            terms.push_back(term_info.get_single_term());
        }
        return phrase_prefix_query_impl(idx, terms, docids, max_expansions, decode_context,
                                        &planning_timer);
    };

    const bool index_has_common_grams = has_common_grams_capability(idx, common_grams_identity);

    const bool can_plan_common_grams = gram_query_info.has_common_gram() && index_has_common_grams;
    if (!can_plan_common_grams) {
        if (query_stats != nullptr) {
            ++query_stats->common_grams_plain_plans;
            if (gram_query_info.has_common_gram()) {
                ++query_stats->common_grams_fallback_incompatible;
            } else {
                ++query_stats->common_grams_fallback_no_gram;
            }
        }
        return execute_plain();
    }
    DORIS_CHECK(!gram_query_info.term_infos.empty());
    DORIS_CHECK(gram_query_info.term_infos.back().is_single_term());

    PhysicalPhrasePlan gram_leading;
    bool gram_representable = false;
    RETURN_IF_ERROR(build_physical_phrase_plan_prefix(
            idx, gram_query_info, gram_query_info.term_infos.size() - 1,
            /*allow_common_grams=*/true, &gram_leading, &gram_representable));
    if (!gram_representable) {
        if (query_stats != nullptr) {
            ++query_stats->common_grams_plain_plans;
            ++query_stats->common_grams_fallback_incompatible;
        }
        return execute_plain();
    }

    internal::ResolvedPhrasePlan selected_leading;
    std::vector<ResolvedQueryTerm> selected_tail_terms;
    DORIS_CHECK_LE(plain_leading.phrase_plan_index.size(),
                   static_cast<size_t>(std::numeric_limits<uint32_t>::max()));
    const uint32_t plain_tail_position_offset =
            static_cast<uint32_t>(plain_leading.phrase_plan_index.size());
    uint32_t selected_tail_position_offset = plain_tail_position_offset;
    bool authoritative_empty = false;
    bool execute_as_exact_phrase = false;
    bool hybrid_executed = false;
    const Status planning_status = [&]() -> Status {
        const bool maps_tail_to_gram =
                gram_query_info.term_infos.back().key_kind == segment_v2::TermKeyKind::kCommonGram;
        std::vector<std::string> logical_tail_terms;
        std::vector<ResolvedQueryTerm> plain_tail_terms;
        std::vector<LogicalIndexReader::PrefixHit> tail_hits;
        RETURN_IF_ERROR(internal::visit_expanded_plain_terms(
                idx, plain_query_info.term_infos.back().get_single_term(),
                [](std::string_view) { return true; },
                [&](LogicalIndexReader::PrefixHit&& hit, bool*) {
                    tail_hits.push_back(std::move(hit));
                    return Status::OK();
                },
                max_expansions));
        if (tail_hits.empty()) {
            if (query_stats != nullptr) {
                ++query_stats->common_grams_plain_plans;
                ++query_stats->common_grams_fallback_prefix_tail_empty;
                ++query_stats->common_grams_authoritative_empty;
            }
            authoritative_empty = true;
            return Status::OK();
        }
        if (maps_tail_to_gram) {
            logical_tail_terms.reserve(tail_hits.size());
        } else {
            DORIS_CHECK(gram_query_info.term_infos.back().key_kind ==
                        segment_v2::TermKeyKind::kPlain);
        }
        plain_tail_terms.reserve(tail_hits.size());
        for (auto& hit : tail_hits) {
            if (maps_tail_to_gram) {
                logical_tail_terms.push_back(std::move(hit.term));
            }
            plain_tail_terms.push_back(ResolvedQueryTerm {.entry = std::move(hit.entry),
                                                          .frq_base = hit.frq_base,
                                                          .prx_base = hit.prx_base});
        }

        const auto select_plain_plan = [&]() -> Status {
            if (query_stats != nullptr) {
                ++query_stats->common_grams_plain_plans;
                ++query_stats->common_grams_fallback_incompatible;
            }
            std::vector<std::string> batch_terms = plain_leading.unique_terms;
            std::ranges::sort(batch_terms);
            std::vector<ResolvedQueryTerm> resolved;
            std::vector<uint8_t> found;
            RETURN_IF_ERROR(
                    internal::resolve_query_terms_batch(idx, batch_terms, &resolved, &found));
            if (!all_plan_terms_present(plain_leading, batch_terms, found)) {
                if (query_stats != nullptr) {
                    ++query_stats->common_grams_authoritative_empty;
                }
                authoritative_empty = true;
                return Status::OK();
            }
            selected_leading =
                    materialize_resolved_phrase_plan(plain_leading, batch_terms, &resolved);
            selected_tail_terms = std::move(plain_tail_terms);
            selected_tail_position_offset = plain_tail_position_offset;
            return Status::OK();
        };

        std::vector<std::string> mapped_tail_terms;
        if (maps_tail_to_gram) {
            DORIS_CHECK_GE(plain_query_info.term_infos.size(), 2U);
            const std::string& left =
                    plain_query_info.term_infos[plain_query_info.term_infos.size() - 2]
                            .get_single_term();
            mapped_tail_terms.reserve(logical_tail_terms.size());
            for (const std::string& tail : logical_tail_terms) {
                std::string gram;
                auto encoded =
                        segment_v2::inverted_index::try_encode_common_gram(left, tail, &gram);
                if (!encoded.has_value()) {
                    return std::move(encoded.error());
                }
                if (!*encoded) {
                    return select_plain_plan();
                }
                mapped_tail_terms.push_back(std::move(gram));
            }
        }

        std::vector<std::string> batch_terms = plain_leading.unique_terms;
        batch_terms.insert(batch_terms.end(), gram_leading.unique_terms.begin(),
                           gram_leading.unique_terms.end());
        batch_terms.insert(batch_terms.end(), mapped_tail_terms.begin(), mapped_tail_terms.end());
        std::ranges::sort(batch_terms);
        batch_terms.erase(std::unique(batch_terms.begin(), batch_terms.end()), batch_terms.end());

        std::vector<ResolvedQueryTerm> resolved;
        std::vector<uint8_t> found;
        RETURN_IF_ERROR(internal::resolve_query_terms_batch(idx, batch_terms, &resolved, &found));
        const bool plain_present = all_plan_terms_present(plain_leading, batch_terms, found);
        const bool gram_present = all_plan_terms_present(gram_leading, batch_terms, found);
        if (!plain_present || !gram_present) {
            if (query_stats != nullptr) {
                if (plain_present) {
                    ++query_stats->common_grams_gram_plans;
                    ++query_stats->common_grams_authoritative_empty;
                } else {
                    ++query_stats->common_grams_plain_plans;
                    ++query_stats->common_grams_authoritative_empty;
                }
            }
            if (selected_plan != nullptr && plain_present) {
                *selected_plan = PhrasePrefixPlanKind::kCommonGrams;
            }
            authoritative_empty = true;
            return Status::OK();
        }

        std::vector<size_t> present_mapped_tail_indices;
        std::vector<ResolvedMappedTail> present_mapped_tails;
        std::vector<uint32_t> present_gram_tail_ordinals;
        if (maps_tail_to_gram) {
            present_mapped_tail_indices.reserve(mapped_tail_terms.size());
            present_mapped_tails.reserve(mapped_tail_terms.size());
            present_gram_tail_ordinals.reserve(mapped_tail_terms.size());
            for (size_t ordinal = 0; ordinal < mapped_tail_terms.size(); ++ordinal) {
                const std::string& term = mapped_tail_terms[ordinal];
                const size_t batch_index = resolved_batch_index(batch_terms, term);
                if (found[batch_index] != 0) {
                    present_mapped_tail_indices.push_back(batch_index);
                    DORIS_CHECK_LE(ordinal,
                                   static_cast<size_t>(std::numeric_limits<uint32_t>::max()));
                    const uint32_t expansion_ordinal = static_cast<uint32_t>(ordinal);
                    present_mapped_tails.push_back(ResolvedMappedTail {
                            .batch_index = batch_index, .expansion_ordinal = expansion_ordinal});
                    present_gram_tail_ordinals.push_back(expansion_ordinal);
                }
            }
            if (present_mapped_tail_indices.empty()) {
                if (query_stats != nullptr) {
                    ++query_stats->common_grams_gram_plans;
                    ++query_stats->common_grams_authoritative_empty;
                }
                if (selected_plan != nullptr) {
                    *selected_plan = PhrasePrefixPlanKind::kCommonGrams;
                }
                authoritative_empty = true;
                return Status::OK();
            }
        }

        std::optional<HybridPrefixPlanArtifact> hybrid_artifact;
        if (idx.common_grams_posting_policy() == format::CommonGramsPostingPolicy::kHybridV1) {
            hybrid_artifact = try_build_hybrid_prefix_plan_artifact(
                    plain_leading, gram_leading, batch_terms, resolved, present_mapped_tails,
                    maps_tail_to_gram);
        }
        const bool hybrid_plan_requires_plain_verification = hybrid_artifact.has_value();
        const bool plain_needs_positions = !plain_leading.phrase_plan_index.empty();
        const bool gram_needs_positions =
                !gram_leading.phrase_plan_index.empty() && !hybrid_plan_requires_plain_verification;
        auto plain_raw_cost = phrase_plan_raw_cost(plain_leading, batch_terms, resolved, found,
                                                   plain_needs_positions);
        const auto plain_tail_raw_cost =
                alternative_clause_raw_cost(plain_tail_terms, plain_needs_positions);
        append_alternative_clause_cost(plain_tail_raw_cost, &plain_raw_cost);
        const uint64_t plain_cost = segment_v2::inverted_index::estimate_common_grams_plan_cost(
                plain_raw_cost, plain_needs_positions ? cost_model.position_verify_factor : 0);
        segment_v2::inverted_index::CommonGramsPlanRawCost gram_raw_cost;
        uint64_t gram_cost = 0;
        if (hybrid_plan_requires_plain_verification) {
            const HybridPrefixCostEstimate hybrid_cost = estimate_hybrid_prefix_plan_cost(
                    *hybrid_artifact, batch_terms, resolved, found, plain_tail_terms,
                    cost_model.position_verify_factor);
            gram_raw_cost = hybrid_cost.raw_cost;
            gram_cost = hybrid_cost.estimated_cost;
        } else {
            gram_raw_cost = phrase_plan_raw_cost(gram_leading, batch_terms, resolved, found,
                                                 gram_needs_positions);
            if (maps_tail_to_gram) {
                append_alternative_clause_cost(
                        alternative_clause_raw_cost(resolved, present_mapped_tail_indices,
                                                    gram_needs_positions),
                        &gram_raw_cost);
            } else {
                append_alternative_clause_cost(plain_tail_raw_cost, &gram_raw_cost);
            }
            gram_cost = segment_v2::inverted_index::estimate_common_grams_plan_cost(
                    gram_raw_cost, gram_needs_positions ? cost_model.position_verify_factor : 0);
        }
        const bool cost_prefers_gram = segment_v2::inverted_index::common_grams_plan_cost_wins(
                plain_cost, gram_cost, cost_model.common_grams_cost_ratio_percent);
        const bool use_gram =
                apply_common_grams_plan_debug_override(cost_prefers_gram, debug_override);
        if (query_stats != nullptr) {
            query_stats->common_grams_plain_posting_bytes += plain_raw_cost.posting_bytes_or_df_sum;
            query_stats->common_grams_gram_posting_bytes += gram_raw_cost.posting_bytes_or_df_sum;
            query_stats->common_grams_plain_estimated_candidate_df +=
                    plain_raw_cost.estimated_candidate_df;
            query_stats->common_grams_gram_estimated_candidate_df +=
                    gram_raw_cost.estimated_candidate_df;
            query_stats->common_grams_plain_estimated_cost += plain_cost;
            query_stats->common_grams_gram_estimated_cost += gram_cost;
        }
        if (query_stats != nullptr) {
            if (use_gram) {
                ++query_stats->common_grams_gram_plans;
            } else {
                ++query_stats->common_grams_plain_plans;
                if (debug_override == CommonGramsPlanDebugOverride::kNone) {
                    ++query_stats->common_grams_fallback_cost;
                }
            }
        }
        if (selected_plan != nullptr) {
            *selected_plan =
                    use_gram ? PhrasePrefixPlanKind::kCommonGrams : PhrasePrefixPlanKind::kPlain;
        }

        const bool hybrid_needs_plain_verification =
                use_gram && hybrid_plan_requires_plain_verification;
        if (hybrid_needs_plain_verification) {
            DORIS_CHECK(hybrid_artifact.has_value());
            bool candidate_intersection_empty = false;
            RETURN_IF_ERROR(execute_hybrid_phrase_prefix_plan(
                    idx, *hybrid_artifact, batch_terms, resolved, plain_tail_terms, docids,
                    decode_context, planning_timer, &candidate_intersection_empty));
            hybrid_executed = true;
            if (candidate_intersection_empty) {
                if (query_stats != nullptr) {
                    ++query_stats->common_grams_authoritative_empty;
                }
                authoritative_empty = true;
                return Status::OK();
            }
            return Status::OK();
        }

        const PhysicalPhrasePlan& selected_physical_leading =
                use_gram ? gram_leading : plain_leading;
        selected_leading =
                materialize_resolved_phrase_plan(selected_physical_leading, batch_terms, &resolved);
        if (!use_gram || !maps_tail_to_gram) {
            selected_tail_terms = std::move(plain_tail_terms);
            selected_tail_position_offset = plain_tail_position_offset;
            return Status::OK();
        }

        if (present_mapped_tail_indices.size() == 1) {
            DORIS_CHECK_GT(plain_tail_position_offset, 0U);
            const uint32_t mapped_tail_position = plain_tail_position_offset - 1;
            const size_t batch_index = present_mapped_tail_indices.front();
            const auto existing =
                    std::ranges::find(selected_leading.unique_terms, batch_terms[batch_index],
                                      [](const ResolvedQueryTerm& term) {
                                          return std::string_view(term.entry.term);
                                      });
            if (existing == selected_leading.unique_terms.end()) {
                append_resolved_phrase_clause(std::move(resolved[batch_index]),
                                              mapped_tail_position, &selected_leading);
            } else {
                selected_leading.phrase_plan_index.push_back(
                        static_cast<size_t>(existing - selected_leading.unique_terms.begin()));
                selected_leading.position_offsets.push_back(mapped_tail_position);
            }
            execute_as_exact_phrase = true;
            return Status::OK();
        }

        selected_tail_terms.reserve(present_mapped_tail_indices.size());
        for (size_t batch_index : present_mapped_tail_indices) {
            const auto existing =
                    std::ranges::find(selected_leading.unique_terms, batch_terms[batch_index],
                                      [](const ResolvedQueryTerm& term) {
                                          return std::string_view(term.entry.term);
                                      });
            if (existing == selected_leading.unique_terms.end()) {
                selected_tail_terms.push_back(std::move(resolved[batch_index]));
            } else {
                selected_tail_terms.push_back(*existing);
            }
        }
        DORIS_CHECK_GT(plain_tail_position_offset, 0U);
        selected_tail_position_offset = plain_tail_position_offset - 1;
        return Status::OK();
    }();
    RETURN_IF_ERROR(planning_status);
    planning_timer.finish();
    if (authoritative_empty) {
        return Status::OK();
    }
    if (hybrid_executed) {
        return Status::OK();
    }
    if (execute_as_exact_phrase) {
        return internal::execute_resolved_phrase_plan(idx, std::move(selected_leading), docids,
                                                      decode_context);
    }
    return execute_resolved_phrase_prefix_terms(
            idx, std::move(selected_leading), std::move(selected_tail_terms),
            selected_tail_position_offset, docids, decode_context);
}

} // namespace doris::snii::query::phrase_impl
