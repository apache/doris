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
#include "storage/index/snii/query/phrase_verify_timer.h"
#include "storage/index/snii/query/prefix_query.h"
#include "storage/index/snii/query/term_query.h"
#include "storage/index/snii/reader/windowed_posting.h"
#include "util/debug_points.h"

namespace doris::snii::query {

using query::internal::DocidChunk;
using query::internal::DocidSource;
using query::internal::ResolvedQueryTerm;
using query::internal::TermPlan;
using reader::LogicalIndexReader;

CommonGramsPlanDebugOverride common_grams_plan_debug_override() {
    CommonGramsPlanDebugOverride result = CommonGramsPlanDebugOverride::kNone;
    DBUG_EXECUTE_IF(COMMON_GRAMS_FORCE_PLAIN_PLAN_DEBUG_POINT,
                    { result = CommonGramsPlanDebugOverride::kForcePlain; });
    DBUG_EXECUTE_IF(COMMON_GRAMS_FORCE_GRAM_PLAN_DEBUG_POINT, {
        DORIS_CHECK(result != CommonGramsPlanDebugOverride::kForcePlain);
        result = CommonGramsPlanDebugOverride::kForceCommonGrams;
    });
    return result;
}

using namespace phrase_impl; // NOLINT(google-build-using-namespace): module-internal impl namespace

namespace internal {

Status validate_prx_frame(std::span<const uint32_t> pos_flat, std::span<const uint32_t> pos_offsets,
                          uint32_t actual_total_docs, uint32_t expected_total_docs,
                          size_t expected_selected_docs,
                          std::span<const uint32_t> selected_doc_ordinals,
                          bool offsets_by_prx_ordinal, bool all_docs_selected) {
    if (!all_docs_selected && expected_selected_docs != selected_doc_ordinals.size()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "phrase_query: selected prx ordinal-count mismatch");
    }
    if (actual_total_docs != expected_total_docs) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "phrase_query: prx total doc-count mismatch");
    }
    const size_t expected_offsets = offsets_by_prx_ordinal
                                            ? static_cast<size_t>(expected_total_docs) + 1
                                            : expected_selected_docs + 1;
    if (pos_offsets.size() != expected_offsets) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                offsets_by_prx_ordinal ? "phrase_query: full prx doc-count mismatch"
                                       : "phrase_query: selected prx/doc-count mismatch");
    }
    if (pos_offsets.back() != pos_flat.size()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "phrase_query: prx final offset mismatch");
    }
    if (offsets_by_prx_ordinal && !selected_doc_ordinals.empty() &&
        static_cast<size_t>(selected_doc_ordinals.back()) + 1 >= pos_offsets.size()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "phrase_query: prx ordinal offset out of range");
    }
    return Status::OK();
}

Status decode_and_validate_prx_frame(ByteSource* source,
                                     std::span<const uint32_t> selected_doc_ordinals,
                                     bool decode_full, bool all_docs_selected,
                                     uint32_t expected_total_docs, size_t expected_selected_docs,
                                     std::vector<uint32_t>* pos_flat,
                                     std::vector<uint32_t>* pos_offsets,
                                     format::PrxDecodeContext* decode_context) {
    DCHECK(decode_full || !all_docs_selected);
    format::PrxDecodedShape decoded_shape;
    format::PrxDecodeContext frame_context {
            .stats = decode_context == nullptr ? nullptr : decode_context->stats,
            .shape = &decoded_shape};
    if (decode_full) {
        if (all_docs_selected) {
            RETURN_IF_ERROR(
                    format::read_prx_window_csr(source, pos_flat, pos_offsets, &frame_context));
        } else {
            RETURN_IF_ERROR(format::read_prx_window_csr_for_selection(
                    source, selected_doc_ordinals, pos_flat, pos_offsets, &frame_context));
        }
    } else {
        RETURN_IF_ERROR(format::read_prx_window_csr_selective(
                source, selected_doc_ordinals, pos_flat, pos_offsets, &frame_context));
    }
    return validate_prx_frame(*pos_flat, *pos_offsets, decoded_shape.total_docs,
                              expected_total_docs, expected_selected_docs, selected_doc_ordinals,
                              decode_full && !all_docs_selected, all_docs_selected);
}

namespace {

using PhraseVerifyClock = std::chrono::steady_clock;

PhraseVerifyClock::time_point phrase_verify_clock_now() {
#ifdef BE_TEST
    testing::note_phrase_verify_clock_read();
#endif
    return PhraseVerifyClock::now();
}

} // namespace

uint64_t exclusive_phrase_verify_ns(uint64_t elapsed_ns, uint64_t decode_ns_before,
                                    uint64_t decode_ns_after) {
    DCHECK_GE(decode_ns_after, decode_ns_before);
    const uint64_t decode_delta = decode_ns_after - decode_ns_before;
    return elapsed_ns > decode_delta ? elapsed_ns - decode_delta : 0;
}

PhraseVerifyTimer::PhraseVerifyTimer(format::PrxDecodeContext* decode_context)
        : stats_(decode_context == nullptr ? nullptr : decode_context->stats) {
    if (stats_ != nullptr) {
        decode_ns_before_ = stats_->decode_ns;
        start_ = phrase_verify_clock_now();
    }
}

void PhraseVerifyTimer::commit_success() {
    if (stats_ == nullptr) {
        return;
    }
    const auto elapsed =
            std::chrono::duration_cast<std::chrono::nanoseconds>(phrase_verify_clock_now() - start_)
                    .count();
    stats_->phrase_verify_ns += exclusive_phrase_verify_ns(static_cast<uint64_t>(elapsed),
                                                           decode_ns_before_, stats_->decode_ns);
}

} // namespace internal

Status phrase_query(const LogicalIndexReader& idx, const std::vector<std::string>& terms,
                    std::vector<uint32_t>* const docids) {
    return phrase_query_impl(idx, terms, docids, nullptr, nullptr, {});
}

Status phrase_query(const LogicalIndexReader& idx, const std::vector<std::string>& terms,
                    std::vector<uint32_t>* const docids, QueryProfile* profile) {
    return phrase_query(idx, terms, docids, profile, {});
}

Status phrase_query(const LogicalIndexReader& idx, const std::vector<std::string>& terms,
                    std::vector<uint32_t>* const docids, QueryProfile* profile,
                    const PhraseQueryOptions& options) {
    QueryProfileScope profile_scope(idx.reader(), profile);
    format::PrxDecodeContext decode_context {
            .stats = profile == nullptr ? nullptr : &profile->prx_decode_stats,
            .query_stats = profile == nullptr ? nullptr : &profile->phrase_query_stats};
    return phrase_query_impl(idx, terms, docids, profile == nullptr ? nullptr : &decode_context,
                             nullptr, options);
}

Status phrase_query_with_frequencies(const LogicalIndexReader& idx,
                                     const std::vector<std::string>& terms,
                                     std::vector<PhraseMatch>* matches, QueryProfile* profile,
                                     const PhraseQueryOptions& options) {
    if (matches == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "phrase_query_with_frequencies: null out");
    }
    matches->clear();
    if (terms.size() < 2) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "phrase_query_with_frequencies: at least two terms are required");
    }
    QueryProfileScope profile_scope(idx.reader(), profile);
    format::PrxDecodeContext decode_context {
            .stats = profile == nullptr ? nullptr : &profile->prx_decode_stats,
            .query_stats = profile == nullptr ? nullptr : &profile->phrase_query_stats};
    return phrase_query_impl(idx, terms, nullptr, profile == nullptr ? nullptr : &decode_context,
                             matches, options);
}

Status planned_exact_phrase_query(
        const LogicalIndexReader& idx, const segment_v2::InvertedIndexQueryInfo& plain_query_info,
        const segment_v2::InvertedIndexQueryInfo& gram_query_info,
        const segment_v2::inverted_index::CommonGramsQueryIdentity* common_grams_identity,
        std::vector<uint32_t>* docids, QueryProfile* profile, ExactPhrasePlanKind* selected_plan,
        segment_v2::inverted_index::CommonGramsPlanCostModel cost_model,
        std::optional<CommonGramsPlanDebugOverride> debug_override) {
    QueryProfileScope profile_scope(idx.reader(), profile);
    format::PrxDecodeContext decode_context {
            .stats = profile == nullptr ? nullptr : &profile->prx_decode_stats,
            .query_stats = profile == nullptr ? nullptr : &profile->phrase_query_stats};
    return planned_exact_phrase_query_impl(
            idx, plain_query_info, gram_query_info, common_grams_identity, docids,
            profile == nullptr ? nullptr : &decode_context, selected_plan, cost_model,
            debug_override.has_value() ? *debug_override : common_grams_plan_debug_override());
}

Status planned_phrase_prefix_query(
        const LogicalIndexReader& idx, const segment_v2::InvertedIndexQueryInfo& plain_query_info,
        const segment_v2::InvertedIndexQueryInfo& gram_query_info,
        const segment_v2::inverted_index::CommonGramsQueryIdentity* common_grams_identity,
        std::vector<uint32_t>* docids, QueryProfile* profile, int32_t max_expansions,
        PhrasePrefixPlanKind* selected_plan,
        segment_v2::inverted_index::CommonGramsPlanCostModel cost_model,
        std::optional<CommonGramsPlanDebugOverride> debug_override) {
    QueryProfileScope profile_scope(idx.reader(), profile);
    format::PrxDecodeContext decode_context {
            .stats = profile == nullptr ? nullptr : &profile->prx_decode_stats,
            .query_stats = profile == nullptr ? nullptr : &profile->phrase_query_stats};
    return planned_phrase_prefix_query_impl(
            idx, plain_query_info, gram_query_info, common_grams_identity, docids, max_expansions,
            profile == nullptr ? nullptr : &decode_context, selected_plan, cost_model,
            debug_override.has_value() ? *debug_override : common_grams_plan_debug_override());
}

Status phrase_prefix_query(const LogicalIndexReader& idx, const std::vector<std::string>& terms,
                           std::vector<uint32_t>* const docids, int32_t max_expansions) {
    return phrase_prefix_query_impl(idx, terms, docids, max_expansions, nullptr, nullptr);
}

Status phrase_prefix_query(const LogicalIndexReader& idx, const std::vector<std::string>& terms,
                           std::vector<uint32_t>* const docids, QueryProfile* profile,
                           int32_t max_expansions) {
    QueryProfileScope profile_scope(idx.reader(), profile);
    format::PrxDecodeContext decode_context {
            .stats = profile == nullptr ? nullptr : &profile->prx_decode_stats,
            .query_stats = profile == nullptr ? nullptr : &profile->phrase_query_stats};
    return phrase_prefix_query_impl(idx, terms, docids, max_expansions,
                                    profile == nullptr ? nullptr : &decode_context, nullptr);
}

Status phrase_prefix_query_with_frequencies(const LogicalIndexReader& idx,
                                            const std::vector<std::string>& terms,
                                            std::vector<PhraseMatch>* matches,
                                            QueryProfile* profile, int32_t max_expansions) {
    if (matches == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "phrase_prefix_query_with_frequencies: null out");
    }
    matches->clear();
    if (terms.size() < 2) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "phrase_prefix_query_with_frequencies: at least two terms are required");
    }
    QueryProfileScope profile_scope(idx.reader(), profile);
    format::PrxDecodeContext decode_context {
            .stats = profile == nullptr ? nullptr : &profile->prx_decode_stats,
            .query_stats = profile == nullptr ? nullptr : &profile->phrase_query_stats};
    return phrase_prefix_query_impl(idx, terms, nullptr, max_expansions,
                                    profile == nullptr ? nullptr : &decode_context, nullptr,
                                    matches);
}

} // namespace doris::snii::query

#ifdef BE_TEST
namespace doris::snii::query::internal::testing {
namespace {

std::atomic<uint64_t>& phrase_verify_clock_read_atomic() {
    static std::atomic<uint64_t> counter {0};
    return counter;
}

} // namespace

uint64_t phrase_verify_clock_read_count() {
    return phrase_verify_clock_read_atomic().load(std::memory_order_relaxed);
}

void reset_phrase_verify_clock_read_count() {
    phrase_verify_clock_read_atomic().store(0, std::memory_order_relaxed);
}

void note_phrase_verify_clock_read() {
    phrase_verify_clock_read_atomic().fetch_add(1, std::memory_order_relaxed);
}

} // namespace doris::snii::query::internal::testing
#endif
