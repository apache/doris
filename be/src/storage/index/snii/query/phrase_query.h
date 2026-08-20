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
#include <optional>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/index/inverted/common_grams/common_grams_query_cost.h"
#include "storage/index/inverted/common_grams/common_grams_segment_metadata.h"
#include "storage/index/inverted/query/query_info.h"
#include "storage/index/snii/format/prx_decode_stats.h"
#include "storage/index/snii/query/query_profile.h"
#include "storage/index/snii/reader/logical_index_reader.h"

// phrase_query -- MATCH_PHRASE: return the sorted docid set in which the terms
// occur consecutively (for some i, every term k appears at position pos+k in
// the same doc). It first builds the docid conjunction with docs-only posting
// reads, then fetches PRX only for chunks that can contain final candidates:
//   1. read preludes / docs-only posting ranges and intersect per-term docids;
//   2. fetch retained PRX chunks and stream positions for survivors;
//   3. for each surviving doc, check that some position p exists with
//      term[0]@p, term[1]@p+1, ... term[n-1]@p+(n-1).
// An empty term list -> empty result. Any term absent -> empty result.
namespace doris::snii::query {

enum class ExactPhrasePlanKind : uint8_t {
    kPlain = 0,
    kCommonGrams = 1,
};

enum class PhrasePrefixPlanKind : uint8_t {
    kPlain = 0,
    kCommonGrams = 1,
};

// Benchmark-only plan override. The debug points are inert unless the process-wide
// enable_debug_points switch is on, and planners apply them only after both complete plans resolve.
enum class CommonGramsPlanDebugOverride : uint8_t {
    kNone = 0,
    kForcePlain = 1,
    kForceCommonGrams = 2,
};

inline constexpr char COMMON_GRAMS_FORCE_PLAIN_PLAN_DEBUG_POINT[] =
        "snii.common_grams.force_plain_plan";
inline constexpr char COMMON_GRAMS_FORCE_GRAM_PLAN_DEBUG_POINT[] =
        "snii.common_grams.force_gram_plan";

CommonGramsPlanDebugOverride common_grams_plan_debug_override();

struct PhraseMatch {
    uint32_t docid = 0;
    float frequency = 0.0F;

    bool operator==(const PhraseMatch&) const = default;
};

struct PhraseQueryOptions {
    uint32_t slop = 0;
    bool ordered = false;
};

Status phrase_query(const reader::LogicalIndexReader& idx, const std::vector<std::string>& terms,
                    std::vector<uint32_t>* docids);
Status phrase_query(const reader::LogicalIndexReader& idx, const std::vector<std::string>& terms,
                    std::vector<uint32_t>* docids, QueryProfile* profile);
Status phrase_query(const reader::LogicalIndexReader& idx, const std::vector<std::string>& terms,
                    std::vector<uint32_t>* docids, QueryProfile* profile,
                    const PhraseQueryOptions& options);

// Scoring-only multi-term entry point. It runs the same opaque-term matcher as
// phrase_query() and returns exact occurrence counts or V3-compatible,
// distance-weighted sloppy-phrase frequencies for each matching doc.
Status phrase_query_with_frequencies(const reader::LogicalIndexReader& idx,
                                     const std::vector<std::string>& terms,
                                     std::vector<PhraseMatch>* matches,
                                     QueryProfile* profile = nullptr,
                                     const PhraseQueryOptions& options = {});

// Selects between equivalent plain and CommonGrams exact-phrase plans above
// the existing opaque-term matcher. A missing or mismatched query identity
// forces the plain plan before any gram DICT lookup.
Status planned_exact_phrase_query(
        const reader::LogicalIndexReader& idx,
        const segment_v2::InvertedIndexQueryInfo& plain_query_info,
        const segment_v2::InvertedIndexQueryInfo& gram_query_info,
        const segment_v2::inverted_index::CommonGramsQueryIdentity* common_grams_identity,
        std::vector<uint32_t>* docids, QueryProfile* profile = nullptr,
        ExactPhrasePlanKind* selected_plan = nullptr,
        segment_v2::inverted_index::CommonGramsPlanCostModel cost_model = {},
        std::optional<CommonGramsPlanDebugOverride> debug_override = std::nullopt);

Status planned_phrase_prefix_query(
        const reader::LogicalIndexReader& idx,
        const segment_v2::InvertedIndexQueryInfo& plain_query_info,
        const segment_v2::InvertedIndexQueryInfo& gram_query_info,
        const segment_v2::inverted_index::CommonGramsQueryIdentity* common_grams_identity,
        std::vector<uint32_t>* docids, QueryProfile* profile = nullptr, int32_t max_expansions = 0,
        PhrasePrefixPlanKind* selected_plan = nullptr,
        segment_v2::inverted_index::CommonGramsPlanCostModel cost_model = {},
        std::optional<CommonGramsPlanDebugOverride> debug_override = std::nullopt);

// phrase_prefix_query -- MATCH_PHRASE_PREFIX: the last item in `terms` is a
// term prefix and preceding items are exact terms. For example {"quick", "bro"}
// matches "quick brown" and "quick bronze". Empty terms -> empty result.
Status phrase_prefix_query(const reader::LogicalIndexReader& idx,
                           const std::vector<std::string>& terms,
                           std::vector<uint32_t>* const docids, int32_t max_expansions = 0);
Status phrase_prefix_query(const reader::LogicalIndexReader& idx,
                           const std::vector<std::string>& terms,
                           std::vector<uint32_t>* const docids, QueryProfile* profile,
                           int32_t max_expansions = 0);

// Scoring-only multi-term entry point. Tail expansions are one logical phrase
// clause, so each phrase start contributes at most once even when several
// expanded terms share that position.
Status phrase_prefix_query_with_frequencies(const reader::LogicalIndexReader& idx,
                                            const std::vector<std::string>& terms,
                                            std::vector<PhraseMatch>* matches,
                                            QueryProfile* profile = nullptr,
                                            int32_t max_expansions = 0);

} // namespace doris::snii::query
