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

#include <array>
#include <cstdint>

#include "runtime/runtime_profile.h"
#include "storage/index/snii/format/prx_decode_stats.h"
#include "storage/index/snii/query/query_profile.h"
#include "storage/olap_common.h"

namespace doris::snii {

#ifdef BE_TEST
namespace testing {

void record_prx_execution_profile_scope_construction();
void record_prx_execution_profile_scope_flush();
void reset_prx_execution_profile_scope_counters();
uint64_t prx_execution_profile_scope_construction_count();
uint64_t prx_execution_profile_scope_flush_count();

} // namespace testing
#endif

inline void add_prx_decode_stats(OlapReaderStatistics* target,
                                 const format::PrxDecodeStats& delta) {
    SniiQueryStats& stats = target->snii_stats;
    stats.prx_raw_frames += static_cast<int64_t>(delta.raw_frames);
    stats.prx_zstd_frames += static_cast<int64_t>(delta.zstd_frames);
    stats.prx_pfor_frames += static_cast<int64_t>(delta.pfor_frames);
    stats.prx_plaintext_bytes += static_cast<int64_t>(delta.plaintext_bytes);
    stats.prx_total_docs += static_cast<int64_t>(delta.total_docs);
    stats.prx_selected_docs += static_cast<int64_t>(delta.selected_docs);
    stats.prx_total_positions += static_cast<int64_t>(delta.total_positions);
    stats.prx_selected_positions += static_cast<int64_t>(delta.selected_positions);
    stats.prx_fetch_ns += static_cast<int64_t>(delta.fetch_ns);
    stats.prx_decode_ns += static_cast<int64_t>(delta.decode_ns);
    stats.prx_phrase_verify_ns += static_cast<int64_t>(delta.phrase_verify_ns);
}

inline void add_phrase_query_stats(OlapReaderStatistics* target,
                                   const format::PhraseQueryExecutionStats& delta) {
    SniiQueryStats& stats = target->snii_stats;
    stats.phrase_candidate_docs += static_cast<int64_t>(delta.exact_candidate_docs);
    stats.phrase_candidate_visits += static_cast<int64_t>(delta.exact_candidate_visits);
    stats.prx_streaming_frames += static_cast<int64_t>(delta.prx_streaming_frames);
    stats.phrase_prefix_leading_candidate_docs +=
            static_cast<int64_t>(delta.prefix_leading_candidate_docs);
    stats.phrase_prefix_tail_candidate_visits +=
            static_cast<int64_t>(delta.prefix_tail_candidate_visits);
    stats.common_grams_candidate_queries +=
            static_cast<int64_t>(delta.common_grams_candidate_queries);
    stats.common_grams_plain_plans += static_cast<int64_t>(delta.common_grams_plain_plans);
    stats.common_grams_gram_plans += static_cast<int64_t>(delta.common_grams_gram_plans);
    stats.common_grams_fallback_no_gram +=
            static_cast<int64_t>(delta.common_grams_fallback_no_gram);
    stats.common_grams_fallback_incompatible +=
            static_cast<int64_t>(delta.common_grams_fallback_incompatible);
    stats.common_grams_fallback_kill_switch +=
            static_cast<int64_t>(delta.common_grams_fallback_kill_switch);
    stats.common_grams_fallback_cost += static_cast<int64_t>(delta.common_grams_fallback_cost);
    stats.common_grams_fallback_base_analyzer_mismatch +=
            static_cast<int64_t>(delta.common_grams_fallback_base_analyzer_mismatch);
    stats.common_grams_fallback_prefix_tail_empty +=
            static_cast<int64_t>(delta.common_grams_fallback_prefix_tail_empty);
    stats.common_grams_authoritative_empty +=
            static_cast<int64_t>(delta.common_grams_authoritative_empty);
    stats.common_grams_plain_posting_bytes +=
            static_cast<int64_t>(delta.common_grams_plain_posting_bytes);
    stats.common_grams_gram_posting_bytes +=
            static_cast<int64_t>(delta.common_grams_gram_posting_bytes);
    stats.common_grams_plain_estimated_candidate_df +=
            static_cast<int64_t>(delta.common_grams_plain_estimated_candidate_df);
    stats.common_grams_gram_estimated_candidate_df +=
            static_cast<int64_t>(delta.common_grams_gram_estimated_candidate_df);
    stats.common_grams_plain_estimated_cost +=
            static_cast<int64_t>(delta.common_grams_plain_estimated_cost);
    stats.common_grams_gram_estimated_cost +=
            static_cast<int64_t>(delta.common_grams_gram_estimated_cost);
    stats.common_grams_planning_ns += static_cast<int64_t>(delta.common_grams_planning_ns);
}

// Exists only around an actual SNII compute execution. Query-cache hits,
// count-only fast paths, and single-flight followers never construct this
// scope, so they cannot contribute another execution's PRX work. The destructor
// flushes already-committed frames on both success and early error returns.
class SniiPrxExecutionProfileScope {
public:
    explicit SniiPrxExecutionProfileScope(OlapReaderStatistics& target) : target_(target) {
#ifdef BE_TEST
        testing::record_prx_execution_profile_scope_construction();
#endif
    }
    ~SniiPrxExecutionProfileScope() {
        add_prx_decode_stats(&target_, profile_.prx_decode_stats);
        add_phrase_query_stats(&target_, profile_.phrase_query_stats);
#ifdef BE_TEST
        testing::record_prx_execution_profile_scope_flush();
#endif
    }

    SniiPrxExecutionProfileScope(const SniiPrxExecutionProfileScope&) = delete;
    SniiPrxExecutionProfileScope& operator=(const SniiPrxExecutionProfileScope&) = delete;
    SniiPrxExecutionProfileScope(SniiPrxExecutionProfileScope&&) = delete;
    SniiPrxExecutionProfileScope& operator=(SniiPrxExecutionProfileScope&&) = delete;

    query::QueryProfile* profile() { return &profile_; }

private:
    OlapReaderStatistics& target_;
    query::QueryProfile profile_;
};

class SniiPrxRuntimeProfileCounters {
public:
    static constexpr std::array<const char*, 11> counter_names() {
        return {"SniiPrxRawFrames",
                "SniiPrxZstdFrames",
                "SniiPrxPforFrames",
                "SniiPrxPlaintextBytes",
                "SniiPrxTotalDocs",
                "SniiPrxSelectedDocs",
                "SniiPrxTotalPositions",
                "SniiPrxSelectedPositions",
                "SniiPrxFetchTime",
                "SniiPrxInclusiveDecodeTime",
                "SniiPrxExclusivePhraseVerifyTime"};
    }

    void initialize(RuntimeProfile* profile) {
        raw_frames_ = profile->add_nonzero_counter("SniiPrxRawFrames", TUnit::UNIT,
                                                   RuntimeProfile::ROOT_COUNTER, 1);
        zstd_frames_ = profile->add_nonzero_counter("SniiPrxZstdFrames", TUnit::UNIT,
                                                    RuntimeProfile::ROOT_COUNTER, 1);
        pfor_frames_ = profile->add_nonzero_counter("SniiPrxPforFrames", TUnit::UNIT,
                                                    RuntimeProfile::ROOT_COUNTER, 1);
        plaintext_bytes_ = profile->add_nonzero_counter("SniiPrxPlaintextBytes", TUnit::BYTES,
                                                        RuntimeProfile::ROOT_COUNTER, 1);
        total_docs_ = profile->add_nonzero_counter("SniiPrxTotalDocs", TUnit::UNIT,
                                                   RuntimeProfile::ROOT_COUNTER, 1);
        selected_docs_ = profile->add_nonzero_counter("SniiPrxSelectedDocs", TUnit::UNIT,
                                                      RuntimeProfile::ROOT_COUNTER, 1);
        total_positions_ = profile->add_nonzero_counter("SniiPrxTotalPositions", TUnit::UNIT,
                                                        RuntimeProfile::ROOT_COUNTER, 1);
        selected_positions_ = profile->add_nonzero_counter("SniiPrxSelectedPositions", TUnit::UNIT,
                                                           RuntimeProfile::ROOT_COUNTER, 1);
        fetch_ns_ = profile->add_nonzero_counter("SniiPrxFetchTime", TUnit::TIME_NS,
                                                 RuntimeProfile::ROOT_COUNTER, 1);
        decode_ns_ = profile->add_nonzero_counter("SniiPrxInclusiveDecodeTime", TUnit::TIME_NS,
                                                  RuntimeProfile::ROOT_COUNTER, 1);
        phrase_verify_ns_ =
                profile->add_nonzero_counter("SniiPrxExclusivePhraseVerifyTime", TUnit::TIME_NS,
                                             RuntimeProfile::ROOT_COUNTER, 1);
    }

    void update(const OlapReaderStatistics& stats) const {
        const SniiQueryStats& s = stats.snii_stats;
        COUNTER_UPDATE(raw_frames_, s.prx_raw_frames);
        COUNTER_UPDATE(zstd_frames_, s.prx_zstd_frames);
        COUNTER_UPDATE(pfor_frames_, s.prx_pfor_frames);
        COUNTER_UPDATE(plaintext_bytes_, s.prx_plaintext_bytes);
        COUNTER_UPDATE(total_docs_, s.prx_total_docs);
        COUNTER_UPDATE(selected_docs_, s.prx_selected_docs);
        COUNTER_UPDATE(total_positions_, s.prx_total_positions);
        COUNTER_UPDATE(selected_positions_, s.prx_selected_positions);
        COUNTER_UPDATE(fetch_ns_, s.prx_fetch_ns);
        COUNTER_UPDATE(decode_ns_, s.prx_decode_ns);
        COUNTER_UPDATE(phrase_verify_ns_, s.prx_phrase_verify_ns);
    }

private:
    RuntimeProfile::Counter* raw_frames_ = nullptr;
    RuntimeProfile::Counter* zstd_frames_ = nullptr;
    RuntimeProfile::Counter* pfor_frames_ = nullptr;
    RuntimeProfile::Counter* plaintext_bytes_ = nullptr;
    RuntimeProfile::Counter* total_docs_ = nullptr;
    RuntimeProfile::Counter* selected_docs_ = nullptr;
    RuntimeProfile::Counter* total_positions_ = nullptr;
    RuntimeProfile::Counter* selected_positions_ = nullptr;
    RuntimeProfile::Counter* fetch_ns_ = nullptr;
    RuntimeProfile::Counter* decode_ns_ = nullptr;
    RuntimeProfile::Counter* phrase_verify_ns_ = nullptr;
};

class SniiPhraseRuntimeProfileCounters {
public:
    void initialize(RuntimeProfile* profile) {
        candidate_docs_ = profile->add_nonzero_counter("SniiPhraseCandidateDocs", TUnit::UNIT,
                                                       RuntimeProfile::ROOT_COUNTER, 1);
        candidate_visits_ = profile->add_nonzero_counter("SniiPhraseCandidateVisits", TUnit::UNIT,
                                                         RuntimeProfile::ROOT_COUNTER, 1);
        streaming_prx_frames_ = profile->add_nonzero_counter(
                "SniiPhraseStreamingPrxFrames", TUnit::UNIT, RuntimeProfile::ROOT_COUNTER, 1);
        prefix_leading_candidate_docs_ =
                profile->add_nonzero_counter("SniiPhrasePrefixLeadingCandidateDocs", TUnit::UNIT,
                                             RuntimeProfile::ROOT_COUNTER, 1);
        prefix_tail_candidate_visits_ =
                profile->add_nonzero_counter("SniiPhrasePrefixTailCandidateVisits", TUnit::UNIT,
                                             RuntimeProfile::ROOT_COUNTER, 1);
        common_grams_candidate_queries_ = profile->add_nonzero_counter(
                "SniiCommonGramsCandidateQueries", TUnit::UNIT, RuntimeProfile::ROOT_COUNTER, 1);
        common_grams_plain_plans_ = profile->add_nonzero_counter(
                "SniiCommonGramsPlainPlans", TUnit::UNIT, RuntimeProfile::ROOT_COUNTER, 1);
        common_grams_gram_plans_ = profile->add_nonzero_counter(
                "SniiCommonGramsGramPlans", TUnit::UNIT, RuntimeProfile::ROOT_COUNTER, 1);
        common_grams_fallback_no_gram_ = profile->add_nonzero_counter(
                "SniiCommonGramsFallbackNoGram", TUnit::UNIT, RuntimeProfile::ROOT_COUNTER, 1);
        common_grams_fallback_incompatible_ =
                profile->add_nonzero_counter("SniiCommonGramsFallbackIncompatible", TUnit::UNIT,
                                             RuntimeProfile::ROOT_COUNTER, 1);
        common_grams_fallback_kill_switch_ = profile->add_nonzero_counter(
                "SniiCommonGramsFallbackKillSwitch", TUnit::UNIT, RuntimeProfile::ROOT_COUNTER, 1);
        common_grams_fallback_cost_ = profile->add_nonzero_counter(
                "SniiCommonGramsFallbackCost", TUnit::UNIT, RuntimeProfile::ROOT_COUNTER, 1);
        common_grams_fallback_base_analyzer_mismatch_ =
                profile->add_nonzero_counter("SniiCommonGramsFallbackBaseAnalyzerMismatch",
                                             TUnit::UNIT, RuntimeProfile::ROOT_COUNTER, 1);
        common_grams_fallback_prefix_tail_empty_ =
                profile->add_nonzero_counter("SniiCommonGramsFallbackPrefixTailEmpty", TUnit::UNIT,
                                             RuntimeProfile::ROOT_COUNTER, 1);
        common_grams_authoritative_empty_ = profile->add_nonzero_counter(
                "SniiCommonGramsAuthoritativeEmpty", TUnit::UNIT, RuntimeProfile::ROOT_COUNTER, 1);
        common_grams_plain_posting_bytes_ = profile->add_nonzero_counter(
                "SniiCommonGramsPlainPostingBytes", TUnit::BYTES, RuntimeProfile::ROOT_COUNTER, 1);
        common_grams_gram_posting_bytes_ = profile->add_nonzero_counter(
                "SniiCommonGramsGramPostingBytes", TUnit::BYTES, RuntimeProfile::ROOT_COUNTER, 1);
        common_grams_plain_estimated_candidate_df_ =
                profile->add_nonzero_counter("SniiCommonGramsPlainEstimatedCandidateDf",
                                             TUnit::UNIT, RuntimeProfile::ROOT_COUNTER, 1);
        common_grams_gram_estimated_candidate_df_ =
                profile->add_nonzero_counter("SniiCommonGramsGramEstimatedCandidateDf", TUnit::UNIT,
                                             RuntimeProfile::ROOT_COUNTER, 1);
        common_grams_plain_estimated_cost_ = profile->add_nonzero_counter(
                "SniiCommonGramsPlainEstimatedCost", TUnit::UNIT, RuntimeProfile::ROOT_COUNTER, 1);
        common_grams_gram_estimated_cost_ = profile->add_nonzero_counter(
                "SniiCommonGramsGramEstimatedCost", TUnit::UNIT, RuntimeProfile::ROOT_COUNTER, 1);
        common_grams_planning_ns_ = profile->add_nonzero_counter(
                "SniiCommonGramsPlanningTime", TUnit::TIME_NS, RuntimeProfile::ROOT_COUNTER, 1);
    }

    void update(const OlapReaderStatistics& stats) const {
        const SniiQueryStats& s = stats.snii_stats;
        COUNTER_UPDATE(candidate_docs_, s.phrase_candidate_docs);
        COUNTER_UPDATE(candidate_visits_, s.phrase_candidate_visits);
        COUNTER_UPDATE(streaming_prx_frames_, s.prx_streaming_frames);
        COUNTER_UPDATE(prefix_leading_candidate_docs_, s.phrase_prefix_leading_candidate_docs);
        COUNTER_UPDATE(prefix_tail_candidate_visits_, s.phrase_prefix_tail_candidate_visits);
        COUNTER_UPDATE(common_grams_candidate_queries_, s.common_grams_candidate_queries);
        COUNTER_UPDATE(common_grams_plain_plans_, s.common_grams_plain_plans);
        COUNTER_UPDATE(common_grams_gram_plans_, s.common_grams_gram_plans);
        COUNTER_UPDATE(common_grams_fallback_no_gram_, s.common_grams_fallback_no_gram);
        COUNTER_UPDATE(common_grams_fallback_incompatible_, s.common_grams_fallback_incompatible);
        COUNTER_UPDATE(common_grams_fallback_kill_switch_, s.common_grams_fallback_kill_switch);
        COUNTER_UPDATE(common_grams_fallback_cost_, s.common_grams_fallback_cost);
        COUNTER_UPDATE(common_grams_fallback_base_analyzer_mismatch_,
                       s.common_grams_fallback_base_analyzer_mismatch);
        COUNTER_UPDATE(common_grams_fallback_prefix_tail_empty_,
                       s.common_grams_fallback_prefix_tail_empty);
        COUNTER_UPDATE(common_grams_authoritative_empty_, s.common_grams_authoritative_empty);
        COUNTER_UPDATE(common_grams_plain_posting_bytes_, s.common_grams_plain_posting_bytes);
        COUNTER_UPDATE(common_grams_gram_posting_bytes_, s.common_grams_gram_posting_bytes);
        COUNTER_UPDATE(common_grams_plain_estimated_candidate_df_,
                       s.common_grams_plain_estimated_candidate_df);
        COUNTER_UPDATE(common_grams_gram_estimated_candidate_df_,
                       s.common_grams_gram_estimated_candidate_df);
        COUNTER_UPDATE(common_grams_plain_estimated_cost_, s.common_grams_plain_estimated_cost);
        COUNTER_UPDATE(common_grams_gram_estimated_cost_, s.common_grams_gram_estimated_cost);
        COUNTER_UPDATE(common_grams_planning_ns_, s.common_grams_planning_ns);
    }

private:
    RuntimeProfile::Counter* candidate_docs_ = nullptr;
    RuntimeProfile::Counter* candidate_visits_ = nullptr;
    RuntimeProfile::Counter* streaming_prx_frames_ = nullptr;
    RuntimeProfile::Counter* prefix_leading_candidate_docs_ = nullptr;
    RuntimeProfile::Counter* prefix_tail_candidate_visits_ = nullptr;
    RuntimeProfile::Counter* common_grams_candidate_queries_ = nullptr;
    RuntimeProfile::Counter* common_grams_plain_plans_ = nullptr;
    RuntimeProfile::Counter* common_grams_gram_plans_ = nullptr;
    RuntimeProfile::Counter* common_grams_fallback_no_gram_ = nullptr;
    RuntimeProfile::Counter* common_grams_fallback_incompatible_ = nullptr;
    RuntimeProfile::Counter* common_grams_fallback_kill_switch_ = nullptr;
    RuntimeProfile::Counter* common_grams_fallback_cost_ = nullptr;
    RuntimeProfile::Counter* common_grams_fallback_base_analyzer_mismatch_ = nullptr;
    RuntimeProfile::Counter* common_grams_fallback_prefix_tail_empty_ = nullptr;
    RuntimeProfile::Counter* common_grams_authoritative_empty_ = nullptr;
    RuntimeProfile::Counter* common_grams_plain_posting_bytes_ = nullptr;
    RuntimeProfile::Counter* common_grams_gram_posting_bytes_ = nullptr;
    RuntimeProfile::Counter* common_grams_plain_estimated_candidate_df_ = nullptr;
    RuntimeProfile::Counter* common_grams_gram_estimated_candidate_df_ = nullptr;
    RuntimeProfile::Counter* common_grams_plain_estimated_cost_ = nullptr;
    RuntimeProfile::Counter* common_grams_gram_estimated_cost_ = nullptr;
    RuntimeProfile::Counter* common_grams_planning_ns_ = nullptr;
};

} // namespace doris::snii
