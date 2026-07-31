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

namespace doris::snii {

// Populated by snii_prx_profile.h from a per-execution query::QueryProfile, and read back out by
// SniiPrxRuntimeProfileCounters / SniiPhraseRuntimeProfileCounters into the scan node's
// RuntimeProfile. Lives in its own header, mirroring InvertedIndexStatistics
// (storage/index/inverted/inverted_index_stats.h), so OlapReaderStatistics -- shared by every
// storage format -- holds one named field here instead of one field per counter.
struct SniiQueryStats {
    int64_t prx_raw_frames = 0;
    int64_t prx_zstd_frames = 0;
    int64_t prx_pfor_frames = 0;
    int64_t prx_plaintext_bytes = 0;
    int64_t prx_total_docs = 0;
    int64_t prx_selected_docs = 0;
    int64_t prx_total_positions = 0;
    int64_t prx_selected_positions = 0;
    int64_t prx_fetch_ns = 0;
    int64_t prx_decode_ns = 0;
    int64_t prx_phrase_verify_ns = 0;

    int64_t phrase_candidate_docs = 0;
    int64_t phrase_candidate_visits = 0;
    int64_t prx_streaming_frames = 0;
    int64_t phrase_prefix_leading_candidate_docs = 0;
    int64_t phrase_prefix_tail_candidate_visits = 0;

    int64_t common_grams_candidate_queries = 0;
    int64_t common_grams_plain_plans = 0;
    int64_t common_grams_gram_plans = 0;
    int64_t common_grams_fallback_no_gram = 0;
    int64_t common_grams_fallback_incompatible = 0;
    int64_t common_grams_fallback_kill_switch = 0;
    int64_t common_grams_fallback_cost = 0;
    int64_t common_grams_fallback_base_analyzer_mismatch = 0;
    int64_t common_grams_fallback_prefix_tail_empty = 0;
    int64_t common_grams_authoritative_empty = 0;
    int64_t common_grams_plain_posting_bytes = 0;
    int64_t common_grams_gram_posting_bytes = 0;
    int64_t common_grams_plain_estimated_candidate_df = 0;
    int64_t common_grams_gram_estimated_candidate_df = 0;
    int64_t common_grams_plain_estimated_cost = 0;
    int64_t common_grams_gram_estimated_cost = 0;
    int64_t common_grams_planning_ns = 0;
};

} // namespace doris::snii
