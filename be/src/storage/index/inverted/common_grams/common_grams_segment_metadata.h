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
#include <string>

#include "common/status.h"
#include "storage/index/inverted/common_grams/common_grams_key_codec.h"

namespace doris::segment_v2::inverted_index {

inline constexpr uint32_t COMMON_GRAMS_SEGMENT_METADATA_VERSION = 1;
inline constexpr uint32_t COMMON_GRAMS_SEMANTICS_VERSION_V1 = 1;
inline constexpr uint32_t COMMON_GRAMS_KEY_VERSION_V1 = 1;
inline constexpr uint32_t COMMON_GRAMS_SCORING_STATS_VERSION_V1 = 1;
inline constexpr uint32_t COMMON_GRAMS_NORM_SEMANTICS_VERSION_V1 = 1;

enum class CommonGramsCoverage : uint8_t {
    kNone = 0,
    kComplete = 1,
    kMixed = 2,
};

enum class ScoringCoverage : uint8_t {
    kNone = 0,
    kComplete = 1,
};

// SNII metadata. A missing record is a legacy segment; a present record must
// validate before any capability is used.
struct CommonGramsSegmentMetadata {
    PlainTermKeyVersion plain_term_key_version = PlainTermKeyVersion::kLegacyRaw;
    CommonGramsCoverage common_grams_coverage = CommonGramsCoverage::kNone;
    uint32_t common_grams_semantics_version = 0;
    uint32_t common_grams_key_version = 0;
    std::string common_grams_dictionary_identity;
    std::string base_analyzer_fingerprint;
    std::string common_grams_fingerprint;
    ScoringCoverage scoring_coverage = ScoringCoverage::kNone;
    uint32_t scoring_stats_version = 0;
    uint32_t norm_semantics_version = 0;
    uint64_t scoring_doc_count = 0;
    uint64_t scoring_token_count = 0;

    bool operator==(const CommonGramsSegmentMetadata&) const = default;
};

struct CommonGramsQueryIdentity {
    std::string common_grams_dictionary_identity;
    std::string base_analyzer_fingerprint;
    std::string common_grams_fingerprint;

    bool operator==(const CommonGramsQueryIdentity&) const = default;
};

CommonGramsSegmentMetadata make_common_grams_segment_metadata(
        const CommonGramsQueryIdentity& identity);
bool common_grams_identity_matches(const CommonGramsSegmentMetadata& metadata,
                                   const CommonGramsQueryIdentity& identity);
Status validate_common_grams_segment_metadata(const CommonGramsSegmentMetadata& metadata);
Status validate_snii_scoring_metadata(const CommonGramsSegmentMetadata* metadata,
                                      uint64_t physical_doc_count,
                                      uint64_t physical_sum_total_term_freq, bool has_scoring_tier,
                                      bool has_positions, bool has_norms);
bool is_common_grams_query_compatible(const CommonGramsSegmentMetadata& metadata,
                                      const CommonGramsQueryIdentity& identity);
bool is_common_grams_query_compatible(const CommonGramsSegmentMetadata& metadata,
                                      const CommonGramsQueryIdentity& identity,
                                      CommonGramsCoverage required_coverage);

} // namespace doris::segment_v2::inverted_index
