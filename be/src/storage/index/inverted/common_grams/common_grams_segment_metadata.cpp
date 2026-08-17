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

#include "storage/index/inverted/common_grams/common_grams_segment_metadata.h"

namespace doris::segment_v2::inverted_index {

CommonGramsSegmentMetadata make_common_grams_segment_metadata(
        const CommonGramsQueryIdentity& identity) {
    CommonGramsSegmentMetadata metadata;
    metadata.plain_term_key_version = PlainTermKeyVersion::kEscapedV1;
    metadata.common_grams_coverage = CommonGramsCoverage::kComplete;
    metadata.common_grams_semantics_version = COMMON_GRAMS_SEMANTICS_VERSION_V1;
    metadata.common_grams_key_version = COMMON_GRAMS_KEY_VERSION_V1;
    metadata.common_grams_dictionary_identity = identity.common_grams_dictionary_identity;
    metadata.base_analyzer_fingerprint = identity.base_analyzer_fingerprint;
    metadata.common_grams_fingerprint = identity.common_grams_fingerprint;
    metadata.scoring_coverage = ScoringCoverage::kComplete;
    metadata.scoring_stats_version = COMMON_GRAMS_SCORING_STATS_VERSION_V1;
    metadata.norm_semantics_version = COMMON_GRAMS_NORM_SEMANTICS_VERSION_V1;
    return metadata;
}

bool common_grams_identity_matches(const CommonGramsSegmentMetadata& metadata,
                                   const CommonGramsQueryIdentity& identity) {
    return metadata.common_grams_dictionary_identity == identity.common_grams_dictionary_identity &&
           metadata.base_analyzer_fingerprint == identity.base_analyzer_fingerprint &&
           metadata.common_grams_fingerprint == identity.common_grams_fingerprint;
}

Status validate_common_grams_segment_metadata(const CommonGramsSegmentMetadata& metadata) {
    switch (metadata.plain_term_key_version) {
    case PlainTermKeyVersion::kLegacyRaw:
    case PlainTermKeyVersion::kEscapedV1:
    case PlainTermKeyVersion::kRawNoInternal:
        break;
    default:
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "common_grams_metadata: invalid plain-term key version");
    }

    switch (metadata.common_grams_coverage) {
    case CommonGramsCoverage::kNone:
    case CommonGramsCoverage::kComplete:
    case CommonGramsCoverage::kMixed:
        break;
    default:
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "common_grams_metadata: invalid coverage");
    }

    switch (metadata.scoring_coverage) {
    case ScoringCoverage::kNone:
    case ScoringCoverage::kComplete:
        break;
    default:
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "common_grams_metadata: invalid scoring coverage");
    }

    if (metadata.plain_term_key_version == PlainTermKeyVersion::kRawNoInternal &&
        metadata.common_grams_coverage != CommonGramsCoverage::kNone) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "common_grams_metadata: raw-no-internal segment has gram coverage");
    }

    if (metadata.common_grams_coverage == CommonGramsCoverage::kComplete &&
        (metadata.plain_term_key_version != PlainTermKeyVersion::kEscapedV1 ||
         metadata.common_grams_semantics_version == 0 || metadata.common_grams_key_version == 0 ||
         metadata.common_grams_dictionary_identity.empty() ||
         metadata.base_analyzer_fingerprint.empty() || metadata.common_grams_fingerprint.empty())) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "common_grams_metadata: incomplete complete-coverage identity");
    }

    if (metadata.scoring_coverage == ScoringCoverage::kComplete &&
        (metadata.scoring_stats_version == 0 || metadata.norm_semantics_version == 0 ||
         metadata.base_analyzer_fingerprint.empty())) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "common_grams_metadata: incomplete scoring identity");
    }
    return Status::OK();
}

Status validate_snii_scoring_metadata(const CommonGramsSegmentMetadata* metadata,
                                      uint64_t physical_doc_count,
                                      uint64_t physical_sum_total_term_freq, bool has_scoring_tier,
                                      bool has_positions, bool has_norms) {
    if (metadata == nullptr) {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED, false>(
                "SNII semantic scoring metadata is missing");
    }
    RETURN_IF_ERROR(validate_common_grams_segment_metadata(*metadata));
    if (metadata->scoring_coverage != ScoringCoverage::kComplete ||
        metadata->scoring_stats_version != COMMON_GRAMS_SCORING_STATS_VERSION_V1 ||
        metadata->norm_semantics_version != COMMON_GRAMS_NORM_SEMANTICS_VERSION_V1) {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED, false>(
                "SNII scoring metadata uses unsupported semantics");
    }
    if (!has_scoring_tier || !has_positions || !has_norms) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "SNII complete scoring metadata requires the scoring tier, positions, and norms");
    }
    if (metadata->scoring_doc_count != physical_doc_count) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "SNII semantic scoring document count {} differs from physical document count {}",
                metadata->scoring_doc_count, physical_doc_count);
    }
    if (metadata->scoring_token_count > physical_sum_total_term_freq) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "SNII semantic scoring token count {} exceeds physical term frequency {}",
                metadata->scoring_token_count, physical_sum_total_term_freq);
    }
    if (metadata->plain_term_key_version == PlainTermKeyVersion::kRawNoInternal &&
        metadata->common_grams_coverage == CommonGramsCoverage::kNone &&
        metadata->scoring_token_count != physical_sum_total_term_freq) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "SNII semantic plain token count {} differs from physical term frequency {}",
                metadata->scoring_token_count, physical_sum_total_term_freq);
    }
    if (physical_sum_total_term_freq != 0 && metadata->scoring_token_count == 0) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "SNII non-empty physical postings have zero semantic scoring tokens");
    }
    return Status::OK();
}

bool is_common_grams_query_compatible(const CommonGramsSegmentMetadata& metadata,
                                      const CommonGramsQueryIdentity& identity) {
    return is_common_grams_query_compatible(metadata, identity, CommonGramsCoverage::kComplete);
}

bool is_common_grams_query_compatible(const CommonGramsSegmentMetadata& metadata,
                                      const CommonGramsQueryIdentity& identity,
                                      CommonGramsCoverage required_coverage) {
    return metadata.plain_term_key_version == PlainTermKeyVersion::kEscapedV1 &&
           metadata.common_grams_coverage == required_coverage &&
           required_coverage != CommonGramsCoverage::kNone &&
           metadata.common_grams_semantics_version == COMMON_GRAMS_SEMANTICS_VERSION_V1 &&
           metadata.common_grams_key_version == COMMON_GRAMS_KEY_VERSION_V1 &&
           common_grams_identity_matches(metadata, identity);
}

} // namespace doris::segment_v2::inverted_index
