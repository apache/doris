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

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/index/inverted/common_grams/common_grams_key_codec.h"
#include "storage/index/inverted/query/query_info.h"
#include "storage/index/snii/reader/logical_index_reader.h"

namespace doris::snii::query::internal {

inline segment_v2::inverted_index::PlainTermKeyVersion plain_term_key_version(
        const reader::LogicalIndexReader& idx) {
    const auto* metadata = idx.common_grams_metadata();
    return metadata == nullptr ? segment_v2::inverted_index::PlainTermKeyVersion::kLegacyRaw
                               : metadata->plain_term_key_version;
}

inline Status route_plain_query_term_view(const reader::LogicalIndexReader& idx,
                                          std::string_view logical_term, std::string* scratch,
                                          std::string_view* physical_term, bool* representable) {
    DORIS_CHECK(scratch != nullptr);
    DORIS_CHECK(physical_term != nullptr);
    DORIS_CHECK(representable != nullptr);
    scratch->clear();
    *physical_term = {};
    *representable = false;

    const auto version = plain_term_key_version(idx);
    if (version == segment_v2::inverted_index::PlainTermKeyVersion::kLegacyRaw &&
        segment_v2::inverted_index::legacy_raw_exact_requires_bypass(logical_term)) {
        return Status::Error<ErrorCode::INVERTED_INDEX_BYPASS>(
                "SNII legacy raw term overlaps an internal term namespace");
    }
    auto encoded =
            segment_v2::inverted_index::try_encode_plain_term_view(logical_term, version, scratch);
    if (!encoded.has_value()) {
        return std::move(encoded.error());
    }
    if (encoded->has_value()) {
        *physical_term = **encoded;
        *representable = true;
    }
    return Status::OK();
}

inline Status route_plain_query_term(const reader::LogicalIndexReader& idx,
                                     std::string_view logical_term, std::string* physical_term,
                                     bool* representable) {
    DORIS_CHECK(physical_term != nullptr);
    std::string scratch;
    std::string_view physical_term_view;
    RETURN_IF_ERROR(route_plain_query_term_view(idx, logical_term, &scratch, &physical_term_view,
                                                representable));
    physical_term->assign(physical_term_view);
    return Status::OK();
}

inline Status route_query_term_view(const reader::LogicalIndexReader& idx,
                                    const segment_v2::TermInfo& term_info, std::string* scratch,
                                    std::string_view* physical_term, bool* representable) {
    DORIS_CHECK(term_info.is_single_term());
    if (term_info.key_kind == segment_v2::TermKeyKind::kCommonGram) {
        return Status::Error<ErrorCode::INVERTED_INDEX_BYPASS>(
                "CommonGrams query plan requires segment capability validation");
    }
    return route_plain_query_term_view(idx, term_info.get_single_term(), scratch, physical_term,
                                       representable);
}

inline Status route_query_term(const reader::LogicalIndexReader& idx,
                               const segment_v2::TermInfo& term_info, std::string* physical_term,
                               bool* representable) {
    DORIS_CHECK(term_info.is_single_term());
    if (term_info.key_kind == segment_v2::TermKeyKind::kCommonGram) {
        return Status::Error<ErrorCode::INVERTED_INDEX_BYPASS>(
                "CommonGrams query plan requires segment capability validation");
    }
    return route_plain_query_term(idx, term_info.get_single_term(), physical_term, representable);
}

inline Status route_query_terms(const reader::LogicalIndexReader& idx,
                                const segment_v2::InvertedIndexQueryInfo& query_info,
                                std::vector<std::string>* routed_terms, bool* all_representable) {
    DORIS_CHECK(routed_terms != nullptr);
    DORIS_CHECK(all_representable != nullptr);
    DORIS_CHECK(routed_terms->size() == query_info.term_infos.size());
    *all_representable = true;
    const auto version = plain_term_key_version(idx);
    size_t output_index = 0;
    std::string scratch;
    for (size_t i = 0; i < query_info.term_infos.size(); ++i) {
        const auto& term_info = query_info.term_infos[i];
        DORIS_CHECK(term_info.is_single_term());
        if (term_info.key_kind == segment_v2::TermKeyKind::kCommonGram) {
            return Status::Error<ErrorCode::INVERTED_INDEX_BYPASS>(
                    "CommonGrams query plan requires segment capability validation");
        }
        const auto logical_term = std::string_view((*routed_terms)[i]);
        if (version == segment_v2::inverted_index::PlainTermKeyVersion::kLegacyRaw &&
            segment_v2::inverted_index::legacy_raw_exact_requires_bypass(logical_term)) {
            return Status::Error<ErrorCode::INVERTED_INDEX_BYPASS>(
                    "SNII legacy raw term overlaps an internal term namespace");
        }
        auto physical_term = segment_v2::inverted_index::try_encode_plain_term_view(
                logical_term, version, &scratch);
        if (!physical_term.has_value()) {
            return std::move(physical_term.error());
        }
        if (!physical_term->has_value()) {
            *all_representable = false;
            continue;
        }
        if (output_index != i) {
            if (scratch.empty()) {
                (*routed_terms)[output_index] = std::move((*routed_terms)[i]);
            } else {
                (*routed_terms)[output_index] = std::move(scratch);
            }
        } else if (!scratch.empty()) {
            (*routed_terms)[i] = std::move(scratch);
        }
        ++output_index;
    }
    routed_terms->resize(output_index);
    return Status::OK();
}

inline Status route_plain_query_terms(const reader::LogicalIndexReader& idx,
                                      const std::vector<std::string>& logical_terms,
                                      std::vector<std::string>* physical_terms,
                                      bool* all_representable) {
    DORIS_CHECK(physical_terms != nullptr);
    DORIS_CHECK(all_representable != nullptr);
    physical_terms->clear();
    physical_terms->reserve(logical_terms.size());
    *all_representable = true;
    for (const std::string& logical_term : logical_terms) {
        std::string physical_term;
        bool representable = false;
        RETURN_IF_ERROR(route_plain_query_term(idx, logical_term, &physical_term, &representable));
        if (representable) {
            physical_terms->push_back(std::move(physical_term));
        } else {
            *all_representable = false;
        }
    }
    return Status::OK();
}

inline Status route_plain_enumeration_prefix(const reader::LogicalIndexReader& idx,
                                             std::string_view logical_prefix,
                                             std::string* physical_prefix, bool* representable) {
    DORIS_CHECK(physical_prefix != nullptr);
    DORIS_CHECK(representable != nullptr);
    physical_prefix->clear();
    *representable = false;

    const auto version = plain_term_key_version(idx);
    if (version == segment_v2::inverted_index::PlainTermKeyVersion::kLegacyRaw &&
        segment_v2::inverted_index::legacy_raw_prefix_requires_bypass(logical_prefix)) {
        return Status::Error<ErrorCode::INVERTED_INDEX_BYPASS>(
                "SNII legacy raw expansion overlaps an internal term namespace");
    }
    auto encoded = segment_v2::inverted_index::try_encode_plain_term(logical_prefix, version,
                                                                     physical_prefix);
    if (!encoded.has_value()) {
        return std::move(encoded.error());
    }
    *representable = encoded.value();
    return Status::OK();
}

} // namespace doris::snii::query::internal
