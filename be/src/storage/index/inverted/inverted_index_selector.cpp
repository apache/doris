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

#include "storage/index/inverted/inverted_index_selector.h"

#include <optional>

#include "common/logging.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "storage/index/inverted/analyzer/analyzer.h"
#include "storage/utils.h"

namespace doris::segment_v2 {

Status add_inverted_index_selection_candidate(
        InvertedIndexSelectionCandidate candidate,
        std::vector<InvertedIndexSelectionCandidate>* candidates,
        InvertedIndexSelectionKeyIndex* key_index) {
    DORIS_CHECK(candidates != nullptr);
    DORIS_CHECK(key_index != nullptr);
    for (const auto& existing : *candidates) {
        if (existing.index_id == candidate.index_id) {
            return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                    "Duplicate inverted index id {} in one field", candidate.index_id);
        }
    }

    const size_t candidate_index = candidates->size();
    candidates->push_back(std::move(candidate));
    (*key_index)[candidates->back().analyzer_key].push_back(candidate_index);
    return Status::OK();
}

Result<size_t> select_best_inverted_index_candidate(
        const std::vector<InvertedIndexSelectionCandidate>& candidates,
        const InvertedIndexSelectionKeyIndex& key_index, FieldType field_type,
        InvertedIndexQueryType query_type, std::string_view normalized_analyzer_key) {
    if (candidates.empty()) {
        return ResultError(Status::Error<ErrorCode::INVERTED_INDEX_NO_TERMS>(
                "No available inverted index candidates"));
    }

    const std::vector<size_t>* exact_candidates = nullptr;
    if (!normalized_analyzer_key.empty()) {
        const auto exact = key_index.find(std::string(normalized_analyzer_key));
        if (exact == key_index.end() || exact->second.empty()) {
            return ResultError(Status::Error<ErrorCode::INVERTED_INDEX_BYPASS>(
                    "No inverted index found for analyzer '{}'", normalized_analyzer_key));
        }
        exact_candidates = &exact->second;
    }

    const size_t candidate_count =
            exact_candidates == nullptr ? candidates.size() : exact_candidates->size();
    auto candidate_at =
            [&](size_t ordinal) -> std::pair<size_t, const InvertedIndexSelectionCandidate&> {
        const size_t index = exact_candidates == nullptr ? ordinal : (*exact_candidates)[ordinal];
        DORIS_CHECK(index < candidates.size());
        return {index, candidates[index]};
    };
    auto pick =
            [&](std::optional<InvertedIndexReaderType> preferred_type) -> std::optional<size_t> {
        std::optional<size_t> best;
        for (size_t ordinal = 0; ordinal < candidate_count; ++ordinal) {
            const auto [index, candidate] = candidate_at(ordinal);
            if (preferred_type.has_value() && candidate.reader_type != *preferred_type) {
                continue;
            }
            if (!best.has_value() || candidate.index_id < candidates[*best].index_id) {
                best = index;
            }
        }
        return best;
    };

    if (is_string_type(field_type)) {
        if (is_match_query(query_type)) {
            if (auto best = pick(InvertedIndexReaderType::FULLTEXT); best.has_value()) {
                return *best;
            }
        }
        if (is_equal_query(query_type)) {
            if (auto best = pick(InvertedIndexReaderType::STRING_TYPE); best.has_value()) {
                return *best;
            }
        }
    } else if (field_is_numeric_type(field_type)) {
        if (is_range_query(query_type)) {
            if (auto best = pick(InvertedIndexReaderType::BKD); best.has_value()) {
                return *best;
            }
        }
        if (auto best = pick(InvertedIndexReaderType::BKD); best.has_value()) {
            return *best;
        }
        if (auto best = pick(InvertedIndexReaderType::STRING_TYPE); best.has_value()) {
            return *best;
        }
    }

    auto best = pick(std::nullopt);
    DORIS_CHECK(best.has_value());
    return *best;
}

FieldType get_inverted_index_leaf_field_type(const DataTypePtr& column_type) {
    DORIS_CHECK(column_type != nullptr);
    DataTypePtr leaf_type = remove_nullable(column_type);
    while (leaf_type->get_storage_field_type() == FieldType::OLAP_FIELD_TYPE_ARRAY) {
        const auto* array_type = dynamic_cast<const DataTypeArray*>(leaf_type.get());
        DORIS_CHECK(array_type != nullptr);
        leaf_type = remove_nullable(array_type->get_nested_type());
    }
    return leaf_type->get_storage_field_type();
}

InvertedIndexReaderType infer_inverted_index_reader_type(
        FieldType field_type, const std::map<std::string, std::string>& properties) {
    if (is_string_type(field_type)) {
        return inverted_index::InvertedIndexAnalyzer::should_analyzer(properties)
                       ? InvertedIndexReaderType::FULLTEXT
                       : InvertedIndexReaderType::STRING_TYPE;
    }
    if (field_is_numeric_type(field_type)) {
        return InvertedIndexReaderType::BKD;
    }
    return InvertedIndexReaderType::UNKNOWN;
}

} // namespace doris::segment_v2
