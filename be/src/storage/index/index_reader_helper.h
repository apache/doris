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

#include <boost/algorithm/string/predicate.hpp>
#include <map>
#include <string>

#include "storage/index/index_iterator.h"
#include "storage/index/inverted/analyzer/analyzer.h"
#include "storage/index/inverted/inverted_index_reader.h"

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"

class IndexReaderHelper {
public:
    static bool is_normalizer_only(const std::map<std::string, std::string>& properties) {
        const auto normalizer = properties.find(INVERTED_INDEX_NORMALIZER_NAME_KEY);
        const auto analyzer = properties.find(INVERTED_INDEX_ANALYZER_NAME_KEY);
        // A custom normalizer can split a value into multiple terms that need positions.
        return !properties.contains(INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY) &&
               normalizer != properties.end() && boost::iequals(normalizer->second, "lowercase") &&
               (analyzer == properties.end() || analyzer->second.empty()) &&
               get_parser_string_from_properties(properties) == INVERTED_INDEX_PARSER_NONE;
    }

    static bool is_fulltext_index(const IndexReaderPtr& reader) {
        if (reader == nullptr || reader->index_type() != IndexType::INVERTED) {
            return false;
        }

        auto inverted_index_reader = std::static_pointer_cast<InvertedIndexReader>(reader);
        return inverted_index_reader->type() == InvertedIndexReaderType::FULLTEXT;
    }

    static bool is_string_index(const IndexReaderPtr& reader) {
        if (reader == nullptr || reader->index_type() != IndexType::INVERTED) {
            return false;
        }

        auto inverted_index_reader = std::static_pointer_cast<InvertedIndexReader>(reader);
        return inverted_index_reader->type() == InvertedIndexReaderType::STRING_TYPE;
    }

    static bool is_bkd_index(const IndexReaderPtr& reader) {
        if (reader == nullptr || reader->index_type() != IndexType::INVERTED) {
            return false;
        }

        auto inverted_index_reader = std::static_pointer_cast<InvertedIndexReader>(reader);
        return inverted_index_reader->type() == InvertedIndexReaderType::BKD;
    }

    static bool is_support_phrase(const IndexReaderPtr& reader) {
        if (reader == nullptr || reader->index_type() != IndexType::INVERTED) {
            return false;
        }

        auto inverted_index_reader = std::static_pointer_cast<InvertedIndexReader>(reader);
        const auto& properties = inverted_index_reader->get_index_properties();
        return get_parser_phrase_support_string_from_properties(properties) ==
                       INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES ||
               is_normalizer_only(properties);
    }

    // only string type or bkd index reader can be used for equal
    static bool has_string_or_bkd_index(const IndexIterator* iter) {
        if (iter == nullptr) {
            return false;
        }

        return iter->get_reader(InvertedIndexReaderType::STRING_TYPE) != nullptr ||
               iter->get_reader(InvertedIndexReaderType::BKD) != nullptr;
    }

    static bool has_bkd_index(const IndexIterator* iter) {
        if (iter == nullptr) {
            return false;
        }

        return iter->get_reader(InvertedIndexReaderType::BKD) != nullptr;
    }

    static bool has_string_index(const IndexIterator* iter) {
        if (iter == nullptr) {
            return false;
        }

        return iter->get_reader(InvertedIndexReaderType::STRING_TYPE) != nullptr;
    }

    static bool is_need_similarity_score(InvertedIndexQueryType query_type,
                                         const TabletIndex* index_meta) {
        return (query_type == InvertedIndexQueryType::MATCH_ANY_QUERY ||
                query_type == InvertedIndexQueryType::MATCH_ALL_QUERY ||
                query_type == InvertedIndexQueryType::MATCH_PHRASE_QUERY ||
                query_type == InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY) &&
               can_score_match(index_meta);
    }

    static bool is_need_similarity_score(TExprOpcode::type query_type,
                                         const TabletIndex* index_meta) {
        return (query_type == TExprOpcode::MATCH_ANY || query_type == TExprOpcode::MATCH_ALL ||
                query_type == TExprOpcode::MATCH_PHRASE ||
                query_type == TExprOpcode::MATCH_PHRASE_PREFIX) &&
               can_score_match(index_meta);
    }

private:
    static bool can_score_match(const TabletIndex* index_meta) {
        const auto& properties = index_meta->properties();
        // A keyword index keeps each value as one term, without positions or norms, and its
        // postings score with unit frequency and length whatever support_phrase says, as an
        // Elasticsearch keyword field does: over tags (x, x, y, z), tag MATCH 'x' scores
        // idf(x) = ln(2) for both matching rows, the same as SEARCH('tag:x').
        if (!inverted_index::InvertedIndexAnalyzer::should_analyzer(properties)) {
            return true;
        }
        return get_parser_phrase_support_string_from_properties(properties) ==
                       INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES ||
               is_normalizer_only(properties);
    }
};

#include "common/compile_check_end.h"
} // namespace doris::segment_v2
