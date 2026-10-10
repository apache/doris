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

#include "storage/index/index_iterator.h"
#include "storage/index/inverted/analyzer/analyzer.h"
#include "storage/index/inverted/inverted_index_reader.h"

namespace doris::segment_v2 {

class IndexReaderHelper {
public:
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
               INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES;
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

    // Positions -- and therefore phrase and relevance work -- are only reachable
    // when the index tokenizes. The reason is on the QUERY side, not the write
    // side: InvertedIndexAnalyzer::get_analyse_result() returns the entire search
    // string as ONE term whenever should_analyzer() is false, and every phrase
    // variant (MATCH_PHRASE, _PREFIX, _EDGE) takes its terms from there. A
    // single-term phrase is just a term query, so no query against a
    // non-tokenizing index can observe a position.
    //
    // Note it is NOT enough to say such an index holds one term per document:
    // an ARRAY column with parser=none emits one term per element and the writer
    // does advance positions between them (SniiIndexColumnWriter::_add_array_values).
    // Those positions are simply unreachable, because the query can never supply
    // a second term to match against them.
    //
    // New indexes no longer carry the option at all (Index.java drops it), but
    // tablet metadata already on disk still does, so the check is repeated here.
    static bool persists_scoring_inputs(const TabletIndex* index_meta) {
        const auto& properties = index_meta->properties();
        return get_parser_phrase_support_string_from_properties(properties) ==
                       INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES &&
               inverted_index::InvertedIndexAnalyzer::should_analyzer(properties);
    }

    static bool is_need_similarity_score(InvertedIndexQueryType query_type,
                                         const TabletIndex* index_meta) {
        if (query_type == InvertedIndexQueryType::MATCH_ANY_QUERY ||
            query_type == InvertedIndexQueryType::MATCH_ALL_QUERY ||
            query_type == InvertedIndexQueryType::MATCH_PHRASE_QUERY ||
            query_type == InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY) {
            if (persists_scoring_inputs(index_meta)) {
                return true;
            }
        }
        return false;
    }

    static bool is_need_similarity_score(TExprOpcode::type query_type,
                                         const TabletIndex* index_meta) {
        if (query_type == TExprOpcode::MATCH_ANY || query_type == TExprOpcode::MATCH_ALL ||
            query_type == TExprOpcode::MATCH_PHRASE ||
            query_type == TExprOpcode::MATCH_PHRASE_PREFIX) {
            if (persists_scoring_inputs(index_meta)) {
                return true;
            }
        }
        return false;
    }
};

} // namespace doris::segment_v2