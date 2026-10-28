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

#include "storage/index/index_reader_helper.h"

#include "storage/index/inverted/analysis/analyzer/analyzer.h"

namespace doris::segment_v2 {
namespace {

bool can_score_match(const TabletIndex* index_meta) {
    const auto& properties = index_meta->properties();
    // A keyword index keeps each value as one term, without positions or norms, and its postings
    // score with unit frequency and length whatever support_phrase says, as an Elasticsearch
    // keyword field does: over tags (x, x, y, z), tag MATCH 'x' scores idf(x) = ln(2) for both
    // matching rows, the same as SEARCH('tag:x'). MATCH statistics collect these terms too.
    if (!inverted_index::InvertedIndexAnalyzer::should_analyzer(properties)) {
        return true;
    }
    return get_parser_phrase_support_string_from_properties(properties) ==
                   INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES ||
           IndexReaderHelper::is_normalizer_only(properties);
}

} // namespace

bool IndexReaderHelper::is_need_similarity_score(InvertedIndexQueryType query_type,
                                                 const TabletIndex* index_meta) {
    return (query_type == InvertedIndexQueryType::MATCH_ANY_QUERY ||
            query_type == InvertedIndexQueryType::MATCH_ALL_QUERY ||
            query_type == InvertedIndexQueryType::MATCH_PHRASE_QUERY ||
            query_type == InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY) &&
           can_score_match(index_meta);
}

bool IndexReaderHelper::is_need_similarity_score(TExprOpcode::type query_type,
                                                 const TabletIndex* index_meta) {
    return (query_type == TExprOpcode::MATCH_ANY || query_type == TExprOpcode::MATCH_ALL ||
            query_type == TExprOpcode::MATCH_PHRASE ||
            query_type == TExprOpcode::MATCH_PHRASE_PREFIX) &&
           can_score_match(index_meta);
}

} // namespace doris::segment_v2
