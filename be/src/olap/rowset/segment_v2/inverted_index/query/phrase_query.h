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

// clang-format off
#include "olap/rowset/segment_v2/inverted_index/query/query.h"
#include "CLucene/search/PhraseQuery.h"
// clang-format on

#include <variant>

#include "olap/rowset/segment_v2/inverted_index/query/phrase_query/exact_phrase_matcher.h"
#include "olap/rowset/segment_v2/inverted_index/query/phrase_query/ordered_sloppy_phrase_matcher.h"
#include "olap/rowset/segment_v2/inverted_index/query/phrase_query/sloppy_phrase_matcher.h"
#include "olap/rowset/segment_v2/inverted_index_query_type.h"

CL_NS_USE(index)
CL_NS_USE(search)

namespace doris::segment_v2 {

using namespace inverted_index;

// ExactPhraseMatcher: x match_phrase 'aaa bbb'
// SloppyPhraseMatcher: x match_phrase 'aaa bbb ~2', support slop
// OrderedSloppyPhraseMatcher: x match_phrase 'aaa bbb ~2+', ensuring that the words appear in the specified order.
using Matcher = std::variant<ExactPhraseMatcher, inverted_index::SloppyPhraseMatcher,
                             OrderedSloppyPhraseMatcher>;

class PhraseQuery : public Query {
public:
    PhraseQuery(const std::shared_ptr<lucene::search::IndexSearcher>& searcher,
                const TQueryOptions& query_options);
    ~PhraseQuery() override;

    void add(const InvertedIndexQueryInfo& query_info) override;
    void add(const std::wstring& field_name, const std::vector<std::string>& terms) override;
    void search(roaring::Roaring& roaring) override;

private:
    // Use bitmap for merging inverted lists
    void search_by_bitmap(roaring::Roaring& roaring);
    // Use skiplist for merging inverted lists
    void search_by_skiplist(roaring::Roaring& roaring);

    int32_t do_next(int32_t doc);
    bool matches(int32_t doc);

    TermPositions* ensure_term_position(const std::string& term, const std::wstring& field_name,
                                        bool is_save_iter = true);
    void init_exact_phrase_matcher(const InvertedIndexQueryInfo& query_info);
    void init_sloppy_phrase_matcher(const InvertedIndexQueryInfo& query_info);
    void init_ordered_sloppy_phrase_matcher(const InvertedIndexQueryInfo& query_info);

public:
    static void parser_slop(std::string& query, InvertedIndexQueryInfo& query_info);
    static void parser_info(std::string& query, const std::string& field_name,
                            InvertedIndexQueryType query_type,
                            const std::map<std::string, std::string>& properties,
                            InvertedIndexQueryInfo& query_info, bool sequential_opt);

private:
    std::shared_ptr<lucene::search::IndexSearcher> _searcher;

    TermPositionIterator _lead1;
    TermPositionIterator _lead2;
    std::vector<TermPositionIterator> _others;
    std::vector<TermPositionIterator> _iterators;

    std::vector<Term*> _terms;
    std::vector<TermDocs*> _term_docs;

    std::vector<std::vector<std::string>> _additional_terms;
    std::vector<Matcher> _matchers;
};

} // namespace doris::segment_v2