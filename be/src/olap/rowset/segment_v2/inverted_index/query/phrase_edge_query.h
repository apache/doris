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

#include <memory>

// clang-format off
#include "olap/rowset/segment_v2/inverted_index/query/query.h"
#include "CLucene/search/MultiPhraseQuery.h"
// clang-format on

CL_NS_USE(search)

namespace doris::segment_v2 {

class PhraseEdgeQuery : public Query {
public:
    PhraseEdgeQuery(const std::shared_ptr<lucene::search::IndexSearcher>& searcher,
                    const TQueryOptions& query_options);
    ~PhraseEdgeQuery() override = default;

    void add(const std::wstring& field_name, const std::vector<std::string>& terms) override;
    void search(roaring::Roaring& roaring) override;

private:
    void search_one_term(roaring::Roaring& roaring);
    void search_multi_term(roaring::Roaring& roaring);

    void add_default_term(const std::wstring& field_name, const std::wstring& ws_term);
    void handle_terms(const std::wstring& field_name, const std::wstring& ws_term,
                      std::vector<CL_NS(index)::Term*>& checked_terms);
    void find_words(const std::function<void(Term*)>& cb);

private:
    std::shared_ptr<lucene::search::IndexSearcher> _searcher;

    std::wstring _field_name;
    std::vector<std::string> _terms;
    std::unique_ptr<CL_NS(search)::MultiPhraseQuery> _query;
    int32_t _max_expansions = 50;
};

} // namespace doris::segment_v2