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

#include "phrase_prefix_query.h"

#include "CLucene/util/stringUtil.h"
#include "olap/rowset//segment_v2/inverted_index/query/prefix_query.h"

namespace doris::segment_v2 {

PhrasePrefixQuery::PhrasePrefixQuery(const std::shared_ptr<lucene::search::IndexSearcher>& searcher,
                                     const TQueryOptions& query_options)
        : _searcher(searcher),
          _query(std::make_unique<CL_NS(search)::MultiPhraseQuery>()),
          _max_expansions(query_options.inverted_index_max_expansions) {}

void PhrasePrefixQuery::add(const std::wstring& field_name, const std::vector<std::string>& terms) {
    if (terms.empty()) {
        _CLTHROWA(CL_ERR_IllegalArgument, "PhrasePrefixQuery::add: terms empty");
    }

    for (size_t i = 0; i < terms.size(); i++) {
        if (i < terms.size() - 1) {
            std::wstring ws = StringUtil::string_to_wstring(terms[i]);
            Term* t = _CLNEW Term(field_name.c_str(), ws.c_str());
            _query->add(t);
            _CLLDECDELETE(t);
        } else {
            std::vector<CL_NS(index)::Term*> prefix_terms;
            PrefixQuery::get_prefix_terms(_searcher->getReader(), field_name, terms[i],
                                          prefix_terms, _max_expansions);
            if (prefix_terms.empty()) {
                std::wstring ws_term = StringUtil::string_to_wstring(terms[i]);
                Term* t = _CLNEW Term(field_name.c_str(), ws_term.c_str());
                prefix_terms.push_back(t);
            }
            _query->add(prefix_terms);
            for (auto& t : prefix_terms) {
                _CLLDECDELETE(t);
            }
        }
    }
}

void PhrasePrefixQuery::search(roaring::Roaring& roaring) {
    _searcher->_search(_query.get(), [&roaring](const int32_t docid, const float_t /*score*/) {
        roaring.add(docid);
    });
}

} // namespace doris::segment_v2