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

#include "olap/rowset//segment_v2/inverted_index/query/prefix_query.h"

namespace doris {

namespace segment_v2 {

PhrasePrefixQuery::PhrasePrefixQuery(const std::shared_ptr<lucene::search::IndexSearcher>& searcher)
        : _searcher(searcher) {}

void PhrasePrefixQuery::add(const std::wstring& field_name, const std::vector<std::string>& terms) {
    if (terms.empty()) {
        return;
    }

    for (size_t i = 0; i < terms.size(); i++) {
        if (i < terms.size() - 1) {
            std::wstring ws = StringUtil::string_to_wstring(terms[i]);
            Term* t = _CLNEW Term(field_name.c_str(), ws.c_str());
            _query.add(t);
            _CLDECDELETE(t);
        } else {
            std::vector<CL_NS(index)::Term*> prefix_terms;
            PrefixQuery::get_prefix_terms(_searcher->getReader(), field_name, terms[i],
                                          prefix_terms, _max_expansions);
            if (prefix_terms.empty()) {
                continue;
            }
            _query.add(prefix_terms);
            for (auto& t : prefix_terms) {
                _CLDECDELETE(t);
            }
        }
    }
}

void PhrasePrefixQuery::search(roaring::Roaring& roaring) {
    _searcher->_search(&_query, [&roaring](const int32_t docid, const float_t /*score*/) {
        roaring.add(docid);
    });
}

} // namespace segment_v2

} // namespace doris