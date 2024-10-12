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

#include "olap/rowset/segment_v2/inverted_index/query_v2/term_query.h"

#include <memory>

namespace doris::segment_v2::idx_query_v2 {

TermQuery::~TermQuery() {
    if (_term_docs) {
        _CLDELETE(_term_docs);
    }
}

TermQuery::TermQuery(const std::shared_ptr<lucene::search::IndexSearcher>& searcher,
                     const TQueryOptions& query_options, QueryInfo query_info) {
    std::wstring ws_term = StringUtil::string_to_wstring(query_info.terms[0]);
    auto* t = _CLNEW Term(query_info.field_name.c_str(), ws_term.c_str());
    _term_docs = searcher->getReader()->termDocs(t);
    _iter = TermIterator(_term_docs);
    _CLDECDELETE(t);
}

} // namespace doris::segment_v2::idx_query_v2