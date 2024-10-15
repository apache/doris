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

#include "disjunction_query.h"

namespace doris::segment_v2 {

DisjunctionQuery::DisjunctionQuery(const std::shared_ptr<lucene::search::IndexSearcher>& searcher,
                                   const TQueryOptions& query_options)
        : _searcher(searcher) {}

void DisjunctionQuery::add(const std::wstring& field_name, const std::vector<std::string>& terms) {
    if (terms.empty()) {
        _CLTHROWA(CL_ERR_IllegalArgument, "DisjunctionQuery::add: terms empty");
    }

    _field_name = field_name;
    _terms = terms;
}

void DisjunctionQuery::search(roaring::Roaring& roaring) {
    auto func = [this, &roaring](const std::string& term, bool first) {
        std::wstring ws_term = StringUtil::string_to_wstring(term);
        auto* t = _CLNEW Term(_field_name.c_str(), ws_term.c_str());
        auto* term_doc = _searcher->getReader()->termDocs(t);
        TermIterator iterator(term_doc);

        DocRange doc_range;
        roaring::Roaring result;
        while (iterator.readRange(&doc_range)) {
            if (doc_range.type_ == DocRangeType::kMany) {
                result.addMany(doc_range.doc_many_size_, doc_range.doc_many->data());
            } else {
                result.addRange(doc_range.doc_range.first, doc_range.doc_range.second);
            }
        }

        _CLDELETE(term_doc);
        _CLDELETE(t);

        if (first) {
            roaring.swap(result);
        } else {
            roaring |= result;
        }
    };
    for (int i = 0; i < _terms.size(); i++) {
        func(_terms[i], i == 0);
    }
}

} // namespace doris::segment_v2