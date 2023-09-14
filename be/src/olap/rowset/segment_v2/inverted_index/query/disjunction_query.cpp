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

namespace doris {

DisjunctionQuery::DisjunctionQuery(IndexReader* reader) : _reader(reader) {}

DisjunctionQuery::~DisjunctionQuery() {
    for (auto& term : _terms) {
        if (term) {
            _CLDELETE(term);
        }
    }
    for (auto& term_doc : _term_docs) {
        if (term_doc) {
            _CLDELETE(term_doc);
        }
    }
}

void DisjunctionQuery::add(const std::wstring& field_name, const std::vector<std::string>& terms) {
    if (terms.size() < 1) {
        _CLTHROWA(CL_ERR_IllegalArgument, "DisjunctionQuery::add: terms.size() < 1");
    }

    for (auto& term : terms) {
        std::wstring ws_term = StringUtil::string_to_wstring(term);
        Term* t = _CLNEW Term(field_name.c_str(), ws_term.c_str());
        _terms.push_back(t);
        TermDocs* term_doc = _reader->termDocs(t);
        _term_docs.push_back(term_doc);
        _term_iterators.emplace_back(term_doc);
    }
}

void DisjunctionQuery::search(roaring::Roaring& roaring) {
    roaring::Roaring result;
    auto func = [&roaring](const TermIterator& term_docs, bool first) {
        roaring::Roaring result;
        DocRange doc_range;
        while (term_docs.readRange(&doc_range)) {
            if (doc_range.type_ == DocRangeType::kMany) {
                result.addMany(doc_range.doc_many_size_, doc_range.doc_many->data());
            } else {
                result.addRange(doc_range.doc_range.first, doc_range.doc_range.second);
            }
        }
        if (first) {
            roaring.swap(result);
        } else {
            roaring |= result;
        }
    };
    for (int i = 0; i < _term_iterators.size(); i++) {
        auto& iter = _term_iterators[i];
        if (i == 0) {
            func(iter, true);
        } else {
            func(iter, false);
        }
    }
}

} // namespace doris