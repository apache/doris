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

#include "phrase_edge_query.h"

#include <climits>
#include <fstream>
#include <functional>
#include <string>
#include <string_view>

#include "CLucene/config/repl_wchar.h"
#include "CLucene/util/stringUtil.h"
#include "common/logging.h"

namespace doris::segment_v2 {

PhraseEdgeQuery::PhraseEdgeQuery(const std::shared_ptr<lucene::search::IndexSearcher>& searcher,
                                 const TQueryOptions& query_options)
        : _searcher(searcher), _query(std::make_unique<CL_NS(search)::MultiPhraseQuery>()) {}

void PhraseEdgeQuery::add(const std::wstring& field_name, const std::vector<std::string>& terms) {
    if (terms.empty()) {
        _CLTHROWA(CL_ERR_IllegalArgument, "PhraseEdgeQuery::add: terms empty");
    }
    _field_name = field_name;
    _terms = terms;
}

void PhraseEdgeQuery::search(roaring::Roaring& roaring) {
    if (_terms.size() == 1) {
        search_one_term(roaring);
    } else {
        search_multi_term(roaring);
    }
}

void PhraseEdgeQuery::search_one_term(roaring::Roaring& roaring) {
    size_t count = 0;
    std::wstring sub_term = StringUtil::string_to_wstring(_terms[0]);
    find_words([this, &count, &sub_term, &roaring](Term* term) {
        std::wstring_view ws_term(term->text(), term->textLength());
        if (ws_term.find(sub_term) == std::wstring::npos) {
            return;
        }

        DocRange doc_range;
        TermDocs* term_doc = _searcher->getReader()->termDocs(term);
        roaring::Roaring result;
        while (term_doc->readRange(&doc_range)) {
            if (doc_range.type_ == DocRangeType::kMany) {
                result.addMany(doc_range.doc_many_size_, doc_range.doc_many->data());
            } else {
                result.addRange(doc_range.doc_range.first, doc_range.doc_range.second);
            }
        }
        _CLDELETE(term_doc);

        if (count) {
            roaring.swap(result);
        } else {
            roaring |= result;
        }
        count++;
    });
}

void PhraseEdgeQuery::search_multi_term(roaring::Roaring& roaring) {
    std::wstring suffix_term = StringUtil::string_to_wstring(_terms[0]);
    std::wstring prefix_term = StringUtil::string_to_wstring(_terms.back());

    std::vector<CL_NS(index)::Term*> suffix_terms;
    std::vector<CL_NS(index)::Term*> prefix_terms;

    find_words([&suffix_term, &suffix_terms, &prefix_term, &prefix_terms](Term* term) {
        std::wstring_view ws_term(term->text(), term->textLength());

        if (ws_term.ends_with(suffix_term)) {
            suffix_terms.push_back(_CL_POINTER(term));
        }

        if (ws_term.starts_with(prefix_term)) {
            prefix_terms.push_back(_CL_POINTER(term));
        }
    });

    for (size_t i = 0; i < _terms.size(); i++) {
        if (i == 0) {
            handle_terms(_field_name, suffix_term, suffix_terms);
        } else if (i == _terms.size() - 1) {
            handle_terms(_field_name, prefix_term, prefix_terms);
        } else {
            std::wstring ws_term = StringUtil::string_to_wstring(_terms[i]);
            add_default_term(_field_name, ws_term);
        }
    }

    _searcher->_search(_query.get(), [&roaring](const int32_t docid, const float_t /*score*/) {
        roaring.add(docid);
    });
}

void PhraseEdgeQuery::add_default_term(const std::wstring& field_name,
                                       const std::wstring& ws_term) {
    Term* t = _CLNEW Term(field_name.c_str(), ws_term.c_str());
    _query->add(t);
    _CLLDECDELETE(t);
}

void PhraseEdgeQuery::handle_terms(const std::wstring& field_name, const std::wstring& ws_term,
                                   std::vector<CL_NS(index)::Term*>& checked_terms) {
    if (checked_terms.empty()) {
        add_default_term(field_name, ws_term);
    } else {
        _query->add(checked_terms);
        for (const auto& t : checked_terms) {
            _CLLDECDELETE(t);
        }
    }
};

void PhraseEdgeQuery::find_words(const std::function<void(Term*)>& cb) {
    Term* term = nullptr;
    TermEnum* enumerator = nullptr;
    try {
        enumerator = _searcher->getReader()->terms();
        while (enumerator->next()) {
            term = enumerator->term();
            cb(term);
            _CLDECDELETE(term);
        }
    }
    _CLFINALLY({
        _CLDECDELETE(term);
        enumerator->close();
        _CLDELETE(enumerator);
    })
}

} // namespace doris::segment_v2