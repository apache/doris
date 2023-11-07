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

#include "range_query.h"

namespace doris {

RangeQuery::RangeQuery(IndexReader* reader) : _reader(reader) {}

RangeQuery::~RangeQuery() {
    for (auto& term_doc : _term_docs) {
        if (term_doc) {
            _CLDELETE(term_doc);
        }
    }
}

Status RangeQuery::add(const std::wstring& field_name, InvertedIndexRangeQueryI* query) {
    std::unique_ptr<lucene::index::Term, void (*)(lucene::index::Term*)> lower_term(
            nullptr, [](lucene::index::Term* term) { _CLDECDELETE(term); });
    std::unique_ptr<lucene::index::Term, void (*)(lucene::index::Term*)> upper_term(
            nullptr, [](lucene::index::Term* term) { _CLDECDELETE(term); });

    if (query->low_value_is_null() && query->high_value_is_null()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_INVALID_PARAMETERS>(
                "StringTypeInvertedIndexReader::handle_range_query error: both low_value and "
                "high_value is null");
    }
    auto search_low = query->get_low_value();
    if (!query->low_value_is_null()) {
        std::wstring search_low_ws = StringUtil::string_to_wstring(search_low);
        lower_term.reset(_CLNEW lucene::index::Term(field_name.c_str(), search_low_ws.c_str()));
    } else {
        lower_term.reset(_CLNEW Term(field_name.c_str(), L""));
    }
    auto search_high = query->get_high_value();
    if (!query->high_value_is_null()) {
        std::wstring search_high_ws = StringUtil::string_to_wstring(search_high);
        upper_term.reset(_CLNEW lucene::index::Term(field_name.c_str(), search_high_ws.c_str()));
    }

    auto* _enumerator = _reader->terms(lower_term.get());
    Term* lastTerm = nullptr;
    try {
        bool checkLower = false;
        if (!query->is_low_value_inclusive()) { // make adjustments to set to exclusive
            checkLower = true;
        }

        do {
            lastTerm = _enumerator->term();
            if (lastTerm != nullptr && lastTerm->field() == field_name) {
                if (!checkLower || _tcscmp(lastTerm->text(), lower_term->text()) > 0) {
                    checkLower = false;
                    if (upper_term != nullptr) {
                        int compare = _tcscmp(upper_term->text(), lastTerm->text());
                        /* if beyond the upper term, or is exclusive and
                             * this is equal to the upper term, break out */
                        if ((compare < 0) || (!query->is_high_value_inclusive() && compare == 0)) {
                            break;
                        }
                    }
                    TermDocs* term_doc = _reader->termDocs(lastTerm);
                    _term_docs.push_back(term_doc);
                    _term_iterators.emplace_back(term_doc);
                }
            } else {
                break;
            }
            _CLDECDELETE(lastTerm);
        } while (_enumerator->next());
    } catch (CLuceneError& e) {
        _CLDECDELETE(lastTerm);
        _enumerator->close();
        _CLDELETE(_enumerator);
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "CLuceneError occured, error msg: {}, search_str: {}", e.what(),
                query->to_string());
    }
    _CLDECDELETE(lastTerm);
    _enumerator->close();
    _CLDELETE(_enumerator);
    return Status::OK();
}

void RangeQuery::search(roaring::Roaring& roaring) {
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