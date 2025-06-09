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

#include "prefix_query.h"

namespace doris::segment_v2 {

PrefixQuery::PrefixQuery(const std::shared_ptr<lucene::search::IndexSearcher>& searcher,
                         const TQueryOptions& query_options, const io::IOContext* io_ctx)
        : _searcher(searcher), _io_ctx(io_ctx) {}

void PrefixQuery::add(const std::wstring& field_name, const std::vector<std::wstring>& terms) {
    if (terms.empty()) {
        _CLTHROWA(CL_ERR_IllegalArgument, "PhraseQuery::add: terms empty");
    }

    std::vector<TermPositionIterator> subs;
    for (const auto& ws_term : terms) {
        auto* term_doc = TermPositionIterator::ensure_term_position(_io_ctx, _searcher->getReader(),
                                                                    field_name, ws_term);
        subs.emplace_back(term_doc);
    }
    _lead1 = std::make_shared<UnionTermIterator<TermPositionIterator>>(subs);
}

void PrefixQuery::search(roaring::Roaring& roaring) {
    while (_lead1->next_doc() != INT32_MAX) {
        roaring.add(_lead1->doc_id());
    }
}

void PrefixQuery::get_prefix_terms(IndexReader* reader, const std::wstring& field_name,
                                   const std::string& prefix,
                                   std::vector<std::wstring>& prefix_terms,
                                   int32_t max_expansions) {
    std::wstring ws_prefix = StringUtil::string_to_wstring(prefix);

    Term* prefix_term = _CLNEW Term(field_name.c_str(), ws_prefix.c_str());
    TermEnum* enumerator = reader->terms(prefix_term, _io_ctx);

    int32_t count = 0;
    Term* lastTerm = nullptr;
    try {
        const TCHAR* prefixText = prefix_term->text();
        const TCHAR* prefixField = prefix_term->field();
        const TCHAR* tmp = nullptr;
        size_t i = 0;
        size_t prefixLen = prefix_term->textLength();
        do {
            lastTerm = enumerator->term();
            if (lastTerm != nullptr && lastTerm->field() == prefixField) {
                size_t termLen = lastTerm->textLength();
                if (prefixLen > termLen) {
                    break;
                }

                tmp = lastTerm->text();

                for (i = prefixLen - 1; i != -1; --i) {
                    if (tmp[i] != prefixText[i]) {
                        tmp = nullptr;
                        break;
                    }
                }
                if (tmp == nullptr) {
                    break;
                }

                if (max_expansions > 0 && count >= max_expansions) {
                    break;
                }

                prefix_terms.emplace_back(tmp, termLen);

                count++;
            } else {
                break;
            }
            _CLDECDELETE(lastTerm);
        } while (enumerator->next());
    }
    _CLFINALLY({
        enumerator->close();
        _CLDELETE(enumerator);
        _CLDECDELETE(lastTerm);
        _CLDECDELETE(prefix_term);
    });
}

} // namespace doris::segment_v2