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

#include "conjunction_query.h"

#include <cstdint>

namespace doris {

ConjunctionQuery::ConjunctionQuery(IndexReader* reader)
        : _reader(reader), _index_version(reader->getIndexVersion()) {}

ConjunctionQuery::~ConjunctionQuery() {
    for (auto& term_doc : _term_docs) {
        if (term_doc) {
            _CLDELETE(term_doc);
        }
    }
    for (auto& term : _terms) {
        if (term) {
            _CLDELETE(term);
        }
    }
}

void ConjunctionQuery::add(const std::wstring& field_name, const std::vector<std::string>& terms) {
    if (terms.empty()) {
        _CLTHROWA(CL_ERR_IllegalArgument, "ConjunctionQuery::add: terms empty");
    }

    std::vector<TermIterator> iterators;
    for (const auto& term : terms) {
        std::wstring ws_term = StringUtil::string_to_wstring(term);
        Term* t = _CLNEW Term(field_name.c_str(), ws_term.c_str());
        _terms.push_back(t);
        TermDocs* term_doc = _reader->termDocs(t);
        _term_docs.push_back(term_doc);
        iterators.emplace_back(term_doc);
    }

    std::sort(iterators.begin(), iterators.end(), [](const TermIterator& a, const TermIterator& b) {
        return a.docFreq() < b.docFreq();
    });

    if (iterators.size() == 1) {
        _lead1 = iterators[0];
    } else {
        _lead1 = iterators[0];
        _lead2 = iterators[1];
        for (int32_t i = 2; i < _terms.size(); i++) {
            _others.push_back(iterators[i]);
        }
    }

    if (_index_version == IndexVersion::kV1 && iterators.size() >= 2) {
        int32_t little = iterators[0].docFreq();
        int32_t big = iterators[iterators.size() - 1].docFreq();
        if (little == 0 || (big / little) > _conjunction_ratio) {
            _use_skip = true;
        }
    }
}

void ConjunctionQuery::search(roaring::Roaring& roaring) {
    if (_lead1.isEmpty()) {
        return;
    }

    if (!_use_skip) {
        search_by_bitmap(roaring);
        return;
    }

    search_by_skiplist(roaring);
}

void ConjunctionQuery::search_by_bitmap(roaring::Roaring& roaring) {
    // can get a term of all docid
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
            roaring &= result;
        }
    };

    // fill the bitmap for the first time
    func(_lead1, true);

    // the second inverted list may be empty
    if (!_lead2.isEmpty()) {
        func(_lead2, false);
    }

    // The inverted index iterators contained in the _others array must not be empty
    for (auto& other : _others) {
        func(other, false);
    }
}

void ConjunctionQuery::search_by_skiplist(roaring::Roaring& roaring) {
    int32_t doc = 0;
    int32_t first_doc = _lead1.nextDoc();
    while ((doc = do_next(first_doc)) != INT32_MAX) {
        roaring.add(doc);
    }
}

int32_t ConjunctionQuery::do_next(int32_t doc) {
    while (true) {
        assert(doc == _lead1.docID());

        // the skip list is used to find the two smallest inverted lists
        int32_t next2 = _lead2.advance(doc);
        if (next2 != doc) {
            doc = _lead1.advance(next2);
            if (next2 != doc) {
                continue;
            }
        }

        // if both lead1 and lead2 exist, use skip list to lookup other inverted indexes
        bool advance_head = false;
        for (auto& other : _others) {
            if (other.isEmpty()) {
                continue;
            }

            if (other.docID() < doc) {
                int32_t next = other.advance(doc);
                if (next > doc) {
                    doc = _lead1.advance(next);
                    advance_head = true;
                    break;
                }
            }
        }
        if (advance_head) {
            continue;
        }

        return doc;
    }
}

} // namespace doris