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

#include "phrase_query.h"

namespace doris::segment_v2 {

PhraseQuery::PhraseQuery(const std::shared_ptr<lucene::search::IndexSearcher>& searcher,
                         const TQueryOptions& query_options)
        : _searcher(searcher) {}

PhraseQuery::~PhraseQuery() {
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

void PhraseQuery::add(const std::wstring& field_name, const std::vector<std::string>& terms) {
    if (terms.empty()) {
        _CLTHROWA(CL_ERR_IllegalArgument, "PhraseQuery::add: terms empty");
    }

    if (terms.size() == 1) {
        std::wstring ws_term = StringUtil::string_to_wstring(terms[0]);
        Term* t = _CLNEW Term(field_name.c_str(), ws_term.c_str());
        _terms.push_back(t);
        TermDocs* term_doc = _searcher->getReader()->termDocs(t);
        _term_docs.push_back(term_doc);
        _lead1 = TermIterator(term_doc);
        return;
    }

    std::vector<TermIterator> iterators;
    for (size_t i = 0; i < terms.size(); i++) {
        std::wstring ws_term = StringUtil::string_to_wstring(terms[i]);
        Term* t = _CLNEW Term(field_name.c_str(), ws_term.c_str());
        _terms.push_back(t);
        TermPositions* term_pos = _searcher->getReader()->termPositions(t);
        _term_docs.push_back(term_pos);
        iterators.emplace_back(term_pos);
        _postings.emplace_back(term_pos, i);
    }

    std::sort(iterators.begin(), iterators.end(), [](const TermIterator& a, const TermIterator& b) {
        return a.docFreq() < b.docFreq();
    });

    _lead1 = iterators[0];
    _lead2 = iterators[1];
    for (int32_t i = 2; i < _terms.size(); i++) {
        _others.push_back(iterators[i]);
    }
}

void PhraseQuery::search(roaring::Roaring& roaring) {
    if (_lead1.isEmpty()) {
        return;
    }
    if (_lead2.isEmpty()) {
        search_by_bitmap(roaring);
        return;
    }
    search_by_skiplist(roaring);
}

void PhraseQuery::search_by_bitmap(roaring::Roaring& roaring) {
    DocRange doc_range;
    while (_lead1.readRange(&doc_range)) {
        if (doc_range.type_ == DocRangeType::kMany) {
            roaring.addMany(doc_range.doc_many_size_, doc_range.doc_many->data());
        } else {
            roaring.addRange(doc_range.doc_range.first, doc_range.doc_range.second);
        }
    }
}

void PhraseQuery::search_by_skiplist(roaring::Roaring& roaring) {
    int32_t doc = 0;
    while ((doc = do_next(_lead1.nextDoc())) != INT32_MAX) {
        reset();
        if (next_match()) {
            roaring.add(doc);
        }
    }
}

int32_t PhraseQuery::do_next(int32_t doc) {
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

bool PhraseQuery::next_match() {
    PostingsAndPosition& lead = _postings[0];
    if (lead._upTo < lead._freq) {
        lead._pos = lead._postings.nextPosition();
        lead._upTo += 1;
    } else {
        return false;
    }

    while (true) {
        int32_t phrasePos = lead._pos - lead._offset;

        bool advance_head = false;
        for (size_t j = 1; j < _postings.size(); ++j) {
            PostingsAndPosition& posting = _postings[j];
            int32_t expectedPos = phrasePos + posting._offset;
            // advance up to the same position as the lead
            if (!advance_position(posting, expectedPos)) {
                return false;
            }

            if (posting._pos != expectedPos) { // we advanced too far
                if (advance_position(lead, posting._pos - posting._offset + lead._offset)) {
                    advance_head = true;
                    break;
                } else {
                    return false;
                }
            }
        }
        if (advance_head) {
            continue;
        }

        return true;
    }

    return false;
}

bool PhraseQuery::advance_position(PostingsAndPosition& posting, int32_t target) {
    while (posting._pos < target) {
        if (posting._upTo == posting._freq) {
            return false;
        } else {
            posting._pos = posting._postings.nextPosition();
            posting._upTo += 1;
        }
    }
    return true;
}

void PhraseQuery::reset() {
    for (PostingsAndPosition& posting : _postings) {
        posting._freq = posting._postings.freq();
        posting._pos = -1;
        posting._upTo = 0;
    }
}

} // namespace doris::segment_v2