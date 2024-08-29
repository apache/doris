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

#include <charconv>

namespace doris::segment_v2 {

template <typename Derived>
bool PhraseMatcherBase<Derived>::matches(int32_t doc) {
    reset(doc);
    return static_cast<Derived*>(this)->next_match();
}

template <typename Derived>
void PhraseMatcherBase<Derived>::reset(int32_t doc) {
    for (PostingsAndPosition& posting : _postings) {
        if (posting._postings.docID() != doc) {
            posting._postings.advance(doc);
        }
        posting._freq = posting._postings.freq();
        posting._pos = -1;
        posting._upTo = 0;
    }
}

template <typename Derived>
bool PhraseMatcherBase<Derived>::advance_position(PostingsAndPosition& posting, int32_t target) {
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

bool ExactPhraseMatcher::next_match() {
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

bool OrderedSloppyPhraseMatcher::next_match() {
    PostingsAndPosition* prev_posting = _postings.data();
    while (prev_posting->_upTo < prev_posting->_freq) {
        prev_posting->_pos = prev_posting->_postings.nextPosition();
        prev_posting->_upTo += 1;
        if (stretch_to_order(prev_posting) && _match_width <= _allowed_slop) {
            return true;
        }
    }
    return false;
}

bool OrderedSloppyPhraseMatcher::stretch_to_order(PostingsAndPosition* prev_posting) {
    _match_width = 0;
    for (size_t i = 1; i < _postings.size(); i++) {
        PostingsAndPosition& posting = _postings[i];
        if (!advance_position(posting, prev_posting->_pos + 1)) {
            return false;
        }
        _match_width += (posting._pos - (prev_posting->_pos + 1));
        prev_posting = &posting;
    }
    return true;
}

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

void PhraseQuery::add(const InvertedIndexQueryInfo& query_info) {
    if (query_info.terms.empty()) {
        _CLTHROWA(CL_ERR_IllegalArgument, "PhraseQuery::add: terms empty");
    }

    _slop = query_info.slop;
    if (_slop == 0 || query_info.ordered) {
        // Logic for no slop query and ordered phrase query
        add(query_info.field_name, query_info.terms);
    } else {
        // Simple slop query follows the default phrase query algorithm
        auto query = std::make_unique<CL_NS(search)::PhraseQuery>();
        for (const auto& term : query_info.terms) {
            std::wstring ws_term = StringUtil::string_to_wstring(term);
            auto* t = _CLNEW lucene::index::Term(query_info.field_name.c_str(), ws_term.c_str());
            query->add(t);
            _CLDECDELETE(t);
        }
        query->setSlop(_slop);
        _matcher = std::move(query);
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
    auto ensureTermPosition = [this, &iterators, &field_name](const std::string& term) {
        std::wstring ws_term = StringUtil::string_to_wstring(term);
        Term* t = _CLNEW Term(field_name.c_str(), ws_term.c_str());
        _terms.push_back(t);
        TermPositions* term_pos = _searcher->getReader()->termPositions(t);
        _term_docs.push_back(term_pos);
        iterators.emplace_back(term_pos);
        return term_pos;
    };

    if (_slop == 0) {
        ExactPhraseMatcher matcher;
        for (size_t i = 0; i < terms.size(); i++) {
            const auto& term = terms[i];
            auto* term_pos = ensureTermPosition(term);
            matcher._postings.emplace_back(term_pos, i);
        }
        _matcher = matcher;
    } else {
        OrderedSloppyPhraseMatcher matcher;
        for (size_t i = 0; i < terms.size(); i++) {
            const auto& term = terms[i];
            auto* term_pos = ensureTermPosition(term);
            matcher._postings.emplace_back(term_pos, i);
        }
        matcher._allowed_slop = _slop;
        _matcher = matcher;
    }

    std::sort(iterators.begin(), iterators.end(), [](const TermIterator& a, const TermIterator& b) {
        return a.docFreq() < b.docFreq();
    });

    _lead1 = iterators[0];
    _lead2 = iterators[1];
    for (int32_t i = 2; i < iterators.size(); i++) {
        _others.push_back(iterators[i]);
    }
}

void PhraseQuery::search(roaring::Roaring& roaring) {
    if (std::holds_alternative<PhraseQueryPtr>(_matcher)) {
        _searcher->_search(
                std::get<PhraseQueryPtr>(_matcher).get(),
                [&roaring](const int32_t docid, const float_t /*score*/) { roaring.add(docid); });
    } else {
        if (_lead1.isEmpty()) {
            return;
        }
        if (_lead2.isEmpty()) {
            search_by_bitmap(roaring);
            return;
        }
        search_by_skiplist(roaring);
    }
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
        if (matches(doc)) {
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

bool PhraseQuery::matches(int32_t doc) {
    return std::visit(
            [&doc](auto&& m) -> bool {
                using T = std::decay_t<decltype(m)>;
                if constexpr (std::is_same_v<T, PhraseQueryPtr>) {
                    _CLTHROWA(CL_ERR_IllegalArgument,
                              "PhraseQueryPtr does not support matches function");
                } else {
                    return m.matches(doc);
                }
            },
            _matcher);
}

void PhraseQuery::parser_slop(std::string& query, InvertedIndexQueryInfo& query_info) {
    auto is_digits = [](const std::string_view& str) {
        return std::all_of(str.begin(), str.end(), [](unsigned char c) { return std::isdigit(c); });
    };

    size_t last_space_pos = query.find_last_of(' ');
    if (last_space_pos != std::string::npos) {
        size_t tilde_pos = last_space_pos + 1;
        if (tilde_pos < query.size() - 1 && query[tilde_pos] == '~') {
            size_t slop_pos = tilde_pos + 1;
            std::string_view slop_str(query.data() + slop_pos, query.size() - slop_pos);
            do {
                if (slop_str.empty()) {
                    break;
                }

                bool ordered = false;
                if (slop_str.size() == 1) {
                    if (!std::isdigit(slop_str[0])) {
                        break;
                    }
                } else {
                    if (slop_str.back() == '+') {
                        ordered = true;
                        slop_str.remove_suffix(1);
                    }
                }

                if (is_digits(slop_str)) {
                    auto result =
                            std::from_chars(slop_str.begin(), slop_str.end(), query_info.slop);
                    if (result.ec != std::errc()) {
                        break;
                    }
                    query_info.ordered = ordered;
                    query = query.substr(0, last_space_pos);
                }
            } while (false);
        }
    }
}

template class PhraseMatcherBase<ExactPhraseMatcher>;
template class PhraseMatcherBase<OrderedSloppyPhraseMatcher>;

} // namespace doris::segment_v2