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

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/split.hpp>
#include <charconv>
#include <vector>

#include "CLucene/index/Terms.h"
#include "olap/rowset/segment_v2/inverted_index/analyzer/analyzer.h"

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

void PhraseQuery::add(const InvertedIndexQueryInfo& query_info) {
    if (query_info.terms.empty()) {
        _CLTHROWA(CL_ERR_IllegalArgument, "PhraseQuery::add: terms empty");
    }

    if (query_info.terms.size() == 1) {
        _lead1 = ensure_term_position(query_info.terms[0], query_info.field_name);
        return;
    }

    if (query_info.slop == 0) {
        init_exact_phrase_matcher(query_info);
    } else if (!query_info.ordered) {
        init_sloppy_phrase_matcher(query_info);
    } else {
        init_ordered_sloppy_phrase_matcher(query_info);
    }

    std::sort(
            _iterators.begin(), _iterators.end(),
            [](const TermIterator& a, const TermIterator& b) { return a.docFreq() < b.docFreq(); });

    _lead1 = _iterators[0];
    _lead2 = _iterators[1];
    for (int32_t i = 2; i < _iterators.size(); i++) {
        _others.push_back(_iterators[i]);
    }
}

TermPositions* PhraseQuery::ensure_term_position(const std::string& term,
                                                 const std::wstring& field_name,
                                                 bool is_save_iter) {
    std::wstring ws_term = StringUtil::string_to_wstring(term);
    Term* t = _CLNEW Term(field_name.c_str(), ws_term.c_str());
    _terms.push_back(t);
    TermPositions* term_pos = _searcher->getReader()->termPositions(t);
    _term_docs.push_back(term_pos);
    if (is_save_iter) {
        _iterators.emplace_back(term_pos);
    }
    return term_pos;
}

void PhraseQuery::init_exact_phrase_matcher(const InvertedIndexQueryInfo& query_info) {
    std::vector<PostingsAndPosition> postings;
    for (size_t i = 0; i < query_info.terms.size(); i++) {
        const auto& term = query_info.terms[i];
        auto* term_pos = ensure_term_position(term, query_info.field_name);
        postings.emplace_back(term_pos, i);
    }
    ExactPhraseMatcher matcher(postings);
    _matchers.emplace_back(std::move(matcher));
}

void PhraseQuery::init_sloppy_phrase_matcher(const InvertedIndexQueryInfo& query_info) {
    std::vector<PostingsAndFreq> postings;
    for (size_t i = 0; i < query_info.terms.size(); i++) {
        const auto& term = query_info.terms[i];
        auto* term_pos = ensure_term_position(term, query_info.field_name);
        postings.emplace_back(term_pos, i, std::vector<std::string> {term});
    }
    SloppyPhraseMatcher matcher(postings, query_info.slop);
    _matchers.emplace_back(std::move(matcher));
}

void PhraseQuery::init_ordered_sloppy_phrase_matcher(const InvertedIndexQueryInfo& query_info) {
    {
        std::vector<PostingsAndPosition> postings;
        for (size_t i = 0; i < query_info.terms.size(); i++) {
            const auto& term = query_info.terms[i];
            auto* term_pos = ensure_term_position(term, query_info.field_name);
            postings.emplace_back(term_pos, i);
        }
        OrderedSloppyPhraseMatcher single_matcher(postings, query_info.slop);
        _matchers.emplace_back(std::move(single_matcher));
    }
    {
        for (auto& terms : _additional_terms) {
            std::vector<PostingsAndPosition> postings;
            for (size_t i = 0; i < terms.size(); i++) {
                const auto& term = terms[i];
                auto* term_pos = ensure_term_position(term, query_info.field_name, false);
                postings.emplace_back(term_pos, i);
            }
            ExactPhraseMatcher single_matcher(postings);
            _matchers.emplace_back(std::move(single_matcher));
        }
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
    return std::ranges::all_of(_matchers, [&doc](auto&& matcher) {
        return std::visit([&doc](auto&& m) -> bool { return m.matches(doc); }, matcher);
    });
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

void PhraseQuery::parser_info(std::string& query, const std::string& field_name,
                              InvertedIndexQueryType query_type,
                              const std::map<std::string, std::string>& properties,
                              InvertedIndexQueryInfo& query_info, bool sequential_opt) {
    parser_slop(query, query_info);
    query_info.terms = inverted_index::InvertedIndexAnalyzer::get_analyse_result(
            query, field_name, query_type, properties);
    if (sequential_opt && query_info.ordered) {
        std::vector<std::string> t_querys;
        boost::split(t_querys, query, boost::algorithm::is_any_of(" "));
        for (auto& t_query : t_querys) {
            auto terms = inverted_index::InvertedIndexAnalyzer::get_analyse_result(
                    t_query, field_name, query_type, properties);
            query_info.additional_terms.emplace_back(std::move(terms));
        }
    }
}

} // namespace doris::segment_v2