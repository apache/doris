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

#include "CLucene/index/Terms.h"
#include "olap/rowset/segment_v2/inverted_index/analyzer/analyzer.h"
#include "olap/rowset/segment_v2/inverted_index/util/term_position_iterator.h"

namespace doris::segment_v2 {

PhraseQuery::PhraseQuery(const std::shared_ptr<lucene::search::IndexSearcher>& searcher,
                         const TQueryOptions& query_options, const io::IOContext* io_ctx)
        : _searcher(searcher), _io_ctx(io_ctx) {}

void PhraseQuery::add(const InvertedIndexQueryInfo& query_info) {
    if (query_info.terms.empty()) {
        _CLTHROWA(CL_ERR_IllegalArgument, "PhraseQuery::add: terms empty");
    }

    if (query_info.terms.size() == 1) {
        auto* term_pos = TermPositionIterator::ensure_term_position(
                _io_ctx, _searcher->getReader(), query_info.field_name, query_info.terms[0]);
        _iterators.emplace_back(std::make_shared<TermPositionIterator>(term_pos));
        _lead1 = &_iterators.at(0);
        return;
    }

    if (query_info.slop == 0) {
        init_exact_phrase_matcher(query_info);
    } else if (!query_info.ordered) {
        init_sloppy_phrase_matcher(query_info);
    } else {
        init_ordered_sloppy_phrase_matcher(query_info);
    }

    std::sort(_iterators.begin(), _iterators.end(), [](const DISI& a, const DISI& b) {
        int64_t freq1 = visit_node(a, DocFreq {});
        int64_t freq2 = visit_node(b, DocFreq {});
        return freq1 < freq2;
    });

    _lead1 = &_iterators.at(0);
    _lead2 = &_iterators.at(1);
    for (int32_t i = 2; i < _iterators.size(); i++) {
        _others.emplace_back(&_iterators[i]);
    }
}

void PhraseQuery::add(const std::wstring& field_name,
                      const std::vector<std::vector<std::wstring>>& terms) {
    if (terms.size() < 2 || terms[0].empty() || terms[1].empty()) {
        _CLTHROWA(CL_ERR_IllegalArgument, "PhraseQuery::add: terms empty");
    }

    init_exact_phrase_matcher(field_name, terms);

    std::sort(_iterators.begin(), _iterators.end(), [](const DISI& a, const DISI& b) {
        int64_t freq1 = visit_node(a, DocFreq {});
        int64_t freq2 = visit_node(b, DocFreq {});
        return freq1 < freq2;
    });

    _lead1 = &_iterators.at(0);
    _lead2 = &_iterators.at(1);
    for (int32_t i = 2; i < _iterators.size(); i++) {
        _others.push_back(&_iterators[i]);
    }
}

void PhraseQuery::init_exact_phrase_matcher(const InvertedIndexQueryInfo& query_info) {
    std::vector<PostingsAndPosition> postings;
    for (size_t i = 0; i < query_info.terms.size(); i++) {
        const auto& term = query_info.terms[i];
        auto* term_pos = TermPositionIterator::ensure_term_position(_io_ctx, _searcher->getReader(),
                                                                    query_info.field_name, term);
        auto iter = std::make_shared<TermPositionIterator>(term_pos);
        _iterators.emplace_back(iter);
        postings.emplace_back(iter, i);
    }
    ExactPhraseMatcher matcher(std::move(postings));
    _matchers.emplace_back(std::move(matcher));
}

void PhraseQuery::init_exact_phrase_matcher(const std::wstring& field_name,
                                            const std::vector<std::vector<std::wstring>>& terms) {
    std::vector<PostingsAndPosition> postings;
    for (size_t i = 0; i < terms.size(); i++) {
        if (i < terms.size() - 1) {
            const auto& term = terms[i][0];
            auto* term_pos = TermPositionIterator::ensure_term_position(
                    _io_ctx, _searcher->getReader(), field_name, term);
            auto iter = std::make_shared<TermPositionIterator>(term_pos);
            _iterators.emplace_back(iter);
            postings.emplace_back(iter, i);
        } else {
            std::vector<TermPositionIterator> subs;
            for (const auto& term : terms[i]) {
                auto* term_pos = TermPositionIterator::ensure_term_position(
                        _io_ctx, _searcher->getReader(), field_name, term);
                subs.emplace_back(term_pos);
            }
            auto iter = std::make_shared<UnionTermIterator<TermPositionIterator>>(std::move(subs));
            _iterators.emplace_back(iter);
            postings.emplace_back(iter, i);
        }
    }
    ExactPhraseMatcher matcher(std::move(postings));
    _matchers.emplace_back(std::move(matcher));
}

void PhraseQuery::init_sloppy_phrase_matcher(const InvertedIndexQueryInfo& query_info) {
    std::vector<PostingsAndFreq> postings;
    for (size_t i = 0; i < query_info.terms.size(); i++) {
        const auto& term = query_info.terms[i];
        auto* term_pos = TermPositionIterator::ensure_term_position(_io_ctx, _searcher->getReader(),
                                                                    query_info.field_name, term);
        auto iter = std::make_shared<TermPositionIterator>(term_pos);
        _iterators.emplace_back(iter);
        postings.emplace_back(iter, i, std::vector<std::string> {term});
    }
    SloppyPhraseMatcher matcher(postings, query_info.slop);
    _matchers.emplace_back(std::move(matcher));
}

void PhraseQuery::init_ordered_sloppy_phrase_matcher(const InvertedIndexQueryInfo& query_info) {
    {
        std::vector<PostingsAndPosition> postings;
        for (size_t i = 0; i < query_info.terms.size(); i++) {
            const auto& term = query_info.terms[i];
            auto* term_pos = TermPositionIterator::ensure_term_position(
                    _io_ctx, _searcher->getReader(), query_info.field_name, term);
            auto iter = std::make_shared<TermPositionIterator>(term_pos);
            _iterators.emplace_back(iter);
            postings.emplace_back(iter, i);
        }
        OrderedSloppyPhraseMatcher single_matcher(std::move(postings), query_info.slop);
        _matchers.emplace_back(std::move(single_matcher));
    }
    {
        for (auto& terms : _additional_terms) {
            std::vector<PostingsAndPosition> postings;
            for (size_t i = 0; i < terms.size(); i++) {
                const auto& term = terms[i];
                auto* term_pos = TermPositionIterator::ensure_term_position(
                        _io_ctx, _searcher->getReader(), query_info.field_name, term);
                auto iter = std::make_shared<TermPositionIterator>(term_pos);
                postings.emplace_back(iter, i);
            }
            ExactPhraseMatcher single_matcher(std::move(postings));
            _matchers.emplace_back(std::move(single_matcher));
        }
    }
}

void PhraseQuery::search(roaring::Roaring& roaring) {
    if (_lead1 == nullptr) {
        return;
    }
    if (_lead2 == nullptr) {
        search_by_bitmap(roaring);
        return;
    }
    search_by_skiplist(roaring);
}

void PhraseQuery::search_by_bitmap(roaring::Roaring& roaring) {
    if (auto* term_iter = std::get_if<TermPosIterPtr>(_lead1)) {
        DocRange doc_range;
        while ((*term_iter)->read_range(&doc_range)) {
            if (doc_range.type_ == DocRangeType::kMany) {
                roaring.addMany(doc_range.doc_many_size_, doc_range.doc_many->data());
            } else {
                roaring.addRange(doc_range.doc_range.first, doc_range.doc_range.second);
            }
        }
    }
}

void PhraseQuery::search_by_skiplist(roaring::Roaring& roaring) {
    int32_t doc = 0;
    while ((doc = do_next(visit_node(*_lead1, NextDoc {}))) != INT32_MAX) {
        if (matches(doc)) {
            roaring.add(doc);
        }
    }
}

int32_t PhraseQuery::do_next(int32_t doc) {
    while (true) {
        assert(doc == visit_node(*_lead1, DocID {}));

        // the skip list is used to find the two smallest inverted lists
        int32_t next2 = visit_node(*_lead2, Advance {}, doc);
        if (next2 != doc) {
            doc = visit_node(*_lead1, Advance {}, next2);
            if (next2 != doc) {
                continue;
            }
        }

        // if both lead1 and lead2 exist, use skip list to lookup other inverted indexes
        bool advance_head = false;
        for (auto& other : _others) {
            if (other == nullptr) {
                continue;
            }

            if (visit_node(*other, DocID {}) < doc) {
                int32_t next = visit_node(*other, Advance {}, doc);
                if (next > doc) {
                    doc = visit_node(*_lead1, Advance {}, next);
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
            if (terms.size() >= 2) {
                query_info.additional_terms.emplace_back(std::move(terms));
            }
        }
    }
}

} // namespace doris::segment_v2