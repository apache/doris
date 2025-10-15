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
#include "olap/rowset/segment_v2/inverted_index/query/query.h"
#include "olap/rowset/segment_v2/inverted_index/query/query_helper.h"
#include "olap/rowset/segment_v2/inverted_index/util/term_position_iterator.h"

namespace doris::segment_v2 {

PhraseQuery::PhraseQuery(SearcherPtr searcher, IndexQueryContextPtr context)
        : _searcher(std::move(searcher)),
          _context(std::move(context)),
          _term_query(_searcher, _context) {}

void PhraseQuery::add(const InvertedIndexQueryInfo& query_info) {
    if (query_info.term_infos.empty()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "term_infos cannot be empty");
    }

    if (query_info.term_infos.size() == 1) {
        _term_query.add(query_info);
        return;
    }

    bool is_similarity = _context->collection_similarity && query_info.is_similarity_score;

    if (query_info.slop == 0) {
        init_exact_phrase_matcher(query_info, is_similarity);
    } else if (!query_info.ordered) {
        init_sloppy_phrase_matcher(query_info, is_similarity);
    } else {
        init_ordered_sloppy_phrase_matcher(query_info, is_similarity);
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

    init_similarities(query_info.field_name, is_similarity);
}

void PhraseQuery::init_exact_phrase_matcher(const InvertedIndexQueryInfo& query_info,
                                            bool is_similarity) {
    std::vector<PostingsAndPosition> postings;
    for (size_t i = 0; i < query_info.term_infos.size(); i++) {
        const auto& term_info = query_info.term_infos[i];
        if (term_info.is_single_term()) {
            const auto& term = term_info.get_single_term();
            auto iter = TermPositionsIterator::create(_context->io_ctx, is_similarity,
                                                      _searcher->getReader(), query_info.field_name,
                                                      term);
            _iterators.emplace_back(iter);
            postings.emplace_back(iter, i);
        } else {
            std::vector<TermPositionsIterPtr> subs;
            for (const auto& term : term_info.get_multi_terms()) {
                auto iter = TermPositionsIterator::create(_context->io_ctx, is_similarity,
                                                          _searcher->getReader(),
                                                          query_info.field_name, term);
                subs.emplace_back(iter);
            }
            auto iter = std::make_shared<UnionTermIterator<TermPositionsIterator>>(std::move(subs));
            _iterators.emplace_back(iter);
            postings.emplace_back(iter, i);
        }
    }
    ExactPhraseMatcher matcher(std::move(postings));
    _matchers.emplace_back(std::move(matcher));
}

void PhraseQuery::init_sloppy_phrase_matcher(const InvertedIndexQueryInfo& query_info,
                                             bool is_similarity) {
    std::vector<PostingsAndFreq> postings;
    for (size_t i = 0; i < query_info.term_infos.size(); i++) {
        const auto& term_info = query_info.term_infos[i];
        if (term_info.is_multi_terms()) {
            throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR, "Not supported yet.");
        }

        const auto& term = term_info.get_single_term();
        auto iter =
                TermPositionsIterator::create(_context->io_ctx, is_similarity,
                                              _searcher->getReader(), query_info.field_name, term);
        _iterators.emplace_back(iter);
        postings.emplace_back(iter, i, std::vector<std::string> {term});
    }
    SloppyPhraseMatcher matcher(postings, query_info.slop);
    _matchers.emplace_back(std::move(matcher));
}

void PhraseQuery::init_ordered_sloppy_phrase_matcher(const InvertedIndexQueryInfo& query_info,
                                                     bool is_similarity) {
    std::vector<PostingsAndPosition> postings;
    for (size_t i = 0; i < query_info.term_infos.size(); i++) {
        const auto& term_info = query_info.term_infos[i];
        if (term_info.is_multi_terms()) {
            throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR, "Not supported yet.");
        }

        auto iter = TermPositionsIterator::create(_context->io_ctx, is_similarity,
                                                  _searcher->getReader(), query_info.field_name,
                                                  term_info.get_single_term());
        _iterators.emplace_back(iter);
        postings.emplace_back(iter, i);
    }
    OrderedSloppyPhraseMatcher single_matcher(std::move(postings), query_info.slop);
    _matchers.emplace_back(std::move(single_matcher));
}

void PhraseQuery::init_similarities(const std::wstring& field_name, bool is_similarity) {
    // TODO: Current implementation - computes BM25 scores separately for each term
    // Note: This approach is suitable for TermQuery but does not conform to BM25 specification for PhraseQuery
    // BM25 phrase query specification requires:
    //   idf component = sum of idf values for all terms
    //   tf component = phrase frequency (number of times entire phrase appears in document)
    //   doc_length = total document length
    //
    // Future optimization direction:
    //   1. Shift to unified phrase scoring: calculate sum of idf for all terms as combined idf
    //   2. Use phrase frequency instead of individual term frequencies
    //   3. Maintain document length normalization
    //   4. Refactor to create a single Similarity object handling the entire phrase
    if (is_similarity) {
        _similarities.resize(_iterators.size());
        for (size_t i = 0; i < _iterators.size(); i++) {
            const auto& iter = _iterators[i];
            if (std::holds_alternative<TermPositionsIterPtr>(iter)) {
                const auto& term_iter = std::get<TermPositionsIterPtr>(iter);
                auto similarity = std::make_unique<BM25Similarity>();
                similarity->for_one_term(_context, field_name, term_iter->term());
                _similarities[i] = std::move(similarity);
            }
        }
    }
}

void PhraseQuery::search(roaring::Roaring& roaring) {
    if (_lead1 == nullptr) {
        _term_query.search(roaring);
        return;
    }

    search_by_skiplist(roaring);
}

void PhraseQuery::search_by_skiplist(roaring::Roaring& roaring) {
    int32_t doc = 0;
    while ((doc = do_next(visit_node(*_lead1, NextDoc {}))) != INT32_MAX) {
        if (!matches(doc)) {
            continue;
        }
        roaring.add(doc);

        if (!_similarities.empty()) {
            QueryHelper::collect(_context, _similarities, _iterators, doc);
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

void PhraseQuery::parser_info(OlapReaderStatistics* stats, std::string& query,
                              const std::map<std::string, std::string>& properties,
                              InvertedIndexQueryInfo& query_info) {
    parser_slop(query, query_info);
    {
        SCOPED_RAW_TIMER(&stats->inverted_index_analyzer_timer);
        query_info.term_infos =
                inverted_index::InvertedIndexAnalyzer::get_analyse_result(query, properties);
    }
}

} // namespace doris::segment_v2