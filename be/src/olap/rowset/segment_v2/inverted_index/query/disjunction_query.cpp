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

#include "olap/rowset/segment_v2/inverted_index/query/query_helper.h"

namespace doris::segment_v2 {

DisjunctionQuery::DisjunctionQuery(SearcherPtr searcher, IndexQueryContextPtr context)
        : _searcher(std::move(searcher)), _context(std::move(context)) {}

void DisjunctionQuery::add(const InvertedIndexQueryInfo& query_info) {
    if (query_info.term_infos.empty()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "term_infos cannot be empty");
    }

    bool is_similarity = _context->collection_similarity && query_info.is_similarity_score;

    _field_name = query_info.field_name;
    _iterators.resize(query_info.term_infos.size());
    for (size_t i = 0; i < query_info.term_infos.size(); i++) {
        const auto& term_info = query_info.term_infos[i];
        if (term_info.is_single_term()) {
            if (query_info.use_mock_iter) {
                auto iter = std::make_shared<MockIterator>();
                iter->set_postings({{0, {0}}, {1, {0}}, {3, {0}}, {5, {0}}});
                _iterators[i].emplace_back(iter);
            } else {
                auto iter = TermIterator::create(_context->io_ctx, is_similarity,
                                                 _searcher->getReader(), query_info.field_name,
                                                 term_info.get_single_term());
                _iterators[i].emplace_back(iter);
            }
        } else {
            for (const auto& term : term_info.get_multi_terms()) {
                if (query_info.use_mock_iter) {
                    auto iter = std::make_shared<MockIterator>();
                    iter->set_postings({{0, {0}}, {1, {0}}, {3, {0}}, {5, {0}}});
                    _iterators[i].emplace_back(iter);
                } else {
                    auto iter = TermIterator::create(_context->io_ctx, is_similarity,
                                                     _searcher->getReader(), query_info.field_name,
                                                     term);
                    _iterators[i].emplace_back(iter);
                }
            }
        }
    }

    if (is_similarity) {
        for (const auto& iters : _iterators) {
            if (iters.size() > 1) {
                throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                                "Scoring is not supported for this query.");
            }
            auto similarity = std::make_unique<BM25Similarity>();
            similarity->for_one_term(_context, _field_name, iters[0]->term());
            _similarities.emplace_back(std::move(similarity));
        }
    }
}

void DisjunctionQuery::search(roaring::Roaring& roaring) {
    auto func = [this, &roaring](size_t i, const TermIterPtr& iter, bool first) {
        DocRange doc_range;
        roaring::Roaring result;
        while (iter->read_range(&doc_range)) {
            if (doc_range.type_ == DocRangeType::kMany) {
                result.addMany(doc_range.doc_many_size_, doc_range.doc_many->data());

                if (!_similarities.empty()) {
                    QueryHelper::collect_many(_context, _similarities[i], doc_range);
                }
            } else {
                result.addRange(doc_range.doc_range.first, doc_range.doc_range.second);

                if (!_similarities.empty()) {
                    QueryHelper::collect_range(_context, _similarities[i], doc_range);
                }
            }
        }

        if (first) {
            roaring.swap(result);
        } else {
            roaring |= result;
        }
    };

    for (size_t i = 0; i < _iterators.size(); i++) {
        if (_iterators[i].size() == 1) {
            func(i, _iterators[i][0], i == 0);
        } else if (_iterators[i].size() > 1) {
            for (size_t j = 0; j < _iterators[i].size(); j++) {
                func(i, _iterators[i][j], (i == 0 && j == 0));
            }
        }
    }
}

} // namespace doris::segment_v2