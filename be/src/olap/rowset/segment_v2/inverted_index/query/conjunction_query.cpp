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

#include "olap/collection_statistics.h"
#include "olap/rowset/segment_v2/inverted_index/query/query_helper.h"
#include "olap/rowset/segment_v2/inverted_index/util/string_helper.h"

namespace doris::segment_v2 {

ConjunctionQuery::ConjunctionQuery(SearcherPtr searcher, IndexQueryContextPtr context)
        : _searcher(std::move(searcher)), _context(std::move(context)) {
    _index_version = _searcher->getReader()->getIndexVersion();
    _conjunction_ratio =
            _context->runtime_state->query_options().inverted_index_conjunction_opt_threshold;
}

ConjunctionQuery::ConjunctionQuery(SearcherPtr searcher, IndexQueryContextPtr context,
                                   bool is_similarity)
        : ConjunctionQuery(std::move(searcher), std::move(context)) {
    _is_similarity = is_similarity;
}

void ConjunctionQuery::add(const InvertedIndexQueryInfo& query_info) {
    if (query_info.term_infos.empty()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "term_infos cannot be empty");
    }

    for (const auto& term_info : query_info.term_infos) {
        if (term_info.is_multi_terms()) {
            throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR, "Not supported yet.");
        }

        auto iter = TermIterator::create(_context->io_ctx, _searcher->getReader(),
                                         query_info.field_name, term_info.get_single_term());
        _iterators.emplace_back(std::move(iter));
    }

    std::sort(_iterators.begin(), _iterators.end(), [](const TermIterPtr& a, const TermIterPtr& b) {
        return a->doc_freq() < b->doc_freq();
    });

    if (_iterators.size() == 1) {
        _lead1 = _iterators[0];
    } else {
        _lead1 = _iterators[0];
        _lead2 = _iterators[1];
        for (size_t i = 2; i < _iterators.size(); i++) {
            _others.emplace_back(_iterators[i]);
        }
    }

    if (_index_version == IndexVersion::kV1 && _iterators.size() >= 2) {
        int32_t little = _iterators[0]->doc_freq();
        int32_t big = _iterators[_iterators.size() - 1]->doc_freq();
        if (little == 0 || (big / little) > _conjunction_ratio) {
            _use_skip = true;
        }
    }

    if (_is_similarity && _context->collection_similarity) {
        for (const auto& iter : _iterators) {
            auto similarity = std::make_unique<BM25Similarity>();
            similarity->for_one_term(_context, query_info.field_name, iter->term());
            _similarities.emplace_back(std::move(similarity));
        }
    }
}

void ConjunctionQuery::pre_search(const InvertedIndexQueryInfo& query_info) {
    if (query_info.term_infos.empty()) {
        return;
    }

    QueryHelper::query_statistics(_context, _searcher, query_info.field_name,
                                  query_info.term_infos);
}

void ConjunctionQuery::search(roaring::Roaring& roaring) {
    if (_lead1 == nullptr) {
        return;
    }

    if (!_use_skip) {
        search_by_bitmap(roaring);
        return;
    }

    search_by_skiplist(roaring);
}

void ConjunctionQuery::search_by_bitmap(roaring::Roaring& roaring) {
    // can get a term of all doc_id
    auto func = [this, &roaring](size_t i, const TermIterPtr& iter, bool first) {
        roaring::Roaring result;
        DocRange doc_range;
        while (iter->read_range(&doc_range)) {
            if (doc_range.type_ == DocRangeType::kMany) {
                result.addMany(doc_range.doc_many_size_, doc_range.doc_many->data());

                if (_is_similarity && _context->collection_similarity) {
                    QueryHelper::collect_many(_context, _similarities[i], doc_range, roaring,
                                              first);
                }
            } else {
                result.addRange(doc_range.doc_range.first, doc_range.doc_range.second);

                if (_is_similarity && _context->collection_similarity) {
                    QueryHelper::collect_range(_context, _similarities[i], doc_range, roaring,
                                               first);
                }
            }
        }
        if (first) {
            roaring.swap(result);
        } else {
            roaring &= result;
        }
    };

    for (size_t i = 0; i < _iterators.size(); i++) {
        func(i, _iterators[i], i == 0);
    }
}

void ConjunctionQuery::search_by_skiplist(roaring::Roaring& roaring) {
    int32_t doc = 0;
    while ((doc = do_next(_lead1->next_doc())) != INT32_MAX) {
        roaring.add(doc);

        if (_is_similarity && _context->collection_similarity) {
            for (size_t i = 0; i < _iterators.size(); i++) {
                auto freq = _iterators[i]->freq();
                auto doc_length = _iterators[i]->norm();
                auto score = _similarities[i]->score(freq, doc_length);
                _context->collection_similarity->collect(doc, score);
            }
        }
    }
}

int32_t ConjunctionQuery::do_next(int32_t doc) {
    while (true) {
        assert(doc == _lead1->doc_id());

        // the skip list is used to find the two smallest inverted lists
        int32_t next2 = _lead2->advance(doc);
        if (next2 != doc) {
            doc = _lead1->advance(next2);
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

            if (other->doc_id() < doc) {
                int32_t next = other->advance(doc);
                if (next > doc) {
                    doc = _lead1->advance(next);
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

} // namespace doris::segment_v2