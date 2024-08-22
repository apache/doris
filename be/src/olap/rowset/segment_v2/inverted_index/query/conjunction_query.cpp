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

namespace doris::segment_v2 {

ConjunctionQuery::ConjunctionQuery(const std::shared_ptr<lucene::search::IndexSearcher>& searcher,
                                   const TQueryOptions& query_options)
        : _searcher(searcher),
          _index_version(_searcher->getReader()->getIndexVersion()),
          _conjunction_ratio(query_options.inverted_index_conjunction_opt_threshold) {}

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

    std::vector<DocTermIterator> iterators;
    bool pre_searched = !_term_docs.empty();
    _field_name = field_name;

    for (size_t i = 0; i < terms.size(); i++) {
        const auto& term = terms[i];
        std::wstring ws_term = StringUtil::string_to_wstring(term);
        Term* t = _CLNEW Term(field_name.c_str(), ws_term.c_str());
        _terms.push_back(t);
        TermDocs* term_doc =
                pre_searched ? _term_docs[i] : _searcher->getReader()->termDocs(t);
        if (!pre_searched) {
            _term_docs.push_back(term_doc);
        }
        iterators.emplace_back(term_doc, term);
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

    index_stats::FullTextSimilarityCollector* full_text_similarity_collector = nullptr;

    if (_tablet_index_stats_collectors) {
        full_text_similarity_collector = dynamic_cast<index_stats::FullTextSimilarityCollector*>(
                _tablet_index_stats_collectors
                        ->get_tablet_index_stats_collector_by_name(
                                index_stats::FULL_TEXT_SIMILARITY_STATS_COLLECTOR)
                        .get());
        if (UNLIKELY(!full_text_similarity_collector)) {
            _CLTHROWA(CL_ERR_IllegalArgument,
                      "DisjunctionQuery::search: FullTextSimilarityCollector is null");
        }
    }

    if (!_use_skip) {
        search_by_bitmap(roaring, full_text_similarity_collector);
        return;
    }

    search_by_skiplist(roaring, full_text_similarity_collector);
}

void ConjunctionQuery::search_by_bitmap(roaring::Roaring& roaring,
    index_stats::FullTextSimilarityCollector* full_text_similarity_collector) {
    // can get a term of all docid
    auto func = [this, &roaring, &full_text_similarity_collector](
                        const DocTermIterator& term_docs, bool first) {
        roaring::Roaring result;
        DocRange doc_range;
        const float idf =
                _bm25_v_proj_col_iters
                        ? full_text_similarity_collector->get_or_calculate_idf(_field_name, term_docs.term())
                        : 0;
        const float avg_dl =
                _bm25_v_proj_col_iters
                        ? full_text_similarity_collector->get_or_calculate_avg_dl(_field_name)
                        : 0;

        while (term_docs.readRange(&doc_range)) {
            if (doc_range.type_ == DocRangeType::kMany) {
                result.addMany(doc_range.doc_many_size_, doc_range.doc_many->data());
                if (_bm25_v_proj_col_iters) {
                    CHECK_EQ(doc_range.doc_many_size_, doc_range.freq_many_size_);
                    for (size_t i = 0; i < doc_range.doc_many_size_; i++) {
                        segment_v2::rowid_t row_id = (*doc_range.doc_many)[i];
                        if (first || roaring.contains(row_id)) {
                            std::for_each(
                                    _bm25_v_proj_col_iters->begin(), _bm25_v_proj_col_iters->end(),
                                    [&](auto& iter) {
                                        iter->cal_and_add_bm25_score(
                                                row_id,
                                                (*doc_range.freq_many)[i],
                                                (*doc_range.norm_many)[i],
                                                idf,
                                                avg_dl);
                                    });
                        }
                    }
                }
            } else {
                result.addRange(doc_range.doc_range.first, doc_range.doc_range.second);
                if (_bm25_v_proj_col_iters) {
                    const uint32_t docs_size =
                            doc_range.doc_range.second - doc_range.doc_range.first;
                    CHECK_EQ(docs_size, doc_range.freq_many_size_);
                    for (uint32_t i = 0; i < docs_size; i++) {
                        segment_v2::rowid_t row_id = doc_range.doc_range.first + i;
                        if (first || roaring.contains(row_id)) {
                            std::for_each(
                                    _bm25_v_proj_col_iters->begin(), _bm25_v_proj_col_iters->end(),
                                    [&](auto& iter) {
                                        iter->cal_and_add_bm25_score(
                                                row_id,
                                                (*doc_range.freq_many)[i],
                                                (*doc_range.norm_many)[i],
                                                idf,
                                                avg_dl);
                                    });
                        }
                    }
                }
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

void ConjunctionQuery::search_by_skiplist(roaring::Roaring& roaring,
    index_stats::FullTextSimilarityCollector* full_text_similarity_collector) {
    int32_t doc = 0;
    const float avg_dl =
            _bm25_v_proj_col_iters
                    ? full_text_similarity_collector->get_or_calculate_avg_dl(_field_name)
                    : 0;
    while ((doc = do_next(_lead1.nextDoc())) != INT32_MAX) {
        roaring.add(doc);
        // All TermIterators now refer to the same rowId, proceed to calculate BM25.
        if (_bm25_v_proj_col_iters) {
            std::for_each(_bm25_v_proj_col_iters->begin(), _bm25_v_proj_col_iters->end(),
                          [&](auto& iter) {
                              iter->cal_and_add_bm25_score(
                                      doc,
                                      _lead1.freq(),
                                      _lead1.norm(),
                                      full_text_similarity_collector->get_or_calculate_idf(
                                              _field_name, _lead1.term()),
                                      avg_dl);
                          });

            if (!_lead2.isEmpty()) {
                std::for_each(_bm25_v_proj_col_iters->begin(), _bm25_v_proj_col_iters->end(),
                              [&](auto& iter) {
                                  iter->cal_and_add_bm25_score(
                                          doc,
                                          _lead2.freq(),
                                          _lead2.norm(),
                                          full_text_similarity_collector->get_or_calculate_idf(
                                                  _field_name, _lead2.term()),
                                          avg_dl);
                              });
            }

            for (auto& other : _others) {
                if (other.isEmpty()) {
                    continue;
                }
                std::for_each(_bm25_v_proj_col_iters->begin(), _bm25_v_proj_col_iters->end(),
                              [&](auto& iter) {
                                  iter->cal_and_add_bm25_score(
                                          doc,
                                          other.freq(),
                                          other.norm(),
                                          full_text_similarity_collector->get_or_calculate_idf(
                                                  _field_name, other.term()),
                                          avg_dl);
                              });
            }
        }
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

void ConjunctionQuery::pre_search(
        const InvertedIndexQueryInfo& query_info) {
    if (!query_info.tablet_index_stats_collectors) {
        throw doris::Exception(ErrorCode::INVERTED_INDEX_INVALID_PARAMETERS,
            "tablet_index_stats_collectors is null");
    }

    index_stats::SegmentColIndexStats stats;
    stats.full_segment_id = query_info.full_segment_id;
    stats.lucene_col_name = &query_info.field_name;
    _term_docs.reserve(query_info.terms.size());

    stats.total_term_cnt += _searcher->sumTotalTermFreq(query_info.field_name.c_str()).value_or(0);

    for (const auto& term : query_info.terms) {
        std::wstring ws_term = StringUtil::string_to_wstring(term);
        auto term_ptr =
                CLuceneUniquePtr<Term>(_CLNEW Term(query_info.field_name.c_str(), ws_term.c_str()));
        TermDocs* term_docs = _searcher->getReader()->termDocs(term_ptr.get(), true);
        _term_docs.emplace_back(term_docs);
        stats.term_doc_freqs[term] += term_docs->docFreq();
    }

    query_info.tablet_index_stats_collectors->collect(stats);
}

} // namespace doris::segment_v2