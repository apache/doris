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

namespace doris::segment_v2 {

DisjunctionQuery::DisjunctionQuery(const std::shared_ptr<lucene::search::IndexSearcher>& searcher,
                                   const TQueryOptions& query_options)
        : _searcher(searcher) {}

DisjunctionQuery::~DisjunctionQuery() {
    for (auto& term_doc : _term_docs) {
        if (term_doc) {
            _CLDELETE(term_doc);
        }
    }
}

void DisjunctionQuery::add(const std::wstring& field_name, const std::vector<std::string>& terms) {
    if (terms.empty()) {
        _CLTHROWA(CL_ERR_IllegalArgument, "DisjunctionQuery::add: terms empty");
    }

    _field_name = field_name;
    _terms = terms;
}

void DisjunctionQuery::search(roaring::Roaring& roaring) {
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

    const bool pre_searched = !_term_docs.empty();

    auto func = [this, &roaring, &full_text_similarity_collector, &pre_searched](
                        const std::string& term, size_t term_i, bool first) {
        std::wstring ws_term = StringUtil::string_to_wstring(term);
        auto term_ptr =
            CLuceneUniquePtr<Term>(_CLNEW Term(_field_name.c_str(), ws_term.c_str()));

        TermDocs* term_doc;
        if (pre_searched) {
            term_doc = _term_docs[term_i];
        } else {
            term_doc = _searcher->getReader()->termDocs(term_ptr.get());
            _term_docs.emplace_back(term_doc); // To uniformly release in the destructor
        }

        TermIterator iterator(term_doc);

        DocRange doc_range;
        roaring::Roaring result;

        const float idf =
                _bm25_v_proj_col_iters
                        ? full_text_similarity_collector->get_or_calculate_idf(_field_name, term)
                        : 0;
        const float avg_dl =
                _bm25_v_proj_col_iters
                        ? full_text_similarity_collector->get_or_calculate_avg_dl(_field_name)
                        : 0;

        while (iterator.readRange(&doc_range)) {
            if (doc_range.type_ == DocRangeType::kMany) {
                result.addMany(doc_range.doc_many_size_, doc_range.doc_many->data());
                if (_bm25_v_proj_col_iters) {
                    CHECK_EQ(doc_range.doc_many_size_, doc_range.freq_many_size_);
                    for (size_t i = 0; i < doc_range.doc_many_size_; i++) {
                        segment_v2::rowid_t row_id = (*doc_range.doc_many)[i];
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
            } else {
                result.addRange(doc_range.doc_range.first, doc_range.doc_range.second);
                if (_bm25_v_proj_col_iters) {
                    const uint32_t docs_size =
                            doc_range.doc_range.second - doc_range.doc_range.first;
                    CHECK_EQ(docs_size, doc_range.freq_many_size_);
                    for (uint32_t i = 0; i < docs_size; i++) {
                        segment_v2::rowid_t row_id = doc_range.doc_range.first + i;
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

        if (first) {
            roaring.swap(result);
        } else {
            roaring |= result;
        }
    };
    for (int i = 0; i < _terms.size(); i++) {
        func(_terms[i], i, i == 0);
    }
}

void DisjunctionQuery::pre_search(const InvertedIndexQueryInfo& query_info) {
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