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

#include "query_helper.h"

namespace doris::segment_v2 {

void QueryHelper::collect_many(const IndexQueryContextPtr& context, const SimilarityPtr& similarity,
                               const DocRange& doc_range, const roaring::Roaring& roaring,
                               bool first) {
    for (size_t j = 0; j < doc_range.doc_many_size_; j++) {
        rowid_t row_id = (*doc_range.doc_many)[j];
        if (first || roaring.contains(row_id)) {
            auto freq = (*doc_range.freq_many)[j];
            auto norm = (*doc_range.norm_many)[j];
            auto score = similarity->score(freq, norm);
            context->collection_similarity->collect(row_id, score);
        }
    }
}

void QueryHelper::collect_range(const IndexQueryContextPtr& context,
                                const SimilarityPtr& similarity, const DocRange& doc_range,
                                const roaring::Roaring& roaring, bool first) {
    const uint32_t docs_size = doc_range.doc_range.second - doc_range.doc_range.first;
    for (uint32_t j = 0; j < docs_size; j++) {
        segment_v2::rowid_t row_id = doc_range.doc_range.first + j;
        if (first || roaring.contains(row_id)) {
            auto freq = (*doc_range.freq_many)[j];
            auto norm = (*doc_range.norm_many)[j];
            auto score = similarity->score(freq, norm);
            context->collection_similarity->collect(row_id, score);
        }
    }
}

void QueryHelper::query_statistics(const IndexQueryContextPtr& context, const SearcherPtr& searcher,
                                   const std::wstring& field_name,
                                   const std::span<const TermInfo>& term_infos) {
    if (!context->collection_statistics) {
        throw Exception(ErrorCode::INDEX_INVALID_PARAMETERS, "collection_statistics is null");
    }

    SegmentColIndexStats stats;
    stats.full_segment_id = context->full_segment_id;
    stats.lucene_col_name = &field_name;
    stats.total_term_cnt += searcher->sumTotalTermFreq(field_name.c_str()).value_or(0);

    for (const auto& term_info : term_infos) {
        auto iter = TermIterator::create(context->io_ctx, searcher->getReader(), field_name,
                                         term_info.get_single_term());
        stats.term_doc_freqs[iter->term()] += iter->doc_freq();
    }

    context->collection_statistics->collect(stats);
}

} // namespace doris::segment_v2