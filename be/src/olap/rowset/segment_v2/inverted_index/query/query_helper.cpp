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
#include "common/compile_check_begin.h"

void QueryHelper::collect(const IndexQueryContextPtr& context,
                          const std::vector<SimilarityPtr>& similarities,
                          const std::vector<TermIterPtr>& iterators, int32_t doc) {
    for (size_t i = 0; i < iterators.size(); i++) {
        auto freq = iterators[i]->freq();
        auto doc_length = iterators[i]->norm();
        auto score = similarities[i]->score(static_cast<float>(freq), doc_length);
        context->collection_similarity->collect(doc, score);
    }
}

void QueryHelper::collect(const IndexQueryContextPtr& context,
                          const std::vector<SimilarityPtr>& similarities,
                          const std::vector<DISI>& iterators, int32_t doc) {
    for (size_t i = 0; i < iterators.size(); i++) {
        const auto& iter = iterators[i];
        if (std::holds_alternative<TermPositionsIterPtr>(iter)) {
            const auto& term_iter = std::get<TermPositionsIterPtr>(iter);
            auto freq = term_iter->freq();
            auto norm = term_iter->norm();
            auto score = similarities[i]->score(static_cast<float>(freq), norm);
            context->collection_similarity->collect(doc, score);
        }
    }
}

void QueryHelper::collect_many(const IndexQueryContextPtr& context, const SimilarityPtr& similarity,
                               const DocRange& doc_range) {
    for (size_t j = 0; j < doc_range.doc_many_size_; j++) {
        rowid_t row_id = (*doc_range.doc_many)[j];
        auto freq = (*doc_range.freq_many)[j];
        auto norm = (*doc_range.norm_many)[j];
        auto score = similarity->score(static_cast<float>(freq), norm);
        context->collection_similarity->collect(row_id, score);
    }
}

void QueryHelper::collect_range(const IndexQueryContextPtr& context,
                                const SimilarityPtr& similarity, const DocRange& doc_range) {
    const uint32_t docs_size = doc_range.doc_range.second - doc_range.doc_range.first;
    for (uint32_t j = 0; j < docs_size; j++) {
        segment_v2::rowid_t row_id = doc_range.doc_range.first + j;
        auto freq = (*doc_range.freq_many)[j];
        auto norm = (*doc_range.norm_many)[j];
        auto score = similarity->score(static_cast<float>(freq), norm);
        context->collection_similarity->collect(row_id, score);
    }
}

#include "common/compile_check_end.h"
} // namespace doris::segment_v2
