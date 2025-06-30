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

#include "collection_statistics.h"

#include "common/exception.h"
#include "olap/rowset/segment_v2/inverted_index/util/string_helper.h"

namespace doris {

using namespace segment_v2::inverted_index;

void CollectionStatistics::collect(const SegmentStats& segment_stats) {
    if (_doc_num < 0) {
        _doc_num = 0;
    }
    _doc_num += segment_stats.row_cnt;
}

void CollectionStatistics::collect(const SegmentColIndexStats& segment_col_index_stats) {
    if (!segment_col_index_stats.full_segment_id) {
        throw Exception(ErrorCode::INVERTED_INDEX_CLUCENE_ERROR, "full_segment_id is null");
    }

    if (!segment_col_index_stats.lucene_col_name) {
        throw Exception(ErrorCode::INVERTED_INDEX_CLUCENE_ERROR, "lucene_col_name is null");
    }

    SegmentCol seg_col {segment_col_index_stats.full_segment_id,
                        segment_col_index_stats.lucene_col_name};

    if (auto iter = _seg_col_collected_stats.find(seg_col);
        iter == _seg_col_collected_stats.end()) {
        // This is the first time the column is encountered for the segment.
        _total_term_cnt_by_col[*segment_col_index_stats.lucene_col_name] +=
                segment_col_index_stats.total_term_cnt;
        _seg_col_collected_stats.emplace(seg_col, CollectedStats {});
    }

    auto& collected_stats = _seg_col_collected_stats.find(seg_col)->second;
    TermDocFreqs& term_doc_freqs = _term_doc_freqs_by_col[*segment_col_index_stats.lucene_col_name];

    for (const auto& [term, doc_freq] : segment_col_index_stats.term_doc_freqs) {
        if (collected_stats.is_new_term(term)) {
            term_doc_freqs[term] += doc_freq;
        }
    }
}

uint64_t CollectionStatistics::get_term_doc_freq_by_col(const std::wstring& lucene_col_name,
                                                        const std::wstring& term) const {
    auto term_doc_freqs_iter = _term_doc_freqs_by_col.find(lucene_col_name);
    if (UNLIKELY(term_doc_freqs_iter == _term_doc_freqs_by_col.end())) {
        throw Exception(ErrorCode::INDEX_INVALID_PARAMETERS, "Not such column {}",
                        StringHelper::to_string(lucene_col_name));
    }

    auto doc_freq_iter = term_doc_freqs_iter->second.find(term);
    if (UNLIKELY(doc_freq_iter == term_doc_freqs_iter->second.end())) {
        throw Exception(ErrorCode::INDEX_INVALID_PARAMETERS, "Not such term {}",
                        StringHelper::to_string(term));
    }

    return doc_freq_iter->second;
}

uint64_t CollectionStatistics::get_total_term_cnt_by_col(
        const std::wstring& lucene_col_name) const {
    auto total_term_cnt_iter = _total_term_cnt_by_col.find(lucene_col_name);
    if (UNLIKELY(total_term_cnt_iter == _total_term_cnt_by_col.end())) {
        throw Exception(ErrorCode::INDEX_INVALID_PARAMETERS, "Not such column {}",
                        StringHelper::to_string(lucene_col_name));
    }

    return total_term_cnt_iter->second;
}

uint64_t CollectionStatistics::get_doc_num() const {
    if (UNLIKELY(_doc_num < 0)) {
        throw Exception(ErrorCode::INDEX_INVALID_PARAMETERS,
                        "No data available for SimilarityCollector");
    }
    return _doc_num;
}

float CollectionStatistics::get_or_calculate_idf(const std::wstring& lucene_col_name,
                                                 const std::wstring& term) {
    static const std::wstring _separator = L"/__datamind_internal_separator__/";

    std::wstring key = lucene_col_name + _separator + term;
    auto it = _idf_by_col_term.find(key);
    if (LIKELY(it != _idf_by_col_term.end())) {
        return it->second;
    }
    const uint64_t doc_num = get_doc_num();
    const uint64_t doc_freq = get_term_doc_freq_by_col(lucene_col_name, term);
    float idf = std::log(1 + (doc_num - doc_freq + (double)0.5) / (doc_freq + (double)0.5));
    _idf_by_col_term[key] = idf;
    return idf;
}

float CollectionStatistics::get_or_calculate_avg_dl(const std::wstring& lucene_col_name) {
    auto it = _avg_dl_by_col.find(lucene_col_name);
    if (LIKELY(it != _avg_dl_by_col.end())) {
        return it->second;
    }

    const uint64_t total_term_cnt = get_total_term_cnt_by_col(lucene_col_name);
    const uint64_t total_doc_cnt = get_doc_num();
    float avg_dl = total_doc_cnt > 0 ? (1.0 * total_term_cnt / total_doc_cnt) : 0;
    _avg_dl_by_col[lucene_col_name] = avg_dl;
    return avg_dl;
}

} // namespace doris