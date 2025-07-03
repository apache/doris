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
#pragma once

#include "olap/olap_common.h"

namespace doris {

using TermDocFreqs = std::unordered_map<std::wstring, uint64_t>;
using TotalTermCnt = uint64_t;

class FullSegmentId {
public:
    FullSegmentId(const RowsetId& rowset_id, const uint32_t segment_id)
            : rowset_id(rowset_id), segment_id(segment_id) {}

    bool operator==(const FullSegmentId& other) const {
        return rowset_id == other.rowset_id && segment_id == other.segment_id;
    }

    bool operator!=(const FullSegmentId& other) const {
        return rowset_id != other.rowset_id || segment_id != other.segment_id;
    }

    bool operator<(const FullSegmentId& other) const {
        if (rowset_id != other.rowset_id) {
            return rowset_id < other.rowset_id;
        }
        return segment_id < other.segment_id;
    }

    std::string to_string() const {
        return fmt::format("{}_{}", rowset_id.to_string(), segment_id);
    }

    const RowsetId rowset_id;
    const uint32_t segment_id;
};
using FullSegmentIdPtr = std::shared_ptr<FullSegmentId>;

class SegmentStats {
public:
    uint64_t row_cnt = 0;
};

class SegmentColIndexStats {
public:
    FullSegmentIdPtr full_segment_id;
    const std::wstring* lucene_col_name = nullptr;
    TermDocFreqs term_doc_freqs;
    TotalTermCnt total_term_cnt = 0;
};

class CollectionStatistics {
public:
    void collect(const SegmentStats& segment_stats);
    void collect(const SegmentColIndexStats& segment_column_stats);

    uint64_t get_term_doc_freq_by_col(const std::wstring& lucene_col_name,
                                      const std::wstring& term) const;
    uint64_t get_total_term_cnt_by_col(const std::wstring& lucene_col_name) const;
    uint64_t get_doc_num() const;
    float get_or_calculate_idf(const std::wstring& lucene_col_name, const std::wstring& term);
    float get_or_calculate_avg_dl(const std::wstring& lucene_col_name);

private:
    class SegmentCol {
    public:
        SegmentCol(FullSegmentIdPtr full_segment_id, const std::wstring* lucene_col_name)
                : _full_segment_id(std::move(full_segment_id)), _lucene_col_name(lucene_col_name) {}

        bool operator<(const SegmentCol& other) const {
            if (*_full_segment_id != *other._full_segment_id) {
                return *_full_segment_id < *other._full_segment_id;
            }
            return *_lucene_col_name < *other._lucene_col_name;
        }

        FullSegmentIdPtr _full_segment_id;
        const std::wstring* _lucene_col_name;
    };

    class CollectedStats {
    public:
        bool is_new_term(const std::wstring& term) { return _collected_terms.insert(term).second; }

    private:
        std::unordered_set<std::wstring> _collected_terms;
    };

    std::unordered_map<std::wstring, TermDocFreqs> _term_doc_freqs_by_col;
    std::unordered_map<std::wstring, TotalTermCnt> _total_term_cnt_by_col;
    std::unordered_map<std::wstring, float> _idf_by_col_term;
    std::unordered_map<std::wstring, float> _avg_dl_by_col;

    std::map<SegmentCol, CollectedStats> _seg_col_collected_stats;

    int64_t _doc_num = -1;
};
using CollectionStatisticsPtr = std::shared_ptr<CollectionStatistics>;

} // namespace doris
