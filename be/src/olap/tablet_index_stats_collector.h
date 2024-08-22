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

#include <memory>
#include <string>
#include <unordered_map>

namespace doris::index_stats {

using TermDocFreqs = std::unordered_map<std::string /*term*/,
                       uint64_t /*number of documents containing the specified term*/>;
// Total number of terms.
using TotalTermCnt = uint64_t;

inline const std::string FULL_TEXT_SIMILARITY_STATS_COLLECTOR =
        "FULL_TEXT_SIMILARITY_STATS_COLLECTOR";

class FullSegmentId {
public:
    FullSegmentId(const RowsetId& rowset_id, const uint32_t segment_id)
            : rowset_id(rowset_id), segment_id(segment_id) {}

    FullSegmentId(const FullSegmentId& other)
           : rowset_id(other.rowset_id), segment_id(other.segment_id) {}

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

    const RowsetId rowset_id;
    const uint32_t segment_id;
};

/**
 *  Segment-level statistics.
 *  Each segment is collected only once.
 */
class SegmentStats {
public:
    // Total number of rows.
    uint64_t row_cnt = 0;
};

/**
 *  Column-level statistics.
 *  A column index might be collected multiple times with different statistics because
 *  a column might be used for different predicates.
 */
class SegmentColIndexStats {
public:
    const FullSegmentId* full_segment_id = nullptr;

    /************* Full-text statistics for a column at the segment level *************/
    const std::wstring* lucene_col_name = nullptr;
    TermDocFreqs term_doc_freqs;
    TotalTermCnt total_term_cnt = 0;
    /************************************** END **************************************/
};

class TabletIndexStatsCollector {
public:
    virtual void collect(const SegmentStats& segment_stats) = 0;
    virtual void collect(const SegmentColIndexStats& segment_column_stats) = 0;
    virtual std::string name() = 0;

    virtual ~TabletIndexStatsCollector() = default;
};

class TabletIndexStatsCollectors {
public:
    void add_tablet_index_stats_collector(std::shared_ptr<TabletIndexStatsCollector>& tablet_index_stats_collector) {
        _collectors.emplace(tablet_index_stats_collector->name(), tablet_index_stats_collector);
    }

    void collect(const SegmentStats& segment_stats) const {
        for (const auto& kv : _collectors) {
            kv.second->collect(segment_stats);
        }
    }

    void collect(const SegmentColIndexStats& segment_col_index_stats) const {
        for (const auto& kv : _collectors) {
            kv.second->collect(segment_col_index_stats);
        }
    }

    std::shared_ptr<TabletIndexStatsCollector>
    get_tablet_index_stats_collector_by_name(const std::string& name) const {
        auto iter = _collectors.find(name);

        if (iter != _collectors.end()) {
            return iter->second;
        }

        return nullptr;
    }
private:
    std::unordered_map<std::string, std::shared_ptr<TabletIndexStatsCollector>> _collectors;
};

/**
 * Collects segment-level index statistics to evaluate similarity at the
 * tablet level.
 */
class FullTextSimilarityCollector final: public TabletIndexStatsCollector {
public:
    void collect(const SegmentStats& segment_stats) override;
    void collect(const SegmentColIndexStats& segment_column_stats) override;

    std::string name() override {
        return FULL_TEXT_SIMILARITY_STATS_COLLECTOR;
    }

    /**
     *  Returns the number of documents containing the specified term.
     */
    uint64_t get_term_doc_freq_by_col(const std::wstring& lucene_col_name,
                                                     const std::string& term) const;

    /**
     *  Returns the total number of terms for the specified column.
     */
    uint64_t get_total_term_cnt_by_col(const std::wstring& lucene_col_name) const;

    /**
     *  Returns the total number of documents at segment level.
     */
    uint64_t get_doc_num() const;

    /**
     *  Returns the IDF of the specific term.
     */
    float get_or_calculate_idf(const std::wstring& lucene_col_name, const std::string& term);

    /**
     *  Returns the average terms of all documents.
     */
    float get_or_calculate_avg_dl(const std::wstring& lucene_col_name);

private:
    class SegmentCol {
    public:
        SegmentCol(const FullSegmentId* full_segment_id, const std::wstring* lucene_col_name)
                : full_segment_id(full_segment_id), lucene_col_name(lucene_col_name) {}

        bool operator<(const SegmentCol& other) const {
            if (*full_segment_id != *other.full_segment_id) {
                return *full_segment_id < *other.full_segment_id;
            }
            return *lucene_col_name < *other.lucene_col_name;
        }

        const FullSegmentId* full_segment_id;
        const std::wstring* lucene_col_name;
    };

    class CollectedStats {
    public:
        bool is_new_term(const std::string& term) {
            return _collected_terms.insert(term).second;
        }
    private:
        std::unordered_set<std::string> _collected_terms;
    };

    std::unordered_map<std::wstring /*lucene column name*/, TermDocFreqs> _term_doc_freqs_by_col;
    std::unordered_map<std::wstring /*lucene column name*/, TotalTermCnt> _total_term_cnt_by_col;
    std::unordered_map<std::string /*lucene column name + term*/, float> _idf_by_col_term;
    std::unordered_map<std::wstring /*lucene column name*/, float> _avg_dl_by_col;

    std::map<SegmentCol, CollectedStats> _seg_col_collected_stats;

    int64_t _doc_num = -1;
};

} // namespace doris::index_stats



