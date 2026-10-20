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

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/be_mock_util.h"
#include "exprs/vexpr_fwd.h"
#include "runtime/runtime_state.h"
#include "storage/index/inverted/common/query_info.h"
#include "storage/index/inverted/reader/predicate_collector.h"
#include "storage/olap_common.h"

namespace doris {

namespace index_query {
class IndexSource;
struct IndexScoringStats;
} // namespace index_query

namespace io {
class FileSystem;
using FileSystemSPtr = std::shared_ptr<FileSystem>;
struct IOContext;
} // namespace io

struct RowSetSplits;

class DeleteBitmap;

class Rowset;
class RowsetSegmentView;
using RowsetSharedPtr = std::shared_ptr<Rowset>;

class TabletIndex;
class TabletSchema;
using TabletSchemaSPtr = std::shared_ptr<TabletSchema>;

// The rows of a merge-on-write tablet that its segments still hold but a scan at `version` does
// not return: rows a later load overwrote or deleted, which the delete bitmap marks, and rows
// written with the delete sign, which every query filters out. Compaction removes both, on each
// replica at its own time, so statistics counting them would differ between the replicas of a
// tablet, and so would every BM25 score.
struct MowHiddenRows {
    std::shared_ptr<DeleteBitmap> delete_bitmap;
    int64_t version = -1;
};

class CollectionStatistics {
public:
    explicit CollectionStatistics(PredicateCollectorPtr search_collector = nullptr)
            : _search_collector(std::move(search_collector)) {}
    virtual ~CollectionStatistics() = default;

    // With `hidden_rows`, the statistics describe only the rows a merge-on-write scan returns,
    // so every replica of a tablet scores alike whatever it has compacted: the hidden rows are
    // left out of the document count and the term document frequencies, and the token count is
    // the sum of the document lengths the norms of the returned rows stand for.
    Status collect(RuntimeState* state, const std::vector<RowSetSplits>& rs_splits,
                   const TabletSchemaSPtr& tablet_schema,
                   const VExprContextSPtrs& common_expr_ctxs_push_down, io::IOContext* io_ctx,
                   const MowHiddenRows* hidden_rows = nullptr);
    Status collect_full_collection(RuntimeState* state, const std::vector<RowsetSharedPtr>& rowsets,
                                   const TabletSchemaSPtr& tablet_schema,
                                   const VExprContextSPtrs& common_expr_ctxs_push_down,
                                   io::IOContext* io_ctx,
                                   const MowHiddenRows* hidden_rows = nullptr);

    MOCK_FUNCTION float get_or_calculate_idf(const std::wstring& lucene_col_name,
                                             const std::wstring& term);
    MOCK_FUNCTION float get_or_calculate_avg_dl(const std::wstring& lucene_col_name);

    // Replaces the collected counters with those of `other`. The derived idf/avgdl caches are
    // reset rather than copied, so the copy can be used by another thread without sharing them.
    void copy_collected_from(const CollectionStatistics& other);

private:
    struct SniiScoringSegmentAccumulator {
        uint64_t doc_count = 0;
        std::unordered_map<std::wstring, uint64_t> token_counts;
        std::unordered_map<std::wstring, std::unordered_map<std::wstring, uint64_t>> term_doc_freqs;
    };

    Status extract_collect_info(RuntimeState* state,
                                const VExprContextSPtrs& common_expr_ctxs_push_down,
                                const TabletSchemaSPtr& tablet_schema,
                                CollectInfoMap* collect_infos);
    Status process_segment(const RowsetSharedPtr& rowset, const RowsetSegmentView& seg,
                           const TabletSchema* tablet_schema, const CollectInfoMap& collect_infos,
                           const MowHiddenRows* hidden_rows, io::IOContext* io_ctx);
    Status admit_snii_scoring_segment(const std::wstring& field_name, uint64_t index_doc_count,
                                      uint64_t sum_total_term_freq, bool has_positions,
                                      bool has_norms,
                                      SniiScoringSegmentAccumulator* segment_accumulator,
                                      bool is_analyzed = true, bool single_term_normalizer = false);
    void commit_snii_scoring_segment(SniiScoringSegmentAccumulator&& segment_accumulator);
    void clear();

    uint64_t get_term_doc_freq_by_col(const std::wstring& lucene_col_name,
                                      const std::wstring& term);
    uint64_t get_total_term_cnt_by_col(const std::wstring& lucene_col_name);
    uint64_t get_doc_num() const;

    uint64_t _total_num_docs = 0;
    std::unordered_map<std::wstring, uint64_t> _total_num_tokens;
    std::unordered_map<std::wstring, std::unordered_map<std::wstring, uint64_t>> _term_doc_freqs;

    std::unordered_map<std::wstring, float> _avg_dl_by_col;
    std::unordered_map<std::wstring, std::unordered_map<std::wstring, float>> _idf_by_col_term;
    PredicateCollectorPtr _search_collector;

    MOCK_DEFINE(friend class BM25SimilarityTest;)
    MOCK_DEFINE(friend class CollectionStatisticsTest;)
    MOCK_DEFINE(friend class BooleanQueryTest;)
    MOCK_DEFINE(friend class OccurBooleanQueryTest;)
};
using CollectionStatisticsPtr = std::shared_ptr<CollectionStatistics>;

// BM25 statistics of a whole tablet, shared by every scanner the tablet is split into.
//
// A parallel scan divides a tablet's rowsets among several scanners. If each scanner
// collected statistics over only its own rowsets, rows with identical content would score
// differently depending on which scanner read them, and the split itself depends on the
// tablet's row count and the number of cores. Collecting once over the entire read source
// gives every row of the tablet the same idf and avgdl.
class TabletCollectionStatistics {
public:
    TabletCollectionStatistics(std::vector<RowsetSharedPtr> rowsets,
                               PredicateCollectorPtr search_collector)
            : _rowsets(std::move(rowsets)), _statistics(std::move(search_collector)) {}

    // Collects the statistics on the first call, then copies them into `statistics`.
    // Later callers wait for the first collection and receive its result or error.
    Status copy_to(RuntimeState* state, const TabletSchemaSPtr& tablet_schema,
                   const VExprContextSPtrs& common_expr_ctxs_push_down, io::IOContext* io_ctx,
                   const MowHiddenRows* hidden_rows, CollectionStatistics* statistics);

private:
    std::mutex _mutex;
    bool _collected = false;
    Status _status;
    std::vector<RowsetSharedPtr> _rowsets;
    CollectionStatistics _statistics;
};
using TabletCollectionStatisticsPtr = std::shared_ptr<TabletCollectionStatistics>;

// Implementation details of the SNII scoring-segment admission math, surfaced so
// collection_statistics_test.cpp can exercise them without compiling the .cpp a
// second time via #include.
namespace collection_statistics_detail {

struct SniiScoringSegmentStats {
    uint64_t doc_count = 0;
    uint64_t token_count = 0;
};

// Analyzed SNII indexes need positions and norms. Keywords and lowercase-only
// normalizers can use unit frequency and norm when those sections are absent.
Result<SniiScoringSegmentStats> resolve_snii_scoring_segment(uint64_t index_doc_count,
                                                             uint64_t sum_total_term_freq,
                                                             bool has_positions, bool has_norms,
                                                             bool is_analyzed = true,
                                                             bool single_term_normalizer = false);

void add_term_doc_frequency(
        std::unordered_map<std::wstring, std::unordered_map<std::wstring, uint64_t>>*
                logical_frequencies,
        const std::wstring& field, const std::wstring& logical_term, uint64_t doc_frequency);

// Turns a segment's scoring statistics into those of its documents other than the ascending
// `hidden_docs`: their count, and the sum of the lengths their norms stand for, or their count
// when the index has no norms.
Status exclude_hidden_docs(index_query::IndexSource& source, std::span<const uint32_t> hidden_docs,
                           index_query::IndexScoringStats* stats);

// Takes from a term's document frequency the documents of `hidden_docs` that hold it.
Status exclude_hidden_term_docs(index_query::IndexSource& source, std::string_view term,
                                std::span<const uint32_t> hidden_docs, uint64_t* doc_frequency);

} // namespace collection_statistics_detail

} // namespace doris
