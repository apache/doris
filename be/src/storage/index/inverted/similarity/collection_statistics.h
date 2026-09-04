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
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/be_mock_util.h"
#include "exprs/vexpr_fwd.h"
#include "runtime/runtime_state.h"
#include "storage/index/inverted/query/query_info.h"
#include "storage/index/inverted/similarity/predicate_collector.h"
#include "storage/olap_common.h"

namespace doris {

namespace io {
class FileSystem;
using FileSystemSPtr = std::shared_ptr<FileSystem>;
struct IOContext;
} // namespace io

struct RowSetSplits;

class Rowset;
class RowsetSegmentView;
using RowsetSharedPtr = std::shared_ptr<Rowset>;

class TabletIndex;
class TabletSchema;
using TabletSchemaSPtr = std::shared_ptr<TabletSchema>;

class CollectionStatistics {
public:
    CollectionStatistics() = default;
    virtual ~CollectionStatistics() = default;

    Status collect(RuntimeState* state, const std::vector<RowSetSplits>& rs_splits,
                   const TabletSchemaSPtr& tablet_schema,
                   const VExprContextSPtrs& common_expr_ctxs_push_down, io::IOContext* io_ctx);
    Status collect_full_collection(RuntimeState* state, const std::vector<RowsetSharedPtr>& rowsets,
                                   const TabletSchemaSPtr& tablet_schema,
                                   const VExprContextSPtrs& common_expr_ctxs_push_down,
                                   io::IOContext* io_ctx);

    MOCK_FUNCTION float get_or_calculate_idf(const std::wstring& lucene_col_name,
                                             const std::wstring& term);
    MOCK_FUNCTION float get_or_calculate_avg_dl(const std::wstring& lucene_col_name);

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
                           io::IOContext* io_ctx);
    Status admit_snii_scoring_segment(const std::wstring& field_name, uint64_t index_doc_count,
                                      uint64_t sum_total_term_freq, bool has_positions,
                                      bool has_norms,
                                      SniiScoringSegmentAccumulator* segment_accumulator);
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

    MOCK_DEFINE(friend class BM25SimilarityTest;)
    MOCK_DEFINE(friend class CollectionStatisticsTest;)
    MOCK_DEFINE(friend class BooleanQueryTest;)
    MOCK_DEFINE(friend class OccurBooleanQueryTest;)
};
using CollectionStatisticsPtr = std::shared_ptr<CollectionStatistics>;

// Implementation details of the SNII scoring-segment admission math, surfaced so
// collection_statistics_test.cpp can exercise them without compiling the .cpp a
// second time via #include.
namespace collection_statistics_detail {

struct SniiScoringSegmentStats {
    uint64_t doc_count = 0;
    uint64_t token_count = 0;
};

// 一个 SNII 段能参与打分的条件：带位置（词频来自位置）且带 norms（新版 writer 对分词 + 带位置
// 的索引一律写出）。老段没有 norms → NOT_SUPPORTED，重建索引或等 compaction 补齐。
Result<SniiScoringSegmentStats> resolve_snii_scoring_segment(uint64_t index_doc_count,
                                                             uint64_t sum_total_term_freq,
                                                             bool has_positions, bool has_norms);

void add_term_doc_frequency(
        std::unordered_map<std::wstring, std::unordered_map<std::wstring, uint64_t>>*
                logical_frequencies,
        const std::wstring& field, const std::wstring& logical_term, uint64_t doc_frequency);

} // namespace collection_statistics_detail

} // namespace doris
