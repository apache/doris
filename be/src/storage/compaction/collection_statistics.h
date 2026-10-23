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

#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/be_mock_util.h"
#include "exprs/vexpr_fwd.h"
#include "runtime/runtime_state.h"
#include "storage/index/inverted/query/query_info.h"
#include "storage/olap_common.h"
#include "storage/predicate_collector.h"

namespace doris {
#include "common/compile_check_begin.h"

namespace io {
class FileSystem;
using FileSystemSPtr = std::shared_ptr<FileSystem>;
struct IOContext;
} // namespace io

struct RowSetSplits;

class Rowset;
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

    MOCK_FUNCTION float get_or_calculate_idf(const std::wstring& lucene_col_name,
                                             const std::wstring& term);
    MOCK_FUNCTION float get_or_calculate_avg_dl(const std::wstring& lucene_col_name);

    // Replaces the collected counters with those of `other`. The derived idf/avgdl caches are
    // reset rather than copied, so the copy can be used by another thread without sharing them.
    void copy_collected_from(const CollectionStatistics& other);

private:
    Status extract_collect_info(RuntimeState* state,
                                const VExprContextSPtrs& common_expr_ctxs_push_down,
                                const TabletSchemaSPtr& tablet_schema,
                                CollectInfoMap* collect_infos);
    Status process_segment(const RowsetSharedPtr& rowset, int32_t seg_id,
                           const TabletSchema* tablet_schema, const CollectInfoMap& collect_infos,
                           io::IOContext* io_ctx);

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

// BM25 statistics of a whole tablet, shared by every scanner the tablet is split into.
//
// A parallel scan divides a tablet's rowsets among several scanners. If each scanner
// collected statistics over only its own rowsets, rows with identical content would score
// differently depending on which scanner read them, and the split itself depends on the
// tablet's row count and the number of cores. Collecting once over the entire read source
// gives every row of the tablet the same idf and avgdl.
class TabletCollectionStatistics {
public:
    explicit TabletCollectionStatistics(std::vector<RowSetSplits> rs_splits);
    ~TabletCollectionStatistics();

    // Collects the statistics on the first call, then copies them into `statistics`.
    // Later callers wait for the first collection and receive its result or error.
    Status copy_to(RuntimeState* state, const TabletSchemaSPtr& tablet_schema,
                   const VExprContextSPtrs& common_expr_ctxs_push_down, io::IOContext* io_ctx,
                   CollectionStatistics* statistics);

private:
    std::mutex _mutex;
    bool _collected = false;
    Status _status;
    std::vector<RowSetSplits> _rs_splits;
    CollectionStatistics _statistics;
};
using TabletCollectionStatisticsPtr = std::shared_ptr<TabletCollectionStatistics>;

#include "common/compile_check_end.h"
} // namespace doris
