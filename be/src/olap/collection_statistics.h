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
#include <string>
#include <unordered_map>

#include "common/be_mock_util.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/inverted_index/query/query_info.h"
#include "runtime/runtime_state.h"
#include "vec/exprs/vexpr_fwd.h"

namespace doris {
#include "common/compile_check_begin.h"

namespace io {
class FileSystem;
using FileSystemSPtr = std::shared_ptr<FileSystem>;
} // namespace io

struct RowSetSplits;

class TabletIndex;
class TabletSchema;
using TabletSchemaSPtr = std::shared_ptr<TabletSchema>;

struct TermInfoComparer {
    bool operator()(const segment_v2::TermInfo& lhs, const segment_v2::TermInfo& rhs) const {
        return lhs.term < rhs.term;
    }
};

class CollectInfo {
public:
    std::set<segment_v2::TermInfo, TermInfoComparer> term_infos;
    const TabletIndex* index_meta = nullptr;
};

class CollectionStatistics {
public:
    CollectionStatistics() = default;
    virtual ~CollectionStatistics() = default;

    Status collect(RuntimeState* state, const std::vector<RowSetSplits>& rs_splits,
                   const TabletSchemaSPtr& tablet_schema,
                   const vectorized::VExprContextSPtrs& common_expr_ctxs_push_down);

    MOCK_FUNCTION float get_or_calculate_idf(const std::wstring& lucene_col_name,
                                             const std::wstring& term);
    MOCK_FUNCTION float get_or_calculate_avg_dl(const std::wstring& lucene_col_name);

private:
    Status extract_collect_info(RuntimeState* state,
                                const vectorized::VExprContextSPtrs& common_expr_ctxs_push_down,
                                const TabletSchemaSPtr& tablet_schema,
                                std::unordered_map<std::wstring, CollectInfo>* collect_infos);
    Status process_segment(const std::string& seg_path, const io::FileSystemSPtr& fs,
                           const TabletSchema* tablet_schema,
                           const std::unordered_map<std::wstring, CollectInfo>& collect_infos);

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
};
using CollectionStatisticsPtr = std::shared_ptr<CollectionStatistics>;

#include "common/compile_check_end.h"
} // namespace doris
