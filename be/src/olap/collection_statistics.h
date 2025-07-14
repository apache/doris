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

#include "olap/olap_common.h"
#include "vec/exprs/vexpr_fwd.h"

namespace doris {

struct RowSetSplits;

class TabletSchema;
using TabletSchemaSPtr = std::shared_ptr<TabletSchema>;

class CollectionStatistics {
public:
    Status collect(const std::vector<RowSetSplits>& rs_splits,
                   const TabletSchemaSPtr& tablet_schema,
                   const vectorized::VExprContextSPtrs& common_expr_ctxs_push_down);

    float get_or_calculate_idf(const std::wstring& lucene_col_name, const std::wstring& term);
    float get_or_calculate_avg_dl(const std::wstring& lucene_col_name);

private:
    uint64_t get_term_doc_freq_by_col(const std::wstring& lucene_col_name,
                                      const std::wstring& term);
    uint64_t get_total_term_cnt_by_col(const std::wstring& lucene_col_name);
    uint64_t get_doc_num() const;

    uint64_t _total_num_docs = 0;
    std::unordered_map<std::wstring, uint64_t> _total_num_tokens;
    std::unordered_map<std::wstring, std::unordered_map<std::wstring, uint64_t>> _term_doc_freqs;

    std::unordered_map<std::wstring, float> _avg_dl_by_col;
    std::unordered_map<std::wstring, std::unordered_map<std::wstring, float>> _idf_by_col_term;
};
using CollectionStatisticsPtr = std::shared_ptr<CollectionStatistics>;

} // namespace doris
