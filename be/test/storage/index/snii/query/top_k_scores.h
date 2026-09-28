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

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <roaring/roaring.hh>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/index/query/roaring_docid_sink.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/query/bm25_scorer.h"
#include "storage/index/snii/query/scoring_query.h"
#include "storage/index/snii/query/term_query.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/stats/snii_stats_provider.h"

namespace doris::snii::snii_test {

// Scores every document holding any of `terms` with the production candidate
// scorer, using segment-local idf and avgdl, and keeps the top `k` by score
// descending, then docid ascending. Absent terms are skipped.
inline Status top_k_scores(const reader::LogicalIndexReader& idx,
                           const stats::SniiStatsProvider& stats,
                           const std::vector<std::string>& terms, uint32_t k,
                           const query::Bm25Params& params, std::vector<query::ScoredDoc>* out) {
    out->clear();
    if (k == 0) {
        return Status::OK();
    }
    roaring::Roaring candidates;
    index_query::RoaringDocIdSink sink(candidates);
    std::vector<query::CollectionScoringTerm> clauses;
    for (const std::string& term : terms) {
        bool found = false;
        format::DictEntry entry;
        uint64_t frq_base = 0;
        uint64_t prx_base = 0;
        RETURN_IF_ERROR(idx.lookup(term, &found, &entry, &frq_base, &prx_base));
        if (!found) {
            continue;
        }
        RETURN_IF_ERROR(query::term_query(idx, term, &sink));
        const double n = static_cast<double>(stats.indexed_doc_count());
        const double df = static_cast<double>(entry.df);
        clauses.push_back(
                {.physical_term = term, .idf = std::log(1.0 + (n - df + 0.5) / (df + 0.5))});
    }
    std::vector<query::ScoredDoc> scored;
    RETURN_IF_ERROR(query::scoring_query_candidates(idx, stats, clauses, candidates, stats.avgdl(),
                                                    params, &scored));
    std::ranges::sort(scored, [](const query::ScoredDoc& a, const query::ScoredDoc& b) {
        return a.score != b.score ? a.score > b.score : a.docid < b.docid;
    });
    if (scored.size() > k) {
        scored.resize(k);
    }
    *out = std::move(scored);
    return Status::OK();
}

} // namespace doris::snii::snii_test
