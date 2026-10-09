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
#include <roaring/roaring.hh>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/query/bm25_scorer.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/stats/snii_stats_provider.h"

// scoring_query -- BM25 scoring over one logical index for one or more query
// terms. The on-disk format stores no term frequencies: tf is the number of
// positions the term has in the document (the Lucene-family definition), so scoring
// needs a positional index with norms.
//   - scoring_query_candidates(): scores an externally computed candidate set
//     with collection-scoped idf / avgdl (the production path).
//   - scoring_query_exhaustive(): scores every document containing any query
//     term with segment-local statistics and returns the top-K (test oracle).
//
// Results of the top-K path are sorted by score descending; ties are broken by
// ascending docid so the ordering is deterministic.
namespace doris::snii::query {

// One scored hit.
struct ScoredDoc {
    uint32_t docid = 0;
    double score = 0.0;
};

// One logical scoring clause after its plain term has been routed to this
// segment's physical key. IDF remains collection-scoped and is never derived
// from the physical term's segment-local dictionary entry.
struct CollectionScoringTerm {
    std::string physical_term;
    double idf = 0.0;
};

// Scores every document in final_candidates using collection-scoped IDF and
// avgdl plus segment-local TF/norm. Results are returned in ascending docid
// order. Repeated terms are repeated scoring clauses. This path deliberately
// does not use the segment-local WAND bounds below.
Status scoring_query_candidates(const reader::LogicalIndexReader& idx,
                                const stats::SniiStatsProvider& segment_stats,
                                const std::vector<CollectionScoringTerm>& terms,
                                const roaring::Roaring& final_candidates, double collection_avgdl,
                                const Bm25Params& params, std::vector<ScoredDoc>* out);

// Exhaustive baseline: score every doc that contains any query term, return the
// top-k by score. params controls k1/b. Unknown terms are skipped.
Status scoring_query_exhaustive(const reader::LogicalIndexReader& idx,
                                const stats::SniiStatsProvider& stats,
                                const std::vector<std::string>& terms, uint32_t k,
                                const Bm25Params& params, std::vector<ScoredDoc>* out);

} // namespace doris::snii::query
