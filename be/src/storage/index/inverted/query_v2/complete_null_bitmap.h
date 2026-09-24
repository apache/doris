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

#include <roaring/roaring.hh>
#include <span>
#include <vector>

#include "storage/index/inverted/query_v2/scorer.h"
#include "storage/index/query/boolean/truth_set.h"

namespace doris::segment_v2::inverted_index::query_v2 {

// Reuses complete bitmap views or consumes a fresh forward-only scorer.
// An optional candidate set restricts both TRUE and UNKNOWN rows.
index_query::TruthSet collect_truth_set(const ScorerPtr& scorer, const NullBitmapResolver* resolver,
                                        const roaring::Roaring* candidates = nullptr);

// Adds the TRUE rows of a fresh scorer to `rows`, taken from its bitmap view or a postings block at
// a time where it has one. UNKNOWN rows are not read.
void collect_true_rows(const ScorerPtr& scorer, roaring::Roaring* rows);

// Intersects the truth sets `collect(scorer, candidates)` returns, reading each scorer only within
// the rows the ones before it leave TRUE or UNKNOWN; an empty intersection ends the loop.
template <typename Collect>
index_query::TruthSet intersect_truth_sets(std::span<ScorerPtr> scorers, uint32_t row_count,
                                           Collect collect) {
    index_query::TruthSet result;
    result.true_rows.addRange(0, row_count);
    roaring::Roaring possible;
    const roaring::Roaring* candidates = nullptr;
    for (ScorerPtr& scorer : scorers) {
        result.intersect_with(collect(scorer, candidates));
        if (result.true_rows.isEmpty() && result.null_rows.isEmpty()) {
            break;
        }
        // Without UNKNOWN rows the TRUE rows are the candidates themselves.
        if (result.null_rows.isEmpty()) {
            candidates = &result.true_rows;
        } else {
            possible = result.true_rows | result.null_rows;
            candidates = &possible;
        }
    }
    return result;
}

ScorerPtr materialize_scorer(ScorerPtr source, bool enable_scoring,
                             const NullBitmapResolver* resolver,
                             const roaring::Roaring* candidates = nullptr);

// Materializes forward-only children only when complete null evaluation needs their TRUE sets.
// Replaced children preserve their scores and restart at the first matching document.
roaring::Roaring complete_null_bitmap(std::vector<ScorerPtr>& children, bool intersection,
                                      bool enable_scoring, const NullBitmapResolver* resolver);

} // namespace doris::segment_v2::inverted_index::query_v2
