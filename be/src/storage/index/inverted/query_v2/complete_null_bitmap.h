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

#include <initializer_list>
#include <optional>
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

// Intersects the truth sets `collect(scorer, candidates)` returns, reading each scorer only within
// the rows the ones before it leave TRUE or UNKNOWN; an empty intersection ends the loop.
template <typename Collect>
index_query::TruthSet intersect_truth_sets(std::span<ScorerPtr> scorers, uint32_t row_count,
                                           Collect collect) {
    index_query::TruthSet result;
    result.true_rows.addRange(0, row_count);
    std::optional<roaring::Roaring> candidates;
    for (ScorerPtr& scorer : scorers) {
        result.intersect_with(collect(scorer, candidates ? &*candidates : nullptr));
        if (result.true_rows.isEmpty() && result.null_rows.isEmpty()) {
            break;
        }
        candidates = result.true_rows | result.null_rows;
    }
    return result;
}

ScorerPtr materialize_scorer(ScorerPtr source, bool enable_scoring,
                             const NullBitmapResolver* resolver,
                             const roaring::Roaring* candidates = nullptr);

ScorerPtr make_nullable_conjunction(const std::vector<ScorerPtr>& sources, bool enable_scoring,
                                    uint32_t row_count, const NullBitmapResolver* resolver);

ScorerPtr make_complete_truth_scorer(ScorerPtr scorer, index_query::TruthSet truth);

ScorerPtr make_complete_null_scorer(ScorerPtr scorer, roaring::Roaring null_rows);

// Equal UNKNOWN sets stay UNKNOWN under AND, OR and a valid minimum-match threshold.
std::optional<roaring::Roaring> shared_null_bitmap(
        std::initializer_list<std::span<const ScorerPtr>> groups,
        const NullBitmapResolver* resolver);

ScorerPtr make_truth_set_scorer(index_query::TruthSet truth, std::vector<ScorerPtr> score_sources,
                                bool enable_scoring);

// Materializes forward-only children only when complete null evaluation needs their TRUE sets.
// Replaced children preserve their scores and restart at the first matching document.
roaring::Roaring complete_null_bitmap(std::vector<ScorerPtr>& children, bool intersection,
                                      bool enable_scoring, const NullBitmapResolver* resolver);

} // namespace doris::segment_v2::inverted_index::query_v2
