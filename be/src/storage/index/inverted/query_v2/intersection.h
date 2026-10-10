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

#include <optional>
#include <roaring/roaring.hh>
#include <vector>

#include "storage/index/inverted/query_v2/scorer.h"

namespace doris::segment_v2::inverted_index::query_v2 {

template <typename TDocSet, typename TOtherDocSet = ScorerPtr>
class Intersection;

template <typename TDocSet, typename TOtherDocSet>
using IntersectionPtr = std::shared_ptr<Intersection<TDocSet, TOtherDocSet>>;

template <typename TDocSet, typename TOtherDocSet>
class Intersection final : public Scorer {
public:
    // With `candidates`, which must outlive it, the intersection only stops on rows they hold.
    Intersection(TDocSet left, TDocSet right, std::vector<TOtherDocSet> others, uint32_t num_docs,
                 const roaring::Roaring* candidates = nullptr);
    ~Intersection() override = default;

    template <typename T = TOtherDocSet>
    static std::enable_if_t<std::is_same_v<TDocSet, T>, IntersectionPtr<TDocSet, TDocSet>> create(
            std::vector<TDocSet>& docsets, uint32_t num_docs,
            const roaring::Roaring* candidates = nullptr);

    uint32_t advance() override;
    uint32_t seek(uint32_t target) override;
    uint32_t doc() const override;
    uint32_t size_hint() const override;
    uint64_t cost() const override;
    uint32_t norm() const override;

    float score() override;

private:
    uint32_t intersect_from(uint32_t candidate);
    // Seeks the lead to `target`. Candidates cheaper than the second member then take it on to its
    // first row they hold, so no other member moves to a row outside them.
    uint32_t seek_lead(uint32_t target);
    // Whether the candidates hold `*doc`; if not, the lead moves on to their next row.
    bool check_candidates(uint32_t* doc);
    // The first candidate row at or after `doc`, or TERMINATED.
    uint32_t next_candidate_row(uint32_t doc);

    TDocSet _left;
    TDocSet _right;
    std::vector<TOtherDocSet> _others;
    std::optional<roaring::Roaring::const_iterator> _candidate_rows;
    // The candidates' rank among the members by cost (the lead is 0, the second member 1). Below 2
    // they move with the lead; otherwise they are checked before the member of that rank, or last.
    size_t _candidates_at = 0;
    bool _candidates_lead = false;

    uint32_t _num_docs = 0;
};

ScorerPtr make_intersect_scorers(std::vector<ScorerPtr> scorers, uint32_t num_docs);

template <typename TDocSet>
auto make_intersection(std::vector<TDocSet> docsets, uint32_t num_docs,
                       const roaring::Roaring* candidates = nullptr) {
    return Intersection<TDocSet, TDocSet>::create(docsets, num_docs, candidates);
}

} // namespace doris::segment_v2::inverted_index::query_v2