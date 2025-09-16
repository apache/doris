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

#include <vector>

#include "olap/rowset/segment_v2/inverted_index/query_v2/scorer.h"

namespace doris::segment_v2::inverted_index::query_v2 {

ScorerPtr intersection_scorer_build(std::vector<ScorerPtr>& scorers);

template <typename PivotScorerPtr>
class IntersectionScorer final : public Scorer {
public:
    IntersectionScorer(PivotScorerPtr left, PivotScorerPtr right, std::vector<ScorerPtr> others);
    ~IntersectionScorer() override = default;

    uint32_t advance() override;
    uint32_t seek(uint32_t target) override;
    uint32_t doc() const override;
    uint32_t size_hint() const override;

    float score() override;

private:
    PivotScorerPtr _left;
    PivotScorerPtr _right;
    std::vector<ScorerPtr> _others;
};

} // namespace doris::segment_v2::inverted_index::query_v2