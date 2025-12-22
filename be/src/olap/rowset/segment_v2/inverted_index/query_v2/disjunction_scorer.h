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

#include <queue>
#include <vector>

#include "olap/rowset/segment_v2/inverted_index/query_v2/scorer.h"

namespace doris::segment_v2::inverted_index::query_v2 {

template <typename ScoreCombinerPtrT>
class DisjunctionScorer : public Scorer {
public:
    DisjunctionScorer(std::vector<ScorerPtr> scorers, ScoreCombinerPtrT score_combiner,
                      size_t minimum_matches_required);
    ~DisjunctionScorer() override = default;

    uint32_t advance() override;
    uint32_t seek(uint32_t target) override;
    uint32_t doc() const override { return _current_doc; }
    uint32_t size_hint() const override;
    float score() override { return _current_score; }

private:
    struct ScorerWrapper {
        ScorerPtr scorer;
        uint32_t current_doc;

        ScorerWrapper(ScorerPtr s) : scorer(std::move(s)), current_doc(scorer->doc()) {}

        bool operator>(const ScorerWrapper& other) const { return current_doc > other.current_doc; }
    };

    void do_advance();

    std::priority_queue<ScorerWrapper, std::vector<ScorerWrapper>, std::greater<ScorerWrapper>>
            _heap;
    size_t _minimum_matches_required;
    ScoreCombinerPtrT _score_combiner;

    uint32_t _current_doc = TERMINATED;
    float _current_score = 0.0F;
    uint32_t _size_hint = 0;
};

template <typename ScoreCombinerPtrT>
ScorerPtr make_disjunction(std::vector<ScorerPtr> scorers, ScoreCombinerPtrT score_combiner,
                           size_t minimum_matches_required);

} // namespace doris::segment_v2::inverted_index::query_v2