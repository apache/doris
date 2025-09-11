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
#include "olap/rowset/segment_v2/inverted_index/util/tiny_set.h"

namespace doris::segment_v2::inverted_index::query_v2 {

template <typename ScoreCombinerPtrU>
ScorerPtr buffered_union_scorer_build(const std::vector<ScorerPtr>& scorers,
                                      ScoreCombinerPtrU score_combiner);

template <typename ScorerPtrT, typename ScoreCombinerPtrT>
class BufferedUnionScorer : public Scorer {
public:
    BufferedUnionScorer(std::vector<ScorerPtrT> scorers, std::vector<TinySetPtr> bitsets,
                        std::vector<ScoreCombinerPtrT> scores, size_t cursor, uint32_t offset,
                        uint32_t doc);
    ~BufferedUnionScorer() override = default;

    uint32_t advance() override;
    uint32_t seek(uint32_t target) override;
    uint32_t doc() const override;
    uint32_t size_hint() const override;

    float score() override;

    template <typename ScoreCombinerPtrU>
    friend ScorerPtr buffered_union_scorer_build(const std::vector<ScorerPtr>& scorers,
                                                 ScoreCombinerPtrU score_combiner);

private:
    bool refill();
    void refill(std::vector<ScorerPtrT>& scorers, const std::vector<TinySetPtr>& bitsets,
                std::vector<ScoreCombinerPtrT>& scores, uint32_t min_doc);
    bool advance_buffered();

    std::vector<ScorerPtrT> _scorers;
    std::vector<TinySetPtr> _bitsets;
    std::vector<ScoreCombinerPtrT> _scores;
    size_t _cursor = 0;
    uint32_t _offset = 0;
    uint32_t _doc = 0;
    float _score = 0.0F;
};

} // namespace doris::segment_v2::inverted_index::query_v2