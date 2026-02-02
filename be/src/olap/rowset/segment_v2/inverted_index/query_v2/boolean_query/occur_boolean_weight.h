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

#include "olap/rowset/segment_v2/inverted_index/query_v2/boolean_query/occur.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/scorer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/term_query/term_scorer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/weight.h"

namespace doris::segment_v2::inverted_index::query_v2 {

using SpecializedScorer = std::variant<std::vector<TermScorerPtr>, ScorerPtr>;

struct Ignored {};
struct Optional {
    SpecializedScorer scorer;
};
struct Required {
    SpecializedScorer scorer;
};
using CombinationMethod = std::variant<Ignored, Optional, Required>;

struct AllAndEmptyScorerCounts {
    size_t num_all_scorers = 0;
    size_t num_empty_scorers = 0;
};

template <typename ScoreCombinerPtrT>
class OccurBooleanWeight : public Weight {
public:
    OccurBooleanWeight(std::vector<std::pair<Occur, WeightPtr>> sub_weights,
                       size_t minimum_number_should_match, bool enable_scoring,
                       ScoreCombinerPtrT score_combiner);
    ~OccurBooleanWeight() override = default;

    ScorerPtr scorer(const QueryExecutionContext& context) override;

private:
    std::unordered_map<Occur, std::vector<ScorerPtr>> per_occur_scorers(
            const QueryExecutionContext& context);
    AllAndEmptyScorerCounts remove_and_count_all_and_empty_scorers(std::vector<ScorerPtr>& scorers);

    template <typename CombinerT>
    SpecializedScorer complex_scorer(const QueryExecutionContext& context, CombinerT combiner);

    template <typename CombinerT>
    std::optional<CombinationMethod> build_should_opt(std::vector<ScorerPtr>& must_scorers,
                                                      std::vector<ScorerPtr> should_scorers,
                                                      CombinerT combiner, size_t num_all_scorers);
    ScorerPtr build_exclude_opt(std::vector<ScorerPtr> must_not_scorers);

    ScorerPtr effective_must_scorer(std::vector<ScorerPtr> must_scorers,
                                    size_t must_num_all_scorers);

    template <typename CombinerT>
    SpecializedScorer effective_should_scorer_for_union(SpecializedScorer should_scorer,
                                                        size_t should_num_all_scorers,
                                                        CombinerT combiner);

    template <typename CombinerT>
    SpecializedScorer build_positive_opt(CombinationMethod& should_opt,
                                         std::vector<ScorerPtr> must_scorers, CombinerT combiner,
                                         const AllAndEmptyScorerCounts& must_special_counts,
                                         const AllAndEmptyScorerCounts& should_special_counts);

    template <typename CombinerT>
    SpecializedScorer scorer_union(std::vector<ScorerPtr> scorers, CombinerT combiner);
    template <typename CombinerT>
    SpecializedScorer scorer_disjunction(std::vector<ScorerPtr> scorers, CombinerT combiner,
                                         size_t minimum_match_required);

    template <typename CombinerT>
    ScorerPtr into_box_scorer(SpecializedScorer&& specialized, CombinerT combiner);

    std::vector<std::pair<Occur, WeightPtr>> _sub_weights;
    size_t _minimum_number_should_match = 1;
    bool _enable_scoring = false;
    ScoreCombinerPtrT _score_combiner;

    uint32_t _max_doc = 0;
};

} // namespace doris::segment_v2::inverted_index::query_v2