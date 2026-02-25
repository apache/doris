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

#include "olap/rowset/segment_v2/inverted_index/query_v2/boolean_query/occur_boolean_weight.h"

#include "olap/rowset/segment_v2/inverted_index/query_v2/all_query/all_query.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/disjunction_scorer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/exclude_scorer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/intersection.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/reqopt_scorer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/union/buffered_union.h"

namespace doris::segment_v2::inverted_index::query_v2 {

template <typename ScoreCombinerPtrT>
OccurBooleanWeight<ScoreCombinerPtrT>::OccurBooleanWeight(
        std::vector<std::pair<Occur, WeightPtr>> sub_weights, size_t minimum_number_should_match,
        bool enable_scoring, ScoreCombinerPtrT score_combiner)
        : _sub_weights(std::move(sub_weights)),
          _minimum_number_should_match(minimum_number_should_match),
          _enable_scoring(enable_scoring),
          _score_combiner(std::move(score_combiner)) {}

template <typename ScoreCombinerPtrT>
ScorerPtr OccurBooleanWeight<ScoreCombinerPtrT>::scorer(const QueryExecutionContext& context) {
    if (_sub_weights.empty()) {
        return std::make_shared<EmptyScorer>();
    }
    if (_sub_weights.size() == 1) {
        const auto& [occur, weight] = _sub_weights[0];
        if (occur == Occur::MUST_NOT) {
            return std::make_shared<EmptyScorer>();
        }
        return weight->scorer(context);
    }
    _max_doc = context.segment_num_rows;
    if (_enable_scoring) {
        auto specialized = complex_scorer(context, _score_combiner);
        return into_box_scorer(std::move(specialized), _score_combiner);
    } else {
        auto combiner = std::make_shared<DoNothingCombiner>();
        auto specialized = complex_scorer(context, combiner);
        return into_box_scorer(std::move(specialized), combiner);
    }
}

template <typename ScoreCombinerPtrT>
std::unordered_map<Occur, std::vector<ScorerPtr>>
OccurBooleanWeight<ScoreCombinerPtrT>::per_occur_scorers(const QueryExecutionContext& context) {
    std::unordered_map<Occur, std::vector<ScorerPtr>> result;
    for (const auto& [occur, weight] : _sub_weights) {
        auto sub_scorer = weight->scorer(context);
        if (sub_scorer) {
            result[occur].push_back(std::move(sub_scorer));
        }
    }
    return result;
}

template <typename ScoreCombinerPtrT>
AllAndEmptyScorerCounts
OccurBooleanWeight<ScoreCombinerPtrT>::remove_and_count_all_and_empty_scorers(
        std::vector<ScorerPtr>& scorers) {
    AllAndEmptyScorerCounts counts;
    auto it = scorers.begin();
    while (it != scorers.end()) {
        if (dynamic_cast<AllScorer*>(it->get()) != nullptr) {
            counts.num_all_scorers++;
            it = scorers.erase(it);
        } else if (dynamic_cast<EmptyScorer*>(it->get()) != nullptr) {
            counts.num_empty_scorers++;
            it = scorers.erase(it);
        } else {
            ++it;
        }
    }
    return counts;
}

template <typename ScoreCombinerPtrT>
template <typename CombinerT>
std::optional<CombinationMethod> OccurBooleanWeight<ScoreCombinerPtrT>::build_should_opt(
        std::vector<ScorerPtr>& must_scorers, std::vector<ScorerPtr> should_scorers,
        CombinerT combiner, size_t num_all_scorers) {
    size_t adjusted_minimum = _minimum_number_should_match > num_all_scorers
                                      ? _minimum_number_should_match - num_all_scorers
                                      : 0;

    size_t num_of_should_scorers = should_scorers.size();
    if (adjusted_minimum > num_of_should_scorers) {
        return std::nullopt;
    }

    if (adjusted_minimum == 0 && num_of_should_scorers == 0) {
        return Ignored {};
    } else if (adjusted_minimum == 0) {
        return Optional {scorer_union(std::move(should_scorers), combiner)};
    } else if (adjusted_minimum == 1) {
        return Required {scorer_union(std::move(should_scorers), combiner)};
    } else if (adjusted_minimum == num_of_should_scorers) {
        // All SHOULD clauses must match - move them to must_scorers (append, not swap)
        for (auto& scorer : should_scorers) {
            must_scorers.push_back(std::move(scorer));
        }
        return Ignored {};
    } else {
        return Required {scorer_disjunction(std::move(should_scorers), combiner, adjusted_minimum)};
    }
}

template <typename ScoreCombinerPtrT>
ScorerPtr OccurBooleanWeight<ScoreCombinerPtrT>::build_exclude_opt(
        std::vector<ScorerPtr> must_not_scorers) {
    if (must_not_scorers.empty()) {
        return nullptr;
    }
    auto do_nothing = std::make_shared<DoNothingCombiner>();
    auto specialized_scorer = scorer_union(std::move(must_not_scorers), do_nothing);
    return into_box_scorer(std::move(specialized_scorer), do_nothing);
}

template <typename ScoreCombinerPtrT>
ScorerPtr OccurBooleanWeight<ScoreCombinerPtrT>::effective_must_scorer(
        std::vector<ScorerPtr> must_scorers, size_t must_num_all_scorers) {
    if (must_scorers.empty()) {
        if (must_num_all_scorers > 0) {
            return std::make_shared<AllScorer>(_max_doc, _enable_scoring);
        }
        return nullptr;
    }
    return make_intersect_scorers(std::move(must_scorers), _max_doc);
}

template <typename ScoreCombinerPtrT>
template <typename CombinerT>
SpecializedScorer OccurBooleanWeight<ScoreCombinerPtrT>::effective_should_scorer_for_union(
        SpecializedScorer should_scorer, size_t should_num_all_scorers, CombinerT combiner) {
    if (should_num_all_scorers > 0) {
        if (_enable_scoring) {
            std::vector<ScorerPtr> scorers;
            scorers.push_back(into_box_scorer(std::move(should_scorer), combiner));
            scorers.push_back(std::make_shared<AllScorer>(_max_doc, _enable_scoring));
            return make_buffered_union(std::move(scorers), combiner);
        } else {
            return std::make_shared<AllScorer>(_max_doc, _enable_scoring);
        }
    }
    return should_scorer;
}

template <typename ScoreCombinerPtrT>
template <typename CombinerT>
SpecializedScorer OccurBooleanWeight<ScoreCombinerPtrT>::build_positive_opt(
        CombinationMethod& should_opt, std::vector<ScorerPtr> must_scorers, CombinerT combiner,
        const AllAndEmptyScorerCounts& must_special_counts,
        const AllAndEmptyScorerCounts& should_special_counts) {
    size_t num_all_scorers =
            must_special_counts.num_all_scorers + should_special_counts.num_all_scorers;
    if (std::holds_alternative<Ignored>(should_opt)) {
        ScorerPtr must_scorer = effective_must_scorer(std::move(must_scorers), num_all_scorers);
        if (must_scorer) {
            return must_scorer;
        }
        return std::make_shared<EmptyScorer>();
    }

    if (std::holds_alternative<Optional>(should_opt)) {
        auto& opt = std::get<Optional>(should_opt);
        ScorerPtr must_scorer =
                effective_must_scorer(std::move(must_scorers), must_special_counts.num_all_scorers);

        if (!must_scorer) {
            return effective_should_scorer_for_union(
                    std::move(opt.scorer), should_special_counts.num_all_scorers, combiner);
        }

        if (_enable_scoring) {
            auto should_boxed = into_box_scorer(std::move(opt.scorer), combiner);
            return make_required_optional_scorer(must_scorer, should_boxed, combiner);
        } else {
            return must_scorer;
        }
    }

    if (std::holds_alternative<Required>(should_opt)) {
        auto& req = std::get<Required>(should_opt);
        ScorerPtr must_scorer =
                effective_must_scorer(std::move(must_scorers), must_special_counts.num_all_scorers);

        if (!must_scorer) {
            return req.scorer;
        }

        auto should_boxed = into_box_scorer(std::move(req.scorer), combiner);
        std::vector<ScorerPtr> scorers;
        scorers.push_back(std::move(must_scorer));
        scorers.push_back(std::move(should_boxed));
        return make_intersect_scorers(std::move(scorers), _max_doc);
    }

    return std::make_shared<EmptyScorer>();
}

template <typename ScoreCombinerPtrT>
template <typename CombinerT>
SpecializedScorer OccurBooleanWeight<ScoreCombinerPtrT>::complex_scorer(
        const QueryExecutionContext& context, CombinerT combiner) {
    auto scorers_by_occur = per_occur_scorers(context);
    auto must_scorers = std::move(scorers_by_occur[Occur::MUST]);
    auto should_scorers = std::move(scorers_by_occur[Occur::SHOULD]);
    auto must_not_scorers = std::move(scorers_by_occur[Occur::MUST_NOT]);

    auto must_special_counts = remove_and_count_all_and_empty_scorers(must_scorers);
    auto should_special_counts = remove_and_count_all_and_empty_scorers(should_scorers);
    auto exclude_special_counts = remove_and_count_all_and_empty_scorers(must_not_scorers);

    if (must_special_counts.num_empty_scorers > 0) {
        return std::make_shared<EmptyScorer>();
    }

    if (exclude_special_counts.num_all_scorers > 0) {
        return std::make_shared<EmptyScorer>();
    }

    auto should_opt = build_should_opt(must_scorers, std::move(should_scorers), combiner,
                                       should_special_counts.num_all_scorers);
    if (!should_opt.has_value()) {
        return std::make_shared<EmptyScorer>();
    }

    ScorerPtr exclude_opt = build_exclude_opt(std::move(must_not_scorers));
    SpecializedScorer positive_opt =
            build_positive_opt(*should_opt, std::move(must_scorers), combiner, must_special_counts,
                               should_special_counts);
    if (exclude_opt) {
        ScorerPtr positive_boxed = into_box_scorer(std::move(positive_opt), combiner);
        return make_exclude(std::move(positive_boxed), std::move(exclude_opt));
    }
    return positive_opt;
}

template <typename ScoreCombinerPtrT>
template <typename CombinerT>
SpecializedScorer OccurBooleanWeight<ScoreCombinerPtrT>::scorer_union(
        std::vector<ScorerPtr> scorers, CombinerT combiner) {
    if (scorers.empty()) {
        return std::make_shared<EmptyScorer>();
    }

    if (scorers.size() == 1) {
        return std::move(scorers[0]);
    }

    bool is_all_term_scorers = true;
    for (const auto& scorer : scorers) {
        auto* term_scorer = dynamic_cast<TermScorer*>(scorer.get());
        if (term_scorer == nullptr) {
            is_all_term_scorers = false;
            break;
        }
    }
    if (is_all_term_scorers) {
        std::vector<TermScorerPtr> term_scorers;
        term_scorers.reserve(scorers.size());
        for (auto& scorer : scorers) {
            term_scorers.push_back(std::dynamic_pointer_cast<TermScorer>(scorer));
        }
        return term_scorers;
    }

    return make_buffered_union(std::move(scorers), combiner);
}

template <typename ScoreCombinerPtrT>
template <typename CombinerT>
SpecializedScorer OccurBooleanWeight<ScoreCombinerPtrT>::scorer_disjunction(
        std::vector<ScorerPtr> scorers, CombinerT combiner, size_t minimum_match_required) {
    if (scorers.empty()) {
        return std::make_shared<EmptyScorer>();
    }

    if (scorers.size() == 1) {
        return std::move(scorers[0]);
    }

    return make_disjunction(std::move(scorers), combiner, minimum_match_required);
}

template <typename ScoreCombinerPtrT>
template <typename CombinerT>
ScorerPtr OccurBooleanWeight<ScoreCombinerPtrT>::into_box_scorer(SpecializedScorer&& specialized,
                                                                 CombinerT combiner) {
    return std::visit(
            [&](auto&& arg) -> ScorerPtr {
                using T = std::decay_t<decltype(arg)>;
                if constexpr (std::is_same_v<T, std::vector<TermScorerPtr>>) {
                    std::vector<ScorerPtr> scorers;
                    scorers.reserve(arg.size());
                    for (auto& ts : arg) {
                        scorers.push_back(std::move(ts));
                    }
                    return make_buffered_union(std::move(scorers), combiner);
                } else {
                    return std::move(arg);
                }
            },
            std::move(specialized));
}

template class OccurBooleanWeight<SumCombinerPtr>;
template class OccurBooleanWeight<DoNothingCombinerPtr>;

} // namespace doris::segment_v2::inverted_index::query_v2