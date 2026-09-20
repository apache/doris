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

#include "storage/index/inverted/query_v2/boolean_query/occur_boolean_weight.h"

#include <algorithm>

#include "core/custom_allocator.h"
#include "storage/index/inverted/query_v2/all_query/all_query.h"
#include "storage/index/inverted/query_v2/complete_null_bitmap.h"
#include "storage/index/inverted/query_v2/disjunction_scorer.h"
#include "storage/index/inverted/query_v2/exclude_scorer.h"
#include "storage/index/inverted/query_v2/intersection.h"
#include "storage/index/inverted/query_v2/intersection_scorer.h"
#include "storage/index/inverted/query_v2/reqopt_scorer.h"
#include "storage/index/inverted/query_v2/union/buffered_union.h"

namespace doris::segment_v2::inverted_index::query_v2 {

template <typename ScoreCombinerPtrT>
OccurBooleanWeight<ScoreCombinerPtrT>::OccurBooleanWeight(
        std::vector<std::pair<Occur, WeightPtr>> sub_weights, std::vector<std::string> binding_keys,
        size_t minimum_number_should_match, bool enable_scoring, ScoreCombinerPtrT score_combiner)
        : _sub_weights(std::move(sub_weights)),
          _binding_keys(std::move(binding_keys)),
          _minimum_number_should_match(minimum_number_should_match),
          _enable_scoring(enable_scoring),
          _score_combiner(std::move(score_combiner)) {
    DCHECK(_binding_keys.empty() || _binding_keys.size() == _sub_weights.size())
            << "binding_keys size (" << _binding_keys.size() << ") must match sub_weights size ("
            << _sub_weights.size() << ") when non-empty";
    // Ensure binding_keys has the same size as sub_weights (pads with empty strings if needed).
    _binding_keys.resize(_sub_weights.size());
}

template <typename ScoreCombinerPtrT>
ScorerPtr OccurBooleanWeight<ScoreCombinerPtrT>::scorer(const QueryExecutionContext& context) {
    return scorer(context, {});
}

template <typename ScoreCombinerPtrT>
ScorerPtr OccurBooleanWeight<ScoreCombinerPtrT>::scorer(const QueryExecutionContext& context,
                                                        const std::string& binding_key) {
    if (_sub_weights.empty()) {
        return std::make_shared<EmptyScorer>();
    }
    if (_sub_weights.size() == 1) {
        const auto& [occur, weight] = _sub_weights[0];
        const size_t should_count = occur == Occur::SHOULD ? 1 : 0;
        if (occur == Occur::MUST_NOT || _minimum_number_should_match > should_count) {
            return std::make_shared<EmptyScorer>();
        }
        return weight->scorer(context, binding_key);
    }
    _max_doc = context.segment_num_rows;
    roaring::Roaring null_rows;
    if (_enable_scoring) {
        auto specialized = complex_scorer(context, _score_combiner, binding_key, &null_rows);
        return make_complete_null_scorer(into_box_scorer(std::move(specialized), _score_combiner),
                                         std::move(null_rows));
    } else {
        auto combiner = std::make_shared<DoNothingCombiner>();
        auto specialized = complex_scorer(context, combiner, binding_key, &null_rows);
        return make_complete_null_scorer(into_box_scorer(std::move(specialized), combiner),
                                         std::move(null_rows));
    }
}

template <typename ScoreCombinerPtrT>
std::unordered_map<Occur, std::vector<ScorerPtr>>
OccurBooleanWeight<ScoreCombinerPtrT>::per_occur_scorers(const QueryExecutionContext& context,
                                                         const std::string& binding_key) {
    std::unordered_map<Occur, std::vector<ScorerPtr>> result;
    for (size_t i = 0; i < _sub_weights.size(); ++i) {
        const auto& [occur, weight] = _sub_weights[i];
        const auto& key = _binding_keys[i].empty() ? binding_key : _binding_keys[i];
        auto sub_scorer = weight->scorer(context, key);
        if (sub_scorer) {
            result[occur].push_back(std::move(sub_scorer));
        }
    }
    return result;
}

template <typename ScoreCombinerPtrT>
AllAndEmptyScorerCounts
OccurBooleanWeight<ScoreCombinerPtrT>::remove_and_count_all_and_empty_scorers(
        std::vector<ScorerPtr>& scorers, bool preserve_all) {
    AllAndEmptyScorerCounts counts;
    auto it = scorers.begin();
    while (it != scorers.end()) {
        if (!preserve_all && dynamic_cast<AllScorer*>(it->get()) != nullptr) {
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
ScorerPtr OccurBooleanWeight<ScoreCombinerPtrT>::build_nullable_scorer(
        std::vector<ScorerPtr>& required, std::vector<ScorerPtr>& optional,
        std::vector<ScorerPtr>& excluded, const NullBitmapResolver* resolver) {
    if ((required.empty() && optional.empty()) || _minimum_number_should_match > optional.size()) {
        return std::make_shared<EmptyScorer>();
    }
    if (required.size() == 1 && _minimum_number_should_match == 0 && excluded.empty()) {
        if (!_enable_scoring || optional.empty()) {
            return std::move(required.front());
        }
        auto optional_scorer = scorer_union(std::move(optional), _score_combiner);
        return make_required_optional_scorer(
                std::move(required.front()),
                into_box_scorer(std::move(optional_scorer), _score_combiner), _score_combiner);
    }
    const auto has_nulls = [resolver](const std::vector<ScorerPtr>& scorers) {
        return std::ranges::any_of(scorers, [resolver](const ScorerPtr& scorer) {
            return scorer->has_null_bitmap(resolver);
        });
    };
    if (!has_nulls(required) && !has_nulls(optional) && !has_nulls(excluded)) {
        return nullptr;
    }

    if (!required.empty() && optional.empty() && excluded.empty()) {
        return make_nullable_conjunction(required, _enable_scoring, _max_doc, resolver);
    }

    std::vector<ScorerPtr> score_sources;
    if (_enable_scoring) {
        score_sources.reserve(required.size() + optional.size());
    }
    const auto collect_positive = [&](ScorerPtr& scorer,
                                      const roaring::Roaring* candidates = nullptr) {
        if (_enable_scoring) {
            scorer = materialize_scorer(std::move(scorer), true, resolver, candidates);
            score_sources.push_back(scorer);
        }
        return collect_truth_set(scorer, resolver, candidates);
    };
    index_query::TruthSet result;
    result.true_rows.addRange(0, _max_doc);
    std::optional<roaring::Roaring> candidates;
    for (auto& scorer : required) {
        result.intersect_with(collect_positive(scorer, candidates ? &*candidates : nullptr));
        if (result.true_rows.isEmpty() && result.null_rows.isEmpty()) {
            return std::make_shared<EmptyScorer>();
        }
        candidates = result.true_rows | result.null_rows;
    }
    DorisVector<index_query::TruthSet> should_results;
    should_results.reserve(optional.size());
    for (auto& scorer : optional) {
        should_results.push_back(collect_positive(scorer));
    }
    size_t minimum = _minimum_number_should_match;
    if (required.empty() && minimum == 0) {
        minimum = 1;
    }
    result.intersect_with(index_query::truth_at_least(should_results, minimum, _max_doc));
    index_query::TruthSet negative;
    for (const auto& scorer : excluded) {
        negative.union_with(collect_truth_set(scorer, resolver));
    }
    result.exclude(negative);
    if (_enable_scoring && required.empty() && minimum == 1 && excluded.empty()) {
        auto scorer = make_buffered_union(score_sources, _score_combiner);
        return make_complete_truth_scorer(std::move(scorer), std::move(result));
    }
    return make_truth_set_scorer(std::move(result), std::move(score_sources), _enable_scoring);
}

template <typename ScoreCombinerPtrT>
template <typename CombinerT>
SpecializedScorer OccurBooleanWeight<ScoreCombinerPtrT>::complex_scorer(
        const QueryExecutionContext& context, CombinerT combiner, const std::string& binding_key,
        roaring::Roaring* complete_nulls) {
    auto scorers_by_occur = per_occur_scorers(context, binding_key);
    auto must_scorers = std::move(scorers_by_occur[Occur::MUST]);
    auto should_scorers = std::move(scorers_by_occur[Occur::SHOULD]);
    auto must_not_scorers = std::move(scorers_by_occur[Occur::MUST_NOT]);

    const auto shared = shared_null_bitmap({must_scorers, should_scorers, must_not_scorers},
                                           context.null_resolver);
    const bool valid_positive = (!must_scorers.empty() || !should_scorers.empty()) &&
                                _minimum_number_should_match <= should_scorers.size();
    if (shared.has_value() && valid_positive) {
        if (complete_nulls != nullptr) {
            *complete_nulls = *shared;
        }
    } else if (auto scorer = build_nullable_scorer(must_scorers, should_scorers, must_not_scorers,
                                                   context.null_resolver);
               scorer != nullptr) {
        return scorer;
    }

    auto must_special_counts =
            remove_and_count_all_and_empty_scorers(must_scorers, _enable_scoring);
    auto should_special_counts =
            remove_and_count_all_and_empty_scorers(should_scorers, _enable_scoring);
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

    // Collect null bitmaps from MUST_NOT scorers (read from index, no iteration needed)
    // and union the scorers into one for lazy exclusion.
    roaring::Roaring exclude_null;
    ScorerPtr exclude_opt =
            build_exclude_opt(std::move(must_not_scorers), context.null_resolver, exclude_null);

    SpecializedScorer positive_opt =
            build_positive_opt(*should_opt, std::move(must_scorers), combiner, must_special_counts,
                               should_special_counts);
    // Use null-bitmap-aware ExcludeScorer for MUST_NOT clauses.
    // ExcludeScorer keeps lazy TRUE exclusion via seek-based iteration and adds
    // O(1) null bitmap checks so that NOT(NULL) = NULL (SQL three-valued logic).
    // Documents where the excluded field is NULL are placed in the null bitmap
    // rather than being incorrectly included in the true result set.
    if (exclude_opt) {
        ScorerPtr positive_boxed = into_box_scorer(std::move(positive_opt), combiner);
        return make_exclude(std::move(positive_boxed), std::move(exclude_opt),
                            std::move(exclude_null), context.null_resolver);
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

template <typename ScoreCombinerPtrT>
ScorerPtr OccurBooleanWeight<ScoreCombinerPtrT>::build_exclude_opt(
        std::vector<ScorerPtr> must_not_scorers, const NullBitmapResolver* resolver,
        roaring::Roaring& exclude_null_out) {
    if (must_not_scorers.empty()) {
        return nullptr;
    }

    // Collect null bitmaps before union (read from index, no iteration needed).
    for (auto& s : must_not_scorers) {
        if (resolver != nullptr && s && s->has_null_bitmap(resolver)) {
            const auto* nb = s->get_null_bitmap(resolver);
            if (nb != nullptr) {
                exclude_null_out |= *nb;
            }
        }
    }

    // Union all MUST_NOT scorers into one for lazy seek-based exclusion.
    auto do_nothing = std::make_shared<DoNothingCombiner>();
    auto specialized = scorer_union(std::move(must_not_scorers), do_nothing);
    return into_box_scorer(std::move(specialized), do_nothing);
}

template class OccurBooleanWeight<SumCombinerPtr>;
template class OccurBooleanWeight<DoNothingCombinerPtr>;

} // namespace doris::segment_v2::inverted_index::query_v2