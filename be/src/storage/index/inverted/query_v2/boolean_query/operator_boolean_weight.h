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

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/check.h"
#include "storage/index/inverted/query_v2/bit_set_query/bit_set_scorer.h"
#include "storage/index/inverted/query_v2/boolean_query/listed_terms.h"
#include "storage/index/inverted/query_v2/boolean_query/operator.h"
#include "storage/index/inverted/query_v2/buffered_union_scorer.h"
#include "storage/index/inverted/query_v2/complete_null_bitmap.h"
#include "storage/index/inverted/query_v2/doc_set.h"
#include "storage/index/inverted/query_v2/intersection_scorer.h"
#include "storage/index/inverted/query_v2/match_all_docs_scorer.h"
#include "storage/index/inverted/query_v2/null_bitmap_fetcher.h"
#include "storage/index/inverted/query_v2/term_query/term_weight.h"
#include "storage/index/inverted/query_v2/weight.h"

namespace doris::segment_v2::inverted_index::query_v2 {

template <typename ScoreCombinerPtrT>
class OperatorBooleanWeight : public Weight {
public:
    OperatorBooleanWeight(OperatorType type, std::vector<WeightPtr> sub_weights,
                          std::vector<std::string> binding_keys, ScoreCombinerPtrT score_combiner)
            : _type(type),
              _sub_weights(std::move(sub_weights)),
              _binding_keys(std::move(binding_keys)),
              _score_combiner(std::move(score_combiner)) {}
    ~OperatorBooleanWeight() override = default;

    ScorerPtr scorer(const QueryExecutionContext& context) override {
        if (_is_do_nothing_combiner()) {
            return build_three_value_scorer(context);
        }
        const auto make_empty = []() -> ScorerPtr { return std::make_shared<EmptyScorer>(); };
        // The term clauses reading a batching source are listed and scored as groups.
        std::vector<ListedTerms> groups;
        if (_type != OperatorType::OP_NOT) {
            groups = listed_terms(context, /*scoring=*/true);
        }

        switch (_type) {
        case OperatorType::OP_AND: {
            auto [include_scorers, exclude_scorers] = collect_and_scorers(context, groups);
            ScorerPtr base_scorer;
            if (include_scorers.empty()) {
                uint32_t max_doc = context.segment_num_rows;
                if (max_doc == 0) {
                    return make_empty();
                }
                base_scorer = std::make_shared<MatchAllDocsScorer>(max_doc, context.sources);
            } else {
                base_scorer = intersection_scorer_build(std::move(include_scorers),
                                                        !_is_do_nothing_combiner(),
                                                        context.null_resolver);
            }

            if (exclude_scorers.empty()) {
                return base_scorer;
            }

            return std::make_shared<AndNotScorer>(
                    std::move(base_scorer), std::move(exclude_scorers), context.null_resolver);
        }
        case OperatorType::OP_NOT: {
            uint32_t max_doc = context.segment_num_rows;
            if (max_doc == 0) {
                return make_empty();
            }
            auto match_all = std::make_shared<MatchAllDocsScorer>(max_doc, context.sources);
            if (_sub_weights.empty()) {
                return match_all;
            }
            auto excludes = per_scorers(context);
            if (excludes.empty()) {
                return match_all;
            }
            return std::make_shared<AndNotScorer>(std::move(match_all), std::move(excludes),
                                                  context.null_resolver);
        }
        case OperatorType::OP_OR: {
            auto sub_scorers = per_scorers(context, groups);
            if (sub_scorers.empty()) {
                return make_empty();
            }
            // One clause, or one listed group, is the disjunction itself.
            if (sub_scorers.size() == 1) {
                return std::move(sub_scorers.front());
            }
            return buffered_union_scorer_build<ScoreCombinerPtrT>(
                    std::move(sub_scorers), _score_combiner, context.segment_num_rows,
                    context.null_resolver);
        }
        default:
            return make_empty();
        }
    }

private:
    // The clauses' scorers to intersect and to exclude, the listed groups each as one scorer
    // of its rows' summed scores.
    std::pair<std::vector<ScorerPtr>, std::vector<ScorerPtr>> collect_and_scorers(
            const QueryExecutionContext& context, std::vector<ListedTerms>& groups) {
        std::pair<std::vector<ScorerPtr>, std::vector<ScorerPtr>> result;
        result.first.reserve(_sub_weights.size());
        result.second.reserve(_sub_weights.size());

        for (size_t i = 0; i < _sub_weights.size(); ++i) {
            if (is_listed(groups, i)) {
                continue;
            }
            const auto& sub_weight = _sub_weights[i];
            const auto& binding_key = _binding_keys[i];
            auto boolean_weight =
                    std::dynamic_pointer_cast<OperatorBooleanWeight<ScoreCombinerPtrT>>(sub_weight);
            if (boolean_weight != nullptr && boolean_weight->_type == OperatorType::OP_NOT) {
                auto excludes = boolean_weight->per_scorers(context);
                for (auto& exclude : excludes) {
                    if (exclude != nullptr) {
                        result.second.emplace_back(std::move(exclude));
                    }
                }
                continue;
            }

            auto scorer = sub_weight->scorer(context, binding_key);
            if (scorer != nullptr) {
                result.first.emplace_back(std::move(scorer));
            }
        }
        for (ListedTerms& group : groups) {
            result.first.emplace_back(group.scored_conjunction());
        }

        return result;
    }

    // The clauses' scorers, the listed groups each as one scorer of its rows' summed scores.
    std::vector<ScorerPtr> per_scorers(const QueryExecutionContext& context,
                                       std::vector<ListedTerms>& groups) {
        std::vector<ScorerPtr> sub_scorers;
        sub_scorers.reserve(_sub_weights.size());
        for (size_t i = 0; i < _sub_weights.size(); ++i) {
            if (is_listed(groups, i)) {
                continue;
            }
            auto scorer = _sub_weights[i]->scorer(context, _binding_keys[i]);
            if (scorer != nullptr) {
                sub_scorers.emplace_back(std::move(scorer));
            }
        }
        for (ListedTerms& group : groups) {
            sub_scorers.emplace_back(group.scored_disjunction());
        }
        return sub_scorers;
    }

    std::vector<ScorerPtr> per_scorers(const QueryExecutionContext& context) {
        std::vector<ScorerPtr> sub_scorers;
        sub_scorers.reserve(_sub_weights.size());
        for (size_t i = 0; i < _sub_weights.size(); ++i) {
            auto scorer = _sub_weights[i]->scorer(context, _binding_keys[i]);
            if (scorer != nullptr) {
                sub_scorers.emplace_back(std::move(scorer));
            }
        }
        return sub_scorers;
    }

    bool _is_do_nothing_combiner() const {
        return std::dynamic_pointer_cast<DoNothingCombiner>(_score_combiner) != nullptr;
    }

    // The term clauses to list together: those reading a source that batches its reads, and on
    // any source those of an unscored conjunction, whose chain intersects block by block, and
    // those of a scored disjunction, which merges each term's rows into the rows scored so far
    // instead of keeping a heap of its terms per row; grouped by source and opened together,
    // with their similarities when `scoring` (a boolean scores with its clauses). The other
    // clauses run through their scorers.
    std::vector<ListedTerms> listed_terms(const QueryExecutionContext& context, bool scoring) {
        const bool any_source = (_type == OperatorType::OP_AND && !scoring) ||
                                (_type == OperatorType::OP_OR && scoring);
        std::vector<ListedTerms> groups;
        for (size_t i = 0; i < _sub_weights.size(); ++i) {
            const auto* term = dynamic_cast<const TermWeight*>(_sub_weights[i].get());
            if (term == nullptr) {
                continue;
            }
            DORIS_CHECK(!scoring || term->scores());
            auto source = lookup_source(term->field(), context, _binding_keys[i]);
            if (source == nullptr || !(any_source || source->batches_reads())) {
                continue;
            }
            auto group = std::ranges::find_if(groups, [&source](const ListedTerms& candidate) {
                return candidate.source() == source;
            });
            if (group == groups.end()) {
                auto nulls = FieldNullBitmapFetcher::fetch(
                        context.null_resolver,
                        logical_field_or_fallback(context, _binding_keys[i], term->field()));
                group = groups.insert(groups.end(), ListedTerms(source, std::move(nulls)));
            }
            group->add(i, term->term(), scoring ? term->similarity() : nullptr);
        }
        for (ListedTerms& group : groups) {
            group.open(_type == OperatorType::OP_AND, scoring);
        }
        return groups;
    }

    static bool is_listed(const std::vector<ListedTerms>& groups, size_t clause) {
        return std::ranges::any_of(
                groups, [clause](const ListedTerms& group) { return group.holds(clause); });
    }

    index_query::TruthSet clause_rows(const QueryExecutionContext& context, size_t clause,
                                      const roaring::Roaring* candidates) {
        const WeightPtr& weight = _sub_weights[clause];
        const std::string& binding_key = _binding_keys[clause];
        if (weight->lists_rows(context, binding_key)) {
            return weight->listed_rows(context, binding_key, candidates);
        }
        return collect_truth_set(weight->scorer(context, binding_key), context.null_resolver,
                                 candidates);
    }

    index_query::TruthSet evaluate_children(const QueryExecutionContext& context) {
        auto groups = listed_terms(context, /*scoring=*/false);
        if (_type == OperatorType::OP_AND) {
            return evaluate_conjunction(context, groups);
        }
        index_query::TruthSet result;
        for (ListedTerms& group : groups) {
            result.union_with(group.disjunction());
        }
        for (size_t i = 0; i < _sub_weights.size(); ++i) {
            if (!is_listed(groups, i)) {
                result.union_with(clause_rows(context, i, nullptr));
            }
        }
        if (_type == OperatorType::OP_NOT) {
            result.negate(context.segment_num_rows);
        }
        return result;
    }

    // Cheaper clauses run first, so the others read only the rows they keep: the scorers and
    // the listed groups in cost order, then the clauses listing their own rows on what is left.
    index_query::TruthSet evaluate_conjunction(const QueryExecutionContext& context,
                                               std::vector<ListedTerms>& groups) {
        index_query::TruthSet result;
        if (std::ranges::any_of(groups,
                                [](const ListedTerms& group) { return group.has_absent_term(); })) {
            return result;
        }
        struct Step {
            uint64_t cost = 0;
            ScorerPtr scorer;
            ListedTerms* group = nullptr;
        };
        std::vector<Step> steps;
        std::vector<size_t> listing;
        for (size_t i = 0; i < _sub_weights.size(); ++i) {
            if (is_listed(groups, i)) {
                continue;
            }
            if (_sub_weights[i]->lists_rows(context, _binding_keys[i])) {
                listing.push_back(i);
                continue;
            }
            auto scorer = _sub_weights[i]->scorer(context, _binding_keys[i]);
            DORIS_CHECK(scorer != nullptr);
            steps.push_back(
                    {.cost = scorer->cost(), .scorer = std::move(scorer), .group = nullptr});
        }
        for (ListedTerms& group : groups) {
            steps.push_back(
                    {.cost = group.cheapest_doc_freq(), .scorer = nullptr, .group = &group});
        }
        std::ranges::stable_sort(steps, {}, &Step::cost);
        result.true_rows.addRange(0, context.segment_num_rows);
        roaring::Roaring possible;
        const roaring::Roaring* candidates = nullptr;
        std::vector<uint32_t> listed_candidates;
        // Narrows the result by one clause's rows; true when nothing can match any more.
        const auto narrow = [&](const index_query::TruthSet& rows) {
            result.intersect_with(rows);
            if (result.null_rows.isEmpty()) {
                candidates = &result.true_rows;
            } else {
                possible = result.true_rows | result.null_rows;
                candidates = &possible;
            }
            return result.true_rows.isEmpty() && result.null_rows.isEmpty();
        };
        for (Step& step : steps) {
            bool exhausted = false;
            if (step.group != nullptr) {
                const std::vector<uint32_t>* chain_candidates = nullptr;
                if (candidates != nullptr) {
                    listed_candidates.resize(candidates->cardinality());
                    candidates->toUint32Array(listed_candidates.data());
                    chain_candidates = &listed_candidates;
                }
                exhausted = narrow(step.group->conjunction(chain_candidates));
            } else {
                exhausted =
                        narrow(collect_truth_set(step.scorer, context.null_resolver, candidates));
            }
            if (exhausted) {
                return result;
            }
        }
        for (const size_t clause : listing) {
            if (narrow(clause_rows(context, clause, candidates))) {
                return result;
            }
        }
        return result;
    }

    ScorerPtr build_three_value_scorer(const QueryExecutionContext& context) {
        auto result = evaluate_children(context);
        auto true_ptr = std::make_shared<roaring::Roaring>(std::move(result.true_rows));
        std::shared_ptr<roaring::Roaring> null_ptr;
        if (!result.null_rows.isEmpty()) {
            null_ptr = std::make_shared<roaring::Roaring>(std::move(result.null_rows));
        }
        return std::make_shared<BitSetScorer>(std::move(true_ptr), std::move(null_ptr));
    }

    OperatorType _type;
    std::vector<WeightPtr> _sub_weights;
    std::vector<std::string> _binding_keys;
    ScoreCombinerPtrT _score_combiner;
};

} // namespace doris::segment_v2::inverted_index::query_v2
