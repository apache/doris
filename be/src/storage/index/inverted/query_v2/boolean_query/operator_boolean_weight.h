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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "storage/index/inverted/query_v2/bit_set_query/bit_set_scorer.h"
#include "storage/index/inverted/query_v2/boolean_query/operator.h"
#include "storage/index/inverted/query_v2/buffered_union_scorer.h"
#include "storage/index/inverted/query_v2/complete_null_bitmap.h"
#include "storage/index/inverted/query_v2/doc_set.h"
#include "storage/index/inverted/query_v2/intersection_scorer.h"
#include "storage/index/inverted/query_v2/match_all_docs_scorer.h"
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

        switch (_type) {
        case OperatorType::OP_AND: {
            auto [include_scorers, exclude_scorers] = collect_and_scorers(context);
            ScorerPtr base_scorer;
            if (include_scorers.empty()) {
                uint32_t max_doc = context.segment_num_rows;
                if (max_doc == 0) {
                    return make_empty();
                }
                base_scorer = std::make_shared<MatchAllDocsScorer>(max_doc, context.readers);
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
            auto match_all = std::make_shared<MatchAllDocsScorer>(max_doc, context.readers);
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
            auto sub_scorers = per_scorers(context);
            if (sub_scorers.empty()) {
                return make_empty();
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
    std::pair<std::vector<ScorerPtr>, std::vector<ScorerPtr>> collect_and_scorers(
            const QueryExecutionContext& context) {
        std::pair<std::vector<ScorerPtr>, std::vector<ScorerPtr>> result;
        result.first.reserve(_sub_weights.size());
        result.second.reserve(_sub_weights.size());

        for (size_t i = 0; i < _sub_weights.size(); ++i) {
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

        return result;
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

    index_query::TruthSet evaluate_children(const QueryExecutionContext& context) {
        index_query::TruthSet result;
        if (_type == OperatorType::OP_AND) {
            result.true_rows.addRange(0, context.segment_num_rows);
        }
        for (size_t i = 0; i < _sub_weights.size(); ++i) {
            auto scorer = _sub_weights[i]->scorer(context, _binding_keys[i]);
            const auto child = collect_truth_set(scorer, context.null_resolver);
            switch (_type) {
            case OperatorType::OP_AND:
                result.intersect_with(child);
                break;
            case OperatorType::OP_OR:
            case OperatorType::OP_NOT:
                result.union_with(child);
                break;
            }
        }
        if (_type == OperatorType::OP_NOT) {
            result.negate(context.segment_num_rows);
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
