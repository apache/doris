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

#include <utility>
#include <vector>

#include "olap/rowset/segment_v2/inverted_index/query_v2/buffered_union_scorer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/intersection_scorer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/match_all_docs_scorer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/operator.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/weight.h"

namespace doris::segment_v2::inverted_index::query_v2 {

template <typename ScoreCombinerPtrT>
class BooleanWeight : public Weight {
public:
    BooleanWeight(OperatorType type, std::vector<WeightPtr> sub_weights,
                  ScoreCombinerPtrT score_combiner)
            : _type(type),
              _sub_weights(std::move(sub_weights)),
              _score_combiner(std::move(score_combiner)) {}
    ~BooleanWeight() override = default;

    ScorerPtr scorer(const CompositeReaderPtr& composite_reader) override {
        const auto make_empty = []() -> ScorerPtr { return std::make_shared<EmptyScorer>(); };

        auto collect_and_scorers = [&]() {
            std::pair<std::vector<ScorerPtr>, std::vector<ScorerPtr>> result;
            result.first.reserve(_sub_weights.size());
            result.second.reserve(_sub_weights.size());

            for (const auto& sub_weight : _sub_weights) {
                auto boolean_weight =
                        std::dynamic_pointer_cast<BooleanWeight<ScoreCombinerPtrT>>(sub_weight);
                if (boolean_weight != nullptr && boolean_weight->_type == OperatorType::OP_NOT) {
                    auto excludes = boolean_weight->per_scorers(composite_reader);
                    for (auto& exclude : excludes) {
                        if (exclude != nullptr) {
                            result.second.emplace_back(std::move(exclude));
                        }
                    }
                    continue;
                }

                auto scorer = sub_weight->scorer(composite_reader);
                if (scorer != nullptr) {
                    result.first.emplace_back(std::move(scorer));
                }
            }

            return result;
        };

        switch (_type) {
        case OperatorType::OP_AND: {
            auto [include_scorers, exclude_scorers] = collect_and_scorers();
            if (include_scorers.empty()) {
                return make_empty();
            }

            auto intersection = intersection_scorer_build(include_scorers);
            if (exclude_scorers.empty()) {
                return intersection;
            }

            return std::make_shared<AndNotScorer>(std::move(intersection),
                                                  std::move(exclude_scorers));
        }
        case OperatorType::OP_NOT: {
            uint32_t max_doc = composite_reader->max_doc();
            if (max_doc == 0) {
                return make_empty();
            }
            auto match_all =
                    std::make_shared<MatchAllDocsScorer>(max_doc, composite_reader->readers());
            if (_sub_weights.empty()) {
                return match_all;
            }
            auto excludes = per_scorers(composite_reader);
            if (excludes.empty()) {
                return match_all;
            }
            return std::make_shared<AndNotScorer>(std::move(match_all), std::move(excludes));
        }
        case OperatorType::OP_OR: {
            auto sub_scorers = per_scorers(composite_reader);
            if (sub_scorers.empty()) {
                return make_empty();
            }
            return buffered_union_scorer_build<ScoreCombinerPtrT>(sub_scorers, _score_combiner);
        }
        default:
            return make_empty();
        }
    }

private:
    std::vector<ScorerPtr> per_scorers(const CompositeReaderPtr& composite_reader) {
        std::vector<ScorerPtr> sub_scorers;
        sub_scorers.reserve(_sub_weights.size());
        for (const auto& sub_weight : _sub_weights) {
            auto scorer = sub_weight->scorer(composite_reader);
            if (scorer != nullptr) {
                sub_scorers.emplace_back(std::move(scorer));
            }
        }
        return sub_scorers;
    }

    OperatorType _type;
    std::vector<WeightPtr> _sub_weights;
    ScoreCombinerPtrT _score_combiner;
};

} // namespace doris::segment_v2::inverted_index::query_v2
