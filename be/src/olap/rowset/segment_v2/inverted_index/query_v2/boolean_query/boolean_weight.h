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

#include "olap/rowset/segment_v2/inverted_index/query_v2/buffered_union_scorer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/intersection_scorer.h"
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

    ScorerPtr scorer(lucene::index::IndexReader* reader) override {
        std::vector<ScorerPtr> sub_scorers = per_scorers(reader);
        if (_type == OperatorType::OP_AND) {
            return intersection_scorer_build(sub_scorers);
        } else if (_type == OperatorType::OP_OR) {
            return buffered_union_scorer_build<ScoreCombinerPtrT>(sub_scorers, _score_combiner);
        }
        return nullptr;
    }

private:
    std::vector<ScorerPtr> per_scorers(lucene::index::IndexReader* reader) {
        std::vector<ScorerPtr> sub_scorers;
        for (const auto& sub_weight : _sub_weights) {
            sub_scorers.emplace_back(sub_weight->scorer(reader));
        }
        return sub_scorers;
    }

    OperatorType _type;
    std::vector<WeightPtr> _sub_weights;
    ScoreCombinerPtrT _score_combiner;
};

} // namespace doris::segment_v2::inverted_index::query_v2