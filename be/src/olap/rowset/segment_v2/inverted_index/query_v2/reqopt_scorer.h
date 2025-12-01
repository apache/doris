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

#include <optional>

#include "olap/rowset/segment_v2/inverted_index/query_v2/score_combiner.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/scorer.h"

namespace doris::segment_v2::inverted_index::query_v2 {

template <typename TScoreCombiner>
class RequiredOptionalScorer final : public Scorer {
public:
    RequiredOptionalScorer(ScorerPtr req_scorer, ScorerPtr opt_scorer, TScoreCombiner combiner)
            : _req_scorer(std::move(req_scorer)),
              _opt_scorer(std::move(opt_scorer)),
              _combiner(std::move(combiner)) {}

    ~RequiredOptionalScorer() override = default;

    uint32_t advance() override {
        _score_cache.reset();
        return _req_scorer->advance();
    }

    uint32_t seek(uint32_t target) override {
        _score_cache.reset();
        return _req_scorer->seek(target);
    }

    uint32_t doc() const override { return _req_scorer->doc(); }

    uint32_t size_hint() const override { return _req_scorer->size_hint(); }

    float score() override {
        if (_score_cache.has_value()) {
            return _score_cache.value();
        }
        uint32_t current_doc = doc();
        auto score_combiner = _combiner->clone();
        score_combiner->update(_req_scorer);
        if (_opt_scorer->doc() <= current_doc && _opt_scorer->seek(current_doc) == current_doc) {
            score_combiner->update(_opt_scorer);
        }
        float combined_score = score_combiner->score();
        _score_cache = combined_score;
        return combined_score;
    }

private:
    ScorerPtr _req_scorer;
    ScorerPtr _opt_scorer;
    TScoreCombiner _combiner;
    std::optional<float> _score_cache;
};

template <typename TScoreCombiner>
auto make_required_optional_scorer(ScorerPtr req_scorer, ScorerPtr opt_scorer,
                                   TScoreCombiner combiner) {
    return std::make_shared<RequiredOptionalScorer<TScoreCombiner>>(
            std::move(req_scorer), std::move(opt_scorer), std::move(combiner));
}

} // namespace doris::segment_v2::inverted_index::query_v2