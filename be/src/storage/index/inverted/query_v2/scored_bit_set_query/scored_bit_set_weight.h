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
#include <utility>

#include "roaring/roaring.hh"
#include "storage/index/inverted/query_v2/scored_bit_set_query/scored_bit_set_scorer.h"
#include "storage/index/inverted/query_v2/weight.h"

namespace doris::segment_v2::inverted_index::query_v2 {

class ScoredBitSetWeight final : public Weight {
public:
    ScoredBitSetWeight(std::shared_ptr<roaring::Roaring> bitmap,
                       std::shared_ptr<roaring::Roaring> null_bitmap, ScoredBitSetMapPtr scores)
            : _bitmap(std::move(bitmap)),
              _null_bitmap(std::move(null_bitmap)),
              _scores(std::move(scores)) {}
    ~ScoredBitSetWeight() override = default;

    ScorerPtr scorer(const QueryExecutionContext& /*context*/) override {
        if ((_bitmap == nullptr || _bitmap->isEmpty()) &&
            (_null_bitmap == nullptr || _null_bitmap->isEmpty())) {
            return std::make_shared<EmptyScorer>();
        }
        auto bitmap = _bitmap ? _bitmap : std::make_shared<roaring::Roaring>();
        return std::make_shared<ScoredBitSetScorer>(std::move(bitmap), _null_bitmap, _scores);
    }

private:
    std::shared_ptr<roaring::Roaring> _bitmap;
    std::shared_ptr<roaring::Roaring> _null_bitmap;
    ScoredBitSetMapPtr _scores;
};
using ScoredBitSetWeightPtr = std::shared_ptr<ScoredBitSetWeight>;

} // namespace doris::segment_v2::inverted_index::query_v2
