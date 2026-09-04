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
#include "storage/index/inverted/query_v2/bit_set_query/bit_set_weight.h"
#include "storage/index/inverted/query_v2/query.h"
#include "storage/index/inverted/query_v2/scored_bit_set_query/scored_bit_set_weight.h"

namespace doris::segment_v2::inverted_index::query_v2 {

// A pre-resolved doc set that also carries a relevance score per document.
//
// This is BitSetQuery plus scores, and exists separately rather than as an option on BitSetQuery
// because BitSetQuery's constant 1.0 is depended on by the CLucene/V3 leaves and by non-scoring
// uses; widening its contract would change their behaviour silently.
//
// Built by the SEARCH leaf builder for clauses the SNII native reader answers with BM25: that
// reader scores inside its own query() call, so by the time the query tree is assembled the
// scores already exist and only need carrying to the scorer.
class ScoredBitSetQuery : public Query {
public:
    ScoredBitSetQuery(std::shared_ptr<roaring::Roaring> bitmap,
                      std::shared_ptr<roaring::Roaring> null_bitmap, ScoredBitSetMapPtr scores)
            : _bitmap(std::move(bitmap)),
              _null_bitmap(std::move(null_bitmap)),
              _scores(std::move(scores)) {
        DCHECK(_scores != nullptr);
    }
    ~ScoredBitSetQuery() override = default;

    WeightPtr weight(bool enable_scoring) override {
        if (!enable_scoring) {
            // A non-scoring execution never calls score(), so give it the plain doc-set weight
            // instead of making it carry the score map through every scorer it builds.
            return std::make_shared<BitSetWeight>(_bitmap, _null_bitmap);
        }
        return std::make_shared<ScoredBitSetWeight>(_bitmap, _null_bitmap, _scores);
    }

private:
    std::shared_ptr<roaring::Roaring> _bitmap;
    std::shared_ptr<roaring::Roaring> _null_bitmap;
    ScoredBitSetMapPtr _scores;
};

using ScoredBitSetQueryPtr = std::shared_ptr<ScoredBitSetQuery>;

} // namespace doris::segment_v2::inverted_index::query_v2
