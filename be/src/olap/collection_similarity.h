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

#include "rowset/segment_v2/common.h"
#include "vec/columns/column.h"

namespace doris {

using ScoreMap = phmap::flat_hash_map<segment_v2::rowid_t, float>;

class CollectionSimilarity {
public:
    CollectionSimilarity() { _bm25_scores.reserve(1024 * 16); }
    ~CollectionSimilarity() = default;

    void collect(segment_v2::rowid_t row_id, float score);
    const ScoreMap& get_bm25_scores() const { return _bm25_scores; }

private:
    ScoreMap _bm25_scores;
};
using CollectionSimilarityPtr = std::shared_ptr<CollectionSimilarity>;

} // namespace doris
