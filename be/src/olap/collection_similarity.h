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
#include "common/compile_check_begin.h"

using ScoreMap = phmap::flat_hash_map<segment_v2::rowid_t, float>;
using ScoreMapIterator = ScoreMap::const_iterator;

enum class OrderType {
    ASC,
    DESC,
};

class CollectionSimilarity {
public:
    CollectionSimilarity() { _bm25_scores.reserve(1024); }
    ~CollectionSimilarity() = default;

    void collect(segment_v2::rowid_t row_id, float score);

    void get_bm25_scores(roaring::Roaring* row_bitmap, vectorized::IColumn::MutablePtr& scores,
                         std::unique_ptr<std::vector<uint64_t>>& row_ids) const;

    void get_topn_bm25_scores(roaring::Roaring* row_bitmap, vectorized::IColumn::MutablePtr& scores,
                              std::unique_ptr<std::vector<uint64_t>>& row_ids, OrderType order_type,
                              size_t top_k) const;

private:
    template <OrderType order, typename Compare>
    void find_top_k_scores(const roaring::Roaring* row_bitmap, const ScoreMap& all_scores,
                           size_t top_k, Compare comp,
                           std::vector<std::pair<uint32_t, float>>& top_k_results) const;

    ScoreMap _bm25_scores;
};
using CollectionSimilarityPtr = std::shared_ptr<CollectionSimilarity>;

#include "common/compile_check_end.h"
} // namespace doris
