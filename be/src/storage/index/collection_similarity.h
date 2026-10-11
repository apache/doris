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

#include <gen_cpp/Opcodes_types.h>

#include "core/column/column.h"
#include "storage/segment/common.h"

namespace doris {

using ScoreMap = phmap::flat_hash_map<segment_v2::rowid_t, float>;
using ScoreMapIterator = ScoreMap::const_iterator;

enum class OrderType {
    ASC,
    DESC,
};

struct ScoreRangeFilter {
    TExprOpcode::type op;
    double threshold;

    bool pass(float score) const {
        return (op == TExprOpcode::GT) ? (score > threshold) : (score >= threshold);
    }
};
using ScoreRangeFilterPtr = std::shared_ptr<ScoreRangeFilter>;

class CollectionSimilarity {
public:
    CollectionSimilarity() { _bm25_scores.reserve(1024); }
    ~CollectionSimilarity() = default;

    void collect(segment_v2::rowid_t row_id, float score);

    // Hands the collected scores over to the caller and leaves this instance empty.
    // A reader that computes per-document scores inside its own query() cannot return them
    // through the query API, so it publishes them into a throwaway CollectionSimilarity that the
    // caller then relocates into a scorer. Moving instead of copying keeps that hand-off free of
    // a full rehash of a map that can hold one entry per matched row.
    ScoreMap release_scores() {
        ScoreMap released = std::move(_bm25_scores);
        // A moved-from flat_hash_map is valid but unspecified, not guaranteed empty, so make the
        // "leaves this instance empty" half of the contract true rather than merely likely.
        _bm25_scores.clear();
        return released;
    }

    void get_bm25_scores(roaring::Roaring* row_bitmap, IColumn::MutablePtr& scores,
                         std::unique_ptr<std::vector<uint64_t>>& row_ids,
                         const ScoreRangeFilterPtr& filter = nullptr) const;

    void get_topn_bm25_scores(roaring::Roaring* row_bitmap, IColumn::MutablePtr& scores,
                              std::unique_ptr<std::vector<uint64_t>>& row_ids, OrderType order_type,
                              size_t top_k, const ScoreRangeFilterPtr& filter = nullptr) const;

private:
    template <OrderType order>
    void find_top_k_scores(const roaring::Roaring* row_bitmap, const ScoreMap& all_scores,
                           size_t top_k, std::vector<std::pair<uint32_t, float>>& top_k_results,
                           const ScoreRangeFilterPtr& filter) const;

    ScoreMap _bm25_scores;
};
using CollectionSimilarityPtr = std::shared_ptr<CollectionSimilarity>;

} // namespace doris
