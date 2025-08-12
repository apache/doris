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

#include "olap/collection_similarity.h"

#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"

namespace doris {
#include "common/compile_check_begin.h"

void CollectionSimilarity::collect(segment_v2::rowid_t row_id, float score) {
    _bm25_scores[row_id] += score;
}

void CollectionSimilarity::get_topn_bm25_scores(roaring::Roaring* row_bitmap,
                                                vectorized::IColumn::MutablePtr& scores,
                                                std::unique_ptr<std::vector<uint64_t>>& row_ids,
                                                OrderType order_type, size_t top_k) const {
    std::vector<std::pair<uint32_t, float>> top_k_results;

    if (order_type == OrderType::DESC) {
        find_top_k_scores(
                _bm25_scores, top_k,
                [](const ScoreMapIterator& a, const ScoreMapIterator& b) {
                    return a->second > b->second;
                },
                top_k_results);
    } else {
        find_top_k_scores(
                _bm25_scores, top_k,
                [](const ScoreMapIterator& a, const ScoreMapIterator& b) {
                    return a->second < b->second;
                },
                top_k_results);
    }

    if (top_k_results.size() < top_k) {
        roaring::Roaring scored_bitmap;
        for (const auto& result : top_k_results) {
            scored_bitmap.add(result.first);
        }

        roaring::Roaring remaining_bitmap = *row_bitmap - scored_bitmap;
        for (uint32_t row_id : remaining_bitmap) {
            top_k_results.emplace_back(row_id, 0.0F);
            if (top_k_results.size() >= top_k) {
                break;
            }
        }
    }

    size_t num_results = top_k_results.size();
    auto score_column = vectorized::ColumnFloat32::create(num_results);
    auto& score_data = score_column->get_data();

    row_ids->resize(num_results);
    roaring::Roaring new_bitmap;

    for (size_t i = 0; i < num_results; ++i) {
        (*row_ids)[i] = top_k_results[i].first;
        score_data[i] = top_k_results[i].second;
        new_bitmap.add(top_k_results[i].first);
    }

    *row_bitmap = std::move(new_bitmap);
    auto null_map = vectorized::ColumnUInt8::create(num_results, 0);
    scores = vectorized::ColumnNullable::create(std::move(score_column), std::move(null_map));
}

template <typename Compare>
void CollectionSimilarity::find_top_k_scores(
        const ScoreMap& all_scores, size_t top_k, Compare comp,
        std::vector<std::pair<uint32_t, float>>& top_k_results) const {
    if (top_k <= 0) {
        return;
    }

    std::priority_queue<ScoreMapIterator, std::vector<ScoreMapIterator>, Compare> top_k_heap(comp);

    for (auto it = all_scores.begin(); it != all_scores.end(); ++it) {
        if (top_k_heap.size() < top_k) {
            top_k_heap.push(it);
        } else if (comp(it, top_k_heap.top())) {
            top_k_heap.pop();
            top_k_heap.push(it);
        }
    }

    top_k_results.reserve(top_k_heap.size());
    while (!top_k_heap.empty()) {
        auto top = top_k_heap.top();
        top_k_results.push_back({top->first, top->second});
        top_k_heap.pop();
    }

    std::reverse(top_k_results.begin(), top_k_results.end());
}

#include "common/compile_check_end.h"
} // namespace doris