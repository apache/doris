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

void CollectionSimilarity::get_bm25_scores(roaring::Roaring* row_bitmap,
                                           vectorized::IColumn::MutablePtr& scores,
                                           std::unique_ptr<std::vector<uint64_t>>& row_ids,
                                           const ScoreRangeFilterPtr& filter) const {
    std::vector<float> filtered_scores;
    filtered_scores.reserve(row_bitmap->cardinality());

    roaring::Roaring new_bitmap;

    for (uint32_t row_id : *row_bitmap) {
        auto it = _bm25_scores.find(row_id);
        float score = (it != _bm25_scores.end()) ? it->second : 0.0F;
        if (filter && !filter->pass(score)) {
            continue;
        }
        row_ids->push_back(row_id);
        filtered_scores.push_back(score);
        new_bitmap.add(row_id);
    }

    size_t num_results = row_ids->size();
    auto score_column = vectorized::ColumnFloat32::create(num_results);
    if (num_results > 0) {
        memcpy(score_column->get_data().data(), filtered_scores.data(),
               num_results * sizeof(float));
    }

    *row_bitmap = std::move(new_bitmap);
    auto null_map = vectorized::ColumnUInt8::create(num_results, 0);
    scores = vectorized::ColumnNullable::create(std::move(score_column), std::move(null_map));
}

void CollectionSimilarity::get_topn_bm25_scores(roaring::Roaring* row_bitmap,
                                                vectorized::IColumn::MutablePtr& scores,
                                                std::unique_ptr<std::vector<uint64_t>>& row_ids,
                                                OrderType order_type, size_t top_k,
                                                const ScoreRangeFilterPtr& filter) const {
    std::vector<std::pair<uint32_t, float>> top_k_results;

    if (order_type == OrderType::DESC) {
        find_top_k_scores<OrderType::DESC>(row_bitmap, _bm25_scores, top_k, top_k_results, filter);
    } else {
        find_top_k_scores<OrderType::ASC>(row_bitmap, _bm25_scores, top_k, top_k_results, filter);
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

template <OrderType order>
void CollectionSimilarity::find_top_k_scores(const roaring::Roaring* row_bitmap,
                                             const ScoreMap& all_scores, size_t top_k,
                                             std::vector<std::pair<uint32_t, float>>& top_k_results,
                                             const ScoreRangeFilterPtr& filter) const {
    if (top_k <= 0) {
        return;
    }

    auto pair_comp = [](const std::pair<uint32_t, float>& a, const std::pair<uint32_t, float>& b) {
        if constexpr (order == OrderType::DESC) {
            return a.second > b.second;
        } else {
            return a.second < b.second;
        }
    };

    std::priority_queue<std::pair<uint32_t, float>, std::vector<std::pair<uint32_t, float>>,
                        decltype(pair_comp)>
            top_k_heap(pair_comp);

    std::vector<uint32_t> zero_score_ids;

    for (uint32_t row_id : *row_bitmap) {
        auto it = all_scores.find(row_id);
        float score = (it != all_scores.end()) ? it->second : 0.0F;

        if (filter && !filter->pass(score)) {
            continue;
        }

        if (score == 0.0F) {
            zero_score_ids.push_back(row_id);
            continue;
        }

        if (top_k_heap.size() < top_k) {
            top_k_heap.emplace(row_id, score);
        } else if (pair_comp({row_id, score}, top_k_heap.top())) {
            top_k_heap.pop();
            top_k_heap.emplace(row_id, score);
        }
    }

    top_k_results.reserve(top_k);
    while (!top_k_heap.empty()) {
        top_k_results.push_back(top_k_heap.top());
        top_k_heap.pop();
    }
    std::ranges::reverse(top_k_results);

    if constexpr (order == OrderType::DESC) {
        // DESC: high scores first, then zeros at the end
        size_t remaining = top_k - top_k_results.size();
        for (size_t i = 0; i < remaining && i < zero_score_ids.size(); ++i) {
            top_k_results.emplace_back(zero_score_ids[i], 0.0F);
        }
    } else {
        // ASC: zeros first, then low scores
        std::vector<std::pair<uint32_t, float>> final_results;
        final_results.reserve(top_k);

        size_t zero_count = std::min(top_k, zero_score_ids.size());
        for (size_t i = 0; i < zero_count; ++i) {
            final_results.emplace_back(zero_score_ids[i], 0.0F);
        }

        size_t remaining = top_k - final_results.size();
        for (size_t i = 0; i < remaining && i < top_k_results.size(); ++i) {
            final_results.push_back(top_k_results[i]);
        }

        top_k_results = std::move(final_results);
    }
}

#include "common/compile_check_end.h"
} // namespace doris