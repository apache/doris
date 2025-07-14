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

void CollectionSimilarity::collect(segment_v2::rowid_t row_id, float score) {
    _bm25_scores[row_id] += score;
}

void CollectionSimilarity::get_bm25_scores(const roaring::Roaring& row_bitmap,
                                           vectorized::IColumn::MutablePtr& scores,
                                           std::unique_ptr<std::vector<uint64_t>>& row_ids) const {
    size_t num_results = row_bitmap.cardinality();
    auto score_column = vectorized::ColumnFloat32::create(num_results);
    auto& score_data = score_column->get_data();

    row_ids->resize(num_results);

    int32_t i = 0;
    for (uint32_t row_id : row_bitmap) {
        (*row_ids)[i] = row_id;
        auto it = _bm25_scores.find(row_id);
        if (it != _bm25_scores.end()) {
            score_data[i] = it->second;
        } else {
            score_data[i] = 0.0;
        }
        i++;
    }

    scores = std::move(score_column);

    // MonotonicStopWatch sw;
    // sw.start();

    // LOG(ERROR) << "--- 1 ---";

    // constexpr size_t top_k = 10;

    // using ScoreMapIterator = ScoreMap::const_iterator;
    // auto compare_score = [](const ScoreMapIterator& a, const ScoreMapIterator& b) {
    //     return a->second > b->second;
    // };
    // std::priority_queue<ScoreMapIterator, std::vector<ScoreMapIterator>, decltype(compare_score)>
    //         top_k_heap(compare_score);

    // for (auto it = _bm25_scores.begin(); it != _bm25_scores.end(); ++it) {
    //     if (top_k_heap.size() < top_k) {
    //         top_k_heap.push(it);
    //     } else if (it->second > top_k_heap.top()->second) {
    //         top_k_heap.pop();
    //         top_k_heap.push(it);
    //     }
    // }

    // size_t num_results = row_bitmap.cardinality();
    // auto score_column = vectorized::ColumnFloat32::create(num_results);
    // auto& score_data = score_column->get_data();

    // std::unordered_map<uint32_t, float> top_k_scores;
    // while (!top_k_heap.empty()) {
    //     auto top = top_k_heap.top();
    //     top_k_scores[top->first] = top->second;
    //     top_k_heap.pop();
    // }

    // LOG(ERROR) << "--- 2 ---";

    // int32_t i = 0;
    // row_ids->resize(num_results);
    // for (uint32_t row_id : row_bitmap) {
    //     (*row_ids)[i] = row_id;
    //     auto it = top_k_scores.find(row_id);
    //     if (it != top_k_scores.end()) {
    //         score_data[i] = it->second;
    //     } else {
    //         score_data[i] = 0.0;
    //     }
    //     i++;
    // }

    // LOG(ERROR) << "--- 3 ---: " << score_column->size() << ", " << row_ids->size() << ", "
    //            << sw.elapsed_time();

    // scores = std::move(score_column);
}

} // namespace doris