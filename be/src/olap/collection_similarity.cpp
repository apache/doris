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

void CollectionSimilarity::get_bm25_scores(vectorized::IColumn::MutablePtr& scores,
                                           std::unique_ptr<std::vector<uint64_t>>& row_ids) const {
    size_t num_results = _bm25_scores.size();
    auto score_column = vectorized::ColumnFloat32::create(num_results);

    int32_t i = 0;
    row_ids->resize(num_results);
    for (const auto& [row_id, score] : _bm25_scores) {
        score_column->get_data()[i] = score;
        (*row_ids)[i] = row_id;
        i++;
    }

    scores = std::move(score_column);
}

} // namespace doris