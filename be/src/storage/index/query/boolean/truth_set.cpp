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

#include "storage/index/query/boolean/truth_set.h"

#include <algorithm>

#include "core/custom_allocator.h"

namespace doris::index_query {

TruthSet truth_at_least(std::span<const TruthSet> inputs, size_t minimum_matches,
                        uint32_t row_count) {
    TruthSet result;
    if (minimum_matches == 0) {
        result.true_rows.addRange(0, row_count);
        return result;
    }
    if (minimum_matches > inputs.size()) {
        return result;
    }
    if (minimum_matches == 1) {
        for (const auto& input : inputs) {
            result.union_with(input);
        }
        return result;
    }
    if (minimum_matches == inputs.size()) {
        result.true_rows.addRange(0, row_count);
        for (const auto& input : inputs) {
            result.intersect_with(input);
        }
        return result;
    }

    DorisVector<roaring::Roaring> true_levels(minimum_matches);
    DorisVector<roaring::Roaring> possible_levels(minimum_matches);
    size_t processed = 0;
    for (const auto& input : inputs) {
        const auto possible = input.true_rows | input.null_rows;
        ++processed;
        for (size_t level = std::min(processed, minimum_matches); level > 1; --level) {
            true_levels[level - 1] |= true_levels[level - 2] & input.true_rows;
            possible_levels[level - 1] |= possible_levels[level - 2] & possible;
        }
        true_levels[0] |= input.true_rows;
        possible_levels[0] |= possible;
    }
    result.true_rows = std::move(true_levels.back());
    result.null_rows = std::move(possible_levels.back());
    result.null_rows -= result.true_rows;
    return result;
}

} // namespace doris::index_query
