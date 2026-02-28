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

#include <algorithm>
#include <cstdint>
#include <vector>

namespace doris::segment_v2::inverted_index::query_v2 {

inline uint32_t estimate_intersection(const std::vector<uint32_t>& docset_sizes,
                                      uint32_t max_docs) {
    if (max_docs == 0 || docset_sizes.empty()) {
        return 0;
    }

    double co_loc_factor = 1.3;

    auto intersection_estimate = static_cast<double>(docset_sizes.front());
    double smallest_docset_size = intersection_estimate;

    for (size_t i = 1; i < docset_sizes.size(); ++i) {
        co_loc_factor = std::max(co_loc_factor - 0.1, 1.0);
        intersection_estimate *=
                (static_cast<double>(docset_sizes[i]) / static_cast<double>(max_docs)) *
                co_loc_factor;
        smallest_docset_size = std::min(smallest_docset_size, static_cast<double>(docset_sizes[i]));
    }

    return static_cast<uint32_t>(std::min(std::round(intersection_estimate), smallest_docset_size));
}

} // namespace doris::segment_v2::inverted_index::query_v2
