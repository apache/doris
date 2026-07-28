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

#include <cstdint>
#include <limits>

namespace doris::segment_v2::inverted_index {

struct CommonGramsPlanRawCost {
    uint64_t posting_bytes_or_df_sum = 0;
    uint64_t estimated_candidate_df = 0;
    uint32_t clause_count = 0;
};

struct CommonGramsPlanCostModel {
    uint32_t position_verify_factor = 0;
    uint32_t common_grams_cost_ratio_percent = 85;
    uint64_t generation = 0;
};

inline uint64_t estimate_common_grams_plan_cost(const CommonGramsPlanRawCost& input,
                                                uint32_t position_verify_factor) {
    const unsigned __int128 estimate =
            static_cast<unsigned __int128>(input.posting_bytes_or_df_sum) +
            static_cast<unsigned __int128>(input.estimated_candidate_df) * position_verify_factor *
                    input.clause_count;
    return estimate > std::numeric_limits<uint64_t>::max() ? std::numeric_limits<uint64_t>::max()
                                                           : static_cast<uint64_t>(estimate);
}

inline bool common_grams_plan_cost_wins(uint64_t plain_cost, uint64_t common_grams_cost,
                                        uint32_t common_grams_cost_ratio_percent) {
    return static_cast<unsigned __int128>(common_grams_cost) * 100 <=
           static_cast<unsigned __int128>(plain_cost) * common_grams_cost_ratio_percent;
}

} // namespace doris::segment_v2::inverted_index
