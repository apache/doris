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

#include "olap/rowset/segment_v2/ann_index/ann_range_search_runtime.h"

#include <fmt/format.h>

#include "olap/rowset/segment_v2/ann_index/ann_search_params.h"

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"

/**
 * @brief Converts the runtime info to search parameters for execution.
 * 
 * This method creates a AnnRangeSearchParams structure that can be passed
 * to the underlying ANN index implementation for performing the actual
 * range search operation.
 * 
 * @return AnnRangeSearchParams configured with the runtime information
 */
AnnRangeSearchParams AnnRangeSearchRuntime::to_range_search_params() const {
    AnnRangeSearchParams params;
    params.query_value = query_value.get();
    params.radius = static_cast<float>(radius);
    params.roaring = nullptr;
    params.is_le_or_lt = is_le_or_lt;
    return params;
}

/**
 * @brief Generates a human-readable string representation for debugging.
 * 
 * Creates a formatted string containing all the important runtime
 * information including search parameters, column indices, and
 * configuration flags. This is primarily used for logging and
 * debugging purposes.
 * 
 * @return Formatted string with runtime information
 */
std::string AnnRangeSearchRuntime::to_string() const {
    return fmt::format(
            "is_ann_range_search: {}, is_le_or_lt: {}, src_col_idx: {}, "
            "dst_col_idx: {}, metric_type {}, radius: {}, user params: {}, query_vector is null: "
            "{}",
            is_ann_range_search, is_le_or_lt, src_col_idx, dst_col_idx,
            metric_to_string(metric_type), radius, user_params.to_string(), query_value == nullptr);
}
} // namespace doris::segment_v2