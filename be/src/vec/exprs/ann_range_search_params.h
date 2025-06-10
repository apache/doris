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

#include <string>

#include "olap/rowset/segment_v2/ann_index_iterator.h"

namespace doris::vectorized {
struct AnnRangeSearchParams {
    bool is_ann_range_search = false;
    bool is_le_or_lt = true;
    size_t src_col_idx = 0;
    int64_t dst_col_idx = -1;
    double radius = 0.0;
    int ef_search = 0;
    std::unique_ptr<float[]> query_value;

    segment_v2::RangeSearchParams toRangeSearchParams() {
        segment_v2::RangeSearchParams params;
        params.query_value = query_value.get();
        params.radius = static_cast<float>(radius);
        params.roaring = nullptr;
        params.is_le_or_lt = is_le_or_lt;
        return params;
    }

    segment_v2::CustomSearchParams toCustomSearchParams() {
        segment_v2::CustomSearchParams params;
        params.ef_search = ef_search;
        return params;
    }

    std::string to_string() const {
        return fmt::format(
                "is_ann_range_search: {}, is_le_or_lt: {}, src_col_idx: {}, "
                "dst_col_idx: {}, radius: {}, ef_search: {}",
                is_ann_range_search, is_le_or_lt, src_col_idx, dst_col_idx, radius, ef_search);
    }
};
} // namespace doris::vectorized
