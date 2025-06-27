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

#include "vec/runtime/vector_search_user_params.h"
#include "vector/metric.h"

namespace doris::vectorized {
struct RangeSearchParams;

struct RangeSearchRuntimeInfo {
    // DefaultConstructor
    RangeSearchRuntimeInfo()
            : is_ann_range_search(false),
              is_le_or_lt(true),
              src_col_idx(0),
              dst_col_idx(-1),
              radius(0.0),
              metric_type(segment_v2::Metric::UNKNOWN) {
        query_value = nullptr;
    }

    // CopyConstructor
    RangeSearchRuntimeInfo(const RangeSearchRuntimeInfo& other)
            : is_ann_range_search(other.is_ann_range_search),
              is_le_or_lt(other.is_le_or_lt),
              src_col_idx(other.src_col_idx),
              dim(other.dim),
              dst_col_idx(other.dst_col_idx),
              radius(other.radius),
              metric_type(other.metric_type),
              user_params(other.user_params) {
        // Do deep copy to query_value.
        if (other.query_value) {
            query_value = std::make_unique<float[]>(other.dim);
            std::copy(other.query_value.get(), other.query_value.get() + other.dim,
                      query_value.get());
        } else {
            query_value = nullptr;
        }
    }

    RangeSearchRuntimeInfo& operator=(const RangeSearchRuntimeInfo& other) {
        is_ann_range_search = other.is_ann_range_search;
        is_le_or_lt = other.is_le_or_lt;
        src_col_idx = other.src_col_idx;
        dst_col_idx = other.dst_col_idx;
        radius = other.radius;
        metric_type = other.metric_type;
        user_params = other.user_params;
        dim = other.dim;
        // Do deep copy to query_value.
        if (other.query_value) {
            query_value = std::make_unique<float[]>(other.dim);
            std::copy(other.query_value.get(), other.query_value.get() + other.dim,
                      query_value.get());
        } else {
            query_value = nullptr;
        }
        return *this;
    }

    RangeSearchParams to_range_search_params() const;

    std::string to_string() const;

    bool is_ann_range_search = false;
    bool is_le_or_lt = true;
    size_t src_col_idx = 0;
    size_t dim = 0;
    int64_t dst_col_idx = -1;
    double radius = 0.0;
    segment_v2::Metric metric_type;
    doris::VectorSearchUserParams user_params;
    std::unique_ptr<float[]> query_value;
};
} // namespace doris::vectorized