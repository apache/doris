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

#include <fmt/format.h>
#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/Opcodes_types.h>

#include <roaring/roaring.hh>
#include <string>

#include "util/runtime_profile.h"
#include "vec/runtime/vector_search_user_params.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

struct AnnIndexStats {
    AnnIndexStats() : search_costs_ns(TUnit::TIME_NS, 0), load_index_costs_ns(TUnit::TIME_NS, 0) {}

    AnnIndexStats(const AnnIndexStats& other)
            : search_costs_ns(TUnit::TIME_NS, other.search_costs_ns.value()),
              load_index_costs_ns(TUnit::TIME_NS, other.load_index_costs_ns.value()) {}

    AnnIndexStats& operator=(const AnnIndexStats& other) {
        if (this != &other) {
            search_costs_ns.set(other.search_costs_ns.value());
            load_index_costs_ns.set(other.load_index_costs_ns.value());
        }
        return *this;
    }

    RuntimeProfile::Counter search_costs_ns;     // time cost of search
    RuntimeProfile::Counter load_index_costs_ns; // time cost of load index
};

struct AnnIndexParam {
    const float* query_value;
    const size_t query_value_size;
    size_t limit;
    doris::VectorSearchUserParams _user_params;
    roaring::Roaring* roaring;
    std::unique_ptr<std::vector<float>> distance = nullptr;
    std::unique_ptr<std::vector<uint64_t>> row_ids = nullptr;
    std::unique_ptr<vectorized::AnnIndexStats> stats = nullptr;
};

struct RangeSearchParams {
    bool is_le_or_lt = true;
    float* query_value = nullptr;
    float radius = -1;
    roaring::Roaring* roaring; // roaring from segment_iterator
    std::string to_string() const {
        DCHECK(roaring != nullptr);
        return fmt::format("is_le_or_lt: {}, radius: {}, input rows {}", is_le_or_lt, radius,
                           roaring->cardinality());
    }
    virtual ~RangeSearchParams() = default;
};

struct RangeSearchResult {
    std::shared_ptr<roaring::Roaring> roaring;
    std::unique_ptr<std::vector<uint64_t>> row_ids;
    std::unique_ptr<float[]> distance;
};

/*
This struct is used to wrap the search result of a vector index.
roaring is a bitmap that contains the row ids that satisfy the search condition.
row_ids is a vector of row ids that are returned by the search, it could be used by virtual_column_iterator to do column filter.
distances is a vector of distances that are returned by the search.
For range search, is condition is not le_or_lt, the row_ids and distances will be nullptr.
*/
struct IndexSearchResult {
    IndexSearchResult() = default;

    std::unique_ptr<float[]> distances = nullptr;
    std::unique_ptr<std::vector<uint64_t>> row_ids = nullptr;
    std::shared_ptr<roaring::Roaring> roaring = nullptr;
};

struct IndexSearchParameters {
    roaring::Roaring* roaring = nullptr;
    bool is_le_or_lt = true;
    virtual ~IndexSearchParameters() = default;
};

struct HNSWSearchParameters : public IndexSearchParameters {
    int ef_search = 16;
    bool check_relative_distance = true;
    bool bounded_queue = true;
};
#include "common/compile_check_end.h"
} // namespace doris::vectorized