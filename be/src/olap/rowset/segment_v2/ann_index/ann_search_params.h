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

/**
 * @file ann_search_params.h
 * @brief Parameter structures and statistics for ANN (Approximate Nearest Neighbor) search operations.
 * 
 * This file defines the core parameter structures used for configuring and executing
 * ANN search operations in Doris. It includes both top-N search and range search
 * parameter definitions, as well as statistics collection structures.
 * 
 * The structures defined here serve as the interface between the query execution
 * engine and the underlying vector index implementations (FAISS, etc.).
 */

#pragma once

#include <fmt/format.h>
#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/Opcodes_types.h>

#include <roaring/roaring.hh>
#include <string>

#include "util/runtime_profile.h"
#include "vec/runtime/vector_search_user_params.h"

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"

struct AnnIndexStats {
    AnnIndexStats()
            : search_costs_ns(TUnit::TIME_NS, 0),
              load_index_costs_ns(TUnit::TIME_NS, 0),
              engine_search_ns(TUnit::TIME_NS, 0),
              result_process_costs_ns(TUnit::TIME_NS, 0),
              engine_convert_ns(TUnit::TIME_NS, 0),
              engine_prepare_ns(TUnit::TIME_NS, 0) {}

    AnnIndexStats(const AnnIndexStats& other)
            : search_costs_ns(TUnit::TIME_NS, other.search_costs_ns.value()),
              load_index_costs_ns(TUnit::TIME_NS, other.load_index_costs_ns.value()),
              engine_search_ns(TUnit::TIME_NS, other.engine_search_ns.value()),
              result_process_costs_ns(TUnit::TIME_NS, other.result_process_costs_ns.value()),
              engine_convert_ns(TUnit::TIME_NS, other.engine_convert_ns.value()),
              engine_prepare_ns(TUnit::TIME_NS, other.engine_prepare_ns.value()) {}

    AnnIndexStats& operator=(const AnnIndexStats& other) {
        if (this != &other) {
            search_costs_ns.set(other.search_costs_ns.value());
            load_index_costs_ns.set(other.load_index_costs_ns.value());
            engine_search_ns.set(other.engine_search_ns.value());
            result_process_costs_ns.set(other.result_process_costs_ns.value());
            engine_convert_ns.set(other.engine_convert_ns.value());
            engine_prepare_ns.set(other.engine_prepare_ns.value());
        }
        return *this;
    }

    RuntimeProfile::Counter search_costs_ns;         // total time cost of TopN search
    RuntimeProfile::Counter load_index_costs_ns;     // time cost of loading ANN index
    RuntimeProfile::Counter engine_search_ns;        // time cost of calling FAISS/search engine
    RuntimeProfile::Counter result_process_costs_ns; // time cost of processing search results
    RuntimeProfile::Counter engine_convert_ns;       // time cost of engine-side conversions
    RuntimeProfile::Counter
            engine_prepare_ns; // time cost before engine search (allocations, setup)
};

struct AnnTopNParam {
    const float* query_value;
    const size_t query_value_size;
    size_t limit;
    doris::VectorSearchUserParams _user_params;
    roaring::Roaring* roaring;
    size_t rows_of_segment = 0;
    std::unique_ptr<std::vector<float>> distance = nullptr;
    std::unique_ptr<std::vector<uint64_t>> row_ids = nullptr;
    std::unique_ptr<AnnIndexStats> stats = nullptr;
};

struct AnnRangeSearchParams {
    bool is_le_or_lt = true;
    float* query_value = nullptr;
    float radius = -1;
    roaring::Roaring* roaring; // roaring from segment_iterator
    std::string to_string() const {
        DCHECK(roaring != nullptr);
        return fmt::format("is_le_or_lt: {}, radius: {}, input rows {}", is_le_or_lt, radius,
                           roaring->cardinality());
    }
    virtual ~AnnRangeSearchParams() = default;
};

struct AnnRangeSearchResult {
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
    // Internal engine timings (ns)
    int64_t engine_search_ns = 0;  // time spent in the underlying index search call
    int64_t engine_convert_ns = 0; // time spent building selectors/results inside the engine
    int64_t engine_prepare_ns = 0; // time spent preparing buffers before engine search
};

struct IndexSearchParameters {
    roaring::Roaring* roaring = nullptr;
    bool is_le_or_lt = true;
    size_t rows_of_segment = 0;
    virtual ~IndexSearchParameters() = default;
};

struct HNSWSearchParameters : public IndexSearchParameters {
    int ef_search = 16;
    bool check_relative_distance = true;
    bool bounded_queue = true;
};
#include "common/compile_check_end.h"
} // namespace doris::segment_v2