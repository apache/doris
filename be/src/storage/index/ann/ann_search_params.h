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

#include <memory>
#include <roaring/roaring.hh>
#include <string>
#include <vector>

#include "exec/scan/vector_search_user_params.h"
#include "runtime/runtime_profile.h"

namespace doris::io {
struct IOContext;
} // namespace doris::io

namespace doris::segment_v2 {

struct AnnIndexResultCacheHandle;

struct AnnIndexStats {
    AnnIndexStats()
            : search_costs_ns(TUnit::TIME_NS, 0),
              load_index_costs_ns(TUnit::TIME_NS, 0),
              engine_search_ns(TUnit::TIME_NS, 0),
              result_process_costs_ns(TUnit::TIME_NS, 0),
              engine_convert_ns(TUnit::TIME_NS, 0),
              engine_prepare_ns(TUnit::TIME_NS, 0),
              ivf_on_disk_load_costs_ns(TUnit::TIME_NS, 0),
              ivf_on_disk_search_costs_ns(TUnit::TIME_NS, 0),
              ivf_on_disk_search_cnt(TUnit::UNIT, 0),
              ivf_on_disk_cache_hit_cnt(TUnit::UNIT, 0),
              ivf_on_disk_cache_miss_cnt(TUnit::UNIT, 0),
              topn_cache_hits(TUnit::UNIT, 0),
              range_cache_hits(TUnit::UNIT, 0),
              fall_back_brute_force_cnt(0),
              range_fallback_by_small_candidate_cnt(0),
              range_fallback_small_candidate_rows(0) {}

    AnnIndexStats(const AnnIndexStats& other)
            : search_costs_ns(TUnit::TIME_NS, other.search_costs_ns.value()),
              load_index_costs_ns(TUnit::TIME_NS, other.load_index_costs_ns.value()),
              engine_search_ns(TUnit::TIME_NS, other.engine_search_ns.value()),
              result_process_costs_ns(TUnit::TIME_NS, other.result_process_costs_ns.value()),
              engine_convert_ns(TUnit::TIME_NS, other.engine_convert_ns.value()),
              engine_prepare_ns(TUnit::TIME_NS, other.engine_prepare_ns.value()),
              ivf_on_disk_load_costs_ns(TUnit::TIME_NS, other.ivf_on_disk_load_costs_ns.value()),
              ivf_on_disk_search_costs_ns(TUnit::TIME_NS,
                                          other.ivf_on_disk_search_costs_ns.value()),
              ivf_on_disk_search_cnt(TUnit::UNIT, other.ivf_on_disk_search_cnt.value()),
              ivf_on_disk_cache_hit_cnt(TUnit::UNIT, other.ivf_on_disk_cache_hit_cnt.value()),
              ivf_on_disk_cache_miss_cnt(TUnit::UNIT, other.ivf_on_disk_cache_miss_cnt.value()),
              topn_cache_hits(TUnit::UNIT, other.topn_cache_hits.value()),
              range_cache_hits(TUnit::UNIT, other.range_cache_hits.value()),
              fall_back_brute_force_cnt(other.fall_back_brute_force_cnt),
              range_fallback_by_small_candidate_cnt(other.range_fallback_by_small_candidate_cnt),
              range_fallback_small_candidate_rows(other.range_fallback_small_candidate_rows) {}

    AnnIndexStats& operator=(const AnnIndexStats& other) {
        if (this != &other) {
            search_costs_ns.set(other.search_costs_ns.value());
            load_index_costs_ns.set(other.load_index_costs_ns.value());
            engine_search_ns.set(other.engine_search_ns.value());
            result_process_costs_ns.set(other.result_process_costs_ns.value());
            engine_convert_ns.set(other.engine_convert_ns.value());
            engine_prepare_ns.set(other.engine_prepare_ns.value());
            ivf_on_disk_load_costs_ns.set(other.ivf_on_disk_load_costs_ns.value());
            ivf_on_disk_search_costs_ns.set(other.ivf_on_disk_search_costs_ns.value());
            ivf_on_disk_search_cnt.set(other.ivf_on_disk_search_cnt.value());
            ivf_on_disk_cache_hit_cnt.set(other.ivf_on_disk_cache_hit_cnt.value());
            ivf_on_disk_cache_miss_cnt.set(other.ivf_on_disk_cache_miss_cnt.value());
            topn_cache_hits.set(other.topn_cache_hits.value());
            range_cache_hits.set(other.range_cache_hits.value());
            fall_back_brute_force_cnt = other.fall_back_brute_force_cnt;
            range_fallback_by_small_candidate_cnt = other.range_fallback_by_small_candidate_cnt;
            range_fallback_small_candidate_rows = other.range_fallback_small_candidate_rows;
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
    RuntimeProfile::Counter ivf_on_disk_load_costs_ns;   // IVF_ON_DISK index load costs
    RuntimeProfile::Counter ivf_on_disk_search_costs_ns; // IVF_ON_DISK search costs
    RuntimeProfile::Counter ivf_on_disk_search_cnt;      // IVF_ON_DISK search count
    RuntimeProfile::Counter ivf_on_disk_cache_hit_cnt;   // IVF_ON_DISK cache hit count
    RuntimeProfile::Counter ivf_on_disk_cache_miss_cnt;  // IVF_ON_DISK cache miss count
    RuntimeProfile::Counter topn_cache_hits; // number of cache hits in ANN TopN result cache
    RuntimeProfile::Counter range_cache_hits;
    int64_t fall_back_brute_force_cnt; // fallback count when ANN range search is bypassed
    int64_t range_fallback_by_small_candidate_cnt;
    int64_t range_fallback_small_candidate_rows;
};

struct AnnTopNParam {
    // =========================
    // TopN execution inputs
    // =========================
    // Query vector data pointer.
    const float* query_value;
    // Query vector dimension.
    const size_t query_value_size;
    // Requested TopK/TopN count.
    size_t limit;
    // Runtime ANN search options (HNSW/IVF specific params).
    doris::VectorSearchUserParams _user_params;
    // Candidate row bitmap after pre-filters.
    // ANN TopN search only evaluates rows inside this bitmap.
    roaring::Roaring* roaring;
    // Total row count of current segment, used by ANN engine for bounds/checks.
    size_t rows_of_segment = 0;
    bool enable_result_cache = true;

    // =========================
    // TopN execution outputs
    // =========================
    // Output distances of returned TopN rows. Aligned with `row_ids`.
    std::shared_ptr<float[]> distance = nullptr;
    // Output row ids corresponding to `distance`.
    std::shared_ptr<std::vector<uint64_t>> row_ids = nullptr;
    // Optional per-call statistics holder.
    std::unique_ptr<AnnIndexStats> stats = nullptr;

    std::string to_string() const {
        return fmt::format(
                "query_value_size: {}, limit: {}, rows_of_segment: {}, hnsw_ef_search: {}, "
                "hnsw_check_relative_distance: {}, hnsw_bounded_queue: {}",
                query_value_size, limit, rows_of_segment, _user_params.hnsw_ef_search,
                _user_params.hnsw_check_relative_distance, _user_params.hnsw_bounded_queue);
    }
};

struct AnnRangeSearchParams {
    bool is_le_or_lt = true;
    const float* query_value = nullptr;
    float radius = -1;
    roaring::Roaring* roaring; // roaring from segment_iterator
    bool enable_result_cache = true;
    std::string to_string() const {
        DCHECK(roaring != nullptr);
        return fmt::format("is_le_or_lt: {}, radius: {}, input rows {}", is_le_or_lt, radius,
                           roaring->cardinality());
    }
    virtual ~AnnRangeSearchParams() = default;
};

struct AnnRangeSearchResult {
    std::shared_ptr<roaring::Roaring> roaring;
    std::shared_ptr<std::vector<uint64_t>> row_ids;
    std::shared_ptr<float[]> distance;

    AnnIndexResultCacheHandle to_cache_handle() const;
    static AnnRangeSearchResult from_cache_handle(const AnnIndexResultCacheHandle& handle);
};

/*
This struct is used to wrap the search result of a vector index.
roaring is a bitmap that contains the row ids that satisfy the search condition.
row_ids is an ordered vector of row ids returned by the search. row_ids[i] is aligned with
distances[i], so virtual_column_iterator can map each distance back to its segment row id.
distances is a vector of distances that are returned by the search.
For range search, is condition is not le_or_lt, the row_ids and distances will be nullptr.
*/
struct IndexSearchResult {
    IndexSearchResult() = default;

    AnnIndexResultCacheHandle to_cache_handle() const;

    // distances from index.
    std::shared_ptr<float[]> distances = nullptr;
    // row_id of each distance. Aligned with distances, so row_ids[i] corresponds to distances[i].
    std::shared_ptr<std::vector<uint64_t>> row_ids = nullptr;
    // roaring from/to doris segment iterator.
    std::shared_ptr<roaring::Roaring> roaring = nullptr;
    // Internal engine timings (ns)
    int64_t engine_search_ns = 0;  // time spent in the underlying index search call
    int64_t engine_convert_ns = 0; // time spent building selectors/results inside the engine
    int64_t engine_prepare_ns = 0; // time spent preparing buffers before engine search
    int64_t ivf_on_disk_cache_hit_cnt = 0;
    int64_t ivf_on_disk_cache_miss_cnt = 0;
};

struct IndexSearchParameters {
    roaring::Roaring* roaring = nullptr;
    bool is_le_or_lt = true;
    size_t rows_of_segment = 0;
    // Caller-owned IOContext, valid for the duration of the search.
    // Propagated via thread_local to CachedRandomAccessReader so that
    // file-cache statistics are attributed to the correct query.
    const io::IOContext* io_ctx = nullptr;
    virtual ~IndexSearchParameters() = default;
};

struct HNSWSearchParameters : public IndexSearchParameters {
    int ef_search = 16;
    bool check_relative_distance = true;
    bool bounded_queue = true;
};

struct IVFSearchParameters : public IndexSearchParameters {
    int nprobe = 1;
};
} // namespace doris::segment_v2
