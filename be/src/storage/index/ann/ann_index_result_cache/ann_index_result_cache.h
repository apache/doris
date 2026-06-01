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

#include "common/config.h"
#include "exec/scan/vector_search_user_params.h"
#include "runtime/memory/lru_cache_policy.h"

namespace doris::segment_v2 {
struct IndexSearchResult;
struct AnnTopNParam;
struct AnnIndexResultCacheHandle;
struct AnnRangeSearchParams;

class AnnIndexResultCache : public LRUCachePolicy {
public:
    static AnnIndexResultCache* create_global_cache(size_t capacity_bytes) {
        auto num_shards = config::ann_index_result_cache_shards;
        LOG_INFO("Creating AnnIndexResultCache with capacity: {} bytes, shards: {}", capacity_bytes,
                 num_shards);
        static AnnIndexResultCache* instance = new AnnIndexResultCache(capacity_bytes, num_shards);
        return instance;
    }

    // Convenience overloads: build key from runtime params
    bool lookup(const std::string& rowset_id, uint32_t segment_id, uint64_t index_id,
                const AnnTopNParam& param, AnnIndexResultCacheHandle* out_handle);

    bool insert(const std::string& rowset_id, uint32_t segment_id, uint64_t index_id,
                const AnnTopNParam& param, AnnIndexResultCacheHandle* handle);

    bool lookup(const std::string& rowset_id, uint32_t segment_id, uint64_t index_id,
                const AnnRangeSearchParams& param, const VectorSearchUserParams& user_params,
                size_t dim, size_t rows_of_segment, AnnIndexResultCacheHandle* out_handle);

    bool insert(const std::string& rowset_id, uint32_t segment_id, uint64_t index_id,
                const AnnRangeSearchParams& param, const VectorSearchUserParams& user_params,
                size_t dim, size_t rows_of_segment, AnnIndexResultCacheHandle* handle);

private:
    // Hidden cache value type to keep implementation details internal
    class CacheValue;

    explicit AnnIndexResultCache(size_t capacity_bytes, uint32_t num_shards)
            : LRUCachePolicy(CachePolicy::CacheType::ANN_INDEX_RESULT_CACHE, capacity_bytes,
                             LRUCacheType::SIZE,
                             config::ann_index_result_cache_stale_sweep_time_sec, num_shards,
                             /*element_count_capacity*/ 0, /*enable_prune*/ true,
                             /*is_lru_k*/ false) {}
};

} // namespace doris::segment_v2
