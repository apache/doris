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

#include "olap/rowset/segment_v2/ann_index/ann_index_result_cache/ann_index_result_cache.h"

#include <cstring>
#include <memory>
#include <string>

#include "olap/lru_cache.h"
#include "olap/rowset/segment_v2/ann_index/ann_index_result_cache/ann_index_topn_cache_handle.h"
#include "olap/rowset/segment_v2/ann_index/ann_search_params.h"
#include "runtime/memory/lru_cache_value_base.h"
#include "util/hash_util.hpp"

namespace doris::segment_v2 {

static std::string make_key_from_param(const std::string& rowset_id, uint32_t segment_id,
                                       const AnnTopNParam& param) {
    size_t seed = 0;
    // Rowset ID
    HashUtil::hash_combine(seed, rowset_id);
    // Segment ID
    HashUtil::hash_combine(seed, static_cast<int64_t>(segment_id));
    // Query vector digest
    const size_t vec_size = param.query_value_size;
    HashUtil::hash_combine(seed, vec_size);
    if (vec_size > 0) {
        for (size_t i = 0; i < vec_size; ++i) {
            HashUtil::hash_combine(seed, param.query_value[i]);
        }
    }

    // Cache is only used when there is no effective pre-filter narrowing.
    // Keep lightweight context in key to avoid accidental cross-context reuse.
    HashUtil::hash_combine(seed, static_cast<uint64_t>(param.roaring == nullptr ? 0 : 1));
    HashUtil::hash_combine(seed,
                           static_cast<uint64_t>(param.roaring ? param.roaring->cardinality() : 0));

    // Segment rows context
    HashUtil::hash_combine(seed, static_cast<uint64_t>(param.rows_of_segment));

    // TopN limit
    HashUtil::hash_combine(seed, static_cast<uint64_t>(param.limit));
    // HNSW specific params
    HashUtil::hash_combine(seed, param._user_params.hnsw_ef_search);
    HashUtil::hash_combine(seed, param._user_params.hnsw_check_relative_distance);
    HashUtil::hash_combine(seed, param._user_params.hnsw_bounded_queue);
    return std::to_string(seed);
}

// Concrete cache value to store ANN top-N results (hidden as a nested class)
class AnnIndexTopnResultCache::CacheValue : public doris::LRUCacheValueBase {
public:
    explicit CacheValue(AnnIndexTopnCacheHandle&& handle)
            : _distances(std::move(handle.distances)),
              _row_ids(std::move(handle.row_ids)),
              _roaring(std::move(handle.roaring)) {}

    AnnIndexTopnCacheHandle to_handle() const {
        AnnIndexTopnCacheHandle h;
        h.row_ids = _row_ids;
        h.distances = _distances;
        h.roaring = _roaring; // share bitmap
        return h;
    }

    size_t value_memory() const {
        size_t size = sizeof(CacheValue);
        size_t n = _row_ids ? _row_ids->size() : 0;
        if (_distances) {
            size += sizeof(float) * n;
        }
        if (_row_ids) {
            size += sizeof(uint64_t) * n + sizeof(std::vector<uint64_t>);
        }
        if (_roaring) {
            size += _roaring->getSizeInBytes();
        }
        return size;
    }

private:
    std::shared_ptr<float[]> _distances;
    std::shared_ptr<std::vector<uint64_t>> _row_ids;
    std::shared_ptr<roaring::Roaring> _roaring;
};

bool AnnIndexTopnResultCache::lookup(const std::string& rowset_id, uint32_t segment_id,
                                     const AnnTopNParam& param,
                                     AnnIndexTopnCacheHandle* out_handle) {
    auto key_str = make_key_from_param(rowset_id, segment_id, param);
    auto cache_key = doris::CacheKey(key_str);
    doris::Cache::Handle* h = LRUCachePolicy::lookup(cache_key);

    if (h == nullptr) {
        return false;
    }

    VLOG_DEBUG << fmt::format(
            "AnnIndexTopnResultCache hit with rowset_id={}, segment_id={}, limit={}", rowset_id,
            segment_id, param.limit);

    auto* val = reinterpret_cast<CacheValue*>(LRUCachePolicy::value(h));

    if (val == nullptr) {
        LRUCachePolicy::release(h);
        return false;
    }
    *out_handle = val->to_handle();
    LRUCachePolicy::release(h);
    return true;
}

void AnnIndexTopnResultCache::insert(const std::string& rowset_id, uint32_t segment_id,
                                     const AnnTopNParam& param, AnnIndexTopnCacheHandle handle) {
    auto key_str = make_key_from_param(rowset_id, segment_id, param);
    auto cache_key = doris::CacheKey(key_str);
    auto* val = new CacheValue(std::move(handle));
    size_t charge = val->value_memory();
    auto* lru_handle = LRUCachePolicy::insert(cache_key, val, charge, charge);
    if (lru_handle != nullptr) {
        LRUCachePolicy::release(lru_handle);
        VLOG_DEBUG << fmt::format(
            "AnnIndexTopnResultCache released insert handle to make entry evictable, "
            "rowset_id={}, segment_id={}, limit={}",
            rowset_id, segment_id, param.limit);
    }
    VLOG_DEBUG << fmt::format(
            "AnnIndexTopnResultCache insert with rowset_id={}, segment_id={}, limit={}, charge={}",
            rowset_id, segment_id, param.limit, charge);
}

} // namespace doris::segment_v2
