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

#include "storage/index/ann/ann_index_result_cache/ann_index_result_cache.h"

#include <xxhash.h>

#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "exec/scan/vector_search_user_params.h"
#include "runtime/memory/lru_cache_value_base.h"
#include "storage/index/ann/ann_index_result_cache/ann_index_result_cache_handle.h"
#include "storage/index/ann/ann_search_params.h"
#include "util/hash_util.hpp"

namespace doris::segment_v2 {

static std::string make_key_from_param(const std::string& rowset_id, uint32_t segment_id,
                                       uint64_t index_id, const AnnTopNParam& param) {
    size_t seed = 0;
    HashUtil::hash_combine(seed, static_cast<uint64_t>(0));
    // Rowset ID
    HashUtil::hash_combine(seed, rowset_id);
    // Segment ID
    HashUtil::hash_combine(seed, static_cast<int64_t>(segment_id));
    // Index identity
    HashUtil::hash_combine(seed, index_id);
    // Query vector digest
    const size_t vec_size = param.query_value_size;
    HashUtil::hash_combine(seed, vec_size);
    if (vec_size > 0) {
        for (size_t i = 0; i < vec_size; ++i) {
            HashUtil::hash_combine(seed, param.query_value[i]);
        }
    }

    if (param.roaring == nullptr || param.roaring->cardinality() == param.rows_of_segment) {
        HashUtil::hash_combine(seed, static_cast<uint64_t>(0));
    } else {
        size_t serialized_size = param.roaring->getSizeInBytes(/*portable=*/true);
        std::vector<char> buf(serialized_size);
        param.roaring->write(buf.data(), /*portable=*/true);
        uint64_t roaring_hash = XXH3_64bits_withSeed(buf.data(), serialized_size, seed);
        HashUtil::hash_combine(seed, roaring_hash);
    }

    // Segment rows context
    HashUtil::hash_combine(seed, static_cast<uint64_t>(param.rows_of_segment));

    // TopN limit
    HashUtil::hash_combine(seed, static_cast<uint64_t>(param.limit));
    // HNSW specific params
    HashUtil::hash_combine(seed, param._user_params.hnsw_ef_search);
    HashUtil::hash_combine(seed, param._user_params.hnsw_check_relative_distance);
    HashUtil::hash_combine(seed, param._user_params.hnsw_bounded_queue);
    HashUtil::hash_combine(seed, param._user_params.ivf_nprobe);
    return std::to_string(seed);
}

static std::string make_key_from_range_param(const std::string& rowset_id, uint32_t segment_id,
                                             uint64_t index_id, const AnnRangeSearchParams& param,
                                             const VectorSearchUserParams& user_params, size_t dim,
                                             size_t rows_of_segment) {
    size_t seed = 0;
    HashUtil::hash_combine(seed, static_cast<uint64_t>(1));
    HashUtil::hash_combine(seed, rowset_id);
    HashUtil::hash_combine(seed, static_cast<int64_t>(segment_id));
    HashUtil::hash_combine(seed, index_id);

    HashUtil::hash_combine(seed, dim);
    if (dim > 0 && param.query_value != nullptr) {
        for (size_t i = 0; i < dim; ++i) {
            HashUtil::hash_combine(seed, param.query_value[i]);
        }
    }

    if (param.roaring == nullptr || param.roaring->cardinality() == rows_of_segment) {
        HashUtil::hash_combine(seed, static_cast<uint64_t>(0));
    } else {
        size_t serialized_size = param.roaring->getSizeInBytes(/*portable=*/true);
        std::vector<char> buf(serialized_size);
        param.roaring->write(buf.data(), /*portable=*/true);
        uint64_t roaring_hash = XXH3_64bits_withSeed(buf.data(), serialized_size, seed);
        HashUtil::hash_combine(seed, roaring_hash);
    }

    HashUtil::hash_combine(seed, static_cast<uint64_t>(rows_of_segment));
    HashUtil::hash_combine(seed, param.radius);
    HashUtil::hash_combine(seed, param.is_le_or_lt);
    HashUtil::hash_combine(seed, user_params.hnsw_ef_search);
    HashUtil::hash_combine(seed, user_params.hnsw_check_relative_distance);
    HashUtil::hash_combine(seed, user_params.hnsw_bounded_queue);
    HashUtil::hash_combine(seed, user_params.ivf_nprobe);
    return std::to_string(seed);
}

class CacheEntryPin {
public:
    CacheEntryPin(LRUCachePolicy* cache, doris::Cache::Handle* handle)
            : _cache(cache), _handle(handle) {
        DCHECK(_cache != nullptr);
        DCHECK(_handle != nullptr);
    }

    ~CacheEntryPin() { _cache->release(_handle); }

    CacheEntryPin(const CacheEntryPin&) = delete;
    CacheEntryPin& operator=(const CacheEntryPin&) = delete;

private:
    LRUCachePolicy* _cache;
    doris::Cache::Handle* _handle;
};

// Concrete cache value to store ANN results (hidden as a nested class).
class AnnIndexResultCache::CacheValue : public doris::LRUCacheValueBase {
public:
    explicit CacheValue(AnnIndexResultCacheHandle&& handle)
            : _distances(std::move(handle.distances)),
              _row_ids(std::move(handle.row_ids)),
              _roaring(std::move(handle.roaring)) {}

    AnnIndexResultCacheHandle to_handle(std::shared_ptr<CacheEntryPin> pin) const {
        AnnIndexResultCacheHandle h;
        if (_row_ids) {
            h.row_ids = std::shared_ptr<std::vector<uint64_t>>(pin, _row_ids.get());
        }
        if (_distances) {
            h.distances = std::shared_ptr<float[]>(pin, _distances.get());
        }
        if (_roaring) {
            h.roaring = std::shared_ptr<roaring::Roaring>(pin, _roaring.get());
        }
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

bool AnnIndexResultCache::lookup(const std::string& rowset_id, uint32_t segment_id,
                                 uint64_t index_id, const AnnTopNParam& param,
                                 AnnIndexResultCacheHandle* out_handle) {
    auto key_str = make_key_from_param(rowset_id, segment_id, index_id, param);
    auto cache_key = doris::CacheKey(key_str);
    doris::Cache::Handle* h = LRUCachePolicy::lookup(cache_key);

    if (h == nullptr) {
        return false;
    }

    VLOG_DEBUG << fmt::format("AnnIndexResultCache hit with rowset_id={}, segment_id={}, limit={}",
                              rowset_id, segment_id, param.limit);

    auto* val = reinterpret_cast<CacheValue*>(LRUCachePolicy::value(h));

    if (val == nullptr) {
        LRUCachePolicy::release(h);
        return false;
    }
    auto pin = std::make_shared<CacheEntryPin>(this, h);
    *out_handle = val->to_handle(std::move(pin));
    return true;
}

bool AnnIndexResultCache::insert(const std::string& rowset_id, uint32_t segment_id,
                                 uint64_t index_id, const AnnTopNParam& param,
                                 AnnIndexResultCacheHandle* handle) {
    DCHECK(handle != nullptr);
    auto key_str = make_key_from_param(rowset_id, segment_id, index_id, param);
    auto cache_key = doris::CacheKey(key_str);
    auto val = std::make_unique<CacheValue>(std::move(*handle));
    size_t charge = val->value_memory();
    const size_t capacity = LRUCachePolicy::get_capacity();
    if (capacity == 0 || charge > capacity) {
        VLOG_DEBUG << fmt::format(
                "Skip AnnIndexResultCache insert because entry is too large, rowset_id={}, "
                "segment_id={}, limit={}, charge={}, capacity={}",
                rowset_id, segment_id, param.limit, charge, capacity);
        return false;
    }
    auto* lru_handle = LRUCachePolicy::insert(cache_key, val.get(), charge, charge);
    if (lru_handle != nullptr) {
        auto pin = std::make_shared<CacheEntryPin>(this, lru_handle);
        *handle = val->to_handle(std::move(pin));
        val.release();
        VLOG_DEBUG << fmt::format(
                "AnnIndexResultCache inserted and pinned handle, rowset_id={}, segment_id={}, "
                "limit={}",
                rowset_id, segment_id, param.limit);
        return true;
    }
    VLOG_DEBUG << fmt::format(
            "AnnIndexResultCache insert with rowset_id={}, segment_id={}, limit={}, charge={}",
            rowset_id, segment_id, param.limit, charge);
    return false;
}

bool AnnIndexResultCache::lookup(const std::string& rowset_id, uint32_t segment_id,
                                 uint64_t index_id, const AnnRangeSearchParams& param,
                                 const VectorSearchUserParams& user_params, size_t dim,
                                 size_t rows_of_segment, AnnIndexResultCacheHandle* out_handle) {
    auto key_str = make_key_from_range_param(rowset_id, segment_id, index_id, param, user_params,
                                             dim, rows_of_segment);
    auto cache_key = doris::CacheKey(key_str);
    doris::Cache::Handle* h = LRUCachePolicy::lookup(cache_key);

    if (h == nullptr) {
        return false;
    }

    VLOG_DEBUG << fmt::format(
            "AnnIndexResultCache range_search hit with rowset_id={}, segment_id={}, radius={}",
            rowset_id, segment_id, param.radius);

    auto* val = reinterpret_cast<CacheValue*>(LRUCachePolicy::value(h));

    if (val == nullptr) {
        LRUCachePolicy::release(h);
        return false;
    }
    auto pin = std::make_shared<CacheEntryPin>(this, h);
    *out_handle = val->to_handle(std::move(pin));
    return true;
}

bool AnnIndexResultCache::insert(const std::string& rowset_id, uint32_t segment_id,
                                 uint64_t index_id, const AnnRangeSearchParams& param,
                                 const VectorSearchUserParams& user_params, size_t dim,
                                 size_t rows_of_segment, AnnIndexResultCacheHandle* handle) {
    DCHECK(handle != nullptr);
    auto key_str = make_key_from_range_param(rowset_id, segment_id, index_id, param, user_params,
                                             dim, rows_of_segment);
    auto cache_key = doris::CacheKey(key_str);
    auto val = std::make_unique<CacheValue>(std::move(*handle));
    size_t charge = val->value_memory();
    const size_t capacity = LRUCachePolicy::get_capacity();
    if (capacity == 0 || charge > capacity) {
        VLOG_DEBUG << fmt::format(
                "Skip AnnIndexResultCache range_search insert because entry is too large, "
                "rowset_id={}, segment_id={}, radius={}, charge={}, capacity={}",
                rowset_id, segment_id, param.radius, charge, capacity);
        return false;
    }
    auto* lru_handle = LRUCachePolicy::insert(cache_key, val.get(), charge, charge);
    if (lru_handle != nullptr) {
        auto pin = std::make_shared<CacheEntryPin>(this, lru_handle);
        *handle = val->to_handle(std::move(pin));
        val.release();
        VLOG_DEBUG << fmt::format(
                "AnnIndexResultCache range_search inserted and pinned handle, rowset_id={}, "
                "segment_id={}, radius={}, charge={}",
                rowset_id, segment_id, param.radius, charge);
        return true;
    }
    VLOG_DEBUG << fmt::format(
            "AnnIndexResultCache range_search insert with rowset_id={}, segment_id={}, "
            "radius={}, charge={}",
            rowset_id, segment_id, param.radius, charge);
    return false;
}

} // namespace doris::segment_v2
