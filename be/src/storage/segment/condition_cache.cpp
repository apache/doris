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

#include "storage/segment/condition_cache.h"

#include <memory>

#include "util/defer_op.h"

namespace doris::segment_v2 {

template <typename KeyType>
bool ConditionCache::lookup(const KeyType& key, ConditionCacheHandle* handle) {
    auto encoded_key = key.encode();
    if (encoded_key.empty()) {
        return false;
    }
    auto lru_handle = LRUCachePolicy::lookup(encoded_key);
    if (!lru_handle) {
        return false;
    }
    *handle = ConditionCacheHandle(this, lru_handle);
    return true;
}

template <typename KeyType>
void ConditionCache::insert(const KeyType& key, std::shared_ptr<std::vector<bool>> result) {
    auto encoded_key = key.encode();
    if (encoded_key.empty()) {
        return;
    }
    std::unique_ptr<ConditionCache::CacheValue> cache_value_ptr =
            std::make_unique<ConditionCache::CacheValue>();
    cache_value_ptr->filter_result = result;

    ConditionCacheHandle(this, LRUCachePolicy::insert(encoded_key, (void*)cache_value_ptr.release(),
                                                      result->capacity(), result->capacity(),
                                                      CachePriority::NORMAL));
}

// Explicit template instantiations
template bool ConditionCache::lookup<ConditionCache::CacheKey>(const CacheKey& key,
                                                               ConditionCacheHandle* handle);
template bool ConditionCache::lookup<ConditionCache::ExternalCacheKey>(
        const ExternalCacheKey& key, ConditionCacheHandle* handle);
template void ConditionCache::insert<ConditionCache::CacheKey>(
        const CacheKey& key, std::shared_ptr<std::vector<bool>> filter_result);
template void ConditionCache::insert<ConditionCache::ExternalCacheKey>(
        const ExternalCacheKey& key, std::shared_ptr<std::vector<bool>> filter_result);

} // namespace doris::segment_v2
