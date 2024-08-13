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

#include "olap/rowset/segment_v2/inverted_index_cache.h"

// IWYU pragma: no_include <bthread/errno.h>
#include <sys/resource.h>

#include <cstring>
// IWYU pragma: no_include <bits/chrono.h>
#include <iostream>
#include <memory>

#include "runtime/exec_env.h"
#include "runtime/thread_context.h"
#include "util/defer_op.h"

namespace doris::segment_v2 {

InvertedIndexSearcherCache* InvertedIndexSearcherCache::create_global_instance(
        size_t capacity, uint32_t num_shards) {
    return new InvertedIndexSearcherCache(capacity, num_shards);
}

InvertedIndexSearcherCache::InvertedIndexSearcherCache(size_t capacity, uint32_t num_shards) {
    uint64_t fd_number = config::min_file_descriptor_number;
    struct rlimit l;
    int ret = getrlimit(RLIMIT_NOFILE, &l);
    if (ret != 0) {
        LOG(WARNING) << "call getrlimit() failed. errno=" << strerror(errno)
                     << ", use default configuration instead.";
    } else {
        fd_number = static_cast<uint64_t>(l.rlim_cur);
    }

    static constexpr size_t fd_bound = 100000;
    size_t search_limit_percent = config::inverted_index_fd_number_limit_percent;
    if (fd_number <= fd_bound) {
        search_limit_percent = size_t(search_limit_percent * 0.25); // default 10%
    } else if (fd_number > fd_bound && fd_number < fd_bound * 5) {
        search_limit_percent = size_t(search_limit_percent * 0.5); // default 20%
    }

    uint64_t open_searcher_limit = fd_number * search_limit_percent / 100;
    LOG(INFO) << "fd_number: " << fd_number
              << ", inverted index open searcher limit: " << open_searcher_limit;
#ifdef BE_TEST
    open_searcher_limit = 2;
#endif

    if (config::enable_inverted_index_cache_check_timestamp) {
        auto get_last_visit_time = [](const void* value) -> int64_t {
            auto* cache_value = (InvertedIndexSearcherCache::CacheValue*)value;
            return cache_value->last_visit_time;
        };
        _policy = std::make_unique<InvertedIndexSearcherCachePolicy>(
                capacity, num_shards, open_searcher_limit, get_last_visit_time, true);
    } else {
        _policy = std::make_unique<InvertedIndexSearcherCachePolicy>(capacity, num_shards,
                                                                     open_searcher_limit);
    }
}

Status InvertedIndexSearcherCache::erase(const std::string& index_file_path) {
    InvertedIndexSearcherCache::CacheKey cache_key(index_file_path);
    _policy->erase(cache_key.index_file_path);
    return Status::OK();
}

int64_t InvertedIndexSearcherCache::mem_consumption() {
    return _policy->mem_consumption();
}

bool InvertedIndexSearcherCache::lookup(const InvertedIndexSearcherCache::CacheKey& key,
                                        InvertedIndexCacheHandle* handle) {
    auto* lru_handle = _policy->lookup(key.index_file_path);
    if (lru_handle == nullptr) {
        return false;
    }
    *handle = InvertedIndexCacheHandle(_policy.get(), lru_handle);
    return true;
}

void InvertedIndexSearcherCache::insert(const InvertedIndexSearcherCache::CacheKey& cache_key,
                                        CacheValue* cache_value) {
    auto* lru_handle = _insert(cache_key, cache_value);
    release(lru_handle);
}

void InvertedIndexSearcherCache::insert(const InvertedIndexSearcherCache::CacheKey& cache_key,
                                        CacheValue* cache_value, InvertedIndexCacheHandle* handle) {
    auto* lru_handle = _insert(cache_key, cache_value);
    *handle = InvertedIndexCacheHandle(_policy.get(), lru_handle);
}

Cache::Handle* InvertedIndexSearcherCache::_insert(const InvertedIndexSearcherCache::CacheKey& key,
                                                   CacheValue* value) {
    Cache::Handle* lru_handle = _policy->insert(key.index_file_path, value, value->size,
                                                value->size, CachePriority::NORMAL);
    return lru_handle;
}

bool InvertedIndexQueryCache::lookup(const CacheKey& key, InvertedIndexQueryCacheHandle* handle) {
    if (key.encode().empty()) {
        return false;
    }
    auto* lru_handle = LRUCachePolicy::lookup(key.encode());
    if (lru_handle == nullptr) {
        return false;
    }
    *handle = InvertedIndexQueryCacheHandle(this, lru_handle);
    return true;
}

void InvertedIndexQueryCache::insert(const CacheKey& key, std::shared_ptr<roaring::Roaring> bitmap,
                                     InvertedIndexQueryCacheHandle* handle) {
    std::unique_ptr<InvertedIndexQueryCache::CacheValue> cache_value_ptr =
            std::make_unique<InvertedIndexQueryCache::CacheValue>();
    cache_value_ptr->bitmap = bitmap;
    if (key.encode().empty()) {
        return;
    }

    auto* lru_handle = LRUCachePolicyTrackingManual::insert(
            key.encode(), (void*)cache_value_ptr.release(), bitmap->getSizeInBytes(),
            bitmap->getSizeInBytes(), CachePriority::NORMAL);
    *handle = InvertedIndexQueryCacheHandle(this, lru_handle);
}

} // namespace doris::segment_v2
