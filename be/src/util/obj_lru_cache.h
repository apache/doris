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

#include "olap/lru_cache.h"
#include "runtime/memory/lru_cache_policy.h"

namespace doris {

// A common object cache depends on an Sharded LRU Cache.
// It has a certain capacity, which determin how many objects it can cache.
// Caller must hold a CacheHandle instance when visiting the cached object.
class ObjLRUCache : public LRUCachePolicy {
public:
    struct ObjKey {
        ObjKey(const std::string& key_) : key(key_) {}

        std::string key;
    };

    class CacheHandle {
    public:
        CacheHandle() = default;
        CacheHandle(Cache* cache, Cache::Handle* handle) : _cache(cache), _handle(handle) {}
        ~CacheHandle() {
            if (_handle != nullptr) {
                _cache->release(_handle);
            }
        }

        CacheHandle(CacheHandle&& other) noexcept {
            std::swap(_cache, other._cache);
            std::swap(_handle, other._handle);
        }

        CacheHandle& operator=(CacheHandle&& other) noexcept {
            std::swap(_cache, other._cache);
            std::swap(_handle, other._handle);
            return *this;
        }

        bool valid() { return _cache != nullptr && _handle != nullptr; }

        Cache* cache() const { return _cache; }
        void* data() const { return _cache->value(_handle); }

    private:
        Cache* _cache = nullptr;
        Cache::Handle* _handle = nullptr;

        // Don't allow copy and assign
        DISALLOW_COPY_AND_ASSIGN(CacheHandle);
    };

    ObjLRUCache(int64_t capacity, uint32_t num_shards = DEFAULT_LRU_CACHE_NUM_SHARDS);

    bool lookup(const ObjKey& key, CacheHandle* handle);

    template <typename T>
    void insert(const ObjKey& key, const T* value, CacheHandle* cache_handle) {
        auto deleter = [](const doris::CacheKey& key, void* value) {
            T* v = (T*)value;
            delete v;
        };
        insert(key, value, cache_handle, deleter);
    }

    template <typename T>
    void insert(const ObjKey& key, const T* value, CacheHandle* cache_handle,
                void (*deleter)(const CacheKey& key, void* value)) {
        if (_enabled) {
            const std::string& encoded_key = key.key;
            auto handle = cache()->insert(encoded_key, (void*)value, 1, deleter,
                                          CachePriority::NORMAL, sizeof(T));
            *cache_handle = CacheHandle {cache(), handle};
        } else {
            cache_handle = nullptr;
        }
    }

    void erase(const ObjKey& key);

private:
    bool _enabled;
};

} // namespace doris
