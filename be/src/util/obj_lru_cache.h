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
class ObjLRUCache : public LRUCachePolicyTrackingManual {
public:
    using LRUCachePolicyTrackingManual::insert;

    struct ObjKey {
        ObjKey(const std::string& key_) : key(key_) {}

        std::string key;
    };

    template <typename T>
    class ObjValue : public LRUCacheValueBase {
    public:
        ObjValue(const T* value) : value(value) {}
        ~ObjValue() override {
            T* v = (T*)value;
            delete v;
        }

        const T* value;
    };

    class CacheHandle {
    public:
        CacheHandle() = default;
        CacheHandle(LRUCachePolicy* cache, Cache::Handle* handle)
                : _cache(cache), _handle(handle) {}
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

        LRUCachePolicy* cache() const { return _cache; }
        template <typename T>
        void* data() const {
            return (void*)((ObjValue<T>*)_cache->value(_handle))->value;
        }

    private:
        LRUCachePolicy* _cache = nullptr;
        Cache::Handle* _handle = nullptr;

        // Don't allow copy and assign
        DISALLOW_COPY_AND_ASSIGN(CacheHandle);
    };

    ObjLRUCache(int64_t capacity, uint32_t num_shards = DEFAULT_LRU_CACHE_NUM_SHARDS);

    bool lookup(const ObjKey& key, CacheHandle* handle);

    template <typename T>
    void insert(const ObjKey& key, const T* value, CacheHandle* cache_handle) {
        if (_enabled) {
            const std::string& encoded_key = key.key;
            auto* obj_value = new ObjValue<T>(value);
            auto* handle = LRUCachePolicyTrackingManual::insert(encoded_key, obj_value, 1,
                                                                sizeof(T), CachePriority::NORMAL);
            *cache_handle = CacheHandle {this, handle};
        } else {
            cache_handle = nullptr;
        }
    }

    void erase(const ObjKey& key);

    bool exceed_prune_limit() override;

private:
    bool _enabled;
};

} // namespace doris
