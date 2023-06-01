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

namespace doris {

class FileMetaCache {
public:
    struct MetaCacheKey {
        MetaCacheKey(const std::string& file, int64_t time) : _file(file), _time(time) {}

        std::string _file;
        int64_t _time;

        std::string encode() const {
            return _file + std::to_string(_time);
        }
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

    FileMetaCache(int64_t capacity, uint32_t num_shards = kDefaultNumShards);

    bool lookup(const MetaCacheKey& key, CacheHandle* handle);

    template<typename T>
    void insert(const MetaCacheKey& key, const T* value, CacheHandle* cache_handle) {
        auto deleter = [](const doris::CacheKey& key, void* value) {
            T* v = (T*) value;
            delete v;
        };
        const std::string& encoded_key = key.encode();
        auto handle =
                _cache->insert(encoded_key, (void*) value, sizeof(T), deleter, CachePriority::NORMAL);
        *cache_handle = CacheHandle {_cache.get(), handle};
    }

    void erase(const MetaCacheKey& key);

private:
    static constexpr uint32_t kDefaultNumShards = 16;
    std::unique_ptr<Cache> _cache = nullptr;
};

}
