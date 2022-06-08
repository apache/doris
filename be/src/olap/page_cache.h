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

#include <memory>
#include <string>
#include <utility>

#include "gen_cpp/segment_v2.pb.h" // for cache allocation
#include "gutil/macros.h"          // for DISALLOW_COPY_AND_ASSIGN
#include "olap/lru_cache.h"
#include "runtime/mem_tracker.h"

namespace doris {

template <typename DataType>
class PageCacheHandle;

// Wrapper around Cache, and used for cache page of column data
// in Segment.
// TODO(zc): We should add some metric to see cache hit/miss rate.
class StoragePageCache {
public:
    // The unique key identifying entries in the page cache.
    // Each cached page corresponds to a specific offset within
    // a file.
    //
    // TODO(zc): Now we use file name(std::string) as a part of
    // key, which is not efficient. We should make it better later
    struct CacheKey {
        CacheKey(std::string fname_, int64_t offset_) : fname(std::move(fname_)), offset(offset_) {}
        std::string fname;
        int64_t offset;

        // Encode to a flat binary which can be used as LRUCache's key
        std::string encode() const {
            std::string key_buf(fname);
            key_buf.append((char*)&offset, sizeof(offset));
            return key_buf;
        }
    };

    static constexpr uint32_t kDefaultNumShards = 16;

    // Create global instance of this class
    static void create_global_cache(size_t capacity, int32_t index_cache_percentage,
                                    uint32_t num_shards = kDefaultNumShards);

    // Return global instance.
    // Client should call create_global_cache before.
    static StoragePageCache* instance() { return _s_instance; }

    StoragePageCache(size_t capacity, int32_t index_cache_percentage, uint32_t num_shards);

    // Lookup the given page in the cache.
    //
    // If the page is found, the cache entry will be written into handle.
    // PageCacheHandle will release cache entry to cache when it
    // destructs.
    //
    // Cache type selection is determined by page_type argument
    //
    // Return true if entry is found, otherwise return false.
    template <typename DataType>
    bool lookup(const CacheKey& key, PageCacheHandle<DataType>* handle,
                segment_v2::PageTypePB page_type) {
        auto cache = _get_page_cache(page_type);
        auto lru_handle = cache->lookup(key.encode());
        if (lru_handle == nullptr) {
            return false;
        }
        *handle = PageCacheHandle<DataType>(cache, lru_handle);
        return true;
    }

    // Insert a page with key into this cache.
    // Given handle will be set to valid reference.
    // This function is thread-safe, and when two clients insert two same key
    // concurrently, this function can assure that only one page is cached.
    // The in_memory page will have higher priority.
    template <typename DataType>
    void insert(const CacheKey& key, const DataType& data, PageCacheHandle<DataType>* handle,
                segment_v2::PageTypePB page_type, bool in_memory = false) {
        // auto deleter = [](const doris::CacheKey& key, void* value) { delete[] (uint8_t*)value; };
        constexpr auto deleter = [] {
            if constexpr (std::is_same_v<DataType, Slice>) {
                return [](const doris::CacheKey& key, void* value) { delete[](uint8_t*) value; };
            } else {
                return [](const doris::CacheKey& key, void* value) { delete (DataType*)(value); };
            }
        }();

        CachePriority priority = CachePriority::NORMAL;
        if (in_memory) {
            priority = CachePriority::DURABLE;
        }

        auto cache = _get_page_cache(page_type);

        if constexpr (std::is_same_v<DataType, Slice>) {
            auto lru_handle = cache->insert(key.encode(), data.data, data.size, deleter, priority);
            *handle = PageCacheHandle<DataType>(cache, lru_handle);
        } else {
            auto lru_handle = cache->insert(key.encode(), const_cast<DataType*>(&data), data.size(),
                                            deleter, priority);
            *handle = PageCacheHandle<DataType>(cache, lru_handle);
        }
    }

    // Page cache available check.
    // When percentage is set to 0 or 100, the index or data cache will not be allocated.
    bool is_cache_available(segment_v2::PageTypePB page_type) {
        return _get_page_cache(page_type) != nullptr;
    }

private:
    StoragePageCache();
    static StoragePageCache* _s_instance;

    int32_t _index_cache_percentage = 0;
    std::unique_ptr<Cache> _data_page_cache = nullptr;
    std::unique_ptr<Cache> _index_page_cache = nullptr;

    std::shared_ptr<MemTracker> _mem_tracker = nullptr;

    Cache* _get_page_cache(segment_v2::PageTypePB page_type) {
        switch (page_type) {
        case segment_v2::DATA_PAGE: {
            return _data_page_cache.get();
        }
        case segment_v2::INDEX_PAGE:
            return _index_page_cache.get();
        default:
            return nullptr;
        }
    }
};

// A handle for StoragePageCache entry. This class make it easy to handle
// Cache entry. Users don't need to release the obtained cache entry. This
// class will release the cache entry when it is destroyed.
template <typename DataType>
class PageCacheHandle {
public:
    PageCacheHandle() {}
    PageCacheHandle(Cache* cache, Cache::Handle* handle) : _cache(cache), _handle(handle) {}
    ~PageCacheHandle() {
        if (_handle != nullptr) {
            _cache->release(_handle);
        }
    }

    PageCacheHandle(PageCacheHandle&& other) noexcept {
        // we can use std::exchange if we switch c++14 on
        std::swap(_cache, other._cache);
        std::swap(_handle, other._handle);
    }

    PageCacheHandle& operator=(PageCacheHandle&& other) noexcept {
        std::swap(_cache, other._cache);
        std::swap(_handle, other._handle);
        return *this;
    }

    Cache* cache() const { return _cache; }

    DataType data() const {
        if constexpr (std::is_same_v<DataType, Slice>)
            return _cache->value_slice(_handle);
        else
            return *(DataType*)(_cache->value(_handle));
    }

private:
    Cache* _cache = nullptr;
    Cache::Handle* _handle = nullptr;

    // Don't allow copy and assign
    DISALLOW_COPY_AND_ASSIGN(PageCacheHandle);
};

} // namespace doris
