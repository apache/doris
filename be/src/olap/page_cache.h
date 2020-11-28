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

#include "gutil/macros.h" // for DISALLOW_COPY_AND_ASSIGN
#include "olap/lru_cache.h"

namespace doris {

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

    // Create global instance of this class
    static void create_global_cache(size_t capacity);

    // Return global instance.
    // Client should call create_global_cache before.
    static StoragePageCache* instance() { return _s_instance; }

    StoragePageCache(size_t capacity);

    // Lookup the given page in the cache.
    //
    // If the page is found, the cache entry will be written into handle.
    // PageCacheHandle will release cache entry to cache when it
    // destructs.
    //
    // Return true if entry is found, otherwise return false.
    bool lookup(const CacheKey& key, PageCacheHandle* handle);

    // Insert a page with key into this cache.
    // Given handle will be set to valid reference.
    // This function is thread-safe, and when two clients insert two same key
    // concurrently, this function can assure that only one page is cached.
    // The in_memory page will have higher priority.
    void insert(const CacheKey& key, const Slice& data, PageCacheHandle* handle,
                bool in_memory = false);

private:
    StoragePageCache();
    static StoragePageCache* _s_instance;

    std::unique_ptr<Cache> _cache = nullptr;
};

// A handle for StoragePageCache entry. This class make it easy to handle
// Cache entry. Users don't need to release the obtained cache entry. This
// class will release the cache entry when it is destroyed.
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
    Slice data() const { return _cache->value_slice(_handle); }

private:
    Cache* _cache = nullptr;
    Cache::Handle* _handle = nullptr;

    // Don't allow copy and assign
    DISALLOW_COPY_AND_ASSIGN(PageCacheHandle);
};

} // namespace doris
