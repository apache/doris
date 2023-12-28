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

#include <butil/macros.h>
#include <gen_cpp/segment_v2.pb.h>
#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <string>
#include <utility>

#include "olap/lru_cache.h"
#include "runtime/memory/lru_cache_policy.h"
#include "util/slice.h"
#include "vec/common/allocator.h"
#include "vec/common/allocator_fwd.h"

namespace doris {

class PageCacheHandle;

template <typename TAllocator>
class PageBase : private TAllocator, public LRUCacheValueBase {
public:
    PageBase() : _data(nullptr), _size(0), _capacity(0) {}

    PageBase(size_t b) : _size(b), _capacity(b) {
        _data = reinterpret_cast<char*>(TAllocator::alloc(_capacity, ALLOCATOR_ALIGNMENT_16));
    }

    PageBase(const PageBase&) = delete;
    PageBase& operator=(const PageBase&) = delete;

    ~PageBase() {
        if (_data != nullptr) {
            DCHECK(_capacity != 0 && _size != 0);
            TAllocator::free(_data, _capacity);
        }
    }

    char* data() { return _data; }
    size_t size() { return _size; }
    size_t capacity() { return _capacity; }

    void reset_size(size_t n) {
        DCHECK(n <= _capacity);
        _size = n;
    }

private:
    char* _data = nullptr;
    // Effective size, smaller than capacity, such as data page remove checksum suffix.
    size_t _size;
    size_t _capacity = 0;
};

using DataPage = PageBase<Allocator<false>>;

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
        CacheKey(std::string fname_, size_t fsize_, int64_t offset_)
                : fname(std::move(fname_)), fsize(fsize_), offset(offset_) {}
        std::string fname;
        size_t fsize;
        int64_t offset;

        // Encode to a flat binary which can be used as LRUCache's key
        std::string encode() const {
            std::string key_buf(fname);
            key_buf.append((char*)&fsize, sizeof(fsize));
            key_buf.append((char*)&offset, sizeof(offset));
            return key_buf;
        }
    };

    class DataPageCache : public LRUCachePolicy {
    public:
        DataPageCache(size_t capacity, uint32_t num_shards)
                : LRUCachePolicy(CachePolicy::CacheType::DATA_PAGE_CACHE, capacity,
                                 LRUCacheType::SIZE, config::data_page_cache_stale_sweep_time_sec,
                                 num_shards) {}
    };

    class IndexPageCache : public LRUCachePolicy {
    public:
        IndexPageCache(size_t capacity, uint32_t num_shards)
                : LRUCachePolicy(CachePolicy::CacheType::INDEXPAGE_CACHE, capacity,
                                 LRUCacheType::SIZE, config::index_page_cache_stale_sweep_time_sec,
                                 num_shards) {}
    };

    class PKIndexPageCache : public LRUCachePolicy {
    public:
        PKIndexPageCache(size_t capacity, uint32_t num_shards)
                : LRUCachePolicy(CachePolicy::CacheType::PK_INDEX_PAGE_CACHE, capacity,
                                 LRUCacheType::SIZE,
                                 config::pk_index_page_cache_stale_sweep_time_sec, num_shards) {}
    };

    static constexpr uint32_t kDefaultNumShards = 16;

    // Create global instance of this class
    static StoragePageCache* create_global_cache(size_t capacity, int32_t index_cache_percentage,
                                                 int64_t pk_index_cache_capacity,
                                                 uint32_t num_shards = kDefaultNumShards);

    // Return global instance.
    // Client should call create_global_cache before.
    static StoragePageCache* instance() { return ExecEnv::GetInstance()->get_storage_page_cache(); }

    StoragePageCache(size_t capacity, int32_t index_cache_percentage,
                     int64_t pk_index_cache_capacity, uint32_t num_shards);

    // Lookup the given page in the cache.
    //
    // If the page is found, the cache entry will be written into handle.
    // PageCacheHandle will release cache entry to cache when it
    // destructs.
    //
    // Cache type selection is determined by page_type argument
    //
    // Return true if entry is found, otherwise return false.
    bool lookup(const CacheKey& key, PageCacheHandle* handle, segment_v2::PageTypePB page_type);

    // Insert a page with key into this cache.
    // Given handle will be set to valid reference.
    // This function is thread-safe, and when two clients insert two same key
    // concurrently, this function can assure that only one page is cached.
    // The in_memory page will have higher priority.
    void insert(const CacheKey& key, DataPage* data, PageCacheHandle* handle,
                segment_v2::PageTypePB page_type, bool in_memory = false);

private:
    StoragePageCache();

    int32_t _index_cache_percentage = 0;
    std::unique_ptr<DataPageCache> _data_page_cache;
    std::unique_ptr<IndexPageCache> _index_page_cache;
    // Cache data for primary key index data page, seperated from data
    // page cache to make it for flexible. we need this cache When construct
    // delete bitmap in unique key with mow
    std::unique_ptr<PKIndexPageCache> _pk_index_page_cache;

    Cache* _get_page_cache(segment_v2::PageTypePB page_type) {
        switch (page_type) {
        case segment_v2::DATA_PAGE: {
            return _data_page_cache->cache();
        }
        case segment_v2::INDEX_PAGE: {
            return _index_page_cache->cache();
        }
        case segment_v2::PRIMARY_KEY_INDEX_PAGE: {
            return _pk_index_page_cache->cache();
        }
        default:
            LOG(FATAL) << "get error type page cache";
        }
        LOG(FATAL) << "__builtin_unreachable";
        __builtin_unreachable();
    }
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
    Slice data() const {
        DataPage* cache_value = (DataPage*)_cache->value(_handle);
        return Slice(cache_value->data(), cache_value->size());
    }

    void update_last_visit_time() {
        DataPage* cache_value = (DataPage*)_cache->value(_handle);
        cache_value->last_visit_time = UnixMillis();
    }

private:
    Cache* _cache = nullptr;
    Cache::Handle* _handle = nullptr;

    // Don't allow copy and assign
    DISALLOW_COPY_AND_ASSIGN(PageCacheHandle);
};

} // namespace doris
