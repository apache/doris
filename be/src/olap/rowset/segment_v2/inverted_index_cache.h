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

#include <CLucene.h>

#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <vector>

#include "io/fs/file_system.h"
#include "olap/lru_cache.h"
#include "runtime/memory/mem_tracker.h"
#include "util/time.h"

namespace doris {

namespace segment_v2 {
using IndexSearcherPtr = std::shared_ptr<lucene::search::IndexSearcher>;

class InvertedIndexCacheHandle;

class InvertedIndexSearcherCache {
public:
    // The cache key of index_searcher lru cache
    struct CacheKey {
        CacheKey(std::string index_file_path) : index_file_path(index_file_path) {}
        std::string index_file_path;
    };

    // The cache value of index_searcher lru cache.
    // Holding a opened index_searcher.
    struct CacheValue {
        // Save the last visit time of this cache entry.
        // Use atomic because it may be modified by multi threads.
        std::atomic<int64_t> last_visit_time = 0;
        IndexSearcherPtr index_searcher;
        size_t size = 0;
    };

    // Create global instance of this class.
    // "capacity" is the capacity of lru cache.
    static void create_global_instance(size_t capacity, uint32_t num_shards = 16);

    // Return global instance.
    // Client should call create_global_cache before.
    static InvertedIndexSearcherCache* instance() { return _s_instance; }

    static IndexSearcherPtr build_index_searcher(const io::FileSystemSPtr& fs,
                                                 const std::string& index_dir,
                                                 const std::string& file_name);

    InvertedIndexSearcherCache(size_t capacity, uint32_t num_shards);

    Status get_index_searcher(const io::FileSystemSPtr& fs, const std::string& index_dir,
                              const std::string& file_name, InvertedIndexCacheHandle* cache_handle,
                              bool use_cache = true);

    // function `insert` called after inverted index writer close
    Status insert(const io::FileSystemSPtr& fs, const std::string& index_dir,
                  const std::string& file_name);

    // function `erase` called after compaction remove segment
    Status erase(const std::string& index_file_path);

private:
    InvertedIndexSearcherCache();

    // Lookup the given index_searcher in the cache.
    // If the index_searcher is found, the cache entry will be written into handle.
    // Return true if entry is found, otherwise return false.
    bool _lookup(const InvertedIndexSearcherCache::CacheKey& key, InvertedIndexCacheHandle* handle);

    // Insert a cache entry by key.
    // And the cache entry will be returned in handle.
    // This function is thread-safe.
    Cache::Handle* _insert(const InvertedIndexSearcherCache::CacheKey& key, CacheValue* value);

private:
    static InvertedIndexSearcherCache* _s_instance;
    // A LRU cache to cache all opened index_searcher
    std::unique_ptr<Cache> _cache = nullptr;
    std::unique_ptr<MemTracker> _mem_tracker = nullptr;
};

using IndexCacheValuePtr = std::unique_ptr<InvertedIndexSearcherCache::CacheValue>;

// A handle for a index_searcher from index_searcher lru cache.
// The handle can ensure that the index_searcher is valid
// and will not be closed while the holder of the handle is accessing the index_searcher.
// The handle will automatically release the cache entry when it is destroyed.
// So the caller need to make sure the handle is valid in lifecycle.
class InvertedIndexCacheHandle {
public:
    InvertedIndexCacheHandle() {}
    InvertedIndexCacheHandle(Cache* cache, Cache::Handle* handle)
            : _cache(cache), _handle(handle) {}

    ~InvertedIndexCacheHandle() {
        if (_handle != nullptr) {
            CHECK(_cache != nullptr);
            CHECK(!owned);
            // only after get_index_searcher call this destructor will
            // add `config::index_cache_entry_stay_time_after_lookup_s` on last_visit_time,
            // this is to extend the retention time of the entries hit by lookup.
            ((InvertedIndexSearcherCache::CacheValue*)_cache->value(_handle))->last_visit_time =
                    UnixMillis() + config::index_cache_entry_stay_time_after_lookup_s * 1000;
            _cache->release(_handle);
        }
    }

    InvertedIndexCacheHandle(InvertedIndexCacheHandle&& other) noexcept {
        std::swap(_cache, other._cache);
        std::swap(_handle, other._handle);
        this->owned = other.owned;
        this->index_searcher = std::move(other.index_searcher);
    }

    InvertedIndexCacheHandle& operator=(InvertedIndexCacheHandle&& other) noexcept {
        std::swap(_cache, other._cache);
        std::swap(_handle, other._handle);
        this->owned = other.owned;
        this->index_searcher = std::move(other.index_searcher);
        return *this;
    }

    IndexSearcherPtr get_index_searcher() {
        if (owned) {
            return index_searcher;
        } else {
            return ((InvertedIndexSearcherCache::CacheValue*)_cache->value(_handle))
                    ->index_searcher;
        }
    }

public:
    // If set to true, the loaded index_searcher will be saved in index_searcher, not in lru cache;
    bool owned = false;
    IndexSearcherPtr index_searcher;

private:
    Cache* _cache = nullptr;
    Cache::Handle* _handle = nullptr;

    // Don't allow copy and assign
    DISALLOW_COPY_AND_ASSIGN(InvertedIndexCacheHandle);
};

} // namespace segment_v2
} // namespace doris
