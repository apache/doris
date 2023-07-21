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

#include <CLucene.h> // IWYU pragma: keep
#include <CLucene/config/repl_wchar.h>
#include <CLucene/util/Misc.h>
#include <butil/macros.h>
#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <atomic>
#include <memory>
#include <roaring/roaring.hh>
#include <string>
#include <utility>

#include "common/config.h"
#include "common/status.h"
#include "io/fs/file_system.h"
#include "io/fs/path.h"
#include "olap/lru_cache.h"
#include "olap/rowset/segment_v2/inverted_index_query_type.h"
#include "runtime/memory/lru_cache_policy.h"
#include "runtime/memory/mem_tracker.h"
#include "util/slice.h"
#include "util/time.h"

namespace lucene {
namespace search {
class IndexSearcher;
} // namespace search
} // namespace lucene

namespace doris {
struct OlapReaderStatistics;

namespace segment_v2 {
using IndexSearcherPtr = std::shared_ptr<lucene::search::IndexSearcher>;

class InvertedIndexCacheHandle;

class InvertedIndexSearcherCache : public LRUCachePolicy {
public:
    // The cache key of index_searcher lru cache
    struct CacheKey {
        CacheKey(std::string index_file_path) : index_file_path(index_file_path) {}
        std::string index_file_path;
    };

    // The cache value of index_searcher lru cache.
    // Holding a opened index_searcher.
    struct CacheValue : public LRUCacheValueBase {
        IndexSearcherPtr index_searcher;
    };

    // Create global instance of this class.
    // "capacity" is the capacity of lru cache.
    static void create_global_instance(size_t capacity, uint32_t num_shards = 16);

    void reset() {
        _cache.reset();
        _mem_tracker.reset();
        // Reset or clear the state of the object.
    }

    static void reset_global_instance() {
        if (_s_instance != nullptr) {
            _s_instance->reset();
        }
    }

    // Return global instance.
    // Client should call create_global_cache before.
    static InvertedIndexSearcherCache* instance() { return _s_instance; }

    static IndexSearcherPtr build_index_searcher(const io::FileSystemSPtr& fs,
                                                 const std::string& index_dir,
                                                 const std::string& file_name);

    InvertedIndexSearcherCache(size_t capacity, uint32_t num_shards);

    Status get_index_searcher(const io::FileSystemSPtr& fs, const std::string& index_dir,
                              const std::string& file_name, InvertedIndexCacheHandle* cache_handle,
                              OlapReaderStatistics* stats, bool use_cache = true);

    // function `insert` called after inverted index writer close
    Status insert(const io::FileSystemSPtr& fs, const std::string& index_dir,
                  const std::string& file_name);

    // function `erase` called after compaction remove segment
    Status erase(const std::string& index_file_path);

    int64_t mem_consumption();

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

class InvertedIndexQueryCacheHandle;

class InvertedIndexQueryCache : public LRUCachePolicy {
public:
    // cache key
    struct CacheKey {
        io::Path index_path;               // index file path
        std::string column_name;           // column name
        InvertedIndexQueryType query_type; // query type
        std::wstring value;                // query value

        // Encode to a flat binary which can be used as LRUCache's key
        std::string encode() const {
            std::string key_buf(index_path.string());
            key_buf.append("/");
            key_buf.append(column_name);
            key_buf.append("/");
            auto query_type_str = InvertedIndexQueryType_toString(query_type);
            if (query_type_str.empty()) {
                return "";
            }
            key_buf.append(query_type_str);
            key_buf.append("/");
            auto str = lucene_wcstoutf8string(value.c_str(), value.length());
            key_buf.append(str);
            return key_buf;
        }
    };

    struct CacheValue : public LRUCacheValueBase {
        std::shared_ptr<roaring::Roaring> bitmap;
    };

    // Create global instance of this class
    static void create_global_cache(size_t capacity, uint32_t num_shards = 16) {
        DCHECK(_s_instance == nullptr);
        static InvertedIndexQueryCache instance(capacity, num_shards);
        _s_instance = &instance;
    }

    // Return global instance.
    // Client should call create_global_cache before.
    static InvertedIndexQueryCache* instance() { return _s_instance; }

    InvertedIndexQueryCache() = delete;

    InvertedIndexQueryCache(size_t capacity, uint32_t num_shards)
            : LRUCachePolicy("InvertedIndexQueryCache", capacity, LRUCacheType::SIZE,
                             config::inverted_index_cache_stale_sweep_time_sec, num_shards) {}

    bool lookup(const CacheKey& key, InvertedIndexQueryCacheHandle* handle);

    void insert(const CacheKey& key, std::shared_ptr<roaring::Roaring> bitmap,
                InvertedIndexQueryCacheHandle* handle);

    int64_t mem_consumption();

private:
    static InvertedIndexQueryCache* _s_instance;
};

class InvertedIndexQueryCacheHandle {
public:
    InvertedIndexQueryCacheHandle() {}

    InvertedIndexQueryCacheHandle(Cache* cache, Cache::Handle* handle)
            : _cache(cache), _handle(handle) {}

    ~InvertedIndexQueryCacheHandle() {
        if (_handle != nullptr) {
            _cache->release(_handle);
        }
    }

    InvertedIndexQueryCacheHandle(InvertedIndexQueryCacheHandle&& other) noexcept {
        // we can use std::exchange if we switch c++14 on
        std::swap(_cache, other._cache);
        std::swap(_handle, other._handle);
    }

    InvertedIndexQueryCacheHandle& operator=(InvertedIndexQueryCacheHandle&& other) noexcept {
        std::swap(_cache, other._cache);
        std::swap(_handle, other._handle);
        return *this;
    }

    Cache* cache() const { return _cache; }
    Slice data() const { return _cache->value_slice(_handle); }

    std::shared_ptr<roaring::Roaring> get_bitmap() const {
        if (!_cache) {
            return nullptr;
        }
        return ((InvertedIndexQueryCache::CacheValue*)_cache->value(_handle))->bitmap;
    }

private:
    Cache* _cache = nullptr;
    Cache::Handle* _handle = nullptr;

    // Don't allow copy and assign
    DISALLOW_COPY_AND_ASSIGN(InvertedIndexQueryCacheHandle);
};

} // namespace segment_v2
} // namespace doris
