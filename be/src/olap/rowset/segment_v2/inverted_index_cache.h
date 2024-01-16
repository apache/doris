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

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wshadow-field"
#endif
#include <CLucene.h> // IWYU pragma: keep
#ifdef __clang__
#pragma clang diagnostic pop
#endif
#include <CLucene/config/repl_wchar.h>
#include <CLucene/util/Misc.h>
#include <butil/macros.h>
#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <atomic>
#include <memory>
#include <optional>
#include <roaring/roaring.hh>
#include <string>
#include <utility>
#include <variant>

#include "common/config.h"
#include "common/status.h"
#include "io/fs/file_system.h"
#include "io/fs/path.h"
#include "olap/lru_cache.h"
#include "olap/rowset/segment_v2/inverted_index_query_type.h"
#include "runtime/exec_env.h"
#include "runtime/memory/lru_cache_policy.h"
#include "runtime/memory/mem_tracker.h"
#include "util/slice.h"
#include "util/time.h"

namespace lucene {
namespace search {
class IndexSearcher;
} // namespace search
namespace util {
namespace bkd {
class bkd_reader;
}
} // namespace util
} // namespace lucene

namespace doris {
struct OlapReaderStatistics;

namespace segment_v2 {
using FulltextIndexSearcherPtr = std::shared_ptr<lucene::search::IndexSearcher>;
using BKDIndexSearcherPtr = std::shared_ptr<lucene::util::bkd::bkd_reader>;
using IndexSearcherPtr = std::variant<FulltextIndexSearcherPtr, BKDIndexSearcherPtr>;
using OptionalIndexSearcherPtr = std::optional<IndexSearcherPtr>;

class InvertedIndexCacheHandle;
class DorisCompoundReader;

class IndexSearcherBuilder {
public:
    virtual Status build(DorisCompoundReader* directory,
                         OptionalIndexSearcherPtr& output_searcher) = 0;
    virtual ~IndexSearcherBuilder() = default;
};

class FulltextIndexSearcherBuilder : public IndexSearcherBuilder {
public:
    Status build(DorisCompoundReader* directory,
                 OptionalIndexSearcherPtr& output_searcher) override;
};

class BKDIndexSearcherBuilder : public IndexSearcherBuilder {
public:
    Status build(DorisCompoundReader* directory,
                 OptionalIndexSearcherPtr& output_searcher) override;
};

class InvertedIndexSearcherCache {
public:
    // The cache key of index_searcher lru cache
    struct CacheKey {
        CacheKey(std::string index_file_path) : index_file_path(std::move(index_file_path)) {}
        std::string index_file_path;
    };

    // The cache value of index_searcher lru cache.
    // Holding an opened index_searcher.
    struct CacheValue : public LRUCacheValueBase {
        IndexSearcherPtr index_searcher;
    };

    // Create global instance of this class.
    // "capacity" is the capacity of lru cache.
    static InvertedIndexSearcherCache* create_global_instance(size_t capacity,
                                                              uint32_t num_shards = 16);

    // Return global instance.
    // Client should call create_global_cache before.
    static InvertedIndexSearcherCache* instance() {
        return ExecEnv::GetInstance()->get_inverted_index_searcher_cache();
    }

    InvertedIndexSearcherCache(size_t capacity, uint32_t num_shards);

    Status get_index_searcher(const io::FileSystemSPtr& fs, const std::string& index_dir,
                              const std::string& file_name, InvertedIndexCacheHandle* cache_handle,
                              OlapReaderStatistics* stats, InvertedIndexReaderType reader_type,
                              bool& has_null, bool use_cache = true);

    // function `insert` called after inverted index writer close
    Status insert(const io::FileSystemSPtr& fs, const std::string& index_dir,
                  const std::string& file_name, InvertedIndexReaderType reader_type);

    // function `erase` called after compaction remove segment
    Status erase(const std::string& index_file_path);

    void release(Cache::Handle* handle) { _policy->cache()->release(handle); }

    int64_t mem_consumption();

private:
    InvertedIndexSearcherCache();

    class InvertedIndexSearcherCachePolicy : public LRUCachePolicy {
    public:
        InvertedIndexSearcherCachePolicy(size_t capacity, uint32_t num_shards,
                                         uint32_t element_count_capacity)
                : LRUCachePolicy(CachePolicy::CacheType::INVERTEDINDEX_SEARCHER_CACHE, capacity,
                                 LRUCacheType::SIZE,
                                 config::inverted_index_cache_stale_sweep_time_sec, num_shards,
                                 element_count_capacity, true) {}
        InvertedIndexSearcherCachePolicy(size_t capacity, uint32_t num_shards,
                                         uint32_t element_count_capacity,
                                         CacheValueTimeExtractor cache_value_time_extractor,
                                         bool cache_value_check_timestamp)
                : LRUCachePolicy(CachePolicy::CacheType::INVERTEDINDEX_SEARCHER_CACHE, capacity,
                                 LRUCacheType::SIZE,
                                 config::inverted_index_cache_stale_sweep_time_sec, num_shards,
                                 element_count_capacity, cache_value_time_extractor,
                                 cache_value_check_timestamp, true) {}
    };

    // Lookup the given index_searcher in the cache.
    // If the index_searcher is found, the cache entry will be written into handle.
    // Return true if entry is found, otherwise return false.
    bool _lookup(const InvertedIndexSearcherCache::CacheKey& key, InvertedIndexCacheHandle* handle);

    // Insert a cache entry by key.
    // And the cache entry will be returned in handle.
    // This function is thread-safe.
    Cache::Handle* _insert(const InvertedIndexSearcherCache::CacheKey& key, CacheValue* value);

    std::unique_ptr<InvertedIndexSearcherCachePolicy> _policy;
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
    // If index searcher include non-null bitmap.
    bool has_null = true;
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
        std::string value;                 // query value

        // Encode to a flat binary which can be used as LRUCache's key
        std::string encode() const {
            std::string key_buf(index_path.string());
            key_buf.append("/");
            key_buf.append(column_name);
            key_buf.append("/");
            auto query_type_str = query_type_to_string(query_type);
            if (query_type_str.empty()) {
                return "";
            }
            key_buf.append(query_type_str);
            key_buf.append("/");
            key_buf.append(value);
            return key_buf;
        }
    };

    struct CacheValue : public LRUCacheValueBase {
        std::shared_ptr<roaring::Roaring> bitmap;
    };

    // Create global instance of this class
    static InvertedIndexQueryCache* create_global_cache(size_t capacity, uint32_t num_shards = 16) {
        InvertedIndexQueryCache* res = new InvertedIndexQueryCache(capacity, num_shards);
        return res;
    }

    // Return global instance.
    // Client should call create_global_cache before.
    static InvertedIndexQueryCache* instance() {
        return ExecEnv::GetInstance()->get_inverted_index_query_cache();
    }

    InvertedIndexQueryCache() = delete;

    InvertedIndexQueryCache(size_t capacity, uint32_t num_shards)
            : LRUCachePolicy(CachePolicy::CacheType::INVERTEDINDEX_QUERY_CACHE, capacity,
                             LRUCacheType::SIZE, config::inverted_index_cache_stale_sweep_time_sec,
                             num_shards) {}

    bool lookup(const CacheKey& key, InvertedIndexQueryCacheHandle* handle);

    void insert(const CacheKey& key, std::shared_ptr<roaring::Roaring> bitmap,
                InvertedIndexQueryCacheHandle* handle);

    int64_t mem_consumption();
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
