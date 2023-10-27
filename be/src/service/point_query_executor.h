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

#include <assert.h>
#include <butil/macros.h>
#include <butil/time.h>
#include <gen_cpp/Metrics_types.h>
#include <parallel_hashmap/phmap.h>
#include <stdint.h>
#include <string.h>

#include <algorithm>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "butil/containers/doubly_buffered_data.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "gutil/int128.h"
#include "olap/lru_cache.h"
#include "olap/olap_common.h"
#include "olap/rowset/rowset.h"
#include "olap/tablet.h"
#include "olap/utils.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "util/mysql_global.h"
#include "util/runtime_profile.h"
#include "util/slice.h"
#include "vec/core/block.h"
#include "vec/data_types/serde/data_type_serde.h"
#include "vec/exprs/vexpr_fwd.h"

namespace doris {

class PTabletKeyLookupRequest;
class PTabletKeyLookupResponse;
class RuntimeState;
class TDescriptorTable;
class TExpr;

// For caching point lookup pre allocted blocks and exprs
class Reusable {
public:
    ~Reusable();

    bool is_expired(int64_t ttl_ms) const {
        return butil::gettimeofday_ms() - _create_timestamp > ttl_ms;
    }

    Status init(const TDescriptorTable& t_desc_tbl, const std::vector<TExpr>& output_exprs,
                int tuple_id, size_t block_size = 1);

    std::unique_ptr<vectorized::Block> get_block();

    const vectorized::DataTypeSerDeSPtrs& get_data_type_serdes() const { return _data_type_serdes; }

    const std::unordered_map<uint32_t, uint32_t>& get_col_uid_to_idx() const {
        return _col_uid_to_idx;
    }

    const std::vector<std::string>& get_col_default_values() const { return _col_default_values; }

    // do not touch block after returned
    void return_block(std::unique_ptr<vectorized::Block>& block);

    TupleDescriptor* tuple_desc() { return _tuple_desc; }

    const vectorized::VExprContextSPtrs& output_exprs() { return _output_exprs_ctxs; }

    int64_t mem_size() const;

private:
    // caching TupleDescriptor, output_expr, etc...
    std::unique_ptr<RuntimeState> _runtime_state;
    DescriptorTbl* _desc_tbl = nullptr;
    TupleDescriptor* _tuple_desc = nullptr;
    std::mutex _block_mutex;
    // prevent from allocte too many tmp blocks
    std::vector<std::unique_ptr<vectorized::Block>> _block_pool;
    vectorized::VExprContextSPtrs _output_exprs_ctxs;
    int64_t _create_timestamp = 0;
    vectorized::DataTypeSerDeSPtrs _data_type_serdes;
    std::unordered_map<uint32_t, uint32_t> _col_uid_to_idx;
    std::vector<std::string> _col_default_values;
    int64_t _mem_size = 0;
};

// RowCache is a LRU cache for row store
class RowCache {
public:
    // The cache key for row lru cache
    struct RowCacheKey {
        RowCacheKey(int64_t tablet_id, const Slice& key) : tablet_id(tablet_id), key(key) {}
        int64_t tablet_id;
        Slice key;

        // Encode to a flat binary which can be used as LRUCache's key
        std::string encode() const {
            std::string full_key;
            full_key.resize(sizeof(int64_t) + key.size);
            int8store(&full_key.front(), tablet_id);
            memcpy((&full_key.front()) + sizeof(tablet_id), key.data, key.size);
            return full_key;
        }
    };

    // A handle for RowCache entry. This class make it easy to handle
    // Cache entry. Users don't need to release the obtained cache entry. This
    // class will release the cache entry when it is destroyed.
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
        Slice data() const { return _cache->value_slice(_handle); }

    private:
        Cache* _cache = nullptr;
        Cache::Handle* _handle = nullptr;

        // Don't allow copy and assign
        DISALLOW_COPY_AND_ASSIGN(CacheHandle);
    };

    // Create global instance of this class
    static RowCache* create_global_cache(int64_t capacity, uint32_t num_shards = kDefaultNumShards);

    static RowCache* instance();

    // Lookup a row key from cache,
    // If the Row key is found, the cache entry will be written into handle.
    // CacheHandle will release cache entry to cache when it destructs
    // Return true if entry is found, otherwise return false.
    bool lookup(const RowCacheKey& key, CacheHandle* handle);

    // Insert a row with key into this cache.
    // This function is thread-safe, and when two clients insert two same key
    // concurrently, this function can assure that only one page is cached.
    // The in_memory page will have higher priority.
    void insert(const RowCacheKey& key, const Slice& data);

    //
    void erase(const RowCacheKey& key);

private:
    static constexpr uint32_t kDefaultNumShards = 128;
    RowCache(int64_t capacity, int num_shards = kDefaultNumShards);
    std::unique_ptr<Cache> _cache = nullptr;
};

// A cache used for prepare stmt.
// One connection per stmt perf uuid
class LookupConnectionCache : public LRUCachePolicy {
public:
    static LookupConnectionCache* instance() {
        return ExecEnv::GetInstance()->get_lookup_connection_cache();
    }

    static LookupConnectionCache* create_global_instance(size_t capacity);

private:
    friend class PointQueryExecutor;
    LookupConnectionCache(size_t capacity)
            : LRUCachePolicy(CachePolicy::CacheType::LOOKUP_CONNECTION_CACHE, capacity,
                             LRUCacheType::SIZE, config::tablet_lookup_cache_clean_interval) {}

    std::string encode_key(__int128_t cache_id) {
        fmt::memory_buffer buffer;
        fmt::format_to(buffer, "{}", cache_id);
        return std::string(buffer.data(), buffer.size());
    }

    void add(__int128_t cache_id, std::shared_ptr<Reusable> item) {
        std::string key = encode_key(cache_id);
        CacheValue* value = new CacheValue;
        value->last_visit_time = UnixMillis();
        value->item = item;
        auto deleter = [](const doris::CacheKey& key, void* value) {
            CacheValue* cache_value = (CacheValue*)value;
            delete cache_value;
        };
        LOG(INFO) << "Add item mem size " << item->mem_size()
                  << ", cache_capacity: " << _cache->get_total_capacity()
                  << ", cache_usage: " << _cache->get_usage()
                  << ", mem_consum: " << _cache->mem_consumption();
        auto lru_handle =
                _cache->insert(key, value, item->mem_size(), deleter, CachePriority::NORMAL);
        _cache->release(lru_handle);
    }

    std::shared_ptr<Reusable> get(__int128_t cache_id) {
        std::string key = encode_key(cache_id);
        auto lru_handle = _cache->lookup(key);
        if (lru_handle) {
            Defer release([cache = _cache.get(), lru_handle] { cache->release(lru_handle); });
            auto value = (CacheValue*)_cache->value(lru_handle);
            value->last_visit_time = UnixMillis();
            return value->item;
        }
        return nullptr;
    }

    struct CacheValue : public LRUCacheValueBase {
        std::shared_ptr<Reusable> item = nullptr;
    };
};

struct Metrics {
    Metrics()
            : init_ns(TUnit::TIME_NS),
              init_key_ns(TUnit::TIME_NS),
              lookup_key_ns(TUnit::TIME_NS),
              lookup_data_ns(TUnit::TIME_NS),
              output_data_ns(TUnit::TIME_NS) {}
    RuntimeProfile::Counter init_ns;
    RuntimeProfile::Counter init_key_ns;
    RuntimeProfile::Counter lookup_key_ns;
    RuntimeProfile::Counter lookup_data_ns;
    RuntimeProfile::Counter output_data_ns;
    OlapReaderStatistics read_stats;
    size_t row_cache_hits = 0;
    bool hit_lookup_cache = false;
    size_t result_data_bytes;
};

// An util to do tablet lookup
class PointQueryExecutor {
public:
    Status init(const PTabletKeyLookupRequest* request, PTabletKeyLookupResponse* response);

    Status lookup_up();

    std::string print_profile();

private:
    Status _init_keys(const PTabletKeyLookupRequest* request);

    Status _lookup_row_key();

    Status _lookup_row_data();

    Status _output_data();

    static void release_rowset(RowsetSharedPtr* r) {
        if (r && *r) {
            VLOG_DEBUG << "release rowset " << (*r)->rowset_id();
            (*r)->release();
        }
        delete r;
    }

    // Read context for each row
    struct RowReadContext {
        RowReadContext() : _rowset_ptr(nullptr, &release_rowset) {}
        std::string _primary_key;
        RowCache::CacheHandle _cached_row_data;
        std::optional<RowLocation> _row_location;
        // rowset will be aquired during read
        // and released after used
        std::unique_ptr<RowsetSharedPtr, decltype(&release_rowset)> _rowset_ptr;
    };

    PTabletKeyLookupResponse* _response;
    TabletSharedPtr _tablet;
    std::vector<RowReadContext> _row_read_ctxs;
    std::shared_ptr<Reusable> _reusable;
    std::unique_ptr<vectorized::Block> _result_block;
    Metrics _profile_metrics;
    bool _binary_row_format = false;
};

} // namespace doris
