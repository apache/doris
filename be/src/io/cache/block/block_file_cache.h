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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/Cache/FileCache.h
// and modified by Doris

#pragma once

#include <bvar/bvar.h>
#include <gen_cpp/Types_types.h>
#include <stdint.h>

#include <cstddef>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "io/cache/block/block_file_cache_fwd.h"
#include "io/cache/block/block_file_cache_settings.h"
#include "io/fs/file_reader.h"
#include "io/io_common.h"
#include "util/hash_util.hpp"
#include "vec/common/uint128.h"

namespace doris {
namespace io {
class FileBlock;

using FileBlockSPtr = std::shared_ptr<FileBlock>;
using FileBlocks = std::list<FileBlockSPtr>;
struct FileBlocksHolder;

enum CacheType {
    INDEX,
    NORMAL,
    DISPOSABLE,
};

struct CacheContext {
    CacheContext(const IOContext* io_ctx) {
        if (io_ctx->is_index_data) {
            cache_type = CacheType::INDEX;
        } else if (io_ctx->is_disposable) {
            cache_type = CacheType::DISPOSABLE;
        } else {
            cache_type = CacheType::NORMAL;
        }
        query_id = io_ctx->query_id ? *io_ctx->query_id : TUniqueId();
    }
    CacheContext() = default;
    TUniqueId query_id;
    CacheType cache_type;
};

/**
 * Local cache for remote filesystem files, represented as a set of non-overlapping non-empty file segments.
 */
class IFileCache {
    friend class FileBlock;
    friend struct FileBlocksHolder;

public:
    /// use version 2 when USE_CACHE_VERSION2 = true, while use version 1 if false
    /// version 1.0: cache_base_path / key / offset
    /// version 2.0: cache_base_path / key_prefix / key / offset
    static constexpr bool USE_CACHE_VERSION2 = true;
    static constexpr int KEY_PREFIX_LENGTH = 3;

    struct Key {
        uint128_t key;
        std::string to_string() const;

        Key() = default;
        explicit Key(const uint128_t& key) : key(key) {}

        bool operator==(const Key& other) const { return key == other.key; }
    };

    IFileCache(const std::string& cache_base_path, const FileCacheSettings& cache_settings);

    virtual ~IFileCache() = default;

    /// Restore cache from local filesystem.
    virtual Status initialize() = 0;

    /// Cache capacity in bytes.
    size_t capacity() const { return _total_size; }

    static Key hash(const std::string& path);

    virtual size_t try_release() = 0;

    std::string get_path_in_local_cache(const Key& key, size_t offset, CacheType type) const;

    std::string get_path_in_local_cache(const Key& key) const;

    std::string get_version_path() const;

    const std::string& get_base_path() const { return _cache_base_path; }

    /**
     * Given an `offset` and `size` representing [offset, offset + size) bytes interval,
     * return list of cached non-overlapping non-empty
     * file segments `[segment1, ..., segmentN]` which intersect with given interval.
     *
     * Segments in returned list are ordered in ascending order and represent a full contiguous
     * interval (no holes). Each segment in returned list has state: DOWNLOADED, DOWNLOADING or EMPTY.
     *
     * As long as pointers to returned file segments are hold
     * it is guaranteed that these file segments are not removed from cache.
     */
    virtual FileBlocksHolder get_or_set(const Key& key, size_t offset, size_t size,
                                        const CacheContext& context) = 0;

    /// For debug.
    virtual std::string dump_structure(const Key& key) = 0;

    virtual size_t get_used_cache_size(CacheType type) const = 0;

    virtual size_t get_file_segments_num(CacheType type) const = 0;

    static std::string cache_type_to_string(CacheType type);
    static CacheType string_to_cache_type(const std::string& str);

    IFileCache& operator=(const IFileCache&) = delete;
    IFileCache(const IFileCache&) = delete;

protected:
    std::string _cache_base_path;
    size_t _total_size = 0;
    size_t _max_file_segment_size = 0;
    size_t _max_query_cache_size = 0;
    // metrics
    std::shared_ptr<bvar::Status<size_t>> _cur_size_metrics;

    bool _is_initialized = false;

    mutable std::mutex _mutex;

    virtual bool try_reserve(const Key& key, const CacheContext& context, size_t offset,
                             size_t size, std::lock_guard<std::mutex>& cache_lock) = 0;

    virtual void remove(FileBlockSPtr file_segment, std::lock_guard<std::mutex>& cache_lock,
                        std::lock_guard<std::mutex>& segment_lock) = 0;

    class LRUQueue {
    public:
        LRUQueue() = default;
        LRUQueue(size_t max_size, size_t max_element_size, int64_t hot_data_interval)
                : max_size(max_size),
                  max_element_size(max_element_size),
                  hot_data_interval(hot_data_interval) {}
        struct FileKeyAndOffset {
            Key key;
            size_t offset;
            size_t size;

            FileKeyAndOffset(const Key& key, size_t offset, size_t size)
                    : key(key), offset(offset), size(size) {}
        };

        using Iterator = typename std::list<FileKeyAndOffset>::iterator;

        size_t get_max_size() const { return max_size; }
        size_t get_max_element_size() const { return max_element_size; }

        size_t get_total_cache_size(std::lock_guard<std::mutex>& /* cache_lock */) const {
            return cache_size;
        }

        size_t get_elements_num(std::lock_guard<std::mutex>& /* cache_lock */) const {
            return queue.size();
        }

        Iterator add(const Key& key, size_t offset, size_t size,
                     std::lock_guard<std::mutex>& cache_lock);

        void remove(Iterator queue_it, std::lock_guard<std::mutex>& cache_lock);

        void move_to_end(Iterator queue_it, std::lock_guard<std::mutex>& cache_lock);

        std::string to_string(std::lock_guard<std::mutex>& cache_lock) const;

        bool contains(const Key& key, size_t offset, std::lock_guard<std::mutex>& cache_lock) const;

        Iterator begin() { return queue.begin(); }

        Iterator end() { return queue.end(); }

        void remove_all(std::lock_guard<std::mutex>& cache_lock);

        int64_t get_hot_data_interval() const { return hot_data_interval; }

    private:
        size_t max_size;
        size_t max_element_size;
        std::list<FileKeyAndOffset> queue;
        size_t cache_size = 0;
        int64_t hot_data_interval {0};
    };

    using AccessKeyAndOffset = std::tuple<Key, size_t>;
    struct KeyAndOffsetHash {
        std::size_t operator()(const AccessKeyAndOffset& key) const {
            return UInt128Hash()(std::get<0>(key).key) ^ std::hash<uint64_t>()(std::get<1>(key));
        }
    };

    using AccessRecord =
            std::unordered_map<AccessKeyAndOffset, LRUQueue::Iterator, KeyAndOffsetHash>;

    /// Used to track and control the cache access of each query.
    /// Through it, we can realize the processing of different queries by the cache layer.
    struct QueryFileCacheContext {
        LRUQueue lru_queue;
        AccessRecord records;

        size_t max_cache_size = 0;

        QueryFileCacheContext(size_t max_cache_size) : max_cache_size(max_cache_size) {}

        void remove(const Key& key, size_t offset, std::lock_guard<std::mutex>& cache_lock);

        void reserve(const Key& key, size_t offset, size_t size,
                     std::lock_guard<std::mutex>& cache_lock);

        size_t get_max_cache_size() const { return max_cache_size; }

        size_t get_cache_size(std::lock_guard<std::mutex>& cache_lock) const {
            return lru_queue.get_total_cache_size(cache_lock);
        }

        LRUQueue& queue() { return lru_queue; }
    };

    using QueryFileCacheContextPtr = std::shared_ptr<QueryFileCacheContext>;
    using QueryFileCacheContextMap = std::unordered_map<TUniqueId, QueryFileCacheContextPtr>;

    QueryFileCacheContextMap _query_map;

    bool _enable_file_cache_query_limit = config::enable_file_cache_query_limit;

    QueryFileCacheContextPtr get_query_context(const TUniqueId& query_id,
                                               std::lock_guard<std::mutex>&);

    void remove_query_context(const TUniqueId& query_id);

    QueryFileCacheContextPtr get_or_set_query_context(const TUniqueId& query_id,
                                                      std::lock_guard<std::mutex>&);

public:
    /// Save a query context information, and adopt different cache policies
    /// for different queries through the context cache layer.
    struct QueryFileCacheContextHolder {
        QueryFileCacheContextHolder(const TUniqueId& query_id, IFileCache* cache,
                                    QueryFileCacheContextPtr context)
                : query_id(query_id), cache(cache), context(context) {}

        QueryFileCacheContextHolder& operator=(const QueryFileCacheContextHolder&) = delete;
        QueryFileCacheContextHolder(const QueryFileCacheContextHolder&) = delete;

        ~QueryFileCacheContextHolder() {
            /// If only the query_map and the current holder hold the context_query,
            /// the query has been completed and the query_context is released.
            if (context) {
                context.reset();
                cache->remove_query_context(query_id);
            }
        }

        const TUniqueId& query_id;
        IFileCache* cache = nullptr;
        QueryFileCacheContextPtr context;
    };
    using QueryFileCacheContextHolderPtr = std::unique_ptr<QueryFileCacheContextHolder>;
    QueryFileCacheContextHolderPtr get_query_context_holder(const TUniqueId& query_id);

private:
    static inline std::list<std::pair<AccessKeyAndOffset, std::shared_ptr<FileReader>>>
            s_file_reader_cache;
    static inline std::unordered_map<AccessKeyAndOffset, decltype(s_file_reader_cache.begin()),
                                     KeyAndOffsetHash>
            s_file_name_to_reader;
    static inline std::mutex s_file_reader_cache_mtx;
    static inline std::atomic_bool s_read_only {false};
    static inline uint64_t _max_file_reader_cache_size = 65533;

public:
    // should be call when BE start
    static void init();

    static void set_read_only(bool read_only);

    static bool read_only() { return s_read_only; }

    static std::weak_ptr<FileReader> cache_file_reader(const AccessKeyAndOffset& key,
                                                       std::shared_ptr<FileReader> file_reader);

    static void remove_file_reader(const AccessKeyAndOffset& key);

    // use for test
    static bool contains_file_reader(const AccessKeyAndOffset& key);
    static size_t file_reader_cache_size();
};

using CloudFileCachePtr = IFileCache*;

struct KeyHash {
    std::size_t operator()(const IFileCache::Key& k) const { return UInt128Hash()(k.key); }
};

} // namespace io
} // namespace doris
