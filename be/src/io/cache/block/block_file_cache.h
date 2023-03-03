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

#include <list>
#include <memory>
#include <unordered_map>

#include "common/config.h"
#include "io/cache/block/block_file_cache_fwd.h"

namespace doris {
namespace io {
class FileBlock;
using FileBlockSPtr = std::shared_ptr<FileBlock>;
using FileBlocks = std::list<FileBlockSPtr>;
struct FileBlocksHolder;
struct ReadSettings;

/**
 * Local cache for remote filesystem files, represented as a set of non-overlapping non-empty file segments.
 */
class IFileCache {
    friend class FileBlock;
    friend struct FileBlocksHolder;

public:
    static const std::string FILE_CACHE_VERSION;
    static const int KEY_PREFIX_LENGTH;

    struct Key {
        uint128_t key;
        std::string to_string() const;

        Key() = default;
        explicit Key(const uint128_t& key_) : key(key_) {}

        bool operator==(const Key& other) const { return key == other.key; }
    };

    IFileCache(const std::string& cache_base_path, const FileCacheSettings& cache_settings);

    virtual ~IFileCache() = default;

    /// Restore cache from local filesystem.
    virtual Status initialize() = 0;

    virtual void remove_if_exists(const Key& key, bool is_persistent) = 0;

    virtual void remove_if_releasable(bool is_persistent) = 0;

    /// Cache capacity in bytes.
    size_t capacity() const { return _max_size; }

    static Key hash(const std::string& path);

    std::string get_path_in_local_cache(const Key& key, size_t offset, bool is_persistent) const;

    std::string get_path_in_local_cache(const Key& key) const;

    std::string get_version_path() const;

    const std::string& get_base_path() const { return _cache_base_path; }

    virtual std::vector<std::string> try_get_cache_paths(const Key& key, bool is_persistent) = 0;

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
                                        bool is_persistent, const TUniqueId& query_id) = 0;

    /// For debug.
    virtual std::string dump_structure(const Key& key, bool is_persistent) = 0;

    virtual size_t get_used_cache_size(bool is_persistent) const = 0;

    virtual size_t get_file_segments_num(bool is_persistent) const = 0;

    IFileCache& operator=(const IFileCache&) = delete;
    IFileCache(const IFileCache&) = delete;

protected:
    std::string _cache_base_path;
    size_t _max_size = 0;
    size_t _max_element_size = 0;
    size_t _persistent_max_size = 0;
    size_t _persistent_max_element_size = 0;
    size_t _max_file_segment_size = 0;
    size_t _max_query_cache_size = 0;

    bool _is_initialized = false;

    mutable std::mutex _mutex;

    virtual bool try_reserve(const Key& key, const TUniqueId& query_id, bool is_persistent,
                             size_t offset, size_t size,
                             std::lock_guard<std::mutex>& cache_lock) = 0;

    virtual void remove(const Key& key, bool is_persistent, size_t offset,
                        std::lock_guard<std::mutex>& cache_lock,
                        std::lock_guard<std::mutex>& segment_lock) = 0;

    class LRUQueue {
    public:
        struct FileKeyAndOffset {
            Key key;
            size_t offset;
            size_t size;
            bool is_persistent;

            FileKeyAndOffset(const Key& key, size_t offset, size_t size, bool is_persistent)
                    : key(key), offset(offset), size(size), is_persistent(is_persistent) {}
        };

        using Iterator = typename std::list<FileKeyAndOffset>::iterator;

        size_t get_total_cache_size(std::lock_guard<std::mutex>& /* cache_lock */) const {
            return cache_size;
        }

        size_t get_elements_num(std::lock_guard<std::mutex>& /* cache_lock */) const {
            return queue.size();
        }

        Iterator add(const Key& key, size_t offset, bool is_persistent, size_t size,
                     std::lock_guard<std::mutex>& cache_lock);

        void remove(Iterator queue_it, std::lock_guard<std::mutex>& cache_lock);

        void move_to_end(Iterator queue_it, std::lock_guard<std::mutex>& cache_lock);

        std::string to_string(std::lock_guard<std::mutex>& cache_lock) const;

        bool contains(const Key& key, size_t offset, std::lock_guard<std::mutex>& cache_lock) const;

        Iterator begin() { return queue.begin(); }

        Iterator end() { return queue.end(); }

        void remove_all(std::lock_guard<std::mutex>& cache_lock);

    private:
        std::list<FileKeyAndOffset> queue;
        size_t cache_size = 0;
    };

    using AccessKeyAndOffset = std::tuple<Key, size_t, bool>;
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

        void remove(const Key& key, size_t offset, bool is_presistent, size_t size,
                    std::lock_guard<std::mutex>& cache_lock);

        void reserve(const Key& key, size_t offset, bool is_presistent, size_t size,
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
};

using CloudFileCachePtr = IFileCache*;

struct KeyHash {
    std::size_t operator()(const IFileCache::Key& k) const { return UInt128Hash()(k.key); }
};

} // namespace io
} // namespace doris
