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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/Cache/FileCache_fwd.h
// and modified by Doris

#pragma once
#include <cstdint>
#include <vector>

#include "io/io_common.h"
#include "vec/common/uint128.h"

namespace doris::io {

inline static constexpr size_t REMOTE_FS_OBJECTS_CACHE_DEFAULT_ELEMENTS = 100 * 1024;
inline static constexpr size_t FILE_CACHE_MAX_FILE_BLOCK_SIZE = 1 * 1024 * 1024;
inline static constexpr size_t DEFAULT_NORMAL_PERCENT = 40;
inline static constexpr size_t DEFAULT_DISPOSABLE_PERCENT = 5;
inline static constexpr size_t DEFAULT_INDEX_PERCENT = 5;
inline static constexpr size_t DEFAULT_TTL_PERCENT = 50;

using uint128_t = vectorized::UInt128;

enum FileCacheType {
    INDEX = 2,
    NORMAL = 1,
    DISPOSABLE = 0,
    TTL = 3,
};
std::string cache_type_to_surfix(FileCacheType type);
FileCacheType surfix_to_cache_type(const std::string& str);

FileCacheType string_to_cache_type(const std::string& str);
std::string cache_type_to_string(FileCacheType type);

struct UInt128Wrapper {
    uint128_t value_;
    [[nodiscard]] std::string to_string() const;

    UInt128Wrapper() = default;
    explicit UInt128Wrapper(const uint128_t& value) : value_(value) {}

    bool operator==(const UInt128Wrapper& other) const { return value_ == other.value_; }

    uint64_t high() const { return static_cast<uint64_t>(value_ >> 64); }
    uint64_t low() const { return static_cast<uint64_t>(value_); }

    friend std::ostream& operator<<(std::ostream& os, const UInt128Wrapper& wrapper) {
        os << "UInt128Wrapper(" << wrapper.high() << ", " << wrapper.low() << ")";
        return os;
    }
};

struct ReadStatistics {
    bool hit_cache = true;
    bool from_peer_cache = false;
    bool skip_cache = false;
    int64_t bytes_read = 0;
    int64_t bytes_write_into_file_cache = 0;
    int64_t remote_read_timer = 0;
    int64_t peer_read_timer = 0;
    int64_t remote_wait_timer = 0; // wait for other downloader
    int64_t local_read_timer = 0;
    int64_t local_write_timer = 0;
    int64_t read_cache_file_directly_timer = 0;
    int64_t cache_get_or_set_timer = 0;
    int64_t lock_wait_timer = 0;
    int64_t get_timer = 0;
    int64_t set_timer = 0;
};

class BlockFileCache;
struct FileBlocksHolder;
using FileBlocksHolderPtr = std::unique_ptr<FileBlocksHolder>;

struct FileCacheAllocatorBuilder {
    bool _is_cold_data;
    uint64_t _expiration_time;
    UInt128Wrapper _cache_hash;
    BlockFileCache* _cache; // Only one ref, the lifetime is owned by FileCache
    FileBlocksHolderPtr allocate_cache_holder(size_t offset, size_t size, int64_t tablet_id) const;
};

struct KeyHash {
    std::size_t operator()(const UInt128Wrapper& w) const {
        return util_hash::HashLen16(w.value_.low(), w.value_.high());
    }
};

using AccessKeyAndOffset = std::pair<UInt128Wrapper, size_t>;
struct KeyAndOffsetHash {
    std::size_t operator()(const AccessKeyAndOffset& key) const {
        return KeyHash()(key.first) ^ std::hash<uint64_t>()(key.second);
    }
};

struct KeyMeta {
    uint64_t expiration_time; // absolute time
    FileCacheType type;
    int64_t tablet_id {0};
};

struct FileCacheKey {
    UInt128Wrapper hash;
    size_t offset;
    KeyMeta meta;
};

struct FileCacheSettings {
    size_t capacity {0};
    size_t disposable_queue_size {0};
    size_t disposable_queue_elements {0};
    size_t index_queue_size {0};
    size_t index_queue_elements {0};
    size_t query_queue_size {0};
    size_t query_queue_elements {0};
    size_t ttl_queue_size {0};
    size_t ttl_queue_elements {0};
    size_t max_file_block_size {0};
    size_t max_query_cache_size {0};
    std::string storage;

    // to string
    std::string to_string() const;
};

FileCacheSettings get_file_cache_settings(size_t capacity, size_t max_query_cache_size,
                                          size_t normal_percent = DEFAULT_NORMAL_PERCENT,
                                          size_t disposable_percent = DEFAULT_DISPOSABLE_PERCENT,
                                          size_t index_percent = DEFAULT_INDEX_PERCENT,
                                          size_t ttl_percent = DEFAULT_TTL_PERCENT,
                                          const std::string& storage = "disk");

struct CacheContext {
    CacheContext(const IOContext* io_context) {
        if (io_context->expiration_time != 0) {
            cache_type = FileCacheType::TTL;
            expiration_time = io_context->expiration_time;
        } else if (io_context->is_index_data) {
            cache_type = FileCacheType::INDEX;
        } else if (io_context->is_disposable) {
            cache_type = FileCacheType::DISPOSABLE;
        } else {
            cache_type = FileCacheType::NORMAL;
        }
        query_id = io_context->query_id ? *io_context->query_id : TUniqueId();
        is_warmup = io_context->is_warmup;
    }
    CacheContext() = default;
    bool operator==(const CacheContext& rhs) const {
        return query_id == rhs.query_id && cache_type == rhs.cache_type &&
               expiration_time == rhs.expiration_time && is_cold_data == rhs.is_cold_data;
    }
    TUniqueId query_id;
    FileCacheType cache_type;
    int64_t expiration_time {0};
    bool is_cold_data {false};
    ReadStatistics* stats;
    bool is_warmup {false};
    int64_t tablet_id {0};
};

template <class Lock>
concept IsXLock = std::same_as<Lock, std::lock_guard<std::mutex>> ||
                  std::same_as<Lock, std::unique_lock<std::mutex>>;

class LRUQueue {
public:
    LRUQueue() = default;
    LRUQueue(size_t max_size, size_t max_element_size, int64_t hot_data_interval)
            : max_size(max_size),
              max_element_size(max_element_size),
              hot_data_interval(hot_data_interval) {}

    struct HashFileKeyAndOffset {
        std::size_t operator()(const std::pair<UInt128Wrapper, size_t>& pair) const {
            return KeyHash()(pair.first) + pair.second;
        }
    };

    struct FileKeyAndOffset {
        UInt128Wrapper hash;
        size_t offset;
        size_t size;

        FileKeyAndOffset(const UInt128Wrapper& hash, size_t offset, size_t size)
                : hash(hash), offset(offset), size(size) {}
    };

    using Iterator = typename std::list<FileKeyAndOffset>::iterator;

    size_t get_max_size() const { return max_size; }
    size_t get_max_element_size() const { return max_element_size; }

    template <class T>
        requires IsXLock<T>
    size_t get_capacity(T& /* cache_lock */) const {
        return cache_size;
    }

    size_t get_capacity_unsafe() const { return cache_size; }

    size_t get_elements_num_unsafe() const { return queue.size(); }

    size_t get_elements_num(std::lock_guard<std::mutex>& /* cache_lock */) const {
        return queue.size();
    }

    Iterator add(const UInt128Wrapper& hash, size_t offset, size_t size,
                 std::lock_guard<std::mutex>& cache_lock);
    template <class T>
        requires IsXLock<T>
    void remove(Iterator queue_it, T& /* cache_lock */) {
        cache_size -= queue_it->size;
        map.erase(std::make_pair(queue_it->hash, queue_it->offset));
        queue.erase(queue_it);
    }

    void move_to_end(Iterator queue_it, std::lock_guard<std::mutex>& cache_lock);

    void resize(Iterator queue_it, size_t new_size, std::lock_guard<std::mutex>& cache_lock);

    std::string to_string(std::lock_guard<std::mutex>& cache_lock) const;

    bool contains(const UInt128Wrapper& hash, size_t offset,
                  std::lock_guard<std::mutex>& cache_lock) const;

    Iterator begin() { return queue.begin(); }

    Iterator end() { return queue.end(); }

    void remove_all(std::lock_guard<std::mutex>& cache_lock);

    Iterator get(const UInt128Wrapper& hash, size_t offset,
                 std::lock_guard<std::mutex>& /* cache_lock */) const;

    int64_t get_hot_data_interval() const { return hot_data_interval; }

    void clear(std::lock_guard<std::mutex>& cache_lock) {
        queue.clear();
        map.clear();
        cache_size = 0;
    }

    size_t levenshtein_distance_from(LRUQueue& base, std::lock_guard<std::mutex>& cache_lock);

    size_t max_size;
    size_t max_element_size;
    std::list<FileKeyAndOffset> queue;
    std::unordered_map<std::pair<UInt128Wrapper, size_t>, Iterator, HashFileKeyAndOffset> map;
    size_t cache_size = 0;
    int64_t hot_data_interval {0};
};
struct FileCacheInfo {
    UInt128Wrapper hash {0};
    uint64_t expiration_time {0};
    uint64_t size {0};
    size_t offset {0};
    bool is_tmp {false};
    FileCacheType cache_type {NORMAL};

    std::string to_string() const;
};

class InconsistencyType {
    uint32_t type;

public:
    enum : uint32_t {
        // No anomaly
        NONE = 0,
        // Missing a block cache metadata in _files
        NOT_LOADED = 1 << 0,
        // A block cache is missing in storage
        MISSING_IN_STORAGE = 1 << 1,
        // Size of a block cache recorded in _files is inconsistent with the storage
        SIZE_INCONSISTENT = 1 << 2,
        // Cache type of a block cache recorded in _files is inconsistent with the storage
        CACHE_TYPE_INCONSISTENT = 1 << 3,
        // Expiration time of a block cache recorded in _files is inconsistent with the storage
        EXPIRATION_TIME_INCONSISTENT = 1 << 4,
        // File in storage has a _tmp suffix, but the state of block cache in _files is not set to downloading
        TMP_FILE_EXPECT_DOWNLOADING_STATE = 1 << 5
    };
    InconsistencyType(uint32_t t = 0) : type(t) {}
    operator uint32_t&() { return type; }

    std::string to_string() const;
};

struct InconsistencyContext {
    // The infos in _files of BlockFileCache.
    std::vector<FileCacheInfo> infos_in_manager;
    std::vector<FileCacheInfo> infos_in_storage;
    std::vector<InconsistencyType> types;
};

std::optional<int64_t> get_tablet_id(std::string file_path);

} // namespace doris::io
