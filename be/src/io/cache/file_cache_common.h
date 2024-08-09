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
#include "io/io_common.h"
#include "vec/common/uint128.h"

namespace doris::io {

inline static constexpr size_t REMOTE_FS_OBJECTS_CACHE_DEFAULT_ELEMENTS = 100 * 1024;
inline static constexpr size_t FILE_CACHE_MAX_FILE_BLOCK_SIZE = 1 * 1024 * 1024;
inline static constexpr size_t DEFAULT_NORMAL_PERCENT = 85;
inline static constexpr size_t DEFAULT_DISPOSABLE_PERCENT = 10;
inline static constexpr size_t DEFAULT_INDEX_PERCENT = 5;

using uint128_t = vectorized::UInt128;

enum class FileCacheType {
    INDEX,
    NORMAL,
    DISPOSABLE,
    TTL,
};

struct UInt128Wrapper {
    uint128_t value_;
    [[nodiscard]] std::string to_string() const;

    UInt128Wrapper() = default;
    explicit UInt128Wrapper(const uint128_t& value) : value_(value) {}

    bool operator==(const UInt128Wrapper& other) const { return value_ == other.value_; }
};

class BlockFileCache;
struct FileBlocksHolder;
using FileBlocksHolderPtr = std::unique_ptr<FileBlocksHolder>;

struct FileCacheAllocatorBuilder {
    bool _is_cold_data;
    uint64_t _expiration_time;
    UInt128Wrapper _cache_hash;
    BlockFileCache* _cache; // Only one ref, the lifetime is owned by FileCache
    FileBlocksHolderPtr allocate_cache_holder(size_t offset, size_t size) const;
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
    size_t max_file_block_size {0};
    size_t max_query_cache_size {0};
};

FileCacheSettings get_file_cache_settings(size_t capacity, size_t max_query_cache_size,
                                          size_t normal_percent = DEFAULT_NORMAL_PERCENT,
                                          size_t disposable_percent = DEFAULT_DISPOSABLE_PERCENT,
                                          size_t index_percent = DEFAULT_INDEX_PERCENT);

struct CacheContext {
    CacheContext(const IOContext* io_context) {
        if (io_context->is_index_data) {
            cache_type = FileCacheType::INDEX;
        } else if (io_context->is_disposable) {
            cache_type = FileCacheType::DISPOSABLE;
        } else if (io_context->expiration_time != 0) {
            cache_type = FileCacheType::TTL;
            expiration_time = io_context->expiration_time;
        } else {
            cache_type = FileCacheType::NORMAL;
        }
        query_id = io_context->query_id ? *io_context->query_id : TUniqueId();
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
};

} // namespace doris::io
