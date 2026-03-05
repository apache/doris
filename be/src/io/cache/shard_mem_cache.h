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
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <unordered_map>

#include "io/cache/file_cache_common.h"
#include "io/cache/file_cache_storage.h"
#include "io/cache/mem_file_cache_storage.h"

namespace doris::io {

class ShardMemHashTable {
public:
    ShardMemHashTable() = default;

    Status remove(const FileCacheKey& key) {
        // find and clear the one in _cache_map
        std::unique_lock<std::shared_mutex> lock(_shard_mt);
        auto map_key = std::make_pair(key.hash, key.offset);
        auto iter = _cache_map.find(map_key);
        if (iter == _cache_map.end()) {
            LOG_WARNING("key not found in cache map")
                    .tag("hash", key.hash.to_string())
                    .tag("offset", key.offset);
            return Status::IOError("key not found in in-memory cache map when remove");
        }
        _cache_map.erase(iter);

        return Status::OK();
    }

    Status append(const FileCacheKey& key, const Slice& value) {
        std::unique_lock<std::shared_mutex> lock(_shard_mt);

        auto map_key = std::make_pair(key.hash, key.offset);
        auto iter = _cache_map.find(map_key);
        if (iter != _cache_map.end()) {
            // despite the name append, it is indeed a put, so the key should not exist
            LOG_WARNING("key already exists in in-memory cache map")
                    .tag("hash", key.hash.to_string())
                    .tag("offset", key.offset);
            DCHECK(false);
            return Status::IOError("key already exists in in-memory cache map");
        }
        // TODO(zhengyu): allocate in mempool
        auto mem_block = MemBlock {
                std::shared_ptr<char[]>(new char[value.size], std::default_delete<char[]>())};
        DCHECK(mem_block.addr != nullptr);
        _cache_map[map_key] = mem_block;
        char* dst = mem_block.addr.get();
        // TODO(zhengyu): zero copy!
        memcpy(dst, value.data, value.size);

        return Status::OK();
    }

    Status read(const FileCacheKey& key, size_t value_offset, Slice buffer) {
        std::shared_lock<std::shared_mutex> lock(_shard_mt);
        auto map_key = std::make_pair(key.hash, key.offset);
        auto iter = _cache_map.find(map_key);
        if (iter == _cache_map.end()) {
            LOG_WARNING("key not found in cache map")
                    .tag("hash", key.hash.to_string())
                    .tag("offset", key.offset);
            return Status::IOError("key not found in in-memory cache map when read");
        }

        auto mem_block = iter->second;
        DCHECK(mem_block.addr != nullptr);
        char* src = mem_block.addr.get();
        char* dst = buffer.data;
        size_t size = buffer.size;
        memcpy(dst, src, size);
        return Status::OK();
    }

    Status clear() {
        std::unique_lock<std::shared_mutex> lock(_shard_mt);
        _cache_map.clear();

        return Status::OK();
    }

private:
    std::shared_mutex _shard_mt;
    std::unordered_map<FileWriterMapKey, MemBlock, FileWriterMapKeyHash> _cache_map;
};
} // namespace doris::io
