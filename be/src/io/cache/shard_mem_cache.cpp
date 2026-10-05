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

#include "io/cache/shard_mem_cache.h"

#include <bvar/bvar.h>

#include <cstring>
#include <memory>
#include <mutex>

#include "common/logging.h"

namespace doris::io {

bvar::Adder<uint64_t> g_mem_file_cache_duplicate_key_replace_num(
        "file_cache", "memory_duplicate_key_replace_num");

Status ShardMemHashTable::remove(const FileCacheKey& key) {
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

Status ShardMemHashTable::append(const FileCacheKey& key, const Slice& value) {
    return appendv(key, &value, 1);
}

Status ShardMemHashTable::appendv(const FileCacheKey& key, const Slice* values, size_t value_cnt) {
    std::unique_lock<std::shared_mutex> lock(_shard_mt);

    auto map_key = std::make_pair(key.hash, key.offset);
    auto iter = _cache_map.find(map_key);
    size_t total_size = 0;
    for (size_t idx = 0; idx < value_cnt; ++idx) {
        total_size += values[idx].size;
    }
    if (total_size == 0) {
        return Status::InvalidArgument("appendv requires non-empty slices");
    }

    MemBlock mem_block;
    mem_block.addr = std::shared_ptr<char[]>(new char[total_size], std::default_delete<char[]>());
    mem_block.size = total_size;
    DCHECK(mem_block.addr != nullptr);
    char* dst = mem_block.addr.get();
    for (size_t idx = 0; idx < value_cnt; ++idx) {
        memcpy(dst, values[idx].data, values[idx].size);
        dst += values[idx].size;
    }
    if (iter != _cache_map.end()) {
        g_mem_file_cache_duplicate_key_replace_num << 1;
        LOG_WARNING("replace duplicate key in in-memory cache map")
                .tag("hash", key.hash.to_string())
                .tag("offset", key.offset)
                .tag("old_size", iter->second.size)
                .tag("new_size", total_size);
        // Replacing the payload keeps existing readers alive while reusing the same key.
        iter->second = std::move(mem_block);
        return Status::OK();
    }

    _cache_map.emplace(map_key, std::move(mem_block));
    return Status::OK();
}

Status ShardMemHashTable::append_iobuf(const FileCacheKey& key, const butil::IOBuf& value) {
    std::unique_lock<std::shared_mutex> lock(_shard_mt);

    auto map_key = std::make_pair(key.hash, key.offset);
    auto iter = _cache_map.find(map_key);

    const size_t total_size = value.length();
    if (total_size == 0) {
        return Status::InvalidArgument("append_iobuf requires non-empty payload");
    }

    MemBlock mem_block;
    mem_block.payload.append(value);
    mem_block.size = total_size;
    mem_block.use_iobuf = true;
    if (iter != _cache_map.end()) {
        g_mem_file_cache_duplicate_key_replace_num << 1;
        LOG_WARNING("replace duplicate key in in-memory cache map")
                .tag("hash", key.hash.to_string())
                .tag("offset", key.offset)
                .tag("old_size", iter->second.size)
                .tag("new_size", total_size);
        iter->second = std::move(mem_block);
        return Status::OK();
    }

    _cache_map.emplace(map_key, std::move(mem_block));
    return Status::OK();
}

Status ShardMemHashTable::read(const FileCacheKey& key, size_t value_offset, Slice buffer) {
    std::shared_lock<std::shared_mutex> lock(_shard_mt);
    auto map_key = std::make_pair(key.hash, key.offset);
    auto iter = _cache_map.find(map_key);
    if (iter == _cache_map.end()) {
        LOG_WARNING("key not found in cache map")
                .tag("hash", key.hash.to_string())
                .tag("offset", key.offset);
        return Status::IOError("key not found in in-memory cache map when read");
    }

    const auto& mem_block = iter->second;
    if (value_offset > mem_block.size || buffer.size > mem_block.size - value_offset) {
        return Status::InternalError(
                "read buffer exceeds cached block, key={}, offset={}, request_offset={}, "
                "request_size={}, cached_size={}",
                key.hash.to_string(), key.offset, value_offset, buffer.size, mem_block.size);
    }
    if (mem_block.use_iobuf) {
        const size_t copied = mem_block.payload.copy_to(buffer.data, buffer.size, value_offset);
        if (copied != buffer.size) {
            return Status::InternalError(
                    "short read from iobuf cache block, key={}, offset={}, request_offset={}, "
                    "request_size={}, actual_size={}",
                    key.hash.to_string(), key.offset, value_offset, buffer.size, copied);
        }
        return Status::OK();
    }
    DCHECK(mem_block.addr != nullptr);
    memcpy(buffer.data, mem_block.addr.get() + value_offset, buffer.size);
    return Status::OK();
}

Status ShardMemHashTable::read_to_iobuf(const FileCacheKey& key, size_t value_offset,
                                        size_t bytes_req, butil::IOBuf* out, size_t* bytes_read) {
    if (out == nullptr || bytes_read == nullptr) {
        return Status::InvalidArgument("read_to_iobuf requires non-null out and bytes_read");
    }
    std::shared_lock<std::shared_mutex> lock(_shard_mt);
    auto map_key = std::make_pair(key.hash, key.offset);
    auto iter = _cache_map.find(map_key);
    if (iter == _cache_map.end()) {
        LOG_WARNING("key not found in cache map")
                .tag("hash", key.hash.to_string())
                .tag("offset", key.offset);
        return Status::IOError("key not found in in-memory cache map when read_to_iobuf");
    }
    const auto& mem_block = iter->second;
    if (value_offset > mem_block.size || bytes_req > mem_block.size - value_offset) {
        return Status::InternalError(
                "read iobuf exceeds cached block, key={}, offset={}, request_offset={}, "
                "request_size={}, cached_size={}",
                key.hash.to_string(), key.offset, value_offset, bytes_req, mem_block.size);
    }
    if (mem_block.use_iobuf) {
        const size_t appended = mem_block.payload.append_to(out, bytes_req, value_offset);
        if (appended != bytes_req) {
            return Status::InternalError(
                    "short append_to from iobuf cache block, key={}, offset={}, request_offset={}, "
                    "request_size={}, actual_size={}",
                    key.hash.to_string(), key.offset, value_offset, bytes_req, appended);
        }
    } else {
        DCHECK(mem_block.addr != nullptr);
        out->append(mem_block.addr.get() + value_offset, bytes_req);
    }
    *bytes_read = bytes_req;
    return Status::OK();
}

Status ShardMemHashTable::clear() {
    std::unique_lock<std::shared_mutex> lock(_shard_mt);
    _cache_map.clear();
    return Status::OK();
}

} // namespace doris::io
