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

#include "io/cache/mem_file_cache_storage.h"

#include <filesystem>
#include <mutex>
#include <system_error>

#include "common/config.h"
#include "common/logging.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/file_block.h"
#include "io/cache/file_cache_common.h"
#include "io/cache/file_cache_storage.h"
#include "io/cache/shard_mem_cache.h"
#include "runtime/exec_env.h"
#include "vec/common/hex.h"

namespace doris::io {

MemFileCacheStorage::~MemFileCacheStorage() = default;

Status MemFileCacheStorage::init(BlockFileCache* _mgr) {
    LOG_INFO("init in-memory file cache storage");
    _mgr->_async_open_done = true; // no data to load for memory storage

    _shard_nums = config::file_cache_mem_storage_shard_num;
    if ((_shard_nums & _shard_nums - 1) != 0) {
        return Status::InternalError(
                "The shard number of memory storage should be power of 2, but currently is {}",
                _shard_nums);
    }
    _shard_mask = _shard_nums - 1;

    for (uint64_t i = 0; i < _shard_nums; i++) {
        _shard_cache_map.emplace_back(std::make_shared<ShardMemHashTable>());
    }

    return Status::OK();
}

Status MemFileCacheStorage::append(const FileCacheKey& key, const Slice& value) {
    uint64_t shard_num = get_shard_num(FileWriterMapKey {std::pair {key.hash, key.offset}});
    return _shard_cache_map[shard_num]->append(key, value);
}

Status MemFileCacheStorage::finalize(const FileCacheKey& key, const size_t size) {
    // do nothing for in memory cache coz nothing to persist
    // download state in FileBlock::finalize will inform the readers when finish
    return Status::OK();
}

Status MemFileCacheStorage::read(const FileCacheKey& key, size_t value_offset, Slice buffer) {
    uint64_t shard_num = get_shard_num(FileWriterMapKey {std::pair {key.hash, key.offset}});
    return _shard_cache_map[shard_num]->read(key, value_offset, buffer);
}

Status MemFileCacheStorage::remove(const FileCacheKey& key) {
    // find and clear the one in _cache_map
    uint64_t shard_num = get_shard_num(FileWriterMapKey {std::pair {key.hash, key.offset}});
    return _shard_cache_map[shard_num]->remove(key);
}

Status MemFileCacheStorage::change_key_meta_type(const FileCacheKey& key, const FileCacheType type,
                                                 const size_t size) {
    // do nothing for in memory cache coz nothing to persist
    return Status::OK();
}

Status MemFileCacheStorage::change_key_meta_expiration(const FileCacheKey& key,
                                                       const uint64_t expiration,
                                                       const size_t size) {
    // do nothing for in memory cache coz nothing to persist
    return Status::OK();
}

void MemFileCacheStorage::load_blocks_directly_unlocked(BlockFileCache* _mgr,
                                                        const FileCacheKey& key,
                                                        std::lock_guard<std::mutex>& cache_lock) {
    // load nothing for in memory cache coz nothing is persisted
}

Status MemFileCacheStorage::clear(std::string& msg) {
    for (auto& shard_cache : _shard_cache_map) {
        Status s = shard_cache->clear();
        if (!s) {
            return s;
        }
    }

    return Status::OK();
}

std::string MemFileCacheStorage::get_local_file(const FileCacheKey& key) {
    return "";
}

} // namespace doris::io
