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
#include <shared_mutex>
#include <thread>

#include "io/cache/file_cache_common.h"
#include "io/cache/file_cache_storage.h"
#include "io/cache/shard_mem_cache.h"

namespace doris::io {

class MemFileCacheStorage : public FileCacheStorage {
public:
    MemFileCacheStorage() = default;
    ~MemFileCacheStorage() override;
    Status init(BlockFileCache* _mgr) override;
    Status append(const FileCacheKey& key, const Slice& value) override;
    Status finalize(const FileCacheKey& key, const size_t size) override;
    Status read(const FileCacheKey& key, size_t value_offset, Slice buffer) override;
    Status remove(const FileCacheKey& key) override;
    Status change_key_meta_type(const FileCacheKey& key, const FileCacheType type,
                                const size_t size) override;
    Status change_key_meta_expiration(const FileCacheKey& key, const uint64_t expiration,
                                      const size_t size) override;
    void load_blocks_directly_unlocked(BlockFileCache* _mgr, const FileCacheKey& key,
                                       std::lock_guard<std::mutex>& cache_lock) override;
    Status clear(std::string& msg) override;
    std::string get_local_file(const FileCacheKey& key) override;

    FileCacheStorageType get_type() override { return MEMORY; }

private:
    uint64_t get_shard_num(const FileWriterMapKey& key) const {
        FileWriterMapKeyHash hash_func;
        std::size_t hash = hash_func(key);

        return hash & _shard_mask;
    }

    // new code
    uint64_t _shard_nums = 1;
    uint64_t _shard_mask = 0;
    std::vector<std::shared_ptr<ShardMemHashTable>> _shard_cache_map;
};

} // namespace doris::io
