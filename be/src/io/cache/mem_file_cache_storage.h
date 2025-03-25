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

namespace doris::io {

struct MemBlock {
    std::shared_ptr<char[]> addr;
};

class MemFileCacheStorage : public FileCacheStorage {
public:
    MemFileCacheStorage() = default;
    ~MemFileCacheStorage() override;
    Status init(BlockFileCache* _mgr) override;
    Status append(const FileCacheKey& key, const Slice& value) override;
    Status finalize(const FileCacheKey& key) override;
    Status read(const FileCacheKey& key, size_t value_offset, Slice buffer) override;
    Status remove(const FileCacheKey& key) override;
    Status change_key_meta_type(const FileCacheKey& key, const FileCacheType type) override;
    Status change_key_meta_expiration(const FileCacheKey& key, const uint64_t expiration) override;
    void load_blocks_directly_unlocked(BlockFileCache* _mgr, const FileCacheKey& key,
                                       std::lock_guard<std::mutex>& cache_lock) override;
    Status clear(std::string& msg) override;
    std::string get_local_file(const FileCacheKey& key) override;

    FileCacheStorageType get_type() override { return MEMORY; }

private:
    std::unordered_map<FileWriterMapKey, MemBlock, FileWriterMapKeyHash> _cache_map;
    std::mutex _cache_map_mtx;
};

} // namespace doris::io
