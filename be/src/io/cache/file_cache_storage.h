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

#include "io/cache/file_cache_common.h"
#include "util/slice.h"

namespace doris::io {

class BlockFileCache;

// The interface is for organizing datas in disk
class FileCacheStorage {
public:
    FileCacheStorage() = default;
    virtual ~FileCacheStorage() = default;
    // init the manager, read the blocks meta into memory
    virtual Status init(BlockFileCache* _mgr) = 0;
    // append datas into block
    virtual Status append(const FileCacheKey& key, const Slice& value) = 0;
    // finalize the block
    virtual Status finalize(const FileCacheKey& key) = 0;
    // read the block
    virtual Status read(const FileCacheKey& key, size_t value_offset, Slice result) = 0;
    // remove the block
    virtual Status remove(const FileCacheKey& key) = 0;
    // change the block meta
    virtual Status change_key_meta_type(const FileCacheKey& key, const FileCacheType type) = 0;
    virtual Status change_key_meta_expiration(const FileCacheKey& key,
                                              const uint64_t expiration) = 0;
    // use when lazy load cache
    virtual void load_blocks_directly_unlocked(BlockFileCache* _mgr, const FileCacheKey& key,
                                               std::lock_guard<std::mutex>& cache_lock) {}
};

} // namespace doris::io
