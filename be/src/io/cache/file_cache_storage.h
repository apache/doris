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

#include "io/cache/file_cache_utils.h"
#include "util/slice.h"

namespace doris::io {

class BlockFileCacheManager;

class FileCacheStorage {
public:
    FileCacheStorage() = default;
    virtual ~FileCacheStorage() = default;
    virtual Status init(BlockFileCacheManager* _mgr) = 0;
    virtual Status append(const FileCacheKey& key, const Slice& value) = 0;
    virtual Status finalize(const FileCacheKey& key) = 0;
    virtual Status read(const FileCacheKey& key, size_t value_offset, Slice result) = 0;
    virtual Status remove(const FileCacheKey& key) = 0;
    virtual Status change_key_meta(const FileCacheKey& key, const KeyMeta& new_meta) = 0;
    // use when lazy load cache
    virtual void load_blocks_directly_unlocked(BlockFileCacheManager* _mgr, const FileCacheKey& key,
                                      std::lock_guard<std::mutex>& cache_lock) {}
};

} // namespace doris::io
