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

#include <shared_mutex>
#include <unordered_map>

#include "io/cache/file_cache_storage.h"

namespace doris::io {

class ShardMemHashTable {
public:
    ShardMemHashTable() = default;

    Status remove(const FileCacheKey& key);
    Status append(const FileCacheKey& key, const Slice& value);
    Status read(const FileCacheKey& key, size_t value_offset, Slice buffer);
    Status clear();

private:
    std::shared_mutex _shard_mt;
    std::unordered_map<FileWriterMapKey, MemBlock, FileWriterMapKeyHash> _cache_map;
};
} // namespace doris::io
