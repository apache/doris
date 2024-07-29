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

#include "io/cache/file_cache_common.h"

#include "common/config.h"
#include "io/cache/block_file_cache.h"
#include "vec/common/hex.h"

namespace doris::io {

FileCacheSettings get_file_cache_settings(size_t capacity, size_t max_query_cache_size,
                                          size_t normal_percent, size_t disposable_percent,
                                          size_t index_percent) {
    io::FileCacheSettings settings;
    if (capacity == 0) return settings;
    settings.capacity = capacity;
    settings.max_file_block_size = config::file_cache_each_block_size;
    settings.max_query_cache_size = max_query_cache_size;
    size_t per_size = settings.capacity / 100;
    settings.disposable_queue_size = per_size * disposable_percent;
    settings.disposable_queue_elements =
            std::max(settings.disposable_queue_size / settings.max_file_block_size,
                     REMOTE_FS_OBJECTS_CACHE_DEFAULT_ELEMENTS);

    settings.index_queue_size = per_size * index_percent;
    settings.index_queue_elements =
            std::max(settings.index_queue_size / settings.max_file_block_size,
                     REMOTE_FS_OBJECTS_CACHE_DEFAULT_ELEMENTS);

    settings.query_queue_size =
            settings.capacity - settings.disposable_queue_size - settings.index_queue_size;
    settings.query_queue_elements =
            std::max(settings.query_queue_size / settings.max_file_block_size,
                     REMOTE_FS_OBJECTS_CACHE_DEFAULT_ELEMENTS);
    return settings;
}

std::string UInt128Wrapper::to_string() const {
    return vectorized::get_hex_uint_lowercase(value_);
}

FileBlocksHolderPtr FileCacheAllocatorBuilder::allocate_cache_holder(size_t offset,
                                                                     size_t size) const {
    CacheContext ctx;
    ctx.cache_type = _expiration_time == 0 ? FileCacheType::NORMAL : FileCacheType::TTL;
    ctx.expiration_time = _expiration_time;
    ctx.is_cold_data = _is_cold_data;
    auto holder = _cache->get_or_set(_cache_hash, offset, size, ctx);
    return std::make_unique<FileBlocksHolder>(std::move(holder));
}

} // namespace doris::io
