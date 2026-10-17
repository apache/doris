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

#include "common/status.h"
#include "io/file_factory.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "util/obj_lru_cache.h"

namespace doris {

// A file meta cache depends on a LRU cache.
// Such as parsed parquet footer.
// Capacity is measured in bytes in memory mode, or entries in legacy mode.
// Active readers pin entries; the LRU converges to capacity as their handles are released.
class FileMetaCache {
public:
    explicit FileMetaCache(int64_t capacity, LRUCacheType type = LRUCacheType::NUMBER,
                           uint32_t num_shards = DEFAULT_LRU_CACHE_NUM_SHARDS)
            : _cache(capacity, num_shards, type, CachePolicy::CacheType::EXTERNAL_FILE_META_CACHE),
              _type(type),
              _num_shards(num_shards) {}

    // Empty memory_limit selects the old entry-count configuration. Percentages use the
    // effective BE memory limit, including its container limit, rather than host RAM.
    static Status create(const std::string& memory_limit, int64_t legacy_capacity,
                         int64_t process_memory_limit, int64_t physical_memory,
                         std::unique_ptr<FileMetaCache>* cache);

    FileMetaCache(const FileMetaCache&) = delete;
    const FileMetaCache& operator=(const FileMetaCache&) = delete;

    ObjLRUCache& cache() { return _cache; }

    static std::string get_key(const std::string file_name, int64_t modification_time,
                               int64_t file_size);

    static std::string get_key(io::FileReaderSPtr file_reader,
                               const io::FileDescription& _file_description);

    bool lookup(const std::string& key, ObjLRUCache::CacheHandle* handle) {
        return _cache.lookup({key}, handle);
    }

    template <typename T>
    bool insert(const std::string& key, std::unique_ptr<T>& value, ObjLRUCache::CacheHandle* handle,
                size_t memory_size) {
        if (!enabled()) {
            return false;
        }
        const size_t capacity = _cache.get_capacity();
        const size_t entry_size =
                sizeof(LRUHandle) - 1 + key.size() + sizeof(ObjLRUCache::ObjValue<T>) + memory_size;
        // An entry larger than its shard would evict every reusable footer and still not fit.
        // For example, a 2 MB footer bypasses a cache with 1 MB per shard. Keep reader ownership.
        if (_type == LRUCacheType::SIZE &&
            entry_size > (capacity + _num_shards - 1) / _num_shards) {
            return false;
        }
        _cache.insert({key}, value.release(), handle, memory_size);
        return true;
    }

    bool enabled() const { return _cache.enabled(); }

    bool should_cache_file_count(size_t file_count) {
        // File count cannot predict retained bytes. In memory mode the weighted LRU bounds
        // residency; the old count-based large-scan admission gate applies only in legacy mode.
        return enabled() && (_type == LRUCacheType::SIZE || file_count < _cache.get_capacity() / 3);
    }

private:
    ObjLRUCache _cache;
    LRUCacheType _type;
    uint32_t _num_shards;
};

} // namespace doris
