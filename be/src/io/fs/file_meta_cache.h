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

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "common/status.h"
#include "io/file_factory.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "util/obj_lru_cache.h"

namespace doris {

enum class FileMetaCacheFormat : uint8_t {
    PARQUET = 1,
    ORC = 2,
    PARQUET_V2 = 3,
    ORC_V2 = 4,
};

struct FileMetaCacheContext {
    FileMetaCacheFormat format;
    const std::string& key;
    int64_t modification_time = 0;
    int64_t file_size = 0;
    bool enable_memory_cache = true;
};

enum class FileMetaCacheLookupState {
    MEMORY_HIT,
    PERSISTED_HIT,
    MISS,
};

struct FileMetaCacheLookupResult {
    FileMetaCacheLookupState state = FileMetaCacheLookupState::MISS;
};

struct FileMetaCacheInsertResult {
    bool memory_inserted = false;
    bool persisted_inserted = false;
};

struct FileMetaCacheProfile {
    int64_t* hit_cache = nullptr;
    int64_t* hit_memory_cache = nullptr;
    int64_t* hit_disk_cache = nullptr;
    int64_t* miss_disk_cache = nullptr;
    int64_t* write_disk_cache = nullptr;
    int64_t* read_disk_cache_time = nullptr;
    int64_t* write_disk_cache_time = nullptr;
};

class FileMetaPersistentCache {
public:
    virtual ~FileMetaPersistentCache() = default;

    virtual Status read(FileMetaCacheFormat format, const std::string& key,
                        int64_t modification_time, int64_t file_size, std::string* payload) = 0;

    virtual Status write(FileMetaCacheFormat format, const std::string& key,
                         int64_t modification_time, int64_t file_size,
                         std::string_view payload) = 0;

    virtual void remove(FileMetaCacheFormat format, const std::string& key) = 0;
};

// A file meta cache depends on a LRU cache.
// Such as parsed parquet footer.
// The capacity will limit the number of cache entries in cache.
class FileMetaCache {
public:
    explicit FileMetaCache(int64_t capacity,
                           std::unique_ptr<FileMetaPersistentCache> persistent_cache = nullptr);

    FileMetaCache(const FileMetaCache&) = delete;
    const FileMetaCache& operator=(const FileMetaCache&) = delete;

    ObjLRUCache& cache() { return _cache; }

    static std::string get_key(const std::string file_name, int64_t modification_time,
                               int64_t file_size);

    static std::string get_key(io::FileReaderSPtr file_reader,
                               const io::FileDescription& _file_description);
    static std::string get_memory_cache_key(FileMetaCacheFormat format, std::string_view key);
    static std::string get_memory_cache_key(const FileMetaCacheContext& context) {
        return get_memory_cache_key(context.format, context.key);
    }

    static bool is_persistent_cache_enabled();
    static bool is_persistent_cache_payload_size_allowed(uint64_t payload_size);

    bool lookup(const std::string& key, ObjLRUCache::CacheHandle* handle) {
        return _cache.lookup({key}, handle);
    }

    template <typename T>
    void insert(const std::string& key, T* value, ObjLRUCache::CacheHandle* handle) {
        _cache.insert({key}, value, handle);
    }

    template <typename T>
    bool insert(const std::string& key, std::unique_ptr<T>& value,
                ObjLRUCache::CacheHandle* handle) {
        DCHECK(value != nullptr);
        if (!_cache.enabled()) {
            return false;
        }
        _cache.insert({key}, value.release(), handle);
        return true;
    }

    bool enabled() const { return _cache.enabled(); }

    FileMetaCacheLookupResult lookup(const FileMetaCacheContext& context,
                                     ObjLRUCache::CacheHandle* handle, std::string* serialized_meta,
                                     FileMetaCacheProfile* profile = nullptr);

    void invalidate_persistent_cache(const FileMetaCacheContext& context);

    template <typename T>
    FileMetaCacheInsertResult insert(const FileMetaCacheContext& context, std::unique_ptr<T>& value,
                                     ObjLRUCache::CacheHandle* handle,
                                     std::string_view serialized_meta,
                                     FileMetaCacheProfile* profile = nullptr) {
        FileMetaCacheInsertResult result;
        int64_t persisted_write_time = 0;
        result.persisted_inserted =
                insert_persistent_cache(context, serialized_meta, &persisted_write_time);
        if (result.persisted_inserted && profile != nullptr) {
            if (profile->write_disk_cache != nullptr) {
                ++(*profile->write_disk_cache);
            }
            if (profile->write_disk_cache_time != nullptr) {
                *profile->write_disk_cache_time += persisted_write_time;
            }
        }
        if (context.enable_memory_cache) {
            result.memory_inserted = insert(get_memory_cache_key(context), value, handle);
        }
        return result;
    }

private:
    bool lookup_persistent_cache(const FileMetaCacheContext& context, std::string* payload,
                                 int64_t* read_time);
    bool insert_persistent_cache(const FileMetaCacheContext& context, std::string_view payload,
                                 int64_t* write_time);

    ObjLRUCache _cache;
    std::unique_ptr<FileMetaPersistentCache> _persistent_cache;
};

} // namespace doris
