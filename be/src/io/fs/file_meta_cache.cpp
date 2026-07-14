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

#include "io/fs/file_meta_cache.h"

#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "util/stopwatch.hpp"

namespace doris {
namespace {

void update_profile_counter(int64_t* counter, int64_t value = 1) {
    if (counter != nullptr) {
        *counter += value;
    }
}

bool is_persistent_cache_identity_stable(const FileMetaCacheContext& context) {
    return context.modification_time > 0;
}

} // namespace

FileMetaCache::FileMetaCache(int64_t capacity,
                             std::unique_ptr<FileMetaPersistentCache> persistent_cache)
        : _cache(capacity), _persistent_cache(std::move(persistent_cache)) {}

std::string FileMetaCache::get_key(const std::string file_name, int64_t modification_time,
                                   int64_t file_size) {
    std::string meta_cache_key;
    meta_cache_key.resize(file_name.size() + 2 * sizeof(int64_t));

    memcpy(meta_cache_key.data(), file_name.data(), file_name.size());
    memcpy(meta_cache_key.data() + file_name.size(), &modification_time, sizeof(int64_t));
    memcpy(meta_cache_key.data() + file_name.size() + sizeof(int64_t), &file_size, sizeof(int64_t));
    return meta_cache_key;
}

std::string FileMetaCache::get_key(io::FileReaderSPtr file_reader,
                                   const io::FileDescription& _file_description) {
    const std::string& file_path = file_reader->path().native();
    std::string file_identity;
    if (_file_description.fs_name.empty()) {
        file_identity = file_path;
    } else {
        file_identity.reserve(_file_description.fs_name.size() + 1 + file_path.size());
        file_identity.append(_file_description.fs_name);
        file_identity.push_back('\0');
        file_identity.append(file_path);
    }
    return FileMetaCache::get_key(
            file_identity, _file_description.mtime,
            _file_description.file_size == -1 ? file_reader->size() : _file_description.file_size);
}

std::string FileMetaCache::get_memory_cache_key(FileMetaCacheFormat format, std::string_view key) {
    std::string memory_cache_key;
    memory_cache_key.reserve(key.size() + sizeof(uint8_t));
    memory_cache_key.push_back(static_cast<char>(format));
    memory_cache_key.append(key.data(), key.size());
    return memory_cache_key;
}

bool FileMetaCache::is_persistent_cache_configured() {
    return config::enable_external_file_meta_disk_cache;
}

bool FileMetaCache::is_persistent_cache_enabled() {
    return is_persistent_cache_configured() &&
           config::external_file_meta_disk_cache_max_entry_bytes > 0;
}

bool FileMetaCache::is_persistent_cache_payload_size_allowed(uint64_t payload_size) {
    const int64_t max_entry_bytes = config::external_file_meta_disk_cache_max_entry_bytes;
    return config::enable_external_file_meta_disk_cache && max_entry_bytes > 0 &&
           payload_size != 0 && std::cmp_less_equal(payload_size, max_entry_bytes);
}

FileMetaCacheLookupResult FileMetaCache::lookup(const FileMetaCacheContext& context,
                                                ObjLRUCache::CacheHandle* handle,
                                                std::string* serialized_meta,
                                                FileMetaCacheProfile* profile) {
    DCHECK(handle != nullptr);
    DCHECK(serialized_meta != nullptr);
    *handle = ObjLRUCache::CacheHandle();
    if (context.enable_memory_cache && lookup(get_memory_cache_key(context), handle)) {
        serialized_meta->clear();
        if (profile != nullptr) {
            update_profile_counter(profile->hit_cache);
            update_profile_counter(profile->hit_memory_cache);
        }
        return {.state = FileMetaCacheLookupState::MEMORY_HIT};
    }

    FileMetaCacheLookupResult result;
    int64_t persisted_read_time = 0;
    if (lookup_persistent_cache(context, serialized_meta, &persisted_read_time)) {
        result.state = FileMetaCacheLookupState::PERSISTED_HIT;
        if (profile != nullptr) {
            update_profile_counter(profile->hit_cache);
            update_profile_counter(profile->hit_disk_cache);
            update_profile_counter(profile->read_disk_cache_time, persisted_read_time);
        }
    } else if (is_persistent_cache_enabled() && is_persistent_cache_identity_stable(context) &&
               profile != nullptr) {
        update_profile_counter(profile->miss_disk_cache);
        update_profile_counter(profile->read_disk_cache_time, persisted_read_time);
    }
    return result;
}

void FileMetaCache::invalidate_persistent_cache(const FileMetaCacheContext& context) {
    if (_persistent_cache != nullptr) {
        _persistent_cache->remove(context.format, context.key);
    }
}

bool FileMetaCache::lookup_persistent_cache(const FileMetaCacheContext& context,
                                            std::string* payload, int64_t* read_time) {
    DCHECK(payload != nullptr);
    DCHECK(read_time != nullptr);
    payload->clear();
    *read_time = 0;
    if (!is_persistent_cache_enabled() || _persistent_cache == nullptr ||
        !is_persistent_cache_identity_stable(context)) {
        return false;
    }

    MonotonicStopWatch watch;
    watch.start();
    Status status = _persistent_cache->read(context.format, context.key, context.modification_time,
                                            context.file_size, payload);
    *read_time = watch.elapsed_time();
    if (!status.ok()) {
        payload->clear();
        VLOG_DEBUG << "lookup file meta persistent cache failed: " << status;
        return false;
    }
    if (!is_persistent_cache_payload_size_allowed(static_cast<uint64_t>(payload->size()))) {
        payload->clear();
        _persistent_cache->remove(context.format, context.key);
        return false;
    }
    return true;
}

bool FileMetaCache::insert_persistent_cache(const FileMetaCacheContext& context,
                                            std::string_view payload, int64_t* write_time) {
    DCHECK(write_time != nullptr);
    *write_time = 0;
    if (!is_persistent_cache_payload_size_allowed(static_cast<uint64_t>(payload.size())) ||
        _persistent_cache == nullptr || !is_persistent_cache_identity_stable(context)) {
        return false;
    }

    MonotonicStopWatch watch;
    watch.start();
    Status status = _persistent_cache->write(context.format, context.key, context.modification_time,
                                             context.file_size, payload);
    *write_time = watch.elapsed_time();
    if (!status.ok()) {
        VLOG_DEBUG << "insert file meta persistent cache failed: " << status;
        return false;
    }
    return true;
}

} // namespace doris
