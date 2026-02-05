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

std::string cache_type_to_surfix(FileCacheType type) {
    switch (type) {
    case FileCacheType::INDEX:
        return "_idx";
    case FileCacheType::DISPOSABLE:
        return "_disposable";
    case FileCacheType::NORMAL:
        return "";
    case FileCacheType::TTL:
        return "_ttl";
    }
    return "";
}

FileCacheType surfix_to_cache_type(const std::string& str) {
    if (str == "idx") {
        return FileCacheType::INDEX;
    } else if (str == "disposable") {
        return FileCacheType::DISPOSABLE;
    } else if (str == "ttl") {
        return FileCacheType::TTL;
    }
    DCHECK(false) << "The string is " << str;
    return FileCacheType::DISPOSABLE;
}

FileCacheType string_to_cache_type(const std::string& str) {
    if (str == "normal") {
        return FileCacheType::NORMAL;
    } else if (str == "index") {
        return FileCacheType::INDEX;
    } else if (str == "disposable") {
        return FileCacheType::DISPOSABLE;
    } else if (str == "ttl") {
        return FileCacheType::TTL;
    }
    DCHECK(false) << "The string is " << str;
    return FileCacheType::NORMAL;
}
std::string cache_type_to_string(FileCacheType type) {
    switch (type) {
    case FileCacheType::INDEX:
        return "index";
    case FileCacheType::DISPOSABLE:
        return "disposable";
    case FileCacheType::NORMAL:
        return "normal";
    case FileCacheType::TTL:
        return "ttl";
    }
    DCHECK(false) << "unknown type: " << type;
    return "normal";
}

std::string FileCacheSettings::to_string() const {
    std::stringstream ss;
    ss << "capacity: " << capacity << ", max_file_block_size: " << max_file_block_size
       << ", max_query_cache_size: " << max_query_cache_size
       << ", disposable_queue_size: " << disposable_queue_size
       << ", disposable_queue_elements: " << disposable_queue_elements
       << ", index_queue_size: " << index_queue_size
       << ", index_queue_elements: " << index_queue_elements
       << ", ttl_queue_size: " << ttl_queue_size << ", ttl_queue_elements: " << ttl_queue_elements
       << ", query_queue_size: " << query_queue_size
       << ", query_queue_elements: " << query_queue_elements << ", storage: " << storage;
    return ss.str();
}

FileCacheSettings get_file_cache_settings(size_t capacity, size_t max_query_cache_size,
                                          size_t normal_percent, size_t disposable_percent,
                                          size_t index_percent, size_t ttl_percent,
                                          const std::string& storage) {
    io::FileCacheSettings settings;
    if (capacity == 0) {
        return settings;
    }
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

    settings.ttl_queue_size = per_size * ttl_percent;
    settings.ttl_queue_elements = std::max(settings.ttl_queue_size / settings.max_file_block_size,
                                           REMOTE_FS_OBJECTS_CACHE_DEFAULT_ELEMENTS);

    settings.query_queue_size = settings.capacity - settings.disposable_queue_size -
                                settings.index_queue_size - settings.ttl_queue_size;
    settings.query_queue_elements =
            std::max(settings.query_queue_size / settings.max_file_block_size,
                     REMOTE_FS_OBJECTS_CACHE_DEFAULT_ELEMENTS);
    settings.storage = storage;
    return settings;
}

std::string UInt128Wrapper::to_string() const {
    return vectorized::get_hex_uint_lowercase(value_);
}

FileBlocksHolderPtr FileCacheAllocatorBuilder::allocate_cache_holder(size_t offset, size_t size,
                                                                     int64_t tablet_id) const {
    CacheContext ctx;
    ctx.cache_type = _expiration_time == 0 ? FileCacheType::NORMAL : FileCacheType::TTL;
    ctx.expiration_time = _expiration_time;
    ctx.is_cold_data = _is_cold_data;
    ctx.tablet_id = tablet_id;
    ReadStatistics stats;
    ctx.stats = &stats;
    auto holder = _cache->get_or_set(_cache_hash, offset, size, ctx);
    return std::make_unique<FileBlocksHolder>(std::move(holder));
}

template size_t LRUQueue::get_capacity(std::lock_guard<std::mutex>& cache_lock) const;
template void LRUQueue::remove(Iterator queue_it, std::lock_guard<std::mutex>& cache_lock);

std::string FileCacheInfo::to_string() const {
    std::stringstream ss;
    ss << "Hash: " << hash.to_string() << "\n"
       << "Expiration Time: " << expiration_time << "\n"
       << "Offset: " << offset << "\n"
       << "Cache Type: " << cache_type_to_string(cache_type) << "\n";
    return ss.str();
}

std::string InconsistencyType::to_string() const {
    std::string result = "Inconsistency Reason: ";
    if (type == NONE) {
        result += "NONE";
    } else {
        if (type & NOT_LOADED) {
            result += "NOT_LOADED ";
        }
        if (type & MISSING_IN_STORAGE) {
            result += "MISSING_IN_STORAGE ";
        }
        if (type & SIZE_INCONSISTENT) {
            result += "SIZE_INCONSISTENT ";
        }
        if (type & CACHE_TYPE_INCONSISTENT) {
            result += "CACHE_TYPE_INCONSISTENT ";
        }
        if (type & EXPIRATION_TIME_INCONSISTENT) {
            result += "EXPIRATION_TIME_INCONSISTENT ";
        }
        if (type & TMP_FILE_EXPECT_DOWNLOADING_STATE) {
            result += "TMP_FILE_EXPECT_DOWNLOADING_STATE";
        }
    }
    result += "\n";
    return result;
}

std::optional<int64_t> get_tablet_id(std::string file_path) {
    // Expected path formats:
    // support both .dat and .idx file extensions
    // support formate see ut. storage_resource_test:StorageResourceTest.ParseTabletIdFromPath

    if (file_path.empty()) {
        return std::nullopt;
    }

    // Find the position of "data/" in the path
    std::string_view path_view = file_path;
    std::string_view data_prefix = DATA_PREFIX;
    size_t data_pos = path_view.find(data_prefix);
    if (data_pos == std::string_view::npos) {
        return std::nullopt;
    }

    if (data_prefix.length() + data_pos >= path_view.length()) {
        return std::nullopt;
    }

    // Extract the part after "data/"
    path_view = path_view.substr(data_pos + data_prefix.length() + 1);

    // Check if path ends with .dat or .idx
    if (!path_view.ends_with(".dat") && !path_view.ends_with(".idx")) {
        return std::nullopt;
    }

    // Count slashes in the remaining path
    size_t slash_count = 0;
    for (char c : path_view) {
        if (c == '/') {
            slash_count++;
        }
    }

    // Split path by '/'
    std::vector<std::string_view> parts;
    size_t start = 0;
    size_t pos = 0;
    while ((pos = path_view.find('/', start)) != std::string_view::npos) {
        if (pos > start) {
            parts.push_back(path_view.substr(start, pos - start));
        }
        start = pos + 1;
    }
    if (start < path_view.length()) {
        parts.push_back(path_view.substr(start));
    }

    if (parts.empty()) {
        return std::nullopt;
    }

    // Determine path version based on slash count and extract tablet_id
    // Version 0: {tablet_id}/{rowset_id}_{seg_id}.dat (1 slash)
    // Version 1: {shard}/{tablet_id}/{rowset_id}/{seg_id}.dat (3 slashes)

    if (slash_count == 1) {
        // Version 0 format: parts[0] should be tablet_id
        if (parts.size() >= 1) {
            try {
                int64_t tablet_id = std::stoll(std::string(parts[0]));
                return tablet_id;
            } catch (const std::exception&) {
                // Not a valid number, return nullopt at last
            }
        }
    } else if (slash_count == 3) {
        // Version 1 format: parts[1] should be tablet_id (parts[0] is shard)
        if (parts.size() >= 2) {
            try {
                int64_t tablet_id = std::stoll(std::string(parts[1]));
                return tablet_id;
            } catch (const std::exception&) {
                // Not a valid number, return nullopt at last
            }
        }
    }

    return std::nullopt;
}

} // namespace doris::io
