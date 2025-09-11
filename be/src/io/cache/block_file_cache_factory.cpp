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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/Cache/FileCacheFactory.cpp
// and modified by Doris

#include "io/cache/block_file_cache_factory.h"

#include <glog/logging.h>

#include <string>
#include <vector>
#if defined(__APPLE__)
#include <sys/mount.h>
#else
#include <sys/statfs.h>
#endif

#include <algorithm>
#include <execution>
#include <ostream>
#include <utility>

#include "common/config.h"
#include "exec/schema_scanner/schema_scanner_helper.h"
#include "io/cache/file_cache_common.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "service/backend_options.h"
#include "vec/core/block.h"

namespace doris {
class TUniqueId;

namespace io {

FileCacheFactory* FileCacheFactory::instance() {
    return ExecEnv::GetInstance()->file_cache_factory();
}

size_t FileCacheFactory::try_release() {
    int elements = 0;
    for (auto& cache : _caches) {
        elements += cache->try_release();
    }
    return elements;
}

size_t FileCacheFactory::try_release(const std::string& base_path) {
    auto iter = _path_to_cache.find(base_path);
    if (iter != _path_to_cache.end()) {
        return iter->second->try_release();
    }
    return 0;
}

Status FileCacheFactory::create_file_cache(const std::string& cache_base_path,
                                           FileCacheSettings file_cache_settings) {
    if (file_cache_settings.storage == "memory") {
        if (cache_base_path != "memory") {
            LOG(WARNING) << "memory storage must use memory path";
            return Status::InvalidArgument("memory storage must use memory path");
        }
    } else {
        const auto& fs = global_local_filesystem();
        bool exists = false;
        RETURN_IF_ERROR(fs->exists(cache_base_path, &exists));
        if (!exists) {
            auto st = fs->create_directory(cache_base_path);
            LOG(INFO) << "path " << cache_base_path << " does not exist, create " << st.msg();
            RETURN_IF_ERROR(st);
        } else if (config::clear_file_cache) {
            RETURN_IF_ERROR(fs->delete_directory(cache_base_path));
            RETURN_IF_ERROR(fs->create_directory(cache_base_path));
        }

        struct statfs stat;
        if (statfs(cache_base_path.c_str(), &stat) < 0) {
            LOG_ERROR("").tag("file cache path", cache_base_path).tag("error", strerror(errno));
            return Status::IOError("{} statfs error {}", cache_base_path, strerror(errno));
        }
        size_t disk_capacity = static_cast<size_t>(static_cast<size_t>(stat.f_blocks) *
                                                   static_cast<size_t>(stat.f_bsize));
        if (file_cache_settings.capacity == 0 || disk_capacity < file_cache_settings.capacity) {
            LOG_INFO(
                    "The cache {} config size {} is larger than disk size {} or zero, recalc "
                    "it.",
                    cache_base_path, file_cache_settings.capacity, disk_capacity);
            file_cache_settings = get_file_cache_settings(disk_capacity,
                                                          file_cache_settings.max_query_cache_size);
        }
        LOG(INFO) << "[FileCache] path: " << cache_base_path
                  << " total_size: " << file_cache_settings.capacity
                  << " disk_total_size: " << disk_capacity;
    }
    auto cache = std::make_unique<BlockFileCache>(cache_base_path, file_cache_settings);
    RETURN_IF_ERROR(cache->initialize());
    {
        std::lock_guard lock(_mtx);
        _path_to_cache[cache_base_path] = cache.get();
        _caches.push_back(std::move(cache));
        _capacity += file_cache_settings.capacity;
    }

    return Status::OK();
}

std::vector<std::string> FileCacheFactory::get_cache_file_by_path(const UInt128Wrapper& hash) {
    io::BlockFileCache* cache = io::FileCacheFactory::instance()->get_by_path(hash);
    auto blocks = cache->get_blocks_by_key(hash);
    std::vector<std::string> ret;
    if (blocks.empty()) {
        return ret;
    } else {
        for (auto& [_, fb] : blocks) {
            ret.emplace_back(fb->get_cache_file());
        }
    }
    return ret;
}

BlockFileCache* FileCacheFactory::get_by_path(const UInt128Wrapper& key) {
    // dont need lock mutex because _caches is immutable after create_file_cache
    return _caches[KeyHash()(key) % _caches.size()].get();
}

BlockFileCache* FileCacheFactory::get_by_path(const std::string& cache_base_path) {
    auto iter = _path_to_cache.find(cache_base_path);
    if (iter == _path_to_cache.end()) {
        return nullptr;
    } else {
        return iter->second;
    }
}

std::vector<BlockFileCache::QueryFileCacheContextHolderPtr>
FileCacheFactory::get_query_context_holders(const TUniqueId& query_id) {
    std::vector<BlockFileCache::QueryFileCacheContextHolderPtr> holders;
    for (const auto& cache : _caches) {
        holders.push_back(cache->get_query_context_holder(query_id));
    }
    return holders;
}

std::string FileCacheFactory::clear_file_caches(bool sync) {
    std::vector<std::string> results(_caches.size());
#ifndef USE_LIBCPP
    std::for_each(std::execution::par, _caches.begin(), _caches.end(), [&](const auto& cache) {
        size_t index = &cache - &_caches[0];
        results[index] =
                sync ? cache->clear_file_cache_directly() : cache->clear_file_cache_async();
    });
#else
    // libcpp do not support std::execution::par
    std::for_each(_caches.begin(), _caches.end(), [&](const auto& cache) {
        size_t index = &cache - &_caches[0];
        results[index] =
                sync ? cache->clear_file_cache_directly() : cache->clear_file_cache_async();
    });
#endif
    std::stringstream ss;
    for (const auto& result : results) {
        ss << result << "\n";
    }
    return ss.str();
}

std::vector<std::string> FileCacheFactory::get_base_paths() {
    std::vector<std::string> paths;
    for (const auto& pair : _path_to_cache) {
        paths.push_back(pair.first);
    }
    return paths;
}

std::string validate_capacity(const std::string& path, int64_t new_capacity,
                              int64_t& valid_capacity) {
    struct statfs stat;
    if (statfs(path.c_str(), &stat) < 0) {
        auto ret = fmt::format("reset capacity {} statfs error {}. ", path, strerror(errno));
        LOG_ERROR(ret);
        valid_capacity = 0; // caller will handle the error
        return ret;
    }
    size_t disk_capacity = static_cast<size_t>(static_cast<size_t>(stat.f_blocks) *
                                               static_cast<size_t>(stat.f_bsize));
    if (new_capacity == 0 || disk_capacity < new_capacity) {
        auto ret = fmt::format(
                "The cache {} config size {} is larger than disk size {} or zero, recalc "
                "it to disk size. ",
                path, new_capacity, disk_capacity);
        valid_capacity = disk_capacity;
        LOG_WARNING(ret);
        return ret;
    }
    valid_capacity = new_capacity;
    return "";
}

std::string FileCacheFactory::reset_capacity(const std::string& path, int64_t new_capacity) {
    std::stringstream ss;
    size_t total_capacity = 0;
    if (path.empty()) {
        for (auto& [p, cache] : _path_to_cache) {
            int64_t valid_capacity = 0;
            ss << validate_capacity(p, new_capacity, valid_capacity);
            if (valid_capacity <= 0) {
                return ss.str();
            }
            ss << cache->reset_capacity(valid_capacity);
            total_capacity += cache->capacity();
        }
        _capacity = total_capacity;
        return ss.str();
    } else {
        if (auto iter = _path_to_cache.find(path); iter != _path_to_cache.end()) {
            int64_t valid_capacity = 0;
            ss << validate_capacity(path, new_capacity, valid_capacity);
            if (valid_capacity <= 0) {
                return ss.str();
            }
            ss << iter->second->reset_capacity(valid_capacity);

            for (auto& [p, cache] : _path_to_cache) {
                total_capacity += cache->capacity();
            }
            _capacity = total_capacity;
            return ss.str();
        }
    }
    return "Unknown the cache path " + path;
}

void FileCacheFactory::get_cache_stats_block(vectorized::Block* block) {
    // std::shared_lock<std::shared_mutex> read_lock(_qs_ctx_map_lock);
    TBackend be = BackendOptions::get_local_backend();
    int64_t be_id = be.id;
    std::string be_ip = be.host;
    for (auto& cache : _caches) {
        std::map<std::string, double> stats = cache->get_stats();
        for (auto& [k, v] : stats) {
            SchemaScannerHelper::insert_int64_value(0, be_id, block);  // be id
            SchemaScannerHelper::insert_string_value(1, be_ip, block); // be ip
            SchemaScannerHelper::insert_string_value(2, cache->get_base_path(),
                                                     block);                       // cache path
            SchemaScannerHelper::insert_string_value(3, k, block);                 // metric name
            SchemaScannerHelper::insert_string_value(4, std::to_string(v), block); // metric value
        }
    }
}

} // namespace io
} // namespace doris
