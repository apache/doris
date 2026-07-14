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

#include <algorithm>
#include <atomic>
#include <execution>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "common/cast_set.h"
#include "common/config.h"
#include "core/block/block.h"
#include "information_schema/schema_scanner_helper.h"
#include "io/cache/file_cache_common.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "service/backend_options.h"
#include "util/slice.h"

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
    std::lock_guard reset_lock(_reset_mtx);
    if (file_cache_settings.storage != "memory" && file_cache_settings.capacity == 0) {
        file_cache_settings.auto_capacity = true;
        file_cache_settings.requested_capacity = 0;
    }
    auto policy = make_file_cache_capacity_policy(file_cache_settings);
    file_cache_settings.storage = policy.storage;
    if (policy.storage == "memory") {
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
    }

    std::optional<FileCacheDiskState> disk_state;
    if (policy.storage != "memory") {
        FileCacheDiskState state;
        RETURN_IF_ERROR(get_file_cache_disk_state(cache_base_path, &state));
        disk_state = state;
    }
    uint64_t effective_capacity = 0;
    bool clamped_by_disk = false;
    RETURN_IF_ERROR(
            resolve_file_cache_capacity(policy, disk_state, &effective_capacity, &clamped_by_disk));
    if (file_cache_settings.capacity != effective_capacity ||
        policy.mode == FileCacheCapacityMode::AUTO) {
        RETURN_IF_ERROR(
                build_file_cache_settings(effective_capacity, policy, &file_cache_settings));
    } else {
        file_cache_settings.requested_capacity = policy.requested_capacity;
        file_cache_settings.auto_capacity = policy.mode == FileCacheCapacityMode::AUTO;
        file_cache_settings.max_file_block_size = policy.max_file_block_size;
    }
    if (clamped_by_disk) {
        LOG(WARNING) << "File cache capacity is clamped by disk. path=" << cache_base_path
                     << " requested_capacity=" << policy.requested_capacity
                     << " disk_total_capacity=" << disk_state->total_capacity;
    }

    auto cache = std::make_unique<BlockFileCache>(cache_base_path, file_cache_settings);
    FileCacheResizePlan initial_plan;
    initial_plan.next_policy = policy;
    initial_plan.next_settings = file_cache_settings;
    initial_plan.disk_state = disk_state;
    initial_plan.source = FileCacheResizeSource::STARTUP;
    initial_plan.clamped_by_disk = clamped_by_disk;
    FileCachePathResizeResult initial_result;
    RETURN_IF_ERROR(cache->apply_reset_capacity(initial_plan, &initial_result));
    RETURN_IF_ERROR(cache->initialize());
    {
        std::lock_guard lock(_mtx);
        _path_to_cache[cache_base_path] = cache.get();
        _caches.push_back(std::move(cache));
    }

    return Status::OK();
}

Status FileCacheFactory::reload_file_cache(const std::vector<CachePath>& cache_base_paths) {
    std::lock_guard reset_lock(_reset_mtx);
    {
        std::lock_guard topology_lock(_mtx);
        for (const auto& cache_path : cache_base_paths) {
            if (_path_to_cache.find(cache_path.path) == _path_to_cache.end()) {
                return Status::InternalError(
                        "Current file cache not support file cache num changes");
            }
        }
    }

    struct PreparedReplacement {
        std::string path;
        FileCacheSettings settings;
        FileCacheCapacityPolicy policy;
        std::optional<FileCacheDiskState> disk_state;
        bool clamped_by_disk = false;
    };
    std::vector<PreparedReplacement> replacements;
    replacements.reserve(cache_base_paths.size());
    for (const auto& cache_path : cache_base_paths) {
        auto settings = cache_path.init_settings();
        auto policy = make_file_cache_capacity_policy(settings);
        std::optional<FileCacheDiskState> disk_state;
        if (policy.storage != "memory") {
            FileCacheDiskState state;
            RETURN_IF_ERROR(get_file_cache_disk_state(cache_path.path, &state));
            disk_state = state;
        }
        uint64_t effective_capacity = 0;
        bool clamped_by_disk = false;
        RETURN_IF_ERROR(resolve_file_cache_capacity(policy, disk_state, &effective_capacity,
                                                    &clamped_by_disk));
        if (settings.capacity != effective_capacity || policy.mode == FileCacheCapacityMode::AUTO) {
            RETURN_IF_ERROR(build_file_cache_settings(effective_capacity, policy, &settings));
        }

        replacements.push_back({.path = cache_path.path,
                                .settings = std::move(settings),
                                .policy = std::move(policy),
                                .disk_state = disk_state,
                                .clamped_by_disk = clamped_by_disk});
    }

    std::lock_guard topology_lock(_mtx);
    for (auto& prepared : replacements) {
        auto cache_map_iter = _path_to_cache.find(prepared.path);
        auto cache_iter = std::find_if(_caches.begin(), _caches.end(),
                                       [cache_map_iter](const auto& cache_uptr) {
                                           return cache_uptr.get() == cache_map_iter->second;
                                       });

        if (cache_iter == _caches.end()) {
            return Status::InternalError("Target reload cache in path {} may have been released",
                                         prepared.path);
        }
        cache_map_iter->second = nullptr;
        cache_iter->reset();
        auto replacement = std::make_unique<BlockFileCache>(prepared.path, prepared.settings);
        FileCacheResizePlan initial_plan;
        initial_plan.next_policy = prepared.policy;
        initial_plan.next_settings = prepared.settings;
        initial_plan.disk_state = prepared.disk_state;
        initial_plan.source = FileCacheResizeSource::RELOAD;
        initial_plan.clamped_by_disk = prepared.clamped_by_disk;
        FileCachePathResizeResult initial_result;
        RETURN_IF_ERROR(replacement->apply_reset_capacity(initial_plan, &initial_result));
        RETURN_IF_ERROR(replacement->initialize());
        *cache_iter = std::move(replacement);
        cache_map_iter->second = cache_iter->get();
    }

    return Status::OK();
}

std::vector<doris::CacheBlockPB> FileCacheFactory::get_cache_data_by_path(const std::string& path) {
    auto cache_hash = BlockFileCache::hash(path);
    return get_cache_data_by_path(cache_hash);
}

std::vector<doris::CacheBlockPB> FileCacheFactory::get_cache_data_by_path(
        const UInt128Wrapper& hash) {
    std::vector<doris::CacheBlockPB> ret;
    BlockFileCache* cache = FileCacheFactory::instance()->get_by_path(hash);
    if (cache == nullptr) {
        return ret;
    }
    auto blocks = cache->get_blocks_by_key(hash);
    for (auto& [offset, fb] : blocks) {
        doris::CacheBlockPB cb;
        cb.set_block_offset(static_cast<int64_t>(offset));
        cb.set_block_size(static_cast<int64_t>(fb->range().size()));
        // try to read data into bytes
        std::string data;
        data.resize(fb->range().size());
        Slice slice(data.data(), data.size());
        // read from beginning of this block
        Status st = fb->read(slice, /*read_offset=*/0);
        if (st.ok()) {
            cb.set_data(data);
        } else {
            // On read failure, skip setting data but still report meta
            VLOG_DEBUG << "read cache block failed: " << st;
        }
        ret.emplace_back(std::move(cb));
    }
    return ret;
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

int64_t FileCacheFactory::get_cache_file_size_by_path(const UInt128Wrapper& hash) {
    io::BlockFileCache* cache = io::FileCacheFactory::instance()->get_by_path(hash);
    auto blocks = cache->get_blocks_by_key(hash);
    if (blocks.empty()) {
        return 0;
    }
    int64_t cache_size = 0;
    for (auto& [_, fb] : blocks) {
        cache_size += fb->range().size();
    }
    return cache_size;
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
FileCacheFactory::get_query_context_holders(const TUniqueId& query_id,
                                            int file_cache_query_limit_percent) {
    std::vector<BlockFileCache::QueryFileCacheContextHolderPtr> holders;
    for (const auto& cache : _caches) {
        holders.push_back(
                cache->get_query_context_holder(query_id, file_cache_query_limit_percent));
    }
    return holders;
}

Status FileCacheFactory::clear_file_caches(bool sync, std::string* ret) {
    DCHECK(ret != nullptr);

    // Sync clear is an operational action and can synchronously remove many files. Keep a single
    // process-wide sync clear in flight, so a second HTTP request fails fast instead of piling onto
    // the same cache instances. Async clear keeps the previous behavior and is not gated here.
    static std::atomic_bool sync_clear_running {false};
    struct SyncClearRunningGuard {
        std::atomic_bool* running = nullptr;
        ~SyncClearRunningGuard() {
            if (running != nullptr) {
                running->store(false, std::memory_order_release);
            }
        }
    } sync_clear_guard;
    if (sync) {
        bool expected = false;
        if (!sync_clear_running.compare_exchange_strong(expected, true, std::memory_order_acq_rel,
                                                        std::memory_order_acquire)) {
            return Status::InvalidArgument("sync clear_file_caches is already running");
        }
        sync_clear_guard.running = &sync_clear_running;
    }

    std::vector<std::string> results(_caches.size());
#ifndef USE_LIBCPP
    std::for_each(std::execution::par, _caches.begin(), _caches.end(), [&](const auto& cache) {
        size_t index = &cache - &_caches[0];
        results[index] = sync ? cache->clear_file_cache_sync() : cache->clear_file_cache_async();
    });
#else
    // libcpp do not support std::execution::par
    std::for_each(_caches.begin(), _caches.end(), [&](const auto& cache) {
        size_t index = &cache - &_caches[0];
        results[index] = sync ? cache->clear_file_cache_sync() : cache->clear_file_cache_async();
    });
#endif
    std::stringstream ss;
    for (const auto& cache_result : results) {
        ss << cache_result << "\n";
    }
    *ret = ss.str();
    return Status::OK();
}

std::string FileCacheFactory::clear_file_caches(bool sync) {
    std::string result;
    auto st = clear_file_caches(sync, &result);
    return st.ok() ? result : st.to_string();
}

void FileCacheFactory::dump_all_caches() {
    for (const auto& cache : _caches) {
        cache->dump_lru_queues(true);
    }
}

std::vector<std::string> FileCacheFactory::get_base_paths() {
    std::vector<std::string> paths;
    for (const auto& pair : _path_to_cache) {
        paths.push_back(pair.first);
    }
    return paths;
}

std::string FileCacheFactory::reset_capacity(const std::string& path, int64_t new_capacity) {
    std::string result;
    auto st = reset_capacity(path, new_capacity, &result);
    return st.ok() ? result : st.to_string();
}

Status FileCacheFactory::reset_capacity(const std::string& path, int64_t new_capacity,
                                        std::string* result) {
    DCHECK(result != nullptr);
    if (new_capacity < 0) {
        return Status::InvalidArgument("file cache capacity must not be negative");
    }

    FileCacheResetResult reset_result;
    RETURN_IF_ERROR(reset_capacity(path, cast_set<uint64_t>(new_capacity), &reset_result));
    std::stringstream ss;
    for (const auto& cache : reset_result.caches) {
        ss << fmt::format(
                "finish reset_capacity, path={} old_capacity={} new_capacity={} mode={} "
                "pending_eviction_bytes={} clamped_by_disk={} changed={}\n",
                cache.path, cache.old_capacity, cache.new_capacity,
                file_cache_capacity_mode_to_string(cache.mode), cache.pending_eviction_bytes,
                cache.clamped_by_disk, cache.changed);
    }
    *result = ss.str();
    return Status::OK();
}

Status FileCacheFactory::reset_capacity(const std::string& path, uint64_t new_capacity,
                                        FileCacheResetResult* result) {
    DCHECK(result != nullptr);
    std::lock_guard reset_lock(_reset_mtx);

    std::vector<BlockFileCache*> caches;
    {
        std::lock_guard topology_lock(_mtx);
        if (!path.empty()) {
            auto iter = _path_to_cache.find(path);
            if (iter == _path_to_cache.end()) {
                return Status::InvalidArgument("unknown file cache path {}", path);
            }
            caches.push_back(iter->second);
        } else {
            caches.reserve(_caches.size());
            for (const auto& cache : _caches) {
                caches.push_back(cache.get());
            }
        }
    }
    std::ranges::sort(caches, {}, &BlockFileCache::get_base_path);

    BlockFileCacheResetRequest request;
    request.requested_capacity = new_capacity;
    request.source = FileCacheResizeSource::HTTP;
    std::vector<FileCacheResizePlan> plans;
    plans.reserve(caches.size());
    for (auto* cache : caches) {
        plans.emplace_back();
        RETURN_IF_ERROR(cache->prepare_reset_capacity(request, std::nullopt, &plans.back()));
    }

    result->caches.clear();
    result->caches.reserve(caches.size());
    for (size_t i = 0; i < caches.size(); ++i) {
        result->caches.emplace_back();
        RETURN_IF_ERROR(caches[i]->apply_reset_capacity(plans[i], &result->caches.back()));
    }
    result->total_capacity = get_capacity();
    return Status::OK();
}

size_t FileCacheFactory::get_capacity() const {
    std::lock_guard lock(_mtx);
    size_t total_capacity = 0;
    for (const auto& cache : _caches) {
        total_capacity += cache->capacity();
    }
    return total_capacity;
}

Status FileCacheFactory::get_cache_infos(const std::string& path,
                                         std::vector<FileCacheRuntimeInfo>* infos) const {
    DCHECK(infos != nullptr);
    infos->clear();
    std::lock_guard lock(_mtx);
    if (!path.empty()) {
        auto iter = _path_to_cache.find(path);
        if (iter == _path_to_cache.end()) {
            return Status::InvalidArgument("unknown file cache path {}", path);
        }
        infos->emplace_back();
        return iter->second->get_runtime_info(&infos->back());
    }

    infos->reserve(_caches.size());
    for (const auto& cache : _caches) {
        infos->emplace_back();
        RETURN_IF_ERROR(cache->get_runtime_info(&infos->back()));
    }
    std::ranges::sort(*infos, {}, &FileCacheRuntimeInfo::path);
    return Status::OK();
}

void FileCacheFactory::get_cache_stats_block(Block* block) {
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
