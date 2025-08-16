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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/Cache/FileCache.cpp
// and modified by Doris

#include "io/cache/block_file_cache.h"

#include <cstdio>
#include <fstream>

#include "common/status.h"
#include "cpp/sync_point.h"
#include "gen_cpp/file_cache.pb.h"
#include "runtime/exec_env.h"

#if defined(__APPLE__)
#include <sys/mount.h>
#else
#include <sys/statfs.h>
#endif

#include <chrono> // IWYU pragma: keep
#include <mutex>
#include <ranges>

#include "common/cast_set.h"
#include "common/config.h"
#include "common/logging.h"
#include "cpp/sync_point.h"
#include "io/cache/file_block.h"
#include "io/cache/file_cache_common.h"
#include "io/cache/fs_file_cache_storage.h"
#include "io/cache/mem_file_cache_storage.h"
#include "util/runtime_profile.h"
#include "util/stopwatch.hpp"
#include "util/thread.h"
#include "util/time.h"
#include "vec/common/sip_hash.h"
#include "vec/common/uint128.h"
namespace doris::io {
#include "common/compile_check_begin.h"

BlockFileCache::BlockFileCache(const std::string& cache_base_path,
                               const FileCacheSettings& cache_settings)
        : _cache_base_path(cache_base_path),
          _capacity(cache_settings.capacity),
          _max_file_block_size(cache_settings.max_file_block_size),
          _max_query_cache_size(cache_settings.max_query_cache_size) {
    _cur_cache_size_metrics = std::make_shared<bvar::Status<size_t>>(_cache_base_path.c_str(),
                                                                     "file_cache_cache_size", 0);
    _cache_capacity_metrics = std::make_shared<bvar::Status<size_t>>(
            _cache_base_path.c_str(), "file_cache_capacity", _capacity);
    _cur_ttl_cache_size_metrics = std::make_shared<bvar::Status<size_t>>(
            _cache_base_path.c_str(), "file_cache_ttl_cache_size", 0);
    _cur_normal_queue_element_count_metrics = std::make_shared<bvar::Status<size_t>>(
            _cache_base_path.c_str(), "file_cache_normal_queue_element_count", 0);
    _cur_ttl_cache_lru_queue_cache_size_metrics = std::make_shared<bvar::Status<size_t>>(
            _cache_base_path.c_str(), "file_cache_ttl_cache_lru_queue_size", 0);
    _cur_ttl_cache_lru_queue_element_count_metrics = std::make_shared<bvar::Status<size_t>>(
            _cache_base_path.c_str(), "file_cache_ttl_cache_lru_queue_element_count", 0);
    _cur_normal_queue_cache_size_metrics = std::make_shared<bvar::Status<size_t>>(
            _cache_base_path.c_str(), "file_cache_normal_queue_cache_size", 0);
    _cur_index_queue_element_count_metrics = std::make_shared<bvar::Status<size_t>>(
            _cache_base_path.c_str(), "file_cache_index_queue_element_count", 0);
    _cur_index_queue_cache_size_metrics = std::make_shared<bvar::Status<size_t>>(
            _cache_base_path.c_str(), "file_cache_index_queue_cache_size", 0);
    _cur_disposable_queue_element_count_metrics = std::make_shared<bvar::Status<size_t>>(
            _cache_base_path.c_str(), "file_cache_disposable_queue_element_count", 0);
    _cur_disposable_queue_cache_size_metrics = std::make_shared<bvar::Status<size_t>>(
            _cache_base_path.c_str(), "file_cache_disposable_queue_cache_size", 0);

    _queue_evict_size_metrics[0] = std::make_shared<bvar::Adder<size_t>>(
            _cache_base_path.c_str(), "file_cache_index_queue_evict_size");
    _queue_evict_size_metrics[1] = std::make_shared<bvar::Adder<size_t>>(
            _cache_base_path.c_str(), "file_cache_normal_queue_evict_size");
    _queue_evict_size_metrics[2] = std::make_shared<bvar::Adder<size_t>>(
            _cache_base_path.c_str(), "file_cache_disposable_queue_evict_size");
    _queue_evict_size_metrics[3] = std::make_shared<bvar::Adder<size_t>>(
            _cache_base_path.c_str(), "file_cache_ttl_cache_evict_size");
    _total_evict_size_metrics = std::make_shared<bvar::Adder<size_t>>(
            _cache_base_path.c_str(), "file_cache_total_evict_size");
    _gc_evict_bytes_metrics = std::make_shared<bvar::Adder<size_t>>(_cache_base_path.c_str(),
                                                                    "file_cache_gc_evict_bytes");
    _gc_evict_count_metrics = std::make_shared<bvar::Adder<size_t>>(_cache_base_path.c_str(),
                                                                    "file_cache_gc_evict_count");

    _evict_by_time_metrics_matrix[FileCacheType::DISPOSABLE][FileCacheType::NORMAL] =
            std::make_shared<bvar::Adder<size_t>>(_cache_base_path.c_str(),
                                                  "file_cache_evict_by_time_disposable_to_normal");
    _evict_by_time_metrics_matrix[FileCacheType::DISPOSABLE][FileCacheType::INDEX] =
            std::make_shared<bvar::Adder<size_t>>(_cache_base_path.c_str(),
                                                  "file_cache_evict_by_time_disposable_to_index");
    _evict_by_time_metrics_matrix[FileCacheType::DISPOSABLE][FileCacheType::TTL] =
            std::make_shared<bvar::Adder<size_t>>(_cache_base_path.c_str(),
                                                  "file_cache_evict_by_time_disposable_to_ttl");
    _evict_by_time_metrics_matrix[FileCacheType::NORMAL][FileCacheType::DISPOSABLE] =
            std::make_shared<bvar::Adder<size_t>>(_cache_base_path.c_str(),
                                                  "file_cache_evict_by_time_normal_to_disposable");
    _evict_by_time_metrics_matrix[FileCacheType::NORMAL][FileCacheType::INDEX] =
            std::make_shared<bvar::Adder<size_t>>(_cache_base_path.c_str(),
                                                  "file_cache_evict_by_time_normal_to_index");
    _evict_by_time_metrics_matrix[FileCacheType::NORMAL][FileCacheType::TTL] =
            std::make_shared<bvar::Adder<size_t>>(_cache_base_path.c_str(),
                                                  "file_cache_evict_by_time_normal_to_ttl");
    _evict_by_time_metrics_matrix[FileCacheType::INDEX][FileCacheType::DISPOSABLE] =
            std::make_shared<bvar::Adder<size_t>>(_cache_base_path.c_str(),
                                                  "file_cache_evict_by_time_index_to_disposable");
    _evict_by_time_metrics_matrix[FileCacheType::INDEX][FileCacheType::NORMAL] =
            std::make_shared<bvar::Adder<size_t>>(_cache_base_path.c_str(),
                                                  "file_cache_evict_by_time_index_to_normal");
    _evict_by_time_metrics_matrix[FileCacheType::INDEX][FileCacheType::TTL] =
            std::make_shared<bvar::Adder<size_t>>(_cache_base_path.c_str(),
                                                  "file_cache_evict_by_time_index_to_ttl");
    _evict_by_time_metrics_matrix[FileCacheType::TTL][FileCacheType::DISPOSABLE] =
            std::make_shared<bvar::Adder<size_t>>(_cache_base_path.c_str(),
                                                  "file_cache_evict_by_time_ttl_to_disposable");
    _evict_by_time_metrics_matrix[FileCacheType::TTL][FileCacheType::NORMAL] =
            std::make_shared<bvar::Adder<size_t>>(_cache_base_path.c_str(),
                                                  "file_cache_evict_by_time_ttl_to_normal");
    _evict_by_time_metrics_matrix[FileCacheType::TTL][FileCacheType::INDEX] =
            std::make_shared<bvar::Adder<size_t>>(_cache_base_path.c_str(),
                                                  "file_cache_evict_by_time_ttl_to_index");

    _evict_by_self_lru_metrics_matrix[FileCacheType::DISPOSABLE] =
            std::make_shared<bvar::Adder<size_t>>(_cache_base_path.c_str(),
                                                  "file_cache_evict_by_self_lru_disposable");
    _evict_by_self_lru_metrics_matrix[FileCacheType::NORMAL] =
            std::make_shared<bvar::Adder<size_t>>(_cache_base_path.c_str(),
                                                  "file_cache_evict_by_self_lru_normal");
    _evict_by_self_lru_metrics_matrix[FileCacheType::INDEX] = std::make_shared<bvar::Adder<size_t>>(
            _cache_base_path.c_str(), "file_cache_evict_by_self_lru_index");
    _evict_by_self_lru_metrics_matrix[FileCacheType::TTL] = std::make_shared<bvar::Adder<size_t>>(
            _cache_base_path.c_str(), "file_cache_evict_by_self_lru_ttl");

    _evict_by_size_metrics_matrix[FileCacheType::DISPOSABLE][FileCacheType::NORMAL] =
            std::make_shared<bvar::Adder<size_t>>(_cache_base_path.c_str(),
                                                  "file_cache_evict_by_size_disposable_to_normal");
    _evict_by_size_metrics_matrix[FileCacheType::DISPOSABLE][FileCacheType::INDEX] =
            std::make_shared<bvar::Adder<size_t>>(_cache_base_path.c_str(),
                                                  "file_cache_evict_by_size_disposable_to_index");
    _evict_by_size_metrics_matrix[FileCacheType::DISPOSABLE][FileCacheType::TTL] =
            std::make_shared<bvar::Adder<size_t>>(_cache_base_path.c_str(),
                                                  "file_cache_evict_by_size_disposable_to_ttl");
    _evict_by_size_metrics_matrix[FileCacheType::NORMAL][FileCacheType::DISPOSABLE] =
            std::make_shared<bvar::Adder<size_t>>(_cache_base_path.c_str(),
                                                  "file_cache_evict_by_size_normal_to_disposable");
    _evict_by_size_metrics_matrix[FileCacheType::NORMAL][FileCacheType::INDEX] =
            std::make_shared<bvar::Adder<size_t>>(_cache_base_path.c_str(),
                                                  "file_cache_evict_by_size_normal_to_index");
    _evict_by_size_metrics_matrix[FileCacheType::NORMAL][FileCacheType::TTL] =
            std::make_shared<bvar::Adder<size_t>>(_cache_base_path.c_str(),
                                                  "file_cache_evict_by_size_normal_to_ttl");
    _evict_by_size_metrics_matrix[FileCacheType::INDEX][FileCacheType::DISPOSABLE] =
            std::make_shared<bvar::Adder<size_t>>(_cache_base_path.c_str(),
                                                  "file_cache_evict_by_size_index_to_disposable");
    _evict_by_size_metrics_matrix[FileCacheType::INDEX][FileCacheType::NORMAL] =
            std::make_shared<bvar::Adder<size_t>>(_cache_base_path.c_str(),
                                                  "file_cache_evict_by_size_index_to_normal");
    _evict_by_size_metrics_matrix[FileCacheType::INDEX][FileCacheType::TTL] =
            std::make_shared<bvar::Adder<size_t>>(_cache_base_path.c_str(),
                                                  "file_cache_evict_by_size_index_to_ttl");
    _evict_by_size_metrics_matrix[FileCacheType::TTL][FileCacheType::DISPOSABLE] =
            std::make_shared<bvar::Adder<size_t>>(_cache_base_path.c_str(),
                                                  "file_cache_evict_by_size_ttl_to_disposable");
    _evict_by_size_metrics_matrix[FileCacheType::TTL][FileCacheType::NORMAL] =
            std::make_shared<bvar::Adder<size_t>>(_cache_base_path.c_str(),
                                                  "file_cache_evict_by_size_ttl_to_normal");
    _evict_by_size_metrics_matrix[FileCacheType::TTL][FileCacheType::INDEX] =
            std::make_shared<bvar::Adder<size_t>>(_cache_base_path.c_str(),
                                                  "file_cache_evict_by_size_ttl_to_index");

    _evict_by_try_release = std::make_shared<bvar::Adder<size_t>>(
            _cache_base_path.c_str(), "file_cache_evict_by_try_release");

    _num_read_blocks = std::make_shared<bvar::Adder<size_t>>(_cache_base_path.c_str(),
                                                             "file_cache_num_read_blocks");
    _num_hit_blocks = std::make_shared<bvar::Adder<size_t>>(_cache_base_path.c_str(),
                                                            "file_cache_num_hit_blocks");
    _num_removed_blocks = std::make_shared<bvar::Adder<size_t>>(_cache_base_path.c_str(),
                                                                "file_cache_num_removed_blocks");

    _num_hit_blocks_5m = std::make_shared<bvar::Window<bvar::Adder<size_t>>>(
            _cache_base_path.c_str(), "file_cache_num_hit_blocks_5m", _num_hit_blocks.get(), 300);
    _num_read_blocks_5m = std::make_shared<bvar::Window<bvar::Adder<size_t>>>(
            _cache_base_path.c_str(), "file_cache_num_read_blocks_5m", _num_read_blocks.get(), 300);
    _num_hit_blocks_1h = std::make_shared<bvar::Window<bvar::Adder<size_t>>>(
            _cache_base_path.c_str(), "file_cache_num_hit_blocks_1h", _num_hit_blocks.get(), 3600);
    _num_read_blocks_1h = std::make_shared<bvar::Window<bvar::Adder<size_t>>>(
            _cache_base_path.c_str(), "file_cache_num_read_blocks_1h", _num_read_blocks.get(),
            3600);

    _hit_ratio = std::make_shared<bvar::Status<double>>(_cache_base_path.c_str(),
                                                        "file_cache_hit_ratio", 0.0);
    _hit_ratio_5m = std::make_shared<bvar::Status<double>>(_cache_base_path.c_str(),
                                                           "file_cache_hit_ratio_5m", 0.0);
    _hit_ratio_1h = std::make_shared<bvar::Status<double>>(_cache_base_path.c_str(),
                                                           "file_cache_hit_ratio_1h", 0.0);
    _disk_limit_mode_metrics = std::make_shared<bvar::Status<size_t>>(
            _cache_base_path.c_str(), "file_cache_disk_limit_mode", 0);
    _need_evict_cache_in_advance_metrics = std::make_shared<bvar::Status<size_t>>(
            _cache_base_path.c_str(), "file_cache_need_evict_cache_in_advance", 0);

    _cache_lock_wait_time_us = std::make_shared<bvar::LatencyRecorder>(
            _cache_base_path.c_str(), "file_cache_cache_lock_wait_time_us");
    _get_or_set_latency_us = std::make_shared<bvar::LatencyRecorder>(
            _cache_base_path.c_str(), "file_cache_get_or_set_latency_us");
    _storage_sync_remove_latency_us = std::make_shared<bvar::LatencyRecorder>(
            _cache_base_path.c_str(), "file_cache_storage_sync_remove_latency_us");
    _storage_retry_sync_remove_latency_us = std::make_shared<bvar::LatencyRecorder>(
            _cache_base_path.c_str(), "file_cache_storage_retry_sync_remove_latency_us");
    _storage_async_remove_latency_us = std::make_shared<bvar::LatencyRecorder>(
            _cache_base_path.c_str(), "file_cache_storage_async_remove_latency_us");
    _evict_in_advance_latency_us = std::make_shared<bvar::LatencyRecorder>(
            _cache_base_path.c_str(), "file_cache_evict_in_advance_latency_us");
    _lru_dump_latency_us = std::make_shared<bvar::LatencyRecorder>(
            _cache_base_path.c_str(), "file_cache_lru_dump_latency_us");
    _recycle_keys_length_recorder = std::make_shared<bvar::LatencyRecorder>(
            _cache_base_path.c_str(), "file_cache_recycle_keys_length");
    _ttl_gc_latency_us = std::make_shared<bvar::LatencyRecorder>(_cache_base_path.c_str(),
                                                                 "file_cache_ttl_gc_latency_us");
    _shadow_queue_levenshtein_distance = std::make_shared<bvar::LatencyRecorder>(
            _cache_base_path.c_str(), "file_cache_shadow_queue_levenshtein_distance");

    _disposable_queue = LRUQueue(cache_settings.disposable_queue_size,
                                 cache_settings.disposable_queue_elements, 60 * 60);
    _index_queue = LRUQueue(cache_settings.index_queue_size, cache_settings.index_queue_elements,
                            7 * 24 * 60 * 60);
    _normal_queue = LRUQueue(cache_settings.query_queue_size, cache_settings.query_queue_elements,
                             24 * 60 * 60);
    _ttl_queue = LRUQueue(cache_settings.ttl_queue_size, cache_settings.ttl_queue_elements,
                          std::numeric_limits<int>::max());

    _lru_recorder = std::make_unique<LRUQueueRecorder>(this);
    _lru_dumper = std::make_unique<CacheLRUDumper>(this, _lru_recorder.get());
    if (cache_settings.storage == "memory") {
        _storage = std::make_unique<MemFileCacheStorage>();
        _cache_base_path = "memory";
    } else {
        _storage = std::make_unique<FSFileCacheStorage>();
    }

    LOG(INFO) << "file cache path= " << _cache_base_path << " " << cache_settings.to_string();
}

UInt128Wrapper BlockFileCache::hash(const std::string& path) {
    uint128_t value;
    sip_hash128(path.data(), path.size(), reinterpret_cast<char*>(&value));
    return UInt128Wrapper(value);
}

BlockFileCache::QueryFileCacheContextHolderPtr BlockFileCache::get_query_context_holder(
        const TUniqueId& query_id) {
    SCOPED_CACHE_LOCK(_mutex, this);
    if (!config::enable_file_cache_query_limit) {
        return {};
    }

    /// if enable_filesystem_query_cache_limit is true,
    /// we create context query for current query.
    auto context = get_or_set_query_context(query_id, cache_lock);
    return std::make_unique<QueryFileCacheContextHolder>(query_id, this, context);
}

BlockFileCache::QueryFileCacheContextPtr BlockFileCache::get_query_context(
        const TUniqueId& query_id, std::lock_guard<std::mutex>& cache_lock) {
    auto query_iter = _query_map.find(query_id);
    return (query_iter == _query_map.end()) ? nullptr : query_iter->second;
}

void BlockFileCache::remove_query_context(const TUniqueId& query_id) {
    SCOPED_CACHE_LOCK(_mutex, this);
    const auto& query_iter = _query_map.find(query_id);

    if (query_iter != _query_map.end() && query_iter->second.use_count() <= 1) {
        _query_map.erase(query_iter);
    }
}

BlockFileCache::QueryFileCacheContextPtr BlockFileCache::get_or_set_query_context(
        const TUniqueId& query_id, std::lock_guard<std::mutex>& cache_lock) {
    if (query_id.lo == 0 && query_id.hi == 0) {
        return nullptr;
    }

    auto context = get_query_context(query_id, cache_lock);
    if (context) {
        return context;
    }

    auto query_context = std::make_shared<QueryFileCacheContext>(_max_query_cache_size);
    auto query_iter = _query_map.emplace(query_id, query_context).first;
    return query_iter->second;
}

void BlockFileCache::QueryFileCacheContext::remove(const UInt128Wrapper& hash, size_t offset,
                                                   std::lock_guard<std::mutex>& cache_lock) {
    auto pair = std::make_pair(hash, offset);
    auto record = records.find(pair);
    DCHECK(record != records.end());
    auto iter = record->second;
    records.erase(pair);
    lru_queue.remove(iter, cache_lock);
}

void BlockFileCache::QueryFileCacheContext::reserve(const UInt128Wrapper& hash, size_t offset,
                                                    size_t size,
                                                    std::lock_guard<std::mutex>& cache_lock) {
    auto pair = std::make_pair(hash, offset);
    if (records.find(pair) == records.end()) {
        auto queue_iter = lru_queue.add(hash, offset, size, cache_lock);
        records.insert({pair, queue_iter});
    }
}

Status BlockFileCache::initialize() {
    SCOPED_CACHE_LOCK(_mutex, this);
    return initialize_unlocked(cache_lock);
}

Status BlockFileCache::initialize_unlocked(std::lock_guard<std::mutex>& cache_lock) {
    DCHECK(!_is_initialized);
    _is_initialized = true;
    if (config::file_cache_background_lru_dump_tail_record_num > 0) {
        // requirements:
        // 1. restored data should not overwrite the last dump
        // 2. restore should happen before load and async load
        // 3. all queues should be restored sequencially to avoid conflict
        // TODO(zhengyu): we can parralize them but will increase complexity, so lets check the time cost
        // to see if any improvement is a necessary
        restore_lru_queues_from_disk(cache_lock);
    }
    RETURN_IF_ERROR(_storage->init(this));
    _cache_background_monitor_thread = std::thread(&BlockFileCache::run_background_monitor, this);
    _cache_background_ttl_gc_thread = std::thread(&BlockFileCache::run_background_ttl_gc, this);
    _cache_background_gc_thread = std::thread(&BlockFileCache::run_background_gc, this);
    _cache_background_evict_in_advance_thread =
            std::thread(&BlockFileCache::run_background_evict_in_advance, this);

    // Initialize LRU dump thread and restore queues
    _cache_background_lru_dump_thread = std::thread(&BlockFileCache::run_background_lru_dump, this);
    _cache_background_lru_log_replay_thread =
            std::thread(&BlockFileCache::run_background_lru_log_replay, this);

    return Status::OK();
}

void BlockFileCache::use_cell(const FileBlockCell& cell, FileBlocks* result, bool move_iter_flag,
                              std::lock_guard<std::mutex>& cache_lock) {
    if (result) {
        result->push_back(cell.file_block);
    }

    auto& queue = get_queue(cell.file_block->cache_type());
    /// Move to the end of the queue. The iterator remains valid.
    if (cell.queue_iterator && move_iter_flag) {
        queue.move_to_end(*cell.queue_iterator, cache_lock);
        _lru_recorder->record_queue_event(cell.file_block->cache_type(),
                                          CacheLRULogType::MOVETOBACK, cell.file_block->_key.hash,
                                          cell.file_block->_key.offset, cell.size());
    }

    cell.update_atime();
}

template <class T>
    requires IsXLock<T>
BlockFileCache::FileBlockCell* BlockFileCache::get_cell(const UInt128Wrapper& hash, size_t offset,
                                                        T& /* cache_lock */) {
    auto it = _files.find(hash);
    if (it == _files.end()) {
        return nullptr;
    }

    auto& offsets = it->second;
    auto cell_it = offsets.find(offset);
    if (cell_it == offsets.end()) {
        return nullptr;
    }

    return &cell_it->second;
}

bool BlockFileCache::need_to_move(FileCacheType cell_type, FileCacheType query_type) const {
    return query_type != FileCacheType::DISPOSABLE && cell_type != FileCacheType::DISPOSABLE;
}

FileBlocks BlockFileCache::get_impl(const UInt128Wrapper& hash, const CacheContext& context,
                                    const FileBlock::Range& range,
                                    std::lock_guard<std::mutex>& cache_lock) {
    /// Given range = [left, right] and non-overlapping ordered set of file blocks,
    /// find list [block1, ..., blockN] of blocks which intersect with given range.
    auto it = _files.find(hash);
    if (it == _files.end()) {
        if (_async_open_done) {
            return {};
        }
        FileCacheKey key;
        key.hash = hash;
        key.meta.type = context.cache_type;
        key.meta.expiration_time = context.expiration_time;
        _storage->load_blocks_directly_unlocked(this, key, cache_lock);

        it = _files.find(hash);
        if (it == _files.end()) [[unlikely]] {
            return {};
        }
    }

    auto& file_blocks = it->second;
    if (file_blocks.empty()) {
        LOG(WARNING) << "file_blocks is empty for hash=" << hash.to_string()
                     << " cache type=" << context.cache_type
                     << " cache expiration time=" << context.expiration_time
                     << " cache range=" << range.left << " " << range.right
                     << " query id=" << context.query_id;
        DCHECK(false);
        _files.erase(hash);
        return {};
    }
    // change to ttl if the blocks aren't ttl
    if (context.cache_type == FileCacheType::TTL && _key_to_time.find(hash) == _key_to_time.end()) {
        for (auto& [_, cell] : file_blocks) {
            Status st = cell.file_block->update_expiration_time(context.expiration_time);
            if (!st.ok()) {
                LOG_WARNING("Failed to change key meta").error(st);
            }

            FileCacheType origin_type = cell.file_block->cache_type();
            if (origin_type == FileCacheType::TTL) continue;
            st = cell.file_block->change_cache_type_between_ttl_and_others(FileCacheType::TTL);
            if (st.ok()) {
                auto& queue = get_queue(origin_type);
                queue.remove(cell.queue_iterator.value(), cache_lock);
                _lru_recorder->record_queue_event(origin_type, CacheLRULogType::REMOVE,
                                                  cell.file_block->get_hash_value(),
                                                  cell.file_block->offset(), cell.size());
                auto& ttl_queue = get_queue(FileCacheType::TTL);
                cell.queue_iterator =
                        ttl_queue.add(cell.file_block->get_hash_value(), cell.file_block->offset(),
                                      cell.file_block->range().size(), cache_lock);
                _lru_recorder->record_queue_event(FileCacheType::TTL, CacheLRULogType::ADD,
                                                  cell.file_block->get_hash_value(),
                                                  cell.file_block->offset(), cell.size());
            } else {
                LOG_WARNING("Failed to change key meta").error(st);
            }
        }
        _key_to_time[hash] = context.expiration_time;
        _time_to_key.insert(std::make_pair(context.expiration_time, hash));
    }
    if (auto iter = _key_to_time.find(hash);
        // TODO(zhengyu): Why the hell the type is NORMAL while context set expiration_time?
        (context.cache_type == FileCacheType::NORMAL || context.cache_type == FileCacheType::TTL) &&
        iter != _key_to_time.end() && iter->second != context.expiration_time) {
        // remove from _time_to_key
        auto _time_to_key_iter = _time_to_key.equal_range(iter->second);
        while (_time_to_key_iter.first != _time_to_key_iter.second) {
            if (_time_to_key_iter.first->second == hash) {
                _time_to_key_iter.first = _time_to_key.erase(_time_to_key_iter.first);
                break;
            }
            _time_to_key_iter.first++;
        }
        for (auto& [_, cell] : file_blocks) {
            Status st = cell.file_block->update_expiration_time(context.expiration_time);
            if (!st.ok()) {
                LOG_WARNING("Failed to change key meta").error(st);
            }
        }
        if (context.expiration_time == 0) {
            for (auto& [_, cell] : file_blocks) {
                auto cache_type = cell.file_block->cache_type();
                if (cache_type != FileCacheType::TTL) continue;
                auto st = cell.file_block->change_cache_type_between_ttl_and_others(
                        FileCacheType::NORMAL);
                if (st.ok()) {
                    if (cell.queue_iterator) {
                        auto& ttl_queue = get_queue(FileCacheType::TTL);
                        ttl_queue.remove(cell.queue_iterator.value(), cache_lock);
                        _lru_recorder->record_queue_event(FileCacheType::TTL,
                                                          CacheLRULogType::REMOVE,
                                                          cell.file_block->get_hash_value(),
                                                          cell.file_block->offset(), cell.size());
                    }
                    auto& queue = get_queue(FileCacheType::NORMAL);
                    cell.queue_iterator =
                            queue.add(cell.file_block->get_hash_value(), cell.file_block->offset(),
                                      cell.file_block->range().size(), cache_lock);
                    _lru_recorder->record_queue_event(FileCacheType::NORMAL, CacheLRULogType::ADD,
                                                      cell.file_block->get_hash_value(),
                                                      cell.file_block->offset(), cell.size());
                } else {
                    LOG_WARNING("Failed to change key meta").error(st);
                }
            }
            _key_to_time.erase(iter);
        } else {
            _time_to_key.insert(std::make_pair(context.expiration_time, hash));
            iter->second = context.expiration_time;
        }
    }

    FileBlocks result;
    auto block_it = file_blocks.lower_bound(range.left);
    if (block_it == file_blocks.end()) {
        /// N - last cached block for given file hash, block{N}.offset < range.left:
        ///   block{N}                       block{N}
        /// [________                         [_______]
        ///     [__________]         OR                  [________]
        ///     ^                                        ^
        ///     range.left                               range.left

        const auto& cell = file_blocks.rbegin()->second;
        if (cell.file_block->range().right < range.left) {
            return {};
        }

        use_cell(cell, &result, need_to_move(cell.file_block->cache_type(), context.cache_type),
                 cache_lock);
    } else { /// block_it <-- segmment{k}
        if (block_it != file_blocks.begin()) {
            const auto& prev_cell = std::prev(block_it)->second;
            const auto& prev_cell_range = prev_cell.file_block->range();

            if (range.left <= prev_cell_range.right) {
                ///   block{k-1}  block{k}
                ///   [________]   [_____
                ///       [___________
                ///       ^
                ///       range.left

                use_cell(prev_cell, &result,
                         need_to_move(prev_cell.file_block->cache_type(), context.cache_type),
                         cache_lock);
            }
        }

        ///  block{k} ...       block{k-1}  block{k}                      block{k}
        ///  [______              [______]     [____                        [________
        ///  [_________     OR              [________      OR    [______]   ^
        ///  ^                              ^                           ^   block{k}.offset
        ///  range.left                     range.left                  range.right

        while (block_it != file_blocks.end()) {
            const auto& cell = block_it->second;
            if (range.right < cell.file_block->range().left) {
                break;
            }

            use_cell(cell, &result, need_to_move(cell.file_block->cache_type(), context.cache_type),
                     cache_lock);
            ++block_it;
        }
    }

    return result;
}

std::string BlockFileCache::clear_file_cache_async() {
    LOG(INFO) << "start clear_file_cache_async, path=" << _cache_base_path;
    _lru_dumper->remove_lru_dump_files();
    int64_t num_cells_all = 0;
    int64_t num_cells_to_delete = 0;
    int64_t num_cells_wait_recycle = 0;
    int64_t num_files_all = 0;
    TEST_SYNC_POINT_CALLBACK("BlockFileCache::clear_file_cache_async");
    {
        SCOPED_CACHE_LOCK(_mutex, this);

        std::vector<FileBlockCell*> deleting_cells;
        for (auto& [_, offset_to_cell] : _files) {
            ++num_files_all;
            for (auto& [_1, cell] : offset_to_cell) {
                ++num_cells_all;
                deleting_cells.push_back(&cell);
            }
        }

        // we cannot delete the element in the loop above, because it will break the iterator
        for (auto& cell : deleting_cells) {
            if (!cell->releasable()) {
                LOG(INFO) << "cell is not releasable, hash="
                          << " offset=" << cell->file_block->offset();
                cell->file_block->set_deleting();
                ++num_cells_wait_recycle;
                continue;
            }
            FileBlockSPtr file_block = cell->file_block;
            if (file_block) {
                std::lock_guard block_lock(file_block->_mutex);
                remove(file_block, cache_lock, block_lock, false);
                ++num_cells_to_delete;
            }
        }
    }
    std::stringstream ss;
    ss << "finish clear_file_cache_async, path=" << _cache_base_path
       << " num_files_all=" << num_files_all << " num_cells_all=" << num_cells_all
       << " num_cells_to_delete=" << num_cells_to_delete
       << " num_cells_wait_recycle=" << num_cells_wait_recycle;
    auto msg = ss.str();
    LOG(INFO) << msg;
    _lru_dumper->remove_lru_dump_files();
    return msg;
}

FileBlocks BlockFileCache::split_range_into_cells(const UInt128Wrapper& hash,
                                                  const CacheContext& context, size_t offset,
                                                  size_t size, FileBlock::State state,
                                                  std::lock_guard<std::mutex>& cache_lock) {
    DCHECK(size > 0);

    auto current_pos = offset;
    auto end_pos_non_included = offset + size;

    size_t current_size = 0;
    size_t remaining_size = size;

    FileBlocks file_blocks;
    while (current_pos < end_pos_non_included) {
        current_size = std::min(remaining_size, _max_file_block_size);
        remaining_size -= current_size;
        state = try_reserve(hash, context, current_pos, current_size, cache_lock)
                        ? state
                        : FileBlock::State::SKIP_CACHE;
        if (state == FileBlock::State::SKIP_CACHE) [[unlikely]] {
            FileCacheKey key;
            key.hash = hash;
            key.offset = current_pos;
            key.meta.type = context.cache_type;
            key.meta.expiration_time = context.expiration_time;
            auto file_block = std::make_shared<FileBlock>(key, current_size, this,
                                                          FileBlock::State::SKIP_CACHE);
            file_blocks.push_back(std::move(file_block));
        } else {
            auto* cell = add_cell(hash, context, current_pos, current_size, state, cache_lock);
            if (cell) {
                file_blocks.push_back(cell->file_block);
                if (!context.is_cold_data) {
                    cell->update_atime();
                }
            }
        }

        current_pos += current_size;
    }

    DCHECK(file_blocks.empty() || offset + size - 1 == file_blocks.back()->range().right);
    return file_blocks;
}

void BlockFileCache::fill_holes_with_empty_file_blocks(FileBlocks& file_blocks,
                                                       const UInt128Wrapper& hash,
                                                       const CacheContext& context,
                                                       const FileBlock::Range& range,
                                                       std::lock_guard<std::mutex>& cache_lock) {
    /// There are blocks [block1, ..., blockN]
    /// (non-overlapping, non-empty, ascending-ordered) which (maybe partially)
    /// intersect with given range.

    /// It can have holes:
    /// [____________________]         -- requested range
    ///     [____]  [_]   [_________]  -- intersecting cache [block1, ..., blockN]
    ///
    /// For each such hole create a cell with file block state EMPTY.

    auto it = file_blocks.begin();
    auto block_range = (*it)->range();

    size_t current_pos = 0;
    if (block_range.left < range.left) {
        ///    [_______     -- requested range
        /// [_______
        /// ^
        /// block1

        current_pos = block_range.right + 1;
        ++it;
    } else {
        current_pos = range.left;
    }

    while (current_pos <= range.right && it != file_blocks.end()) {
        block_range = (*it)->range();

        if (current_pos == block_range.left) {
            current_pos = block_range.right + 1;
            ++it;
            continue;
        }

        DCHECK(current_pos < block_range.left);

        auto hole_size = block_range.left - current_pos;

        file_blocks.splice(it, split_range_into_cells(hash, context, current_pos, hole_size,
                                                      FileBlock::State::EMPTY, cache_lock));

        current_pos = block_range.right + 1;
        ++it;
    }

    if (current_pos <= range.right) {
        ///   ________]     -- requested range
        ///   _____]
        ///        ^
        /// blockN

        auto hole_size = range.right - current_pos + 1;

        file_blocks.splice(file_blocks.end(),
                           split_range_into_cells(hash, context, current_pos, hole_size,
                                                  FileBlock::State::EMPTY, cache_lock));
    }
}

FileBlocksHolder BlockFileCache::get_or_set(const UInt128Wrapper& hash, size_t offset, size_t size,
                                            CacheContext& context) {
    FileBlock::Range range(offset, offset + size - 1);

    ReadStatistics* stats = context.stats;
    DCHECK(stats != nullptr);
    MonotonicStopWatch sw;
    sw.start();
    std::lock_guard cache_lock(_mutex);
    stats->lock_wait_timer += sw.elapsed_time();
    FileBlocks file_blocks;
    int64_t duration = 0;
    {
        SCOPED_RAW_TIMER(&duration);
        if (auto iter = _key_to_time.find(hash);
            context.cache_type == FileCacheType::INDEX && iter != _key_to_time.end()) {
            context.cache_type = FileCacheType::TTL;
            context.expiration_time = iter->second;
        }

        /// Get all blocks which intersect with the given range.
        {
            SCOPED_RAW_TIMER(&stats->get_timer);
            file_blocks = get_impl(hash, context, range, cache_lock);
        }

        if (file_blocks.empty()) {
            SCOPED_RAW_TIMER(&stats->set_timer);
            file_blocks = split_range_into_cells(hash, context, offset, size,
                                                 FileBlock::State::EMPTY, cache_lock);
        } else {
            SCOPED_RAW_TIMER(&stats->set_timer);
            fill_holes_with_empty_file_blocks(file_blocks, hash, context, range, cache_lock);
        }
        DCHECK(!file_blocks.empty());
        *_num_read_blocks << file_blocks.size();
        for (auto& block : file_blocks) {
            if (block->state_unsafe() == FileBlock::State::DOWNLOADED) {
                *_num_hit_blocks << 1;
            }
        }
    }
    *_get_or_set_latency_us << (duration / 1000);
    return FileBlocksHolder(std::move(file_blocks));
}

BlockFileCache::FileBlockCell* BlockFileCache::add_cell(const UInt128Wrapper& hash,
                                                        const CacheContext& context, size_t offset,
                                                        size_t size, FileBlock::State state,
                                                        std::lock_guard<std::mutex>& cache_lock) {
    /// Create a file block cell and put it in `files` map by [hash][offset].
    if (size == 0) {
        return nullptr; /// Empty files are not cached.
    }

    auto& offsets = _files[hash];
    auto itr = offsets.find(offset);
    if (itr != offsets.end()) {
        VLOG_DEBUG << "Cache already exists for hash: " << hash.to_string()
                   << ", offset: " << offset << ", size: " << size
                   << ".\nCurrent cache structure: " << dump_structure_unlocked(hash, cache_lock);
        return &(itr->second);
    }

    FileCacheKey key;
    key.hash = hash;
    key.offset = offset;
    key.meta.type = context.cache_type;
    key.meta.expiration_time = context.expiration_time;
    FileBlockCell cell(std::make_shared<FileBlock>(key, size, this, state), cache_lock);
    Status st;
    if (context.expiration_time == 0 && context.cache_type == FileCacheType::TTL) {
        st = cell.file_block->change_cache_type_between_ttl_and_others(FileCacheType::NORMAL);
    } else if (context.cache_type != FileCacheType::TTL && context.expiration_time != 0) {
        st = cell.file_block->change_cache_type_between_ttl_and_others(FileCacheType::TTL);
    }
    if (!st.ok()) {
        LOG(WARNING) << "Cannot change cache type. expiration_time=" << context.expiration_time
                     << " cache_type=" << cache_type_to_string(context.cache_type)
                     << " error=" << st.msg();
    }

    auto& queue = get_queue(cell.file_block->cache_type());
    cell.queue_iterator = queue.add(hash, offset, size, cache_lock);
    _lru_recorder->record_queue_event(cell.file_block->cache_type(), CacheLRULogType::ADD,
                                      cell.file_block->get_hash_value(), cell.file_block->offset(),
                                      cell.size());

    if (cell.file_block->cache_type() == FileCacheType::TTL) {
        if (_key_to_time.find(hash) == _key_to_time.end()) {
            _key_to_time[hash] = context.expiration_time;
            _time_to_key.insert(std::make_pair(context.expiration_time, hash));
        }
        _cur_ttl_size += cell.size();
    }
    auto [it, _] = offsets.insert(std::make_pair(offset, std::move(cell)));
    _cur_cache_size += size;
    return &(it->second);
}

size_t BlockFileCache::try_release() {
    SCOPED_CACHE_LOCK(_mutex, this);
    std::vector<FileBlockCell*> trash;
    for (auto& [hash, blocks] : _files) {
        for (auto& [offset, cell] : blocks) {
            if (cell.releasable()) {
                trash.emplace_back(&cell);
            } else {
                cell.file_block->set_deleting();
            }
        }
    }
    size_t remove_size = 0;
    for (auto& cell : trash) {
        FileBlockSPtr file_block = cell->file_block;
        std::lock_guard lc(cell->file_block->_mutex);
        remove_size += file_block->range().size();
        remove(file_block, cache_lock, lc);
    }
    *_evict_by_try_release << remove_size;
    LOG(INFO) << "Released " << trash.size() << " blocks in file cache " << _cache_base_path;
    return trash.size();
}

LRUQueue& BlockFileCache::get_queue(FileCacheType type) {
    switch (type) {
    case FileCacheType::INDEX:
        return _index_queue;
    case FileCacheType::DISPOSABLE:
        return _disposable_queue;
    case FileCacheType::NORMAL:
        return _normal_queue;
    case FileCacheType::TTL:
        return _ttl_queue;
    default:
        DCHECK(false);
    }
    return _normal_queue;
}

const LRUQueue& BlockFileCache::get_queue(FileCacheType type) const {
    switch (type) {
    case FileCacheType::INDEX:
        return _index_queue;
    case FileCacheType::DISPOSABLE:
        return _disposable_queue;
    case FileCacheType::NORMAL:
        return _normal_queue;
    case FileCacheType::TTL:
        return _ttl_queue;
    default:
        DCHECK(false);
    }
    return _normal_queue;
}

void BlockFileCache::remove_file_blocks(std::vector<FileBlockCell*>& to_evict,
                                        std::lock_guard<std::mutex>& cache_lock, bool sync) {
    auto remove_file_block_if = [&](FileBlockCell* cell) {
        FileBlockSPtr file_block = cell->file_block;
        if (file_block) {
            std::lock_guard block_lock(file_block->_mutex);
            remove(file_block, cache_lock, block_lock, sync);
        }
    };
    std::for_each(to_evict.begin(), to_evict.end(), remove_file_block_if);
}

void BlockFileCache::remove_file_blocks_and_clean_time_maps(
        std::vector<FileBlockCell*>& to_evict, std::lock_guard<std::mutex>& cache_lock) {
    auto remove_file_block_and_clean_time_maps_if = [&](FileBlockCell* cell) {
        FileBlockSPtr file_block = cell->file_block;
        if (file_block) {
            std::lock_guard block_lock(file_block->_mutex);
            auto hash = cell->file_block->get_hash_value();
            remove(file_block, cache_lock, block_lock);
            if (_files.find(hash) == _files.end()) {
                if (auto iter = _key_to_time.find(hash);
                    _key_to_time.find(hash) != _key_to_time.end()) {
                    auto _time_to_key_iter = _time_to_key.equal_range(iter->second);
                    while (_time_to_key_iter.first != _time_to_key_iter.second) {
                        if (_time_to_key_iter.first->second == hash) {
                            _time_to_key_iter.first = _time_to_key.erase(_time_to_key_iter.first);
                            break;
                        }
                        _time_to_key_iter.first++;
                    }
                    _key_to_time.erase(hash);
                }
            }
        }
    };
    std::for_each(to_evict.begin(), to_evict.end(), remove_file_block_and_clean_time_maps_if);
}

void BlockFileCache::find_evict_candidates(LRUQueue& queue, size_t size, size_t cur_cache_size,
                                           size_t& removed_size,
                                           std::vector<FileBlockCell*>& to_evict,
                                           std::lock_guard<std::mutex>& cache_lock,
                                           size_t& cur_removed_size, bool evict_in_advance) {
    for (const auto& [entry_key, entry_offset, entry_size] : queue) {
        if (!is_overflow(removed_size, size, cur_cache_size, evict_in_advance)) {
            break;
        }
        auto* cell = get_cell(entry_key, entry_offset, cache_lock);

        DCHECK(cell) << "Cache became inconsistent. key: " << entry_key.to_string()
                     << ", offset: " << entry_offset;

        size_t cell_size = cell->size();
        DCHECK(entry_size == cell_size);

        if (cell->releasable()) {
            auto& file_block = cell->file_block;

            std::lock_guard block_lock(file_block->_mutex);
            DCHECK(file_block->_download_state == FileBlock::State::DOWNLOADED);
            to_evict.push_back(cell);
            removed_size += cell_size;
            cur_removed_size += cell_size;
        }
    }
}

// 1. if async load file cache not finish
//     a. evict from lru queue
// 2. if ttl cache
//     a. evict from disposable/normal/index queue one by one
// 3. if dont reach query limit or dont have query limit
//     a. evict from other queue
//     b. evict from current queue
//         a.1 if the data belong write, then just evict cold data
// 4. if reach query limit
//     a. evict from query queue
//     b. evict from other queue
bool BlockFileCache::try_reserve(const UInt128Wrapper& hash, const CacheContext& context,
                                 size_t offset, size_t size,
                                 std::lock_guard<std::mutex>& cache_lock) {
    if (!_async_open_done) {
        return try_reserve_during_async_load(size, cache_lock);
    }
    // use this strategy in scenarios where there is insufficient disk capacity or insufficient number of inodes remaining
    // directly eliminate 5 times the size of the space
    if (_disk_resource_limit_mode) {
        size = 5 * size;
    }

    auto query_context = config::enable_file_cache_query_limit &&
                                         (context.query_id.hi != 0 || context.query_id.lo != 0)
                                 ? get_query_context(context.query_id, cache_lock)
                                 : nullptr;
    if (!query_context) {
        return try_reserve_for_lru(hash, nullptr, context, offset, size, cache_lock);
    } else if (query_context->get_cache_size(cache_lock) + size <=
               query_context->get_max_cache_size()) {
        return try_reserve_for_lru(hash, query_context, context, offset, size, cache_lock);
    }
    int64_t cur_time = std::chrono::duration_cast<std::chrono::seconds>(
                               std::chrono::steady_clock::now().time_since_epoch())
                               .count();
    auto& queue = get_queue(context.cache_type);
    size_t removed_size = 0;
    size_t ghost_remove_size = 0;
    size_t queue_size = queue.get_capacity(cache_lock);
    size_t cur_cache_size = _cur_cache_size;
    size_t query_context_cache_size = query_context->get_cache_size(cache_lock);

    std::vector<LRUQueue::Iterator> ghost;
    std::vector<FileBlockCell*> to_evict;

    size_t max_size = queue.get_max_size();
    auto is_overflow = [&] {
        return _disk_resource_limit_mode ? removed_size < size
                                         : cur_cache_size + size - removed_size > _capacity ||
                                                   (queue_size + size - removed_size > max_size) ||
                                                   (query_context_cache_size + size -
                                                            (removed_size + ghost_remove_size) >
                                                    query_context->get_max_cache_size());
    };

    /// Select the cache from the LRU queue held by query for expulsion.
    for (auto iter = query_context->queue().begin(); iter != query_context->queue().end(); iter++) {
        if (!is_overflow()) {
            break;
        }

        auto* cell = get_cell(iter->hash, iter->offset, cache_lock);

        if (!cell) {
            /// The cache corresponding to this record may be swapped out by
            /// other queries, so it has become invalid.
            ghost.push_back(iter);
            ghost_remove_size += iter->size;
        } else {
            size_t cell_size = cell->size();
            DCHECK(iter->size == cell_size);

            if (cell->releasable()) {
                auto& file_block = cell->file_block;

                std::lock_guard block_lock(file_block->_mutex);
                DCHECK(file_block->_download_state == FileBlock::State::DOWNLOADED);
                to_evict.push_back(cell);
                removed_size += cell_size;
            }
        }
    }

    auto remove_file_block_if = [&](FileBlockCell* cell) {
        FileBlockSPtr file_block = cell->file_block;
        if (file_block) {
            query_context->remove(file_block->get_hash_value(), file_block->offset(), cache_lock);
            std::lock_guard block_lock(file_block->_mutex);
            remove(file_block, cache_lock, block_lock);
        }
    };

    for (auto& iter : ghost) {
        query_context->remove(iter->hash, iter->offset, cache_lock);
    }

    std::for_each(to_evict.begin(), to_evict.end(), remove_file_block_if);

    if (is_overflow() &&
        !try_reserve_from_other_queue(context.cache_type, size, cur_time, cache_lock)) {
        return false;
    }
    query_context->reserve(hash, offset, size, cache_lock);
    return true;
}

void BlockFileCache::try_evict_in_advance(size_t size, std::lock_guard<std::mutex>& cache_lock) {
    UInt128Wrapper hash = UInt128Wrapper();
    size_t offset = 0;
    CacheContext context;
    /* we pick NORMAL and TTL cache to evict in advance
     * we reserve for them but won't acutually give space to them
     * on the contrary, NORMAL and TTL may sacrifice by LRU evicting themselves
     * other cache types cannot be exempted because we will evict what they have stolen before LRU evicting
     * in summary: all cache types will shrink somewhat, and NORMAL and TTL shrink the most, to make sure the cache is not full
     */
    context.cache_type = FileCacheType::NORMAL;
    try_reserve_for_lru(hash, nullptr, context, offset, size, cache_lock, true);
    context.cache_type = FileCacheType::TTL;
    try_reserve_for_lru(hash, nullptr, context, offset, size, cache_lock, true);
}

bool BlockFileCache::remove_if_ttl_file_blocks(const UInt128Wrapper& file_key, bool remove_directly,
                                               std::lock_guard<std::mutex>& cache_lock, bool sync) {
    auto& ttl_queue = get_queue(FileCacheType::TTL);
    if (auto iter = _key_to_time.find(file_key);
        _key_to_time.find(file_key) != _key_to_time.end()) {
        if (!remove_directly) {
            auto it = _files.find(file_key);
            if (it != _files.end()) {
                for (auto& [_, cell] : it->second) {
                    if (cell.file_block->cache_type() != FileCacheType::TTL) {
                        continue;
                    }
                    Status st = cell.file_block->update_expiration_time(0);
                    if (!st.ok()) {
                        LOG_WARNING("Failed to update expiration time to 0").error(st);
                    }

                    if (cell.file_block->cache_type() == FileCacheType::NORMAL) continue;
                    st = cell.file_block->change_cache_type_between_ttl_and_others(
                            FileCacheType::NORMAL);
                    if (st.ok()) {
                        if (cell.queue_iterator) {
                            ttl_queue.remove(cell.queue_iterator.value(), cache_lock);
                            _lru_recorder->record_queue_event(
                                    FileCacheType::TTL, CacheLRULogType::REMOVE,
                                    cell.file_block->get_hash_value(), cell.file_block->offset(),
                                    cell.size());
                        }
                        auto& queue = get_queue(FileCacheType::NORMAL);
                        cell.queue_iterator = queue.add(
                                cell.file_block->get_hash_value(), cell.file_block->offset(),
                                cell.file_block->range().size(), cache_lock);
                        _lru_recorder->record_queue_event(FileCacheType::NORMAL,
                                                          CacheLRULogType::ADD,
                                                          cell.file_block->get_hash_value(),
                                                          cell.file_block->offset(), cell.size());
                    } else {
                        LOG_WARNING("Failed to change cache type to normal").error(st);
                    }
                }
            }
        } else {
            std::vector<FileBlockCell*> to_remove;
            auto it = _files.find(file_key);
            if (it != _files.end()) {
                for (auto& [_, cell] : it->second) {
                    if (cell.releasable()) {
                        to_remove.push_back(&cell);
                    } else {
                        cell.file_block->set_deleting();
                    }
                }
            }
            std::for_each(to_remove.begin(), to_remove.end(), [&](FileBlockCell* cell) {
                FileBlockSPtr file_block = cell->file_block;
                std::lock_guard block_lock(file_block->_mutex);
                remove(file_block, cache_lock, block_lock, sync);
            });
        }
        // remove from _time_to_key
        // the param hash maybe be passed by _time_to_key, if removed it, cannot use it anymore
        auto _time_to_key_iter = _time_to_key.equal_range(iter->second);
        while (_time_to_key_iter.first != _time_to_key_iter.second) {
            if (_time_to_key_iter.first->second == file_key) {
                _time_to_key_iter.first = _time_to_key.erase(_time_to_key_iter.first);
                break;
            }
            _time_to_key_iter.first++;
        }
        _key_to_time.erase(iter);
        return true;
    }
    return false;
}

// remove specific cache synchronously, for critical operations
// if in use, cache meta will be deleted after use and the block file is then deleted asynchronously
void BlockFileCache::remove_if_cached(const UInt128Wrapper& file_key) {
    SCOPED_CACHE_LOCK(_mutex, this);
    bool is_ttl_file = remove_if_ttl_file_blocks(file_key, true, cache_lock, true);
    if (!is_ttl_file) {
        auto iter = _files.find(file_key);
        std::vector<FileBlockCell*> to_remove;
        if (iter != _files.end()) {
            for (auto& [_, cell] : iter->second) {
                if (cell.releasable()) {
                    to_remove.push_back(&cell);
                } else {
                    cell.file_block->set_deleting();
                }
            }
        }
        remove_file_blocks(to_remove, cache_lock, true);
    }
}

// the async version of remove_if_cached, for background operations
// cache meta is deleted synchronously if not in use, and the block file is deleted asynchronously
// if in use, cache meta will be deleted after use and the block file is then deleted asynchronously
void BlockFileCache::remove_if_cached_async(const UInt128Wrapper& file_key) {
    SCOPED_CACHE_LOCK(_mutex, this);
    bool is_ttl_file = remove_if_ttl_file_blocks(file_key, true, cache_lock, /*sync*/ false);
    if (!is_ttl_file) {
        auto iter = _files.find(file_key);
        std::vector<FileBlockCell*> to_remove;
        if (iter != _files.end()) {
            for (auto& [_, cell] : iter->second) {
                *_gc_evict_bytes_metrics << cell.size();
                *_gc_evict_count_metrics << 1;
                if (cell.releasable()) {
                    to_remove.push_back(&cell);
                } else {
                    cell.file_block->set_deleting();
                }
            }
        }
        remove_file_blocks(to_remove, cache_lock, false);
    }
}

std::vector<FileCacheType> BlockFileCache::get_other_cache_type_without_ttl(
        FileCacheType cur_cache_type) {
    switch (cur_cache_type) {
    case FileCacheType::TTL:
        return {FileCacheType::DISPOSABLE, FileCacheType::NORMAL, FileCacheType::INDEX};
    case FileCacheType::INDEX:
        return {FileCacheType::DISPOSABLE, FileCacheType::NORMAL};
    case FileCacheType::NORMAL:
        return {FileCacheType::DISPOSABLE, FileCacheType::INDEX};
    case FileCacheType::DISPOSABLE:
        return {FileCacheType::NORMAL, FileCacheType::INDEX};
    default:
        return {};
    }
    return {};
}

std::vector<FileCacheType> BlockFileCache::get_other_cache_type(FileCacheType cur_cache_type) {
    switch (cur_cache_type) {
    case FileCacheType::TTL:
        return {FileCacheType::DISPOSABLE, FileCacheType::NORMAL, FileCacheType::INDEX};
    case FileCacheType::INDEX:
        return {FileCacheType::DISPOSABLE, FileCacheType::NORMAL, FileCacheType::TTL};
    case FileCacheType::NORMAL:
        return {FileCacheType::DISPOSABLE, FileCacheType::INDEX, FileCacheType::TTL};
    case FileCacheType::DISPOSABLE:
        return {FileCacheType::NORMAL, FileCacheType::INDEX, FileCacheType::TTL};
    default:
        return {};
    }
    return {};
}

void BlockFileCache::reset_range(const UInt128Wrapper& hash, size_t offset, size_t old_size,
                                 size_t new_size, std::lock_guard<std::mutex>& cache_lock) {
    DCHECK(_files.find(hash) != _files.end() &&
           _files.find(hash)->second.find(offset) != _files.find(hash)->second.end());
    FileBlockCell* cell = get_cell(hash, offset, cache_lock);
    DCHECK(cell != nullptr);
    if (cell->queue_iterator) {
        auto& queue = get_queue(cell->file_block->cache_type());
        DCHECK(queue.contains(hash, offset, cache_lock));
        auto iter = queue.get(hash, offset, cache_lock);
        iter->size = new_size;
        queue.cache_size -= old_size;
        queue.cache_size += new_size;
    }
    _cur_cache_size -= old_size;
    _cur_cache_size += new_size;
}

bool BlockFileCache::try_reserve_from_other_queue_by_time_interval(
        FileCacheType cur_type, std::vector<FileCacheType> other_cache_types, size_t size,
        int64_t cur_time, std::lock_guard<std::mutex>& cache_lock, bool evict_in_advance) {
    size_t removed_size = 0;
    size_t cur_cache_size = _cur_cache_size;
    std::vector<FileBlockCell*> to_evict;
    for (FileCacheType cache_type : other_cache_types) {
        auto& queue = get_queue(cache_type);
        size_t remove_size_per_type = 0;
        for (const auto& [entry_key, entry_offset, entry_size] : queue) {
            if (!is_overflow(removed_size, size, cur_cache_size, evict_in_advance)) {
                break;
            }
            auto* cell = get_cell(entry_key, entry_offset, cache_lock);
            DCHECK(cell) << "Cache became inconsistent. UInt128Wrapper: " << entry_key.to_string()
                         << ", offset: " << entry_offset;

            size_t cell_size = cell->size();
            DCHECK(entry_size == cell_size);

            if (cell->atime == 0 ? true : cell->atime + queue.get_hot_data_interval() > cur_time) {
                break;
            }

            if (cell->releasable()) {
                auto& file_block = cell->file_block;
                std::lock_guard block_lock(file_block->_mutex);
                DCHECK(file_block->_download_state == FileBlock::State::DOWNLOADED);
                to_evict.push_back(cell);
                removed_size += cell_size;
                remove_size_per_type += cell_size;
            }
        }
        *(_evict_by_time_metrics_matrix[cache_type][cur_type]) << remove_size_per_type;
    }
    bool is_sync_removal = !evict_in_advance;
    remove_file_blocks(to_evict, cache_lock, is_sync_removal);

    return !is_overflow(removed_size, size, cur_cache_size, evict_in_advance);
}

bool BlockFileCache::is_overflow(size_t removed_size, size_t need_size, size_t cur_cache_size,
                                 bool evict_in_advance) const {
    bool ret = false;
    if (evict_in_advance) { // we don't need to check _need_evict_cache_in_advance
        ret = (removed_size < need_size);
        return ret;
    }
    if (_disk_resource_limit_mode) {
        ret = (removed_size < need_size);
    } else {
        ret = (cur_cache_size + need_size - removed_size > _capacity);
    }
    return ret;
}

bool BlockFileCache::try_reserve_from_other_queue_by_size(
        FileCacheType cur_type, std::vector<FileCacheType> other_cache_types, size_t size,
        std::lock_guard<std::mutex>& cache_lock, bool evict_in_advance) {
    size_t removed_size = 0;
    size_t cur_cache_size = _cur_cache_size;
    std::vector<FileBlockCell*> to_evict;
    // we follow the privilege defined in get_other_cache_types to evict
    for (FileCacheType cache_type : other_cache_types) {
        auto& queue = get_queue(cache_type);

        // we will not drain each of them to the bottom -- i.e., we only
        // evict what they have stolen.
        size_t cur_queue_size = queue.get_capacity(cache_lock);
        size_t cur_queue_max_size = queue.get_max_size();
        if (cur_queue_size <= cur_queue_max_size) {
            continue;
        }
        size_t cur_removed_size = 0;
        find_evict_candidates(queue, size, cur_cache_size, removed_size, to_evict, cache_lock,
                              cur_removed_size, evict_in_advance);
        *(_evict_by_size_metrics_matrix[cache_type][cur_type]) << cur_removed_size;
    }
    bool is_sync_removal = !evict_in_advance;
    remove_file_blocks(to_evict, cache_lock, is_sync_removal);
    return !is_overflow(removed_size, size, cur_cache_size, evict_in_advance);
}

bool BlockFileCache::try_reserve_from_other_queue(FileCacheType cur_cache_type, size_t size,
                                                  int64_t cur_time,
                                                  std::lock_guard<std::mutex>& cache_lock,
                                                  bool evict_in_advance) {
    // currently, TTL cache is not considered as a candidate
    auto other_cache_types = get_other_cache_type_without_ttl(cur_cache_type);
    bool reserve_success = try_reserve_from_other_queue_by_time_interval(
            cur_cache_type, other_cache_types, size, cur_time, cache_lock, evict_in_advance);
    if (reserve_success || !config::file_cache_enable_evict_from_other_queue_by_size) {
        return reserve_success;
    }

    other_cache_types = get_other_cache_type(cur_cache_type);
    auto& cur_queue = get_queue(cur_cache_type);
    size_t cur_queue_size = cur_queue.get_capacity(cache_lock);
    size_t cur_queue_max_size = cur_queue.get_max_size();
    // Hit the soft limit by self, cannot remove from other queues
    if (_cur_cache_size + size > _capacity && cur_queue_size + size > cur_queue_max_size) {
        return false;
    }
    return try_reserve_from_other_queue_by_size(cur_cache_type, other_cache_types, size, cache_lock,
                                                evict_in_advance);
}

bool BlockFileCache::try_reserve_for_lru(const UInt128Wrapper& hash,
                                         QueryFileCacheContextPtr query_context,
                                         const CacheContext& context, size_t offset, size_t size,
                                         std::lock_guard<std::mutex>& cache_lock,
                                         bool evict_in_advance) {
    int64_t cur_time = std::chrono::duration_cast<std::chrono::seconds>(
                               std::chrono::steady_clock::now().time_since_epoch())
                               .count();
    if (!try_reserve_from_other_queue(context.cache_type, size, cur_time, cache_lock,
                                      evict_in_advance)) {
        auto& queue = get_queue(context.cache_type);
        size_t removed_size = 0;
        size_t cur_cache_size = _cur_cache_size;

        std::vector<FileBlockCell*> to_evict;
        size_t cur_removed_size = 0;
        find_evict_candidates(queue, size, cur_cache_size, removed_size, to_evict, cache_lock,
                              cur_removed_size, evict_in_advance);
        bool is_sync_removal = !evict_in_advance;
        remove_file_blocks(to_evict, cache_lock, is_sync_removal);
        *(_evict_by_self_lru_metrics_matrix[context.cache_type]) << cur_removed_size;

        if (is_overflow(removed_size, size, cur_cache_size, evict_in_advance)) {
            return false;
        }
    }

    if (query_context) {
        query_context->reserve(hash, offset, size, cache_lock);
    }
    return true;
}

template <class T, class U>
    requires IsXLock<T> && IsXLock<U>
void BlockFileCache::remove(FileBlockSPtr file_block, T& cache_lock, U& block_lock, bool sync) {
    auto hash = file_block->get_hash_value();
    auto offset = file_block->offset();
    auto type = file_block->cache_type();
    auto expiration_time = file_block->expiration_time();
    auto* cell = get_cell(hash, offset, cache_lock);
    DCHECK(cell);
    DCHECK(cell->queue_iterator);
    if (cell->queue_iterator) {
        auto& queue = get_queue(file_block->cache_type());
        queue.remove(*cell->queue_iterator, cache_lock);
        _lru_recorder->record_queue_event(file_block->cache_type(), CacheLRULogType::REMOVE,
                                          cell->file_block->get_hash_value(),
                                          cell->file_block->offset(), cell->size());
    }
    *_queue_evict_size_metrics[static_cast<int>(file_block->cache_type())]
            << file_block->range().size();
    *_total_evict_size_metrics << file_block->range().size();
    if (file_block->state_unlock(block_lock) == FileBlock::State::DOWNLOADED) {
        FileCacheKey key;
        key.hash = hash;
        key.offset = offset;
        key.meta.type = type;
        key.meta.expiration_time = expiration_time;
        if (sync) {
            int64_t duration_ns = 0;
            Status st;
            {
                SCOPED_RAW_TIMER(&duration_ns);
                st = _storage->remove(key);
            }
            *_storage_sync_remove_latency_us << (duration_ns / 1000);
            if (!st.ok()) {
                LOG_WARNING("").error(st);
            }
        } else {
            // the file will be deleted in the bottom half
            // so there will be a window that the file is not in the cache but still in the storage
            // but it's ok, because the rowset is stale already
            bool ret = _recycle_keys.enqueue(key);
            if (ret) [[likely]] {
                *_recycle_keys_length_recorder << _recycle_keys.size_approx();
            } else {
                LOG_WARNING("Failed to push recycle key to queue, do it synchronously");
                int64_t duration_ns = 0;
                Status st;
                {
                    SCOPED_RAW_TIMER(&duration_ns);
                    st = _storage->remove(key);
                }
                *_storage_retry_sync_remove_latency_us << (duration_ns / 1000);
                if (!st.ok()) {
                    LOG_WARNING("").error(st);
                }
            }
        }
    } else if (file_block->state_unlock(block_lock) == FileBlock::State::DOWNLOADING) {
        file_block->set_deleting();
        return;
    }
    _cur_cache_size -= file_block->range().size();
    if (FileCacheType::TTL == type) {
        _cur_ttl_size -= file_block->range().size();
    }
    auto it = _files.find(hash);
    if (it != _files.end()) {
        it->second.erase(file_block->offset());
        if (it->second.empty()) {
            _files.erase(hash);
        }
    }
    *_num_removed_blocks << 1;
}

size_t BlockFileCache::get_used_cache_size(FileCacheType cache_type) const {
    SCOPED_CACHE_LOCK(_mutex, this);
    return get_used_cache_size_unlocked(cache_type, cache_lock);
}

size_t BlockFileCache::get_used_cache_size_unlocked(FileCacheType cache_type,
                                                    std::lock_guard<std::mutex>& cache_lock) const {
    return get_queue(cache_type).get_capacity(cache_lock);
}

size_t BlockFileCache::get_available_cache_size(FileCacheType cache_type) const {
    SCOPED_CACHE_LOCK(_mutex, this);
    return get_available_cache_size_unlocked(cache_type, cache_lock);
}

size_t BlockFileCache::get_available_cache_size_unlocked(
        FileCacheType cache_type, std::lock_guard<std::mutex>& cache_lock) const {
    return get_queue(cache_type).get_max_element_size() -
           get_used_cache_size_unlocked(cache_type, cache_lock);
}

size_t BlockFileCache::get_file_blocks_num(FileCacheType cache_type) const {
    SCOPED_CACHE_LOCK(_mutex, this);
    return get_file_blocks_num_unlocked(cache_type, cache_lock);
}

size_t BlockFileCache::get_file_blocks_num_unlocked(FileCacheType cache_type,
                                                    std::lock_guard<std::mutex>& cache_lock) const {
    return get_queue(cache_type).get_elements_num(cache_lock);
}

BlockFileCache::FileBlockCell::FileBlockCell(FileBlockSPtr file_block,
                                             std::lock_guard<std::mutex>& cache_lock)
        : file_block(file_block) {
    /**
     * Cell can be created with either DOWNLOADED or EMPTY file block's state.
     * File block acquires DOWNLOADING state and creates LRUQueue iterator on first
     * successful getOrSetDownaloder call.
     */

    switch (file_block->_download_state) {
    case FileBlock::State::DOWNLOADED:
    case FileBlock::State::EMPTY:
    case FileBlock::State::SKIP_CACHE: {
        break;
    }
    default:
        DCHECK(false) << "Can create cell with either EMPTY, DOWNLOADED, SKIP_CACHE state, got: "
                      << FileBlock::state_to_string(file_block->_download_state);
    }
    if (file_block->cache_type() == FileCacheType::TTL) {
        update_atime();
    }
}

LRUQueue::Iterator LRUQueue::add(const UInt128Wrapper& hash, size_t offset, size_t size,
                                 std::lock_guard<std::mutex>& /* cache_lock */) {
    cache_size += size;
    auto iter = queue.insert(queue.end(), FileKeyAndOffset(hash, offset, size));
    map.insert(std::make_pair(std::make_pair(hash, offset), iter));
    return iter;
}

void LRUQueue::remove_all(std::lock_guard<std::mutex>& /* cache_lock */) {
    queue.clear();
    map.clear();
    cache_size = 0;
}

void LRUQueue::move_to_end(Iterator queue_it, std::lock_guard<std::mutex>& /* cache_lock */) {
    queue.splice(queue.end(), queue, queue_it);
}
bool LRUQueue::contains(const UInt128Wrapper& hash, size_t offset,
                        std::lock_guard<std::mutex>& /* cache_lock */) const {
    return map.find(std::make_pair(hash, offset)) != map.end();
}

LRUQueue::Iterator LRUQueue::get(const UInt128Wrapper& hash, size_t offset,
                                 std::lock_guard<std::mutex>& /* cache_lock */) const {
    auto itr = map.find(std::make_pair(hash, offset));
    if (itr != map.end()) {
        return itr->second;
    }
    return std::list<FileKeyAndOffset>::iterator();
}

std::string LRUQueue::to_string(std::lock_guard<std::mutex>& /* cache_lock */) const {
    std::string result;
    for (const auto& [hash, offset, size] : queue) {
        if (!result.empty()) {
            result += ", ";
        }
        result += fmt::format("{}: [{}, {}]", hash.to_string(), offset, offset + size - 1);
    }
    return result;
}

size_t LRUQueue::levenshtein_distance_from(LRUQueue& base,
                                           std::lock_guard<std::mutex>& cache_lock) {
    std::list<FileKeyAndOffset> target_queue = this->queue;
    std::list<FileKeyAndOffset> base_queue = base.queue;
    std::vector<FileKeyAndOffset> vec1(target_queue.begin(), target_queue.end());
    std::vector<FileKeyAndOffset> vec2(base_queue.begin(), base_queue.end());

    size_t m = vec1.size();
    size_t n = vec2.size();

    // Create a 2D vector (matrix) to store the Levenshtein distances
    // dp[i][j] will hold the distance between the first i elements of vec1 and the first j elements of vec2
    std::vector<std::vector<size_t>> dp(m + 1, std::vector<size_t>(n + 1, 0));

    // Initialize the first row and column of the matrix
    // The distance between an empty list and a list of length k is k (all insertions or deletions)
    for (size_t i = 0; i <= m; ++i) {
        dp[i][0] = i;
    }
    for (size_t j = 0; j <= n; ++j) {
        dp[0][j] = j;
    }

    // Fill the matrix using dynamic programming
    for (size_t i = 1; i <= m; ++i) {
        for (size_t j = 1; j <= n; ++j) {
            // Check if the current elements of both vectors are equal
            size_t cost = (vec1[i - 1].hash == vec2[j - 1].hash &&
                           vec1[i - 1].offset == vec2[j - 1].offset)
                                  ? 0
                                  : 1;
            // Calculate the minimum cost of three possible operations:
            // 1. Insertion: dp[i][j-1] + 1
            // 2. Deletion: dp[i-1][j] + 1
            // 3. Substitution: dp[i-1][j-1] + cost (0 if elements are equal, 1 if not)
            dp[i][j] = std::min({dp[i - 1][j] + 1, dp[i][j - 1] + 1, dp[i - 1][j - 1] + cost});
        }
    }
    // The bottom-right cell of the matrix contains the Levenshtein distance
    return dp[m][n];
}

std::string BlockFileCache::dump_structure(const UInt128Wrapper& hash) {
    SCOPED_CACHE_LOCK(_mutex, this);
    return dump_structure_unlocked(hash, cache_lock);
}

std::string BlockFileCache::dump_structure_unlocked(const UInt128Wrapper& hash,
                                                    std::lock_guard<std::mutex>&) {
    std::stringstream result;
    auto it = _files.find(hash);
    if (it == _files.end()) {
        return std::string("");
    }
    const auto& cells_by_offset = it->second;

    for (const auto& [_, cell] : cells_by_offset) {
        result << cell.file_block->get_info_for_log() << " "
               << cache_type_to_string(cell.file_block->cache_type()) << "\n";
    }

    return result.str();
}

std::string BlockFileCache::dump_single_cache_type(const UInt128Wrapper& hash, size_t offset) {
    SCOPED_CACHE_LOCK(_mutex, this);
    return dump_single_cache_type_unlocked(hash, offset, cache_lock);
}

std::string BlockFileCache::dump_single_cache_type_unlocked(const UInt128Wrapper& hash,
                                                            size_t offset,
                                                            std::lock_guard<std::mutex>&) {
    std::stringstream result;
    auto it = _files.find(hash);
    if (it == _files.end()) {
        return std::string("");
    }
    const auto& cells_by_offset = it->second;
    const auto& cell = cells_by_offset.find(offset);

    return cache_type_to_string(cell->second.file_block->cache_type());
}

void BlockFileCache::change_cache_type(const UInt128Wrapper& hash, size_t offset,
                                       FileCacheType new_type,
                                       std::lock_guard<std::mutex>& cache_lock) {
    if (auto iter = _files.find(hash); iter != _files.end()) {
        auto& file_blocks = iter->second;
        if (auto cell_it = file_blocks.find(offset); cell_it != file_blocks.end()) {
            FileBlockCell& cell = cell_it->second;
            auto& cur_queue = get_queue(cell.file_block->cache_type());
            DCHECK(cell.queue_iterator.has_value());
            cur_queue.remove(*cell.queue_iterator, cache_lock);
            _lru_recorder->record_queue_event(
                    cell.file_block->cache_type(), CacheLRULogType::REMOVE,
                    cell.file_block->get_hash_value(), cell.file_block->offset(), cell.size());
            auto& new_queue = get_queue(new_type);
            cell.queue_iterator =
                    new_queue.add(hash, offset, cell.file_block->range().size(), cache_lock);
            _lru_recorder->record_queue_event(new_type, CacheLRULogType::ADD,
                                              cell.file_block->get_hash_value(),
                                              cell.file_block->offset(), cell.size());
        }
    }
}

// @brief: get a path's disk capacity used percent, inode used percent
// @param: path
// @param: percent.first disk used percent, percent.second inode used percent
int disk_used_percentage(const std::string& path, std::pair<int, int>* percent) {
    struct statfs stat;
    int ret = statfs(path.c_str(), &stat);
    if (ret != 0) {
        return ret;
    }
    // https://github.com/coreutils/coreutils/blob/master/src/df.c#L1195
    // v->used = stat.f_blocks - stat.f_bfree
    // nonroot_total = stat.f_blocks - stat.f_bfree + stat.f_bavail
    uintmax_t u100 = (stat.f_blocks - stat.f_bfree) * 100;
    uintmax_t nonroot_total = stat.f_blocks - stat.f_bfree + stat.f_bavail;
    int capacity_percentage = int(u100 / nonroot_total + (u100 % nonroot_total != 0));

    unsigned long long inode_free = stat.f_ffree;
    unsigned long long inode_total = stat.f_files;
    int inode_percentage = cast_set<int>(inode_free * 100 / inode_total);
    percent->first = capacity_percentage;
    percent->second = 100 - inode_percentage;

    // Add sync point for testing
    TEST_SYNC_POINT_CALLBACK("BlockFileCache::disk_used_percentage:1", percent);

    return 0;
}

std::string BlockFileCache::reset_capacity(size_t new_capacity) {
    using namespace std::chrono;
    int64_t space_released = 0;
    size_t old_capacity = 0;
    std::stringstream ss;
    ss << "finish reset_capacity, path=" << _cache_base_path;
    auto adjust_start_time = steady_clock::time_point();
    {
        SCOPED_CACHE_LOCK(_mutex, this);
        if (new_capacity < _capacity && new_capacity < _cur_cache_size) {
            int64_t need_remove_size = _cur_cache_size - new_capacity;
            auto remove_blocks = [&](LRUQueue& queue) -> int64_t {
                int64_t queue_released = 0;
                std::vector<FileBlockCell*> to_evict;
                for (const auto& [entry_key, entry_offset, entry_size] : queue) {
                    if (need_remove_size <= 0) {
                        break;
                    }
                    need_remove_size -= entry_size;
                    space_released += entry_size;
                    queue_released += entry_size;
                    auto* cell = get_cell(entry_key, entry_offset, cache_lock);
                    if (!cell->releasable()) {
                        cell->file_block->set_deleting();
                        continue;
                    }
                    to_evict.push_back(cell);
                }
                for (auto& cell : to_evict) {
                    FileBlockSPtr file_block = cell->file_block;
                    std::lock_guard block_lock(file_block->_mutex);
                    remove(file_block, cache_lock, block_lock);
                }
                return queue_released;
            };
            int64_t queue_released = remove_blocks(_disposable_queue);
            ss << " disposable_queue released " << queue_released;
            queue_released = remove_blocks(_normal_queue);
            ss << " normal_queue released " << queue_released;
            queue_released = remove_blocks(_index_queue);
            ss << " index_queue released " << queue_released;
            queue_released = remove_blocks(_ttl_queue);
            ss << " ttl_queue released " << queue_released;

            _disk_resource_limit_mode = true;
            _disk_limit_mode_metrics->set_value(1);
            ss << " total_space_released=" << space_released;
        }
        old_capacity = _capacity;
        _capacity = new_capacity;
        _cache_capacity_metrics->set_value(_capacity);
    }
    auto use_time = duration_cast<milliseconds>(steady_clock::time_point() - adjust_start_time);
    LOG(INFO) << "Finish tag deleted block. path=" << _cache_base_path
              << " use_time=" << cast_set<int64_t>(use_time.count());
    ss << " old_capacity=" << old_capacity << " new_capacity=" << new_capacity;
    LOG(INFO) << ss.str();
    return ss.str();
}

void BlockFileCache::check_disk_resource_limit() {
    if (_storage->get_type() != FileCacheStorageType::DISK) {
        return;
    }
    if (_capacity > _cur_cache_size) {
        _disk_resource_limit_mode = false;
        _disk_limit_mode_metrics->set_value(0);
    }
    std::pair<int, int> percent;
    int ret = disk_used_percentage(_cache_base_path, &percent);
    if (ret != 0) {
        LOG_ERROR("").tag("file cache path", _cache_base_path).tag("error", strerror(errno));
        return;
    }
    auto [space_percentage, inode_percentage] = percent;
    auto is_insufficient = [](const int& percentage) {
        return percentage >= config::file_cache_enter_disk_resource_limit_mode_percent;
    };
    DCHECK_GE(space_percentage, 0);
    DCHECK_LE(space_percentage, 100);
    DCHECK_GE(inode_percentage, 0);
    DCHECK_LE(inode_percentage, 100);
    // ATTN: due to that can be changed dynamically, set it to default value if it's invalid
    // FIXME: reject with config validator
    if (config::file_cache_enter_disk_resource_limit_mode_percent <
        config::file_cache_exit_disk_resource_limit_mode_percent) {
        LOG_WARNING("config error, set to default value")
                .tag("enter", config::file_cache_enter_disk_resource_limit_mode_percent)
                .tag("exit", config::file_cache_exit_disk_resource_limit_mode_percent);
        config::file_cache_enter_disk_resource_limit_mode_percent = 88;
        config::file_cache_exit_disk_resource_limit_mode_percent = 80;
    }
    if (is_insufficient(space_percentage) || is_insufficient(inode_percentage)) {
        _disk_resource_limit_mode = true;
        _disk_limit_mode_metrics->set_value(1);
    } else if (_disk_resource_limit_mode &&
               (space_percentage < config::file_cache_exit_disk_resource_limit_mode_percent) &&
               (inode_percentage < config::file_cache_exit_disk_resource_limit_mode_percent)) {
        _disk_resource_limit_mode = false;
        _disk_limit_mode_metrics->set_value(0);
    }
    if (_disk_resource_limit_mode) {
        LOG(WARNING) << "file_cache=" << get_base_path() << " space_percent=" << space_percentage
                     << " inode_percent=" << inode_percentage
                     << " is_space_insufficient=" << is_insufficient(space_percentage)
                     << " is_inode_insufficient=" << is_insufficient(inode_percentage)
                     << " mode run in resource limit";
    }
}

void BlockFileCache::check_need_evict_cache_in_advance() {
    if (_storage->get_type() != FileCacheStorageType::DISK) {
        return;
    }

    std::pair<int, int> percent;
    int ret = disk_used_percentage(_cache_base_path, &percent);
    if (ret != 0) {
        LOG_ERROR("").tag("file cache path", _cache_base_path).tag("error", strerror(errno));
        return;
    }
    auto [space_percentage, inode_percentage] = percent;
    int size_percentage = static_cast<int>(_cur_cache_size * 100 / _capacity);
    auto is_insufficient = [](const int& percentage) {
        return percentage >= config::file_cache_enter_need_evict_cache_in_advance_percent;
    };
    DCHECK_GE(space_percentage, 0);
    DCHECK_LE(space_percentage, 100);
    DCHECK_GE(inode_percentage, 0);
    DCHECK_LE(inode_percentage, 100);
    // ATTN: due to that can be changed dynamically, set it to default value if it's invalid
    // FIXME: reject with config validator
    if (config::file_cache_enter_need_evict_cache_in_advance_percent <=
        config::file_cache_exit_need_evict_cache_in_advance_percent) {
        LOG_WARNING("config error, set to default value")
                .tag("enter", config::file_cache_enter_need_evict_cache_in_advance_percent)
                .tag("exit", config::file_cache_exit_need_evict_cache_in_advance_percent);
        config::file_cache_enter_need_evict_cache_in_advance_percent = 78;
        config::file_cache_exit_need_evict_cache_in_advance_percent = 75;
    }
    if (is_insufficient(space_percentage) || is_insufficient(inode_percentage) ||
        is_insufficient(size_percentage)) {
        _need_evict_cache_in_advance = true;
        _need_evict_cache_in_advance_metrics->set_value(1);
    } else if (_need_evict_cache_in_advance &&
               (space_percentage < config::file_cache_exit_need_evict_cache_in_advance_percent) &&
               (inode_percentage < config::file_cache_exit_need_evict_cache_in_advance_percent) &&
               (size_percentage < config::file_cache_exit_need_evict_cache_in_advance_percent)) {
        _need_evict_cache_in_advance = false;
        _need_evict_cache_in_advance_metrics->set_value(0);
    }
    if (_need_evict_cache_in_advance) {
        LOG(WARNING) << "file_cache=" << get_base_path() << " space_percent=" << space_percentage
                     << " inode_percent=" << inode_percentage << " size_percent=" << size_percentage
                     << " is_space_insufficient=" << is_insufficient(space_percentage)
                     << " is_inode_insufficient=" << is_insufficient(inode_percentage)
                     << " is_size_insufficient=" << is_insufficient(size_percentage)
                     << " need evict cache in advance";
    }
}

void BlockFileCache::run_background_monitor() {
    Thread::set_self_name("run_background_monitor");
    while (!_close) {
        int64_t interval_ms = config::file_cache_background_monitor_interval_ms;
        TEST_SYNC_POINT_CALLBACK("BlockFileCache::set_sleep_time", &interval_ms);
        check_disk_resource_limit();
        if (config::enable_evict_file_cache_in_advance) {
            check_need_evict_cache_in_advance();
        } else {
            _need_evict_cache_in_advance = false;
            _need_evict_cache_in_advance_metrics->set_value(0);
        }

        {
            std::unique_lock close_lock(_close_mtx);
            _close_cv.wait_for(close_lock, std::chrono::milliseconds(interval_ms));
            if (_close) {
                break;
            }
        }
        // report
        {
            SCOPED_CACHE_LOCK(_mutex, this);
            _cur_cache_size_metrics->set_value(_cur_cache_size);
            _cur_ttl_cache_size_metrics->set_value(_cur_cache_size -
                                                   _index_queue.get_capacity(cache_lock) -
                                                   _normal_queue.get_capacity(cache_lock) -
                                                   _disposable_queue.get_capacity(cache_lock));
            _cur_ttl_cache_lru_queue_cache_size_metrics->set_value(
                    _ttl_queue.get_capacity(cache_lock));
            _cur_ttl_cache_lru_queue_element_count_metrics->set_value(
                    _ttl_queue.get_elements_num(cache_lock));
            _cur_normal_queue_cache_size_metrics->set_value(_normal_queue.get_capacity(cache_lock));
            _cur_normal_queue_element_count_metrics->set_value(
                    _normal_queue.get_elements_num(cache_lock));
            _cur_index_queue_cache_size_metrics->set_value(_index_queue.get_capacity(cache_lock));
            _cur_index_queue_element_count_metrics->set_value(
                    _index_queue.get_elements_num(cache_lock));
            _cur_disposable_queue_cache_size_metrics->set_value(
                    _disposable_queue.get_capacity(cache_lock));
            _cur_disposable_queue_element_count_metrics->set_value(
                    _disposable_queue.get_elements_num(cache_lock));

            if (_num_read_blocks->get_value() > 0) {
                _hit_ratio->set_value((double)_num_hit_blocks->get_value() /
                                      (double)_num_read_blocks->get_value());
            }
            if (_num_read_blocks_5m->get_value() > 0) {
                _hit_ratio_5m->set_value((double)_num_hit_blocks_5m->get_value() /
                                         (double)_num_read_blocks_5m->get_value());
            }
            if (_num_read_blocks_1h->get_value() > 0) {
                _hit_ratio_1h->set_value((double)_num_hit_blocks_1h->get_value() /
                                         (double)_num_read_blocks_1h->get_value());
            }
        }
    }
}

void BlockFileCache::run_background_ttl_gc() {
    Thread::set_self_name("run_background_ttl_gc");
    while (!_close) {
        int64_t interval_ms = config::file_cache_background_ttl_gc_interval_ms;
        int64_t batch_size = config::file_cache_background_ttl_gc_batch;
        TEST_SYNC_POINT_CALLBACK("BlockFileCache::set_sleep_time", &interval_ms);
        {
            std::unique_lock close_lock(_close_mtx);
            _close_cv.wait_for(close_lock, std::chrono::milliseconds(interval_ms));
            if (_close) {
                break;
            }
        }
        int64_t duration_ns = 0;
        {
            int64_t cur_time = UnixSeconds();
            int64_t count = 0;
            SCOPED_CACHE_LOCK(_mutex, this);
            SCOPED_RAW_TIMER(&duration_ns);
            while (!_time_to_key.empty()) {
                auto begin = _time_to_key.begin();
                if (cur_time < begin->first || count > batch_size) {
                    break;
                }
                remove_if_ttl_file_blocks(begin->second, false, cache_lock, false);
                ++count;
            }
        }
        *_ttl_gc_latency_us << (duration_ns / 1000);
    }
}

void BlockFileCache::run_background_gc() {
    Thread::set_self_name("run_background_gc");
    FileCacheKey key;
    size_t batch_count = 0;
    while (!_close) {
        int64_t interval_ms = config::file_cache_background_gc_interval_ms;
        size_t batch_limit = config::file_cache_remove_block_qps_limit * interval_ms / 1000;
        {
            std::unique_lock close_lock(_close_mtx);
            _close_cv.wait_for(close_lock, std::chrono::milliseconds(interval_ms));
            if (_close) {
                break;
            }
        }

        while (batch_count < batch_limit && _recycle_keys.try_dequeue(key)) {
            int64_t duration_ns = 0;
            Status st;
            {
                SCOPED_RAW_TIMER(&duration_ns);
                st = _storage->remove(key);
            }
            *_storage_async_remove_latency_us << (duration_ns / 1000);

            if (!st.ok()) {
                LOG_WARNING("").error(st);
            }
            batch_count++;
        }
        *_recycle_keys_length_recorder << _recycle_keys.size_approx();
        batch_count = 0;
    }
}

void BlockFileCache::run_background_evict_in_advance() {
    Thread::set_self_name("run_background_evict_in_advance");
    LOG(INFO) << "Starting background evict in advance thread";
    int64_t batch = 0;
    while (!_close) {
        {
            std::unique_lock close_lock(_close_mtx);
            _close_cv.wait_for(
                    close_lock,
                    std::chrono::milliseconds(config::file_cache_evict_in_advance_interval_ms));
            if (_close) {
                LOG(INFO) << "Background evict in advance thread exiting due to cache closing";
                break;
            }
        }
        batch = config::file_cache_evict_in_advance_batch_bytes;

        // Skip if eviction not needed or too many pending recycles
        if (!_need_evict_cache_in_advance ||
            _recycle_keys.size_approx() >=
                    config::file_cache_evict_in_advance_recycle_keys_num_threshold) {
            continue;
        }

        int64_t duration_ns = 0;
        {
            SCOPED_CACHE_LOCK(_mutex, this);
            SCOPED_RAW_TIMER(&duration_ns);
            try_evict_in_advance(batch, cache_lock);
        }
        *_evict_in_advance_latency_us << (duration_ns / 1000);
    }
}

void BlockFileCache::modify_expiration_time(const UInt128Wrapper& hash,
                                            uint64_t new_expiration_time) {
    SCOPED_CACHE_LOCK(_mutex, this);
    // 1. If new_expiration_time is equal to zero
    if (new_expiration_time == 0) {
        remove_if_ttl_file_blocks(hash, false, cache_lock, false);
        return;
    }
    // 2. If the hash in ttl cache, modify its expiration time.
    if (auto iter = _key_to_time.find(hash); iter != _key_to_time.end()) {
        // remove from _time_to_key
        auto _time_to_key_iter = _time_to_key.equal_range(iter->second);
        while (_time_to_key_iter.first != _time_to_key_iter.second) {
            if (_time_to_key_iter.first->second == hash) {
                _time_to_key_iter.first = _time_to_key.erase(_time_to_key_iter.first);
                break;
            }
            _time_to_key_iter.first++;
        }
        _time_to_key.insert(std::make_pair(new_expiration_time, hash));
        iter->second = new_expiration_time;
        auto it = _files.find(hash);
        if (it != _files.end()) {
            for (auto& [_, cell] : it->second) {
                Status st = cell.file_block->update_expiration_time(new_expiration_time);
                if (!st.ok()) {
                    LOG_WARNING("Failed to modify expiration time").error(st);
                }
            }
        }

        return;
    }
    // 3. change to ttl if the blocks aren't ttl
    if (auto iter = _files.find(hash); iter != _files.end()) {
        for (auto& [_, cell] : iter->second) {
            Status st = cell.file_block->update_expiration_time(new_expiration_time);
            if (!st.ok()) {
                LOG_WARNING("").error(st);
            }

            FileCacheType origin_type = cell.file_block->cache_type();
            if (origin_type == FileCacheType::TTL) continue;
            st = cell.file_block->change_cache_type_between_ttl_and_others(FileCacheType::TTL);
            if (st.ok()) {
                auto& queue = get_queue(origin_type);
                queue.remove(cell.queue_iterator.value(), cache_lock);
                _lru_recorder->record_queue_event(origin_type, CacheLRULogType::REMOVE,
                                                  cell.file_block->get_hash_value(),
                                                  cell.file_block->offset(), cell.size());
                auto& ttl_queue = get_queue(FileCacheType::TTL);
                cell.queue_iterator = ttl_queue.add(hash, cell.file_block->offset(),
                                                    cell.file_block->range().size(), cache_lock);
                _lru_recorder->record_queue_event(FileCacheType::TTL, CacheLRULogType::ADD,
                                                  cell.file_block->get_hash_value(),
                                                  cell.file_block->offset(), cell.size());
            }
            if (!st.ok()) {
                LOG_WARNING("").error(st);
            }
        }
        _key_to_time[hash] = new_expiration_time;
        _time_to_key.insert(std::make_pair(new_expiration_time, hash));
    }
}

std::vector<std::tuple<size_t, size_t, FileCacheType, uint64_t>>
BlockFileCache::get_hot_blocks_meta(const UInt128Wrapper& hash) const {
    int64_t cur_time = std::chrono::duration_cast<std::chrono::seconds>(
                               std::chrono::steady_clock::now().time_since_epoch())
                               .count();
    SCOPED_CACHE_LOCK(_mutex, this);
    std::vector<std::tuple<size_t, size_t, FileCacheType, uint64_t>> blocks_meta;
    if (auto iter = _files.find(hash); iter != _files.end()) {
        for (auto& pair : _files.find(hash)->second) {
            const FileBlockCell* cell = &pair.second;
            if (cell->file_block->cache_type() != FileCacheType::DISPOSABLE) {
                if (cell->file_block->cache_type() == FileCacheType::TTL ||
                    (cell->atime != 0 &&
                     cur_time - cell->atime <
                             get_queue(cell->file_block->cache_type()).get_hot_data_interval())) {
                    blocks_meta.emplace_back(pair.first, cell->size(),
                                             cell->file_block->cache_type(),
                                             cell->file_block->expiration_time());
                }
            }
        }
    }
    return blocks_meta;
}

bool BlockFileCache::try_reserve_during_async_load(size_t size,
                                                   std::lock_guard<std::mutex>& cache_lock) {
    size_t removed_size = 0;
    size_t normal_queue_size = _normal_queue.get_capacity(cache_lock);
    size_t disposable_queue_size = _disposable_queue.get_capacity(cache_lock);
    size_t index_queue_size = _index_queue.get_capacity(cache_lock);

    std::vector<FileBlockCell*> to_evict;
    auto collect_eliminate_fragments = [&](LRUQueue& queue) {
        for (const auto& [entry_key, entry_offset, entry_size] : queue) {
            if (!_disk_resource_limit_mode || removed_size >= size) {
                break;
            }
            auto* cell = get_cell(entry_key, entry_offset, cache_lock);

            DCHECK(cell) << "Cache became inconsistent. UInt128Wrapper: " << entry_key.to_string()
                         << ", offset: " << entry_offset;

            size_t cell_size = cell->size();
            DCHECK(entry_size == cell_size);

            if (cell->releasable()) {
                auto& file_block = cell->file_block;

                std::lock_guard block_lock(file_block->_mutex);
                DCHECK(file_block->_download_state == FileBlock::State::DOWNLOADED);
                to_evict.push_back(cell);
                removed_size += cell_size;
            }
        }
    };
    if (disposable_queue_size != 0) {
        collect_eliminate_fragments(get_queue(FileCacheType::DISPOSABLE));
    }
    if (normal_queue_size != 0) {
        collect_eliminate_fragments(get_queue(FileCacheType::NORMAL));
    }
    if (index_queue_size != 0) {
        collect_eliminate_fragments(get_queue(FileCacheType::INDEX));
    }
    remove_file_blocks(to_evict, cache_lock, true);

    return !_disk_resource_limit_mode || removed_size >= size;
}

std::string BlockFileCache::clear_file_cache_directly() {
    _lru_dumper->remove_lru_dump_files();
    using namespace std::chrono;
    std::stringstream ss;
    auto start = steady_clock::now();
    SCOPED_CACHE_LOCK(_mutex, this);
    LOG_INFO("start clear_file_cache_directly").tag("path", _cache_base_path);

    std::string clear_msg;
    auto s = _storage->clear(clear_msg);
    if (!s.ok()) {
        return clear_msg;
    }

    int64_t num_files = _files.size();
    int64_t cache_size = _cur_cache_size;
    int64_t index_queue_size = _index_queue.get_elements_num(cache_lock);
    int64_t normal_queue_size = _normal_queue.get_elements_num(cache_lock);
    int64_t disposible_queue_size = _disposable_queue.get_elements_num(cache_lock);
    int64_t ttl_queue_size = _ttl_queue.get_elements_num(cache_lock);

    int64_t clear_fd_duration = 0;
    {
        // clear FDCache to release fd
        SCOPED_RAW_TIMER(&clear_fd_duration);
        for (const auto& [file_key, file_blocks] : _files) {
            for (const auto& [offset, file_block_cell] : file_blocks) {
                AccessKeyAndOffset access_key_and_offset(file_key, offset);
                FDCache::instance()->remove_file_reader(access_key_and_offset);
            }
        }
    }

    _files.clear();
    _cur_cache_size = 0;
    _cur_ttl_size = 0;
    _time_to_key.clear();
    _key_to_time.clear();
    _index_queue.clear(cache_lock);
    _normal_queue.clear(cache_lock);
    _disposable_queue.clear(cache_lock);
    _ttl_queue.clear(cache_lock);
    ss << "finish clear_file_cache_directly"
       << " path=" << _cache_base_path
       << " time_elapsed_ms=" << duration_cast<milliseconds>(steady_clock::now() - start).count()
       << " fd_clear_time_ms=" << (clear_fd_duration / 1000000) << " num_files=" << num_files
       << " cache_size=" << cache_size << " index_queue_size=" << index_queue_size
       << " normal_queue_size=" << normal_queue_size
       << " disposible_queue_size=" << disposible_queue_size << "ttl_queue_size=" << ttl_queue_size;
    auto msg = ss.str();
    LOG(INFO) << msg;
    _lru_dumper->remove_lru_dump_files();
    return msg;
}

std::map<size_t, FileBlockSPtr> BlockFileCache::get_blocks_by_key(const UInt128Wrapper& hash) {
    std::map<size_t, FileBlockSPtr> offset_to_block;
    SCOPED_CACHE_LOCK(_mutex, this);
    if (_files.contains(hash)) {
        for (auto& [offset, cell] : _files[hash]) {
            if (cell.file_block->state() == FileBlock::State::DOWNLOADED) {
                offset_to_block.emplace(offset, cell.file_block);
                use_cell(cell, nullptr,
                         need_to_move(cell.file_block->cache_type(), FileCacheType::DISPOSABLE),
                         cache_lock);
            }
        }
    }
    return offset_to_block;
}

void BlockFileCache::update_ttl_atime(const UInt128Wrapper& hash) {
    SCOPED_CACHE_LOCK(_mutex, this);
    if (auto iter = _files.find(hash); iter != _files.end()) {
        for (auto& [_, cell] : iter->second) {
            cell.update_atime();
        }
    };
}

void BlockFileCache::run_background_lru_log_replay() {
    Thread::set_self_name("run_background_lru_log_replay");
    while (!_close) {
        int64_t interval_ms = config::file_cache_background_lru_log_replay_interval_ms;
        {
            std::unique_lock close_lock(_close_mtx);
            _close_cv.wait_for(close_lock, std::chrono::milliseconds(interval_ms));
            if (_close) {
                break;
            }
        }

        _lru_recorder->replay_queue_event(FileCacheType::TTL);
        _lru_recorder->replay_queue_event(FileCacheType::INDEX);
        _lru_recorder->replay_queue_event(FileCacheType::NORMAL);
        _lru_recorder->replay_queue_event(FileCacheType::DISPOSABLE);

        if (config::enable_evaluate_shadow_queue_diff) {
            SCOPED_CACHE_LOCK(_mutex, this);
            _lru_recorder->evaluate_queue_diff(_ttl_queue, "ttl", cache_lock);
            _lru_recorder->evaluate_queue_diff(_index_queue, "index", cache_lock);
            _lru_recorder->evaluate_queue_diff(_normal_queue, "normal", cache_lock);
            _lru_recorder->evaluate_queue_diff(_disposable_queue, "disposable", cache_lock);
        }
    }
}

void BlockFileCache::run_background_lru_dump() {
    Thread::set_self_name("run_background_lru_dump");
    while (!_close) {
        int64_t interval_ms = config::file_cache_background_lru_dump_interval_ms;
        {
            std::unique_lock close_lock(_close_mtx);
            _close_cv.wait_for(close_lock, std::chrono::milliseconds(interval_ms));
            if (_close) {
                break;
            }
        }

        if (config::file_cache_background_lru_dump_tail_record_num > 0 &&
            !ExecEnv::GetInstance()->get_is_upgrading()) {
            _lru_dumper->dump_queue("disposable");
            _lru_dumper->dump_queue("normal");
            _lru_dumper->dump_queue("index");
            _lru_dumper->dump_queue("ttl");
            _lru_dumper->set_first_dump_done();
        }
    }
}

void BlockFileCache::restore_lru_queues_from_disk(std::lock_guard<std::mutex>& cache_lock) {
    // keep this order coz may be duplicated in different queue, we use the first appearence
    _lru_dumper->restore_queue(_ttl_queue, "ttl", cache_lock);
    _lru_dumper->restore_queue(_index_queue, "index", cache_lock);
    _lru_dumper->restore_queue(_normal_queue, "normal", cache_lock);
    _lru_dumper->restore_queue(_disposable_queue, "disposable", cache_lock);
}

std::map<std::string, double> BlockFileCache::get_stats() {
    std::map<std::string, double> stats;
    stats["hits_ratio"] = (double)_hit_ratio->get_value();
    stats["hits_ratio_5m"] = (double)_hit_ratio_5m->get_value();
    stats["hits_ratio_1h"] = (double)_hit_ratio_1h->get_value();

    stats["index_queue_max_size"] = (double)_index_queue.get_max_size();
    stats["index_queue_curr_size"] = (double)_cur_index_queue_cache_size_metrics->get_value();
    stats["index_queue_max_elements"] = (double)_index_queue.get_max_element_size();
    stats["index_queue_curr_elements"] =
            (double)_cur_index_queue_element_count_metrics->get_value();

    stats["ttl_queue_max_size"] = (double)_ttl_queue.get_max_size();
    stats["ttl_queue_curr_size"] = (double)_cur_ttl_cache_lru_queue_cache_size_metrics->get_value();
    stats["ttl_queue_max_elements"] = (double)_ttl_queue.get_max_element_size();
    stats["ttl_queue_curr_elements"] =
            (double)_cur_ttl_cache_lru_queue_element_count_metrics->get_value();

    stats["normal_queue_max_size"] = (double)_normal_queue.get_max_size();
    stats["normal_queue_curr_size"] = (double)_cur_normal_queue_cache_size_metrics->get_value();
    stats["normal_queue_max_elements"] = (double)_normal_queue.get_max_element_size();
    stats["normal_queue_curr_elements"] =
            (double)_cur_normal_queue_element_count_metrics->get_value();

    stats["disposable_queue_max_size"] = (double)_disposable_queue.get_max_size();
    stats["disposable_queue_curr_size"] =
            (double)_cur_disposable_queue_cache_size_metrics->get_value();
    stats["disposable_queue_max_elements"] = (double)_disposable_queue.get_max_element_size();
    stats["disposable_queue_curr_elements"] =
            (double)_cur_disposable_queue_element_count_metrics->get_value();

    stats["total_removed_counts"] = (double)_num_removed_blocks->get_value();
    stats["total_hit_counts"] = (double)_num_hit_blocks->get_value();
    stats["total_read_counts"] = (double)_num_read_blocks->get_value();
    stats["need_evict_cache_in_advance"] = (double)_need_evict_cache_in_advance;
    stats["disk_resource_limit_mode"] = (double)_disk_resource_limit_mode;

    return stats;
}

// for be UTs
std::map<std::string, double> BlockFileCache::get_stats_unsafe() {
    std::map<std::string, double> stats;
    stats["hits_ratio"] = (double)_hit_ratio->get_value();
    stats["hits_ratio_5m"] = (double)_hit_ratio_5m->get_value();
    stats["hits_ratio_1h"] = (double)_hit_ratio_1h->get_value();

    stats["index_queue_max_size"] = (double)_index_queue.get_max_size();
    stats["index_queue_curr_size"] = (double)_index_queue.get_capacity_unsafe();
    stats["index_queue_max_elements"] = (double)_index_queue.get_max_element_size();
    stats["index_queue_curr_elements"] = (double)_index_queue.get_elements_num_unsafe();

    stats["ttl_queue_max_size"] = (double)_ttl_queue.get_max_size();
    stats["ttl_queue_curr_size"] = (double)_ttl_queue.get_capacity_unsafe();
    stats["ttl_queue_max_elements"] = (double)_ttl_queue.get_max_element_size();
    stats["ttl_queue_curr_elements"] = (double)_ttl_queue.get_elements_num_unsafe();

    stats["normal_queue_max_size"] = (double)_normal_queue.get_max_size();
    stats["normal_queue_curr_size"] = (double)_normal_queue.get_capacity_unsafe();
    stats["normal_queue_max_elements"] = (double)_normal_queue.get_max_element_size();
    stats["normal_queue_curr_elements"] = (double)_normal_queue.get_elements_num_unsafe();

    stats["disposable_queue_max_size"] = (double)_disposable_queue.get_max_size();
    stats["disposable_queue_curr_size"] = (double)_disposable_queue.get_capacity_unsafe();
    stats["disposable_queue_max_elements"] = (double)_disposable_queue.get_max_element_size();
    stats["disposable_queue_curr_elements"] = (double)_disposable_queue.get_elements_num_unsafe();

    return stats;
}

template void BlockFileCache::remove(FileBlockSPtr file_block,
                                     std::lock_guard<std::mutex>& cache_lock,
                                     std::lock_guard<std::mutex>& block_lock, bool sync);

#include "common/compile_check_end.h"

} // namespace doris::io
