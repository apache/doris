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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/Cache/LRUFileCachePriority.h
// and modified by Doris

#pragma once

#include <cstddef>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/status.h"
#include "io/cache/block/block_file_cache.h"
#include "io/cache/block/block_file_segment.h"
#include "util/metrics.h"

namespace doris {
class TUniqueId;

namespace io {
/**
 * Local cache for remote filesystem files, represented as a set of non-overlapping non-empty file segments.
 * Implements LRU eviction policy.
 */
class LRUFileCache final : public IFileCache {
public:
    /**
     * cache_base_path: the file cache path
     * cache_settings: the file cache setttings
     */
    LRUFileCache(const std::string& cache_base_path, const FileCacheSettings& cache_settings);
    ~LRUFileCache() override {
        _close = true;
        if (_cache_background_thread.joinable()) {
            _cache_background_thread.join();
        }
    };

    /**
     * get the files which range contain [offset, offset+size-1]
     */
    FileBlocksHolder get_or_set(const Key& key, size_t offset, size_t size,
                                const CacheContext& context) override;

    // init file cache
    Status initialize() override;

    size_t get_used_cache_size(CacheType type) const override;

    size_t get_file_segments_num(CacheType type) const override;

private:
    struct FileBlockCell {
        FileBlockSPtr file_block;
        CacheType cache_type;

        /// Iterator is put here on first reservation attempt, if successful.
        std::optional<LRUQueue::Iterator> queue_iterator;

        mutable int64_t atime {0};
        void update_atime() const {
            atime = std::chrono::duration_cast<std::chrono::seconds>(
                            std::chrono::steady_clock::now().time_since_epoch())
                            .count();
        }

        /// Pointer to file block is always hold by the cache itself.
        /// Apart from pointer in cache, it can be hold by cache users, when they call
        /// getorSet(), but cache users always hold it via FileBlocksHolder.
        bool releasable() const { return file_block.unique(); }

        size_t size() const { return file_block->_segment_range.size(); }

        FileBlockCell(FileBlockSPtr file_block, CacheType cache_type,
                      std::lock_guard<std::mutex>& cache_lock);

        FileBlockCell(FileBlockCell&& other) noexcept
                : file_block(std::move(other.file_block)),
                  cache_type(other.cache_type),
                  queue_iterator(other.queue_iterator),
                  atime(other.atime) {}

        FileBlockCell& operator=(const FileBlockCell&) = delete;
        FileBlockCell(const FileBlockCell&) = delete;
    };

    using FileBlocksByOffset = std::map<size_t, FileBlockCell>;

    struct HashCachedFileKey {
        std::size_t operator()(const Key& k) const { return KeyHash()(k); }
    };

    using CachedFiles = std::unordered_map<Key, FileBlocksByOffset, HashCachedFileKey>;

    CachedFiles _files;
    size_t _cur_cache_size = 0;

    // The three queues are level queue.
    // It means as level1/level2/level3 queue.
    // but the level2 is maximum.
    // If some datas are importance, we can cache it into index queue
    // If some datas are just use once, we can cache it into disposable queue
    // The size proportion is [1:17:2].
    LRUQueue _index_queue;
    LRUQueue _normal_queue;
    LRUQueue _disposable_queue;

    size_t try_release() override;

    LRUFileCache::LRUQueue& get_queue(CacheType type);
    const LRUFileCache::LRUQueue& get_queue(CacheType type) const;

    FileBlocks get_impl(const Key& key, const CacheContext& context, const FileBlock::Range& range,
                        std::lock_guard<std::mutex>& cache_lock);

    FileBlockCell* get_cell(const Key& key, size_t offset, std::lock_guard<std::mutex>& cache_lock);

    FileBlockCell* add_cell(const Key& key, const CacheContext& context, size_t offset, size_t size,
                            FileBlock::State state, std::lock_guard<std::mutex>& cache_lock);

    void use_cell(const FileBlockCell& cell, FileBlocks& result, bool not_need_move,
                  std::lock_guard<std::mutex>& cache_lock);

    bool try_reserve(const Key& key, const CacheContext& context, size_t offset, size_t size,
                     std::lock_guard<std::mutex>& cache_lock) override;

    bool try_reserve_for_lru(const Key& key, QueryFileCacheContextPtr query_context,
                             const CacheContext& context, size_t offset, size_t size,
                             std::lock_guard<std::mutex>& cache_lock);

    std::vector<CacheType> get_other_cache_type(CacheType cur_cache_type);

    bool try_reserve_from_other_queue(CacheType cur_cache_type, size_t offset, int64_t cur_time,
                                      std::lock_guard<std::mutex>& cache_lock);

    void remove(FileBlockSPtr file_block, std::lock_guard<std::mutex>& cache_lock,
                std::lock_guard<std::mutex>& segment_lock) override;

    void change_cache_type(const Key& key, size_t offset, CacheType new_type,
                           std::lock_guard<doris::Mutex>& cache_lock) override;

    size_t get_available_cache_size(CacheType cache_type) const;

    Status load_cache_info_into_memory(std::lock_guard<std::mutex>& cache_lock);

    Status write_file_cache_version() const;

    std::string read_file_cache_version() const;

    FileBlocks split_range_into_cells(const Key& key, const CacheContext& context, size_t offset,
                                      size_t size, FileBlock::State state,
                                      std::lock_guard<std::mutex>& cache_lock);

    std::string dump_structure_unlocked(const Key& key, std::lock_guard<std::mutex>& cache_lock);

    void fill_holes_with_empty_file_blocks(FileBlocks& file_blocks, const Key& key,
                                           const CacheContext& context,
                                           const FileBlock::Range& range,
                                           std::lock_guard<std::mutex>& cache_lock);

    size_t get_used_cache_size_unlocked(CacheType type,
                                        std::lock_guard<std::mutex>& cache_lock) const;

    size_t get_available_cache_size_unlocked(CacheType type,
                                             std::lock_guard<std::mutex>& cache_lock) const;

    size_t get_file_segments_num_unlocked(CacheType type,
                                          std::lock_guard<std::mutex>& cache_lock) const;

    bool need_to_move(CacheType cell_type, CacheType query_type) const;

    void run_background_operation();

    void update_cache_metrics() const;

public:
    std::string dump_structure(const Key& key) override;

private:
    std::atomic_bool _close {false};
    std::thread _cache_background_thread;
    size_t _num_read_segments = 0;
    size_t _num_hit_segments = 0;
    size_t _num_removed_segments = 0;

    std::shared_ptr<MetricEntity> _entity = nullptr;

    DoubleGauge* file_cache_hits_ratio = nullptr;
    UIntGauge* file_cache_removed_elements = nullptr;

    UIntGauge* file_cache_index_queue_max_size = nullptr;
    UIntGauge* file_cache_index_queue_curr_size = nullptr;
    UIntGauge* file_cache_index_queue_max_elements = nullptr;
    UIntGauge* file_cache_index_queue_curr_elements = nullptr;

    UIntGauge* file_cache_normal_queue_max_size = nullptr;
    UIntGauge* file_cache_normal_queue_curr_size = nullptr;
    UIntGauge* file_cache_normal_queue_max_elements = nullptr;
    UIntGauge* file_cache_normal_queue_curr_elements = nullptr;

    UIntGauge* file_cache_disposable_queue_max_size = nullptr;
    UIntGauge* file_cache_disposable_queue_curr_size = nullptr;
    UIntGauge* file_cache_disposable_queue_max_elements = nullptr;
    UIntGauge* file_cache_disposable_queue_curr_elements = nullptr;
    UIntGauge* file_cache_segment_reader_cache_size = nullptr;
};

} // namespace io
} // namespace doris
