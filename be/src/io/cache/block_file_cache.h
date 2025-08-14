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

#include <bvar/bvar.h>
#include <concurrentqueue.h>

#include <algorithm>
#include <boost/lockfree/spsc_queue.hpp>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>

#include "io/cache/cache_lru_dumper.h"
#include "io/cache/file_block.h"
#include "io/cache/file_cache_common.h"
#include "io/cache/file_cache_storage.h"
#include "io/cache/lru_queue_recorder.h"
#include "util/runtime_profile.h"
#include "util/threadpool.h"

namespace doris::io {
using RecycleFileCacheKeys = moodycamel::ConcurrentQueue<FileCacheKey>;

class LockScopedTimer {
public:
    LockScopedTimer() : start_(std::chrono::steady_clock::now()) {}
    ~LockScopedTimer() {
        auto end = std::chrono::steady_clock::now();
        auto duration_us =
                std::chrono::duration_cast<std::chrono::microseconds>(end - start_).count();
        if (duration_us > config::cache_lock_held_long_tail_threshold_us) {
            LOG(WARNING) << "Lock held time " << std::to_string(duration_us) << "us. "
                         << get_stack_trace();
        }
    }

private:
    std::chrono::time_point<std::chrono::steady_clock> start_;
};

// Note: the cache_lock is scoped, so do not add do...while(0) here.
#ifdef ENABLE_CACHE_LOCK_DEBUG
#define SCOPED_CACHE_LOCK(MUTEX, cache)                                                           \
    std::chrono::time_point<std::chrono::steady_clock> start_time =                               \
            std::chrono::steady_clock::now();                                                     \
    std::lock_guard cache_lock(MUTEX);                                                            \
    std::chrono::time_point<std::chrono::steady_clock> acq_time =                                 \
            std::chrono::steady_clock::now();                                                     \
    auto duration_us =                                                                            \
            std::chrono::duration_cast<std::chrono::microseconds>(acq_time - start_time).count(); \
    *(cache->_cache_lock_wait_time_us) << duration_us;                                            \
    if (duration_us > config::cache_lock_wait_long_tail_threshold_us) {                           \
        LOG(WARNING) << "Lock wait time " << std::to_string(duration_us) << "us. "                \
                     << get_stack_trace() << std::endl;                                           \
    }                                                                                             \
    LockScopedTimer cache_lock_timer;
#else
#define SCOPED_CACHE_LOCK(MUTEX, cache) std::lock_guard cache_lock(MUTEX);
#endif

class FSFileCacheStorage;

// The BlockFileCache is responsible for the management of the blocks
// The current strategies are lru and ttl.
class BlockFileCache {
    friend class FSFileCacheStorage;
    friend class MemFileCacheStorage;
    friend class FileBlock;
    friend struct FileBlocksHolder;
    friend class CacheLRUDumper;
    friend class LRUQueueRecorder;

public:
    // hash the file_name to uint128
    static UInt128Wrapper hash(const std::string& path);

    BlockFileCache(const std::string& cache_base_path, const FileCacheSettings& cache_settings);

    virtual ~BlockFileCache() {
        {
            std::lock_guard lock(_close_mtx);
            _close = true;
        }
        _close_cv.notify_all();
        if (_cache_background_monitor_thread.joinable()) {
            _cache_background_monitor_thread.join();
        }
        if (_cache_background_ttl_gc_thread.joinable()) {
            _cache_background_ttl_gc_thread.join();
        }
        if (_cache_background_gc_thread.joinable()) {
            _cache_background_gc_thread.join();
        }
        if (_cache_background_evict_in_advance_thread.joinable()) {
            _cache_background_evict_in_advance_thread.join();
        }
        if (_cache_background_lru_dump_thread.joinable()) {
            _cache_background_lru_dump_thread.join();
        }
        if (_cache_background_lru_log_replay_thread.joinable()) {
            _cache_background_lru_log_replay_thread.join();
        }
    }

    /// Restore cache from local filesystem.
    Status initialize();

    /// Cache capacity in bytes.
    [[nodiscard]] size_t capacity() const { return _capacity; }

    // try to release all releasable block
    // it maybe hang the io/system
    size_t try_release();

    [[nodiscard]] const std::string& get_base_path() const { return _cache_base_path; }

    /**
         * Given an `offset` and `size` representing [offset, offset + size) bytes interval,
         * return list of cached non-overlapping non-empty
         * file blocks `[block1, ..., blockN]` which intersect with given interval.
         *
         * blocks in returned list are ordered in ascending order and represent a full contiguous
         * interval (no holes). Each block in returned list has state: DOWNLOADED, DOWNLOADING or EMPTY.
         *
         * As long as pointers to returned file blocks are hold
         * it is guaranteed that these file blocks are not removed from cache.
         */
    FileBlocksHolder get_or_set(const UInt128Wrapper& hash, size_t offset, size_t size,
                                CacheContext& context);

    /**
     * Clear all cached data for this cache instance async
     *
     * @returns summary message
     */
    std::string clear_file_cache_async();
    std::string clear_file_cache_directly();

    /**
     * Reset the cache capacity. If the new_capacity is smaller than _capacity, the redundant data will be remove async.
     *
     * @returns summary message
     */
    std::string reset_capacity(size_t new_capacity);

    std::map<size_t, FileBlockSPtr> get_blocks_by_key(const UInt128Wrapper& hash);
    /// For debug and UT
    std::string dump_structure(const UInt128Wrapper& hash);
    std::string dump_single_cache_type(const UInt128Wrapper& hash, size_t offset);

    [[nodiscard]] size_t get_used_cache_size(FileCacheType type) const;

    [[nodiscard]] size_t get_file_blocks_num(FileCacheType type) const;

    // change the block cache type
    void change_cache_type(const UInt128Wrapper& hash, size_t offset, FileCacheType new_type,
                           std::lock_guard<std::mutex>& cache_lock);

    // remove all blocks that belong to the key
    void remove_if_cached(const UInt128Wrapper& key);
    void remove_if_cached_async(const UInt128Wrapper& key);

    // modify the expiration time about the key
    void modify_expiration_time(const UInt128Wrapper& key, uint64_t new_expiration_time);

    // Shrink the block size. old_size is always larged than new_size.
    void reset_range(const UInt128Wrapper&, size_t offset, size_t old_size, size_t new_size,
                     std::lock_guard<std::mutex>& cache_lock);

    // get the hotest blocks message by key
    // The tuple is composed of <offset, size, cache_type, expiration_time>
    [[nodiscard]] std::vector<std::tuple<size_t, size_t, FileCacheType, uint64_t>>
    get_hot_blocks_meta(const UInt128Wrapper& hash) const;

    [[nodiscard]] bool get_async_open_success() const { return _async_open_done; }

    BlockFileCache& operator=(const BlockFileCache&) = delete;
    BlockFileCache(const BlockFileCache&) = delete;

    // try to reserve the new space for the new block if the cache is full
    bool try_reserve(const UInt128Wrapper& hash, const CacheContext& context, size_t offset,
                     size_t size, std::lock_guard<std::mutex>& cache_lock);

    /**
     * Proactively evict cache blocks to free up space before cache is full.
     * 
     * This function attempts to evict blocks from both NORMAL and TTL queues to maintain 
     * cache size below high watermark. Unlike try_reserve() which blocks until space is freed,
     * this function initiates asynchronous eviction in background.
     * 
     * @param size Number of bytes to try to evict
     * @param cache_lock Lock that must be held while accessing cache data structures
     * 
     * @pre Caller must hold cache_lock
     * @pre _need_evict_cache_in_advance must be true
     * @pre _recycle_keys queue must have capacity for evicted blocks
     */
    void try_evict_in_advance(size_t size, std::lock_guard<std::mutex>& cache_lock);

    void update_ttl_atime(const UInt128Wrapper& hash);

    std::map<std::string, double> get_stats();

    // for be UTs
    std::map<std::string, double> get_stats_unsafe();

    using AccessRecord =
            std::unordered_map<AccessKeyAndOffset, LRUQueue::Iterator, KeyAndOffsetHash>;

    /// Used to track and control the cache access of each query.
    /// Through it, we can realize the processing of different queries by the cache layer.
    struct QueryFileCacheContext {
        LRUQueue lru_queue;
        AccessRecord records;

        QueryFileCacheContext(size_t max_cache_size) : lru_queue(max_cache_size, 0, 0) {}

        void remove(const UInt128Wrapper& hash, size_t offset,
                    std::lock_guard<std::mutex>& cache_lock);

        void reserve(const UInt128Wrapper& hash, size_t offset, size_t size,
                     std::lock_guard<std::mutex>& cache_lock);

        size_t get_max_cache_size() const { return lru_queue.get_max_size(); }

        size_t get_cache_size(std::lock_guard<std::mutex>& cache_lock) const {
            return lru_queue.get_capacity(cache_lock);
        }

        LRUQueue& queue() { return lru_queue; }
    };

    using QueryFileCacheContextPtr = std::shared_ptr<QueryFileCacheContext>;
    using QueryFileCacheContextMap = std::unordered_map<TUniqueId, QueryFileCacheContextPtr>;

    QueryFileCacheContextPtr get_query_context(const TUniqueId& query_id,
                                               std::lock_guard<std::mutex>&);

    void remove_query_context(const TUniqueId& query_id);

    QueryFileCacheContextPtr get_or_set_query_context(const TUniqueId& query_id,
                                                      std::lock_guard<std::mutex>&);

    /// Save a query context information, and adopt different cache policies
    /// for different queries through the context cache layer.
    struct QueryFileCacheContextHolder {
        QueryFileCacheContextHolder(const TUniqueId& query_id, BlockFileCache* mgr,
                                    QueryFileCacheContextPtr context)
                : query_id(query_id), mgr(mgr), context(context) {}

        QueryFileCacheContextHolder& operator=(const QueryFileCacheContextHolder&) = delete;
        QueryFileCacheContextHolder(const QueryFileCacheContextHolder&) = delete;

        ~QueryFileCacheContextHolder() {
            /// If only the query_map and the current holder hold the context_query,
            /// the query has been completed and the query_context is released.
            if (context) {
                context.reset();
                mgr->remove_query_context(query_id);
            }
        }

        const TUniqueId& query_id;
        BlockFileCache* mgr = nullptr;
        QueryFileCacheContextPtr context;
    };
    using QueryFileCacheContextHolderPtr = std::unique_ptr<QueryFileCacheContextHolder>;
    QueryFileCacheContextHolderPtr get_query_context_holder(const TUniqueId& query_id);

    int64_t approximate_available_cache_size() const {
        return std::max<int64_t>(
                _cache_capacity_metrics->get_value() - _cur_cache_size_metrics->get_value(), 0);
    }

private:
    struct FileBlockCell {
        FileBlockSPtr file_block;
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
        bool releasable() const { return file_block.use_count() == 1; }

        size_t size() const { return file_block->_block_range.size(); }

        FileBlockCell(FileBlockSPtr file_block, std::lock_guard<std::mutex>& cache_lock);
        FileBlockCell(FileBlockCell&& other) noexcept
                : file_block(std::move(other.file_block)),
                  queue_iterator(other.queue_iterator),
                  atime(other.atime) {}

        FileBlockCell& operator=(const FileBlockCell&) = delete;
        FileBlockCell(const FileBlockCell&) = delete;
    };

    LRUQueue& get_queue(FileCacheType type);
    const LRUQueue& get_queue(FileCacheType type) const;

    template <class T, class U>
        requires IsXLock<T> && IsXLock<U>
    void remove(FileBlockSPtr file_block, T& cache_lock, U& segment_lock, bool sync = true);

    FileBlocks get_impl(const UInt128Wrapper& hash, const CacheContext& context,
                        const FileBlock::Range& range, std::lock_guard<std::mutex>& cache_lock);

    template <class T>
        requires IsXLock<T>
    FileBlockCell* get_cell(const UInt128Wrapper& hash, size_t offset, T& cache_lock);

    virtual FileBlockCell* add_cell(const UInt128Wrapper& hash, const CacheContext& context,
                                    size_t offset, size_t size, FileBlock::State state,
                                    std::lock_guard<std::mutex>& cache_lock);

    Status initialize_unlocked(std::lock_guard<std::mutex>& cache_lock);

    void use_cell(const FileBlockCell& cell, FileBlocks* result, bool not_need_move,
                  std::lock_guard<std::mutex>& cache_lock);

    bool try_reserve_for_lru(const UInt128Wrapper& hash, QueryFileCacheContextPtr query_context,
                             const CacheContext& context, size_t offset, size_t size,
                             std::lock_guard<std::mutex>& cache_lock,
                             bool evict_in_advance = false);

    bool try_reserve_during_async_load(size_t size, std::lock_guard<std::mutex>& cache_lock);

    std::vector<FileCacheType> get_other_cache_type(FileCacheType cur_cache_type);
    std::vector<FileCacheType> get_other_cache_type_without_ttl(FileCacheType cur_cache_type);

    bool try_reserve_from_other_queue(FileCacheType cur_cache_type, size_t offset, int64_t cur_time,
                                      std::lock_guard<std::mutex>& cache_lock,
                                      bool evict_in_advance = false);

    size_t get_available_cache_size(FileCacheType cache_type) const;

    FileBlocks split_range_into_cells(const UInt128Wrapper& hash, const CacheContext& context,
                                      size_t offset, size_t size, FileBlock::State state,
                                      std::lock_guard<std::mutex>& cache_lock);

    std::string dump_structure_unlocked(const UInt128Wrapper& hash,
                                        std::lock_guard<std::mutex>& cache_lock);

    std::string dump_single_cache_type_unlocked(const UInt128Wrapper& hash, size_t offset,
                                                std::lock_guard<std::mutex>& cache_lock);

    void fill_holes_with_empty_file_blocks(FileBlocks& file_blocks, const UInt128Wrapper& hash,
                                           const CacheContext& context,
                                           const FileBlock::Range& range,
                                           std::lock_guard<std::mutex>& cache_lock);

    size_t get_used_cache_size_unlocked(FileCacheType type,
                                        std::lock_guard<std::mutex>& cache_lock) const;

    void check_disk_resource_limit();
    void check_need_evict_cache_in_advance();

    size_t get_available_cache_size_unlocked(FileCacheType type,
                                             std::lock_guard<std::mutex>& cache_lock) const;

    size_t get_file_blocks_num_unlocked(FileCacheType type,
                                        std::lock_guard<std::mutex>& cache_lock) const;

    bool need_to_move(FileCacheType cell_type, FileCacheType query_type) const;

    bool remove_if_ttl_file_blocks(const UInt128Wrapper& file_key, bool remove_directly,
                                   std::lock_guard<std::mutex>&, bool sync);

    void run_background_monitor();
    void run_background_ttl_gc();
    void run_background_gc();
    void run_background_lru_log_replay();
    void run_background_lru_dump();
    void restore_lru_queues_from_disk(std::lock_guard<std::mutex>& cache_lock);
    void run_background_evict_in_advance();

    bool try_reserve_from_other_queue_by_time_interval(FileCacheType cur_type,
                                                       std::vector<FileCacheType> other_cache_types,
                                                       size_t size, int64_t cur_time,
                                                       std::lock_guard<std::mutex>& cache_lock,
                                                       bool evict_in_advance);

    bool try_reserve_from_other_queue_by_size(FileCacheType cur_type,
                                              std::vector<FileCacheType> other_cache_types,
                                              size_t size, std::lock_guard<std::mutex>& cache_lock,
                                              bool evict_in_advance);

    bool is_overflow(size_t removed_size, size_t need_size, size_t cur_cache_size,
                     bool evict_in_advance) const;

    void remove_file_blocks(std::vector<FileBlockCell*>&, std::lock_guard<std::mutex>&, bool sync);

    void remove_file_blocks_and_clean_time_maps(std::vector<FileBlockCell*>&,
                                                std::lock_guard<std::mutex>&);

    void find_evict_candidates(LRUQueue& queue, size_t size, size_t cur_cache_size,
                               size_t& removed_size, std::vector<FileBlockCell*>& to_evict,
                               std::lock_guard<std::mutex>& cache_lock, size_t& cur_removed_size,
                               bool evict_in_advance);

    Status check_ofstream_status(std::ofstream& out, std::string& filename);
    Status dump_one_lru_entry(std::ofstream& out, std::string& filename, const UInt128Wrapper& hash,
                              size_t offset, size_t size);
    Status finalize_dump(std::ofstream& out, size_t entry_num, std::string& tmp_filename,
                         std::string& final_filename, size_t& file_size);
    Status check_ifstream_status(std::ifstream& in, std::string& filename);
    Status parse_dump_footer(std::ifstream& in, std::string& filename, size_t& entry_num);
    Status parse_one_lru_entry(std::ifstream& in, std::string& filename, UInt128Wrapper& hash,
                               size_t& offset, size_t& size);
    void remove_lru_dump_files();

    // info
    std::string _cache_base_path;
    size_t _capacity = 0;
    size_t _max_file_block_size = 0;
    size_t _max_query_cache_size = 0;

    mutable std::mutex _mutex;
    bool _close {false};
    std::mutex _close_mtx;
    std::condition_variable _close_cv;
    std::thread _cache_background_monitor_thread;
    std::thread _cache_background_ttl_gc_thread;
    std::thread _cache_background_gc_thread;
    std::thread _cache_background_evict_in_advance_thread;
    std::thread _cache_background_lru_dump_thread;
    std::thread _cache_background_lru_log_replay_thread;
    std::atomic_bool _async_open_done {false};
    // disk space or inode is less than the specified value
    bool _disk_resource_limit_mode {false};
    bool _need_evict_cache_in_advance {false};
    bool _is_initialized {false};

    // strategy
    using FileBlocksByOffset = std::map<size_t, FileBlockCell>;
    using CachedFiles = std::unordered_map<UInt128Wrapper, FileBlocksByOffset, KeyHash>;
    CachedFiles _files;
    QueryFileCacheContextMap _query_map;
    size_t _cur_cache_size = 0;
    size_t _cur_ttl_size = 0;
    std::multimap<uint64_t, UInt128Wrapper> _time_to_key;
    std::unordered_map<UInt128Wrapper, uint64_t, KeyHash> _key_to_time;
    // The three queues are level queue.
    // It means as level1/level2/level3 queue.
    // but the level2 is maximum.
    // If some datas are importance, we can cache it into index queue
    // If some datas are just use once, we can cache it into disposable queue
    // The size proportion is [1:17:2].
    LRUQueue _index_queue;
    LRUQueue _normal_queue;
    LRUQueue _disposable_queue;
    LRUQueue _ttl_queue;

    // keys for async remove
    RecycleFileCacheKeys _recycle_keys;

    std::unique_ptr<LRUQueueRecorder> _lru_recorder;
    std::unique_ptr<CacheLRUDumper> _lru_dumper;

    // metrics
    std::shared_ptr<bvar::Status<size_t>> _cache_capacity_metrics;
    std::shared_ptr<bvar::Status<size_t>> _cur_cache_size_metrics;
    std::shared_ptr<bvar::Status<size_t>> _cur_ttl_cache_size_metrics;
    std::shared_ptr<bvar::Status<size_t>> _cur_ttl_cache_lru_queue_cache_size_metrics;
    std::shared_ptr<bvar::Status<size_t>> _cur_ttl_cache_lru_queue_element_count_metrics;
    std::shared_ptr<bvar::Status<size_t>> _cur_normal_queue_element_count_metrics;
    std::shared_ptr<bvar::Status<size_t>> _cur_normal_queue_cache_size_metrics;
    std::shared_ptr<bvar::Status<size_t>> _cur_index_queue_element_count_metrics;
    std::shared_ptr<bvar::Status<size_t>> _cur_index_queue_cache_size_metrics;
    std::shared_ptr<bvar::Status<size_t>> _cur_disposable_queue_element_count_metrics;
    std::shared_ptr<bvar::Status<size_t>> _cur_disposable_queue_cache_size_metrics;
    std::array<std::shared_ptr<bvar::Adder<size_t>>, 4> _queue_evict_size_metrics;
    std::shared_ptr<bvar::Adder<size_t>> _total_evict_size_metrics;
    std::shared_ptr<bvar::Adder<size_t>> _gc_evict_bytes_metrics;
    std::shared_ptr<bvar::Adder<size_t>> _gc_evict_count_metrics;
    std::shared_ptr<bvar::Adder<size_t>> _evict_by_time_metrics_matrix[4][4];
    std::shared_ptr<bvar::Adder<size_t>> _evict_by_size_metrics_matrix[4][4];
    std::shared_ptr<bvar::Adder<size_t>> _evict_by_self_lru_metrics_matrix[4];
    std::shared_ptr<bvar::Adder<size_t>> _evict_by_try_release;

    std::shared_ptr<bvar::Window<bvar::Adder<size_t>>> _num_hit_blocks_5m;
    std::shared_ptr<bvar::Window<bvar::Adder<size_t>>> _num_read_blocks_5m;
    std::shared_ptr<bvar::Window<bvar::Adder<size_t>>> _num_hit_blocks_1h;
    std::shared_ptr<bvar::Window<bvar::Adder<size_t>>> _num_read_blocks_1h;

    std::shared_ptr<bvar::Adder<size_t>> _num_read_blocks;
    std::shared_ptr<bvar::Adder<size_t>> _num_hit_blocks;
    std::shared_ptr<bvar::Adder<size_t>> _num_removed_blocks;

    std::shared_ptr<bvar::Status<double>> _hit_ratio;
    std::shared_ptr<bvar::Status<double>> _hit_ratio_5m;
    std::shared_ptr<bvar::Status<double>> _hit_ratio_1h;
    std::shared_ptr<bvar::Status<size_t>> _disk_limit_mode_metrics;
    std::shared_ptr<bvar::Status<size_t>> _need_evict_cache_in_advance_metrics;

    std::shared_ptr<bvar::LatencyRecorder> _cache_lock_wait_time_us;
    std::shared_ptr<bvar::LatencyRecorder> _get_or_set_latency_us;
    std::shared_ptr<bvar::LatencyRecorder> _storage_sync_remove_latency_us;
    std::shared_ptr<bvar::LatencyRecorder> _storage_retry_sync_remove_latency_us;
    std::shared_ptr<bvar::LatencyRecorder> _storage_async_remove_latency_us;
    std::shared_ptr<bvar::LatencyRecorder> _evict_in_advance_latency_us;
    std::shared_ptr<bvar::LatencyRecorder> _recycle_keys_length_recorder;
    std::shared_ptr<bvar::LatencyRecorder> _ttl_gc_latency_us;

    std::shared_ptr<bvar::LatencyRecorder> _shadow_queue_levenshtein_distance;
    // keep _storage last so it will deconstruct first
    // otherwise, load_cache_info_into_memory might crash
    // coz it will use other members of BlockFileCache
    // so join this async load thread first
    std::unique_ptr<FileCacheStorage> _storage;
    std::shared_ptr<bvar::LatencyRecorder> _lru_dump_latency_us;
};

} // namespace doris::io
