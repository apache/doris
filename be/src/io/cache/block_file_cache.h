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

#include <memory>
#include <mutex>
#include <optional>
#include <thread>

#include "io/cache/file_block.h"
#include "io/cache/file_cache_common.h"
#include "io/cache/file_cache_storage.h"

namespace doris::io {

template <class Lock>
concept IsXLock = std::same_as<Lock, std::lock_guard<std::mutex>> ||
                  std::same_as<Lock, std::unique_lock<std::mutex>>;

class FSFileCacheStorage;

// The BlockFileCache is responsible for the management of the blocks
// The current strategies are lru and ttl.
class BlockFileCache {
    friend class FSFileCacheStorage;
    friend class FileBlock;
    friend struct FileBlocksHolder;

public:
    static std::string cache_type_to_string(FileCacheType type);
    static FileCacheType string_to_cache_type(const std::string& str);
    // hash the file_name to uint128
    static UInt128Wrapper hash(const std::string& path);

    BlockFileCache(const std::string& cache_base_path, const FileCacheSettings& cache_settings);

    ~BlockFileCache() {
        {
            std::lock_guard lock(_close_mtx);
            _close = true;
        }
        _close_cv.notify_all();
        if (_cache_background_thread.joinable()) {
            _cache_background_thread.join();
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
    std::map<size_t, FileBlockSPtr> get_blocks_by_key(const UInt128Wrapper& hash);
    /// For debug.
    std::string dump_structure(const UInt128Wrapper& hash);

    [[nodiscard]] size_t get_used_cache_size(FileCacheType type) const;

    [[nodiscard]] size_t get_file_blocks_num(FileCacheType type) const;

    // change the block cache type
    void change_cache_type(const UInt128Wrapper& hash, size_t offset, FileCacheType new_type,
                           std::lock_guard<std::mutex>& cache_lock);

    // remove all blocks that belong to the key
    void remove_if_cached(const UInt128Wrapper& key);

    // modify the expiration time about the key
    void modify_expiration_time(const UInt128Wrapper& key, uint64_t new_expiration_time);

    // Shrink the block size. old_size is always larged than new_size.
    void reset_range(const UInt128Wrapper&, size_t offset, size_t old_size, size_t new_size,
                     std::lock_guard<std::mutex>& cache_lock);

    // get the hotest blocks message by key
    // The tuple is composed of <offset, size, cache_type, expiration_time>
    [[nodiscard]] std::vector<std::tuple<size_t, size_t, FileCacheType, uint64_t>>
    get_hot_blocks_meta(const UInt128Wrapper& hash) const;

    [[nodiscard]] bool get_lazy_open_success() const { return _lazy_open_done; }

    BlockFileCache& operator=(const BlockFileCache&) = delete;
    BlockFileCache(const BlockFileCache&) = delete;

    // try to reserve the new space for the new block if the cache is full
    bool try_reserve(const UInt128Wrapper& hash, const CacheContext& context, size_t offset,
                     size_t size, std::lock_guard<std::mutex>& cache_lock);

    void update_ttl_atime(const UInt128Wrapper& hash);

    class LRUQueue {
    public:
        LRUQueue() = default;
        LRUQueue(size_t max_size, size_t max_element_size, int64_t hot_data_interval)
                : max_size(max_size),
                  max_element_size(max_element_size),
                  hot_data_interval(hot_data_interval) {}

        struct HashFileKeyAndOffset {
            std::size_t operator()(const std::pair<UInt128Wrapper, size_t>& pair) const {
                return KeyHash()(pair.first) + pair.second;
            }
        };

        struct FileKeyAndOffset {
            UInt128Wrapper hash;
            size_t offset;
            size_t size;

            FileKeyAndOffset(const UInt128Wrapper& hash, size_t offset, size_t size)
                    : hash(hash), offset(offset), size(size) {}
        };

        using Iterator = typename std::list<FileKeyAndOffset>::iterator;

        size_t get_max_size() const { return max_size; }
        size_t get_max_element_size() const { return max_element_size; }

        template <class T>
            requires IsXLock<T>
        size_t get_capacity(T& /* cache_lock */) const {
            return cache_size;
        }

        size_t get_elements_num(std::lock_guard<std::mutex>& /* cache_lock */) const {
            return queue.size();
        }

        Iterator add(const UInt128Wrapper& hash, size_t offset, size_t size,
                     std::lock_guard<std::mutex>& cache_lock);
        template <class T>
            requires IsXLock<T>
        void remove(Iterator queue_it, T& cache_lock);

        void move_to_end(Iterator queue_it, std::lock_guard<std::mutex>& cache_lock);

        std::string to_string(std::lock_guard<std::mutex>& cache_lock) const;

        bool contains(const UInt128Wrapper& hash, size_t offset,
                      std::lock_guard<std::mutex>& cache_lock) const;

        Iterator begin() { return queue.begin(); }

        Iterator end() { return queue.end(); }

        void remove_all(std::lock_guard<std::mutex>& cache_lock);

        Iterator get(const UInt128Wrapper& hash, size_t offset,
                     std::lock_guard<std::mutex>& /* cache_lock */) const;

        int64_t get_hot_data_interval() const { return hot_data_interval; }

        void clear(std::lock_guard<std::mutex>& cache_lock) {
            queue.clear();
            map.clear();
            cache_size = 0;
        }

        size_t max_size;
        size_t max_element_size;
        std::list<FileKeyAndOffset> queue;
        std::unordered_map<std::pair<UInt128Wrapper, size_t>, Iterator, HashFileKeyAndOffset> map;
        size_t cache_size = 0;
        int64_t hot_data_interval {0};
    };

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

private:
    struct FileBlockCell {
        FileBlockSPtr file_block;
        /// Iterator is put here on first reservation attempt, if successful.
        std::optional<LRUQueue::Iterator> queue_iterator;

        mutable int64_t atime {0};
        mutable bool is_deleted {false};
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

    BlockFileCache::LRUQueue& get_queue(FileCacheType type);
    const BlockFileCache::LRUQueue& get_queue(FileCacheType type) const;

    template <class T, class U>
        requires IsXLock<T> && IsXLock<U>
    void remove(FileBlockSPtr file_block, T& cache_lock, U& segment_lock);

    FileBlocks get_impl(const UInt128Wrapper& hash, const CacheContext& context,
                        const FileBlock::Range& range, std::lock_guard<std::mutex>& cache_lock);

    template <class T>
        requires IsXLock<T>
    FileBlockCell* get_cell(const UInt128Wrapper& hash, size_t offset, T& cache_lock);

    FileBlockCell* add_cell(const UInt128Wrapper& hash, const CacheContext& context, size_t offset,
                            size_t size, FileBlock::State state,
                            std::lock_guard<std::mutex>& cache_lock);

    Status initialize_unlocked(std::lock_guard<std::mutex>& cache_lock);

    void use_cell(const FileBlockCell& cell, FileBlocks* result, bool not_need_move,
                  std::lock_guard<std::mutex>& cache_lock);

    bool try_reserve_for_lru(const UInt128Wrapper& hash, QueryFileCacheContextPtr query_context,
                             const CacheContext& context, size_t offset, size_t size,
                             std::lock_guard<std::mutex>& cache_lock);

    bool try_reserve_for_lazy_load(size_t size, std::lock_guard<std::mutex>& cache_lock);

    std::vector<FileCacheType> get_other_cache_type(FileCacheType cur_cache_type);

    bool try_reserve_from_other_queue(FileCacheType cur_cache_type, size_t offset, int64_t cur_time,
                                      std::lock_guard<std::mutex>& cache_lock);

    size_t get_available_cache_size(FileCacheType cache_type) const;

    bool try_reserve_for_ttl(size_t size, std::lock_guard<std::mutex>& cache_lock);

    bool try_reserve_for_ttl_without_lru(size_t size, std::lock_guard<std::mutex>& cache_lock);

    FileBlocks split_range_into_cells(const UInt128Wrapper& hash, const CacheContext& context,
                                      size_t offset, size_t size, FileBlock::State state,
                                      std::lock_guard<std::mutex>& cache_lock);

    std::string dump_structure_unlocked(const UInt128Wrapper& hash,
                                        std::lock_guard<std::mutex>& cache_lock);

    void fill_holes_with_empty_file_blocks(FileBlocks& file_blocks, const UInt128Wrapper& hash,
                                           const CacheContext& context,
                                           const FileBlock::Range& range,
                                           std::lock_guard<std::mutex>& cache_lock);

    size_t get_used_cache_size_unlocked(FileCacheType type,
                                        std::lock_guard<std::mutex>& cache_lock) const;

    void check_disk_resource_limit(const std::string& path);

    size_t get_available_cache_size_unlocked(FileCacheType type,
                                             std::lock_guard<std::mutex>& cache_lock) const;

    size_t get_file_blocks_num_unlocked(FileCacheType type,
                                        std::lock_guard<std::mutex>& cache_lock) const;

    bool need_to_move(FileCacheType cell_type, FileCacheType query_type) const;

    bool remove_if_ttl_file_unlock(const UInt128Wrapper& file_key, bool remove_directly,
                                   std::lock_guard<std::mutex>&);

    void run_background_operation();

    void recycle_deleted_blocks();

    bool try_reserve_from_other_queue_by_hot_interval(std::vector<FileCacheType> other_cache_types,
                                                      size_t size, int64_t cur_time,
                                                      std::lock_guard<std::mutex>& cache_lock);

    bool try_reserve_from_other_queue_by_size(std::vector<FileCacheType> other_cache_types,
                                              size_t size, std::lock_guard<std::mutex>& cache_lock);

    bool is_overflow(size_t removed_size, size_t need_size, size_t cur_cache_size) const;

    void remove_file_blocks(std::vector<FileBlockCell*>&, std::lock_guard<std::mutex>&);

    void remove_file_blocks_and_clean_time_maps(std::vector<FileBlockCell*>&,
                                                std::lock_guard<std::mutex>&);

    void find_evict_candidates(LRUQueue& queue, size_t size, size_t cur_cache_size,
                               size_t& removed_size, std::vector<FileBlockCell*>& to_evict,
                               std::lock_guard<std::mutex>& cache_lock);
    // info
    std::string _cache_base_path;
    size_t _capacity = 0;
    size_t _max_file_block_size = 0;
    size_t _max_query_cache_size = 0;

    mutable std::mutex _mutex;
    std::unique_ptr<FileCacheStorage> _storage;
    bool _close {false};
    std::mutex _close_mtx;
    std::condition_variable _close_cv;
    std::thread _cache_background_thread;
    std::atomic_bool _lazy_open_done {false};
    bool _async_clear_file_cache {false};
    // disk space or inode is less than the specified value
    bool _disk_resource_limit_mode {false};
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

    // metrics
    size_t _num_read_blocks = 0;
    size_t _num_hit_blocks = 0;
    size_t _num_removed_blocks = 0;
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
};

} // namespace doris::io
