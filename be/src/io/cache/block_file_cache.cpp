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

#include "common/status.h"

#if defined(__APPLE__)
#include <sys/mount.h>
#else
#include <sys/statfs.h>
#endif

#include <chrono> // IWYU pragma: keep
#include <mutex>
#include <ranges>

#include "common/config.h"
#include "common/logging.h"
#include "cpp/sync_point.h"
#include "io/cache/file_block.h"
#include "io/cache/file_cache_common.h"
#include "io/cache/fs_file_cache_storage.h"
#include "util/time.h"
#include "vec/common/sip_hash.h"
#include "vec/common/uint128.h"

namespace doris::io {

BlockFileCache::BlockFileCache(const std::string& cache_base_path,
                               const FileCacheSettings& cache_settings)
        : _cache_base_path(cache_base_path),
          _capacity(cache_settings.capacity),
          _max_file_block_size(cache_settings.max_file_block_size),
          _max_query_cache_size(cache_settings.max_query_cache_size) {
    _cur_cache_size_metrics = std::make_shared<bvar::Status<size_t>>(_cache_base_path.c_str(),
                                                                     "file_cache_cache_size", 0);
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

    _disposable_queue = LRUQueue(cache_settings.disposable_queue_size,
                                 cache_settings.disposable_queue_elements, 60 * 60);
    _index_queue = LRUQueue(cache_settings.index_queue_size, cache_settings.index_queue_elements,
                            7 * 24 * 60 * 60);
    _normal_queue = LRUQueue(cache_settings.query_queue_size, cache_settings.query_queue_elements,
                             24 * 60 * 60);
    _ttl_queue = LRUQueue(std::numeric_limits<int>::max(), std::numeric_limits<int>::max(),
                          std::numeric_limits<int>::max());

    LOG(INFO) << fmt::format(
            "file cache path={}, disposable queue size={} elements={}, index queue size={} "
            "elements={}, query queue "
            "size={} elements={}",
            cache_base_path, cache_settings.disposable_queue_size,
            cache_settings.disposable_queue_elements, cache_settings.index_queue_size,
            cache_settings.index_queue_elements, cache_settings.query_queue_size,
            cache_settings.query_queue_elements);
}

UInt128Wrapper BlockFileCache::hash(const std::string& path) {
    uint128_t value;
    sip_hash128(path.data(), path.size(), reinterpret_cast<char*>(&value));
    return UInt128Wrapper(value);
}

std::string BlockFileCache::cache_type_to_string(FileCacheType type) {
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

FileCacheType BlockFileCache::string_to_cache_type(const std::string& str) {
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

BlockFileCache::QueryFileCacheContextHolderPtr BlockFileCache::get_query_context_holder(
        const TUniqueId& query_id) {
    std::lock_guard cache_lock(_mutex);

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
    std::lock_guard cache_lock(_mutex);
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
    std::lock_guard cache_lock(_mutex);
    return initialize_unlocked(cache_lock);
}

Status BlockFileCache::initialize_unlocked(std::lock_guard<std::mutex>& cache_lock) {
    DCHECK(!_is_initialized);
    _is_initialized = true;
    _storage = std::make_unique<FSFileCacheStorage>();
    RETURN_IF_ERROR(_storage->init(this));
    _cache_background_thread = std::thread(&BlockFileCache::run_background_operation, this);

    return Status::OK();
}

void BlockFileCache::use_cell(const FileBlockCell& cell, FileBlocks* result, bool move_iter_flag,
                              std::lock_guard<std::mutex>& cache_lock) {
    if (result) {
        result->push_back(cell.file_block);
    }

    if (cell.file_block->cache_type() != FileCacheType::TTL ||
        config::enable_ttl_cache_evict_using_lru) {
        auto& queue = get_queue(cell.file_block->cache_type());
        DCHECK(cell.queue_iterator) << "impossible";
        /// Move to the end of the queue. The iterator remains valid.
        if (move_iter_flag) {
            queue.move_to_end(*cell.queue_iterator, cache_lock);
        }
    }
    cell.update_atime();
    cell.is_deleted = false;
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
    DCHECK(!file_blocks.empty());
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
                if (config::enable_ttl_cache_evict_using_lru) {
                    auto& ttl_queue = get_queue(FileCacheType::TTL);
                    cell.queue_iterator = ttl_queue.add(
                            cell.file_block->get_hash_value(), cell.file_block->offset(),
                            cell.file_block->range().size(), cache_lock);
                } else {
                    cell.queue_iterator.reset();
                }
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
                    if (config::enable_ttl_cache_evict_using_lru) {
                        auto& ttl_queue = get_queue(FileCacheType::TTL);
                        ttl_queue.remove(cell.queue_iterator.value(), cache_lock);
                    }
                    auto& queue = get_queue(FileCacheType::NORMAL);
                    cell.queue_iterator =
                            queue.add(cell.file_block->get_hash_value(), cell.file_block->offset(),
                                      cell.file_block->range().size(), cache_lock);
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
    int64_t num_cells_all = 0;
    int64_t num_cells_to_delete = 0;
    int64_t num_files_all = 0;
    {
        std::lock_guard cache_lock(_mutex);
        if (!_async_clear_file_cache) {
            for (auto& [_, offset_to_cell] : _files) {
                ++num_files_all;
                for (auto& [_, cell] : offset_to_cell) {
                    ++num_cells_all;
                    if (cell.releasable()) {
                        cell.is_deleted = true;
                        ++num_cells_to_delete;
                    }
                }
            }
            _async_clear_file_cache = true;
        }
    }
    std::stringstream ss;
    ss << "finish clear_file_cache_async, path=" << _cache_base_path
       << " num_files_all=" << num_files_all << " num_cells_all=" << num_cells_all
       << " num_cells_to_delete=" << num_cells_to_delete;
    auto msg = ss.str();
    LOG(INFO) << msg;
    return msg;
}

void BlockFileCache::recycle_deleted_blocks() {
    using namespace std::chrono;
    static int remove_batch = 100;
    TEST_SYNC_POINT_CALLBACK("BlockFileCache::set_remove_batch", &remove_batch);
    TEST_SYNC_POINT_CALLBACK("BlockFileCache::recycle_deleted_blocks");
    std::unique_lock cache_lock(_mutex);
    auto remove_file_block = [&cache_lock, this](FileBlockCell* cell) {
        std::lock_guard segment_lock(cell->file_block->_mutex);
        remove(cell->file_block, cache_lock, segment_lock);
    };
    int i = 0;
    std::condition_variable cond;
    auto start_time = steady_clock::time_point();
    if (_async_clear_file_cache) {
        LOG_INFO("Start clear file cache async").tag("path", _cache_base_path);
        auto remove_file_block = [&cache_lock, this](FileBlockCell* cell) {
            std::lock_guard segment_lock(cell->file_block->_mutex);
            remove(cell->file_block, cache_lock, segment_lock);
        };
        static int remove_batch = 100;
        TEST_SYNC_POINT_CALLBACK("BlockFileCache::set_remove_batch", &remove_batch);
        int i = 0;
        std::condition_variable cond;
        auto iter_queue = [&](LRUQueue& queue) {
            bool end = false;
            while (queue.get_capacity(cache_lock) != 0 && !end) {
                std::vector<FileBlockCell*> cells;
                for (const auto& [entry_key, entry_offset, _] : queue) {
                    if (i == remove_batch) {
                        i = 0;
                        break;
                    }
                    auto* cell = get_cell(entry_key, entry_offset, cache_lock);
                    if (!cell) continue;
                    if (!cell->is_deleted) {
                        end = true;
                        break;
                    } else if (cell->releasable()) {
                        i++;
                        cells.push_back(cell);
                    }
                }
                std::ranges::for_each(cells, remove_file_block);
                // just for sleep
                cond.wait_for(cache_lock, std::chrono::microseconds(100));
            }
        };
        iter_queue(get_queue(FileCacheType::DISPOSABLE));
        iter_queue(get_queue(FileCacheType::NORMAL));
        iter_queue(get_queue(FileCacheType::INDEX));
    }
    if (_async_clear_file_cache || config::file_cache_ttl_valid_check_interval_second != 0) {
        std::vector<UInt128Wrapper> ttl_keys;
        ttl_keys.reserve(_key_to_time.size());
        for (auto& [key, _] : _key_to_time) {
            ttl_keys.push_back(key);
        }
        for (UInt128Wrapper& hash : ttl_keys) {
            if (i >= remove_batch) {
                // just for sleep
                cond.wait_for(cache_lock, std::chrono::microseconds(100));
                i = 0;
            }
            if (auto iter = _files.find(hash); iter != _files.end()) {
                std::vector<FileBlockCell*> cells;
                cells.reserve(iter->second.size());
                for (auto& [_, cell] : iter->second) {
                    cell.is_deleted =
                            cell.is_deleted
                                    ? true
                                    : (config::file_cache_ttl_valid_check_interval_second == 0
                                               ? false
                                               : std::chrono::duration_cast<std::chrono::seconds>(
                                                         std::chrono::steady_clock::now()
                                                                 .time_since_epoch())
                                                                         .count() -
                                                                 cell.atime >
                                                         config::file_cache_ttl_valid_check_interval_second);
                    if (!cell.is_deleted) {
                        continue;
                    } else if (cell.releasable()) {
                        cells.emplace_back(&cell);
                        i++;
                    }
                }
                std::ranges::for_each(cells, remove_file_block);
            }
        }
        if (_async_clear_file_cache) {
            _async_clear_file_cache = false;
            auto use_time = duration_cast<milliseconds>(steady_clock::time_point() - start_time);
            LOG_INFO("End clear file cache async")
                    .tag("path", _cache_base_path)
                    .tag("use_time", static_cast<int64_t>(use_time.count()));
        }
    }
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

    std::lock_guard cache_lock(_mutex);
    if (auto iter = _key_to_time.find(hash);
        context.cache_type == FileCacheType::INDEX && iter != _key_to_time.end()) {
        context.cache_type = FileCacheType::TTL;
        context.expiration_time = iter->second;
    }

    /// Get all blocks which intersect with the given range.
    auto file_blocks = get_impl(hash, context, range, cache_lock);

    if (file_blocks.empty()) {
        file_blocks = split_range_into_cells(hash, context, offset, size, FileBlock::State::EMPTY,
                                             cache_lock);
    } else {
        fill_holes_with_empty_file_blocks(file_blocks, hash, context, range, cache_lock);
    }
    DCHECK(!file_blocks.empty());
    _num_read_blocks += file_blocks.size();
    for (auto& block : file_blocks) {
        if (block->state() == FileBlock::State::DOWNLOADED) {
            _num_hit_blocks++;
        }
    }
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

    DCHECK_EQ(_files[hash].count(offset), 0)
            << "Cache already exists for hash: " << hash.to_string() << ", offset: " << offset
            << ", size: " << size
            << ".\nCurrent cache structure: " << dump_structure_unlocked(hash, cache_lock);

    auto& offsets = _files[hash];

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
    if (cell.file_block->cache_type() != FileCacheType::TTL ||
        config::enable_ttl_cache_evict_using_lru) {
        auto& queue = get_queue(cell.file_block->cache_type());
        cell.queue_iterator = queue.add(hash, offset, size, cache_lock);
    }
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
    std::lock_guard l(_mutex);
    std::vector<FileBlockCell*> trash;
    for (auto& [hash, blocks] : _files) {
        for (auto& [offset, cell] : blocks) {
            if (cell.releasable()) {
                trash.emplace_back(&cell);
            }
        }
    }
    for (auto& cell : trash) {
        FileBlockSPtr file_block = cell->file_block;
        std::lock_guard lc(cell->file_block->_mutex);
        remove(file_block, l, lc);
    }
    LOG(INFO) << "Released " << trash.size() << " blocks in file cache " << _cache_base_path;
    return trash.size();
}

BlockFileCache::LRUQueue& BlockFileCache::get_queue(FileCacheType type) {
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

const BlockFileCache::LRUQueue& BlockFileCache::get_queue(FileCacheType type) const {
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
                                        std::lock_guard<std::mutex>& cache_lock) {
    auto remove_file_block_if = [&](FileBlockCell* cell) {
        FileBlockSPtr file_block = cell->file_block;
        if (file_block) {
            std::lock_guard block_lock(file_block->_mutex);
            remove(file_block, cache_lock, block_lock);
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
                                           std::lock_guard<std::mutex>& cache_lock, bool is_ttl) {
    for (const auto& [entry_key, entry_offset, entry_size] : queue) {
        if (!is_overflow(removed_size, size, cur_cache_size, is_ttl)) {
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
        }
    }
}

bool BlockFileCache::try_reserve_for_ttl_without_lru(size_t size,
                                                     std::lock_guard<std::mutex>& cache_lock) {
    size_t removed_size = 0;
    size_t cur_cache_size = _cur_cache_size;
    auto limit = config::max_ttl_cache_ratio * _capacity;
    if ((_cur_ttl_size + size) * 100 > limit) {
        return false;
    }

    size_t normal_queue_size = _normal_queue.get_capacity(cache_lock);
    size_t disposable_queue_size = _disposable_queue.get_capacity(cache_lock);
    size_t index_queue_size = _index_queue.get_capacity(cache_lock);
    if (is_overflow(removed_size, size, cur_cache_size) && normal_queue_size == 0 &&
        disposable_queue_size == 0 && index_queue_size == 0) {
        return false;
    }
    std::vector<FileBlockCell*> to_evict;
    auto collect_eliminate_fragments = [&](LRUQueue& queue) {
        find_evict_candidates(queue, size, cur_cache_size, removed_size, to_evict, cache_lock,
                              false);
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
    remove_file_blocks(to_evict, cache_lock);
    if (is_overflow(removed_size, size, cur_cache_size)) {
        return false;
    }
    return true;
}

bool BlockFileCache::try_reserve_for_ttl(size_t size, std::lock_guard<std::mutex>& cache_lock) {
    if (try_reserve_for_ttl_without_lru(size, cache_lock)) {
        return true;
    } else if (config::enable_ttl_cache_evict_using_lru) {
        auto& queue = get_queue(FileCacheType::TTL);
        size_t removed_size = 0;
        size_t cur_cache_size = _cur_cache_size;

        std::vector<FileBlockCell*> to_evict;
        find_evict_candidates(queue, size, cur_cache_size, removed_size, to_evict, cache_lock,
                              true);
        remove_file_blocks_and_clean_time_maps(to_evict, cache_lock);

        return !is_overflow(removed_size, size, cur_cache_size);
    } else {
        return false;
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

    if (context.cache_type == FileCacheType::TTL) {
        return try_reserve_for_ttl(size, cache_lock);
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

    std::vector<BlockFileCache::LRUQueue::Iterator> ghost;
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

bool BlockFileCache::remove_if_ttl_file_unlock(const UInt128Wrapper& file_key, bool remove_directly,
                                               std::lock_guard<std::mutex>& cache_lock) {
    auto& ttl_queue = get_queue(FileCacheType::TTL);
    if (auto iter = _key_to_time.find(file_key);
        _key_to_time.find(file_key) != _key_to_time.end()) {
        if (!remove_directly) {
            for (auto& [_, cell] : _files[file_key]) {
                if (cell.file_block->cache_type() == FileCacheType::TTL) {
                    Status st = cell.file_block->update_expiration_time(0);
                    if (!st.ok()) {
                        LOG_WARNING("Failed to update expiration time to 0").error(st);
                    }
                }

                if (cell.file_block->cache_type() == FileCacheType::NORMAL) continue;
                auto st = cell.file_block->change_cache_type_between_ttl_and_others(
                        FileCacheType::NORMAL);
                if (st.ok()) {
                    if (config::enable_ttl_cache_evict_using_lru) {
                        ttl_queue.remove(cell.queue_iterator.value(), cache_lock);
                    }
                    auto& queue = get_queue(FileCacheType::NORMAL);
                    cell.queue_iterator =
                            queue.add(cell.file_block->get_hash_value(), cell.file_block->offset(),
                                      cell.file_block->range().size(), cache_lock);
                } else {
                    LOG_WARNING("Failed to change cache type to normal").error(st);
                }
            }
        } else {
            std::vector<FileBlockCell*> to_remove;
            for (auto& [_, cell] : _files[file_key]) {
                if (cell.releasable()) {
                    to_remove.push_back(&cell);
                } else {
                    cell.is_deleted = true;
                }
            }
            std::for_each(to_remove.begin(), to_remove.end(), [&](FileBlockCell* cell) {
                FileBlockSPtr file_block = cell->file_block;
                std::lock_guard block_lock(file_block->_mutex);
                remove(file_block, cache_lock, block_lock);
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

void BlockFileCache::remove_if_cached(const UInt128Wrapper& file_key) {
    std::lock_guard cache_lock(_mutex);
    bool is_ttl_file = remove_if_ttl_file_unlock(file_key, true, cache_lock);
    if (!is_ttl_file) {
        auto iter = _files.find(file_key);
        std::vector<FileBlockCell*> to_remove;
        if (iter != _files.end()) {
            for (auto& [_, cell] : iter->second) {
                if (cell.releasable()) {
                    to_remove.push_back(&cell);
                }
            }
        }
        remove_file_blocks(to_remove, cache_lock);
    }
}

std::vector<FileCacheType> BlockFileCache::get_other_cache_type(FileCacheType cur_cache_type) {
    switch (cur_cache_type) {
    case FileCacheType::INDEX:
        return {FileCacheType::DISPOSABLE, FileCacheType::NORMAL};
    case FileCacheType::NORMAL:
        return {FileCacheType::DISPOSABLE, FileCacheType::INDEX};
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
    if (cell->file_block->cache_type() != FileCacheType::TTL) {
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

bool BlockFileCache::try_reserve_from_other_queue_by_hot_interval(
        std::vector<FileCacheType> other_cache_types, size_t size, int64_t cur_time,
        std::lock_guard<std::mutex>& cache_lock) {
    size_t removed_size = 0;
    size_t cur_cache_size = _cur_cache_size;
    std::vector<FileBlockCell*> to_evict;
    for (FileCacheType cache_type : other_cache_types) {
        auto& queue = get_queue(cache_type);
        for (const auto& [entry_key, entry_offset, entry_size] : queue) {
            if (!is_overflow(removed_size, size, cur_cache_size)) {
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
            }
        }
    }
    remove_file_blocks(to_evict, cache_lock);

    return !is_overflow(removed_size, size, cur_cache_size);
}

bool BlockFileCache::is_overflow(size_t removed_size, size_t need_size, size_t cur_cache_size,
                                 bool is_ttl) const {
    bool ret = false;
    if (_disk_resource_limit_mode) {
        ret = (removed_size < need_size);
    } else {
        ret = (cur_cache_size + need_size - removed_size > _capacity);
    }
    if (is_ttl) {
        size_t ttl_threshold = config::max_ttl_cache_ratio * _capacity / 100;
        return (ret || ((cur_cache_size + need_size - removed_size) > ttl_threshold));
    }
    return ret;
}

bool BlockFileCache::try_reserve_from_other_queue_by_size(
        std::vector<FileCacheType> other_cache_types, size_t size,
        std::lock_guard<std::mutex>& cache_lock) {
    size_t removed_size = 0;
    size_t cur_cache_size = _cur_cache_size;
    std::vector<FileBlockCell*> to_evict;
    for (FileCacheType cache_type : other_cache_types) {
        auto& queue = get_queue(cache_type);
        find_evict_candidates(queue, size, cur_cache_size, removed_size, to_evict, cache_lock,
                              false);
    }
    remove_file_blocks(to_evict, cache_lock);
    return !is_overflow(removed_size, size, cur_cache_size);
}

bool BlockFileCache::try_reserve_from_other_queue(FileCacheType cur_cache_type, size_t size,
                                                  int64_t cur_time,
                                                  std::lock_guard<std::mutex>& cache_lock) {
    // disposable queue cannot reserve other queues
    if (cur_cache_type == FileCacheType::DISPOSABLE) {
        return false;
    }
    auto other_cache_types = get_other_cache_type(cur_cache_type);
    bool reserve_success = try_reserve_from_other_queue_by_hot_interval(other_cache_types, size,
                                                                        cur_time, cache_lock);
    if (reserve_success || !config::file_cache_enable_evict_from_other_queue_by_size) {
        return reserve_success;
    }
    auto& cur_queue = get_queue(cur_cache_type);
    size_t cur_queue_size = cur_queue.get_capacity(cache_lock);
    size_t cur_queue_max_size = cur_queue.get_max_size();
    // Hit the soft limit by self, cannot remove from other queues
    if (_cur_cache_size + size > _capacity && cur_queue_size + size > cur_queue_max_size) {
        return false;
    }
    return try_reserve_from_other_queue_by_size(other_cache_types, size, cache_lock);
}

bool BlockFileCache::try_reserve_for_lru(const UInt128Wrapper& hash,
                                         QueryFileCacheContextPtr query_context,
                                         const CacheContext& context, size_t offset, size_t size,
                                         std::lock_guard<std::mutex>& cache_lock) {
    int64_t cur_time = std::chrono::duration_cast<std::chrono::seconds>(
                               std::chrono::steady_clock::now().time_since_epoch())
                               .count();
    if (!try_reserve_from_other_queue(context.cache_type, size, cur_time, cache_lock)) {
        auto& queue = get_queue(context.cache_type);
        size_t removed_size = 0;
        size_t cur_cache_size = _cur_cache_size;

        std::vector<FileBlockCell*> to_evict;
        find_evict_candidates(queue, size, cur_cache_size, removed_size, to_evict, cache_lock,
                              false);
        remove_file_blocks(to_evict, cache_lock);

        if (is_overflow(removed_size, size, cur_cache_size)) {
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
void BlockFileCache::remove(FileBlockSPtr file_block, T& cache_lock, U& block_lock) {
    auto hash = file_block->get_hash_value();
    auto offset = file_block->offset();
    auto type = file_block->cache_type();
    auto expiration_time = file_block->expiration_time();
    auto* cell = get_cell(hash, offset, cache_lock);
    DCHECK(cell);
    if (cell->queue_iterator) {
        auto& queue = get_queue(file_block->cache_type());
        queue.remove(*cell->queue_iterator, cache_lock);
    }
    *_queue_evict_size_metrics[static_cast<int>(file_block->cache_type())]
            << file_block->range().size();
    *_total_evict_size_metrics << file_block->range().size();
    if (cell->file_block->state_unlock(block_lock) == FileBlock::State::DOWNLOADED) {
        FileCacheKey key;
        key.hash = hash;
        key.offset = offset;
        key.meta.type = type;
        key.meta.expiration_time = expiration_time;
        Status st = _storage->remove(key);
        if (!st.ok()) {
            LOG_WARNING("").error(st);
        }
    }
    _cur_cache_size -= file_block->range().size();
    if (FileCacheType::TTL == type) {
        _cur_ttl_size -= file_block->range().size();
    }
    auto& offsets = _files[hash];
    offsets.erase(file_block->offset());
    if (offsets.empty()) {
        _files.erase(hash);
    }
    _num_removed_blocks++;
}

size_t BlockFileCache::get_used_cache_size(FileCacheType cache_type) const {
    std::lock_guard cache_lock(_mutex);
    return get_used_cache_size_unlocked(cache_type, cache_lock);
}

size_t BlockFileCache::get_used_cache_size_unlocked(FileCacheType cache_type,
                                                    std::lock_guard<std::mutex>& cache_lock) const {
    return get_queue(cache_type).get_capacity(cache_lock);
}

size_t BlockFileCache::get_available_cache_size(FileCacheType cache_type) const {
    std::lock_guard cache_lock(_mutex);
    return get_available_cache_size_unlocked(cache_type, cache_lock);
}

size_t BlockFileCache::get_available_cache_size_unlocked(
        FileCacheType cache_type, std::lock_guard<std::mutex>& cache_lock) const {
    return get_queue(cache_type).get_max_element_size() -
           get_used_cache_size_unlocked(cache_type, cache_lock);
}

size_t BlockFileCache::get_file_blocks_num(FileCacheType cache_type) const {
    std::lock_guard cache_lock(_mutex);
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

BlockFileCache::LRUQueue::Iterator BlockFileCache::LRUQueue::add(
        const UInt128Wrapper& hash, size_t offset, size_t size,
        std::lock_guard<std::mutex>& /* cache_lock */) {
    cache_size += size;
    auto iter = queue.insert(queue.end(), FileKeyAndOffset(hash, offset, size));
    map.insert(std::make_pair(std::make_pair(hash, offset), iter));
    return iter;
}

template <class T>
    requires IsXLock<T>
void BlockFileCache::LRUQueue::remove(Iterator queue_it, T& /* cache_lock */) {
    cache_size -= queue_it->size;
    map.erase(std::make_pair(queue_it->hash, queue_it->offset));
    queue.erase(queue_it);
}

void BlockFileCache::LRUQueue::remove_all(std::lock_guard<std::mutex>& /* cache_lock */) {
    queue.clear();
    map.clear();
    cache_size = 0;
}

void BlockFileCache::LRUQueue::move_to_end(Iterator queue_it,
                                           std::lock_guard<std::mutex>& /* cache_lock */) {
    queue.splice(queue.end(), queue, queue_it);
}
bool BlockFileCache::LRUQueue::contains(const UInt128Wrapper& hash, size_t offset,
                                        std::lock_guard<std::mutex>& /* cache_lock */) const {
    return map.find(std::make_pair(hash, offset)) != map.end();
}

BlockFileCache::LRUQueue::Iterator BlockFileCache::LRUQueue::get(
        const UInt128Wrapper& hash, size_t offset,
        std::lock_guard<std::mutex>& /* cache_lock */) const {
    return map.find(std::make_pair(hash, offset))->second;
}

std::string BlockFileCache::LRUQueue::to_string(
        std::lock_guard<std::mutex>& /* cache_lock */) const {
    std::string result;
    for (const auto& [hash, offset, size] : queue) {
        if (!result.empty()) {
            result += ", ";
        }
        result += fmt::format("{}: [{}, {}]", hash.to_string(), offset, offset + size - 1);
    }
    return result;
}

std::string BlockFileCache::dump_structure(const UInt128Wrapper& hash) {
    std::lock_guard cache_lock(_mutex);
    return dump_structure_unlocked(hash, cache_lock);
}

std::string BlockFileCache::dump_structure_unlocked(const UInt128Wrapper& hash,
                                                    std::lock_guard<std::mutex>&) {
    std::stringstream result;
    const auto& cells_by_offset = _files[hash];

    for (const auto& [_, cell] : cells_by_offset) {
        result << cell.file_block->get_info_for_log() << " "
               << cache_type_to_string(cell.file_block->cache_type()) << "\n";
    }

    return result.str();
}

std::string BlockFileCache::dump_single_cache_type(const UInt128Wrapper& hash, size_t offset) {
    std::lock_guard cache_lock(_mutex);
    return dump_single_cache_type_unlocked(hash, offset, cache_lock);
}

std::string BlockFileCache::dump_single_cache_type_unlocked(const UInt128Wrapper& hash,
                                                            size_t offset,
                                                            std::lock_guard<std::mutex>&) {
    std::stringstream result;
    const auto& cells_by_offset = _files[hash];
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
            auto& new_queue = get_queue(new_type);
            cell.queue_iterator =
                    new_queue.add(hash, offset, cell.file_block->range().size(), cache_lock);
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
    int capacity_percentage = u100 / nonroot_total + (u100 % nonroot_total != 0);

    unsigned long long inode_free = stat.f_ffree;
    unsigned long long inode_total = stat.f_files;
    int inode_percentage = int(inode_free * 1.0 / inode_total * 100);
    percent->first = capacity_percentage;
    percent->second = 100 - inode_percentage;
    return 0;
}

std::string BlockFileCache::reset_capacity(size_t new_capacity) {
    using namespace std::chrono;
    int64_t space_released = 0;
    size_t old_capacity = 0;
    std::stringstream ss;
    ss << "finish reset_capacity, path=" << _cache_base_path;
    auto start_time = steady_clock::time_point();
    {
        std::lock_guard cache_lock(_mutex);
        if (new_capacity < _capacity && new_capacity < _cur_cache_size) {
            int64_t need_remove_size = _cur_cache_size - new_capacity;
            auto remove_blocks = [&](LRUQueue& queue) -> int64_t {
                int64_t queue_released = 0;
                for (const auto& [entry_key, entry_offset, entry_size] : queue) {
                    if (need_remove_size <= 0) return queue_released;
                    auto* cell = get_cell(entry_key, entry_offset, cache_lock);
                    if (!cell->releasable()) continue;
                    cell->is_deleted = true;
                    need_remove_size -= entry_size;
                    space_released += entry_size;
                    queue_released += entry_size;
                }
                return queue_released;
            };
            int64_t queue_released = remove_blocks(_disposable_queue);
            ss << " disposable_queue released " << queue_released;
            queue_released = remove_blocks(_normal_queue);
            ss << " normal_queue released " << queue_released;
            queue_released = remove_blocks(_index_queue);
            ss << " index_queue released " << queue_released;
            if (need_remove_size >= 0) {
                queue_released = 0;
                for (auto& [_, key] : _time_to_key) {
                    for (auto& [_, cell] : _files[key]) {
                        if (need_remove_size <= 0) break;
                        cell.is_deleted = true;
                        need_remove_size -= cell.file_block->range().size();
                        space_released += cell.file_block->range().size();
                        queue_released += cell.file_block->range().size();
                    }
                }
                ss << " ttl_queue released " << queue_released;
            }
            _disk_resource_limit_mode = true;
            _async_clear_file_cache = true;
            ss << " total_space_released=" << space_released;
        }
        old_capacity = _capacity;
        _capacity = new_capacity;
    }
    auto use_time = duration_cast<milliseconds>(steady_clock::time_point() - start_time);
    LOG(INFO) << "Finish tag deleted block. path=" << _cache_base_path
              << " use_time=" << static_cast<int64_t>(use_time.count());
    ss << " old_capacity=" << old_capacity << " new_capacity=" << new_capacity;
    LOG(INFO) << ss.str();
    return ss.str();
}

void BlockFileCache::check_disk_resource_limit() {
    if (_capacity > _cur_cache_size) {
        _disk_resource_limit_mode = false;
    }
    std::pair<int, int> percent;
    int ret = disk_used_percentage(_cache_base_path, &percent);
    if (ret != 0) {
        LOG_ERROR("").tag("file cache path", _cache_base_path).tag("error", strerror(errno));
        return;
    }
    auto [capacity_percentage, inode_percentage] = percent;
    auto inode_is_insufficient = [](const int& inode_percentage) {
        return inode_percentage >= config::file_cache_enter_disk_resource_limit_mode_percent;
    };
    DCHECK(capacity_percentage >= 0 && capacity_percentage <= 100);
    DCHECK(inode_percentage >= 0 && inode_percentage <= 100);
    // ATTN: due to that can be change, so if its invalid, set it to default value
    if (config::file_cache_enter_disk_resource_limit_mode_percent <=
        config::file_cache_exit_disk_resource_limit_mode_percent) {
        LOG_WARNING("config error, set to default value")
                .tag("enter", config::file_cache_enter_disk_resource_limit_mode_percent)
                .tag("exit", config::file_cache_exit_disk_resource_limit_mode_percent);
        config::file_cache_enter_disk_resource_limit_mode_percent = 90;
        config::file_cache_exit_disk_resource_limit_mode_percent = 80;
    }
    if (capacity_percentage >= config::file_cache_enter_disk_resource_limit_mode_percent ||
        inode_is_insufficient(inode_percentage)) {
        _disk_resource_limit_mode = true;
    } else if (_disk_resource_limit_mode &&
               (capacity_percentage < config::file_cache_exit_disk_resource_limit_mode_percent) &&
               (inode_percentage < config::file_cache_exit_disk_resource_limit_mode_percent)) {
        _disk_resource_limit_mode = false;
    }
    if (_disk_resource_limit_mode) {
        // log per mins
        LOG_EVERY_N(WARNING, 3) << "file cache background thread space percent="
                                << capacity_percentage << " inode percent=" << inode_percentage
                                << " is inode insufficient="
                                << inode_is_insufficient(inode_percentage)
                                << " mode run in resource limit";
    }
}

void BlockFileCache::run_background_operation() {
    int64_t interval_time_seconds = 20;
    while (!_close) {
        TEST_SYNC_POINT_CALLBACK("BlockFileCache::set_sleep_time", &interval_time_seconds);
        check_disk_resource_limit();
        {
            std::unique_lock close_lock(_close_mtx);
            _close_cv.wait_for(close_lock, std::chrono::seconds(interval_time_seconds));
            if (_close) {
                break;
            }
        }
        recycle_deleted_blocks();
        // gc
        int64_t cur_time = UnixSeconds();
        std::lock_guard cache_lock(_mutex);
        while (!_time_to_key.empty()) {
            auto begin = _time_to_key.begin();
            if (cur_time < begin->first) {
                break;
            }
            remove_if_ttl_file_unlock(begin->second, false, cache_lock);
        }

        // report
        _cur_cache_size_metrics->set_value(_cur_cache_size);
        _cur_ttl_cache_size_metrics->set_value(_cur_cache_size -
                                               _index_queue.get_capacity(cache_lock) -
                                               _normal_queue.get_capacity(cache_lock) -
                                               _disposable_queue.get_capacity(cache_lock));
        _cur_ttl_cache_lru_queue_cache_size_metrics->set_value(_ttl_queue.get_capacity(cache_lock));
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
    }
}

void BlockFileCache::modify_expiration_time(const UInt128Wrapper& hash,
                                            uint64_t new_expiration_time) {
    std::lock_guard cache_lock(_mutex);
    // 1. If new_expiration_time is equal to zero
    if (new_expiration_time == 0) {
        remove_if_ttl_file_unlock(hash, false, cache_lock);
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
        for (auto& [_, cell] : _files[hash]) {
            Status st = cell.file_block->update_expiration_time(new_expiration_time);
            if (!st.ok()) {
                LOG_WARNING("Failed to modify expiration time").error(st);
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
                if (config::enable_ttl_cache_evict_using_lru) {
                    auto& ttl_queue = get_queue(FileCacheType::TTL);
                    cell.queue_iterator =
                            ttl_queue.add(hash, cell.file_block->offset(),
                                          cell.file_block->range().size(), cache_lock);
                } else {
                    cell.queue_iterator.reset();
                }
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
    std::lock_guard cache_lock(_mutex);
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
    remove_file_blocks(to_evict, cache_lock);

    return !_disk_resource_limit_mode || removed_size >= size;
}

std::string BlockFileCache::clear_file_cache_directly() {
    using namespace std::chrono;
    std::stringstream ss;
    auto start = steady_clock::now();
    std::lock_guard cache_lock(_mutex);
    LOG_INFO("start clear_file_cache_directly").tag("path", _cache_base_path);

    auto st = global_local_filesystem()->delete_directory(_cache_base_path);
    if (!st.ok()) {
        ss << " failed to clear_file_cache_directly, path=" << _cache_base_path
           << " delete dir failed: " << st;
        LOG(WARNING) << ss.str();
        return ss.str();
    }
    st = global_local_filesystem()->create_directory(_cache_base_path);
    if (!st.ok()) {
        ss << " failed to clear_file_cache_directly, path=" << _cache_base_path
           << " create dir failed: " << st;
        LOG(WARNING) << ss.str();
        return ss.str();
    }
    int64_t num_files = _files.size();
    int64_t cache_size = _cur_cache_size;
    int64_t index_queue_size = _index_queue.get_elements_num(cache_lock);
    int64_t normal_queue_size = _normal_queue.get_elements_num(cache_lock);
    int64_t disposible_queue_size = _disposable_queue.get_elements_num(cache_lock);
    int64_t ttl_queue_size = _ttl_queue.get_elements_num(cache_lock);
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
       << " time_elapsed=" << duration_cast<milliseconds>(steady_clock::now() - start).count()
       << " num_files=" << num_files << " cache_size=" << cache_size
       << " index_queue_size=" << index_queue_size << " normal_queue_size=" << normal_queue_size
       << " disposible_queue_size=" << disposible_queue_size;
    if (config::enable_ttl_cache_evict_using_lru) {
        ss << "ttl_queue_size=" << ttl_queue_size;
    }
    auto msg = ss.str();
    LOG(INFO) << msg;
    return msg;
}

std::map<size_t, FileBlockSPtr> BlockFileCache::get_blocks_by_key(const UInt128Wrapper& hash) {
    std::map<size_t, FileBlockSPtr> offset_to_block;
    std::lock_guard cache_lock(_mutex);
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
    std::lock_guard lock(_mutex);
    if (auto iter = _files.find(hash); iter != _files.end()) {
        for (auto& [_, cell] : iter->second) {
            cell.update_atime();
        }
    };
}

template void BlockFileCache::remove(FileBlockSPtr file_block,
                                     std::lock_guard<std::mutex>& cache_lock,
                                     std::lock_guard<std::mutex>& block_lock);
} // namespace doris::io
