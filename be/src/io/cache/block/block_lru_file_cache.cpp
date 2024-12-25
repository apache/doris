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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/Cache/LRUFileCachePriority.cpp
// and modified by Doris

#include "io/cache/block/block_lru_file_cache.h"

#include <fmt/format.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>
#include <stdint.h>

#include <algorithm>

#include "common/compiler_util.h" // IWYU pragma: keep
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <cstring>
#include <filesystem>
#include <iterator>
#include <list>
#include <ostream>
#include <random>
#include <system_error>
#include <utility>

#include "common/logging.h"
#include "common/status.h"
#include "io/cache/block/block_file_cache.h"
#include "io/cache/block/block_file_cache_fwd.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "io/fs/path.h"
#include "util/doris_metrics.h"
#include "util/metrics.h"
#include "util/slice.h"
#include "util/stopwatch.hpp"
#include "vec/common/hex.h"

namespace fs = std::filesystem;

namespace doris {
namespace io {

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(file_cache_hits_ratio, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(file_cache_hits_ratio_5m, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(file_cache_hits_ratio_1h, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(file_cache_removed_elements, MetricUnit::OPERATIONS);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(file_cache_index_queue_max_size, MetricUnit::BYTES);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(file_cache_index_queue_curr_size, MetricUnit::BYTES);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(file_cache_index_queue_max_elements, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(file_cache_index_queue_curr_elements, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(file_cache_normal_queue_max_size, MetricUnit::BYTES);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(file_cache_normal_queue_curr_size, MetricUnit::BYTES);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(file_cache_normal_queue_max_elements, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(file_cache_normal_queue_curr_elements, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(file_cache_disposable_queue_max_size, MetricUnit::BYTES);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(file_cache_disposable_queue_curr_size, MetricUnit::BYTES);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(file_cache_disposable_queue_max_elements, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(file_cache_disposable_queue_curr_elements, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(file_cache_segment_reader_cache_size, MetricUnit::NOUNIT);

bvar::Adder<int64_t> g_file_cache_num_read_segments("doris_file_cache_num_read_segments");
bvar::Adder<int64_t> g_file_cache_num_hit_segments("doris_file_cache_num_hit_segments");
bvar::Window<bvar::Adder<int64_t>> g_file_cache_num_hit_segments_5m(&g_file_cache_num_hit_segments,
                                                                    300);
bvar::Window<bvar::Adder<int64_t>> g_file_cache_num_read_segments_5m(
        &g_file_cache_num_read_segments, 300);
bvar::Window<bvar::Adder<int64_t>> g_file_cache_num_hit_segments_1h(&g_file_cache_num_hit_segments,
                                                                    3600);
bvar::Window<bvar::Adder<int64_t>> g_file_cache_num_read_segments_1h(
        &g_file_cache_num_read_segments, 3600);

LRUFileCache::LRUFileCache(const std::string& cache_base_path,
                           const FileCacheSettings& cache_settings)
        : IFileCache(cache_base_path, cache_settings) {
    _disposable_queue = LRUQueue(cache_settings.disposable_queue_size,
                                 cache_settings.disposable_queue_elements, 60 * 60);
    _index_queue = LRUQueue(cache_settings.index_queue_size, cache_settings.index_queue_elements,
                            7 * 24 * 60 * 60);
    _normal_queue = LRUQueue(cache_settings.query_queue_size, cache_settings.query_queue_elements,
                             24 * 60 * 60);

    _entity = DorisMetrics::instance()->metric_registry()->register_entity(
            "lru_file_cache", {{"path", _cache_base_path}});
    _entity->register_hook(_cache_base_path, std::bind(&LRUFileCache::update_cache_metrics, this));

    DOUBLE_GAUGE_METRIC_REGISTER(_entity, file_cache_hits_ratio);
    DOUBLE_GAUGE_METRIC_REGISTER(_entity, file_cache_hits_ratio_5m);
    DOUBLE_GAUGE_METRIC_REGISTER(_entity, file_cache_hits_ratio_1h);
    INT_UGAUGE_METRIC_REGISTER(_entity, file_cache_removed_elements);

    INT_UGAUGE_METRIC_REGISTER(_entity, file_cache_index_queue_max_size);
    INT_UGAUGE_METRIC_REGISTER(_entity, file_cache_index_queue_curr_size);
    INT_UGAUGE_METRIC_REGISTER(_entity, file_cache_index_queue_max_elements);
    INT_UGAUGE_METRIC_REGISTER(_entity, file_cache_index_queue_curr_elements);

    INT_UGAUGE_METRIC_REGISTER(_entity, file_cache_normal_queue_max_size);
    INT_UGAUGE_METRIC_REGISTER(_entity, file_cache_normal_queue_curr_size);
    INT_UGAUGE_METRIC_REGISTER(_entity, file_cache_normal_queue_max_elements);
    INT_UGAUGE_METRIC_REGISTER(_entity, file_cache_normal_queue_curr_elements);

    INT_UGAUGE_METRIC_REGISTER(_entity, file_cache_disposable_queue_max_size);
    INT_UGAUGE_METRIC_REGISTER(_entity, file_cache_disposable_queue_curr_size);
    INT_UGAUGE_METRIC_REGISTER(_entity, file_cache_disposable_queue_max_elements);
    INT_UGAUGE_METRIC_REGISTER(_entity, file_cache_disposable_queue_curr_elements);
    INT_UGAUGE_METRIC_REGISTER(_entity, file_cache_segment_reader_cache_size);

    LOG(INFO) << fmt::format(
            "file cache path={}, disposable queue size={} elements={}, index queue size={} "
            "elements={}, query queue "
            "size={} elements={}",
            cache_base_path, cache_settings.disposable_queue_size,
            cache_settings.disposable_queue_elements, cache_settings.index_queue_size,
            cache_settings.index_queue_elements, cache_settings.query_queue_size,
            cache_settings.query_queue_elements);
}

Status LRUFileCache::initialize() {
    MonotonicStopWatch watch;
    watch.start();
    if (!_is_initialized) {
        if (fs::exists(_cache_base_path)) {
            // the cache already exists, try to load cache info asyncly
            _lazy_open_done = false;
            _cache_background_load_thread = std::thread([this]() {
                MonotonicStopWatch watch;
                watch.start();
                std::lock_guard<std::mutex> cache_lock(_mutex);
                Status s = load_cache_info_into_memory(cache_lock);
                if (s.ok()) {
                    _lazy_open_done = true;
                } else {
                    LOG(WARNING) << fmt::format("Failed to load cache info from {}: {}",
                                                _cache_base_path, s.to_string());
                }
                int64_t cost = watch.elapsed_time() / 1000 / 1000;
                LOG(INFO) << fmt::format(
                        "FileCache lazy load done path={}, disposable queue size={} elements={}, "
                        "index queue size={} elements={}, query queue size={} elements={}, init "
                        "cost(ms)={}",
                        _cache_base_path, _disposable_queue.get_total_cache_size(cache_lock),
                        _disposable_queue.get_elements_num(cache_lock),
                        _index_queue.get_total_cache_size(cache_lock),
                        _index_queue.get_elements_num(cache_lock),
                        _normal_queue.get_total_cache_size(cache_lock),
                        _normal_queue.get_elements_num(cache_lock), cost);
            });
        } else {
            std::error_code ec;
            fs::create_directories(_cache_base_path, ec);
            if (ec) {
                return Status::IOError("cannot create {}: {}", _cache_base_path,
                                       std::strerror(ec.value()));
            }
            RETURN_IF_ERROR(write_file_cache_version());
        }
    }
    _is_initialized = true;
    _cache_background_thread = std::thread(&LRUFileCache::run_background_operation, this);
    int64_t cost = watch.elapsed_time() / 1000 / 1000;
    LOG(INFO) << fmt::format("After initialize file cache path={}, init cost(ms)={}",
                             _cache_base_path, cost);
    return Status::OK();
}

void LRUFileCache::wait_lazy_open() {
    if (!_lazy_open_done && _cache_background_load_thread.joinable()) {
        _cache_background_load_thread.join();
    }
}

void LRUFileCache::use_cell(const FileBlockCell& cell, FileBlocks& result, bool move_iter_flag,
                            std::lock_guard<std::mutex>& cache_lock) {
    auto file_block = cell.file_block;
    auto& queue = get_queue(cell.cache_type);
    DCHECK(!(file_block->is_downloaded() &&
             fs::file_size(get_path_in_local_cache(file_block->key(), file_block->offset(),
                                                   cell.cache_type)) == 0))
            << "Cannot have zero size downloaded file segments. Current file segment: "
            << file_block->range().to_string();

    result.push_back(cell.file_block);

    DCHECK(cell.queue_iterator);
    // Move to the end of the queue. The iterator remains valid.
    if (move_iter_flag) {
        queue.move_to_end(*cell.queue_iterator, cache_lock);
    }
    cell.update_atime();
}

LRUFileCache::FileBlockCell* LRUFileCache::get_cell(const Key& key, size_t offset,
                                                    std::lock_guard<std::mutex>& /* cache_lock */) {
    auto it = _files.find(key);
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

bool LRUFileCache::need_to_move(CacheType cell_type, CacheType query_type) const {
    return query_type != CacheType::DISPOSABLE || cell_type == CacheType::DISPOSABLE ? true : false;
}

FileBlocks LRUFileCache::get_impl(const Key& key, const CacheContext& context,
                                  const FileBlock::Range& range,
                                  std::lock_guard<std::mutex>& cache_lock) {
    /// Given range = [left, right] and non-overlapping ordered set of file segments,
    /// find list [segment1, ..., segmentN] of segments which intersect with given range.
    auto it = _files.find(key);
    if (it == _files.end()) {
        return {};
    }

    const auto& file_blocks = it->second;
    if (file_blocks.empty()) {
        auto key_path = get_path_in_local_cache(key);

        _files.erase(key);

        /// Note: it is guaranteed that there is no concurrency with files deletion,
        /// because cache files are deleted only inside IFileCache and under cache lock.
        if (fs::exists(key_path)) {
            std::error_code ec;
            fs::remove_all(key_path, ec);
            if (ec) {
                LOG(WARNING) << ec.message();
            }
        }

        return {};
    }

    FileBlocks result;
    auto segment_it = file_blocks.lower_bound(range.left);
    if (segment_it == file_blocks.end()) {
        /// N - last cached segment for given file key, segment{N}.offset < range.left:
        ///   segment{N}                       segment{N}
        /// [________                         [_______]
        ///     [__________]         OR                  [________]
        ///     ^                                        ^
        ///     range.left                               range.left

        const auto& cell = file_blocks.rbegin()->second;
        if (cell.file_block->range().right < range.left) {
            return {};
        }

        use_cell(cell, result, need_to_move(cell.cache_type, context.cache_type), cache_lock);
    } else { /// segment_it <-- segmment{k}
        if (segment_it != file_blocks.begin()) {
            const auto& prev_cell = std::prev(segment_it)->second;
            const auto& prev_cell_range = prev_cell.file_block->range();

            if (range.left <= prev_cell_range.right) {
                ///   segment{k-1}  segment{k}
                ///   [________]   [_____
                ///       [___________
                ///       ^
                ///       range.left

                use_cell(prev_cell, result, need_to_move(prev_cell.cache_type, context.cache_type),
                         cache_lock);
            }
        }

        ///  segment{k} ...       segment{k-1}  segment{k}                      segment{k}
        ///  [______              [______]     [____                        [________
        ///  [_________     OR              [________      OR    [______]   ^
        ///  ^                              ^                           ^   segment{k}.offset
        ///  range.left                     range.left                  range.right

        while (segment_it != file_blocks.end()) {
            const auto& cell = segment_it->second;
            if (range.right < cell.file_block->range().left) {
                break;
            }

            use_cell(cell, result, need_to_move(cell.cache_type, context.cache_type), cache_lock);
            ++segment_it;
        }
    }

    return result;
}

FileBlocks LRUFileCache::split_range_into_cells(const Key& key, const CacheContext& context,
                                                size_t offset, size_t size, FileBlock::State state,
                                                std::lock_guard<std::mutex>& cache_lock) {
    DCHECK(size > 0);

    auto current_pos = offset;
    auto end_pos_non_included = offset + size;

    size_t current_size = 0;
    size_t remaining_size = size;

    FileBlocks file_blocks;
    while (current_pos < end_pos_non_included) {
        current_size = std::min(remaining_size, _max_file_segment_size);
        remaining_size -= current_size;
        state = try_reserve(key, context, current_pos, current_size, cache_lock, false)
                        ? state
                        : FileBlock::State::SKIP_CACHE;
        if (UNLIKELY(state == FileBlock::State::SKIP_CACHE)) {
            auto file_block =
                    std::make_shared<FileBlock>(current_pos, current_size, key, this,
                                                FileBlock::State::SKIP_CACHE, context.cache_type);
            file_blocks.push_back(std::move(file_block));
        } else {
            auto* cell = add_cell(key, context, current_pos, current_size, state, cache_lock);
            if (cell) {
                file_blocks.push_back(cell->file_block);
                cell->update_atime();
            }
        }

        current_pos += current_size;
    }

    DCHECK(file_blocks.empty() || offset + size - 1 == file_blocks.back()->range().right);
    return file_blocks;
}

void LRUFileCache::fill_holes_with_empty_file_blocks(FileBlocks& file_blocks, const Key& key,
                                                     const CacheContext& context,
                                                     const FileBlock::Range& range,
                                                     std::lock_guard<std::mutex>& cache_lock) {
    /// There are segments [segment1, ..., segmentN]
    /// (non-overlapping, non-empty, ascending-ordered) which (maybe partially)
    /// intersect with given range.

    /// It can have holes:
    /// [____________________]         -- requested range
    ///     [____]  [_]   [_________]  -- intersecting cache [segment1, ..., segmentN]
    ///
    /// For each such hole create a cell with file segment state EMPTY.

    auto it = file_blocks.begin();
    auto segment_range = (*it)->range();

    size_t current_pos;
    if (segment_range.left < range.left) {
        ///    [_______     -- requested range
        /// [_______
        /// ^
        /// segment1

        current_pos = segment_range.right + 1;
        ++it;
    } else {
        current_pos = range.left;
    }

    while (current_pos <= range.right && it != file_blocks.end()) {
        segment_range = (*it)->range();

        if (current_pos == segment_range.left) {
            current_pos = segment_range.right + 1;
            ++it;
            continue;
        }

        DCHECK(current_pos < segment_range.left);

        auto hole_size = segment_range.left - current_pos;

        file_blocks.splice(it, split_range_into_cells(key, context, current_pos, hole_size,
                                                      FileBlock::State::EMPTY, cache_lock));

        current_pos = segment_range.right + 1;
        ++it;
    }

    if (current_pos <= range.right) {
        ///   ________]     -- requested range
        ///   _____]
        ///        ^
        /// segmentN

        auto hole_size = range.right - current_pos + 1;

        file_blocks.splice(file_blocks.end(),
                           split_range_into_cells(key, context, current_pos, hole_size,
                                                  FileBlock::State::EMPTY, cache_lock));
    }
}

FileBlocksHolder LRUFileCache::get_or_set(const Key& key, size_t offset, size_t size,
                                          const CacheContext& context) {
    if (!_lazy_open_done) {
        // Cache is not ready yet
        VLOG_NOTICE << fmt::format(
                "Cache is not ready yet, skip cache for key: {}, offset: {}, size: {}.",
                key.to_string(), offset, size);
        FileBlocks file_blocks = {std::make_shared<FileBlock>(
                offset, size, key, this, FileBlock::State::SKIP_CACHE, context.cache_type)};
        return FileBlocksHolder(std::move(file_blocks));
    }

    FileBlock::Range range(offset, offset + size - 1);

    std::lock_guard cache_lock(_mutex);

    /// Get all segments which intersect with the given range.
    auto file_blocks = get_impl(key, context, range, cache_lock);

    if (file_blocks.empty()) {
        file_blocks = split_range_into_cells(key, context, offset, size, FileBlock::State::EMPTY,
                                             cache_lock);
    } else {
        fill_holes_with_empty_file_blocks(file_blocks, key, context, range, cache_lock);
    }

    DCHECK(!file_blocks.empty());
    g_file_cache_num_read_segments << file_blocks.size();
    for (auto& segment : file_blocks) {
        if (segment->state() == FileBlock::State::DOWNLOADED) {
            g_file_cache_num_hit_segments << 1;
        }
    }
    return FileBlocksHolder(std::move(file_blocks));
}

LRUFileCache::FileBlockCell* LRUFileCache::add_cell(const Key& key, const CacheContext& context,
                                                    size_t offset, size_t size,
                                                    FileBlock::State state,
                                                    std::lock_guard<std::mutex>& cache_lock) {
    /// Create a file segment cell and put it in `files` map by [key][offset].
    if (size == 0) {
        return nullptr; /// Empty files are not cached.
    }
    DCHECK(_files[key].count(offset) == 0)
            << "Cache already exists for key: " << key.to_string() << ", offset: " << offset
            << ", size: " << size
            << ".\nCurrent cache structure: " << dump_structure_unlocked(key, cache_lock);

    auto& offsets = _files[key];
    if (offsets.empty()) {
        auto key_path = get_path_in_local_cache(key);
        if (!fs::exists(key_path)) {
            std::error_code ec;
            fs::create_directories(key_path, ec);
            if (ec) {
                LOG(WARNING) << fmt::format("cannot create {}: {}", key_path,
                                            std::strerror(ec.value()));
                state = FileBlock::State::SKIP_CACHE;
            }
        }
    }

    FileBlockCell cell(
            std::make_shared<FileBlock>(offset, size, key, this, state, context.cache_type),
            context.cache_type, cache_lock);
    auto& queue = get_queue(context.cache_type);
    cell.queue_iterator = queue.add(key, offset, size, cache_lock);
    auto [it, inserted] = offsets.insert({offset, std::move(cell)});
    _cur_cache_size += size;

    DCHECK(inserted) << "Failed to insert into cache key: " << key.to_string()
                     << ", offset: " << offset << ", size: " << size;

    return &(it->second);
}

size_t LRUFileCache::try_release() {
    std::lock_guard<std::mutex> l(_mutex);
    std::vector<FileBlockCell*> trash;
    for (auto& [key, segments] : _files) {
        for (auto& [offset, cell] : segments) {
            if (cell.releasable()) {
                trash.emplace_back(&cell);
            }
        }
    }
    for (auto& cell : trash) {
        FileBlockSPtr file_block = cell->file_block;
        std::lock_guard<std::mutex> lc(cell->file_block->_mutex);
        remove(file_block, l, lc);
    }
    LOG(INFO) << "Released " << trash.size() << " segments in file cache " << _cache_base_path;
    return trash.size();
}

LRUFileCache::LRUQueue& LRUFileCache::get_queue(CacheType type) {
    switch (type) {
    case CacheType::INDEX:
        return _index_queue;
    case CacheType::DISPOSABLE:
        return _disposable_queue;
    case CacheType::NORMAL:
        return _normal_queue;
    default:
        DCHECK(false);
    }
    return _normal_queue;
}

const LRUFileCache::LRUQueue& LRUFileCache::get_queue(CacheType type) const {
    switch (type) {
    case CacheType::INDEX:
        return _index_queue;
    case CacheType::DISPOSABLE:
        return _disposable_queue;
    case CacheType::NORMAL:
        return _normal_queue;
    default:
        DCHECK(false);
    }
    return _normal_queue;
}

// 1. if dont reach query limit or dont have query limit
//     a. evict from other queue
//     b. evict from current queue
//         a.1 if the data belong write, then just evict cold data
// 2. if reach query limit
//     a. evict from query queue
//     b. evict from other queue
bool LRUFileCache::try_reserve(const Key& key, const CacheContext& context, size_t offset,
                               size_t size, std::lock_guard<std::mutex>& cache_lock,
                               bool skip_round_check) {
    auto query_context =
            _enable_file_cache_query_limit && (context.query_id.hi != 0 || context.query_id.lo != 0)
                    ? get_query_context(context.query_id, cache_lock)
                    : nullptr;
    if (!query_context) {
        return try_reserve_for_lru(key, nullptr, context, offset, size, skip_round_check,
                                   cache_lock);
    } else if (query_context->get_cache_size(cache_lock) + size <=
               query_context->get_max_cache_size()) {
        return try_reserve_for_lru(key, query_context, context, offset, size, skip_round_check,
                                   cache_lock);
    }
    int64_t cur_time = std::chrono::duration_cast<std::chrono::seconds>(
                               std::chrono::steady_clock::now().time_since_epoch())
                               .count();
    auto& queue = get_queue(context.cache_type);
    size_t removed_size = 0;
    size_t queue_size = queue.get_total_cache_size(cache_lock);
    size_t cur_cache_size = _cur_cache_size;
    size_t query_context_cache_size = query_context->get_cache_size(cache_lock);

    std::vector<IFileCache::LRUQueue::Iterator> ghost;
    std::vector<FileBlockCell*> trash;
    std::vector<FileBlockCell*> to_evict;

    size_t max_size = queue.get_max_size();
    auto is_overflow = [&] {
        return cur_cache_size + size - removed_size > _total_size ||
               (queue_size + size - removed_size > max_size) ||
               (query_context_cache_size + size - removed_size >
                query_context->get_max_cache_size());
    };

    /// Select the cache from the LRU queue held by query for expulsion.
    for (auto iter = query_context->queue().begin(); iter != query_context->queue().end(); iter++) {
        if (!is_overflow()) {
            break;
        }

        auto* cell = get_cell(iter->key, iter->offset, cache_lock);

        if (!cell) {
            /// The cache corresponding to this record may be swapped out by
            /// other queries, so it has become invalid.
            ghost.push_back(iter);
            removed_size += iter->size;
        } else {
            size_t cell_size = cell->size();
            DCHECK(iter->size == cell_size);

            if (cell->releasable()) {
                auto& file_block = cell->file_block;
                std::lock_guard segment_lock(file_block->_mutex);

                switch (file_block->_download_state) {
                case FileBlock::State::DOWNLOADED: {
                    to_evict.push_back(cell);
                    break;
                }
                default: {
                    trash.push_back(cell);
                    break;
                }
                }
                removed_size += cell_size;
            }
        }
    }

    auto remove_file_block_if = [&](FileBlockCell* cell) {
        FileBlockSPtr file_block = cell->file_block;
        if (file_block) {
            query_context->remove(file_block->key(), file_block->offset(), cache_lock);
            std::lock_guard segment_lock(file_block->_mutex);
            remove(file_block, cache_lock, segment_lock);
        }
    };

    for (auto& iter : ghost) {
        query_context->remove(iter->key, iter->offset, cache_lock);
    }

    std::for_each(trash.begin(), trash.end(), remove_file_block_if);
    std::for_each(to_evict.begin(), to_evict.end(), remove_file_block_if);

    if (is_overflow() &&
        !try_reserve_from_other_queue(context.cache_type, size, cur_time, cache_lock)) {
        return false;
    }
    query_context->reserve(key, offset, size, cache_lock);
    return true;
}

std::vector<CacheType> LRUFileCache::get_other_cache_type(CacheType cur_cache_type) {
    switch (cur_cache_type) {
    case CacheType::INDEX:
        return {CacheType::DISPOSABLE, CacheType::NORMAL};
    case CacheType::NORMAL:
        return {CacheType::DISPOSABLE, CacheType::INDEX};
    case CacheType::DISPOSABLE:
        return {CacheType::NORMAL, CacheType::INDEX};
    default:
        return {};
    }
    return {};
}

bool LRUFileCache::try_reserve_from_other_queue(CacheType cur_cache_type, size_t size,
                                                int64_t cur_time,
                                                std::lock_guard<std::mutex>& cache_lock) {
    auto other_cache_types = get_other_cache_type(cur_cache_type);
    size_t removed_size = 0;
    size_t cur_cache_size = _cur_cache_size;
    auto is_overflow = [&] { return cur_cache_size + size - removed_size > _total_size; };
    std::vector<FileBlockCell*> to_evict;
    std::vector<FileBlockCell*> trash;
    for (CacheType cache_type : other_cache_types) {
        auto& queue = get_queue(cache_type);
        for (const auto& [entry_key, entry_offset, entry_size] : queue) {
            if (!is_overflow()) {
                break;
            }
            auto* cell = get_cell(entry_key, entry_offset, cache_lock);
            DCHECK(cell) << "Cache became inconsistent. Key: " << entry_key.to_string()
                         << ", offset: " << entry_offset;

            size_t cell_size = cell->size();
            DCHECK(entry_size == cell_size);

            if (cell->atime == 0 ? true : cell->atime + queue.get_hot_data_interval() > cur_time) {
                break;
            }

            if (cell->releasable()) {
                auto& file_block = cell->file_block;

                std::lock_guard segment_lock(file_block->_mutex);

                switch (file_block->_download_state) {
                case FileBlock::State::DOWNLOADED: {
                    to_evict.push_back(cell);
                    break;
                }
                default: {
                    trash.push_back(cell);
                    break;
                }
                }

                removed_size += cell_size;
            }
        }
    }
    auto remove_file_block_if = [&](FileBlockCell* cell) {
        FileBlockSPtr file_block = cell->file_block;
        if (file_block) {
            std::lock_guard segment_lock(file_block->_mutex);
            remove(file_block, cache_lock, segment_lock);
        }
    };

    std::for_each(trash.begin(), trash.end(), remove_file_block_if);
    std::for_each(to_evict.begin(), to_evict.end(), remove_file_block_if);

    if (is_overflow()) {
        return false;
    }

    return true;
}

bool LRUFileCache::try_reserve_for_lru(const Key& key, QueryFileCacheContextPtr query_context,
                                       const CacheContext& context, size_t offset, size_t size,
                                       bool skip_round_check,
                                       std::lock_guard<std::mutex>& cache_lock) {
    int64_t cur_time = std::chrono::duration_cast<std::chrono::seconds>(
                               std::chrono::steady_clock::now().time_since_epoch())
                               .count();
    if (!try_reserve_from_other_queue(context.cache_type, size, cur_time, cache_lock)) {
        auto& queue = get_queue(context.cache_type);
        size_t removed_size = 0;
        size_t queue_element_size = queue.get_elements_num(cache_lock);
        size_t queue_size = queue.get_total_cache_size(cache_lock);
        size_t cur_cache_size = _cur_cache_size;

        size_t max_size = queue.get_max_size();
        size_t max_element_size = queue.get_max_element_size();
        auto is_overflow = [&] {
            return cur_cache_size + size - removed_size > _total_size ||
                   (queue_size + size - removed_size > max_size) ||
                   queue_element_size >= max_element_size;
        };

        std::vector<FileBlockCell*> to_evict;
        std::vector<FileBlockCell*> trash;
        size_t evict_num = 0;
        for (const auto& [entry_key, entry_offset, entry_size] : queue) {
            if (!is_overflow() ||
                (!skip_round_check && evict_num > config::file_cache_max_evict_num_per_round)) {
                break;
            }
            auto* cell = get_cell(entry_key, entry_offset, cache_lock);

            DCHECK(cell) << "Cache became inconsistent. Key: " << entry_key.to_string()
                         << ", offset: " << entry_offset;

            size_t cell_size = cell->size();
            DCHECK(entry_size == cell_size);

            if (cell->releasable()) {
                auto& file_block = cell->file_block;

                std::lock_guard segment_lock(file_block->_mutex);

                switch (file_block->_download_state) {
                case FileBlock::State::DOWNLOADED: {
                    /// Cell will actually be removed only if
                    /// we managed to reserve enough space.

                    to_evict.push_back(cell);
                    break;
                }
                default: {
                    trash.push_back(cell);
                    break;
                }
                }

                removed_size += cell_size;
                --queue_element_size;
                ++evict_num;
            }
        }

        auto remove_file_block_if = [&](FileBlockCell* cell) {
            FileBlockSPtr file_block = cell->file_block;
            if (file_block) {
                std::lock_guard segment_lock(file_block->_mutex);
                remove(file_block, cache_lock, segment_lock);
            }
        };

        if (evict_num > config::file_cache_max_evict_num_per_round) {
            LOG(INFO) << "debug evict from file cache number: " << evict_num;
        }
        std::for_each(trash.begin(), trash.end(), remove_file_block_if);
        std::for_each(to_evict.begin(), to_evict.end(), remove_file_block_if);

        if (is_overflow()) {
            return false;
        }
    }

    if (query_context) {
        query_context->reserve(key, offset, size, cache_lock);
    }
    return true;
}

void LRUFileCache::remove(FileBlockSPtr file_block, std::lock_guard<std::mutex>& cache_lock,
                          std::lock_guard<std::mutex>&) {
    auto key = file_block->key();
    auto offset = file_block->offset();
    auto type = file_block->cache_type();
    auto* cell = get_cell(key, offset, cache_lock);
    // It will be removed concurrently
    if (!cell) {
        return;
    }

    if (cell->queue_iterator) {
        auto& queue = get_queue(file_block->cache_type());
        queue.remove(*cell->queue_iterator, cache_lock);
    }
    _cur_cache_size -= file_block->range().size();
    auto& offsets = _files[file_block->key()];
    offsets.erase(file_block->offset());

    auto cache_file_path = get_path_in_local_cache(key, offset, type);
    if (std::filesystem::exists(cache_file_path)) {
        std::error_code ec;
        std::filesystem::remove(cache_file_path, ec);
        if (ec) {
            LOG(ERROR) << ec.message();
        }
    }
    _num_removed_segments++;
    if (offsets.empty()) {
        auto key_path = get_path_in_local_cache(key);
        _files.erase(key);
        std::error_code ec;
        std::filesystem::remove_all(key_path, ec);
        if (ec) {
            LOG(ERROR) << ec.message();
        }
    }
}

Status LRUFileCache::load_cache_info_into_memory(std::lock_guard<std::mutex>& cache_lock) {
    /// version 1.0: cache_base_path / key / offset
    /// version 2.0: cache_base_path / key_prefix / key / offset
    if (USE_CACHE_VERSION2 && read_file_cache_version() != "2.0") {
        // move directories format as version 2.0
        std::error_code ec;
        fs::directory_iterator key_it {_cache_base_path, ec};
        if (ec) {
            return Status::InternalError("Failed to list dir {}: {}", _cache_base_path,
                                         ec.message());
        }
        for (; key_it != fs::directory_iterator(); ++key_it) {
            if (key_it->is_directory()) {
                std::string cache_key = key_it->path().filename().native();
                if (cache_key.size() > KEY_PREFIX_LENGTH) {
                    std::string key_prefix =
                            fs::path(_cache_base_path) / cache_key.substr(0, KEY_PREFIX_LENGTH);
                    if (!fs::exists(key_prefix)) {
                        std::error_code ec;
                        fs::create_directories(key_prefix, ec);
                        if (ec) {
                            LOG(WARNING) << "Failed to create new version cached directory: "
                                         << ec.message();
                            continue;
                        }
                    }
                    std::error_code ec;
                    std::filesystem::rename(key_it->path(), key_prefix / cache_key, ec);
                    if (ec) {
                        LOG(WARNING)
                                << "Failed to move old version cached directory: " << ec.message();
                    }
                }
            }
        }
        if (!write_file_cache_version().ok()) {
            LOG(WARNING) << "Failed to write version hints for file cache";
        }
    }

    Key key;
    uint64_t offset = 0;
    size_t size = 0;
    std::vector<std::pair<Key, size_t>> queue_entries;
    std::vector<std::string> need_to_check_if_empty_dir;
    Status st = Status::OK();
    size_t scan_file_num = 0;
    auto scan_file_cache = [&](fs::directory_iterator& key_it) {
        for (; key_it != fs::directory_iterator(); ++key_it) {
            key = Key(
                    vectorized::unhex_uint<uint128_t>(key_it->path().filename().native().c_str()));
            CacheContext context;
            context.query_id = TUniqueId();
            std::error_code ec;
            fs::directory_iterator offset_it {key_it->path(), ec};
            if (ec) [[unlikely]] {
                LOG(WARNING) << "filesystem error, failed to iterate directory, file="
                             << key_it->path() << " error=" << ec.message();
                continue;
            }
            for (; offset_it != fs::directory_iterator(); ++offset_it) {
                auto offset_with_suffix = offset_it->path().filename().native();
                auto delim_pos = offset_with_suffix.find('_');
                CacheType cache_type = CacheType::NORMAL;
                bool parsed = true;
                try {
                    if (delim_pos == std::string::npos) {
                        offset = stoull(offset_with_suffix);
                    } else {
                        offset = stoull(offset_with_suffix.substr(0, delim_pos));
                        std::string suffix = offset_with_suffix.substr(delim_pos + 1);
                        // not need persistent any more
                        if (suffix == "persistent") {
                            std::filesystem::remove(offset_it->path(), ec);
                            if (ec) {
                                st = Status::IOError(ec.message());
                                break;
                            }
                            continue;
                        } else {
                            cache_type = string_to_cache_type(suffix);
                        }
                    }
                } catch (...) {
                    parsed = false;
                }

                if (!parsed) {
                    st = Status::IOError("Unexpected file: {}", offset_it->path().native());
                    break;
                }
                size = offset_it->file_size(ec);
                if (ec) [[unlikely]] {
                    st = Status::IOError(ec.message());
                    break;
                }

                if (size == 0) {
                    fs::remove(offset_it->path(), ec);
                    if (ec) {
                        LOG(WARNING) << ec.message();
                    }
                    continue;
                }
                context.cache_type = cache_type;
                if (try_reserve(key, context, offset, size, cache_lock, true)) {
                    add_cell(key, context, offset, size, FileBlock::State::DOWNLOADED, cache_lock);
                    queue_entries.emplace_back(key, offset);
                } else {
                    std::filesystem::remove(offset_it->path(), ec);
                    if (ec) {
                        st = Status::IOError(ec.message());
                    }
                    need_to_check_if_empty_dir.push_back(key_it->path());
                }
                scan_file_num += 1;
                if (scan_file_num % config::async_file_cache_init_file_num_interval == 0) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(
                            config::async_file_cache_init_sleep_interval_ms));
                }
            }
        }
    };

    std::error_code ec;
    if constexpr (USE_CACHE_VERSION2) {
        fs::directory_iterator key_prefix_it {_cache_base_path, ec};
        if (ec) [[unlikely]] {
            return Status::InternalError("Failed to list dir {}: {}", _cache_base_path,
                                         ec.message());
        }
        for (; key_prefix_it != fs::directory_iterator(); ++key_prefix_it) {
            if (!key_prefix_it->is_directory()) {
                // maybe version hits file
                continue;
            }
            if (key_prefix_it->path().filename().native().size() != KEY_PREFIX_LENGTH) {
                LOG(WARNING) << "Unknown directory " << key_prefix_it->path().native()
                             << ", try to remove it";
                std::error_code ec;
                std::filesystem::remove(key_prefix_it->path(), ec);
                if (ec) {
                    LOG(WARNING) << "filesystem error, failed to remove file, file="
                                 << key_prefix_it->path() << " error=" << ec.message();
                }
                continue;
            }
            fs::directory_iterator key_it {key_prefix_it->path(), ec};
            if (ec) [[unlikely]] {
                return Status::IOError(ec.message());
            }
            scan_file_cache(key_it);
        }
    } else {
        fs::directory_iterator key_it {_cache_base_path, ec};
        if (ec) [[unlikely]] {
            return Status::IOError(ec.message());
        }
        scan_file_cache(key_it);
    }
    if (!st) {
        return st;
    }

    std::for_each(need_to_check_if_empty_dir.cbegin(), need_to_check_if_empty_dir.cend(),
                  [](auto& dir) {
                      std::error_code ec;
                      if (std::filesystem::is_empty(dir, ec) && !ec) {
                          std::filesystem::remove(dir, ec);
                      }
                      if (ec) {
                          LOG(ERROR) << ec.message();
                      }
                  });

    /// Shuffle cells to have random order in LRUQueue as at startup all cells have the same priority.
    auto rng = std::default_random_engine(
            static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::shuffle(queue_entries.begin(), queue_entries.end(), rng);
    for (const auto& [key, offset] : queue_entries) {
        auto* cell = get_cell(key, offset, cache_lock);
        if (cell) {
            auto& queue = get_queue(cell->cache_type);
            queue.move_to_end(*cell->queue_iterator, cache_lock);
        }
    }
    return st;
}

Status LRUFileCache::write_file_cache_version() const {
    if constexpr (USE_CACHE_VERSION2) {
        std::string version_path = get_version_path();
        Slice version("2.0");
        FileWriterPtr version_writer;
        RETURN_IF_ERROR(global_local_filesystem()->create_file(version_path, &version_writer));
        RETURN_IF_ERROR(version_writer->append(version));
        return version_writer->close();
    }
    return Status::OK();
}

std::string LRUFileCache::read_file_cache_version() const {
    std::string version_path = get_version_path();
    const FileSystemSPtr& fs = global_local_filesystem();
    bool exists = false;
    static_cast<void>(fs->exists(version_path, &exists));
    if (!exists) {
        return "1.0";
    }
    FileReaderSPtr version_reader;
    int64_t file_size = -1;
    static_cast<void>(fs->file_size(version_path, &file_size));
    char version[file_size];

    static_cast<void>(fs->open_file(version_path, &version_reader));
    size_t bytes_read = 0;
    static_cast<void>(version_reader->read_at(0, Slice(version, file_size), &bytes_read));
    static_cast<void>(version_reader->close());
    return std::string(version, bytes_read);
}

size_t LRUFileCache::get_used_cache_size(CacheType cache_type) const {
    std::lock_guard cache_lock(_mutex);
    return get_used_cache_size_unlocked(cache_type, cache_lock);
}

size_t LRUFileCache::get_used_cache_size_unlocked(CacheType cache_type,
                                                  std::lock_guard<std::mutex>& cache_lock) const {
    return get_queue(cache_type).get_total_cache_size(cache_lock);
}

size_t LRUFileCache::get_available_cache_size(CacheType cache_type) const {
    std::lock_guard cache_lock(_mutex);
    return get_available_cache_size_unlocked(cache_type, cache_lock);
}

size_t LRUFileCache::get_available_cache_size_unlocked(
        CacheType cache_type, std::lock_guard<std::mutex>& cache_lock) const {
    return get_queue(cache_type).get_max_element_size() -
           get_used_cache_size_unlocked(cache_type, cache_lock);
}

size_t LRUFileCache::get_file_segments_num(CacheType cache_type) const {
    std::lock_guard cache_lock(_mutex);
    return get_file_segments_num_unlocked(cache_type, cache_lock);
}

size_t LRUFileCache::get_file_segments_num_unlocked(CacheType cache_type,
                                                    std::lock_guard<std::mutex>& cache_lock) const {
    return get_queue(cache_type).get_elements_num(cache_lock);
}

void LRUFileCache::change_cache_type(const IFileCache::Key& key, size_t offset, CacheType new_type,
                                     std::lock_guard<std::mutex>& cache_lock) {
    if (auto iter = _files.find(key); iter != _files.end()) {
        auto& file_blocks = iter->second;
        if (auto cell_it = file_blocks.find(offset); cell_it != file_blocks.end()) {
            FileBlockCell& cell = cell_it->second;
            auto& cur_queue = get_queue(cell.cache_type);
            cell.cache_type = new_type;
            DCHECK(cell.queue_iterator.has_value());
            cur_queue.remove(*cell.queue_iterator, cache_lock);
            auto& new_queue = get_queue(new_type);
            cell.queue_iterator =
                    new_queue.add(key, offset, cell.file_block->range().size(), cache_lock);
        }
    }
}

LRUFileCache::FileBlockCell::FileBlockCell(FileBlockSPtr file_block, CacheType cache_type,
                                           std::lock_guard<std::mutex>& cache_lock)
        : file_block(file_block), cache_type(cache_type) {
    /**
     * Cell can be created with either DOWNLOADED or EMPTY file segment's state.
     * File segment acquires DOWNLOADING state and creates LRUQueue iterator on first
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
}

IFileCache::LRUQueue::Iterator IFileCache::LRUQueue::add(
        const IFileCache::Key& key, size_t offset, size_t size,
        std::lock_guard<std::mutex>& /* cache_lock */) {
    cache_size += size;
    return queue.insert(queue.end(), FileKeyAndOffset(key, offset, size));
}

void IFileCache::LRUQueue::remove(Iterator queue_it,
                                  std::lock_guard<std::mutex>& /* cache_lock */) {
    cache_size -= queue_it->size;
    queue.erase(queue_it);
}

void IFileCache::LRUQueue::remove_all(std::lock_guard<std::mutex>& /* cache_lock */) {
    queue.clear();
    cache_size = 0;
}

void IFileCache::LRUQueue::move_to_end(Iterator queue_it,
                                       std::lock_guard<std::mutex>& /* cache_lock */) {
    queue.splice(queue.end(), queue, queue_it);
}
bool IFileCache::LRUQueue::contains(const IFileCache::Key& key, size_t offset,
                                    std::lock_guard<std::mutex>& /* cache_lock */) const {
    /// This method is used for assertions in debug mode.
    /// So we do not care about complexity here.
    for (const auto& [entry_key, entry_offset, size] : queue) {
        if (key == entry_key && offset == entry_offset) {
            return true;
        }
    }
    return false;
}

std::string IFileCache::LRUQueue::to_string(std::lock_guard<std::mutex>& /* cache_lock */) const {
    std::string result;
    for (const auto& [key, offset, size] : queue) {
        if (!result.empty()) {
            result += ", ";
        }
        result += fmt::format("{}: [{}, {}]", key.to_string(), offset, offset + size - 1);
    }
    return result;
}

std::string LRUFileCache::dump_structure(const Key& key) {
    std::lock_guard cache_lock(_mutex);
    return dump_structure_unlocked(key, cache_lock);
}

std::string LRUFileCache::dump_structure_unlocked(const Key& key, std::lock_guard<std::mutex>&) {
    std::stringstream result;
    const auto& cells_by_offset = _files[key];

    for (const auto& [_, cell] : cells_by_offset) {
        result << cell.file_block->get_info_for_log() << " "
               << cache_type_to_string(cell.cache_type) << "\n";
    }

    return result.str();
}

void LRUFileCache::run_background_operation() {
    int64_t interval_time_seconds = 20;
    while (!_close) {
        std::this_thread::sleep_for(std::chrono::seconds(interval_time_seconds));
        // report
        _cur_size_metrics->set_value(_cur_cache_size);
    }
}

void LRUFileCache::update_cache_metrics() const {
    std::lock_guard<std::mutex> l(_mutex);
    double hit_ratio = 0;
    double hit_ratio_5m = 0;
    double hit_ratio_1h = 0;
    if (g_file_cache_num_read_segments.get_value() > 0) {
        hit_ratio = ((double)g_file_cache_num_hit_segments.get_value()) /
                    ((double)g_file_cache_num_read_segments.get_value());
    }
    if (g_file_cache_num_read_segments_5m.get_value() > 0) {
        hit_ratio_5m = ((double)g_file_cache_num_hit_segments_5m.get_value()) /
                       ((double)g_file_cache_num_read_segments_5m.get_value());
    } else {
        hit_ratio_5m = 0.0;
    }
    if (g_file_cache_num_read_segments_1h.get_value() > 0) {
        hit_ratio_1h = ((double)g_file_cache_num_hit_segments_1h.get_value()) /
                       ((double)g_file_cache_num_read_segments_1h.get_value());
    } else {
        hit_ratio_1h = 0.0;
    }

    file_cache_hits_ratio->set_value(hit_ratio);
    file_cache_hits_ratio_5m->set_value(hit_ratio_5m);
    file_cache_hits_ratio_1h->set_value(hit_ratio_1h);
    file_cache_removed_elements->set_value(_num_removed_segments);

    file_cache_index_queue_max_size->set_value(_index_queue.get_max_size());
    file_cache_index_queue_curr_size->set_value(_index_queue.get_total_cache_size(l));
    file_cache_index_queue_max_elements->set_value(_index_queue.get_max_element_size());
    file_cache_index_queue_curr_elements->set_value(_index_queue.get_elements_num(l));

    file_cache_normal_queue_max_size->set_value(_normal_queue.get_max_size());
    file_cache_normal_queue_curr_size->set_value(_normal_queue.get_total_cache_size(l));
    file_cache_normal_queue_max_elements->set_value(_normal_queue.get_max_element_size());
    file_cache_normal_queue_curr_elements->set_value(_normal_queue.get_elements_num(l));

    file_cache_disposable_queue_max_size->set_value(_disposable_queue.get_max_size());
    file_cache_disposable_queue_curr_size->set_value(_disposable_queue.get_total_cache_size(l));
    file_cache_disposable_queue_max_elements->set_value(_disposable_queue.get_max_element_size());
    file_cache_disposable_queue_curr_elements->set_value(_disposable_queue.get_elements_num(l));
    file_cache_segment_reader_cache_size->set_value(IFileCache::file_reader_cache_size());
}

std::map<std::string, double> LRUFileCache::get_stats() {
    update_cache_metrics();
    std::map<std::string, double> stats;
    stats["hits_ratio"] = (double)file_cache_hits_ratio->value();
    stats["hits_ratio_5m"] = (double)file_cache_hits_ratio_5m->value();
    stats["hits_ratio_1h"] = (double)file_cache_hits_ratio_1h->value();

    stats["index_queue_max_size"] = (double)file_cache_index_queue_max_size->value();
    stats["index_queue_curr_size"] = (double)file_cache_index_queue_curr_size->value();
    stats["index_queue_max_elements"] = (double)file_cache_index_queue_max_elements->value();
    stats["index_queue_curr_elements"] = (double)file_cache_index_queue_curr_elements->value();

    stats["normal_queue_max_size"] = (double)file_cache_normal_queue_max_size->value();
    stats["normal_queue_curr_size"] = (double)file_cache_normal_queue_curr_size->value();
    stats["normal_queue_max_elements"] = (double)file_cache_normal_queue_max_elements->value();
    stats["normal_queue_curr_elements"] = (double)file_cache_normal_queue_curr_elements->value();

    stats["disposable_queue_max_size"] = (double)file_cache_disposable_queue_max_size->value();
    stats["disposable_queue_curr_size"] = (double)file_cache_disposable_queue_curr_size->value();
    stats["disposable_queue_max_elements"] =
            (double)file_cache_disposable_queue_max_elements->value();
    stats["disposable_queue_curr_elements"] =
            (double)file_cache_disposable_queue_curr_elements->value();

    stats["segment_reader_cache_size"] = (double)IFileCache::file_reader_cache_size();

    return stats;
}

} // namespace io
} // namespace doris
