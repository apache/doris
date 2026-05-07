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

#include "io/cache/cache_block_aware_prefetch_remote_reader.h"

#include <algorithm>
#include <limits>
#include <unordered_set>
#include <utility>

#include "common/logging.h"

namespace doris::io {

namespace {

CacheBlockRange cache_block_range(size_t block_id, size_t cache_block_size) {
    return {block_id * cache_block_size, cache_block_size};
}

bool trigger_file_range_is_before_current_offset(const FileAccessRange& range,
                                                 size_t current_file_offset) {
    if (current_file_offset <= range.offset) {
        return false;
    }
    return current_file_offset - range.offset >= range.size;
}

bool same_trigger_file_range(const FileAccessRange& lhs, const FileAccessRange& rhs) {
    return lhs.offset == rhs.offset && lhs.size == rhs.size;
}

} // namespace

CacheBlockAwarePrefetchRemoteReader::CacheBlockAwarePrefetchRemoteReader(
        FileReaderSPtr remote_file_reader, const FileReaderOptions& opts)
        : CachedRemoteFileReader(std::move(remote_file_reader), opts) {}

Status CacheBlockAwarePrefetchRemoteReader::set_read_pattern(
        CacheBlockReadPattern pattern, const CacheBlockPrefetchPolicy& policy) {
    if (policy.max_prefetch_blocks == 0 || policy.cache_block_size == 0) {
        return Status::InvalidArgument(
                "cache block prefetch policy requires positive window and block size");
    }

    auto plan = detail::CacheBlockPrefetchPlan::from_read_pattern(std::move(pattern),
                                                                  policy.cache_block_size);

    std::lock_guard lock(_pattern_mutex);
    if (plan.empty()) {
        _prefetch_cursor.reset();
        return Status::OK();
    }
    _prefetch_cursor.emplace(std::move(plan), policy.max_prefetch_blocks);
    return Status::OK();
}

void CacheBlockAwarePrefetchRemoteReader::clear_read_pattern() {
    std::lock_guard lock(_pattern_mutex);
    _prefetch_cursor.reset();
}

void CacheBlockAwarePrefetchRemoteReader::async_touch_initial_window(const IOContext* io_ctx) {
    std::vector<CacheBlockRange> ranges;
    {
        std::lock_guard lock(_pattern_mutex);
        if (!_prefetch_cursor.has_value()) {
            return;
        }
        ranges = _prefetch_cursor->next_initial_touch_ranges();
    }
    _async_touch_ranges(std::move(ranges), io_ctx);
}

bool CacheBlockAwarePrefetchRemoteReader::has_read_pattern() const {
    std::lock_guard lock(_pattern_mutex);
    return _prefetch_cursor.has_value();
}

Status CacheBlockAwarePrefetchRemoteReader::read_at_impl(size_t offset, Slice result,
                                                         size_t* bytes_read,
                                                         const IOContext* io_ctx) {
    // Normal foreground reads drive the prefetch window by the real file offset
    // that PageIO is about to read. Dry-run reads are submitted by
    // CachedRemoteFileReader::async_touch_local_cache() to warm the file cache;
    // they must not recursively schedule more prefetch work.
    if (io_ctx == nullptr || !io_ctx->is_dryrun) {
        _prefetch(offset, io_ctx);
    }
    return CachedRemoteFileReader::read_at_impl(offset, result, bytes_read, io_ctx);
}

void CacheBlockAwarePrefetchRemoteReader::_prefetch(size_t current_file_offset,
                                                    const IOContext* io_ctx) {
    std::vector<CacheBlockRange> ranges;
    {
        std::lock_guard lock(_pattern_mutex);
        if (!_prefetch_cursor.has_value()) {
            return;
        }
        ranges = _prefetch_cursor->next_touch_ranges(current_file_offset);
    }

    _async_touch_ranges(std::move(ranges), io_ctx);
}

void CacheBlockAwarePrefetchRemoteReader::_async_touch_ranges(std::vector<CacheBlockRange> ranges,
                                                              const IOContext* io_ctx) {
    for (const auto& range : ranges) {
        async_touch_local_cache(range.offset, range.size, io_ctx);
    }
}

namespace detail {

CacheBlockPrefetchCursor::CacheBlockPrefetchCursor(CacheBlockPrefetchPlan plan,
                                                   size_t max_prefetch_blocks)
        : _plan(std::move(plan)), _max_prefetch_blocks(max_prefetch_blocks) {
    DCHECK(_max_prefetch_blocks > 0);
}

std::vector<CacheBlockRange> CacheBlockPrefetchCursor::next_touch_ranges(
        size_t current_file_offset) {
    std::vector<CacheBlockRange> ranges;
    const auto entries = _plan.entries();
    if (entries.empty() || _next_touch_index >= entries.size()) {
        return ranges;
    }

    _advance_current_index(current_file_offset);
    if (_current_index >= entries.size()) {
        return ranges;
    }

    _next_touch_index = std::max(_next_touch_index, _current_index);
    while (_next_touch_index < entries.size()) {
        const bool has_window_capacity = _prefetched_window_size() < _max_prefetch_blocks;
        if (!has_window_capacity && !_next_range_continues_current_file_range()) {
            break;
        }
        ranges.push_back(entries[_next_touch_index++].cache_block_range);
    }
    return ranges;
}

std::vector<CacheBlockRange> CacheBlockPrefetchCursor::next_initial_touch_ranges() {
    const auto entries = _plan.entries();
    if (entries.empty() || _current_index >= entries.size()) {
        return {};
    }
    return next_touch_ranges(entries[_current_index].trigger_file_range.offset);
}

void CacheBlockPrefetchCursor::_advance_current_index(size_t current_file_offset) {
    auto current_range_is_behind = [this,
                                    current_file_offset](const CacheBlockPrefetchPlanEntry& entry) {
        if (_plan.direction() == CacheBlockReadDirection::FORWARD) {
            return trigger_file_range_is_before_current_offset(entry.trigger_file_range,
                                                               current_file_offset);
        }
        return entry.trigger_file_range.offset > current_file_offset;
    };
    const auto entries = _plan.entries();
    while (_current_index < entries.size() && current_range_is_behind(entries[_current_index])) {
        ++_current_index;
    }
}

bool CacheBlockPrefetchCursor::_next_range_continues_current_file_range() const {
    const auto entries = _plan.entries();
    return _next_touch_index > _current_index && _next_touch_index < entries.size() &&
           same_trigger_file_range(entries[_next_touch_index].trigger_file_range,
                                   entries[_next_touch_index - 1].trigger_file_range);
}

CacheBlockPrefetchPlan CacheBlockPrefetchPlan::from_read_pattern(CacheBlockReadPattern pattern,
                                                                 size_t cache_block_size) {
    const auto direction = pattern.direction;
    if (pattern.direction == CacheBlockReadDirection::FORWARD) {
        std::stable_sort(pattern.ranges.begin(), pattern.ranges.end(),
                         [](const FileAccessRange& lhs, const FileAccessRange& rhs) {
                             return lhs.offset < rhs.offset;
                         });
    } else {
        std::stable_sort(pattern.ranges.begin(), pattern.ranges.end(),
                         [](const FileAccessRange& lhs, const FileAccessRange& rhs) {
                             return lhs.offset > rhs.offset;
                         });
    }

    std::vector<CacheBlockPrefetchPlanEntry> entries;
    std::unordered_set<size_t> added_blocks;
    for (const auto& range : pattern.ranges) {
        if (range.size == 0) {
            continue;
        }
        DORIS_CHECK(range.size - 1 <= std::numeric_limits<size_t>::max() - range.offset);

        size_t start_block = range.offset / cache_block_size;
        size_t end_block = (range.offset + range.size - 1) / cache_block_size;
        if (pattern.direction == CacheBlockReadDirection::FORWARD) {
            for (size_t block_id = start_block;; ++block_id) {
                if (added_blocks.emplace(block_id).second) {
                    entries.push_back({
                            .cache_block_range = cache_block_range(block_id, cache_block_size),
                            .trigger_file_range = range,
                    });
                }
                if (block_id == end_block) {
                    break;
                }
            }
        } else {
            for (size_t block_id = end_block;; --block_id) {
                if (added_blocks.emplace(block_id).second) {
                    entries.push_back({
                            .cache_block_range = cache_block_range(block_id, cache_block_size),
                            .trigger_file_range = range,
                    });
                }
                if (block_id == start_block) {
                    break;
                }
            }
        }
    }
    return CacheBlockPrefetchPlan(direction, std::move(entries));
}

} // namespace detail

} // namespace doris::io
