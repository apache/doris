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

CacheBlockAwarePrefetchRemoteReader::CacheBlockAwarePrefetchRemoteReader(
        FileReaderSPtr remote_file_reader, const FileReaderOptions& opts)
        : CachedRemoteFileReader(std::move(remote_file_reader), opts) {}

Status CacheBlockAwarePrefetchRemoteReader::set_read_pattern(
        CacheBlockReadPattern pattern, const CacheBlockPrefetchPolicy& policy) {
    if (policy.max_prefetch_blocks == 0 || policy.cache_block_size == 0) {
        return Status::InvalidArgument(
                "cache block prefetch policy requires positive window and block size");
    }

    ReadPatternState state;
    state.direction = pattern.direction;
    state.policy = policy;
    state.block_sequence = _build_block_sequence(std::move(pattern), policy.cache_block_size);

    std::lock_guard lock(_pattern_mutex);
    if (state.block_sequence.empty()) {
        _has_pattern = false;
        _pattern = {};
        return Status::OK();
    }
    _pattern = std::move(state);
    _has_pattern = true;
    return Status::OK();
}

void CacheBlockAwarePrefetchRemoteReader::clear_read_pattern() {
    std::lock_guard lock(_pattern_mutex);
    _has_pattern = false;
    _pattern = {};
}

bool CacheBlockAwarePrefetchRemoteReader::has_read_pattern() const {
    std::lock_guard lock(_pattern_mutex);
    return _has_pattern;
}

Status CacheBlockAwarePrefetchRemoteReader::read_at_impl(size_t offset, Slice result,
                                                         size_t* bytes_read,
                                                         const IOContext* io_ctx) {
    // Normal foreground reads drive the prefetch window by the real file offset
    // that PageIO is about to read. Dry-run reads are submitted by
    // CachedRemoteFileReader::prefetch_range() to warm the file cache; they must
    // not recursively schedule more prefetch work.
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
        if (!_has_pattern) {
            return;
        }
        ranges = _next_prefetch_ranges(&_pattern, current_file_offset);
    }

    for (const auto& range : ranges) {
        prefetch_range(range.offset, range.size, io_ctx);
    }
}

std::vector<CacheBlockAwarePrefetchRemoteReader::CacheBlockInfo>
CacheBlockAwarePrefetchRemoteReader::_build_block_sequence(CacheBlockReadPattern pattern,
                                                           size_t cache_block_size) {
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

    std::vector<CacheBlockInfo> block_sequence;
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
                    block_sequence.push_back({block_id, range.offset});
                }
                if (block_id == end_block) {
                    break;
                }
            }
        } else {
            for (size_t block_id = end_block;; --block_id) {
                if (added_blocks.emplace(block_id).second) {
                    block_sequence.push_back({block_id, range.offset});
                }
                if (block_id == start_block) {
                    break;
                }
            }
        }
    }
    return block_sequence;
}

std::vector<CacheBlockRange> CacheBlockAwarePrefetchRemoteReader::_next_prefetch_ranges(
        ReadPatternState* state, size_t current_file_offset) {
    DCHECK(state != nullptr);
    std::vector<CacheBlockRange> ranges;
    if (state->block_sequence.empty() ||
        state->prefetched_index >= static_cast<int>(state->block_sequence.size()) - 1) {
        return ranges;
    }

    const int block_sequence_size = static_cast<int>(state->block_sequence.size());
    if (state->direction == CacheBlockReadDirection::FORWARD) {
        while (state->current_block_index < block_sequence_size &&
               state->block_sequence[state->current_block_index].trigger_offset <
                       current_file_offset) {
            state->current_block_index++;
        }
    } else {
        while (state->current_block_index < block_sequence_size &&
               state->block_sequence[state->current_block_index].trigger_offset >
                       current_file_offset) {
            state->current_block_index++;
        }
    }
    if (state->current_block_index >= block_sequence_size) {
        return ranges;
    }

    state->prefetched_index = std::max(state->prefetched_index, state->current_block_index - 1);
    while (state->prefetched_index + 1 < block_sequence_size) {
        const bool has_window_capacity =
                static_cast<size_t>(state->window_size()) < state->policy.max_prefetch_blocks;
        const bool is_completing_started_file_range =
                state->prefetched_index >= state->current_block_index &&
                state->block_sequence[state->prefetched_index + 1].trigger_offset ==
                        state->block_sequence[state->prefetched_index].trigger_offset;
        if (!has_window_capacity && !is_completing_started_file_range) {
            break;
        }
        const auto& block = state->block_sequence[++state->prefetched_index];
        ranges.push_back(_block_id_to_range(block.block_id, state->policy.cache_block_size));
    }
    return ranges;
}

} // namespace doris::io
