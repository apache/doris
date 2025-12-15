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

#include <cstddef>
#include <cstdint>
#include <roaring/roaring.hh>
#include <vector>

#include "common/status.h"
#include "olap/rowset/segment_v2/common.h"

namespace doris {
namespace io {
class FileReader;
} // namespace io
class StorageReadOptions;

namespace segment_v2 {
class OrdinalIndexReader;
class ColumnReader;

enum class PrefetcherInitMethod : int { FROM_ROWIDS = 0, ALL_DATA_BLOCKS = 1 };

// Configuration for segment prefetcher
struct SegmentPrefetcherConfig {
    // Number of file cache blocks to prefetch ahead
    size_t prefetch_window_size = 4;

    // File cache block size in bytes (default 1MB)
    size_t block_size = 1024 * 1024;

    SegmentPrefetcherConfig(size_t window_size, size_t blk_size)
            : prefetch_window_size(window_size), block_size(blk_size) {}
};

// Block range representing [offset, offset + size) in the segment file
struct BlockRange {
    uint64_t offset;
    uint64_t size;

    BlockRange(uint64_t off, uint64_t sz) : offset(off), size(sz) {}

    bool operator==(const BlockRange& other) const {
        return offset == other.offset && size == other.size;
    }
};

// Represents a block with its first rowid for reading
struct BlockInfo {
    size_t block_id;
    rowid_t first_rowid;

    BlockInfo(size_t bid, rowid_t rid) : block_id(bid), first_rowid(rid) {}
};

struct SegmentPrefetchParams {
    SegmentPrefetcherConfig config;
    const StorageReadOptions& read_options;
};

// SegmentPrefetcher maintains block sequence and triggers prefetch to keep
// N blocks ahead of current reading position.
//
// Key design:
// - Monotonic reading: rowids are read in order (forward or backward)
// - Trigger condition: when current_rowid reaches a block boundary, prefetch next N blocks
// - No deduplication needed: reading is monotonic, blocks are naturally processed in order
class SegmentPrefetcher {
public:
    explicit SegmentPrefetcher(const SegmentPrefetcherConfig& config) : _config(config) {}

    ~SegmentPrefetcher() = default;

    Status init(std::shared_ptr<ColumnReader> column_reader,
                const StorageReadOptions& read_options);

    bool need_prefetch(rowid_t current_rowid, std::vector<BlockRange>* out_ranges);

    static void build_blocks_by_rowids(const roaring::Roaring& row_bitmap,
                                       const std::vector<SegmentPrefetcher*>& prefetchers);
    void begin_build_blocks_by_rowids();
    void add_rowids(const rowid_t* rowids, uint32_t num);
    void finish_build_blocks_by_rowids();

    void build_all_data_blocks();

private:
    // Parameters:
    //   row_bitmap: The complete bitmap of rowids to scan
    //   ordinal_index: Ordinal index reader (must be loaded)
    //
    // For forward reading: first_rowid is the first rowid we need to read in each block
    // For backward reading: first_rowid is the last rowid we need to read in each block
    //   (since we read backwards, this is the first one we'll encounter)
    void _build_block_sequence_from_bitmap(const roaring::Roaring& row_bitmap,
                                           OrdinalIndexReader* ordinal_index);
    size_t _offset_to_block_id(uint64_t offset) const { return offset / _config.block_size; }

    BlockRange _block_id_to_range(size_t block_id) const {
        return {block_id * _config.block_size, _config.block_size};
    }

    int window_size() const { return _prefetched_index - _current_block_index + 1; }

    std::string debug_string() const {
        return fmt::format(
                "[internal state] _is_forward={}, _prefetched_index={}, _current_block_index={}, "
                "window_size={}, block.size()={}, path={}",
                _is_forward, _prefetched_index, _current_block_index, window_size(),
                _block_sequence.size(), _path);
    }

    void reset_blocks();

private:
    SegmentPrefetcherConfig _config;
    std::string _path;

    // Sequence of blocks with their first rowid (in reading order)
    std::vector<BlockInfo> _block_sequence;

    bool _is_forward = true;

    int _prefetched_index = -1;
    int _current_block_index = 0;

    int page_idx = 0;
    // For each page, track the first rowid we need to read
    // For forward: the smallest rowid in this page
    // For backward: the largest rowid in this page (first one we'll encounter when reading backwards)
    size_t last_block_id = static_cast<size_t>(-1);
    rowid_t current_block_first_rowid = 0;

    OrdinalIndexReader* ordinal_index = nullptr;
};

} // namespace segment_v2
} // namespace doris
