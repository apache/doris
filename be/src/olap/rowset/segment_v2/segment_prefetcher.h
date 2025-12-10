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
#include "olap/iterators.h"
#include "olap/rowset/segment_v2/common.h"

namespace doris {
namespace io {
class FileReader;
} // namespace io

namespace segment_v2 {
class OrdinalIndexReader;
class ColumnReader;

// Configuration for segment prefetcher
struct SegmentPrefetcherConfig {
    // Number of file cache blocks to prefetch ahead
    size_t prefetch_window_size = 4;

    // File cache block size in bytes (default 1MB)
    size_t block_size = 1024 * 1024;

    SegmentPrefetcherConfig() = default;
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
    const roaring::Roaring& row_bitmap;
    const StorageReadOptions& read_options;
};

// SegmentPrefetcher maintains block sequence and triggers prefetch to keep
// N blocks ahead of current reading position.
//
// Key design:
// - Monotonic reading: rowids are read in order (forward or backward)
// - Trigger condition: when current_rowid reaches a block boundary, prefetch next N blocks
// - No deduplication needed: reading is monotonic, blocks are naturally processed in order
//
// Usage:
//   SegmentPrefetcher prefetcher(config);
//   prefetcher.init(row_bitmap, ordinal_index, is_reverse);
//   // In each next_batch():
//   std::vector<BlockRange> ranges;
//   if (prefetcher.need_prefetch(current_first_rowid, &ranges)) {
//       for (auto& range : ranges) {
//           file_reader->prefetch_range(range.offset, range.size);
//       }
//   }
//
class SegmentPrefetcher {
public:
    explicit SegmentPrefetcher(const SegmentPrefetcherConfig& config) : _config(config) {}

    ~SegmentPrefetcher() = default;

    // Initialize prefetcher with the full row bitmap and ordinal index.
    //
    // Parameters:
    //   row_bitmap: The complete bitmap of rowids to scan
    //   ordinal_index: Ordinal index reader for mapping rowid -> page pointer
    //   is_reverse: Whether reading in reverse order
    //
    // Returns OK on success, error status on failure
    Status init(const roaring::Roaring& row_bitmap, std::shared_ptr<ColumnReader> column_reader,
                const StorageReadOptions& read_options);

    // Check if prefetch is needed for current_rowid and return blocks to prefetch.
    // This maintains N blocks ahead of the current reading position.
    //
    // Parameters:
    //   current_rowid: The first rowid being read in current batch
    //   out_ranges: Output vector of BlockRange to prefetch (only filled if return true)
    //
    // Returns true if prefetch is needed, false otherwise
    bool need_prefetch(rowid_t current_rowid, std::vector<BlockRange>* out_ranges);

    // Reset the prefetcher state
    void reset() {
        _block_sequence.clear();
        _next_prefetch_index = 0;
        _last_search_index = 0;
    }

private:
    // Build block sequence directly from OrdinalIndexReader's internal data.
    // This is much more efficient than calling seek_at_or_before for each rowid.
    // Complexity: O(M + num_pages) where M is the number of rowids in the bitmap,
    // compared to O(M * log(num_pages)) for the per-rowid seek approach.
    //
    // Parameters:
    //   row_bitmap: The complete bitmap of rowids to scan
    //   ordinal_index: Ordinal index reader (must be loaded)
    //
    // For forward reading: first_rowid is the first rowid we need to read in each block
    // For backward reading: first_rowid is the last rowid we need to read in each block
    //   (since we read backwards, this is the first one we'll encounter)
    void _build_block_sequence_from_bitmap(const roaring::Roaring& row_bitmap,
                                           OrdinalIndexReader* ordinal_index);
    // Calculate file cache block ID from file offset
    size_t _offset_to_block_id(uint64_t offset) const { return offset / _config.block_size; }

    // Calculate file cache block range (offset, size) from block ID
    BlockRange _block_id_to_range(size_t block_id) const {
        return BlockRange(block_id * _config.block_size, _config.block_size);
    }

private:
    SegmentPrefetcherConfig _config;

    // Sequence of blocks with their first rowid (in reading order)
    std::vector<BlockInfo> _block_sequence;

    // Reading direction: true for forward, false for backward/reverse
    bool _is_forward = true;

    // Next index in _block_sequence to prefetch
    size_t _next_prefetch_index = 0;

    // Last searched block index for monotonic access optimization
    size_t _last_search_index = 0;
};

} // namespace segment_v2
} // namespace doris
