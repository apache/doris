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

#include "olap/rowset/segment_v2/segment_prefetcher.h"

#include <algorithm>

#include "common/logging.h"
#include "olap/iterators.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/rowset/segment_v2/ordinal_page_index.h"

namespace doris::segment_v2 {

void SegmentPrefetcher::_build_block_sequence_from_bitmap(const roaring::Roaring& row_bitmap,
                                                          OrdinalIndexReader* ordinal_index) {
    // Access internal data of OrdinalIndexReader directly (as friend)
    const auto& ordinals = ordinal_index->_ordinals; // ordinals[i] = first ordinal of page i
    const auto& pages = ordinal_index->_pages;       // pages[i] = page pointer of page i
    const int num_pages = ordinal_index->_num_pages;

    if (num_pages == 0) {
        return;
    }

    // Current page index we're examining
    int page_idx = 0;

    // For each page, track the first rowid we need to read
    // For forward: the smallest rowid in this page
    // For backward: the largest rowid in this page (first one we'll encounter when reading backwards)
    size_t last_block_id = static_cast<size_t>(-1);
    rowid_t current_block_first_rowid = 0;

    if (_is_forward) {
        // Forward reading: iterate bitmap in ascending order
        for (auto it = row_bitmap.begin(); it != row_bitmap.end(); ++it) {
            rowid_t rowid = *it;

            // Advance page_idx until we find the page containing this rowid
            // A rowid belongs to page i if: ordinals[i] <= rowid < ordinals[i+1]
            while (page_idx < num_pages - 1 && ordinals[page_idx + 1] <= rowid) {
                page_idx++;
            }

            // Skip if rowid is beyond all pages
            if (page_idx >= num_pages || rowid < ordinals[page_idx]) {
                continue;
            }

            // Calculate file cache block ID from page offset
            size_t block_id = _offset_to_block_id(pages[page_idx].offset);

            // Only add new block when block_id changes
            if (block_id != last_block_id) {
                // Save the previous block
                if (last_block_id != static_cast<size_t>(-1)) {
                    _block_sequence.emplace_back(last_block_id, current_block_first_rowid);
                }
                last_block_id = block_id;
                current_block_first_rowid = rowid;
            }
        }
    } else {
        // Backward reading: we need the last rowid in each block as the "first" rowid
        // (because when reading backwards, we encounter the largest rowid first)
        //
        // Strategy: iterate forward through bitmap, but for each block,
        // keep updating current_block_first_rowid to the latest (largest) rowid in that block
        for (auto it = row_bitmap.begin(); it != row_bitmap.end(); ++it) {
            rowid_t rowid = *it;

            // Advance page_idx until we find the page containing this rowid
            while (page_idx < num_pages - 1 && ordinals[page_idx + 1] <= rowid) {
                page_idx++;
            }

            // Skip if rowid is beyond all pages
            if (page_idx >= num_pages || rowid < ordinals[page_idx]) {
                continue;
            }

            // Calculate file cache block ID from page offset
            size_t block_id = _offset_to_block_id(pages[page_idx].offset);

            if (block_id != last_block_id) {
                // Save the previous block with its last (largest) rowid
                if (last_block_id != static_cast<size_t>(-1)) {
                    _block_sequence.emplace_back(last_block_id, current_block_first_rowid);
                }
                last_block_id = block_id;
            }
            // Always update to the current (larger) rowid for backward reading
            current_block_first_rowid = rowid;
        }

        // After collecting all blocks, reverse the sequence for backward reading
        // so that blocks are in the order they'll be accessed
    }

    // Add the last block
    if (last_block_id != static_cast<size_t>(-1)) {
        _block_sequence.emplace_back(last_block_id, current_block_first_rowid);
    }

    // For backward reading, reverse the block sequence
    if (!_is_forward && !_block_sequence.empty()) {
        std::ranges::reverse(_block_sequence);
    }
}

Status SegmentPrefetcher::init(const roaring::Roaring& row_bitmap,
                               std::shared_ptr<ColumnReader> column_reader,
                               const StorageReadOptions& read_options) {
    DCHECK(column_reader != nullptr);

    _block_sequence.clear();
    _next_prefetch_index = 0;
    _is_forward = !read_options.read_orderby_key_reverse;

    if (row_bitmap.isEmpty()) {
        return Status::OK();
    }

    // Get ordinal index reader
    OrdinalIndexReader* ordinal_index = nullptr;
    RETURN_IF_ERROR(column_reader->get_ordinal_index_reader(ordinal_index, read_options.stats));

    if (ordinal_index == nullptr) {
        return Status::OK();
    }

    // Build block sequence efficiently using direct access to ordinal index internals
    _build_block_sequence_from_bitmap(row_bitmap, ordinal_index);

    std::string msg {};
    for (const auto& block : _block_sequence) {
        msg += fmt::format("({},{}), ", block.block_id, block.first_rowid);
    }

    LOG_INFO(
            "[verbose] SegmentPrefetcher initialized with block count={}, is_forward={}, blocks: "
            "(block_id, first_rowid)=[{}]",
            _block_sequence.size(), _is_forward, msg);

    return Status::OK();
}

bool SegmentPrefetcher::need_prefetch(rowid_t current_rowid, std::vector<BlockRange>* out_ranges) {
    DCHECK(out_ranges != nullptr);

    if (_block_sequence.empty() || _next_prefetch_index >= _block_sequence.size()) {
        return false;
    }

    // Find the index of the block that should be read for current_rowid
    // This is the block that contains or will contain current_rowid
    size_t current_block_index = _next_prefetch_index;

    if (_is_forward) {
        // Forward: find the largest index where first_rowid <= current_rowid
        while (current_block_index < _block_sequence.size() - 1 &&
               _block_sequence[current_block_index + 1].first_rowid <= current_rowid) {
            current_block_index++;
        }
    } else {
        // Backward: find the largest index where first_rowid >= current_rowid
        while (current_block_index < _block_sequence.size() - 1 &&
               _block_sequence[current_block_index + 1].first_rowid >= current_rowid) {
            current_block_index++;
        }
    }

    // If current_block_index is still before _next_prefetch_index,
    // it means current_rowid is in a block we've already scheduled for prefetch
    // In this case, no need to prefetch
    if (current_block_index < _next_prefetch_index) {
        return false;
    }

    // If current_block_index >= _next_prefetch_index, we need to prefetch
    // Start prefetching from current_block_index to maintain N blocks ahead
    out_ranges->clear();
    out_ranges->reserve(_config.prefetch_window_size);

    size_t end_index =
            std::min(current_block_index + _config.prefetch_window_size, _block_sequence.size());

    std::string msg {};
    for (size_t i = current_block_index; i < end_index; ++i) {
        out_ranges->push_back(_block_id_to_range(_block_sequence[i].block_id));
        msg += fmt::format("({},{}), ", _block_sequence[i].block_id,
                           _block_sequence[i].first_rowid);
    }

    // Update next prefetch index
    _next_prefetch_index = end_index;

    LOG_INFO(
            "[verbose] SegmentPrefetcher prefetch triggered at rowid={}, prefetching {} blocks: "
            "(block_id, size)=[{}]",
            current_rowid, out_ranges->size(), msg);
    return !out_ranges->empty();
}

} // namespace doris::segment_v2
