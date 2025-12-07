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

#include "common/logging.h"
#include "olap/iterators.h"
#include "olap/rowset/segment_v2/ordinal_page_index.h"
#include "olap/rowset/segment_v2/page_pointer.h"

namespace doris {
namespace segment_v2 {

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

    size_t last_block_id = static_cast<size_t>(-1);
    rowid_t last_rowid = 0;

    std::string msg {};

    ColumnIteratorOptions iter_opts;
    iter_opts.stats = read_options.stats;

    // Iterate through all rowids in the bitmap to build block sequence
    for (auto it = row_bitmap.begin(); it != row_bitmap.end(); ++it) {
        rowid_t rowid = *it;

        // Use ordinal index to find the page containing this rowid
        OrdinalPageIndexIterator iter;
        RETURN_IF_ERROR(column_reader->seek_at_or_before(rowid, &iter, iter_opts));
        if (!iter.valid()) {
            continue;
        }

        // Get page pointer and calculate block ID
        const PagePointer& page_pointer = iter.page();
        size_t block_id = _offset_to_block_id(page_pointer.offset);

        // Only add new block when block_id changes
        if (block_id != last_block_id) {
            // Use the previous rowid as the first rowid for the previous block
            if (last_block_id != static_cast<size_t>(-1)) {
                _block_sequence.emplace_back(last_block_id, last_rowid);
                msg += fmt::format("({},{}), ", last_block_id, last_rowid);
            }
            last_block_id = block_id;
            last_rowid = rowid;
        }
    }

    // Add the last block
    if (last_block_id != static_cast<size_t>(-1)) {
        _block_sequence.emplace_back(last_block_id, last_rowid);
        msg += fmt::format("({},{}), ", last_block_id, last_rowid);
    }

    LOG_INFO(
            "[verbose] SegmentPrefetcher initialized with block count={}, blocks: (block_id, "
            "first_rowid)=[{}]",
            _block_sequence.size(), msg);

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

} // namespace segment_v2
} // namespace doris
