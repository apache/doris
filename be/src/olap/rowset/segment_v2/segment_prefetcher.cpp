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
#include <ranges>

#include "common/config.h"
#include "common/logging.h"
#include "olap/iterators.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/rowset/segment_v2/ordinal_page_index.h"

namespace doris::segment_v2 {

void SegmentPrefetcher::add_rowids(const rowid_t* rowids, uint32_t num) {
    if (ordinal_index == nullptr) {
        return;
    }
    const auto& ordinals = ordinal_index->_ordinals; // ordinals[i] = first ordinal of page i
    const auto& pages = ordinal_index->_pages;       // pages[i] = page pointer of page i
    const int num_pages = ordinal_index->_num_pages;
    for (uint32_t i = 0; i < num; ++i) {
        rowid_t rowid = rowids[i];

        if (_is_forward) {
            while (page_idx < num_pages - 1 && ordinals[page_idx + 1] <= rowid) {
                page_idx++;
            }

            const auto& page = pages[page_idx];
            size_t page_start_block = _offset_to_block_id(page.offset);
            size_t page_end_block = _offset_to_block_id(page.offset + page.size - 1);

            // If page spans two blocks, assign it to the next block (page_end_block)
            size_t block_id =
                    (page_start_block != page_end_block) ? page_end_block : page_start_block;

            if (block_id != last_block_id) {
                if (last_block_id != static_cast<size_t>(-1)) {
                    _block_sequence.emplace_back(last_block_id, current_block_first_rowid);
                }
                last_block_id = block_id;
                current_block_first_rowid = rowid;
            }
        } else {
            // Backward reading: we need the last rowid in each block as the "first" rowid
            // (because when reading backwards, we encounter the largest rowid first)
            //
            // Strategy: iterate forward through bitmap, but for each block,
            // keep updating current_block_first_rowid to the latest (largest) rowid in that block
            while (page_idx < num_pages - 1 && ordinals[page_idx + 1] <= rowid) {
                page_idx++;
            }
            size_t block_id = _offset_to_block_id(pages[page_idx].offset);

            if (block_id != last_block_id) {
                if (last_block_id != static_cast<size_t>(-1)) {
                    _block_sequence.emplace_back(last_block_id, current_block_first_rowid);
                }
                last_block_id = block_id;
            }
            current_block_first_rowid = rowid;
        }
    }
}

void SegmentPrefetcher::build_all_data_blocks() {
    if (ordinal_index == nullptr) {
        return;
    }
    reset_blocks();
    const auto& ordinals = ordinal_index->_ordinals; // ordinals[i] = first ordinal of page i
    const auto& pages = ordinal_index->_pages;       // pages[i] = page pointer of page i
    const int num_pages = ordinal_index->_num_pages;

    last_block_id = static_cast<size_t>(-1);
    current_block_first_rowid = 0;

    for (page_idx = 0; page_idx < num_pages; ++page_idx) {
        const auto& page = pages[page_idx];

        if (_is_forward) {
            size_t page_start_block = _offset_to_block_id(page.offset);
            size_t page_end_block = _offset_to_block_id(page.offset + page.size - 1);

            // If page spans two blocks, assign it to the next block (page_end_block)
            size_t block_id =
                    (page_start_block != page_end_block) ? page_end_block : page_start_block;

            if (block_id != last_block_id) {
                if (last_block_id != static_cast<size_t>(-1)) {
                    _block_sequence.emplace_back(last_block_id, current_block_first_rowid);
                }
                last_block_id = block_id;
                current_block_first_rowid = static_cast<rowid_t>(ordinals[page_idx]);
            }
        } else {
            // Backward: use the last ordinal in each block as first_rowid
            size_t block_id = _offset_to_block_id(page.offset);
            if (block_id != last_block_id) {
                if (last_block_id != static_cast<size_t>(-1)) {
                    _block_sequence.emplace_back(last_block_id, current_block_first_rowid);
                }
                last_block_id = block_id;
            }
            current_block_first_rowid = static_cast<rowid_t>(ordinals[page_idx]);
        }
    }

    // Add the last block
    if (last_block_id != static_cast<size_t>(-1)) {
        _block_sequence.emplace_back(last_block_id, current_block_first_rowid);
    }

    // Reverse for backward reading
    if (!_is_forward && !_block_sequence.empty()) {
        std::ranges::reverse(_block_sequence);
    }
}

void SegmentPrefetcher::build_blocks_by_rowids(const roaring::Roaring& row_bitmap,
                                               const std::vector<SegmentPrefetcher*>& prefetchers) {
    for (auto* prefetcher : prefetchers) {
        prefetcher->begin_build_blocks_by_rowids();
    }

    int batch_size = config::segment_file_cache_consume_rowids_batch_size;
    std::vector<rowid_t> rowids(batch_size);
    roaring::api::roaring_uint32_iterator_t iter;
    roaring::api::roaring_init_iterator(&row_bitmap.roaring, &iter);
    uint32_t num = roaring::api::roaring_read_uint32_iterator(&iter, rowids.data(), batch_size);

    for (; num > 0;
         num = roaring::api::roaring_read_uint32_iterator(&iter, rowids.data(), batch_size)) {
        for (auto* prefetcher : prefetchers) {
            prefetcher->add_rowids(rowids.data(), num);
        }
    }

    for (auto* prefetcher : prefetchers) {
        prefetcher->finish_build_blocks_by_rowids();
    }
}

void SegmentPrefetcher::begin_build_blocks_by_rowids() {
    reset_blocks();
    page_idx = 0;
}

void SegmentPrefetcher::finish_build_blocks_by_rowids() {
    if (ordinal_index == nullptr) {
        return;
    }
    if (last_block_id != static_cast<size_t>(-1)) {
        _block_sequence.emplace_back(last_block_id, current_block_first_rowid);
    }

    if (!_is_forward && !_block_sequence.empty()) {
        std::ranges::reverse(_block_sequence);
    }

    LOG_IF(INFO, config::enable_segment_prefetch_verbose_log) << fmt::format(
            "[verbose] SegmentPrefetcher initialized with block count={}, is_forward={}, "
            "num_pages={}, path={}, blocks: (block_id, first_rowid)=[{}]",
            _block_sequence.size(), _is_forward, ordinal_index->_num_pages, _path,
            fmt::join(_block_sequence | std::views::transform([](const auto& b) {
                          return fmt::format("({}, {})", b.block_id, b.first_rowid);
                      }),
                      ","));
}

void SegmentPrefetcher::reset_blocks() {
    _block_sequence.clear();
    _current_block_index = 0;
    _prefetched_index = -1;
}

Status SegmentPrefetcher::init(std::shared_ptr<ColumnReader> column_reader,
                               const StorageReadOptions& read_options) {
    DCHECK(column_reader != nullptr);

    reset_blocks();
    _is_forward = !read_options.read_orderby_key_reverse;
    _path = column_reader->_file_reader->path().filename().native();

    RETURN_IF_ERROR(column_reader->get_ordinal_index_reader(ordinal_index, read_options.stats));
    return Status::OK();
}

bool SegmentPrefetcher::need_prefetch(rowid_t current_rowid, std::vector<BlockRange>* out_ranges) {
    DCHECK(out_ranges != nullptr);
    LOG_IF(INFO, config::enable_segment_prefetch_verbose_log)
            << fmt::format("[verbose] SegmentPrefetcher need_prefetch enter current_rowid={}, {}",
                           current_rowid, debug_string());
    if (_block_sequence.empty() ||
        _prefetched_index >= static_cast<int>(_block_sequence.size()) - 1) {
        return false;
    }

    LOG_IF(INFO, config::enable_segment_prefetch_verbose_log) << fmt::format(
            "[verbose] SegmentPrefetcher need_prefetch called with current_rowid={}, {}, "
            "block=(id={}, first_rowid={})",
            current_rowid, debug_string(), _block_sequence[_current_block_index].block_id,
            _block_sequence[_current_block_index].first_rowid);
    if (_is_forward) {
        while (_current_block_index + 1 < _block_sequence.size() &&
               _block_sequence[_current_block_index + 1].first_rowid <= current_rowid) {
            _current_block_index++;
        }
    } else {
        while (_current_block_index + 1 < _block_sequence.size() &&
               _block_sequence[_current_block_index + 1].first_rowid >= current_rowid) {
            _current_block_index++;
        }
    }

    out_ranges->clear();
    // for non-predicate column, some rowids in row_bitmap may be filtered out after vec evaluation of predicate columns,
    // so we should not prefetch for these rows
    _prefetched_index = std::max(_prefetched_index, _current_block_index - 1);
    while (_prefetched_index + 1 < _block_sequence.size() &&
           window_size() < _config.prefetch_window_size) {
        out_ranges->push_back(_block_id_to_range(_block_sequence[++_prefetched_index].block_id));
    }

    LOG_IF(INFO, config::enable_segment_prefetch_verbose_log) << fmt::format(
            "[verbose] SegmentPrefetcher need_prefetch after calc with current_rowid={}, {}, "
            "block=(id={}, first_rowid={})",
            current_rowid, debug_string(), _block_sequence[_current_block_index].block_id,
            _block_sequence[_current_block_index].first_rowid);

    bool triggered = !out_ranges->empty();
    if (triggered) {
        LOG_IF(INFO, config::enable_segment_prefetch_verbose_log) << fmt::format(
                "[verbose] SegmentPrefetcher prefetch triggered at rowid={}, {}, prefetch {} "
                "blocks: (offset, size)=[{}]",
                current_rowid, debug_string(), out_ranges->size(),
                fmt::join(*out_ranges | std::views::transform([](const auto& b) {
                    return fmt::format("({}, {})", b.offset, b.size);
                }),
                          ","));
    }
    return triggered;
}

} // namespace doris::segment_v2
