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

#include "storage/segment/segment_file_access_range_builder.h"

#include <algorithm>
#include <ranges>
#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "storage/index/ordinal_page_index.h"

namespace doris::segment_v2 {

SegmentFileAccessRangeBuilder::SegmentFileAccessRangeBuilder(OrdinalIndexReader* ordinal_index,
                                                             io::CacheBlockReadDirection direction)
        : _ordinal_index(ordinal_index), _direction(direction) {
    DCHECK(_ordinal_index != nullptr);
}

void SegmentFileAccessRangeBuilder::reset() {
    _access_ranges.clear();
    _next_page_hint = 0;
    _pending_page_index.reset();
}

void SegmentFileAccessRangeBuilder::add_ascending_rowids(std::span<const rowid_t> rowids) {
    DCHECK(_ordinal_index != nullptr);
    if (rowids.empty()) {
        return;
    }
    DCHECK(std::ranges::is_sorted(rowids));
    const auto& ordinals = _ordinal_index->_ordinals;
    const int num_pages = _ordinal_index->_num_pages;
    DORIS_CHECK(num_pages > 0);
    for (const rowid_t rowid : rowids) {
        while (_next_page_hint < num_pages - 1 && ordinals[_next_page_hint + 1] <= rowid) {
            _next_page_hint++;
        }

        if (!_pending_page_index.has_value() || _next_page_hint != *_pending_page_index) {
            if (_pending_page_index.has_value()) {
                _append_page_access_range(*_pending_page_index);
            }
            _pending_page_index = _next_page_hint;
        }
    }
}

std::vector<io::FileAccessRange> SegmentFileAccessRangeBuilder::finish_by_rowids() {
    DCHECK(_ordinal_index != nullptr);
    if (_pending_page_index.has_value()) {
        _append_page_access_range(*_pending_page_index);
    }
    _reverse_if_backward();
    auto output = std::move(_access_ranges);
    reset();
    return output;
}

std::vector<io::FileAccessRange> SegmentFileAccessRangeBuilder::build_all_data_page_ranges() {
    DCHECK(_ordinal_index != nullptr);
    reset();
    const int num_pages = _ordinal_index->_num_pages;

    for (int page_index = 0; page_index < num_pages; ++page_index) {
        _append_page_access_range(page_index);
    }

    _reverse_if_backward();
    auto output = std::move(_access_ranges);
    reset();
    return output;
}

void SegmentFileAccessRangeBuilder::add_rowids_from_bitmap(
        const roaring::Roaring& row_bitmap,
        std::span<SegmentFileAccessRangeBuilder* const> builders) {
    for (auto* builder : builders) {
        builder->reset();
    }

    int batch_size = config::segment_file_cache_consume_rowids_batch_size;
    DORIS_CHECK(batch_size > 0);
    std::vector<rowid_t> rowids(batch_size);
    roaring::api::roaring_uint32_iterator_t iter;
    roaring::api::roaring_init_iterator(&row_bitmap.roaring, &iter);
    uint32_t num = roaring::api::roaring_read_uint32_iterator(&iter, rowids.data(), batch_size);

    for (; num > 0;
         num = roaring::api::roaring_read_uint32_iterator(&iter, rowids.data(), batch_size)) {
        for (auto* builder : builders) {
            builder->add_ascending_rowids(std::span(rowids.data(), num));
        }
    }
}

void SegmentFileAccessRangeBuilder::_append_page_access_range(int page_index) {
    const auto& page = _ordinal_index->_pages[page_index];
    _access_ranges.push_back(io::FileAccessRange {
            .offset = page.offset,
            .size = page.size,
    });
}

void SegmentFileAccessRangeBuilder::_reverse_if_backward() {
    if (!_is_forward() && !_access_ranges.empty()) {
        std::ranges::reverse(_access_ranges);
    }
}

} // namespace doris::segment_v2
