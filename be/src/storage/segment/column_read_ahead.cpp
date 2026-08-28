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

#include "storage/segment/column_read_ahead.h"

#include <algorithm>
#include <limits>
#include <utility>

#include "common/cast_set.h"
#include "common/logging.h"

namespace doris::segment_v2 {

const ColumnReadAheadOptions& ColumnReadAheadContext::options(ColumnReadAheadRole role) const {
    return role == ColumnReadAheadRole::EAGER ? eager_options : lazy_options;
}

const ColumnReadAheadOptions& ColumnReadAheadRequest::options() const {
    DORIS_CHECK(context != nullptr);
    return context->options(role);
}

void ColumnReadAheadRequest::sanity_check() const {
    DORIS_CHECK(current_rowids != nullptr);
    DORIS_CHECK(current_rowid_count > 0);
    DORIS_CHECK(scan_rowids != nullptr);
    DORIS_CHECK(context != nullptr);
    DORIS_CHECK(std::is_sorted(current_rowids, current_rowids + current_rowid_count));
}

Status ColumnReadAheadOptions::validate() const {
    if (high_watermark_bytes == 0) {
        return Status::InvalidArgument("column read-ahead high watermark must be positive");
    }
    if (low_watermark_bytes >= high_watermark_bytes) {
        return Status::InvalidArgument(
                "column read-ahead low watermark must be smaller than high watermark");
    }
    return Status::OK();
}

ColumnReadAhead::ColumnReadAhead(std::vector<ColumnReadAheadPage> pages,
                                 ColumnReadAheadOptions options, bool reverse)
        : _pages(std::move(pages)), _options(options), _reverse(reverse) {
    DORIS_CHECK(!_pages.empty());
    for (size_t index = 0; index < _pages.size(); ++index) {
        const auto& page = _pages[index];
        DORIS_CHECK(page.page_index == cast_set<int32_t>(index));
        DORIS_CHECK(page.first_ordinal <= page.last_ordinal);
        DORIS_CHECK(page.range.size > 0);
        if (index > 0) {
            DORIS_CHECK(_pages[index - 1].last_ordinal < page.first_ordinal);
        }
    }
}

Status ColumnReadAhead::create(std::vector<ColumnReadAheadPage> pages,
                               ColumnReadAheadOptions options, bool reverse,
                               std::unique_ptr<ColumnReadAhead>* output) {
    DORIS_CHECK(output != nullptr);
    RETURN_IF_ERROR(options.validate());
    DORIS_CHECK(!pages.empty());
    output->reset(new ColumnReadAhead(std::move(pages), options, reverse));
    return Status::OK();
}

void ColumnReadAhead::plan(const rowid_t* current_rowids, size_t count,
                           const roaring::Roaring& scan_rowids, ColumnReadAheadPlan* output) {
    DORIS_CHECK(current_rowids != nullptr);
    DORIS_CHECK(count > 0);
    DORIS_CHECK(output != nullptr);
    DORIS_CHECK(std::is_sorted(current_rowids, current_rowids + count));
    output->column = this;
    output->new_pages.clear();
    output->released_pages.clear();

    _discard_passed_pages(current_rowids, count, output);
    for (size_t index = 0; index < count; ++index) {
        _add_page(_page_for_ordinal(current_rowids[index]), output);
    }

    _sync_future_cursor(current_rowids, count, scan_rowids);
    const bool replenish = !_planned_once || _pending_bytes <= _options.low_watermark_bytes;
    _planned_once = true;
    if (replenish && _pending_bytes < _options.high_watermark_bytes) {
        _extend_window(scan_rowids, output);
    }
}

void ColumnReadAhead::complete(int32_t page_index) {
    auto entry = _window.find(page_index);
    DORIS_CHECK(entry != _window.end());
    _complete(_pages[page_index], &entry->second);
}

bool ColumnReadAhead::pending(int32_t page_index) const {
    const auto entry = _window.find(page_index);
    return entry != _window.end() && entry->second.pending;
}

const ColumnReadAheadPage& ColumnReadAhead::_page_for_ordinal(rowid_t ordinal) const {
    const auto page =
            std::ranges::upper_bound(_pages, ordinal, {}, &ColumnReadAheadPage::first_ordinal);
    DORIS_CHECK(page != _pages.begin());
    const auto& result = *std::prev(page);
    DORIS_CHECK(ordinal <= result.last_ordinal);
    return result;
}

void ColumnReadAhead::_add_page(const ColumnReadAheadPage& page, ColumnReadAheadPlan* output) {
    if (_window.contains(page.page_index)) {
        return;
    }
    const auto [entry, inserted] = _window.emplace(page.page_index, WindowEntry {});
    DORIS_CHECK(inserted);
    static_cast<void>(entry);
    DORIS_CHECK(page.range.size <= std::numeric_limits<size_t>::max() - _pending_bytes);
    _pending_bytes += page.range.size;
    output->new_pages.push_back(page);
}

void ColumnReadAhead::_discard_passed_pages(const rowid_t* current_rowids, size_t count,
                                            ColumnReadAheadPlan* output) {
    const rowid_t first = current_rowids[0];
    const rowid_t last = current_rowids[count - 1];
    for (auto entry = _window.begin(); entry != _window.end();) {
        const auto& page = _pages[entry->first];
        const bool passed = _reverse ? page.first_ordinal > last : page.last_ordinal < first;
        if (!passed) {
            ++entry;
            continue;
        }
        _complete(page, &entry->second);
        output->released_pages.push_back(page);
        entry = _window.erase(entry);
    }
}

void ColumnReadAhead::_sync_future_cursor(const rowid_t* current_rowids, size_t count,
                                          const roaring::Roaring& scan_rowids) {
    if (!_reverse) {
        const auto next_rank = cast_set<int64_t>(scan_rowids.rank(current_rowids[count - 1]));
        if (!_planned_once || _next_scan_rank < next_rank) {
            _next_scan_rank = next_rank;
        }
    } else {
        const uint64_t preceding_rows =
                current_rowids[0] == 0 ? 0 : scan_rowids.rank(current_rowids[0] - 1);
        const auto next_rank = cast_set<int64_t>(preceding_rows) - 1;
        if (!_planned_once || _next_scan_rank > next_rank) {
            _next_scan_rank = next_rank;
        }
    }
}

void ColumnReadAhead::_extend_window(const roaring::Roaring& scan_rowids,
                                     ColumnReadAheadPlan* output) {
    const auto cardinality = cast_set<int64_t>(scan_rowids.cardinality());
    while (_pending_bytes < _options.high_watermark_bytes && _next_scan_rank >= 0 &&
           _next_scan_rank < cardinality) {
        uint32_t ordinal = 0;
        const bool selected = scan_rowids.select(cast_set<uint32_t>(_next_scan_rank), &ordinal);
        DORIS_CHECK(selected);
        const auto& page = _page_for_ordinal(ordinal);
        _add_page(page, output);
        _advance_future_cursor_past(page, scan_rowids);
    }
}

void ColumnReadAhead::_advance_future_cursor_past(const ColumnReadAheadPage& page,
                                                  const roaring::Roaring& scan_rowids) {
    if (!_reverse) {
        _next_scan_rank = cast_set<int64_t>(scan_rowids.rank(cast_set<rowid_t>(page.last_ordinal)));
        return;
    }
    const uint64_t preceding_rows =
            page.first_ordinal == 0 ? 0
                                    : scan_rowids.rank(cast_set<rowid_t>(page.first_ordinal - 1));
    _next_scan_rank = cast_set<int64_t>(preceding_rows) - 1;
}

void ColumnReadAhead::_complete(const ColumnReadAheadPage& page, WindowEntry* entry) {
    DORIS_CHECK(entry != nullptr);
    if (entry->pending) {
        DORIS_CHECK(page.range.size <= _pending_bytes);
        _pending_bytes -= page.range.size;
        entry->pending = false;
    }
}

} // namespace doris::segment_v2
