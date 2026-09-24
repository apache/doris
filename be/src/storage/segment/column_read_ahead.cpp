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
#include "io/fs/read_ahead_metrics.h"
#include "runtime/runtime_profile.h"

namespace doris::segment_v2 {

void ColumnReadAheadPlan::reset(ColumnReadAhead* owner) {
    column = owner;
    new_pages.clear();
    released_pages.clear();
    window_discard_ns = 0;
    current_batch_plan_ns = 0;
    window_extend_ns = 0;
}

void ColumnReadAheadPlan::update_statistics(io::ReadAheadStatistics* statistics) const {
    if (statistics != nullptr) {
        COUNTER_UPDATE(&statistics->window_discard_time, window_discard_ns);
        COUNTER_UPDATE(&statistics->current_batch_plan_time, current_batch_plan_ns);
        COUNTER_UPDATE(&statistics->window_extend_time, window_extend_ns);
    }
}

const ColumnReadAheadOptions& ColumnReadAheadContext::options(ColumnReadAheadRole role) const {
    return role == ColumnReadAheadRole::EAGER ? eager_options : lazy_options;
}

const ColumnReadAheadOptions& ColumnReadAheadRequest::options() const {
    DORIS_CHECK(context != nullptr);
    return context->options(role);
}

void ColumnReadAheadRequest::sanity_check() const {
    DORIS_CHECK(scan_rowids != nullptr);
    DORIS_CHECK(context != nullptr);
    if (page_driven) {
        DORIS_CHECK(!scan_rowids->isEmpty());
    } else {
        DORIS_CHECK(current_rowids != nullptr);
        DORIS_CHECK(current_rowid_count > 0);
        DCHECK(std::is_sorted(current_rowids, current_rowids + current_rowid_count));
    }
}

Status ColumnReadAheadOptions::validate() const {
    if (window_bytes == 0) {
        return Status::InvalidArgument("column read-ahead window bytes must be positive");
    }
    return Status::OK();
}

ColumnReadAhead::ColumnReadAhead(std::vector<ColumnReadAheadPage> pages,
                                 ColumnReadAheadOptions options, bool reverse)
        : _pages(std::move(pages)),
          _options(options),
          _reverse(reverse),
          _next_page_index(reverse ? cast_set<int64_t>(_pages.size()) - 1 : 0),
          _reverse_low_page_index(cast_set<int32_t>(_pages.size())) {
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
    DCHECK(std::is_sorted(current_rowids, current_rowids + count));
    output->reset(this);
    _select_candidate_pages(scan_rowids);
    _discard_passed_pages(_page_for_ordinal(current_rowids[_reverse ? count - 1 : 0]).page_index,
                          output);
    SCOPED_RAW_TIMER(&output->current_batch_plan_ns);
    const rowid_t* begin = current_rowids;
    const rowid_t* end = current_rowids + count;
    while (begin != end) {
        const auto& page = _page_for_ordinal(_reverse ? end[-1] : *begin);
        _plan_page(page.page_index, output);
        // Row IDs are sorted. Locate the next distinct page instead of looking up every row.
        if (_reverse) {
            end = std::lower_bound(begin, end, page.first_ordinal);
        } else {
            begin = std::upper_bound(begin, end, page.last_ordinal);
        }
    }
}

void ColumnReadAhead::_select_candidate_pages(const roaring::Roaring& scan_rowids) {
    _candidate_pages.clear();
    for (auto row = scan_rowids.begin(); row != scan_rowids.end();) {
        const auto& page = _page_for_ordinal(*row);
        _candidate_pages.push_back(page.page_index);
        if (page.last_ordinal >= std::numeric_limits<rowid_t>::max()) {
            break;
        }
        row.equalorlarger(cast_set<rowid_t>(page.last_ordinal + 1));
    }
}

void ColumnReadAhead::start(const roaring::Roaring& scan_rowids, ColumnReadAheadPlan* output) {
    DORIS_CHECK(!scan_rowids.isEmpty());
    _select_candidate_pages(scan_rowids);
    advance(_reverse ? _candidate_pages.back() : _candidate_pages.front(), output);
}

void ColumnReadAhead::advance(int32_t page_index, ColumnReadAheadPlan* output) {
    DCHECK_GE(page_index, 0);
    DCHECK_LT(page_index, _pages.size());
    output->reset(this);
    if (!_reverse) {
        _discard_passed_pages(page_index, output);
    } else if (page_index < _reverse_low_page_index) {
        _discard_passed_pages(_reverse_low_page_index, output);
        _reverse_low_page_index = page_index;
    }
    _plan_page(page_index, output);
}

void ColumnReadAhead::_plan_page(int32_t page_index, ColumnReadAheadPlan* output) {
    _add_page(_pages[page_index], output);
    const bool beyond_window =
            _reverse ? page_index <= _next_page_index : page_index >= _next_page_index;
    if (beyond_window) {
        _next_page_index = page_index;
        _extend_window(output);
    }
    if (_next_trigger_page_index >= 0 && (_reverse ? page_index <= _next_trigger_page_index
                                                   : page_index >= _next_trigger_page_index)) {
        _extend_window(output);
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
    if (!_window.try_emplace(page.page_index).second) {
        return;
    }
    DORIS_CHECK(page.range.size <= std::numeric_limits<size_t>::max() - _pending_bytes);
    _pending_bytes += page.range.size;
    output->new_pages.push_back(page);
}

void ColumnReadAhead::_discard_passed_pages(int32_t page_index, ColumnReadAheadPlan* output) {
    SCOPED_RAW_TIMER(&output->window_discard_ns);
    // Only the prefix/suffix behind the reader can retire; the rest of the window stays intact.
    auto entry = _reverse ? _window.upper_bound(page_index) : _window.begin();
    while (entry != _window.end() && (_reverse || entry->first < page_index)) {
        const auto& page = _pages[entry->first];
        _complete(page, &entry->second);
        output->released_pages.push_back(page);
        entry = _window.erase(entry);
    }
}

void ColumnReadAhead::_extend_window(ColumnReadAheadPlan* output) {
    SCOPED_RAW_TIMER(&output->window_extend_ns);
    _next_trigger_page_index = -1;
    if (_next_page_index < 0 || _next_page_index >= cast_set<int64_t>(_pages.size())) {
        return;
    }

    const auto next = _reverse ? std::upper_bound(_candidate_pages.begin(), _candidate_pages.end(),
                                                  _next_page_index)
                               : std::lower_bound(_candidate_pages.begin(), _candidate_pages.end(),
                                                  _next_page_index);
    int64_t position = std::distance(_candidate_pages.begin(), next) - (_reverse ? 1 : 0);
    size_t window_bytes = 0;
    size_t page_count = 0;
    while (window_bytes < _options.window_bytes && position >= 0 &&
           position < cast_set<int64_t>(_candidate_pages.size())) {
        const auto& page = _pages[_candidate_pages[position]];
        _add_page(page, output);
        DORIS_CHECK(page.range.size <= std::numeric_limits<size_t>::max() - window_bytes);
        window_bytes += page.range.size;
        if (page_count < 2) {
            _next_trigger_page_index = page.page_index;
        }
        ++page_count;
        _next_page_index = cast_set<int64_t>(page.page_index) + (_reverse ? -1 : 1);
        position += _reverse ? -1 : 1;
    }
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
