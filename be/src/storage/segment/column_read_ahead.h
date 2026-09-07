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
#include <map>
#include <memory>
#include <roaring/roaring.hh>
#include <vector>

#include "common/status.h"
#include "io/fs/file_range_coalescer.h"
#include "storage/segment/common.h"

namespace doris::segment_v2 {

struct ColumnReadAheadOptions {
    /// Refill target measured in compressed data-page bytes; the final page may overshoot it.
    size_t high_watermark_bytes {1};
    /// Refill trigger measured after consumed/discarded pages leave pending-byte accounting.
    size_t low_watermark_bytes {0};

    Status validate() const;
    bool operator==(const ColumnReadAheadOptions&) const = default;
};

struct ColumnReadAheadPage {
    /// Stable position in the column's ordered data-page list.
    int32_t page_index {0};
    /// Inclusive row-ordinal bounds represented by this data page.
    ordinal_t first_ordinal {0};
    ordinal_t last_ordinal {0};
    /// Compressed page bytes in the segment file.
    io::FileRange range;

    bool operator==(const ColumnReadAheadPage&) const = default;
};

class ColumnReadAhead;
class SegmentReadAhead;

struct ColumnReadAheadPlan {
    ColumnReadAhead* column {nullptr};
    /// Pages newly entering the window and eligible for cache probe and range planning.
    std::vector<ColumnReadAheadPage> new_pages;
    /// Window entries that the scan has passed; the coordinator drops any remaining registration.
    std::vector<ColumnReadAheadPage> released_pages;

    bool empty() const { return new_pages.empty() && released_pages.empty(); }
};

enum class ColumnReadAheadRole : uint8_t {
    /// The column has no earlier filtering dependency and uses the larger look-ahead window.
    EAGER,
    /// The column depends on earlier filtering and uses the smaller look-ahead window.
    LAZY,
};

struct ColumnReadAheadContext {
    ColumnReadAheadOptions eager_options;
    ColumnReadAheadOptions lazy_options;
    SegmentReadAhead* segment {nullptr};

    const ColumnReadAheadOptions& options(ColumnReadAheadRole role) const;
};

struct ColumnReadAheadRequest {
    /// Sorted row IDs needed by the current batch; their pages enter the window before extension.
    const rowid_t* current_rowids {nullptr};
    size_t current_rowid_count {0};
    /// All row IDs still eligible in scan order, used only to extend beyond the current batch.
    const roaring::Roaring* scan_rowids {nullptr};
    const ColumnReadAheadContext* context {nullptr};
    ColumnReadAheadRole role {ColumnReadAheadRole::EAGER};
    bool reverse {false};

    const ColumnReadAheadOptions& options() const;
    void sanity_check() const;
};

/// Maintains the byte window of one physical column. The class only decides which compressed
/// data pages belong to the window; it does not inspect caches, coalesce ranges, or perform IO.
class ColumnReadAhead {
public:
    ColumnReadAhead(const ColumnReadAhead&) = delete;
    ColumnReadAhead& operator=(const ColumnReadAhead&) = delete;

    /// `pages` must be non-empty and ordered by disjoint row-ordinal spans, with page_index equal
    /// to each page's vector position.
    static Status create(std::vector<ColumnReadAheadPage> pages, ColumnReadAheadOptions options,
                         bool reverse, std::unique_ptr<ColumnReadAhead>* output);

    /// Adds every page touched by the current batch, discards predictions already passed by the
    /// scan, and extends through remaining scan rowids when the window reaches its low watermark.
    /// Current-batch pages may take the window beyond the high watermark.
    void plan(const rowid_t* current_rowids, size_t count, const roaring::Roaring& scan_rowids,
              ColumnReadAheadPlan* output);

    /// Stop counting a planned page toward the byte window. The completed entry remains in the
    /// window until the scan passes it, so repeated row IDs in the same page do not submit it
    /// again.
    void complete(int32_t page_index);

    size_t pending_bytes() const { return _pending_bytes; }
    bool reverse() const { return _reverse; }
    const ColumnReadAheadOptions& options() const { return _options; }
    bool pending(int32_t page_index) const;

private:
    struct WindowEntry {
        bool pending {true};
    };

    ColumnReadAhead(std::vector<ColumnReadAheadPage> pages, ColumnReadAheadOptions options,
                    bool reverse);

    const ColumnReadAheadPage& _page_for_ordinal(rowid_t ordinal) const;
    /// Add one page only if the window has not already seen it.
    void _add_page(const ColumnReadAheadPage& page, ColumnReadAheadPlan* output);
    /// Remove window entries strictly behind the current batch in the configured scan direction.
    void _discard_passed_pages(const rowid_t* current_rowids, size_t count,
                               ColumnReadAheadPlan* output);
    /// Move the future-row cursor past the current batch without moving it backward.
    void _sync_future_cursor(const rowid_t* current_rowids, size_t count,
                             const roaring::Roaring& scan_rowids);
    /// Add whole pages selected by future scan rows until the high watermark is reached.
    void _extend_window(const roaring::Roaring& scan_rowids, ColumnReadAheadPlan* output);
    void _advance_future_cursor_past(const ColumnReadAheadPage& page,
                                     const roaring::Roaring& scan_rowids);
    void _complete(const ColumnReadAheadPage& page, WindowEntry* entry);

    const std::vector<ColumnReadAheadPage> _pages;
    const ColumnReadAheadOptions _options;
    const bool _reverse;
    std::map<int32_t, WindowEntry> _window;
    size_t _pending_bytes {0};
    int64_t _next_scan_rank {0};
    bool _planned_once {false};
};

} // namespace doris::segment_v2
