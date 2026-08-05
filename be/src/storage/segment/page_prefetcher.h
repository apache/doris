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
#include <functional>
#include <memory>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/segment/common.h"
#include "storage/segment/page_prefetch_io_service.h"

namespace doris::io {
class CachedRemoteFileReader;
}

namespace doris::segment_v2 {

class PagePrefetchIOService;

struct PageCandidate {
    uint32_t page_index = 0;
    ordinal_t first_ordinal = 0;
    ordinal_t last_ordinal = 0;
    uint64_t offset = 0;
    uint32_t size = 0;
};

struct PagePrefetchOptions {
    size_t window_pages = 16;
    size_t min_window_pages = 1;
    size_t max_window_pages = 64;
    size_t max_gap_bytes = 64 * 1024;
    size_t max_range_bytes = 4 * 1024 * 1024;
    size_t max_pages_per_range = 32;
    double max_read_amplification_ratio = 2.0;
    double writeback_min_block_coverage = 0.5;
    bool adaptive_window = false;
};

/// Return whether one planned fetch stays within the configured read amplification limit.
bool within_read_amplification(size_t fetched_bytes, size_t requested_bytes,
                               double max_read_amplification_ratio);

struct PageFetchPlan {
    std::vector<PageFetchRangeSpec> ranges;
    std::unordered_map<uint32_t, std::pair<size_t, size_t>> page_to_range;
    size_t candidate_pages = 0;
    size_t requested_page_bytes = 0;
    size_t fetched_bytes = 0;
};

Status validate_page_candidates(const std::vector<PageCandidate>& pages, uint64_t file_size);

/// Coalesces validated page intervals without performing IO or cache block completion.
class PageReadPlanner {
public:
    Status plan(const std::vector<PageCandidate>& pages, uint64_t file_size,
                const PagePrefetchOptions& options, PageFetchPlan* plan) const;
};

/// Selects a fixed page window in consumption order and returns new candidates in file order.
class FixedPagePrefetchWindow {
public:
    static bool needs_refill(size_t unconsumed_planned_pages, size_t target_window_pages);

    Status select_ordinal_range(const std::vector<PageCandidate>& all_pages, uint64_t file_size,
                                ordinal_t first_ordinal, size_t ordinal_count, bool is_forward,
                                size_t target_window_pages,
                                const std::unordered_set<uint32_t>& tracked_pages,
                                std::vector<PageCandidate>* selected_pages) const;

    Status select_rowids(const std::vector<PageCandidate>& all_pages, uint64_t file_size,
                         const rowid_t* rowids, size_t rowid_count,
                         const std::unordered_set<uint32_t>& tracked_pages,
                         std::vector<PageCandidate>* selected_pages) const;
};

struct PagePrefetchRequest {
    enum class Kind : uint8_t {
        ORDINAL_RANGE,
        ROWIDS,
    };

    Kind kind = Kind::ORDINAL_RANGE;
    ordinal_t first_ordinal = 0;
    size_t ordinal_count = 0;
    const rowid_t* rowids = nullptr;
    size_t rowid_count = 0;
    bool is_forward = true;
};

struct PrefetchedPageSlice {
    std::shared_ptr<PrefetchRange> range;
    size_t descriptor_index = 0;
    Slice data;
};

struct PagePrefetcherStatistics {
    size_t candidate_pages = 0;
    size_t submitted_pages = 0;
    size_t consumed_pages = 0;
    size_t page_cache_skipped_pages = 0;
    size_t ready_hits = 0;
    size_t fallback_pages = 0;
    size_t submitted_ranges = 0;
    size_t throttled_ranges = 0;
    size_t cancelled_ranges = 0;
    int64_t wait_time_ns = 0;
    int64_t io_time_ns = 0;
    size_t requested_page_bytes = 0;
    size_t fetched_bytes = 0;
    size_t coalesced_gap_bytes = 0;
    size_t block_fill_bytes = 0;
    size_t cache_or_inflight_bytes = 0;
    size_t remote_bytes = 0;
    size_t writeback_eligible_blocks = 0;
};

struct PagePrefetcherContext {
    PagePrefetchIOService* io_service = nullptr;
    std::shared_ptr<io::CachedRemoteFileReader> reader;
    std::shared_ptr<PagePrefetchQueryContext> query_context;
    PagePrefetchSafeIOContext io_context;
    std::vector<PageCandidate> pages;
    uint64_t file_size = 0;
    PagePrefetchOptions options;
    std::function<bool(const PageCandidate&)> page_cache_probe;
};

/// Owns the speculative page state for one physical FileColumnIterator. All methods except
/// cancel() are called by the iterator's query thread; range completion remains worker-owned.
class PagePrefetcher {
public:
    explicit PagePrefetcher(PagePrefetcherContext context);
    ~PagePrefetcher();

    Status prepare(const PagePrefetchRequest& request);
    Result<std::optional<PrefetchedPageSlice>> acquire(uint32_t page_index);
    void mark_consumed(uint32_t page_index);
    void mark_decode_failed(uint32_t page_index);
    void mark_page_cache_hit(uint32_t page_index);
    void mark_skipped_before(uint32_t page_index, bool is_forward);
    void cancel();

    const PagePrefetcherStatistics& statistics() const { return _statistics; }
    size_t tracked_pages() const { return _entries.size(); }

private:
    struct PageEntry {
        enum class State : uint8_t {
            PLANNED,
            FALLBACK,
            CONSUMED,
            SKIPPED,
        };

        std::shared_ptr<PrefetchRange> range;
        size_t descriptor_index = 0;
        State state = State::PLANNED;
    };

    Status _select_candidates(const PagePrefetchRequest& request,
                              const std::unordered_set<uint32_t>& tracked_pages,
                              std::vector<PageCandidate>* selected_pages);
    void _mark_fallback(uint32_t page_index);
    void _merge_range_statistics(const std::shared_ptr<PrefetchRange>& range);

    PagePrefetchIOService* const _io_service;
    const std::shared_ptr<io::CachedRemoteFileReader> _reader;
    const std::shared_ptr<PagePrefetchQueryContext> _query_context;
    const PagePrefetchSafeIOContext _io_context;
    const std::vector<PageCandidate> _pages;
    const uint64_t _file_size;
    const PagePrefetchOptions _options;
    const std::function<bool(const PageCandidate&)> _page_cache_probe;
    std::unordered_map<uint32_t, PageEntry> _entries;
    PagePrefetcherStatistics _statistics;
};

} // namespace doris::segment_v2
