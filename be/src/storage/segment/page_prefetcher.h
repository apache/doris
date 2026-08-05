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
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/segment/common.h"
#include "storage/segment/page_prefetch_io_service.h"

namespace doris::segment_v2 {

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

} // namespace doris::segment_v2
