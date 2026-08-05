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

#include "storage/segment/page_prefetcher.h"

#include <algorithm>
#include <limits>

#include "common/cast_set.h"
#include "common/logging.h"

namespace doris::segment_v2 {
namespace {

constexpr uint32_t MIN_PAGE_SIZE = 8;

Status find_page_for_ordinal(const std::vector<PageCandidate>& pages, ordinal_t ordinal,
                             size_t* page_position) {
    DORIS_CHECK(page_position != nullptr);
    auto iterator = std::upper_bound(
            pages.begin(), pages.end(), ordinal,
            [](ordinal_t value, const PageCandidate& page) { return value < page.first_ordinal; });
    if (iterator == pages.begin()) {
        return Status::InvalidArgument("ordinal {} is before the first page", ordinal);
    }
    --iterator;
    if (ordinal > iterator->last_ordinal) {
        return Status::InvalidArgument("ordinal {} is not covered by a data page", ordinal);
    }
    *page_position = cast_set<size_t>(iterator - pages.begin());
    return Status::OK();
}

void sort_by_file_offset(std::vector<PageCandidate>* pages) {
    DORIS_CHECK(pages != nullptr);
    std::sort(pages->begin(), pages->end(),
              [](const PageCandidate& left, const PageCandidate& right) {
                  return left.offset < right.offset;
              });
}

bool within_read_amplification(size_t fetched_bytes, size_t requested_bytes,
                               double max_read_amplification_ratio) {
    DORIS_CHECK(requested_bytes > 0);
    return static_cast<long double>(fetched_bytes) / static_cast<long double>(requested_bytes) <=
           static_cast<long double>(max_read_amplification_ratio);
}

} // namespace

Status validate_page_candidates(const std::vector<PageCandidate>& pages, uint64_t file_size) {
    uint64_t previous_end = 0;
    uint32_t previous_page_index = 0;
    ordinal_t previous_last_ordinal = 0;
    bool has_previous = false;
    for (const auto& page : pages) {
        if (page.size < MIN_PAGE_SIZE) {
            return Status::Corruption("data page {} has invalid size {}", page.page_index,
                                      page.size);
        }
        if (page.offset > file_size || page.size > file_size - page.offset) {
            return Status::Corruption("data page {} at offset {} with size {} exceeds file size {}",
                                      page.page_index, page.offset, page.size, file_size);
        }
        if (page.first_ordinal > page.last_ordinal) {
            return Status::Corruption("data page {} has invalid ordinal range [{}, {}]",
                                      page.page_index, page.first_ordinal, page.last_ordinal);
        }
        const uint64_t page_end = page.offset + page.size;
        if (has_previous) {
            if (page.offset < previous_end) {
                return Status::Corruption("data page {} overlaps or precedes the previous page",
                                          page.page_index);
            }
            if (page.page_index <= previous_page_index) {
                return Status::Corruption("data page index {} does not follow {}", page.page_index,
                                          previous_page_index);
            }
            if (page.first_ordinal <= previous_last_ordinal) {
                return Status::Corruption("data page {} ordinal range overlaps the previous page",
                                          page.page_index);
            }
        }
        previous_end = page_end;
        previous_page_index = page.page_index;
        previous_last_ordinal = page.last_ordinal;
        has_previous = true;
    }
    return Status::OK();
}

Status PageReadPlanner::plan(const std::vector<PageCandidate>& pages, uint64_t file_size,
                             const PagePrefetchOptions& options, PageFetchPlan* plan) const {
    DORIS_CHECK(plan != nullptr);
    DORIS_CHECK(options.max_gap_bytes > 0);
    DORIS_CHECK(options.max_range_bytes > 0);
    DORIS_CHECK(options.max_pages_per_range > 0);
    DORIS_CHECK(options.max_read_amplification_ratio >= 1.0);
    RETURN_IF_ERROR(validate_page_candidates(pages, file_size));

    PageFetchPlan result;
    result.candidate_pages = pages.size();
    for (const auto& page : pages) {
        if (page.size > options.max_range_bytes) {
            return Status::InvalidArgument("data page {} size {} exceeds max prefetch range {}",
                                           page.page_index, page.size, options.max_range_bytes);
        }
        if (result.requested_page_bytes >
            std::numeric_limits<size_t>::max() - static_cast<size_t>(page.size)) {
            return Status::Corruption("total requested page bytes overflow");
        }
        result.requested_page_bytes += page.size;

        bool merge = false;
        if (!result.ranges.empty()) {
            auto& range = result.ranges.back();
            const uint64_t range_end = range.offset + range.size;
            const uint64_t page_end = page.offset + page.size;
            const uint64_t gap = page.offset - range_end;
            const uint64_t merged_size = page_end - range.offset;
            const size_t merged_requested_bytes = range.requested_page_bytes + page.size;
            merge = gap <= options.max_gap_bytes && merged_size <= options.max_range_bytes &&
                    range.pages.size() < options.max_pages_per_range &&
                    within_read_amplification(cast_set<size_t>(merged_size), merged_requested_bytes,
                                              options.max_read_amplification_ratio);
        }

        if (!merge) {
            PageFetchRangeSpec range;
            range.offset = page.offset;
            range.size = page.size;
            range.requested_page_bytes = page.size;
            range.pages.push_back(PageSliceDescriptor {
                    .page_index = page.page_index,
                    .page_offset = page.offset,
                    .page_size = page.size,
                    .buffer_offset = 0,
            });
            result.ranges.push_back(std::move(range));
            continue;
        }

        auto& range = result.ranges.back();
        const uint64_t page_end = page.offset + page.size;
        range.size = cast_set<size_t>(page_end - range.offset);
        range.requested_page_bytes += page.size;
        range.coalesced_gap_bytes = range.size - range.requested_page_bytes;
        range.pages.push_back(PageSliceDescriptor {
                .page_index = page.page_index,
                .page_offset = page.offset,
                .page_size = page.size,
                .buffer_offset = cast_set<size_t>(page.offset - range.offset),
        });
    }

    for (size_t range_index = 0; range_index < result.ranges.size(); ++range_index) {
        const auto& range = result.ranges[range_index];
        DORIS_CHECK(range.size == range.requested_page_bytes + range.coalesced_gap_bytes);
        DORIS_CHECK(result.fetched_bytes <= std::numeric_limits<size_t>::max() - range.size);
        result.fetched_bytes += range.size;
        for (size_t descriptor_index = 0; descriptor_index < range.pages.size();
             ++descriptor_index) {
            const auto& descriptor = range.pages[descriptor_index];
            DORIS_CHECK(descriptor.buffer_offset + descriptor.page_size <= range.size);
            const auto [iterator, inserted] = result.page_to_range.emplace(
                    descriptor.page_index, std::pair {range_index, descriptor_index});
            static_cast<void>(iterator);
            DORIS_CHECK(inserted);
        }
    }
    DORIS_CHECK(result.page_to_range.size() == result.candidate_pages);
    DORIS_CHECK(result.requested_page_bytes == 0 ||
                within_read_amplification(result.fetched_bytes, result.requested_page_bytes,
                                          options.max_read_amplification_ratio));
    *plan = std::move(result);
    return Status::OK();
}

bool FixedPagePrefetchWindow::needs_refill(size_t unconsumed_planned_pages,
                                           size_t target_window_pages) {
    DORIS_CHECK(target_window_pages > 0);
    return unconsumed_planned_pages <= std::max<size_t>(1, target_window_pages / 2);
}

Status FixedPagePrefetchWindow::select_ordinal_range(
        const std::vector<PageCandidate>& all_pages, uint64_t file_size, ordinal_t first_ordinal,
        size_t ordinal_count, bool is_forward, size_t target_window_pages,
        const std::unordered_set<uint32_t>& tracked_pages,
        std::vector<PageCandidate>* selected_pages) const {
    DORIS_CHECK(selected_pages != nullptr);
    DORIS_CHECK(target_window_pages > 0);
    if (ordinal_count == 0) {
        return Status::InvalidArgument("ordinal prefetch count must be positive");
    }
    RETURN_IF_ERROR(validate_page_candidates(all_pages, file_size));

    const ordinal_t ordinal_distance = cast_set<ordinal_t>(ordinal_count - 1);
    ordinal_t last_required_ordinal = 0;
    if (is_forward) {
        if (first_ordinal > std::numeric_limits<ordinal_t>::max() - ordinal_distance) {
            return Status::InvalidArgument("forward ordinal prefetch range overflows");
        }
        last_required_ordinal = first_ordinal + ordinal_distance;
    } else {
        if (first_ordinal < ordinal_distance) {
            return Status::InvalidArgument("reverse ordinal prefetch range underflows");
        }
        last_required_ordinal = first_ordinal - ordinal_distance;
    }

    size_t first_page_position = 0;
    size_t last_required_page_position = 0;
    RETURN_IF_ERROR(find_page_for_ordinal(all_pages, first_ordinal, &first_page_position));
    RETURN_IF_ERROR(
            find_page_for_ordinal(all_pages, last_required_ordinal, &last_required_page_position));
    DORIS_CHECK(is_forward ? first_page_position <= last_required_page_position
                           : first_page_position >= last_required_page_position);

    std::vector<PageCandidate> result;
    size_t page_position = first_page_position;
    size_t window_positions = 0;
    while (true) {
        const auto& page = all_pages[page_position];
        if (!tracked_pages.contains(page.page_index)) {
            result.push_back(page);
        }
        ++window_positions;

        const bool required_pages_covered = is_forward
                                                    ? page_position >= last_required_page_position
                                                    : page_position <= last_required_page_position;
        if (required_pages_covered && window_positions >= target_window_pages) {
            break;
        }
        if (is_forward) {
            if (page_position + 1 == all_pages.size()) {
                break;
            }
            ++page_position;
        } else {
            if (page_position == 0) {
                break;
            }
            --page_position;
        }
    }
    sort_by_file_offset(&result);
    *selected_pages = std::move(result);
    return Status::OK();
}

Status FixedPagePrefetchWindow::select_rowids(const std::vector<PageCandidate>& all_pages,
                                              uint64_t file_size, const rowid_t* rowids,
                                              size_t rowid_count,
                                              const std::unordered_set<uint32_t>& tracked_pages,
                                              std::vector<PageCandidate>* selected_pages) const {
    DORIS_CHECK(selected_pages != nullptr);
    if (rowid_count == 0) {
        selected_pages->clear();
        return Status::OK();
    }
    DORIS_CHECK(rowids != nullptr);
    RETURN_IF_ERROR(validate_page_candidates(all_pages, file_size));

    std::unordered_set<uint32_t> selected_page_indexes;
    std::vector<PageCandidate> result;
    for (size_t index = 0; index < rowid_count; ++index) {
        size_t page_position = 0;
        RETURN_IF_ERROR(find_page_for_ordinal(all_pages, rowids[index], &page_position));
        const auto& page = all_pages[page_position];
        if (tracked_pages.contains(page.page_index) ||
            !selected_page_indexes.emplace(page.page_index).second) {
            continue;
        }
        result.push_back(page);
    }
    sort_by_file_offset(&result);
    *selected_pages = std::move(result);
    return Status::OK();
}

} // namespace doris::segment_v2
