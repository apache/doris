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
#include <chrono>
#include <limits>

#include "common/cast_set.h"
#include "common/config.h"
#include "common/logging.h"
#include "io/cache/cached_remote_file_reader.h"
#include "storage/segment/file_cache_writeback_coordinator.h"

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

} // namespace

bool within_read_amplification(size_t fetched_bytes, size_t requested_bytes,
                               double max_read_amplification_ratio) {
    DORIS_CHECK(requested_bytes > 0);
    return static_cast<long double>(fetched_bytes) / static_cast<long double>(requested_bytes) <=
           static_cast<long double>(max_read_amplification_ratio);
}

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

PagePrefetcher::PagePrefetcher(PagePrefetcherContext context)
        : _io_service(context.io_service),
          _reader(std::move(context.reader)),
          _query_context(std::move(context.query_context)),
          _io_context(std::move(context.io_context)),
          _pages(std::move(context.pages)),
          _file_size(context.file_size),
          _options(context.options),
          _page_cache_probe(std::move(context.page_cache_probe)) {
    DORIS_CHECK(_io_service != nullptr);
    DORIS_CHECK(_reader != nullptr);
    DORIS_CHECK(_query_context != nullptr);
    DORIS_CHECK(_file_size == _reader->size());
    DORIS_CHECK(_options.window_pages > 0);
    DORIS_CHECK(_options.min_window_pages > 0);
    DORIS_CHECK(_options.min_window_pages <= _options.window_pages);
    DORIS_CHECK(_options.window_pages <= _options.max_window_pages);
    DORIS_CHECK(_options.max_gap_bytes > 0);
    DORIS_CHECK(_options.max_range_bytes > 0);
    DORIS_CHECK(_options.max_pages_per_range > 0);
    DORIS_CHECK(_options.max_read_amplification_ratio >= 1.0);
    DORIS_CHECK(_options.writeback_min_block_coverage > 0.0);
    DORIS_CHECK(_options.writeback_min_block_coverage <= 1.0);
}

PagePrefetcher::~PagePrefetcher() {
    cancel();
}

Status PagePrefetcher::prepare(const PagePrefetchRequest& request) {
    if (!config::enable_query_page_prefetch || !config::enable_async_file_cache_write) {
        return Status::OK();
    }
    if (_query_context->cancelled()) {
        return Status::Cancelled("page prefetch query is cancelled");
    }
    if ((request.kind == PagePrefetchRequest::Kind::ORDINAL_RANGE && request.ordinal_count == 0) ||
        (request.kind == PagePrefetchRequest::Kind::ROWIDS && request.rowid_count == 0)) {
        return Status::OK();
    }

    if (request.kind == PagePrefetchRequest::Kind::ORDINAL_RANGE) {
        size_t page_position = 0;
        RETURN_IF_ERROR(find_page_for_ordinal(_pages, request.first_ordinal, &page_position));
        mark_skipped_before(_pages[page_position].page_index, request.is_forward);
    }

    std::unordered_set<uint32_t> tracked_pages;
    tracked_pages.reserve(_entries.size());
    for (const auto& [page_index, entry] : _entries) {
        static_cast<void>(entry);
        tracked_pages.emplace(page_index);
    }

    std::vector<PageCandidate> selected_pages;
    RETURN_IF_ERROR(_select_candidates(request, tracked_pages, &selected_pages));
    _statistics.candidate_pages += selected_pages.size();

    std::vector<PageCandidate> pages_to_submit;
    pages_to_submit.reserve(selected_pages.size());
    for (const auto& page : selected_pages) {
        if (_page_cache_probe && _page_cache_probe(page)) {
            const auto [iterator, inserted] = _entries.emplace(
                    page.page_index, PageEntry {.range = nullptr,
                                                .descriptor_index = 0,
                                                .state = PageEntry::State::SKIPPED});
            static_cast<void>(iterator);
            DORIS_CHECK(inserted);
            ++_statistics.page_cache_skipped_pages;
        } else {
            pages_to_submit.push_back(page);
        }
    }
    if (pages_to_submit.empty()) {
        return Status::OK();
    }

    PageFetchPlan plan;
    Status plan_status;
    const size_t block_size = static_cast<size_t>(config::file_cache_each_block_size);
    if (!_io_context.remote_only_on_miss && _options.max_range_bytes >= block_size) {
        FileCacheWritebackCoordinator coordinator;
        plan_status =
                coordinator.plan_block_completion(pages_to_submit, _file_size, _options, &plan);
    } else {
        PageReadPlanner planner;
        plan_status = planner.plan(pages_to_submit, _file_size, _options, &plan);
    }
    if (plan_status.is<ErrorCode::INVALID_ARGUMENT>()) {
        for (const auto& page : pages_to_submit) {
            _mark_fallback(page.page_index);
        }
        return Status::OK();
    }
    RETURN_IF_ERROR(plan_status);

    for (auto& range_spec : plan.ranges) {
        const auto descriptors = range_spec.pages;
        const size_t requested_page_bytes = range_spec.requested_page_bytes;
        const size_t fetched_bytes = range_spec.size;
        const size_t coalesced_gap_bytes = range_spec.coalesced_gap_bytes;
        const size_t block_fill_bytes = range_spec.block_fill_bytes;
        const size_t writeback_eligible_blocks = range_spec.complete_blocks.size();
        auto submit = _io_service->try_submit(std::move(range_spec), _reader, _io_context,
                                              _query_context);
        if (submit.range == nullptr) {
            for (const auto& descriptor : descriptors) {
                _mark_fallback(descriptor.page_index);
            }
            if (submit.reject_reason == PagePrefetchRejectReason::QUERY_CANCELLED) {
                ++_statistics.cancelled_ranges;
                return Status::Cancelled("page prefetch query is cancelled");
            }
            ++_statistics.throttled_ranges;
            continue;
        }

        ++_statistics.submitted_ranges;
        _statistics.submitted_pages += descriptors.size();
        _statistics.requested_page_bytes += requested_page_bytes;
        _statistics.fetched_bytes += fetched_bytes;
        _statistics.coalesced_gap_bytes += coalesced_gap_bytes;
        _statistics.block_fill_bytes += block_fill_bytes;
        _statistics.writeback_eligible_blocks += writeback_eligible_blocks;
        for (size_t descriptor_index = 0; descriptor_index < descriptors.size();
             ++descriptor_index) {
            const auto [iterator, inserted] =
                    _entries.emplace(descriptors[descriptor_index].page_index,
                                     PageEntry {.range = submit.range,
                                                .descriptor_index = descriptor_index,
                                                .state = PageEntry::State::PLANNED});
            static_cast<void>(iterator);
            DORIS_CHECK(inserted);
        }
    }
    return Status::OK();
}

Result<std::optional<PrefetchedPageSlice>> PagePrefetcher::acquire(uint32_t page_index) {
    auto iterator = _entries.find(page_index);
    if (iterator == _entries.end()) {
        _mark_fallback(page_index);
        return std::optional<PrefetchedPageSlice> {};
    }
    auto& entry = iterator->second;
    if (entry.state != PageEntry::State::PLANNED) {
        return std::optional<PrefetchedPageSlice> {};
    }
    DORIS_CHECK(entry.range != nullptr);

    const bool ready_before_consume = entry.range->state() == PrefetchRange::State::READY;
    const auto wait_start = std::chrono::steady_clock::now();
    Status status = entry.range->wait_for_consume();
    _statistics.wait_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(
                                        std::chrono::steady_clock::now() - wait_start)
                                        .count();
    _merge_range_statistics(entry.range);
    if (!status.ok()) {
        entry.state = PageEntry::State::FALLBACK;
        entry.range.reset();
        ++_statistics.fallback_pages;
        if (_query_context->cancelled()) {
            return ResultError(Status::Cancelled("page prefetch query is cancelled"));
        }
        return std::optional<PrefetchedPageSlice> {};
    }
    if (ready_before_consume) {
        ++_statistics.ready_hits;
    }
    return std::optional<PrefetchedPageSlice>(PrefetchedPageSlice {
            .range = entry.range,
            .descriptor_index = entry.descriptor_index,
            .data = entry.range->page_slice(entry.descriptor_index),
    });
}

void PagePrefetcher::mark_consumed(uint32_t page_index) {
    auto iterator = _entries.find(page_index);
    DORIS_CHECK(iterator != _entries.end());
    auto& entry = iterator->second;
    DORIS_CHECK(entry.state == PageEntry::State::PLANNED);
    DORIS_CHECK(entry.range != nullptr);
    FileCacheWritebackCoordinator(_io_service).mark_page_consumed(entry.range, page_index);
    entry.state = PageEntry::State::CONSUMED;
    entry.range.reset();
    ++_statistics.consumed_pages;
}

void PagePrefetcher::mark_decode_failed(uint32_t page_index) {
    auto iterator = _entries.find(page_index);
    DORIS_CHECK(iterator != _entries.end());
    auto& entry = iterator->second;
    DORIS_CHECK(entry.state == PageEntry::State::PLANNED);
    DORIS_CHECK(entry.range != nullptr);
    FileCacheWritebackCoordinator(_io_service).invalidate_page(entry.range, page_index);
    entry.state = PageEntry::State::FALLBACK;
    entry.range.reset();
    ++_statistics.fallback_pages;
}

void PagePrefetcher::mark_page_cache_hit(uint32_t page_index) {
    auto iterator = _entries.find(page_index);
    if (iterator == _entries.end() || iterator->second.state != PageEntry::State::PLANNED) {
        return;
    }
    iterator->second.state = PageEntry::State::SKIPPED;
    iterator->second.range.reset();
    ++_statistics.page_cache_skipped_pages;
}

void PagePrefetcher::mark_skipped_before(uint32_t page_index, bool is_forward) {
    for (auto& [tracked_page_index, entry] : _entries) {
        const bool skipped =
                is_forward ? tracked_page_index < page_index : tracked_page_index > page_index;
        if (skipped && entry.state == PageEntry::State::PLANNED) {
            entry.state = PageEntry::State::SKIPPED;
            entry.range.reset();
        }
    }
}

void PagePrefetcher::cancel() {
    std::unordered_set<PrefetchRange*> cancelled_ranges;
    for (auto& [page_index, entry] : _entries) {
        static_cast<void>(page_index);
        if (entry.range != nullptr && cancelled_ranges.emplace(entry.range.get()).second) {
            entry.range->request_cancel();
        }
        entry.range.reset();
    }
}

Status PagePrefetcher::_select_candidates(const PagePrefetchRequest& request,
                                          const std::unordered_set<uint32_t>& tracked_pages,
                                          std::vector<PageCandidate>* selected_pages) {
    DORIS_CHECK(selected_pages != nullptr);
    FixedPagePrefetchWindow window;
    if (request.kind == PagePrefetchRequest::Kind::ROWIDS) {
        return window.select_rowids(_pages, _file_size, request.rowids, request.rowid_count,
                                    tracked_pages, selected_pages);
    }

    size_t unconsumed_planned_pages = 0;
    for (const auto& [page_index, entry] : _entries) {
        static_cast<void>(page_index);
        unconsumed_planned_pages += entry.state == PageEntry::State::PLANNED;
    }
    const size_t target_window_pages =
            FixedPagePrefetchWindow::needs_refill(unconsumed_planned_pages, _options.window_pages)
                    ? _options.window_pages
                    : 1;
    return window.select_ordinal_range(_pages, _file_size, request.first_ordinal,
                                       request.ordinal_count, request.is_forward,
                                       target_window_pages, tracked_pages, selected_pages);
}

void PagePrefetcher::_mark_fallback(uint32_t page_index) {
    const auto [iterator, inserted] =
            _entries.emplace(page_index, PageEntry {.range = nullptr,
                                                    .descriptor_index = 0,
                                                    .state = PageEntry::State::FALLBACK});
    if (inserted) {
        ++_statistics.fallback_pages;
        return;
    }
    auto& entry = iterator->second;
    if (entry.state == PageEntry::State::PLANNED) {
        entry.state = PageEntry::State::FALLBACK;
        entry.range.reset();
        ++_statistics.fallback_pages;
    }
}

void PagePrefetcher::_merge_range_statistics(const std::shared_ptr<PrefetchRange>& range) {
    DORIS_CHECK(range != nullptr);
    RangeReadStats range_statistics;
    if (!range->take_read_stats_once(&range_statistics)) {
        return;
    }
    _statistics.io_time_ns += range_statistics.remote_io_time_ns;
    _statistics.cache_or_inflight_bytes += range_statistics.cache_or_inflight_bytes;
    _statistics.remote_bytes += range_statistics.remote_bytes;
}

} // namespace doris::segment_v2
