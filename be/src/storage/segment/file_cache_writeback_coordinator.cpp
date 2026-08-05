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

#include "storage/segment/file_cache_writeback_coordinator.h"

#include <algorithm>
#include <limits>
#include <map>
#include <set>
#include <utility>

#include "common/cast_set.h"
#include "common/config.h"
#include "common/logging.h"

namespace doris::segment_v2 {
namespace {

struct BlockCoverage {
    uint64_t offset = 0;
    size_t valid_size = 0;
    size_t covered_page_bytes = 0;
    std::vector<uint32_t> source_page_indexes;
};

struct FetchComponent {
    uint64_t left = 0;
    uint64_t right = 0;
    std::vector<const PageCandidate*> pages;
    std::vector<const BlockCoverage*> blocks;
};

size_t requested_bytes(const std::vector<const PageCandidate*>& pages) {
    size_t bytes = 0;
    for (const auto* page : pages) {
        DORIS_CHECK(page != nullptr);
        DORIS_CHECK(bytes <= std::numeric_limits<size_t>::max() - page->size);
        bytes += page->size;
    }
    return bytes;
}

Status build_final_plan(const std::vector<PageCandidate>& pages, uint64_t file_size,
                        const PagePrefetchOptions& options,
                        const std::map<uint64_t, BlockCoverage>& block_coverage,
                        const std::set<uint64_t>& selected_blocks, PageFetchPlan* plan) {
    DORIS_CHECK(plan != nullptr);
    struct Interval {
        uint64_t left = 0;
        uint64_t right = 0;
    };
    std::vector<Interval> intervals;
    intervals.reserve(pages.size() + selected_blocks.size());
    for (const auto& page : pages) {
        intervals.push_back({page.offset, page.offset + page.size});
    }
    for (uint64_t block_offset : selected_blocks) {
        const auto iterator = block_coverage.find(block_offset);
        DORIS_CHECK(iterator != block_coverage.end());
        intervals.push_back({block_offset, block_offset + iterator->second.valid_size});
    }
    std::sort(intervals.begin(), intervals.end(), [](const Interval& left, const Interval& right) {
        return std::pair {left.left, left.right} < std::pair {right.left, right.right};
    });

    std::vector<FetchComponent> components;
    for (const auto& interval : intervals) {
        DORIS_CHECK(interval.left < interval.right);
        if (!components.empty() && interval.left < components.back().right) {
            components.back().right = std::max(components.back().right, interval.right);
        } else {
            components.push_back(
                    {.left = interval.left, .right = interval.right, .pages = {}, .blocks = {}});
        }
    }

    size_t component_index = 0;
    for (const auto& page : pages) {
        const uint64_t page_right = page.offset + page.size;
        while (component_index < components.size() &&
               components[component_index].right <= page.offset) {
            ++component_index;
        }
        DORIS_CHECK(component_index < components.size());
        auto& component = components[component_index];
        DORIS_CHECK(component.left <= page.offset);
        DORIS_CHECK(page_right <= component.right);
        component.pages.push_back(&page);
    }
    component_index = 0;
    for (uint64_t block_offset : selected_blocks) {
        const auto& block = block_coverage.at(block_offset);
        const uint64_t block_right = block.offset + block.valid_size;
        while (component_index < components.size() &&
               components[component_index].right <= block.offset) {
            ++component_index;
        }
        DORIS_CHECK(component_index < components.size());
        auto& component = components[component_index];
        DORIS_CHECK(component.left <= block.offset);
        DORIS_CHECK(block_right <= component.right);
        component.blocks.push_back(&block);
    }

    std::vector<FetchComponent> final_ranges;
    for (auto& component : components) {
        const size_t component_size = cast_set<size_t>(component.right - component.left);
        const size_t component_requested_bytes = requested_bytes(component.pages);
        if (component_size > options.max_range_bytes ||
            component.pages.size() > options.max_pages_per_range ||
            !within_read_amplification(component_size, component_requested_bytes,
                                       options.max_read_amplification_ratio)) {
            return Status::InvalidArgument("complete block cannot satisfy prefetch range limits");
        }

        bool merge = false;
        if (!final_ranges.empty()) {
            auto& range = final_ranges.back();
            const uint64_t gap = component.left - range.right;
            const uint64_t merged_size = component.right - range.left;
            const size_t merged_page_count = range.pages.size() + component.pages.size();
            const size_t merged_requested_bytes =
                    requested_bytes(range.pages) + component_requested_bytes;
            merge = gap <= options.max_gap_bytes && merged_size <= options.max_range_bytes &&
                    merged_page_count <= options.max_pages_per_range &&
                    within_read_amplification(cast_set<size_t>(merged_size), merged_requested_bytes,
                                              options.max_read_amplification_ratio);
        }
        if (!merge) {
            final_ranges.push_back(std::move(component));
            continue;
        }
        auto& range = final_ranges.back();
        range.right = component.right;
        range.pages.insert(range.pages.end(), component.pages.begin(), component.pages.end());
        range.blocks.insert(range.blocks.end(), component.blocks.begin(), component.blocks.end());
    }

    PageFetchPlan result;
    result.candidate_pages = pages.size();
    std::set<uint64_t> mapped_blocks;
    for (size_t range_index = 0; range_index < final_ranges.size(); ++range_index) {
        const auto& range = final_ranges[range_index];
        PageFetchRangeSpec spec;
        spec.offset = range.left;
        spec.size = cast_set<size_t>(range.right - range.left);
        spec.requested_page_bytes = requested_bytes(range.pages);
        size_t selected_block_bytes = 0;
        size_t requested_bytes_in_selected_blocks = 0;
        for (const auto* block : range.blocks) {
            DORIS_CHECK(block != nullptr);
            DORIS_CHECK(mapped_blocks.emplace(block->offset).second);
            DORIS_CHECK(selected_block_bytes <=
                        std::numeric_limits<size_t>::max() - block->valid_size);
            selected_block_bytes += block->valid_size;
            for (const auto* page : range.pages) {
                const uint64_t intersection_left = std::max(block->offset, page->offset);
                const uint64_t intersection_right =
                        std::min(block->offset + block->valid_size, page->offset + page->size);
                if (intersection_left < intersection_right) {
                    requested_bytes_in_selected_blocks +=
                            cast_set<size_t>(intersection_right - intersection_left);
                }
            }
            spec.complete_blocks.push_back(CompleteBlockSlice {
                    .block_offset = block->offset,
                    .valid_size = block->valid_size,
                    .buffer_offset = cast_set<size_t>(block->offset - range.left),
                    .source_page_indexes = block->source_page_indexes,
            });
        }
        DORIS_CHECK(requested_bytes_in_selected_blocks <= selected_block_bytes);
        spec.block_fill_bytes = selected_block_bytes - requested_bytes_in_selected_blocks;
        DORIS_CHECK(spec.requested_page_bytes + spec.block_fill_bytes <= spec.size);
        spec.coalesced_gap_bytes = spec.size - spec.requested_page_bytes - spec.block_fill_bytes;

        for (size_t descriptor_index = 0; descriptor_index < range.pages.size();
             ++descriptor_index) {
            const auto* page = range.pages[descriptor_index];
            DORIS_CHECK(page != nullptr);
            const size_t buffer_offset = cast_set<size_t>(page->offset - range.left);
            DORIS_CHECK(buffer_offset + page->size <= spec.size);
            spec.pages.push_back(PageSliceDescriptor {
                    .page_index = page->page_index,
                    .page_offset = page->offset,
                    .page_size = page->size,
                    .buffer_offset = buffer_offset,
            });
            DORIS_CHECK(
                    result.page_to_range
                            .emplace(page->page_index, std::pair {range_index, descriptor_index})
                            .second);
        }
        DORIS_CHECK(result.requested_page_bytes <=
                    std::numeric_limits<size_t>::max() - spec.requested_page_bytes);
        DORIS_CHECK(result.fetched_bytes <= std::numeric_limits<size_t>::max() - spec.size);
        result.requested_page_bytes += spec.requested_page_bytes;
        result.fetched_bytes += spec.size;
        result.ranges.push_back(std::move(spec));
    }
    DORIS_CHECK(result.page_to_range.size() == pages.size());
    DORIS_CHECK(mapped_blocks.size() == selected_blocks.size());
    DORIS_CHECK(result.requested_page_bytes == 0 ||
                within_read_amplification(result.fetched_bytes, result.requested_page_bytes,
                                          options.max_read_amplification_ratio));
    DORIS_CHECK(result.ranges.empty() ||
                result.ranges.back().offset + result.ranges.back().size <= file_size);
    *plan = std::move(result);
    return Status::OK();
}

} // namespace

Status FileCacheWritebackCoordinator::plan_block_completion(const std::vector<PageCandidate>& pages,
                                                            uint64_t file_size,
                                                            const PagePrefetchOptions& options,
                                                            PageFetchPlan* plan) const {
    DORIS_CHECK(plan != nullptr);
    DORIS_CHECK(options.writeback_min_block_coverage > 0.0);
    DORIS_CHECK(options.writeback_min_block_coverage <= 1.0);
    const size_t block_size = static_cast<size_t>(config::file_cache_each_block_size);
    DORIS_CHECK(block_size > 0);
    DORIS_CHECK(options.max_range_bytes >= block_size);

    PageReadPlanner page_planner;
    PageFetchPlan result;
    RETURN_IF_ERROR(page_planner.plan(pages, file_size, options, &result));
    if (pages.empty()) {
        *plan = std::move(result);
        return Status::OK();
    }

    std::map<uint64_t, BlockCoverage> block_coverage;
    for (const auto& page : pages) {
        const uint64_t page_right = page.offset + page.size;
        uint64_t block_offset = page.offset / block_size * block_size;
        while (block_offset < page_right) {
            const uint64_t block_right =
                    file_size - block_offset < block_size ? file_size : block_offset + block_size;
            auto [iterator, inserted] = block_coverage.try_emplace(
                    block_offset,
                    BlockCoverage {.offset = block_offset,
                                   .valid_size = cast_set<size_t>(block_right - block_offset),
                                   .covered_page_bytes = 0,
                                   .source_page_indexes = {}});
            auto& coverage = iterator->second;
            if (!inserted) {
                DORIS_CHECK(coverage.valid_size == block_right - block_offset);
            }
            const uint64_t intersection_left = std::max(block_offset, page.offset);
            const uint64_t intersection_right = std::min(block_right, page_right);
            DORIS_CHECK(intersection_left < intersection_right);
            coverage.covered_page_bytes += cast_set<size_t>(intersection_right - intersection_left);
            coverage.source_page_indexes.push_back(page.page_index);
            block_offset = block_right;
        }
    }

    std::set<uint64_t> selected_blocks;
    for (const auto& [block_offset, coverage] : block_coverage) {
        const double coverage_ratio = static_cast<double>(coverage.covered_page_bytes) /
                                      static_cast<double>(coverage.valid_size);
        if (coverage_ratio < options.writeback_min_block_coverage) {
            continue;
        }
        selected_blocks.emplace(block_offset);
        PageFetchPlan candidate;
        if (!build_final_plan(pages, file_size, options, block_coverage, selected_blocks,
                              &candidate)
                     .ok()) {
            selected_blocks.erase(block_offset);
            continue;
        }
        result = std::move(candidate);
    }
    *plan = std::move(result);
    return Status::OK();
}

void FileCacheWritebackCoordinator::mark_page_consumed(const std::shared_ptr<PrefetchRange>& range,
                                                       uint32_t page_index) const {
    DORIS_CHECK(range != nullptr);
    DORIS_CHECK(std::any_of(range->spec().pages.begin(), range->spec().pages.end(),
                            [page_index](const PageSliceDescriptor& page) {
                                return page.page_index == page_index;
                            }));
    if (!config::enable_query_page_prefetch || !config::enable_async_file_cache_write ||
        range->spec().complete_blocks.empty()) {
        return;
    }
    const auto* writeback_ctx = range->writeback_context();
    DORIS_CHECK(writeback_ctx != nullptr);
    if (writeback_ctx->remote_only_on_miss) {
        return;
    }

    auto claimed_blocks = range->claim_complete_blocks_for_page(page_index);
    if (claimed_blocks.empty()) {
        return;
    }
    DORIS_CHECK(_io_service != nullptr);
    for (size_t block_index : claimed_blocks) {
        _io_service->try_submit_writeback_copy({
                .range = range,
                .complete_block_index = block_index,
        });
    }
}

void FileCacheWritebackCoordinator::invalidate_page(const std::shared_ptr<PrefetchRange>& range,
                                                    uint32_t page_index) const {
    DORIS_CHECK(range != nullptr);
    DORIS_CHECK(std::any_of(range->spec().pages.begin(), range->spec().pages.end(),
                            [page_index](const PageSliceDescriptor& page) {
                                return page.page_index == page_index;
                            }));
    range->invalidate_complete_blocks_for_page(page_index);
}

} // namespace doris::segment_v2
