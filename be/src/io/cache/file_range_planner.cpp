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

#include "io/cache/file_range_planner.h"

#include <algorithm>
#include <cmath>
#include <map>
#include <utility>

#include "common/logging.h"

namespace doris::io {

Status FileRangePlanOptions::validate() const {
    RETURN_IF_ERROR(coalesce_options.validate());
    if (cache_block_size == 0) {
        return Status::InvalidArgument("file range plan cache block size must be positive");
    }
    if (!std::isfinite(block_fill_min_coverage) || block_fill_min_coverage <= 0.0 ||
        block_fill_min_coverage > 1.0) {
        return Status::InvalidArgument(
                "file range plan block fill coverage must be finite and in (0, 1]");
    }
    return Status::OK();
}

namespace {

struct BlockCandidate {
    FileRange range;
    size_t missing_base_bytes {0};
};

Status validate_input_ranges(const std::vector<FileRange>& input_ranges, size_t file_size) {
    size_t previous_end = 0;
    for (const auto& range : input_ranges) {
        if (range.size == 0) {
            return Status::InvalidArgument("input file range at offset {} is empty", range.offset);
        }
        if (range.offset > file_size || range.size > file_size - range.offset) {
            return Status::InvalidArgument(
                    "input file range at offset {} with size {} exceeds file size {}", range.offset,
                    range.size, file_size);
        }
        if (range.offset < previous_end) {
            return Status::InvalidArgument(
                    "input file ranges must be sorted and disjoint: offset {} follows end {}",
                    range.offset, previous_end);
        }
        previous_end = range.end();
    }
    return Status::OK();
}

FileRange block_range(size_t offset, size_t file_size, size_t block_size) {
    const size_t block_offset = offset / block_size * block_size;
    return {.offset = block_offset, .size = std::min(block_size, file_size - block_offset)};
}

size_t intersection_bytes(const FileRange& left, const FileRange& right) {
    const size_t begin = std::max(left.offset, right.offset);
    const size_t end = std::min(left.end(), right.end());
    return begin < end ? end - begin : 0;
}

// Count coverage in `target` with a monotonic cursor over sorted, disjoint ranges. The caller must
// visit targets in ascending order.
size_t covered_bytes(const std::vector<FileRange>& ranges, const FileRange& target,
                     size_t* cursor) {
    while (*cursor < ranges.size() && ranges[*cursor].end() <= target.offset) {
        ++*cursor;
    }

    size_t result = 0;
    for (size_t index = *cursor; index < ranges.size() && ranges[index].offset < target.end();
         ++index) {
        result += intersection_bytes(ranges[index], target);
    }
    return result;
}

void append_boundary_block(std::vector<FileRange>* blocks, const FileRange& range, size_t file_size,
                           size_t block_size) {
    const FileRange block = block_range(range.offset, file_size, block_size);
    if (!blocks->empty() && blocks->back().offset == block.offset) {
        DORIS_CHECK(blocks->back() == block);
        return;
    }
    DORIS_CHECK(blocks->empty() || blocks->back().end() <= block.offset);
    blocks->push_back(block);
}

std::vector<BlockCandidate> find_block_candidates(const std::vector<FileRange>& input_ranges,
                                                  const std::vector<FileRange>& base_ranges,
                                                  size_t file_size,
                                                  const FileRangePlanOptions& options) {
    // Interior blocks are already covered by a base read, so only its two boundary blocks can add
    // bytes. This bounds the candidate count to twice the number of base ranges.
    std::vector<FileRange> boundary_blocks;
    boundary_blocks.reserve(base_ranges.size() * 2);
    for (const auto& base : base_ranges) {
        append_boundary_block(&boundary_blocks, base, file_size, options.cache_block_size);
        append_boundary_block(&boundary_blocks, {.offset = base.end() - 1, .size = 1}, file_size,
                              options.cache_block_size);
    }

    std::vector<BlockCandidate> candidates;
    candidates.reserve(boundary_blocks.size());
    size_t input_cursor = 0;
    size_t base_cursor = 0;
    for (const auto& block : boundary_blocks) {
        const size_t input_bytes = covered_bytes(input_ranges, block, &input_cursor);
        const double coverage = static_cast<double>(input_bytes) / static_cast<double>(block.size);
        if (coverage < options.block_fill_min_coverage) {
            continue;
        }
        const size_t base_bytes = covered_bytes(base_ranges, block, &base_cursor);
        DORIS_CHECK(base_bytes <= block.size);
        if (base_bytes < block.size) {
            candidates.push_back({.range = block, .missing_base_bytes = block.size - base_bytes});
        }
    }
    std::ranges::sort(candidates, [](const BlockCandidate& left, const BlockCandidate& right) {
        return std::pair {left.missing_base_bytes, left.range.offset} <
               std::pair {right.missing_base_bytes, right.range.offset};
    });
    return candidates;
}

// Treat each base range as one component, then try boundary-block fills from the fewest added bytes
// to the most. An accepted fill joins only the components it overlaps and still obeys the physical
// range-size limit.
std::vector<FileRange> choose_components(const std::vector<FileRange>& input_ranges,
                                         const std::vector<FileRange>& base_ranges,
                                         size_t file_size, const FileRangePlanOptions& options) {
    std::map<size_t, FileRange> components;
    for (const auto& base : base_ranges) {
        components.emplace(base.offset, base);
    }

    // Try the cheapest fills first. The interval map updates only components touched by a
    // candidate instead of rebuilding every component after each attempt.
    const auto candidates = find_block_candidates(input_ranges, base_ranges, file_size, options);
    for (const auto& candidate : candidates) {
        auto first = components.lower_bound(candidate.range.offset);
        if (first != components.begin()) {
            auto previous = first;
            --previous;
            if (previous->second.end() > candidate.range.offset) {
                first = previous;
            }
        }
        DORIS_CHECK(first != components.end());
        DORIS_CHECK(first->second.offset < candidate.range.end());

        size_t merged_offset = std::min(candidate.range.offset, first->second.offset);
        size_t merged_end = candidate.range.end();
        auto last = first;
        while (last != components.end() && last->second.offset < candidate.range.end()) {
            DORIS_CHECK(last->second.end() > candidate.range.offset);
            merged_end = std::max(merged_end, last->second.end());
            ++last;
        }
        if (merged_end - merged_offset > options.coalesce_options.max_range_bytes) {
            continue;
        }

        components.erase(first, last);
        components.emplace(merged_offset,
                           FileRange {.offset = merged_offset, .size = merged_end - merged_offset});
    }

    std::vector<FileRange> result;
    result.reserve(components.size());
    for (const auto& [offset, component] : components) {
        DORIS_CHECK(offset == component.offset);
        result.push_back(component);
    }
    return result;
}

// Map every original page range back to the containing physical read with one monotonic scan.
FileRangePlan build_final_plan(const std::vector<FileRange>& input_ranges,
                               std::vector<FileRange> components) {
    FileRangePlan result {.ranges = std::move(components), .input_locations = {}};
    result.input_locations.reserve(input_ranges.size());
    size_t range_index = 0;
    for (const auto& input : input_ranges) {
        while (range_index < result.ranges.size() &&
               result.ranges[range_index].end() <= input.offset) {
            ++range_index;
        }
        DORIS_CHECK(range_index < result.ranges.size());
        const auto& range = result.ranges[range_index];
        DORIS_CHECK(range.offset <= input.offset);
        DORIS_CHECK(input.end() <= range.end());
        result.input_locations.push_back(
                {.range_index = range_index, .buffer_offset = input.offset - range.offset});
    }

    return result;
}

} // namespace

Status FileRangePlanner::plan(const std::vector<FileRange>& input_ranges, size_t file_size,
                              const FileRangePlanOptions& options,
                              FileRangePlan* const output_plan) {
    DORIS_CHECK(output_plan != nullptr);
    RETURN_IF_ERROR(options.validate());
    RETURN_IF_ERROR(validate_input_ranges(input_ranges, file_size));

    const auto base_ranges = FileRangeCoalescer::coalesce(input_ranges, options.coalesce_options);
    auto components = choose_components(input_ranges, base_ranges, file_size, options);
    *output_plan = build_final_plan(input_ranges, std::move(components));
    return Status::OK();
}

} // namespace doris::io
