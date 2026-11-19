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

#include "io/cache/hole_fill_planner.h"

#include <algorithm>
#include <limits>
#include <utility>

#include "common/logging.h"

namespace doris::io {

Status HoleFillPlanner::plan(const FileRange& block,
                             const std::vector<FileRange>& covered_intervals,
                             const FileRangeCoalesceOptions& options,
                             std::vector<FileRange>* output_ranges) {
    DORIS_CHECK(output_ranges != nullptr);
    RETURN_IF_ERROR(options.validate());
    if (block.size == 0) {
        return Status::InvalidArgument("hole-fill block at offset {} is empty", block.offset);
    }
    if (block.offset > std::numeric_limits<size_t>::max() - block.size) {
        return Status::InvalidArgument("hole-fill block at offset {} with size {} overflows",
                                       block.offset, block.size);
    }
    for (const auto& covered : covered_intervals) {
        if (covered.size == 0) {
            return Status::InvalidArgument("covered interval at offset {} is empty",
                                           covered.offset);
        }
        if (covered.offset < block.offset || covered.offset > block.end() ||
            covered.size > block.end() - covered.offset) {
            return Status::InvalidArgument(
                    "covered interval at offset {} with size {} exceeds block [{}, {})",
                    covered.offset, covered.size, block.offset, block.end());
        }
    }

    auto sorted_coverage = covered_intervals;
    std::ranges::sort(sorted_coverage, [](const FileRange& left, const FileRange& right) {
        return left.offset < right.offset;
    });
    std::vector<FileRange> covered_union;
    covered_union.reserve(sorted_coverage.size());
    for (const auto& covered : sorted_coverage) {
        if (covered_union.empty() || covered_union.back().end() < covered.offset) {
            covered_union.push_back(covered);
        } else {
            covered_union.back().size = std::max(covered_union.back().end(), covered.end()) -
                                        covered_union.back().offset;
        }
    }

    std::vector<FileRange> holes;
    size_t cursor = block.offset;
    for (const auto& covered : covered_union) {
        DORIS_CHECK(cursor <= covered.offset);
        if (cursor < covered.offset) {
            holes.push_back({.offset = cursor, .size = covered.offset - cursor});
        }
        cursor = covered.end();
    }
    if (cursor < block.end()) {
        holes.push_back({.offset = cursor, .size = block.end() - cursor});
    }

    auto coalesced_holes = FileRangeCoalescer::coalesce(holes, options);
    for (const auto& hole : coalesced_holes) {
        DORIS_CHECK(block.offset <= hole.offset);
        DORIS_CHECK(hole.end() <= block.end());
    }
    *output_ranges = std::move(coalesced_holes);
    return Status::OK();
}

} // namespace doris::io
