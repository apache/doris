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
#include <vector>

#include "common/status.h"
#include "io/fs/file_range_coalescer.h"

namespace doris::io {

struct FileRangePlanOptions {
    /// Limits for the initial range coalescing pass.
    FileRangeCoalesceOptions coalesce_options;
    /// File Cache block size used when considering boundary-block completion.
    size_t cache_block_size {1};
    /// Minimum input-byte coverage required before completing a boundary block.
    double block_fill_min_coverage {1.0};

    Status validate() const;
};

/// The location of one input range in its final read buffer.
struct FileRangeLocation {
    /// Index into FileRangePlan::ranges.
    size_t range_index {0};
    /// Offset of the original input's first byte within that physical range buffer.
    size_t buffer_offset {0};

    bool operator==(const FileRangeLocation&) const = default;
};

struct FileRangePlan {
    /// Physical ranges to read after coalescing and optional cache-block completion.
    std::vector<FileRange> ranges;
    /// One location per input in caller-supplied order; every input is contained by that range.
    std::vector<FileRangeLocation> input_locations;

    bool operator==(const FileRangePlan&) const = default;
};

/// Builds a read-only plan without allocating buffers or performing file IO.
class FileRangePlanner {
public:
    /// `input_ranges` must be sorted by offset and mutually disjoint. The planner first coalesces
    /// them, then completes high-coverage cache blocks when the resulting physical ranges remain
    /// within `max_range_bytes`. One input larger than that limit remains a standalone read. The
    /// physical EOF block uses its valid file bytes as the coverage denominator.
    static Status plan(const std::vector<FileRange>& input_ranges, size_t file_size,
                       const FileRangePlanOptions& options, FileRangePlan* output_plan);
};

} // namespace doris::io
