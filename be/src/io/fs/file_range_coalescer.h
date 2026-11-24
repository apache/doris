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

namespace doris::io {

/// A half-open byte range [offset, offset + size) in one file.
struct FileRange {
    size_t offset {0};
    size_t size {0};

    size_t end() const { return offset + size; }

    bool operator==(const FileRange&) const = default;
};

/// Limits applied while adjacent input ranges are grouped into one physical read.
struct FileRangeCoalesceOptions {
    /// Maximum unread gap that may be included between two input ranges.
    size_t max_gap_bytes {0};
    /// Maximum size of a combined range. One indivisible input may exceed this limit.
    size_t max_range_bytes {1};
    /// Maximum combined-range bytes divided by bytes covered by its input ranges.
    double max_read_amplification_ratio {1.0};

    /// Validate the option values independently of any input ranges.
    Status validate() const;
};

/// Groups sorted file ranges without performing file IO.
class FileRangeCoalescer {
public:
    /// `options` must have passed validation. Each input range must be non-empty. Input ranges
    /// must be sorted by offset and mutually disjoint. Callers establish these invariants before
    /// entering the hot path.
    /// `max_range_bytes` limits combining multiple inputs; one indivisible input may be larger and
    /// is returned unchanged.
    static std::vector<FileRange> coalesce(const std::vector<FileRange>& input_ranges,
                                           const FileRangeCoalesceOptions& options);
};

} // namespace doris::io
