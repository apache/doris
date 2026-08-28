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

#include "io/fs/file_range_coalescer.h"

#include <cmath>
#include <limits>

#include "common/logging.h"

namespace doris::io {

Status FileRangeCoalesceOptions::validate() const {
    if (max_range_bytes == 0) {
        return Status::InvalidArgument("file range coalescing max range bytes must be positive");
    }
    if (!std::isfinite(max_read_amplification_ratio) || max_read_amplification_ratio < 1.0) {
        return Status::InvalidArgument(
                "file range coalescing read amplification ratio must be finite and at least 1");
    }
    return Status::OK();
}

namespace {

bool within_read_amplification(size_t range_bytes, size_t input_bytes,
                               double max_read_amplification_ratio) {
    return static_cast<long double>(range_bytes) <=
           static_cast<long double>(input_bytes) * max_read_amplification_ratio;
}

} // namespace

std::vector<FileRange> FileRangeCoalescer::coalesce(const std::vector<FileRange>& input_ranges,
                                                    const FileRangeCoalesceOptions& options) {
    std::vector<FileRange> result;
    result.reserve(input_ranges.size());
    size_t current_input_bytes = 0;
    for (const auto& range : input_ranges) {
        DCHECK(range.size > 0);
        DCHECK(range.offset <= std::numeric_limits<size_t>::max() - range.size);
        if (result.empty()) {
            result.push_back(range);
            current_input_bytes = range.size;
            continue;
        }

        auto& current = result.back();
        DCHECK(current.end() <= range.offset);
        const size_t gap_bytes = range.offset - current.end();
        const size_t merged_bytes = range.end() - current.offset;
        const size_t merged_input_bytes = current_input_bytes + range.size;
        if (gap_bytes <= options.max_gap_bytes && merged_bytes <= options.max_range_bytes &&
            within_read_amplification(merged_bytes, merged_input_bytes,
                                      options.max_read_amplification_ratio)) {
            current.size = merged_bytes;
            current_input_bytes = merged_input_bytes;
        } else {
            result.push_back(range);
            current_input_bytes = range.size;
        }
    }
    return result;
}

} // namespace doris::io
