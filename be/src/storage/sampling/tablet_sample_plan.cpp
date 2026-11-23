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

#include "storage/sampling/tablet_sample_plan.h"

#include <algorithm>

namespace doris {

std::vector<SampleRowsetSlice> SampleRangeSplitter::split(
        const std::vector<segment_v2::RowRanges>& segments) {
    DORIS_CHECK_GT(_budget, 0);
    std::vector<SampleRowsetSlice> slices;
    for (size_t pos = 0; pos < segments.size(); ++pos) {
        const auto& selected = segments[pos];
        for (size_t idx = 0; idx < selected.range_size(); ++idx) {
            const auto range = selected.get_range(idx);
            for (auto from = range.from(); from < range.to();) {
                if (slices.empty() || slices.back().finishes_scanner) {
                    slices.push_back({pos, {}, false});
                }
                auto& slice = slices.back();
                // E.g. positions 1 and 3 in this slice require an explicit empty entry for 2.
                slice.ranges.resize(pos - slice.first_pos + 1);
                const auto count = std::min(_remaining, range.to() - from);
                slice.ranges.back().add({from, from + count});
                from += count;
                _remaining -= count;
                if (_remaining == 0) {
                    slice.finishes_scanner = true;
                    _remaining = _budget;
                }
            }
        }
    }
    return slices;
}

} // namespace doris
