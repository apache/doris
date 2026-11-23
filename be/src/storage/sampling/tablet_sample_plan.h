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

#include <cstdint>
#include <utility>
#include <vector>

#include "storage/rowset_id.h"
#include "storage/segment/row_ranges.h"

namespace doris {

// Input and output use the same order as the captured TabletReadSource and its segment positions.
struct SampleSegment {
    RowsetId rowset_id;
    int64_t physical_id;
    uint32_t rows;
};

// Row ranges are fixed as a unit once the cap has been checked. The scanner may consume the
// completed ranges once to transfer ownership into its version-pinned read source.
class TabletSamplePlan {
public:
    TabletSamplePlan() = default;
    TabletSamplePlan(std::vector<segment_v2::RowRanges> segments, uint64_t population_units,
                     uint64_t selected_units, uint64_t selected_rows)
            : _segments(std::move(segments)),
              _population_units(population_units),
              _selected_units(selected_units),
              _selected_rows(selected_rows) {}

    const std::vector<segment_v2::RowRanges>& segments() const { return _segments; }
    std::vector<segment_v2::RowRanges> take_segments() && { return std::move(_segments); }
    uint64_t population_units() const { return _population_units; }
    uint64_t selected_units() const { return _selected_units; }
    uint64_t selected_rows() const { return _selected_rows; }

private:
    std::vector<segment_v2::RowRanges> _segments;
    uint64_t _population_units = 0;
    uint64_t _selected_units = 0;
    uint64_t _selected_rows = 0;
};

// A scanner can end in the middle of a selected interval, or span several rowsets.
// Each call consumes one rowset, emitting one slice per scanner that touches it.
struct SampleRowsetSlice {
    size_t first_pos;
    std::vector<segment_v2::RowRanges> ranges;
    bool finishes_scanner;
};

class SampleRangeSplitter {
public:
    explicit SampleRangeSplitter(int64_t budget) : _budget(budget), _remaining(budget) {}
    std::vector<SampleRowsetSlice> split(const std::vector<segment_v2::RowRanges>& segments);

private:
    int64_t _budget;
    int64_t _remaining;
};

} // namespace doris
