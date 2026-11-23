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

#include "storage/sampling/fixed_rowid_block_sampler.h"

#include <algorithm>
#include <queue>
#include <utility>
#include <vector>

#include "common/check.h"
#include "common/status.h"
#include "storage/sampling/dependent_rounding.h"
#include "storage/sampling/tablet_sample_plan.h"

namespace doris {
namespace {

segment_v2::RowRanges fixed_block_select(const SampleSegment& segment, uint64_t tablet_hash,
                                         uint64_t unit_rows, uint64_t units, uint64_t k) {
    if (k == 0) {
        return {};
    }
    if (k == units) {
        return segment_v2::RowRanges::create_single(segment.rows);
    }
    auto hash = sampling_mix(tablet_hash, segment.rowset_id.hi);
    hash = sampling_mix(hash, segment.rowset_id.mi);
    hash = sampling_mix(hash, segment.rowset_id.lo);
    hash = sampling_mix(hash, segment.physical_id);
    // Only keep the k best (hash, unit) pairs. Unit breaks ties independently of traversal order.
    using Candidate = std::pair<uint64_t, uint32_t>;
    std::priority_queue<Candidate> best;
    for (uint64_t unit = 0; unit < units; ++unit) {
        Candidate candidate {sampling_mix(hash, unit), unit};
        if (best.size() < k) {
            best.push(candidate);
        } else if (candidate < best.top()) {
            best.pop();
            best.push(candidate);
        }
    }
    std::vector<uint32_t> ids;
    ids.reserve(k);
    while (!best.empty()) {
        ids.push_back(best.top().second);
        best.pop();
    }
    std::ranges::sort(ids);
    segment_v2::RowRanges ranges;
    for (uint64_t unit : ids) {
        ranges.add(
                {static_cast<int64_t>(unit * unit_rows),
                 static_cast<int64_t>(std::min<uint64_t>((unit + 1) * unit_rows, segment.rows))});
    }
    return ranges;
}

} // namespace

FixedRowIdBlockSampler::FixedRowIdBlockSampler(double probability, int64_t seed, int64_t unit_rows,
                                               int64_t cap)
        : _probability(probability), _seed(seed), _unit_rows(unit_rows), _cap(cap) {
    DORIS_CHECK_GT(unit_rows, 0);
}

Status FixedRowIdBlockSampler::build(int64_t tablet_id, std::span<const SampleSegment> segments,
                                     TabletSamplePlan* plan) const {
    const DependentRounding rounding(_probability, _seed, tablet_id);
    auto units_in = [this](uint32_t rows) -> uint64_t {
        return rows / _unit_rows + (rows % _unit_rows != 0);
    };
    uint64_t population_units = 0;
    for (const auto& segment : segments) {
        population_units += units_in(segment.rows);
    }
    const uint64_t selected_units = rounding.cumulative(population_units);
    if (selected_units > _cap) {
        return Status::InvalidArgument(
                "tablet_id={} population_units={} selected_units={} exceed cap={}", tablet_id,
                population_units, selected_units, _cap);
    }
    const auto tablet_hash =
            sampling_mix(sampling_mix(sampling_domain("fixed-rowid-block"), _seed), tablet_id);
    uint64_t prefix = 0;
    uint64_t selected = 0;
    uint64_t selected_rows = 0;
    std::vector<segment_v2::RowRanges> selected_ranges;
    selected_ranges.reserve(segments.size());
    for (const auto& segment : segments) {
        const auto units = units_in(segment.rows);
        prefix += units;
        const auto next = rounding.cumulative(prefix);
        auto ranges = fixed_block_select(segment, tablet_hash, _unit_rows, units, next - selected);
        selected_rows += ranges.count();
        selected_ranges.emplace_back(std::move(ranges));
        selected = next;
    }
    *plan = TabletSamplePlan(std::move(selected_ranges), population_units, selected_units,
                             selected_rows);
    return Status::OK();
}

} // namespace doris
