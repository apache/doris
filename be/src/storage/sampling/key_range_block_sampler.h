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
#include <optional>
#include <string>
#include <vector>

#include "storage/sampling/tablet_sampling_algorithm.h"

namespace doris {

struct KeyRangeBound {
    std::string prefix;
    std::vector<std::string> tuple;
};

struct KeyRangeSampleInterval {
    std::optional<KeyRangeBound> lower;
    std::optional<KeyRangeBound> upper;
};

struct KeyRangeSamplePlanSpec {
    std::vector<KeyRangeSampleInterval> intervals;
    uint64_t population_units = 0;
    uint64_t selected_units = 0;
};

struct WeightedKeyRangeCut {
    KeyRangeBound bound;
    uint64_t weight = 0;
};

class KeyRangeBlockSampler final : public TabletSamplingAlgorithm {
public:
    KeyRangeBlockSampler(double probability, int64_t seed, int64_t unit_rows, int64_t cap,
                         std::optional<KeyRangeSamplePlanSpec> shared_spec = std::nullopt);

    // Collects start-bound mass chunks across `segments` (at most `max_cuts + 1` chunks) so a
    // coordinator can merge key density across multiple backends of a RANDOM partition.
    static Status collect_weighted_cuts(std::span<const SampleSegment> segments,
                                        uint64_t min_unit_rows, uint64_t max_cuts,
                                        std::vector<WeightedKeyRangeCut>* weighted_cuts,
                                        uint64_t* total_rows);

    // Partitions (-inf, +inf) along short-key / sort-key cuts and draws half-open key intervals.
    // When `empty_as_universal_unit` is true (e.g. an unwritten partition slice whose sibling
    // buckets may hold rows), an empty segment list still forms 1 universal interval (-inf, +inf)
    // so every key in the partition keeps exact marginal probability.
    Status sample_intervals(int64_t domain_id, std::span<const SampleSegment> segments,
                            bool empty_as_universal_unit, KeyRangeSamplePlanSpec* spec) const;

    // Projects pre-selected half-open key intervals onto `segments` using monotonic cursors.
    Status locate_intervals(int64_t tablet_id, std::span<const SampleSegment> segments,
                            const KeyRangeSamplePlanSpec& spec, TabletSamplePlan* plan) const;

    Status build(int64_t tablet_id, std::span<const SampleSegment> segments,
                 TabletSamplePlan* plan) const override;

private:
    double _probability;
    int64_t _seed;
    uint64_t _unit_rows;
    uint64_t _cap;
    std::optional<KeyRangeSamplePlanSpec> _shared_spec;
};

} // namespace doris
