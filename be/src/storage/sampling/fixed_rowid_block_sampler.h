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

#include "storage/sampling/tablet_sampling_algorithm.h"

namespace doris {

class FixedRowIdBlockSampler final : public TabletSamplingAlgorithm {
public:
    FixedRowIdBlockSampler(double probability, int64_t seed, int64_t unit_rows, int64_t cap);

    Status build(int64_t tablet_id, std::span<const SampleSegment> segments,
                 TabletSamplePlan* plan) const override;

private:
    double _probability;
    int64_t _seed;
    uint64_t _unit_rows;
    uint64_t _cap;
};

} // namespace doris
