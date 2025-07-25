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

#include "vec/exec/executor/tools/simulator/split_specification.h"

#include "vec/exec/executor/tools/simulator/simulation_split.h"

namespace doris {
namespace vectorized {

std::unique_ptr<SimulationSplit> LeafSplitSpecification::instantiate(SimulationTask* task) const {
    return std::make_unique<LeafSplit>(task, std::chrono::nanoseconds {_scheduled_time_nanos},
                                       std::chrono::nanoseconds {_per_quanta_nanos});
}

std::unique_ptr<SimulationSplit> IntermediateSplitSpecification::instantiate(
        SimulationTask* task) const {
    auto wall_time = std::chrono::nanoseconds(_wall_time_nanos);
    auto per_quanta = std::chrono::nanoseconds(per_quanta_nanos());
    auto between_quanta = std::chrono::nanoseconds(_between_quanta_nanos);
    auto scheduled_time = std::chrono::nanoseconds(scheduled_time_nanos());

    return std::make_unique<IntermediateSplit>(task, wall_time, _num_quantas, per_quanta,
                                               between_quanta, scheduled_time);
}

} // namespace vectorized
} // namespace doris
