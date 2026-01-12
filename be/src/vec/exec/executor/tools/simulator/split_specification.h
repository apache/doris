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

#include <chrono>
#include <functional>
#include <memory>

namespace doris {
namespace vectorized {

class SimulationTask;
class SimulationSplit;

class SplitSpecification {
public:
    virtual ~SplitSpecification() = default;

    int64_t scheduled_time_nanos() const { return _scheduled_time_nanos; }

    int64_t per_quanta_nanos() const { return _per_quanta_nanos; }

    virtual std::unique_ptr<SimulationSplit> instantiate(SimulationTask* task) const = 0;

protected:
    SplitSpecification(int64_t scheduled_time, int64_t per_quanta)
            : _scheduled_time_nanos(scheduled_time), _per_quanta_nanos(per_quanta) {}

    const int64_t _scheduled_time_nanos;
    const int64_t _per_quanta_nanos;
};

class LeafSplitSpecification : public SplitSpecification {
public:
    LeafSplitSpecification(int64_t scheduled_time, int64_t per_quanta)
            : SplitSpecification(scheduled_time, per_quanta) {}

    std::unique_ptr<SimulationSplit> instantiate(SimulationTask* task) const override;
};

class IntermediateSplitSpecification : public SplitSpecification {
public:
    IntermediateSplitSpecification(int64_t scheduled_time, int64_t wall_time, int64_t num_quantas,
                                   int64_t per_quanta, int64_t between_quanta)
            : SplitSpecification(scheduled_time, per_quanta),
              _wall_time_nanos(wall_time),
              _num_quantas(num_quantas),
              _between_quanta_nanos(between_quanta) {}

    int64_t get_wall_time_nanos() const { return _wall_time_nanos; }

    int64_t get_num_quantas() const { return _num_quantas; }

    int64_t get_between_quanta_nanos() const { return _between_quanta_nanos; }

    std::unique_ptr<SimulationSplit> instantiate(SimulationTask* task) const override;

private:
    const int64_t _wall_time_nanos;
    const int64_t _num_quantas;
    const int64_t _between_quanta_nanos;
};

} // namespace vectorized
} // namespace doris
