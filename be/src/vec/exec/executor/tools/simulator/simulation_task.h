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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <numeric>
#include <unordered_set>
#include <vector>

#include "vec/exec/executor/time_sharing/time_sharing_task_executor.h"
#include "vec/exec/executor/tools/simulator/simulation_controller.h"
#include "vec/exec/executor/tools/simulator/simulation_split.h"
#include "vec/exec/executor/tools/simulator/split_specification.h"

namespace doris {
namespace vectorized {

class SimulationSplit;

class SimulationTask {
public:
    SimulationTask(TimeSharingTaskExecutor& executor,
                   const SimulationController::TasksSpecification& spec,
                   const std::string& task_id);

    Status init();

    virtual ~SimulationTask() = default;

    const std::unordered_set<std::shared_ptr<SimulationSplit>>& running_splits() const;
    const std::unordered_set<std::shared_ptr<SimulationSplit>>& completed_splits() const;
    const std::shared_ptr<TaskHandle>& task_handle() const;
    const SimulationController::TasksSpecification& specification() const;

    void set_killed();
    bool is_killed() const;

    void split_complete(std::shared_ptr<SimulationSplit> split);

    int64_t get_total_wait_time_nanos() const;
    int64_t get_processed_time_nanos() const;
    int64_t get_scheduled_time_nanos() const;
    int64_t get_calls() const;

    virtual void schedule(TimeSharingTaskExecutor& executor, int num_splits) = 0;

protected:
    const SimulationController::TasksSpecification _specification;
    const std::string _task_id;
    TimeSharingTaskExecutor& _executor;
    std::unordered_set<std::shared_ptr<SimulationSplit>> _running_splits;
    std::unordered_set<std::shared_ptr<SimulationSplit>> _completed_splits;
    mutable std::mutex _splits_mutex;

    std::atomic<bool> _killed {false};
    std::shared_ptr<class TaskHandle> _task_handle;

private:
    template <typename C, typename F>
    static int64_t accumulate_time(const C& container, F func) {
        return std::accumulate(
                container.begin(), container.end(), 0LL,
                [&](int64_t sum, const auto& split) { return sum + ((*split).*func)(); });
    }
};

class LeafTask : public SimulationTask {
    SimulationController::TasksSpecification _tasks_spec;

public:
    LeafTask(TimeSharingTaskExecutor& executor,
             const SimulationController::TasksSpecification& spec, const std::string& task_id);

    void schedule(TimeSharingTaskExecutor& executor, int num_splits) override;
};

class IntermediateTask : public SimulationTask {
private:
    std::shared_ptr<vectorized::SplitSpecification> _split_spec;

public:
    IntermediateTask(TimeSharingTaskExecutor& executor,
                     const SimulationController::TasksSpecification& spec,
                     const std::string& task_id);

    void schedule(TimeSharingTaskExecutor& executor, int num_splits) override;
};

} // namespace vectorized
} // namespace doris
