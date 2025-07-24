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

#include "vec/exec/executor/tools/simulator/simulation_task.h"

#include <numeric>

#include "vec/exec/executor/tools/simulator/simulation_split.h"

namespace doris {
namespace vectorized {

SimulationTask::SimulationTask(TimeSharingTaskExecutor& executor,
                               const SimulationController::TasksSpecification& spec,
                               const std::string& task_id)
        : _specification(spec), _task_id(task_id), _executor(executor) {}

Status SimulationTask::init() {
    _task_handle = DORIS_TRY(_executor.create_task(
            _task_id, []() { return 0.0; }, 10, std::chrono::milliseconds(1), std::nullopt));
    return Status::OK();
}

const std::unordered_set<std::shared_ptr<SimulationSplit>>& SimulationTask::running_splits() const {
    std::lock_guard<std::mutex> lock(_splits_mutex);
    return _running_splits;
}
const std::unordered_set<std::shared_ptr<SimulationSplit>>& SimulationTask::completed_splits()
        const {
    std::lock_guard<std::mutex> lock(_splits_mutex);
    return _completed_splits;
}

const std::shared_ptr<TaskHandle>& SimulationTask::task_handle() const {
    return _task_handle;
}

const SimulationController::TasksSpecification& SimulationTask::specification() const {
    return _specification;
}

void SimulationTask::split_complete(std::shared_ptr<SimulationSplit> split) {
    std::lock_guard<std::mutex> lock(_splits_mutex);
    _running_splits.erase(split);
    _completed_splits.insert(split);
}

int64_t SimulationTask::get_total_wait_time_nanos() const {
    std::lock_guard<std::mutex> lock(_splits_mutex);
    return accumulate_time(_running_splits, &SimulationSplit::waited_nanos) +
           accumulate_time(_completed_splits, &SimulationSplit::waited_nanos);
}

int64_t SimulationTask::get_processed_time_nanos() const {
    std::lock_guard<std::mutex> lock(_splits_mutex);
    return accumulate_time(_running_splits, &SimulationSplit::processed_nanos) +
           accumulate_time(_completed_splits, &SimulationSplit::processed_nanos);
}

int64_t SimulationTask::get_scheduled_time_nanos() const {
    std::lock_guard<std::mutex> lock(_splits_mutex);
    return accumulate_time(_running_splits, &SimulationSplit::scheduled_time_nanos) +
           accumulate_time(_completed_splits, &SimulationSplit::scheduled_time_nanos);
}

int64_t SimulationTask::get_calls() const {
    std::lock_guard<std::mutex> lock(_splits_mutex);
    return accumulate_time(_running_splits, &SimulationSplit::calls) +
           accumulate_time(_completed_splits, &SimulationSplit::calls);
}

LeafTask::LeafTask(TimeSharingTaskExecutor& executor,
                   const SimulationController::TasksSpecification& spec, const std::string& task_id)
        : SimulationTask(executor, spec, task_id), _tasks_spec(spec) {}

void LeafTask::schedule(TimeSharingTaskExecutor& executor, int num_splits) {
    std::vector<std::shared_ptr<SplitRunner>> new_splits;
    for (int i = 0; i < num_splits; ++i) {
        auto split = _tasks_spec.next_specification()->instantiate(this);
        new_splits.push_back(std::shared_ptr<SplitRunner>(split.release()));
    }

    {
        std::lock_guard<std::mutex> lock(_splits_mutex);
        for (const auto& split : new_splits) {
            std::shared_ptr<SimulationSplit> simulation_split =
                    std::dynamic_pointer_cast<SimulationSplit>(split);
            if (simulation_split) {
                _running_splits.insert(simulation_split);
            }
        }
    }

    executor.enqueue_splits(_task_handle, false, new_splits);
}

IntermediateTask::IntermediateTask(TimeSharingTaskExecutor& executor,
                                   const SimulationController::TasksSpecification& spec,
                                   const std::string& task_id)
        : SimulationTask(executor, spec, task_id), _split_spec(spec.next_specification()) {}

void IntermediateTask::schedule(TimeSharingTaskExecutor& executor, int num_splits) {
    std::vector<std::shared_ptr<SplitRunner>> new_splits;
    for (int i = 0; i < num_splits; ++i) {
        auto split = _split_spec->instantiate(this);
        new_splits.push_back(std::shared_ptr<SplitRunner>(split.release()));
    }

    {
        std::lock_guard<std::mutex> lock(_splits_mutex);
        for (const auto& split : new_splits) {
            std::shared_ptr<SimulationSplit> simulation_split =
                    std::dynamic_pointer_cast<SimulationSplit>(split);
            if (simulation_split) {
                _running_splits.insert(simulation_split);
            }
        }
    }

    executor.enqueue_splits(_task_handle, true, new_splits);
}

void SimulationTask::set_killed() {
    _killed = true;
}

bool SimulationTask::is_killed() const {
    return _killed.load();
}

} // namespace vectorized
} // namespace doris
