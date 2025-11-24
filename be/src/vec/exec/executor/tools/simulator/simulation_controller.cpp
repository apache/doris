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

#include "vec/exec/executor/tools/simulator/simulation_controller.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common/exception.h"
#include "vec/exec/executor/time_sharing/time_sharing_task_executor.h"
#include "vec/exec/executor/tools/simulator/simulation_task.h"

namespace doris {
namespace vectorized {

SimulationController::SimulationController(
        std::shared_ptr<TimeSharingTaskExecutor> executor,
        std::function<void(SimulationController&, TimeSharingTaskExecutor&)> callback)
        : _task_executor(std::move(executor)),
          _callback(std::move(callback)),
          _running_tasks(),
          _completed_tasks() {}

void SimulationController::add_tasks_specification(const TasksSpecification& spec) {
    std::lock_guard<std::mutex> lock(_mutex);
    _specifications_enabled[spec] = false;
}

void SimulationController::clear_pending_queue() {
    std::cout << "Clearing pending queue." << std::endl;
    _clear_pending_queue.store(true);
}

void SimulationController::stop() {
    _stopped.store(true);
    if (_controller_thread.joinable()) {
        _controller_thread.join();
    }
}

Status SimulationController::enable_specification(const TasksSpecification& spec) {
    std::lock_guard<std::mutex> lock(_mutex);
    auto it = _specifications_enabled.find(spec);
    if (it != _specifications_enabled.end()) {
        bool was_enabled = it->second;
        it->second = true;

        if (!was_enabled) {
            RETURN_IF_ERROR(_start_spec(spec, lock));
        }
    }
    return Status::OK();
}

void SimulationController::run_callback() {
    if (_callback) {
        _callback(*this, *_task_executor);
    }
}

void SimulationController::run() {
    _controller_thread = std::thread([this] {
        while (!_stopped.load()) {
            auto status = _replace_completed_tasks();
            if (!status.ok()) {
                throw doris::Exception(status.code(), status.to_string());
            }
            _schedule_splits();
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
    });
}

Status SimulationController::_start_spec(const TasksSpecification& spec,
                                         const std::lock_guard<std::mutex>& lock) {
    if (!_specifications_enabled[spec]) {
        return Status::OK();
    }

    for (int i = 0; i < spec.get_num_concurrent_tasks(); ++i) {
        RETURN_IF_ERROR(_create_task(spec, lock));
    }
    return Status::OK();
}

Status SimulationController::_create_task(const TasksSpecification& specification,
                                          const std::lock_guard<std::mutex>& lock) {
    int running_count = 0;
    int completed_count = 0;

    auto running_range = _running_tasks.equal_range(specification);
    for (auto it = running_range.first; it != running_range.second; ++it) {
        running_count++;
    }

    auto completed_range = _completed_tasks.equal_range(specification);
    for (auto it = completed_range.first; it != completed_range.second; ++it) {
        completed_count++;
    }

    std::string task_id =
            specification.get_name() + "_" + std::to_string(running_count + completed_count);

    std::shared_ptr<SimulationTask> new_task;

    if (specification.get_type() == TasksSpecification::Type::LEAF) {
        new_task = std::make_shared<LeafTask>(*_task_executor, specification, task_id);
    } else {
        new_task = std::make_shared<IntermediateTask>(*_task_executor, specification, task_id);
    }
    RETURN_IF_ERROR(new_task->init());
    _running_tasks.insert({specification, new_task});
    return Status::OK();
}

void SimulationController::_schedule_splits() {
    std::lock_guard<std::mutex> lock(_mutex);

    if (_clear_pending_queue.load()) {
        if (_task_executor->waiting_splits_size() >
            (_task_executor->intermediate_splits_size() - _task_executor->blocked_splits_size())) {
            return;
        }
        std::cout << "Cleared pending queue" << std::endl;
        _clear_pending_queue.store(false);
    }

    for (auto& [spec, enabled] : _specifications_enabled) {
        if (!enabled) continue;

        auto range = _running_tasks.equal_range(spec);
        for (auto it = range.first; it != range.second; ++it) {
            auto& task = it->second;
            if (spec.get_type() == TasksSpecification::Type::LEAF) {
                int remaining_splits =
                        spec.get_num_splits_per_task() -
                        (task->running_splits().size() + task->completed_splits().size());
                int candidate_splits = DEFAULT_MIN_SPLITS - task->running_splits().size();

                for (int i = 0; i < std::min(remaining_splits, candidate_splits); ++i) {
                    task->schedule(*_task_executor, 1);
                }
            } else {
                int remaining_splits =
                        spec.get_num_splits_per_task() -
                        (task->running_splits().size() + task->completed_splits().size());
                task->schedule(*_task_executor, remaining_splits);
            }
        }
    }
}

Status SimulationController::_replace_completed_tasks() {
    std::lock_guard<std::mutex> lock(_mutex);
    bool moved;
    do {
        moved = false;

        for (auto& [spec, enabled] : _specifications_enabled) {
            int running_count = 0;
            int completed_count = 0;

            auto running_range = _running_tasks.equal_range(spec);
            for (auto it = running_range.first; it != running_range.second; ++it) {
                running_count++;
            }

            auto completed_range = _completed_tasks.equal_range(spec);
            for (auto it = completed_range.first; it != completed_range.second; ++it) {
                completed_count++;
            }

            if (spec.get_total_tasks() > 0 && enabled &&
                (completed_count + running_count) >= spec.get_total_tasks()) {
                std::cout << "\n"
                          << spec.get_name() << " disabled for reaching target "
                          << spec.get_total_tasks().value() << "\n\n";
                enabled = false;
                run_callback();
                continue;
            }

            for (auto it = running_range.first; it != running_range.second;) {
                auto& task = it->second;
                if (task->completed_splits().size() >= spec.get_num_splits_per_task()) {
                    _completed_tasks.insert({spec, task});
                    RETURN_IF_ERROR(_task_executor->remove_task(task->task_handle()));

                    auto current = it++;
                    _running_tasks.erase(current);

                    if (enabled) {
                        RETURN_IF_ERROR(_create_task(spec, lock));
                    }

                    moved = true;
                    break;
                } else {
                    ++it;
                }
            }
        }
    } while (moved);
    return Status::OK();
}

const std::unordered_map<SimulationController::TasksSpecification, bool,
                         SimulationController::TasksSpecHash>&
SimulationController::specifications_enabled() const {
    return _specifications_enabled;
}

const std::multimap<SimulationController::TasksSpecification, std::shared_ptr<SimulationTask>>&
SimulationController::running_tasks() const {
    return _running_tasks;
}

const std::multimap<SimulationController::TasksSpecification, std::shared_ptr<SimulationTask>>&
SimulationController::completed_tasks() const {
    return _completed_tasks;
}

} // namespace vectorized
} // namespace doris
