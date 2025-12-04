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

#include <atomic>
#include <condition_variable>
#include <functional>
#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "vec/exec/executor/time_sharing/time_sharing_task_executor.h"
#include "vec/exec/executor/tools/simulator/split_generators.h"
#include "vec/exec/executor/tools/simulator/split_specification.h"

namespace doris {
namespace vectorized {

class SimulationTask;

class SimulationController {
public:
    class TasksSpecification {
    public:
        enum class Type { LEAF, INTERMEDIATE };

        TasksSpecification(Type type, std::string name, std::optional<int> total_tasks,
                           int num_concurrent_tasks, int num_splits_per_task,
                           std::shared_ptr<SplitGenerator> split_generator)
                : _type(type),
                  _name(std::move(name)),
                  _total_tasks(total_tasks),
                  _num_concurrent_tasks(num_concurrent_tasks),
                  _num_splits_per_task(num_splits_per_task),
                  _split_generator(std::move(split_generator)) {}

        bool operator==(const TasksSpecification& other) const {
            return _type == other._type && _name == other._name &&
                   _total_tasks == other._total_tasks &&
                   _num_concurrent_tasks == other._num_concurrent_tasks &&
                   _num_splits_per_task == other._num_splits_per_task;
            // Note: We don't compare _split_generator as it might not be comparable
            // and it's not needed for equality comparison in this context
        }

        bool operator<(const TasksSpecification& other) const {
            if (_type != other._type) return _type < other._type;
            if (_name != other._name) return _name < other._name;
            if (_total_tasks != other._total_tasks) return _total_tasks < other._total_tasks;
            if (_num_concurrent_tasks != other._num_concurrent_tasks)
                return _num_concurrent_tasks < other._num_concurrent_tasks;
            return _num_splits_per_task < other._num_splits_per_task;
        }

        Type get_type() const { return _type; }

        std::string get_name() const { return _name; }

        int get_num_concurrent_tasks() const { return _num_concurrent_tasks; }

        int get_num_splits_per_task() const { return _num_splits_per_task; }

        std::optional<int> get_total_tasks() const { return _total_tasks; }

        std::unique_ptr<SplitSpecification> next_specification() const {
            return _split_generator->next();
        }

    private:
        Type _type;
        std::string _name;
        std::optional<int> _total_tasks;
        int _num_concurrent_tasks;
        int _num_splits_per_task;
        std::shared_ptr<SplitGenerator> _split_generator;
    };

    struct TasksSpecHash {
        size_t operator()(const TasksSpecification& ts) const {
            size_t hash = std::hash<std::string> {}(ts.get_name());
            hash ^= std::hash<int> {}(static_cast<int>(ts.get_type())) + 0x9e3779b9 + (hash << 6) +
                    (hash >> 2);
            hash ^= std::hash<int> {}(ts.get_num_concurrent_tasks()) + 0x9e3779b9 + (hash << 6) +
                    (hash >> 2);
            hash ^= std::hash<int> {}(ts.get_num_splits_per_task()) + 0x9e3779b9 + (hash << 6) +
                    (hash >> 2);

            if (ts.get_total_tasks().has_value()) {
                hash ^= std::hash<int> {}(ts.get_total_tasks().value()) + 0x9e3779b9 + (hash << 6) +
                        (hash >> 2);
            }

            return hash;
        }
    };

    SimulationController(
            std::shared_ptr<TimeSharingTaskExecutor> executor,
            std::function<void(SimulationController&, TimeSharingTaskExecutor&)> callback);

    void add_tasks_specification(const TasksSpecification& spec);
    void clear_pending_queue();
    void stop();
    Status enable_specification(const TasksSpecification& spec);
    void run();
    void run_callback();

    const std::unordered_map<TasksSpecification, bool, TasksSpecHash>& specifications_enabled()
            const;
    const std::multimap<TasksSpecification, std::shared_ptr<SimulationTask>>& running_tasks() const;
    const std::multimap<TasksSpecification, std::shared_ptr<SimulationTask>>& completed_tasks()
            const;

private:
    Status _start_spec(const TasksSpecification& spec, const std::lock_guard<std::mutex>& lock);
    void _schedule_splits();
    Status _replace_completed_tasks();
    Status _create_task(const TasksSpecification& specification,
                        const std::lock_guard<std::mutex>& lock);

    std::shared_ptr<TimeSharingTaskExecutor> _task_executor;
    std::function<void(SimulationController&, TimeSharingTaskExecutor&)> _callback;

    std::unordered_map<TasksSpecification, bool, TasksSpecHash> _specifications_enabled;
    std::multimap<TasksSpecification, std::shared_ptr<SimulationTask>> _running_tasks;
    std::multimap<TasksSpecification, std::shared_ptr<SimulationTask>> _completed_tasks;

    std::atomic<bool> _clear_pending_queue {false};
    std::atomic<bool> _stopped {false};
    std::thread _controller_thread;
    mutable std::mutex _mutex;

    static constexpr int DEFAULT_MIN_SPLITS = 3;
};

} // namespace vectorized
} // namespace doris
