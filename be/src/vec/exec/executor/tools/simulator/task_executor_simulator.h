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

#include <memory>
#include <nlohmann/json.hpp>
#include <optional>
#include <string>
#include <vector>

#include "vec/exec/executor/time_sharing/time_sharing_task_executor.h"
#include "vec/exec/executor/tools/simulator/simulation_controller.h"
#include "vec/exec/executor/tools/simulator/simulation_fifo_split_queue.h"
#include "vec/exec/executor/tools/simulator/simulation_task.h"
#include "vec/exec/executor/tools/simulator/split_generators.h"

namespace doris {
namespace vectorized {

class TaskSimulator {
public:
    struct SimulatorConfig {
        struct ThreadConfig {
            int max_thread_num;
            int min_thread_num;
        } thread_config;

        struct ExecutorConfig {
            int min_concurrency;
            int guaranteed_concurrency_per_task;
            int max_concurrency_per_task;
        } executor_config;

        struct SplitQueueConfig {
            std::string type; // "fifo" or "multi_level"
            int levels;       // only used when type = "multi_levelL"
        } split_queue_config;

        struct TaskSpecConfig {
            std::string name;
            std::string type;
            std::optional<int> total_tasks;
            int num_concurrent_tasks;
            int num_splits_per_task;
            std::string split_generator_type;
            std::optional<int64_t> split_duration_nanos;
            std::optional<int64_t> split_wait_time_nanos;
        };
        std::vector<TaskSpecConfig> task_specs;

        struct RuntimeConfig {
            int64_t clear_pending_queue_count;
            int64_t clear_pending_queue_interval_seconds;
        } runtime_config;
    };

    TaskSimulator(const std::string& config_file);
    ~TaskSimulator() = default;

    Status init();
    Status run();
    void stop();

private:
    Status _load_config(const std::string& config_file);
    Status _create_task_executor();
    Status _create_split_generator(const TaskSimulator::SimulatorConfig::TaskSpecConfig& spec,
                                   std::shared_ptr<SplitGenerator>& generator);
    void _print_summary_stats();
    static std::string _format_stats(std::vector<int64_t>& list);

    std::string _config_file;
    SimulatorConfig _config;
    std::shared_ptr<TimeSharingTaskExecutor> _task_executor;
    std::shared_ptr<SplitQueue> _split_queue;
    std::shared_ptr<SimulationController> _controller;
    std::chrono::time_point<std::chrono::high_resolution_clock> _start_time;
};

} // namespace vectorized
} // namespace doris
