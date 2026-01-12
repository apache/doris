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

#include "vec/exec/executor/tools/simulator/task_executor_simulator.h"

#include <chrono>
#include <fstream>
#include <iostream>
#include <thread>

#include "vec/exec/executor/time_sharing/multilevel_split_queue.h"
#include "vec/exec/executor/tools/simulator/histogram.h"

namespace doris {
namespace vectorized {

TaskSimulator::TaskSimulator(const std::string& config_file) : _config_file(config_file) {
    _start_time = std::chrono::high_resolution_clock::now();
}

Status TaskSimulator::_load_config(const std::string& config_file) {
    try {
        std::ifstream f(config_file);
        if (!f.is_open()) {
            return Status::IOError("Failed to open config file: " + config_file);
        }
        nlohmann::json j;
        f >> j;

        if (!j.contains("thread_config")) {
            return Status::InvalidArgument("Missing 'thread_config' section");
        }
        if (!j.contains("executor_config")) {
            return Status::InvalidArgument("Missing 'executor_config' section");
        }
        if (!j.contains("task_specs")) {
            return Status::InvalidArgument("Missing 'task_specs' section");
        }
        if (!j.contains("runtime_config")) {
            return Status::InvalidArgument("Missing 'runtime_config' section");
        }
        if (!j.contains("split_queue_config")) {
            return Status::InvalidArgument("Missing 'split_queue_config' section");
        }

        try {
            _config.split_queue_config.type = j["split_queue_config"]["type"].get<std::string>();
            if (_config.split_queue_config.type == "multi-level") {
                if (!j["split_queue_config"].contains("levels")) {
                    return Status::InvalidArgument("multi-level queue requires 'levels' parameter");
                }
                _config.split_queue_config.levels = j["split_queue_config"]["levels"].get<int>();
            }
        } catch (const nlohmann::json::exception& e) {
            return Status::InvalidArgument("Invalid split_queue_config: " + std::string(e.what()));
        }

        try {
            _config.thread_config.max_thread_num = j["thread_config"]["max_thread_num"].get<int>();
            _config.thread_config.min_thread_num = j["thread_config"]["min_thread_num"].get<int>();
        } catch (const nlohmann::json::exception& e) {
            return Status::InvalidArgument("Invalid thread_config: " + std::string(e.what()));
        }

        try {
            _config.executor_config.min_concurrency =
                    j["executor_config"]["min_concurrency"].get<int>();
            _config.executor_config.guaranteed_concurrency_per_task =
                    j["executor_config"]["guaranteed_concurrency_per_task"].get<int>();
            _config.executor_config.max_concurrency_per_task =
                    j["executor_config"]["max_concurrency_per_task"].get<int>();
        } catch (const nlohmann::json::exception& e) {
            return Status::InvalidArgument("Invalid executor_config: " + std::string(e.what()));
        }

        try {
            for (const auto& spec : j["task_specs"]) {
                SimulatorConfig::TaskSpecConfig task_spec;

                if (!spec.contains("name") || !spec.contains("type") ||
                    !spec.contains("num_concurrent_tasks") ||
                    !spec.contains("num_splits_per_task") ||
                    !spec.contains("split_generator_type")) {
                    return Status::InvalidArgument("Task spec missing required fields");
                }

                task_spec.name = spec["name"].get<std::string>();
                task_spec.type = spec["type"].get<std::string>();
                if (spec.contains("total_tasks") && !spec["total_tasks"].is_null()) {
                    task_spec.total_tasks = spec["total_tasks"].get<int>();
                }
                task_spec.num_concurrent_tasks = spec["num_concurrent_tasks"].get<int>();
                task_spec.num_splits_per_task = spec["num_splits_per_task"].get<int>();
                task_spec.split_generator_type = spec["split_generator_type"].get<std::string>();

                if (spec.contains("split_duration_nanos") &&
                    !spec["split_duration_nanos"].is_null()) {
                    task_spec.split_duration_nanos = spec["split_duration_nanos"].get<int64_t>();
                }
                if (spec.contains("split_wait_time_nanos") &&
                    !spec["split_wait_time_nanos"].is_null()) {
                    task_spec.split_wait_time_nanos = spec["split_wait_time_nanos"].get<int64_t>();
                }

                _config.task_specs.push_back(task_spec);
            }
        } catch (const nlohmann::json::exception& e) {
            return Status::InvalidArgument("Invalid task_specs: " + std::string(e.what()));
        }

        try {
            _config.runtime_config.clear_pending_queue_count =
                    j["runtime_config"]["clear_pending_queue_count"].get<int64_t>();
            _config.runtime_config.clear_pending_queue_interval_seconds =
                    j["runtime_config"]["clear_pending_queue_interval_seconds"].get<int64_t>();
        } catch (const nlohmann::json::exception& e) {
            return Status::InvalidArgument("Invalid runtime_config: " + std::string(e.what()));
        }

        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to parse config file: " + std::string(e.what()));
    }
}

Status TaskSimulator::_create_task_executor() {
    if (_config.split_queue_config.type == "fifo") {
        _split_queue = std::make_shared<SimulationFIFOSplitQueue>();
    } else if (_config.split_queue_config.type == "multi_level") {
        _split_queue = std::make_shared<MultilevelSplitQueue>(_config.split_queue_config.levels);
    } else {
        return Status::InvalidArgument("Unknown split queue type: " +
                                       _config.split_queue_config.type);
    }

    TimeSharingTaskExecutor::ThreadConfig thread_config;
    thread_config.max_thread_num = _config.thread_config.max_thread_num;
    thread_config.min_thread_num = _config.thread_config.min_thread_num;

    _task_executor = std::make_shared<TimeSharingTaskExecutor>(
            thread_config, _config.executor_config.min_concurrency,
            _config.executor_config.guaranteed_concurrency_per_task,
            _config.executor_config.max_concurrency_per_task, std::make_shared<SystemTicker>(),
            _split_queue);

    RETURN_IF_ERROR(_task_executor->init());
    RETURN_IF_ERROR(_task_executor->start());

    _controller = std::make_shared<SimulationController>(
            _task_executor, [this](SimulationController& controller,
                                   TimeSharingTaskExecutor& executor) { _print_summary_stats(); });

    return Status::OK();
}

Status TaskSimulator::_create_split_generator(
        const TaskSimulator::SimulatorConfig::TaskSpecConfig& spec,
        std::shared_ptr<SplitGenerator>& generator) {
    if (spec.split_generator_type == "slow_leaf") {
        generator = std::make_shared<SlowLeafSplitGenerator>();
    } else if (spec.split_generator_type == "fast_leaf") {
        generator = std::make_shared<FastLeafSplitGenerator>();
    } else if (spec.split_generator_type == "intermediate") {
        generator = std::make_shared<IntermediateSplitGenerator>();
    } else if (spec.split_generator_type == "simple") {
        if (!spec.split_duration_nanos || !spec.split_wait_time_nanos) {
            return Status::InvalidArgument("Simple generator requires duration and wait time");
        }
        generator = std::make_shared<SimpleLeafSplitGenerator>(spec.split_duration_nanos.value(),
                                                               spec.split_wait_time_nanos.value());
    } else if (spec.split_generator_type == "aggregated_leaf") {
        generator = std::make_shared<AggregatedLeafSplitGenerator>();
    } else if (spec.split_generator_type == "l4_leaf") {
        generator = std::make_shared<L4LeafSplitGenerator>();
    } else if (spec.split_generator_type == "quanta_exceeding_split_generator") {
        generator = std::make_shared<QuantaExceedingSplitGenerator>();
    } else {
        return Status::InvalidArgument("Unknown split generator type: " +
                                       spec.split_generator_type);
    }

    return Status::OK();
}

Status TaskSimulator::init() {
    RETURN_IF_ERROR(_load_config(_config_file));
    RETURN_IF_ERROR(_create_task_executor());

    for (const auto& spec : _config.task_specs) {
        std::shared_ptr<SplitGenerator> generator;
        RETURN_IF_ERROR(_create_split_generator(spec, generator));

        SimulationController::TasksSpecification task_spec(
                spec.type == "leaf" ? SimulationController::TasksSpecification::Type::LEAF
                                    : SimulationController::TasksSpecification::Type::INTERMEDIATE,
                spec.name, spec.total_tasks, spec.num_concurrent_tasks, spec.num_splits_per_task,
                generator);

        _controller->add_tasks_specification(task_spec);
        RETURN_IF_ERROR(_controller->enable_specification(task_spec));
    }

    return Status::OK();
}

Status TaskSimulator::run() {
    std::cout << "Starting simulation..." << std::endl;
    _controller->run();

    for (int i = 0; i < _config.runtime_config.clear_pending_queue_count; i++) {
        std::this_thread::sleep_for(
                std::chrono::seconds(_config.runtime_config.clear_pending_queue_interval_seconds));
        _controller->clear_pending_queue();
    }

    stop();
    return Status::OK();
}

void TaskSimulator::stop() {
    std::cout << "Stopping simulation..." << std::endl;
    std::cout << "_controller stopping..." << std::endl;
    _controller->stop();
    std::cout << "_controller stopped" << std::endl;
    std::cout << "_task_executor stopping..." << std::endl;
    _task_executor->stop();
    std::cout << "_task_executor stopped" << std::endl;

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration =
            std::chrono::duration_cast<std::chrono::milliseconds>(end_time - _start_time).count();

    std::cout << "Simulation completed. Runtime: " << duration << " ms" << std::endl;
    _print_summary_stats();
}

void TaskSimulator::_print_summary_stats() {
    auto spec_enabled = _controller->specifications_enabled();

    auto completed_tasks = _controller->completed_tasks();
    auto running_tasks = _controller->running_tasks();

    std::unordered_set<std::shared_ptr<SimulationTask>> all_tasks;
    for (const auto& [spec, task] : completed_tasks) {
        all_tasks.insert(task);
    }
    for (const auto& [spec, task] : running_tasks) {
        all_tasks.insert(task);
    }

    int64_t completed_splits = 0;
    for (const auto& [spec, task] : completed_tasks) {
        completed_splits += task->completed_splits().size();
    }

    int64_t running_splits = 0;
    for (const auto& [spec, task] : running_tasks) {
        //running_splits += task->running_splits().size();
        running_splits += task->completed_splits().size();
    }

    std::cout << "Completed tasks: " << completed_tasks.size() << std::endl;
    std::cout << "Remaining tasks: " << running_tasks.size() << std::endl;
    std::cout << "Completed splits: " << completed_splits << std::endl;
    std::cout << "Remaining splits: " << running_splits << std::endl;

    std::cout << std::endl;
    std::cout << "Completed tasks  L0: " << _task_executor->completed_tasks_level0() << std::endl;
    std::cout << "Completed tasks  L1: " << _task_executor->completed_tasks_level1() << std::endl;
    std::cout << "Completed tasks  L2: " << _task_executor->completed_tasks_level2() << std::endl;
    std::cout << "Completed tasks  L3: " << _task_executor->completed_tasks_level3() << std::endl;
    std::cout << "Completed tasks  L4: " << _task_executor->completed_tasks_level4() << std::endl;
    std::cout << std::endl;

    std::cout << "Completed splits L0: " << _task_executor->completed_splits_level0() << std::endl;
    std::cout << "Completed splits L1: " << _task_executor->completed_splits_level1() << std::endl;
    std::cout << "Completed splits L2: " << _task_executor->completed_splits_level2() << std::endl;
    std::cout << "Completed splits L3: " << _task_executor->completed_splits_level3() << std::endl;
    std::cout << "Completed splits L4: " << _task_executor->completed_splits_level4() << std::endl;

    std::vector<int64_t> time_buckets = {
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::milliseconds(0))
                    .count(),
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::milliseconds(1000))
                    .count(),
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::milliseconds(10000))
                    .count(),
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::milliseconds(60000))
                    .count(),
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::milliseconds(300000))
                    .count(),
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::hours(1)).count(),
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::hours(24)).count()};

    auto levels_histogram = Histogram<int64_t>::from_continuous(time_buckets);

    std::vector<std::shared_ptr<SimulationTask>> leaf_completed_tasks;
    for (const auto& [spec, task] : completed_tasks) {
        if (spec.get_type() == SimulationController::TasksSpecification::Type::LEAF) {
            leaf_completed_tasks.push_back(task);
        }
    }

    std::vector<std::shared_ptr<SimulationTask>> leaf_running_tasks;
    for (const auto& [spec, task] : running_tasks) {
        if (spec.get_type() == SimulationController::TasksSpecification::Type::LEAF) {
            leaf_running_tasks.push_back(task);
        }
    }

    std::cout << std::endl;
    std::cout << "Levels - Completed Task Processed Time" << std::endl;
    levels_histogram.print_distribution(
            leaf_completed_tasks,
            std::function<int64_t(std::shared_ptr<SimulationTask>)>(
                    [](std::shared_ptr<SimulationTask> task) {
                        return task->get_scheduled_time_nanos();
                    }),
            std::function<int64_t(std::shared_ptr<SimulationTask>)>(
                    [](std::shared_ptr<SimulationTask> task) {
                        return task->get_processed_time_nanos();
                    }),
            std::function<std::string(int64_t)>(
                    [](int64_t nanos) { return HistogramUtils::format_nanos(nanos); }),
            std::function<std::string(std::vector<int64_t>&)>(
                    [](std::vector<int64_t>& list) { return _format_stats(list); }));

    std::cout << std::endl;
    std::cout << "Levels - Running Task Processed Time" << std::endl;
    levels_histogram.print_distribution(
            leaf_running_tasks,
            std::function<int64_t(std::shared_ptr<SimulationTask>)>(
                    [](std::shared_ptr<SimulationTask> task) {
                        return task->get_scheduled_time_nanos();
                    }),
            std::function<int64_t(std::shared_ptr<SimulationTask>)>(
                    [](std::shared_ptr<SimulationTask> task) {
                        return task->get_processed_time_nanos();
                    }),
            std::function<std::string(int64_t)>(
                    [](int64_t nanos) { return HistogramUtils::format_nanos(nanos); }),
            std::function<std::string(std::vector<int64_t>&)>(
                    [](std::vector<int64_t>& list) { return _format_stats(list); }));

    std::cout << std::endl;
    std::cout << "Levels - All Task Wait Time" << std::endl;
    levels_histogram.print_distribution(
            leaf_running_tasks,
            std::function<int64_t(std::shared_ptr<SimulationTask>)>(
                    [](std::shared_ptr<SimulationTask> task) {
                        return task->get_scheduled_time_nanos();
                    }),
            std::function<int64_t(std::shared_ptr<SimulationTask>)>(
                    [](std::shared_ptr<SimulationTask> task) {
                        return task->get_total_wait_time_nanos();
                    }),
            std::function<std::string(int64_t)>(
                    [](int64_t nanos) { return HistogramUtils::format_nanos(nanos); }),
            std::function<std::string(std::vector<int64_t>&)>(
                    [](std::vector<int64_t>& list) { return _format_stats(list); }));

    std::unordered_set<std::string> specifications;
    //        for (const auto& task : all_tasks) {
    //            specifications.insert(task->specification().get_name());
    //        }
    for (const auto& [spec, task] : running_tasks) {
        specifications.insert(task->specification().get_name());
    }

    std::cout << std::endl;
    std::cout << "Specification - Processed time" << std::endl;
    auto spec_names = std::vector<std::string>(specifications.begin(), specifications.end());
    auto all_tasks_vec =
            std::vector<std::shared_ptr<SimulationTask>>(all_tasks.begin(), all_tasks.end());
    auto spec_histogram = Histogram<std::string>::from_discrete(spec_names);
    spec_histogram.print_distribution(
            all_tasks_vec,
            std::function<std::string(std::shared_ptr<SimulationTask>)>(
                    [](std::shared_ptr<SimulationTask> task) {
                        return task->specification().get_name();
                    }),
            std::function<int64_t(std::shared_ptr<SimulationTask>)>(
                    [](std::shared_ptr<SimulationTask> task) {
                        return task->get_processed_time_nanos();
                    }),
            std::function<std::string(std::string)>([](std::string s) { return s; }),
            std::function<std::string(std::vector<int64_t>&)>(
                    [](std::vector<int64_t>& list) { return _format_stats(list); }));

    std::cout << std::endl;
    std::cout << "Specification - Wait time" << std::endl;
    spec_histogram.print_distribution(
            all_tasks_vec,
            std::function<std::string(std::shared_ptr<SimulationTask>)>(
                    [](std::shared_ptr<SimulationTask> task) {
                        return task->specification().get_name();
                    }),
            std::function<int64_t(std::shared_ptr<SimulationTask>)>(
                    [](std::shared_ptr<SimulationTask> task) {
                        return task->get_total_wait_time_nanos();
                    }),
            std::function<std::string(std::string)>([](std::string s) { return s; }),
            std::function<std::string(std::vector<int64_t>&)>(
                    [](std::vector<int64_t>& list) { return _format_stats(list); }));

    std::cout << std::endl;
    std::cout << "Breakdown by specification" << std::endl;
    std::cout << "##########################" << std::endl;

    for (const auto& [spec, enabled] : spec_enabled) {
        std::vector<std::shared_ptr<SimulationTask>> all_spec_tasks;

        auto completed_range = completed_tasks.equal_range(spec);
        for (auto it = completed_range.first; it != completed_range.second; ++it) {
            all_spec_tasks.push_back(it->second);
        }

        auto running_range = running_tasks.equal_range(spec);
        for (auto it = running_range.first; it != running_range.second; ++it) {
            all_spec_tasks.push_back(it->second);
        }

        //            if (all_spec_tasks.empty()) {
        //                continue;
        //            }

        std::cout << spec.get_name() << std::endl;
        std::cout << "=============================" << std::endl;

        size_t completed_count = std::distance(completed_range.first, completed_range.second);
        size_t running_count = std::distance(running_range.first, running_range.second);

        std::cout << "Completed tasks           : " << completed_count << std::endl;
        std::cout << "In-progress tasks         : " << running_count << std::endl;
        std::cout << "Total tasks               : " << spec.get_total_tasks().value_or(0)
                  << std::endl;
        std::cout << "Splits/task               : " << spec.get_num_splits_per_task() << std::endl;

        int64_t scheduled_time_sum = 0;
        int64_t processed_time_sum = 0;
        int64_t wait_time_sum = 0;
        int64_t calls_sum = 0;

        for (const auto& task : all_spec_tasks) {
            scheduled_time_sum += task->get_scheduled_time_nanos();
            processed_time_sum += task->get_processed_time_nanos();
            wait_time_sum += task->get_total_wait_time_nanos();
            calls_sum += task->get_calls();
        }

        std::cout << "Current required time     : "
                  << HistogramUtils::format_nanos(scheduled_time_sum) << std::endl;
        std::cout << "Completed scheduled time  : "
                  << HistogramUtils::format_nanos(processed_time_sum) << std::endl;
        std::cout << "Total wait time           : " << HistogramUtils::format_nanos(wait_time_sum)
                  << std::endl;
        std::cout << "Total calls           : " << calls_sum << std::endl;

        std::cout << std::endl;
        std::cout << "All Tasks by Scheduled time - Processed Time" << std::endl;
        levels_histogram.print_distribution(
                all_spec_tasks,
                std::function<int64_t(std::shared_ptr<SimulationTask>)>(
                        [](std::shared_ptr<SimulationTask> task) {
                            return task->get_scheduled_time_nanos();
                        }),
                std::function<int64_t(std::shared_ptr<SimulationTask>)>(
                        [](std::shared_ptr<SimulationTask> task) {
                            return task->get_processed_time_nanos();
                        }),
                std::function<std::string(int64_t)>(
                        [](int64_t nanos) { return HistogramUtils::format_nanos(nanos); }),
                std::function<std::string(std::vector<int64_t>&)>(
                        [](std::vector<int64_t>& list) { return _format_stats(list); }));

        std::cout << std::endl;
        std::cout << "All Tasks by Scheduled time - Wait Time" << std::endl;
        levels_histogram.print_distribution(
                all_spec_tasks,
                std::function<int64_t(std::shared_ptr<SimulationTask>)>(
                        [](std::shared_ptr<SimulationTask> task) {
                            return task->get_scheduled_time_nanos();
                        }),
                std::function<int64_t(std::shared_ptr<SimulationTask>)>(
                        [](std::shared_ptr<SimulationTask> task) {
                            return task->get_total_wait_time_nanos();
                        }),
                std::function<std::string(int64_t)>(
                        [](int64_t nanos) { return HistogramUtils::format_nanos(nanos); }),
                std::function<std::string(std::vector<int64_t>&)>(
                        [](std::vector<int64_t>& list) { return _format_stats(list); }));

        std::cout << std::endl;
        std::cout << "Complete Tasks by Scheduled time - Wait Time" << std::endl;

        std::vector<std::shared_ptr<SimulationTask>> completed_spec_tasks;
        for (auto it = completed_range.first; it != completed_range.second; ++it) {
            completed_spec_tasks.push_back(it->second);
        }

        levels_histogram.print_distribution(
                completed_spec_tasks,
                std::function<int64_t(std::shared_ptr<SimulationTask>)>(
                        [](std::shared_ptr<SimulationTask> task) {
                            return task->get_scheduled_time_nanos();
                        }),
                std::function<int64_t(std::shared_ptr<SimulationTask>)>(
                        [](std::shared_ptr<SimulationTask> task) {
                            return task->get_total_wait_time_nanos();
                        }),
                std::function<std::string(int64_t)>(
                        [](int64_t nanos) { return HistogramUtils::format_nanos(nanos); }),
                std::function<std::string(std::vector<int64_t>&)>(
                        [](std::vector<int64_t>& list) { return _format_stats(list); }));
    }
}

std::string TaskSimulator::_format_stats(std::vector<int64_t>& list) {
    int64_t min = std::numeric_limits<int64_t>::max();
    int64_t max = std::numeric_limits<int64_t>::min();
    int64_t sum = 0;

    for (int64_t val : list) {
        min = std::min(min, val);
        max = std::max(max, val);
        sum += val;
    }

    double avg = list.empty() ? 0 : static_cast<double>(sum) / list.size();

    std::ostringstream oss;
    oss << "Min: "
        << HistogramUtils::format_nanos(min == std::numeric_limits<int64_t>::max() ? 0 : min)
        << "  Max: "
        << HistogramUtils::format_nanos(max == std::numeric_limits<int64_t>::min() ? 0 : max)
        << "  Avg: " << HistogramUtils::format_nanos(static_cast<int64_t>(avg))
        << "  Sum: " << HistogramUtils::format_nanos(sum);
    return oss.str();
}

} // namespace vectorized
} // namespace doris

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <config_file>" << std::endl;
        return 1;
    }

    std::string config_file = argv[1];
    doris::vectorized::TaskSimulator simulator(config_file);

    auto status = simulator.init();
    if (!status.ok()) {
        std::cerr << "Failed to initialize simulator: " << status.to_string() << std::endl;
        return 1;
    }

    status = simulator.run();
    if (!status.ok()) {
        std::cerr << "Simulation failed: " << status.to_string() << std::endl;
        return 1;
    }

    return 0;
}
