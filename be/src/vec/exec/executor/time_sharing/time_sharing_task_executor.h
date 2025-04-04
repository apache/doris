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
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "bvar/latency_recorder.h"
#include "util/threadpool.h"
#include "vec/exec/executor/listenable_future.h"
#include "vec/exec/executor/stats.h"
#include "vec/exec/executor/task_executor.h"
#include "vec/exec/executor/ticker.h"
#include "vec/exec/executor/time_sharing/multilevel_split_queue.h"
#include "vec/exec/executor/time_sharing/prioritized_split_runner.h"

namespace doris {
namespace vectorized {

/**
 * ThreadSafe
 */
class TimeSharingTaskExecutor : public TaskExecutor {
    ENABLE_FACTORY_CREATOR(TimeSharingTaskExecutor);

public:
    struct ThreadConfig {
        std::string thread_name;
        int max_thread_num;
        int min_thread_num;
        int max_queue_size = 0;
        std::weak_ptr<CgroupCpuCtl> cgroup_cpu_ctl;
    };

    TimeSharingTaskExecutor(ThreadConfig config, int min_concurrency,
                            int guaranteed_concurrency_per_task, int max_concurrency_per_task,
                            std::shared_ptr<Ticker> ticker,
                            std::chrono::milliseconds stuck_split_warning_threshold =
                                    std::chrono::milliseconds(60000),
                            std::shared_ptr<MultilevelSplitQueue> split_queue = nullptr);

    ~TimeSharingTaskExecutor() override;

    Status init() override;

    Status start() override;
    void stop() override;

    Result<std::shared_ptr<TaskHandle>> create_task(
            const TaskId& task_id, std::function<double()> utilization_supplier,
            int initial_split_concurrency,
            std::chrono::nanoseconds split_concurrency_adjust_frequency,
            std::optional<int> max_concurrency_per_task) override;

    Status add_task(const TaskId& task_id, std::shared_ptr<TaskHandle> task_handle) override;

    Status remove_task(std::shared_ptr<TaskHandle> task_handle) override;

    Result<std::vector<SharedListenableFuture<Void>>> enqueue_splits(
            std::shared_ptr<TaskHandle> task_handle, bool intermediate,
            const std::vector<std::shared_ptr<SplitRunner>>& splits) override;

    size_t waiting_splits_size() const { return _waiting_splits->size(); }

    size_t intermediate_splits_size() const {
        std::lock_guard<std::mutex> guard(_mutex);
        return _intermediate_splits.size();
    }

    size_t running_splits_size() const { return _running_splits.size(); }

    size_t blocked_splits_size() const { return _blocked_splits.size(); }

    size_t total_splits_size() const {
        std::lock_guard<std::mutex> guard(_mutex);
        return _all_splits.size();
    }

    size_t tasks_size() const {
        std::lock_guard<std::mutex> guard(_mutex);
        return _tasks.size();
    }

    int64_t completed_tasks_level0() const { return _completed_tasks_per_level[0]; }

    int64_t completed_tasks_level1() const { return _completed_tasks_per_level[1]; }

    int64_t completed_tasks_level2() const { return _completed_tasks_per_level[2]; }

    int64_t completed_tasks_level3() const { return _completed_tasks_per_level[3]; }

    int64_t completed_tasks_level4() const { return _completed_tasks_per_level[4]; }

    int64_t completed_splits_level0() const { return _completed_splits_per_level[0]; }

    int64_t completed_splits_level1() const { return _completed_splits_per_level[1]; }

    int64_t completed_splits_level2() const { return _completed_splits_per_level[2]; }

    int64_t completed_splits_level3() const { return _completed_splits_per_level[3]; }

    int64_t completed_splits_level4() const { return _completed_splits_per_level[4]; }

    int64_t running_tasks_level0() const { return _get_running_tasks_for_level(0); }

    int64_t running_tasks_level1() const { return _get_running_tasks_for_level(1); }

    int64_t running_tasks_level2() const { return _get_running_tasks_for_level(2); }

    int64_t running_tasks_level3() const { return _get_running_tasks_for_level(3); }

    int64_t running_tasks_level4() const { return _get_running_tasks_for_level(4); }

    ThreadPool* thread_pool() const { return _thread_pool.get(); };

private:
    /*class ScopedRunnerTracker {
    public:
        ScopedRunnerTracker(
                std::mutex& mutex,
                std::unordered_set<std::shared_ptr<PrioritizedSplitRunner>>& running_splits,
                std::shared_ptr<PrioritizedSplitRunner> split)
                : _mutex(mutex), _running_splits(running_splits), _split(std::move(split)) {
            std::lock_guard<std::mutex> guard(_mutex);
            _running_splits.insert(_split);
        }

        ~ScopedRunnerTracker() {
            std::lock_guard<std::mutex> guard(_mutex);
            _running_splits.erase(_split);
        }

    private:
        std::mutex& _mutex;
        std::unordered_set<std::shared_ptr<PrioritizedSplitRunner>>& _running_splits;
        std::shared_ptr<PrioritizedSplitRunner> _split;
    };*/

    /*class TaskRunner {
    public:
        TaskRunner(TimeSharingTaskExecutor& executor);
        void run();
        void stop();

    private:
        TimeSharingTaskExecutor& _executor;
        std::atomic<bool> _running {true};
    };*/

    Status _add_runner_thread();
    void _schedule_task_if_necessary(std::shared_ptr<TimeSharingTaskHandle> task_handle,
                                     std::lock_guard<std::mutex>& guard);
    void _add_new_entrants(std::lock_guard<std::mutex>& guard);
    void _start_intermediate_split(std::shared_ptr<PrioritizedSplitRunner> split,
                                   std::lock_guard<std::mutex>& guard);
    void _start_split(std::shared_ptr<PrioritizedSplitRunner> split,
                      std::lock_guard<std::mutex>& guard);
    std::shared_ptr<PrioritizedSplitRunner> _poll_next_split_worker(
            std::lock_guard<std::mutex>& guard);
    void _record_leaf_splits_size(std::lock_guard<std::mutex>& guard);
    void _split_finished(std::shared_ptr<PrioritizedSplitRunner> split, const Status& status);
    void _interrupt();

    int64_t _get_running_tasks_for_level(int level) const;

    std::unique_ptr<ThreadPool> _thread_pool;
    ThreadConfig _thread_config;
    const int _min_concurrency;
    const int _guaranteed_concurrency_per_task;
    const int _max_concurrency_per_task;
    std::shared_ptr<Ticker> _ticker;
    const std::chrono::milliseconds _stuck_split_warning_threshold;
    std::shared_ptr<MultilevelSplitQueue> _waiting_splits;

    mutable std::mutex _mutex;
    std::condition_variable _condition;
    std::atomic<bool> _stopped {false};

    //std::vector<std::unique_ptr<TaskRunner>> _task_runners;

    std::unordered_map<TaskId, std::shared_ptr<TimeSharingTaskHandle>> _tasks;

    std::unordered_set<std::shared_ptr<PrioritizedSplitRunner>> _all_splits;
    std::unordered_set<std::shared_ptr<PrioritizedSplitRunner>> _intermediate_splits;
    std::unordered_set<std::shared_ptr<PrioritizedSplitRunner>> _running_splits;
    std::unordered_map<std::shared_ptr<PrioritizedSplitRunner>, SharedListenableFuture<Void>>
            _blocked_splits;
    std::array<std::atomic<int64_t>, 5> _completed_tasks_per_level = {0, 0, 0, 0, 0};
    std::array<std::atomic<int64_t>, 5> _completed_splits_per_level = {0, 0, 0, 0, 0};

    bvar::LatencyRecorder _split_queued_time;
};

} // namespace vectorized
} // namespace doris
