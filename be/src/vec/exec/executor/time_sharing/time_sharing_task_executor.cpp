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

#include "vec/exec/executor/time_sharing/time_sharing_task_executor.h"

#include <functional>
#include <iostream>

#include "util/defer_op.h"
#include "util/thread.h"
#include "util/threadpool.h"
#include "vec/exec/executor/time_sharing/time_sharing_task_handle.h"

namespace doris {
namespace vectorized {

TimeSharingTaskExecutor::TimeSharingTaskExecutor(
        ThreadConfig thread_config, int min_concurrency, int guaranteed_concurrency_per_task,
        int max_concurrency_per_task, std::shared_ptr<Ticker> ticker,
        std::chrono::milliseconds stuck_split_warning_threshold,
        std::shared_ptr<MultilevelSplitQueue> split_queue)
        : _thread_config(thread_config),
          _min_concurrency(min_concurrency),
          _guaranteed_concurrency_per_task(guaranteed_concurrency_per_task),
          _max_concurrency_per_task(max_concurrency_per_task),
          _ticker(ticker != nullptr ? ticker : std::make_shared<SystemTicker>()),
          _stuck_split_warning_threshold(stuck_split_warning_threshold),
          _waiting_splits(split_queue != nullptr ? std::move(split_queue)
                                                 : std::make_shared<MultilevelSplitQueue>(2)) {}

Status TimeSharingTaskExecutor::init() {
    static_cast<void>(_stuck_split_warning_threshold);

    ThreadPoolBuilder builder(_thread_config.thread_name);
    builder.set_min_threads(_thread_config.min_thread_num)
            .set_max_threads(_thread_config.max_thread_num)
            .set_max_queue_size(_thread_config.max_queue_size)
            .set_cgroup_cpu_ctl(_thread_config.cgroup_cpu_ctl);
    RETURN_IF_ERROR(builder.build(&_thread_pool));
    return Status::OK();
}

TimeSharingTaskExecutor::~TimeSharingTaskExecutor() {
    if (!_stopped.exchange(true)) {
        stop();
    }

    std::vector<std::shared_ptr<PrioritizedSplitRunner>> splits_to_destroy;
    {
        std::lock_guard<std::mutex> guard(_mutex);
        for (auto& [task_id, handle] : _tasks) {
            if (handle->is_closed()) {
                LOG(WARNING) << "task is already destroyed, task_id: " << task_id.to_string();
                continue;
            }
            auto task_splits = handle->close();
            splits_to_destroy.insert(splits_to_destroy.end(),
                                     std::make_move_iterator(task_splits.begin()),
                                     std::make_move_iterator(task_splits.end()));
            _record_leaf_splits_size(guard);
        }
        //_tasks.clear();
        //_all_splits.clear();
        //_intermediate_splits.clear();
        //_blocked_splits.clear();
        _waiting_splits->remove_all(splits_to_destroy);
    }

    if (splits_to_destroy.empty()) {
        return;
    }

    for (auto& split : splits_to_destroy) {
        split->close(Status::OK());
    }

    //if (_thread_pool) {
    //    _thread_pool->shutdown();
    //}
}

Status TimeSharingTaskExecutor::start() {
    std::lock_guard<std::mutex> guard(_mutex);
    for (int i = 0; i < _thread_config.min_thread_num; ++i) {
        RETURN_IF_ERROR(_add_runner_thread());
    }
    return Status::OK();
}

void TimeSharingTaskExecutor::stop() {
    _waiting_splits->interrupt();
    {
        std::lock_guard<std::mutex> guard(_mutex);
        _stopped = true;
    }
    if (_thread_pool) {
        _thread_pool->shutdown();
        _thread_pool->wait();
    }
}

Status TimeSharingTaskExecutor::_add_runner_thread() {
    return _thread_pool->submit_func([this]() {
        Thread::set_self_name("SplitRunner");

        while (!_stopped) {
            std::shared_ptr<PrioritizedSplitRunner> split;
            split = _waiting_splits->take();
            if (!split) {
                return;
            }

            //            ScopedRunnerTracker tracker(_mutex, _running_splits, split);
            {
                std::lock_guard<std::mutex> guard(_mutex);
                _running_splits.insert(split);
            }
            Defer defer {[&]() {
                std::lock_guard<std::mutex> guard(_mutex);
                _running_splits.erase(split);
            }};
            Result<SharedListenableFuture<Void>> blocked_future_result = split->process();
            if (!blocked_future_result.has_value()) {
                return;
            }
            auto blocked_future = blocked_future_result.value();

            if (split->is_finished()) {
                _split_finished(split, split->finished_status());
            } else {
                std::lock_guard<std::mutex> guard(_mutex);
                if (blocked_future.is_done()) {
                    _waiting_splits->offer(split);
                } else {
                    _blocked_splits[split] = blocked_future;

                    _blocked_splits[split].add_callback(
                            [this, split](const Void& value, const Status& status) {
                                if (status.ok()) {
                                    std::lock_guard<std::mutex> guard(_mutex);
                                    _blocked_splits.erase(split);
                                    split->reset_level_priority();
                                    _waiting_splits->offer(split);
                                } else {
                                    LOG(WARNING) << "blocked split is failed, split_id: "
                                                 << split->split_id() << ", status: " << status;
                                    _split_finished(split, status);
                                }
                            });
                }
            }
        }
    });
}

Result<std::shared_ptr<TaskHandle>> TimeSharingTaskExecutor::create_task(
        const TaskId& task_id, std::function<double()> utilization_supplier,
        int initial_split_concurrency, std::chrono::nanoseconds split_concurrency_adjust_frequency,
        std::optional<int> max_concurrency_per_task) {
    auto task_handle = std::make_shared<TimeSharingTaskHandle>(
            task_id, _waiting_splits, utilization_supplier, initial_split_concurrency,
            split_concurrency_adjust_frequency, max_concurrency_per_task);
    RETURN_IF_ERROR_RESULT(task_handle->init());

    std::lock_guard<std::mutex> lock(_mutex);

    _tasks[task_id] = task_handle;

    return task_handle;
}

Status TimeSharingTaskExecutor::add_task(const TaskId& task_id,
                                         std::shared_ptr<TaskHandle> task_handle) {
    std::lock_guard<std::mutex> lock(_mutex);
    _tasks[task_id] =
            std::dynamic_pointer_cast<doris::vectorized::TimeSharingTaskHandle>(task_handle);
    return Status::OK();
}

Status TimeSharingTaskExecutor::remove_task(std::shared_ptr<TaskHandle> task_handle) {
    auto handle = std::dynamic_pointer_cast<TimeSharingTaskHandle>(task_handle);
    std::vector<std::shared_ptr<PrioritizedSplitRunner>> splits_to_destroy;

    {
        std::lock_guard<std::mutex> guard(_mutex);
        auto it = _tasks.find(handle->task_id());
        if (it == _tasks.end() || handle->is_closed()) {
            return Status::OK();
        }
        _tasks.erase(it);

        // Task is already closed
        if (task_handle->is_closed()) {
            return Status::OK();
        }

        splits_to_destroy = handle->close();

        for (const auto& split : splits_to_destroy) {
            _all_splits.erase(split);
            _intermediate_splits.erase(split);
            _blocked_splits.erase(split);
        }
        _waiting_splits->remove_all(splits_to_destroy);
        _record_leaf_splits_size(guard);
    }

    // call destroy outside of synchronized block as it is expensive and doesn't need a lock on the task executor
    for (auto& split : splits_to_destroy) {
        split->close(Status::OK());
    }

    // record completed stats
    int64_t thread_usage_nanos = handle->scheduled_nanos();
    int level = MultilevelSplitQueue::compute_level(thread_usage_nanos);
    _completed_tasks_per_level[level]++;

    if (splits_to_destroy.empty()) {
        return Status::OK();
    }

    // replace blocked splits that were terminated
    {
        std::lock_guard<std::mutex> guard(_mutex);
        _add_new_entrants(guard);
        _record_leaf_splits_size(guard);
    }
    return Status::OK();
}

Result<std::vector<SharedListenableFuture<Void>>> TimeSharingTaskExecutor::enqueue_splits(
        std::shared_ptr<TaskHandle> task_handle, bool intermediate,
        const std::vector<std::shared_ptr<SplitRunner>>& splits) {
    std::vector<std::shared_ptr<PrioritizedSplitRunner>> splits_to_destroy;
    Defer defer {[&]() {
        for (auto& split : splits_to_destroy) {
            split->close(Status::OK());
        }
    }};
    std::vector<SharedListenableFuture<Void>> finished_futures;
    auto handle = std::dynamic_pointer_cast<TimeSharingTaskHandle>(task_handle);
    {
        std::lock_guard<std::mutex> guard(_mutex);
        for (const auto& task_split : splits) {
            TaskId task_id = handle->task_id();
            int split_id = handle->next_split_id();

            auto prioritized_split =
                    PrioritizedSplitRunner::create_shared(handle, split_id, task_split, _ticker);
            RETURN_IF_ERROR_RESULT(prioritized_split->init());
            if (intermediate) {
                if (handle->record_intermediate_split(prioritized_split)) {
                    _start_intermediate_split(prioritized_split, guard);
                } else {
                    splits_to_destroy.push_back(prioritized_split);
                }
            } else {
                if (handle->enqueue_split(prioritized_split)) {
                    _schedule_task_if_necessary(handle, guard);
                    _add_new_entrants(guard);
                } else {
                    splits_to_destroy.push_back(prioritized_split);
                }
            }
            finished_futures.push_back(prioritized_split->finished_future());
        }
        _record_leaf_splits_size(guard);
    }
    return finished_futures;
}

void TimeSharingTaskExecutor::_split_finished(std::shared_ptr<PrioritizedSplitRunner> split,
                                              const Status& status) {
    _completed_splits_per_level[split->priority().level()]++;
    {
        std::lock_guard<std::mutex> guard(_mutex);
        _all_splits.erase(split);

        auto task_handle = split->task_handle();
        task_handle->split_finished(split);

        _schedule_task_if_necessary(task_handle, guard);

        _add_new_entrants(guard);
        _record_leaf_splits_size(guard);
    }
    // call close outside of synchronized block as it is expensive and doesn't need a lock on the task executor
    split->close(status);
}

void TimeSharingTaskExecutor::_schedule_task_if_necessary(
        std::shared_ptr<TimeSharingTaskHandle> task_handle, std::lock_guard<std::mutex>& guard) {
    int guaranteed_concurrency = std::min(
            _guaranteed_concurrency_per_task,
            task_handle->max_concurrency_per_task().value_or(std::numeric_limits<int>::max()));
    int splits_to_schedule = guaranteed_concurrency - task_handle->running_leaf_splits();

    for (int i = 0; i < splits_to_schedule; ++i) {
        auto split = task_handle->poll_next_split();
        if (!split) return;

        _start_split(split, guard);
        auto elapsed_nanos = std::chrono::nanoseconds(
                std::chrono::steady_clock::now().time_since_epoch().count() -
                split->created_nanos());
        _split_queued_time
                << std::chrono::duration_cast<std::chrono::microseconds>(elapsed_nanos).count();
    }
    _record_leaf_splits_size(guard);
}

void TimeSharingTaskExecutor::_add_new_entrants(std::lock_guard<std::mutex>& guard) {
    int running = _all_splits.size() - _intermediate_splits.size();
    for (int i = 0; i < _min_concurrency - running; i++) {
        auto split = _poll_next_split_worker(guard);
        if (!split) {
            break;
        }

        auto elapsed_nanos = std::chrono::nanoseconds(
                std::chrono::steady_clock::now().time_since_epoch().count() -
                split->created_nanos());
        _split_queued_time
                << std::chrono::duration_cast<std::chrono::microseconds>(elapsed_nanos).count();
        _start_split(split, guard);
    }
}

void TimeSharingTaskExecutor::_start_intermediate_split(
        std::shared_ptr<PrioritizedSplitRunner> split, std::lock_guard<std::mutex>& guard) {
    _start_split(split, guard);
    _intermediate_splits.insert(split);
}

void TimeSharingTaskExecutor::_start_split(std::shared_ptr<PrioritizedSplitRunner> split,
                                           std::lock_guard<std::mutex>& guard) {
    _all_splits.insert(split);
    _waiting_splits->offer(split);
}

std::shared_ptr<PrioritizedSplitRunner> TimeSharingTaskExecutor::_poll_next_split_worker(
        std::lock_guard<std::mutex>& guard) {
    // 遍历任务列表，找到第一个能产生分片的任务
    // 然后将该任务移到列表末尾，实现轮询调度
    for (auto it = _tasks.begin(); it != _tasks.end();) {
        auto task = it->second;
        // 跳过已经运行配置的最大驱动数的任务
        if (task->running_leaf_splits() >=
            task->max_concurrency_per_task().value_or(_max_concurrency_per_task)) {
            ++it;
            continue;
        }

        auto split = task->poll_next_split();
        if (split) {
            // 将任务移到列表末尾
            auto task_copy = task;
            auto task_id = it->first;
            it = _tasks.erase(it);
            _tasks[task_id] = task_copy;
            return split;
        }
        ++it;
    }
    return nullptr;
}

void TimeSharingTaskExecutor::_record_leaf_splits_size(std::lock_guard<std::mutex>& guard) {
    //    auto now = _ticker->read();
    //    auto time_difference = now - _last_leaf_splits_size_record_time;
    //
    //    if (time_difference > 0) {
    //        // 记录当前叶子分片的数量和持续时间
    //        //        _leaf_splits_size.add(_last_leaf_splits_size, std::chrono::nanoseconds(time_difference));
    //        _last_leaf_splits_size_record_time = now;
    //    }
    //    // always record new lastLeafSplitsSize as it might have changed
    //    // even if timeDifference is 0
    //    _last_leaf_splits_size = _all_splits.size() - _intermediate_splits.size();
}

void TimeSharingTaskExecutor::_interrupt() {
    std::lock_guard<std::mutex> guard(_mutex);
    _condition.notify_all();
    _waiting_splits->interrupt();
}

int64_t TimeSharingTaskExecutor::_get_running_tasks_for_level(int level) const {
    std::lock_guard<std::mutex> guard(_mutex);
    int64_t count = 0;
    for (const auto& [task_id, task] : _tasks) {
        if (task->priority().level() == level) {
            count++;
        }
    }
    return count;
}

} // namespace vectorized
} // namespace doris
