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
#include <cstdint>
#include <memory>

#include "common/be_mock_util.h"
#include "common/status.h"
#include "util/threadpool.h"
#include "vec/exec/executor/listenable_future.h"
#include "vec/exec/executor/ticker.h"
#include "vec/exec/executor/time_sharing/time_sharing_task_executor.h"
#include "vec/exec/scan/scanner_context.h"

namespace doris {
class ExecEnv;

namespace vectorized {
class Scanner;
} // namespace vectorized

template <typename T>
class BlockingQueue;
} // namespace doris

namespace doris::vectorized {
class ScannerDelegate;
class ScanTask;
class ScannerContext;
class SimplifiedScanScheduler;

// Responsible for the scheduling and execution of all Scanners of a BE node.
// Execution thread pool
//     When a ScannerContext is launched, it will submit the running scanners to this scheduler.
//     The scheduling thread will submit the running scanner and its ScannerContext
//     to the execution thread pool to do the actual scan task.
//     Each Scanner will act as a producer, read the next block and put it into
//     the corresponding block queue.
//     The corresponding ScanNode will act as a consumer to consume blocks from the block queue.
//     After the block is consumed, the unfinished scanner will resubmit to this scheduler.
class ScannerScheduler {
public:
    ScannerScheduler();
    virtual ~ScannerScheduler();

    [[nodiscard]] Status init(ExecEnv* env);

    MOCK_FUNCTION Status submit(std::shared_ptr<ScannerContext> ctx,
                                std::shared_ptr<ScanTask> scan_task);

    void stop();

    int remote_thread_pool_max_thread_num() const { return _remote_thread_pool_max_thread_num; }

    static int get_remote_scan_thread_num();

    static int get_remote_scan_thread_queue_size();

private:
    static void _scanner_scan(std::shared_ptr<ScannerContext> ctx,
                              std::shared_ptr<ScanTask> scan_task);

    // true is the scheduler is closed.
    std::atomic_bool _is_closed = {false};
    bool _is_init = false;
    int _remote_thread_pool_max_thread_num;
};

struct SimplifiedScanTask {
    SimplifiedScanTask() = default;
    SimplifiedScanTask(std::function<bool()> scan_func,
                       std::shared_ptr<vectorized::ScannerContext> scanner_context,
                       std::shared_ptr<ScanTask> scan_task) {
        this->scan_func = scan_func;
        this->scanner_context = scanner_context;
        this->scan_task = scan_task;
    }

    std::function<bool()> scan_func;
    std::shared_ptr<vectorized::ScannerContext> scanner_context = nullptr;
    std::shared_ptr<ScanTask> scan_task = nullptr;
};

class ScannerSplitRunner : public SplitRunner {
public:
    ScannerSplitRunner(std::string name, std::function<bool()> scan_func)
            : _name(std::move(name)), _scan_func(scan_func), _started(false) {}

    Status init() override { return Status::OK(); }

    Result<SharedListenableFuture<Void>> process_for(std::chrono::nanoseconds) override;

    void close(const Status& status) override {}

    std::string get_info() const override { return ""; }

    bool is_finished() override;

    Status finished_status() override;

    bool is_started() const;

    bool is_auto_reschedule() const override;

private:
    std::string _name;
    std::function<bool()> _scan_func;

    std::atomic<bool> _started;
    SharedListenableFuture<Void> _completion_future;
};

class SimplifiedScanScheduler {
public:
    SimplifiedScanScheduler(std::string sched_name, std::shared_ptr<CgroupCpuCtl> cgroup_cpu_ctl,
                            std::string workload_group = "system")
            : _is_stop(false),
              _cgroup_cpu_ctl(cgroup_cpu_ctl),
              _sched_name(sched_name),
              _workload_group(workload_group) {}

    MOCK_FUNCTION ~SimplifiedScanScheduler() {
#ifndef BE_TEST
        stop();
#endif
        LOG(INFO) << "Scanner sche " << _sched_name << " shutdown";
    }

    void stop() {
        _is_stop.store(true);
        _task_executor->stop();
    }

    Status start(int max_thread_num, int min_thread_num, int queue_size) {
        TimeSharingTaskExecutor::ThreadConfig thread_config;
        thread_config.thread_name = _sched_name;
        thread_config.workload_group = _workload_group;
        thread_config.max_thread_num = max_thread_num;
        thread_config.min_thread_num = min_thread_num;
        thread_config.max_queue_size = queue_size;
        thread_config.cgroup_cpu_ctl = _cgroup_cpu_ctl;
        _task_executor = TimeSharingTaskExecutor::create_shared(
                thread_config, max_thread_num * 2, config::task_executor_min_concurrency_per_task,
                config::task_executor_max_concurrency_per_task, std::make_shared<SystemTicker>());
        RETURN_IF_ERROR(_task_executor->init());
        RETURN_IF_ERROR(_task_executor->start());
        return Status::OK();
    }

    Status submit_scan_task(SimplifiedScanTask scan_task) {
        if (!_is_stop) {
            std::shared_ptr<SplitRunner> split_runner;
            if (scan_task.scan_task->is_first_schedule) {
                split_runner = std::make_shared<ScannerSplitRunner>("scanner_split_runner",
                                                                    scan_task.scan_func);
                RETURN_IF_ERROR(split_runner->init());
                _task_executor->enqueue_splits(scan_task.scanner_context->task_handle(), false,
                                               {split_runner});
                scan_task.scan_task->is_first_schedule = false;
            } else {
                split_runner = scan_task.scan_task->split_runner.lock();
                if (split_runner == nullptr) {
                    return Status::OK();
                }
                _task_executor->re_enqueue_split(scan_task.scanner_context->task_handle(), false,
                                                 split_runner);
            }
            scan_task.scan_task->split_runner = split_runner;
            return Status::OK();
        } else {
            return Status::InternalError<false>("scanner pool {} is shutdown.", _sched_name);
        }
    }

    // A task has only one split. When the split is created, the task is created according to the task_id,
    // and the task is automatically removed when the split ends.
    // Now it is only for PInternalService::multiget_data_v2 used by TopN materialization.
    Status submit_scan_task(SimplifiedScanTask scan_task, const std::string& task_id_string) {
        if (!_is_stop) {
            vectorized::TaskId task_id(task_id_string);
            std::shared_ptr<TaskHandle> task_handle = DORIS_TRY(_task_executor->create_task(
                    task_id, []() { return 0.0; }, config::task_executor_initial_split_concurrency,
                    std::chrono::milliseconds(100), std::nullopt));

            auto wrapped_scan_func = [this, task_handle, scan_func = scan_task.scan_func]() {
                bool result = scan_func();
                if (result) {
                    static_cast<void>(_task_executor->remove_task(task_handle));
                }
                return result;
            };

            auto split_runner =
                    std::make_shared<ScannerSplitRunner>("scanner_split_runner", wrapped_scan_func);
            RETURN_IF_ERROR(split_runner->init());

            _task_executor->enqueue_splits(task_handle, false, {split_runner});
            return Status::OK();
        } else {
            return Status::InternalError<false>("scanner pool {} is shutdown.", _sched_name);
        }
    }

    void reset_thread_num(int new_max_thread_num, int new_min_thread_num) {
        auto task_executor = std::dynamic_pointer_cast<doris::vectorized::TimeSharingTaskExecutor>(
                _task_executor);
        int cur_max_thread_num = task_executor->max_threads();
        int cur_min_thread_num = task_executor->min_threads();
        if (cur_max_thread_num == new_max_thread_num && cur_min_thread_num == new_min_thread_num) {
            return;
        }
        if (new_max_thread_num >= cur_max_thread_num) {
            Status st_max = task_executor->set_max_threads(new_max_thread_num);
            if (!st_max.ok()) {
                LOG(WARNING) << "Failed to set max threads for scan thread pool: "
                             << st_max.to_string();
            }
            Status st_min = task_executor->set_min_threads(new_min_thread_num);
            if (!st_min.ok()) {
                LOG(WARNING) << "Failed to set min threads for scan thread pool: "
                             << st_min.to_string();
            }
        } else {
            Status st_min = task_executor->set_min_threads(new_min_thread_num);
            if (!st_min.ok()) {
                LOG(WARNING) << "Failed to set min threads for scan thread pool: "
                             << st_min.to_string();
            }
            Status st_max = task_executor->set_max_threads(new_max_thread_num);
            if (!st_max.ok()) {
                LOG(WARNING) << "Failed to set max threads for scan thread pool: "
                             << st_max.to_string();
            }
        }
    }

    void reset_max_thread_num(int thread_num) {
        auto task_executor = std::dynamic_pointer_cast<doris::vectorized::TimeSharingTaskExecutor>(
                _task_executor);
        int max_thread_num = task_executor->max_threads();

        if (max_thread_num != thread_num) {
            Status st = task_executor->set_max_threads(thread_num);
            if (!st.ok()) {
                LOG(INFO) << "reset max thread num failed, sche name=" << _sched_name;
            }
        }
    }

    void reset_min_thread_num(int thread_num) {
        auto task_executor = std::dynamic_pointer_cast<doris::vectorized::TimeSharingTaskExecutor>(
                _task_executor);
        int min_thread_num = task_executor->min_threads();

        if (min_thread_num != thread_num) {
            Status st = task_executor->set_min_threads(thread_num);
            if (!st.ok()) {
                LOG(INFO) << "reset min thread num failed, sche name=" << _sched_name;
            }
        }
    }

    MOCK_FUNCTION int get_queue_size() {
        auto task_executor = std::dynamic_pointer_cast<doris::vectorized::TimeSharingTaskExecutor>(
                _task_executor);
        return task_executor->get_queue_size();
    }

    MOCK_FUNCTION int get_active_threads() {
        auto task_executor = std::dynamic_pointer_cast<doris::vectorized::TimeSharingTaskExecutor>(
                _task_executor);
        return task_executor->num_active_threads();
    }

    int get_max_threads() {
        auto task_executor = std::dynamic_pointer_cast<doris::vectorized::TimeSharingTaskExecutor>(
                _task_executor);
        return task_executor->max_threads();
    }

    std::vector<int> thread_debug_info() {
        auto task_executor = std::dynamic_pointer_cast<doris::vectorized::TimeSharingTaskExecutor>(
                _task_executor);
        return task_executor->debug_info();
    }

    std::shared_ptr<TaskExecutor> task_executor() const { return _task_executor; }

    MOCK_FUNCTION Status schedule_scan_task(std::shared_ptr<ScannerContext> scanner_ctx,
                                            std::shared_ptr<ScanTask> current_scan_task,
                                            std::unique_lock<std::mutex>& transfer_lock);

private:
    std::atomic<bool> _is_stop;
    std::weak_ptr<CgroupCpuCtl> _cgroup_cpu_ctl;
    std::string _sched_name;
    std::string _workload_group;
    std::shared_mutex _lock;
    std::shared_ptr<TaskExecutor> _task_executor = nullptr;
};

} // namespace doris::vectorized
