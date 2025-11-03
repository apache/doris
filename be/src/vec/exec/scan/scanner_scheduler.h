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
class Block;
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

    static void _make_sure_virtual_col_is_materialized(const std::shared_ptr<Scanner>& scanner,
                                                       vectorized::Block* block);

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

// Abstract interface for scan scheduler
class SimplifiedScanScheduler {
public:
    virtual ~SimplifiedScanScheduler() {}

    virtual Status start(int max_thread_num, int min_thread_num, int queue_size) = 0;
    virtual void stop() = 0;
    virtual Status submit_scan_task(SimplifiedScanTask scan_task) = 0;
    virtual Status submit_scan_task(SimplifiedScanTask scan_task,
                                    const std::string& task_id_string) = 0;

    virtual void reset_thread_num(int new_max_thread_num, int new_min_thread_num) = 0;
    virtual void reset_max_thread_num(int thread_num) = 0;
    virtual void reset_min_thread_num(int thread_num) = 0;
    virtual int get_max_threads() = 0;

    virtual int get_queue_size() = 0;
    virtual int get_active_threads() = 0;
    virtual std::vector<int> thread_debug_info() = 0;

    virtual Status schedule_scan_task(std::shared_ptr<ScannerContext> scanner_ctx,
                                      std::shared_ptr<ScanTask> current_scan_task,
                                      std::unique_lock<std::mutex>& transfer_lock) = 0;
};

class ThreadPoolSimplifiedScanScheduler : public SimplifiedScanScheduler {
public:
    ThreadPoolSimplifiedScanScheduler(std::string sched_name,
                                      std::shared_ptr<CgroupCpuCtl> cgroup_cpu_ctl,
                                      std::string workload_group = "system")
            : _is_stop(false),
              _cgroup_cpu_ctl(cgroup_cpu_ctl),
              _sched_name(sched_name),
              _workload_group(workload_group) {}

    virtual ~ThreadPoolSimplifiedScanScheduler() override {
#ifndef BE_TEST
        stop();
#endif
        LOG(INFO) << "Scanner sche " << _sched_name << " shutdown";
    }

    virtual void stop() override {
        _is_stop.store(true);
        _scan_thread_pool->shutdown();
        _scan_thread_pool->wait();
    }

    virtual Status start(int max_thread_num, int min_thread_num, int queue_size) override {
        RETURN_IF_ERROR(ThreadPoolBuilder(_sched_name, _workload_group)
                                .set_min_threads(min_thread_num)
                                .set_max_threads(max_thread_num)
                                .set_max_queue_size(queue_size)
                                .set_cgroup_cpu_ctl(_cgroup_cpu_ctl)
                                .build(&_scan_thread_pool));
        return Status::OK();
    }

    virtual Status submit_scan_task(SimplifiedScanTask scan_task) override {
        if (!_is_stop) {
            return _scan_thread_pool->submit_func([scan_task] { scan_task.scan_func(); });
        } else {
            return Status::InternalError<false>("scanner pool {} is shutdown.", _sched_name);
        }
    }

    virtual Status submit_scan_task(SimplifiedScanTask scan_task,
                                    const std::string& task_id_string) override {
        return submit_scan_task(scan_task);
    }

    virtual void reset_thread_num(int new_max_thread_num, int new_min_thread_num) override {
        int cur_max_thread_num = _scan_thread_pool->max_threads();
        int cur_min_thread_num = _scan_thread_pool->min_threads();
        if (cur_max_thread_num == new_max_thread_num && cur_min_thread_num == new_min_thread_num) {
            return;
        }
        if (new_max_thread_num >= cur_max_thread_num) {
            Status st_max = _scan_thread_pool->set_max_threads(new_max_thread_num);
            if (!st_max.ok()) {
                LOG(WARNING) << "Failed to set max threads for scan thread pool: "
                             << st_max.to_string();
            }
            Status st_min = _scan_thread_pool->set_min_threads(new_min_thread_num);
            if (!st_min.ok()) {
                LOG(WARNING) << "Failed to set min threads for scan thread pool: "
                             << st_min.to_string();
            }
        } else {
            Status st_min = _scan_thread_pool->set_min_threads(new_min_thread_num);
            if (!st_min.ok()) {
                LOG(WARNING) << "Failed to set min threads for scan thread pool: "
                             << st_min.to_string();
            }
            Status st_max = _scan_thread_pool->set_max_threads(new_max_thread_num);
            if (!st_max.ok()) {
                LOG(WARNING) << "Failed to set max threads for scan thread pool: "
                             << st_max.to_string();
            }
        }
    }

    virtual void reset_max_thread_num(int thread_num) override {
        int max_thread_num = _scan_thread_pool->max_threads();

        if (max_thread_num != thread_num) {
            Status st = _scan_thread_pool->set_max_threads(thread_num);
            if (!st.ok()) {
                LOG(INFO) << "reset max thread num failed, sche name=" << _sched_name;
            }
        }
    }

    virtual void reset_min_thread_num(int thread_num) override {
        int min_thread_num = _scan_thread_pool->min_threads();

        if (min_thread_num != thread_num) {
            Status st = _scan_thread_pool->set_min_threads(thread_num);
            if (!st.ok()) {
                LOG(INFO) << "reset min thread num failed, sche name=" << _sched_name;
            }
        }
    }

    virtual int get_queue_size() override { return _scan_thread_pool->get_queue_size(); }

    virtual int get_active_threads() override { return _scan_thread_pool->num_active_threads(); }

    virtual int get_max_threads() override { return _scan_thread_pool->max_threads(); }

    virtual std::vector<int> thread_debug_info() override {
        return _scan_thread_pool->debug_info();
    }

    virtual Status schedule_scan_task(std::shared_ptr<ScannerContext> scanner_ctx,
                                      std::shared_ptr<ScanTask> current_scan_task,
                                      std::unique_lock<std::mutex>& transfer_lock) override;

private:
    std::unique_ptr<ThreadPool> _scan_thread_pool;
    std::atomic<bool> _is_stop;
    std::weak_ptr<CgroupCpuCtl> _cgroup_cpu_ctl;
    std::string _sched_name;
    std::string _workload_group;
    std::shared_mutex _lock;
};

class TaskExecutorSimplifiedScanScheduler : public SimplifiedScanScheduler {
public:
    TaskExecutorSimplifiedScanScheduler(std::string sched_name,
                                        std::shared_ptr<CgroupCpuCtl> cgroup_cpu_ctl,
                                        std::string workload_group = "system")
            : _is_stop(false),
              _cgroup_cpu_ctl(cgroup_cpu_ctl),
              _sched_name(sched_name),
              _workload_group(workload_group) {}

    virtual ~TaskExecutorSimplifiedScanScheduler() override {
#ifndef BE_TEST
        stop();
#endif
        LOG(INFO) << "Scanner sche " << _sched_name << " shutdown";
    }

    virtual void stop() override {
        _is_stop.store(true);
        _task_executor->stop();
    }

    virtual Status start(int max_thread_num, int min_thread_num, int queue_size) override {
        TimeSharingTaskExecutor::ThreadConfig thread_config;
        thread_config.thread_name = _sched_name;
        thread_config.workload_group = _workload_group;
        thread_config.max_thread_num = max_thread_num;
        thread_config.min_thread_num = min_thread_num;
        thread_config.max_queue_size = queue_size;
        thread_config.cgroup_cpu_ctl = _cgroup_cpu_ctl;
        _task_executor = TimeSharingTaskExecutor::create_shared(
                thread_config, max_thread_num * 2, config::task_executor_min_concurrency_per_task,
                config::task_executor_max_concurrency_per_task, std::make_shared<SystemTicker>(),
                nullptr, false);
        RETURN_IF_ERROR(_task_executor->init());
        RETURN_IF_ERROR(_task_executor->start());
        return Status::OK();
    }

    virtual Status submit_scan_task(SimplifiedScanTask scan_task) override {
        if (!_is_stop) {
            std::shared_ptr<SplitRunner> split_runner;
            if (scan_task.scan_task->is_first_schedule) {
                split_runner = std::make_shared<ScannerSplitRunner>("scanner_split_runner",
                                                                    scan_task.scan_func);
                RETURN_IF_ERROR(split_runner->init());
                auto result = _task_executor->enqueue_splits(
                        scan_task.scanner_context->task_handle(), false, {split_runner});
                if (!result.has_value()) {
                    LOG(WARNING) << "enqueue_splits failed: " << result.error();
                    return result.error();
                }
                scan_task.scan_task->is_first_schedule = false;
            } else {
                split_runner = scan_task.scan_task->split_runner.lock();
                if (split_runner == nullptr) {
                    return Status::OK();
                }
                RETURN_IF_ERROR(_task_executor->re_enqueue_split(
                        scan_task.scanner_context->task_handle(), false, split_runner));
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
    virtual Status submit_scan_task(SimplifiedScanTask scan_task,
                                    const std::string& task_id_string) override {
        if (!_is_stop) {
            vectorized::TaskId task_id(task_id_string);
            std::shared_ptr<TaskHandle> task_handle = DORIS_TRY(_task_executor->create_task(
                    task_id, []() { return 0.0; },
                    config::task_executor_initial_max_concurrency_per_task,
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

            auto result = _task_executor->enqueue_splits(task_handle, false, {split_runner});
            if (!result.has_value()) {
                LOG(WARNING) << "enqueue_splits failed: " << result.error();
                return result.error();
            }
            return Status::OK();
        } else {
            return Status::InternalError<false>("scanner pool {} is shutdown.", _sched_name);
        }
    }

    virtual void reset_thread_num(int new_max_thread_num, int new_min_thread_num) override {
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

    virtual void reset_max_thread_num(int thread_num) override {
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

    virtual void reset_min_thread_num(int thread_num) override {
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

    virtual int get_queue_size() override {
        auto task_executor = std::dynamic_pointer_cast<doris::vectorized::TimeSharingTaskExecutor>(
                _task_executor);
        return task_executor->get_queue_size();
    }

    virtual int get_active_threads() override {
        auto task_executor = std::dynamic_pointer_cast<doris::vectorized::TimeSharingTaskExecutor>(
                _task_executor);
        return task_executor->num_active_threads();
    }

    virtual int get_max_threads() override {
        auto task_executor = std::dynamic_pointer_cast<doris::vectorized::TimeSharingTaskExecutor>(
                _task_executor);
        return task_executor->max_threads();
    }

    virtual std::vector<int> thread_debug_info() override {
        auto task_executor = std::dynamic_pointer_cast<doris::vectorized::TimeSharingTaskExecutor>(
                _task_executor);
        return task_executor->debug_info();
    }

    std::shared_ptr<TaskExecutor> task_executor() const { return _task_executor; }

    virtual Status schedule_scan_task(std::shared_ptr<ScannerContext> scanner_ctx,
                                      std::shared_ptr<ScanTask> current_scan_task,
                                      std::unique_lock<std::mutex>& transfer_lock) override;

private:
    std::atomic<bool> _is_stop;
    std::weak_ptr<CgroupCpuCtl> _cgroup_cpu_ctl;
    std::string _sched_name;
    std::string _workload_group;
    std::shared_mutex _lock;
    std::shared_ptr<TaskExecutor> _task_executor = nullptr;
};

} // namespace doris::vectorized
