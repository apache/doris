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

#include "common/status.h"
#include "util/threadpool.h"
#include "vec/exec/scan/vscanner.h"

namespace doris {
class ExecEnv;

namespace vectorized {
class VScanner;
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
    ~ScannerScheduler();

    [[nodiscard]] Status init(ExecEnv* env);

    void submit(std::shared_ptr<ScannerContext> ctx, std::shared_ptr<ScanTask> scan_task);

    void stop();

    std::unique_ptr<ThreadPoolToken> new_limited_scan_pool_token(ThreadPool::ExecutionMode mode,
                                                                 int max_concurrency);

    int remote_thread_pool_max_thread_num() const { return _remote_thread_pool_max_thread_num; }

    static int get_remote_scan_thread_num();

    static int get_remote_scan_thread_queue_size();

private:
    static void _scanner_scan(std::shared_ptr<ScannerContext> ctx,
                              std::shared_ptr<ScanTask> scan_task);

    void _register_metrics();

    static void _deregister_metrics();

    // execution thread pool
    // _local_scan_thread_pool is for local scan task(typically, olap scanner)
    // _remote_scan_thread_pool is for remote scan task(cold data on s3, hdfs, etc.)
    // _limited_scan_thread_pool is a special pool for queries with resource limit
    std::unique_ptr<vectorized::SimplifiedScanScheduler> _local_scan_thread_pool;
    std::unique_ptr<vectorized::SimplifiedScanScheduler> _remote_scan_thread_pool;
    std::unique_ptr<ThreadPool> _limited_scan_thread_pool;

    // true is the scheduler is closed.
    std::atomic_bool _is_closed = {false};
    bool _is_init = false;
    int _remote_thread_pool_max_thread_num;
};

struct SimplifiedScanTask {
    SimplifiedScanTask() = default;
    SimplifiedScanTask(std::function<void()> scan_func,
                       std::shared_ptr<vectorized::ScannerContext> scanner_context) {
        this->scan_func = scan_func;
        this->scanner_context = scanner_context;
    }

    std::function<void()> scan_func;
    std::shared_ptr<vectorized::ScannerContext> scanner_context = nullptr;
};

class SimplifiedScanScheduler {
public:
    SimplifiedScanScheduler(std::string sched_name, CgroupCpuCtl* cgroup_cpu_ctl) {
        _is_stop.store(false);
        _cgroup_cpu_ctl = cgroup_cpu_ctl;
        _sched_name = sched_name;
    }

    ~SimplifiedScanScheduler() {
        stop();
        LOG(INFO) << "Scanner sche " << _sched_name << " shutdown";
    }

    void stop() {
        _is_stop.store(true);
        _scan_thread_pool->shutdown();
        _scan_thread_pool->wait();
    }

    Status start(int max_thread_num, int min_thread_num, int queue_size) {
        RETURN_IF_ERROR(ThreadPoolBuilder(_sched_name)
                                .set_min_threads(min_thread_num)
                                .set_max_threads(max_thread_num)
                                .set_max_queue_size(queue_size)
                                .set_cgroup_cpu_ctl(_cgroup_cpu_ctl)
                                .build(&_scan_thread_pool));
        return Status::OK();
    }

    Status submit_scan_task(SimplifiedScanTask scan_task) {
        if (!_is_stop) {
            return _scan_thread_pool->submit_func([scan_task] { scan_task.scan_func(); });
        } else {
            return Status::InternalError<false>("scanner pool {} is shutdown.", _sched_name);
        }
    }

    void reset_thread_num(int new_max_thread_num, int new_min_thread_num) {
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

    void reset_max_thread_num(int thread_num) {
        int max_thread_num = _scan_thread_pool->max_threads();

        if (max_thread_num != thread_num) {
            Status st = _scan_thread_pool->set_max_threads(thread_num);
            if (!st.ok()) {
                LOG(INFO) << "reset max thread num failed, sche name=" << _sched_name;
            }
        }
    }

    void reset_min_thread_num(int thread_num) {
        int min_thread_num = _scan_thread_pool->min_threads();

        if (min_thread_num != thread_num) {
            Status st = _scan_thread_pool->set_min_threads(thread_num);
            if (!st.ok()) {
                LOG(INFO) << "reset min thread num failed, sche name=" << _sched_name;
            }
        }
    }

    int get_queue_size() { return _scan_thread_pool->get_queue_size(); }

    int get_active_threads() { return _scan_thread_pool->num_active_threads(); }

    std::vector<int> thread_debug_info() { return _scan_thread_pool->debug_info(); }

private:
    std::unique_ptr<ThreadPool> _scan_thread_pool;
    std::atomic<bool> _is_stop;
    CgroupCpuCtl* _cgroup_cpu_ctl = nullptr;
    std::string _sched_name;
};

} // namespace doris::vectorized
