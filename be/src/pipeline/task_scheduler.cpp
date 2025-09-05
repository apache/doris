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

#include "task_scheduler.h"

#include <fmt/format.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/types.pb.h>
#include <glog/logging.h>
#include <sched.h>

// IWYU pragma: no_include <bits/chrono.h>
#include <algorithm>
#include <chrono> // IWYU pragma: keep
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <thread>
#include <utility>

#include "common/logging.h"
#include "common/status.h"
#include "pipeline/pipeline_task.h"
#include "pipeline_fragment_context.h"
#include "runtime/exec_env.h"
#include "runtime/query_context.h"
#include "runtime/thread_context.h"
#include "util/thread.h"
#include "util/threadpool.h"
#include "util/time.h"
#include "util/uid_util.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"
TaskScheduler::~TaskScheduler() {
    stop();
    LOG(INFO) << "Task scheduler " << _name << " shutdown";
}

Status TaskScheduler::start() {
    RETURN_IF_ERROR(ThreadPoolBuilder(_name)
                            .set_min_threads(_num_threads)
                            .set_max_threads(_num_threads)
                            .set_max_queue_size(0)
                            .set_cgroup_cpu_ctl(_cgroup_cpu_ctl)
                            .build(&_fix_thread_pool));
    LOG_INFO("TaskScheduler set cores").tag("size", _num_threads);
    for (int32_t i = 0; i < _num_threads; ++i) {
        RETURN_IF_ERROR(_fix_thread_pool->submit_func([this, i] { _do_work(i); }));
    }
    return Status::OK();
}

Status TaskScheduler::submit(PipelineTaskSPtr task) {
    return _task_queue.push_back(task);
}

// after close_task, task maybe destructed.
void close_task(PipelineTask* task, Status exec_status, PipelineFragmentContext* ctx) {
    // Has to attach memory tracker here, because the close task will also release some memory.
    // Should count the memory to the query or the query's memory will not decrease when part of
    // task finished.
    SCOPED_ATTACH_TASK(task->runtime_state());
    if (!exec_status.ok()) {
        ctx->cancel(exec_status);
        LOG(WARNING) << fmt::format("Pipeline task failed. query_id: {} reason: {}",
                                    print_id(ctx->get_query_id()), exec_status.to_string());
    }
    Status status = task->close(exec_status);
    if (!status.ok()) {
        ctx->cancel(status);
    }
    status = task->finalize();
    if (!status.ok()) {
        ctx->cancel(status);
    }
}

void TaskScheduler::_do_work(int index) {
    while (!_need_to_stop) {
        auto task = _task_queue.take(index);
        if (!task) {
            continue;
        }

        // The task is already running, maybe block in now dependency wake up by other thread
        // but the block thread still hold the task, so put it back to the queue, until the hold
        // thread set task->set_running(false)
        if (task->is_running()) {
            static_cast<void>(_task_queue.push_back(task, index));
            continue;
        }
        if (task->is_finalized()) {
            continue;
        }
        auto fragment_context = task->fragment_context().lock();
        if (!fragment_context) {
            // Fragment already finished
            continue;
        }
        task->set_running(true);
        DCHECK_EQ(task->get_thread_id(), index);
        bool done = false;
        auto status = Status::OK();
        int64_t exec_ns = 0;
        SCOPED_RAW_TIMER(&exec_ns);
        Defer task_running_defer {[&]() {
            // If fragment is finished, fragment context will be de-constructed with all tasks in it.
            if (done || !status.ok()) {
                auto id = task->pipeline_id();
                close_task(task.get(), status, fragment_context.get());
                task->set_running(false);
                fragment_context->decrement_running_task(id);
            } else {
                task->set_running(false);
            }
            _task_queue.update_statistics(task.get(), exec_ns);
        }};
        bool canceled = fragment_context->is_canceled();

        // Close task if canceled
        if (canceled) {
            status = fragment_context->get_query_ctx()->exec_status();
            DCHECK(!status.ok());
            continue;
        }

        // Main logics of execution
        ASSIGN_STATUS_IF_CATCH_EXCEPTION(
                //TODO: use a better enclose to abstracting these
                if (ExecEnv::GetInstance()->pipeline_tracer_context()->enabled()) {
                    TUniqueId query_id = fragment_context->get_query_id();
                    std::string task_name = task->task_name();

                    std::thread::id tid = std::this_thread::get_id();
                    uint64_t thread_id = *reinterpret_cast<uint64_t*>(&tid);
                    uint64_t start_time = MonotonicMicros();

                    status = task->execute(&done);

                    uint64_t end_time = MonotonicMicros();
                    ExecEnv::GetInstance()->pipeline_tracer_context()->record(
                            {query_id, task_name, static_cast<uint32_t>(index), thread_id,
                             start_time, end_time});
                } else { status = task->execute(&done); },
                status);
        fragment_context->trigger_report_if_necessary();
    }
}

void TaskScheduler::stop() {
    if (!_shutdown) {
        _task_queue.close();
        if (_fix_thread_pool) {
            _need_to_stop = true;
            _fix_thread_pool->shutdown();
            _fix_thread_pool->wait();
        }
        // Should set at the ending of the stop to ensure that the
        // pool is stopped. For example, if there are 2 threads call stop
        // then if one thread set shutdown = false, then another thread will
        // not check it and will free task scheduler.
        _shutdown = true;
    }
}

Status HybridTaskScheduler::submit(PipelineTaskSPtr task) {
    if (task->is_blockable()) {
        task->set_on_blocking_scheduler(true);
        return _blocking_scheduler.submit(task);
    } else {
        task->set_on_blocking_scheduler(false);
        return _simple_scheduler.submit(task);
    }
}

Status HybridTaskScheduler::start() {
    RETURN_IF_ERROR(_blocking_scheduler.start());
    RETURN_IF_ERROR(_simple_scheduler.start());
    return Status::OK();
}

void HybridTaskScheduler::stop() {
    _blocking_scheduler.stop();
    _simple_scheduler.stop();
}

} // namespace doris::pipeline
