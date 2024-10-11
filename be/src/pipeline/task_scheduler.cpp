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
#include <chrono> // IWYU pragma: keep
#include <functional>
#include <ostream>
#include <string>
#include <thread>
#include <utility>

#include "common/logging.h"
#include "pipeline/pipeline_task.h"
#include "pipeline/task_queue.h"
#include "pipeline_fragment_context.h"
#include "runtime/exec_env.h"
#include "runtime/query_context.h"
#include "util/thread.h"
#include "util/threadpool.h"
#include "util/time.h"
#include "util/uid_util.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::pipeline {

TaskScheduler::~TaskScheduler() {
    stop();
    LOG(INFO) << "Task scheduler " << _name << " shutdown";
}

Status TaskScheduler::start() {
    int cores = _task_queue->cores();
    RETURN_IF_ERROR(ThreadPoolBuilder(_name)
                            .set_min_threads(cores)
                            .set_max_threads(cores)
                            .set_max_queue_size(0)
                            .set_cgroup_cpu_ctl(_cgroup_cpu_ctl)
                            .build(&_fix_thread_pool));
    LOG_INFO("TaskScheduler set cores").tag("size", cores);
    _markers.resize(cores, true);
    for (size_t i = 0; i < cores; ++i) {
        RETURN_IF_ERROR(_fix_thread_pool->submit_func([this, i] { _do_work(i); }));
    }
    return Status::OK();
}

Status TaskScheduler::schedule_task(PipelineTask* task) {
    return _task_queue->push_back(task);
}

// after _close_task, task maybe destructed.
void _close_task(PipelineTask* task, Status exec_status) {
    // Has to attach memory tracker here, because the close task will also release some memory.
    // Should count the memory to the query or the query's memory will not decrease when part of
    // task finished.
    SCOPED_ATTACH_TASK(task->runtime_state());
    if (task->is_finalized()) {
        task->set_running(false);
        return;
    }
    // close_a_pipeline may delete fragment context and will core in some defer
    // code, because the defer code will access fragment context it self.
    auto lock_for_context = task->fragment_context()->shared_from_this();
    // is_pending_finish does not check status, so has to check status in close API.
    // For example, in async writer, the writer may failed during dealing with eos_block
    // but it does not return error status. Has to check the error status in close API.
    // We have already refactor all source and sink api, the close API does not need waiting
    // for pending finish now. So that could call close directly.
    Status status = task->close(exec_status);
    if (!status.ok()) {
        task->fragment_context()->cancel(status);
    }
    task->finalize();
    task->set_running(false);
    task->fragment_context()->close_a_pipeline();
}

void TaskScheduler::_do_work(size_t index) {
    while (_markers[index]) {
        auto* task = _task_queue->take(index);
        if (!task) {
            continue;
        }
        if (task->is_running()) {
            static_cast<void>(_task_queue->push_back(task, index));
            continue;
        }
        task->log_detail_if_need();
        task->set_running(true);
        task->set_task_queue(_task_queue.get());
        auto* fragment_ctx = task->fragment_context();
        bool canceled = fragment_ctx->is_canceled();

        // If the state is PENDING_FINISH, then the task is come from blocked queue, its is_pending_finish
        // has to return false. The task is finished and need to close now.
        if (canceled) {
            // may change from pending FINISH，should called cancel
            // also may change form BLOCK, other task called cancel

            // If pipeline is canceled, it will report after pipeline closed, and will propagate
            // errors to downstream through exchange. So, here we needn't send_report.
            // fragment_ctx->send_report(true);
            _close_task(task, fragment_ctx->get_query_ctx()->exec_status());
            continue;
        }

        // task exec
        bool eos = false;
        auto status = Status::OK();

#ifdef __APPLE__
        uint32_t core_id = 0;
#else
        uint32_t core_id = sched_getcpu();
#endif
        ASSIGN_STATUS_IF_CATCH_EXCEPTION(
                //TODO: use a better enclose to abstracting these
                if (ExecEnv::GetInstance()->pipeline_tracer_context()->enabled()) {
                    TUniqueId query_id = task->query_context()->query_id();
                    std::string task_name = task->task_name();

                    std::thread::id tid = std::this_thread::get_id();
                    uint64_t thread_id = *reinterpret_cast<uint64_t*>(&tid);
                    uint64_t start_time = MonotonicMicros();

                    status = task->execute(&eos);

                    uint64_t end_time = MonotonicMicros();
                    ExecEnv::GetInstance()->pipeline_tracer_context()->record(
                            {query_id, task_name, core_id, thread_id, start_time, end_time});
                } else { status = task->execute(&eos); },
                status);

        task->set_previous_core_id(index);

        if (!status.ok()) {
            // Print detail informations below when you debugging here.
            //
            // LOG(WARNING)<< "task:\n"<<task->debug_string();

            // exec failed，cancel all fragment instance
            fragment_ctx->cancel(status);
            LOG(WARNING) << fmt::format("Pipeline task failed. query_id: {} reason: {}",
                                        print_id(task->query_context()->query_id()),
                                        status.to_string());
            _close_task(task, status);
            continue;
        }
        fragment_ctx->trigger_report_if_necessary();

        if (eos) {
            // is pending finish will add the task to dependency's blocking queue, and then the task will be
            // added to running queue when dependency is ready.
            if (task->is_pending_finish()) {
                // Only meet eos, should set task to PENDING_FINISH state
                task->set_running(false);
            } else {
                Status exec_status = fragment_ctx->get_query_ctx()->exec_status();
                _close_task(task, exec_status);
            }
            continue;
        }

        task->set_running(false);
    }
}

void TaskScheduler::stop() {
    if (!_shutdown) {
        if (_task_queue) {
            _task_queue->close();
        }
        if (_fix_thread_pool) {
            for (size_t i = 0; i < _markers.size(); ++i) {
                _markers[i] = false;
            }
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

} // namespace doris::pipeline
