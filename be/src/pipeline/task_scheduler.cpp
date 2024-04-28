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
#include "pipeline/pipeline_x/pipeline_x_task.h"
#include "pipeline/task_queue.h"
#include "pipeline_fragment_context.h"
#include "runtime/exec_env.h"
#include "runtime/query_context.h"
#include "util/debug_util.h"
#include "util/sse_util.hpp"
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
    // Must be mutil number of cpu cores
    RETURN_IF_ERROR(ThreadPoolBuilder(_name)
                            .set_min_threads(cores)
                            .set_max_threads(cores)
                            .set_max_queue_size(0)
                            .set_cgroup_cpu_ctl(_cgroup_cpu_ctl)
                            .build(&_fix_thread_pool));
    _markers.reserve(cores);
    for (size_t i = 0; i < cores; ++i) {
        _markers.push_back(std::make_unique<std::atomic<bool>>(true));
        RETURN_IF_ERROR(_fix_thread_pool->submit_func([this, i] { _do_work(i); }));
    }
    return Status::OK();
}

Status TaskScheduler::schedule_task(PipelineTask* task) {
    return _task_queue->push_back(task);
    // TODO control num of task
}

// after _close_task, task maybe destructed.
void _close_task(PipelineTask* task, PipelineTaskState state, Status exec_status) {
    // Has to attach memory tracker here, because the close task will also release some memory.
    // Should count the memory to the query or the query's memory will not decrease when part of
    // task finished.
    SCOPED_ATTACH_TASK(task->runtime_state());
    // close_a_pipeline may delete fragment context and will core in some defer
    // code, because the defer code will access fragment context it self.
    auto lock_for_context = task->fragment_context()->shared_from_this();
    // is_pending_finish does not check status, so has to check status in close API.
    // For example, in async writer, the writer may failed during dealing with eos_block
    // but it does not return error status. Has to check the error status in close API.
    // We have already refactor all source and sink api, the close API does not need waiting
    // for pending finish now. So that could call close directly.
    Status status = task->close(exec_status);
    if (!status.ok() && state != PipelineTaskState::CANCELED) {
        task->fragment_context()->cancel(PPlanFragmentCancelReason::INTERNAL_ERROR,
                                         std::string(status.msg()));
        state = PipelineTaskState::CANCELED;
    }
    task->set_state(state);
    task->set_close_pipeline_time();
    task->finalize();
    task->set_running(false);
    task->fragment_context()->close_a_pipeline();
}

void TaskScheduler::_do_work(size_t index) {
    const auto& marker = _markers[index];
    while (*marker) {
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

        auto state = task->get_state();
        // If the state is PENDING_FINISH, then the task is come from blocked queue, its is_pending_finish
        // has to return false. The task is finished and need to close now.
        if (state == PipelineTaskState::PENDING_FINISH) {
            Status exec_status = fragment_ctx->get_query_ctx()->exec_status();
            _close_task(task, canceled ? PipelineTaskState::CANCELED : PipelineTaskState::FINISHED,
                        exec_status);
            continue;
        }

        DCHECK(state != PipelineTaskState::FINISHED && state != PipelineTaskState::CANCELED)
                << "task already finish: " << task->debug_string();

        if (canceled) {
            // may change from pending FINISH，should called cancel
            // also may change form BLOCK, other task called cancel

            // If pipeline is canceled, it will report after pipeline closed, and will propagate
            // errors to downstream through exchange. So, here we needn't send_report.
            // fragment_ctx->send_report(true);
            Status cancel_status = fragment_ctx->get_query_ctx()->exec_status();
            _close_task(task, PipelineTaskState::CANCELED, cancel_status);
            continue;
        }

        task->set_state(PipelineTaskState::RUNNABLE);

        // task exec
        bool eos = false;
        auto status = Status::OK();

        try {
            // This will enable exception handling logic in allocator.h when memory allocate
            // failed or sysem memory is not sufficient.
            doris::enable_thread_catch_bad_alloc++;
            Defer defer {[&]() { doris::enable_thread_catch_bad_alloc--; }};
            //TODO: use a better enclose to abstracting these
            if (ExecEnv::GetInstance()->pipeline_tracer_context()->enabled()) {
                TUniqueId query_id = task->query_context()->query_id();
                std::string task_name = task->task_name();
#ifdef __APPLE__
                uint32_t core_id = 0;
#else
                uint32_t core_id = sched_getcpu();
#endif
                std::thread::id tid = std::this_thread::get_id();
                uint64_t thread_id = *reinterpret_cast<uint64_t*>(&tid);
                uint64_t start_time = MonotonicMicros();

                status = task->execute(&eos);

                uint64_t end_time = MonotonicMicros();
                auto state = task->get_state();
                std::string state_name = get_state_name(state);
                ExecEnv::GetInstance()->pipeline_tracer_context()->record(
                        {query_id, task_name, core_id, thread_id, start_time, end_time,
                         state_name});
            } else {
                status = task->execute(&eos);
            }
        } catch (const Exception& e) {
            status = e.to_status();
        }

        task->set_previous_core_id(index);

        if (status.is<ErrorCode::END_OF_FILE>()) {
            // Sink operator finished, just close task now.
            _close_task(task, PipelineTaskState::FINISHED, Status::OK());
            continue;
        } else if (!status.ok()) {
            task->set_eos_time();
            LOG(WARNING) << fmt::format("Pipeline task failed. query_id: {} reason: {}",
                                        print_id(task->query_context()->query_id()),
                                        status.to_string());
            // Print detail informations below when you debugging here.
            //
            // LOG(WARNING)<< "task:\n"<<task->debug_string();

            // exec failed，cancel all fragment instance
            fragment_ctx->cancel(PPlanFragmentCancelReason::INTERNAL_ERROR,
                                 std::string(status.to_string_no_stack()));
            _close_task(task, PipelineTaskState::CANCELED, status);
            continue;
        }
        fragment_ctx->trigger_report_if_necessary();

        if (eos) {
            task->set_eos_time();
            // TODO: pipeline parallel need to wait the last task finish to call finalize
            //  and find_p_dependency
            VLOG_DEBUG << fmt::format("Try close task: {}, fragment_ctx->is_canceled(): {}",
                                      print_id(task->query_context()->query_id()),
                                      fragment_ctx->is_canceled());
            // is pending finish will add the task to dependency's blocking queue, and then the task will be
            // added to running queue when dependency is ready.
            if (task->is_pending_finish()) {
                // Only meet eos, should set task to PENDING_FINISH state
                task->set_state(PipelineTaskState::PENDING_FINISH);
                task->set_running(false);
            } else {
                // Close the task directly?
                Status exec_status = fragment_ctx->get_query_ctx()->exec_status();
                _close_task(task,
                            canceled ? PipelineTaskState::CANCELED : PipelineTaskState::FINISHED,
                            exec_status);
            }
            continue;
        }

        auto pipeline_state = task->get_state();
        switch (pipeline_state) {
        case PipelineTaskState::BLOCKED_FOR_SOURCE:
        case PipelineTaskState::BLOCKED_FOR_SINK:
        case PipelineTaskState::BLOCKED_FOR_RF:
        case PipelineTaskState::BLOCKED_FOR_DEPENDENCY:
            task->set_running(false);
            break;
        case PipelineTaskState::RUNNABLE:
            task->set_running(false);
            static_cast<void>(_task_queue->push_back(task, index));
            break;
        default:
            DCHECK(false) << "error state after run task, " << get_state_name(pipeline_state)
                          << " task: " << task->debug_string();
            break;
        }
    }
}

void TaskScheduler::stop() {
    if (!this->_shutdown.load()) {
        if (_task_queue) {
            _task_queue->close();
        }
        if (_fix_thread_pool) {
            for (const auto& marker : _markers) {
                marker->store(false);
            }
            _fix_thread_pool->shutdown();
            _fix_thread_pool->wait();
        }
        // Should set at the ending of the stop to ensure that the
        // pool is stopped. For example, if there are 2 threads call stop
        // then if one thread set shutdown = false, then another thread will
        // not check it and will free task scheduler.
        this->_shutdown.store(true);
    }
}

} // namespace doris::pipeline
