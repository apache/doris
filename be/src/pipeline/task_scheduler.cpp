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

#include <algorithm>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <functional>
#include <ostream>
#include <string>
#include <thread>

#include "common/logging.h"
#include "common/signal_handler.h"
#include "pipeline/pipeline_task.h"
#include "pipeline/pipeline_x/dependency.h"
#include "pipeline/pipeline_x/pipeline_x_task.h"
#include "pipeline/task_queue.h"
#include "pipeline_fragment_context.h"
#include "runtime/query_context.h"
#include "util/debug_util.h"
#include "util/sse_util.hpp"
#include "util/thread.h"
#include "util/threadpool.h"
#include "util/uid_util.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::pipeline {

BlockedTaskScheduler::BlockedTaskScheduler(std::string name)
        : _name(name), _started(false), _shutdown(false) {}

Status BlockedTaskScheduler::start() {
    LOG(INFO) << "BlockedTaskScheduler start";
    RETURN_IF_ERROR(Thread::create(
            "BlockedTaskScheduler", _name, [this]() { this->_schedule(); }, &_thread));
    while (!this->_started.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    LOG(INFO) << "BlockedTaskScheduler started";
    return Status::OK();
}

void BlockedTaskScheduler::shutdown() {
    LOG(INFO) << "Start shutdown BlockedTaskScheduler";
    if (!this->_shutdown) {
        this->_shutdown = true;
        if (_thread) {
            _task_cond.notify_one();
            _thread->join();
        }
    }
}

Status BlockedTaskScheduler::add_blocked_task(PipelineTask* task) {
    if (this->_shutdown) {
        return Status::InternalError("BlockedTaskScheduler shutdown");
    }
    std::unique_lock<std::mutex> lock(_task_mutex);
    if (task->is_pipelineX()) {
        // put this task into current dependency's blocking queue and wait for event notification
        // instead of using a separate BlockedTaskScheduler.
        task->set_running(false);
        return Status::OK();
    }
    _blocked_tasks.push_back(task);
    _task_cond.notify_one();
    task->set_running(false);
    return Status::OK();
}

void BlockedTaskScheduler::_schedule() {
    _started.store(true);
    std::list<PipelineTask*> local_blocked_tasks;
    int empty_times = 0;

    while (!_shutdown) {
        {
            std::unique_lock<std::mutex> lock(this->_task_mutex);
            local_blocked_tasks.splice(local_blocked_tasks.end(), _blocked_tasks);
            if (local_blocked_tasks.empty()) {
                while (!_shutdown.load() && _blocked_tasks.empty()) {
                    _task_cond.wait_for(lock, std::chrono::milliseconds(10));
                }

                if (_shutdown.load()) {
                    break;
                }

                DCHECK(!_blocked_tasks.empty());
                local_blocked_tasks.splice(local_blocked_tasks.end(), _blocked_tasks);
            }
        }

        auto origin_local_block_tasks_size = local_blocked_tasks.size();
        auto iter = local_blocked_tasks.begin();
        VecDateTimeValue now = VecDateTimeValue::local_time();
        while (iter != local_blocked_tasks.end()) {
            auto* task = *iter;
            auto state = task->get_state();
            if (state == PipelineTaskState::PENDING_FINISH) {
                // should cancel or should finish
                if (task->is_pending_finish()) {
                    VLOG_DEBUG << "Task pending" << task->debug_string();
                    iter++;
                } else {
                    _make_task_run(local_blocked_tasks, iter, PipelineTaskState::PENDING_FINISH);
                }
            } else if (task->query_context()->is_cancelled()) {
                _make_task_run(local_blocked_tasks, iter);
            } else if (task->query_context()->is_timeout(now)) {
                LOG(WARNING) << "Timeout, query_id=" << print_id(task->query_context()->query_id())
                             << ", instance_id=" << print_id(task->instance_id())
                             << ", task info: " << task->debug_string();

                task->query_context()->cancel(true, "", Status::Cancelled(""));
                _make_task_run(local_blocked_tasks, iter);
            } else if (state == PipelineTaskState::BLOCKED_FOR_DEPENDENCY) {
                if (task->has_dependency()) {
                    iter++;
                } else {
                    _make_task_run(local_blocked_tasks, iter);
                }
            } else if (state == PipelineTaskState::BLOCKED_FOR_SOURCE) {
                if (task->source_can_read()) {
                    _make_task_run(local_blocked_tasks, iter);
                } else {
                    iter++;
                }
            } else if (state == PipelineTaskState::BLOCKED_FOR_RF) {
                if (task->runtime_filters_are_ready_or_timeout()) {
                    _make_task_run(local_blocked_tasks, iter);
                } else {
                    iter++;
                }
            } else if (state == PipelineTaskState::BLOCKED_FOR_SINK) {
                if (task->sink_can_write()) {
                    _make_task_run(local_blocked_tasks, iter);
                } else {
                    iter++;
                }
            } else {
                // TODO: DCHECK the state
                _make_task_run(local_blocked_tasks, iter);
            }
        }

        if (origin_local_block_tasks_size == 0 ||
            local_blocked_tasks.size() == origin_local_block_tasks_size) {
            empty_times += 1;
        } else {
            empty_times = 0;
        }

        if (empty_times != 0 && (empty_times & (EMPTY_TIMES_TO_YIELD - 1)) == 0) {
#ifdef __x86_64__
            _mm_pause();
#else
            sched_yield();
#endif
        }
        if (empty_times == EMPTY_TIMES_TO_YIELD * 10) {
            empty_times = 0;
            sched_yield();
        }
    }
    LOG(INFO) << "BlockedTaskScheduler schedule thread stop";
}

void BlockedTaskScheduler::_make_task_run(std::list<PipelineTask*>& local_tasks,
                                          std::list<PipelineTask*>::iterator& task_itr,
                                          PipelineTaskState t_state) {
    auto task = *task_itr;
    task->set_state(t_state);
    local_tasks.erase(task_itr++);
    static_cast<void>(task->get_task_queue()->push_back(task));
}

TaskScheduler::~TaskScheduler() {
    stop();
    LOG(INFO) << "Task scheduler " << _name << " shutdown";
}

Status TaskScheduler::start() {
    int cores = _task_queue->cores();
    // Must be mutil number of cpu cores
    static_cast<void>(ThreadPoolBuilder(_name)
                              .set_min_threads(cores)
                              .set_max_threads(cores)
                              .set_max_queue_size(0)
                              .set_cgroup_cpu_ctl(_cgroup_cpu_ctl)
                              .build(&_fix_thread_pool));
    _markers.reserve(cores);
    for (size_t i = 0; i < cores; ++i) {
        _markers.push_back(std::make_unique<std::atomic<bool>>(true));
        RETURN_IF_ERROR(
                _fix_thread_pool->submit_func(std::bind(&TaskScheduler::_do_work, this, i)));
    }
    return Status::OK();
}

Status TaskScheduler::schedule_task(PipelineTask* task) {
    return _task_queue->push_back(task);
    // TODO control num of task
}

void TaskScheduler::_do_work(size_t index) {
    const auto& marker = _markers[index];
    while (*marker) {
        auto* task = _task_queue->take(index);
        if (!task) {
            continue;
        }
        if (task->is_pipelineX() && task->is_running()) {
            static_cast<void>(_task_queue->push_back(task, index));
            continue;
        }
        task->set_running(true);
        task->set_task_queue(_task_queue.get());
        auto* fragment_ctx = task->fragment_context();
        signal::query_id_hi = fragment_ctx->get_query_id().hi;
        signal::query_id_lo = fragment_ctx->get_query_id().lo;
        bool canceled = fragment_ctx->is_canceled();

        auto state = task->get_state();
        if (state == PipelineTaskState::PENDING_FINISH) {
            DCHECK(task->is_pipelineX() || !task->is_pending_finish())
                    << "must not pending close " << task->debug_string();
            Status exec_status = fragment_ctx->get_query_ctx()->exec_status();
            _try_close_task(task,
                            canceled ? PipelineTaskState::CANCELED : PipelineTaskState::FINISHED,
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
            _try_close_task(task, PipelineTaskState::CANCELED, cancel_status);
            continue;
        }

        if (task->is_pipelineX()) {
            task->set_state(PipelineTaskState::RUNNABLE);
        }

        DCHECK(task->is_pipelineX() || task->get_state() == PipelineTaskState::RUNNABLE)
                << "state:" << get_state_name(task->get_state())
                << " task: " << task->debug_string();
        // task exec
        bool eos = false;
        auto status = Status::OK();

        try {
            status = task->execute(&eos);
        } catch (const Exception& e) {
            status = e.to_status();
        }

        task->set_previous_core_id(index);
        if (!status.ok()) {
            task->set_eos_time();
            LOG(WARNING) << fmt::format(
                    "Pipeline task failed. query_id: {} reason: {}",
                    PrintInstanceStandardInfo(task->query_context()->query_id(),
                                              task->fragment_context()->get_fragment_instance_id()),
                    status.msg());
            // Print detail informations below when you debugging here.
            //
            // LOG(WARNING)<< "task:\n"<<task->debug_string();

            // exec failed，cancel all fragment instance
            fragment_ctx->cancel(PPlanFragmentCancelReason::INTERNAL_ERROR, status.msg());
            _try_close_task(task, PipelineTaskState::CANCELED, status);
            continue;
        }
        fragment_ctx->trigger_report_if_necessary();

        if (eos) {
            task->set_eos_time();
            // TODO: pipeline parallel need to wait the last task finish to call finalize
            //  and find_p_dependency
            VLOG_DEBUG << fmt::format(
                    "Try close task: {}, fragment_ctx->is_canceled(): {}",
                    PrintInstanceStandardInfo(task->query_context()->query_id(),
                                              task->fragment_context()->get_fragment_instance_id()),
                    fragment_ctx->is_canceled());
            _try_close_task(task,
                            fragment_ctx->is_canceled() ? PipelineTaskState::CANCELED
                                                        : PipelineTaskState::FINISHED,
                            status);
            continue;
        }

        auto pipeline_state = task->get_state();
        switch (pipeline_state) {
        case PipelineTaskState::BLOCKED_FOR_SOURCE:
        case PipelineTaskState::BLOCKED_FOR_SINK:
        case PipelineTaskState::BLOCKED_FOR_RF:
        case PipelineTaskState::BLOCKED_FOR_DEPENDENCY:
            static_cast<void>(_blocked_task_scheduler->add_blocked_task(task));
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

void TaskScheduler::_try_close_task(PipelineTask* task, PipelineTaskState state,
                                    Status exec_status) {
    // close_a_pipeline may delete fragment context and will core in some defer
    // code, because the defer code will access fragment context it self.
    std::shared_ptr<TaskExecutionContext> lock_for_context =
            task->fragment_context()->shared_from_this();
    auto status = task->try_close(exec_status);
    auto cancel = [&]() {
        task->query_context()->cancel(true, status.to_string(),
                                      Status::Cancelled(status.to_string()));
        state = PipelineTaskState::CANCELED;
    };

    auto try_close_failed = !status.ok() && state != PipelineTaskState::CANCELED;
    if (try_close_failed) {
        cancel();
    }
    if (!task->is_pipelineX() && task->is_pending_finish()) {
        task->set_state(PipelineTaskState::PENDING_FINISH);
        // After the task is added to the block queue, it maybe run by another thread
        // and the task maybe released in the other thread. And will core at
        // task set running.
        static_cast<void>(_blocked_task_scheduler->add_blocked_task(task));
        task->set_running(false);
        return;
    } else if (task->is_pending_finish()) {
        task->set_state(PipelineTaskState::PENDING_FINISH);
        task->set_running(false);
        return;
    }

    status = task->close(exec_status);
    if (!status.ok() && state != PipelineTaskState::CANCELED) {
        cancel();
    }
    task->set_state(state);
    task->set_close_pipeline_time();
    task->finalize();
    task->set_running(false);
    task->fragment_context()->close_a_pipeline();
}

void TaskScheduler::stop() {
    if (!this->_shutdown.load()) {
        this->_shutdown.store(true);
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
    }
}

} // namespace doris::pipeline
