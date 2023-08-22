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

#include "common/signal_handler.h"
#include "pipeline/pipeline_task.h"
#include "pipeline/task_queue.h"
#include "pipeline_fragment_context.h"
#include "runtime/query_context.h"
#include "util/sse_util.hpp"
#include "util/thread.h"
#include "util/threadpool.h"
#include "util/uid_util.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::pipeline {

BlockedTaskScheduler::BlockedTaskScheduler(std::shared_ptr<TaskQueue> task_queue)
        : _task_queue(std::move(task_queue)), _started(false), _shutdown(false) {}

Status BlockedTaskScheduler::start() {
    LOG(INFO) << "BlockedTaskScheduler start";
    RETURN_IF_ERROR(Thread::create(
            "BlockedTaskScheduler", "schedule_blocked_pipeline", [this]() { this->_schedule(); },
            &_thread));
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
    _blocked_tasks.push_back(task);
    _task_cond.notify_one();
    return Status::OK();
}

void BlockedTaskScheduler::_schedule() {
    _started.store(true);
    std::list<PipelineTask*> local_blocked_tasks;
    int empty_times = 0;
    std::vector<PipelineTask*> ready_tasks;

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

        auto iter = local_blocked_tasks.begin();
        vectorized::VecDateTimeValue now = vectorized::VecDateTimeValue::local_time();
        while (iter != local_blocked_tasks.end()) {
            auto* task = *iter;
            auto state = task->get_state();
            if (state == PipelineTaskState::PENDING_FINISH) {
                // should cancel or should finish
                if (task->is_pending_finish()) {
                    VLOG_DEBUG << "Task pending" << task->debug_string();
                    iter++;
                } else {
                    _make_task_run(local_blocked_tasks, iter, ready_tasks,
                                   PipelineTaskState::PENDING_FINISH);
                }
            } else if (task->fragment_context()->is_canceled()) {
                _make_task_run(local_blocked_tasks, iter, ready_tasks);
            } else if (task->query_context()->is_timeout(now)) {
                LOG(WARNING) << "Timeout, query_id=" << print_id(task->query_context()->query_id())
                             << ", instance_id="
                             << print_id(task->fragment_context()->get_fragment_instance_id());

                task->fragment_context()->cancel(PPlanFragmentCancelReason::TIMEOUT);
                _make_task_run(local_blocked_tasks, iter, ready_tasks);
            } else if (state == PipelineTaskState::BLOCKED_FOR_DEPENDENCY) {
                if (task->has_dependency()) {
                    iter++;
                } else {
                    _make_task_run(local_blocked_tasks, iter, ready_tasks);
                }
            } else if (state == PipelineTaskState::BLOCKED_FOR_SOURCE) {
                if (task->source_can_read()) {
                    _make_task_run(local_blocked_tasks, iter, ready_tasks);
                } else {
                    iter++;
                }
            } else if (state == PipelineTaskState::BLOCKED_FOR_RF) {
                if (task->runtime_filters_are_ready_or_timeout()) {
                    _make_task_run(local_blocked_tasks, iter, ready_tasks);
                } else {
                    iter++;
                }
            } else if (state == PipelineTaskState::BLOCKED_FOR_SINK) {
                if (task->sink_can_write()) {
                    _make_task_run(local_blocked_tasks, iter, ready_tasks);
                } else {
                    iter++;
                }
            } else {
                // TODO: DCHECK the state
                _make_task_run(local_blocked_tasks, iter, ready_tasks);
            }
        }

        if (ready_tasks.empty()) {
            empty_times += 1;
        } else {
            empty_times = 0;
            for (auto& task : ready_tasks) {
                task->stop_schedule_watcher();
                _task_queue->push_back(task);
            }
            ready_tasks.clear();
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
                                          std::vector<PipelineTask*>& ready_tasks,
                                          PipelineTaskState t_state) {
    auto task = *task_itr;
    task->start_schedule_watcher();
    task->set_state(t_state);
    local_tasks.erase(task_itr++);
    ready_tasks.emplace_back(task);
}

TaskScheduler::~TaskScheduler() {
    shutdown();
}

Status TaskScheduler::start() {
    int cores = _task_queue->cores();
    // Must be mutil number of cpu cores
    ThreadPoolBuilder("TaskSchedulerThreadPool")
            .set_min_threads(cores)
            .set_max_threads(cores)
            .set_max_queue_size(0)
            .build(&_fix_thread_pool);
    _markers.reserve(cores);
    for (size_t i = 0; i < cores; ++i) {
        _markers.push_back(std::make_unique<std::atomic<bool>>(true));
        RETURN_IF_ERROR(
                _fix_thread_pool->submit_func(std::bind(&TaskScheduler::_do_work, this, i)));
    }
    return _blocked_task_scheduler->start();
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
        task->set_task_queue(_task_queue.get());
        auto* fragment_ctx = task->fragment_context();
        signal::query_id_hi = fragment_ctx->get_query_id().hi;
        signal::query_id_lo = fragment_ctx->get_query_id().lo;
        bool canceled = fragment_ctx->is_canceled();

        auto check_state = task->get_state();
        if (check_state == PipelineTaskState::PENDING_FINISH) {
            DCHECK(!task->is_pending_finish()) << "must not pending close " << task->debug_string();
            _try_close_task(task,
                            canceled ? PipelineTaskState::CANCELED : PipelineTaskState::FINISHED);
            continue;
        }
        DCHECK(check_state != PipelineTaskState::FINISHED &&
               check_state != PipelineTaskState::CANCELED)
                << "task already finish";

        if (canceled) {
            // may change from pending FINISH，should called cancel
            // also may change form BLOCK, other task called cancel

            // If pipeline is canceled caused by memory limit, we should send report to FE in order
            // to cancel all pipeline tasks in this query
            fragment_ctx->send_report(true);
            _try_close_task(task, PipelineTaskState::CANCELED);
            continue;
        }

        DCHECK(check_state == PipelineTaskState::RUNNABLE);
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
            LOG(WARNING) << fmt::format("Pipeline task failed. reason: {}", status.to_string());
            // Print detail informations below when you debugging here.
            //
            // LOG(WARNING)<< "task:\n"<<task->debug_string();

            // exec failed，cancel all fragment instance
            fragment_ctx->cancel(PPlanFragmentCancelReason::INTERNAL_ERROR, status.to_string());
            fragment_ctx->send_report(true);
            _try_close_task(task, PipelineTaskState::CANCELED);
            continue;
        }

        if (eos) {
            task->set_eos_time();
            // TODO: pipeline parallel need to wait the last task finish to call finalize
            //  and find_p_dependency
            status = task->finalize();
            if (!status.ok()) {
                // execute failed，cancel all fragment
                fragment_ctx->cancel(PPlanFragmentCancelReason::INTERNAL_ERROR,
                                     "finalize fail:" + status.to_string());
            } else {
                _try_close_task(task, fragment_ctx->is_canceled() ? PipelineTaskState::CANCELED
                                                                  : PipelineTaskState::FINISHED);
            }
            continue;
        }

        auto pipeline_state = task->get_state();
        switch (pipeline_state) {
        case PipelineTaskState::BLOCKED_FOR_SOURCE:
        case PipelineTaskState::BLOCKED_FOR_SINK:
        case PipelineTaskState::BLOCKED_FOR_RF:
        case PipelineTaskState::BLOCKED_FOR_DEPENDENCY:
            _blocked_task_scheduler->add_blocked_task(task);
            break;
        case PipelineTaskState::RUNNABLE:
            _task_queue->push_back(task, index);
            break;
        default:
            DCHECK(false) << "error state after run task, " << get_state_name(pipeline_state);
            break;
        }
    }
}

void TaskScheduler::_try_close_task(PipelineTask* task, PipelineTaskState state) {
    auto status = task->try_close();
    if (!status.ok() && state != PipelineTaskState::CANCELED) {
        // Call `close` if `try_close` failed to make sure allocated resources are released
        task->close();
        task->fragment_context()->cancel(PPlanFragmentCancelReason::INTERNAL_ERROR,
                                         status.to_string());
        state = PipelineTaskState::CANCELED;
    } else if (task->is_pending_finish()) {
        task->set_state(PipelineTaskState::PENDING_FINISH);
        _blocked_task_scheduler->add_blocked_task(task);
        return;
    } else {
        status = task->close();
        if (!status.ok() && state != PipelineTaskState::CANCELED) {
            task->fragment_context()->cancel(PPlanFragmentCancelReason::INTERNAL_ERROR,
                                             status.to_string());
            state = PipelineTaskState::CANCELED;
        }
        DCHECK(!task->is_pending_finish()) << task->debug_string();
    }
    task->set_state(state);
    task->set_close_pipeline_time();
    task->fragment_context()->close_a_pipeline();
}

void TaskScheduler::shutdown() {
    if (!this->_shutdown.load()) {
        this->_shutdown.store(true);
        _blocked_task_scheduler->shutdown();
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
