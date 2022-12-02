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

#include "common/signal_handler.h"
#include "pipeline_fragment_context.h"
#include "util/thread.h"

namespace doris::pipeline {

Status BlockedTaskScheduler::start() {
    LOG(INFO) << "BlockedTaskScheduler start";
    RETURN_IF_ERROR(Thread::create(
            "BlockedTaskScheduler", "schedule_blocked_pipeline", [this]() { this->_schedule(); },
            &_thread));
    while (!this->_started.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
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

void BlockedTaskScheduler::add_blocked_task(PipelineTask* task) {
    std::unique_lock<std::mutex> lock(_task_mutex);
    _blocked_tasks.push_back(task);
    _task_cond.notify_one();
}

void BlockedTaskScheduler::_schedule() {
    LOG(INFO) << "BlockedTaskScheduler schedule thread start";
    _started.store(true);
    std::list<PipelineTask*> local_blocked_tasks;
    int empty_times = 0;
    std::vector<PipelineTask*> ready_tasks;

    while (!_shutdown.load()) {
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
        DateTimeValue now = DateTimeValue::local_time();
        while (iter != local_blocked_tasks.end()) {
            auto* task = *iter;
            auto state = task->get_state();
            if (state == PENDING_FINISH) {
                // should cancel or should finish
                if (task->is_pending_finish()) {
                    iter++;
                } else {
                    _make_task_run(local_blocked_tasks, iter, ready_tasks, PENDING_FINISH);
                }
            } else if (task->fragment_context()->is_canceled()) {
                if (task->is_pending_finish()) {
                    task->set_state(PENDING_FINISH);
                    iter++;
                } else {
                    _make_task_run(local_blocked_tasks, iter, ready_tasks);
                }
            } else if (task->query_fragments_context()->is_timeout(now)) {
                LOG(WARNING) << "Timeout, query_id="
                             << print_id(task->query_fragments_context()->query_id)
                             << ", instance_id="
                             << print_id(task->fragment_context()->get_fragment_id());

                task->fragment_context()->cancel(PPlanFragmentCancelReason::TIMEOUT);

                if (task->is_pending_finish()) {
                    task->set_state(PENDING_FINISH);
                    iter++;
                } else {
                    _make_task_run(local_blocked_tasks, iter, ready_tasks);
                }
            } else if (state == BLOCKED_FOR_DEPENDENCY) {
                if (task->has_dependency()) {
                    iter++;
                } else {
                    _make_task_run(local_blocked_tasks, iter, ready_tasks);
                }
            } else if (state == BLOCKED_FOR_SOURCE) {
                if (task->source_can_read()) {
                    _make_task_run(local_blocked_tasks, iter, ready_tasks);
                } else {
                    iter++;
                }
            } else if (state == BLOCKED_FOR_SINK) {
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

/////////////////////////  TaskScheduler  ///////////////////////////////////////////////////////////////////////////

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
        LOG(INFO) << "Start TaskScheduler thread " << i;
        _markers.push_back(std::make_unique<std::atomic<bool>>(true));
        RETURN_IF_ERROR(
                _fix_thread_pool->submit_func(std::bind(&TaskScheduler::_do_work, this, i)));
    }
    return _blocked_task_scheduler->start();
}

Status TaskScheduler::schedule_task(PipelineTask* task) {
    if (task->is_blocking_state()) {
        _blocked_task_scheduler->add_blocked_task(task);
    } else {
        _task_queue->push_back(task);
    }
    // TODO control num of task
    return Status::OK();
}

void TaskScheduler::_do_work(size_t index) {
    LOG(INFO) << "Start TaskScheduler worker " << index;
    auto queue = _task_queue;
    const auto& marker = _markers[index];
    while (*marker) {
        auto task = queue->try_take(index);
        if (!task) {
            task = queue->steal_take(index);
            if (!task) {
                // TODO: The take is a stock method, rethink the logic
                task = queue->take(index);
                if (!task) {
                    continue;
                }
            }
        }
        task->stop_worker_watcher();
        auto* fragment_ctx = task->fragment_context();
        doris::signal::query_id_hi = fragment_ctx->get_query_id().hi;
        doris::signal::query_id_lo = fragment_ctx->get_query_id().lo;
        bool canceled = fragment_ctx->is_canceled();

        auto check_state = task->get_state();
        if (check_state == PENDING_FINISH) {
            bool is_pending = task->is_pending_finish();
            DCHECK(!is_pending) << "must not pending close " << task->debug_string();
            _try_close_task(task, canceled ? CANCELED : FINISHED);
            continue;
        }
        DCHECK(check_state != FINISHED && check_state != CANCELED) << "task already finish";

        if (canceled) {
            // may change from pending FINISH，should called cancel
            // also may change form BLOCK, other task called cancel
            _try_close_task(task, CANCELED);
            continue;
        }

        DCHECK(check_state == RUNNABLE);
        // task exec
        bool eos = false;
        auto status = task->execute(&eos);
        task->set_previous_core_id(index);
        if (!status.ok()) {
            LOG(WARNING) << "Pipeline taks execute task fail " << task->debug_string();
            // exec failed，cancel all fragment instance
            fragment_ctx->cancel(PPlanFragmentCancelReason::INTERNAL_ERROR, "execute fail");
            _try_close_task(task, CANCELED);
            continue;
        }

        if (eos) {
            // TODO: pipeline parallel need to wait the last task finish to call finalize
            //  and find_p_dependency
            status = task->finalize();
            if (!status.ok()) {
                // execute failed，cancel all fragment
                fragment_ctx->cancel(PPlanFragmentCancelReason::INTERNAL_ERROR,
                                     "finalize fail:" + status.to_string());
                _try_close_task(task, CANCELED);
            } else {
                task->finish_p_dependency();
                _try_close_task(task, FINISHED);
            }
            continue;
        }

        auto pipeline_state = task->get_state();
        switch (pipeline_state) {
        case BLOCKED_FOR_SOURCE:
        case BLOCKED_FOR_SINK:
            _blocked_task_scheduler->add_blocked_task(task);
            break;
        case RUNNABLE:
            queue->push_back(task, index);
            break;
        default:
            DCHECK(false) << "error state after run task, " << get_state_name(pipeline_state);
            break;
        }
    }
    LOG(INFO) << "Stop TaskScheduler worker " << index;
}

void TaskScheduler::_try_close_task(PipelineTask* task, PipelineTaskState state) {
    // state only should be CANCELED or FINISHED
    if (task->is_pending_finish()) {
        task->set_state(PENDING_FINISH);
        _blocked_task_scheduler->add_blocked_task(task);
    } else {
        auto status = task->close();
        if (!status.ok()) {
            // TODO: LOG warning
        }
        task->set_state(state);
        // TODO: rethink the logic
        if (state == CANCELED) {
            task->finish_p_dependency();
        }
        task->fragment_context()->close_a_pipeline();
    }
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