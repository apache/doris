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
#include "pipeline/task_queue.h"
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

TaskScheduler::~TaskScheduler() {
    stop();
    LOG(INFO) << "Task scheduler " << _name << " shutdown";
}

Status TaskScheduler::start() {
    int cores = _task_queue->cores();
    RETURN_IF_ERROR(ThreadPoolBuilder(_name)
                            .set_min_threads(cores + 1)
                            .set_max_threads(cores + 1)
                            .set_max_queue_size(0)
                            .set_cgroup_cpu_ctl(_cgroup_cpu_ctl)
                            .build(&_fix_thread_pool));
    LOG_INFO("TaskScheduler set cores").tag("size", cores);
    for (size_t i = 0; i < cores; ++i) {
        RETURN_IF_ERROR(_fix_thread_pool->submit_func([this, i] { _do_work(i); }));
    }

    RETURN_IF_ERROR(_fix_thread_pool->submit_func([this] { _paused_queries_handler(); }));
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
    while (!_need_to_stop) {
        auto* task = _task_queue->take(index);
        if (!task) {
            continue;
        }

        if (task->is_running()) {
            static_cast<void>(_task_queue->push_back(task, index));
            continue;
        }
        task->set_running(true);

        task->log_detail_if_need();
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

void TaskScheduler::add_paused_task(PipelineTask* task) {
    std::lock_guard<std::mutex> lock(_paused_queries_lock);
    auto query_ctx_sptr = task->runtime_state()->get_query_ctx()->shared_from_this();
    DCHECK(query_ctx_sptr != nullptr);
    auto wg = query_ctx_sptr->workload_group();
    auto&& [it, inserted] = _paused_queries_list[wg].emplace(std::move(query_ctx_sptr));
    if (inserted) {
        LOG(INFO) << "here insert one new paused query: " << print_id(it->get()->query_id());
    }

    _paused_queries_cv.notify_all();
}

/**
 * Strategy 1: A revocable query should not have any running task(PipelineTask).
 * strategy 2: If the workload group is below low water mark, we make all queries in this wg runnable.
 * strategy 3: Pick the query which has the max revocable size to revoke memory.
 * strategy 4: If all queries are not revocable and they all have not any running task,
 *             we choose the max memory usage query to cancel.
 */
void TaskScheduler::_paused_queries_handler() {
    while (!_need_to_stop) {
        {
            std::unique_lock<std::mutex> lock(_paused_queries_lock);
            if (_paused_queries_list.empty()) {
                _paused_queries_cv.wait(lock, [&] { return !_paused_queries_list.empty(); });
            }

            if (_need_to_stop) {
                break;
            }

            if (_paused_queries_list.empty()) {
                continue;
            }

            for (auto it = _paused_queries_list.begin(); it != _paused_queries_list.end();) {
                auto& queries_list = it->second;
                const auto& wg = it->first;
                if (queries_list.empty()) {
                    LOG(INFO) << "wg: " << wg->debug_string() << " has no paused query";
                    it = _paused_queries_list.erase(it);
                    continue;
                }

                bool is_low_wartermark = false;
                bool is_high_wartermark = false;

                wg->check_mem_used(&is_low_wartermark, &is_high_wartermark);

                if (!is_low_wartermark && !is_high_wartermark) {
                    LOG(INFO) << "**** there are " << queries_list.size() << " to resume";
                    for (const auto& query : queries_list) {
                        LOG(INFO) << "**** resume paused query: " << print_id(query->query_id());
                        query->set_memory_sufficient(true);
                    }

                    queries_list.clear();
                    it = _paused_queries_list.erase(it);
                    continue;
                } else {
                    ++it;
                }

                std::shared_ptr<QueryContext> max_revocable_query;
                std::shared_ptr<QueryContext> max_memory_usage_query;
                std::shared_ptr<QueryContext> running_query;
                bool has_running_query = false;
                size_t max_revocable_size = 0;
                size_t max_memory_usage = 0;
                auto it_to_remove = queries_list.end();

                for (auto query_it = queries_list.begin(); query_it != queries_list.end();) {
                    const auto& query_ctx = *query_it;
                    size_t revocable_size = 0;
                    size_t memory_usage = 0;
                    bool has_running_task = false;

                    if (query_ctx->is_cancelled()) {
                        LOG(INFO) << "query: " << print_id(query_ctx->query_id())
                                  << "was canceled, remove from paused list";
                        query_it = queries_list.erase(query_it);
                        continue;
                    }

                    query_ctx->get_revocable_info(revocable_size, memory_usage, has_running_task);
                    if (has_running_task) {
                        has_running_query = true;
                        running_query = query_ctx;
                        break;
                    } else if (revocable_size > max_revocable_size) {
                        max_revocable_query = query_ctx;
                        max_revocable_size = revocable_size;
                        it_to_remove = query_it;
                    } else if (memory_usage > max_memory_usage) {
                        max_memory_usage_query = query_ctx;
                        max_memory_usage = memory_usage;
                        it_to_remove = query_it;
                    }

                    ++query_it;
                }

                if (has_running_query) {
                    LOG(INFO) << "has running task, query: " << print_id(running_query->query_id());
                    std::this_thread::sleep_for(std::chrono::milliseconds(500));
                } else if (max_revocable_query) {
                    queries_list.erase(it_to_remove);
                    queries_list.insert(queries_list.begin(), max_revocable_query);

                    auto revocable_tasks = max_revocable_query->get_revocable_tasks();
                    DCHECK(!revocable_tasks.empty());

                    LOG(INFO) << "query: " << print_id(max_revocable_query->query_id()) << ", has "
                              << revocable_tasks.size()
                              << " tasks to revoke memory, max revocable size: "
                              << max_revocable_size;
                    SCOPED_ATTACH_TASK(max_revocable_query.get());
                    for (auto* task : revocable_tasks) {
                        auto st = task->revoke_memory();
                        if (!st.ok()) {
                            max_revocable_query->cancel(st);
                            break;
                        }
                    }
                } else if (max_memory_usage_query) {
                    bool new_is_low_wartermark = false;
                    bool new_is_high_wartermark = false;
                    wg->check_mem_used(&new_is_low_wartermark, &new_is_high_wartermark);
                    if (new_is_high_wartermark) {
                        LOG(INFO) << "memory insufficient and cannot find revocable query, cancel "
                                     "the query: "
                                  << print_id(max_memory_usage_query->query_id())
                                  << ", usage: " << max_memory_usage
                                  << ", wg info: " << wg->debug_string();
                        max_memory_usage_query->cancel(Status::InternalError(
                                "memory insufficient and cannot find revocable query, cancel the "
                                "biggest usage({}) query({})",
                                max_memory_usage, print_id(max_memory_usage_query->query_id())));
                    } else {
                        LOG(INFO) << "new_is_high_wartermark is false, resume max memory usage "
                                     "paused query: "
                                  << print_id(max_memory_usage_query->query_id());
                        max_memory_usage_query->set_memory_sufficient(true);
                        queries_list.erase(it_to_remove);
                    }
                }
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

void TaskScheduler::stop() {
    if (!_shutdown) {
        if (_task_queue) {
            _task_queue->close();
        }
        if (_fix_thread_pool) {
            _need_to_stop = true;
            _paused_queries_cv.notify_all();
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
