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

#include "runtime/workload_management/query_task_controller.h"

#include "pipeline/pipeline_fragment_context.h"
#include "runtime/query_context.h"
#include "runtime/workload_management/task_controller.h"

namespace doris {
#include "common/compile_check_begin.h"

std::unique_ptr<TaskController> QueryTaskController::create(QueryContext* query_ctx) {
    return QueryTaskController::create_unique(query_ctx->shared_from_this());
}

bool QueryTaskController::is_cancelled() const {
    auto query_ctx = query_ctx_.lock();
    if (query_ctx == nullptr) {
        return false;
    }
    return query_ctx->is_cancelled();
}

bool QueryTaskController::cancel_impl(const Status& reason, int fragment_id) {
    auto query_ctx = query_ctx_.lock();
    if (query_ctx == nullptr) {
        return false;
    }
    query_ctx->cancel(reason, fragment_id);
    return true;
}

bool QueryTaskController::is_pure_load_task() const {
    auto query_ctx = query_ctx_.lock();
    if (query_ctx == nullptr) {
        return false;
    }
    return query_ctx->is_pure_load_task();
}

int32_t QueryTaskController::get_slot_count() const {
    auto query_ctx = query_ctx_.lock();
    if (query_ctx == nullptr) {
        return 0;
    }
    return query_ctx->get_slot_count();
}

bool QueryTaskController::is_enable_reserve_memory() const {
    auto query_ctx = query_ctx_.lock();
    if (query_ctx == nullptr) {
        return false;
    }
    return query_ctx->query_options().__isset.enable_reserve_memory &&
           query_ctx->query_options().enable_reserve_memory && enable_reserve_memory_;
}

void QueryTaskController::set_memory_sufficient(bool sufficient) {
    auto query_ctx = query_ctx_.lock();
    if (query_ctx == nullptr) {
        return;
    }
    query_ctx->set_memory_sufficient(sufficient);
}

int64_t QueryTaskController::memory_sufficient_time() {
    auto query_ctx = query_ctx_.lock();
    if (query_ctx == nullptr) {
        return 0;
    }
    return query_ctx->get_memory_sufficient_dependency()->watcher_elapse_time();
}

void QueryTaskController::get_revocable_info(size_t* revocable_size, size_t* memory_usage,
                                             bool* has_running_task) {
    auto query_ctx = query_ctx_.lock();
    if (query_ctx == nullptr) {
        return;
    }
    *revocable_size = 0;
    std::lock_guard<std::mutex> lock(query_ctx->_pipeline_map_write_lock);
    for (auto&& [fragment_id, fragment_wptr] : query_ctx->_fragment_id_to_pipeline_ctx) {
        auto fragment_ctx = fragment_wptr.lock();
        if (!fragment_ctx) {
            continue;
        }

        *revocable_size += fragment_ctx->get_revocable_size(has_running_task);

        // Should wait for all tasks are not running before revoking memory.
        if (*has_running_task) {
            break;
        }
    }

    *memory_usage = query_ctx->query_mem_tracker()->consumption();
}

size_t QueryTaskController::get_revocable_size() {
    size_t revocable_size = 0;
    size_t memory_usage = 0;
    bool has_running_task;
    get_revocable_info(&revocable_size, &memory_usage, &has_running_task);
    return revocable_size;
}

Status QueryTaskController::revoke_memory() {
    auto query_ctx = query_ctx_.lock();
    if (query_ctx == nullptr) {
        return Status::OK();
    }
    std::vector<std::pair<size_t, pipeline::PipelineTask*>> tasks;
    std::vector<std::shared_ptr<pipeline::PipelineFragmentContext>> fragments;
    std::lock_guard<std::mutex> lock(query_ctx->_pipeline_map_write_lock);
    for (auto&& [fragment_id, fragment_wptr] : query_ctx->_fragment_id_to_pipeline_ctx) {
        auto fragment_ctx = fragment_wptr.lock();
        if (!fragment_ctx) {
            continue;
        }

        auto tasks_of_fragment = fragment_ctx->get_revocable_tasks();
        for (auto* task : tasks_of_fragment) {
            tasks.emplace_back(task->get_revocable_size(), task);
        }
        fragments.emplace_back(std::move(fragment_ctx));
    }

    std::sort(tasks.begin(), tasks.end(), [](auto&& l, auto&& r) { return l.first > r.first; });

    // Do not use memlimit, use current memory usage.
    // For example, if current limit is 1.6G, but current used is 1G, if reserve failed
    // should free 200MB memory, not 300MB
    const auto target_revoking_size = static_cast<int64_t>(
            static_cast<double>(query_ctx->query_mem_tracker()->consumption()) * 0.2);
    size_t revoked_size = 0;
    size_t total_revokable_size = 0;

    std::vector<pipeline::PipelineTask*> chosen_tasks;
    for (auto&& [revocable_size, task] : tasks) {
        // Only revoke the largest task to ensure memory is used as much as possible
        // break;
        if (revoked_size < target_revoking_size) {
            chosen_tasks.emplace_back(task);
            revoked_size += revocable_size;
        }
        total_revokable_size += revocable_size;
    }

    std::weak_ptr<QueryContext> this_ctx = query_ctx;
    auto spill_context = std::make_shared<pipeline::SpillContext>(
            chosen_tasks.size(), query_ctx->query_id(),
            [this_ctx, this](pipeline::SpillContext* context) {
                auto query_context = this_ctx.lock();
                if (!query_context) {
                    return;
                }

                LOG(INFO) << debug_string() << ", context: " << ((void*)context)
                          << " all spill tasks done, resume it.";
                query_context->set_memory_sufficient(true);
            });

    LOG(INFO) << fmt::format(
            "{}, spill context: {}, revokable mem: {}/{}, tasks count: {}/{}", debug_string(),
            ((void*)spill_context.get()), PrettyPrinter::print_bytes(revoked_size),
            PrettyPrinter::print_bytes(total_revokable_size), chosen_tasks.size(), tasks.size());

    for (auto* task : chosen_tasks) {
        RETURN_IF_ERROR(task->revoke_memory(spill_context));
    }
    return Status::OK();
}

std::vector<pipeline::PipelineTask*> QueryTaskController::get_revocable_tasks() {
    std::vector<pipeline::PipelineTask*> tasks;
    auto query_ctx = query_ctx_.lock();
    if (query_ctx == nullptr) {
        return tasks;
    }
    std::lock_guard<std::mutex> lock(query_ctx->_pipeline_map_write_lock);
    for (auto&& [fragment_id, fragment_wptr] : query_ctx->_fragment_id_to_pipeline_ctx) {
        auto fragment_ctx = fragment_wptr.lock();
        if (!fragment_ctx) {
            continue;
        }
        auto tasks_of_fragment = fragment_ctx->get_revocable_tasks();
        tasks.insert(tasks.end(), tasks_of_fragment.cbegin(), tasks_of_fragment.cend());
    }
    return tasks;
}

#include "common/compile_check_end.h"
} // namespace doris
