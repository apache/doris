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
#include <functional>
#include <vector>

#include "runtime/workload_management/query_task_controller.h"

namespace doris {

struct MockQueryTaskController : public QueryTaskController {
    ENABLE_FACTORY_CREATOR(MockQueryTaskController);

    MockQueryTaskController(const std::shared_ptr<QueryContext>& query_ctx)
            : QueryTaskController(query_ctx) {}

    static std::unique_ptr<MockQueryTaskController> create(QueryTaskController* controller) {
        auto ctx = MockQueryTaskController::create_unique(controller->query_ctx_.lock());
        ctx->set_task_id(controller->task_id());
        ctx->set_fe_addr(controller->fe_addr());
        ctx->set_query_type(controller->query_type());
        return ctx;
    }

    void set_cancelled_time(int64_t ctime) { cancelled_time_ = ctime; }

    // Memory reclamation asks every candidate query whether it is cancelled while it scans a
    // workload group, so this is where a test can let another query release memory during that
    // scan.
    bool is_cancelled() const override {
        if (on_is_cancelled_) {
            on_is_cancelled_();
        }
        return QueryTaskController::is_cancelled();
    }

    // Pipeline state seen by WorkloadGroupMgr::handle_single_query_. The query context of a
    // unit test has no fragments, so the real implementation never reports a running or a
    // revocable task; these knobs simulate them.
    // NOLINTNEXTLINE(readability-make-member-function-const): overrides a non-const virtual.
    void get_revocable_info(size_t* revocable_size, size_t* memory_usage,
                            bool* has_running_task) override {
        QueryTaskController::get_revocable_info(revocable_size, memory_usage, has_running_task);
        *has_running_task = has_running_task_;
    }

    // The manager only checks whether the list is empty before it calls revoke_memory(), which
    // is mocked below, so a placeholder entry is enough to stand for a revocable task.
    std::vector<PipelineTask*> get_revocable_tasks() override {
        if (on_get_revocable_tasks_) {
            on_get_revocable_tasks_();
        }
        return has_revocable_task_ ? std::vector<PipelineTask*> {nullptr}
                                   : std::vector<PipelineTask*> {};
    }

    // The spill of a unit test completes at once: like the spill callback of the real
    // implementation, resume the query so that its blocked tasks retry their reservations. The
    // spilled tasks have nothing left to revoke.
    Status revoke_memory() override {
        ++revoke_memory_calls_;
        RETURN_IF_ERROR(revoke_memory_status_);
        has_revocable_task_ = false;
        set_memory_sufficient(true);
        return Status::OK();
    }

    bool has_running_task_ {false};
    bool has_revocable_task_ {false};
    Status revoke_memory_status_ {Status::OK()};
    int revoke_memory_calls_ {0};
    // Runs inside get_revocable_tasks(), the last inspection of the pipeline tasks after the
    // manager has observed the memory pressure and before it decides what to do with the query.
    std::function<void()> on_get_revocable_tasks_;
    // Runs inside is_cancelled(), while memory reclamation scans the workload group of this
    // query.
    std::function<void()> on_is_cancelled_;
};

} // namespace doris
