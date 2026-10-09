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

    // Pipeline state seen by WorkloadGroupMgr::handle_single_query_. The query context of a
    // unit test has no fragments, so the real implementation never reports a running or a
    // revocable task; these knobs simulate them.
    // NOLINTNEXTLINE(readability-make-member-function-const): overrides a non-const virtual.
    void get_revocable_info(size_t* revocable_size, size_t* memory_usage,
                            bool* has_running_task) override {
        QueryTaskController::get_revocable_info(revocable_size, memory_usage, has_running_task);
        *has_running_task = has_running_task_;
        if (on_get_revocable_info_) {
            on_get_revocable_info_();
        }
    }

    // The manager only checks whether the list is empty before it calls revoke_memory(), which
    // is mocked below, so a placeholder entry is enough to stand for a revocable task.
    std::vector<PipelineTask*> get_revocable_tasks() override {
        return has_revocable_task_ ? std::vector<PipelineTask*> {nullptr}
                                   : std::vector<PipelineTask*> {};
    }

    Status revoke_memory() override {
        ++revoke_memory_calls_;
        return revoke_memory_status_;
    }

    bool has_running_task_ {false};
    bool has_revocable_task_ {false};
    Status revoke_memory_status_ {Status::OK()};
    int revoke_memory_calls_ {0};
    // Runs inside get_revocable_info(), after the manager has observed the memory pressure
    // and before it decides what to do with the query.
    std::function<void()> on_get_revocable_info_;
};

} // namespace doris
