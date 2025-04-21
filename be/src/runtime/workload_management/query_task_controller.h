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

#include "common/factory_creator.h"
#include "runtime/workload_management/task_controller.h"

namespace doris {
#include "common/compile_check_begin.h"

class QueryContext;

class QueryTaskController : public TaskController {
    ENABLE_FACTORY_CREATOR(QueryTaskController);

public:
    static std::unique_ptr<TaskController> create(QueryContext* query_ctx);
    ~QueryTaskController() override = default;

    bool is_cancelled() const override;
    bool cancel_impl(const Status& reason, int fragment_id);
    bool cancel_impl(const Status& reason) override { return cancel_impl(reason, -1); }
    bool is_pure_load_task() const override;
    int32_t get_slot_count() const override;
    bool is_enable_reserve_memory() const override;
    void set_memory_sufficient(bool sufficient) override;
    int64_t memory_sufficient_time() override;
    void get_revocable_info(size_t* revocable_size, size_t* memory_usage,
                            bool* has_running_task) override;
    size_t get_revocable_size() override;
    Status revoke_memory() override;
    std::vector<pipeline::PipelineTask*> get_revocable_tasks() override;

protected:
    QueryTaskController(const std::shared_ptr<QueryContext>& query_ctx) : query_ctx_(query_ctx) {}

    const std::weak_ptr<QueryContext> query_ctx_;
};

#include "common/compile_check_end.h"
} // namespace doris
