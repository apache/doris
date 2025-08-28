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
};

} // namespace doris
