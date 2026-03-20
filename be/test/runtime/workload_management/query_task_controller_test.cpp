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

#include <gtest/gtest.h>

#include "testutil/mock/mock_query_context.h"

namespace doris {

class QueryTaskControllerTest : public testing::Test {
public:
    QueryTaskControllerTest() = default;
    ~QueryTaskControllerTest() override = default;
};

TEST_F(QueryTaskControllerTest, TestGetUser) {
    // 1. Create MockQueryContext
    auto query_ctx = MockQueryContext::create();

    // 2. Create QueryTaskController
    auto task_controller = QueryTaskController::create(query_ctx);

    // 3. Test default (set_rsc_info = false)
    EXPECT_EQ("", task_controller->get_user());

    // 4. Test with user set but set_rsc_info = false
    query_ctx->user = "test_user";
    EXPECT_EQ("", task_controller->get_user());

    // 5. Test with set_rsc_info = true
    query_ctx->set_rsc_info = true;
    EXPECT_EQ("test_user", task_controller->get_user());

    // 6. Test when QueryContext is destroyed
    std::shared_ptr<QueryContext> ctx_ptr = MockQueryContext::create();
    auto controller_ptr = QueryTaskController::create(ctx_ptr);
    ctx_ptr->set_rsc_info = true;
    ctx_ptr->user = "user1";
    EXPECT_EQ("user1", controller_ptr->get_user());

    ctx_ptr.reset(); // Destroy the context
    EXPECT_EQ("", controller_ptr->get_user());
}

} // namespace doris
