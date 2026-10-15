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

#include "runtime/workload_management/resource_context.h"

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "common/config.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/workload_group/workload_group.h"
#include "testutil/mock/mock_query_context.h"
#include "util/uid_util.h"

namespace doris {

class ResourceContextLeakCheckTest : public testing::Test {
protected:
    void SetUp() override {
        _saved_graceful_exit_check = config::enable_graceful_exit_check;
        config::enable_graceful_exit_check = true;
    }

    void TearDown() override { config::enable_graceful_exit_check = _saved_graceful_exit_check; }

    static TUniqueId make_task_id(int64_t lo) {
        TUniqueId task_id;
        task_id.__set_hi(0x1eac0c4ec4ec0000);
        task_id.__set_lo(lo);
        return task_id;
    }

    // The line that describe_live_contexts() printed for the task, or an empty string.
    static std::string line_of(const std::string& description, const TUniqueId& task_id) {
        size_t pos = description.find("task_id=" + print_id(task_id) + ",");
        if (pos == std::string::npos) {
            return "";
        }
        size_t begin = description.rfind('\n', pos) + 1;
        size_t end = description.find('\n', pos);
        return description.substr(begin, end == std::string::npos ? end : end - begin);
    }

private:
    bool _saved_graceful_exit_check = false;
};

TEST_F(ResourceContextLeakCheckTest, DescribesContextUntilItIsReleased) {
    TUniqueId task_id = make_task_id(1);
    auto context = ResourceContext::create_shared();
    context->task_controller()->set_task_id(task_id);
    context->memory_context()->set_mem_tracker(MemTrackerLimiter::create_shared(
            MemTrackerLimiter::Type::LOAD, "Load#Id=" + print_id(task_id)));

    std::string description = ResourceContext::describe_live_contexts(1000);
    EXPECT_NE(description.find("resource contexts are still alive"), std::string::npos)
            << description;
    std::string line = line_of(description, task_id);
    EXPECT_EQ(0, line.find(fmt::format("  {}: references=1,", fmt::ptr(context.get())))) << line;
    EXPECT_NE(line.find("finished=false"), std::string::npos) << line;
    EXPECT_NE(line.find("mem_tracker=Load#Id=" + print_id(task_id) + " ("), std::string::npos)
            << line;
    EXPECT_NE(line.find("workload_group=none"), std::string::npos) << line;
    // Only a query's controller knows its QueryContext.
    EXPECT_EQ(line.find("query_context="), std::string::npos) << line;

    {
        auto another_holder = context;
        line = line_of(ResourceContext::describe_live_contexts(1000), task_id);
        EXPECT_NE(line.find("references=2,"), std::string::npos) << line;
    }

    context->task_controller()->finish();
    line = line_of(ResourceContext::describe_live_contexts(1000), task_id);
    EXPECT_NE(line.find("finished=true"), std::string::npos) << line;

    // What doris_main logs at exit is the same description.
    EXPECT_NE(line_of(ResourceContext::log_live_contexts(), task_id), "");

    context.reset();
    EXPECT_EQ("", line_of(ResourceContext::describe_live_contexts(1000), task_id));
    EXPECT_EQ("", line_of(ResourceContext::log_live_contexts(), task_id));
}

TEST_F(ResourceContextLeakCheckTest, ShowsTheWorkloadGroup) {
    TUniqueId task_id = make_task_id(4);
    auto context = ResourceContext::create_shared();
    context->task_controller()->set_task_id(task_id);
    WorkloadGroupInfo wg_info {.id = 6894204, .name = "resource_context_test_wg"};
    context->set_workload_group(std::make_shared<WorkloadGroup>(wg_info));

    std::string line = line_of(ResourceContext::describe_live_contexts(1000), task_id);
    EXPECT_NE(line.find("workload_group=resource_context_test_wg(6894204)"), std::string::npos)
            << line;
    EXPECT_NE(line.find("mem_tracker=none"), std::string::npos) << line;
}

TEST_F(ResourceContextLeakCheckTest, DoesNotTrackContextsCreatedOutsideLeakCheckMode) {
    config::enable_graceful_exit_check = false;
    TUniqueId task_id = make_task_id(2);
    auto context = ResourceContext::create_shared();
    context->task_controller()->set_task_id(task_id);

    config::enable_graceful_exit_check = true;
    EXPECT_EQ("", line_of(ResourceContext::describe_live_contexts(1000), task_id));
    // Must not try to unregister a context that was never registered.
    context.reset();
}

TEST_F(ResourceContextLeakCheckTest, TellsWhetherTheQueryContextIsStillAlive) {
    TUniqueId task_id = make_task_id(3);
    auto query_ctx = MockQueryContext::create(task_id);
    std::shared_ptr<ResourceContext> context = query_ctx->resource_ctx();

    std::string line = line_of(ResourceContext::describe_live_contexts(1000), task_id);
    EXPECT_NE(line.find("query_context=alive"), std::string::npos) << line;

    // The shape of the leak this is meant to explain: the QueryContext is gone, but its
    // resource context is still held by someone else.
    query_ctx.reset();
    line = line_of(ResourceContext::describe_live_contexts(1000), task_id);
    EXPECT_NE(line.find("references=1,"), std::string::npos) << line;
    EXPECT_NE(line.find("finished=true"), std::string::npos) << line;
    EXPECT_NE(line.find("query_context=destroyed"), std::string::npos) << line;
}

TEST_F(ResourceContextLeakCheckTest, LimitsTheNumberOfDescribedContexts) {
    std::vector<std::shared_ptr<ResourceContext>> contexts;
    for (int64_t i = 0; i < 3; ++i) {
        contexts.push_back(ResourceContext::create_shared());
        contexts.back()->task_controller()->set_task_id(make_task_id(10 + i));
    }

    std::string description = ResourceContext::describe_live_contexts(1);
    int described = 0;
    for (int64_t i = 0; i < 3; ++i) {
        described += line_of(description, make_task_id(10 + i)).empty() ? 0 : 1;
    }
    EXPECT_LE(described, 1) << description;
    EXPECT_NE(description.find("\n  ... and "), std::string::npos) << description;
}

} // namespace doris
