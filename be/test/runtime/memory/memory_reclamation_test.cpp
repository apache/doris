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

#include "runtime/memory/memory_reclamation.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdint>

#include "pipeline/thrift_builder.h"
#include "runtime/workload_group/workload_group.h"
#include "testutil/mock/mock_query_task_controller.h"

namespace doris {

class MemoryReclamationTest : public testing::Test {
public:
    MemoryReclamationTest() = default;
    ~MemoryReclamationTest() override = default;
    void SetUp() override {}
    void TearDown() override {}

private:
    std::vector<std::shared_ptr<QueryContext>> _create_query_ctxs(
            int num, TQueryType::type query_type,
            std::vector<std::shared_ptr<ResourceContext>>& resource_ctxs) {
        std::vector<std::shared_ptr<QueryContext>> query_ctxs;
        for (int i = 0; i < num; i++) {
            query_ctxs.push_back(_create_query_ctx(i, query_type));
            resource_ctxs.push_back(query_ctxs.back()->resource_ctx());
        }
        return query_ctxs;
    }

    std::shared_ptr<QueryContext> _create_query_ctx(int64_t id, TQueryType::type query_type) {
        TQueryOptions query_options = TQueryOptionsBuilder().set_query_type(query_type).build();
        TUniqueId query_id;
        query_id.hi = id;
        query_id.lo = id;
        auto fe_address = TNetworkAddress();
        fe_address.hostname = LOCALHOST;
        fe_address.port = DUMMY_PORT;

        auto ctx = QueryContext::create(query_id, ExecEnv::GetInstance(), query_options, fe_address,
                                        true, fe_address, QuerySource::INTERNAL_FRONTEND);
        ctx->resource_ctx()->set_task_controller(MockQueryTaskController::create(
                static_cast<QueryTaskController*>(ctx->resource_ctx()->task_controller())));
        return ctx;
    }

    const std::string LOCALHOST = BackendOptions::get_localhost();
    const int DUMMY_PORT = config::brpc_port;
};

TEST_F(MemoryReclamationTest, TestRevokeTasksMemoryOneQuery) {
    std::unique_ptr<RuntimeProfile> profile;
    std::vector<std::shared_ptr<ResourceContext>> resource_ctxs;
    auto ctxs = _create_query_ctxs(1, TQueryType::type::SELECT, resource_ctxs);

    profile = std::make_unique<RuntimeProfile>("MemoryReclamationTest");
    EXPECT_EQ(MemoryReclamation::revoke_tasks_memory(0, resource_ctxs, "MemoryReclamationTest",
                                                     profile.get(),
                                                     MemoryReclamation::PriorityCmpFunc::TOP_MEMORY,
                                                     {MemoryReclamation::FilterFunc::IS_QUERY},
                                                     MemoryReclamation::ActionFunc::CANCEL),
              0);
    EXPECT_FALSE(resource_ctxs[0]->task_controller()->is_cancelled());

    resource_ctxs[0]->memory_context()->mem_tracker()->consume(1024);
    profile = std::make_unique<RuntimeProfile>("MemoryReclamationTest");
    EXPECT_EQ(MemoryReclamation::revoke_tasks_memory(1024, resource_ctxs, "MemoryReclamationTest",
                                                     profile.get(),
                                                     MemoryReclamation::PriorityCmpFunc::TOP_MEMORY,
                                                     {MemoryReclamation::FilterFunc::IS_LOAD},
                                                     MemoryReclamation::ActionFunc::CANCEL),
              0);
    EXPECT_FALSE(resource_ctxs[0]->task_controller()->is_cancelled());

    profile = std::make_unique<RuntimeProfile>("MemoryReclamationTest");
    EXPECT_EQ(MemoryReclamation::revoke_tasks_memory(
                      512, resource_ctxs, "MemoryReclamationTest", profile.get(),
                      MemoryReclamation::PriorityCmpFunc::TOP_MEMORY,
                      {MemoryReclamation::FilterFunc::IS_QUERY,
                       MemoryReclamation::FilterFunc::EXCLUDE_IS_SMALL},
                      MemoryReclamation::ActionFunc::CANCEL),
              0);
    EXPECT_FALSE(resource_ctxs[0]->task_controller()->is_cancelled());

    profile = std::make_unique<RuntimeProfile>("MemoryReclamationTest");
    EXPECT_EQ(MemoryReclamation::revoke_tasks_memory(512, resource_ctxs, "MemoryReclamationTest",
                                                     profile.get(),
                                                     MemoryReclamation::PriorityCmpFunc::TOP_MEMORY,
                                                     {MemoryReclamation::FilterFunc::IS_QUERY},
                                                     MemoryReclamation::ActionFunc::CANCEL),
              1024);
    EXPECT_TRUE(resource_ctxs[0]->task_controller()->is_cancelled());

    resource_ctxs[0]->memory_context()->mem_tracker()->consume(-512);
    profile = std::make_unique<RuntimeProfile>("MemoryReclamationTest");
    EXPECT_EQ(MemoryReclamation::revoke_tasks_memory(1024, resource_ctxs, "MemoryReclamationTest",
                                                     profile.get(),
                                                     MemoryReclamation::PriorityCmpFunc::TOP_MEMORY,
                                                     {MemoryReclamation::FilterFunc::IS_QUERY},
                                                     MemoryReclamation::ActionFunc::CANCEL),
              512);
    EXPECT_TRUE(resource_ctxs[0]->task_controller()->is_cancelled());

    auto* mock_controller =
            static_cast<MockQueryTaskController*>(resource_ctxs[0]->task_controller());
    mock_controller->set_cancelled_time(mock_controller->cancelled_time() -
                                        config::revoke_memory_max_tolerance_ms - 100000000);
    profile = std::make_unique<RuntimeProfile>("MemoryReclamationTest");
    EXPECT_EQ(MemoryReclamation::revoke_tasks_memory(1024, resource_ctxs, "MemoryReclamationTest",
                                                     profile.get(),
                                                     MemoryReclamation::PriorityCmpFunc::TOP_MEMORY,
                                                     {MemoryReclamation::FilterFunc::IS_QUERY},
                                                     MemoryReclamation::ActionFunc::CANCEL),
              0);
    EXPECT_TRUE(resource_ctxs[0]->task_controller()->is_cancelled());
}

TEST_F(MemoryReclamationTest, TestRevokeTasksMemoryOneLoad) {
    std::unique_ptr<RuntimeProfile> profile;
    std::vector<std::shared_ptr<ResourceContext>> resource_ctxs;
    auto ctxs = _create_query_ctxs(1, TQueryType::type::LOAD, resource_ctxs);

    resource_ctxs[0]->memory_context()->mem_tracker()->consume(1024);
    profile = std::make_unique<RuntimeProfile>("MemoryReclamationTest");
    EXPECT_FALSE(resource_ctxs[0]->task_controller()->is_cancelled());
    EXPECT_EQ(MemoryReclamation::revoke_tasks_memory(1024, resource_ctxs, "MemoryReclamationTest",
                                                     profile.get(),
                                                     MemoryReclamation::PriorityCmpFunc::TOP_MEMORY,
                                                     {MemoryReclamation::FilterFunc::IS_LOAD},
                                                     MemoryReclamation::ActionFunc::CANCEL),
              1024);
    EXPECT_TRUE(resource_ctxs[0]->task_controller()->is_cancelled());

    resource_ctxs[0]->memory_context()->mem_tracker()->consume(-512);
    profile = std::make_unique<RuntimeProfile>("MemoryReclamationTest");
    EXPECT_EQ(MemoryReclamation::revoke_tasks_memory(1024, resource_ctxs, "MemoryReclamationTest",
                                                     profile.get(),
                                                     MemoryReclamation::PriorityCmpFunc::TOP_MEMORY,
                                                     {MemoryReclamation::FilterFunc::IS_QUERY},
                                                     MemoryReclamation::ActionFunc::CANCEL),
              0);
    EXPECT_TRUE(resource_ctxs[0]->task_controller()->is_cancelled());

    profile = std::make_unique<RuntimeProfile>("MemoryReclamationTest");
    EXPECT_EQ(MemoryReclamation::revoke_tasks_memory(1024, resource_ctxs, "MemoryReclamationTest",
                                                     profile.get(),
                                                     MemoryReclamation::PriorityCmpFunc::TOP_MEMORY,
                                                     {MemoryReclamation::FilterFunc::IS_LOAD},
                                                     MemoryReclamation::ActionFunc::CANCEL),
              512);
    EXPECT_TRUE(resource_ctxs[0]->task_controller()->is_cancelled());
}

TEST_F(MemoryReclamationTest, TestRevokeTasksMemoryMutlQueryTopMemory) {
    std::unique_ptr<RuntimeProfile> profile;
    std::vector<std::shared_ptr<ResourceContext>> resource_ctxs;
    auto ctxs = _create_query_ctxs(3, TQueryType::type::SELECT, resource_ctxs);

    resource_ctxs[0]->memory_context()->mem_tracker()->consume(1024);
    resource_ctxs[1]->memory_context()->mem_tracker()->consume(1024000000);
    resource_ctxs[2]->memory_context()->mem_tracker()->consume(1024000000000);

    profile = std::make_unique<RuntimeProfile>("MemoryReclamationTest");
    EXPECT_EQ(MemoryReclamation::revoke_tasks_memory(
                      512, resource_ctxs, "MemoryReclamationTest", profile.get(),
                      MemoryReclamation::PriorityCmpFunc::TOP_MEMORY,
                      {MemoryReclamation::FilterFunc::IS_QUERY,
                       MemoryReclamation::FilterFunc::EXCLUDE_IS_SMALL},
                      MemoryReclamation::ActionFunc::CANCEL),
              1024000000000);
    EXPECT_FALSE(resource_ctxs[0]->task_controller()->is_cancelled());
    EXPECT_FALSE(resource_ctxs[1]->task_controller()->is_cancelled());
    EXPECT_TRUE(resource_ctxs[2]->task_controller()->is_cancelled());

    profile = std::make_unique<RuntimeProfile>("MemoryReclamationTest");
    EXPECT_EQ(MemoryReclamation::revoke_tasks_memory(
                      1024000000000 + 1, resource_ctxs, "MemoryReclamationTest", profile.get(),
                      MemoryReclamation::PriorityCmpFunc::TOP_MEMORY,
                      {MemoryReclamation::FilterFunc::IS_QUERY,
                       MemoryReclamation::FilterFunc::EXCLUDE_IS_SMALL},
                      MemoryReclamation::ActionFunc::CANCEL),
              1024000000000 + 1024000000);
    EXPECT_FALSE(resource_ctxs[0]->task_controller()->is_cancelled());
    EXPECT_TRUE(resource_ctxs[1]->task_controller()->is_cancelled());
    EXPECT_TRUE(resource_ctxs[2]->task_controller()->is_cancelled());

    profile = std::make_unique<RuntimeProfile>("MemoryReclamationTest");
    EXPECT_EQ(MemoryReclamation::revoke_tasks_memory(
                      1024000000000 + 1024000000 + 1, resource_ctxs, "MemoryReclamationTest",
                      profile.get(), MemoryReclamation::PriorityCmpFunc::TOP_MEMORY,
                      {MemoryReclamation::FilterFunc::IS_QUERY,
                       MemoryReclamation::FilterFunc::EXCLUDE_IS_SMALL},
                      MemoryReclamation::ActionFunc::CANCEL),
              1024000000000 + 1024000000);
    EXPECT_FALSE(resource_ctxs[0]->task_controller()->is_cancelled());
    EXPECT_TRUE(resource_ctxs[1]->task_controller()->is_cancelled());
    EXPECT_TRUE(resource_ctxs[2]->task_controller()->is_cancelled());

    profile = std::make_unique<RuntimeProfile>("MemoryReclamationTest");
    EXPECT_EQ(MemoryReclamation::revoke_tasks_memory(1024000000000 + 1024000000 + 1, resource_ctxs,
                                                     "MemoryReclamationTest", profile.get(),
                                                     MemoryReclamation::PriorityCmpFunc::TOP_MEMORY,
                                                     {MemoryReclamation::FilterFunc::IS_QUERY},
                                                     MemoryReclamation::ActionFunc::CANCEL),
              1024000000000 + 1024000000 + 1024);
    EXPECT_TRUE(resource_ctxs[0]->task_controller()->is_cancelled());
    EXPECT_TRUE(resource_ctxs[1]->task_controller()->is_cancelled());
    EXPECT_TRUE(resource_ctxs[2]->task_controller()->is_cancelled());
}

TEST_F(MemoryReclamationTest, TestRevokeTasksMemoryMutlQueryTopOvercommited) {
    std::unique_ptr<RuntimeProfile> profile;
    std::vector<std::shared_ptr<ResourceContext>> resource_ctxs;
    auto ctxs = _create_query_ctxs(3, TQueryType::type::SELECT, resource_ctxs);

    resource_ctxs[0]->memory_context()->mem_tracker()->consume(1024);
    resource_ctxs[0]->memory_context()->set_mem_limit(1024 / 5);
    resource_ctxs[1]->memory_context()->mem_tracker()->consume(1024000);
    resource_ctxs[1]->memory_context()->set_mem_limit(1024000 / 10);
    resource_ctxs[2]->memory_context()->mem_tracker()->consume(1024000000);
    resource_ctxs[2]->memory_context()->set_mem_limit(1024000000 / 2);

    profile = std::make_unique<RuntimeProfile>("MemoryReclamationTest");
    EXPECT_EQ(MemoryReclamation::revoke_tasks_memory(
                      512, resource_ctxs, "MemoryReclamationTest", profile.get(),
                      MemoryReclamation::PriorityCmpFunc::TOP_OVERCOMMITED_MEMORY,
                      {MemoryReclamation::FilterFunc::IS_QUERY},
                      MemoryReclamation::ActionFunc::CANCEL),
              1024000);
    EXPECT_FALSE(resource_ctxs[0]->task_controller()->is_cancelled());
    EXPECT_TRUE(resource_ctxs[1]->task_controller()->is_cancelled());
    EXPECT_FALSE(resource_ctxs[2]->task_controller()->is_cancelled());

    profile = std::make_unique<RuntimeProfile>("MemoryReclamationTest");
    EXPECT_EQ(MemoryReclamation::revoke_tasks_memory(
                      1024000 + 1, resource_ctxs, "MemoryReclamationTest", profile.get(),
                      MemoryReclamation::PriorityCmpFunc::TOP_OVERCOMMITED_MEMORY,
                      {MemoryReclamation::FilterFunc::IS_QUERY},
                      MemoryReclamation::ActionFunc::CANCEL),
              1024000 + 1024);
    EXPECT_TRUE(resource_ctxs[0]->task_controller()->is_cancelled());
    EXPECT_TRUE(resource_ctxs[1]->task_controller()->is_cancelled());
    EXPECT_FALSE(resource_ctxs[2]->task_controller()->is_cancelled());

    profile = std::make_unique<RuntimeProfile>("MemoryReclamationTest");
    EXPECT_EQ(MemoryReclamation::revoke_tasks_memory(
                      1024000000, resource_ctxs, "MemoryReclamationTest", profile.get(),
                      MemoryReclamation::PriorityCmpFunc::TOP_OVERCOMMITED_MEMORY,
                      {MemoryReclamation::FilterFunc::IS_QUERY},
                      MemoryReclamation::ActionFunc::CANCEL),
              1024000000 + 1024000 + 1024);
    EXPECT_TRUE(resource_ctxs[0]->task_controller()->is_cancelled());
    EXPECT_TRUE(resource_ctxs[1]->task_controller()->is_cancelled());
    EXPECT_TRUE(resource_ctxs[2]->task_controller()->is_cancelled());
}

TEST_F(MemoryReclamationTest, TestRevokeTasksMemoryMixTaskNoFilter) {
    std::unique_ptr<RuntimeProfile> profile;
    std::vector<std::shared_ptr<ResourceContext>> resource_ctxs;
    auto query_ctxs = _create_query_ctxs(3, TQueryType::type::SELECT, resource_ctxs);
    auto load_ctxs = _create_query_ctxs(2, TQueryType::type::LOAD, resource_ctxs);

    resource_ctxs[0]->memory_context()->mem_tracker()->consume(1024);
    resource_ctxs[0]->memory_context()->set_mem_limit(1024 / 5);
    resource_ctxs[1]->memory_context()->mem_tracker()->consume(1024000000000);
    resource_ctxs[1]->memory_context()->set_mem_limit(1024000000000 / 10);
    resource_ctxs[2]->memory_context()->mem_tracker()->consume(1024000000000);
    resource_ctxs[2]->memory_context()->set_mem_limit(-1);
    resource_ctxs[3]->memory_context()->mem_tracker()->consume(2048);
    resource_ctxs[3]->memory_context()->set_mem_limit(-1);
    resource_ctxs[4]->memory_context()->mem_tracker()->consume(2048000000000);
    resource_ctxs[4]->memory_context()->set_mem_limit(-1);

    profile = std::make_unique<RuntimeProfile>("MemoryReclamationTest");
    EXPECT_EQ(MemoryReclamation::revoke_tasks_memory(
                      1024000, resource_ctxs, "MemoryReclamationTest", profile.get(),
                      MemoryReclamation::PriorityCmpFunc::TOP_OVERCOMMITED_MEMORY, {},
                      MemoryReclamation::ActionFunc::CANCEL),
              1024000000000);
    EXPECT_FALSE(resource_ctxs[0]->task_controller()->is_cancelled());
    EXPECT_TRUE(resource_ctxs[1]->task_controller()->is_cancelled());
    EXPECT_FALSE(resource_ctxs[2]->task_controller()->is_cancelled());
    EXPECT_FALSE(resource_ctxs[3]->task_controller()->is_cancelled());
    EXPECT_FALSE(resource_ctxs[4]->task_controller()->is_cancelled());

    profile = std::make_unique<RuntimeProfile>("MemoryReclamationTest");
    EXPECT_EQ(MemoryReclamation::revoke_tasks_memory(
                      1024000000000 + 1, resource_ctxs, "MemoryReclamationTest", profile.get(),
                      MemoryReclamation::PriorityCmpFunc::TOP_OVERCOMMITED_MEMORY, {},
                      MemoryReclamation::ActionFunc::CANCEL),
              1024000000000 + 1024);
    EXPECT_TRUE(resource_ctxs[0]->task_controller()->is_cancelled());
    EXPECT_TRUE(resource_ctxs[1]->task_controller()->is_cancelled());
    EXPECT_FALSE(resource_ctxs[2]->task_controller()->is_cancelled());
    EXPECT_FALSE(resource_ctxs[3]->task_controller()->is_cancelled());
    EXPECT_FALSE(resource_ctxs[4]->task_controller()->is_cancelled());

    auto* mock_controller =
            static_cast<MockQueryTaskController*>(resource_ctxs[0]->task_controller());
    mock_controller->set_cancelled_time(mock_controller->cancelled_time() -
                                        config::revoke_memory_max_tolerance_ms - 100000000);
    mock_controller = static_cast<MockQueryTaskController*>(resource_ctxs[1]->task_controller());
    mock_controller->set_cancelled_time(mock_controller->cancelled_time() -
                                        config::revoke_memory_max_tolerance_ms - 100000000);
    profile = std::make_unique<RuntimeProfile>("MemoryReclamationTest");
    EXPECT_EQ(MemoryReclamation::revoke_tasks_memory(
                      1, resource_ctxs, "MemoryReclamationTest", profile.get(),
                      MemoryReclamation::PriorityCmpFunc::TOP_OVERCOMMITED_MEMORY, {},
                      MemoryReclamation::ActionFunc::CANCEL),
              0);
    EXPECT_TRUE(resource_ctxs[0]->task_controller()->is_cancelled());
    EXPECT_TRUE(resource_ctxs[1]->task_controller()->is_cancelled());
    EXPECT_FALSE(resource_ctxs[2]->task_controller()->is_cancelled());
    EXPECT_FALSE(resource_ctxs[3]->task_controller()->is_cancelled());
    EXPECT_FALSE(resource_ctxs[4]->task_controller()->is_cancelled());

    EXPECT_EQ(MemoryReclamation::revoke_tasks_memory(1, resource_ctxs, "MemoryReclamationTest",
                                                     profile.get(),
                                                     MemoryReclamation::PriorityCmpFunc::TOP_MEMORY,
                                                     {}, MemoryReclamation::ActionFunc::CANCEL),
              2048000000000);
    EXPECT_TRUE(resource_ctxs[0]->task_controller()->is_cancelled());
    EXPECT_TRUE(resource_ctxs[1]->task_controller()->is_cancelled());
    EXPECT_FALSE(resource_ctxs[2]->task_controller()->is_cancelled());
    EXPECT_FALSE(resource_ctxs[3]->task_controller()->is_cancelled());
    EXPECT_TRUE(resource_ctxs[4]->task_controller()->is_cancelled());

    profile = std::make_unique<RuntimeProfile>("MemoryReclamationTest");
    EXPECT_EQ(MemoryReclamation::revoke_tasks_memory(2048000000000 + 1, resource_ctxs,
                                                     "MemoryReclamationTest", profile.get(),
                                                     MemoryReclamation::PriorityCmpFunc::TOP_MEMORY,
                                                     {}, MemoryReclamation::ActionFunc::CANCEL),
              2048000000000 + 1024000000000);
    EXPECT_TRUE(resource_ctxs[0]->task_controller()->is_cancelled());
    EXPECT_TRUE(resource_ctxs[1]->task_controller()->is_cancelled());
    EXPECT_TRUE(resource_ctxs[2]->task_controller()->is_cancelled());
    EXPECT_FALSE(resource_ctxs[3]->task_controller()->is_cancelled());
    EXPECT_TRUE(resource_ctxs[4]->task_controller()->is_cancelled());

    profile = std::make_unique<RuntimeProfile>("MemoryReclamationTest");
    EXPECT_EQ(MemoryReclamation::revoke_tasks_memory(INT64_MAX, resource_ctxs,
                                                     "MemoryReclamationTest", profile.get(),
                                                     MemoryReclamation::PriorityCmpFunc::TOP_MEMORY,
                                                     {}, MemoryReclamation::ActionFunc::CANCEL),
              2048000000000 + 1024000000000 + 2048);
    EXPECT_TRUE(resource_ctxs[0]->task_controller()->is_cancelled());
    EXPECT_TRUE(resource_ctxs[1]->task_controller()->is_cancelled());
    EXPECT_TRUE(resource_ctxs[2]->task_controller()->is_cancelled());
    EXPECT_TRUE(resource_ctxs[3]->task_controller()->is_cancelled());
    EXPECT_TRUE(resource_ctxs[4]->task_controller()->is_cancelled());
}

} // end namespace doris
