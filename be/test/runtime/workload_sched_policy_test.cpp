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

#include "runtime/workload_management/workload_sched_policy.h"

#include <gen_cpp/BackendService_types.h>
#include <gtest/gtest.h>

#include "pipeline/task_scheduler.h"
#include "runtime/workload_group/workload_group.h"
#include "runtime/workload_management/resource_context.h"
#include "runtime/workload_management/workload_condition.h"
#include "util/threadpool.h"
#include "vec/exec/scan/scanner_scheduler.h"

namespace doris {

class WorkloadSchedPolicyTest : public testing::Test {
public:
    WorkloadSchedPolicyTest() = default;
    ~WorkloadSchedPolicyTest() override = default;

protected:
    std::unique_ptr<WorkloadCondition> create_workload_condition(TWorkloadMetricType::type mtype,
                                                                 TCompareOperator::type otype,
                                                                 std::string value) {
        TWorkloadCondition cond;
        cond.metric_name = mtype;
        cond.op = otype;
        cond.value = value;
        return WorkloadConditionFactory::create_workload_condition(&cond);
    }

    std::unique_ptr<WorkloadAction> create_workload_action(TWorkloadActionType::type atype) {
        TWorkloadAction action;
        action.action = atype;
        return WorkloadActionFactory::create_workload_action(&action);
    }

    WorkloadAction::RuntimeContext create_runtime_context() {
        std::shared_ptr<ResourceContext> resource_ctx = ResourceContext::create_shared();
        WorkloadAction::RuntimeContext action_runtime_ctx(resource_ctx);
        return action_runtime_ctx;
    }

    void SetUp() override {}
};

TEST_F(WorkloadSchedPolicyTest, one_policy_one_condition) {
    // 1 empty resource
    {
        std::shared_ptr<WorkloadSchedPolicy> policy = std::make_shared<WorkloadSchedPolicy>();
        std::vector<std::unique_ptr<WorkloadCondition>> cond_ptr_list;
        std::vector<std::unique_ptr<WorkloadAction>> action_ptr_list;
        std::set<int64_t> wg_id_set;
        policy->init(0, "p1", 0, true, 0, wg_id_set, std::move(cond_ptr_list),
                     std::move(action_ptr_list));

        WorkloadAction::RuntimeContext action_runtime_ctx = create_runtime_context();
        EXPECT_TRUE(policy->is_match(&action_runtime_ctx));
    }
    {
        std::shared_ptr<WorkloadSchedPolicy> policy = std::make_shared<WorkloadSchedPolicy>();
        std::vector<std::unique_ptr<WorkloadCondition>> cond_ptr_list;
        cond_ptr_list.push_back(create_workload_condition(TWorkloadMetricType::type::QUERY_TIME,
                                                          TCompareOperator::type::GREATER, "10"));
        std::vector<std::unique_ptr<WorkloadAction>> action_ptr_list;
        action_ptr_list.push_back(create_workload_action(TWorkloadActionType::type::CANCEL_QUERY));
        std::set<int64_t> wg_id_set;
        policy->init(0, "p1", 0, true, 0, wg_id_set, std::move(cond_ptr_list),
                     std::move(action_ptr_list));

        WorkloadAction::RuntimeContext action_runtime_ctx = create_runtime_context();
        EXPECT_FALSE(policy->is_match(&action_runtime_ctx));
    }

    // 2 check query time
    {
        std::shared_ptr<WorkloadSchedPolicy> policy = std::make_shared<WorkloadSchedPolicy>();
        std::vector<std::unique_ptr<WorkloadCondition>> cond_ptr_list;
        cond_ptr_list.push_back(create_workload_condition(TWorkloadMetricType::type::QUERY_TIME,
                                                          TCompareOperator::type::GREATER, "10"));
        std::vector<std::unique_ptr<WorkloadAction>> action_ptr_list;
        action_ptr_list.push_back(create_workload_action(TWorkloadActionType::type::CANCEL_QUERY));
        std::set<int64_t> wg_id_set;
        policy->init(0, "p1", 0, true, 0, wg_id_set, std::move(cond_ptr_list),
                     std::move(action_ptr_list));

        WorkloadAction::RuntimeContext action_runtime_ctx = create_runtime_context();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        action_runtime_ctx.resource_ctx->task_controller()->finish();
        EXPECT_TRUE(policy->is_match(&action_runtime_ctx))
                << ": " << action_runtime_ctx.resource_ctx->task_controller()->running_time();
    }

    // 3 check scan rows
    {
        std::shared_ptr<WorkloadSchedPolicy> policy = std::make_shared<WorkloadSchedPolicy>();
        std::vector<std::unique_ptr<WorkloadCondition>> cond_ptr_list;
        cond_ptr_list.push_back(create_workload_condition(TWorkloadMetricType::type::BE_SCAN_ROWS,
                                                          TCompareOperator::type::GREATER, "100"));
        std::vector<std::unique_ptr<WorkloadAction>> action_ptr_list;
        action_ptr_list.push_back(create_workload_action(TWorkloadActionType::type::CANCEL_QUERY));
        std::set<int64_t> wg_id_set;
        policy->init(0, "p1", 0, true, 0, wg_id_set, std::move(cond_ptr_list),
                     std::move(action_ptr_list));

        WorkloadAction::RuntimeContext action_runtime_ctx = create_runtime_context();
        action_runtime_ctx.resource_ctx->io_context()->update_scan_rows(99);
        EXPECT_FALSE(policy->is_match(&action_runtime_ctx))
                << ": " << action_runtime_ctx.resource_ctx->io_context()->scan_rows();
        action_runtime_ctx.resource_ctx->io_context()->update_scan_rows(2);
        EXPECT_TRUE(policy->is_match(&action_runtime_ctx))
                << ": " << action_runtime_ctx.resource_ctx->io_context()->scan_rows();
        action_runtime_ctx.resource_ctx->io_context()->update_scan_rows(-2);
        EXPECT_FALSE(policy->is_match(&action_runtime_ctx))
                << ": " << action_runtime_ctx.resource_ctx->io_context()->scan_rows();
    }

    // 4 check scan bytes
    {
        std::shared_ptr<WorkloadSchedPolicy> policy = std::make_shared<WorkloadSchedPolicy>();
        std::vector<std::unique_ptr<WorkloadCondition>> cond_ptr_list;
        cond_ptr_list.push_back(create_workload_condition(TWorkloadMetricType::type::BE_SCAN_BYTES,
                                                          TCompareOperator::type::GREATER, "1000"));
        std::vector<std::unique_ptr<WorkloadAction>> action_ptr_list;
        action_ptr_list.push_back(create_workload_action(TWorkloadActionType::type::CANCEL_QUERY));
        std::set<int64_t> wg_id_set;
        policy->init(0, "p1", 0, true, 0, wg_id_set, std::move(cond_ptr_list),
                     std::move(action_ptr_list));

        WorkloadAction::RuntimeContext action_runtime_ctx = create_runtime_context();
        action_runtime_ctx.resource_ctx->io_context()->update_scan_bytes(999);
        EXPECT_FALSE(policy->is_match(&action_runtime_ctx))
                << ": " << action_runtime_ctx.resource_ctx->io_context()->scan_bytes();
        action_runtime_ctx.resource_ctx->io_context()->update_scan_bytes(2);
        EXPECT_TRUE(policy->is_match(&action_runtime_ctx))
                << ": " << action_runtime_ctx.resource_ctx->io_context()->scan_bytes();
        action_runtime_ctx.resource_ctx->io_context()->update_scan_bytes(-2);
        EXPECT_FALSE(policy->is_match(&action_runtime_ctx))
                << ": " << action_runtime_ctx.resource_ctx->io_context()->scan_bytes();
    }

    // 5 check query be memory bytes
    {
        std::shared_ptr<WorkloadSchedPolicy> policy = std::make_shared<WorkloadSchedPolicy>();
        std::vector<std::unique_ptr<WorkloadCondition>> cond_ptr_list;
        cond_ptr_list.push_back(
                create_workload_condition(TWorkloadMetricType::type::QUERY_BE_MEMORY_BYTES,
                                          TCompareOperator::type::GREATER, "10000"));
        std::vector<std::unique_ptr<WorkloadAction>> action_ptr_list;
        action_ptr_list.push_back(create_workload_action(TWorkloadActionType::type::CANCEL_QUERY));
        std::set<int64_t> wg_id_set;
        policy->init(0, "p1", 0, true, 0, wg_id_set, std::move(cond_ptr_list),
                     std::move(action_ptr_list));

        WorkloadAction::RuntimeContext action_runtime_ctx = create_runtime_context();
        std::shared_ptr<MemTrackerLimiter> mem_tracker =
                MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::QUERY, "Test");
        action_runtime_ctx.resource_ctx->memory_context()->set_mem_tracker(mem_tracker);
        action_runtime_ctx.resource_ctx->memory_context()->mem_tracker()->consume(9999);
        EXPECT_FALSE(policy->is_match(&action_runtime_ctx))
                << ": "
                << action_runtime_ctx.resource_ctx->memory_context()->current_memory_bytes();
        action_runtime_ctx.resource_ctx->memory_context()->mem_tracker()->consume(2);
        EXPECT_TRUE(policy->is_match(&action_runtime_ctx))
                << ": "
                << action_runtime_ctx.resource_ctx->memory_context()->current_memory_bytes();
        action_runtime_ctx.resource_ctx->memory_context()->mem_tracker()->consume(-2);
        EXPECT_FALSE(policy->is_match(&action_runtime_ctx))
                << ": "
                << action_runtime_ctx.resource_ctx->memory_context()->current_memory_bytes();
    }
}

TEST_F(WorkloadSchedPolicyTest, one_policy_mutl_condition) {
    // 1. init policy
    std::shared_ptr<WorkloadSchedPolicy> policy = std::make_shared<WorkloadSchedPolicy>();
    std::vector<std::unique_ptr<WorkloadCondition>> cond_ptr_list;
    cond_ptr_list.push_back(create_workload_condition(TWorkloadMetricType::type::QUERY_TIME,
                                                      TCompareOperator::type::GREATER, "10"));
    cond_ptr_list.push_back(create_workload_condition(TWorkloadMetricType::type::BE_SCAN_ROWS,
                                                      TCompareOperator::type::GREATER, "100"));
    cond_ptr_list.push_back(create_workload_condition(TWorkloadMetricType::type::BE_SCAN_BYTES,
                                                      TCompareOperator::type::GREATER, "1000"));
    cond_ptr_list.push_back(
            create_workload_condition(TWorkloadMetricType::type::QUERY_BE_MEMORY_BYTES,
                                      TCompareOperator::type::GREATER, "10000"));
    std::vector<std::unique_ptr<WorkloadAction>> action_ptr_list;
    action_ptr_list.push_back(create_workload_action(TWorkloadActionType::type::CANCEL_QUERY));
    std::set<int64_t> wg_id_set;
    policy->init(0, "p1", 0, true, 0, wg_id_set, std::move(cond_ptr_list),
                 std::move(action_ptr_list));

    // 2. is match
    WorkloadAction::RuntimeContext action_runtime_ctx = create_runtime_context();
    std::shared_ptr<MemTrackerLimiter> mem_tracker =
            MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::QUERY, "Test");
    action_runtime_ctx.resource_ctx->memory_context()->set_mem_tracker(mem_tracker);
    EXPECT_FALSE(policy->is_match(&action_runtime_ctx));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    action_runtime_ctx.resource_ctx->task_controller()->finish();
    EXPECT_FALSE(policy->is_match(&action_runtime_ctx));
    action_runtime_ctx.resource_ctx->io_context()->update_scan_rows(101);
    EXPECT_FALSE(policy->is_match(&action_runtime_ctx));
    action_runtime_ctx.resource_ctx->io_context()->update_scan_bytes(1001);
    EXPECT_FALSE(policy->is_match(&action_runtime_ctx));
    action_runtime_ctx.resource_ctx->memory_context()->mem_tracker()->consume(10001);
    EXPECT_TRUE(policy->is_match(&action_runtime_ctx));
}

TEST_F(WorkloadSchedPolicyTest, test_operator) {
    // LESS
    {
        std::shared_ptr<WorkloadSchedPolicy> policy = std::make_shared<WorkloadSchedPolicy>();
        std::vector<std::unique_ptr<WorkloadCondition>> cond_ptr_list;
        cond_ptr_list.push_back(create_workload_condition(TWorkloadMetricType::type::BE_SCAN_ROWS,
                                                          TCompareOperator::type::LESS, "100"));
        std::vector<std::unique_ptr<WorkloadAction>> action_ptr_list;
        std::set<int64_t> wg_id_set;
        policy->init(0, "p1", 0, true, 0, wg_id_set, std::move(cond_ptr_list),
                     std::move(action_ptr_list));

        WorkloadAction::RuntimeContext action_runtime_ctx = create_runtime_context();
        EXPECT_TRUE(policy->is_match(&action_runtime_ctx));
        action_runtime_ctx.resource_ctx->io_context()->update_scan_rows(100);
        EXPECT_FALSE(policy->is_match(&action_runtime_ctx));
    }

    // EQUAL
    {
        std::shared_ptr<WorkloadSchedPolicy> policy = std::make_shared<WorkloadSchedPolicy>();
        std::vector<std::unique_ptr<WorkloadCondition>> cond_ptr_list;
        cond_ptr_list.push_back(create_workload_condition(TWorkloadMetricType::type::BE_SCAN_ROWS,
                                                          TCompareOperator::type::EQUAL, "100"));
        std::vector<std::unique_ptr<WorkloadAction>> action_ptr_list;
        std::set<int64_t> wg_id_set;
        policy->init(0, "p1", 0, true, 0, wg_id_set, std::move(cond_ptr_list),
                     std::move(action_ptr_list));

        WorkloadAction::RuntimeContext action_runtime_ctx = create_runtime_context();
        EXPECT_FALSE(policy->is_match(&action_runtime_ctx));
        action_runtime_ctx.resource_ctx->io_context()->update_scan_rows(100);
        EXPECT_TRUE(policy->is_match(&action_runtime_ctx));
    }
}

TEST_F(WorkloadSchedPolicyTest, test_wg_id_set) {
    std::shared_ptr<WorkloadSchedPolicy> policy = std::make_shared<WorkloadSchedPolicy>();
    std::vector<std::unique_ptr<WorkloadCondition>> cond_ptr_list;
    cond_ptr_list.push_back(create_workload_condition(TWorkloadMetricType::type::BE_SCAN_ROWS,
                                                      TCompareOperator::type::GREATER_EQUAL, "0"));
    std::vector<std::unique_ptr<WorkloadAction>> action_ptr_list;
    std::set<int64_t> wg_id_set;
    wg_id_set.insert(1);
    policy->init(0, "p1", 0, true, 0, wg_id_set, std::move(cond_ptr_list),
                 std::move(action_ptr_list));

    WorkloadAction::RuntimeContext action_runtime_ctx = create_runtime_context();
    EXPECT_FALSE(policy->is_match(&action_runtime_ctx));
    TWorkloadGroupInfo tworkload_group_info;
    tworkload_group_info.__isset.id = true;
    tworkload_group_info.id = 1;
    tworkload_group_info.__isset.version = true;
    tworkload_group_info.version = 1;
    WorkloadGroupInfo workload_group_info =
            WorkloadGroupInfo::parse_topic_info(tworkload_group_info);
    auto workload_group = std::make_shared<WorkloadGroup>(workload_group_info);
    action_runtime_ctx.resource_ctx->set_workload_group(workload_group);
    EXPECT_TRUE(policy->is_match(&action_runtime_ctx));
}
} // namespace doris
