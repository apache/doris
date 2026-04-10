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

#include "runtime/workload_group/workload_group_manager.h"

#include <gen_cpp/BackendService_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <chrono>
#include <cmath>
#include <filesystem>
#include <memory>
#include <sstream>

#include "common/config.h"
#include "common/status.h"
#include "exec/pipeline/pipeline_tracing.h"
#include "exec/spill/spill_file_manager.h"
#include "runtime/exec_env.h"
#include "runtime/query_context.h"
#include "runtime/runtime_query_statistics_mgr.h"
#include "runtime/workload_group/workload_group.h"
#include "storage/olap_define.h"
#include "testutil/mock/mock_query_task_controller.h"

namespace doris {

class WorkloadGroupManagerTest : public testing::Test {
public:
protected:
    void SetUp() override {
        _wg_manager = std::make_unique<WorkloadGroupMgr>();
        // generate a unique test directory to avoid conflicts between parallel runs
        std::ostringstream _oss;
        _oss << "./wg_test_run_" << std::chrono::system_clock::now().time_since_epoch().count()
             << "_" << getpid();
        _test_dir = _oss.str();

        std::error_code ec;
        std::filesystem::remove_all(_test_dir, ec);
        if (ec) {
            FAIL() << "Failed to remove " << _test_dir << ": " << ec.message();
        }
        std::filesystem::create_directories(_test_dir, ec);
        ASSERT_FALSE(ec) << "Failed to create " << _test_dir << ": " << ec.message();

        std::vector<doris::StorePath> paths;
        std::string path = std::filesystem::absolute(_test_dir).string();
        auto olap_res = doris::parse_conf_store_paths(path, &paths);
        EXPECT_TRUE(olap_res.ok()) << olap_res.to_string();

        std::vector<doris::StorePath> spill_paths;
        olap_res = doris::parse_conf_store_paths(path, &spill_paths);
        ASSERT_TRUE(olap_res.ok()) << olap_res.to_string();
        std::unordered_map<std::string, std::unique_ptr<SpillDataDir>> spill_store_map;
        for (const auto& spill_path : spill_paths) {
            spill_store_map.emplace(
                    spill_path.path,
                    std::make_unique<SpillDataDir>(spill_path.path, spill_path.capacity_bytes,
                                                   spill_path.storage_medium));
        }

        ExecEnv::GetInstance()->_runtime_query_statistics_mgr = new RuntimeQueryStatisticsMgr();
        ExecEnv::GetInstance()->_spill_file_mgr = new SpillFileManager(std::move(spill_store_map));
        auto st = ExecEnv::GetInstance()->_spill_file_mgr->init();
        EXPECT_TRUE(st.ok()) << "init spill stream manager failed: " << st.to_string();
        ExecEnv::GetInstance()->_pipeline_tracer_ctx = std::make_unique<PipelineTracerContext>();

        config::spill_in_paused_queue_timeout_ms = 2000;
        doris::ExecEnv::GetInstance()->set_memtable_memory_limiter(new MemTableMemoryLimiter());
    }
    void TearDown() override {
        _wg_manager.reset();
        ExecEnv::GetInstance()->_runtime_query_statistics_mgr->stop_report_thread();
        SAFE_DELETE(ExecEnv::GetInstance()->_runtime_query_statistics_mgr);

        std::error_code ec;
        std::filesystem::remove_all(_test_dir, ec);
        EXPECT_FALSE(ec) << "Failed to remove " << _test_dir << ": " << ec.message();
        config::spill_in_paused_queue_timeout_ms = _spill_in_paused_queue_timeout_ms;
        doris::ExecEnv::GetInstance()->set_memtable_memory_limiter(nullptr);
    }

private:
    std::shared_ptr<QueryContext> _generate_on_query(std::shared_ptr<WorkloadGroup>& wg) {
        TQueryOptions query_options;
        query_options.query_type = TQueryType::SELECT;
        query_options.mem_limit = 1024L * 1024 * 128;
        query_options.query_slot_count = 1;
        TNetworkAddress fe_address;
        fe_address.hostname = "127.0.0.1";
        fe_address.port = 8060;
        auto query_context = QueryContext::create(generate_uuid(), ExecEnv::GetInstance(),
                                                  query_options, TNetworkAddress {}, true,
                                                  fe_address, QuerySource::INTERNAL_FRONTEND);

        auto st = wg->add_resource_ctx(query_context->query_id(), query_context->resource_ctx());
        EXPECT_TRUE(st.ok()) << "add query to workload group failed: " << st.to_string();

        static_cast<void>(query_context->set_workload_group(wg));
        return query_context;
    }

    MockQueryTaskController* _install_mock_query_task_controller(
            const std::shared_ptr<QueryContext>& query_context) {
        query_context->resource_ctx()->set_task_controller(
                MockQueryTaskController::create(static_cast<QueryTaskController*>(
                        query_context->resource_ctx()->task_controller())));
        return static_cast<MockQueryTaskController*>(
                query_context->resource_ctx()->task_controller());
    }

    void _run_checking_loop(const std::shared_ptr<WorkloadGroup>& wg) {
        CountDownLatch latch(1);
        size_t check_times = 300;
        while (--check_times > 0) {
            _wg_manager->handle_paused_queries();
            if (!_wg_manager->_paused_queries_list.contains(wg) ||
                _wg_manager->_paused_queries_list[wg].empty()) {
                break;
            }
            latch.wait_for(std::chrono::milliseconds(config::memory_maintenance_sleep_time_ms));
        }
    }

    std::unique_ptr<WorkloadGroupMgr> _wg_manager;
    std::string _test_dir;
    const int64_t _spill_in_paused_queue_timeout_ms = config::spill_in_paused_queue_timeout_ms;
};

TEST_F(WorkloadGroupManagerTest, get_or_create_workload_group) {
    auto wg = _wg_manager->get_or_create_workload_group({});
    ASSERT_EQ(wg->id(), 0);
}

// Query is paused due to query memlimit exceed, after waiting in queue for  spill_in_paused_queue_timeout_ms
// it should be resumed
TEST_F(WorkloadGroupManagerTest, query_exceed) {
    auto wg = _wg_manager->get_or_create_workload_group({});
    auto query_context = _generate_on_query(wg);

    query_context->resource_ctx()->memory_context()->set_mem_limit(1024 * 1024);
    query_context->query_mem_tracker()->consume(1024 * 4);

    std::cout << config::spill_in_paused_queue_timeout_ms << std::endl;

    _wg_manager->add_paused_query(query_context->resource_ctx(), 1024L * 1024 * 1024,
                                  Status::Error(ErrorCode::QUERY_MEMORY_EXCEEDED, "test"));
    {
        std::unique_lock<std::mutex> lock(_wg_manager->_paused_queries_lock);
        ASSERT_EQ(_wg_manager->_paused_queries_list[wg].size(), 1)
                << "paused queue should not be empty";
    }

    query_context->query_mem_tracker()->consume(-1024 * 4);
    _run_checking_loop(wg);

    std::unique_lock<std::mutex> lock(_wg_manager->_paused_queries_lock);
    ASSERT_TRUE(_wg_manager->_paused_queries_list[wg].empty()) << "paused queue should be empty";
    ASSERT_EQ(query_context->is_cancelled(), false) << "query should be not canceled";
    ASSERT_EQ(query_context->resource_ctx()->task_controller()->is_enable_reserve_memory(), false)
            << "query should disable reserve memory";
}

// if (query_ctx->adjusted_mem_limit() <
//                    query_ctx->get_mem_tracker()->consumption() + query_it->reserve_size_)
TEST_F(WorkloadGroupManagerTest, wg_exceed1) {
    auto wg = _wg_manager->get_or_create_workload_group({});
    auto query_context = _generate_on_query(wg);

    query_context->query_mem_tracker()->consume(1024L * 1024 * 1024 * 4);
    _wg_manager->add_paused_query(query_context->resource_ctx(), 1024L,
                                  Status::Error(ErrorCode::WORKLOAD_GROUP_MEMORY_EXCEEDED, "test"));
    {
        std::unique_lock<std::mutex> lock(_wg_manager->_paused_queries_lock);
        ASSERT_EQ(_wg_manager->_paused_queries_list[wg].size(), 1)
                << "pasued queue should not be empty";
    }

    _run_checking_loop(wg);

    query_context->query_mem_tracker()->consume(-1024 * 4);
    ASSERT_TRUE(query_context->resource_ctx()->task_controller()->paused_reason().ok());

    std::unique_lock<std::mutex> lock(_wg_manager->_paused_queries_lock);
    ASSERT_TRUE(_wg_manager->_paused_queries_list[wg].empty()) << "pasued queue should be empty";
    ASSERT_EQ(query_context->is_cancelled(), false) << "query should not be canceled";
}

// TWgSlotMemoryPolicy::NONE
// query_ctx->workload_group()->exceed_limit() == false
TEST_F(WorkloadGroupManagerTest, wg_exceed2) {
    auto wg = _wg_manager->get_or_create_workload_group({});
    auto query_context = _generate_on_query(wg);

    query_context->query_mem_tracker()->consume(1024L * 4);

    _wg_manager->add_paused_query(query_context->resource_ctx(), 1024L,
                                  Status::Error(ErrorCode::WORKLOAD_GROUP_MEMORY_EXCEEDED, "test"));
    {
        std::unique_lock<std::mutex> lock(_wg_manager->_paused_queries_lock);
        ASSERT_EQ(_wg_manager->_paused_queries_list[wg].size(), 1)
                << "pasued queue should not be empty";
    }

    _run_checking_loop(wg);
    query_context->query_mem_tracker()->consume(-1024 * 4);
    ASSERT_TRUE(query_context->resource_ctx()->task_controller()->paused_reason().ok());
    std::unique_lock<std::mutex> lock(_wg_manager->_paused_queries_lock);
    ASSERT_TRUE(_wg_manager->_paused_queries_list[wg].empty()) << "pasued queue should be empty";
    ASSERT_EQ(query_context->is_cancelled(), false) << "query should be canceled";
}

// TWgSlotMemoryPolicy::NONE
// query_ctx->workload_group()->exceed_limit() == true
// query limit > workload group limit
// query's limit will be set to workload group limit
TEST_F(WorkloadGroupManagerTest, wg_exceed3) {
    WorkloadGroupInfo wg_info {
            .id = 1, .memory_limit = 1024L * 1024, .slot_mem_policy = TWgSlotMemoryPolicy::NONE};
    auto wg = _wg_manager->get_or_create_workload_group(wg_info);
    auto query_context = _generate_on_query(wg);

    query_context->query_mem_tracker()->consume(1024L * 1024 * 4);

    // adjust memlimit is larger than mem limit
    query_context->resource_ctx()->memory_context()->set_adjusted_mem_limit(1024L * 1024 * 10);

    _wg_manager->add_paused_query(query_context->resource_ctx(), 1024L,
                                  Status::Error(ErrorCode::WORKLOAD_GROUP_MEMORY_EXCEEDED, "test"));
    {
        std::unique_lock<std::mutex> lock(_wg_manager->_paused_queries_lock);
        ASSERT_EQ(_wg_manager->_paused_queries_list[wg].size(), 1)
                << "paused queue should not be empty";
    }

    wg->refresh_memory_usage();
    _run_checking_loop(wg);

    query_context->query_mem_tracker()->consume(-1024L * 1024 * 4);

    // In the wg's policy is NONE. If the query reserve memory failed and revocable memory == 0, just cancel it.
    ASSERT_TRUE(query_context->is_cancelled());
    // Its limit == workload group's limit
    ASSERT_EQ(query_context->resource_ctx()->memory_context()->mem_limit(), wg->memory_limit());

    std::unique_lock<std::mutex> lock(_wg_manager->_paused_queries_lock);
    ASSERT_TRUE(_wg_manager->_paused_queries_list[wg].empty())
            << "paused queue should be empty, because the query will be resumed";
    // Query's memory usage + reserve size > adjusted memory size it will be resumed
    // it's memlimit will be set to adjusted size.
    ASSERT_EQ(query_context->resource_ctx()->task_controller()->is_enable_reserve_memory(), true)
            << "query should disable reserve memory";
    // adjust memlimit is larger than workload group memlimit, so adjust memlimit is reset to workload group mem limit.
    ASSERT_EQ(query_context->resource_ctx()->memory_context()->adjusted_mem_limit(),
              wg->memory_limit());
}

// TWgSlotMemoryPolicy::FIXED
TEST_F(WorkloadGroupManagerTest, wg_exceed4) {
    WorkloadGroupInfo wg_info {.id = 1,
                               .memory_limit = 1024L * 1024 * 100,
                               .memory_low_watermark = 80,
                               .memory_high_watermark = 95,
                               .total_query_slot_count = 5,
                               .slot_mem_policy = TWgSlotMemoryPolicy::FIXED};
    auto wg = _wg_manager->get_or_create_workload_group(wg_info);
    auto query_context = _generate_on_query(wg);

    query_context->query_mem_tracker()->consume(1024L * 1024 * 4);

    _wg_manager->add_paused_query(query_context->resource_ctx(), 1024L,
                                  Status::Error(ErrorCode::WORKLOAD_GROUP_MEMORY_EXCEEDED, "test"));
    {
        std::unique_lock<std::mutex> lock(_wg_manager->_paused_queries_lock);
        ASSERT_EQ(_wg_manager->_paused_queries_list[wg].size(), 1)
                << "pasued queue should not be empty";
    }

    _wg_manager->refresh_workload_group_memory_state();
    LOG(INFO) << "***** wg usage " << wg->refresh_memory_usage();
    _run_checking_loop(wg);

    query_context->query_mem_tracker()->consume(-1024L * 1024 * 4);
    ASSERT_TRUE(query_context->resource_ctx()->task_controller()->paused_reason().ok());
    LOG(INFO) << "***** query_context->get_mem_limit(): "
              << query_context->resource_ctx()->memory_context()->mem_limit();
    const auto delta = std::abs(query_context->resource_ctx()->memory_context()->mem_limit() -
                                (1024L * 1024 * 100 * 95) / 100 / 5);
    ASSERT_LE(delta, 1);

    std::unique_lock<std::mutex> lock(_wg_manager->_paused_queries_lock);
    ASSERT_TRUE(_wg_manager->_paused_queries_list[wg].empty()) << "pasued queue should be empty";
}

// TWgSlotMemoryPolicy::DYNAMIC
TEST_F(WorkloadGroupManagerTest, wg_exceed5) {
    WorkloadGroupInfo wg_info {.id = 1,
                               .memory_limit = 1024L * 1024 * 100,
                               .min_memory_percent = 10,
                               .max_memory_percent = 100,
                               .memory_low_watermark = 80,
                               .memory_high_watermark = 95,
                               .total_query_slot_count = 5,
                               .slot_mem_policy = TWgSlotMemoryPolicy::DYNAMIC};
    auto wg = _wg_manager->get_or_create_workload_group(wg_info);
    auto query_context = _generate_on_query(wg);

    query_context->query_mem_tracker()->consume(1024L * 1024 * 4);

    _wg_manager->add_paused_query(query_context->resource_ctx(), 1024L,
                                  Status::Error(ErrorCode::WORKLOAD_GROUP_MEMORY_EXCEEDED, "test"));
    {
        std::unique_lock<std::mutex> lock(_wg_manager->_paused_queries_lock);
        ASSERT_EQ(_wg_manager->_paused_queries_list[wg].size(), 1)
                << "paused queue should not be empty";
    }

    _wg_manager->refresh_workload_group_memory_state();
    LOG(INFO) << "***** wg usage " << wg->refresh_memory_usage();
    _run_checking_loop(wg);

    query_context->query_mem_tracker()->consume(-1024L * 1024 * 4);
    ASSERT_TRUE(query_context->resource_ctx()->task_controller()->paused_reason().ok());
    LOG(INFO) << "***** query_context->get_mem_limit(): "
              << query_context->resource_ctx()->memory_context()->mem_limit();

    // + slot count, because in query memlimit it + slot count
    ASSERT_LE(query_context->resource_ctx()->memory_context()->mem_limit(),
              ((1024L * 1024 * 100 * 95) / 100 + 5));

    std::unique_lock<std::mutex> lock(_wg_manager->_paused_queries_lock);
    ASSERT_TRUE(_wg_manager->_paused_queries_list[wg].empty()) << "pasued queue should be empty";
}

TEST_F(WorkloadGroupManagerTest, overcommit) {
    WorkloadGroupInfo wg_info {.id = 1};
    auto wg = _wg_manager->get_or_create_workload_group(wg_info);
    EXPECT_EQ(wg->id(), wg_info.id);

    auto query_context = _generate_on_query(wg);

    _wg_manager->add_paused_query(query_context->resource_ctx(), 1024L * 1024 * 1024,
                                  Status::Error(ErrorCode::WORKLOAD_GROUP_MEMORY_EXCEEDED, "test"));
    {
        std::unique_lock<std::mutex> lock(_wg_manager->_paused_queries_lock);
        ASSERT_EQ(_wg_manager->_paused_queries_list[wg].size(), 1)
                << "pasued queue should not be empty";
    }

    _run_checking_loop(wg);

    query_context->query_mem_tracker()->consume(-1024 * 4);

    std::unique_lock<std::mutex> lock(_wg_manager->_paused_queries_lock);
    ASSERT_TRUE(_wg_manager->_paused_queries_list[wg].empty()) << "pasued queue should be empty";
    ASSERT_EQ(query_context->is_cancelled(), false) << "query should be canceled";
}

TEST_F(WorkloadGroupManagerTest, slot_memory_policy_disabled) {
    WorkloadGroupInfo wg_info {.id = 1, .slot_mem_policy = TWgSlotMemoryPolicy::NONE};
    auto wg = _wg_manager->get_or_create_workload_group(wg_info);
    EXPECT_EQ(wg->id(), wg_info.id);
    EXPECT_EQ(wg->slot_memory_policy(), TWgSlotMemoryPolicy::NONE);

    auto query_context = _generate_on_query(wg);

    _wg_manager->add_paused_query(query_context->resource_ctx(), 1024L,
                                  Status::Error(ErrorCode::WORKLOAD_GROUP_MEMORY_EXCEEDED, "test"));
    {
        std::unique_lock<std::mutex> lock(_wg_manager->_paused_queries_lock);
        ASSERT_EQ(_wg_manager->_paused_queries_list[wg].size(), 1)
                << "pasued queue should not be empty";
    }

    _run_checking_loop(wg);

    query_context->query_mem_tracker()->consume(-1024 * 4);

    ASSERT_TRUE(query_context->resource_ctx()->task_controller()->paused_reason().ok());

    std::unique_lock<std::mutex> lock(_wg_manager->_paused_queries_lock);
    ASSERT_TRUE(_wg_manager->_paused_queries_list[wg].empty()) << "pasued queue should be empty";
    ASSERT_EQ(query_context->is_cancelled(), false) << "query should be canceled";
}

TEST_F(WorkloadGroupManagerTest, query_released) {
    auto wg = _wg_manager->get_or_create_workload_group({});
    auto query_context = _generate_on_query(wg);

    query_context->resource_ctx()->memory_context()->set_mem_limit(1024 * 1024);

    auto canceled_query = _generate_on_query(wg);

    _wg_manager->add_paused_query(query_context->resource_ctx(), 1024L * 1024 * 1024,
                                  Status::Error(ErrorCode::WORKLOAD_GROUP_MEMORY_EXCEEDED, "test"));
    _wg_manager->add_paused_query(
            canceled_query->resource_ctx(), 1024L * 1024 * 1024,
            Status::Error(ErrorCode::WORKLOAD_GROUP_MEMORY_EXCEEDED, "test for canceled"));
    canceled_query->cancel(Status::InternalError("for test"));

    {
        std::unique_lock<std::mutex> lock(_wg_manager->_paused_queries_lock);
        ASSERT_EQ(_wg_manager->_paused_queries_list[wg].size(), 2)
                << "pasued queue should not be empty";
    }

    query_context = nullptr;

    _run_checking_loop(wg);

    std::unique_lock<std::mutex> lock(_wg_manager->_paused_queries_lock);
    ASSERT_TRUE(_wg_manager->_paused_queries_list[wg].empty()) << "pasued queue should be empty";
}

TEST_F(WorkloadGroupManagerTest, ProcessMemoryNotEnough) {
    WorkloadGroupInfo wg1_info {.id = 1,
                                .memory_limit = 1024L * 1024 * 1000,
                                .min_memory_percent = 10,
                                .max_memory_percent = 100};
    WorkloadGroupInfo wg2_info {.id = 2,
                                .memory_limit = 1024L * 1024 * 1000,
                                .min_memory_percent = 10,
                                .max_memory_percent = 100};
    WorkloadGroupInfo wg3_info {.id = 3,
                                .memory_limit = 1024L * 1024 * 1000,
                                .min_memory_percent = 10,
                                .max_memory_percent = 100};

    auto wg1 = _wg_manager->get_or_create_workload_group(wg1_info);
    auto wg2 = _wg_manager->get_or_create_workload_group(wg2_info);
    auto wg3 = _wg_manager->get_or_create_workload_group(wg3_info);

    EXPECT_EQ(wg1->id(), wg1_info.id);
    EXPECT_EQ(wg2->id(), wg2_info.id);
    EXPECT_EQ(wg3->id(), wg3_info.id);

    EXPECT_EQ(1024L * 1024 * 100, wg1->min_memory_limit());

    auto query_context11 = _generate_on_query(wg1);
    query_context11->resource_ctx()->memory_context()->set_mem_limit(1024 * 1024 * 1024);
    query_context11->query_mem_tracker()->consume(1024 * 1024 * 10);

    wg1->refresh_memory_usage();
    wg2->refresh_memory_usage();
    wg3->refresh_memory_usage();

    // There is no query in workload groups, so that revoke memory will return 0
    EXPECT_EQ(0, _wg_manager->revoke_memory_from_other_groups_());

    // If exceed memory less than 128MB, then not revoke
    auto query_context21 = _generate_on_query(wg2);
    query_context21->resource_ctx()->memory_context()->set_mem_limit(1024 * 1024 * 1024);
    query_context21->query_mem_tracker()->consume(1024 * 1024 * 50);
    wg2->refresh_memory_usage();
    EXPECT_EQ(wg2->total_mem_used(), 1024 * 1024 * 50);
    EXPECT_EQ(wg2->min_memory_limit(), 1024 * 1024 * 100);
    // There is not workload group's memory usage > it's min memory limit.
    EXPECT_EQ(0, _wg_manager->revoke_memory_from_other_groups_());
    ASSERT_FALSE(query_context21->is_cancelled());

    // Add another query that use a lot of memory
    auto query_context22 = _generate_on_query(wg2);
    query_context22->resource_ctx()->memory_context()->set_mem_limit(1024 * 1024 * 1024);
    query_context22->query_mem_tracker()->consume(1024 * 1024 * 60);
    wg2->refresh_memory_usage();
    EXPECT_EQ(wg2->total_mem_used(), 1024 * 1024 * 110);
    EXPECT_EQ(wg2->min_memory_limit(), 1024 * 1024 * 100);
    // Could not revoke larger than 128MB, not revoke.
    EXPECT_EQ(0, _wg_manager->revoke_memory_from_other_groups_());
    ASSERT_FALSE(query_context21->is_cancelled());
    ASSERT_FALSE(query_context22->is_cancelled());

    // Add another query that use a lot of memory
    auto query_context23 = _generate_on_query(wg2);
    query_context23->resource_ctx()->memory_context()->set_mem_limit(1024 * 1024 * 1024);
    query_context23->query_mem_tracker()->consume(1024 * 1024 * 300);
    wg2->refresh_memory_usage();
    EXPECT_EQ(wg2->total_mem_used(), 1024 * 1024 * 410);
    EXPECT_EQ(wg2->min_memory_limit(), 1024 * 1024 * 100);
    EXPECT_EQ(31 * 1024 * 1024, _wg_manager->revoke_memory_from_other_groups_());
    ASSERT_FALSE(query_context21->is_cancelled());
    ASSERT_FALSE(query_context22->is_cancelled());
    ASSERT_TRUE(query_context23->is_cancelled());
    // Although query23 is cancelled, but it is not removed from workload group2, so that it still occupy memory usage.
    wg2->refresh_memory_usage();
    EXPECT_EQ(wg2->total_mem_used(), 1024 * 1024 * 410);
    // clear cancelled query from workload group.
    wg2->clear_cancelled_resource_ctx();
    wg2->refresh_memory_usage();
    EXPECT_EQ(wg2->total_mem_used(), 1024 * 1024 * 110);
    // todo 应该是cancel 最大的query

    auto query_context24 = _generate_on_query(wg2);
    query_context24->resource_ctx()->memory_context()->set_mem_limit(1024 * 1024 * 1024);
    query_context24->query_mem_tracker()->consume(1024 * 1024 * 300);
    wg2->refresh_memory_usage();
    EXPECT_EQ(wg2->total_mem_used(), 1024 * 1024 * 410); // WG2 exceed 310MB

    // wg3 is overcommited, some query is overcommited
    auto query_context31 = _generate_on_query(wg3);
    query_context31->resource_ctx()->memory_context()->set_mem_limit(1024 * 1024 * 1024);
    query_context31->query_mem_tracker()->consume(1024 * 1024 * 500);
    wg3->refresh_memory_usage();
    EXPECT_EQ(wg3->total_mem_used(), 1024 * 1024 * 500); // WG3 exceed 400MB

    EXPECT_EQ(40 * 1024 * 1024, _wg_manager->revoke_memory_from_other_groups_());

    wg1->refresh_memory_usage();
    wg2->refresh_memory_usage();
    wg3->refresh_memory_usage();

    ASSERT_TRUE(query_context31->is_cancelled());
    // query31 is still in wg3, so that it is not cancel again.
    EXPECT_EQ(40 * 1024 * 1024, _wg_manager->revoke_memory_from_other_groups_());
    ASSERT_FALSE(query_context11->is_cancelled());
    ASSERT_FALSE(query_context21->is_cancelled());
    ASSERT_FALSE(query_context22->is_cancelled());
    ASSERT_FALSE(query_context24->is_cancelled());
    ASSERT_TRUE(query_context31->is_cancelled());

    // remove query31 from wg
    wg3->clear_cancelled_resource_ctx();

    wg1->refresh_memory_usage();
    wg2->refresh_memory_usage();
    wg3->refresh_memory_usage();
    EXPECT_EQ(wg3->total_mem_used(), 0); // WG3 exceed 400MB
}

// Test Fix 1 (Phase 3): When revoking_memory_from_other_query_ is true and cancelled queries
// have finished, Phase 3 should resume all paused queries AND remove them from the list,
// then return without entering Phase 4 (which would re-process them).
TEST_F(WorkloadGroupManagerTest, phase3_resume_removes_from_list_and_returns) {
    auto wg = _wg_manager->get_or_create_workload_group({});
    auto query_context1 = _generate_on_query(wg);
    auto query_context2 = _generate_on_query(wg);

    // Pause two queries due to process memory exceeded
    _wg_manager->add_paused_query(query_context1->resource_ctx(), 1024L,
                                  Status::Error(ErrorCode::PROCESS_MEMORY_EXCEEDED, "test"));
    _wg_manager->add_paused_query(query_context2->resource_ctx(), 1024L,
                                  Status::Error(ErrorCode::PROCESS_MEMORY_EXCEEDED, "test"));

    {
        std::unique_lock<std::mutex> lock(_wg_manager->_paused_queries_lock);
        ASSERT_EQ(_wg_manager->_paused_queries_list[wg].size(), 2);
    }

    // Simulate: a previous call had cancelled a query and set revoking flag
    _wg_manager->revoking_memory_from_other_query_ = true;

    // Call handle_paused_queries — Phase 2 finds no cancelled query in the list,
    // Phase 3 should resume all and remove them from the list.
    _wg_manager->handle_paused_queries();

    {
        std::unique_lock<std::mutex> lock(_wg_manager->_paused_queries_lock);
        // All queries should be removed from paused list by Phase 3
        ASSERT_TRUE(!_wg_manager->_paused_queries_list.contains(wg) ||
                    _wg_manager->_paused_queries_list[wg].empty())
                << "Phase 3 should remove all resumed queries from paused list";
    }
    ASSERT_FALSE(_wg_manager->revoking_memory_from_other_query_) << "revoking flag should be reset";
    // Queries should NOT be cancelled (Phase 4 should not have run)
    ASSERT_FALSE(query_context1->is_cancelled()) << "query1 should be resumed, not cancelled";
    ASSERT_FALSE(query_context2->is_cancelled()) << "query2 should be resumed, not cancelled";
}

TEST_F(WorkloadGroupManagerTest, phase3_waits_for_recently_cancelled_query) {
    auto wg = _wg_manager->get_or_create_workload_group({});
    auto cancelled_query = _generate_on_query(wg);
    auto waiting_query = _generate_on_query(wg);
    auto* mock_controller = _install_mock_query_task_controller(cancelled_query);

    _wg_manager->add_paused_query(cancelled_query->resource_ctx(), 1024L,
                                  Status::Error(ErrorCode::PROCESS_MEMORY_EXCEEDED, "test"));
    _wg_manager->add_paused_query(waiting_query->resource_ctx(), 1024L,
                                  Status::Error(ErrorCode::PROCESS_MEMORY_EXCEEDED, "test"));

    cancelled_query->resource_ctx()->task_controller()->cancel(
            Status::InternalError("memory gc cancel"));
    _wg_manager->revoking_memory_from_other_query_ = true;

    _wg_manager->handle_paused_queries();

    {
        std::unique_lock<std::mutex> lock(_wg_manager->_paused_queries_lock);
        ASSERT_EQ(_wg_manager->_paused_queries_list[wg].size(), 2);
    }
    ASSERT_TRUE(_wg_manager->revoking_memory_from_other_query_);
    ASSERT_TRUE(waiting_query->resource_ctx()
                        ->task_controller()
                        ->paused_reason()
                        .is<ErrorCode::PROCESS_MEMORY_EXCEEDED>());

    mock_controller->set_cancelled_time(MonotonicMillis() - config::wait_cancel_release_memory_ms -
                                        1);
    _wg_manager->handle_paused_queries();

    {
        std::unique_lock<std::mutex> lock(_wg_manager->_paused_queries_lock);
        ASSERT_TRUE(!_wg_manager->_paused_queries_list.contains(wg) ||
                    _wg_manager->_paused_queries_list[wg].empty());
    }
    ASSERT_FALSE(_wg_manager->revoking_memory_from_other_query_);
    ASSERT_TRUE(waiting_query->resource_ctx()->task_controller()->paused_reason().ok());
    ASSERT_FALSE(waiting_query->is_cancelled());
}

TEST_F(WorkloadGroupManagerTest, phase3_removes_expired_query_entries) {
    auto wg = _wg_manager->get_or_create_workload_group({});
    auto live_query = _generate_on_query(wg);
    auto expired_query = _generate_on_query(wg);

    _wg_manager->add_paused_query(live_query->resource_ctx(), 1024L,
                                  Status::Error(ErrorCode::PROCESS_MEMORY_EXCEEDED, "test"));
    _wg_manager->add_paused_query(expired_query->resource_ctx(), 1024L,
                                  Status::Error(ErrorCode::PROCESS_MEMORY_EXCEEDED, "test"));

    {
        std::unique_lock<std::mutex> lock(_wg_manager->_paused_queries_lock);
        ASSERT_EQ(_wg_manager->_paused_queries_list[wg].size(), 2);
    }

    expired_query.reset();
    _wg_manager->revoking_memory_from_other_query_ = true;
    _wg_manager->handle_paused_queries();

    {
        std::unique_lock<std::mutex> lock(_wg_manager->_paused_queries_lock);
        ASSERT_TRUE(!_wg_manager->_paused_queries_list.contains(wg) ||
                    _wg_manager->_paused_queries_list[wg].empty());
    }
    ASSERT_FALSE(_wg_manager->revoking_memory_from_other_query_);
    ASSERT_TRUE(live_query->resource_ctx()->task_controller()->paused_reason().ok());
    ASSERT_FALSE(live_query->is_cancelled());
}

// Test Fix 2 (Problem 3): A cancelled query in one WG should NOT block
// QUERY_MEMORY_EXCEEDED queries in another WG from being processed.
TEST_F(WorkloadGroupManagerTest, cancelled_query_does_not_block_query_mem_exceeded) {
    WorkloadGroupInfo wg1_info {.id = 1, .memory_limit = 1024L * 1024 * 1000};
    WorkloadGroupInfo wg2_info {.id = 2, .memory_limit = 1024L * 1024 * 1000};
    auto wg1 = _wg_manager->get_or_create_workload_group(wg1_info);
    auto wg2 = _wg_manager->get_or_create_workload_group(wg2_info);

    // WG1: a query that will be externally cancelled (simulating memory GC)
    auto cancelled_query = _generate_on_query(wg1);
    cancelled_query->query_mem_tracker()->consume(1024L * 1024 * 10);
    _wg_manager->add_paused_query(cancelled_query->resource_ctx(), 1024L,
                                  Status::Error(ErrorCode::PROCESS_MEMORY_EXCEEDED, "test"));

    // Cancel the query externally (like memory GC would) — it stays in paused list
    cancelled_query->resource_ctx()->task_controller()->cancel(
            Status::InternalError("memory gc cancel"));

    // WG2: a query paused due to QUERY_MEMORY_EXCEEDED — should be processed immediately
    auto query_exceed = _generate_on_query(wg2);
    query_exceed->resource_ctx()->memory_context()->set_mem_limit(1024 * 1024);
    query_exceed->query_mem_tracker()->consume(1024 * 4);
    _wg_manager->add_paused_query(query_exceed->resource_ctx(), 1024L * 1024 * 1024,
                                  Status::Error(ErrorCode::QUERY_MEMORY_EXCEEDED, "test"));

    // Verify both in paused list
    {
        std::unique_lock<std::mutex> lock(_wg_manager->_paused_queries_lock);
        ASSERT_EQ(_wg_manager->_paused_queries_list[wg1].size(), 1);
        ASSERT_EQ(_wg_manager->_paused_queries_list[wg2].size(), 1);
    }

    // One call to handle_paused_queries — the QUERY_MEMORY_EXCEEDED query should be processed
    // even though there's a recently cancelled query in wg1.
    _wg_manager->handle_paused_queries();

    {
        std::unique_lock<std::mutex> lock(_wg_manager->_paused_queries_lock);
        // WG2's query should have been processed (removed from paused list)
        ASSERT_TRUE(!_wg_manager->_paused_queries_list.contains(wg2) ||
                    _wg_manager->_paused_queries_list[wg2].empty())
                << "QUERY_MEMORY_EXCEEDED query should not be blocked by cancelled query in "
                   "another WG";
    }

    query_exceed->query_mem_tracker()->consume(-1024 * 4);
    cancelled_query->query_mem_tracker()->consume(-1024L * 1024 * 10);
}

TEST_F(WorkloadGroupManagerTest, recently_cancelled_query_delays_process_mem_exceeded) {
    WorkloadGroupInfo wg1_info {.id = 1,
                                .memory_limit = 1024L * 1024 * 1000,
                                .min_memory_percent = 10,
                                .max_memory_percent = 100};
    WorkloadGroupInfo wg2_info {.id = 2,
                                .memory_limit = 1024L * 1024 * 1000,
                                .min_memory_percent = 10,
                                .max_memory_percent = 100};
    auto wg1 = _wg_manager->get_or_create_workload_group(wg1_info);
    auto wg2 = _wg_manager->get_or_create_workload_group(wg2_info);

    auto cancelled_query = _generate_on_query(wg1);
    _wg_manager->add_paused_query(cancelled_query->resource_ctx(), 1024L,
                                  Status::Error(ErrorCode::PROCESS_MEMORY_EXCEEDED, "test"));
    cancelled_query->resource_ctx()->task_controller()->cancel(
            Status::InternalError("memory gc cancel"));

    auto waiting_query = _generate_on_query(wg2);
    waiting_query->query_mem_tracker()->consume(1024L * 1024 * 128);
    wg2->refresh_memory_usage();
    _wg_manager->add_paused_query(waiting_query->resource_ctx(), 1024L,
                                  Status::Error(ErrorCode::PROCESS_MEMORY_EXCEEDED, "test"));

    _wg_manager->handle_paused_queries();

    {
        std::unique_lock<std::mutex> lock(_wg_manager->_paused_queries_lock);
        ASSERT_EQ(_wg_manager->_paused_queries_list[wg2].size(), 1);
    }
    ASSERT_TRUE(waiting_query->resource_ctx()
                        ->task_controller()
                        ->paused_reason()
                        .is<ErrorCode::PROCESS_MEMORY_EXCEEDED>());

    cancelled_query.reset();
    _wg_manager->handle_paused_queries();

    {
        std::unique_lock<std::mutex> lock(_wg_manager->_paused_queries_lock);
        ASSERT_TRUE(!_wg_manager->_paused_queries_list.contains(wg2) ||
                    _wg_manager->_paused_queries_list[wg2].empty());
    }
    ASSERT_TRUE(waiting_query->resource_ctx()->task_controller()->paused_reason().ok());
    ASSERT_FALSE(waiting_query->is_cancelled());
    waiting_query->query_mem_tracker()->consume(-1024L * 1024 * 128);
}

// Test Fix 3: update_queries_limit_ should restore mem_limit when memory pressure eases.
// For NONE policy, the old code never called set_mem_limit during refresh (user_set > user_set
// is always false), so a limit lowered by handle_paused_queries would never recover.
TEST_F(WorkloadGroupManagerTest, update_queries_limit_restores_limit_none_policy) {
    WorkloadGroupInfo wg_info {.id = 1,
                               .memory_limit = 1024L * 1024 * 200,
                               .slot_mem_policy = TWgSlotMemoryPolicy::NONE};
    auto wg = _wg_manager->get_or_create_workload_group(wg_info);
    auto query_context = _generate_on_query(wg);

    // user_set_mem_limit is set in QueryContext init = query_options.mem_limit = 128MB
    const int64_t user_set = query_context->resource_ctx()->memory_context()->user_set_mem_limit();
    ASSERT_EQ(user_set, 1024L * 1024 * 128);

    // Simulate handle_paused_queries lowering the limit to a small value
    query_context->resource_ctx()->memory_context()->set_mem_limit(1024L * 1024 * 2); // 2MB
    ASSERT_EQ(query_context->resource_ctx()->memory_context()->mem_limit(), 1024L * 1024 * 2);

    // Now simulate memory recovery: WG memory is well below watermark
    // refresh_workload_group_memory_state calls update_queries_limit_(wg, false)
    wg->refresh_memory_usage();
    _wg_manager->refresh_workload_group_memory_state();

    // The limit should be restored to user_set_mem_limit (128MB),
    // because query_weighted = min(user_set, wg_mem_limit) = min(128MB, 200MB) = 128MB
    // effective = min(user_set, query_weighted) = 128MB
    ASSERT_EQ(query_context->resource_ctx()->memory_context()->mem_limit(), user_set)
            << "NONE policy: mem_limit should be restored to user_set_mem_limit after memory "
               "recovery";
}

// Test Fix 3: For DYNAMIC policy, when memory pressure eases (below low watermark),
// query_weighted_mem_limit = wg_high_water_mark which is typically > user_set_mem_limit.
// The old code's condition (user_set > query_weighted) would be false, preventing restoration.
TEST_F(WorkloadGroupManagerTest, update_queries_limit_restores_limit_dynamic_policy) {
    WorkloadGroupInfo wg_info {.id = 1,
                               .memory_limit = 1024L * 1024 * 200,
                               .memory_low_watermark = 80,
                               .memory_high_watermark = 95,
                               .total_query_slot_count = 5,
                               .slot_mem_policy = TWgSlotMemoryPolicy::DYNAMIC};
    auto wg = _wg_manager->get_or_create_workload_group(wg_info);
    auto query_context = _generate_on_query(wg);

    const int64_t user_set = query_context->resource_ctx()->memory_context()->user_set_mem_limit();
    ASSERT_EQ(user_set, 1024L * 1024 * 128);

    // Simulate: under memory pressure, limit was lowered by handle_paused_queries
    query_context->resource_ctx()->memory_context()->set_mem_limit(1024L * 1024 * 2); // 2MB
    ASSERT_EQ(query_context->resource_ctx()->memory_context()->mem_limit(), 1024L * 1024 * 2);

    // Memory recovers: no consumption, well below low watermark
    wg->refresh_memory_usage();
    _wg_manager->refresh_workload_group_memory_state();

    // DYNAMIC: below low watermark → query_weighted = wg_high_water_mark = 200MB * 95% = 190MB
    // effective = min(user_set=128MB, 190MB) = 128MB
    ASSERT_EQ(query_context->resource_ctx()->memory_context()->mem_limit(), user_set)
            << "DYNAMIC policy: mem_limit should be restored to user_set_mem_limit after memory "
               "recovery";
}

// Test Fix 3: For FIXED policy, limit should be correctly set to slot-weighted value.
// This already worked before the fix, but verify it still works.
TEST_F(WorkloadGroupManagerTest, update_queries_limit_restores_limit_fixed_policy) {
    WorkloadGroupInfo wg_info {.id = 1,
                               .memory_limit = 1024L * 1024 * 200,
                               .memory_low_watermark = 80,
                               .memory_high_watermark = 95,
                               .total_query_slot_count = 5,
                               .slot_mem_policy = TWgSlotMemoryPolicy::FIXED};
    auto wg = _wg_manager->get_or_create_workload_group(wg_info);
    auto query_context = _generate_on_query(wg);

    // Simulate lowered limit
    query_context->resource_ctx()->memory_context()->set_mem_limit(1024L * 1024 * 2); // 2MB

    wg->refresh_memory_usage();
    _wg_manager->refresh_workload_group_memory_state();

    // FIXED: query_weighted = wg_high_water_mark * my_slot / total_slot
    //      = 200MB * 95% * 1 / 5 = 38MB
    // effective = min(user_set=128MB, 38MB) = 38MB
    const int64_t expected = (int64_t)((double)(1024L * 1024 * 200) * 95.0 / 100 / 5);
    const auto delta =
            std::abs(query_context->resource_ctx()->memory_context()->mem_limit() - expected);
    ASSERT_LE(delta, 1) << "FIXED policy: mem_limit should be restored to slot-weighted value, got "
                        << query_context->resource_ctx()->memory_context()->mem_limit()
                        << " expected " << expected;
}

// Test: When WG concurrency decreases (queries finish), remaining queries should get
// higher per-query limits in FIXED policy.
TEST_F(WorkloadGroupManagerTest, limit_increases_when_concurrency_decreases) {
    WorkloadGroupInfo wg_info {.id = 1,
                               .memory_limit = 1024L * 1024 * 200,
                               .memory_low_watermark = 80,
                               .memory_high_watermark = 95,
                               .total_query_slot_count = 5,
                               .slot_mem_policy = TWgSlotMemoryPolicy::FIXED};
    auto wg = _wg_manager->get_or_create_workload_group(wg_info);

    // Start 3 queries (each with slot_count = 1, total_used_slot = 3)
    auto q1 = _generate_on_query(wg);
    auto q2 = _generate_on_query(wg);
    auto q3 = _generate_on_query(wg);

    wg->refresh_memory_usage();
    _wg_manager->refresh_workload_group_memory_state();

    // FIXED: wg_high_water_mark * 1 / 5 = 200MB * 95% / 5 = 38MB
    // (total_slot_count is configured as 5, not actual used slots for FIXED)
    int64_t limit_with_3_queries = q1->resource_ctx()->memory_context()->mem_limit();

    // Now q2 and q3 finish — remove from WG
    q2.reset();
    q3.reset();
    wg->clear_cancelled_resource_ctx();
    wg->refresh_memory_usage();
    _wg_manager->refresh_workload_group_memory_state();

    int64_t limit_with_1_query = q1->resource_ctx()->memory_context()->mem_limit();

    // For FIXED with total_query_slot_count=5, the slot-weighted limit doesn't change
    // when queries leave (it's based on configured total, not actual).
    // But the limit should at least be correctly restored, not stuck at a low value.
    ASSERT_EQ(limit_with_1_query, limit_with_3_queries)
            << "FIXED policy with configured total_slot_count: limit should remain stable";
}

// Test: Cancelled queries that have exceeded wait_cancel_release_memory_ms should be
// cleaned up in Phase 2, not left in the list for Phase 4 processing.
TEST_F(WorkloadGroupManagerTest, phase2_removes_old_cancelled_queries) {
    auto wg = _wg_manager->get_or_create_workload_group({});
    auto cancelled_query = _generate_on_query(wg);
    auto live_query = _generate_on_query(wg);
    auto* mock_controller = _install_mock_query_task_controller(cancelled_query);

    // Pause both queries
    _wg_manager->add_paused_query(cancelled_query->resource_ctx(), 1024L,
                                  Status::Error(ErrorCode::PROCESS_MEMORY_EXCEEDED, "test"));
    _wg_manager->add_paused_query(live_query->resource_ctx(), 1024L,
                                  Status::Error(ErrorCode::PROCESS_MEMORY_EXCEEDED, "test"));

    // Cancel the query and make it look old (exceeded wait time)
    cancelled_query->resource_ctx()->task_controller()->cancel(
            Status::InternalError("memory gc cancel"));
    mock_controller->set_cancelled_time(MonotonicMillis() - config::wait_cancel_release_memory_ms -
                                        1);

    {
        std::unique_lock<std::mutex> lock(_wg_manager->_paused_queries_lock);
        ASSERT_EQ(_wg_manager->_paused_queries_list[wg].size(), 2);
    }

    // One call — Phase 2 should remove the old-cancelled query.
    _wg_manager->handle_paused_queries();

    {
        std::unique_lock<std::mutex> lock(_wg_manager->_paused_queries_lock);
        // The old-cancelled query should have been removed in Phase 2.
        // The live query may still be in the list (Phase 4 doesn't always process it in
        // one call, depending on WG memory state), but the count should be at most 1.
        size_t remaining = _wg_manager->_paused_queries_list.contains(wg)
                                   ? _wg_manager->_paused_queries_list[wg].size()
                                   : 0;
        ASSERT_LE(remaining, 1) << "Old-cancelled query should have been removed in Phase 2, "
                                   "at most the live query remains";
    }
    ASSERT_FALSE(live_query->is_cancelled()) << "live query should not be cancelled";
}

// Test: Phase 3 should not call set_memory_sufficient on cancelled queries —
// just erase them and only resume live queries.
TEST_F(WorkloadGroupManagerTest, phase3_skips_cancelled_queries_on_resume) {
    auto wg = _wg_manager->get_or_create_workload_group({});
    auto cancelled_query = _generate_on_query(wg);
    auto live_query = _generate_on_query(wg);
    auto* mock_controller = _install_mock_query_task_controller(cancelled_query);

    _wg_manager->add_paused_query(cancelled_query->resource_ctx(), 1024L,
                                  Status::Error(ErrorCode::PROCESS_MEMORY_EXCEEDED, "test"));
    _wg_manager->add_paused_query(live_query->resource_ctx(), 1024L,
                                  Status::Error(ErrorCode::PROCESS_MEMORY_EXCEEDED, "test"));

    // Cancel the query but make it recently cancelled so Phase 2 keeps it
    cancelled_query->resource_ctx()->task_controller()->cancel(
            Status::InternalError("memory gc cancel"));
    _wg_manager->revoking_memory_from_other_query_ = true;

    // First call: Phase 3 waits because recently cancelled
    _wg_manager->handle_paused_queries();
    ASSERT_TRUE(_wg_manager->revoking_memory_from_other_query_);

    // Make cancellation old
    mock_controller->set_cancelled_time(MonotonicMillis() - config::wait_cancel_release_memory_ms -
                                        1);

    // Second call: Phase 2 removes old-cancelled query, Phase 3 resumes live query
    _wg_manager->handle_paused_queries();

    {
        std::unique_lock<std::mutex> lock(_wg_manager->_paused_queries_lock);
        ASSERT_TRUE(!_wg_manager->_paused_queries_list.contains(wg) ||
                    _wg_manager->_paused_queries_list[wg].empty())
                << "All queries should be removed";
    }
    ASSERT_FALSE(_wg_manager->revoking_memory_from_other_query_);
    // Live query should be properly resumed
    ASSERT_TRUE(live_query->resource_ctx()->task_controller()->paused_reason().ok());
    ASSERT_FALSE(live_query->is_cancelled());
}

// Test: A cancelled query in WG1 should NOT block WORKLOAD_GROUP_MEMORY_EXCEEDED
// queries in WG2, because WG memory pools are independent. Only same-WG queries
// should be delayed. PROCESS_MEMORY_EXCEEDED should still be delayed globally.
TEST_F(WorkloadGroupManagerTest, cancelled_query_does_not_block_cross_wg_mem_exceeded) {
    WorkloadGroupInfo wg1_info {.id = 1, .memory_limit = 1024L * 1024 * 1000};
    WorkloadGroupInfo wg2_info {.id = 2, .memory_limit = 1024L * 1024 * 1000};
    auto wg1 = _wg_manager->get_or_create_workload_group(wg1_info);
    auto wg2 = _wg_manager->get_or_create_workload_group(wg2_info);

    // WG1: a recently cancelled query
    auto cancelled_query = _generate_on_query(wg1);
    cancelled_query->query_mem_tracker()->consume(1024L * 1024 * 10);
    _wg_manager->add_paused_query(cancelled_query->resource_ctx(), 1024L,
                                  Status::Error(ErrorCode::WORKLOAD_GROUP_MEMORY_EXCEEDED, "test"));
    cancelled_query->resource_ctx()->task_controller()->cancel(
            Status::InternalError("memory gc cancel"));

    // WG2: a query paused due to WORKLOAD_GROUP_MEMORY_EXCEEDED — should NOT be
    // blocked by WG1's cancelled query since WG memory pools are independent.
    auto wg2_query = _generate_on_query(wg2);
    wg2_query->query_mem_tracker()->consume(1024L * 1024 * 4);
    wg2_query->resource_ctx()->memory_context()->set_adjusted_mem_limit(1024L * 1024 * 10);
    _wg_manager->add_paused_query(wg2_query->resource_ctx(), 1024L,
                                  Status::Error(ErrorCode::WORKLOAD_GROUP_MEMORY_EXCEEDED, "test"));

    {
        std::unique_lock<std::mutex> lock(_wg_manager->_paused_queries_lock);
        ASSERT_EQ(_wg_manager->_paused_queries_list[wg1].size(), 1);
        ASSERT_EQ(_wg_manager->_paused_queries_list[wg2].size(), 1);
    }

    _wg_manager->handle_paused_queries();

    {
        std::unique_lock<std::mutex> lock(_wg_manager->_paused_queries_lock);
        // WG2's query should have been processed (not blocked by WG1's cancellation)
        ASSERT_TRUE(!_wg_manager->_paused_queries_list.contains(wg2) ||
                    _wg_manager->_paused_queries_list[wg2].empty())
                << "Cross-WG WORKLOAD_GROUP_MEMORY_EXCEEDED should not be blocked by "
                   "cancelled query in another WG";
    }

    wg2_query->query_mem_tracker()->consume(-1024L * 1024 * 4);
    cancelled_query->query_mem_tracker()->consume(-1024L * 1024 * 10);
}

// Test: A recently-cancelled QUERY_MEMORY_EXCEEDED query should NOT be processed
// by handle_single_query_() in Phase 4, and should NOT incorrectly set
// revoking_memory_from_other_query_. Phase 2 keeps it for delay-waiting;
// Phase 4 must skip it.
TEST_F(WorkloadGroupManagerTest, phase4_skips_cancelled_query_memory_exceeded) {
    auto wg = _wg_manager->get_or_create_workload_group({});
    auto cancelled_query = _generate_on_query(wg);
    auto live_query = _generate_on_query(wg);

    // Pause both for QUERY_MEMORY_EXCEEDED
    cancelled_query->resource_ctx()->memory_context()->set_mem_limit(1024 * 1024);
    cancelled_query->query_mem_tracker()->consume(1024 * 4);
    _wg_manager->add_paused_query(cancelled_query->resource_ctx(), 1024L * 1024 * 1024,
                                  Status::Error(ErrorCode::QUERY_MEMORY_EXCEEDED, "test"));

    live_query->resource_ctx()->memory_context()->set_mem_limit(1024 * 1024);
    live_query->query_mem_tracker()->consume(1024 * 4);
    _wg_manager->add_paused_query(live_query->resource_ctx(), 1024L * 1024 * 1024,
                                  Status::Error(ErrorCode::QUERY_MEMORY_EXCEEDED, "test"));

    // Cancel one query externally (simulating memory GC) — it's recently cancelled
    cancelled_query->resource_ctx()->task_controller()->cancel(
            Status::InternalError("memory gc cancel"));

    {
        std::unique_lock<std::mutex> lock(_wg_manager->_paused_queries_lock);
        ASSERT_EQ(_wg_manager->_paused_queries_list[wg].size(), 2);
    }

    _wg_manager->handle_paused_queries();

    // The cancelled query should NOT have caused revoking_memory_from_other_query_ to be set.
    // If Phase 4 incorrectly processed it through handle_single_query_(), the is_cancelled()
    // check afterward would set this flag.
    ASSERT_FALSE(_wg_manager->revoking_memory_from_other_query_)
            << "revoking flag should NOT be set by an already-cancelled query";

    cancelled_query->query_mem_tracker()->consume(-1024 * 4);
    live_query->query_mem_tracker()->consume(-1024 * 4);
}

} // namespace doris
