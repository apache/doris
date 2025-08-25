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

#include <cmath>
#include <cstdlib>
#include <memory>

#include "common/config.h"
#include "common/status.h"
#include "olap/olap_define.h"
#include "pipeline/pipeline_tracing.h"
#include "runtime/exec_env.h"
#include "runtime/query_context.h"
#include "runtime/runtime_query_statistics_mgr.h"
#include "runtime/workload_group/workload_group.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris {

class WorkloadGroupManagerTest : public testing::Test {
public:
protected:
    void SetUp() override {
        _wg_manager = std::make_unique<WorkloadGroupMgr>();
        EXPECT_EQ(system("rm -rf ./wg_test_run && mkdir -p ./wg_test_run"), 0);

        std::vector<doris::StorePath> paths;
        std::string path = std::filesystem::absolute("./wg_test_run").string();
        auto olap_res = doris::parse_conf_store_paths(path, &paths);
        EXPECT_TRUE(olap_res.ok()) << olap_res.to_string();

        std::vector<doris::StorePath> spill_paths;
        olap_res = doris::parse_conf_store_paths(path, &spill_paths);
        ASSERT_TRUE(olap_res.ok()) << olap_res.to_string();
        std::unordered_map<std::string, std::unique_ptr<vectorized::SpillDataDir>> spill_store_map;
        for (const auto& spill_path : spill_paths) {
            spill_store_map.emplace(
                    spill_path.path,
                    std::make_unique<vectorized::SpillDataDir>(
                            spill_path.path, spill_path.capacity_bytes, spill_path.storage_medium));
        }

        ExecEnv::GetInstance()->_runtime_query_statistics_mgr = new RuntimeQueryStatisticsMgr();
        ExecEnv::GetInstance()->_spill_stream_mgr =
                new vectorized::SpillStreamManager(std::move(spill_store_map));
        auto st = ExecEnv::GetInstance()->_spill_stream_mgr->init();
        EXPECT_TRUE(st.ok()) << "init spill stream manager failed: " << st.to_string();
        ExecEnv::GetInstance()->_pipeline_tracer_ctx =
                std::make_unique<pipeline::PipelineTracerContext>();

        config::spill_in_paused_queue_timeout_ms = 2000;
        doris::ExecEnv::GetInstance()->set_memtable_memory_limiter(new MemTableMemoryLimiter());
    }
    void TearDown() override {
        _wg_manager.reset();
        ExecEnv::GetInstance()->_runtime_query_statistics_mgr->stop_report_thread();
        SAFE_DELETE(ExecEnv::GetInstance()->_runtime_query_statistics_mgr);

        EXPECT_EQ(system("rm -rf ./wg_test_run"), 0);
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

    // Query was not cancelled, because the query's limit is bigger than the wg's limit and the wg's policy is NONE.
    ASSERT_FALSE(query_context->is_cancelled());
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

} // namespace doris
