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

#include <fmt/format.h>
#include <gen_cpp/BackendService_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <chrono>
#include <cmath>
#include <filesystem>
#include <functional>
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "cpp/sync_point.h"
#include "exec/pipeline/dependency.h"
#include "exec/spill/spill_file_manager.h"
#include "load/memtable/memtable_memory_limiter.h"
#include "runtime/exec_env.h"
#include "runtime/memory/global_memory_arbitrator.h"
#include "runtime/query_context.h"
#include "runtime/runtime_query_statistics_mgr.h"
#include "runtime/thread_context.h"
#include "runtime/workload_group/workload_group.h"
#include "storage/adaptive_thread_pool_controller.h"
#include "storage/olap_define.h"
#include "storage/storage_engine.h"
#include "testutil/mock/mock_query_task_controller.h"
#include "util/defer_op.h"
#include "util/mem_info.h"
#include "util/threadpool.h"

namespace doris {

// Process memory limit for the PROCESS_MEMORY_EXCEEDED cases. The workload group memory limits
// are derived from the process memory limit whenever their usage is refreshed, so it is set before
// the workload group is created and kept; process memory pressure is simulated by the soft memory
// limit, the memory growth since the last refresh and the system available memory instead.
static constexpr int64_t kProcessMemLimitForWg = 1024L * 1024 * 1000;
// System available memory far above the warning water mark, so that only the process memory
// limits decide the process memory predicates unless a case lowers it on purpose.
static constexpr int64_t kLargeSysMemAvailable = std::numeric_limits<int64_t>::max() / 2;
static constexpr int64_t kProcessPausedReserveSize = 1024L;

class WorkloadGroupManagerTest : public testing::Test {
public:
protected:
    void SetUp() override {
        _original_mem_limit = MemInfo::mem_limit();
        _original_soft_mem_limit = MemInfo::soft_mem_limit();
        // The process memory predicates also depend on the system available memory, which is
        // read from the runner otherwise: a runner below the water marks reports pressure before
        // a case sets any process memory limit.
        _original_sys_mem_available =
                MemInfo::set_sys_mem_available_for_test(kLargeSysMemAvailable);
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
        config::spill_in_paused_queue_timeout_ms = 2000;
        doris::ExecEnv::GetInstance()->set_memtable_memory_limiter(new MemTableMemoryLimiter());
    }
    void TearDown() override {
        for (auto& [query, memory] : _consumed_memory) {
            query->query_mem_tracker()->consume(-memory);
        }
        _consumed_memory.clear();
        MemInfo::set_mem_limit_for_test(_original_mem_limit);
        MemInfo::set_soft_mem_limit_for_test(_original_soft_mem_limit);
        MemInfo::set_sys_mem_available_for_test(_original_sys_mem_available);
        GlobalMemoryArbitrator::reset_refresh_interval_memory_growth();
        _wg_manager.reset();
        ExecEnv::GetInstance()->spill_file_mgr()->stop();
        SAFE_DELETE(ExecEnv::GetInstance()->_spill_file_mgr);
        ExecEnv::GetInstance()->_runtime_query_statistics_mgr->stop_report_thread();
        SAFE_DELETE(ExecEnv::GetInstance()->_runtime_query_statistics_mgr);

        std::error_code ec;
        std::filesystem::remove_all(_test_dir, ec);
        EXPECT_FALSE(ec) << "Failed to remove " << _test_dir << ": " << ec.message();
        config::spill_in_paused_queue_timeout_ms = _spill_in_paused_queue_timeout_ms;
        doris::ExecEnv::GetInstance()->set_memtable_memory_limiter(nullptr);
    }

private:
    std::shared_ptr<QueryContext> _generate_on_query(std::shared_ptr<WorkloadGroup>& wg,
                                                     int64_t mem_limit = 1024L * 1024 * 128,
                                                     bool has_mem_limit = false) {
        TQueryOptions query_options;
        query_options.query_type = TQueryType::SELECT;
        query_options.mem_limit = mem_limit;
        query_options.__isset.mem_limit = has_mem_limit;
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

    void _run_checking_loop(const std::shared_ptr<WorkloadGroup>& wg, size_t check_times = 300) {
        CountDownLatch latch(1);
        while (check_times > 0) {
            --check_times;
            _wg_manager->handle_paused_queries();
            if (!_wg_manager->_paused_queries_list.contains(wg) ||
                _wg_manager->_paused_queries_list[wg].empty()) {
                break;
            }
            latch.wait_for(std::chrono::milliseconds(config::memory_maintenance_sleep_time_ms));
        }
    }

    // Helpers for the PROCESS_MEMORY_EXCEEDED cases.

    // A workload group whose min memory limit is 100 MiB, so that a query consuming more than
    // that routes into handle_single_query_ directly, and a smaller one is routed through the
    // other workload groups first.
    std::shared_ptr<WorkloadGroup> _create_wg_with_min_memory(uint64_t id) {
        MemInfo::set_mem_limit_for_test(kProcessMemLimitForWg);
        WorkloadGroupInfo wg_info {.id = id,
                                   .memory_limit = kProcessMemLimitForWg,
                                   .min_memory_percent = 10,
                                   .max_memory_percent = 100};
        auto wg = _wg_manager->get_or_create_workload_group(wg_info);
        EXPECT_EQ(wg->min_memory_limit(), 1024L * 1024 * 100);
        return wg;
    }

    // A query on `wg` that consumes `memory` bytes until TearDown.
    std::shared_ptr<QueryContext> _create_query_with_memory(std::shared_ptr<WorkloadGroup>& wg,
                                                            int64_t memory) {
        auto query = _generate_on_query(wg);
        query->query_mem_tracker()->consume(memory);
        _consumed_memory.emplace_back(query, memory);
        wg->refresh_memory_usage();
        return query;
    }

    // Release `memory` bytes of `query` without refreshing the usage of its workload group:
    // total_mem_used() keeps the value cached by the last refresh, as it does in production
    // until the next maintenance round or revocation refreshes it.
    void _release_query_memory(const std::shared_ptr<QueryContext>& query, int64_t memory) {
        query->query_mem_tracker()->consume(-memory);
        _consumed_memory.emplace_back(query, -memory);
    }

    // The process memory predicates are controlled from both sides in the following helpers:
    // the process memory usage is the vm_rss of PerfCounters (never refreshed in a unit test)
    // plus the memory growth since the last refresh, which the helpers set; the system available
    // memory is set in SetUp far above the water marks. The process memory limit is kept at
    // kProcessMemLimitForWg, which refresh_memory_usage() derives the workload group limits from.

    // Process soft memory limit is exceeded, hard memory limit is not.
    static void _exceed_process_soft_mem_limit() {
        GlobalMemoryArbitrator::reset_refresh_interval_memory_growth();
        MemInfo::set_mem_limit_for_test(kProcessMemLimitForWg);
        MemInfo::set_soft_mem_limit_for_test(1);
        ASSERT_TRUE(GlobalMemoryArbitrator::is_exceed_soft_mem_limit(kProcessPausedReserveSize));
        ASSERT_FALSE(GlobalMemoryArbitrator::is_exceed_hard_mem_limit());
    }

    // Both the soft and the hard process memory limits are exceeded: the process has grown by
    // its whole memory limit since the last refresh.
    static void _exceed_process_hard_mem_limit() {
        MemInfo::set_mem_limit_for_test(kProcessMemLimitForWg);
        MemInfo::set_soft_mem_limit_for_test(kProcessMemLimitForWg);
        GlobalMemoryArbitrator::refresh_interval_memory_growth = kProcessMemLimitForWg;
        ASSERT_TRUE(GlobalMemoryArbitrator::is_exceed_hard_mem_limit());
    }

    // Neither process memory limit is exceeded: the process memory limit has its whole headroom
    // and the system available memory is far above the warning water mark.
    static void _relieve_process_mem_limit() {
        GlobalMemoryArbitrator::reset_refresh_interval_memory_growth();
        MemInfo::set_mem_limit_for_test(kProcessMemLimitForWg);
        MemInfo::set_soft_mem_limit_for_test(kProcessMemLimitForWg);
        MemInfo::set_sys_mem_available_for_test(kLargeSysMemAvailable);
        ASSERT_FALSE(GlobalMemoryArbitrator::is_exceed_soft_mem_limit(kProcessPausedReserveSize));
    }

    void _pause_for_process_memory(const std::shared_ptr<QueryContext>& query) {
        _wg_manager->add_paused_query(query->resource_ctx(), kProcessPausedReserveSize,
                                      Status::Error(ErrorCode::PROCESS_MEMORY_EXCEEDED, "test"));
    }

    size_t _paused_query_count(const std::shared_ptr<WorkloadGroup>& wg) {
        std::unique_lock<std::mutex> lock(_wg_manager->_paused_queries_lock);
        if (!_wg_manager->_paused_queries_list.contains(wg)) {
            return 0;
        }
        return _wg_manager->_paused_queries_list[wg].size();
    }

    // The reservation recorded for the single paused query of `wg`.
    int64_t _recorded_reserve_size(const std::shared_ptr<WorkloadGroup>& wg) {
        std::unique_lock<std::mutex> lock(_wg_manager->_paused_queries_lock);
        auto& queries = _wg_manager->_paused_queries_list[wg];
        EXPECT_EQ(queries.size(), 1);
        return queries.begin()->reserve_size_;
    }

    // Backdate the wait of the single paused query of `wg` by `ms` instead of sleeping, the timer
    // is monotonic: both its entry and the wait start that its task controller carries across a
    // resume are moved back.
    void _backdate_paused_query(const std::shared_ptr<WorkloadGroup>& wg, int64_t ms) {
        std::unique_lock<std::mutex> lock(_wg_manager->_paused_queries_lock);
        auto& queries = _wg_manager->_paused_queries_list[wg];
        ASSERT_EQ(queries.size(), 1);
        auto node = queries.extract(queries.begin());
        node.value().enqueue_at -= ms;
        auto resource_ctx = node.value().resource_ctx_.lock();
        ASSERT_TRUE(resource_ctx != nullptr);
        resource_ctx->task_controller()->end_process_memory_wait();
        ASSERT_EQ(
                resource_ctx->task_controller()->start_process_memory_wait(node.value().enqueue_at),
                node.value().enqueue_at);
        queries.insert(std::move(node));
    }

    // How long the single paused query of `wg` has waited.
    int64_t _paused_elapsed_time(const std::shared_ptr<WorkloadGroup>& wg) {
        std::unique_lock<std::mutex> lock(_wg_manager->_paused_queries_lock);
        auto& queries = _wg_manager->_paused_queries_list[wg];
        EXPECT_EQ(queries.size(), 1);
        return queries.begin()->elapsed_time();
    }

    static void _assert_still_paused(const std::shared_ptr<QueryContext>& query) {
        ASSERT_FALSE(query->is_cancelled()) << query->exec_status().to_string();
        ASSERT_TRUE(query->resource_ctx()
                            ->task_controller()
                            ->paused_reason()
                            .is<ErrorCode::PROCESS_MEMORY_EXCEEDED>());
    }

    static void _assert_resumed(const std::shared_ptr<QueryContext>& query) {
        ASSERT_FALSE(query->is_cancelled()) << query->exec_status().to_string();
        ASSERT_TRUE(query->resource_ctx()->task_controller()->paused_reason().ok());
    }

    static void _assert_cancelled_by_process_memory(const std::shared_ptr<QueryContext>& query,
                                                    bool exceed_hard_limit) {
        ASSERT_TRUE(query->is_cancelled());
        const auto status = query->exec_status().to_string();
        ASSERT_TRUE(query->exec_status().is<ErrorCode::MEM_LIMIT_EXCEEDED>()) << status;
        ASSERT_NE(status.find(fmt::format("exceed hard limit: {}", exceed_hard_limit)),
                  std::string::npos)
                << status;
    }

    std::unique_ptr<WorkloadGroupMgr> _wg_manager;
    std::string _test_dir;
    const int64_t _spill_in_paused_queue_timeout_ms = config::spill_in_paused_queue_timeout_ms;
    int64_t _original_mem_limit {0};
    int64_t _original_soft_mem_limit {0};
    int64_t _original_sys_mem_available {0};
    std::vector<std::pair<std::shared_ptr<QueryContext>, int64_t>> _consumed_memory;
};

TEST_F(WorkloadGroupManagerTest, get_or_create_workload_group) {
    auto wg = _wg_manager->get_or_create_workload_group({});
    ASSERT_EQ(wg->id(), 0);
}

TEST_F(WorkloadGroupManagerTest, refresh_memory_usage_updates_memory_limits) {
    const int64_t original_mem_limit = MemInfo::mem_limit();
    Defer restore_mem_limit {[&]() { MemInfo::set_mem_limit_for_test(original_mem_limit); }};
    const int64_t initial_mem_limit = 1024L * 1024 * 1024;
    MemInfo::set_mem_limit_for_test(initial_mem_limit);

    WorkloadGroupInfo wg_info {.id = 1,
                               .memory_limit = initial_mem_limit / 2,
                               .min_memory_percent = 25,
                               .max_memory_percent = 50};
    auto wg = _wg_manager->get_or_create_workload_group(wg_info);

    EXPECT_EQ(wg->memory_limit(), initial_mem_limit / 2);
    EXPECT_EQ(wg->min_memory_limit(), initial_mem_limit / 4);

    const int64_t updated_mem_limit = initial_mem_limit * 2;
    MemInfo::set_mem_limit_for_test(updated_mem_limit);
    wg->refresh_memory_usage();

    EXPECT_EQ(wg->memory_limit(), updated_mem_limit / 2);
    EXPECT_EQ(wg->min_memory_limit(), updated_mem_limit / 4);
}

TEST_F(WorkloadGroupManagerTest, handle_paused_queries_ignores_empty_workload_group) {
    auto wg = _wg_manager->get_or_create_workload_group({});

    _wg_manager->handle_paused_queries();

    std::unique_lock<std::mutex> lock(_wg_manager->_paused_queries_lock);
    ASSERT_FALSE(_wg_manager->_paused_queries_list.contains(wg));
}

TEST_F(WorkloadGroupManagerTest, refresh_restores_query_limit_after_cgroup_expands) {
    const int64_t original_mem_limit = MemInfo::mem_limit();
    Defer restore_mem_limit {[&]() { MemInfo::set_mem_limit_for_test(original_mem_limit); }};
    const int64_t small_mem_limit = 1024L * 1024 * 20;
    const int64_t large_mem_limit = 1024L * 1024 * 100;
    MemInfo::set_mem_limit_for_test(small_mem_limit);
    WorkloadGroupInfo wg_info {.id = 1,
                               .memory_limit = small_mem_limit,
                               .max_memory_percent = 100,
                               .slot_mem_policy = TWgSlotMemoryPolicy::NONE};
    auto wg = _wg_manager->get_or_create_workload_group(wg_info);
    auto query_context = _generate_on_query(wg, large_mem_limit, true);
    auto query_without_mem_limit = _generate_on_query(wg);

    ASSERT_EQ(query_context->resource_ctx()->memory_context()->mem_limit(), small_mem_limit);
    ASSERT_EQ(query_context->resource_ctx()->memory_context()->user_set_mem_limit(),
              large_mem_limit);
    ASSERT_EQ(query_without_mem_limit->resource_ctx()->memory_context()->mem_limit(),
              small_mem_limit);
    ASSERT_EQ(query_without_mem_limit->resource_ctx()->memory_context()->user_set_mem_limit(),
              1LL << 60);

    MemInfo::set_mem_limit_for_test(large_mem_limit);
    _wg_manager->refresh_workload_group_memory_state();

    ASSERT_EQ(wg->memory_limit(), large_mem_limit);
    ASSERT_EQ(query_context->resource_ctx()->memory_context()->mem_limit(), large_mem_limit);
    ASSERT_EQ(query_without_mem_limit->resource_ctx()->memory_context()->mem_limit(),
              large_mem_limit);
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
    auto query_context = _generate_on_query(wg, 1024L * 1024 * 128, true);

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
    WorkloadGroupInfo wg_info {.id = 1,
                               .memory_low_watermark = 80,
                               .memory_high_watermark = 95,
                               .slot_mem_policy = TWgSlotMemoryPolicy::NONE};
    auto wg = _wg_manager->get_or_create_workload_group(wg_info);
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

TEST_F(WorkloadGroupManagerTest, wg_reserve_failed_before_query_limit_and_high_watermark) {
    WorkloadGroupInfo wg_info {.id = 1,
                               .memory_limit = 5000,
                               .memory_low_watermark = 50,
                               .memory_high_watermark = 60,
                               .slot_mem_policy = TWgSlotMemoryPolicy::NONE};
    auto wg = _wg_manager->get_or_create_workload_group(wg_info);
    auto query_context = _generate_on_query(wg, 4096);
    {
        ThreadContext thread_context;
        thread_context.attach_task(query_context->resource_ctx());
        Defer cleanup {[&]() {
            {
                std::unique_lock<std::mutex> lock(_wg_manager->_paused_queries_lock);
                _wg_manager->_paused_queries_list.erase(wg);
            }
            query_context->set_memory_sufficient(true);
            thread_context.thread_mem_tracker_mgr->shrink_reserved();
            thread_context.detach_task();
        }};

        auto st = thread_context.thread_mem_tracker_mgr->try_reserve(2048);
        ASSERT_TRUE(st.ok()) << st.to_string();
        ASSERT_EQ(query_context->resource_ctx()->memory_context()->current_memory_bytes(), 2048);
        ASSERT_EQ(query_context->resource_ctx()->memory_context()->reserved_consumption(), 2048);
        ASSERT_LT(query_context->resource_ctx()->memory_context()->current_memory_bytes(),
                  query_context->resource_ctx()->memory_context()->mem_limit());
        ASSERT_LT(query_context->resource_ctx()->memory_context()->current_memory_bytes() + 1024,
                  query_context->resource_ctx()->memory_context()->mem_limit());

        bool exceed_low_watermark = false;
        bool exceed_high_watermark = false;
        wg->check_mem_used(&exceed_low_watermark, &exceed_high_watermark);
        ASSERT_FALSE(exceed_low_watermark);
        ASSERT_FALSE(exceed_high_watermark);

        st = thread_context.thread_mem_tracker_mgr->try_reserve(1024);
        ASSERT_TRUE(st.is<ErrorCode::WORKLOAD_GROUP_MEMORY_EXCEEDED>()) << st.to_string();
        ASSERT_FALSE(st.is<ErrorCode::QUERY_MEMORY_EXCEEDED>()) << st.to_string();
        ASSERT_EQ(query_context->resource_ctx()->memory_context()->current_memory_bytes(), 2048);
        ASSERT_EQ(query_context->resource_ctx()->memory_context()->reserved_consumption(), 2048);

        _wg_manager->add_paused_query(query_context->resource_ctx(), 1024, st);
        ASSERT_FALSE(query_context->get_memory_sufficient_dependency()->ready());
        {
            std::unique_lock<std::mutex> lock(_wg_manager->_paused_queries_lock);
            ASSERT_EQ(_wg_manager->_paused_queries_list[wg].size(), 1)
                    << "paused queue should not be empty";
        }

        _run_checking_loop(wg, 3);

        ASSERT_FALSE(query_context->get_memory_sufficient_dependency()->ready());
        ASSERT_TRUE(query_context->resource_ctx()
                            ->task_controller()
                            ->paused_reason()
                            .is<ErrorCode::WORKLOAD_GROUP_MEMORY_EXCEEDED>());
        ASSERT_FALSE(query_context->is_cancelled());
        {
            std::unique_lock<std::mutex> lock(_wg_manager->_paused_queries_lock);
            ASSERT_EQ(_wg_manager->_paused_queries_list[wg].size(), 1)
                    << "paused queue should keep the query";
        }
    }
    ASSERT_EQ(query_context->resource_ctx()->memory_context()->current_memory_bytes(), 0);
    ASSERT_EQ(query_context->resource_ctx()->memory_context()->reserved_consumption(), 0);
}

// TWgSlotMemoryPolicy::NONE
// query_ctx->workload_group()->exceed_limit() == true
// query limit > workload group limit
// query's limit will be set to workload group limit
TEST_F(WorkloadGroupManagerTest, wg_exceed3) {
    const int64_t original_mem_limit = MemInfo::mem_limit();
    Defer restore_mem_limit {[&]() { MemInfo::set_mem_limit_for_test(original_mem_limit); }};
    MemInfo::set_mem_limit_for_test(1024L * 1024 * 100);
    WorkloadGroupInfo wg_info {.id = 1,
                               .memory_limit = 1024L * 1024,
                               .max_memory_percent = 1,
                               .slot_mem_policy = TWgSlotMemoryPolicy::NONE};
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
    const int64_t original_mem_limit = MemInfo::mem_limit();
    Defer restore_mem_limit {[&]() { MemInfo::set_mem_limit_for_test(original_mem_limit); }};
    MemInfo::set_mem_limit_for_test(1024L * 1024 * 100);
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
    const int64_t original_mem_limit = MemInfo::mem_limit();
    Defer restore_mem_limit {[&]() { MemInfo::set_mem_limit_for_test(original_mem_limit); }};
    MemInfo::set_mem_limit_for_test(1024L * 1024 * 100);
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
    WorkloadGroupInfo wg_info {.id = 1,
                               .memory_low_watermark = 80,
                               .memory_high_watermark = 95,
                               .slot_mem_policy = TWgSlotMemoryPolicy::NONE};
    auto wg = _wg_manager->get_or_create_workload_group(wg_info);
    EXPECT_EQ(wg->id(), wg_info.id);

    auto query_context = _generate_on_query(wg, 1024L * 1024 * 128, true);

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
    WorkloadGroupInfo wg_info {.id = 1,
                               .memory_low_watermark = 80,
                               .memory_high_watermark = 95,
                               .slot_mem_policy = TWgSlotMemoryPolicy::NONE};
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
    const int64_t original_mem_limit = MemInfo::mem_limit();
    Defer restore_mem_limit {[&]() { MemInfo::set_mem_limit_for_test(original_mem_limit); }};
    MemInfo::set_mem_limit_for_test(1024L * 1024 * 1000);
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
    EXPECT_EQ(0, _wg_manager->revoke_memory_from_other_groups_(kProcessPausedReserveSize));

    // If exceed memory less than 128MB, then not revoke
    auto query_context21 = _generate_on_query(wg2);
    query_context21->resource_ctx()->memory_context()->set_mem_limit(1024 * 1024 * 1024);
    query_context21->query_mem_tracker()->consume(1024 * 1024 * 50);
    wg2->refresh_memory_usage();
    EXPECT_EQ(wg2->total_mem_used(), 1024 * 1024 * 50);
    EXPECT_EQ(wg2->min_memory_limit(), 1024 * 1024 * 100);
    // There is not workload group's memory usage > it's min memory limit.
    EXPECT_EQ(0, _wg_manager->revoke_memory_from_other_groups_(kProcessPausedReserveSize));
    ASSERT_FALSE(query_context21->is_cancelled());

    // Add another query that use a lot of memory
    auto query_context22 = _generate_on_query(wg2);
    query_context22->resource_ctx()->memory_context()->set_mem_limit(1024 * 1024 * 1024);
    query_context22->query_mem_tracker()->consume(1024 * 1024 * 60);
    wg2->refresh_memory_usage();
    EXPECT_EQ(wg2->total_mem_used(), 1024 * 1024 * 110);
    EXPECT_EQ(wg2->min_memory_limit(), 1024 * 1024 * 100);
    // Could not revoke larger than 128MB, not revoke.
    EXPECT_EQ(0, _wg_manager->revoke_memory_from_other_groups_(kProcessPausedReserveSize));
    ASSERT_FALSE(query_context21->is_cancelled());
    ASSERT_FALSE(query_context22->is_cancelled());

    // Add another query that use a lot of memory
    auto query_context23 = _generate_on_query(wg2);
    query_context23->resource_ctx()->memory_context()->set_mem_limit(1024 * 1024 * 1024);
    query_context23->query_mem_tracker()->consume(1024 * 1024 * 300);
    wg2->refresh_memory_usage();
    EXPECT_EQ(wg2->total_mem_used(), 1024 * 1024 * 410);
    EXPECT_EQ(wg2->min_memory_limit(), 1024 * 1024 * 100);
    // WG2 exceeds its min memory by 310MB, so 31MB should be revoked. The largest query (300MB)
    // is cancelled and its whole memory is reported as revoked.
    EXPECT_EQ(300L * 1024 * 1024,
              _wg_manager->revoke_memory_from_other_groups_(kProcessPausedReserveSize));
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

    // WG3 exceeds most, 40MB should be revoked, and the cancelled query31 frees 500MB.
    EXPECT_EQ(500L * 1024 * 1024,
              _wg_manager->revoke_memory_from_other_groups_(kProcessPausedReserveSize));

    wg1->refresh_memory_usage();
    wg2->refresh_memory_usage();
    wg3->refresh_memory_usage();

    ASSERT_TRUE(query_context31->is_cancelled());
    // query31 is still in wg3, so that it is not cancel again. It was cancelled just now and is
    // still releasing memory, so its memory is counted as revoked again.
    EXPECT_EQ(500L * 1024 * 1024,
              _wg_manager->revoke_memory_from_other_groups_(kProcessPausedReserveSize));
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
    const int64_t original_mem_limit = MemInfo::mem_limit();
    Defer restore_mem_limit {[&]() { MemInfo::set_mem_limit_for_test(original_mem_limit); }};
    MemInfo::set_mem_limit_for_test(1024L * 1024 * 1000);
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
    const int64_t original_mem_limit = MemInfo::mem_limit();
    Defer restore_mem_limit {[&]() { MemInfo::set_mem_limit_for_test(original_mem_limit); }};
    MemInfo::set_mem_limit_for_test(1024L * 1024 * 200);
    WorkloadGroupInfo wg_info {.id = 1,
                               .memory_limit = 1024L * 1024 * 200,
                               .slot_mem_policy = TWgSlotMemoryPolicy::NONE};
    auto wg = _wg_manager->get_or_create_workload_group(wg_info);
    auto query_context = _generate_on_query(wg, 1024L * 1024 * 128, true);

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
    const int64_t original_mem_limit = MemInfo::mem_limit();
    Defer restore_mem_limit {[&]() { MemInfo::set_mem_limit_for_test(original_mem_limit); }};
    MemInfo::set_mem_limit_for_test(1024L * 1024 * 200);
    WorkloadGroupInfo wg_info {.id = 1,
                               .memory_limit = 1024L * 1024 * 200,
                               .memory_low_watermark = 80,
                               .memory_high_watermark = 95,
                               .total_query_slot_count = 5,
                               .slot_mem_policy = TWgSlotMemoryPolicy::DYNAMIC};
    auto wg = _wg_manager->get_or_create_workload_group(wg_info);
    auto query_context = _generate_on_query(wg, 1024L * 1024 * 128, true);

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
    const int64_t original_mem_limit = MemInfo::mem_limit();
    Defer restore_mem_limit {[&]() { MemInfo::set_mem_limit_for_test(original_mem_limit); }};
    MemInfo::set_mem_limit_for_test(1024L * 1024 * 200);
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
    const int64_t original_mem_limit = MemInfo::mem_limit();
    Defer restore_mem_limit {[&]() { MemInfo::set_mem_limit_for_test(original_mem_limit); }};
    MemInfo::set_mem_limit_for_test(1024L * 1024 * 200);
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
    WorkloadGroupInfo wg1_info {
            .id = 1, .memory_limit = 1024L * 1024 * 1000, .memory_high_watermark = 80};
    WorkloadGroupInfo wg2_info {
            .id = 2, .memory_limit = 1024L * 1024 * 1000, .memory_high_watermark = 80};
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

TEST_F(WorkloadGroupManagerTest, FailedInternalGroupSetupCancelsAdaptiveFlush) {
    auto* env = ExecEnv::GetInstance();
    auto saved_engine = std::move(env->_storage_engine);
    const auto saved_config = std::make_tuple(
            config::enable_adaptive_flush_threads, config::enable_task_executor_in_internal_table,
            config::enable_task_executor_in_external_table, config::pipeline_executor_size,
            config::blocking_pipeline_executor_size, config::doris_scanner_thread_pool_thread_num,
            config::doris_max_remote_scanner_thread_pool_thread_num,
            config::doris_scanner_min_thread_pool_thread_num, config::min_active_scan_threads,
            config::min_active_file_scan_threads, config::flush_thread_num_per_store);
    Defer restore {[&] {
        env->set_storage_engine(std::move(saved_engine));
        std::tie(config::enable_adaptive_flush_threads,
                 config::enable_task_executor_in_internal_table,
                 config::enable_task_executor_in_external_table, config::pipeline_executor_size,
                 config::blocking_pipeline_executor_size,
                 config::doris_scanner_thread_pool_thread_num,
                 config::doris_max_remote_scanner_thread_pool_thread_num,
                 config::doris_scanner_min_thread_pool_thread_num, config::min_active_scan_threads,
                 config::min_active_file_scan_threads, config::flush_thread_num_per_store) =
                saved_config;
    }};
    env->set_storage_engine(std::make_unique<StorageEngine>(EngineOptions {}));
    auto* controller = env->storage_engine().adaptive_thread_controller();
    config::enable_adaptive_flush_threads = true;
    config::enable_task_executor_in_internal_table = false;
    config::enable_task_executor_in_external_table = false;
    config::pipeline_executor_size = 1;
    config::blocking_pipeline_executor_size = 1;
    config::doris_scanner_thread_pool_thread_num = 1;
    config::doris_max_remote_scanner_thread_pool_thread_num = 1;
    config::doris_scanner_min_thread_pool_thread_num = 1;
    config::min_active_scan_threads = 1;
    config::min_active_file_scan_threads = 1;
    config::flush_thread_num_per_store = 1;

    auto* sp = SyncPoint::get_instance();
    sp->enable_processing();
    Defer disable_sync_points {[&] { sp->disable_processing(); }};
    int failed_starts = 0;
    int cancelled_registrations = 0;
    SyncPoint::CallbackGuard start_guard;
    SyncPoint::CallbackGuard cancel_guard;
    sp->set_call_back(
            "WorkloadGroup::upsert_thread_pool_no_lock::task_scheduler_start",
            [&](auto&& args) {
                auto* result = try_any_cast_ret<Status>(args);
                result->first = Status::InternalError<false>("injected pipeline scheduler failure");
                result->second = true;
                ++failed_starts;
            },
            &start_guard);
    sp->set_call_back(
            "AdaptiveThreadPoolController::cancel_stopped",
            [&](auto&&) { ++cancelled_registrations; }, &cancel_guard);

    const auto status = _wg_manager->create_internal_wg();
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("injected pipeline scheduler failure"), std::string::npos);
    EXPECT_EQ(failed_starts, 1);
    EXPECT_TRUE(_wg_manager->_workload_groups.empty());
    // The flush pool was registered despite the earlier scheduler failure, and
    // must be cancelled even though the WG never entered the manager's map.
    EXPECT_EQ(cancelled_registrations, 1);
    {
        std::lock_guard<std::mutex> lock(controller->_mutex);
        EXPECT_TRUE(controller->_pool_groups.empty());
    }
    // Also clean up if an assertion above detects a missing cancellation.
    controller->stop();
}

// Exercise the actual registration/cancellation paths without starting query schedulers.
TEST_F(WorkloadGroupManagerTest, AdaptiveFlushRegistrationSurvivesIdChangeAndReuse) {
    auto* env = ExecEnv::GetInstance();
    auto saved_engine = std::move(env->_storage_engine);
    const bool saved_adaptive = config::enable_adaptive_flush_threads;
    Defer restore {[&] {
        env->set_storage_engine(std::move(saved_engine));
        config::enable_adaptive_flush_threads = saved_adaptive;
    }};
    env->set_storage_engine(std::make_unique<StorageEngine>(EngineOptions {}));
    auto* controller = env->storage_engine().adaptive_thread_controller();
    config::enable_adaptive_flush_threads = true;

    auto wg = _wg_manager->get_or_create_workload_group({.id = 1, .name = "normal"});
    ASSERT_TRUE(ThreadPoolBuilder("wg_flush_test")
                        .set_min_threads(1)
                        .set_max_threads(2)
                        .build(&wg->_memtable_flush_pool)
                        .ok());
    wg->register_adaptive_flush_no_lock();
    const auto key = wg->_adaptive_flush_key;
    ASSERT_GT(controller->get_current_threads(key), 0);
    _wg_manager->reset_workload_group_id("normal", 100);
    EXPECT_EQ(wg->id(), 100);
    EXPECT_EQ(wg->_adaptive_flush_key, key);

    // A second WG using the original ID must not replace the first registration.
    auto reused = _wg_manager->get_or_create_workload_group({.id = 1, .name = "reused"});
    ASSERT_TRUE(ThreadPoolBuilder("wg_flush_reused")
                        .set_min_threads(1)
                        .set_max_threads(2)
                        .build(&reused->_memtable_flush_pool)
                        .ok());
    reused->register_adaptive_flush_no_lock();
    const auto reused_key = reused->_adaptive_flush_key;
    EXPECT_NE(key, reused_key);

    // Disabling adjustment must not disable cleanup of already registered pools.
    config::enable_adaptive_flush_threads = false;
    wg->try_stop_schedulers();
    wg->destroy_schedulers();
    EXPECT_EQ(controller->get_current_threads(key), 0);
    EXPECT_GT(controller->get_current_threads(reused_key), 0);
    // Direct scheduler destruction must also drain the registration.
    reused->destroy_schedulers();
    EXPECT_EQ(controller->get_current_threads(reused_key), 0);
    config::enable_adaptive_flush_threads = true;
    controller->adjust_once();
}

// When the process memory is exceeded and the paused query has no revocable memory, the query
// should be kept paused instead of being cancelled immediately, so that it can be resumed once
// other queries release memory.
TEST_F(WorkloadGroupManagerTest, process_mem_exceeded_keeps_paused_and_resumes) {
    auto wg = _create_wg_with_min_memory(1);
    // Let the workload group use more than its min memory limit, so that the paused query is
    // handled by handle_single_query_ directly instead of waiting for other workload groups.
    auto query = _create_query_with_memory(wg, 1024L * 1024 * 128);
    ASSERT_GT(wg->total_mem_used(), wg->min_memory_limit());

    _exceed_process_soft_mem_limit();
    _pause_for_process_memory(query);

    config::spill_in_paused_queue_timeout_ms = 60 * 1000;
    for (int i = 0; i < 3; ++i) {
        _wg_manager->handle_paused_queries();
        _assert_still_paused(query);
        ASSERT_EQ(_paused_query_count(wg), 1);
    }

    // Process memory pressure is relieved, the query should be resumed.
    _relieve_process_mem_limit();
    _wg_manager->handle_paused_queries();
    _assert_resumed(query);
    ASSERT_EQ(_paused_query_count(wg), 0);
}

// When the process memory is still exceeded after the paused query has waited for
// `spill_in_paused_queue_timeout_ms`, the query should be cancelled.
TEST_F(WorkloadGroupManagerTest, process_mem_exceeded_cancels_after_timeout) {
    auto wg = _create_wg_with_min_memory(1);
    auto query = _create_query_with_memory(wg, 1024L * 1024 * 128);
    ASSERT_GT(wg->total_mem_used(), wg->min_memory_limit());

    _exceed_process_soft_mem_limit();
    _pause_for_process_memory(query);

    config::spill_in_paused_queue_timeout_ms = 60 * 1000;
    _wg_manager->handle_paused_queries();
    _assert_still_paused(query);
    _backdate_paused_query(wg, config::spill_in_paused_queue_timeout_ms + 1);

    _wg_manager->handle_paused_queries();
    _assert_cancelled_by_process_memory(query, false);
}

// The recovery check must test the recorded reservation, not a larger probe: after other
// queries release some memory, a small reservation fits under the process soft limit while a
// 32 MiB probe does not, and the query must be resumed instead of waiting for the timeout.
TEST_F(WorkloadGroupManagerTest, process_mem_exceeded_resumes_when_reservation_fits_soft_limit) {
    auto wg = _create_wg_with_min_memory(1);
    auto query = _create_query_with_memory(wg, 1024L * 1024 * 128);
    ASSERT_GT(wg->total_mem_used(), wg->min_memory_limit());

    _exceed_process_soft_mem_limit();
    _pause_for_process_memory(query);

    config::spill_in_paused_queue_timeout_ms = 60 * 1000;
    _wg_manager->handle_paused_queries();
    _assert_still_paused(query);

    // Leave 16 MiB headroom below the soft limit: more than the recorded reservation, less
    // than a 32 MiB probe.
    MemInfo::set_soft_mem_limit_for_test(GlobalMemoryArbitrator::process_memory_usage() +
                                         1024L * 1024 * 16);
    ASSERT_FALSE(GlobalMemoryArbitrator::is_exceed_soft_mem_limit(kProcessPausedReserveSize));
    ASSERT_TRUE(GlobalMemoryArbitrator::is_exceed_soft_mem_limit(1024L * 1024 * 32));
    _backdate_paused_query(wg, config::spill_in_paused_queue_timeout_ms + 1);

    _wg_manager->handle_paused_queries();
    _assert_resumed(query);
    ASSERT_EQ(_paused_query_count(wg), 0);
}

// Same as above for the system available memory boundary: the recorded reservation keeps the
// system available memory above the warning water mark while a 32 MiB probe does not.
TEST_F(WorkloadGroupManagerTest,
       process_mem_exceeded_resumes_when_reservation_fits_sys_mem_available) {
    auto wg = _create_wg_with_min_memory(1);
    auto query = _create_query_with_memory(wg, 1024L * 1024 * 128);
    ASSERT_GT(wg->total_mem_used(), wg->min_memory_limit());

    _exceed_process_soft_mem_limit();
    _pause_for_process_memory(query);

    config::spill_in_paused_queue_timeout_ms = 60 * 1000;
    _wg_manager->handle_paused_queries();
    _assert_still_paused(query);

    // The process limits have their whole headroom, only the system available memory is close
    // to the warning water mark: 16 MiB above it.
    _relieve_process_mem_limit();
    ASSERT_GT(MemInfo::sys_mem_available_warning_water_mark(), 0);
    MemInfo::set_sys_mem_available_for_test(MemInfo::sys_mem_available_warning_water_mark() +
                                            1024L * 1024 * 16);
    ASSERT_FALSE(GlobalMemoryArbitrator::is_exceed_soft_mem_limit(kProcessPausedReserveSize));
    ASSERT_TRUE(GlobalMemoryArbitrator::is_exceed_soft_mem_limit(1024L * 1024 * 32));
    _backdate_paused_query(wg, config::spill_in_paused_queue_timeout_ms + 1);

    _wg_manager->handle_paused_queries();
    _assert_resumed(query);
    ASSERT_EQ(_paused_query_count(wg), 0);
}

// A paused query whose workload group uses no more than its min memory limit is not handled by
// handle_single_query_. It should still be resumed as soon as the process memory pressure is
// relieved, instead of waiting for the timeout.
TEST_F(WorkloadGroupManagerTest, process_mem_exceeded_below_min_memory_resumes) {
    auto wg = _create_wg_with_min_memory(1);
    auto query = _create_query_with_memory(wg, 1024L * 4);
    ASSERT_LE(wg->total_mem_used(), wg->min_memory_limit());

    _exceed_process_soft_mem_limit();
    _pause_for_process_memory(query);

    config::spill_in_paused_queue_timeout_ms = 60 * 1000;
    _wg_manager->handle_paused_queries();
    _assert_still_paused(query);
    ASSERT_EQ(_paused_query_count(wg), 1);

    _relieve_process_mem_limit();
    _wg_manager->handle_paused_queries();
    _assert_resumed(query);
    ASSERT_EQ(_paused_query_count(wg), 0);
}

// A paused query whose workload group uses no more than its min memory limit should still be
// cancelled once it has waited for `spill_in_paused_queue_timeout_ms` under process memory
// pressure.
TEST_F(WorkloadGroupManagerTest, process_mem_exceeded_below_min_memory_cancels_after_timeout) {
    auto wg = _create_wg_with_min_memory(1);
    auto query = _create_query_with_memory(wg, 1024L * 4);
    ASSERT_LE(wg->total_mem_used(), wg->min_memory_limit());

    _exceed_process_soft_mem_limit();
    _pause_for_process_memory(query);

    config::spill_in_paused_queue_timeout_ms = 60 * 1000;
    _wg_manager->handle_paused_queries();
    _assert_still_paused(query);
    _backdate_paused_query(wg, config::spill_in_paused_queue_timeout_ms + 1);

    _wg_manager->handle_paused_queries();
    _assert_cancelled_by_process_memory(query, false);
}

// A paused query whose workload group uses no more than its min memory limit should also be
// cancelled immediately at the process hard limit when no other workload group can release
// memory. This path must not rely on the memory GC daemon, which may be disabled.
TEST_F(WorkloadGroupManagerTest, process_mem_exceeded_below_min_memory_cancels_at_hard_limit) {
    auto wg = _create_wg_with_min_memory(1);
    auto query = _create_query_with_memory(wg, 1024L * 4);
    ASSERT_LE(wg->total_mem_used(), wg->min_memory_limit());

    _exceed_process_hard_mem_limit();
    _pause_for_process_memory(query);

    config::spill_in_paused_queue_timeout_ms = 60 * 1000;
    _wg_manager->handle_paused_queries();
    _assert_cancelled_by_process_memory(query, true);
}

// Another workload group exceeds its min memory limit by more than 128 MiB, but every query in
// it is too small to be cancelled, so revoking memory from it frees nothing. The paused query
// must not be treated as if memory had been revoked (which would resume it without any memory
// being freed), it keeps waiting below the hard limit and is cancelled at the hard limit.
TEST_F(WorkloadGroupManagerTest, process_mem_exceeded_below_min_memory_with_non_reclaimable_peer) {
    auto wg = _create_wg_with_min_memory(1);
    auto query = _create_query_with_memory(wg, 1024L * 4);
    ASSERT_LE(wg->total_mem_used(), wg->min_memory_limit());

    auto peer_wg = _create_wg_with_min_memory(2);
    std::vector<std::shared_ptr<QueryContext>> peer_queries;
    for (int i = 0; i < 10; ++i) {
        // Not larger than SMALL_MEMORY_TASK (32 MiB), so memory reclamation skips it.
        peer_queries.push_back(_create_query_with_memory(peer_wg, 1024L * 1024 * 30));
    }
    ASSERT_GT(peer_wg->total_mem_used(), peer_wg->min_memory_limit() + (1 << 27));

    _exceed_process_soft_mem_limit();
    _pause_for_process_memory(query);

    config::spill_in_paused_queue_timeout_ms = 60 * 1000;
    for (int i = 0; i < 3; ++i) {
        _wg_manager->handle_paused_queries();
        _assert_still_paused(query);
        ASSERT_EQ(_paused_query_count(wg), 1);
        ASSERT_FALSE(_wg_manager->revoking_memory_from_other_query_);
    }

    _exceed_process_hard_mem_limit();
    _wg_manager->handle_paused_queries();
    _assert_cancelled_by_process_memory(query, true);
    for (const auto& peer_query : peer_queries) {
        ASSERT_FALSE(peer_query->is_cancelled());
    }
}

// Two workload groups exceed their min memory limit by more than 128 MiB. The one that exceeds
// most only holds queries too small to be cancelled, so revoking memory from it frees nothing;
// the other one holds a single cancellable query. The paused query must not fall through to the
// hard-limit fallback while the second workload group can still release memory: its query is
// cancelled and the paused query waits for the release.
TEST_F(WorkloadGroupManagerTest, process_mem_exceeded_below_min_memory_tries_next_peer) {
    auto wg = _create_wg_with_min_memory(1);
    auto query = _create_query_with_memory(wg, 1024L * 4);
    ASSERT_LE(wg->total_mem_used(), wg->min_memory_limit());

    auto small_peer_wg = _create_wg_with_min_memory(2);
    std::vector<std::shared_ptr<QueryContext>> small_peer_queries;
    for (int i = 0; i < 10; ++i) {
        // Not larger than SMALL_MEMORY_TASK (32 MiB), so memory reclamation skips it.
        small_peer_queries.push_back(_create_query_with_memory(small_peer_wg, 1024L * 1024 * 30));
    }
    auto large_peer_wg = _create_wg_with_min_memory(3);
    auto large_peer_query = _create_query_with_memory(large_peer_wg, 1024L * 1024 * 250);
    ASSERT_GT(large_peer_wg->total_mem_used(), large_peer_wg->min_memory_limit() + (1 << 27));
    // The non-reclaimable workload group exceeds its min memory most, so it is tried first.
    ASSERT_GT(small_peer_wg->total_mem_used() - small_peer_wg->min_memory_limit(),
              large_peer_wg->total_mem_used() - large_peer_wg->min_memory_limit());

    _exceed_process_hard_mem_limit();
    _pause_for_process_memory(query);

    config::spill_in_paused_queue_timeout_ms = 60 * 1000;
    _wg_manager->handle_paused_queries();
    ASSERT_TRUE(large_peer_query->is_cancelled());
    for (const auto& small_peer_query : small_peer_queries) {
        ASSERT_FALSE(small_peer_query->is_cancelled());
    }
    _assert_still_paused(query);
    ASSERT_EQ(_paused_query_count(wg), 1);
    ASSERT_TRUE(_wg_manager->revoking_memory_from_other_query_);
}

// Revoking memory from another workload group cancels one of its queries, which holds its
// memory until the cancellation completes. While that cancellation is in flight (within
// `revoke_memory_max_tolerance_ms`), memory reclamation keeps reporting its memory as revoked,
// so the paused query waits for the release instead of being cancelled, also at the hard limit.
// Once the cancellation has taken longer than the tolerance, the peer no longer counts,
// nothing is revoked and the paused query falls through to the hard-limit fallback.
TEST_F(WorkloadGroupManagerTest, process_mem_exceeded_below_min_memory_waits_for_cancelling_peer) {
    auto wg = _create_wg_with_min_memory(1);
    auto query = _create_query_with_memory(wg, 1024L * 4);
    ASSERT_LE(wg->total_mem_used(), wg->min_memory_limit());

    auto peer_wg = _create_wg_with_min_memory(2);
    auto peer_query = _create_query_with_memory(peer_wg, 1024L * 1024 * 300);
    auto* peer_controller = _install_mock_query_task_controller(peer_query);
    ASSERT_GT(peer_wg->total_mem_used(), peer_wg->min_memory_limit() + (1 << 27));

    _exceed_process_hard_mem_limit();
    _pause_for_process_memory(query);

    config::spill_in_paused_queue_timeout_ms = 60 * 1000;
    // The peer query is cancelled, the paused query waits for its memory to be released.
    _wg_manager->handle_paused_queries();
    ASSERT_TRUE(peer_query->is_cancelled());
    _assert_still_paused(query);
    ASSERT_TRUE(_wg_manager->revoking_memory_from_other_query_);

    // The cancelled peer is not in the paused list, so the next round resumes the paused
    // query, whose reservation fails again and pauses it again.
    _wg_manager->handle_paused_queries();
    _assert_resumed(query);
    ASSERT_FALSE(_wg_manager->revoking_memory_from_other_query_);
    _pause_for_process_memory(query);

    // The peer still holds its memory and its cancellation is in flight: it is reported as
    // revoked memory again, and the paused query keeps waiting for it.
    _wg_manager->handle_paused_queries();
    _assert_still_paused(query);
    ASSERT_TRUE(_wg_manager->revoking_memory_from_other_query_);

    _wg_manager->handle_paused_queries();
    _assert_resumed(query);
    _pause_for_process_memory(query);

    // The cancellation has taken longer than the tolerance: the peer no longer counts, nothing
    // is revoked and the paused query is cancelled at the hard limit.
    peer_controller->set_cancelled_time(peer_controller->cancelled_time() -
                                        config::revoke_memory_max_tolerance_ms - 1);
    _wg_manager->handle_paused_queries();
    _assert_cancelled_by_process_memory(query, true);
}

// When the process reaches the hard memory limit, the paused query should be cancelled without
// waiting for the timeout, so that the protection does not depend on memory gc.
TEST_F(WorkloadGroupManagerTest, process_mem_exceeded_cancels_at_hard_limit) {
    auto wg = _create_wg_with_min_memory(1);
    auto query = _create_query_with_memory(wg, 1024L * 1024 * 128);
    ASSERT_GT(wg->total_mem_used(), wg->min_memory_limit());

    _exceed_process_hard_mem_limit();
    _pause_for_process_memory(query);

    config::spill_in_paused_queue_timeout_ms = 60 * 1000;
    _wg_manager->handle_paused_queries();
    _assert_cancelled_by_process_memory(query, true);
}

// A query with a running task cannot be spilled, so it keeps waiting for the task to yield.
// The wait is still bounded: at the timeout the query is cancelled, cancelling is safe while
// the task runs.
TEST_F(WorkloadGroupManagerTest, process_mem_exceeded_running_task_cancels_after_timeout) {
    auto wg = _create_wg_with_min_memory(1);
    auto query = _create_query_with_memory(wg, 1024L * 1024 * 128);
    ASSERT_GT(wg->total_mem_used(), wg->min_memory_limit());
    auto* mock_controller = _install_mock_query_task_controller(query);
    mock_controller->has_running_task_ = true;
    // Revocable memory is not spilled while a task runs.
    mock_controller->has_revocable_task_ = true;

    _exceed_process_soft_mem_limit();
    _pause_for_process_memory(query);

    config::spill_in_paused_queue_timeout_ms = 60 * 1000;
    for (int i = 0; i < 3; ++i) {
        _wg_manager->handle_paused_queries();
        _assert_still_paused(query);
        ASSERT_EQ(_paused_query_count(wg), 1);
        ASSERT_EQ(mock_controller->revoke_memory_calls_, 0);
    }

    _backdate_paused_query(wg, config::spill_in_paused_queue_timeout_ms + 1);
    _wg_manager->handle_paused_queries();
    ASSERT_EQ(mock_controller->revoke_memory_calls_, 0);
    _assert_cancelled_by_process_memory(query, false);
    const auto status = query->exec_status().to_string();
    ASSERT_NE(status.find("has running task: true"), std::string::npos) << status;
}

// At the hard limit a running task must not postpone the protection: spilling is unsafe while
// the task runs, so the query is cancelled at once.
TEST_F(WorkloadGroupManagerTest, process_mem_exceeded_running_task_cancels_at_hard_limit) {
    auto wg = _create_wg_with_min_memory(1);
    auto query = _create_query_with_memory(wg, 1024L * 1024 * 128);
    ASSERT_GT(wg->total_mem_used(), wg->min_memory_limit());
    auto* mock_controller = _install_mock_query_task_controller(query);
    mock_controller->has_running_task_ = true;

    _exceed_process_hard_mem_limit();
    _pause_for_process_memory(query);

    config::spill_in_paused_queue_timeout_ms = 60 * 1000;
    _wg_manager->handle_paused_queries();
    _assert_cancelled_by_process_memory(query, true);
    const auto status = query->exec_status().to_string();
    ASSERT_NE(status.find("has running task: true"), std::string::npos) << status;
}

// A query with revocable memory is spilled rather than cancelled, also at the hard limit. If
// the spill resumes the query without freeing memory and it is paused again without revocable
// memory, the next round cancels it at the hard limit.
TEST_F(WorkloadGroupManagerTest, process_mem_exceeded_spills_revocable_tasks_at_hard_limit) {
    auto wg = _create_wg_with_min_memory(1);
    auto query = _create_query_with_memory(wg, 1024L * 1024 * 128);
    ASSERT_GT(wg->total_mem_used(), wg->min_memory_limit());
    auto* mock_controller = _install_mock_query_task_controller(query);
    mock_controller->has_revocable_task_ = true;

    _exceed_process_hard_mem_limit();
    _pause_for_process_memory(query);
    ASSERT_FALSE(query->get_memory_sufficient_dependency()->ready());

    config::spill_in_paused_queue_timeout_ms = 60 * 1000;
    _wg_manager->handle_paused_queries();
    ASSERT_EQ(mock_controller->revoke_memory_calls_, 1);
    // The spill completed and resumed the query: its memory dependency is ready again, the
    // paused reason is cleared and the entry is gone, so its tasks retry their reservations.
    _assert_resumed(query);
    ASSERT_TRUE(query->get_memory_sufficient_dependency()->ready());
    ASSERT_EQ(_paused_query_count(wg), 0);

    // The spill freed nothing and the process is still at its hard limit, so the retried
    // reservation fails and the query is paused again, now without revocable memory.
    _pause_for_process_memory(query);
    ASSERT_FALSE(query->get_memory_sufficient_dependency()->ready());
    ASSERT_FALSE(mock_controller->has_revocable_task_);
    _wg_manager->handle_paused_queries();
    ASSERT_EQ(mock_controller->revoke_memory_calls_, 1);
    _assert_cancelled_by_process_memory(query, true);
}

// The process memory pressure observed at the beginning of the round may be relieved while the
// manager inspects the pipeline tasks, up to the last scan for revocable tasks. The timeout
// decision must re-check the pressure after that scan and resume the query instead of
// cancelling it from the stale observation.
TEST_F(WorkloadGroupManagerTest, process_mem_exceeded_recovery_before_timeout_decision_resumes) {
    auto wg = _create_wg_with_min_memory(1);
    auto query = _create_query_with_memory(wg, 1024L * 1024 * 128);
    ASSERT_GT(wg->total_mem_used(), wg->min_memory_limit());
    auto* mock_controller = _install_mock_query_task_controller(query);

    _exceed_process_soft_mem_limit();
    _pause_for_process_memory(query);

    config::spill_in_paused_queue_timeout_ms = 60 * 1000;
    _wg_manager->handle_paused_queries();
    _assert_still_paused(query);
    _backdate_paused_query(wg, config::spill_in_paused_queue_timeout_ms + 1);

    // The pressure is relieved after the round has observed it, routed the query and
    // inspected its pipeline tasks for the last time.
    mock_controller->on_get_revocable_tasks_ = []() { _relieve_process_mem_limit(); };
    _wg_manager->handle_paused_queries();
    _assert_resumed(query);
    ASSERT_EQ(_paused_query_count(wg), 0);
}

// Two tasks of one query fail process reservations of different sizes before the next
// maintenance round. The query is resumed as a whole, so the recorded reservation must be the
// largest pending one: once the smaller one fits, the query stays paused (keeping its timer)
// until the larger one fits too. The pending requests do not have to fit at the same time: a
// task releases its reservation after each block, so the tasks can reserve one after another
// once the largest request fits.
TEST_F(WorkloadGroupManagerTest, process_mem_exceeded_keeps_largest_pending_reservation) {
    auto wg = _create_wg_with_min_memory(1);
    auto query = _create_query_with_memory(wg, 1024L * 1024 * 128);
    ASSERT_GT(wg->total_mem_used(), wg->min_memory_limit());

    _exceed_process_soft_mem_limit();
    _pause_for_process_memory(query);
    ASSERT_EQ(_recorded_reserve_size(wg), kProcessPausedReserveSize);
    // A sibling task fails a larger reservation: the entry is shared and keeps the larger size.
    _wg_manager->add_paused_query(query->resource_ctx(), 1024L * 1024 * 64,
                                  Status::Error(ErrorCode::PROCESS_MEMORY_EXCEEDED, "test"));
    ASSERT_EQ(_recorded_reserve_size(wg), 1024L * 1024 * 64);
    // A later smaller failure does not lower it.
    _wg_manager->add_paused_query(query->resource_ctx(), 1024L * 1024 * 4,
                                  Status::Error(ErrorCode::PROCESS_MEMORY_EXCEEDED, "test"));
    ASSERT_EQ(_recorded_reserve_size(wg), 1024L * 1024 * 64);

    config::spill_in_paused_queue_timeout_ms = 60 * 1000;
    _wg_manager->handle_paused_queries();
    _assert_still_paused(query);

    // 16 MiB headroom: the 1 KiB and 4 MiB reservations fit, the 64 MiB one does not. The
    // query must not be woken up only to fail the 64 MiB reservation again.
    MemInfo::set_soft_mem_limit_for_test(GlobalMemoryArbitrator::process_memory_usage() +
                                         1024L * 1024 * 16);
    ASSERT_FALSE(GlobalMemoryArbitrator::is_exceed_soft_mem_limit(1024L * 1024 * 4));
    ASSERT_TRUE(GlobalMemoryArbitrator::is_exceed_soft_mem_limit(1024L * 1024 * 64));
    for (int i = 0; i < 3; ++i) {
        _wg_manager->handle_paused_queries();
        _assert_still_paused(query);
        ASSERT_EQ(_paused_query_count(wg), 1);
    }

    // 65 MiB headroom: the largest pending reservation fits, although not all of them do at the
    // same time. The query is resumed; the tasks reserve one after another.
    MemInfo::set_soft_mem_limit_for_test(GlobalMemoryArbitrator::process_memory_usage() +
                                         1024L * 1024 * 65);
    ASSERT_TRUE(GlobalMemoryArbitrator::is_exceed_soft_mem_limit(1024L * 1024 * 68));
    _wg_manager->handle_paused_queries();
    _assert_resumed(query);
    ASSERT_EQ(_paused_query_count(wg), 0);
}

// Same two pending reservations near the timeout: the query keeps the entry it was paused with
// instead of being woken up by the smaller reservation and starting a fresh wait, so the bounded
// wait still ends with the cancellation.
TEST_F(WorkloadGroupManagerTest,
       process_mem_exceeded_largest_pending_reservation_cancels_after_timeout) {
    auto wg = _create_wg_with_min_memory(1);
    auto query = _create_query_with_memory(wg, 1024L * 1024 * 128);
    ASSERT_GT(wg->total_mem_used(), wg->min_memory_limit());

    _exceed_process_soft_mem_limit();
    _pause_for_process_memory(query);
    _wg_manager->add_paused_query(query->resource_ctx(), 1024L * 1024 * 64,
                                  Status::Error(ErrorCode::PROCESS_MEMORY_EXCEEDED, "test"));
    ASSERT_EQ(_recorded_reserve_size(wg), 1024L * 1024 * 64);

    config::spill_in_paused_queue_timeout_ms = 60 * 1000;
    _wg_manager->handle_paused_queries();
    _assert_still_paused(query);

    MemInfo::set_soft_mem_limit_for_test(GlobalMemoryArbitrator::process_memory_usage() +
                                         1024L * 1024 * 16);
    ASSERT_FALSE(GlobalMemoryArbitrator::is_exceed_soft_mem_limit(kProcessPausedReserveSize));
    ASSERT_TRUE(GlobalMemoryArbitrator::is_exceed_soft_mem_limit(1024L * 1024 * 64));
    _backdate_paused_query(wg, config::spill_in_paused_queue_timeout_ms + 1);

    _wg_manager->handle_paused_queries();
    _assert_cancelled_by_process_memory(query, false);
}

// A query resumed because its recorded reservation fits is paused again when the retry fails
// (another query took the memory first, or a smaller sibling does not fit next to the request
// that was resumed for). The new entry continues the wait the query was resumed from instead of
// starting a fresh one, so the bounded wait still ends with the cancellation.
TEST_F(WorkloadGroupManagerTest, process_mem_exceeded_failed_retry_continues_the_wait) {
    auto wg = _create_wg_with_min_memory(1);
    auto query = _create_query_with_memory(wg, 1024L * 1024 * 128);
    ASSERT_GT(wg->total_mem_used(), wg->min_memory_limit());

    _exceed_process_soft_mem_limit();
    _pause_for_process_memory(query);
    config::spill_in_paused_queue_timeout_ms = 60 * 1000;
    _wg_manager->handle_paused_queries();
    _assert_still_paused(query);
    _backdate_paused_query(wg, config::spill_in_paused_queue_timeout_ms + 1);

    // The pressure is relieved and the query is resumed to retry its reservation.
    _relieve_process_mem_limit();
    _wg_manager->handle_paused_queries();
    _assert_resumed(query);
    ASSERT_EQ(_paused_query_count(wg), 0);

    // The retry fails before any reservation of the query succeeded: the query has been waiting
    // since its first failure and is cancelled at the timeout instead of waiting another one.
    _exceed_process_soft_mem_limit();
    _pause_for_process_memory(query);
    ASSERT_GT(_paused_elapsed_time(wg), config::spill_in_paused_queue_timeout_ms);
    _wg_manager->handle_paused_queries();
    _assert_cancelled_by_process_memory(query, false);
}

// A successful reservation ends the wait: the query made progress, so a later failure starts a
// new bounded wait.
TEST_F(WorkloadGroupManagerTest, process_mem_exceeded_successful_reservation_starts_new_wait) {
    auto wg = _create_wg_with_min_memory(1);
    auto query = _create_query_with_memory(wg, 1024L * 1024 * 128);
    ASSERT_GT(wg->total_mem_used(), wg->min_memory_limit());

    _exceed_process_soft_mem_limit();
    _pause_for_process_memory(query);
    config::spill_in_paused_queue_timeout_ms = 60 * 1000;
    _wg_manager->handle_paused_queries();
    _assert_still_paused(query);
    _backdate_paused_query(wg, config::spill_in_paused_queue_timeout_ms + 1);

    _relieve_process_mem_limit();
    _wg_manager->handle_paused_queries();
    _assert_resumed(query);
    ASSERT_EQ(_paused_query_count(wg), 0);

    // The retried reservation succeeds (PipelineTask::_try_to_reserve_memory), a later one fails.
    query->resource_ctx()->task_controller()->end_process_memory_wait();
    _exceed_process_soft_mem_limit();
    _pause_for_process_memory(query);
    ASSERT_LT(_paused_elapsed_time(wg), config::spill_in_paused_queue_timeout_ms);
    for (int i = 0; i < 3; ++i) {
        _wg_manager->handle_paused_queries();
        _assert_still_paused(query);
        ASSERT_EQ(_paused_query_count(wg), 1);
    }
}

// Two workload groups exceed their min memory limit by more than 128 MiB when the walk starts.
// The first one holds only a query whose cancellation has already taken longer than
// `revoke_memory_max_tolerance_ms`, so it releases nothing; while it is scanned, the second one
// drops back within its min memory. The second one must then be skipped from its current usage
// instead of having its remaining query cancelled for the excess it had when the walk started.
TEST_F(WorkloadGroupManagerTest, process_mem_exceeded_below_min_memory_skips_peer_within_min) {
    auto wg = _create_wg_with_min_memory(1);
    auto query = _create_query_with_memory(wg, 1024L * 4);
    ASSERT_LE(wg->total_mem_used(), wg->min_memory_limit());

    auto first_peer_wg = _create_wg_with_min_memory(2);
    auto first_peer_query = _create_query_with_memory(first_peer_wg, 1024L * 1024 * 300);
    auto* first_peer_controller = _install_mock_query_task_controller(first_peer_query);
    first_peer_controller->cancel(Status::InternalError("memory gc cancel"));
    first_peer_controller->set_cancelled_time(first_peer_controller->cancelled_time() -
                                              config::revoke_memory_max_tolerance_ms - 1);

    auto second_peer_wg = _create_wg_with_min_memory(3);
    auto second_peer_query = _create_query_with_memory(second_peer_wg, 1024L * 1024 * 250);
    ASSERT_GT(second_peer_wg->total_mem_used(), second_peer_wg->min_memory_limit() + (1 << 27));
    // The first peer exceeds its min memory most, so it is scanned first.
    ASSERT_GT(first_peer_wg->total_mem_used() - first_peer_wg->min_memory_limit(),
              second_peer_wg->total_mem_used() - second_peer_wg->min_memory_limit());

    // While the first peer is scanned, the second peer's query releases 180 MiB and the
    // workload group drops to 70 MiB, within its 100 MiB min memory; its query is still larger
    // than SMALL_MEMORY_TASK, so it could be cancelled. Nothing refreshes the workload group
    // usage meanwhile, so its cached usage still shows the 250 MiB of the snapshot.
    bool released = false;
    first_peer_controller->on_is_cancelled_ = [&]() {
        if (released) {
            return;
        }
        released = true;
        _release_query_memory(second_peer_query, 1024L * 1024 * 180);
        ASSERT_GT(second_peer_wg->total_mem_used(), second_peer_wg->min_memory_limit());
    };

    _exceed_process_hard_mem_limit();
    _pause_for_process_memory(query);

    config::spill_in_paused_queue_timeout_ms = 60 * 1000;
    _wg_manager->handle_paused_queries();
    ASSERT_TRUE(released);
    ASSERT_FALSE(second_peer_query->is_cancelled());
    // The walk refreshed the second peer's usage before deciding on it.
    ASSERT_EQ(second_peer_wg->total_mem_used(), 1024L * 1024 * 70);
    ASSERT_EQ(second_peer_wg->min_memory_limit(), 1024L * 1024 * 100);
    // Nothing could be revoked, the paused query falls through to the hard-limit fallback.
    _assert_cancelled_by_process_memory(query, true);
}

// Two workload groups exceed their min memory limit by more than 128 MiB. The first one holds
// only a query whose cancellation has already taken longer than `revoke_memory_max_tolerance_ms`,
// so it releases nothing; while it is scanned, the process memory pressure is relieved (other
// queries or the cache released memory). The second one must not have its query cancelled for
// pressure that is gone, and the paused query must be resumed instead of being cancelled at the
// hard limit it was observed under before the walk.
TEST_F(WorkloadGroupManagerTest,
       process_mem_exceeded_below_min_memory_resumes_when_relieved_during_peer_scan) {
    auto wg = _create_wg_with_min_memory(1);
    auto query = _create_query_with_memory(wg, 1024L * 4);
    ASSERT_LE(wg->total_mem_used(), wg->min_memory_limit());

    auto first_peer_wg = _create_wg_with_min_memory(2);
    auto first_peer_query = _create_query_with_memory(first_peer_wg, 1024L * 1024 * 300);
    auto* first_peer_controller = _install_mock_query_task_controller(first_peer_query);
    first_peer_controller->cancel(Status::InternalError("memory gc cancel"));
    first_peer_controller->set_cancelled_time(first_peer_controller->cancelled_time() -
                                              config::revoke_memory_max_tolerance_ms - 1);

    auto second_peer_wg = _create_wg_with_min_memory(3);
    auto second_peer_query = _create_query_with_memory(second_peer_wg, 1024L * 1024 * 250);
    ASSERT_GT(second_peer_wg->total_mem_used(), second_peer_wg->min_memory_limit() + (1 << 27));
    // The first peer exceeds its min memory most, so it is scanned first.
    ASSERT_GT(first_peer_wg->total_mem_used() - first_peer_wg->min_memory_limit(),
              second_peer_wg->total_mem_used() - second_peer_wg->min_memory_limit());

    bool relieved = false;
    first_peer_controller->on_is_cancelled_ = [&]() {
        if (relieved) {
            return;
        }
        relieved = true;
        _relieve_process_mem_limit();
    };

    _exceed_process_hard_mem_limit();
    _pause_for_process_memory(query);

    config::spill_in_paused_queue_timeout_ms = 60 * 1000;
    _wg_manager->handle_paused_queries();
    ASSERT_TRUE(relieved);
    ASSERT_FALSE(second_peer_query->is_cancelled());
    ASSERT_FALSE(_wg_manager->revoking_memory_from_other_query_);
    _assert_resumed(query);
    ASSERT_EQ(_paused_query_count(wg), 0);
}

} // namespace doris
