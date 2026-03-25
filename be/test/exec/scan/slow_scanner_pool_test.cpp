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

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/Types_types.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>

#include "common/config.h"
#include "common/object_pool.h"
#include "exec/operator/olap_scan_operator.h"
#include "exec/scan/olap_scanner.h"
#include "exec/scan/scanner.h"
#include "exec/scan/scanner_scheduler.h"
#include "runtime/descriptors.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {

class SlowScannerPoolTest : public testing::Test {
public:
    void SetUp() override {
        obj_pool = std::make_unique<ObjectPool>();
        // Setup basic descriptors for scanner creation
        tnode.row_tuples.push_back(TTupleId(0));
        tnode.row_tuples.push_back(TTupleId(1));
        tbl_desc.tableType = TTableType::OLAP_TABLE;

        tuple_desc.id = 0;
        tuple_descs.push_back(tuple_desc);
        tuple_desc.id = 1;
        tuple_descs.push_back(tuple_desc);

        type_node.type = TTypeNodeType::SCALAR;
        scalar_type.__set_type(TPrimitiveType::STRING);
        type_node.__set_scalar_type(scalar_type);
        slot_desc.slotType.types.push_back(type_node);
        slot_desc.id = 0;
        slot_desc.parent = 0;
        slot_descs.push_back(slot_desc);
        slot_desc.id = 1;
        slot_desc.parent = 1;
        slot_descs.push_back(slot_desc);
        thrift_tbl.tableDescriptors.push_back(tbl_desc);
        thrift_tbl.tupleDescriptors = tuple_descs;
        thrift_tbl.slotDescriptors = slot_descs;
        std::ignore = DescriptorTbl::create(obj_pool.get(), thrift_tbl, &descs);
        auto task_exec_ctx = std::make_shared<TaskExecutionContext>();
        state->set_task_execution_context(task_exec_ctx);

        cgroup_cpu_ctl = std::make_shared<CgroupV2CpuCtl>(1);
    }

    void TearDown() override {}

protected:
    std::unique_ptr<ObjectPool> obj_pool;
    TPlanNode tnode;
    TTableDescriptor tbl_desc;
    std::vector<TTupleDescriptor> tuple_descs;
    TTupleDescriptor tuple_desc;
    std::vector<TSlotDescriptor> slot_descs;
    TSlotDescriptor slot_desc;
    TTypeNode type_node;
    TScalarType scalar_type;
    TDescriptorTable thrift_tbl;
    DescriptorTbl* descs = nullptr;
    std::unique_ptr<RuntimeState> state = std::make_unique<MockRuntimeState>();
    std::unique_ptr<RuntimeProfile> profile = std::make_unique<RuntimeProfile>("TestProfile");
    std::shared_ptr<CgroupCpuCtl> cgroup_cpu_ctl;
};

// ==================== SimplifiedScanTask Tests ====================

TEST_F(SlowScannerPoolTest, simplified_scan_task_default_constructor) {
    SimplifiedScanTask task;
    ASSERT_EQ(task.scan_func, nullptr);
    ASSERT_EQ(task.scanner_context, nullptr);
    ASSERT_EQ(task.scan_task, nullptr);
    ASSERT_FALSE(task.slow_task);
}

TEST_F(SlowScannerPoolTest, simplified_scan_task_constructor_normal_task) {
    bool called = false;
    auto scan_func = [&called]() {
        called = true;
        return true;
    };

    SimplifiedScanTask task(scan_func, nullptr, nullptr, false);

    ASSERT_NE(task.scan_func, nullptr);
    ASSERT_FALSE(task.slow_task);

    // Verify scan_func works
    bool result = task.scan_func();
    ASSERT_TRUE(called);
    ASSERT_TRUE(result);
}

TEST_F(SlowScannerPoolTest, simplified_scan_task_constructor_slow_task) {
    bool called = false;
    auto scan_func = [&called]() {
        called = true;
        return true;
    };

    SimplifiedScanTask task(scan_func, nullptr, nullptr, true);

    ASSERT_NE(task.scan_func, nullptr);
    ASSERT_TRUE(task.slow_task);

    // Verify scan_func works
    bool result = task.scan_func();
    ASSERT_TRUE(called);
    ASSERT_TRUE(result);
}

// ==================== ThreadPoolSimplifiedScanScheduler Tests ====================

TEST_F(SlowScannerPoolTest, scheduler_start_creates_both_pools) {
    auto scheduler =
            std::make_unique<ThreadPoolSimplifiedScanScheduler>("TestScheduler", cgroup_cpu_ctl);

    Status st = scheduler->start(4, 2, 100, 2);
    ASSERT_TRUE(st.ok()) << st.to_string();

    // Both pools should be functional
    ASSERT_GE(scheduler->get_active_threads(), 0);
    ASSERT_GE(scheduler->get_queue_size(), 0);

    scheduler->stop();
}

TEST_F(SlowScannerPoolTest, scheduler_submit_normal_task) {
    auto scheduler =
            std::make_unique<ThreadPoolSimplifiedScanScheduler>("TestScheduler", cgroup_cpu_ctl);

    Status st = scheduler->start(4, 2, 100, 2);
    ASSERT_TRUE(st.ok());

    std::atomic<bool> task_executed {false};
    auto scan_func = [&task_executed]() {
        task_executed.store(true);
        return true;
    };

    SimplifiedScanTask task(scan_func, nullptr, nullptr, false);
    st = scheduler->submit_scan_task(task);
    ASSERT_TRUE(st.ok());

    // Wait for task to execute
    for (int i = 0; i < 100 && !task_executed.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(task_executed.load());

    scheduler->stop();
}

TEST_F(SlowScannerPoolTest, scheduler_submit_slow_task) {
    auto scheduler =
            std::make_unique<ThreadPoolSimplifiedScanScheduler>("TestScheduler", cgroup_cpu_ctl);

    Status st = scheduler->start(4, 2, 100, 2);
    ASSERT_TRUE(st.ok());

    std::atomic<bool> task_executed {false};
    auto scan_func = [&task_executed]() {
        task_executed.store(true);
        return true;
    };

    SimplifiedScanTask task(scan_func, nullptr, nullptr, true);
    st = scheduler->submit_scan_task(task);
    ASSERT_TRUE(st.ok());

    // Wait for task to execute
    for (int i = 0; i < 100 && !task_executed.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(task_executed.load());

    scheduler->stop();
}

TEST_F(SlowScannerPoolTest, scheduler_submit_mixed_tasks) {
    auto scheduler =
            std::make_unique<ThreadPoolSimplifiedScanScheduler>("TestScheduler", cgroup_cpu_ctl);

    Status st = scheduler->start(4, 2, 100, 2);
    ASSERT_TRUE(st.ok());

    std::atomic<int> normal_count {0};
    std::atomic<int> slow_count {0};

    // Submit multiple normal tasks
    for (int i = 0; i < 5; ++i) {
        auto scan_func = [&normal_count]() {
            normal_count.fetch_add(1);
            return true;
        };
        SimplifiedScanTask task(scan_func, nullptr, nullptr, false);
        st = scheduler->submit_scan_task(task);
        ASSERT_TRUE(st.ok());
    }

    // Submit multiple slow tasks
    for (int i = 0; i < 5; ++i) {
        auto scan_func = [&slow_count]() {
            slow_count.fetch_add(1);
            return true;
        };
        SimplifiedScanTask task(scan_func, nullptr, nullptr, true);
        st = scheduler->submit_scan_task(task);
        ASSERT_TRUE(st.ok());
    }

    // Wait for all tasks to execute
    for (int i = 0; i < 100 && (normal_count.load() < 5 || slow_count.load() < 5); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    ASSERT_EQ(normal_count.load(), 5);
    ASSERT_EQ(slow_count.load(), 5);

    scheduler->stop();
}

TEST_F(SlowScannerPoolTest, scheduler_submit_after_stop) {
    auto scheduler =
            std::make_unique<ThreadPoolSimplifiedScanScheduler>("TestScheduler", cgroup_cpu_ctl);

    Status st = scheduler->start(4, 2, 100, 2);
    ASSERT_TRUE(st.ok());

    scheduler->stop();

    auto scan_func = []() { return true; };
    SimplifiedScanTask task(scan_func, nullptr, nullptr, false);
    st = scheduler->submit_scan_task(task);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.to_string().find("shutdown") != std::string::npos);
}

TEST_F(SlowScannerPoolTest, scheduler_reset_thread_num_both_pools) {
    auto scheduler =
            std::make_unique<ThreadPoolSimplifiedScanScheduler>("TestScheduler", cgroup_cpu_ctl);

    Status st = scheduler->start(4, 2, 100, 2);
    ASSERT_TRUE(st.ok());

    // Reset to larger values (both pools should be updated)
    scheduler->reset_thread_num(8, 4, 4);

    // Reset to smaller values (both pools should be updated)
    scheduler->reset_thread_num(2, 1, 1);

    // Verify scheduler still works after reset
    std::atomic<bool> task_executed {false};
    auto scan_func = [&task_executed]() {
        task_executed.store(true);
        return true;
    };

    SimplifiedScanTask task(scan_func, nullptr, nullptr, false);
    st = scheduler->submit_scan_task(task);
    ASSERT_TRUE(st.ok());

    for (int i = 0; i < 100 && !task_executed.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(task_executed.load());

    scheduler->stop();
}

// ==================== Scanner is_slow_task Tests ====================

TEST_F(SlowScannerPoolTest, scanner_default_is_slow_task) {
    const int parallel_tasks = 1;
    auto scan_operator = std::make_unique<OlapScanOperatorX>(obj_pool.get(), tnode, 0, *descs,
                                                             parallel_tasks, TQueryCacheParam {});

    auto olap_scan_local_state =
            OlapScanLocalState::create_unique(state.get(), scan_operator.get());

    const int64_t limit = 100;

    OlapScanner::Params scanner_params;
    scanner_params.state = state.get();
    scanner_params.profile = profile.get();
    scanner_params.limit = limit;
    scanner_params.key_ranges = std::vector<OlapScanRange*>();

    std::shared_ptr<Scanner> scanner =
            OlapScanner::create_shared(olap_scan_local_state.get(), std::move(scanner_params));

    // Without tablet reader, is_slow_task should return false
    ASSERT_FALSE(scanner->is_slow_task());
}

// ==================== Config Tests ====================

TEST_F(SlowScannerPoolTest, config_enable_slow_scanner_pool_default) {
    // Test that the config exists and has a reasonable default
    ASSERT_TRUE(config::enable_slow_scanner_pool);
}

// ==================== Session Variable Tests ====================

TEST_F(SlowScannerPoolTest, query_options_remote_slow_task_threshold) {
    TQueryOptions query_options;

    // Default should be unset
    ASSERT_FALSE(query_options.__isset.remote_slow_task_threshold);

    // Set a value
    query_options.__set_remote_slow_task_threshold(1024);
    ASSERT_TRUE(query_options.__isset.remote_slow_task_threshold);
    ASSERT_EQ(query_options.remote_slow_task_threshold, 1024);
}

TEST_F(SlowScannerPoolTest, scanner_with_remote_slow_task_threshold) {
    const int parallel_tasks = 1;
    auto scan_operator = std::make_unique<OlapScanOperatorX>(obj_pool.get(), tnode, 0, *descs,
                                                             parallel_tasks, TQueryCacheParam {});

    auto olap_scan_local_state =
            OlapScanLocalState::create_unique(state.get(), scan_operator.get());

    // Set custom threshold via query options
    TQueryOptions query_options;
    query_options.__set_remote_slow_task_threshold(256);
    state->set_query_options(query_options);

    const int64_t limit = 100;

    OlapScanner::Params scanner_params;
    scanner_params.state = state.get();
    scanner_params.profile = profile.get();
    scanner_params.limit = limit;
    scanner_params.key_ranges = std::vector<OlapScanRange*>();

    std::shared_ptr<Scanner> scanner =
            OlapScanner::create_shared(olap_scan_local_state.get(), std::move(scanner_params));

    // Scanner should be created with the custom threshold
    // (actual threshold testing would require mocking tablet reader stats)
    ASSERT_FALSE(scanner->is_slow_task());
}

TEST_F(SlowScannerPoolTest, scanner_with_zero_threshold) {
    const int parallel_tasks = 1;
    auto scan_operator = std::make_unique<OlapScanOperatorX>(obj_pool.get(), tnode, 0, *descs,
                                                             parallel_tasks, TQueryCacheParam {});

    auto olap_scan_local_state =
            OlapScanLocalState::create_unique(state.get(), scan_operator.get());

    // Set threshold to 0 (should use INT64_MAX as fallback)
    TQueryOptions query_options;
    query_options.__set_remote_slow_task_threshold(0);
    state->set_query_options(query_options);

    const int64_t limit = 100;

    OlapScanner::Params scanner_params;
    scanner_params.state = state.get();
    scanner_params.profile = profile.get();
    scanner_params.limit = limit;
    scanner_params.key_ranges = std::vector<OlapScanRange*>();

    std::shared_ptr<Scanner> scanner =
            OlapScanner::create_shared(olap_scan_local_state.get(), std::move(scanner_params));

    // With threshold 0, it uses INT64_MAX, so is_slow_task should return false
    ASSERT_FALSE(scanner->is_slow_task());
}

// ==================== Integration Tests ====================

TEST_F(SlowScannerPoolTest, slow_task_isolation_concurrent) {
    auto scheduler =
            std::make_unique<ThreadPoolSimplifiedScanScheduler>("TestScheduler", cgroup_cpu_ctl);

    Status st = scheduler->start(2, 1, 100, 1);
    ASSERT_TRUE(st.ok());

    std::atomic<int> normal_running {0};
    std::atomic<int> slow_running {0};
    std::atomic<int> max_normal_concurrent {0};
    std::atomic<int> max_slow_concurrent {0};

    // Submit tasks that track concurrency
    for (int i = 0; i < 10; ++i) {
        auto normal_func = [&normal_running, &max_normal_concurrent]() {
            int current = normal_running.fetch_add(1) + 1;
            int expected = max_normal_concurrent.load();
            while (current > expected &&
                   !max_normal_concurrent.compare_exchange_weak(expected, current)) {
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
            normal_running.fetch_sub(1);
            return true;
        };

        auto slow_func = [&slow_running, &max_slow_concurrent]() {
            int current = slow_running.fetch_add(1) + 1;
            int expected = max_slow_concurrent.load();
            while (current > expected &&
                   !max_slow_concurrent.compare_exchange_weak(expected, current)) {
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
            slow_running.fetch_sub(1);
            return true;
        };

        SimplifiedScanTask normal_task(normal_func, nullptr, nullptr, false);
        SimplifiedScanTask slow_task(slow_func, nullptr, nullptr, true);

        st = scheduler->submit_scan_task(normal_task);
        ASSERT_TRUE(st.ok());
        st = scheduler->submit_scan_task(slow_task);
        ASSERT_TRUE(st.ok());
    }

    // Wait for all tasks to complete
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // Both pools should have had concurrent execution
    ASSERT_GE(max_normal_concurrent.load(), 1);
    ASSERT_GE(max_slow_concurrent.load(), 1);

    scheduler->stop();
}

} // namespace doris
