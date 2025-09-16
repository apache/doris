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

#include "vec/exec/scan/scanner_context.h"

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/Types_types.h>
#include <gtest/gtest.h>

#include <list>
#include <memory>
#include <mutex>
#include <tuple>

#include "common/object_pool.h"
#include "mock_scanner_scheduler.h"
#include "mock_simplified_scan_scheduler.h"
#include "pipeline/dependency.h"
#include "pipeline/exec/olap_scan_operator.h"
#include "runtime/descriptors.h"
#include "runtime/query_context.h"
#include "testutil/mock/mock_runtime_state.h"
#include "vec/core/block.h"
#include "vec/exec/scan/olap_scanner.h"
#include "vec/exec/scan/scan_node.h"
#include "vec/exec/scan/scanner_scheduler.h"

namespace doris::vectorized {
class ScannerContextTest : public testing::Test {
public:
    void SetUp() override {
        obj_pool = std::make_unique<ObjectPool>();
        // This ScanNode has two tuples.
        // First one is input tuple, second one is output tuple.
        tnode.row_tuples.push_back(TTupleId(0));
        tnode.row_tuples.push_back(TTupleId(1));
        std::vector<bool> null_map {false, false};
        tnode.nullable_tuples = null_map;
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
        output_tuple_desc = descs->get_tuple_descriptor(0);
    }

private:
    class MockBlock : public Block {
        MockBlock() = default;
        MOCK_CONST_METHOD0(allocated_bytes, size_t());
        MOCK_METHOD0(mem_reuse, bool());
        MOCK_METHOD1(clear_column_data, void(int64_t));
    };

    class MockRuntimeStateLocal : public RuntimeState {
        MockRuntimeStateLocal() = default;
        MOCK_CONST_METHOD0(is_cancelled, bool());
        MOCK_CONST_METHOD0(cancel_reason, Status());
    };

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
    std::unique_ptr<RuntimeProfile::Counter> max_concurrency_counter =
            std::make_unique<RuntimeProfile::Counter>(TUnit::UNIT, 1, 3);
    std::unique_ptr<RuntimeProfile::Counter> min_concurrency_counter =
            std::make_unique<RuntimeProfile::Counter>(TUnit::UNIT, 1, 3);

    std::unique_ptr<RuntimeProfile::Counter> newly_create_free_blocks_num =
            std::make_unique<RuntimeProfile::Counter>(TUnit::UNIT, 1, 3);
    std::unique_ptr<RuntimeProfile::Counter> scanner_memory_used_counter =
            std::make_unique<RuntimeProfile::Counter>(TUnit::UNIT, 1, 3);

    TupleDescriptor* output_tuple_desc = nullptr;
    RowDescriptor* output_row_descriptor = nullptr;
    std::shared_ptr<pipeline::Dependency> scan_dependency =
            pipeline::Dependency::create_shared(0, 0, "TestScanDependency");
    std::shared_ptr<CgroupCpuCtl> cgroup_cpu_ctl = std::make_shared<CgroupV2CpuCtl>(1);
    std::unique_ptr<SimplifiedScanScheduler> scan_scheduler =
            std::make_unique<ThreadPoolSimplifiedScanScheduler>("ForTest", cgroup_cpu_ctl);
};

TEST_F(ScannerContextTest, test_init) {
    const int parallel_tasks = 1;
    auto scan_operator = std::make_unique<pipeline::OlapScanOperatorX>(
            obj_pool.get(), tnode, 0, *descs, parallel_tasks, TQueryCacheParam {});

    auto olap_scan_local_state =
            pipeline::OlapScanLocalState::create_unique(state.get(), scan_operator.get());

    const int64_t limit = 100;

    OlapScanner::Params scanner_params;
    scanner_params.state = state.get();
    scanner_params.profile = profile.get();
    scanner_params.limit = limit;
    scanner_params.key_ranges = std::vector<OlapScanRange*>(); // empty

    std::shared_ptr<Scanner> scanner =
            OlapScanner::create_shared(olap_scan_local_state.get(), std::move(scanner_params));

    std::list<std::shared_ptr<ScannerDelegate>> scanners;
    for (int i = 0; i < 11; ++i) {
        scanners.push_back(std::make_shared<ScannerDelegate>(scanner));
    }

    std::shared_ptr<ScannerContext> scanner_context = ScannerContext::create_shared(
            state.get(), olap_scan_local_state.get(), output_tuple_desc, output_row_descriptor,
            scanners, limit, scan_dependency, parallel_tasks);

    scan_operator->_should_run_serial = false;

    olap_scan_local_state->_max_scan_concurrency = max_concurrency_counter.get();
    olap_scan_local_state->_min_scan_concurrency = min_concurrency_counter.get();

    olap_scan_local_state->_parent = scan_operator.get();

    // User specified num_scanner_threads is less than _max_scan_concurrency that we calculated
    TQueryOptions query_options;
    query_options.__set_num_scanner_threads(2);
    query_options.__set_max_column_reader_num(0);
    state->set_query_options(query_options);
    std::unique_ptr<MockSimplifiedScanScheduler> scheduler =
            std::make_unique<MockSimplifiedScanScheduler>(cgroup_cpu_ctl);
    EXPECT_CALL(*scheduler, schedule_scan_task(testing::_, testing::_, testing::_))
            .WillRepeatedly(testing::Return(Status::OK()));
    scanner_context->_scanner_scheduler = scheduler.get();

    // max_scan_concurrency that we calculate will be 10 / 1 = 10;
    scanner_context->_min_scan_concurrency_of_scan_scheduler = 10;
    Status st = scanner_context->init();
    ASSERT_TRUE(st.ok());
    // actual max_scan_concurrency will be 2 since user specified num_scanner_threads is 2.
    ASSERT_EQ(scanner_context->_max_scan_concurrency, 2);

    query_options.__set_num_scanner_threads(0);
    state->set_query_options(query_options);

    st = scanner_context->init();
    ASSERT_TRUE(st.ok());

    ASSERT_EQ(scanner_context->_max_scan_concurrency,
              scanner_context->_min_scan_concurrency_of_scan_scheduler / parallel_tasks);
}

TEST_F(ScannerContextTest, test_serial_run) {
    const int parallel_tasks = 1;
    auto scan_operator = std::make_unique<pipeline::OlapScanOperatorX>(
            obj_pool.get(), tnode, 0, *descs, parallel_tasks, TQueryCacheParam {});

    auto olap_scan_local_state =
            pipeline::OlapScanLocalState::create_unique(state.get(), scan_operator.get());

    const int64_t limit = 100;

    OlapScanner::Params scanner_params;
    scanner_params.state = state.get();
    scanner_params.profile = profile.get();
    scanner_params.limit = limit;
    scanner_params.key_ranges = std::vector<OlapScanRange*>(); // empty

    std::shared_ptr<Scanner> scanner =
            OlapScanner::create_shared(olap_scan_local_state.get(), std::move(scanner_params));

    std::list<std::shared_ptr<ScannerDelegate>> scanners;
    for (int i = 0; i < 11; ++i) {
        scanners.push_back(std::make_shared<ScannerDelegate>(scanner));
    }

    std::shared_ptr<ScannerContext> scanner_context = ScannerContext::create_shared(
            state.get(), olap_scan_local_state.get(), output_tuple_desc, output_row_descriptor,
            scanners, limit, scan_dependency, parallel_tasks);

    scan_operator->_should_run_serial = true;

    olap_scan_local_state->_max_scan_concurrency = max_concurrency_counter.get();
    olap_scan_local_state->_min_scan_concurrency = min_concurrency_counter.get();

    olap_scan_local_state->_parent = scan_operator.get();

    TQueryOptions query_options;
    query_options.__set_num_scanner_threads(2);
    query_options.__set_max_column_reader_num(0);
    state->set_query_options(query_options);
    std::unique_ptr<MockSimplifiedScanScheduler> scheduler =
            std::make_unique<MockSimplifiedScanScheduler>(cgroup_cpu_ctl);
    EXPECT_CALL(*scheduler, schedule_scan_task(testing::_, testing::_, testing::_))
            .WillRepeatedly(testing::Return(Status::OK()));
    scanner_context->_scanner_scheduler = scheduler.get();

    scanner_context->_min_scan_concurrency_of_scan_scheduler = 10;
    Status st = scanner_context->init();
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(scanner_context->_max_scan_concurrency, 1);

    query_options.__set_num_scanner_threads(0);
    state->set_query_options(query_options);
    st = scanner_context->init();
    ASSERT_TRUE(st.ok());

    ASSERT_EQ(scanner_context->_max_scan_concurrency, 1);
}

TEST_F(ScannerContextTest, test_max_column_reader_num) {
    const int parallel_tasks = 1;
    auto scan_operator = std::make_unique<pipeline::OlapScanOperatorX>(
            obj_pool.get(), tnode, 0, *descs, parallel_tasks, TQueryCacheParam {});

    auto olap_scan_local_state =
            pipeline::OlapScanLocalState::create_unique(state.get(), scan_operator.get());

    const int64_t limit = 100;

    OlapScanner::Params scanner_params;
    scanner_params.state = state.get();
    scanner_params.profile = profile.get();
    scanner_params.limit = limit;
    scanner_params.key_ranges = std::vector<OlapScanRange*>(); // empty

    std::shared_ptr<Scanner> scanner =
            OlapScanner::create_shared(olap_scan_local_state.get(), std::move(scanner_params));

    std::list<std::shared_ptr<ScannerDelegate>> scanners;
    for (int i = 0; i < 20; ++i) {
        scanners.push_back(std::make_shared<ScannerDelegate>(scanner));
    }

    std::shared_ptr<ScannerContext> scanner_context = ScannerContext::create_shared(
            state.get(), olap_scan_local_state.get(), output_tuple_desc, output_row_descriptor,
            scanners, limit, scan_dependency, parallel_tasks);

    scan_operator->_should_run_serial = false;

    olap_scan_local_state->_max_scan_concurrency = max_concurrency_counter.get();
    olap_scan_local_state->_min_scan_concurrency = min_concurrency_counter.get();

    olap_scan_local_state->_parent = scan_operator.get();

    TQueryOptions query_options;
    query_options.__set_num_scanner_threads(20);
    query_options.__set_max_column_reader_num(1);
    state->set_query_options(query_options);
    std::unique_ptr<MockSimplifiedScanScheduler> scheduler =
            std::make_unique<MockSimplifiedScanScheduler>(cgroup_cpu_ctl);
    EXPECT_CALL(*scheduler, schedule_scan_task(testing::_, testing::_, testing::_))
            .WillRepeatedly(testing::Return(Status::OK()));
    scanner_context->_scanner_scheduler = scheduler.get();
    scanner_context->_min_scan_concurrency_of_scan_scheduler = 10;
    Status st = scanner_context->init();
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(scanner_context->_max_scan_concurrency, 1);
}

TEST_F(ScannerContextTest, test_push_back_scan_task) {
    const int parallel_tasks = 1;
    auto scan_operator = std::make_unique<pipeline::OlapScanOperatorX>(
            obj_pool.get(), tnode, 0, *descs, parallel_tasks, TQueryCacheParam {});

    auto olap_scan_local_state =
            pipeline::OlapScanLocalState::create_unique(state.get(), scan_operator.get());

    const int64_t limit = 100;

    OlapScanner::Params scanner_params;
    scanner_params.state = state.get();
    scanner_params.profile = profile.get();
    scanner_params.limit = limit;
    scanner_params.key_ranges = std::vector<OlapScanRange*>(); // empty

    std::shared_ptr<Scanner> scanner =
            OlapScanner::create_shared(olap_scan_local_state.get(), std::move(scanner_params));

    std::list<std::shared_ptr<ScannerDelegate>> scanners;
    for (int i = 0; i < 11; ++i) {
        scanners.push_back(std::make_shared<ScannerDelegate>(scanner));
    }

    std::shared_ptr<ScannerContext> scanner_context = ScannerContext::create_shared(
            state.get(), olap_scan_local_state.get(), output_tuple_desc, output_row_descriptor,
            scanners, limit, scan_dependency, parallel_tasks);

    scanner_context->_num_scheduled_scanners = 11;

    for (int i = 0; i < 5; ++i) {
        auto scan_task = std::make_shared<ScanTask>(std::make_shared<ScannerDelegate>(scanner));
        scanner_context->push_back_scan_task(scan_task);
        ASSERT_EQ(scanner_context->_num_scheduled_scanners, 10 - i);
    }
}

TEST_F(ScannerContextTest, get_margin) {
    const int parallel_tasks = 4;
    auto scan_operator = std::make_unique<pipeline::OlapScanOperatorX>(
            obj_pool.get(), tnode, 0, *descs, parallel_tasks, TQueryCacheParam {});

    auto olap_scan_local_state =
            pipeline::OlapScanLocalState::create_unique(state.get(), scan_operator.get());

    const int64_t limit = 100;

    OlapScanner::Params scanner_params;
    scanner_params.state = state.get();
    scanner_params.profile = profile.get();
    scanner_params.limit = limit;
    scanner_params.key_ranges = std::vector<OlapScanRange*>(); // empty

    std::shared_ptr<Scanner> scanner =
            OlapScanner::create_shared(olap_scan_local_state.get(), std::move(scanner_params));

    std::list<std::shared_ptr<ScannerDelegate>> scanners;
    for (int i = 0; i < 11; ++i) {
        scanners.push_back(std::make_shared<ScannerDelegate>(scanner));
    }

    std::shared_ptr<ScannerContext> scanner_context = ScannerContext::create_shared(
            state.get(), olap_scan_local_state.get(), output_tuple_desc, output_row_descriptor,
            scanners, limit, scan_dependency, parallel_tasks);

    std::mutex transfer_mutex;
    std::unique_lock<std::mutex> transfer_lock(transfer_mutex);
    std::shared_mutex scheduler_mutex;
    std::unique_lock<std::shared_mutex> scheduler_lock(scheduler_mutex);
    scanner_context->_scanner_scheduler = scan_scheduler.get();
    scanner_context->_min_scan_concurrency_of_scan_scheduler = 20;
    // _task_queue.size is 0.
    // _num_schedule_scanners is 0.
    std::shared_ptr<CgroupCpuCtl> cgroup_cpu_ctl = std::make_shared<CgroupV2CpuCtl>(1);

    // Has not submit any scan tasks.
    // ScanScheduler is empty too.
    // So margin shuold be equal to _min_scan_concurrency_of_scan_scheduler / parallel_tasks.
    // We can make full utilization of the resource.
    std::unique_ptr<MockSimplifiedScanScheduler> scheduler =
            std::make_unique<MockSimplifiedScanScheduler>(cgroup_cpu_ctl);
    EXPECT_CALL(*scheduler, get_active_threads()).WillOnce(testing::Return(0));
    EXPECT_CALL(*scheduler, get_queue_size()).WillOnce(testing::Return(0));
    scanner_context->_scanner_scheduler = scheduler.get();
    int32_t margin = scanner_context->_get_margin(transfer_lock, scheduler_lock);

    ASSERT_EQ(margin, scanner_context->_min_scan_concurrency_of_scan_scheduler);

    // ScanSchedule has 5 active threads and 10 tasks in queue.
    // So remaing margin(3) is less than parallel_tasks(4).
    scheduler = std::make_unique<MockSimplifiedScanScheduler>(cgroup_cpu_ctl);
    EXPECT_CALL(*scheduler, get_active_threads()).WillOnce(testing::Return(5));
    EXPECT_CALL(*scheduler, get_queue_size()).WillOnce(testing::Return(10));
    scanner_context->_scanner_scheduler = scheduler.get();
    scanner_context->_min_scan_concurrency_of_scan_scheduler = 18;
    margin = scanner_context->_get_margin(transfer_lock, scheduler_lock);
    // 18 - （5 + 10） = 3
    ASSERT_EQ(margin, 3);

    // ScanSchedule has 10 active threads and 2 tasks in queue.
    // Remaing margin(8) is greater than parallel_tasks(4).
    // So margin should be equal to margin(8)/parallel_tasks(4) == 2.
    scheduler = std::make_unique<MockSimplifiedScanScheduler>(cgroup_cpu_ctl);
    EXPECT_CALL(*scheduler, get_active_threads()).WillOnce(testing::Return(10));
    EXPECT_CALL(*scheduler, get_queue_size()).WillOnce(testing::Return(2));
    scanner_context->_scanner_scheduler = scheduler.get();
    scanner_context->_min_scan_concurrency_of_scan_scheduler = 20;
    margin = scanner_context->_get_margin(transfer_lock, scheduler_lock);
    ASSERT_EQ(margin, (scanner_context->_min_scan_concurrency_of_scan_scheduler - 12));

    // ScanSchedule is busy.
    // Just submit _min_scan_concurrency tasks.
    scheduler = std::make_unique<MockSimplifiedScanScheduler>(cgroup_cpu_ctl);
    EXPECT_CALL(*scheduler, get_active_threads()).WillOnce(testing::Return(50));
    EXPECT_CALL(*scheduler, get_queue_size()).WillOnce(testing::Return(10));
    scanner_context->_scanner_scheduler = scheduler.get();
    scanner_context->_min_scan_concurrency_of_scan_scheduler = 20;
    scanner_context->_num_scheduled_scanners = 0;
    margin = scanner_context->_get_margin(transfer_lock, scheduler_lock);
    ASSERT_EQ(margin, scanner_context->_min_scan_concurrency);

    // ScanSchedule is busy.
    // _min_scan_concurrency is already satisfied.
    scheduler = std::make_unique<MockSimplifiedScanScheduler>(cgroup_cpu_ctl);
    EXPECT_CALL(*scheduler, get_active_threads()).WillOnce(testing::Return(50));
    EXPECT_CALL(*scheduler, get_queue_size()).WillOnce(testing::Return(10));
    scanner_context->_scanner_scheduler = scheduler.get();
    scanner_context->_min_scan_concurrency_of_scan_scheduler = 20;
    scanner_context->_num_scheduled_scanners = 20;
    margin = scanner_context->_get_margin(transfer_lock, scheduler_lock);
    ASSERT_EQ(margin, 0);
}

TEST_F(ScannerContextTest, pull_next_scan_task) {
    const int parallel_tasks = 4;
    auto scan_operator = std::make_unique<pipeline::OlapScanOperatorX>(
            obj_pool.get(), tnode, 0, *descs, parallel_tasks, TQueryCacheParam {});

    auto olap_scan_local_state =
            pipeline::OlapScanLocalState::create_unique(state.get(), scan_operator.get());

    const int64_t limit = 100;

    OlapScanner::Params scanner_params;
    scanner_params.state = state.get();
    scanner_params.profile = profile.get();
    scanner_params.limit = limit;
    scanner_params.key_ranges = std::vector<OlapScanRange*>(); // empty

    std::shared_ptr<Scanner> scanner =
            OlapScanner::create_shared(olap_scan_local_state.get(), std::move(scanner_params));

    std::list<std::shared_ptr<ScannerDelegate>> scanners;
    for (int i = 0; i < 11; ++i) {
        scanners.push_back(std::make_shared<ScannerDelegate>(scanner));
    }

    std::shared_ptr<ScannerContext> scanner_context = ScannerContext::create_shared(
            state.get(), olap_scan_local_state.get(), output_tuple_desc, output_row_descriptor,
            scanners, limit, scan_dependency, parallel_tasks);

    std::mutex transfer_mutex;
    std::unique_lock<std::mutex> transfer_lock(transfer_mutex);
    std::shared_mutex scheduler_mutex;
    std::unique_lock<std::shared_mutex> scheduler_lock(scheduler_mutex);
    scanner_context->_scanner_scheduler = scan_scheduler.get();
    scanner_context->_min_scan_concurrency_of_scan_scheduler = 20;
    std::shared_ptr<CgroupCpuCtl> cgroup_cpu_ctl = std::make_shared<CgroupV2CpuCtl>(1);
    std::unique_ptr<MockSimplifiedScanScheduler> scheduler =
            std::make_unique<MockSimplifiedScanScheduler>(cgroup_cpu_ctl);

    scanner_context->_scanner_scheduler = scan_scheduler.get();
    scanner_context->_max_scan_concurrency = 1;
    std::shared_ptr<ScanTask> pull_scan_task =
            scanner_context->_pull_next_scan_task(nullptr, scanner_context->_max_scan_concurrency);
    ASSERT_EQ(pull_scan_task, nullptr);
    auto scan_task = std::make_shared<ScanTask>(std::make_shared<ScannerDelegate>(scanner));
    pull_scan_task = scanner_context->_pull_next_scan_task(scan_task,
                                                           scanner_context->_max_scan_concurrency);
    ASSERT_EQ(pull_scan_task, nullptr);

    scanner_context->_max_scan_concurrency = 2;
    BlockUPtr cached_block = Block::create_unique();
    scan_task->cached_blocks.emplace_back(std::move(cached_block), 0);
    EXPECT_ANY_THROW(scanner_context->_pull_next_scan_task(
            scan_task, scanner_context->_max_scan_concurrency - 1));
    scan_task->cached_blocks.clear();
    scan_task->eos = true;
    EXPECT_ANY_THROW(scanner_context->_pull_next_scan_task(
            scan_task, scanner_context->_max_scan_concurrency - 1));

    scan_task->cached_blocks.clear();
    scan_task->eos = false;
    pull_scan_task = scanner_context->_pull_next_scan_task(
            scan_task, scanner_context->_max_scan_concurrency - 1);
    EXPECT_EQ(pull_scan_task.get(), scan_task.get());

    scanner_context->_pending_scanners = std::stack<std::weak_ptr<ScannerDelegate>>();
    pull_scan_task = scanner_context->_pull_next_scan_task(
            nullptr, scanner_context->_max_scan_concurrency - 1);
    EXPECT_EQ(pull_scan_task, nullptr);

    scanner_context->_pending_scanners.push(std::make_shared<ScannerDelegate>(scanner));
    pull_scan_task = scanner_context->_pull_next_scan_task(
            nullptr, scanner_context->_max_scan_concurrency - 1);
    EXPECT_NE(pull_scan_task, nullptr);
}

TEST_F(ScannerContextTest, schedule_scan_task) {
    const int parallel_tasks = 4;
    auto scan_operator = std::make_unique<pipeline::OlapScanOperatorX>(
            obj_pool.get(), tnode, 0, *descs, parallel_tasks, TQueryCacheParam {});

    auto olap_scan_local_state =
            pipeline::OlapScanLocalState::create_unique(state.get(), scan_operator.get());

    const int64_t limit = 100;

    OlapScanner::Params scanner_params;
    scanner_params.state = state.get();
    scanner_params.profile = profile.get();
    scanner_params.limit = limit;
    scanner_params.key_ranges = std::vector<OlapScanRange*>(); // empty

    std::shared_ptr<Scanner> scanner =
            OlapScanner::create_shared(olap_scan_local_state.get(), std::move(scanner_params));

    std::list<std::shared_ptr<ScannerDelegate>> scanners;
    for (int i = 0; i < 15; ++i) {
        scanners.push_back(std::make_shared<ScannerDelegate>(scanner));
    }

    std::shared_ptr<ScannerContext> scanner_context = ScannerContext::create_shared(
            state.get(), olap_scan_local_state.get(), output_tuple_desc, output_row_descriptor,
            scanners, limit, scan_dependency, parallel_tasks);

    std::mutex transfer_mutex;
    std::unique_lock<std::mutex> transfer_lock(transfer_mutex);
    std::shared_mutex scheduler_mutex;
    std::unique_lock<std::shared_mutex> scheduler_lock(scheduler_mutex);
    std::shared_ptr<CgroupCpuCtl> cgroup_cpu_ctl = std::make_shared<CgroupV2CpuCtl>(1);

    // Scan resource is enough.
    std::unique_ptr<MockSimplifiedScanScheduler> scheduler =
            std::make_unique<MockSimplifiedScanScheduler>(cgroup_cpu_ctl);
    EXPECT_CALL(*scheduler, get_active_threads()).WillRepeatedly(testing::Return(0));
    EXPECT_CALL(*scheduler, get_queue_size()).WillRepeatedly(testing::Return(0));

    std::unique_ptr<MockScannerScheduler> scanner_scheduler =
            std::make_unique<MockScannerScheduler>();
    EXPECT_CALL(*scanner_scheduler, submit(testing::_, testing::_))
            .WillRepeatedly(testing::Return(Status::OK()));

    scanner_context->_scanner_scheduler_global = scanner_scheduler.get();
    scanner_context->_scanner_scheduler = scheduler.get();
    scanner_context->_max_scan_concurrency = 1;
    scanner_context->_max_scan_concurrency = 1;
    scanner_context->_min_scan_concurrency_of_scan_scheduler = 20;

    Status st = scanner_context->schedule_scan_task(nullptr, transfer_lock, scheduler_lock);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(scanner_context->_num_scheduled_scanners, 1);

    scanner_context->_max_scan_concurrency = 10;
    scanner_context->_max_scan_concurrency = 1;
    scanner_context->_min_scan_concurrency_of_scan_scheduler = 20;
    st = scanner_context->schedule_scan_task(nullptr, transfer_lock, scheduler_lock);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(scanner_context->_num_scheduled_scanners, scanner_context->_max_scan_concurrency);

    scanner_context = ScannerContext::create_shared(
            state.get(), olap_scan_local_state.get(), output_tuple_desc, output_row_descriptor,
            scanners, limit, scan_dependency, parallel_tasks);

    scanner_context->_scanner_scheduler_global = scanner_scheduler.get();
    scanner_context->_scanner_scheduler = scheduler.get();

    scanner_context->_max_scan_concurrency = 100;
    scanner_context->_min_scan_concurrency = 1;
    scanner_context->_min_scan_concurrency_of_scan_scheduler = 20;
    int margin = scanner_context->_get_margin(transfer_lock, scheduler_lock);
    ASSERT_EQ(margin, scanner_context->_min_scan_concurrency_of_scan_scheduler);
    st = scanner_context->schedule_scan_task(nullptr, transfer_lock, scheduler_lock);
    ASSERT_TRUE(st.ok());
    // 15 since we have 15 scanners.
    ASSERT_EQ(scanner_context->_num_scheduled_scanners, 15);

    scanners = std::list<std::shared_ptr<ScannerDelegate>>();
    for (int i = 0; i < 1; ++i) {
        scanners.push_back(std::make_shared<ScannerDelegate>(scanner));
    }

    scanner_context = ScannerContext::create_shared(
            state.get(), olap_scan_local_state.get(), output_tuple_desc, output_row_descriptor,
            scanners, limit, scan_dependency, parallel_tasks);

    scanner_context->_scanner_scheduler_global = scanner_scheduler.get();
    scanner_context->_scanner_scheduler = scheduler.get();

    scanner_context->_max_scan_concurrency = 1;
    scanner_context->_min_scan_concurrency = 1;
    scanner_context->_min_scan_concurrency_of_scan_scheduler = 20;
    st = scanner_context->schedule_scan_task(nullptr, transfer_lock, scheduler_lock);
    auto scan_task = std::make_shared<ScanTask>(std::make_shared<ScannerDelegate>(scanner));
    st = scanner_context->schedule_scan_task(scan_task, transfer_lock, scheduler_lock);
    // current scan task is added back.
    ASSERT_EQ(scanner_context->_pending_scanners.size(), 1);
    ASSERT_EQ(scanner_context->_num_scheduled_scanners, 1);

    scanner_context = ScannerContext::create_shared(
            state.get(), olap_scan_local_state.get(), output_tuple_desc, output_row_descriptor,
            scanners, limit, scan_dependency, parallel_tasks);

    scanner_context->_scanner_scheduler_global = scanner_scheduler.get();
    scanner_context->_scanner_scheduler = scheduler.get();

    scanner_context->_max_scan_concurrency = 1;
    scanner_context->_min_scan_concurrency = 1;
    scanner_context->_min_scan_concurrency_of_scan_scheduler = 20;
    st = scanner_context->schedule_scan_task(nullptr, transfer_lock, scheduler_lock);
    scan_task = std::make_shared<ScanTask>(std::make_shared<ScannerDelegate>(scanner));
    scan_task->cached_blocks.emplace_back(Block::create_unique(), 0);
    // Illigeal situation.
    // If current scan task has cached block, it should not be called with this methods.
    EXPECT_ANY_THROW(std::ignore = scanner_context->schedule_scan_task(scan_task, transfer_lock,
                                                                       scheduler_lock));
}

TEST_F(ScannerContextTest, scan_queue_mem_limit) {
    state->_query_options.__set_scan_queue_mem_limit(100);
    ASSERT_EQ(state->scan_queue_mem_limit(), 100);

    state->_query_options.__isset.scan_queue_mem_limit = false;
    state->_query_options.__set_mem_limit(200);
    ASSERT_EQ(state->scan_queue_mem_limit(), 200 / 20);

    const int parallel_tasks = 1;
    auto scan_operator = std::make_unique<pipeline::OlapScanOperatorX>(
            obj_pool.get(), tnode, 0, *descs, parallel_tasks, TQueryCacheParam {});

    auto olap_scan_local_state =
            pipeline::OlapScanLocalState::create_unique(state.get(), scan_operator.get());
    olap_scan_local_state->_max_scan_concurrency = max_concurrency_counter.get();
    olap_scan_local_state->_min_scan_concurrency = min_concurrency_counter.get();

    olap_scan_local_state->_parent = scan_operator.get();

    const int64_t limit = 100;

    OlapScanner::Params scanner_params;
    scanner_params.state = state.get();
    scanner_params.profile = profile.get();
    scanner_params.limit = limit;
    scanner_params.key_ranges = std::vector<OlapScanRange*>(); // empty

    std::shared_ptr<Scanner> scanner =
            OlapScanner::create_shared(olap_scan_local_state.get(), std::move(scanner_params));

    std::list<std::shared_ptr<ScannerDelegate>> scanners;
    for (int i = 0; i < 11; ++i) {
        scanners.push_back(std::make_shared<ScannerDelegate>(scanner));
    }

    std::shared_ptr<ScannerContext> scanner_context = ScannerContext::create_shared(
            state.get(), olap_scan_local_state.get(), output_tuple_desc, output_row_descriptor,
            scanners, limit, scan_dependency, parallel_tasks);

    std::unique_ptr<MockSimplifiedScanScheduler> scheduler =
            std::make_unique<MockSimplifiedScanScheduler>(cgroup_cpu_ctl);
    EXPECT_CALL(*scheduler, schedule_scan_task(testing::_, testing::_, testing::_))
            .WillRepeatedly(testing::Return(Status::OK()));
    scanner_context->_scanner_scheduler = scheduler.get();
    // max_scan_concurrency that we calculate will be 10 / 1 = 10;
    scanner_context->_min_scan_concurrency_of_scan_scheduler = 10;

    std::ignore = scanner_context->init();
    ASSERT_EQ(scanner_context->_max_bytes_in_queue, (1024 * 1024 * 10) * (1 / 300 + 1));
}

TEST_F(ScannerContextTest, get_free_block) {
    const int parallel_tasks = 1;
    auto scan_operator = std::make_unique<pipeline::OlapScanOperatorX>(
            obj_pool.get(), tnode, 0, *descs, parallel_tasks, TQueryCacheParam {});

    auto olap_scan_local_state =
            pipeline::OlapScanLocalState::create_unique(state.get(), scan_operator.get());

    const int64_t limit = 100;

    OlapScanner::Params scanner_params;
    scanner_params.state = state.get();
    scanner_params.profile = profile.get();
    scanner_params.limit = limit;
    scanner_params.key_ranges = std::vector<OlapScanRange*>(); // empty

    std::shared_ptr<Scanner> scanner =
            OlapScanner::create_shared(olap_scan_local_state.get(), std::move(scanner_params));

    std::list<std::shared_ptr<ScannerDelegate>> scanners;
    for (int i = 0; i < 11; ++i) {
        scanners.push_back(std::make_shared<ScannerDelegate>(scanner));
    }

    std::shared_ptr<ScannerContext> scanner_context = ScannerContext::create_shared(
            state.get(), olap_scan_local_state.get(), output_tuple_desc, output_row_descriptor,
            scanners, limit, scan_dependency, parallel_tasks);
    scanner_context->_newly_create_free_blocks_num = newly_create_free_blocks_num.get();
    scanner_context->_newly_create_free_blocks_num->set(0L);
    scanner_context->_scanner_memory_used_counter = scanner_memory_used_counter.get();
    scanner_context->_scanner_memory_used_counter->set(0L);
    BlockUPtr block = scanner_context->get_free_block(/*force=*/true);
    ASSERT_NE(block, nullptr);
    ASSERT_TRUE(scanner_context->_newly_create_free_blocks_num->value() == 1);

    scanner_context->_max_bytes_in_queue = 200;
    // no free block
    // force is false, _block_memory_usage < _max_bytes_in_queue
    block = scanner_context->get_free_block(/*force=*/false);
    ASSERT_NE(block, nullptr);
    ASSERT_TRUE(scanner_context->_newly_create_free_blocks_num->value() == 2);

    std::unique_ptr<MockBlock> return_block = std::make_unique<MockBlock>();
    EXPECT_CALL(*return_block, allocated_bytes()).WillRepeatedly(testing::Return(100));
    EXPECT_CALL(*return_block, mem_reuse()).WillRepeatedly(testing::Return(true));
    scanner_context->_free_blocks.enqueue(std::move(return_block));
    // get free block from queue
    block = scanner_context->get_free_block(/*force=*/false);
    ASSERT_NE(block, nullptr);
    ASSERT_EQ(scanner_context->_block_memory_usage, -100);
    ASSERT_EQ(scanner_context->_scanner_memory_used_counter->value(), -100);
}

TEST_F(ScannerContextTest, return_free_block) {
    const int parallel_tasks = 1;
    auto scan_operator = std::make_unique<pipeline::OlapScanOperatorX>(
            obj_pool.get(), tnode, 0, *descs, parallel_tasks, TQueryCacheParam {});

    auto olap_scan_local_state =
            pipeline::OlapScanLocalState::create_unique(state.get(), scan_operator.get());

    const int64_t limit = 100;

    OlapScanner::Params scanner_params;
    scanner_params.state = state.get();
    scanner_params.profile = profile.get();
    scanner_params.limit = limit;
    scanner_params.key_ranges = std::vector<OlapScanRange*>(); // empty

    std::shared_ptr<Scanner> scanner =
            OlapScanner::create_shared(olap_scan_local_state.get(), std::move(scanner_params));

    std::list<std::shared_ptr<ScannerDelegate>> scanners;
    for (int i = 0; i < 11; ++i) {
        scanners.push_back(std::make_shared<ScannerDelegate>(scanner));
    }

    std::shared_ptr<ScannerContext> scanner_context = ScannerContext::create_shared(
            state.get(), olap_scan_local_state.get(), output_tuple_desc, output_row_descriptor,
            scanners, limit, scan_dependency, parallel_tasks);
    scanner_context->_newly_create_free_blocks_num = newly_create_free_blocks_num.get();
    scanner_context->_scanner_memory_used_counter = scanner_memory_used_counter.get();
    scanner_context->_max_bytes_in_queue = 200;
    scanner_context->_block_memory_usage = 0;

    std::unique_ptr<MockBlock> return_block = std::make_unique<MockBlock>();
    EXPECT_CALL(*return_block, allocated_bytes()).WillRepeatedly(testing::Return(100));
    EXPECT_CALL(*return_block, mem_reuse()).WillRepeatedly(testing::Return(true));
    EXPECT_CALL(*return_block, clear_column_data(testing::_)).WillRepeatedly(testing::Return());

    scanner_context->return_free_block(std::move(return_block));
    ASSERT_EQ(scanner_context->_block_memory_usage, 100);
    ASSERT_EQ(scanner_context->_scanner_memory_used_counter->value(), 100);
    // free_block queue is stabilized, so size_approx is accurate.
    ASSERT_EQ(scanner_context->_free_blocks.size_approx(), 1);
}

TEST_F(ScannerContextTest, get_block_from_queue) {
    const int parallel_tasks = 1;
    auto scan_operator = std::make_unique<pipeline::OlapScanOperatorX>(
            obj_pool.get(), tnode, 0, *descs, parallel_tasks, TQueryCacheParam {});

    auto olap_scan_local_state =
            pipeline::OlapScanLocalState::create_unique(state.get(), scan_operator.get());

    const int64_t limit = 100;

    OlapScanner::Params scanner_params;
    scanner_params.state = state.get();
    scanner_params.profile = profile.get();
    scanner_params.limit = limit;
    scanner_params.key_ranges = std::vector<OlapScanRange*>(); // empty

    std::shared_ptr<Scanner> scanner =
            OlapScanner::create_shared(olap_scan_local_state.get(), std::move(scanner_params));

    std::list<std::shared_ptr<ScannerDelegate>> scanners;
    for (int i = 0; i < 11; ++i) {
        scanners.push_back(std::make_shared<ScannerDelegate>(scanner));
    }

    std::shared_ptr<ScannerContext> scanner_context = ScannerContext::create_shared(
            state.get(), olap_scan_local_state.get(), output_tuple_desc, output_row_descriptor,
            scanners, limit, scan_dependency, parallel_tasks);
    scanner_context->_newly_create_free_blocks_num = newly_create_free_blocks_num.get();
    scanner_context->_scanner_memory_used_counter = scanner_memory_used_counter.get();
    scanner_context->_max_bytes_in_queue = 200;
    scanner_context->_block_memory_usage = 0;

    std::unique_ptr<MockBlock> return_block = std::make_unique<MockBlock>();
    EXPECT_CALL(*return_block, allocated_bytes()).WillRepeatedly(testing::Return(100));
    EXPECT_CALL(*return_block, mem_reuse()).WillRepeatedly(testing::Return(true));
    EXPECT_CALL(*return_block, clear_column_data(testing::_)).WillRepeatedly(testing::Return());

    std::unique_ptr<MockRuntimeStateLocal> mock_runtime_state =
            std::make_unique<MockRuntimeStateLocal>();
    EXPECT_CALL(*mock_runtime_state, is_cancelled()).WillOnce(testing::Return(true));
    EXPECT_CALL(*mock_runtime_state, cancel_reason())
            .WillOnce(testing::Return(Status::Cancelled("TestCancelMsg")));
    bool eos = false;
    Status st = scanner_context->get_block_from_queue(mock_runtime_state.get(), return_block.get(),
                                                      &eos, 0);
    EXPECT_TRUE(!st.ok());
    EXPECT_EQ(st.msg(), "TestCancelMsg");

    EXPECT_CALL(*mock_runtime_state, is_cancelled()).WillRepeatedly(testing::Return(false));

    scanner_context->_process_status = Status::InternalError("TestCancel");
    st = scanner_context->get_block_from_queue(mock_runtime_state.get(), return_block.get(), &eos,
                                               0);
    EXPECT_TRUE(!st.ok());
    EXPECT_TRUE(st.msg() == "TestCancel");

    scanner_context->_process_status = Status::OK();
    scanner_context->_is_finished = false;
    scanner_context->_should_stop = false;
    auto scan_task = std::make_shared<ScanTask>(std::make_shared<ScannerDelegate>(scanner));
    scan_task->set_eos(true);
    scanner_context->_tasks_queue.push_back(scan_task);
    std::unique_ptr<MockSimplifiedScanScheduler> scheduler =
            std::make_unique<MockSimplifiedScanScheduler>(cgroup_cpu_ctl);
    EXPECT_CALL(*scheduler, schedule_scan_task(testing::_, testing::_, testing::_))
            .WillOnce(testing::Return(Status::OK()));
    scanner_context->_scanner_scheduler = scheduler.get();
    scanner_context->_num_finished_scanners = 0;
    EXPECT_CALL(*return_block, mem_reuse()).WillRepeatedly(testing::Return(false));
    st = scanner_context->get_block_from_queue(mock_runtime_state.get(), return_block.get(), &eos,
                                               0);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(scanner_context->_num_finished_scanners, 1);
}

} // namespace doris::vectorized
