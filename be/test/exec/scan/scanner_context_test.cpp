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

#include "exec/scan/scanner_context.h"

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/Types_types.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <thread>
#include <tuple>
#include <vector>

#include "common/config.h"
#include "common/object_pool.h"
#include "core/block/block.h"
#include "exec/operator/olap_scan_operator.h"
#include "exec/pipeline/dependency.h"
#include "exec/scan/mock_simplified_scan_scheduler.h"
#include "exec/scan/olap_scanner.h"
#include "exec/scan/scan_node.h"
#include "exec/scan/scanner.h"
#include "exec/scan/scanner_scheduler.h"
#include "runtime/descriptors.h"
#include "runtime/query_context.h"
#include "runtime/task_execution_context.h"
#include "testutil/mock/mock_runtime_state.h"
#include "util/countdown_latch.h"
#include "util/debug_points.h"
#include "util/defer_op.h"

namespace doris {
// A scanner that produces `blocks_per_scanner` one-row blocks and then reports EOS, without any
// tablet or file behind it. It lets the ThreadPool scheduler chain (admit -> execute -> publish ->
// consume -> re-admit) run end to end in a unit test. `overlap` is counted down the first time two
// attempts run concurrently; the first attempt waits for it so the peak concurrency observed by the
// test does not depend on timing.
class ChainMockScanner : public Scanner {
public:
    ChainMockScanner(RuntimeState* state, ScanLocalStateBase* local_state, RuntimeProfile* profile,
                     int blocks_per_scanner, std::atomic<int>* running,
                     std::atomic<int>* peak_running, CountDownLatch* overlap)
            : Scanner(state, local_state, -1, profile),
              _blocks_left(blocks_per_scanner),
              _running(running),
              _peak_running(peak_running),
              _overlap(overlap) {}

protected:
    Status _get_block_impl(RuntimeState* /*state*/, Block* block, bool* eof) override {
        const int running = ++*_running;
        Defer done([&] { --*_running; });
        int peak = _peak_running->load();
        while (running > peak && !_peak_running->compare_exchange_weak(peak, running)) {
        }
        if (running >= 2) {
            _overlap->count_down();
        } else {
            // Bounded so a scheduler that never admits a second scanner fails the test instead of
            // hanging it.
            static_cast<void>(_overlap->wait_for(std::chrono::seconds(5)));
        }
        if (_blocks_left == 0) {
            *eof = true;
            return Status::OK();
        }
        --_blocks_left;
        block->get_by_position(0).column->assert_mutable()->insert_default();
        *eof = false;
        return Status::OK();
    }

    // The local state in these tests has no profile counters.
    void _collect_profile_before_close() override {}

private:
    int _blocks_left;
    std::atomic<int>* _running;
    std::atomic<int>* _peak_running;
    CountDownLatch* _overlap;
};

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
    std::shared_ptr<Dependency> scan_dependency =
            Dependency::create_shared(0, 0, "TestScanDependency");
    std::shared_ptr<CgroupCpuCtl> cgroup_cpu_ctl = std::make_shared<CgroupV2CpuCtl>(1);
    std::unique_ptr<ScannerScheduler> scan_scheduler =
            std::make_unique<ThreadPoolSimplifiedScanScheduler>("ForTest", cgroup_cpu_ctl);
};

TEST_F(ScannerContextTest, test_init) {
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

    // User specified max_scanners_concurrency is less than _max_scan_concurrency that we calculated
    TQueryOptions query_options;
    query_options.__set_max_scanners_concurrency(2);
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
    // actual max_scan_concurrency will be 2 since user specified max_scanners_concurrency is 2.
    ASSERT_EQ(scanner_context->_max_scan_concurrency, 1);

    query_options.__set_max_scanners_concurrency(0);
    state->set_query_options(query_options);

    st = scanner_context->init();
    ASSERT_TRUE(st.ok());
}

TEST_F(ScannerContextTest, test_serial_run) {
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
    query_options.__set_max_scanners_concurrency(2);
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

    query_options.__set_max_scanners_concurrency(0);
    state->set_query_options(query_options);
    st = scanner_context->init();
    ASSERT_TRUE(st.ok());

    ASSERT_EQ(scanner_context->_max_scan_concurrency, 1);
}

TEST_F(ScannerContextTest, test_max_column_reader_num) {
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
    query_options.__set_max_scanners_concurrency(20);
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
    auto scan_operator = std::make_unique<OlapScanOperatorX>(obj_pool.get(), tnode, 0, *descs,
                                                             parallel_tasks, TQueryCacheParam {});

    auto olap_scan_local_state =
            OlapScanLocalState::create_unique(state.get(), scan_operator.get());

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
    auto scan_operator = std::make_unique<OlapScanOperatorX>(obj_pool.get(), tnode, 0, *descs,
                                                             parallel_tasks, TQueryCacheParam {});

    auto olap_scan_local_state =
            OlapScanLocalState::create_unique(state.get(), scan_operator.get());

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

    // Downstream is waiting for scan data while scheduler is busy. The scan operator should
    // refill up to max scan concurrency instead of being limited by min scan concurrency.
    scheduler = std::make_unique<MockSimplifiedScanScheduler>(cgroup_cpu_ctl);
    EXPECT_CALL(*scheduler, get_active_threads()).WillOnce(testing::Return(50));
    EXPECT_CALL(*scheduler, get_queue_size()).WillOnce(testing::Return(10));
    scanner_context->_scanner_scheduler = scheduler.get();
    scanner_context->_min_scan_concurrency_of_scan_scheduler = 20;
    scanner_context->_num_scheduled_scanners = 0;
    scanner_context->_min_scan_concurrency = 1;
    scanner_context->_max_scan_concurrency = 8;
    scanner_context->_scan_starving = true;
    margin = scanner_context->_get_margin(transfer_lock, scheduler_lock);
    ASSERT_EQ(margin, scanner_context->_max_scan_concurrency);

    // If there is already a produced task waiting in the queue, downstream is not scan-starved.
    scheduler = std::make_unique<MockSimplifiedScanScheduler>(cgroup_cpu_ctl);
    EXPECT_CALL(*scheduler, get_active_threads()).WillOnce(testing::Return(50));
    EXPECT_CALL(*scheduler, get_queue_size()).WillOnce(testing::Return(10));
    scanner_context->_scanner_scheduler = scheduler.get();
    scanner_context->_tasks_queue.push_back(
            std::make_shared<ScanTask>(std::make_shared<ScannerDelegate>(scanner)));
    margin = scanner_context->_get_margin(transfer_lock, scheduler_lock);
    ASSERT_EQ(margin, 0);
    scanner_context->_tasks_queue.clear();
}

TEST_F(ScannerContextTest, pull_next_scan_task) {
    const int parallel_tasks = 4;
    auto scan_operator = std::make_unique<OlapScanOperatorX>(obj_pool.get(), tnode, 0, *descs,
                                                             parallel_tasks, TQueryCacheParam {});

    auto olap_scan_local_state =
            OlapScanLocalState::create_unique(state.get(), scan_operator.get());

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

    scanner_context->_pending_scanners = std::stack<std::shared_ptr<ScanTask>>();
    pull_scan_task = scanner_context->_pull_next_scan_task(
            nullptr, scanner_context->_max_scan_concurrency - 1);
    EXPECT_EQ(pull_scan_task, nullptr);

    scanner_context->_pending_scanners.push(
            std::make_shared<ScanTask>(std::make_shared<ScannerDelegate>(scanner)));
    pull_scan_task = scanner_context->_pull_next_scan_task(
            nullptr, scanner_context->_max_scan_concurrency - 1);
    EXPECT_NE(pull_scan_task, nullptr);
}

TEST_F(ScannerContextTest, schedule_scan_task) {
    const int parallel_tasks = 4;
    auto scan_operator = std::make_unique<OlapScanOperatorX>(obj_pool.get(), tnode, 0, *descs,
                                                             parallel_tasks, TQueryCacheParam {});

    auto olap_scan_local_state =
            OlapScanLocalState::create_unique(state.get(), scan_operator.get());

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
    auto scan_operator = std::make_unique<OlapScanOperatorX>(obj_pool.get(), tnode, 0, *descs,
                                                             parallel_tasks, TQueryCacheParam {});

    auto olap_scan_local_state =
            OlapScanLocalState::create_unique(state.get(), scan_operator.get());
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
    auto scan_operator = std::make_unique<OlapScanOperatorX>(obj_pool.get(), tnode, 0, *descs,
                                                             parallel_tasks, TQueryCacheParam {});

    auto olap_scan_local_state =
            OlapScanLocalState::create_unique(state.get(), scan_operator.get());

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
    auto scan_operator = std::make_unique<OlapScanOperatorX>(obj_pool.get(), tnode, 0, *descs,
                                                             parallel_tasks, TQueryCacheParam {});

    auto olap_scan_local_state =
            OlapScanLocalState::create_unique(state.get(), scan_operator.get());

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
    auto scan_operator = std::make_unique<OlapScanOperatorX>(obj_pool.get(), tnode, 0, *descs,
                                                             parallel_tasks, TQueryCacheParam {});

    auto olap_scan_local_state =
            OlapScanLocalState::create_unique(state.get(), scan_operator.get());

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

TEST_F(ScannerContextTest, thread_pool_admission_state) {
    const int parallel_tasks = 1;
    auto scan_operator = std::make_unique<OlapScanOperatorX>(obj_pool.get(), tnode, 0, *descs,
                                                             parallel_tasks, TQueryCacheParam {});
    auto olap_scan_local_state =
            OlapScanLocalState::create_unique(state.get(), scan_operator.get());

    OlapScanner::Params scanner_params;
    scanner_params.state = state.get();
    scanner_params.profile = profile.get();
    scanner_params.limit = -1;
    scanner_params.key_ranges = std::vector<OlapScanRange*>();
    std::shared_ptr<Scanner> scanner =
            OlapScanner::create_shared(olap_scan_local_state.get(), std::move(scanner_params));
    std::list<std::shared_ptr<ScannerDelegate>> scanners {
            std::make_shared<ScannerDelegate>(scanner)};
    auto scanner_context = ScannerContext::create_shared(
            state.get(), olap_scan_local_state.get(), output_tuple_desc, output_row_descriptor,
            scanners, -1, scan_dependency, parallel_tasks);

    // An idle pool: admission is bounded only by the per-Context limit.
    std::unique_ptr<MockSimplifiedScanScheduler> scheduler =
            std::make_unique<MockSimplifiedScanScheduler>(cgroup_cpu_ctl);
    EXPECT_CALL(*scheduler, get_active_threads()).WillRepeatedly(testing::Return(0));
    EXPECT_CALL(*scheduler, get_queue_size()).WillRepeatedly(testing::Return(0));
    scanner_context->_scanner_scheduler = scheduler.get();
    scanner_context->_min_scan_concurrency_of_scan_scheduler = 20;

    std::unique_lock<std::mutex> context_transfer_lock(scanner_context->transfer_lock());
    scanner_context->_pending_scanners = std::stack<std::shared_ptr<ScanTask>>();
    scanner_context->_tasks_queue.clear();
    scanner_context->_num_scheduled_scanners = 0;
    // Even if the effective limit is temporarily zero, one pending task must run so it can publish
    // a block or EOS and prevent the Context from stalling.
    scanner_context->_max_scan_concurrency = 0;

    EXPECT_FALSE(scanner_context->can_admit_scan_task(context_transfer_lock, false));

    // A consumed non-EOS scanner must be eligible for another Context admission.
    auto consumed_task = std::make_shared<ScanTask>(scanners.front());
    scanner_context->push_pending_scan_task(consumed_task, context_transfer_lock);
    EXPECT_TRUE(scanner_context->can_admit_scan_task(context_transfer_lock, false));

    EXPECT_FALSE(scanner_context->is_context_queued(context_transfer_lock));
    scanner_context->set_context_queued(true, context_transfer_lock);
    EXPECT_TRUE(scanner_context->is_context_queued(context_transfer_lock));
    scanner_context->set_context_queued(false, context_transfer_lock);

    // The Context admits exactly one scanner and counts it as scheduled. Its runnable was queued
    // (at 1000) before the scanner was returned to pending (at 2500), as with LIFO re-admission
    // of a scanner whose previous attempt ran while the runnable waited. Only the part of the
    // runnable's wait during which the scanner was pending is its wait-worker time.
    EXPECT_GT(consumed_task->pending_since_ns, 0);
    consumed_task->pending_since_ns = 2500;
    int64_t wait_worker_time = scanner->get_scanner_wait_worker_timer();
    auto admitted_task = scanner_context->try_get_next_scan_task(context_transfer_lock, 1000, 3000);
    EXPECT_EQ(admitted_task, consumed_task);
    EXPECT_EQ(scanner->get_scanner_wait_worker_timer(), wait_worker_time + 500);
    EXPECT_EQ(scanner_context->_num_scheduled_scanners, 1);
    EXPECT_TRUE(scanner_context->_pending_scanners.empty());

    // A scanner already pending when the runnable was queued waited for the whole queue time.
    scanner_context->_num_scheduled_scanners = 0;
    auto early_task = std::make_shared<ScanTask>(scanners.front());
    scanner_context->push_pending_scan_task(early_task, context_transfer_lock);
    early_task->pending_since_ns = 500;
    wait_worker_time = scanner->get_scanner_wait_worker_timer();
    EXPECT_EQ(scanner_context->try_get_next_scan_task(context_transfer_lock, 1000, 3000),
              early_task);
    EXPECT_EQ(scanner->get_scanner_wait_worker_timer(), wait_worker_time + 2000);
    EXPECT_EQ(scanner_context->_num_scheduled_scanners, 1);

    auto blocked_task = std::make_shared<ScanTask>(std::make_shared<ScannerDelegate>(scanner));
    scanner_context->push_pending_scan_task(blocked_task, context_transfer_lock);
    EXPECT_FALSE(scanner_context->can_admit_scan_task(context_transfer_lock, false));
    EXPECT_EQ(scanner_context->try_get_next_scan_task(context_transfer_lock, 0, 0), nullptr);

    // A stopped Context admits nothing even when nothing is progressing.
    scanner_context->_num_scheduled_scanners = 0;
    EXPECT_TRUE(scanner_context->can_admit_scan_task(context_transfer_lock, false));
    scanner_context->_should_stop = true;
    EXPECT_FALSE(scanner_context->can_admit_scan_task(context_transfer_lock, false));
}

TEST_F(ScannerContextTest, thread_pool_admission_reads_queue_before_active_threads) {
    const int parallel_tasks = 2;
    auto scan_operator = std::make_unique<OlapScanOperatorX>(obj_pool.get(), tnode, 0, *descs,
                                                             parallel_tasks, TQueryCacheParam {});
    auto olap_scan_local_state =
            OlapScanLocalState::create_unique(state.get(), scan_operator.get());

    OlapScanner::Params scanner_params;
    scanner_params.state = state.get();
    scanner_params.profile = profile.get();
    scanner_params.limit = -1;
    scanner_params.key_ranges = std::vector<OlapScanRange*>();
    std::shared_ptr<Scanner> scanner =
            OlapScanner::create_shared(olap_scan_local_state.get(), std::move(scanner_params));
    std::list<std::shared_ptr<ScannerDelegate>> scanners;
    for (int i = 0; i < 2; ++i) {
        scanners.push_back(std::make_shared<ScannerDelegate>(scanner));
    }
    auto scanner_context = ScannerContext::create_shared(
            state.get(), olap_scan_local_state.get(), output_tuple_desc, output_row_descriptor,
            scanners, -1, scan_dependency, parallel_tasks);

    // A worker moves a task from queued to active under one pool lock, while admission reads the
    // two counters under separate locks. Reading the queue first means a dequeue in between is
    // counted twice (defer) rather than missed (overbook the last slot and fail the submission).
    std::unique_ptr<MockSimplifiedScanScheduler> scheduler =
            std::make_unique<MockSimplifiedScanScheduler>(cgroup_cpu_ctl);
    {
        testing::InSequence sequence;
        EXPECT_CALL(*scheduler, get_queue_size()).WillOnce(testing::Return(1));
        EXPECT_CALL(*scheduler, get_active_threads()).WillOnce(testing::Return(3));
    }
    scanner_context->_scanner_scheduler = scheduler.get();
    scanner_context->_min_scan_concurrency_of_scan_scheduler = 4;
    scanner_context->_max_scan_concurrency = parallel_tasks;
    scanner_context->_min_scan_concurrency = 1;
    scanner_context->_scan_starving = false;
    scanner_context->_num_scheduled_scanners = 1;

    std::unique_lock<std::mutex> transfer_lock(scanner_context->transfer_lock());
    ASSERT_FALSE(scanner_context->_pending_scanners.empty());
    // The over-counted pool looks full, so the Context stays at its minimum concurrency.
    EXPECT_FALSE(scanner_context->can_admit_scan_task(transfer_lock, false));
}

TEST_F(ScannerContextTest, thread_pool_admission_follows_margin_limits) {
    const int parallel_tasks = 4;
    auto scan_operator = std::make_unique<OlapScanOperatorX>(obj_pool.get(), tnode, 0, *descs,
                                                             parallel_tasks, TQueryCacheParam {});
    auto olap_scan_local_state =
            OlapScanLocalState::create_unique(state.get(), scan_operator.get());

    OlapScanner::Params scanner_params;
    scanner_params.state = state.get();
    scanner_params.profile = profile.get();
    scanner_params.limit = -1;
    scanner_params.key_ranges = std::vector<OlapScanRange*>();
    std::shared_ptr<Scanner> scanner =
            OlapScanner::create_shared(olap_scan_local_state.get(), std::move(scanner_params));

    // A single-worker pool whose worker is parked: active + queued == 1, i.e. the pool has no
    // slack once the scheduler-wide budget is 1.
    ThreadPoolSimplifiedScanScheduler scheduler("saturated_pool_test", cgroup_cpu_ctl);
    ASSERT_TRUE(scheduler.start(1, 1, 4, 1).ok());
    CountDownLatch task_started(1);
    CountDownLatch release_task(1);
    Defer cleanup = [&] {
        release_task.count_down();
        scheduler.stop();
    };
    ASSERT_TRUE(scheduler
                        .submit_scan_task(SimplifiedScanTask(
                                [&] {
                                    task_started.count_down();
                                    release_task.wait();
                                    return true;
                                },
                                nullptr, nullptr))
                        .ok());
    ASSERT_TRUE(task_started.wait_for(std::chrono::seconds(5)));
    ASSERT_EQ(scheduler.get_active_threads(), 1);
    ASSERT_EQ(scheduler.get_queue_size(), 0);

    std::list<std::shared_ptr<ScannerDelegate>> scanners;
    for (int i = 0; i < 4; ++i) {
        scanners.push_back(std::make_shared<ScannerDelegate>(scanner));
    }
    auto scanner_context = ScannerContext::create_shared(
            state.get(), olap_scan_local_state.get(), output_tuple_desc, output_row_descriptor,
            scanners, -1, scan_dependency, parallel_tasks);
    scanner_context->_scanner_scheduler = &scheduler;
    scanner_context->_min_scan_concurrency = 1;

    std::unique_lock<std::mutex> transfer_lock(scanner_context->transfer_lock());
    ASSERT_EQ(scanner_context->_max_scan_concurrency, parallel_tasks);

    // Saturated and not starving: the minimum concurrency is the ceiling, as _get_margin()
    // enforces on the TaskExecutor path.
    scanner_context->_min_scan_concurrency_of_scan_scheduler = 1;
    scanner_context->_scan_starving = false;
    scanner_context->_num_scheduled_scanners = 0;
    EXPECT_TRUE(scanner_context->can_admit_scan_task(transfer_lock, false));
    scanner_context->_num_scheduled_scanners = 1;
    EXPECT_FALSE(scanner_context->can_admit_scan_task(transfer_lock, false));
    // A pool worker admitting the task it runs itself does not count its own thread, so the
    // parked worker alone leaves one slot of slack, just as it does for the operator thread when
    // the budget is two.
    EXPECT_TRUE(scanner_context->can_admit_scan_task(transfer_lock, true));
    scanner_context->_min_scan_concurrency_of_scan_scheduler = 2;
    EXPECT_TRUE(scanner_context->can_admit_scan_task(transfer_lock, false));
    scanner_context->_min_scan_concurrency_of_scan_scheduler = 1;

    // A larger minimum raises the saturated ceiling accordingly.
    scanner_context->_min_scan_concurrency = 2;
    EXPECT_TRUE(scanner_context->can_admit_scan_task(transfer_lock, false));
    scanner_context->_num_scheduled_scanners = 2;
    EXPECT_FALSE(scanner_context->can_admit_scan_task(transfer_lock, false));

    // A starving operator with an empty result queue lets the Context ramp to its maximum.
    scanner_context->_scan_starving = true;
    EXPECT_TRUE(scanner_context->can_admit_scan_task(transfer_lock, false));
    scanner_context->_num_scheduled_scanners = parallel_tasks;
    EXPECT_FALSE(scanner_context->can_admit_scan_task(transfer_lock, false));

    // Cached results end starvation for the margin, and they occupy a concurrency slot.
    scanner_context->_num_scheduled_scanners = 1;
    scanner_context->_tasks_queue.push_back(
            std::make_shared<ScanTask>(std::make_shared<ScannerDelegate>(scanner)));
    EXPECT_FALSE(scanner_context->can_admit_scan_task(transfer_lock, false));
    scanner_context->_tasks_queue.clear();

    // With slack the Context may ramp to its maximum again, but never beyond it.
    scanner_context->_scan_starving = false;
    scanner_context->_min_scan_concurrency_of_scan_scheduler = 20;
    scanner_context->_num_scheduled_scanners = parallel_tasks - 1;
    EXPECT_TRUE(scanner_context->can_admit_scan_task(transfer_lock, false));
    scanner_context->_num_scheduled_scanners = parallel_tasks;
    EXPECT_FALSE(scanner_context->can_admit_scan_task(transfer_lock, false));
}

TEST_F(ScannerContextTest, debug_string_reports_context_queue_state) {
    const int parallel_tasks = 3;
    auto scan_operator = std::make_unique<OlapScanOperatorX>(obj_pool.get(), tnode, 0, *descs,
                                                             parallel_tasks, TQueryCacheParam {});
    auto olap_scan_local_state =
            OlapScanLocalState::create_unique(state.get(), scan_operator.get());

    OlapScanner::Params scanner_params;
    scanner_params.state = state.get();
    scanner_params.profile = profile.get();
    scanner_params.limit = -1;
    scanner_params.key_ranges = std::vector<OlapScanRange*>();
    std::shared_ptr<Scanner> scanner =
            OlapScanner::create_shared(olap_scan_local_state.get(), std::move(scanner_params));
    std::list<std::shared_ptr<ScannerDelegate>> scanners {
            std::make_shared<ScannerDelegate>(scanner)};
    auto scanner_context = ScannerContext::create_shared(
            state.get(), olap_scan_local_state.get(), output_tuple_desc, output_row_descriptor,
            scanners, 7, scan_dependency, parallel_tasks);

    // Every value is distinct so a misplaced placeholder is visible in the output.
    scanner_context->_num_scheduled_scanners = 2;
    scanner_context->_is_context_queued = true;
    scanner_context->_num_finished_scanners = 5;

    const std::string debug = scanner_context->debug_string();
    EXPECT_NE(debug.find("limit: 7, _num_running_scanners: 2, _is_context_queued: true, "
                         "_num_finished_scanners: 5, _max_thread_num: 3,"),
              std::string::npos)
            << debug;
}

TEST_F(ScannerContextTest, thread_pool_submit_failure_policy) {
    const int parallel_tasks = 2;
    auto scan_operator = std::make_unique<OlapScanOperatorX>(obj_pool.get(), tnode, 0, *descs,
                                                             parallel_tasks, TQueryCacheParam {});
    auto olap_scan_local_state =
            OlapScanLocalState::create_unique(state.get(), scan_operator.get());

    OlapScanner::Params scanner_params;
    scanner_params.state = state.get();
    scanner_params.profile = profile.get();
    scanner_params.limit = -1;
    scanner_params.key_ranges = std::vector<OlapScanRange*>();
    std::shared_ptr<Scanner> scanner =
            OlapScanner::create_shared(olap_scan_local_state.get(), std::move(scanner_params));

    std::list<std::shared_ptr<ScannerDelegate>> scanners;
    for (int i = 0; i < 2; ++i) {
        scanners.push_back(std::make_shared<ScannerDelegate>(scanner));
    }
    auto scanner_context = ScannerContext::create_shared(
            state.get(), olap_scan_local_state.get(), output_tuple_desc, output_row_descriptor,
            scanners, -1, scan_dependency, parallel_tasks);

    // One worker, zero queue capacity, worker occupied: every submit_func() is rejected.
    ThreadPoolSimplifiedScanScheduler scheduler("submit_failure_policy_test", cgroup_cpu_ctl);
    ASSERT_TRUE(scheduler.start(1, 1, 0, 1).ok());
    CountDownLatch task_started(1);
    CountDownLatch release_task(1);
    Defer cleanup = [&] {
        release_task.count_down();
        scheduler.stop();
    };
    ASSERT_TRUE(scheduler
                        .submit_scan_task(SimplifiedScanTask(
                                [&] {
                                    task_started.count_down();
                                    release_task.wait();
                                    return true;
                                },
                                nullptr, nullptr))
                        .ok());
    ASSERT_TRUE(task_started.wait_for(std::chrono::seconds(5)));
    scanner_context->_scanner_scheduler = &scheduler;

    std::unique_lock<std::mutex> transfer_lock(scanner_context->transfer_lock());
    ASSERT_FALSE(scanner_context->_pending_scanners.empty());

    // Context submission is fail-fast. Retrying here would couple the scanner scheduler to
    // ThreadPool's internal rejection/retention behavior.
    Status surfaced = scheduler.schedule_scan_task(scanner_context, nullptr, transfer_lock);
    EXPECT_TRUE(surfaced.is<ErrorCode::TOO_MANY_TASKS>()) << surfaced.to_string();
    EXPECT_TRUE(scanner_context->done());
    EXPECT_FALSE(scanner_context->_process_status.ok());
    EXPECT_TRUE(scan_dependency->ready());
    // The marker is set before submit_func(). This rejected runnable was not retained, but the
    // terminal Context no longer needs the marker cleared or another submission attempted.
    EXPECT_TRUE(scanner_context->is_context_queued(transfer_lock));
}

TEST_F(ScannerContextTest, thread_pool_budget_check_and_submit_are_atomic_across_contexts) {
    // Three workers and no queue capacity. Two workers are parked, as if each ran a scanner of
    // another Context, so the pool can accept exactly one more runnable.
    ThreadPoolSimplifiedScanScheduler scheduler("budget_race_test", cgroup_cpu_ctl);
    ASSERT_TRUE(scheduler.start(3, 3, 0, 3).ok());
    CountDownLatch tasks_started(2);
    CountDownLatch release_tasks(1);
    Defer cleanup = [&] {
        release_tasks.count_down();
        scheduler.stop();
    };
    for (int i = 0; i < 2; ++i) {
        ASSERT_TRUE(scheduler
                            .submit_scan_task(SimplifiedScanTask(
                                    [&] {
                                        tasks_started.count_down();
                                        release_tasks.wait();
                                        return true;
                                    },
                                    nullptr, nullptr))
                            .ok());
    }
    ASSERT_TRUE(tasks_started.wait_for(std::chrono::seconds(5)));
    ASSERT_EQ(scheduler.get_active_threads(), 2);

    const int parallel_tasks = 4;
    auto scan_operator = std::make_unique<OlapScanOperatorX>(obj_pool.get(), tnode, 0, *descs,
                                                             parallel_tasks, TQueryCacheParam {});
    auto olap_scan_local_state =
            OlapScanLocalState::create_unique(state.get(), scan_operator.get());
    OlapScanner::Params scanner_params;
    scanner_params.state = state.get();
    scanner_params.profile = profile.get();
    scanner_params.limit = -1;
    scanner_params.key_ranges = std::vector<OlapScanRange*>();
    std::shared_ptr<Scanner> scanner =
            OlapScanner::create_shared(olap_scan_local_state.get(), std::move(scanner_params));

    // Each Context has one scanner running and another pending, and has reached its minimum
    // concurrency. It may only ramp up while the pool has slack.
    std::vector<std::shared_ptr<ScannerContext>> contexts;
    for (int i = 0; i < 2; ++i) {
        std::list<std::shared_ptr<ScannerDelegate>> scanners;
        for (int j = 0; j < 2; ++j) {
            scanners.push_back(std::make_shared<ScannerDelegate>(scanner));
        }
        auto scanner_context = ScannerContext::create_shared(
                state.get(), olap_scan_local_state.get(), output_tuple_desc, output_row_descriptor,
                scanners, -1, scan_dependency, parallel_tasks);
        scanner_context->_scanner_scheduler = &scheduler;
        scanner_context->_min_scan_concurrency_of_scan_scheduler = 3;
        scanner_context->_max_scan_concurrency = parallel_tasks;
        scanner_context->_min_scan_concurrency = 1;
        scanner_context->_scan_starving = false;
        scanner_context->_num_scheduled_scanners = 1;
        ASSERT_FALSE(scanner_context->_pending_scanners.empty());
        contexts.push_back(std::move(scanner_context));
    }

    // Park the first Context between its budget check and its submission until the second one
    // reaches the same point, or for a bounded time if the scheduler serializes them. Without a
    // scheduler-wide lock both pass the check on the last free slot and one submission fails.
    std::atomic<int> entered {0};
    CountDownLatch second_entered(1);
    const bool old_enable_debug_points = config::enable_debug_points;
    config::enable_debug_points = true;
    DebugPoints::instance()->add_with_callback(
            "ThreadPoolSimplifiedScanScheduler.schedule_scan_task.before_submit",
            std::function<void()>([&] {
                if (entered.fetch_add(1) == 0) {
                    static_cast<void>(second_entered.wait_for(std::chrono::milliseconds(500)));
                } else {
                    second_entered.count_down();
                }
            }));
    Defer cleanup_debug_point = [&] {
        DebugPoints::instance()->remove(
                "ThreadPoolSimplifiedScanScheduler.schedule_scan_task.before_submit");
        config::enable_debug_points = old_enable_debug_points;
    };

    Status statuses[2];
    std::vector<std::thread> threads;
    for (int i = 0; i < 2; ++i) {
        threads.emplace_back([&, i] {
            std::unique_lock<std::mutex> transfer_lock(contexts[i]->transfer_lock());
            statuses[i] = scheduler.schedule_scan_task(contexts[i], nullptr, transfer_lock);
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    // The Context that lost the last slot defers instead of failing its query.
    for (int i = 0; i < 2; ++i) {
        EXPECT_TRUE(statuses[i].ok()) << statuses[i].to_string();
        EXPECT_FALSE(contexts[i]->done());
        std::unique_lock<std::mutex> transfer_lock(contexts[i]->transfer_lock());
        EXPECT_TRUE(contexts[i]->_process_status.ok());
    }
}

TEST_F(ScannerContextTest, run_context_publishes_admission_failure) {
    const bool old_enable_debug_points = config::enable_debug_points;
    config::enable_debug_points = true;
    DebugPoints::instance()->add("ThreadPoolSimplifiedScanScheduler._run_context.inject_failure");
    Defer cleanup_debug_point = [&] {
        DebugPoints::instance()->remove(
                "ThreadPoolSimplifiedScanScheduler._run_context.inject_failure");
        config::enable_debug_points = old_enable_debug_points;
    };

    const int parallel_tasks = 2;
    auto scan_operator = std::make_unique<OlapScanOperatorX>(obj_pool.get(), tnode, 0, *descs,
                                                             parallel_tasks, TQueryCacheParam {});
    auto olap_scan_local_state =
            OlapScanLocalState::create_unique(state.get(), scan_operator.get());

    OlapScanner::Params scanner_params;
    scanner_params.state = state.get();
    scanner_params.profile = profile.get();
    scanner_params.limit = -1;
    scanner_params.key_ranges = std::vector<OlapScanRange*>();
    std::shared_ptr<Scanner> scanner =
            OlapScanner::create_shared(olap_scan_local_state.get(), std::move(scanner_params));

    std::list<std::shared_ptr<ScannerDelegate>> scanners;
    for (int i = 0; i < 2; ++i) {
        scanners.push_back(std::make_shared<ScannerDelegate>(scanner));
    }
    // The worker's task_exec_ctx() must resolve, otherwise _run_context() exits before admission.
    // HasTaskExecutionCtx snapshots the weak_ptr at construction, so set it before create_shared.
    auto task_execution_context = std::make_shared<TaskExecutionContext>();
    state->set_task_execution_context(task_execution_context);
    auto scanner_context = ScannerContext::create_shared(
            state.get(), olap_scan_local_state.get(), output_tuple_desc, output_row_descriptor,
            scanners, -1, scan_dependency, parallel_tasks);

    ThreadPoolSimplifiedScanScheduler scheduler("run_context_failure_test", cgroup_cpu_ctl);
    ASSERT_TRUE(scheduler.start(1, 1, 1, 1).ok());
    Defer cleanup = [&] { scheduler.stop(); };
    scanner_context->_scanner_scheduler = &scheduler;

    {
        std::unique_lock<std::mutex> transfer_lock(scanner_context->transfer_lock());
        ASSERT_TRUE(scheduler.schedule_scan_task(scanner_context, nullptr, transfer_lock).ok());
        ASSERT_TRUE(scanner_context->is_context_queued(transfer_lock));
    }

    // The worker admits a scanner and hits the injected exception. It must publish the failure
    // as a completed task instead of terminating the process or leaking the scheduled slot.
    bool published = false;
    for (int i = 0; i < 10000; ++i) {
        std::unique_lock<std::mutex> transfer_lock(scanner_context->transfer_lock());
        if (!scanner_context->_tasks_queue.empty()) {
            published = true;
            break;
        }
        transfer_lock.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(published);

    std::unique_lock<std::mutex> transfer_lock(scanner_context->transfer_lock());
    ASSERT_EQ(scanner_context->_tasks_queue.size(), 1);
    EXPECT_FALSE(scanner_context->_tasks_queue.front()->status_ok());
    EXPECT_FALSE(scanner_context->_process_status.ok());
    EXPECT_EQ(scanner_context->_num_scheduled_scanners, 0);
    EXPECT_FALSE(scanner_context->is_context_queued(transfer_lock));
}

TEST_F(ScannerContextTest, thread_pool_context_chain_runs_all_scanners) {
    const int parallel_tasks = 2;
    const int scanner_count = 3;
    const int blocks_per_scanner = 4;
    // Return after every block so each scanner needs several admissions before it reaches EOS.
    const int32_t old_doris_scanner_row_bytes = config::doris_scanner_row_bytes;
    config::doris_scanner_row_bytes = 1;
    Defer restore_config = [&] { config::doris_scanner_row_bytes = old_doris_scanner_row_bytes; };

    auto scan_operator = std::make_unique<OlapScanOperatorX>(obj_pool.get(), tnode, 0, *descs,
                                                             parallel_tasks, TQueryCacheParam {});
    auto olap_scan_local_state =
            OlapScanLocalState::create_unique(state.get(), scan_operator.get());
    olap_scan_local_state->_parent = scan_operator.get();
    olap_scan_local_state->_max_scan_concurrency = max_concurrency_counter.get();
    olap_scan_local_state->_min_scan_concurrency = min_concurrency_counter.get();
    RuntimeProfile::HighWaterMarkCounter peak_running_scanner(TUnit::UNIT, 0, "");
    olap_scan_local_state->_peak_running_scanner = &peak_running_scanner;
    scan_operator->_should_run_serial = false;
    TQueryOptions query_options;
    query_options.__set_max_column_reader_num(0);
    state->set_query_options(query_options);

    std::atomic<int> running {0};
    std::atomic<int> peak_running {0};
    CountDownLatch overlap(1);
    std::list<std::shared_ptr<ScannerDelegate>> scanners;
    for (int i = 0; i < scanner_count; ++i) {
        std::shared_ptr<Scanner> scanner = std::make_shared<ChainMockScanner>(
                state.get(), olap_scan_local_state.get(), profile.get(), blocks_per_scanner,
                &running, &peak_running, &overlap);
        scanners.push_back(std::make_shared<ScannerDelegate>(scanner));
    }

    // The worker's task_exec_ctx() must resolve, otherwise _run_context() exits before admission.
    auto task_execution_context = std::make_shared<TaskExecutionContext>();
    state->set_task_execution_context(task_execution_context);
    auto scanner_context = ScannerContext::create_shared(
            state.get(), olap_scan_local_state.get(), output_tuple_desc, output_row_descriptor,
            scanners, -1, scan_dependency, parallel_tasks);
    scanner_context->_newly_create_free_blocks_num = newly_create_free_blocks_num.get();
    scanner_context->_scanner_memory_used_counter = scanner_memory_used_counter.get();

    // Two workers so the successor runnable can overlap with the executing scanner.
    ThreadPoolSimplifiedScanScheduler scheduler("context_chain_test", cgroup_cpu_ctl);
    ASSERT_TRUE(scheduler.start(2, 2, 16, 1).ok());
    Defer cleanup = [&] { scheduler.stop(); };
    scanner_context->_scanner_scheduler = &scheduler;
    // The two-thread pool never reaches this budget, so the Context may ramp to its maximum.
    scanner_context->_min_scan_concurrency_of_scan_scheduler = 20;
    ASSERT_EQ(scanner_context->_max_scan_concurrency, parallel_tasks);

    // init() performs the bootstrap submission of the first Context runnable.
    ASSERT_TRUE(scanner_context->init().ok());

    int64_t rows = 0;
    bool eos = false;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
    while (!eos) {
        ASSERT_LT(std::chrono::steady_clock::now(), deadline) << scanner_context->debug_string();
        // One Context submission represents all pending scanners, so the pool never holds more
        // than one runnable for this Context.
        EXPECT_LE(scheduler.get_queue_size(), 1);
        Block block;
        Status st = scanner_context->get_block_from_queue(state.get(), &block, &eos, 0);
        ASSERT_TRUE(st.ok()) << st.to_string();
        rows += block.rows();
        if (!eos && block.rows() == 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    // Every consumed non-EOS scanner was re-admitted until it reported EOS.
    EXPECT_EQ(rows, scanner_count * blocks_per_scanner);
    std::unique_lock<std::mutex> transfer_lock(scanner_context->transfer_lock());
    EXPECT_EQ(scanner_context->_num_finished_scanners, scanner_count);
    EXPECT_EQ(scanner_context->_num_scheduled_scanners, 0);
    EXPECT_TRUE(scanner_context->_pending_scanners.empty());
    EXPECT_FALSE(scanner_context->is_context_queued(transfer_lock));
    EXPECT_TRUE(scanner_context->_process_status.ok());
    // The successor runnable ramped concurrency to the per-Context limit, and never beyond it.
    EXPECT_EQ(peak_running.load(), parallel_tasks);
}

TEST_F(ScannerContextTest, successor_submission_is_not_scanner_wait_time) {
    const int parallel_tasks = 2;
    const int scanner_count = 2;
    auto scan_operator = std::make_unique<OlapScanOperatorX>(obj_pool.get(), tnode, 0, *descs,
                                                             parallel_tasks, TQueryCacheParam {});
    auto olap_scan_local_state =
            OlapScanLocalState::create_unique(state.get(), scan_operator.get());
    olap_scan_local_state->_parent = scan_operator.get();
    olap_scan_local_state->_max_scan_concurrency = max_concurrency_counter.get();
    olap_scan_local_state->_min_scan_concurrency = min_concurrency_counter.get();
    RuntimeProfile::HighWaterMarkCounter peak_running_scanner(TUnit::UNIT, 0, "");
    olap_scan_local_state->_peak_running_scanner = &peak_running_scanner;
    scan_operator->_should_run_serial = false;
    TQueryOptions query_options;
    query_options.__set_max_column_reader_num(0);
    state->set_query_options(query_options);

    // The scanners never wait for each other: the latch is already open.
    std::atomic<int> running {0};
    std::atomic<int> peak_running {0};
    CountDownLatch overlap(0);
    std::vector<std::shared_ptr<Scanner>> scanner_ptrs;
    std::list<std::shared_ptr<ScannerDelegate>> scanners;
    for (int i = 0; i < scanner_count; ++i) {
        std::shared_ptr<Scanner> scanner = std::make_shared<ChainMockScanner>(
                state.get(), olap_scan_local_state.get(), profile.get(), 1, &running, &peak_running,
                &overlap);
        scanner_ptrs.push_back(scanner);
        scanners.push_back(std::make_shared<ScannerDelegate>(scanner));
    }

    // The worker's task_exec_ctx() must resolve, otherwise _run_context() exits before admission.
    auto task_execution_context = std::make_shared<TaskExecutionContext>();
    state->set_task_execution_context(task_execution_context);
    auto scanner_context = ScannerContext::create_shared(
            state.get(), olap_scan_local_state.get(), output_tuple_desc, output_row_descriptor,
            scanners, -1, scan_dependency, parallel_tasks);
    scanner_context->_newly_create_free_blocks_num = newly_create_free_blocks_num.get();
    scanner_context->_scanner_memory_used_counter = scanner_memory_used_counter.get();

    ThreadPoolSimplifiedScanScheduler scheduler("successor_wait_time_test", cgroup_cpu_ctl);
    ASSERT_TRUE(scheduler.start(1, 1, 16, 1).ok());
    Defer cleanup = [&] { scheduler.stop(); };
    scanner_context->_scanner_scheduler = &scheduler;
    // The pool never reaches this budget, so the runnable submits a successor for the second
    // pending scanner after admitting the first one.
    scanner_context->_min_scan_concurrency_of_scan_scheduler = 20;

    // Make the successor submission from the pool worker slow, as when ThreadPool synchronously
    // creates a thread. The bootstrap submission from this thread is not delayed.
    const int64_t submit_delay_ms = 1000;
    const auto test_thread_id = std::this_thread::get_id();
    const bool old_enable_debug_points = config::enable_debug_points;
    config::enable_debug_points = true;
    DebugPoints::instance()->add_with_callback(
            "ThreadPoolSimplifiedScanScheduler.schedule_scan_task.before_submit",
            std::function<void()>([&] {
                if (std::this_thread::get_id() != test_thread_id) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(submit_delay_ms));
                }
            }));
    Defer cleanup_debug_point = [&] {
        DebugPoints::instance()->remove(
                "ThreadPoolSimplifiedScanScheduler.schedule_scan_task.before_submit");
        config::enable_debug_points = old_enable_debug_points;
    };

    // init() performs the bootstrap submission of the first Context runnable.
    ASSERT_TRUE(scanner_context->init().ok());

    bool published = false;
    for (int i = 0; i < 20000; ++i) {
        std::unique_lock<std::mutex> transfer_lock(scanner_context->transfer_lock());
        if (scanner_context->_tasks_queue.size() == scanner_count) {
            published = true;
            break;
        }
        transfer_lock.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(published) << scanner_context->debug_string();

    // Both scanners ran on a worker right after admission. Neither may be charged the delayed
    // successor submission as time spent waiting for a worker; charging it would add the whole
    // delay on top of the runnable's own queue wait, which only covers the worker wake-up.
    for (const auto& scanner : scanner_ptrs) {
        EXPECT_LT(scanner->get_scanner_wait_worker_timer(), submit_delay_ms * 1000 * 1000);
    }
}

TEST_F(ScannerContextTest, thread_pool_context_runnable_is_deduplicated) {
    const int parallel_tasks = 2;
    auto scan_operator = std::make_unique<OlapScanOperatorX>(obj_pool.get(), tnode, 0, *descs,
                                                             parallel_tasks, TQueryCacheParam {});
    auto olap_scan_local_state =
            OlapScanLocalState::create_unique(state.get(), scan_operator.get());

    OlapScanner::Params scanner_params;
    scanner_params.state = state.get();
    scanner_params.profile = profile.get();
    scanner_params.limit = -1;
    scanner_params.key_ranges = std::vector<OlapScanRange*>();
    std::shared_ptr<Scanner> scanner =
            OlapScanner::create_shared(olap_scan_local_state.get(), std::move(scanner_params));

    std::list<std::shared_ptr<ScannerDelegate>> scanners;
    for (int i = 0; i < 3; ++i) {
        scanners.push_back(std::make_shared<ScannerDelegate>(scanner));
    }
    auto scanner_context = ScannerContext::create_shared(
            state.get(), olap_scan_local_state.get(), output_tuple_desc, output_row_descriptor,
            scanners, -1, scan_dependency, parallel_tasks);
    scanner_context->_newly_create_free_blocks_num = newly_create_free_blocks_num.get();
    scanner_context->_scanner_memory_used_counter = scanner_memory_used_counter.get();

    // One worker, parked, with queue capacity: a submitted Context runnable stays observable in
    // the queue instead of being executed or rejected.
    ThreadPoolSimplifiedScanScheduler scheduler("context_dedup_test", cgroup_cpu_ctl);
    ASSERT_TRUE(scheduler.start(1, 1, 4, 1).ok());
    CountDownLatch task_started(1);
    CountDownLatch release_task(1);
    Defer cleanup = [&] {
        release_task.count_down();
        scheduler.stop();
    };
    ASSERT_TRUE(scheduler
                        .submit_scan_task(SimplifiedScanTask(
                                [&] {
                                    task_started.count_down();
                                    release_task.wait();
                                    return true;
                                },
                                nullptr, nullptr))
                        .ok());
    ASSERT_TRUE(task_started.wait_for(std::chrono::seconds(5)));
    scanner_context->_scanner_scheduler = &scheduler;
    scanner_context->_min_scan_concurrency_of_scan_scheduler = 20;

    auto completed_task = std::make_shared<ScanTask>(scanners.front());
    {
        std::unique_lock<std::mutex> transfer_lock(scanner_context->transfer_lock());
        ASSERT_TRUE(scheduler.schedule_scan_task(scanner_context, nullptr, transfer_lock).ok());
        EXPECT_TRUE(scanner_context->is_context_queued(transfer_lock));
        EXPECT_EQ(scheduler.get_queue_size(), 1);

        // A second scheduling attempt while a runnable is queued must not add another runnable.
        ASSERT_TRUE(scheduler.schedule_scan_task(scanner_context, nullptr, transfer_lock).ok());
        EXPECT_TRUE(scanner_context->is_context_queued(transfer_lock));
        EXPECT_EQ(scheduler.get_queue_size(), 1);

        // Publish a completed non-EOS result so the operator can consume it below.
        completed_task->cached_blocks.emplace_back(Block::create_unique(), 0);
        scanner_context->_tasks_queue.push_back(completed_task);
        scanner_context->_num_scheduled_scanners = 1;
    }

    // Consuming a non-EOS result returns the scanner to the admission queue. The queued runnable
    // will see it, so no additional runnable is submitted.
    Block block;
    bool eos = false;
    Status st = scanner_context->get_block_from_queue(state.get(), &block, &eos, 0);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_FALSE(eos);
    {
        std::unique_lock<std::mutex> transfer_lock(scanner_context->transfer_lock());
        EXPECT_TRUE(completed_task->cached_blocks.empty());
        EXPECT_TRUE(scanner_context->_tasks_queue.empty());
        ASSERT_FALSE(scanner_context->_pending_scanners.empty());
        EXPECT_EQ(scanner_context->_pending_scanners.top(), completed_task);
        EXPECT_TRUE(scanner_context->is_context_queued(transfer_lock));
        EXPECT_EQ(scheduler.get_queue_size(), 1);
        // Cancel the query before the parked worker runs the queued runnable, so it exits without
        // touching the OlapScanner that has no tablet behind it.
        scanner_context->_should_stop = true;
    }
}

TEST_F(ScannerContextTest, thread_pool_stopped_scheduler_fails_context) {
    const int parallel_tasks = 2;
    auto scan_operator = std::make_unique<OlapScanOperatorX>(obj_pool.get(), tnode, 0, *descs,
                                                             parallel_tasks, TQueryCacheParam {});
    auto olap_scan_local_state =
            OlapScanLocalState::create_unique(state.get(), scan_operator.get());

    OlapScanner::Params scanner_params;
    scanner_params.state = state.get();
    scanner_params.profile = profile.get();
    scanner_params.limit = -1;
    scanner_params.key_ranges = std::vector<OlapScanRange*>();
    std::shared_ptr<Scanner> scanner =
            OlapScanner::create_shared(olap_scan_local_state.get(), std::move(scanner_params));

    std::list<std::shared_ptr<ScannerDelegate>> scanners {
            std::make_shared<ScannerDelegate>(scanner)};
    auto scanner_context = ScannerContext::create_shared(
            state.get(), olap_scan_local_state.get(), output_tuple_desc, output_row_descriptor,
            scanners, -1, scan_dependency, parallel_tasks);

    ThreadPoolSimplifiedScanScheduler scheduler("stopped_scheduler_test", cgroup_cpu_ctl);
    ASSERT_TRUE(scheduler.start(1, 1, 1, 1).ok());
    scheduler.stop();
    scanner_context->_scanner_scheduler = &scheduler;

    std::unique_lock<std::mutex> transfer_lock(scanner_context->transfer_lock());
    ASSERT_FALSE(scanner_context->_pending_scanners.empty());
    Status surfaced = scheduler.schedule_scan_task(scanner_context, nullptr, transfer_lock);
    EXPECT_TRUE(surfaced.is<ErrorCode::INTERNAL_ERROR>()) << surfaced.to_string();
    // The Context is terminal and the operator is woken to observe the failure. No runnable was
    // submitted, so the marker stays clear.
    EXPECT_TRUE(scanner_context->done());
    EXPECT_FALSE(scanner_context->_process_status.ok());
    EXPECT_TRUE(scan_dependency->ready());
    EXPECT_FALSE(scanner_context->is_context_queued(transfer_lock));
}

TEST_F(ScannerContextTest, thread_pool_stopped_scheduler_fails_queued_context) {
    const int parallel_tasks = 2;
    auto scan_operator = std::make_unique<OlapScanOperatorX>(obj_pool.get(), tnode, 0, *descs,
                                                             parallel_tasks, TQueryCacheParam {});
    auto olap_scan_local_state =
            OlapScanLocalState::create_unique(state.get(), scan_operator.get());

    OlapScanner::Params scanner_params;
    scanner_params.state = state.get();
    scanner_params.profile = profile.get();
    scanner_params.limit = -1;
    scanner_params.key_ranges = std::vector<OlapScanRange*>();
    std::shared_ptr<Scanner> scanner =
            OlapScanner::create_shared(olap_scan_local_state.get(), std::move(scanner_params));

    std::list<std::shared_ptr<ScannerDelegate>> scanners {
            std::make_shared<ScannerDelegate>(scanner)};
    auto scanner_context = ScannerContext::create_shared(
            state.get(), olap_scan_local_state.get(), output_tuple_desc, output_row_descriptor,
            scanners, -1, scan_dependency, parallel_tasks);

    // A single worker parked with queue capacity keeps the submitted Context runnable queued.
    ThreadPoolSimplifiedScanScheduler scheduler("stopped_queued_scheduler_test", cgroup_cpu_ctl);
    ASSERT_TRUE(scheduler.start(1, 1, 4, 1).ok());
    CountDownLatch task_started(1);
    CountDownLatch release_task(1);
    Defer cleanup = [&] {
        release_task.count_down();
        // The test sets only the stop flag below; clear it so stop() really shuts the pool down.
        scheduler._is_stop = false;
        scheduler.stop();
    };
    ASSERT_TRUE(scheduler
                        .submit_scan_task(SimplifiedScanTask(
                                [&] {
                                    task_started.count_down();
                                    release_task.wait();
                                    return true;
                                },
                                nullptr, nullptr))
                        .ok());
    ASSERT_TRUE(task_started.wait_for(std::chrono::seconds(5)));
    scanner_context->_scanner_scheduler = &scheduler;

    {
        std::unique_lock<std::mutex> transfer_lock(scanner_context->transfer_lock());
        ASSERT_TRUE(scheduler.schedule_scan_task(scanner_context, nullptr, transfer_lock).ok());
        ASSERT_TRUE(scanner_context->is_context_queued(transfer_lock));
    }

    // Stopping the pool drops the queued runnable, so the marker is never cleared by it. The next
    // scheduling attempt must fail the Context instead of waiting for that runnable forever. Only
    // the stop flag is set here; the pool itself is shut down by the cleanup above.
    scheduler._is_stop = true;
    std::unique_lock<std::mutex> transfer_lock(scanner_context->transfer_lock());
    Status surfaced = scheduler.schedule_scan_task(scanner_context, nullptr, transfer_lock);
    EXPECT_TRUE(surfaced.is<ErrorCode::INTERNAL_ERROR>()) << surfaced.to_string();
    EXPECT_TRUE(scanner_context->done());
    EXPECT_FALSE(scanner_context->_process_status.ok());
    EXPECT_TRUE(scan_dependency->ready());
}

TEST_F(ScannerContextTest, run_context_publishes_successor_submit_failure) {
    const int parallel_tasks = 2;
    auto scan_operator = std::make_unique<OlapScanOperatorX>(obj_pool.get(), tnode, 0, *descs,
                                                             parallel_tasks, TQueryCacheParam {});
    auto olap_scan_local_state =
            OlapScanLocalState::create_unique(state.get(), scan_operator.get());

    OlapScanner::Params scanner_params;
    scanner_params.state = state.get();
    scanner_params.profile = profile.get();
    scanner_params.limit = -1;
    scanner_params.key_ranges = std::vector<OlapScanRange*>();
    std::shared_ptr<Scanner> scanner =
            OlapScanner::create_shared(olap_scan_local_state.get(), std::move(scanner_params));

    std::list<std::shared_ptr<ScannerDelegate>> scanners;
    for (int i = 0; i < 2; ++i) {
        scanners.push_back(std::make_shared<ScannerDelegate>(scanner));
    }
    // The worker's task_exec_ctx() must resolve, otherwise _run_context() exits before admission.
    auto task_execution_context = std::make_shared<TaskExecutionContext>();
    state->set_task_execution_context(task_execution_context);
    auto scanner_context = ScannerContext::create_shared(
            state.get(), olap_scan_local_state.get(), output_tuple_desc, output_row_descriptor,
            scanners, -1, scan_dependency, parallel_tasks);

    // One worker and no queue capacity: the first Context runnable is accepted by the idle worker,
    // but the successor it submits while running is rejected.
    ThreadPoolSimplifiedScanScheduler scheduler("successor_failure_test", cgroup_cpu_ctl);
    ASSERT_TRUE(scheduler.start(1, 1, 0, 1).ok());
    Defer cleanup = [&] { scheduler.stop(); };
    scanner_context->_scanner_scheduler = &scheduler;
    // The pool never reaches this budget, so the second pending scanner is admissible and the
    // runnable tries to submit its successor.
    scanner_context->_min_scan_concurrency_of_scan_scheduler = 20;

    {
        std::unique_lock<std::mutex> transfer_lock(scanner_context->transfer_lock());
        ASSERT_TRUE(scheduler.schedule_scan_task(scanner_context, nullptr, transfer_lock).ok());
    }

    // The admitted scanner is published with the submission failure instead of being executed,
    // so its scheduled slot is released and the operator observes the error.
    bool published = false;
    for (int i = 0; i < 10000; ++i) {
        std::unique_lock<std::mutex> transfer_lock(scanner_context->transfer_lock());
        if (!scanner_context->_tasks_queue.empty()) {
            published = true;
            break;
        }
        transfer_lock.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(published);

    std::unique_lock<std::mutex> transfer_lock(scanner_context->transfer_lock());
    ASSERT_EQ(scanner_context->_tasks_queue.size(), 1);
    EXPECT_TRUE(scanner_context->_tasks_queue.front()->get_status().is<ErrorCode::TOO_MANY_TASKS>())
            << scanner_context->_tasks_queue.front()->get_status().to_string();
    EXPECT_TRUE(scanner_context->_process_status.is<ErrorCode::TOO_MANY_TASKS>());
    EXPECT_TRUE(scanner_context->done());
    EXPECT_EQ(scanner_context->_num_scheduled_scanners, 0);
    // The second scanner was never admitted.
    EXPECT_EQ(scanner_context->_pending_scanners.size(), 1);
}

TEST_F(ScannerContextTest, terminal_eos_skips_context_submission) {
    // A full pool: one parked worker and no queue capacity, so any submission would fail.
    ThreadPoolSimplifiedScanScheduler scheduler("terminal_eos_test", cgroup_cpu_ctl);
    ASSERT_TRUE(scheduler.start(1, 1, 0, 1).ok());
    CountDownLatch task_started(1);
    CountDownLatch release_task(1);
    Defer cleanup = [&] {
        release_task.count_down();
        scheduler.stop();
    };
    ASSERT_TRUE(scheduler
                        .submit_scan_task(SimplifiedScanTask(
                                [&] {
                                    task_started.count_down();
                                    release_task.wait();
                                    return true;
                                },
                                nullptr, nullptr))
                        .ok());
    ASSERT_TRUE(task_started.wait_for(std::chrono::seconds(5)));
    ASSERT_EQ(scheduler.get_active_threads(), 1);

    const int parallel_tasks = 1;
    auto scan_operator = std::make_unique<OlapScanOperatorX>(obj_pool.get(), tnode, 0, *descs,
                                                             parallel_tasks, TQueryCacheParam {});
    auto olap_scan_local_state =
            OlapScanLocalState::create_unique(state.get(), scan_operator.get());

    OlapScanner::Params scanner_params;
    scanner_params.state = state.get();
    scanner_params.profile = profile.get();
    scanner_params.limit = 100;
    scanner_params.key_ranges = std::vector<OlapScanRange*>();
    std::shared_ptr<Scanner> scanner =
            OlapScanner::create_shared(olap_scan_local_state.get(), std::move(scanner_params));

    std::list<std::shared_ptr<ScannerDelegate>> scanners {
            std::make_shared<ScannerDelegate>(scanner)};
    auto scanner_context = ScannerContext::create_shared(
            state.get(), olap_scan_local_state.get(), output_tuple_desc, output_row_descriptor,
            scanners, 100, scan_dependency, parallel_tasks);
    scanner_context->_scanner_scheduler = &scheduler;

    // The only scanner has reported EOS and its result waits for the operator.
    scanner_context->_pending_scanners = std::stack<std::shared_ptr<ScanTask>>();
    auto eos_task = std::make_shared<ScanTask>(scanners.front());
    eos_task->set_eos(true);
    scanner_context->_tasks_queue.push_back(eos_task);
    scanner_context->_num_scheduled_scanners = 0;

    MockRuntimeStateLocal mock_runtime_state;
    EXPECT_CALL(mock_runtime_state, is_cancelled()).WillRepeatedly(testing::Return(false));
    Block block;
    bool eos = false;
    Status status = scanner_context->get_block_from_queue(&mock_runtime_state, &block, &eos, 0);

    // All scanners completed: the Context finishes without submitting a runnable, even though
    // the pool could not accept one.
    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_TRUE(eos);
    EXPECT_EQ(scanner_context->_num_finished_scanners, 1);
    EXPECT_EQ(scheduler.get_queue_size(), 0);
    std::unique_lock<std::mutex> transfer_lock(scanner_context->transfer_lock());
    EXPECT_FALSE(scanner_context->is_context_queued(transfer_lock));
}

} // namespace doris
