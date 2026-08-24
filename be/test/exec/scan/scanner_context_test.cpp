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

#include <chrono>
#include <list>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <tuple>

#include "common/config.h"
#include "common/object_pool.h"
#include "core/block/block.h"
#include "exec/operator/olap_scan_operator.h"
#include "exec/pipeline/dependency.h"
#include "exec/scan/mock_simplified_scan_scheduler.h"
#include "exec/scan/olap_scanner.h"
#include "exec/scan/scan_node.h"
#include "exec/scan/scanner_scheduler.h"
#include "runtime/descriptors.h"
#include "runtime/query_context.h"
#include "runtime/task_execution_context.h"
#include "storage/options.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_meta.h"
#include "testutil/mock/mock_runtime_state.h"
#include "util/countdown_latch.h"
#include "util/debug_points.h"
#include "util/defer_op.h"

namespace doris {
class ScannerContextTest : public testing::Test {
public:
    void SetUp() override {
        obj_pool = std::make_unique<ObjectPool>();
        // This ScanNode has two tuples.
        // First one is input tuple, second one is output tuple.
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
    std::shared_ptr<Dependency> scan_dependency =
            Dependency::create_shared(0, 0, "TestScanDependency");
    std::shared_ptr<CgroupCpuCtl> cgroup_cpu_ctl = std::make_shared<CgroupV2CpuCtl>(1);
    std::unique_ptr<ScannerScheduler> scan_scheduler =
            std::make_unique<ThreadPoolSimplifiedScanScheduler>("ForTest", cgroup_cpu_ctl);
    std::atomic<int64_t> shared_limit {-1};
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
            state.get(), olap_scan_local_state.get(), output_tuple_desc, false, scanners, limit,
            scan_dependency, &shared_limit, nullptr, nullptr, 0, false, parallel_tasks);

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
    // init() is invoked twice below, and each invocation performs one initial scheduling attempt.
    // Keep this expectation explicit so changing bootstrap scheduling updates this test too.
    EXPECT_CALL(*scheduler, schedule_scan_task(testing::_, testing::_, testing::_))
            .Times(2)
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

TEST_F(ScannerContextTest, inverted_index_profile_collection_is_additive_and_idempotent) {
    auto engine = std::make_unique<StorageEngine>(EngineOptions {});
    auto tablet_meta = std::make_shared<TabletMeta>(1, 2, 15673, 15674, 4, 5, TTabletSchema {}, 6,
                                                    std::unordered_map<uint32_t, uint32_t> {{7, 8}},
                                                    UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK,
                                                    TCompressionType::LZ4F);
    auto tablet = std::make_shared<Tablet>(*engine, std::move(tablet_meta), nullptr);
    const int parallel_tasks = 1;
    auto scan_operator = std::make_unique<OlapScanOperatorX>(obj_pool.get(), tnode, 0, *descs,
                                                             parallel_tasks, TQueryCacheParam {});
    auto local_state = OlapScanLocalState::create_unique(state.get(), scan_operator.get());
    const std::vector<TScanRangeParams> scan_ranges;
    const std::map<int, std::pair<std::shared_ptr<BasicSharedState>,
                                  std::vector<std::shared_ptr<Dependency>>>>
            shared_state_map;
    LocalStateInfo local_state_info {profile.get(), scan_ranges, nullptr, shared_state_map, 0};
    const Status init_status = local_state->init(state.get(), local_state_info);
    ASSERT_TRUE(init_status.ok()) << init_status.to_string();

    auto make_scanner = [&]() {
        OlapScanner::Params params;
        params.state = state.get();
        params.profile = profile.get();
        params.version = 0;
        params.limit = -1;
        params.aggregation = false;
        return OlapScanner::create_shared(local_state.get(), std::move(params));
    };
    auto scanner1 = make_scanner();
    auto scanner2 = make_scanner();
    scanner1->_tablet_reader_params.tablet = tablet;
    scanner2->_tablet_reader_params.tablet = tablet;
    scanner1->_tablet_reader = std::make_unique<TabletReader>();
    scanner2->_tablet_reader = std::make_unique<TabletReader>();
    auto* stats1 = scanner1->_tablet_reader->mutable_stats();
    stats1->snii_stats.prx_raw_frames = 1;
    stats1->snii_stats.prx_plaintext_bytes = 10;
    stats1->snii_stats.prx_decode_ns = 100;
    stats1->snii_stats.phrase_candidate_docs = 3;
    stats1->snii_stats.common_grams_gram_plans = 1;
    stats1->snii_stats.common_grams_fallback_kill_switch = 5;
    stats1->snii_stats.common_grams_plain_posting_bytes = 10;
    stats1->snii_stats.common_grams_gram_posting_bytes = 20;
    stats1->snii_stats.common_grams_plain_estimated_candidate_df = 30;
    stats1->snii_stats.common_grams_gram_estimated_candidate_df = 40;
    stats1->snii_stats.common_grams_plain_estimated_cost = 50;
    stats1->snii_stats.common_grams_gram_estimated_cost = 60;
    stats1->snii_stats.common_grams_fallback_base_analyzer_mismatch = 61;
    stats1->snii_stats.common_grams_fallback_prefix_tail_empty = 62;
    stats1->snii_stats.common_grams_planning_ns = 65;
    auto* stats2 = scanner2->_tablet_reader->mutable_stats();
    stats2->snii_stats.prx_raw_frames = 2;
    stats2->snii_stats.prx_plaintext_bytes = 20;
    stats2->snii_stats.prx_decode_ns = 200;
    stats2->snii_stats.phrase_candidate_docs = 4;
    stats2->snii_stats.common_grams_gram_plans = 2;
    stats2->snii_stats.common_grams_fallback_kill_switch = 6;
    stats2->snii_stats.common_grams_plain_posting_bytes = 1;
    stats2->snii_stats.common_grams_gram_posting_bytes = 2;
    stats2->snii_stats.common_grams_plain_estimated_candidate_df = 3;
    stats2->snii_stats.common_grams_gram_estimated_candidate_df = 4;
    stats2->snii_stats.common_grams_plain_estimated_cost = 5;
    stats2->snii_stats.common_grams_gram_estimated_cost = 6;
    stats2->snii_stats.common_grams_fallback_base_analyzer_mismatch = 7;
    stats2->snii_stats.common_grams_fallback_prefix_tail_empty = 8;
    stats2->snii_stats.common_grams_planning_ns = 11;

    RuntimeProfile* index_filter = local_state->_index_filter_profile.get();
    ASSERT_NE(index_filter, nullptr);
    auto* raw_frames = index_filter->get_counter("SniiPrxRawFrames");
    auto* plaintext_bytes = index_filter->get_counter("SniiPrxPlaintextBytes");
    auto* decode_time = index_filter->get_counter("SniiPrxInclusiveDecodeTime");
    auto* phrase_candidate_docs = index_filter->get_counter("SniiPhraseCandidateDocs");
    auto* common_grams_gram_plans = index_filter->get_counter("SniiCommonGramsGramPlans");
    auto* common_grams_fallback_kill_switch =
            index_filter->get_counter("SniiCommonGramsFallbackKillSwitch");
    struct ExpectedSniiCounter {
        const char* name;
        RuntimeProfile::Counter* counter;
        int64_t scanner1_value;
        int64_t combined_value;
    };
    const ExpectedSniiCounter snii_counters[] = {
            {"SniiCommonGramsPlainPostingBytes",
             index_filter->get_counter("SniiCommonGramsPlainPostingBytes"), 10, 11},
            {"SniiCommonGramsGramPostingBytes",
             index_filter->get_counter("SniiCommonGramsGramPostingBytes"), 20, 22},
            {"SniiCommonGramsPlainEstimatedCandidateDf",
             index_filter->get_counter("SniiCommonGramsPlainEstimatedCandidateDf"), 30, 33},
            {"SniiCommonGramsGramEstimatedCandidateDf",
             index_filter->get_counter("SniiCommonGramsGramEstimatedCandidateDf"), 40, 44},
            {"SniiCommonGramsPlainEstimatedCost",
             index_filter->get_counter("SniiCommonGramsPlainEstimatedCost"), 50, 55},
            {"SniiCommonGramsGramEstimatedCost",
             index_filter->get_counter("SniiCommonGramsGramEstimatedCost"), 60, 66},
            {"SniiCommonGramsFallbackBaseAnalyzerMismatch",
             index_filter->get_counter("SniiCommonGramsFallbackBaseAnalyzerMismatch"), 61, 68},
            {"SniiCommonGramsFallbackPrefixTailEmpty",
             index_filter->get_counter("SniiCommonGramsFallbackPrefixTailEmpty"), 62, 70},
            {"SniiCommonGramsPlanningTime",
             index_filter->get_counter("SniiCommonGramsPlanningTime"), 65, 76},
    };

    std::vector<TRuntimeProfileNode> zero_nodes;
    index_filter->to_thrift(&zero_nodes);
    ASSERT_EQ(zero_nodes.size(), 1U);
    for (const auto& expected : snii_counters) {
        bool serialized = false;
        for (const auto& thrift_counter : zero_nodes.front().counters) {
            serialized |= thrift_counter.name == expected.name;
        }
        EXPECT_FALSE(serialized) << expected.name;
    }
    ASSERT_NE(raw_frames, nullptr);
    ASSERT_NE(plaintext_bytes, nullptr);
    ASSERT_NE(decode_time, nullptr);
    ASSERT_NE(phrase_candidate_docs, nullptr);
    ASSERT_NE(common_grams_gram_plans, nullptr);
    ASSERT_NE(common_grams_fallback_kill_switch, nullptr);
    for (const auto& expected : snii_counters) {
        ASSERT_NE(expected.counter, nullptr) << expected.name;
        EXPECT_NE(dynamic_cast<RuntimeProfile::NonZeroCounter*>(expected.counter), nullptr)
                << expected.name;
    }

    scanner1->_collect_profile_before_close();
    EXPECT_EQ(raw_frames->value(), 1);
    EXPECT_EQ(plaintext_bytes->value(), 10);
    EXPECT_EQ(decode_time->value(), 100);
    EXPECT_EQ(phrase_candidate_docs->value(), 3);
    EXPECT_EQ(common_grams_gram_plans->value(), 1);
    EXPECT_EQ(common_grams_fallback_kill_switch->value(), 5);
    for (const auto& expected : snii_counters) {
        EXPECT_EQ(expected.counter->value(), expected.scanner1_value) << expected.name;
    }

    scanner1->_collect_profile_before_close();
    EXPECT_EQ(raw_frames->value(), 1);
    EXPECT_EQ(plaintext_bytes->value(), 10);
    EXPECT_EQ(decode_time->value(), 100);
    EXPECT_EQ(phrase_candidate_docs->value(), 3);
    EXPECT_EQ(common_grams_gram_plans->value(), 1);
    EXPECT_EQ(common_grams_fallback_kill_switch->value(), 5);
    for (const auto& expected : snii_counters) {
        EXPECT_EQ(expected.counter->value(), expected.scanner1_value) << expected.name;
    }

    scanner2->_collect_profile_before_close();
    EXPECT_EQ(raw_frames->value(), 3);
    EXPECT_EQ(plaintext_bytes->value(), 30);
    EXPECT_EQ(decode_time->value(), 300);
    EXPECT_EQ(phrase_candidate_docs->value(), 7);
    EXPECT_EQ(common_grams_gram_plans->value(), 3);
    EXPECT_EQ(common_grams_fallback_kill_switch->value(), 11);
    for (const auto& expected : snii_counters) {
        EXPECT_EQ(expected.counter->value(), expected.combined_value) << expected.name;
    }
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
            state.get(), olap_scan_local_state.get(), output_tuple_desc, false, scanners, limit,
            scan_dependency, &shared_limit, nullptr, nullptr, 0, false, parallel_tasks);

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
            state.get(), olap_scan_local_state.get(), output_tuple_desc, false, scanners, limit,
            scan_dependency, &shared_limit, nullptr, nullptr, 0, false, parallel_tasks);

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

TEST_F(ScannerContextTest, test_push_completed_scan_task) {
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
            state.get(), olap_scan_local_state.get(), output_tuple_desc, false, scanners, limit,
            scan_dependency, &shared_limit, nullptr, nullptr, 0, false, parallel_tasks);

    scanner_context->_in_flight_tasks_num = 11;

    for (int i = 0; i < 5; ++i) {
        auto scan_task = std::make_shared<ScanTask>(std::make_shared<ScannerDelegate>(scanner));
        scanner_context->push_completed_scan_task(scan_task);
        ASSERT_EQ(scanner_context->_in_flight_tasks_num, 10 - i);
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
            state.get(), olap_scan_local_state.get(), output_tuple_desc, false, scanners, limit,
            scan_dependency, &shared_limit, nullptr, nullptr, 0, false, parallel_tasks);

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
    scanner_context->_in_flight_tasks_num = 0;
    margin = scanner_context->_get_margin(transfer_lock, scheduler_lock);
    ASSERT_EQ(margin, scanner_context->_min_scan_concurrency);

    // ScanSchedule is busy.
    // _min_scan_concurrency is already satisfied.
    scheduler = std::make_unique<MockSimplifiedScanScheduler>(cgroup_cpu_ctl);
    EXPECT_CALL(*scheduler, get_active_threads()).WillOnce(testing::Return(50));
    EXPECT_CALL(*scheduler, get_queue_size()).WillOnce(testing::Return(10));
    scanner_context->_scanner_scheduler = scheduler.get();
    scanner_context->_min_scan_concurrency_of_scan_scheduler = 20;
    scanner_context->_in_flight_tasks_num = 20;
    margin = scanner_context->_get_margin(transfer_lock, scheduler_lock);
    ASSERT_EQ(margin, 0);
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
            state.get(), olap_scan_local_state.get(), output_tuple_desc, false, scanners, limit,
            scan_dependency, &shared_limit, nullptr, nullptr, 0, false, parallel_tasks);

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
    scan_task->cached_block = std::move(cached_block);
    EXPECT_ANY_THROW(scanner_context->_pull_next_scan_task(
            scan_task, scanner_context->_max_scan_concurrency - 1));
    scan_task->cached_block.reset();
    scan_task->_state = ScanTask::State::IN_FLIGHT;
    scan_task->set_state(ScanTask::State::EOS);
    EXPECT_ANY_THROW(scanner_context->_pull_next_scan_task(
            scan_task, scanner_context->_max_scan_concurrency - 1));

    scan_task->cached_block.reset();
    scan_task->_state = ScanTask::State::IN_FLIGHT;
    pull_scan_task = scanner_context->_pull_next_scan_task(
            scan_task, scanner_context->_max_scan_concurrency - 1);
    EXPECT_EQ(pull_scan_task.get(), scan_task.get());

    scanner_context->_pending_tasks = std::stack<std::shared_ptr<ScanTask>>();
    pull_scan_task = scanner_context->_pull_next_scan_task(
            nullptr, scanner_context->_max_scan_concurrency - 1);
    EXPECT_EQ(pull_scan_task, nullptr);

    scanner_context->_pending_tasks.push(
            std::make_shared<ScanTask>(std::make_shared<ScannerDelegate>(scanner)));
    pull_scan_task = scanner_context->_pull_next_scan_task(
            nullptr, scanner_context->_max_scan_concurrency - 1);
    EXPECT_NE(pull_scan_task, nullptr);
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
            state.get(), olap_scan_local_state.get(), output_tuple_desc, false, scanners, -1,
            scan_dependency, &shared_limit, nullptr, nullptr, 0, false, parallel_tasks);

    std::unique_lock<std::mutex> context_transfer_lock(scanner_context->transfer_lock());
    scanner_context->_pending_tasks = std::stack<std::shared_ptr<ScanTask>>();
    scanner_context->_completed_tasks.clear();
    scanner_context->_in_flight_tasks_num = 0;
    // Even if the effective limit is temporarily zero, one pending task must run so it can publish
    // a block or EOS and prevent the Context from stalling.
    scanner_context->_max_scan_concurrency = 0;

    EXPECT_FALSE(scanner_context->can_admit_scan_task(context_transfer_lock));

    auto completed_task = std::make_shared<ScanTask>(std::make_shared<ScannerDelegate>(scanner));
    completed_task->set_state(ScanTask::State::IN_FLIGHT);
    completed_task->cached_block = Block::create_unique();
    completed_task->set_state(ScanTask::State::COMPLETED);
    completed_task->cached_block.reset();
    // A consumed non-EOS result must be eligible for another Context admission. This also covers
    // the COMPLETED -> PENDING transition used by ThreadPool scheduling.
    scanner_context->push_pending_scan_task(completed_task, context_transfer_lock);
    EXPECT_TRUE(scanner_context->can_admit_scan_task(context_transfer_lock));

    EXPECT_FALSE(scanner_context->is_context_queued(context_transfer_lock));
    scanner_context->set_context_queued(true, context_transfer_lock);
    EXPECT_TRUE(scanner_context->is_context_queued(context_transfer_lock));
    scanner_context->set_context_queued(false, context_transfer_lock);

    // The Context can admit exactly one scanner at its configured concurrency limit.
    auto admitted_task = scanner_context->try_get_next_scan_task(context_transfer_lock);
    EXPECT_EQ(admitted_task, completed_task);
    EXPECT_EQ(admitted_task->_state, ScanTask::State::IN_FLIGHT);
    EXPECT_EQ(scanner_context->_in_flight_tasks_num, 1);

    auto blocked_task = std::make_shared<ScanTask>(std::make_shared<ScannerDelegate>(scanner));
    scanner_context->push_pending_scan_task(blocked_task, context_transfer_lock);
    EXPECT_FALSE(scanner_context->can_admit_scan_task(context_transfer_lock));
    EXPECT_EQ(scanner_context->try_get_next_scan_task(context_transfer_lock), nullptr);
}

TEST_F(ScannerContextTest, thread_pool_admission_refreshes_adaptive_limit) {
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
    for (int i = 0; i < 5; ++i) {
        scanners.push_back(std::make_shared<ScannerDelegate>(scanner));
    }

    TUniqueId query_id = state->get_query_ctx()->query_id();
    const int64_t query_mem_limit = 1024LL * 1024 * 1024;
    auto arbitrator = MemShareArbitrator::create_shared(query_id, query_mem_limit, 0.3);
    auto limiter = MemLimiter::create_shared(query_id, parallel_tasks, false,
                                             static_cast<int64_t>(query_mem_limit * 0.3));
    // 200MB budget with 100MB estimated blocks: max_count = 2, so instance 1 gets exactly one
    // adaptive slot. ins_idx = 1 keeps _available_pickup_scanner_count() away from the
    // arbitrator-driven limit adjustment, which would overwrite this deterministic setup.
    limiter->update_open_tasks_count(1);
    limiter->update_mem_limit(200LL * 1024 * 1024);
    limiter->reestimated_block_mem_bytes(100LL * 1024 * 1024);

    auto scanner_context = ScannerContext::create_shared(
            state.get(), olap_scan_local_state.get(), output_tuple_desc, false, scanners, -1,
            scan_dependency, &shared_limit, arbitrator, limiter, 1, true, parallel_tasks);

    std::unique_lock<std::mutex> transfer_lock(scanner_context->transfer_lock());
    ASSERT_TRUE(scanner_context->_enable_adaptive_scanners);
    ASSERT_LT(1, scanner_context->_max_scan_concurrency);
    EXPECT_EQ(scanner_context->_adaptive_processor->expected_scanners, 0);

    // Admission refreshes the adaptive limit. Nothing is progressing yet, so the first scanner is
    // admitted regardless, but expected_scanners must no longer stay at its initial zero.
    auto first_task = scanner_context->try_get_next_scan_task(transfer_lock);
    ASSERT_NE(first_task, nullptr);
    EXPECT_EQ(scanner_context->_adaptive_processor->expected_scanners, 1);

    // One task is in flight and the refreshed adaptive limit is one: admission must refuse the
    // next scanner even though _max_scan_concurrency would still allow it.
    EXPECT_FALSE(scanner_context->can_admit_scan_task(transfer_lock));
    EXPECT_EQ(scanner_context->try_get_next_scan_task(transfer_lock), nullptr);
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
            state.get(), olap_scan_local_state.get(), output_tuple_desc, false, scanners, -1,
            scan_dependency, &shared_limit, nullptr, nullptr, 0, false, parallel_tasks);

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
    ASSERT_FALSE(scanner_context->_pending_tasks.empty());

    // Context submission is fail-fast regardless of other progress. Retrying here would couple
    // the scanner scheduler to ThreadPool's internal rejection/retention behavior.
    Status surfaced = scheduler.schedule_scan_task(scanner_context, nullptr, transfer_lock);
    EXPECT_TRUE(surfaced.is<ErrorCode::TOO_MANY_TASKS>()) << surfaced.to_string();
    EXPECT_TRUE(scanner_context->done());
    EXPECT_FALSE(scanner_context->_process_status.ok());
    EXPECT_TRUE(scan_dependency->ready());
    // The marker is set before submit_func(). This rejected runnable was not retained, but the
    // terminal Context no longer needs the marker cleared or another submission attempted.
    EXPECT_TRUE(scanner_context->is_context_queued(transfer_lock));
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
            state.get(), olap_scan_local_state.get(), output_tuple_desc, false, scanners, -1,
            scan_dependency, &shared_limit, nullptr, nullptr, 0, false, parallel_tasks);

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
    // as a completed task instead of terminating the process or leaking the in-flight slot.
    bool published = false;
    for (int i = 0; i < 10000; ++i) {
        std::unique_lock<std::mutex> transfer_lock(scanner_context->transfer_lock());
        if (!scanner_context->_completed_tasks.empty()) {
            published = true;
            break;
        }
        transfer_lock.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(published);

    std::unique_lock<std::mutex> transfer_lock(scanner_context->transfer_lock());
    ASSERT_EQ(scanner_context->_completed_tasks.size(), 1);
    EXPECT_FALSE(scanner_context->_completed_tasks.front()->status_ok());
    EXPECT_EQ(scanner_context->_in_flight_tasks_num, 0);
    EXPECT_FALSE(scanner_context->is_context_queued(transfer_lock));
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
            state.get(), olap_scan_local_state.get(), output_tuple_desc, false, scanners, limit,
            scan_dependency, &shared_limit, nullptr, nullptr, 0, false, parallel_tasks);

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
    ASSERT_EQ(scanner_context->_in_flight_tasks_num, 1);

    scanner_context->_max_scan_concurrency = 10;
    scanner_context->_max_scan_concurrency = 1;
    scanner_context->_min_scan_concurrency_of_scan_scheduler = 20;
    st = scanner_context->schedule_scan_task(nullptr, transfer_lock, scheduler_lock);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(scanner_context->_in_flight_tasks_num, scanner_context->_max_scan_concurrency);

    scanner_context = ScannerContext::create_shared(
            state.get(), olap_scan_local_state.get(), output_tuple_desc, false, scanners, limit,
            scan_dependency, &shared_limit, nullptr, nullptr, 0, false, parallel_tasks);

    scanner_context->_scanner_scheduler = scheduler.get();

    scanner_context->_max_scan_concurrency = 100;
    scanner_context->_min_scan_concurrency = 1;
    scanner_context->_min_scan_concurrency_of_scan_scheduler = 20;
    int margin = scanner_context->_get_margin(transfer_lock, scheduler_lock);
    ASSERT_EQ(margin, scanner_context->_min_scan_concurrency_of_scan_scheduler);
    st = scanner_context->schedule_scan_task(nullptr, transfer_lock, scheduler_lock);
    ASSERT_TRUE(st.ok());
    // 15 since we have 15 scanners.
    ASSERT_EQ(scanner_context->_in_flight_tasks_num, 15);

    scanners = std::list<std::shared_ptr<ScannerDelegate>>();
    for (int i = 0; i < 1; ++i) {
        scanners.push_back(std::make_shared<ScannerDelegate>(scanner));
    }

    scanner_context = ScannerContext::create_shared(
            state.get(), olap_scan_local_state.get(), output_tuple_desc, false, scanners, limit,
            scan_dependency, &shared_limit, nullptr, nullptr, 0, false, parallel_tasks);

    scanner_context->_scanner_scheduler = scheduler.get();

    scanner_context->_max_scan_concurrency = 1;
    scanner_context->_min_scan_concurrency = 1;
    scanner_context->_min_scan_concurrency_of_scan_scheduler = 20;
    st = scanner_context->schedule_scan_task(nullptr, transfer_lock, scheduler_lock);
    auto scan_task = std::make_shared<ScanTask>(std::make_shared<ScannerDelegate>(scanner));
    st = scanner_context->schedule_scan_task(scan_task, transfer_lock, scheduler_lock);
    // current scan task is added back.
    ASSERT_EQ(scanner_context->_pending_tasks.size(), 1);
    ASSERT_EQ(scanner_context->_in_flight_tasks_num, 1);

    scanner_context = ScannerContext::create_shared(
            state.get(), olap_scan_local_state.get(), output_tuple_desc, false, scanners, limit,
            scan_dependency, &shared_limit, nullptr, nullptr, 0, false, parallel_tasks);

    scanner_context->_scanner_scheduler = scheduler.get();

    scanner_context->_max_scan_concurrency = 1;
    scanner_context->_min_scan_concurrency = 1;
    scanner_context->_min_scan_concurrency_of_scan_scheduler = 20;
    st = scanner_context->schedule_scan_task(nullptr, transfer_lock, scheduler_lock);
    scan_task = std::make_shared<ScanTask>(std::make_shared<ScannerDelegate>(scanner));
    scan_task->cached_block = Block::create_unique();
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
            state.get(), olap_scan_local_state.get(), output_tuple_desc, false, scanners, limit,
            scan_dependency, &shared_limit, nullptr, nullptr, 0, false, parallel_tasks);

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
            state.get(), olap_scan_local_state.get(), output_tuple_desc, false, scanners, limit,
            scan_dependency, &shared_limit, nullptr, nullptr, 0, false, parallel_tasks);
    scanner_context->_newly_create_free_blocks_num = newly_create_free_blocks_num.get();
    scanner_context->_newly_create_free_blocks_num->set(int64_t(0));
    scanner_context->_scanner_memory_used_counter = scanner_memory_used_counter.get();
    scanner_context->_scanner_memory_used_counter->set(int64_t(0));
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
            state.get(), olap_scan_local_state.get(), output_tuple_desc, false, scanners, limit,
            scan_dependency, &shared_limit, nullptr, nullptr, 0, false, parallel_tasks);
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
            state.get(), olap_scan_local_state.get(), output_tuple_desc, false, scanners, limit,
            scan_dependency, &shared_limit, nullptr, nullptr, 0, false, parallel_tasks);
    shared_limit.store(limit);
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
    scan_task->_state = ScanTask::State::IN_FLIGHT;
    scan_task->set_state(ScanTask::State::EOS);
    scanner_context->_completed_tasks.push_back(scan_task);
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

TEST_F(ScannerContextTest, terminal_eos_skips_context_submission) {
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

    auto verify_terminal_context = [&](int scanner_count, int64_t remaining_limit) {
        const int parallel_tasks = 1;
        auto scan_operator = std::make_unique<OlapScanOperatorX>(
                obj_pool.get(), tnode, 0, *descs, parallel_tasks, TQueryCacheParam {});
        auto olap_scan_local_state =
                OlapScanLocalState::create_unique(state.get(), scan_operator.get());

        OlapScanner::Params scanner_params;
        scanner_params.state = state.get();
        scanner_params.profile = profile.get();
        scanner_params.limit = 100;
        scanner_params.key_ranges = std::vector<OlapScanRange*>();
        std::shared_ptr<Scanner> scanner =
                OlapScanner::create_shared(olap_scan_local_state.get(), std::move(scanner_params));

        std::list<std::shared_ptr<ScannerDelegate>> scanners;
        for (int i = 0; i < scanner_count; ++i) {
            scanners.push_back(std::make_shared<ScannerDelegate>(scanner));
        }
        auto scanner_context = ScannerContext::create_shared(
                state.get(), olap_scan_local_state.get(), output_tuple_desc, false, scanners, 100,
                scan_dependency, &shared_limit, nullptr, nullptr, 0, false, parallel_tasks);
        scanner_context->_scanner_scheduler = &scheduler;

        scanner_context->_pending_tasks = std::stack<std::shared_ptr<ScanTask>>();
        auto scanner_iter = scanners.begin();
        auto eos_task = std::make_shared<ScanTask>(*scanner_iter++);
        eos_task->set_state(ScanTask::State::IN_FLIGHT);
        eos_task->set_state(ScanTask::State::EOS);
        scanner_context->_completed_tasks.push_back(eos_task);
        while (scanner_iter != scanners.end()) {
            scanner_context->_pending_tasks.push(std::make_shared<ScanTask>(*scanner_iter++));
        }
        scanner_context->_in_flight_tasks_num = 0;
        shared_limit.store(remaining_limit);

        MockRuntimeStateLocal mock_runtime_state;
        EXPECT_CALL(mock_runtime_state, is_cancelled()).WillRepeatedly(testing::Return(false));
        Block block;
        bool eos = false;
        Status status = scanner_context->get_block_from_queue(&mock_runtime_state, &block, &eos, 0);

        EXPECT_TRUE(status.ok()) << status.to_string();
        EXPECT_TRUE(eos);
        EXPECT_EQ(scheduler.get_queue_size(), 0);
    };

    // All scanners completed: no runnable is needed even if the pool cannot accept one.
    verify_terminal_context(1, 100);
    // Shared LIMIT completed the Context while another scanner is pending. Check terminal state
    // before rescheduling so the full pool cannot turn successful EOS into TOO_MANY_TASKS.
    verify_terminal_context(2, 0);
}

/**
    MemShareArbitrator Tests (5 tests)
  - scanner_mem_share_arbitrator_basic: Tests initialization, query_id, memory limits, and initial state
  - scanner_mem_share_arbitrator_register_scan_node: Tests registering scan nodes and default memory allocation (64MB)
  - scanner_mem_share_arbitrator_update_mem_bytes: Tests updating memory bytes and handling zero values
  - scanner_mem_share_arbitrator_proportional_sharing: Tests proportional memory distribution across multiple contexts
  - scanner_mem_share_arbitrator_zero_ratio: Tests edge case with zero scan ratio

  MemLimiter Tests (9 tests)

  - scanner_mem_limiter_basic: Tests initialization and default values
  - scanner_mem_limiter_reestimated_block_mem_bytes: Tests averaging algorithm for block memory estimation
  - scanner_mem_limiter_reestimated_zero_value: Tests that zero values are properly ignored
  - scanner_mem_limiter_available_scanner_count: Tests scanner count calculation based on memory limits
  - scanner_mem_limiter_serial_scan: Tests serial scan mode behavior
  - scanner_mem_limiter_update_running_tasks_count: Tests atomic counter updates
  - scanner_mem_limiter_update_open_tasks_count: Tests context count tracking
  - scanner_mem_limiter_update_arb_mem_bytes: Tests memory capping at query limit
  - scanner_mem_limiter_available_count_distribution: Tests fair distribution across parallel instances

  ScannerContext with Memory Control Tests (4 tests)
  - scanner_context_with_adaptive_memory: Tests integration with arbitrator and limiter
  - scanner_context_adjust_scan_mem_limit: Tests dynamic memory limit adjustment
  - scanner_context_reestimated_block_mem_bytes: Tests block memory re-estimation propagation
  - scanner_context_update_peak_running_scanner: Tests peak scanner tracking with memory control

  Total: 18 new test cases

  All tests follow the existing patterns in the codebase and cover:
  - Normal operation scenarios
  - Edge cases (zero values, limits, etc.)
  - Integration between components
  - Atomic operations and thread safety
  - Memory distribution algorithms
*/
// ==================== MemShareArbitrator Tests ====================
TEST_F(ScannerContextTest, scanner_mem_share_arbitrator_basic) {
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 2;
    int64_t query_mem_limit = 1024 * 1024 * 1024;
    double max_scan_ratio = 0.3;

    auto arbitrator = MemShareArbitrator::create_shared(query_id, query_mem_limit, max_scan_ratio);

    ASSERT_EQ(arbitrator->query_id.hi, 1);
    ASSERT_EQ(arbitrator->query_id.lo, 2);
    ASSERT_EQ(arbitrator->query_mem_limit, query_mem_limit);
    ASSERT_EQ(arbitrator->mem_limit, static_cast<int64_t>(query_mem_limit * max_scan_ratio));
    ASSERT_EQ(arbitrator->total_mem_bytes.load(), 0);
}

TEST_F(ScannerContextTest, scanner_mem_share_arbitrator_register_scan_node) {
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 2;
    int64_t query_mem_limit = 1024 * 1024 * 1024;
    double max_scan_ratio = 0.3;

    auto arbitrator = MemShareArbitrator::create_shared(query_id, query_mem_limit, max_scan_ratio);

    arbitrator->register_scan_node();
    ASSERT_EQ(arbitrator->total_mem_bytes.load(), 64 * 1024 * 1024);

    arbitrator->register_scan_node();
    ASSERT_EQ(arbitrator->total_mem_bytes.load(), 128 * 1024 * 1024);
}

TEST_F(ScannerContextTest, scanner_mem_share_arbitrator_update_mem_bytes) {
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 2;
    int64_t query_mem_limit = 1024 * 1024 * 1024;
    double max_scan_ratio = 0.3;

    auto arbitrator = MemShareArbitrator::create_shared(query_id, query_mem_limit, max_scan_ratio);

    int64_t new_limit = arbitrator->update_mem_bytes(0, 100 * 1024 * 1024);
    ASSERT_EQ(arbitrator->total_mem_bytes.load(), 100 * 1024 * 1024);
    ASSERT_GT(new_limit, 0);

    new_limit = arbitrator->update_mem_bytes(100 * 1024 * 1024, 0);
    ASSERT_EQ(new_limit, 0);
    ASSERT_EQ(arbitrator->total_mem_bytes.load(), 0);
}

TEST_F(ScannerContextTest, scanner_mem_share_arbitrator_proportional_sharing) {
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 2;
    int64_t query_mem_limit = 1024 * 1024 * 1024;
    double max_scan_ratio = 0.5;

    auto arbitrator = MemShareArbitrator::create_shared(query_id, query_mem_limit, max_scan_ratio);

    int64_t limit1 = arbitrator->update_mem_bytes(0, 200 * 1024 * 1024);
    int64_t limit2 = arbitrator->update_mem_bytes(0, 300 * 1024 * 1024);

    ASSERT_LT(limit2, limit1);
    ASSERT_EQ(arbitrator->total_mem_bytes.load(), 500 * 1024 * 1024);
}

TEST_F(ScannerContextTest, scanner_mem_share_arbitrator_zero_ratio) {
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 2;
    int64_t query_mem_limit = 1024 * 1024 * 1024;
    double max_scan_ratio = 0.0;

    auto arbitrator = MemShareArbitrator::create_shared(query_id, query_mem_limit, max_scan_ratio);

    ASSERT_GE(arbitrator->mem_limit, 1);
}

// ==================== MemLimiter Tests ====================
TEST_F(ScannerContextTest, scanner_mem_limiter_basic) {
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 2;
    int64_t parallelism = 4;
    bool serial_scan = false;
    int64_t mem_limit = 512 * 1024 * 1024;

    auto limiter = MemLimiter::create_shared(query_id, parallelism, serial_scan, mem_limit);

    ASSERT_EQ(limiter->get_estimated_block_mem_bytes(), 0);
    ASSERT_EQ(limiter->get_arb_scanner_mem_bytes(), 0);
}

TEST_F(ScannerContextTest, scanner_mem_limiter_reestimated_block_mem_bytes) {
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 2;
    int64_t parallelism = 4;
    bool serial_scan = false;
    int64_t mem_limit = 512 * 1024 * 1024;

    auto limiter = MemLimiter::create_shared(query_id, parallelism, serial_scan, mem_limit);

    limiter->reestimated_block_mem_bytes(100 * 1024 * 1024);
    ASSERT_EQ(limiter->get_estimated_block_mem_bytes(), 100 * 1024 * 1024);

    limiter->reestimated_block_mem_bytes(200 * 1024 * 1024);
    ASSERT_EQ(limiter->get_estimated_block_mem_bytes(), 150 * 1024 * 1024);

    limiter->reestimated_block_mem_bytes(300 * 1024 * 1024);
    ASSERT_EQ(limiter->get_estimated_block_mem_bytes(), 200 * 1024 * 1024);
}

TEST_F(ScannerContextTest, scanner_mem_limiter_reestimated_zero_value) {
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 2;
    int64_t parallelism = 4;
    bool serial_scan = false;
    int64_t mem_limit = 512 * 1024 * 1024;

    auto limiter = MemLimiter::create_shared(query_id, parallelism, serial_scan, mem_limit);

    limiter->reestimated_block_mem_bytes(100 * 1024 * 1024);
    ASSERT_EQ(limiter->get_estimated_block_mem_bytes(), 100 * 1024 * 1024);

    limiter->reestimated_block_mem_bytes(0);
    ASSERT_EQ(limiter->get_estimated_block_mem_bytes(), 100 * 1024 * 1024);
}

TEST_F(ScannerContextTest, scanner_mem_limiter_available_scanner_count) {
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 2;
    int64_t parallelism = 4;
    bool serial_scan = false;
    int64_t mem_limit = 512 * 1024 * 1024;

    auto limiter = MemLimiter::create_shared(query_id, parallelism, serial_scan, mem_limit);

    limiter->update_mem_limit(400 * 1024 * 1024);
    limiter->reestimated_block_mem_bytes(100 * 1024 * 1024);

    int count = limiter->available_scanner_count(0);
    ASSERT_GE(count, 1);
}

TEST_F(ScannerContextTest, scanner_mem_limiter_serial_scan) {
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 2;
    int64_t parallelism = 4;
    bool serial_scan = true;
    int64_t mem_limit = 512 * 1024 * 1024;

    auto limiter = MemLimiter::create_shared(query_id, parallelism, serial_scan, mem_limit);

    limiter->update_mem_limit(400 * 1024 * 1024);
    limiter->reestimated_block_mem_bytes(100 * 1024 * 1024);

    int count = limiter->available_scanner_count(0);
    ASSERT_GE(count, 1);
}

TEST_F(ScannerContextTest, scanner_mem_limiter_update_running_tasks_count) {
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 2;
    int64_t parallelism = 4;
    bool serial_scan = false;
    int64_t mem_limit = 512 * 1024 * 1024;

    auto limiter = MemLimiter::create_shared(query_id, parallelism, serial_scan, mem_limit);

    ASSERT_EQ(limiter->update_running_tasks_count(5), 5);
    ASSERT_EQ(limiter->update_running_tasks_count(-2), 3);
    ASSERT_EQ(limiter->update_running_tasks_count(1), 4);
}

TEST_F(ScannerContextTest, scanner_mem_limiter_update_open_tasks_count) {
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 2;
    int64_t parallelism = 4;
    bool serial_scan = false;
    int64_t mem_limit = 512 * 1024 * 1024;

    auto limiter = MemLimiter::create_shared(query_id, parallelism, serial_scan, mem_limit);

    ASSERT_EQ(limiter->update_open_tasks_count(1), 0);
    ASSERT_EQ(limiter->update_open_tasks_count(1), 1);
    ASSERT_EQ(limiter->update_open_tasks_count(-1), 2);
    ASSERT_EQ(limiter->update_open_tasks_count(-1), 1);
}

TEST_F(ScannerContextTest, scanner_mem_limiter_update_arb_mem_bytes) {
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 2;
    int64_t parallelism = 4;
    bool serial_scan = false;
    int64_t mem_limit = 512 * 1024 * 1024;

    auto limiter = MemLimiter::create_shared(query_id, parallelism, serial_scan, mem_limit);

    limiter->update_arb_mem_bytes(100 * 1024 * 1024);
    ASSERT_EQ(limiter->get_arb_scanner_mem_bytes(), 100 * 1024 * 1024);

    limiter->update_arb_mem_bytes(1024 * 1024 * 1024);
    ASSERT_EQ(limiter->get_arb_scanner_mem_bytes(), mem_limit);
}

TEST_F(ScannerContextTest, scanner_mem_limiter_available_count_distribution) {
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 2;
    int64_t parallelism = 3;
    bool serial_scan = false;
    int64_t mem_limit = 512 * 1024 * 1024;

    auto limiter = MemLimiter::create_shared(query_id, parallelism, serial_scan, mem_limit);

    limiter->update_mem_limit(500 * 1024 * 1024);
    limiter->reestimated_block_mem_bytes(100 * 1024 * 1024);

    int count0 = limiter->available_scanner_count(0);
    int count1 = limiter->available_scanner_count(1);
    int count2 = limiter->available_scanner_count(2);

    ASSERT_GE(count0, 1);
    ASSERT_GE(count1, 1);
    ASSERT_GE(count2, 1);
}

// ==================== ScannerContext with Memory Control Tests ====================
TEST_F(ScannerContextTest, scanner_context_with_adaptive_memory) {
    const int parallel_tasks = 2;
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

    std::list<std::shared_ptr<ScannerDelegate>> scanners;
    for (int i = 0; i < 5; ++i) {
        scanners.push_back(std::make_shared<ScannerDelegate>(scanner));
    }

    TUniqueId query_id = state->get_query_ctx()->query_id();
    int64_t query_mem_limit = 1024 * 1024 * 1024;
    auto arbitrator = MemShareArbitrator::create_shared(query_id, query_mem_limit, 0.3);
    auto limiter = MemLimiter::create_shared(query_id, parallel_tasks, false,
                                             static_cast<int64_t>(query_mem_limit * 0.3));

    std::shared_ptr<ScannerContext> scanner_context = ScannerContext::create_shared(
            state.get(), olap_scan_local_state.get(), output_tuple_desc, false, scanners, limit,
            scan_dependency, &shared_limit, arbitrator, limiter, 0, true, parallel_tasks);

    limiter->update_open_tasks_count(1);
    ASSERT_TRUE(scanner_context->_enable_adaptive_scanners);
    ASSERT_NE(scanner_context->_mem_share_arb, nullptr);
    ASSERT_NE(scanner_context->_scanner_mem_limiter, nullptr);
}

TEST_F(ScannerContextTest, scanner_context_adjust_scan_mem_limit) {
    const int parallel_tasks = 2;
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

    std::list<std::shared_ptr<ScannerDelegate>> scanners;
    for (int i = 0; i < 5; ++i) {
        scanners.push_back(std::make_shared<ScannerDelegate>(scanner));
    }

    TUniqueId query_id = state->get_query_ctx()->query_id();
    int64_t query_mem_limit = 1024 * 1024 * 1024;
    auto arbitrator = MemShareArbitrator::create_shared(query_id, query_mem_limit, 0.3);
    auto limiter = MemLimiter::create_shared(query_id, parallel_tasks, false,
                                             static_cast<int64_t>(query_mem_limit * 0.3));

    std::shared_ptr<ScannerContext> scanner_context = ScannerContext::create_shared(
            state.get(), olap_scan_local_state.get(), output_tuple_desc, false, scanners, limit,
            scan_dependency, &shared_limit, arbitrator, limiter, 0, true, parallel_tasks);

    int64_t old_mem = 100 * 1024 * 1024;
    int64_t new_mem = 200 * 1024 * 1024;
    scanner_context->_adjust_scan_mem_limit(old_mem, new_mem);

    limiter->update_open_tasks_count(1);
    ASSERT_GT(arbitrator->total_mem_bytes.load(), 0);
}

TEST_F(ScannerContextTest, scanner_context_reestimated_block_mem_bytes) {
    const int parallel_tasks = 2;
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

    std::list<std::shared_ptr<ScannerDelegate>> scanners;
    for (int i = 0; i < 5; ++i) {
        scanners.push_back(std::make_shared<ScannerDelegate>(scanner));
    }

    TUniqueId query_id = state->get_query_ctx()->query_id();
    int64_t query_mem_limit = 1024 * 1024 * 1024;
    auto arbitrator = MemShareArbitrator::create_shared(query_id, query_mem_limit, 0.3);
    auto limiter = MemLimiter::create_shared(query_id, parallel_tasks, false,
                                             static_cast<int64_t>(query_mem_limit * 0.3));

    std::shared_ptr<ScannerContext> scanner_context = ScannerContext::create_shared(
            state.get(), olap_scan_local_state.get(), output_tuple_desc, false, scanners, limit,
            scan_dependency, &shared_limit, arbitrator, limiter, 0, true, parallel_tasks);

    scanner_context->reestimated_block_mem_bytes(150 * 1024 * 1024);
    ASSERT_GT(limiter->get_estimated_block_mem_bytes(), 0);
    limiter->update_open_tasks_count(1);
}

TEST_F(ScannerContextTest, scanner_context_update_peak_running_scanner) {
    const int parallel_tasks = 2;
    auto scan_operator = std::make_unique<OlapScanOperatorX>(obj_pool.get(), tnode, 0, *descs,
                                                             parallel_tasks, TQueryCacheParam {});

    auto olap_scan_local_state =
            OlapScanLocalState::create_unique(state.get(), scan_operator.get());
    olap_scan_local_state->_parent = scan_operator.get();

    const int64_t limit = 100;

    OlapScanner::Params scanner_params;
    scanner_params.state = state.get();
    scanner_params.profile = profile.get();
    scanner_params.limit = limit;
    scanner_params.key_ranges = std::vector<OlapScanRange*>();

    std::shared_ptr<Scanner> scanner =
            OlapScanner::create_shared(olap_scan_local_state.get(), std::move(scanner_params));

    std::list<std::shared_ptr<ScannerDelegate>> scanners;
    for (int i = 0; i < 5; ++i) {
        scanners.push_back(std::make_shared<ScannerDelegate>(scanner));
    }

    TUniqueId query_id = state->get_query_ctx()->query_id();
    int64_t query_mem_limit = 1024 * 1024 * 1024;
    auto arbitrator = MemShareArbitrator::create_shared(query_id, query_mem_limit, 0.3);
    auto limiter = MemLimiter::create_shared(query_id, parallel_tasks, false,
                                             static_cast<int64_t>(query_mem_limit * 0.3));

    std::shared_ptr<ScannerContext> scanner_context = ScannerContext::create_shared(
            state.get(), olap_scan_local_state.get(), output_tuple_desc, false, scanners, limit,
            scan_dependency, &shared_limit, arbitrator, limiter, 0, true, parallel_tasks);

    scanner_context->update_peak_running_scanner(3);
    ASSERT_EQ(limiter->update_running_tasks_count(0), 3);
    limiter->update_open_tasks_count(1);
}

} // namespace doris
