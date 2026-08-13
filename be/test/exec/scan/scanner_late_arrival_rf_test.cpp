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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <latch>
#include <list>
#include <thread>

#include "common/object_pool.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_number.h"
#include "exec/operator/mock_scan_operator.h"
#include "exec/operator/olap_scan_operator.h"
#include "exec/runtime_filter/runtime_filter_consumer.h"
#include "exec/runtime_filter/runtime_filter_consumer_helper.h"
#include "exec/runtime_filter/runtime_filter_producer.h"
#include "exec/runtime_filter/runtime_filter_test_utils.h"
#include "exec/scan/parallel_scanner_builder.h"
#include "exec/scan/scanner.h"
#include "exec/scan/scanner_context.h"
#include "exec/scan/scanner_scheduler.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "storage/iterator/block_reader.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/tablet/tablet_meta.h"
#include "testutil/column_helper.h"
#include "testutil/desc_tbl_builder.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"
#include "util/defer_op.h"
#include "util/raw_value.h"

namespace doris {

// Minimal concrete Scanner so the abstract base can be instantiated. The
// behavior under test (`Scanner::try_append_late_arrival_runtime_filter`) lives
// entirely in the base class and never calls `_get_block_impl`.
class TestScanner final : public Scanner {
public:
    TestScanner(RuntimeState* state, ScanLocalStateBase* local_state, int64_t limit,
                RuntimeProfile* profile)
            : Scanner(state, local_state, limit, profile) {}

    void add_block(Block block) { _blocks.push_back(std::move(block)); }

protected:
    Status _get_block_impl(RuntimeState* /*state*/, Block* block, bool* eof) override {
        if (_blocks.empty()) {
            *eof = true;
            return Status::OK();
        }
        *eof = false;
        block->swap(_blocks.front());
        _blocks.pop_front();
        return Status::OK();
    }

private:
    std::list<Block> _blocks;
};

class FakeTablet final : public BaseTablet {
public:
    FakeTablet(int64_t partition_id, int64_t tablet_id)
            : BaseTablet(create_meta(partition_id, tablet_id)) {}

    std::string tablet_path() const override { return ""; }

    bool exceed_version_limit(int32_t /*limit*/) override { return false; }

    Result<std::unique_ptr<RowsetWriter>> create_rowset_writer(RowsetWriterContext& /*context*/,
                                                               bool /*vertical*/) override {
        return ResultError(Status::NotSupported("fake tablet"));
    }

    Result<std::unique_ptr<RowsetWriter>> create_transient_rowset_writer(
            const Rowset& /*rowset*/, std::shared_ptr<PartialUpdateInfo> /*partial_update_info*/,
            int64_t /*txn_expiration*/ = 0) override {
        return ResultError(Status::NotSupported("fake tablet"));
    }

    Status capture_rs_readers(const Version& /*spec_version*/,
                              std::vector<RowSetSplits>* /*rs_splits*/,
                              const CaptureRowsetOps& /*opts*/) override {
        return Status::NotSupported("fake tablet");
    }

    Status save_delete_bitmap(const TabletTxnInfo* /*txn_info*/, int64_t /*txn_id*/,
                              DeleteBitmapPtr /*delete_bitmap*/, RowsetWriter* /*rowset_writer*/,
                              const RowsetIdUnorderedSet& /*cur_rowset_ids*/,
                              int64_t /*lock_id*/ = -1,
                              int64_t /*next_visible_version*/ = -1) override {
        return Status::NotSupported("fake tablet");
    }

    CalcDeleteBitmapExecutor* calc_delete_bitmap_executor() override { return nullptr; }

    void clear_cache() override {}

    Versions calc_missed_versions(int64_t /*spec_version*/,
                                  Versions /*existing_versions*/) const override {
        return {};
    }

    size_t tablet_footprint() override { return 0; }

private:
    static TabletMetaSharedPtr create_meta(int64_t partition_id, int64_t tablet_id) {
        auto meta = std::make_shared<TabletMeta>(std::make_shared<TabletSchema>());
        meta->_partition_id = partition_id;
        meta->_tablet_id = tablet_id;
        return meta;
    }
};

class LateBucketScanner final : public Scanner {
public:
    LateBucketScanner(RuntimeState* state, OlapScanLocalState* local_state, int32_t bucket_seq,
                      int32_t bucket_num, bool has_matching_row, RuntimeProfile* profile,
                      std::latch* prepare_started, std::latch* filter_published)
            : Scanner(state, local_state, -1, profile),
              _olap_local_state(local_state),
              _bucket_seq(bucket_seq),
              _bucket_num(bucket_num),
              _has_matching_row(has_matching_row),
              _prepare_started(prepare_started),
              _filter_published(filter_published) {}

    bool is_pruned_by_runtime_filter() const override {
        return _olap_local_state->_is_tablet_pruned_by_runtime_filter(1, _bucket_seq, _bucket_num);
    }

    int read_calls() const { return _read_calls.load(); }
    int open_calls() const { return _open_calls.load(); }
    int prepare_calls() const { return _prepare_calls.load(); }
    int release_calls() const { return _release_calls.load(); }

    void release_unopened_resources() override {
        ++_release_calls;
        Scanner::release_unopened_resources();
    }

protected:
    Status _prepare_impl() override {
        ++_prepare_calls;
        _prepare_started->count_down();
        _filter_published->wait();
        return Scanner::_prepare_impl();
    }

    Status _open_impl(RuntimeState* state) override {
        ++_open_calls;
        return Scanner::_open_impl(state);
    }

    Status _get_block_impl(RuntimeState* /*state*/, Block* block, bool* eof) override {
        ++_read_calls;
        if (_returned) {
            *eof = true;
            return Status::OK();
        }
        _returned = true;
        *eof = false;
        if (_has_matching_row) {
            *block = ColumnHelper::create_block<DataTypeInt32>({7});
        } else {
            *block = ColumnHelper::create_block<DataTypeInt32>(std::vector<int32_t> {});
        }
        return Status::OK();
    }

private:
    OlapScanLocalState* _olap_local_state;
    int32_t _bucket_seq;
    int32_t _bucket_num;
    bool _has_matching_row;
    std::latch* _prepare_started;
    std::latch* _filter_published;
    std::atomic<int> _read_calls {0};
    std::atomic<int> _open_calls {0};
    std::atomic<int> _prepare_calls {0};
    std::atomic<int> _release_calls {0};
    bool _returned = false;
};

class ScannerLateArrivalRfTest : public RuntimeFilterTest {
public:
    void SetUp() override {
        RuntimeFilterTest::SetUp();
        // The runtime filter helper schedules a timer on the global queue when
        // it initialises, so the queue must exist before init() is called.
        ExecEnv::GetInstance()->_init_runtime_filter_timer_queue();
    }
};

// Regression: PR #59786 accidentally removed `_applied_rf_num = arrived_rf_num`
// from `Scanner::try_append_late_arrival_runtime_filter`, leaving the counter
// at 0 forever. Subsequent calls then re-clone conjunct ctxs every time, the
// `ApplyAllRuntimeFilters=True` info-string is never emitted, and the
// `_applied_rf_num <= _total_rf_num` DCHECK becomes dead. This test asserts
// the counter advances after RFs arrive and that the second call short-circuits
// via the fast path at the top of the function.
TEST_F(ScannerLateArrivalRfTest, applied_rf_num_advances_after_late_arrival) {
    std::vector<TRuntimeFilterDesc> rf_descs = {
            TRuntimeFilterDescBuilder().add_planId_to_target_expr(0).build(),
            TRuntimeFilterDescBuilder().add_planId_to_target_expr(0).build()};

    SlotDescriptor slot_desc;
    slot_desc._type = DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, false);
    TupleDescriptor tuple_desc;
    tuple_desc.add_slot(&slot_desc);
    RowDescriptor row_desc;
    _tbl._slot_desc_map[0] = &slot_desc;
    const_cast<std::vector<TupleDescriptor*>&>(row_desc._tuple_desc_map).push_back(&tuple_desc);

    // MockScanOperatorX feeds `_runtime_filter_descs` and `_row_descriptor`
    // into the local state's helper. Access to the protected members is
    // permitted by the build-wide `-fno-access-control` flag.
    auto op = std::make_shared<MockScanOperatorX>();
    op->_runtime_filter_descs = rf_descs;
    op->_row_descriptor = row_desc;

    auto local_state = std::make_shared<MockScanLocalState>(_runtime_states[0].get(), op.get());

    std::vector<std::shared_ptr<Dependency>> rf_dependencies;
    ASSERT_TRUE(local_state->_helper.init(_runtime_states[0].get(), true, 0, 0, rf_dependencies, "")
                        .ok());

    auto scanner = std::make_unique<TestScanner>(_runtime_states[0].get(), local_state.get(),
                                                 -1 /*limit*/, &_profile);
    ASSERT_TRUE(scanner->init(_runtime_states[0].get(), {}).ok());
    ASSERT_EQ(scanner->_total_rf_num, 2);
    ASSERT_EQ(scanner->_applied_rf_num, 0);

    std::shared_ptr<RuntimeFilterProducer> producer;
    ASSERT_TRUE(RuntimeFilterProducer::create(_query_ctx.get(), rf_descs.data(), &producer).ok());
    producer->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::READY);
    local_state->_helper._consumers[0]->signal(producer.get());
    local_state->_helper._consumers[1]->signal(producer.get());

    // First call after both RFs arrived: counter must advance to total. Before
    // the fix this stayed at 0 because the assignment was missing.
    ASSERT_TRUE(scanner->try_append_late_arrival_runtime_filter().ok());
    ASSERT_EQ(scanner->_applied_rf_num, 2);

    // Second call: must hit the fast-path early return without re-cloning.
    // We clear `_conjuncts` and verify the function does NOT repopulate them;
    // if `_applied_rf_num` were still 0 the function would call
    // `clone_conjunct_ctxs` and overwrite the cleared vector.
    scanner->_conjuncts.clear();
    ASSERT_TRUE(scanner->try_append_late_arrival_runtime_filter().ok());
    ASSERT_EQ(scanner->_applied_rf_num, 2);
    ASSERT_TRUE(scanner->_conjuncts.empty());
}

TEST_F(ScannerLateArrivalRfTest, bucket_pruning_after_probe_tasks_start) {
    constexpr int scan_node_id = 0;
    constexpr int bucket_num = 4;
    constexpr int filter_value = 7;

    auto desc = TRuntimeFilterDescBuilder().add_planId_to_target_expr(scan_node_id).build();
    desc.__set_bucket_pruning_target_ids({scan_node_id});

    ObjectPool pool;
    DescriptorTblBuilder desc_builder(&pool);
    desc_builder.declare_tuple() << TupleDescBuilder::SlotType {std::make_shared<DataTypeInt32>(),
                                                                "dist_col"};
    DescriptorTbl* desc_tbl = desc_builder.build();
    ASSERT_NE(desc_tbl, nullptr);

    TOlapScanNode olap_scan_node;
    olap_scan_node.__set_tuple_id(0);
    olap_scan_node.__set_keyType(TKeysType::DUP_KEYS);
    olap_scan_node.__set_key_column_name({"dist_col"});
    olap_scan_node.__set_key_column_type({TPrimitiveType::INT});

    TPlanNode plan_node;
    plan_node.__set_node_id(scan_node_id);
    plan_node.__set_node_type(TPlanNodeType::OLAP_SCAN_NODE);
    plan_node.__set_num_children(0);
    plan_node.__set_limit(-1);
    plan_node.__set_row_tuples({0});
    plan_node.__set_runtime_filters({desc});
    plan_node.__set_olap_scan_node(olap_scan_node);

    auto op = std::make_shared<OlapScanOperatorX>(&pool, plan_node, 0, *desc_tbl, bucket_num,
                                                  TQueryCacheParam {});
    auto* state = _runtime_states[0].get();
    state->set_desc_tbl(desc_tbl);
    TQueryOptions query_options =
            TQueryOptionsBuilder().set_runtime_filter_max_in_num(1024).build();
    query_options.__set_enable_runtime_filter_bucket_prune(true);
    state->set_query_options(query_options);

    auto local_state = OlapScanLocalState::create_shared(state, op.get());
    std::vector<std::shared_ptr<Dependency>> rf_dependencies;
    ASSERT_TRUE(local_state->_helper.init(state, true, 0, 0, rf_dependencies, "").ok());
    ASSERT_TRUE(
            local_state->_helper
                    .acquire_runtime_filter(state, local_state->_conjuncts, op->row_descriptor())
                    .ok());
    ASSERT_TRUE(local_state->_conjuncts.empty());
    auto task_exec_ctx = std::make_shared<TaskExecutionContext>();
    state->set_task_execution_context(task_exec_ctx);
    for (int bucket_seq = 0; bucket_seq < bucket_num; ++bucket_seq) {
        auto scan_range = std::make_unique<TPaloScanRange>();
        scan_range->__set_tablet_id(100 + bucket_seq);
        scan_range->__set_bucket_seq(bucket_seq);
        scan_range->__set_bucket_num(bucket_num);
        local_state->_scan_ranges.push_back(std::move(scan_range));
    }
    local_state->_has_rf_bucket_prune_metadata = true;
    RuntimeProfile scan_profile("late bucket scan");
    local_state->_buckets_pruned_by_rf_counter =
            ADD_COUNTER(&scan_profile, "BucketsPrunedByRuntimeFilter", TUnit::UNIT);
    local_state->_scan_timer = ADD_TIMER(&scan_profile, "ScannerGetBlockTime");
    local_state->_scan_cpu_timer = ADD_TIMER(&scan_profile, "ScannerCpuTime");
    local_state->_filter_timer = ADD_TIMER(&scan_profile, "ScannerFilterTime");
    local_state->_rows_read_counter = ADD_COUNTER(&scan_profile, "RowsRead", TUnit::UNIT);

    uint32_t hash = RawValue::zlib_crc32(&filter_value, sizeof(filter_value), TYPE_INT, 0);
    int selected_bucket = static_cast<int>(hash % bucket_num);
    std::latch prepare_started(bucket_num);
    std::latch filter_published(1);
    std::list<std::shared_ptr<ScannerDelegate>> scanner_delegates;
    std::vector<std::shared_ptr<LateBucketScanner>> scanners;
    for (int bucket_seq = 0; bucket_seq < bucket_num; ++bucket_seq) {
        auto scanner = std::make_shared<LateBucketScanner>(
                state, local_state.get(), bucket_seq, bucket_num, bucket_seq == selected_bucket,
                &scan_profile, &prepare_started, &filter_published);
        ASSERT_TRUE(scanner->init(state, {}).ok());
        scanners.push_back(scanner);
        ScannerSPtr scanner_base = scanner;
        scanner_delegates.push_back(std::make_shared<ScannerDelegate>(scanner_base));
    }

    auto dependency = Dependency::create_shared(0, 0, "late bucket scan dependency");
    std::atomic<int64_t> shared_limit {-1};
    auto scanner_context = ScannerContext::create_shared(
            state, local_state.get(), desc_tbl->get_tuple_descriptor(0), nullptr, scanner_delegates,
            -1, dependency, &shared_limit, nullptr, nullptr, 0, false, bucket_num);
    scanner_context->_newly_create_free_blocks_num =
            ADD_COUNTER(&scan_profile, "NewlyCreatedFreeBlocks", TUnit::UNIT);
    scanner_context->_scanner_memory_used_counter =
            ADD_COUNTER(&scan_profile, "ScannerMemoryUsed", TUnit::BYTES);
    scanner_context->_max_bytes_in_queue = 10 * 1024 * 1024;
    std::vector<std::shared_ptr<ScanTask>> tasks;
    for (const auto& scanner_delegate : scanner_delegates) {
        auto task = std::make_shared<ScanTask>(scanner_delegate);
        task->set_state(ScanTask::State::IN_FLIGHT);
        tasks.push_back(std::move(task));
    }
    scanner_context->_in_flight_tasks_num = bucket_num;

    std::shared_ptr<RuntimeFilterProducer> producer;
    ASSERT_TRUE(RuntimeFilterProducer::create(_query_ctx.get(), &desc, &producer).ok());
    ASSERT_TRUE(producer->init(1).ok());
    auto filter_column = ColumnInt32::create();
    filter_column->insert_value(filter_value);
    ASSERT_TRUE(producer->insert(std::move(filter_column), 0).ok());
    producer->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::READY);

    std::vector<std::thread> probe_threads;
    bool filter_released = false;
    Defer probe_thread_guard {[&] {
        if (!filter_released) {
            filter_published.count_down();
        }
        for (auto& thread : probe_threads) {
            if (thread.joinable()) {
                thread.join();
            }
        }
    }};
    for (const auto& task : tasks) {
        probe_threads.emplace_back([scanner_context, task] {
            ScannerScheduler::_scanner_scan(scanner_context, task);
        });
    }

    // Every task has passed the scheduler's pre-prepare pruning check while the RF is not ready.
    prepare_started.wait();
    EXPECT_EQ(local_state->_rf_bucket_pruner.pruned_tablet_count(), 0);

    local_state->_helper._consumers[0]->signal(producer.get());
    filter_published.count_down();
    filter_released = true;

    for (auto& thread : probe_threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }

    for (int bucket_seq = 0; bucket_seq < bucket_num; ++bucket_seq) {
        if (bucket_seq == selected_bucket) {
            EXPECT_GT(scanners[bucket_seq]->open_calls(), 0);
            EXPECT_GT(scanners[bucket_seq]->read_calls(), 0);
            EXPECT_EQ(scanners[bucket_seq]->release_calls(), 0);
            EXPECT_NE(tasks[bucket_seq]->cached_block, nullptr);
        } else {
            EXPECT_EQ(scanners[bucket_seq]->open_calls(), 0);
            EXPECT_EQ(scanners[bucket_seq]->read_calls(), 0);
            EXPECT_EQ(scanners[bucket_seq]->release_calls(), 1);
        }
    }
    int64_t result_rows = 0;
    for (const auto& task : tasks) {
        if (task->cached_block != nullptr) {
            result_rows += task->cached_block->rows();
        }
    }
    ASSERT_EQ(result_rows, 1);
    ASSERT_GT(local_state->_buckets_pruned_by_rf_counter->value(), 0);
    ASSERT_GT(local_state->_rf_bucket_pruner.pruned_tablet_count(), 0);
    ASSERT_LT(local_state->_rf_bucket_pruner.pruned_tablet_count(), bucket_num);
}

TEST_F(ScannerLateArrivalRfTest, bounded_concurrency_prunes_scanner_before_first_schedule) {
    constexpr int scan_node_id = 0;
    constexpr int32_t bucket_num = 2;
    constexpr int32_t active_bucket = 0;
    constexpr int32_t pending_bucket = 1;

    ObjectPool pool;
    DescriptorTblBuilder desc_builder(&pool);
    desc_builder.declare_tuple() << TupleDescBuilder::SlotType {std::make_shared<DataTypeInt32>(),
                                                                "dist_col"};
    DescriptorTbl* desc_tbl = desc_builder.build();
    ASSERT_NE(desc_tbl, nullptr);

    TOlapScanNode olap_scan_node;
    olap_scan_node.__set_tuple_id(0);
    olap_scan_node.__set_keyType(TKeysType::DUP_KEYS);
    TPlanNode plan_node;
    plan_node.__set_node_id(scan_node_id);
    plan_node.__set_node_type(TPlanNodeType::OLAP_SCAN_NODE);
    plan_node.__set_num_children(0);
    plan_node.__set_limit(-1);
    plan_node.__set_row_tuples({0});
    plan_node.__set_olap_scan_node(olap_scan_node);

    auto op = std::make_shared<OlapScanOperatorX>(&pool, plan_node, 0, *desc_tbl, bucket_num,
                                                  TQueryCacheParam {});
    auto* state = _runtime_states[0].get();
    state->set_desc_tbl(desc_tbl);
    auto task_exec_ctx = std::make_shared<TaskExecutionContext>();
    state->set_task_execution_context(task_exec_ctx);
    auto local_state = OlapScanLocalState::create_shared(state, op.get());
    local_state->_has_rf_bucket_prune_metadata = true;

    RuntimeProfile scan_profile("bounded late bucket scan");
    local_state->_scan_timer = ADD_TIMER(&scan_profile, "ScannerGetBlockTime");
    local_state->_scan_cpu_timer = ADD_TIMER(&scan_profile, "ScannerCpuTime");
    local_state->_filter_timer = ADD_TIMER(&scan_profile, "ScannerFilterTime");
    local_state->_rows_read_counter = ADD_COUNTER(&scan_profile, "RowsRead", TUnit::UNIT);

    std::latch prepare_started(2);
    std::latch filter_published(0);
    auto active_scanner = std::make_shared<LateBucketScanner>(
            state, local_state.get(), active_bucket, bucket_num, true, &scan_profile,
            &prepare_started, &filter_published);
    auto pending_scanner = std::make_shared<LateBucketScanner>(
            state, local_state.get(), pending_bucket, bucket_num, false, &scan_profile,
            &prepare_started, &filter_published);
    ASSERT_TRUE(active_scanner->init(state, {}).ok());
    ASSERT_TRUE(pending_scanner->init(state, {}).ok());

    ScannerSPtr pending_scanner_base = pending_scanner;
    ScannerSPtr active_scanner_base = active_scanner;
    std::list<std::shared_ptr<ScannerDelegate>> scanner_delegates;
    scanner_delegates.push_back(std::make_shared<ScannerDelegate>(pending_scanner_base));
    scanner_delegates.push_back(std::make_shared<ScannerDelegate>(active_scanner_base));

    auto dependency = Dependency::create_shared(0, 0, "bounded late bucket scan dependency");
    std::atomic<int64_t> shared_limit {-1};
    auto scanner_context = ScannerContext::create_shared(
            state, local_state.get(), desc_tbl->get_tuple_descriptor(0), nullptr, scanner_delegates,
            -1, dependency, &shared_limit, nullptr, nullptr, 0, false, 1);
    scanner_context->_newly_create_free_blocks_num =
            ADD_COUNTER(&scan_profile, "NewlyCreatedFreeBlocks", TUnit::UNIT);
    scanner_context->_scanner_memory_used_counter =
            ADD_COUNTER(&scan_profile, "ScannerMemoryUsed", TUnit::BYTES);
    scanner_context->_max_bytes_in_queue = 10 * 1024 * 1024;

    auto active_task = scanner_context->_pull_next_scan_task(nullptr, 0);
    ASSERT_NE(active_task, nullptr);
    ASSERT_EQ(active_task->scanner.lock()->_scanner, active_scanner);
    EXPECT_EQ(scanner_context->_pull_next_scan_task(nullptr, 1), nullptr);

    active_task->set_state(ScanTask::State::IN_FLIGHT);
    scanner_context->_in_flight_tasks_num = 1;
    ScannerScheduler::_scanner_scan(scanner_context, active_task);
    ASSERT_EQ(active_scanner->prepare_calls(), 1);
    ASSERT_EQ(active_scanner->open_calls(), 1);
    ASSERT_EQ(scanner_context->_in_flight_tasks_num, 0);
    ASSERT_EQ(scanner_context->_completed_tasks.size(), 1);
    scanner_context->_completed_tasks.clear();

    // Model the active scanner applying a newly published filter before the pending scanner gets
    // its sole concurrency slot, so the shared pruner already excludes the pending bucket.
    local_state->_rf_bucket_pruner._selected_buckets_by_num[bucket_num] = {active_bucket};

    auto pending_task = scanner_context->_pull_next_scan_task(nullptr, 0);
    ASSERT_NE(pending_task, nullptr);
    ASSERT_EQ(pending_task->scanner.lock()->_scanner, pending_scanner);
    pending_task->set_state(ScanTask::State::IN_FLIGHT);
    scanner_context->_in_flight_tasks_num = 1;
    ScannerScheduler::_scanner_scan(scanner_context, pending_task);

    EXPECT_EQ(pending_scanner->prepare_calls(), 0);
    EXPECT_EQ(pending_scanner->open_calls(), 0);
    EXPECT_EQ(pending_scanner->read_calls(), 0);
    EXPECT_EQ(pending_scanner->release_calls(), 1);
    EXPECT_TRUE(pending_task->is_eos());
}

TEST_F(ScannerLateArrivalRfTest, parallel_scanner_factory_preserves_bucket_identity) {
    constexpr int scan_node_id = 0;
    constexpr int32_t bucket_seq = 3;
    constexpr int32_t bucket_num = 8;
    constexpr int64_t partition_id = 10;
    constexpr int64_t tablet_id = 20;

    ObjectPool pool;
    DescriptorTblBuilder desc_builder(&pool);
    desc_builder.declare_tuple() << TupleDescBuilder::SlotType {std::make_shared<DataTypeInt32>(),
                                                                "dist_col"};
    DescriptorTbl* desc_tbl = desc_builder.build();
    ASSERT_NE(desc_tbl, nullptr);

    TOlapScanNode olap_scan_node;
    olap_scan_node.__set_tuple_id(0);
    olap_scan_node.__set_keyType(TKeysType::DUP_KEYS);
    TPlanNode plan_node;
    plan_node.__set_node_id(scan_node_id);
    plan_node.__set_node_type(TPlanNodeType::OLAP_SCAN_NODE);
    plan_node.__set_num_children(0);
    plan_node.__set_limit(-1);
    plan_node.__set_row_tuples({0});
    plan_node.__set_olap_scan_node(olap_scan_node);

    auto op = std::make_shared<OlapScanOperatorX>(&pool, plan_node, 0, *desc_tbl, bucket_num,
                                                  TQueryCacheParam {});
    auto* state = _runtime_states[0].get();
    state->set_desc_tbl(desc_tbl);
    auto local_state = OlapScanLocalState::create_shared(state, op.get());
    local_state->_has_rf_bucket_prune_metadata = true;
    local_state->_rf_bucket_pruner._selected_buckets_by_num[bucket_num] = {bucket_seq - 1};

    auto tablet = std::make_shared<FakeTablet>(partition_id, tablet_id);
    std::vector<TabletWithVersion> tablets {{tablet, 1}};
    std::vector<TabletReadSource> read_sources(1);
    std::vector<std::unique_ptr<TPaloScanRange>> scan_ranges;
    auto scan_range = std::make_unique<TPaloScanRange>();
    scan_range->__set_tablet_id(tablet_id);
    scan_range->__set_bucket_seq(bucket_seq);
    scan_range->__set_bucket_num(bucket_num);
    scan_ranges.push_back(std::move(scan_range));
    auto profile = std::make_shared<RuntimeProfile>("parallel scanner bucket identity");
    ParallelScannerBuilder builder(local_state.get(), tablets, read_sources, scan_ranges, profile,
                                   {}, state, -1, true, true);

    auto scanner = builder._build_scanner(tablet, 1, {}, *scan_ranges.front(), TabletReadSource {},
                                          io::FileCacheStatistics {});
    EXPECT_EQ(scanner->_bucket_seq, bucket_seq);
    EXPECT_EQ(scanner->_bucket_num, bucket_num);
    EXPECT_TRUE(scanner->is_pruned_by_runtime_filter());
}

TEST_F(ScannerLateArrivalRfTest, high_cardinality_ineligible_parallel_builder_reuses_scan_ranges) {
    constexpr size_t scan_range_count = 20'000;
    constexpr int64_t first_tablet_id = 100;

    std::vector<TabletWithVersion> tablets;
    std::vector<TabletReadSource> read_sources(scan_range_count);
    std::vector<std::unique_ptr<TPaloScanRange>> scan_ranges;
    tablets.reserve(scan_range_count);
    scan_ranges.reserve(scan_range_count);
    for (size_t i = 0; i < scan_range_count; ++i) {
        int64_t tablet_id = first_tablet_id + static_cast<int64_t>(i);
        tablets.push_back({std::make_shared<FakeTablet>(1, tablet_id), 1});
        auto scan_range = std::make_unique<TPaloScanRange>();
        scan_range->__set_tablet_id(tablet_id);
        scan_ranges.push_back(std::move(scan_range));
    }
    std::vector<OlapScanRange*> key_ranges;
    std::shared_ptr<RuntimeProfile> profile;

    ParallelScannerBuilder builder(nullptr, tablets, read_sources, scan_ranges, profile, key_ranges,
                                   nullptr, -1, true, true);

    EXPECT_EQ(&builder._scan_ranges, &scan_ranges);
    EXPECT_EQ(builder._scan_ranges.size(), scan_range_count);
    EXPECT_FALSE(builder._scan_ranges.front()->__isset.bucket_seq);
    EXPECT_FALSE(builder._scan_ranges.front()->__isset.bucket_num);
}

TEST_F(ScannerLateArrivalRfTest, olap_scanner_releases_resources_before_open) {
    constexpr int scan_node_id = 0;
    constexpr int64_t partition_id = 10;
    constexpr int64_t tablet_id = 20;

    ObjectPool pool;
    DescriptorTblBuilder desc_builder(&pool);
    desc_builder.declare_tuple() << TupleDescBuilder::SlotType {std::make_shared<DataTypeInt32>(),
                                                                "dist_col"};
    DescriptorTbl* desc_tbl = desc_builder.build();
    ASSERT_NE(desc_tbl, nullptr);

    TOlapScanNode olap_scan_node;
    olap_scan_node.__set_tuple_id(0);
    olap_scan_node.__set_keyType(TKeysType::DUP_KEYS);
    TPlanNode plan_node;
    plan_node.__set_node_id(scan_node_id);
    plan_node.__set_node_type(TPlanNodeType::OLAP_SCAN_NODE);
    plan_node.__set_num_children(0);
    plan_node.__set_limit(-1);
    plan_node.__set_row_tuples({0});
    plan_node.__set_olap_scan_node(olap_scan_node);

    auto op = std::make_shared<OlapScanOperatorX>(&pool, plan_node, 0, *desc_tbl, 1,
                                                  TQueryCacheParam {});
    auto* state = _runtime_states[0].get();
    state->set_desc_tbl(desc_tbl);
    auto local_state = OlapScanLocalState::create_shared(state, op.get());
    auto unprepared_tablet = std::make_shared<FakeTablet>(partition_id, tablet_id);
    auto delete_predicate = std::make_shared<RowsetMeta>();
    std::weak_ptr<BaseTablet> unprepared_tablet_ref = unprepared_tablet;
    std::weak_ptr<RowsetMeta> delete_predicate_ref = delete_predicate;
    RuntimeProfile profile("unopened scanner cleanup");
    OlapScanner::Params params;
    params.state = state;
    params.profile = &profile;
    params.tablet = unprepared_tablet;
    params.version = 1;
    params.read_source.rs_splits.emplace_back();
    params.read_source.delete_predicates.push_back(delete_predicate);
    params.limit = -1;
    params.aggregation = true;
    auto unprepared_scanner = OlapScanner::create_shared(local_state.get(), std::move(params));
    unprepared_tablet.reset();
    delete_predicate.reset();

    ASSERT_FALSE(unprepared_scanner->has_prepared());
    ASSERT_FALSE(unprepared_scanner->is_open());
    ASSERT_FALSE(unprepared_tablet_ref.expired());
    ASSERT_FALSE(delete_predicate_ref.expired());
    ASSERT_FALSE(unprepared_scanner->_tablet_reader_params.rs_splits.empty());
    ASSERT_FALSE(unprepared_scanner->_tablet_reader_params.delete_predicates.empty());

    unprepared_scanner->release_unopened_resources();

    EXPECT_FALSE(unprepared_scanner->has_prepared());
    EXPECT_FALSE(unprepared_scanner->is_open());
    EXPECT_TRUE(unprepared_tablet_ref.expired());
    EXPECT_TRUE(delete_predicate_ref.expired());
    EXPECT_EQ(unprepared_scanner->_tablet_reader_params.tablet, nullptr);
    EXPECT_TRUE(unprepared_scanner->_tablet_reader_params.rs_splits.empty());
    EXPECT_TRUE(unprepared_scanner->_tablet_reader_params.delete_predicates.empty());

    auto prepared_tablet = std::make_shared<FakeTablet>(partition_id, tablet_id + 1);
    OlapScanner::Params prepared_params;
    prepared_params.state = state;
    prepared_params.profile = &profile;
    prepared_params.tablet = prepared_tablet;
    prepared_params.version = 1;
    prepared_params.limit = -1;
    prepared_params.aggregation = true;
    auto prepared_scanner =
            OlapScanner::create_shared(local_state.get(), std::move(prepared_params));
    ASSERT_TRUE(prepared_scanner->Scanner::_prepare_impl().ok());
    prepared_scanner->_tablet_reader = std::make_unique<BlockReader>();
    prepared_scanner->_tablet_reader_params.rs_splits.emplace_back();

    ASSERT_TRUE(prepared_scanner->has_prepared());
    ASSERT_FALSE(prepared_scanner->is_open());
    ASSERT_NE(prepared_scanner->_tablet_reader, nullptr);
    ASSERT_FALSE(prepared_scanner->_tablet_reader_params.rs_splits.empty());

    prepared_scanner->release_unopened_resources();

    EXPECT_FALSE(prepared_scanner->has_prepared());
    EXPECT_FALSE(prepared_scanner->is_open());
    EXPECT_EQ(prepared_scanner->_tablet_reader, nullptr);
    EXPECT_TRUE(prepared_scanner->_tablet_reader_params.rs_splits.empty());
    EXPECT_EQ(prepared_scanner->_tablet_reader_params.tablet, nullptr);
    prepared_scanner->update_realtime_counters();
}

TEST(ScannerProjectionTest, merges_padding_block_when_limit_eos_without_extra_flag) {
    ObjectPool pool;
    auto data_type = std::make_shared<DataTypeInt32>();
    auto row_descriptor = MockRowDescriptor({data_type}, &pool);

    MockRuntimeState state;
    state._batch_size = 6;

    auto op = std::make_shared<MockScanOperatorX>();
    op->_row_descriptor = row_descriptor;
    op->_output_row_descriptor =
            std::make_unique<MockRowDescriptor>(std::vector<DataTypePtr> {data_type}, &pool);
    op->_output_tuple_desc = op->_output_row_descriptor->tuple_descriptors()[0];

    auto local_state = std::make_shared<MockScanLocalState>(&state, op.get());
    local_state->_projections = MockSlotRef::create_mock_contexts(0, data_type);

    RuntimeProfile profile("scanner");
    TestScanner scanner(&state, local_state.get(), 7, &profile);
    ASSERT_TRUE(scanner.init(&state, {}).ok());
    scanner.add_block(ColumnHelper::create_block<DataTypeInt32>({0, 1}));
    scanner.add_block(ColumnHelper::create_block<DataTypeInt32>({2, 3, 4, 5, 6}));

    Block first_output;
    bool eos = false;
    ASSERT_TRUE(scanner.get_block_after_projects(&state, &first_output, &eos).ok());
    EXPECT_TRUE(eos);
    EXPECT_EQ(first_output.rows(), 7);
}

TEST(ScannerProjectionTest, publishes_shared_column_and_reuses_output_block) {
    ObjectPool pool;
    auto data_type = std::make_shared<DataTypeInt32>();
    auto row_descriptor = MockRowDescriptor({data_type}, &pool);

    MockRuntimeState state;
    state._batch_size = 4;

    auto op = std::make_shared<MockScanOperatorX>();
    op->_row_descriptor = row_descriptor;
    op->_output_row_descriptor =
            std::make_unique<MockRowDescriptor>(std::vector<DataTypePtr> {data_type}, &pool);
    op->_output_tuple_desc = op->_output_row_descriptor->tuple_descriptors()[0];

    auto local_state = std::make_shared<MockScanLocalState>(&state, op.get());
    local_state->_projections = MockSlotRef::create_mock_contexts(0, data_type);

    RuntimeProfile profile("scanner");
    TestScanner scanner(&state, local_state.get(), -1, &profile);
    ASSERT_TRUE(scanner.init(&state, {}).ok());

    Block first_input = ColumnHelper::create_block<DataTypeInt32>({1, 2});
    const auto* first_column = first_input.get_by_position(0).column.get();
    scanner.add_block(std::move(first_input));

    Block second_input = ColumnHelper::create_block<DataTypeInt32>({3, 4});
    const auto* second_column = second_input.get_by_position(0).column.get();
    scanner.add_block(std::move(second_input));

    Block output;
    bool eos = false;
    ASSERT_TRUE(scanner.get_block_after_projects(&state, &output, &eos).ok());
    EXPECT_FALSE(eos);
    EXPECT_EQ(output.get_by_position(0).column.get(), first_column);
    EXPECT_EQ(output.get_by_position(0).column->get_int(0), 1);
    EXPECT_EQ(output.get_by_position(0).column->get_int(1), 2);

    output.clear_column_data();
    ASSERT_TRUE(scanner.get_block_after_projects(&state, &output, &eos).ok());
    EXPECT_FALSE(eos);
    EXPECT_EQ(output.get_by_position(0).column.get(), second_column);
    EXPECT_EQ(output.get_by_position(0).column->get_int(0), 3);
    EXPECT_EQ(output.get_by_position(0).column->get_int(1), 4);
}

} // namespace doris
