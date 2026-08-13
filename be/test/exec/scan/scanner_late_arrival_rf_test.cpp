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
#include "exec/scan/scanner.h"
#include "exec/scan/scanner_context.h"
#include "exec/scan/scanner_scheduler.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "testutil/column_helper.h"
#include "testutil/desc_tbl_builder.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"
#include "util/raw_value.h"

namespace doris {

// Minimal concrete Scanner so the abstract base can be instantiated. The
// behavior under test (`Scanner::try_append_late_arrival_runtime_filter`) lives
// entirely in the base class and never calls `_get_block_impl`.
class TestScanner final : public Scanner {
public:
    TestScanner(RuntimeState* state, ScanLocalStateBase* local_state, int64_t limit,
                RuntimeProfile* profile, bool allow_padding = true)
            : Scanner(state, local_state, limit, profile), _allow_padding(allow_padding) {}

    void add_block(Block block) { _blocks.push_back(std::move(block)); }

protected:
    bool _can_merge_padding_blocks(const Block& /*left*/, const Block& /*right*/) const override {
        return _allow_padding;
    }

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
    bool _allow_padding = true;
    std::list<Block> _blocks;
};

class LateBucketScanner final : public Scanner {
public:
    LateBucketScanner(RuntimeState* state, OlapScanLocalState* local_state, int64_t tablet_id,
                      bool has_matching_row, RuntimeProfile* profile, std::latch* prepare_started,
                      std::latch* filter_published)
            : Scanner(state, local_state, -1, profile),
              _olap_local_state(local_state),
              _tablet_id(tablet_id),
              _has_matching_row(has_matching_row),
              _prepare_started(prepare_started),
              _filter_published(filter_published) {}

    bool is_pruned_by_runtime_filter() const override {
        return _olap_local_state->_is_tablet_pruned_by_runtime_filter(1, _tablet_id);
    }

    int read_calls() const { return _read_calls.load(); }

protected:
    Status _prepare_impl() override {
        _prepare_started->count_down();
        _filter_published->wait();
        return Scanner::_prepare_impl();
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
    int64_t _tablet_id;
    bool _has_matching_row;
    std::latch* _prepare_started;
    std::latch* _filter_published;
    std::atomic<int> _read_calls {0};
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
        local_state->_rf_bucket_prune_ranges.push_back({100 + bucket_seq, bucket_seq, bucket_num});
    }
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
                state, local_state.get(), 100 + bucket_seq, bucket_seq == selected_bucket,
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

    std::vector<std::thread> probe_threads;
    for (const auto& task : tasks) {
        probe_threads.emplace_back([scanner_context, task] {
            ScannerScheduler::_scanner_scan(scanner_context, task);
        });
    }

    // Every task has passed the scheduler's pre-prepare pruning check while the RF is not ready.
    prepare_started.wait();
    ASSERT_EQ(local_state->_rf_bucket_pruner.pruned_tablet_count(), 0);

    std::shared_ptr<RuntimeFilterProducer> producer;
    ASSERT_TRUE(RuntimeFilterProducer::create(_query_ctx.get(), &desc, &producer).ok());
    ASSERT_TRUE(producer->init(1).ok());
    auto filter_column = ColumnInt32::create();
    filter_column->insert_value(filter_value);
    ASSERT_TRUE(producer->insert(std::move(filter_column), 0).ok());
    producer->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::READY);
    local_state->_helper._consumers[0]->signal(producer.get());
    filter_published.count_down();

    for (auto& thread : probe_threads) {
        thread.join();
    }

    for (int bucket_seq = 0; bucket_seq < bucket_num; ++bucket_seq) {
        if (bucket_seq == selected_bucket) {
            EXPECT_GT(scanners[bucket_seq]->read_calls(), 0);
            EXPECT_NE(tasks[bucket_seq]->cached_block, nullptr);
        } else {
            EXPECT_EQ(scanners[bucket_seq]->read_calls(), 0);
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

TEST(ScannerProjectionTest, merges_padding_block_when_limit_eos_without_extra_flag) {
    ObjectPool pool;
    auto data_type = std::make_shared<DataTypeInt32>();
    auto row_descriptor = MockRowDescriptor({data_type}, &pool);

    MockRuntimeState state;
    state._batch_size = 6;

    auto op = std::make_shared<MockScanOperatorX>();
    op->_row_descriptor = row_descriptor;
    auto& projection = op->set_projection_for_test(
            MockRowDescriptor(std::vector<DataTypePtr> {data_type}, &pool));
    op->_output_tuple_desc = projection.output_row_descriptor.tuple_descriptors()[0];

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
    auto& projection = op->set_projection_for_test(
            MockRowDescriptor(std::vector<DataTypePtr> {data_type}, &pool));
    op->_output_tuple_desc = projection.output_row_descriptor.tuple_descriptors()[0];

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

TEST(ScannerProjectionTest, projects_incompatible_blocks_before_reading_the_next_block) {
    ObjectPool pool;
    auto data_type = std::make_shared<DataTypeInt32>();
    auto row_descriptor = MockRowDescriptor({data_type}, &pool);

    MockRuntimeState state;
    state._batch_size = 8;

    auto op = std::make_shared<MockScanOperatorX>();
    op->_row_descriptor = row_descriptor;
    auto& projection = op->set_projection_for_test(
            MockRowDescriptor(std::vector<DataTypePtr> {data_type}, &pool));
    op->_output_tuple_desc = projection.output_row_descriptor.tuple_descriptors()[0];

    auto local_state = std::make_shared<MockScanLocalState>(&state, op.get());
    local_state->_projections = MockSlotRef::create_mock_contexts(0, data_type);

    RuntimeProfile profile("scanner");
    TestScanner scanner(&state, local_state.get(), -1, &profile, false);
    ASSERT_TRUE(scanner.init(&state, {}).ok());
    scanner.add_block(ColumnHelper::create_block<DataTypeInt32>({0, 1}));
    scanner.add_block(ColumnHelper::create_block<DataTypeInt32>({2, 3, 4}));

    Block first_output;
    bool eos = false;
    ASSERT_TRUE(scanner.get_block_after_projects(&state, &first_output, &eos).ok());
    EXPECT_FALSE(eos);
    EXPECT_EQ(first_output.rows(), 2);

    Block second_output;
    ASSERT_TRUE(scanner.get_block_after_projects(&state, &second_output, &eos).ok());
    EXPECT_FALSE(eos);
    EXPECT_EQ(second_output.rows(), 3);

    Block final_output;
    ASSERT_TRUE(scanner.get_block_after_projects(&state, &final_output, &eos).ok());
    EXPECT_TRUE(eos);
    EXPECT_EQ(final_output.rows(), 0);
}

} // namespace doris
