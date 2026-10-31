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

#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/QueryCache_types.h>
#include <gtest/gtest.h>

#include "common/object_pool.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "exec/operator/olap_scan_operator.h"
#include "exec/scan/mock_simplified_scan_scheduler.h"
#include "exec/scan/scanner_context.h"
#include "exprs/mock_vexpr.h"
#include "exprs/vexpr_context.h"
#include "runtime/descriptors.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_literal_expr.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"

namespace doris {
class ScanOperatorTest : public testing::Test {
public:
    void SetUp() override {
        obj_pool = std::make_unique<ObjectPool>();
        // This ScanNode has two tuples.
        // First one is input tuple, second one is output tuple.
        tbl_desc.tableType = TTableType::OLAP_TABLE;

        tuple_desc.id = 0;
        tuple_descs.push_back(tuple_desc);
        tuple_desc.id = 1;
        tuple_descs.push_back(tuple_desc);
        thrift_tbl.tableDescriptors.push_back(tbl_desc);
        thrift_tbl.tupleDescriptors = tuple_descs;
        thrift_tbl.slotDescriptors = slot_descs;
        scalar_type.__set_type(TPrimitiveType::STRING);
        std::ignore = DescriptorTbl::create(obj_pool.get(), thrift_tbl, &descs);
    }

private:
    std::unique_ptr<ObjectPool> obj_pool;
    TTupleDescriptor tuple_desc;
    std::vector<TTupleDescriptor> tuple_descs;
    DescriptorTbl* descs = nullptr;
    TTableDescriptor tbl_desc;
    TScalarType scalar_type;
    TDescriptorTable thrift_tbl;
    std::vector<TSlotDescriptor> slot_descs;
    std::unique_ptr<RuntimeState> state = std::make_unique<RuntimeState>();
};

TEST_F(ScanOperatorTest, adaptive_pipeline_task_serial_read_on_limit) {
    const int parallel_pipeline_task_num = 24;
    TPlanNode tnode;
    tnode.row_tuples.push_back(TTupleId(0));
    tnode.row_tuples.push_back(TTupleId(1));

    // Scan with conjuncts
    TExpr conjunct;
    std::vector<TExpr> conjuncts {conjunct};
    tnode.__set_conjuncts(conjuncts);
    auto scan_operator = std::make_unique<OlapScanOperatorX>(
            obj_pool.get(), tnode, 0, *descs, parallel_pipeline_task_num, TQueryCacheParam {});

    TQueryOptions query_options;
    // enable_adaptive_pipeline_task_serial_read_on_limit is true
    query_options.__set_enable_adaptive_pipeline_task_serial_read_on_limit(true);
    state->set_query_options(query_options);

    std::ignore = scan_operator->init(tnode, state.get());
    // With conjuncts, should_run_serial is false
    ASSERT_EQ(scan_operator->_should_run_serial, false);

    // Scan without conjuncts
    conjuncts.clear();
    tnode.__set_conjuncts(conjuncts);
    // limit 10
    tnode.__set_limit(10);
    scan_operator = std::make_unique<OlapScanOperatorX>(
            obj_pool.get(), tnode, 0, *descs, parallel_pipeline_task_num, TQueryCacheParam {});

    // enable_adaptive_pipeline_task_serial_read_on_limit is true
    query_options.__set_enable_adaptive_pipeline_task_serial_read_on_limit(true);
    query_options.__set_adaptive_pipeline_task_serial_read_on_limit(10);
    state->set_query_options(query_options);

    std::ignore = scan_operator->init(tnode, state.get());
    // Without conjuncts, limit 10 <= adaptive_pipeline_task_serial_read_on_limit 10
    ASSERT_EQ(scan_operator->_should_run_serial, true);

    query_options.__set_adaptive_pipeline_task_serial_read_on_limit(9);
    state->set_query_options(query_options);
    std::ignore = scan_operator->init(tnode, state.get());
    // Without conjuncts, limit 10 > adaptive_pipeline_task_serial_read_on_limit 9
    ASSERT_EQ(scan_operator->_should_run_serial, true);

    query_options.__set_enable_adaptive_pipeline_task_serial_read_on_limit(false);
    query_options.__set_adaptive_pipeline_task_serial_read_on_limit(900);
    state->set_query_options(query_options);
    scan_operator->_should_run_serial = false;
    std::ignore = scan_operator->init(tnode, state.get());
    // Without conjuncts, enable_adaptive_pipeline_task_serial_read_on_limit is false
    ASSERT_EQ(scan_operator->_should_run_serial, false);
}

class ScanProjectionTest : public testing::Test {
protected:
    void create_scan(int64_t limit, DataTypePtr value_type = std::make_shared<DataTypeInt32>()) {
        // Release query-local states before replacing their operator and descriptors.
        scanner_context.reset();
        state.reset();
        op.reset();
        state = std::make_unique<MockRuntimeState>();
        descs = std::make_unique<MockDescriptorTbl>(DataTypes {data_type, value_type}, &pool);
        TPlanNode tnode;
        tnode.row_tuples = {0};
        tnode.__set_limit(limit);
        op = std::make_unique<OlapScanOperatorX>(&pool, tnode, 0, *descs, 1, TQueryCacheParam {});
        op->_output_tuple_desc = descs->get_tuple_descriptor(0);
        op->set_projection_for_test(MockRowDescriptor({value_type}, &pool));

        auto local = OlapScanLocalState::create_unique(state.get(), op.get());
        LocalStateInfo info {.parent_profile = &profile,
                             .scan_ranges = {},
                             .shared_state = nullptr,
                             .shared_state_map = {},
                             .task_idx = 0};
        ASSERT_TRUE(local->init(state.get(), info).ok());
        local->_projections = MockSlotRef::create_mock_contexts(1, value_type);
        std::list<std::shared_ptr<ScannerDelegate>> scanners;
        scanner_context = ScannerContext::create_shared(
                state.get(), local.get(), op->_output_tuple_desc, scanners, limit,
                local->_scan_dependency, &op->_shared_scan_limit, nullptr, nullptr, 0, false, 1);
        scanner_context->_scanner_memory_used_counter =
                ADD_COUNTER(&profile, "QueuedBlockBytes", TUnit::BYTES);
        scanner_context->_newly_create_free_blocks_num =
                ADD_COUNTER(&profile, "NewRawBlocks", TUnit::UNIT);
        local->_scanner_ctx.store(scanner_context);
        local_state = local.get();
        state->resize_op_id_to_local_state(-1);
        state->emplace_local_state(op->operator_id(), std::move(local));
    }

    void enqueue(Block input) {
        auto raw_block = scanner_context->get_free_block(true);
        ASSERT_NE(raw_block, nullptr);
        ASSERT_EQ(raw_block->columns(), 2);
        raw_block->swap(input);
        auto task = std::make_shared<ScanTask>(std::weak_ptr<ScannerDelegate> {});
        task->cached_block = std::move(raw_block);
        scanner_context->inc_block_usage(task->cached_block->allocated_bytes());
        ++scanner_context->_in_flight_tasks_num;
        scanner_context->push_completed_scan_task(task);
    }

    Block input_block() {
        auto input = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4});
        input.insert(ColumnHelper::create_column_with_name<DataTypeInt32>({10, 20, 30, 40}));
        return input;
    }

    std::shared_ptr<testing::StrictMock<MockVExpr>> mock_projection() {
        auto expr = std::make_shared<testing::StrictMock<MockVExpr>>();
        expr->data_type() = data_type;
        local_state->_projections = {VExprContext::create_shared(expr)};
        return expr;
    }

    ObjectPool pool;
    RuntimeProfile profile {"scan projection"};
    DataTypePtr data_type = std::make_shared<DataTypeInt32>();
    std::unique_ptr<MockDescriptorTbl> descs;
    std::unique_ptr<OlapScanOperatorX> op;
    std::unique_ptr<MockRuntimeState> state;
    OlapScanLocalState* local_state = nullptr;
    std::shared_ptr<ScannerContext> scanner_context;
};

TEST_F(ScanProjectionTest, projects_raw_queue_block_after_limit) {
    for (int64_t limit : {-1, 2, 0, 5}) {
        SCOPED_TRACE(limit);
        create_scan(limit);
        // The intermediate projection drops the first raw column before the final expression.
        local_state->_intermediate_projections = {MockSlotRef::create_mock_contexts(1, data_type)};
        auto expr = mock_projection();
        const size_t expected_rows = limit < 0 ? 4 : std::min<int64_t>(limit, 4);
        EXPECT_CALL(*expr,
                    execute_column_impl(testing::_, testing::_, testing::_, testing::_, testing::_))
                .Times(expected_rows == 0 ? 0 : 1)
                .WillRepeatedly([&](VExprContext*, const Block* block, const Selector* selector,
                                    size_t count, ColumnPtr& result) {
                    EXPECT_EQ(selector, nullptr);
                    EXPECT_EQ(count, expected_rows);
                    EXPECT_EQ(block->rows(), expected_rows);
                    EXPECT_EQ(block->columns(), 1);
                    result = block->get_by_position(0).column;
                    return Status::OK();
                });
        enqueue(input_block());

        Block output;
        bool eos = false;
        auto status = op->get_block_after_projects(state.get(), &output, &eos);
        ASSERT_TRUE(status.ok()) << status.to_string();
        EXPECT_TRUE(eos);
        EXPECT_EQ(output.rows(), expected_rows);
        EXPECT_EQ(local_state->_num_rows_returned, expected_rows);
        EXPECT_EQ(scanner_context->block_memory_usage(), 0);
        EXPECT_TRUE(scanner_context->_should_stop);
        if (expected_rows > 0) {
            ASSERT_EQ(output.columns(), 1);
            EXPECT_EQ(output.get_by_position(0).column->get_int(0), 10);
            EXPECT_EQ(output.get_by_position(0).column->get_int(expected_rows - 1),
                      expected_rows * 10);
        }
    }
}

TEST_F(ScanProjectionTest, preserves_nulls_after_limit) {
    auto nullable_type = make_nullable(data_type);
    create_scan(3, nullable_type);
    auto input = input_block();
    input.get_by_position(1).type = nullable_type;
    input.get_by_position(1).column =
            ColumnHelper::create_nullable_column<DataTypeInt32>({10, 20, 30, 40}, {0, 1, 0, 1});
    enqueue(std::move(input));

    Block output;
    bool eos = false;
    auto status = op->get_block_after_projects(state.get(), &output, &eos);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(output.rows(), 3);
    const auto& column = output.get_by_position(0).column;
    EXPECT_FALSE(column->is_null_at(0));
    EXPECT_TRUE(column->is_null_at(1));
    EXPECT_FALSE(column->is_null_at(2));
    EXPECT_EQ(column->get_int(2), 30);
    EXPECT_TRUE(eos);
}

TEST_F(ScanProjectionTest, materializes_constant_after_limit) {
    create_scan(2);
    local_state->_projections = {MockLiteral::create_const<DataTypeInt32>(42, 1)};
    enqueue(input_block());

    Block output;
    bool eos = false;
    auto status = op->get_block_after_projects(state.get(), &output, &eos);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(output.rows(), 2);
    EXPECT_FALSE(is_column_const(*output.get_by_position(0).column));
    EXPECT_EQ(output.get_by_position(0).column->get_int(0), 42);
    EXPECT_EQ(output.get_by_position(0).column->get_int(1), 42);
}

TEST_F(ScanProjectionTest, skips_projection_for_empty_block) {
    create_scan(-1);
    auto expr = mock_projection();
    EXPECT_CALL(*expr,
                execute_column_impl(testing::_, testing::_, testing::_, testing::_, testing::_))
            .Times(0);
    auto input = input_block();
    input.clear_column_data();
    enqueue(std::move(input));

    Block output;
    bool eos = false;
    EXPECT_TRUE(op->get_block_after_projects(state.get(), &output, &eos).ok());
    EXPECT_EQ(output.rows(), 0);
    EXPECT_TRUE(eos);
}

TEST_F(ScanProjectionTest, propagates_projection_error) {
    create_scan(-1);
    auto expr = mock_projection();
    EXPECT_CALL(*expr,
                execute_column_impl(testing::_, testing::_, testing::_, testing::_, testing::_))
            .WillOnce(testing::Return(Status::InternalError("scan projection failed")));
    enqueue(input_block());

    Block output;
    bool eos = false;
    auto status = op->get_block_after_projects(state.get(), &output, &eos);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("scan projection failed"), std::string::npos);
    EXPECT_EQ(local_state->_rows_returned_counter->value(), 0);
}

TEST_F(ScanProjectionTest, reuses_raw_blocks_without_recycling_projected_schema) {
    create_scan(-1);
    MockSimplifiedScanScheduler scheduler(nullptr);
    scanner_context->_scanner_scheduler = &scheduler;
    EXPECT_CALL(scheduler, schedule_scan_task(testing::_, testing::_, testing::_))
            .WillOnce(testing::Return(Status::OK()));
    scanner_context->_max_bytes_in_queue = 1024 * 1024;
    enqueue(input_block());
    enqueue(input_block());

    Block output;
    bool eos = false;
    for (int i = 0; i < 2; ++i) {
        output.clear_column_data();
        auto status = op->get_block_after_projects(state.get(), &output, &eos);
        ASSERT_TRUE(status.ok()) << status.to_string();
        ASSERT_EQ(output.columns(), 1);
        ASSERT_EQ(output.rows(), 4);
        EXPECT_EQ(output.get_by_position(0).column->get_int(0), 10);
        EXPECT_EQ(output.get_by_position(0).column->get_int(3), 40);
        EXPECT_EQ(eos, i == 1);
    }
    auto recycled = scanner_context->get_free_block(false);
    ASSERT_NE(recycled, nullptr);
    EXPECT_EQ(recycled->columns(), 2);
    EXPECT_EQ(recycled->rows(), 0);
}

TEST_F(ScanProjectionTest, reserves_memory_for_consumer_projection) {
    create_scan(-1);
    const size_t scan_bytes = config::doris_scanner_row_bytes;
    const size_t projection_bytes =
            std::max<size_t>(scan_bytes, state->minimum_operator_memory_required_bytes()) * 2;
    local_state->estimate_memory_usage() = projection_bytes;
    EXPECT_EQ(op->get_reserve_mem_size(state.get()), projection_bytes);
    // With an opened scan, retain the larger scan estimate without adding it twice.
    local_state->_opened = true;
    local_state->_memory_used_counter->set(projection_bytes * 3);
    EXPECT_EQ(op->get_reserve_mem_size(state.get()), projection_bytes * 3);
    scanner_context->inc_block_usage(projection_bytes * 3);
    EXPECT_EQ(op->get_reserve_mem_size(state.get()), projection_bytes);

    op->reset_reserve_mem_size(state.get());
    EXPECT_EQ(local_state->estimate_memory_usage(), 0);
    op->_projection.reset();
    EXPECT_EQ(op->get_reserve_mem_size(state.get()), scan_bytes);
}
} // namespace doris
