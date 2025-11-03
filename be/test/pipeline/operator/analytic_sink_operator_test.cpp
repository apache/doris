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

#include "pipeline/exec/analytic_sink_operator.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>

#include "pipeline/exec/analytic_source_operator.h"
#include "pipeline/exec/repeat_operator.h"
#include "pipeline/exec/sort_source_operator.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_agg_fn_evaluator.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type.h"
namespace doris::pipeline {

using namespace vectorized;

class MockAnalyticSinkOperator : public OperatorXBase {
public:
    Status get_block_after_projects(RuntimeState* state, vectorized::Block* block,
                                    bool* eos) override {
        return Status::OK();
    }

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos) override {
        return Status::OK();
    }
    Status setup_local_state(RuntimeState* state, LocalStateInfo& info) override {
        return Status::OK();
    }

    const RowDescriptor& row_desc() const override { return *_mock_row_desc; }

private:
    std::unique_ptr<MockRowDescriptor> _mock_row_desc;
};

struct AnalyticSinkOperatorTest : public ::testing::Test {
    void Initialize(int batch_size) {
        sink = std::make_unique<AnalyticSinkOperatorX>(&pool);
        source = std::make_unique<AnalyticSourceOperatorX>();
        state = std::make_shared<MockRuntimeState>();
        state->batsh_size = batch_size;
        std::cout << "AnalyticSinkOperatorTest::SetUp() batch_size: " << batch_size << std::endl;
        _child_op = std::make_unique<MockAnalyticSinkOperator>();
        for (int i = 0; i < batch_size; i++) {
            _data_vals.push_back(i);
        }
    }

    void create_operator(bool has_window, size_t agg_functions_size, std::string function_name,
                         DataTypes args_types) {
        sink->_agg_functions_size = agg_functions_size;
        sink->_has_window = has_window;
        if (agg_functions_size == 1) {
            create_agg_function(agg_functions_size, function_name, args_types);
        }
        EXPECT_TRUE(sink->set_child(_child_op));
    }

    void create_window_type(bool has_window_start, bool has_window_end,
                            TAnalyticWindow temp_window) {
        sink->_window = temp_window;
        sink->_has_window_start = has_window_start;
        sink->_has_window_end = has_window_end;
    }

    void create_agg_function(size_t agg_functions_size, std::string function_name,
                             DataTypes args_types) {
        sink->_agg_functions.resize(agg_functions_size);
        sink->_num_agg_input.resize(agg_functions_size);
        sink->_offsets_of_aggregate_states.resize(agg_functions_size);
        sink->_change_to_nullable_flags.resize(agg_functions_size);
        sink->_agg_functions[0] = create_agg_fn(pool, function_name, args_types, false, true);
        sink->_num_agg_input[0] = 1;
        sink->_offsets_of_aggregate_states[0] = 0;
        sink->_total_size_of_aggregate_states = 100;
    }

    void create_local_state() {
        shared_state = sink->create_shared_state();
        {
            sink_local_state_uptr = AnalyticSinkLocalState ::create_unique(sink.get(), state.get());
            sink_local_state = sink_local_state_uptr.get();
            LocalSinkStateInfo info {.task_idx = 0,
                                     .parent_profile = &profile,
                                     .sender_id = 0,
                                     .shared_state = shared_state.get(),
                                     .shared_state_map = {},
                                     .tsink = TDataSink {}};
            sink_local_state_uptr->_fn_place_ptr = buffer;
            sink_local_state_uptr->_agg_input_columns.resize(1);
            sink_local_state_uptr->_agg_input_columns[0].resize(1);
            EXPECT_TRUE(sink_local_state_uptr->init(state.get(), info).ok());
            state->emplace_sink_local_state(0, std::move(sink_local_state_uptr));
        }

        {
            source_local_state_uptr = AnalyticLocalState::create_unique(state.get(), source.get());
            source_local_state = source_local_state_uptr.get();
            LocalStateInfo info {.parent_profile = &profile,
                                 .scan_ranges = {},
                                 .shared_state = shared_state.get(),
                                 .shared_state_map = {},
                                 .task_idx = 0};

            EXPECT_TRUE(source_local_state_uptr->init(state.get(), info).ok());
            state->resize_op_id_to_local_state(-100);
            state->emplace_local_state(source->operator_id(), std::move(source_local_state_uptr));
        }

        { EXPECT_TRUE(sink_local_state->open(state.get()).ok()); }

        { EXPECT_TRUE(source_local_state->open(state.get()).ok()); }
    }

    bool is_block(std::vector<Dependency*> deps) {
        for (auto* dep : deps) {
            if (!dep->ready()) {
                return true;
            }
        }
        return false;
    }

    bool is_ready(std::vector<Dependency*> deps) {
        for (auto* dep : deps) {
            if (!dep->ready()) {
                return false;
            }
        }
        return true;
    }

    RuntimeProfile profile {"test"};
    std::unique_ptr<AnalyticSinkOperatorX> sink;
    std::unique_ptr<AnalyticSourceOperatorX> source;

    std::unique_ptr<AnalyticSinkLocalState> sink_local_state_uptr;
    AnalyticSinkLocalState* sink_local_state;
    std::unique_ptr<AnalyticLocalState> source_local_state_uptr;
    AnalyticLocalState* source_local_state;

    std::shared_ptr<MockRuntimeState> state;
    std::shared_ptr<MockAnalyticSinkOperator> _child_op;
    std::shared_ptr<BasicSharedState> shared_state;

    ObjectPool pool;
    char buffer[100];
    std::vector<int64_t> _data_vals;
};

TEST_F(AnalyticSinkOperatorTest, withoutAggFunction) {
    Initialize(10);
    create_operator(false, 0, "", {});
    create_local_state();
    // test without agg function and no window: _get_next_for_partition
    // do nothing only input and output block
    {
        vectorized::Block block = ColumnHelper::create_block<DataTypeInt64>({2, 3, 1});
        auto st = sink->sink(state.get(), &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();
    }

    {
        vectorized::Block block = ColumnHelper::create_block<DataTypeInt64>({});
        bool eos = false;
        auto st = source->get_block(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_EQ(block.rows(), 3);
        std::cout << "source get block: \n" << block.dump_data() << std::endl;
        EXPECT_TRUE(ColumnHelper::block_equal(
                block, ColumnHelper::create_block<DataTypeInt64>({2, 3, 1})));

        vectorized::Block block2 = ColumnHelper::create_block<DataTypeInt64>({});
        bool eos2 = false;
        auto st2 = source->get_block(state.get(), &block2, &eos2);
        EXPECT_TRUE(st2.ok()) << st2.msg();
        EXPECT_TRUE(eos2);
        EXPECT_EQ(block2.rows(), 0);
    }
    std::cout << "######### withoutAggFunction test end #########" << std::endl;
}

TEST_F(AnalyticSinkOperatorTest, AggFunction) {
    Initialize(10);
    create_operator(false, 1, "sum", {std::make_shared<DataTypeInt64>()});
    sink->_agg_expr_ctxs.resize(1);
    sink->_agg_expr_ctxs[0] =
            MockSlotRef::create_mock_contexts(0, std::make_shared<DataTypeInt64>());
    create_local_state();
    // test with sum agg function and no window: _get_next_for_partition
    // do sum of _data_vals, return all sum is 45
    {
        vectorized::Block block = ColumnHelper::create_block<DataTypeInt64>(_data_vals);
        auto st = sink->sink(state.get(), &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();
    }

    {
        vectorized::Block block = ColumnHelper::create_block<DataTypeInt64>({});
        bool eos = false;
        auto st = source->get_block(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        std::cout << "source get block: \n" << block.dump_data() << std::endl;
        std::vector<int64_t> expect_vals(10, 45);
        EXPECT_EQ(block.rows(), expect_vals.size());
        EXPECT_TRUE(ColumnHelper::block_equal(
                block, ColumnHelper::create_block<DataTypeInt64>(_data_vals, expect_vals)));

        vectorized::Block block2 = ColumnHelper::create_block<DataTypeInt64>({});
        bool eos2 = false;
        auto st2 = source->get_block(state.get(), &block2, &eos2);
        EXPECT_TRUE(st2.ok()) << st2.msg();
        EXPECT_TRUE(eos2);
        EXPECT_EQ(block2.rows(), 0);
    }
    std::cout << "######### AggFunction with sum test end #########" << std::endl;
}

TEST_F(AnalyticSinkOperatorTest, AggFunction2) {
    int batch_size = 2;
    Initialize(batch_size);
    create_operator(true, 1, "row_number", {});
    sink->_agg_expr_ctxs.resize(1);
    sink->_agg_expr_ctxs[0] =
            MockSlotRef::create_mock_contexts(0, std::make_shared<DataTypeInt64>());
    TAnalyticWindow temp_window;
    temp_window.type = TAnalyticWindowType::ROWS;
    TAnalyticWindowBoundary window_end;
    window_end.type = TAnalyticWindowBoundaryType::CURRENT_ROW;
    temp_window.__set_window_end(window_end);
    create_window_type(false, true, temp_window);
    create_local_state();
    // test with row_number agg function and has window: _get_next_for_unbounded_rows

    auto sink_data = [&](int row_count, bool eos) {
        std::vector<int64_t> data_vals;
        for (int i = 0; i < batch_size; i++) {
            data_vals.push_back(row_count + i);
        }
        vectorized::Block block = ColumnHelper::create_block<DataTypeInt64>(data_vals);
        auto st = sink->sink(state.get(), &block, eos);
        EXPECT_TRUE(st.ok()) << st.msg();
    };

    {
        int row_count = 0;
        for (int i = 0; i < 5; i++) {
            sink_data(row_count, i == 4);
            row_count += batch_size;
        }
    }

    auto compare_block_result = [&](int row_count) {
        std::vector<int64_t> expect_vals;
        std::vector<int64_t> data_vals;
        for (int i = 0; i < batch_size; i++) {
            data_vals.push_back(row_count + i);
            expect_vals.push_back(row_count + i + 1);
        }
        vectorized::Block block = ColumnHelper::create_block<DataTypeInt64>({});
        bool eos = false;
        auto st = source->get_block(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        std::cout << "source get from block is: \n" << block.dump_data() << std::endl;
        std::cout << "block for real result is: \n "
                  << ColumnHelper::create_block<DataTypeInt64>(data_vals, expect_vals).dump_data()
                  << std::endl;
        EXPECT_TRUE(ColumnHelper::block_equal(
                block, ColumnHelper::create_block<DataTypeInt64>(data_vals, expect_vals)));
    };

    {
        int row_count = 0;
        for (int i = 0; i < 5; i++) {
            compare_block_result(row_count);
            row_count += batch_size;
        }
        vectorized::Block block2 = ColumnHelper::create_block<DataTypeInt64>({});
        bool eos2 = false;
        auto st2 = source->get_block(state.get(), &block2, &eos2);
        EXPECT_TRUE(st2.ok()) << st2.msg();
        EXPECT_EQ(block2.rows(), 0);
        EXPECT_TRUE(eos2);
    }
    std::cout << "######### AggFunction with row_number test end #########" << std::endl;
}

TEST_F(AnalyticSinkOperatorTest, AggFunction3) {
    int batch_size = 2;
    Initialize(batch_size);
    create_operator(true, 1, "sum", {std::make_shared<DataTypeInt64>()});
    sink->_agg_expr_ctxs.resize(1);
    sink->_agg_expr_ctxs[0] =
            MockSlotRef::create_mock_contexts(0, std::make_shared<DataTypeInt64>());
    TAnalyticWindow temp_window;
    temp_window.type = TAnalyticWindowType::ROWS;
    TAnalyticWindowBoundary window_end;
    window_end.type = TAnalyticWindowBoundaryType::CURRENT_ROW;
    temp_window.__set_window_end(window_end);
    create_window_type(false, true, temp_window);
    create_local_state();
    // test with row_number agg function and has window: _get_next_for_unbounded_rows

    auto sink_data = [&](int row_count, bool eos) {
        std::vector<int64_t> data_vals;
        for (int i = 0; i < batch_size; i++) {
            data_vals.push_back(row_count + i);
        }
        vectorized::Block block = ColumnHelper::create_block<DataTypeInt64>(data_vals);
        auto st = sink->sink(state.get(), &block, eos);
        EXPECT_TRUE(st.ok()) << st.msg();
    };

    {
        int row_count = 0;
        for (int i = 0; i < 5; i++) {
            sink_data(row_count, i == 4);
            row_count += batch_size;
        }
    }

    auto compare_block_result = [&](int row_count, std::vector<int64_t> data_vals,
                                    std::vector<int64_t> expect_vals) {
        std::vector<int64_t> expect_vals_tmp;
        std::vector<int64_t> data_vals_tmp;
        for (int i = 0; i < batch_size; i++) {
            data_vals_tmp.push_back(data_vals[i + row_count]);
            expect_vals_tmp.push_back(expect_vals[i + row_count]);
        }
        vectorized::Block block = ColumnHelper::create_block<DataTypeInt64>({});
        bool eos = false;
        auto st = source->get_block(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        std::cout << "source get from block is: \n" << block.dump_data() << std::endl;
        std::cout << "block for real result is: \n "
                  << ColumnHelper::create_block<DataTypeInt64>(data_vals_tmp, expect_vals_tmp)
                             .dump_data()
                  << std::endl;
        EXPECT_TRUE(ColumnHelper::block_equal(
                block, ColumnHelper::create_block<DataTypeInt64>(data_vals_tmp, expect_vals_tmp)));
    };

    {
        int row_count = 0;
        std::vector<int64_t> data_vals {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        std::vector<int64_t> expect_vals {0, 1, 3, 6, 10, 15, 21, 28, 36, 45}; //sum
        for (int i = 0; i < 5; i++) {
            compare_block_result(row_count, data_vals, expect_vals);
            row_count += batch_size;
        }
        vectorized::Block block2 = ColumnHelper::create_block<DataTypeInt64>({});
        bool eos2 = false;
        auto st2 = source->get_block(state.get(), &block2, &eos2);
        EXPECT_TRUE(st2.ok()) << st2.msg();
        EXPECT_EQ(block2.rows(), 0);
        EXPECT_TRUE(eos2);
    }
    std::cout << "######### AggFunction with row_number test end #########" << std::endl;
}

TEST_F(AnalyticSinkOperatorTest, AggFunction4) {
    int batch_size = 2;
    Initialize(batch_size);
    create_operator(true, 1, "sum", {std::make_shared<DataTypeInt64>()});
    sink->_agg_expr_ctxs.resize(1);
    sink->_agg_expr_ctxs[0] =
            MockSlotRef::create_mock_contexts(0, std::make_shared<DataTypeInt64>());
    TAnalyticWindow temp_window;
    temp_window.type = TAnalyticWindowType::ROWS;
    TAnalyticWindowBoundary window_start;
    window_start.type = TAnalyticWindowBoundaryType::PRECEDING;
    window_start.__set_rows_offset_value(1);
    temp_window.__set_window_start(window_start);
    TAnalyticWindowBoundary window_end;
    window_end.type = TAnalyticWindowBoundaryType::CURRENT_ROW;
    temp_window.__set_window_end(window_end);
    create_window_type(true, true, temp_window);
    create_local_state();
    // test with row_number agg function and has window: _get_next_for_unbounded_rows

    auto sink_data = [&](int row_count, bool eos) {
        std::vector<int64_t> data_vals;
        for (int i = 0; i < batch_size; i++) {
            data_vals.push_back(row_count + i);
        }
        vectorized::Block block = ColumnHelper::create_block<DataTypeInt64>(data_vals);
        auto st = sink->sink(state.get(), &block, eos);
        EXPECT_TRUE(st.ok()) << st.msg();
    };

    {
        int row_count = 0;
        for (int i = 0; i < 5; i++) {
            sink_data(row_count, i == 4);
            row_count += batch_size;
        }
    }

    auto compare_block_result = [&](int row_count, std::vector<int64_t> data_vals,
                                    std::vector<int64_t> expect_vals) {
        std::vector<int64_t> expect_vals_tmp;
        std::vector<int64_t> data_vals_tmp;
        for (int i = 0; i < batch_size; i++) {
            data_vals_tmp.push_back(data_vals[i + row_count]);
            expect_vals_tmp.push_back(expect_vals[i + row_count]);
        }
        vectorized::Block block = ColumnHelper::create_block<DataTypeInt64>({});
        bool eos = false;
        auto st = source->get_block(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        std::cout << "source get from block is: \n" << block.dump_data() << std::endl;
        std::cout << "block for real result is: \n "
                  << ColumnHelper::create_block<DataTypeInt64>(data_vals_tmp, expect_vals_tmp)
                             .dump_data()
                  << std::endl;
        EXPECT_TRUE(ColumnHelper::block_equal(
                block, ColumnHelper::create_block<DataTypeInt64>(data_vals_tmp, expect_vals_tmp)));
    };

    {
        int row_count = 0;
        std::vector<int64_t> data_vals {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        std::vector<int64_t> expect_vals {0, 1, 3, 5, 7, 9, 11, 13, 15, 17}; //sum
        for (int i = 0; i < 5; i++) {
            compare_block_result(row_count, data_vals, expect_vals);
            row_count += batch_size;
        }
        vectorized::Block block2 = ColumnHelper::create_block<DataTypeInt64>({});
        bool eos2 = false;
        auto st2 = source->get_block(state.get(), &block2, &eos2);
        EXPECT_TRUE(st2.ok()) << st2.msg();
        EXPECT_EQ(block2.rows(), 0);
        EXPECT_TRUE(eos2);
    }
    std::cout << "######### AggFunction with row_number test end #########" << std::endl;
}

TEST_F(AnalyticSinkOperatorTest, AggFunction5) {
    int batch_size = 2;
    Initialize(batch_size);
    create_operator(true, 1, "sum", {std::make_shared<DataTypeInt64>()});
    sink->_agg_expr_ctxs.resize(1);
    sink->_agg_expr_ctxs[0] =
            MockSlotRef::create_mock_contexts(0, std::make_shared<DataTypeInt64>());
    TAnalyticWindow temp_window;
    temp_window.type = TAnalyticWindowType::RANGE;
    TAnalyticWindowBoundary window_start;
    window_start.type = TAnalyticWindowBoundaryType::PRECEDING;
    window_start.__set_rows_offset_value(1);
    temp_window.__set_window_start(window_start);
    TAnalyticWindowBoundary window_end;
    window_end.type = TAnalyticWindowBoundaryType::CURRENT_ROW;
    temp_window.__set_window_end(window_end);
    create_window_type(true, true, temp_window);
    create_local_state();
    // test with row_number agg function and has window: _get_next_for_unbounded_rows

    auto sink_data = [&](int row_count, bool eos) {
        std::vector<int64_t> data_vals;
        for (int i = 0; i < batch_size; i++) {
            data_vals.push_back(row_count + i);
        }
        vectorized::Block block = ColumnHelper::create_block<DataTypeInt64>(data_vals);
        auto st = sink->sink(state.get(), &block, eos);
        EXPECT_TRUE(st.ok()) << st.msg();
    };

    {
        int row_count = 0;
        for (int i = 0; i < 5; i++) {
            sink_data(row_count, i == 4);
            row_count += batch_size;
        }
    }

    auto compare_block_result = [&](int row_count, std::vector<int64_t> data_vals,
                                    std::vector<int64_t> expect_vals) {
        std::vector<int64_t> expect_vals_tmp;
        std::vector<int64_t> data_vals_tmp;
        for (int i = 0; i < batch_size; i++) {
            data_vals_tmp.push_back(data_vals[i + row_count]);
            expect_vals_tmp.push_back(expect_vals[i + row_count]);
        }
        vectorized::Block block = ColumnHelper::create_block<DataTypeInt64>({});
        bool eos = false;
        auto st = source->get_block(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        std::cout << "source get from block is: \n" << block.dump_data() << std::endl;
        std::cout << "block for real result is: \n "
                  << ColumnHelper::create_block<DataTypeInt64>(data_vals_tmp, expect_vals_tmp)
                             .dump_data()
                  << std::endl;
        EXPECT_TRUE(ColumnHelper::block_equal(
                block, ColumnHelper::create_block<DataTypeInt64>(data_vals_tmp, expect_vals_tmp)));
    };

    {
        int row_count = 0;
        std::vector<int64_t> data_vals {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        std::vector<int64_t> expect_vals {0, 1, 3, 5, 7, 9, 11, 13, 15, 17}; //sum
        for (int i = 0; i < 5; i++) {
            compare_block_result(row_count, data_vals, expect_vals);
            row_count += batch_size;
        }
        vectorized::Block block2 = ColumnHelper::create_block<DataTypeInt64>({});
        bool eos2 = false;
        auto st2 = source->get_block(state.get(), &block2, &eos2);
        EXPECT_TRUE(st2.ok()) << st2.msg();
        EXPECT_EQ(block2.rows(), 0);
        EXPECT_TRUE(eos2);
    }
    std::cout << "######### AggFunction with row_number test end #########" << std::endl;
}

TEST_F(AnalyticSinkOperatorTest, AggFunction6) {
    int batch_size = 1;
    Initialize(batch_size);
    create_operator(true, 1, "first_value", {std::make_shared<DataTypeInt64>()});
    sink->_agg_expr_ctxs.resize(1);
    sink->_agg_expr_ctxs[0] =
            MockSlotRef::create_mock_contexts(2, std::make_shared<DataTypeInt64>());
    sink->_partition_by_eq_expr_ctxs =
            MockSlotRef::create_mock_contexts(0, std::make_shared<DataTypeInt64>());
    sink->_order_by_eq_expr_ctxs =
            MockSlotRef::create_mock_contexts(1, std::make_shared<DataTypeInt64>());
    TAnalyticWindow temp_window;
    temp_window.type = TAnalyticWindowType::ROWS;
    TAnalyticWindowBoundary window_end;
    window_end.type = TAnalyticWindowBoundaryType::CURRENT_ROW;
    temp_window.__set_window_end(window_end);
    create_window_type(false, true, temp_window);
    create_local_state();
    // test with row_number agg function and has window: _get_next_for_unbounded_rows
    std::vector<int64_t> suppkey = {5,  5,  17, 17, 26, 26, 32, 32, 36, 36,
                                    40, 40, 41, 41, 51, 51, 87, 87, 93, 93};
    std::vector<int64_t> orderkey = {5, 5, 7, 7, 7, 7, 7, 7, 6, 6, 7, 7, 5, 5, 7, 7, 5, 5, 7, 7};
    std::vector<int64_t> quantity = {50, 50, 46, 46, 35, 35, 28, 28, 37, 37,
                                     38, 38, 26, 26, 12, 12, 15, 15, 9,  9};
    std::vector<int64_t> first_value_quantity_A = {50, 50, 46, 46, 35, 35, 28, 28, 37, 37,
                                                   38, 38, 26, 26, 12, 12, 15, 15, 9,  9};

    auto sink_data = [&](int row_count, bool eos) {
        std::vector<int64_t> col1, col2, col3;
        for (int i = 0; i < batch_size; i++) {
            col1.push_back(suppkey[row_count + i]);
            col2.push_back(orderkey[row_count + i]);
            col3.push_back(quantity[row_count + i]);
        }
        vectorized::Block block;

        block.insert(ColumnHelper::create_column_with_name<DataTypeInt64>(col1));
        block.insert(ColumnHelper::create_column_with_name<DataTypeInt64>(col2));
        block.insert(ColumnHelper::create_column_with_name<DataTypeInt64>(col3));
        auto st = sink->sink(state.get(), &block, eos);
        EXPECT_TRUE(st.ok()) << st.msg();
    };

    {
        int row_count = 0;
        for (int i = 0; i < 5; i++) {
            sink_data(row_count, i == 4);
            row_count += batch_size;
        }
    }

    auto compare_block_result = [&](int row_count) {
        std::vector<int64_t> col1, col2, col3, expect_vals;
        for (int i = 0; i < batch_size; i++) {
            col1.push_back(suppkey[row_count + i]);
            col2.push_back(orderkey[row_count + i]);
            col3.push_back(quantity[row_count + i]);
            expect_vals.push_back(first_value_quantity_A[row_count + i]);
        }

        vectorized::Block block;
        block.insert(ColumnHelper::create_column_with_name<DataTypeInt64>({}));
        block.insert(ColumnHelper::create_column_with_name<DataTypeInt64>({}));
        block.insert(ColumnHelper::create_column_with_name<DataTypeInt64>({}));
        block.insert(ColumnHelper::create_column_with_name<DataTypeInt64>({}));
        bool eos = false;
        auto st = source->get_block(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        std::cout << "source get from block is: \n" << block.dump_data() << std::endl;

        vectorized::Block result_block;
        result_block.insert(ColumnHelper::create_column_with_name<DataTypeInt64>(col1));
        result_block.insert(ColumnHelper::create_column_with_name<DataTypeInt64>(col2));
        result_block.insert(ColumnHelper::create_column_with_name<DataTypeInt64>(col3));
        result_block.insert(ColumnHelper::create_column_with_name<DataTypeInt64>(expect_vals));
        std::cout << "block for real result is: \n " << result_block.dump_data() << std::endl;
        EXPECT_TRUE(ColumnHelper::block_equal(block, result_block));
    };

    {
        int row_count = 0;
        std::vector<int64_t> data_vals {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        std::vector<int64_t> expect_vals {0, 0, 1, 2, 3, 4, 5, 6, 7, 8}; //sum
        for (int i = 0; i < 5; i++) {
            compare_block_result(row_count);
            row_count += batch_size;
        }
        vectorized::Block block2 = ColumnHelper::create_block<DataTypeInt64>({});
        bool eos2 = false;
        auto st2 = source->get_block(state.get(), &block2, &eos2);
        EXPECT_TRUE(st2.ok()) << st2.msg();
        EXPECT_EQ(block2.rows(), 0);
        EXPECT_TRUE(eos2);
    }
    std::cout << "######### AggFunction with row_number test end #########" << std::endl;
}

TEST_F(AnalyticSinkOperatorTest, AggFunction7) {
    int batch_size = 2;
    Initialize(batch_size);
    create_operator(true, 1, "sum", {std::make_shared<DataTypeInt64>()});
    sink->_agg_expr_ctxs.resize(1);
    sink->_agg_expr_ctxs[0] =
            MockSlotRef::create_mock_contexts(0, std::make_shared<DataTypeInt64>());
    sink->_partition_by_eq_expr_ctxs =
            MockSlotRef::create_mock_contexts(0, std::make_shared<DataTypeInt64>());
    sink->_order_by_eq_expr_ctxs =
            MockSlotRef::create_mock_contexts(0, std::make_shared<DataTypeInt64>());
    TAnalyticWindow temp_window;
    temp_window.type = TAnalyticWindowType::RANGE;
    TAnalyticWindowBoundary window_end;
    window_end.type = TAnalyticWindowBoundaryType::CURRENT_ROW;
    temp_window.__set_window_end(window_end);
    create_window_type(false, true, temp_window);
    sink->_has_range_window = true;

    create_local_state();

    auto sink_data = [&](int row_count, bool eos) {
        std::vector<int64_t> data_vals;
        for (int i = 0; i < batch_size; i++) {
            data_vals.push_back(row_count + i);
        }
        vectorized::Block block = ColumnHelper::create_block<DataTypeInt64>(data_vals);
        auto st = sink->sink(state.get(), &block, eos);
        EXPECT_TRUE(st.ok()) << st.msg();
    };

    {
        int row_count = 0;
        for (int i = 0; i < 5; i++) {
            sink_data(row_count, i == 4);
            row_count += batch_size;
        }
    }

    auto compare_block_result = [&](int row_count, std::vector<int64_t> data_vals,
                                    std::vector<int64_t> expect_vals) {
        std::vector<int64_t> expect_vals_tmp;
        std::vector<int64_t> data_vals_tmp;
        for (int i = 0; i < batch_size; i++) {
            data_vals_tmp.push_back(data_vals[i + row_count]);
            expect_vals_tmp.push_back(expect_vals[i + row_count]);
        }
        vectorized::Block block = ColumnHelper::create_block<DataTypeInt64>({});
        bool eos = false;
        auto st = source->get_block(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        std::cout << "source get from block is: \n" << block.dump_data() << std::endl;
        std::cout << "block for real result is: \n "
                  << ColumnHelper::create_block<DataTypeInt64>(data_vals_tmp, expect_vals_tmp)
                             .dump_data()
                  << std::endl;
        EXPECT_TRUE(ColumnHelper::block_equal(
                block, ColumnHelper::create_block<DataTypeInt64>(data_vals_tmp, expect_vals_tmp)));
    };

    {
        int row_count = 0;
        std::vector<int64_t> data_vals {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        std::vector<int64_t> expect_vals {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}; //sum
        for (int i = 0; i < 5; i++) {
            compare_block_result(row_count, data_vals, expect_vals);
            row_count += batch_size;
        }
        vectorized::Block block2 = ColumnHelper::create_block<DataTypeInt64>({});
        bool eos2 = false;
        auto st2 = source->get_block(state.get(), &block2, &eos2);
        EXPECT_TRUE(st2.ok()) << st2.msg();
        EXPECT_EQ(block2.rows(), 0);
        EXPECT_TRUE(eos2);
    }
    Status exec_status = Status::OK();
    auto st = sink_local_state->close(state.get(), exec_status);
    EXPECT_TRUE(st.ok());
    st = source_local_state->close(state.get());
    EXPECT_TRUE(st.ok());
    std::cout << "######### AggFunction with row_number test end #########" << std::endl;
}

// range between is not support by FE, shouldn't consider this test
TEST_F(AnalyticSinkOperatorTest, AggFunction8) {
    int batch_size = 1;
    Initialize(batch_size);
    create_operator(true, 1, "sum", {std::make_shared<DataTypeInt64>()});
    sink->_agg_expr_ctxs.resize(1);
    sink->_agg_expr_ctxs[0] =
            MockSlotRef::create_mock_contexts(2, std::make_shared<DataTypeInt64>());
    sink->_partition_by_eq_expr_ctxs =
            MockSlotRef::create_mock_contexts(0, std::make_shared<DataTypeInt64>());
    sink->_order_by_eq_expr_ctxs =
            MockSlotRef::create_mock_contexts(1, std::make_shared<DataTypeInt64>());
    sink->_range_between_expr_ctxs = MockSlotRef::create_mock_contexts(
            {std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt64>()});
    TAnalyticWindow temp_window;
    temp_window.type = TAnalyticWindowType::RANGE;
    TAnalyticWindowBoundary window_end;
    window_end.type = TAnalyticWindowBoundaryType::CURRENT_ROW;
    temp_window.__set_window_end(window_end);
    create_window_type(true, true, temp_window);
    sink->_has_range_window = true;
    create_local_state();
    // test with row_number agg function and has window: _get_next_for_unbounded_rows
    std::vector<int64_t> suppkey = {5,  5,  17, 17, 26, 26, 32, 32, 36, 36,
                                    40, 40, 41, 41, 51, 51, 87, 87, 93, 93};
    std::vector<int64_t> orderkey = {5, 5, 7, 7, 7, 7, 7, 7, 6, 6, 7, 7, 5, 5, 7, 7, 5, 5, 7, 7};
    std::vector<int64_t> quantity = {50, 50, 46, 46, 35, 35, 28, 28, 37, 37,
                                     38, 38, 26, 26, 12, 12, 15, 15, 9,  9};
    std::vector<int64_t> first_value_quantity_A = {100, 100, 0,  92, 35, 35, 28, 28, 37, 37,
                                                   38,  38,  26, 26, 12, 12, 15, 15, 9,  9};

    auto sink_data = [&](int row_count, bool eos) {
        std::vector<int64_t> col1, col2, col3;
        for (int i = 0; i < batch_size; i++) {
            col1.push_back(suppkey[row_count + i]);
            col2.push_back(orderkey[row_count + i]);
            col3.push_back(quantity[row_count + i]);
        }
        vectorized::Block block;

        block.insert(ColumnHelper::create_column_with_name<DataTypeInt64>(col1));
        block.insert(ColumnHelper::create_column_with_name<DataTypeInt64>(col2));
        block.insert(ColumnHelper::create_column_with_name<DataTypeInt64>(col3));
        auto st = sink->sink(state.get(), &block, eos);
        EXPECT_TRUE(st.ok()) << st.msg();
    };

    {
        int row_count = 0;
        for (int i = 0; i < 5; i++) {
            sink_data(row_count, i == 4);
            row_count += batch_size;
        }
    }

    auto compare_block_result = [&](int row_count) {
        std::vector<int64_t> col1, col2, col3, expect_vals;
        for (int i = 0; i < batch_size; i++) {
            col1.push_back(suppkey[row_count + i]);
            col2.push_back(orderkey[row_count + i]);
            col3.push_back(quantity[row_count + i]);
            expect_vals.push_back(first_value_quantity_A[row_count + i]);
        }

        vectorized::Block block;
        block.insert(ColumnHelper::create_column_with_name<DataTypeInt64>({}));
        block.insert(ColumnHelper::create_column_with_name<DataTypeInt64>({}));
        block.insert(ColumnHelper::create_column_with_name<DataTypeInt64>({}));
        block.insert(ColumnHelper::create_column_with_name<DataTypeInt64>({}));
        bool eos = false;
        auto st = source->get_block(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        std::cout << "source get from block is: \n" << block.dump_data() << std::endl;

        vectorized::Block result_block;
        result_block.insert(ColumnHelper::create_column_with_name<DataTypeInt64>(col1));
        result_block.insert(ColumnHelper::create_column_with_name<DataTypeInt64>(col2));
        result_block.insert(ColumnHelper::create_column_with_name<DataTypeInt64>(col3));
        result_block.insert(ColumnHelper::create_column_with_name<DataTypeInt64>(expect_vals));
        std::cout << "block for real result is: \n " << result_block.dump_data() << std::endl;
        EXPECT_TRUE(ColumnHelper::block_equal(block, result_block));
    };

    {
        int row_count = 0;
        for (int i = 0; i < 5; i++) {
            compare_block_result(row_count);
            row_count += batch_size;
        }
        vectorized::Block block2 = ColumnHelper::create_block<DataTypeInt64>({});
        bool eos2 = false;
        auto st2 = source->get_block(state.get(), &block2, &eos2);
        EXPECT_TRUE(st2.ok()) << st2.msg();
        EXPECT_EQ(block2.rows(), 0);
        EXPECT_TRUE(eos2);
    }
    std::cout << "######### AggFunction with row_number test end #########" << std::endl;
}

} // namespace doris::pipeline