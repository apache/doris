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
#include "vec/columns/columns_number.h"
#include "vec/core/block.h"
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
    void SetUp() override {
        state = std::make_shared<MockRuntimeState>();
        state->batsh_size = 10;
        _child_op = std::make_unique<MockAnalyticSinkOperator>();
        for (int i = 0; i < state->batsh_size; i++) {
            _data_vals.push_back(i);
        }
    }

    void create_operator(size_t agg_functions_size, std::string function_name, bool has_window) {
        sink = std::make_unique<AnalyticSinkOperatorX>(&pool);
        sink->_agg_functions_size = agg_functions_size;
        sink->_has_window = has_window;
        if (agg_functions_size == 1) {
            sink->_agg_functions.resize(agg_functions_size);
            sink->_num_agg_input.resize(agg_functions_size);
            sink->_agg_expr_ctxs.resize(agg_functions_size);
            sink->_offsets_of_aggregate_states.resize(agg_functions_size);
            sink->_change_to_nullable_flags.resize(agg_functions_size);
            sink->_agg_functions[0] =
                    create_agg_fn(pool, function_name, {std::make_shared<DataTypeInt64>()}, false);
            sink->_num_agg_input[0] = 1;
            sink->_offsets_of_aggregate_states[0] = 0;
        }

        EXPECT_TRUE(sink->set_child(_child_op));
        source = std::make_unique<AnalyticSourceOperatorX>();
        create_local_state();
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
            sink_local_state_uptr->_fn_place_ptr = &buffer[0];
            sink_local_state_uptr->_agg_input_columns.resize(1);
            sink_local_state_uptr->_agg_input_columns[0].resize(1);
            auto col_data = ColumnInt64::create();
            for (int i = 0; i < _data_vals.size(); i++) {
                col_data->insert_value(_data_vals[i]);
            }
            sink_local_state_uptr->_agg_input_columns[0][0] = std::move(col_data);
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

    ObjectPool pool;
    char buffer[100];

    std::shared_ptr<BasicSharedState> shared_state;
    std::vector<int64_t> _data_vals;
};

TEST_F(AnalyticSinkOperatorTest, withoutAggFunction) {
    create_operator(0, "", false);
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
}

TEST_F(AnalyticSinkOperatorTest, AggFunction) {
    create_operator(1, "sum", false);
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
}

} // namespace doris::pipeline