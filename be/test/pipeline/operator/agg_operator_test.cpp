
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

#include <gtest/gtest.h>

#include <memory>

#include "pipeline/dependency.h"
#include "pipeline/exec/aggregation_sink_operator.h"
#include "pipeline/exec/aggregation_source_operator.h"
#include "pipeline/exec/assert_num_rows_operator.h"
#include "pipeline/exec/mock_operator.h"
#include "pipeline/operator/operator_helper.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_agg_fn_evaluator.h"
#include "testutil/mock/mock_slot_ref.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"

namespace doris::pipeline {

auto static init_sink_and_source(std::shared_ptr<AggSinkOperatorX> sink_op,
                                 std::shared_ptr<AggSourceOperatorX> source_op,
                                 OperatorContext& ctx) {
    auto shared_state = sink_op->create_shared_state();
    {
        auto local_state = AggSinkOperatorX::LocalState ::create_unique(sink_op.get(), &ctx.state);
        LocalSinkStateInfo info {.task_idx = 0,
                                 .parent_profile = &ctx.profile,
                                 .sender_id = 0,
                                 .shared_state = shared_state.get(),
                                 .shared_state_map = {},
                                 .tsink = TDataSink {}};
        EXPECT_TRUE(local_state->init(&ctx.state, info).ok());
        ctx.state.emplace_sink_local_state(0, std::move(local_state));
    }

    {
        auto local_state =
                AggSourceOperatorX::LocalState::create_unique(&ctx.state, source_op.get());
        LocalStateInfo info {.parent_profile = &ctx.profile,
                             .scan_ranges = {},
                             .shared_state = shared_state.get(),
                             .shared_state_map = {},
                             .task_idx = 0};

        EXPECT_TRUE(local_state->init(&ctx.state, info).ok());
        ctx.state.resize_op_id_to_local_state(-100);
        ctx.state.emplace_local_state(source_op->operator_id(), std::move(local_state));
    }

    {
        auto* sink_local_state = ctx.state.get_sink_local_state();
        EXPECT_TRUE(sink_local_state->open(&ctx.state).ok());
    }

    {
        auto* source_local_state = ctx.state.get_local_state(source_op->operator_id());
        EXPECT_TRUE(source_local_state->open(&ctx.state).ok());
    }
    return shared_state;
}

struct MockAggsinkOperator : public AggSinkOperatorX {
    MockAggsinkOperator() = default;

    Status _init_probe_expr_ctx(RuntimeState* state) override { return Status::OK(); }

    Status _init_aggregate_evaluators(RuntimeState* state) override { return Status::OK(); }

    Status _check_agg_fn_output() override { return Status::OK(); }
};

struct MockAggSourceOperator : public AggSourceOperatorX {
    MockAggSourceOperator() = default;
    RowDescriptor& row_descriptor() override { return *mock_row_descriptor; }
    std::unique_ptr<RowDescriptor> mock_row_descriptor;
};

std::shared_ptr<AggSinkOperatorX> create_agg_sink_op(OperatorContext& ctx, bool is_merge,
                                                     bool without_key) {
    auto op = std::make_shared<MockAggsinkOperator>();
    op->_aggregate_evaluators.push_back(
            vectorized::create_mock_agg_fn_evaluator(ctx.pool, is_merge, without_key));
    op->_pool = &ctx.pool;
    EXPECT_TRUE(op->prepare(&ctx.state).ok());
    return op;
}

std::shared_ptr<AggSourceOperatorX> create_agg_source_op(OperatorContext& ctx, bool without_key,
                                                         bool needs_finalize) {
    auto op = std::make_shared<MockAggSourceOperator>();
    op->mock_row_descriptor.reset(
            new MockRowDescriptor {{std::make_shared<vectorized::DataTypeInt64>()}, &ctx.pool});
    op->_without_key = without_key;
    op->_needs_finalize = needs_finalize;
    EXPECT_TRUE(op->prepare(&ctx.state).ok());
    return op;
}

TEST(AggOperatorTestWithOutGroupBy, test_need_finalize) {
    using namespace vectorized;
    OperatorContext ctx;

    auto sink_op = create_agg_sink_op(ctx, false, true);

    auto source_op = create_agg_source_op(ctx, true, true);

    auto shared_state = init_sink_and_source(sink_op, source_op, ctx);

    {
        vectorized::Block block = ColumnHelper::create_block<DataTypeInt64>({1, 2, 3});
        auto st = sink_op->sink(&ctx.state, &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();
    }

    {
        vectorized::Block block = ColumnHelper::create_block<DataTypeInt64>({});
        bool eos = false;
        auto st = source_op->get_block(&ctx.state, &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(eos);
        EXPECT_EQ(block.rows(), 1);
        EXPECT_TRUE(
                ColumnHelper::block_equal(block, ColumnHelper::create_block<DataTypeInt64>({6})));
    }
}

TEST(AggOperatorTestWithOutGroupBy, test_no_need_finalize) {
    using namespace vectorized;
    OperatorContext ctx;

    auto sink_op = create_agg_sink_op(ctx, false, true);

    auto source_op = create_agg_source_op(ctx, true, false);

    auto shared_state = init_sink_and_source(sink_op, source_op, ctx);

    {
        vectorized::Block block = ColumnHelper::create_block<DataTypeInt64>({1, 2, 3});
        auto st = sink_op->sink(&ctx.state, &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();
    }

    {
        vectorized::Block block = ColumnHelper::create_block<DataTypeInt64>({});
        bool eos = false;
        auto st = source_op->get_block(&ctx.state, &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(eos);
        EXPECT_EQ(block.rows(), 1);
        EXPECT_TRUE(
                check_and_get_column<ColumnFixedLengthObject>(*block.get_by_position(0).column));
    }
}

vectorized::Block test_agg_1_phase(vectorized::Block origin_block) {
    using namespace vectorized;
    OperatorContext ctx;

    auto sink_op = create_agg_sink_op(ctx, false, true);

    auto source_op = create_agg_source_op(ctx, true, false);

    auto shared_state = init_sink_and_source(sink_op, source_op, ctx);

    EXPECT_TRUE(sink_op->sink(&ctx.state, &origin_block, true).ok());

    vectorized::Block serialize_block = ColumnHelper::create_block<DataTypeInt64>({});

    bool eos = false;
    EXPECT_TRUE(source_op->get_block(&ctx.state, &serialize_block, &eos).ok());
    EXPECT_TRUE(eos);
    EXPECT_EQ(serialize_block.rows(), 1);
    EXPECT_TRUE(check_and_get_column<ColumnFixedLengthObject>(
            *serialize_block.get_by_position(0).column));

    return serialize_block;
}

void test_agg_2_phase(vectorized::Block serialize_block) {
    using namespace vectorized;
    OperatorContext ctx;

    auto sink_op = create_agg_sink_op(ctx, true, false);

    auto source_op = create_agg_source_op(ctx, true, true);

    auto shared_state2 = init_sink_and_source(sink_op, source_op, ctx);

    EXPECT_TRUE(sink_op->sink(&ctx.state, &serialize_block, true).ok());

    vectorized::Block result_block = ColumnHelper::create_block<DataTypeInt64>({});

    bool eos = false;
    EXPECT_TRUE(source_op->get_block(&ctx.state, &result_block, &eos).ok());

    EXPECT_TRUE(eos);
    EXPECT_EQ(result_block.rows(), 1);
    EXPECT_TRUE(ColumnHelper::block_equal(result_block,
                                          ColumnHelper::create_block<DataTypeInt64>({6})));
}

TEST(AggOperatorTestWithOutGroupBy, test_2_phase) {
    using namespace vectorized;
    auto serialize_block = test_agg_1_phase(ColumnHelper::create_block<DataTypeInt64>({1, 2, 3}));
    test_agg_2_phase(serialize_block);
}

TEST(AggOperatorTestWithOutGroupBy, test_multi_input) {
    using namespace vectorized;
    OperatorContext ctx;
    auto sink_op = std::make_shared<MockAggsinkOperator>();
    sink_op->_aggregate_evaluators.push_back(vectorized::create_mock_agg_fn_evaluator(
            ctx.pool, MockSlotRef::create_mock_contexts(0, std::make_shared<const DataTypeInt64>()),
            false, true));
    sink_op->_aggregate_evaluators.push_back(vectorized::create_mock_agg_fn_evaluator(
            ctx.pool, MockSlotRef::create_mock_contexts(1, std::make_shared<const DataTypeInt64>()),
            false, true));
    sink_op->_pool = &ctx.pool;
    EXPECT_TRUE(sink_op->prepare(&ctx.state).ok());

    auto source_op = std::make_shared<MockAggSourceOperator>();
    source_op->mock_row_descriptor.reset(
            new MockRowDescriptor {{std::make_shared<vectorized::DataTypeInt64>(),
                                    std::make_shared<vectorized::DataTypeInt64>()},
                                   &ctx.pool});
    source_op->_without_key = true;
    source_op->_needs_finalize = true;
    EXPECT_TRUE(source_op->prepare(&ctx.state).ok());

    auto shared_state = init_sink_and_source(sink_op, source_op, ctx);

    {
        vectorized::Block block {ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3}),
                                 ColumnHelper::create_column_with_name<DataTypeInt64>({4, 5, 6})};
        auto st = sink_op->sink(&ctx.state, &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();
    }

    {
        vectorized::Block block;
        bool eos = false;
        auto st = source_op->get_block(&ctx.state, &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(ColumnHelper::block_equal(
                block,
                vectorized::Block {ColumnHelper::create_column_with_name<DataTypeInt64>({6}),
                                   ColumnHelper::create_column_with_name<DataTypeInt64>({15})}));
    }
}

struct AggOperatorTestWithGroupBy : public testing::Test {
public:
    void SetUp() override {}
};

TEST_F(AggOperatorTestWithGroupBy, test_need_finalize_only_key) {
    /*
    group by key  and sum(value)    
    +---------------+
    |column(Int64)  |
    +---------------+
    |              1|
    |              2|
    |              3|
    |              1|
    |              2|
    |              3|
    +---------------+

    +---------------+---------------+
    |(Int64)        |(Int64)        |
    +---------------+---------------+
    |              1|              2|
    |              2|              4|
    |              3|              6|
    +---------------+---------------+
*/

    using namespace vectorized;
    OperatorContext ctx;
    auto sink_op = std::make_shared<MockAggsinkOperator>();
    sink_op->_aggregate_evaluators.push_back(
            vectorized::create_mock_agg_fn_evaluator(ctx.pool, false, false));
    sink_op->_pool = &ctx.pool;
    EXPECT_TRUE(sink_op->prepare(&ctx.state).ok());
    sink_op->_probe_expr_ctxs =
            MockSlotRef::create_mock_contexts(std::make_shared<DataTypeInt64>());

    auto source_op = std::make_shared<MockAggSourceOperator>();
    source_op->mock_row_descriptor.reset(
            new MockRowDescriptor {{std::make_shared<vectorized::DataTypeInt64>(),
                                    std::make_shared<vectorized::DataTypeInt64>()},
                                   &ctx.pool});
    source_op->_without_key = false;
    source_op->_needs_finalize = true;
    EXPECT_TRUE(source_op->prepare(&ctx.state).ok());

    auto shared_state = init_sink_and_source(sink_op, source_op, ctx);

    {
        vectorized::Block block = ColumnHelper::create_block<DataTypeInt64>({1, 2, 3, 1, 2, 3});
        auto st = sink_op->sink(&ctx.state, &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();
    }

    {
        vectorized::Block block;
        bool eos = false;
        auto st = source_op->get_block(&ctx.state, &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(ColumnHelper::block_equal(
                block, vectorized::Block {
                               ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3}),
                               ColumnHelper::create_column_with_name<DataTypeInt64>({2, 4, 6})}));
    }
}

TEST_F(AggOperatorTestWithGroupBy, test_need_finalize) {
    /*
         group by key   |  sum(value)    
        +---------------+---------------+
        |column(Int64)  |column(Int64)  |
        +---------------+---------------+
        |              1|              1|
        |              1|              1|
        |              2|            100|
        |              2|            100|
        |              2|            100|
        |              3|           1000|
        +---------------+---------------+

        +---------------+---------------+
        |(Int64)        |(Int64)        |
        +---------------+---------------+
        |              1|              2|
        |              2|            300|
        |              3|           1000|
        +---------------+---------------+
    */
    using namespace vectorized;
    OperatorContext ctx;
    auto sink_op = std::make_shared<MockAggsinkOperator>();
    sink_op->_aggregate_evaluators.push_back(vectorized::create_mock_agg_fn_evaluator(
            ctx.pool, MockSlotRef::create_mock_contexts(1, std::make_shared<DataTypeInt64>()),
            false, false));
    sink_op->_pool = &ctx.pool;
    EXPECT_TRUE(sink_op->prepare(&ctx.state).ok());
    sink_op->_probe_expr_ctxs =
            MockSlotRef::create_mock_contexts(0, std::make_shared<DataTypeInt64>());

    auto source_op = std::make_shared<MockAggSourceOperator>();
    source_op->mock_row_descriptor.reset(
            new MockRowDescriptor {{std::make_shared<vectorized::DataTypeInt64>(),
                                    std::make_shared<vectorized::DataTypeInt64>()},
                                   &ctx.pool});
    source_op->_without_key = false;
    source_op->_needs_finalize = true;
    EXPECT_TRUE(source_op->prepare(&ctx.state).ok());

    auto shared_state = init_sink_and_source(sink_op, source_op, ctx);

    {
        vectorized::Block block {
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 2, 2, 2, 3}),
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 100, 100, 100, 1000})};
        auto st = sink_op->sink(&ctx.state, &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();
    }

    {
        vectorized::Block block;
        bool eos = false;
        auto st = source_op->get_block(&ctx.state, &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(ColumnHelper::block_equal(
                block,
                vectorized::Block {
                        ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3}),
                        ColumnHelper::create_column_with_name<DataTypeInt64>({2, 300, 1000})}));
    }
}

TEST_F(AggOperatorTestWithGroupBy, test_2_phase) {
    /*
         group by key   |  sum(value)    
        +---------------+---------------+
        |column(Int64)  |column(Int64)  |
        +---------------+---------------+
        |              1|              1|
        |              1|              1|
        |              2|            100|
        |              2|            100|
        |              2|            100|
        |              3|           1000|
        +---------------+---------------+

        +---------------+---------------+
        |(Int64)        |(Int64)        |
        +---------------+---------------+
        |              1|              2|
        |              2|            300|
        |              3|           1000|
        +---------------+---------------+
    */
    auto phase1 = []() {
        using namespace vectorized;
        OperatorContext ctx;
        auto sink_op = std::make_shared<MockAggsinkOperator>();
        sink_op->_aggregate_evaluators.push_back(vectorized::create_mock_agg_fn_evaluator(
                ctx.pool, MockSlotRef::create_mock_contexts(1, std::make_shared<DataTypeInt64>()),
                false, false));
        sink_op->_pool = &ctx.pool;
        EXPECT_TRUE(sink_op->prepare(&ctx.state).ok());
        sink_op->_probe_expr_ctxs =
                MockSlotRef::create_mock_contexts(0, std::make_shared<DataTypeInt64>());

        auto source_op = std::make_shared<MockAggSourceOperator>();
        source_op->mock_row_descriptor.reset(
                new MockRowDescriptor {{std::make_shared<vectorized::DataTypeInt64>(),
                                        std::make_shared<vectorized::DataTypeInt64>()},
                                       &ctx.pool});
        source_op->_without_key = false;
        source_op->_needs_finalize = false;
        EXPECT_TRUE(source_op->prepare(&ctx.state).ok());

        auto shared_state = init_sink_and_source(sink_op, source_op, ctx);

        {
            vectorized::Block block {
                    ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 2, 2, 2, 3}),
                    ColumnHelper::create_column_with_name<DataTypeInt64>(
                            {1, 1, 100, 100, 100, 1000})};
            auto st = sink_op->sink(&ctx.state, &block, true);
            EXPECT_TRUE(st.ok()) << st.msg();
        }

        {
            vectorized::Block block;
            bool eos = false;
            auto st = source_op->get_block(&ctx.state, &block, &eos);
            EXPECT_TRUE(st.ok()) << st.msg();
            return block;
        }
    };

    auto phase2 = [](vectorized::Block& serialize_block) {
        using namespace vectorized;
        OperatorContext ctx;
        auto sink_op = std::make_shared<MockAggsinkOperator>();
        sink_op->_aggregate_evaluators.push_back(vectorized::create_mock_agg_fn_evaluator(
                ctx.pool, MockSlotRef::create_mock_contexts(1, std::make_shared<DataTypeInt64>()),
                true, false));
        sink_op->_pool = &ctx.pool;
        EXPECT_TRUE(sink_op->prepare(&ctx.state).ok());
        sink_op->_probe_expr_ctxs =
                MockSlotRef::create_mock_contexts(0, std::make_shared<DataTypeInt64>());

        auto source_op = std::make_shared<MockAggSourceOperator>();
        source_op->mock_row_descriptor.reset(
                new MockRowDescriptor {{std::make_shared<vectorized::DataTypeInt64>(),
                                        std::make_shared<vectorized::DataTypeInt64>()},
                                       &ctx.pool});
        source_op->_without_key = false;
        source_op->_needs_finalize = true;
        EXPECT_TRUE(source_op->prepare(&ctx.state).ok());

        auto shared_state = init_sink_and_source(sink_op, source_op, ctx);

        {
            auto st = sink_op->sink(&ctx.state, &serialize_block, true);
            EXPECT_TRUE(st.ok()) << st.msg();
        }

        {
            vectorized::Block block;
            bool eos = false;
            auto st = source_op->get_block(&ctx.state, &block, &eos);
            EXPECT_TRUE(st.ok()) << st.msg();
            EXPECT_TRUE(ColumnHelper::block_equal(
                    block,
                    vectorized::Block {
                            ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3}),
                            ColumnHelper::create_column_with_name<DataTypeInt64>({2, 300, 1000})}));
        }
    };
    auto block = phase1();
    phase2(block);
}

TEST_F(AggOperatorTestWithGroupBy, other_case_1) {
    using namespace vectorized;
    OperatorContext ctx;
    auto sink_op = std::make_shared<MockAggsinkOperator>();
    sink_op->_aggregate_evaluators.push_back(vectorized::create_mock_agg_fn_evaluator(
            ctx.pool, MockSlotRef::create_mock_contexts(1, std::make_shared<DataTypeInt64>()),
            false, false));
    sink_op->_pool = &ctx.pool;
    EXPECT_TRUE(sink_op->prepare(&ctx.state).ok());
    sink_op->_is_merge = true;
    sink_op->_probe_expr_ctxs =
            MockSlotRef::create_mock_contexts(0, std::make_shared<DataTypeInt64>());

    auto source_op = std::make_shared<MockAggSourceOperator>();
    source_op->mock_row_descriptor.reset(
            new MockRowDescriptor {{std::make_shared<vectorized::DataTypeInt64>(),
                                    std::make_shared<vectorized::DataTypeInt64>()},
                                   &ctx.pool});
    source_op->_without_key = false;
    source_op->_needs_finalize = false;
    EXPECT_TRUE(source_op->prepare(&ctx.state).ok());

    auto shared_state = init_sink_and_source(sink_op, source_op, ctx);

    {
        vectorized::Block block {
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 2, 2, 2, 3}),
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 100, 100, 100, 1000})};
        auto st = sink_op->sink(&ctx.state, &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();
    }
}

TEST(AggOperatorTestWithOutGroupBy, other_case_1) {
    using namespace vectorized;
    OperatorContext ctx;

    auto sink_op = create_agg_sink_op(ctx, false, true);

    sink_op->_is_merge = true;

    auto source_op = create_agg_source_op(ctx, true, true);

    auto shared_state = init_sink_and_source(sink_op, source_op, ctx);

    {
        vectorized::Block block = ColumnHelper::create_block<DataTypeInt64>({1, 2, 3});
        auto st = sink_op->sink(&ctx.state, &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();
    }

    {
        vectorized::Block block = ColumnHelper::create_block<DataTypeInt64>({});
        bool eos = false;
        auto st = source_op->get_block(&ctx.state, &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(eos);
        EXPECT_EQ(block.rows(), 1);
        EXPECT_TRUE(
                ColumnHelper::block_equal(block, ColumnHelper::create_block<DataTypeInt64>({6})));
    }
}

TEST(AggOperatorTestWithOutGroupBy, other_case_2) {
    using namespace vectorized;
    OperatorContext ctx;

    auto sink_op = create_agg_sink_op(ctx, false, true);

    sink_op->_is_merge = true;

    auto source_op = create_agg_source_op(ctx, true, true);

    auto shared_state = init_sink_and_source(sink_op, source_op, ctx);

    static_cast<AggSharedState*>(shared_state.get())->make_nullable_keys.push_back(0);

    auto* local_state =
            static_cast<AggLocalState*>(ctx.state.get_local_state(source_op->operator_id()));

    Block block = ColumnHelper::create_block<DataTypeInt64>({1, 2, 3});
    local_state->make_nullable_output_key(&block);
    EXPECT_TRUE(ColumnHelper::block_equal(block, ColumnHelper::create_nullable_block<DataTypeInt64>(
                                                         {1, 2, 3}, {false, false, false})));
}

TEST_F(AggOperatorTestWithGroupBy, other_case_2) {
    using namespace vectorized;
    OperatorContext ctx;
    auto sink_op = std::make_shared<MockAggsinkOperator>();
    sink_op->_aggregate_evaluators.push_back(
            vectorized::create_mock_agg_fn_evaluator(ctx.pool, false, false));
    sink_op->_pool = &ctx.pool;
    EXPECT_TRUE(sink_op->prepare(&ctx.state).ok());
    sink_op->_probe_expr_ctxs = MockSlotRef::create_mock_contexts(
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()));

    auto source_op = std::make_shared<MockAggSourceOperator>();
    source_op->mock_row_descriptor.reset(new MockRowDescriptor {
            {std::make_shared<DataTypeNullable>(std::make_shared<vectorized::DataTypeInt64>()),
             std::make_shared<vectorized::DataTypeInt64>()},
            &ctx.pool});
    source_op->_without_key = false;
    source_op->_needs_finalize = true;
    EXPECT_TRUE(source_op->prepare(&ctx.state).ok());

    auto shared_state = init_sink_and_source(sink_op, source_op, ctx);

    {
        vectorized::Block block = ColumnHelper::create_nullable_block<DataTypeInt64>(
                {1, 2, 3, 1, 2, 3}, {false, false, false, true, true, true});
        auto* local_state =
                static_cast<AggSinkOperatorX::LocalState*>(ctx.state.get_sink_local_state());

        vectorized::ColumnRawPtrs key_columns;
        key_columns.push_back(block.get_by_position(0).column.get());

        local_state->_places.resize(block.rows());
        local_state->_emplace_into_hash_table(local_state->_places.data(), key_columns,
                                              block.rows());

        EXPECT_EQ(local_state->_get_hash_table_size(), 4); // [1,2,3,null]
    }
}

TEST_F(AggOperatorTestWithGroupBy, other_case_3) {
    auto phase1 = []() {
        using namespace vectorized;
        OperatorContext ctx;
        auto sink_op = std::make_shared<MockAggsinkOperator>();
        sink_op->_aggregate_evaluators.push_back(vectorized::create_mock_agg_fn_evaluator(
                ctx.pool, MockSlotRef::create_mock_contexts(1, std::make_shared<DataTypeInt64>()),
                false, false));
        sink_op->_pool = &ctx.pool;
        EXPECT_TRUE(sink_op->prepare(&ctx.state).ok());
        sink_op->_probe_expr_ctxs = MockSlotRef::create_mock_contexts(
                0, std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()));

        auto source_op = std::make_shared<MockAggSourceOperator>();
        source_op->mock_row_descriptor.reset(new MockRowDescriptor {
                {std::make_shared<DataTypeNullable>(std::make_shared<vectorized::DataTypeInt64>()),
                 std::make_shared<vectorized::DataTypeInt64>()},
                &ctx.pool});
        source_op->_without_key = false;
        source_op->_needs_finalize = false;
        EXPECT_TRUE(source_op->prepare(&ctx.state).ok());

        auto shared_state = init_sink_and_source(sink_op, source_op, ctx);

        {
            vectorized::Block block {
                    ColumnHelper::create_nullable_column_with_name<DataTypeInt64>(
                            {1, 1, 2, 2, 2, 3}, {false, false, false, true, false, false}),
                    ColumnHelper::create_column_with_name<DataTypeInt64>(
                            {1, 1, 100, 100, 100, 1000})};
            auto st = sink_op->sink(&ctx.state, &block, true);
            EXPECT_TRUE(st.ok()) << st.msg();
        }

        {
            vectorized::Block block;
            bool eos = false;
            auto st = source_op->get_block(&ctx.state, &block, &eos);
            EXPECT_TRUE(st.ok()) << st.msg();
            return block;
        }
    };

    auto phase2 = [](vectorized::Block& serialize_block) {
        using namespace vectorized;
        OperatorContext ctx;
        auto sink_op = std::make_shared<MockAggsinkOperator>();
        sink_op->_aggregate_evaluators.push_back(vectorized::create_mock_agg_fn_evaluator(
                ctx.pool, MockSlotRef::create_mock_contexts(1, std::make_shared<DataTypeInt64>()),
                true, false));
        sink_op->_pool = &ctx.pool;
        EXPECT_TRUE(sink_op->prepare(&ctx.state).ok());
        sink_op->_probe_expr_ctxs = MockSlotRef::create_mock_contexts(
                0, std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()));

        auto source_op = std::make_shared<MockAggSourceOperator>();
        source_op->mock_row_descriptor.reset(new MockRowDescriptor {
                {std::make_shared<DataTypeNullable>(std::make_shared<vectorized::DataTypeInt64>()),
                 std::make_shared<vectorized::DataTypeInt64>()},
                &ctx.pool});
        source_op->_without_key = false;
        source_op->_needs_finalize = true;
        EXPECT_TRUE(source_op->prepare(&ctx.state).ok());

        auto shared_state = init_sink_and_source(sink_op, source_op, ctx);

        {
            auto st = sink_op->sink(&ctx.state, &serialize_block, true);
            EXPECT_TRUE(st.ok()) << st.msg();
        }

        {
            vectorized::Block block;
            bool eos = false;
            auto st = source_op->get_block(&ctx.state, &block, &eos);
            EXPECT_TRUE(st.ok()) << st.msg();

            std::cout << block.dump_data() << std::endl;
            EXPECT_TRUE(ColumnHelper::block_equal(
                    block, vectorized::Block {
                                   ColumnHelper::create_nullable_column_with_name<DataTypeInt64>(
                                           {1, 2, 3, 0}, {false, false, false, true}),
                                   ColumnHelper::create_column_with_name<DataTypeInt64>(
                                           {2, 200, 1000, 100})}));
        }
    };
    auto block = phase1();
    phase2(block);
}

TEST(AggOperatorTestWithOutGroupBy, other_case_3) {
    using namespace vectorized;
    OperatorContext ctx;

    auto sink_op = create_agg_sink_op(ctx, false, true);

    sink_op->_is_merge = true;

    auto source_op = std::make_shared<MockAggSourceOperator>();
    source_op->mock_row_descriptor.reset(new MockRowDescriptor {
            {std::make_shared<DataTypeNullable>(std::make_shared<vectorized::DataTypeInt64>())},
            &ctx.pool});
    source_op->_without_key = true;
    source_op->_needs_finalize = true;
    EXPECT_TRUE(source_op->prepare(&ctx.state).ok());

    auto shared_state = init_sink_and_source(sink_op, source_op, ctx);

    {
        vectorized::Block block = ColumnHelper::create_block<DataTypeInt64>({1, 2, 3});
        auto st = sink_op->sink(&ctx.state, &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();
    }

    {
        vectorized::Block block = ColumnHelper::create_block<DataTypeInt64>({});
        bool eos = false;
        auto st = source_op->get_block(&ctx.state, &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(eos);
        EXPECT_EQ(block.rows(), 1);
        EXPECT_TRUE(ColumnHelper::block_equal(
                block, ColumnHelper::create_nullable_block<DataTypeInt64>({6}, {false})));
    }
}

} // namespace doris::pipeline
