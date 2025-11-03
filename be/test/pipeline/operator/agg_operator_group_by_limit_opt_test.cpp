
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

struct AggOperatorGroupByLimitOptTestWithGroupBy : public testing::Test {
public:
    void SetUp() override {}
};

TEST_F(AggOperatorGroupByLimitOptTestWithGroupBy, test_need_finalize_without_order_by) {
    /* 
        select column1, sum(column2) from test_table group by column1 limit 3;
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

        limit 3

        hash map [1,2,3]

        +---------------+---------------+
        |column(Int64)  |column(Int64)  |
        +---------------+---------------+
        |              1|              1|
        |              1|              1|
        |              4|            100|
        |              4|            100|
        |              5|            100|
        |              5|           1000|
        +---------------+---------------+

        skip 4 , 5


        +---------------+---------------+
        |(Int64)        |(Int64)        |
        +---------------+---------------+
        |              1|              4|
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
    sink_op->_limit = 3;
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

        std::cout << block.dump_data() << std::endl;
        auto st = sink_op->sink(&ctx.state, &block, false);
        EXPECT_TRUE(st.ok()) << st.msg();
    }

    {
        vectorized::Block block {
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 4, 4, 5, 5}),
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 100, 100, 100, 1000})};
        std::cout << block.dump_data() << std::endl;
        auto st = sink_op->sink(&ctx.state, &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();
    }

    {
        vectorized::Block block;
        bool eos = false;
        auto st = source_op->get_block(&ctx.state, &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        std::cout << block.dump_data() << std::endl;
        EXPECT_TRUE(ColumnHelper::block_equal(
                block,
                vectorized::Block {
                        ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3}),
                        ColumnHelper::create_column_with_name<DataTypeInt64>({4, 300, 1000})}));
    }
}

TEST_F(AggOperatorGroupByLimitOptTestWithGroupBy, test_need_finalize_with_order_by) {
    /* 
        select column1, sum(column2) from test_table group by column1 order by  column1 limit 3;
        

        +---------------+---------------+
        |column(Int64)  |column(Int64)  |
        +---------------+---------------+
        |              1|              1|
        |              2|              2|
        |              3|              3|
        |              4|              4|
        |              5|              5|
        |              6|              6|
        +---------------+---------------+

        limit 3

        heap [1,2,3]

        +---------------+---------------+
        |column(Int64)  |column(Int64)  |
        +---------------+---------------+
        |              1|             10|
        |              2|             20|
        |              7|            100|
        |              8|            100|
        |              9|            100|
        |             10|           1000|
        +---------------+---------------+

        skip 7,8,9,10

        +---------------+---------------+
        |(Int64)        |(Int64)        |
        +---------------+---------------+
        |              1|             11|
        |              2|             22|
        |              3|              3|
        +---------------+---------------+
    */
    using namespace vectorized;
    OperatorContext ctx;
    auto sink_op = std::make_shared<MockAggsinkOperator>();
    sink_op->_aggregate_evaluators.push_back(vectorized::create_mock_agg_fn_evaluator(
            ctx.pool, MockSlotRef::create_mock_contexts(1, std::make_shared<DataTypeInt64>()),
            false, false));
    sink_op->_pool = &ctx.pool;
    sink_op->_limit = 3;
    EXPECT_TRUE(sink_op->prepare(&ctx.state).ok());
    sink_op->_probe_expr_ctxs =
            MockSlotRef::create_mock_contexts(0, std::make_shared<DataTypeInt64>());

    sink_op->_order_directions = {1};
    sink_op->_null_directions = {1};
    sink_op->_do_sort_limit = true;

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
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3, 4, 5, 6}),
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3, 4, 5, 6})};

        std::cout << block.dump_data() << std::endl;
        auto st = sink_op->sink(&ctx.state, &block, false);
        EXPECT_TRUE(st.ok()) << st.msg();
    }

    {
        vectorized::Block block {
                ColumnHelper::create_column_with_name<DataTypeInt64>({0, 2, 7, 8, 9, 10}),
                ColumnHelper::create_column_with_name<DataTypeInt64>({0, 20, 100, 100, 100, 1000})};
        std::cout << block.dump_data() << std::endl;
        auto st = sink_op->sink(&ctx.state, &block, true);
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
                               ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 0}),
                               ColumnHelper::create_column_with_name<DataTypeInt64>({1, 22, 0})}));
    }
}

TEST_F(AggOperatorGroupByLimitOptTestWithGroupBy, test_2_phase_without_order_by) {
    /*
        select column1, sum(column2) from test_table group by column1 limit 3;
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

        limit 3

        hash map [1,2,3]

        +---------------+---------------+
        |column(Int64)  |column(Int64)  |
        +---------------+---------------+
        |              1|              1|
        |              1|              1|
        |              4|            100|
        |              4|            100|
        |              5|            100|
        |              5|           1000|
        +---------------+---------------+

        skip 4 , 5


        +---------------+---------------+
        |(Int64)        |(Int64)        |
        +---------------+---------------+
        |              1|              4|
        |              2|            300|
        |              3|           1000|
        +---------------+---------------+
    */

    using namespace vectorized;

    auto phase1 = [](Block block) {
        OperatorContext ctx1;
        auto sink_op1 = std::make_shared<MockAggsinkOperator>();
        sink_op1->_aggregate_evaluators.push_back(vectorized::create_mock_agg_fn_evaluator(
                ctx1.pool, MockSlotRef::create_mock_contexts(1, std::make_shared<DataTypeInt64>()),
                false, false));
        sink_op1->_pool = &ctx1.pool;
        EXPECT_TRUE(sink_op1->prepare(&ctx1.state).ok());
        sink_op1->_probe_expr_ctxs =
                MockSlotRef::create_mock_contexts(0, std::make_shared<DataTypeInt64>());

        auto source_op1 = std::make_shared<MockAggSourceOperator>();
        source_op1->mock_row_descriptor.reset(
                new MockRowDescriptor {{std::make_shared<vectorized::DataTypeInt64>(),
                                        std::make_shared<vectorized::DataTypeInt64>()},
                                       &ctx1.pool});
        source_op1->_without_key = false;
        source_op1->_needs_finalize = false;
        EXPECT_TRUE(source_op1->prepare(&ctx1.state).ok());

        auto shared_state1 = init_sink_and_source(sink_op1, source_op1, ctx1);

        {
            std::cout << block.dump_data() << std::endl;
            auto st = sink_op1->sink(&ctx1.state, &block, true);
            EXPECT_TRUE(st.ok()) << st.msg();
        }

        vectorized::Block serialize_block;

        {
            bool eos = false;
            auto st = source_op1->get_block(&ctx1.state, &serialize_block, &eos);
            EXPECT_TRUE(st.ok()) << st.msg();
            std::cout << "source_op1 output rows " << serialize_block.rows() << std::endl;
        }

        return serialize_block;
    };

    OperatorContext ctx2;
    auto sink_op2 = std::make_shared<MockAggsinkOperator>();
    sink_op2->_aggregate_evaluators.push_back(vectorized::create_mock_agg_fn_evaluator(
            ctx2.pool, MockSlotRef::create_mock_contexts(1, std::make_shared<DataTypeInt64>()),
            true, false));
    sink_op2->_pool = &ctx2.pool;
    sink_op2->_limit = 3;
    EXPECT_TRUE(sink_op2->prepare(&ctx2.state).ok());
    sink_op2->_probe_expr_ctxs =
            MockSlotRef::create_mock_contexts(0, std::make_shared<DataTypeInt64>());

    auto source_op2 = std::make_shared<MockAggSourceOperator>();
    source_op2->mock_row_descriptor.reset(
            new MockRowDescriptor {{std::make_shared<vectorized::DataTypeInt64>(),
                                    std::make_shared<vectorized::DataTypeInt64>()},
                                   &ctx2.pool});
    source_op2->_without_key = false;
    source_op2->_needs_finalize = true;
    EXPECT_TRUE(source_op2->prepare(&ctx2.state).ok());

    auto shared_state2 = init_sink_and_source(sink_op2, source_op2, ctx2);

    using namespace vectorized;

    {
        vectorized::Block block {
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 2, 2, 2, 3}),
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 100, 100, 100, 1000})};
        auto serialize_block = phase1(block);
        auto st = sink_op2->sink(&ctx2.state, &serialize_block, false);
        EXPECT_TRUE(st.ok()) << st.msg();
    }

    {
        vectorized::Block block {
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 4, 4, 5, 5}),
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 100, 100, 100, 1000})};
        auto serialize_block = phase1(block);
        auto st = sink_op2->sink(&ctx2.state, &serialize_block, true);
        EXPECT_TRUE(st.ok()) << st.msg();
    }

    {
        vectorized::Block block;
        bool eos = false;
        auto st = source_op2->get_block(&ctx2.state, &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        std::cout << block.dump_data() << std::endl;
        EXPECT_TRUE(ColumnHelper::block_equal(
                block,
                vectorized::Block {
                        ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3}),
                        ColumnHelper::create_column_with_name<DataTypeInt64>({4, 300, 1000})}));
    }
}

TEST_F(AggOperatorGroupByLimitOptTestWithGroupBy, test_2_phase_with_order_by) {
    /* 
        select column1, sum(column2) from test_table group by column1 order by  column1 limit 3;
        

        +---------------+---------------+
        |column(Int64)  |column(Int64)  |
        +---------------+---------------+
        |              1|              1|
        |              2|              2|
        |              3|              3|
        |              4|              4|
        |              5|              5|
        |              6|              6|
        +---------------+---------------+

        limit 3

        heap [1,2,3]

        +---------------+---------------+
        |column(Int64)  |column(Int64)  |
        +---------------+---------------+
        |              1|             10|
        |              2|             20|
        |              7|            100|
        |              8|            100|
        |              9|            100|
        |             10|           1000|
        +---------------+---------------+

        skip 7,8,9,10

        +---------------+---------------+
        |(Int64)        |(Int64)        |
        +---------------+---------------+
        |              1|             11|
        |              2|             22|
        |              3|              3|
        +---------------+---------------+
    */

    using namespace vectorized;

    auto phase1 = [](Block block) {
        OperatorContext ctx1;
        auto sink_op1 = std::make_shared<MockAggsinkOperator>();
        sink_op1->_aggregate_evaluators.push_back(vectorized::create_mock_agg_fn_evaluator(
                ctx1.pool, MockSlotRef::create_mock_contexts(1, std::make_shared<DataTypeInt64>()),
                false, false));
        sink_op1->_pool = &ctx1.pool;
        EXPECT_TRUE(sink_op1->prepare(&ctx1.state).ok());
        sink_op1->_probe_expr_ctxs =
                MockSlotRef::create_mock_contexts(0, std::make_shared<DataTypeInt64>());

        auto source_op1 = std::make_shared<MockAggSourceOperator>();
        source_op1->mock_row_descriptor.reset(
                new MockRowDescriptor {{std::make_shared<vectorized::DataTypeInt64>(),
                                        std::make_shared<vectorized::DataTypeInt64>()},
                                       &ctx1.pool});
        source_op1->_without_key = false;
        source_op1->_needs_finalize = false;
        EXPECT_TRUE(source_op1->prepare(&ctx1.state).ok());

        auto shared_state1 = init_sink_and_source(sink_op1, source_op1, ctx1);

        {
            std::cout << block.dump_data() << std::endl;
            auto st = sink_op1->sink(&ctx1.state, &block, true);
            EXPECT_TRUE(st.ok()) << st.msg();
        }

        vectorized::Block serialize_block;

        {
            bool eos = false;
            auto st = source_op1->get_block(&ctx1.state, &serialize_block, &eos);
            EXPECT_TRUE(st.ok()) << st.msg();
            std::cout << "source_op1 output rows " << serialize_block.rows() << std::endl;
        }

        return serialize_block;
    };

    OperatorContext ctx2;
    auto sink_op2 = std::make_shared<MockAggsinkOperator>();
    sink_op2->_aggregate_evaluators.push_back(vectorized::create_mock_agg_fn_evaluator(
            ctx2.pool, MockSlotRef::create_mock_contexts(1, std::make_shared<DataTypeInt64>()),
            true, false));
    sink_op2->_pool = &ctx2.pool;
    sink_op2->_limit = 3;
    sink_op2->_order_directions = {1};
    sink_op2->_null_directions = {1};
    sink_op2->_do_sort_limit = true;

    EXPECT_TRUE(sink_op2->prepare(&ctx2.state).ok());
    sink_op2->_probe_expr_ctxs =
            MockSlotRef::create_mock_contexts(0, std::make_shared<DataTypeInt64>());

    auto source_op2 = std::make_shared<MockAggSourceOperator>();
    source_op2->mock_row_descriptor.reset(
            new MockRowDescriptor {{std::make_shared<vectorized::DataTypeInt64>(),
                                    std::make_shared<vectorized::DataTypeInt64>()},
                                   &ctx2.pool});
    source_op2->_without_key = false;
    source_op2->_needs_finalize = true;
    EXPECT_TRUE(source_op2->prepare(&ctx2.state).ok());

    auto shared_state2 = init_sink_and_source(sink_op2, source_op2, ctx2);

    using namespace vectorized;

    {
        vectorized::Block block {
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3, 4, 5, 6}),
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3, 4, 5, 6})};
        auto serialize_block = phase1(block);
        auto st = sink_op2->sink(&ctx2.state, &serialize_block, false);
        EXPECT_TRUE(st.ok()) << st.msg();
    }

    {
        vectorized::Block block {
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 7, 8, 9, 10}),
                ColumnHelper::create_column_with_name<DataTypeInt64>(
                        {10, 20, 100, 100, 100, 1000})};
        auto serialize_block = phase1(block);
        auto st = sink_op2->sink(&ctx2.state, &serialize_block, true);
        EXPECT_TRUE(st.ok()) << st.msg();
    }

    {
        vectorized::Block block;
        bool eos = false;
        auto st = source_op2->get_block(&ctx2.state, &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        std::cout << block.dump_data() << std::endl;
        EXPECT_TRUE(ColumnHelper::block_equal(
                block, vectorized::Block {
                               ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3}),
                               ColumnHelper::create_column_with_name<DataTypeInt64>({11, 22, 3})}));
    }
}

TEST_F(AggOperatorGroupByLimitOptTestWithGroupBy, other_case_1) {
    using namespace vectorized;
    OperatorContext ctx;
    auto sink_op = std::make_shared<MockAggsinkOperator>();
    sink_op->_aggregate_evaluators.push_back(vectorized::create_mock_agg_fn_evaluator(
            ctx.pool, MockSlotRef::create_mock_contexts(1, std::make_shared<DataTypeInt64>()),
            false, false));
    sink_op->_pool = &ctx.pool;
    sink_op->_limit = 3;
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
    source_op->_needs_finalize = true;
    EXPECT_TRUE(source_op->prepare(&ctx.state).ok());

    auto shared_state = init_sink_and_source(sink_op, source_op, ctx);

    {
        vectorized::Block block {
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 2, 2, 2, 3}),
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 100, 100, 100, 1000})};

        std::cout << block.dump_data() << std::endl;
        auto st = sink_op->sink(&ctx.state, &block, false);
        EXPECT_TRUE(st.ok()) << st.msg();
    }

    {
        vectorized::Block block {
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 4, 4, 5, 5}),
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 100, 100, 100, 1000})};
        std::cout << block.dump_data() << std::endl;
        auto st = sink_op->sink(&ctx.state, &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();
    }

    {
        vectorized::Block block;
        bool eos = false;
        auto st = source_op->get_block(&ctx.state, &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        std::cout << block.dump_data() << std::endl;
        EXPECT_TRUE(ColumnHelper::block_equal(
                block,
                vectorized::Block {
                        ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3}),
                        ColumnHelper::create_column_with_name<DataTypeInt64>({4, 300, 1000})}));
    }
}

TEST_F(AggOperatorGroupByLimitOptTestWithGroupBy, other_case_2) {
    using namespace vectorized;
    OperatorContext ctx;
    auto sink_op = std::make_shared<MockAggsinkOperator>();
    sink_op->_aggregate_evaluators.push_back(vectorized::create_mock_agg_fn_evaluator(
            ctx.pool, MockSlotRef::create_mock_contexts(1, std::make_shared<DataTypeInt64>()),
            false, false));
    sink_op->_pool = &ctx.pool;
    sink_op->_limit = 3;
    EXPECT_TRUE(sink_op->prepare(&ctx.state).ok());
    sink_op->_probe_expr_ctxs = MockSlotRef::create_mock_contexts(
            0, std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()));

    sink_op->_order_directions = {1};
    sink_op->_null_directions = {0};
    sink_op->_do_sort_limit = true;

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
        vectorized::Block block {
                ColumnHelper::create_nullable_column_with_name<DataTypeInt64>(
                        {1, 2, 3, 4, 5, 6}, {false, false, false, false, false, false}),
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3, 4, 5, 6})};

        std::cout << block.dump_data() << std::endl;
        auto st = sink_op->sink(&ctx.state, &block, false);
        EXPECT_TRUE(st.ok()) << st.msg();
    }

    {
        vectorized::Block block {
                ColumnHelper::create_nullable_column_with_name<DataTypeInt64>(
                        {0, 2, 7, 8, 9, 10}, {true, false, false, false, false, false}),
                ColumnHelper::create_column_with_name<DataTypeInt64>({0, 20, 100, 100, 100, 1000})};
        std::cout << block.dump_data() << std::endl;
        auto st = sink_op->sink(&ctx.state, &block, true);
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
                                       {1, 2, 0}, {false, false, true}),
                               ColumnHelper::create_column_with_name<DataTypeInt64>({1, 22, 0})}));
    }
}

TEST_F(AggOperatorGroupByLimitOptTestWithGroupBy, other_case_3) {
    using namespace vectorized;
    OperatorContext ctx;
    auto sink_op = std::make_shared<MockAggsinkOperator>();
    sink_op->_aggregate_evaluators.push_back(vectorized::create_mock_agg_fn_evaluator(
            ctx.pool, MockSlotRef::create_mock_contexts(1, std::make_shared<DataTypeInt64>()),
            false, false));
    sink_op->_pool = &ctx.pool;
    sink_op->_limit = 3;
    EXPECT_TRUE(sink_op->prepare(&ctx.state).ok());
    sink_op->_probe_expr_ctxs = MockSlotRef::create_mock_contexts(
            0, std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()));

    sink_op->_order_directions = {1};
    sink_op->_null_directions = {0};
    sink_op->_do_sort_limit = true;

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
        vectorized::Block block {
                ColumnHelper::create_nullable_column_with_name<DataTypeInt64>(
                        {1, 2, 3, 4, 5, 6}, {false, false, false, false, false, true}),
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3, 4, 5, 0})};

        std::cout << block.dump_data() << std::endl;
        auto st = sink_op->sink(&ctx.state, &block, false);
        EXPECT_TRUE(st.ok()) << st.msg();
    }

    {
        vectorized::Block block {
                ColumnHelper::create_nullable_column_with_name<DataTypeInt64>(
                        {0, 2, 7, 8, 9, 10}, {true, false, false, false, false, false}),
                ColumnHelper::create_column_with_name<DataTypeInt64>({0, 20, 100, 100, 100, 1000})};
        std::cout << block.dump_data() << std::endl;
        auto st = sink_op->sink(&ctx.state, &block, true);
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
                                       {1, 2, 0}, {false, false, true}),
                               ColumnHelper::create_column_with_name<DataTypeInt64>({1, 22, 0})}));
    }
}

} // namespace doris::pipeline
