
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

#include <functional>
#include <map>
#include <memory>
#include <vector>

#include "common/config.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "exec/exchange/local_exchange_source_operator.h"
#include "exec/operator/aggregation_sink_operator.h"
#include "exec/operator/aggregation_source_operator.h"
#include "exec/operator/assert_num_rows_operator.h"
#include "exec/operator/bucketed_aggregation_sink_operator.h"
#include "exec/operator/bucketed_aggregation_source_operator.h"
#include "exec/operator/mock_operator.h"
#include "exec/operator/operator_helper.h"
#include "exec/pipeline/dependency.h"
#include "exec/pipeline/pipeline.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_agg_fn_evaluator.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_slot_ref.h"
#include "util/debug_points.h"

namespace doris {

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

class MockDistributionOperator final : public OperatorX<MockLocalState> {
public:
    MockDistributionOperator(ExchangeType exchange_type) : _exchange_type(exchange_type) {}

    Status get_block_impl(RuntimeState* /*state*/, Block* /*block*/, bool* eos) override {
        *eos = true;
        return Status::OK();
    }

    DataDistribution required_data_distribution(RuntimeState* /*state*/) const override {
        return {_exchange_type};
    }

private:
    ExchangeType _exchange_type;
};

std::shared_ptr<AggSinkOperatorX> create_agg_sink_op(OperatorContext& ctx, bool is_merge,
                                                     bool without_key) {
    auto op = std::make_shared<MockAggsinkOperator>();
    op->_aggregate_evaluators.push_back(
            create_mock_agg_fn_evaluator(ctx.pool, is_merge, without_key));
    op->_pool = &ctx.pool;
    EXPECT_TRUE(op->prepare(&ctx.state).ok());
    return op;
}

TEST(AggOperatorRequiredDistributionTest, require_hash_shuffle_after_non_hash_child_exchange) {
    OperatorContext ctx;
    TQueryOptions query_options;
    query_options.__set_enable_local_exchange_before_agg(false);
    ctx.state.set_query_options(query_options);
    auto sink_op = std::make_shared<MockAggsinkOperator>();
    sink_op->_partition_exprs.emplace_back();
    sink_op->_needs_finalize = false;
    OperatorPtr child =
            std::make_shared<MockDistributionOperator>(ExchangeType::ADAPTIVE_PASSTHROUGH);
    sink_op->_child = child;

    const auto distribution = sink_op->required_data_distribution(&ctx.state);
    EXPECT_EQ(ExchangeType::HASH_SHUFFLE, distribution.distribution_type);
}

TEST(AggOperatorRequiredDistributionTest, toggle_hash_shuffle_for_safe_child) {
    OperatorContext ctx;
    auto sink_op = std::make_shared<MockAggsinkOperator>();
    sink_op->_partition_exprs.emplace_back();
    sink_op->_needs_finalize = false;
    sink_op->_child = std::make_shared<MockDistributionOperator>(ExchangeType::NOOP);

    TQueryOptions query_options;
    query_options.__set_enable_local_exchange_before_agg(false);
    ctx.state.set_query_options(query_options);
    EXPECT_EQ(ExchangeType::NOOP,
              sink_op->required_data_distribution(&ctx.state).distribution_type);

    query_options.__set_enable_local_exchange_before_agg(true);
    ctx.state.set_query_options(query_options);
    EXPECT_EQ(ExchangeType::HASH_SHUFFLE,
              sink_op->required_data_distribution(&ctx.state).distribution_type);
}

TEST(AggOperatorRequiredDistributionTest, require_hash_shuffle_after_non_hash_local_exchange) {
    OperatorContext ctx;
    TQueryOptions query_options;
    query_options.__set_enable_local_exchange_before_agg(false);
    ctx.state.set_query_options(query_options);
    auto sink_op = std::make_shared<MockAggsinkOperator>();
    sink_op->_needs_finalize = false;
    OperatorPtr child = std::make_shared<LocalExchangeSourceOperatorX>();
    EXPECT_TRUE(child->init(ExchangeType::ADAPTIVE_PASSTHROUGH).ok());
    sink_op->_child = child;

    TExpr distinct_agg_expr;
    distinct_agg_expr.nodes.emplace_back();
    distinct_agg_expr.nodes[0].fn.name.function_name = "multi_distinct_count";
    TPlanNode tnode;
    tnode.agg_node.aggregate_functions.push_back(distinct_agg_expr);
    tnode.__set_distribute_expr_lists({{TExpr {}}});
    sink_op->update_operator(tnode, false, false);

    const auto distribution = sink_op->required_data_distribution(&ctx.state);
    EXPECT_EQ(ExchangeType::HASH_SHUFFLE, distribution.distribution_type);

    Pipeline pipeline(0, 4, 4);
    EXPECT_TRUE(pipeline.add_operator(child, 0).ok());
    pipeline.set_data_distribution(DataDistribution(ExchangeType::HASH_SHUFFLE));
    EXPECT_TRUE(pipeline.need_to_local_exchange(distribution, 1));
}

TEST(AggOperatorRequiredDistributionTest, bucketed_agg_sink_passthrough_after_serial_child) {
    OperatorContext ctx;
    DescriptorTbl descs;
    auto sink_op = std::make_shared<BucketedAggSinkOperatorX>(&ctx.pool, 0, 0, TPlanNode {}, descs);

    // Each instance aggregates independently, so a non-serial child needs no local exchange.
    auto child = std::make_shared<MockDistributionOperator>(ExchangeType::NOOP);
    sink_op->_child = child;
    EXPECT_EQ(ExchangeType::NOOP,
              sink_op->required_data_distribution(&ctx.state).distribution_type);

    // A serial child must be fanned out, otherwise the sink pipeline runs with one task.
    child->set_serial_operator();
    EXPECT_EQ(ExchangeType::PASSTHROUGH,
              sink_op->required_data_distribution(&ctx.state).distribution_type);
}

// An expression that may block in execute(), like an AI or remote function.
class MockBlockableExpr final : public VSlotRef {
public:
    MockBlockableExpr() { _node_type = TExprNodeType::SLOT_REF; }

    Status execute(VExprContext* context, Block* block, int* result_column_id) const override {
        *result_column_id = 0;
        return Status::OK();
    }
    const std::string& expr_name() const override { return _name; }
    bool is_blockable() const override { return true; }

private:
    std::string _name = "MockBlockableExpr";
};

TEST(AggOperatorBlockableTest, bucketed_agg_follows_blockable_aggregate) {
    OperatorContext sink_ctx;
    OperatorContext source_ctx;
    DescriptorTbl descs;

    auto sink_op =
            std::make_shared<BucketedAggSinkOperatorX>(&sink_ctx.pool, 0, 0, TPlanNode {}, descs);
    sink_op->_aggregate_evaluators.push_back(create_mock_agg_fn_evaluator(sink_ctx.pool));
    // is_blockable() is asked when the task is submitted, before the local state is opened.
    sink_ctx.state.emplace_sink_local_state(
            0, BucketedAggSinkLocalState::create_unique(sink_op.get(), &sink_ctx.state));

    MockDescriptorTbl source_descs {{std::make_shared<DataTypeInt64>()}, &source_ctx.pool};
    TPlanNode source_tnode;
    source_tnode.row_tuples.push_back(0);
    source_tnode.nullable_tuples.push_back(false);
    auto source_op = std::make_shared<BucketedAggSourceOperatorX>(&source_ctx.pool, source_tnode, 0,
                                                                  source_descs);
    source_op->set_sink_operator(sink_op);
    // The sink of the source pipeline itself never blocks.
    auto downstream_sink_op =
            std::make_shared<BucketedAggSinkOperatorX>(&source_ctx.pool, 1, 1, TPlanNode {}, descs);
    source_ctx.state.emplace_sink_local_state(
            1,
            BucketedAggSinkLocalState::create_unique(downstream_sink_op.get(), &source_ctx.state));

    EXPECT_FALSE(sink_op->has_blockable_aggregate());
    EXPECT_FALSE(sink_op->is_blockable(&sink_ctx.state));
    EXPECT_FALSE(source_op->is_blockable(&source_ctx.state));

    sink_op->_aggregate_evaluators.push_back(create_mock_agg_fn_evaluator(
            sink_ctx.pool, {VExprContext::create_shared(std::make_shared<MockBlockableExpr>())}));

    EXPECT_TRUE(sink_op->has_blockable_aggregate());
    EXPECT_TRUE(sink_op->is_blockable(&sink_ctx.state));
    // The source merges and finalizes the same aggregate functions.
    EXPECT_TRUE(source_op->is_blockable(&source_ctx.state));
}

// Two sink instances and one running source instance on a single BucketedAggSharedState,
// wired the same way as PipelineFragmentContext does it. The aggregation is
// "select key, sum(key) group by key" over one Int64 column.
struct BucketedAggOperatorTest : public testing::Test {
    static constexpr int SOURCE_OPERATOR_ID = 0;
    static constexpr int NUM_INSTANCES = 2;
    static constexpr const char* BEFORE_BLOCK_DEBUG_POINT =
            "BucketedAggLocalState._get_results.before_block";

    void SetUp() override {
        sink_op = std::make_shared<BucketedAggSinkOperatorX>(&pool, 1, SOURCE_OPERATOR_ID,
                                                             TPlanNode {}, DescriptorTbl {});
        auto* evaluator = create_mock_agg_fn_evaluator(pool);
        sink_op->_aggregate_evaluators.push_back(evaluator);
        sink_op->_probe_expr_ctxs =
                MockSlotRef::create_mock_contexts(std::make_shared<DataTypeInt64>());
        sink_op->_offsets_of_aggregate_states = {0};
        sink_op->_total_size_of_aggregate_states = evaluator->function()->size_of_data();
        sink_op->_align_aggregate_states = evaluator->function()->align_of_data();

        MockDescriptorTbl source_descs {
                {std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt64>()}, &pool};
        TPlanNode source_tnode;
        source_tnode.row_tuples.push_back(0);
        source_tnode.nullable_tuples.push_back(false);
        source_tnode.limit = -1;
        source_tnode.bucketed_agg_node.need_finalize = true;
        source_op = std::make_shared<BucketedAggSourceOperatorX>(&pool, source_tnode,
                                                                 SOURCE_OPERATOR_ID, source_descs);
        source_op->set_sink_operator(sink_op);

        shared_state = BucketedAggSharedState::create_shared();
        for (int i = 0; i < NUM_INSTANCES; ++i) {
            auto sink_dep = std::make_shared<Dependency>(SOURCE_OPERATOR_ID, 0,
                                                         "BUCKETED_AGG_SINK_DEPENDENCY");
            sink_dep->set_shared_state(shared_state.get());
            shared_state->sink_deps.push_back(sink_dep);
        }
        shared_state->create_source_dependencies(NUM_INSTANCES, SOURCE_OPERATOR_ID, 0,
                                                 "BUCKETED_AGG_SOURCE");
        shared_state_map.insert({SOURCE_OPERATOR_ID, {shared_state, shared_state->sink_deps}});

        for (int i = 0; i < NUM_INSTANCES; ++i) {
            auto& ctx = sink_ctxs[i];
            ctx.state.set_task_num(NUM_INSTANCES);
            auto local_state = BucketedAggSinkLocalState::create_unique(sink_op.get(), &ctx.state);
            LocalSinkStateInfo info {.task_idx = i,
                                     .parent_profile = &ctx.profile,
                                     .sender_id = 0,
                                     .shared_state = nullptr,
                                     .shared_state_map = shared_state_map,
                                     .tsink = TDataSink {}};
            ASSERT_TRUE(local_state->init(&ctx.state, info).ok());
            ctx.state.emplace_sink_local_state(sink_op->operator_id(), std::move(local_state));
            ASSERT_TRUE(ctx.state.get_sink_local_state()->open(&ctx.state).ok());
        }

        auto local_state = BucketedAggLocalState::create_unique(&source_ctx.state, source_op.get());
        LocalStateInfo info {.parent_profile = &source_ctx.profile,
                             .scan_ranges = {},
                             .shared_state = nullptr,
                             .shared_state_map = shared_state_map,
                             .task_idx = 0};
        ASSERT_TRUE(local_state->init(&source_ctx.state, info).ok());
        source_ctx.state.resize_op_id_to_local_state(-100);
        source_ctx.state.emplace_local_state(source_op->operator_id(), std::move(local_state));
        ASSERT_TRUE(source_ctx.state.get_local_state(source_op->operator_id())
                            ->open(&source_ctx.state)
                            .ok());

        source_dep = shared_state->source_deps[0].get();

        saved_enable_debug_points = config::enable_debug_points;
        config::enable_debug_points = true;
    }

    void TearDown() override {
        DebugPoints::instance()->remove(BEFORE_BLOCK_DEBUG_POINT);
        config::enable_debug_points = saved_enable_debug_points;
    }

    Status sink(int instance, const std::vector<int64_t>& keys, bool eos) {
        Block block = ColumnHelper::create_block<DataTypeInt64>(keys);
        return sink_op->sink(&sink_ctxs[instance].state, &block, eos);
    }

    // Runs the source instance the way its pipeline task would: the task is only scheduled
    // while the source dependency is ready, so a blocked dependency with nobody left to wake
    // it up means the query hangs.
    std::map<int64_t, int64_t> read_until_eos() {
        std::map<int64_t, int64_t> result;
        bool eos = false;
        for (int round = 0; round < 1000 && !eos; ++round) {
            if (!source_dep->ready()) {
                ADD_FAILURE() << "the source is blocked and will never be woken up";
                break;
            }
            Block block;
            auto st = source_op->get_block(&source_ctx.state, &block, &eos);
            if (!st.ok()) {
                ADD_FAILURE() << st.msg();
                break;
            }
            if (block.rows() == 0) {
                continue;
            }
            const auto& keys =
                    assert_cast<const ColumnInt64&>(*block.get_by_position(0).column).get_data();
            const auto& sums =
                    assert_cast<const ColumnInt64&>(*block.get_by_position(1).column).get_data();
            for (size_t i = 0; i < block.rows(); ++i) {
                EXPECT_TRUE(result.emplace(keys[i], sums[i]).second) << "duplicate key " << keys[i];
            }
        }
        EXPECT_TRUE(eos);
        return result;
    }

    // Declared first: the shared state destroys its aggregate states with evaluators owned
    // by the pool.
    ObjectPool pool;
    std::shared_ptr<BucketedAggSinkOperatorX> sink_op;
    std::shared_ptr<BucketedAggSourceOperatorX> source_op;
    std::shared_ptr<BucketedAggSharedState> shared_state;
    std::map<int,
             std::pair<std::shared_ptr<BasicSharedState>, std::vector<std::shared_ptr<Dependency>>>>
            shared_state_map;
    OperatorContext sink_ctxs[NUM_INSTANCES];
    OperatorContext source_ctx;
    Dependency* source_dep = nullptr;
    bool saved_enable_debug_points = false;
};

// With one sink still running the source has nothing to output, so it blocks itself. The
// last sink then finds the dependency blocked and wakes the source up.
TEST_F(BucketedAggOperatorTest, source_blocks_until_last_sink_finishes) {
    ASSERT_TRUE(sink(0, {1, 2, 3}, true).ok());
    ASSERT_TRUE(source_dep->ready());

    Block block;
    bool eos = false;
    auto st = source_op->get_block(&source_ctx.state, &block, &eos);
    ASSERT_TRUE(st.ok()) << st.msg();
    EXPECT_EQ(block.rows(), 0);
    EXPECT_FALSE(eos);
    EXPECT_FALSE(source_dep->ready());

    ASSERT_TRUE(sink(1, {1, 2, 4}, true).ok());
    EXPECT_TRUE(source_dep->ready());

    EXPECT_EQ(read_until_eos(), (std::map<int64_t, int64_t> {{1, 2}, {2, 4}, {3, 3}, {4, 4}}));
}

// The last sink finishes after the source has scanned all buckets but before it blocks.
// The source dependency is still ready at that moment, so the sink's set_ready() does
// nothing and no later event will wake the source up: the generation re-check after
// block() is the only thing that keeps the source runnable.
TEST_F(BucketedAggOperatorTest, last_sink_finishes_between_source_scan_and_block) {
    ASSERT_TRUE(sink(0, {1, 2, 3}, true).ok());
    ASSERT_TRUE(source_dep->ready());

    int injected = 0;
    std::function<void()> finish_last_sink = [&]() {
        ++injected;
        EXPECT_TRUE(source_dep->ready());
        const auto generation = shared_state->state_generation.load();
        auto st = sink(1, {1, 2, 4}, true);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_EQ(shared_state->state_generation.load(), generation + 1);
    };
    DebugPoints::instance()->add_with_callback(BEFORE_BLOCK_DEBUG_POINT, finish_last_sink);

    Block block;
    bool eos = false;
    auto st = source_op->get_block(&source_ctx.state, &block, &eos);
    DebugPoints::instance()->remove(BEFORE_BLOCK_DEBUG_POINT);
    ASSERT_TRUE(st.ok()) << st.msg();
    EXPECT_EQ(injected, 1);
    EXPECT_EQ(block.rows(), 0);
    EXPECT_FALSE(eos);
    EXPECT_TRUE(source_dep->ready());

    EXPECT_EQ(read_until_eos(), (std::map<int64_t, int64_t> {{1, 2}, {2, 4}, {3, 3}, {4, 4}}));
}

std::shared_ptr<AggSourceOperatorX> create_agg_source_op(OperatorContext& ctx, bool without_key,
                                                         bool needs_finalize) {
    auto op = std::make_shared<MockAggSourceOperator>();
    op->mock_row_descriptor.reset(
            new MockRowDescriptor {{std::make_shared<DataTypeInt64>()}, &ctx.pool});
    op->_without_key = without_key;
    op->_needs_finalize = needs_finalize;
    EXPECT_TRUE(op->prepare(&ctx.state).ok());
    return op;
}

TEST(AggOperatorTestWithOutGroupBy, test_need_finalize) {
    OperatorContext ctx;

    auto sink_op = create_agg_sink_op(ctx, false, true);

    auto source_op = create_agg_source_op(ctx, true, true);

    auto shared_state = init_sink_and_source(sink_op, source_op, ctx);

    {
        Block block = ColumnHelper::create_block<DataTypeInt64>({1, 2, 3});
        auto st = sink_op->sink(&ctx.state, &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();
    }

    {
        Block block = ColumnHelper::create_block<DataTypeInt64>({});
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
    OperatorContext ctx;

    auto sink_op = create_agg_sink_op(ctx, false, true);

    auto source_op = create_agg_source_op(ctx, true, false);

    auto shared_state = init_sink_and_source(sink_op, source_op, ctx);

    {
        Block block = ColumnHelper::create_block<DataTypeInt64>({1, 2, 3});
        auto st = sink_op->sink(&ctx.state, &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();
    }

    {
        Block block = ColumnHelper::create_block<DataTypeInt64>({});
        bool eos = false;
        auto st = source_op->get_block(&ctx.state, &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(eos);
        EXPECT_EQ(block.rows(), 1);
        EXPECT_TRUE(
                check_and_get_column<ColumnFixedLengthObject>(*block.get_by_position(0).column));
    }
}

Block test_agg_1_phase(Block origin_block) {
    OperatorContext ctx;

    auto sink_op = create_agg_sink_op(ctx, false, true);

    auto source_op = create_agg_source_op(ctx, true, false);

    auto shared_state = init_sink_and_source(sink_op, source_op, ctx);

    EXPECT_TRUE(sink_op->sink(&ctx.state, &origin_block, true).ok());

    Block serialize_block = ColumnHelper::create_block<DataTypeInt64>({});

    bool eos = false;
    EXPECT_TRUE(source_op->get_block(&ctx.state, &serialize_block, &eos).ok());
    EXPECT_TRUE(eos);
    EXPECT_EQ(serialize_block.rows(), 1);
    EXPECT_TRUE(check_and_get_column<ColumnFixedLengthObject>(
            *serialize_block.get_by_position(0).column));

    return serialize_block;
}

void test_agg_2_phase(Block serialize_block) {
    OperatorContext ctx;

    auto sink_op = create_agg_sink_op(ctx, true, false);

    auto source_op = create_agg_source_op(ctx, true, true);

    auto shared_state2 = init_sink_and_source(sink_op, source_op, ctx);

    EXPECT_TRUE(sink_op->sink(&ctx.state, &serialize_block, true).ok());

    Block result_block = ColumnHelper::create_block<DataTypeInt64>({});

    bool eos = false;
    EXPECT_TRUE(source_op->get_block(&ctx.state, &result_block, &eos).ok());

    EXPECT_TRUE(eos);
    EXPECT_EQ(result_block.rows(), 1);
    EXPECT_TRUE(ColumnHelper::block_equal(result_block,
                                          ColumnHelper::create_block<DataTypeInt64>({6})));
}

TEST(AggOperatorTestWithOutGroupBy, test_2_phase) {
    auto serialize_block = test_agg_1_phase(ColumnHelper::create_block<DataTypeInt64>({1, 2, 3}));
    test_agg_2_phase(serialize_block);
}

TEST(AggOperatorTestWithOutGroupBy, test_multi_input) {
    OperatorContext ctx;
    auto sink_op = std::make_shared<MockAggsinkOperator>();
    sink_op->_aggregate_evaluators.push_back(create_mock_agg_fn_evaluator(
            ctx.pool, MockSlotRef::create_mock_contexts(0, std::make_shared<const DataTypeInt64>()),
            false, true));
    sink_op->_aggregate_evaluators.push_back(create_mock_agg_fn_evaluator(
            ctx.pool, MockSlotRef::create_mock_contexts(1, std::make_shared<const DataTypeInt64>()),
            false, true));
    sink_op->_pool = &ctx.pool;
    EXPECT_TRUE(sink_op->prepare(&ctx.state).ok());

    auto source_op = std::make_shared<MockAggSourceOperator>();
    source_op->mock_row_descriptor.reset(new MockRowDescriptor {
            {std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt64>()}, &ctx.pool});
    source_op->_without_key = true;
    source_op->_needs_finalize = true;
    EXPECT_TRUE(source_op->prepare(&ctx.state).ok());

    auto shared_state = init_sink_and_source(sink_op, source_op, ctx);

    {
        Block block {ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3}),
                     ColumnHelper::create_column_with_name<DataTypeInt64>({4, 5, 6})};
        auto st = sink_op->sink(&ctx.state, &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();
    }

    {
        Block block;
        bool eos = false;
        auto st = source_op->get_block(&ctx.state, &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(ColumnHelper::block_equal(
                block, Block {ColumnHelper::create_column_with_name<DataTypeInt64>({6}),
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

    OperatorContext ctx;
    auto sink_op = std::make_shared<MockAggsinkOperator>();
    sink_op->_aggregate_evaluators.push_back(create_mock_agg_fn_evaluator(ctx.pool, false, false));
    sink_op->_pool = &ctx.pool;
    EXPECT_TRUE(sink_op->prepare(&ctx.state).ok());
    sink_op->_probe_expr_ctxs =
            MockSlotRef::create_mock_contexts(std::make_shared<DataTypeInt64>());

    auto source_op = std::make_shared<MockAggSourceOperator>();
    source_op->mock_row_descriptor.reset(new MockRowDescriptor {
            {std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt64>()}, &ctx.pool});
    source_op->_without_key = false;
    source_op->_needs_finalize = true;
    EXPECT_TRUE(source_op->prepare(&ctx.state).ok());

    auto shared_state = init_sink_and_source(sink_op, source_op, ctx);

    {
        Block block = ColumnHelper::create_block<DataTypeInt64>({1, 2, 3, 1, 2, 3});
        auto st = sink_op->sink(&ctx.state, &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();
    }

    {
        Block block;
        bool eos = false;
        auto st = source_op->get_block(&ctx.state, &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(ColumnHelper::block_equal(
                block, Block {ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3}),
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
    OperatorContext ctx;
    auto sink_op = std::make_shared<MockAggsinkOperator>();
    sink_op->_aggregate_evaluators.push_back(create_mock_agg_fn_evaluator(
            ctx.pool, MockSlotRef::create_mock_contexts(1, std::make_shared<DataTypeInt64>()),
            false, false));
    sink_op->_pool = &ctx.pool;
    EXPECT_TRUE(sink_op->prepare(&ctx.state).ok());
    sink_op->_probe_expr_ctxs =
            MockSlotRef::create_mock_contexts(0, std::make_shared<DataTypeInt64>());

    auto source_op = std::make_shared<MockAggSourceOperator>();
    source_op->mock_row_descriptor.reset(new MockRowDescriptor {
            {std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt64>()}, &ctx.pool});
    source_op->_without_key = false;
    source_op->_needs_finalize = true;
    EXPECT_TRUE(source_op->prepare(&ctx.state).ok());

    auto shared_state = init_sink_and_source(sink_op, source_op, ctx);

    {
        Block block {
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 2, 2, 2, 3}),
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 100, 100, 100, 1000})};
        auto st = sink_op->sink(&ctx.state, &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();
    }

    {
        Block block;
        bool eos = false;
        auto st = source_op->get_block(&ctx.state, &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(ColumnHelper::block_equal(
                block,
                Block {ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3}),
                       ColumnHelper::create_column_with_name<DataTypeInt64>({2, 300, 1000})}));
    }
}

TEST_F(AggOperatorTestWithGroupBy, test_need_finalize_mem_reuse_with_shared_output_columns) {
    OperatorContext ctx;
    auto sink_op = std::make_shared<MockAggsinkOperator>();
    sink_op->_aggregate_evaluators.push_back(create_mock_agg_fn_evaluator(
            ctx.pool, MockSlotRef::create_mock_contexts(1, std::make_shared<DataTypeInt64>()),
            false, false));
    sink_op->_pool = &ctx.pool;
    EXPECT_TRUE(sink_op->prepare(&ctx.state).ok());
    sink_op->_probe_expr_ctxs =
            MockSlotRef::create_mock_contexts(0, std::make_shared<DataTypeInt64>());

    auto source_op = std::make_shared<MockAggSourceOperator>();
    source_op->mock_row_descriptor.reset(new MockRowDescriptor {
            {std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt64>()}, &ctx.pool});
    source_op->_without_key = false;
    source_op->_needs_finalize = true;
    EXPECT_TRUE(source_op->prepare(&ctx.state).ok());

    auto shared_state = init_sink_and_source(sink_op, source_op, ctx);

    {
        Block block {
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 2, 2, 2, 3}),
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 100, 100, 100, 1000})};
        auto st = sink_op->sink(&ctx.state, &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();
    }

    Block block {ColumnHelper::create_column_with_name<DataTypeInt64>({}),
                 ColumnHelper::create_column_with_name<DataTypeInt64>({})};
    auto old_key_column = block.get_by_position(0).column;
    auto old_value_column = block.get_by_position(1).column;
    bool eos = false;
    auto st = source_op->get_block(&ctx.state, &block, &eos);
    ASSERT_TRUE(st.ok()) << st.to_string();

    EXPECT_TRUE(eos);
    EXPECT_EQ(old_key_column->size(), 0);
    EXPECT_EQ(old_value_column->size(), 0);
    EXPECT_TRUE(ColumnHelper::block_equal(
            block, Block {ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3}),
                          ColumnHelper::create_column_with_name<DataTypeInt64>({2, 300, 1000})}));
}

TEST_F(AggOperatorTestWithGroupBy, test_no_need_finalize_mem_reuse_with_shared_output_columns) {
    OperatorContext ctx;
    auto sink_op = std::make_shared<MockAggsinkOperator>();
    sink_op->_aggregate_evaluators.push_back(create_mock_agg_fn_evaluator(
            ctx.pool, MockSlotRef::create_mock_contexts(1, std::make_shared<DataTypeInt64>()),
            false, false));
    sink_op->_pool = &ctx.pool;
    EXPECT_TRUE(sink_op->prepare(&ctx.state).ok());
    sink_op->_probe_expr_ctxs =
            MockSlotRef::create_mock_contexts(0, std::make_shared<DataTypeInt64>());

    auto source_op = std::make_shared<MockAggSourceOperator>();
    source_op->mock_row_descriptor.reset(new MockRowDescriptor {
            {std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt64>()}, &ctx.pool});
    source_op->_without_key = false;
    source_op->_needs_finalize = false;
    EXPECT_TRUE(source_op->prepare(&ctx.state).ok());

    auto shared_state = init_sink_and_source(sink_op, source_op, ctx);

    {
        Block block {
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 2, 2, 2, 3}),
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 100, 100, 100, 1000})};
        auto st = sink_op->sink(&ctx.state, &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();
    }

    const auto& aggregate_function = sink_op->_aggregate_evaluators[0]->function();
    auto serialized_type = aggregate_function->get_serialized_type();
    Block block {ColumnHelper::create_column_with_name<DataTypeInt64>({}),
                 ColumnWithTypeAndName(aggregate_function->create_serialize_column(),
                                       serialized_type, "")};
    auto old_key_column = block.get_by_position(0).column;
    auto old_value_column = block.get_by_position(1).column;
    bool eos = false;
    auto st = source_op->get_block(&ctx.state, &block, &eos);
    ASSERT_TRUE(st.ok()) << st.to_string();

    EXPECT_TRUE(eos);
    EXPECT_EQ(block.rows(), 3);
    EXPECT_EQ(old_key_column->size(), 0);
    EXPECT_EQ(old_value_column->size(), 0);
    EXPECT_TRUE(check_and_get_column<ColumnFixedLengthObject>(*block.get_by_position(1).column));
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
        OperatorContext ctx;
        auto sink_op = std::make_shared<MockAggsinkOperator>();
        sink_op->_aggregate_evaluators.push_back(create_mock_agg_fn_evaluator(
                ctx.pool, MockSlotRef::create_mock_contexts(1, std::make_shared<DataTypeInt64>()),
                false, false));
        sink_op->_pool = &ctx.pool;
        EXPECT_TRUE(sink_op->prepare(&ctx.state).ok());
        sink_op->_probe_expr_ctxs =
                MockSlotRef::create_mock_contexts(0, std::make_shared<DataTypeInt64>());

        auto source_op = std::make_shared<MockAggSourceOperator>();
        source_op->mock_row_descriptor.reset(new MockRowDescriptor {
                {std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt64>()}, &ctx.pool});
        source_op->_without_key = false;
        source_op->_needs_finalize = false;
        EXPECT_TRUE(source_op->prepare(&ctx.state).ok());

        auto shared_state = init_sink_and_source(sink_op, source_op, ctx);

        {
            Block block {ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 2, 2, 2, 3}),
                         ColumnHelper::create_column_with_name<DataTypeInt64>(
                                 {1, 1, 100, 100, 100, 1000})};
            auto st = sink_op->sink(&ctx.state, &block, true);
            EXPECT_TRUE(st.ok()) << st.msg();
        }

        {
            Block block;
            bool eos = false;
            auto st = source_op->get_block(&ctx.state, &block, &eos);
            EXPECT_TRUE(st.ok()) << st.msg();
            return block;
        }
    };

    auto phase2 = [](Block& serialize_block) {
        OperatorContext ctx;
        auto sink_op = std::make_shared<MockAggsinkOperator>();
        sink_op->_aggregate_evaluators.push_back(create_mock_agg_fn_evaluator(
                ctx.pool, MockSlotRef::create_mock_contexts(1, std::make_shared<DataTypeInt64>()),
                true, false));
        sink_op->_pool = &ctx.pool;
        EXPECT_TRUE(sink_op->prepare(&ctx.state).ok());
        sink_op->_probe_expr_ctxs =
                MockSlotRef::create_mock_contexts(0, std::make_shared<DataTypeInt64>());

        auto source_op = std::make_shared<MockAggSourceOperator>();
        source_op->mock_row_descriptor.reset(new MockRowDescriptor {
                {std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt64>()}, &ctx.pool});
        source_op->_without_key = false;
        source_op->_needs_finalize = true;
        EXPECT_TRUE(source_op->prepare(&ctx.state).ok());

        auto shared_state = init_sink_and_source(sink_op, source_op, ctx);

        {
            auto st = sink_op->sink(&ctx.state, &serialize_block, true);
            EXPECT_TRUE(st.ok()) << st.msg();
        }

        {
            Block block;
            bool eos = false;
            auto st = source_op->get_block(&ctx.state, &block, &eos);
            EXPECT_TRUE(st.ok()) << st.msg();
            EXPECT_TRUE(ColumnHelper::block_equal(
                    block,
                    Block {ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3}),
                           ColumnHelper::create_column_with_name<DataTypeInt64>({2, 300, 1000})}));
        }
    };
    auto block = phase1();
    phase2(block);
}

TEST_F(AggOperatorTestWithGroupBy, other_case_1) {
    OperatorContext ctx;
    auto sink_op = std::make_shared<MockAggsinkOperator>();
    sink_op->_aggregate_evaluators.push_back(create_mock_agg_fn_evaluator(
            ctx.pool, MockSlotRef::create_mock_contexts(1, std::make_shared<DataTypeInt64>()),
            false, false));
    sink_op->_pool = &ctx.pool;
    EXPECT_TRUE(sink_op->prepare(&ctx.state).ok());
    sink_op->_is_merge = true;
    sink_op->_probe_expr_ctxs =
            MockSlotRef::create_mock_contexts(0, std::make_shared<DataTypeInt64>());

    auto source_op = std::make_shared<MockAggSourceOperator>();
    source_op->mock_row_descriptor.reset(new MockRowDescriptor {
            {std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt64>()}, &ctx.pool});
    source_op->_without_key = false;
    source_op->_needs_finalize = false;
    EXPECT_TRUE(source_op->prepare(&ctx.state).ok());

    auto shared_state = init_sink_and_source(sink_op, source_op, ctx);

    {
        Block block {
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 2, 2, 2, 3}),
                ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 100, 100, 100, 1000})};
        auto st = sink_op->sink(&ctx.state, &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();
    }
}

TEST(AggOperatorTestWithOutGroupBy, other_case_1) {
    OperatorContext ctx;

    auto sink_op = create_agg_sink_op(ctx, false, true);

    sink_op->_is_merge = true;

    auto source_op = create_agg_source_op(ctx, true, true);

    auto shared_state = init_sink_and_source(sink_op, source_op, ctx);

    {
        Block block = ColumnHelper::create_block<DataTypeInt64>({1, 2, 3});
        auto st = sink_op->sink(&ctx.state, &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();
    }

    {
        Block block = ColumnHelper::create_block<DataTypeInt64>({});
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
    OperatorContext ctx;
    auto sink_op = std::make_shared<MockAggsinkOperator>();
    sink_op->_aggregate_evaluators.push_back(create_mock_agg_fn_evaluator(ctx.pool, false, false));
    sink_op->_pool = &ctx.pool;
    EXPECT_TRUE(sink_op->prepare(&ctx.state).ok());
    sink_op->_probe_expr_ctxs = MockSlotRef::create_mock_contexts(
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()));

    auto source_op = std::make_shared<MockAggSourceOperator>();
    source_op->mock_row_descriptor.reset(new MockRowDescriptor {
            {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()),
             std::make_shared<DataTypeInt64>()},
            &ctx.pool});
    source_op->_without_key = false;
    source_op->_needs_finalize = true;
    EXPECT_TRUE(source_op->prepare(&ctx.state).ok());

    auto shared_state = init_sink_and_source(sink_op, source_op, ctx);

    {
        Block block = ColumnHelper::create_nullable_block<DataTypeInt64>(
                {1, 2, 3, 1, 2, 3}, {false, false, false, true, true, true});
        auto* local_state =
                static_cast<AggSinkOperatorX::LocalState*>(ctx.state.get_sink_local_state());

        ColumnRawPtrs key_columns;
        key_columns.push_back(block.get_by_position(0).column.get());

        local_state->_places.resize(block.rows());
        local_state->_emplace_into_hash_table(local_state->_places.data(), key_columns,
                                              block.rows());

        EXPECT_EQ(local_state->get_hash_table_size(), 4); // [1,2,3,null]
    }
}

TEST_F(AggOperatorTestWithGroupBy, other_case_3) {
    auto phase1 = []() {
        OperatorContext ctx;
        auto sink_op = std::make_shared<MockAggsinkOperator>();
        sink_op->_aggregate_evaluators.push_back(create_mock_agg_fn_evaluator(
                ctx.pool, MockSlotRef::create_mock_contexts(1, std::make_shared<DataTypeInt64>()),
                false, false));
        sink_op->_pool = &ctx.pool;
        EXPECT_TRUE(sink_op->prepare(&ctx.state).ok());
        sink_op->_probe_expr_ctxs = MockSlotRef::create_mock_contexts(
                0, std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()));

        auto source_op = std::make_shared<MockAggSourceOperator>();
        source_op->mock_row_descriptor.reset(new MockRowDescriptor {
                {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()),
                 std::make_shared<DataTypeInt64>()},
                &ctx.pool});
        source_op->_without_key = false;
        source_op->_needs_finalize = false;
        EXPECT_TRUE(source_op->prepare(&ctx.state).ok());

        auto shared_state = init_sink_and_source(sink_op, source_op, ctx);

        {
            Block block {ColumnHelper::create_nullable_column_with_name<DataTypeInt64>(
                                 {1, 1, 2, 2, 2, 3}, {false, false, false, true, false, false}),
                         ColumnHelper::create_column_with_name<DataTypeInt64>(
                                 {1, 1, 100, 100, 100, 1000})};
            auto st = sink_op->sink(&ctx.state, &block, true);
            EXPECT_TRUE(st.ok()) << st.msg();
        }

        {
            Block block;
            bool eos = false;
            auto st = source_op->get_block(&ctx.state, &block, &eos);
            EXPECT_TRUE(st.ok()) << st.msg();
            return block;
        }
    };

    auto phase2 = [](Block& serialize_block) {
        OperatorContext ctx;
        auto sink_op = std::make_shared<MockAggsinkOperator>();
        sink_op->_aggregate_evaluators.push_back(create_mock_agg_fn_evaluator(
                ctx.pool, MockSlotRef::create_mock_contexts(1, std::make_shared<DataTypeInt64>()),
                true, false));
        sink_op->_pool = &ctx.pool;
        EXPECT_TRUE(sink_op->prepare(&ctx.state).ok());
        sink_op->_probe_expr_ctxs = MockSlotRef::create_mock_contexts(
                0, std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()));

        auto source_op = std::make_shared<MockAggSourceOperator>();
        source_op->mock_row_descriptor.reset(new MockRowDescriptor {
                {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()),
                 std::make_shared<DataTypeInt64>()},
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
            Block block;
            bool eos = false;
            auto st = source_op->get_block(&ctx.state, &block, &eos);
            EXPECT_TRUE(st.ok()) << st.msg();

            std::cout << block.dump_data() << std::endl;
            EXPECT_TRUE(ColumnHelper::block_equal(
                    block, Block {ColumnHelper::create_nullable_column_with_name<DataTypeInt64>(
                                          {1, 2, 3, 0}, {false, false, false, true}),
                                  ColumnHelper::create_column_with_name<DataTypeInt64>(
                                          {2, 200, 1000, 100})}));
        }
    };
    auto block = phase1();
    phase2(block);
}

TEST(AggOperatorTestWithOutGroupBy, other_case_3) {
    OperatorContext ctx;

    auto sink_op = create_agg_sink_op(ctx, false, true);

    sink_op->_is_merge = true;

    auto source_op = std::make_shared<MockAggSourceOperator>();
    source_op->mock_row_descriptor.reset(new MockRowDescriptor {
            {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())}, &ctx.pool});
    source_op->_without_key = true;
    source_op->_needs_finalize = true;
    EXPECT_TRUE(source_op->prepare(&ctx.state).ok());

    auto shared_state = init_sink_and_source(sink_op, source_op, ctx);

    {
        Block block = ColumnHelper::create_block<DataTypeInt64>({1, 2, 3});
        auto st = sink_op->sink(&ctx.state, &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();
    }

    {
        Block block = ColumnHelper::create_block<DataTypeInt64>({});
        bool eos = false;
        auto st = source_op->get_block(&ctx.state, &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(eos);
        EXPECT_EQ(block.rows(), 1);
        EXPECT_TRUE(ColumnHelper::block_equal(
                block, ColumnHelper::create_nullable_block<DataTypeInt64>({6}, {false})));
    }
}

} // namespace doris
