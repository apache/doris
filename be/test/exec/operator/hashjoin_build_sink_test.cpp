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

#include "exec/operator/hashjoin_build_sink.h"

#include <gen_cpp/DataSinks_types.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <gmock/gmock-actions.h>
#include <gmock/gmock-function-mocker.h>
#include <gmock/gmock-spec-builders.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <variant>
#include <vector>

#include "common/config.h"
#include "core/block/block.h"
#include "core/column/column_const.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "exec/operator/hash_join_test_helper.h"
#include "exec/operator/operator.h"
#include "exec/pipeline/pipeline_task.h"
#include "exprs/vexpr_context.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {

class HashJoinBuildSinkTest : public testing::Test {
public:
    void SetUp() override { _helper.SetUp(); }
    void TearDown() override { _helper.TearDown(); }

    template <typename Func>
    void run_test_block(Func test_block) {
        auto testing_join_ops = {TJoinOp::INNER_JOIN,
                                 TJoinOp::LEFT_OUTER_JOIN,
                                 TJoinOp::RIGHT_OUTER_JOIN,
                                 TJoinOp::FULL_OUTER_JOIN,
                                 TJoinOp::LEFT_SEMI_JOIN,
                                 TJoinOp::RIGHT_SEMI_JOIN,
                                 TJoinOp::LEFT_ANTI_JOIN,
                                 TJoinOp::RIGHT_ANTI_JOIN,
                                 TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN,
                                 TJoinOp::NULL_AWARE_LEFT_SEMI_JOIN};
        auto testing_key_types = {
                TPrimitiveType::BOOLEAN,    TPrimitiveType::TINYINT,    TPrimitiveType::SMALLINT,
                TPrimitiveType::INT,        TPrimitiveType::BIGINT,     TPrimitiveType::FLOAT,
                TPrimitiveType::DOUBLE,     TPrimitiveType::DATE,       TPrimitiveType::DATETIME,
                TPrimitiveType::BINARY,     TPrimitiveType::CHAR,       TPrimitiveType::LARGEINT,
                TPrimitiveType::VARCHAR,    TPrimitiveType::DECIMALV2,  TPrimitiveType::STRING,
                TPrimitiveType::DATEV2,     TPrimitiveType::DATETIMEV2, TPrimitiveType::TIMEV2,
                TPrimitiveType::DECIMAL32,  TPrimitiveType::DECIMAL64,  TPrimitiveType::DECIMAL128I,
                TPrimitiveType::DECIMAL256, TPrimitiveType::IPV4,       TPrimitiveType::IPV6};

        for (const auto& op_type : testing_join_ops) {
            for (const auto key_type : testing_key_types) {
                for (auto left_nullable : {true, false}) {
                    for (auto right_nullable : {true, false}) {
                        test_block(op_type, {key_type}, {left_nullable}, {right_nullable});
                    }
                }

                for (const auto key_type2 : testing_key_types) {
                    for (auto left_nullable : {true, false}) {
                        for (auto right_nullable : {true, false}) {
                            test_block(op_type, {key_type, key_type2},
                                       {left_nullable, right_nullable},
                                       {right_nullable, left_nullable});
                        }
                    }
                }
            }
        }
    }

protected:
    HashJoinTestHelper _helper;
};

class MockAsofBuildExpr : public VExpr {
public:
    explicit MockAsofBuildExpr(DataTypePtr type) : VExpr(std::move(type), false) {}

    const std::string& expr_name() const override { return _name; }

    MOCK_METHOD(Status, execute_column_impl,
                (VExprContext*, const Block*, const Selector*, size_t, ColumnPtr&),
                (const, override));

private:
    const std::string _name = "asof_key";
};

TEST_F(HashJoinBuildSinkTest, MaterializeAsofKeyWithoutEvaluatingMockRow) {
    for (const auto join_op : {TJoinOp::ASOF_LEFT_INNER_JOIN, TJoinOp::ASOF_LEFT_OUTER_JOIN}) {
        for (bool nullable : {false, true}) {
            for (bool constant : {false, true}) {
                auto tnode = _helper.create_test_plan_node(join_op, {TPrimitiveType::INT}, {false},
                                                           {false});
                auto [probe, sink] = _helper.create_operators(tnode);
                HashJoinBuildSinkLocalState local_state(sink.get(), _helper.runtime_state.get());
                DataTypePtr type = std::make_shared<DataTypeInt32>();
                ColumnPtr keys = ColumnHelper::create_column<DataTypeInt32>({1800, 1801, 1802});
                if (nullable) {
                    type = make_nullable(type);
                    keys = ColumnHelper::create_nullable_column<DataTypeInt32>({1800, 0, 1802},
                                                                               {0, 1, 0});
                }
                if (constant) {
                    keys = ColumnConst::create(keys->cut(0, 1), 3);
                }
                auto expr = std::make_shared<testing::StrictMock<MockAsofBuildExpr>>(type);
                sink->_asof_build_side_expr = std::make_shared<VExprContext>(expr);
                RuntimeProfile::Counter timer(TUnit::TIME_NS);
                VExprContextSPtrs equality_exprs;
                std::vector<int> equality_ids;

                // No execute call is permitted for the synthetic default input.
                Block mock {{ColumnHelper::create_column<DataTypeInt32>({1970}),
                             std::make_shared<DataTypeInt32>(), "dt"}};
                auto st = local_state._do_evaluate(mock, equality_exprs, timer, equality_ids, true);
                ASSERT_TRUE(st.ok()) << st;
                ASSERT_EQ(local_state._asof_build_col_id, 1);
                ASSERT_EQ(mock.get_by_position(1).column->size(), 1);

                Block real {{ColumnHelper::create_column<DataTypeInt32>({2200, 2201, 2202}),
                             std::make_shared<DataTypeInt32>(), "dt"}};
                EXPECT_CALL(*expr,
                            execute_column_impl(testing::_, testing::_, nullptr, 3, testing::_))
                        .Times(2)
                        .WillRepeatedly(testing::DoAll(testing::SetArgReferee<4>(keys),
                                                       testing::Return(Status::OK())));
                st = local_state._do_evaluate(real, equality_exprs, timer, equality_ids);
                ASSERT_TRUE(st.ok()) << st;
                auto materialized = real.get_by_position(1).column;
                EXPECT_FALSE(is_column_const(*materialized));
                EXPECT_TRUE(ColumnHelper::column_equal(materialized,
                                                       keys->convert_to_full_column_if_const()));

                auto merged = MutableBlock::build_mutable_block(std::move(mock));
                ASSERT_TRUE(merged.merge_ignore_overflow(std::move(real)).ok());
                Block next {{ColumnHelper::create_column<DataTypeInt32>({2200, 2201, 2202}),
                             std::make_shared<DataTypeInt32>(), "dt"}};
                ASSERT_TRUE(
                        local_state._do_evaluate(next, equality_exprs, timer, equality_ids).ok());
                ASSERT_TRUE(merged.merge_ignore_overflow(std::move(next)).ok());
                auto block = merged.to_block();
                ASSERT_EQ(block.rows(), 7);
                EXPECT_TRUE(ColumnHelper::column_equal(block.get_by_position(1).column->cut(1, 3),
                                                       materialized));
                EXPECT_TRUE(ColumnHelper::column_equal(block.get_by_position(1).column->cut(4, 3),
                                                       materialized));
            }
        }
    }
}

TEST_F(HashJoinBuildSinkTest, PropagateAsofKeyEvaluationError) {
    auto tnode = _helper.create_test_plan_node(TJoinOp::ASOF_LEFT_INNER_JOIN, {TPrimitiveType::INT},
                                               {false}, {false});
    auto [probe, sink] = _helper.create_operators(tnode);
    HashJoinBuildSinkLocalState local_state(sink.get(), _helper.runtime_state.get());
    auto type = std::make_shared<DataTypeInt32>();
    auto expr = std::make_shared<testing::StrictMock<MockAsofBuildExpr>>(type);
    sink->_asof_build_side_expr = std::make_shared<VExprContext>(expr);
    RuntimeProfile::Counter timer(TUnit::TIME_NS);
    VExprContextSPtrs equality_exprs;
    std::vector<int> equality_ids;
    Block mock {{ColumnHelper::create_column<DataTypeInt32>({1970}), type, "dt"}};
    ASSERT_TRUE(local_state._do_evaluate(mock, equality_exprs, timer, equality_ids, true).ok());
    Block real {{ColumnHelper::create_column<DataTypeInt32>({1600}), type, "dt"}};
    EXPECT_CALL(*expr, execute_column_impl(testing::_, &real, nullptr, 1, testing::_))
            .WillOnce(testing::Return(Status::InvalidArgument("TIMESTAMP_NS overflow")));
    auto st = local_state._do_evaluate(real, equality_exprs, timer, equality_ids);
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("TIMESTAMP_NS overflow"), std::string::npos);
    EXPECT_EQ(real.columns(), 1);
}

TEST_F(HashJoinBuildSinkTest, Init) {
    auto test_block = [&](TJoinOp::type op_type, const std::vector<TPrimitiveType::type>& key_types,
                          const std::vector<bool>& left_nullables,
                          const std::vector<bool>& right_nullables) {
        auto tnode =
                _helper.create_test_plan_node(op_type, key_types, left_nullables, right_nullables);
        auto [probe_operator, sink_operator] = _helper.create_operators(tnode);
        ASSERT_TRUE(probe_operator);
        ASSERT_TRUE(sink_operator);

        auto runtime_state = std::make_unique<MockRuntimeState>();
        runtime_state->_query_ctx = _helper.query_ctx.get();
        runtime_state->_query_id = _helper.query_ctx->query_id();
        runtime_state->resize_op_id_to_local_state(-100);
        runtime_state->set_max_operator_id(-100);
        runtime_state->set_desc_tbl(_helper.desc_tbl);

        auto st = sink_operator->init(tnode, runtime_state.get());
        ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();

        st = sink_operator->prepare(runtime_state.get());
        ASSERT_TRUE(st.ok()) << "prepare failed: " << st.to_string();

        auto shared_state = sink_operator->create_shared_state();
        ASSERT_TRUE(shared_state);

        std::map<int, std::pair<std::shared_ptr<BasicSharedState>,
                                std::vector<std::shared_ptr<Dependency>>>>
                shared_state_map;
        LocalSinkStateInfo info {.task_idx = 0,
                                 .parent_profile = _helper.runtime_profile.get(),
                                 .sender_id = 0,
                                 .shared_state = shared_state.get(),
                                 .shared_state_map = shared_state_map,
                                 .tsink = TDataSink()};
        st = sink_operator->setup_local_state(runtime_state.get(), info);
        ASSERT_TRUE(st.ok()) << "setup_local_state failed: " << st.to_string();

        st = probe_operator->init(tnode, runtime_state.get());
        ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();
        st = probe_operator->prepare(runtime_state.get());
        ASSERT_TRUE(st.ok()) << "prepare failed: " << st.to_string();

        LocalStateInfo info2 {.parent_profile = _helper.runtime_profile.get(),
                              .scan_ranges = {},
                              .shared_state = shared_state.get(),
                              .shared_state_map = shared_state_map,
                              .task_idx = 0};

        st = probe_operator->setup_local_state(runtime_state.get(), info2);
        ASSERT_TRUE(st.ok()) << "setup_local_state failed: " << st.to_string();

        auto* probe_local_state = runtime_state->get_local_state(probe_operator->operator_id());
        ASSERT_TRUE(probe_local_state);

        st = probe_local_state->open(runtime_state.get());
        ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

        auto* local_state = reinterpret_cast<HashJoinBuildSinkLocalState*>(
                runtime_state->get_sink_local_state());
        ASSERT_TRUE(local_state);

        st = local_state->open(runtime_state.get());
        ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

        ASSERT_GT(sink_operator->get_memory_usage_debug_str(runtime_state.get()).size(), 0);

        st = local_state->close(runtime_state.get(), st);
        ASSERT_TRUE(st.ok()) << "close failed: " << st.to_string();

        st = sink_operator->close(runtime_state.get(), st);
        ASSERT_TRUE(st.ok()) << "close failed: " << st.to_string();

        st = probe_operator->close(runtime_state.get());
        ASSERT_TRUE(st.ok()) << "close failed: " << st.to_string();

        st = probe_local_state->close(runtime_state.get());
        ASSERT_TRUE(st.ok()) << "close failed: " << st.to_string();
    };

    run_test_block(test_block);
}

TEST_F(HashJoinBuildSinkTest, Sink) {
    auto test_block = [&](TJoinOp::type op_type, const std::vector<TPrimitiveType::type>& key_types,
                          const std::vector<bool>& left_nullables,
                          const std::vector<bool>& right_nullables) {
        auto tnode =
                _helper.create_test_plan_node(op_type, key_types, left_nullables, right_nullables);
        auto [probe_operator, sink_operator] = _helper.create_operators(tnode);
        ASSERT_TRUE(probe_operator);
        ASSERT_TRUE(sink_operator);

        auto runtime_state = std::make_unique<MockRuntimeState>();
        runtime_state->_query_ctx = _helper.query_ctx.get();
        runtime_state->_query_id = _helper.query_ctx->query_id();
        runtime_state->resize_op_id_to_local_state(-100);
        runtime_state->set_max_operator_id(-100);
        runtime_state->set_desc_tbl(_helper.desc_tbl);

        auto st = sink_operator->init(TDataSink());
        ASSERT_FALSE(st.ok());

        st = sink_operator->init(tnode, runtime_state.get());
        ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();

        st = sink_operator->prepare(runtime_state.get());
        ASSERT_TRUE(st.ok()) << "prepare failed: " << st.to_string();

        auto shared_state = sink_operator->create_shared_state();
        ASSERT_TRUE(shared_state);

        shared_state->create_source_dependency(sink_operator->operator_id(), tnode.node_id,
                                               "HashJoinSinkTestDep");

        std::map<int, std::pair<std::shared_ptr<BasicSharedState>,
                                std::vector<std::shared_ptr<Dependency>>>>
                shared_state_map;
        LocalSinkStateInfo info {.task_idx = 0,
                                 .parent_profile = _helper.runtime_profile.get(),
                                 .sender_id = 0,
                                 .shared_state = shared_state.get(),
                                 .shared_state_map = shared_state_map,
                                 .tsink = TDataSink()};
        st = sink_operator->setup_local_state(runtime_state.get(), info);
        ASSERT_TRUE(st.ok()) << "setup_local_state failed: " << st.to_string();

        auto* local_state = reinterpret_cast<HashJoinBuildSinkLocalState*>(
                runtime_state->get_sink_local_state());
        ASSERT_TRUE(local_state);

        st = local_state->open(runtime_state.get());
        ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

        ASSERT_EQ(sink_operator->get_reserve_mem_size(runtime_state.get(), false), 0);

        const auto& row_desc = sink_operator->child()->row_desc();
        Block block(row_desc.tuple_descriptors()[0]->slots(), 0);

        auto mutable_block = MutableBlock(block.clone_empty());
        for (auto& col : mutable_block.mutable_columns()) {
            col->insert_default();
            if (col->is_nullable()) {
                auto& nullable_column = assert_cast<ColumnNullable&>(*col);
                nullable_column.insert_not_null_elements(1);
            } else {
                col->insert_default();
            }
        }

        auto block2 = mutable_block.to_block();
        st = sink_operator->sink(runtime_state.get(), &block2, false);
        ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();

        ASSERT_GT(sink_operator->get_reserve_mem_size(runtime_state.get(), true), 0);
        st = sink_operator->sink(runtime_state.get(), &block, true);
        ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();

        ASSERT_EQ(sink_operator->get_reserve_mem_size(runtime_state.get(), true), 0);

        ASSERT_GT(sink_operator->get_memory_usage(runtime_state.get()), 0);

        st = local_state->close(runtime_state.get(), st);
        ASSERT_TRUE(st.ok()) << "close failed: " << st.to_string();

        st = sink_operator->close(runtime_state.get(), st);
        ASSERT_TRUE(st.ok()) << "close failed: " << st.to_string();
    };

    run_test_block(test_block);
}

TEST_F(HashJoinBuildSinkTest, Terminate) {
    auto test_block = [&](TJoinOp::type op_type, const std::vector<TPrimitiveType::type>& key_types,
                          const std::vector<bool>& left_nullables,
                          const std::vector<bool>& right_nullables) {
        auto tnode =
                _helper.create_test_plan_node(op_type, key_types, left_nullables, right_nullables);
        auto [probe_operator, sink_operator] = _helper.create_operators(tnode);
        ASSERT_TRUE(probe_operator);
        ASSERT_TRUE(sink_operator);

        auto runtime_state = std::make_unique<MockRuntimeState>();
        runtime_state->_query_ctx = _helper.query_ctx.get();
        runtime_state->_query_id = _helper.query_ctx->query_id();
        runtime_state->resize_op_id_to_local_state(-100);
        runtime_state->set_max_operator_id(-100);
        runtime_state->set_desc_tbl(_helper.desc_tbl);

        auto st = sink_operator->init(TDataSink());
        ASSERT_FALSE(st.ok());

        st = sink_operator->init(tnode, runtime_state.get());
        ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();

        st = sink_operator->prepare(runtime_state.get());
        ASSERT_TRUE(st.ok()) << "prepare failed: " << st.to_string();

        auto shared_state = sink_operator->create_shared_state();
        ASSERT_TRUE(shared_state);

        shared_state->create_source_dependency(sink_operator->operator_id(), tnode.node_id,
                                               "HashJoinSinkTestDep");

        std::map<int, std::pair<std::shared_ptr<BasicSharedState>,
                                std::vector<std::shared_ptr<Dependency>>>>
                shared_state_map;
        LocalSinkStateInfo info {.task_idx = 0,
                                 .parent_profile = _helper.runtime_profile.get(),
                                 .sender_id = 0,
                                 .shared_state = shared_state.get(),
                                 .shared_state_map = shared_state_map,
                                 .tsink = TDataSink()};
        st = sink_operator->setup_local_state(runtime_state.get(), info);
        ASSERT_TRUE(st.ok()) << "setup_local_state failed: " << st.to_string();

        auto* local_state = reinterpret_cast<HashJoinBuildSinkLocalState*>(
                runtime_state->get_sink_local_state());
        ASSERT_TRUE(local_state);

        st = local_state->open(runtime_state.get());
        ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

        ASSERT_EQ(sink_operator->get_reserve_mem_size(runtime_state.get(), false), 0);

        const auto& row_desc = sink_operator->child()->row_desc();
        Block block(row_desc.tuple_descriptors()[0]->slots(), 0);

        auto mutable_block = MutableBlock(block.clone_empty());
        for (auto& col : mutable_block.mutable_columns()) {
            col->insert_default();
            if (col->is_nullable()) {
                auto& nullable_column = assert_cast<ColumnNullable&>(*col);
                nullable_column.insert_not_null_elements(1);
            } else {
                col->insert_default();
            }
        }

        auto block2 = mutable_block.to_block();
        st = sink_operator->sink(runtime_state.get(), &block2, false);
        ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();

        st = sink_operator->terminate(runtime_state.get());
        ASSERT_TRUE(st.ok()) << "terminate failed: " << st.to_string();

        ASSERT_GT(sink_operator->get_reserve_mem_size(runtime_state.get(), true), 0);
        st = sink_operator->sink(runtime_state.get(), &block, true);
        ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();

        ASSERT_EQ(sink_operator->get_reserve_mem_size(runtime_state.get(), true), 0);

        ASSERT_GT(sink_operator->get_memory_usage(runtime_state.get()), 0);

        st = local_state->close(runtime_state.get(), st);
        ASSERT_TRUE(st.ok()) << "close failed: " << st.to_string();

        st = sink_operator->close(runtime_state.get(), st);
        ASSERT_TRUE(st.ok()) << "close failed: " << st.to_string();
    };

    run_test_block(test_block);
}

} // namespace doris
