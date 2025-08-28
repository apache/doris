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

#include "pipeline/exec/hashjoin_build_sink.h"

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
#include <vector>

#include "common/config.h"
#include "hash_join_test_helper.h"
#include "pipeline/exec/operator.h"
#include "pipeline/pipeline_task.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "testutil/mock/mock_runtime_state.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr_context.h"

namespace doris::pipeline {

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
        vectorized::Block block(row_desc.tuple_descriptors()[0]->slots(), 0);

        auto mutable_block = vectorized::MutableBlock(block.clone_empty());
        for (auto& col : mutable_block.mutable_columns()) {
            col->insert_default();
            if (col->is_nullable()) {
                auto& nullable_column = assert_cast<vectorized::ColumnNullable&>(*col);
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
        vectorized::Block block(row_desc.tuple_descriptors()[0]->slots(), 0);

        auto mutable_block = vectorized::MutableBlock(block.clone_empty());
        for (auto& col : mutable_block.mutable_columns()) {
            col->insert_default();
            if (col->is_nullable()) {
                auto& nullable_column = assert_cast<vectorized::ColumnNullable&>(*col);
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

} // namespace doris::pipeline