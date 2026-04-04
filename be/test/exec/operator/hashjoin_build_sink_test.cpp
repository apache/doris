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
#include <variant>
#include <vector>

#include "common/config.h"
#include "core/block/block.h"
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

// ============================================================================
// Tests for shared hash table signaling when builder task is terminated.
//
// The problematic timing sequence (before the fix):
//   Thread A: triggers make_all_runnable
//   Thread A: make_all_runnable wakes up task0 (the builder)
//   Thread B: task0 is terminated (wake_up_early) and close() runs ->
//             unconditionally sets _signaled=true and wakes other tasks
//   Thread C: task1 (non-builder) wakes up, calls sink() ->
//             sees _signaled=true, enters std::visit on monostate hash table -> crash
//   Thread A: make_all_runnable continues
//
// The fix: In close(), only set _signaled=true when the builder was NOT terminated
// (i.e., the hash table was actually built). When terminated, _signaled stays false,
// so non-builder tasks return EOF safely.
// ============================================================================

class SharedHashTableSignalTest : public testing::Test {
public:
    void SetUp() override { _helper.SetUp(); }
    void TearDown() override { _helper.TearDown(); }

protected:
    // Set up a broadcast join with shared hash table containing num_instances tasks.
    // Returns the sink operator, shared state, and runtime states for all tasks.
    struct BroadcastJoinSetup {
        std::shared_ptr<HashJoinBuildSinkOperatorX> sink_op;
        std::shared_ptr<HashJoinProbeOperatorX> probe_op;
        std::shared_ptr<BasicSharedState> shared_state;
        // runtime_state for the builder (task_idx=0)
        MockRuntimeState* builder_state;
        // runtime_states for non-builder tasks (task_idx=1..N-1)
        std::vector<std::shared_ptr<MockRuntimeState>> non_builder_states;
    };

    BroadcastJoinSetup setup_broadcast_join(int num_instances) {
        BroadcastJoinSetup result;

        auto tnode = _helper.create_test_plan_node(TJoinOp::INNER_JOIN, {TPrimitiveType::INT},
                                                   {false}, {false});

        tnode.hash_join_node.__isset.is_broadcast_join = true;
        tnode.hash_join_node.is_broadcast_join = true;

        auto [probe_op, sink_op] = _helper.create_operators(tnode);
        EXPECT_TRUE(probe_op);
        EXPECT_TRUE(sink_op);

        auto st = sink_op->init(tnode, _helper.runtime_state.get());
        EXPECT_TRUE(st.ok()) << st.to_string();

        st = sink_op->prepare(_helper.runtime_state.get());
        EXPECT_TRUE(st.ok()) << st.to_string();

        st = probe_op->init(tnode, _helper.runtime_state.get());
        EXPECT_TRUE(st.ok()) << st.to_string();

        st = probe_op->prepare(_helper.runtime_state.get());
        EXPECT_TRUE(st.ok()) << st.to_string();

        // Create shared state for broadcast join with num_instances
        auto shared_state = HashJoinSharedState::create_shared(num_instances);
        shared_state->create_source_dependencies(num_instances, sink_op->operator_id(),
                                                 sink_op->node_id(), "HASH_JOIN_PROBE");

        // Set up non-builder tasks first (task_idx=1..N-1)
        for (int i = 1; i < num_instances; i++) {
            auto non_builder_state = std::make_shared<MockRuntimeState>();
            non_builder_state->_query_ctx = _helper.runtime_state->_query_ctx;
            non_builder_state->_query_id = _helper.runtime_state->_query_id;
            non_builder_state->resize_op_id_to_local_state(-100);
            non_builder_state->set_max_operator_id(-100);

            LocalSinkStateInfo info {
                    .task_idx = i,
                    .parent_profile = _helper.runtime_profile.get(),
                    .sender_id = 0,
                    .shared_state = shared_state.get(),
                    .shared_state_map = {},
                    .tsink = TDataSink(),
            };
            st = sink_op->setup_local_state(non_builder_state.get(), info);
            EXPECT_TRUE(st.ok()) << st.to_string();

            auto* local_state = assert_cast<HashJoinBuildSinkLocalState*>(
                    non_builder_state->get_sink_local_state());
            EXPECT_TRUE(local_state);
            st = local_state->open(non_builder_state.get());
            EXPECT_TRUE(st.ok()) << st.to_string();

            // Non-builder should have _should_build_hash_table=false
            EXPECT_FALSE(local_state->_should_build_hash_table);

            result.non_builder_states.push_back(non_builder_state);
        }

        // Set up builder task (task_idx=0) using _helper.runtime_state
        LocalSinkStateInfo builder_info {
                .task_idx = 0,
                .parent_profile = _helper.runtime_profile.get(),
                .sender_id = 0,
                .shared_state = shared_state.get(),
                .shared_state_map = {},
                .tsink = TDataSink(),
        };
        st = sink_op->setup_local_state(_helper.runtime_state.get(), builder_info);
        EXPECT_TRUE(st.ok()) << st.to_string();

        auto* builder_local_state = assert_cast<HashJoinBuildSinkLocalState*>(
                _helper.runtime_state->get_sink_local_state());
        EXPECT_TRUE(builder_local_state);
        st = builder_local_state->open(_helper.runtime_state.get());
        EXPECT_TRUE(st.ok()) << st.to_string();

        // Builder should have _should_build_hash_table=true
        EXPECT_TRUE(builder_local_state->_should_build_hash_table);

        result.sink_op = sink_op;
        result.probe_op = probe_op;
        result.shared_state = shared_state;
        result.builder_state = _helper.runtime_state.get();

        return result;
    }

    HashJoinTestHelper _helper;
};

// Test the fix: when the builder task is terminated (woken up early because the probe side
// finished first), close() should NOT set _signaled=true. This prevents non-builder tasks
// from entering std::visit on an uninitialized (monostate) hash table.
//
// Reproduces the problematic timing sequence:
//   1. Builder task is woken up and gets terminated (wake_up_early)
//   2. Builder's terminate() sets _terminated=true (like pipeline_task.cpp's Defer)
//   3. Builder's close() runs — should NOT set _signaled=true
//   4. Non-builder task calls sink(eos=true) — should see _signaled=false and return EOF
TEST_F(SharedHashTableSignalTest, BuilderTerminatedDoesNotSignal) {
    auto setup = setup_broadcast_join(2);

    auto* builder_local_state =
            assert_cast<HashJoinBuildSinkLocalState*>(setup.builder_state->get_sink_local_state());

    // Verify initial state: _signaled should be false, hash table should be monostate
    ASSERT_FALSE(setup.sink_op->_signaled);
    ASSERT_TRUE(
            std::holds_alternative<std::monostate>(setup.shared_state->cast<HashJoinSharedState>()
                                                           ->hash_table_variant_vector.front()
                                                           ->method_variant));

    // Step 1-2: Simulate the builder being terminated (wake_up_early path).
    // In the real code path, pipeline_task.cpp's execute() Defer calls terminate()
    // BEFORE close() runs.
    auto st = builder_local_state->terminate(setup.builder_state);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_TRUE(builder_local_state->_terminated);

    // Step 3: Builder's close() runs. With the fix, _signaled should stay false
    // because the builder was terminated without building the hash table.
    st = setup.sink_op->close(setup.builder_state, Status::OK());
    ASSERT_TRUE(st.ok()) << st.to_string();

    // Key assertion: _signaled must remain false after terminated builder's close()
    ASSERT_FALSE(setup.sink_op->_signaled) << "_signaled should NOT be set when builder was "
                                              "terminated without building hash table";

    // Step 4: Non-builder task calls sink(eos=true).
    // It should see _signaled=false and return EOF, NOT crash on monostate.
    auto* non_builder_state = setup.non_builder_states[0].get();
    Block empty_block;
    st = setup.sink_op->sink(non_builder_state, &empty_block, true);
    ASSERT_TRUE(st.is<ErrorCode::END_OF_FILE>())
            << "Non-builder should return EOF when builder was terminated, got: " << st.to_string();

    // Clean up the non-builder
    st = setup.sink_op->close(non_builder_state, Status::OK());
    ASSERT_TRUE(st.ok()) << st.to_string();
}

// Test the normal path: when the builder completes normally (not terminated),
// close() should set _signaled=true, and non-builder tasks should proceed
// to share the hash table successfully.
TEST_F(SharedHashTableSignalTest, BuilderNormalCompletionSignals) {
    auto setup = setup_broadcast_join(2);

    auto* builder_local_state =
            assert_cast<HashJoinBuildSinkLocalState*>(setup.builder_state->get_sink_local_state());

    // Verify initial state
    ASSERT_FALSE(setup.sink_op->_signaled);
    ASSERT_FALSE(builder_local_state->_terminated);

    // Builder sinks data normally
    auto build_block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3});
    auto st = setup.sink_op->sink(setup.builder_state, &build_block, false);
    ASSERT_TRUE(st.ok()) << st.to_string();

    // Builder receives eos
    Block empty_block;
    st = setup.sink_op->sink(setup.builder_state, &empty_block, true);
    ASSERT_TRUE(st.ok()) << st.to_string();

    // Hash table should have been built (no longer monostate)
    ASSERT_FALSE(
            std::holds_alternative<std::monostate>(setup.shared_state->cast<HashJoinSharedState>()
                                                           ->hash_table_variant_vector.front()
                                                           ->method_variant));

    // Builder closes normally (not terminated)
    ASSERT_FALSE(builder_local_state->_terminated);
    st = setup.sink_op->close(setup.builder_state, Status::OK());
    ASSERT_TRUE(st.ok()) << st.to_string();

    // Key assertion: _signaled should be true after normal builder close
    ASSERT_TRUE(setup.sink_op->_signaled)
            << "_signaled should be set when builder completed normally";

    // Non-builder task calls sink(eos=true) — should succeed, sharing the hash table
    auto* non_builder_state = setup.non_builder_states[0].get();
    st = setup.sink_op->sink(non_builder_state, &empty_block, true);
    ASSERT_TRUE(st.ok()) << "Non-builder should succeed when builder signaled, got: "
                         << st.to_string();

    // Clean up
    st = setup.sink_op->close(non_builder_state, Status::OK());
    ASSERT_TRUE(st.ok()) << st.to_string();
}

// Test with multiple non-builder tasks: when builder is terminated,
// ALL non-builder tasks should return EOF safely (no crash on monostate).
TEST_F(SharedHashTableSignalTest, MultipleNonBuildersAllReturnEOFWhenTerminated) {
    constexpr int num_instances = 4;
    auto setup = setup_broadcast_join(num_instances);

    auto* builder_local_state =
            assert_cast<HashJoinBuildSinkLocalState*>(setup.builder_state->get_sink_local_state());

    // Terminate the builder (simulate wake_up_early)
    auto st = builder_local_state->terminate(setup.builder_state);
    ASSERT_TRUE(st.ok()) << st.to_string();

    // Builder close — should NOT set _signaled
    st = setup.sink_op->close(setup.builder_state, Status::OK());
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_FALSE(setup.sink_op->_signaled);

    // All non-builder tasks should return EOF safely
    for (int i = 0; i < num_instances - 1; i++) {
        auto* non_builder_state = setup.non_builder_states[i].get();
        Block empty_block;
        st = setup.sink_op->sink(non_builder_state, &empty_block, true);
        ASSERT_TRUE(st.is<ErrorCode::END_OF_FILE>())
                << "Non-builder task " << (i + 1) << " should return EOF, got: " << st.to_string();

        st = setup.sink_op->close(non_builder_state, Status::OK());
        ASSERT_TRUE(st.ok()) << st.to_string();
    }
}

} // namespace doris