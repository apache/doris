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

#include "pipeline/exec/partitioned_hash_join_sink_operator.h"

#include <gen_cpp/DataSinks_types.h>
#include <gen_cpp/Descriptors_types.h>
#include <gmock/gmock-actions.h>
#include <gmock/gmock-function-mocker.h>
#include <gmock/gmock-spec-builders.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "common/config.h"
#include "partitioned_hash_join_test_helper.h"
#include "pipeline/common/data_gen_functions/vnumbers_tvf.h"
#include "pipeline/exec/operator.h"
#include "pipeline/exec/partitioned_hash_join_probe_operator.h"
#include "pipeline/pipeline_task.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_operators.h"
#include "testutil/mock/mock_runtime_state.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_number.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {

class PartitionedHashJoinSinkOperatorTest : public testing::Test {
public:
    ~PartitionedHashJoinSinkOperatorTest() override = default;
    void SetUp() override { _helper.SetUp(); }

    void TearDown() override { _helper.TearDown(); }

protected:
    PartitionedHashJoinTestHelper _helper;
};

TEST_F(PartitionedHashJoinSinkOperatorTest, Init) {
    TPlanNode tnode = _helper.create_test_plan_node();
    const DescriptorTbl& desc_tbl = _helper.runtime_state->desc_tbl();

    ASSERT_EQ(desc_tbl.get_tuple_descs().size(), 2);

    tnode.row_tuples.push_back(desc_tbl.get_tuple_descs().front()->id());
    tnode.nullable_tuples.push_back(false);

    PartitionedHashJoinSinkOperatorX operator_x(
            _helper.obj_pool.get(), 0, 0, tnode, desc_tbl,
            PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT);

    auto child = std::make_shared<MockChildOperator>();
    child->_row_descriptor = RowDescriptor(_helper.runtime_state->desc_tbl(), {1}, {false});
    EXPECT_TRUE(operator_x.set_child(child));

    ASSERT_TRUE(operator_x.init(tnode, _helper.runtime_state.get()));
    ASSERT_EQ(operator_x._partition_count, PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT);
    ASSERT_TRUE(operator_x._partitioner != nullptr);

    // ObjectPool* pool, int operator_id, int dest_id,
    //                            const TPlanNode& tnode, const DescriptorTbl& descs
    // ObjectPool* pool, const TPlanNode& tnode, int operator_id,
    //                           const DescriptorTbl& descs
    operator_x.set_inner_operators(
            std::make_shared<MockHashJoinBuildOperator>(_helper.obj_pool.get(), 0, 0, tnode,
                                                        _helper.runtime_state->desc_tbl()),
            std::make_shared<MockHashJoinProbeOperator>(_helper.obj_pool.get(), tnode, 0,
                                                        _helper.runtime_state->desc_tbl()));

    auto st = operator_x.prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "Prepare failed: " << st.to_string();
}

TEST_F(PartitionedHashJoinSinkOperatorTest, InitLocalState) {
    auto [_, sink_operator] = _helper.create_operators();

    auto shared_state = std::make_shared<MockPartitionedHashJoinSharedState>();
    auto local_state_uptr = PartitionedHashJoinSinkLocalState::create_unique(
            sink_operator.get(), _helper.runtime_state.get());
    auto local_state = local_state_uptr.get();
    shared_state = std::make_shared<MockPartitionedHashJoinSharedState>();

    _helper.runtime_state->emplace_sink_local_state(sink_operator->operator_id(),
                                                    std::move(local_state_uptr));

    auto st = sink_operator->init(sink_operator->_tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "Init failed: " << st.to_string();

    EXPECT_TRUE(sink_operator->_inner_sink_operator->set_child(nullptr));
    sink_operator->_inner_probe_operator->_build_side_child = nullptr;
    sink_operator->_inner_probe_operator->_child = nullptr;

    st = sink_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "Prepare failed: " << st.to_string();

    RuntimeProfile runtime_profile("test");
    TDataSink t_sink;
    LocalSinkStateInfo info {.task_idx = 0,
                             .parent_profile = &runtime_profile,
                             .sender_id = 0,
                             .shared_state = shared_state.get(),
                             .shared_state_map = {},
                             .tsink = t_sink};
    st = local_state->init(_helper.runtime_state.get(), info);
    ASSERT_TRUE(st) << "init failed: " << st.to_string();

    st = local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st) << "open failed: " << st.to_string();

    local_state->update_memory_usage();

    shared_state->need_to_spill = false;
    auto reserve_size = local_state->get_reserve_mem_size(_helper.runtime_state.get(), false);

    shared_state->need_to_spill = true;
    reserve_size = local_state->get_reserve_mem_size(_helper.runtime_state.get(), false);
    ASSERT_EQ(reserve_size,
              sink_operator->_partition_count * vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM);

    auto* finish_dep = local_state->finishdependency();
    ASSERT_TRUE(finish_dep != nullptr);

    shared_state->need_to_spill = false;

    st = local_state->close(_helper.runtime_state.get(), Status::OK());
    ASSERT_TRUE(st) << "close failed: " << st.to_string();

    st = local_state->close(_helper.runtime_state.get(), Status::OK());
    ASSERT_TRUE(st) << "close failed: " << st.to_string();
}

TEST_F(PartitionedHashJoinSinkOperatorTest, InitBuildExprs) {
    TPlanNode tnode = _helper.create_test_plan_node();
    // 添加多个等值连接条件来测试表达式构建
    for (int i = 0; i < 3; i++) {
        TEqJoinCondition eq_cond;
        eq_cond.left = TExpr();
        eq_cond.right = TExpr();
        tnode.hash_join_node.eq_join_conjuncts.push_back(eq_cond);
    }

    DescriptorTbl desc_tbl;
    PartitionedHashJoinSinkOperatorX operator_x(
            _helper.obj_pool.get(), 0, 0, tnode, desc_tbl,
            PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT);

    ASSERT_TRUE(operator_x.init(tnode, _helper.runtime_state.get()));
    ASSERT_EQ(operator_x._build_exprs.size(), 4); // 1个初始 + 3个新增
}

TEST_F(PartitionedHashJoinSinkOperatorTest, Prepare) {
    auto [_, sink_operator] = _helper.create_operators();

    const auto& tnode = sink_operator->_tnode;

    // 初始化操作符
    ASSERT_TRUE(sink_operator->init(tnode, _helper.runtime_state.get()));

    sink_operator->_partitioner =
            std::make_unique<MockPartitioner>(PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT);

    sink_operator->_inner_sink_operator->_child = nullptr;
    sink_operator->_inner_probe_operator->_build_side_child = nullptr;
    sink_operator->_inner_probe_operator->_child = nullptr;

    auto st = sink_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st) << "Open failed: " << st.to_string();

    ASSERT_TRUE(sink_operator->_partitioner != nullptr);
}

TEST_F(PartitionedHashJoinSinkOperatorTest, Sink) {
    auto [_, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto* sink_local_state = _helper.create_sink_local_state(_helper.runtime_state.get(),
                                                             sink_operator.get(), shared_state);

    auto read_dependency =
            Dependency::create_shared(sink_operator->operator_id(), sink_operator->node_id(),
                                      "HashJoinBuildReadDependency", false);
    sink_local_state->_shared_state->need_to_spill = false;

    shared_state->source_deps.emplace_back(read_dependency);

    vectorized::Block block;
    bool eos = true;

    ASSERT_EQ(read_dependency->_ready.load(), false);
    auto st = sink_operator->sink(_helper.runtime_state.get(), &block, eos);
    ASSERT_TRUE(st.ok()) << "Sink failed: " << st.to_string();

    ASSERT_EQ(read_dependency->_ready.load(), true);
}

TEST_F(PartitionedHashJoinSinkOperatorTest, SinkEosAndSpill) {
    auto [_, sink_operator] = _helper.create_operators();

    auto shared_state = std::make_shared<MockPartitionedHashJoinSharedState>();

    LocalSinkStateInfo sink_info {.task_idx = 0,
                                  .parent_profile = _helper.operator_profile.get(),
                                  .sender_id = 0,
                                  .shared_state = shared_state.get(),
                                  .shared_state_map = {},
                                  .tsink = TDataSink()};
    auto st = sink_operator->setup_local_state(_helper.runtime_state.get(), sink_info);
    ASSERT_TRUE(st.ok()) << "Setup local state failed: " << st.to_string();

    st = sink_operator->init(sink_operator->_tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "Init failed: " << st.to_string();

    sink_operator->_inner_sink_operator->_child = nullptr;
    sink_operator->_inner_probe_operator->_build_side_child = nullptr;
    sink_operator->_inner_probe_operator->_child = nullptr;
    st = sink_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "Open failed: " << st.to_string();

    auto* sink_local_state = reinterpret_cast<PartitionedHashJoinSinkLocalState*>(
            _helper.runtime_state->get_sink_local_state());
    ASSERT_TRUE(sink_local_state != nullptr) << "no sink local state";

    st = sink_local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "Open failed: " << st.to_string();

    auto read_dependency =
            Dependency::create_shared(sink_operator->operator_id(), sink_operator->node_id(),
                                      "HashJoinBuildReadDependency", false);
    shared_state->source_deps.emplace_back(read_dependency);

    vectorized::Block block;

    // sink empty block
    sink_local_state->_shared_state->need_to_spill = false;
    ASSERT_EQ(read_dependency->_ready.load(), false);
    st = sink_operator->sink(_helper.runtime_state.get(), &block, false);
    ASSERT_TRUE(st.ok()) << "Sink failed: " << st.to_string();

    block = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3});

    // sink non-empty block
    st = sink_operator->sink(_helper.runtime_state.get(), &block, false);
    ASSERT_TRUE(st.ok()) << "Sink failed: " << st.to_string();

    sink_local_state->_shared_state->need_to_spill = true;
    ASSERT_EQ(read_dependency->_ready.load(), false);
    st = sink_operator->sink(_helper.runtime_state.get(), &block, false);
    ASSERT_TRUE(st.ok()) << "Sink failed: " << st.to_string();

    while (sink_local_state->_spill_dependency->_ready.load() == false) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    ASSERT_EQ(read_dependency->_ready.load(), false);
    st = sink_operator->sink(_helper.runtime_state.get(), &block, true);
    ASSERT_TRUE(st.ok()) << "Sink failed: " << st.to_string();

    while (read_dependency->_ready.load() == false) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    ASSERT_TRUE(sink_local_state->_dependency->_ready.load());
}

TEST_F(PartitionedHashJoinSinkOperatorTest, RevokeMemoryEmpty) {
    auto [_, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto* sink_state = _helper.create_sink_local_state(_helper.runtime_state.get(),
                                                       sink_operator.get(), shared_state);

    // Expect revoke memory to trigger spilling
    auto status = sink_state->revoke_memory(_helper.runtime_state.get(), nullptr);
    ASSERT_TRUE(status.ok()) << "Revoke memory failed: " << status.to_string();
    ASSERT_TRUE(sink_state->_shared_state->need_to_spill);
}

TEST_F(PartitionedHashJoinSinkOperatorTest, RevokeMemory) {
    auto [_, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto sink_state = _helper.create_sink_local_state(_helper.runtime_state.get(),
                                                      sink_operator.get(), shared_state);

    // prepare & set child operator
    auto child = std::dynamic_pointer_cast<MockChildOperator>(sink_operator->child());

    RowDescriptor row_desc(_helper.runtime_state->desc_tbl(), {1}, {false});
    child->_row_descriptor = row_desc;
    EXPECT_EQ(child->row_desc().num_slots(), 1);

    const auto& tnode = sink_operator->_tnode;
    // prepare and set partitioner
    auto partitioner = std::make_unique<SpillPartitionerType>(sink_operator->_partition_count);
    auto status = partitioner->init({tnode.hash_join_node.eq_join_conjuncts[0].right});
    ASSERT_TRUE(status.ok()) << "Init partitioner failed: " << status.to_string();
    status = partitioner->prepare(_helper.runtime_state.get(), sink_operator->_child->row_desc());
    ASSERT_TRUE(status.ok()) << "Prepare partitioner failed: " << status.to_string();
    sink_state->_partitioner = std::move(partitioner);
    sink_state->_shared_state->need_to_spill = false;

    DCHECK_GE(sink_operator->_child->row_desc().get_column_id(1), 0);

    for (uint32_t i = 0; i != sink_operator->_partition_count; ++i) {
        auto& spilling_stream = sink_state->_shared_state->spilled_streams[i];
        auto st = (ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
                _helper.runtime_state.get(), spilling_stream,
                print_id(_helper.runtime_state->query_id()), fmt::format("hash_build_sink_{}", i),
                sink_operator->node_id(), std::numeric_limits<int32_t>::max(),
                std::numeric_limits<size_t>::max(), sink_state->operator_profile()));
        ASSERT_TRUE(st.ok()) << "Register spill stream failed: " << st.to_string();
    }

    auto& inner_sink = sink_operator->_inner_sink_operator;

    auto inner_sink_local_state = std::make_unique<MockHashJoinBuildSinkLocalState>(
            inner_sink.get(), sink_state->_shared_state->inner_runtime_state.get());
    inner_sink_local_state->_hash_table_memory_usage =
            sink_state->custom_profile()->add_counter("HashTableMemoryUsage", TUnit::BYTES);
    inner_sink_local_state->_build_arena_memory_usage =
            sink_state->operator_profile()->add_counter("BuildArenaMemoryUsage", TUnit::BYTES);

    auto block = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3});
    ASSERT_EQ(block.rows(), 3);
    inner_sink_local_state->_build_side_mutable_block = std::move(block);

    sink_state->_shared_state->inner_runtime_state->emplace_sink_local_state(
            0, std::move(inner_sink_local_state));

    sink_state->_finish_dependency =
            Dependency::create_shared(sink_operator->operator_id(), sink_operator->node_id(),
                                      "HashJoinBuildFinishDependency", true);

    // Expect revoke memory to trigger spilling
    status = sink_state->revoke_memory(_helper.runtime_state.get(), nullptr);
    ASSERT_TRUE(status.ok()) << "Revoke memory failed: " << status.to_string();
    ASSERT_TRUE(sink_state->_shared_state->need_to_spill);

    std::cout << "wait for spill dependency ready" << std::endl;
    while (sink_state->_spill_dependency->_ready.load() == false) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    std::cout << "spill dependency ready" << std::endl;

    std::cout << "profile: " << sink_state->operator_profile()->pretty_print() << std::endl;

    auto written_rows_counter = sink_state->custom_profile()->get_counter("SpillWriteRows");
    auto written_rows = written_rows_counter->value();
    ASSERT_EQ(written_rows, 2) << "SpillWriteRows: " << written_rows_counter->value();

    std::vector<int32_t> large_data(3 * 1024 * 1024);
    std::iota(large_data.begin(), large_data.end(), 0);
    vectorized::Block large_block =
            vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(large_data);

    sink_state->_shared_state->partitioned_build_blocks[0] =
            vectorized::MutableBlock::create_unique(std::move(large_block));
    status = sink_state->revoke_memory(_helper.runtime_state.get(), nullptr);
    ASSERT_TRUE(status.ok()) << "Revoke memory failed: " << status.to_string();
    std::cout << "wait for spill dependency ready" << std::endl;
    while (sink_state->_spill_dependency->_ready.load() == false) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    std::cout << "spill dependency ready" << std::endl;

    ASSERT_EQ(written_rows + 3 * 1024 * 1024, written_rows_counter->value());
}

} // namespace doris::pipeline