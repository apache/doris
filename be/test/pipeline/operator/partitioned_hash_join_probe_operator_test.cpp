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

#include "pipeline/exec/partitioned_hash_join_probe_operator.h"

#include <gtest/gtest.h>

#include <memory>

#include "common/config.h"
#include "partitioned_hash_join_test_helper.h"
#include "pipeline/exec/hashjoin_build_sink.h"
#include "pipeline/exec/partitioned_hash_join_sink_operator.h"
#include "pipeline/pipeline_task.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "testutil/column_helper.h"
#include "testutil/creators.h"
#include "testutil/mock/mock_operators.h"
#include "testutil/mock/mock_runtime_state.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_number.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {
class PartitionedHashJoinProbeOperatorTest : public testing::Test {
public:
    void SetUp() override { _helper.SetUp(); }
    void TearDown() override { _helper.TearDown(); }

protected:
    PartitionedHashJoinTestHelper _helper;
};

TEST_F(PartitionedHashJoinProbeOperatorTest, debug_string) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    auto debug_string = local_state->debug_string(0);
    std::cout << "debug string: " << debug_string << std::endl;

    shared_state->need_to_spill = false;
    debug_string = local_state->debug_string(0);
    std::cout << "debug string: " << debug_string << std::endl;

    ASSERT_TRUE(local_state->operator_profile()->pretty_print().find("ExecTime") !=
                std::string::npos);
}

TEST_F(PartitionedHashJoinProbeOperatorTest, InitAndOpen) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    auto local_state = PartitionedHashJoinProbeLocalState::create_shared(
            _helper.runtime_state.get(), probe_operator.get());

    auto shared_state = std::make_shared<PartitionedHashJoinSharedState>();
    LocalStateInfo info {.parent_profile = _helper.operator_profile.get(),
                         .scan_ranges = {},
                         .shared_state = shared_state.get(),
                         .shared_state_map = {},
                         .task_idx = 0};
    auto st = local_state->init(_helper.runtime_state.get(), info);
    ASSERT_TRUE(st) << "init failed: " << st.to_string();

    // before opening, should setup probe_operator's partitioner.
    const auto& tnode = probe_operator->_tnode;
    auto child = std::make_shared<MockChildOperator>();
    RowDescriptor row_desc(_helper.runtime_state->desc_tbl(), {0}, {false});
    child->_row_descriptor = row_desc;

    probe_operator->_inner_sink_operator->_child = nullptr;
    probe_operator->_inner_probe_operator->_child = nullptr;
    probe_operator->_inner_probe_operator->_build_side_child = nullptr;

    st = probe_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st) << "init failed: " << st.to_string();

    st = probe_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st) << "prepare failed: " << st.to_string();

    st = local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st) << "open failed: " << st.to_string();

    local_state->_shared_state->inner_shared_state = std::make_shared<MockHashJoinSharedState>();
    local_state->_shared_state->inner_runtime_state = std::make_unique<MockRuntimeState>();
    local_state->_shared_state->inner_runtime_state->set_desc_tbl(
            &(_helper.runtime_state->desc_tbl()));
    local_state->_shared_state->inner_runtime_state->resize_op_id_to_local_state(-100);

    auto mock_inner_sink_operator = probe_operator->_inner_sink_operator;
    probe_operator->_inner_sink_operator = std::make_shared<HashJoinBuildSinkOperatorX>(
            _helper.obj_pool.get(), 0, 0, tnode, _helper.runtime_state->desc_tbl());
    EXPECT_TRUE(probe_operator->_inner_sink_operator->set_child(mock_inner_sink_operator->child()));

    st = probe_operator->_inner_sink_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st) << "init inner sink operator failed: " << st.to_string();

    auto inner_probe_state = std::make_unique<HashJoinProbeLocalState>(
            _helper.runtime_state.get(), probe_operator->_inner_probe_operator.get());

    st = inner_probe_state->init(local_state->_shared_state->inner_runtime_state.get(), info);
    ASSERT_TRUE(st) << "init failed: " << st.to_string();

    local_state->_shared_state->inner_runtime_state->emplace_local_state(
            probe_operator->_inner_probe_operator->operator_id(), std::move(inner_probe_state));

    auto inner_sink_state = std::make_unique<HashJoinBuildSinkLocalState>(
            probe_operator->_inner_sink_operator.get(), _helper.runtime_state.get());

    LocalSinkStateInfo sink_info {0,  _helper.operator_profile.get(),
                                  -1, local_state->_shared_state->inner_shared_state.get(),
                                  {}, {}};
    st = probe_operator->_inner_sink_operator->prepare(
            local_state->_shared_state->inner_runtime_state.get());
    ASSERT_TRUE(st) << "prepare failed: " << st.to_string();

    st = inner_sink_state->init(local_state->_shared_state->inner_runtime_state.get(), sink_info);
    ASSERT_TRUE(st) << "init failed: " << st.to_string();

    local_state->_shared_state->inner_runtime_state->emplace_sink_local_state(
            0, std::move(inner_sink_state));

    local_state->_shared_state->need_to_spill = false;
    local_state->update_profile_from_inner();

    local_state->_shared_state->need_to_spill = true;
    local_state->update_profile_from_inner();

    st = local_state->close(_helper.runtime_state.get());
    ASSERT_TRUE(st) << "close failed: " << st.to_string();

    st = local_state->close(_helper.runtime_state.get());
    ASSERT_TRUE(st) << "close failed: " << st.to_string();

    auto debug_string = local_state->debug_string(0);
    std::cout << "debug string: " << debug_string << std::endl;

    // close is reentrant.
    st = local_state->close(_helper.runtime_state.get());
    ASSERT_TRUE(st) << "close failed: " << st.to_string();
}

TEST_F(PartitionedHashJoinProbeOperatorTest, spill_probe_blocks) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    RowDescriptor row_desc(_helper.runtime_state->desc_tbl(), {0}, {false});
    const auto& tnode = probe_operator->_tnode;
    local_state->_partitioner = create_spill_partitioner(
            _helper.runtime_state.get(), PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT,
            {tnode.hash_join_node.eq_join_conjuncts[0].left}, row_desc);

    // create probe blocks
    for (int32_t i = 0; i != PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT; ++i) {
        if (i % 2 == 0) {
            continue;
        }

        vectorized::Block block = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(
                {1 * i, 2 * i, 3 * i});
        local_state->_probe_blocks[i].emplace_back(std::move(block));
    }

    std::vector<int32_t> large_data(3 * 1024 * 1024);
    std::iota(large_data.begin(), large_data.end(), 0);
    vectorized::Block large_block =
            vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(large_data);

    std::vector<int32_t> small_data(3 * 1024);
    std::iota(small_data.begin(), small_data.end(), 3 * 1024 * 1024);
    vectorized::Block small_block =
            vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(small_data);

    // add a large block to the last partition
    local_state->_partitioned_blocks[PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT - 1] =
            vectorized::MutableBlock::create_unique(std::move(large_block));

    // add a small block to the first partition
    local_state->_partitioned_blocks[0] =
            vectorized::MutableBlock::create_unique(std::move(small_block));

    local_state->_shared_state->need_to_spill = false;
    local_state->update_profile_from_inner();

    local_state->_shared_state->need_to_spill = true;
    auto st = local_state->spill_probe_blocks(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "spill probe blocks failed: " << st.to_string();

    std::cout << "wait for spill dependency ready" << std::endl;
    while (local_state->_spill_dependency->_ready.load() == false) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    std::cout << "spill dependency ready" << std::endl;

    local_state->update_profile_from_inner();

    std::cout << "profile: " << local_state->custom_profile()->pretty_print() << std::endl;

    for (int32_t i = 0; i != PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT; ++i) {
        if (!local_state->_probe_spilling_streams[i]) {
            continue;
        }
        ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(
                local_state->_probe_spilling_streams[i]);
        local_state->_probe_spilling_streams[i].reset();
    }

    auto* write_rows_counter = local_state->custom_profile()->get_counter("SpillWriteRows");
    ASSERT_EQ(write_rows_counter->value(),
              (PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT / 2) * 3 + 3 * 1024 * 1024);
}

TEST_F(PartitionedHashJoinProbeOperatorTest, RecoverProbeBlocksFromDisk) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    // Create and register a spill stream for testing
    const uint32_t test_partition = 0;
    auto& spill_stream = local_state->_probe_spilling_streams[test_partition];
    ASSERT_TRUE(ExecEnv::GetInstance()
                        ->spill_stream_mgr()
                        ->register_spill_stream(
                                _helper.runtime_state.get(), spill_stream,
                                print_id(_helper.runtime_state->query_id()), "hash_probe",
                                probe_operator->node_id(), std::numeric_limits<int32_t>::max(),
                                std::numeric_limits<size_t>::max(), local_state->operator_profile())
                        .ok());

    // Write some test data to spill stream
    {
        vectorized::Block block =
                vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3});
        ASSERT_TRUE(spill_stream->spill_block(_helper.runtime_state.get(), block, false).ok());
        ASSERT_TRUE(spill_stream->spill_eof().ok());
    }

    // Test recovery
    bool has_data = false;
    ASSERT_TRUE(local_state
                        ->recover_probe_blocks_from_disk(_helper.runtime_state.get(),
                                                         test_partition, has_data)
                        .ok());
    ASSERT_TRUE(has_data);

    // Wait for async recovery to complete
    while (local_state->_spill_dependency->_ready.load() == false) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    std::cout << "profile: " << local_state->custom_profile()->pretty_print() << std::endl;

    // Verify recovered data
    auto& probe_blocks = local_state->_probe_blocks[test_partition];
    ASSERT_FALSE(probe_blocks.empty());
    ASSERT_EQ(probe_blocks[0].rows(), 3);

    // Verify counters
    auto* recovery_rows_counter =
            local_state->custom_profile()->get_counter("SpillRecoveryProbeRows");
    ASSERT_EQ(recovery_rows_counter->value(), 3);
    auto* recovery_blocks_counter =
            local_state->custom_profile()->get_counter("SpillReadBlockCount");
    ASSERT_EQ(recovery_blocks_counter->value(), 1);

    // Verify stream cleanup
    ASSERT_EQ(local_state->_probe_spilling_streams[test_partition], nullptr);
}

TEST_F(PartitionedHashJoinProbeOperatorTest, RecoverProbeBlocksFromDiskLargeData) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    // Create and register a spill stream for testing
    const uint32_t test_partition = 0;
    auto& spill_stream = local_state->_probe_spilling_streams[test_partition];
    ASSERT_TRUE(ExecEnv::GetInstance()
                        ->spill_stream_mgr()
                        ->register_spill_stream(
                                _helper.runtime_state.get(), spill_stream,
                                print_id(_helper.runtime_state->query_id()), "hash_probe",
                                probe_operator->node_id(), std::numeric_limits<int32_t>::max(),
                                std::numeric_limits<size_t>::max(), local_state->operator_profile())
                        .ok());

    // Write some test data to spill stream
    {
        // create block larger than 32MB(4 * (8 * 1024 * 1024 + 10))
        std::vector<int32_t> large_data(8 * 1024 * 1024 + 10);
        std::iota(large_data.begin(), large_data.end(), 0);
        vectorized::Block large_block =
                vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(large_data);

        ASSERT_TRUE(
                spill_stream->spill_block(_helper.runtime_state.get(), large_block, false).ok());

        vectorized::Block block =
                vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3});
        ASSERT_TRUE(spill_stream->spill_block(_helper.runtime_state.get(), block, false).ok());
        ASSERT_TRUE(spill_stream->spill_eof().ok());
    }

    // Test recovery
    bool has_data = true;
    while (has_data) {
        ASSERT_TRUE(local_state
                            ->recover_probe_blocks_from_disk(_helper.runtime_state.get(),
                                                             test_partition, has_data)
                            .ok());

        // Wait for async recovery to complete
        while (local_state->_spill_dependency->_ready.load() == false) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }

    std::cout << "profile: " << local_state->custom_profile()->pretty_print() << std::endl;

    // Verify recovered data
    auto& probe_blocks = local_state->_probe_blocks[test_partition];
    ASSERT_FALSE(probe_blocks.empty());
    ASSERT_EQ(probe_blocks[0].rows(), 8 * 1024 * 1024 + 10);
    ASSERT_EQ(probe_blocks[1].rows(), 3);

    // Verify counters
    auto* recovery_rows_counter =
            local_state->custom_profile()->get_counter("SpillRecoveryProbeRows");
    ASSERT_EQ(recovery_rows_counter->value(), 3 + 8 * 1024 * 1024 + 10);
    auto* recovery_blocks_counter =
            local_state->custom_profile()->get_counter("SpillReadBlockCount");
    ASSERT_EQ(recovery_blocks_counter->value(), 2);

    // Verify stream cleanup
    ASSERT_EQ(local_state->_probe_spilling_streams[test_partition], nullptr);
}

TEST_F(PartitionedHashJoinProbeOperatorTest, RecoverProbeBlocksFromDiskEmpty) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    // Test multiple cases
    const uint32_t test_partition = 0;

    auto& spilled_stream = local_state->_probe_spilling_streams[test_partition];
    ASSERT_TRUE(ExecEnv::GetInstance()
                        ->spill_stream_mgr()
                        ->register_spill_stream(
                                _helper.runtime_state.get(), spilled_stream,
                                print_id(_helper.runtime_state->query_id()), "hash_probe",
                                probe_operator->node_id(), std::numeric_limits<int32_t>::max(),
                                std::numeric_limits<size_t>::max(), local_state->operator_profile())
                        .ok());
    ASSERT_TRUE(spilled_stream->spill_eof().ok());

    bool has_data = false;
    ASSERT_TRUE(local_state
                        ->recover_probe_blocks_from_disk(_helper.runtime_state.get(),
                                                         test_partition, has_data)
                        .ok());
    ASSERT_TRUE(has_data);
    while (local_state->_spill_dependency->_ready.load() == false) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    ASSERT_TRUE(local_state->_probe_blocks[test_partition].empty())
            << "probe blocks not empty: " << local_state->_probe_blocks[test_partition].size();

    ASSERT_TRUE(spilled_stream == nullptr);
}

TEST_F(PartitionedHashJoinProbeOperatorTest, RecoverProbeBlocksFromDiskError) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    // Test multiple cases
    const uint32_t test_partition = 0;

    auto& spilling_stream = local_state->_probe_spilling_streams[test_partition];
    ASSERT_TRUE(ExecEnv::GetInstance()
                        ->spill_stream_mgr()
                        ->register_spill_stream(
                                _helper.runtime_state.get(), spilling_stream,
                                print_id(_helper.runtime_state->query_id()), "hash_probe",
                                probe_operator->node_id(), std::numeric_limits<int32_t>::max(),
                                std::numeric_limits<size_t>::max(), local_state->operator_profile())
                        .ok());

    // Write some test data to spill stream
    {
        vectorized::Block block =
                vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3});
        ASSERT_TRUE(spilling_stream->spill_block(_helper.runtime_state.get(), block, false).ok());
        ASSERT_TRUE(spilling_stream->spill_eof().ok());
    }

    SpillableDebugPointHelper dp_helper("fault_inject::spill_stream::read_next_block");
    bool has_data = false;
    ASSERT_TRUE(local_state
                        ->recover_probe_blocks_from_disk(_helper.runtime_state.get(),
                                                         test_partition, has_data)
                        .ok());
    ASSERT_TRUE(has_data);
    while (local_state->_spill_dependency->_ready.load() == false) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(spilling_stream);
    spilling_stream.reset();

    ASSERT_FALSE(dp_helper.get_spill_status().ok());
    ASSERT_TRUE(dp_helper.get_spill_status().to_string().find(
                        "fault_inject spill_stream read_next_block") != std::string::npos)
            << "unexpected error: " << dp_helper.get_spill_status().to_string();
}

TEST_F(PartitionedHashJoinProbeOperatorTest, RecoverBuildBlocksFromDisk) {
    // Setup test environment
    auto [probe_operator, sink_operator] = _helper.create_operators();

    // Initialize local state
    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    // Create and register spill stream with test data
    const uint32_t test_partition = 0;
    auto& spilled_stream = local_state->_shared_state->spilled_streams[test_partition];
    ASSERT_TRUE(ExecEnv::GetInstance()
                        ->spill_stream_mgr()
                        ->register_spill_stream(
                                _helper.runtime_state.get(), spilled_stream,
                                print_id(_helper.runtime_state->query_id()), "hash_build",
                                probe_operator->node_id(), std::numeric_limits<int32_t>::max(),
                                std::numeric_limits<size_t>::max(), local_state->operator_profile())
                        .ok());

    // Write test data
    {
        vectorized::Block block =
                vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3});
        ASSERT_TRUE(spilled_stream->spill_block(_helper.runtime_state.get(), block, false).ok());
        ASSERT_TRUE(spilled_stream->spill_eof().ok());
    }

    // Test recovery
    bool has_data = false;
    ASSERT_TRUE(local_state
                        ->recover_build_blocks_from_disk(_helper.runtime_state.get(),
                                                         test_partition, has_data)
                        .ok());
    ASSERT_TRUE(has_data);

    // Wait for async recovery
    while (local_state->_spill_dependency->_ready.load() == false) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    // Verify recovered data
    ASSERT_TRUE(local_state->_recovered_build_block != nullptr);
    ASSERT_EQ(local_state->_recovered_build_block->rows(), 3);

    // Verify counters
    auto* recovery_rows_counter =
            local_state->custom_profile()->get_counter("SpillRecoveryBuildRows");
    ASSERT_EQ(recovery_rows_counter->value(), 3);
    auto* recovery_blocks_counter =
            local_state->custom_profile()->get_counter("SpillReadBlockCount");
    ASSERT_EQ(recovery_blocks_counter->value(), 1);

    // Verify stream cleanup
    ASSERT_EQ(local_state->_shared_state->spilled_streams[test_partition], nullptr);
}

TEST_F(PartitionedHashJoinProbeOperatorTest, RecoverBuildBlocksFromDiskCanceled) {
    // Setup test environment
    auto [probe_operator, sink_operator] = _helper.create_operators();

    // Initialize local state
    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    // Create and register spill stream with test data
    const uint32_t test_partition = 0;
    auto& spilled_stream = local_state->_shared_state->spilled_streams[test_partition];
    ASSERT_TRUE(ExecEnv::GetInstance()
                        ->spill_stream_mgr()
                        ->register_spill_stream(
                                _helper.runtime_state.get(), spilled_stream,
                                print_id(_helper.runtime_state->query_id()), "hash_build",
                                probe_operator->node_id(), std::numeric_limits<int32_t>::max(),
                                std::numeric_limits<size_t>::max(), local_state->operator_profile())
                        .ok());

    // Write test data
    {
        vectorized::Block block =
                vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3});
        ASSERT_TRUE(spilled_stream->spill_block(_helper.runtime_state.get(), block, false).ok());
        ASSERT_TRUE(spilled_stream->spill_eof().ok());
    }

    // Test recovery
    bool has_data = false;
    ASSERT_TRUE(local_state
                        ->recover_build_blocks_from_disk(_helper.runtime_state.get(),
                                                         test_partition, has_data)
                        .ok());
    ASSERT_TRUE(has_data);

    // Wait for async recovery
    while (local_state->_spill_dependency->_ready.load() == false) {
        _helper.runtime_state->_query_ctx->_exec_status.update(Status::Cancelled("test canceled"));
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    ASSERT_TRUE(_helper.runtime_state->is_cancelled());
}

TEST_F(PartitionedHashJoinProbeOperatorTest, need_more_input_data) {
    // Setup test environment
    auto [probe_operator, sink_operator] = _helper.create_operators();

    // Initialize local state
    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    local_state->_shared_state->need_to_spill = true;
    local_state->_child_eos = false;
    ASSERT_EQ(probe_operator->need_more_input_data(_helper.runtime_state.get()),
              !local_state->_child_eos);

    local_state->_child_eos = true;
    ASSERT_EQ(probe_operator->need_more_input_data(_helper.runtime_state.get()),
              !local_state->_child_eos);

    local_state->_shared_state->need_to_spill = false;
    auto inner_operator = std::dynamic_pointer_cast<MockHashJoinProbeOperator>(
            probe_operator->_inner_probe_operator);

    inner_operator->need_more_data = true;
    ASSERT_EQ(probe_operator->need_more_input_data(_helper.runtime_state.get()), true);
    inner_operator->need_more_data = false;
    ASSERT_EQ(probe_operator->need_more_input_data(_helper.runtime_state.get()), false);
}

TEST_F(PartitionedHashJoinProbeOperatorTest, revocable_mem_size) {
    // Setup test environment
    auto [probe_operator, sink_operator] = _helper.create_operators();

    // Initialize local state
    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    local_state->_child_eos = true;
    ASSERT_EQ(probe_operator->revocable_mem_size(_helper.runtime_state.get()), 0);

    local_state->_child_eos = false;
    auto block1 = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3});
    local_state->_probe_blocks[0].emplace_back(block1);
    ASSERT_EQ(probe_operator->revocable_mem_size(_helper.runtime_state.get()),
              block1.allocated_bytes());
    auto block2 =
            vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3, 5, 6, 7});
    local_state->_partitioned_blocks[0] =
            vectorized::MutableBlock::create_unique(std::move(block2));

    // block2 is small, so it should not be counted
    ASSERT_EQ(probe_operator->revocable_mem_size(_helper.runtime_state.get()),
              block1.allocated_bytes());

    // Create large input block (> 32k)
    std::vector<int32_t> large_data(9 * 1024);
    std::iota(large_data.begin(), large_data.end(), 0);
    vectorized::Block large_block =
            vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(large_data);

    const auto large_size = large_block.allocated_bytes();
    local_state->_partitioned_blocks[0] =
            vectorized::MutableBlock::create_unique(std::move(large_block));
    ASSERT_EQ(probe_operator->revocable_mem_size(_helper.runtime_state.get()),
              block1.allocated_bytes() + large_size);

    local_state->_child_eos = true;
    ASSERT_EQ(probe_operator->revocable_mem_size(_helper.runtime_state.get()), 0);
}

TEST_F(PartitionedHashJoinProbeOperatorTest, get_reserve_mem_size) {
    // Setup test environment
    auto [probe_operator, sink_operator] = _helper.create_operators();

    // Initialize local state
    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    local_state->_shared_state->need_to_spill = true;
    local_state->_child_eos = false;

    local_state->_need_to_setup_internal_operators = false;
    ASSERT_EQ(probe_operator->get_reserve_mem_size(_helper.runtime_state.get()),
              vectorized::SpillStream::MAX_SPILL_WRITE_BATCH_MEM);

    local_state->_need_to_setup_internal_operators = true;
    ASSERT_GT(probe_operator->get_reserve_mem_size(_helper.runtime_state.get()),
              vectorized::SpillStream::MAX_SPILL_WRITE_BATCH_MEM);

    const auto default_reserve_size =
            _helper.runtime_state->minimum_operator_memory_required_bytes() +
            probe_operator->get_child()->get_reserve_mem_size(_helper.runtime_state.get());
    local_state->_shared_state->need_to_spill = false;
    ASSERT_EQ(probe_operator->get_reserve_mem_size(_helper.runtime_state.get()),
              default_reserve_size);

    local_state->_shared_state->need_to_spill = true;
    local_state->_child_eos = true;
    ASSERT_EQ(probe_operator->get_reserve_mem_size(_helper.runtime_state.get()),
              default_reserve_size);
}

TEST_F(PartitionedHashJoinProbeOperatorTest, RecoverBuildBlocksFromDiskEmpty) {
    // Similar setup as above...
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    // Test empty stream
    const uint32_t test_partition = 0;
    auto& spilled_stream = local_state->_shared_state->spilled_streams[test_partition];
    ASSERT_TRUE(ExecEnv::GetInstance()
                        ->spill_stream_mgr()
                        ->register_spill_stream(
                                _helper.runtime_state.get(), spilled_stream,
                                print_id(_helper.runtime_state->query_id()), "hash_build",
                                probe_operator->node_id(), std::numeric_limits<int32_t>::max(),
                                std::numeric_limits<size_t>::max(), local_state->operator_profile())
                        .ok());

    ASSERT_TRUE(spilled_stream->spill_eof().ok());

    bool has_data = false;
    ASSERT_TRUE(local_state
                        ->recover_build_blocks_from_disk(_helper.runtime_state.get(),
                                                         test_partition, has_data)
                        .ok());
    ASSERT_TRUE(has_data);

    while (local_state->_spill_dependency->_ready.load() == false) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    ASSERT_EQ(spilled_stream, nullptr);
    ASSERT_TRUE(local_state->_recovered_build_block == nullptr);
}

TEST_F(PartitionedHashJoinProbeOperatorTest, RecoverBuildBlocksFromDiskLargeData) {
    // Similar setup as above...
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    // Test empty stream
    const uint32_t test_partition = 0;
    auto& spilled_stream = local_state->_shared_state->spilled_streams[test_partition];
    ASSERT_TRUE(ExecEnv::GetInstance()
                        ->spill_stream_mgr()
                        ->register_spill_stream(
                                _helper.runtime_state.get(), spilled_stream,
                                print_id(_helper.runtime_state->query_id()), "hash_build",
                                probe_operator->node_id(), std::numeric_limits<int32_t>::max(),
                                std::numeric_limits<size_t>::max(), local_state->operator_profile())
                        .ok());

    // Write some test data to spill stream
    {
        // create block larger than 32MB(4 * (8 * 1024 * 1024 + 10))
        std::vector<int32_t> large_data(8 * 1024 * 1024 + 10);
        std::iota(large_data.begin(), large_data.end(), 0);
        vectorized::Block large_block =
                vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(large_data);

        ASSERT_TRUE(
                spilled_stream->spill_block(_helper.runtime_state.get(), large_block, false).ok());

        vectorized::Block block =
                vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3});
        ASSERT_TRUE(spilled_stream->spill_block(_helper.runtime_state.get(), block, false).ok());
    }
    ASSERT_TRUE(spilled_stream->spill_eof().ok());

    bool has_data = false;
    do {
        ASSERT_TRUE(local_state
                            ->recover_build_blocks_from_disk(_helper.runtime_state.get(),
                                                             test_partition, has_data)
                            .ok());

        while (local_state->_spill_dependency->_ready.load() == false) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        ASSERT_TRUE(local_state->_recovered_build_block);
    } while (has_data);

    ASSERT_EQ(spilled_stream, nullptr);

    // Verify recovered data
    ASSERT_EQ(local_state->_recovered_build_block->rows(), 8 * 1024 * 1024 + 10 + 3);

    // Verify counters
    auto* recovery_rows_counter =
            local_state->custom_profile()->get_counter("SpillRecoveryBuildRows");
    ASSERT_EQ(recovery_rows_counter->value(), 8 * 1024 * 1024 + 10 + 3);
    auto* recovery_blocks_counter =
            local_state->custom_profile()->get_counter("SpillReadBlockCount");
    ASSERT_EQ(recovery_blocks_counter->value(), 2);
}

TEST_F(PartitionedHashJoinProbeOperatorTest, RecoverBuildBlocksFromDiskError) {
    // Similar setup code as above...
    // Similar setup as above...
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    // Test empty stream
    const uint32_t test_partition = 0;
    auto& spilled_stream = local_state->_shared_state->spilled_streams[test_partition];
    ASSERT_TRUE(ExecEnv::GetInstance()
                        ->spill_stream_mgr()
                        ->register_spill_stream(
                                _helper.runtime_state.get(), spilled_stream,
                                print_id(_helper.runtime_state->query_id()), "hash_build",
                                probe_operator->node_id(), std::numeric_limits<int32_t>::max(),
                                std::numeric_limits<size_t>::max(), local_state->operator_profile())
                        .ok());

    ASSERT_TRUE(spilled_stream->spill_eof().ok());

    ASSERT_TRUE(local_state->_recovered_build_block == nullptr);

    // Test error handling with fault injection
    SpillableDebugPointHelper dp_helper(
            "fault_inject::partitioned_hash_join_probe::recover_build_blocks");
    bool has_data = false;
    auto status = local_state->recover_build_blocks_from_disk(_helper.runtime_state.get(),
                                                              test_partition, has_data);

    ASSERT_TRUE(status.ok()) << "recover build blocks failed: " << status.to_string();
    while (local_state->_spill_dependency->_ready.load() == false) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    auto spill_status = dp_helper.get_spill_status();
    ASSERT_FALSE(spill_status.ok());
    ASSERT_TRUE(spill_status.to_string().find("fault_inject partitioned_hash_join_probe "
                                              "recover_build_blocks failed") != std::string::npos)
            << "incorrect recover build blocks status: " << spill_status.to_string();
}

TEST_F(PartitionedHashJoinProbeOperatorTest, GetBlockTestNonSpill) {
    // Setup operators and pipeline
    auto [probe_operator, sink_operator] = _helper.create_operators();

    // Initialize local state
    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    // Setup child data
    auto input_block = vectorized::Block::create_unique();
    input_block->swap(vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3}));

    auto probe_side_source_operator =
            std::dynamic_pointer_cast<MockChildOperator>(probe_operator->get_child());
    probe_side_source_operator->set_block(std::move(*input_block));

    local_state->_shared_state->need_to_spill = false;

    // Test non empty input block path
    {
        vectorized::Block output_block;
        bool eos = false;

        auto st = probe_operator->get_block(_helper.runtime_state.get(), &output_block, &eos);
        ASSERT_TRUE(st.ok());
        ASSERT_FALSE(eos);
    }

    // Test empty input block case
    {
        auto empty_block = vectorized::Block::create_unique();
        probe_side_source_operator->set_block(std::move(*empty_block));

        vectorized::Block output_block;
        bool eos = false;

        auto st = probe_operator->get_block(_helper.runtime_state.get(), &output_block, &eos);
        ASSERT_TRUE(st.ok());
        ASSERT_FALSE(eos);
        ASSERT_EQ(output_block.rows(), 0);
    }

    // Test end of stream case
    {
        probe_side_source_operator->set_eos();
        vectorized::Block output_block;
        bool eos = false;

        auto st = probe_operator->get_block(_helper.runtime_state.get(), &output_block, &eos);
        ASSERT_TRUE(st.ok()) << "get block failed: " << st.to_string();
        ASSERT_TRUE(eos);
    }
}

TEST_F(PartitionedHashJoinProbeOperatorTest, PushEmptyBlock) {
    // Setup test environment
    auto [probe_operator, sink_operator] = _helper.create_operators();

    // Setup local state
    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    // Create empty input block
    vectorized::Block empty_block;

    // Test pushing empty block without EOS
    auto st = probe_operator->push(_helper.runtime_state.get(), &empty_block, false);
    ASSERT_TRUE(st.ok());

    // Verify no partitioned blocks were created
    for (uint32_t i = 0; i < probe_operator->_partition_count; ++i) {
        ASSERT_EQ(local_state->_partitioned_blocks[i], nullptr);
    }
}

TEST_F(PartitionedHashJoinProbeOperatorTest, PushPartitionData) {
    // Setup test environment
    auto [probe_operator, sink_operator] = _helper.create_operators();

    // Initialize local state
    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);
    // Setup row descriptor and partitioner
    RowDescriptor row_desc(_helper.runtime_state->desc_tbl(), {0}, {false});
    const auto& tnode = probe_operator->_tnode;
    local_state->_partitioner = create_spill_partitioner(
            _helper.runtime_state.get(), PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT,
            {tnode.hash_join_node.eq_join_conjuncts[0].left}, row_desc);

    // Create test input block
    vectorized::Block input_block =
            vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3, 4, 5});

    // Test pushing data
    auto st = probe_operator->push(_helper.runtime_state.get(), &input_block, false);
    ASSERT_TRUE(st.ok());

    // Verify partitioned blocks
    int64_t total_partitioned_rows = 0;
    for (uint32_t i = 0; i < probe_operator->_partition_count; ++i) {
        if (local_state->_partitioned_blocks[i]) {
            total_partitioned_rows += local_state->_partitioned_blocks[i]->rows();
        }
    }
    ASSERT_EQ(total_partitioned_rows, 5); // All rows should be partitioned
}

TEST_F(PartitionedHashJoinProbeOperatorTest, PushWithEOS) {
    // Setup test environment
    auto [probe_operator, sink_operator] = _helper.create_operators();

    // Initialize local state
    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    // Setup row descriptor and partitioner
    RowDescriptor row_desc(_helper.runtime_state->desc_tbl(), {0}, {false});
    const auto& tnode = probe_operator->_tnode;
    local_state->_partitioner = create_spill_partitioner(
            _helper.runtime_state.get(), PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT,
            {tnode.hash_join_node.eq_join_conjuncts[0].left}, row_desc);

    // Create test data and push with EOS
    vectorized::Block input_block =
            vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3});

    auto st = probe_operator->push(_helper.runtime_state.get(), &input_block, false);
    ASSERT_TRUE(st.ok()) << "Push failed: " << st.to_string();

    input_block.clear();
    st = probe_operator->push(_helper.runtime_state.get(), &input_block, true);
    ASSERT_TRUE(st.ok()) << "Push failed: " << st.to_string();

    // Verify all data is moved to probe blocks due to EOS
    int64_t total_probe_block_rows = 0;
    for (uint32_t i = 0; i < probe_operator->_partition_count; ++i) {
        for (const auto& block : local_state->_probe_blocks[i]) {
            total_probe_block_rows += block.rows();
        }
    }
    ASSERT_EQ(total_probe_block_rows, 3); // All rows should be in probe blocks

    // Verify partitioned blocks are cleared
    for (uint32_t i = 0; i < probe_operator->_partition_count; ++i) {
        ASSERT_EQ(local_state->_partitioned_blocks[i], nullptr);
    }
}

TEST_F(PartitionedHashJoinProbeOperatorTest, PushLargeBlock) {
    // Setup test environment
    auto [probe_operator, sink_operator] = _helper.create_operators();

    // Initialize local state
    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    // Setup row descriptor and partitioner
    RowDescriptor row_desc(_helper.runtime_state->desc_tbl(), {0}, {false});
    const auto& tnode = probe_operator->_tnode;
    local_state->_partitioner = create_spill_partitioner(
            _helper.runtime_state.get(), PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT,
            {tnode.hash_join_node.eq_join_conjuncts[0].left}, row_desc);

    // Create large input block (> 2M rows)
    std::vector<int32_t> large_data(3 * 1024 * 1024);
    std::iota(large_data.begin(), large_data.end(), 0);
    vectorized::Block large_block =
            vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(large_data);

    // Push large block
    auto st = probe_operator->push(_helper.runtime_state.get(), &large_block, false);
    ASSERT_TRUE(st.ok());

    // Verify some partitions have blocks moved to probe_blocks due to size threshold
    bool found_probe_blocks = false;
    size_t partitioned_rows_count = 0;
    for (uint32_t i = 0; i < probe_operator->_partition_count; ++i) {
        if (!local_state->_probe_blocks[i].empty()) {
            for (auto& block : local_state->_probe_blocks[i]) {
                if (!block.empty()) {
                    partitioned_rows_count += block.rows();
                    found_probe_blocks = true;
                }
            }
        }
        if (local_state->_partitioned_blocks[i] && !local_state->_partitioned_blocks[i]->empty()) {
            partitioned_rows_count += local_state->_partitioned_blocks[i]->rows();
            found_probe_blocks = true;
        }
    }

    ASSERT_EQ(partitioned_rows_count, large_block.rows());
    ASSERT_TRUE(found_probe_blocks);

    // Verify bytes counter
    auto* probe_blocks_bytes = local_state->custom_profile()->get_counter("ProbeBloksBytesInMem");
    ASSERT_GT(probe_blocks_bytes->value(), 0);
}

TEST_F(PartitionedHashJoinProbeOperatorTest, PullBasic) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    local_state->_need_to_setup_internal_operators = true;
    local_state->_partition_cursor = 0;

    vectorized::Block test_block;
    bool eos = false;

    auto st = probe_operator->pull(_helper.runtime_state.get(), &test_block, &eos);
    ASSERT_TRUE(st.ok()) << "Pull failed: " << st.to_string();
    ASSERT_FALSE(eos) << "First pull should not be eos";

    ASSERT_EQ(1, local_state->_partition_cursor) << "Partition cursor should be 1";
}

TEST_F(PartitionedHashJoinProbeOperatorTest, PullMultiplePartitions) {
    auto [probe_operator, sink_operator] = _helper.create_operators();
    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    for (uint32_t i = 0; i < PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT; i++) {
        auto& probe_blocks = local_state->_probe_blocks[i];
        probe_blocks.emplace_back(
                vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3}));
    }

    vectorized::Block output_block;
    bool eos = false;

    for (uint32_t i = 0; i < PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT; i++) {
        local_state->_partition_cursor = i;
        local_state->_need_to_setup_internal_operators = true;

        auto st = probe_operator->pull(_helper.runtime_state.get(), &output_block, &eos);
        ASSERT_TRUE(st.ok()) << "Pull failed for partition " << i;

        if (i == PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT - 1) {
            ASSERT_TRUE(eos) << "Last partition should be eos";
        } else {
            ASSERT_FALSE(eos) << "Non-last partition should not be eos";
        }
    }
}

TEST_F(PartitionedHashJoinProbeOperatorTest, PullWithDiskRecovery) {
    auto [probe_operator, sink_operator] = _helper.create_operators();
    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    local_state->_shared_state->need_to_spill = true;

    const uint32_t test_partition = 0;
    auto& spilled_stream = local_state->_shared_state->spilled_streams[test_partition];
    auto& spilling_stream = local_state->_probe_spilling_streams[test_partition];

    local_state->_need_to_setup_internal_operators = true;

    auto st = ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
            _helper.runtime_state.get(), spilled_stream,
            print_id(_helper.runtime_state->query_id()), "hash_probe_spilled",
            probe_operator->node_id(), std::numeric_limits<int32_t>::max(),
            std::numeric_limits<size_t>::max(), local_state->operator_profile());

    ASSERT_TRUE(st) << "Register spill stream failed: " << st.to_string();
    st = ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
            _helper.runtime_state.get(), spilling_stream,
            print_id(_helper.runtime_state->query_id()), "hash_probe", probe_operator->node_id(),
            std::numeric_limits<int32_t>::max(), std::numeric_limits<size_t>::max(),
            local_state->operator_profile());

    ASSERT_TRUE(st) << "Register spill stream failed: " << st.to_string();

    vectorized::Block spill_block =
            vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3});
    st = spilled_stream->spill_block(_helper.runtime_state.get(), spill_block, true);
    ASSERT_TRUE(st) << "Spill block failed: " << st.to_string();
    st = spilling_stream->spill_block(_helper.runtime_state.get(), spill_block, false);
    ASSERT_TRUE(st) << "Spill block failed: " << st.to_string();

    vectorized::Block output_block;
    bool eos = false;

    st = probe_operator->pull(_helper.runtime_state.get(), &output_block, &eos);
    ASSERT_TRUE(st.ok()) << "Pull failed: " << st.to_string();
    while (local_state->_spill_dependency->_ready.load() == false) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    st = probe_operator->pull(_helper.runtime_state.get(), &output_block, &eos);
    while (local_state->_spill_dependency->_ready.load() == false) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    ASSERT_TRUE(st.ok()) << "Pull failed: " << st.to_string();
    ASSERT_FALSE(eos) << "Should not be eos during disk recovery";

    ASSERT_GT(local_state->_recovery_probe_rows->value(), 0)
            << "Should have recovered some rows from disk";
}

TEST_F(PartitionedHashJoinProbeOperatorTest, PullWithEmptyPartition) {
    auto [probe_operator, sink_operator] = _helper.create_operators();
    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    // 设置空分区
    local_state->_partition_cursor = 0;
    local_state->_need_to_setup_internal_operators = true;

    vectorized::Block output_block;
    bool eos = false;

    auto st = probe_operator->pull(_helper.runtime_state.get(), &output_block, &eos);
    ASSERT_TRUE(st.ok()) << "Pull failed for empty partition";
    ASSERT_FALSE(eos) << "Should not be eos for first empty partition";

    // 验证分区游标已更新
    ASSERT_EQ(1, local_state->_partition_cursor)
            << "Partition cursor should move to next after empty partition";
}

TEST_F(PartitionedHashJoinProbeOperatorTest, Other) {
    auto [probe_operator, _] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    local_state->_shared_state->need_to_spill = true;
    ASSERT_FALSE(probe_operator->_should_revoke_memory(_helper.runtime_state.get()));

    auto st = probe_operator->_revoke_memory(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "Revoke memory failed: " << st.to_string();
}

} // namespace doris::pipeline