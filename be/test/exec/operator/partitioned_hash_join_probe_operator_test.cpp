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

#include "exec/operator/partitioned_hash_join_probe_operator.h"

#include <gtest/gtest.h>

#include <memory>

#include "common/config.h"
#include "core/block/block.h"
#include "core/data_type/data_type_number.h"
#include "exec/operator/hashjoin_build_sink.h"
#include "exec/operator/partitioned_hash_join_sink_operator.h"
#include "exec/operator/partitioned_hash_join_test_helper.h"
#include "exec/pipeline/pipeline_task.h"
#include "exec/spill/spill_file_manager.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/runtime_profile.h"
#include "testutil/column_helper.h"
#include "testutil/creators.h"
#include "testutil/mock/mock_operators.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {
class PartitionedHashJoinProbeOperatorTest : public testing::Test {
public:
    void SetUp() override { _helper.SetUp(); }
    void TearDown() override { _helper.TearDown(); }

protected:
    PartitionedHashJoinTestHelper _helper;
};

namespace {

SpillFileSPtr create_probe_test_spill_file(RuntimeState* state, RuntimeProfile* profile,
                                           int node_id, const std::string& prefix,
                                           const std::vector<std::vector<int32_t>>& batches) {
    SpillFileSPtr spill_file;
    auto relative_path = fmt::format("{}/{}-{}-{}", print_id(state->query_id()), prefix, node_id,
                                     ExecEnv::GetInstance()->spill_file_mgr()->next_id());
    auto st =
            ExecEnv::GetInstance()->spill_file_mgr()->create_spill_file(relative_path, spill_file);
    EXPECT_TRUE(st.ok()) << "create spill file failed: " << st.to_string();
    if (!st.ok()) {
        return nullptr;
    }

    SpillFileWriterSPtr writer;
    st = spill_file->create_writer(state, profile, writer);
    EXPECT_TRUE(st.ok()) << "create writer failed: " << st.to_string();
    if (!st.ok()) {
        return nullptr;
    }

    for (const auto& batch : batches) {
        Block block = ColumnHelper::create_block<DataTypeInt32>(batch);
        st = writer->write_block(state, block);
        EXPECT_TRUE(st.ok()) << "write block failed: " << st.to_string();
        if (!st.ok()) {
            return nullptr;
        }
    }

    st = writer->close();
    EXPECT_TRUE(st.ok()) << "close writer failed: " << st.to_string();
    if (!st.ok()) {
        return nullptr;
    }
    return spill_file;
}

int64_t count_spill_rows(RuntimeState* state, RuntimeProfile* profile,
                         const SpillFileSPtr& spill_file) {
    auto reader = spill_file->create_reader(state, profile);
    auto st = reader->open();
    EXPECT_TRUE(st.ok()) << "open reader failed: " << st.to_string();
    if (!st.ok()) {
        return 0;
    }

    int64_t rows = 0;
    bool eos = false;
    while (!eos) {
        Block block;
        st = reader->read(&block, &eos);
        EXPECT_TRUE(st.ok()) << "read block failed: " << st.to_string();
        if (!st.ok()) {
            return rows;
        }
        rows += block.rows();
    }
    st = reader->close();
    EXPECT_TRUE(st.ok()) << "close reader failed: " << st.to_string();
    return rows;
}

Status prepare_probe_local_state_for_repartition(PartitionedHashJoinProbeOperatorX* probe_operator,
                                                 PartitionedHashJoinProbeLocalState* local_state,
                                                 RuntimeState* state) {
    RETURN_IF_ERROR(probe_operator->init(probe_operator->_tnode, state));
    probe_operator->_inner_sink_operator->_child = nullptr;
    probe_operator->_inner_probe_operator->_child = nullptr;
    probe_operator->_inner_probe_operator->_build_side_child = nullptr;
    RETURN_IF_ERROR(probe_operator->prepare(state));
    return local_state->open(state);
}

} // namespace

TEST_F(PartitionedHashJoinProbeOperatorTest, debug_string) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    auto debug_string = local_state->debug_string(0);
    std::cout << "debug string: " << debug_string << std::endl;

    shared_state->_is_spilled = false;
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
    RowDescriptor row_desc(_helper.runtime_state->desc_tbl(), {0});
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

    local_state->_shared_state->_inner_shared_state = std::make_shared<MockHashJoinSharedState>();
    local_state->_shared_state->_inner_runtime_state = std::make_unique<MockRuntimeState>();
    local_state->_shared_state->_inner_runtime_state->set_desc_tbl(
            &(_helper.runtime_state->desc_tbl()));
    local_state->_shared_state->_inner_runtime_state->resize_op_id_to_local_state(-100);

    auto mock_inner_sink_operator = probe_operator->_inner_sink_operator;
    probe_operator->_inner_sink_operator = std::make_shared<HashJoinBuildSinkOperatorX>(
            _helper.obj_pool.get(), 0, 0, tnode, _helper.runtime_state->desc_tbl());
    EXPECT_TRUE(probe_operator->_inner_sink_operator->set_child(mock_inner_sink_operator->child()));

    st = probe_operator->_inner_sink_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st) << "init inner sink operator failed: " << st.to_string();

    auto inner_probe_state = std::make_unique<HashJoinProbeLocalState>(
            _helper.runtime_state.get(), probe_operator->_inner_probe_operator.get());

    st = inner_probe_state->init(local_state->_shared_state->_inner_runtime_state.get(), info);
    ASSERT_TRUE(st) << "init failed: " << st.to_string();

    local_state->_shared_state->_inner_runtime_state->emplace_local_state(
            probe_operator->_inner_probe_operator->operator_id(), std::move(inner_probe_state));

    auto inner_sink_state = std::make_unique<HashJoinBuildSinkLocalState>(
            probe_operator->_inner_sink_operator.get(), _helper.runtime_state.get());

    LocalSinkStateInfo sink_info {0,  _helper.operator_profile.get(),
                                  -1, local_state->_shared_state->_inner_shared_state.get(),
                                  {}, {}};
    st = probe_operator->_inner_sink_operator->prepare(
            local_state->_shared_state->_inner_runtime_state.get());
    ASSERT_TRUE(st) << "prepare failed: " << st.to_string();

    st = inner_sink_state->init(local_state->_shared_state->_inner_runtime_state.get(), sink_info);
    ASSERT_TRUE(st) << "init failed: " << st.to_string();

    local_state->_shared_state->_inner_runtime_state->emplace_sink_local_state(
            0, std::move(inner_sink_state));

    local_state->_shared_state->_is_spilled = false;
    local_state->update_profile_from_inner();

    local_state->_shared_state->_is_spilled = true;
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

TEST_F(PartitionedHashJoinProbeOperatorTest, CloseReleasesSpillResources) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    auto create_spill_file = [&](const std::string& prefix,
                                 std::initializer_list<int32_t> values) -> SpillFileSPtr {
        SpillFileSPtr spill_file;
        auto relative_path = fmt::format("{}/{}-{}-{}", print_id(_helper.runtime_state->query_id()),
                                         prefix, probe_operator->node_id(),
                                         ExecEnv::GetInstance()->spill_file_mgr()->next_id());
        auto st = ExecEnv::GetInstance()->spill_file_mgr()->create_spill_file(relative_path,
                                                                              spill_file);
        EXPECT_TRUE(st.ok()) << "create spill file failed: " << st.to_string();
        if (!st.ok()) {
            return nullptr;
        }

        SpillFileWriterSPtr writer;
        st = spill_file->create_writer(_helper.runtime_state.get(), local_state->operator_profile(),
                                       writer);
        EXPECT_TRUE(st.ok()) << "create spill writer failed: " << st.to_string();
        if (!st.ok()) {
            return nullptr;
        }

        Block block = ColumnHelper::create_block<DataTypeInt32>(values);
        st = writer->write_block(_helper.runtime_state.get(), block);
        EXPECT_TRUE(st.ok()) << "write spill block failed: " << st.to_string();
        if (!st.ok()) {
            return nullptr;
        }

        st = writer->close();
        EXPECT_TRUE(st.ok()) << "close spill writer failed: " << st.to_string();
        if (!st.ok()) {
            return nullptr;
        }
        return spill_file;
    };
    auto expect_spill_file_deleted = [&](const SpillFileSPtr& spill_file) {
        auto reader = spill_file->create_reader(_helper.runtime_state.get(),
                                                local_state->operator_profile());
        auto st = reader->open();
        EXPECT_FALSE(st.ok()) << "spill file should have been deleted";
    };

    auto queued_build_file = create_spill_file("hash_build_close_queue", {1, 2, 3});
    auto queued_probe_file = create_spill_file("hash_probe_close_queue", {4, 5, 6});
    auto current_build_file = create_spill_file("hash_build_close_current", {7, 8, 9});
    auto current_probe_file = create_spill_file("hash_probe_close_current", {10, 11, 12});

    ASSERT_TRUE(queued_build_file != nullptr);
    ASSERT_TRUE(queued_probe_file != nullptr);
    ASSERT_TRUE(current_build_file != nullptr);
    ASSERT_TRUE(current_probe_file != nullptr);

    SpillFileWriterSPtr writer;
    auto st = local_state->acquire_spill_writer(_helper.runtime_state.get(), 0, writer);
    ASSERT_TRUE(st.ok()) << "acquire spill writer failed: " << st.to_string();
    Block writer_block = ColumnHelper::create_block<DataTypeInt32>({13, 14, 15});
    st = writer->write_block(_helper.runtime_state.get(), writer_block);
    ASSERT_TRUE(st.ok()) << "write spill block failed: " << st.to_string();

    local_state->_spill_partition_queue.emplace_back(queued_build_file, queued_probe_file, 2);
    local_state->_current_partition =
            JoinSpillPartitionInfo(current_build_file, current_probe_file, 1);
    local_state->_queue_probe_blocks.emplace_back(
            ColumnHelper::create_block<DataTypeInt32>({16, 17, 18}));

    local_state->_current_build_reader = current_build_file->create_reader(
            _helper.runtime_state.get(), local_state->operator_profile());
    st = local_state->_current_build_reader->open();
    ASSERT_TRUE(st.ok()) << "open current build reader failed: " << st.to_string();

    local_state->_current_probe_reader = current_probe_file->create_reader(
            _helper.runtime_state.get(), local_state->operator_profile());
    st = local_state->_current_probe_reader->open();
    ASSERT_TRUE(st.ok()) << "open current probe reader failed: " << st.to_string();

    st = local_state->close(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "close failed: " << st.to_string();

    ASSERT_TRUE(local_state->_probe_writers.empty());
    ASSERT_TRUE(local_state->_probe_spilling_groups[0] != nullptr);
    ASSERT_TRUE(local_state->_probe_spilling_groups[0]->ready_for_reading());
    ASSERT_EQ(local_state->_current_build_reader, nullptr);
    ASSERT_EQ(local_state->_current_probe_reader, nullptr);
    ASSERT_TRUE(local_state->_spill_partition_queue.empty());
    ASSERT_FALSE(local_state->_current_partition.is_valid());
    ASSERT_EQ(local_state->_current_partition.build_file, nullptr);
    ASSERT_EQ(local_state->_current_partition.probe_file, nullptr);
    ASSERT_TRUE(local_state->_queue_probe_blocks.empty());
    ASSERT_TRUE(local_state->_closed);

    expect_spill_file_deleted(queued_build_file);
    expect_spill_file_deleted(queued_probe_file);
    expect_spill_file_deleted(current_build_file);
    expect_spill_file_deleted(current_probe_file);

    ExecEnv::GetInstance()->spill_file_mgr()->delete_spill_file(
            local_state->_probe_spilling_groups[0]);
    local_state->_probe_spilling_groups[0].reset();
}

TEST_F(PartitionedHashJoinProbeOperatorTest, CloseReturnsWriterCloseError) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    SpillFileWriterSPtr writer;
    auto st = local_state->acquire_spill_writer(_helper.runtime_state.get(), 0, writer);
    ASSERT_TRUE(st.ok()) << "acquire spill writer failed: " << st.to_string();

    local_state->_spill_partition_queue.emplace_back(JoinSpillPartitionInfo(nullptr, nullptr, 0));
    local_state->_current_partition = JoinSpillPartitionInfo(nullptr, nullptr, 1);
    local_state->_queue_probe_blocks.emplace_back(ColumnHelper::create_block<DataTypeInt32>({1}));

    {
        SpillableDebugPointHelper dp_helper("fault_inject::spill_file::spill_eof");
        st = local_state->close(_helper.runtime_state.get());
    }
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.to_string().find("fault_inject spill_file spill_eof failed") !=
                std::string::npos)
            << "unexpected error: " << st.to_string();

    ASSERT_TRUE(local_state->_probe_writers.empty());
    ASSERT_TRUE(local_state->_spill_partition_queue.empty());
    ASSERT_FALSE(local_state->_current_partition.is_valid());
    ASSERT_TRUE(local_state->_queue_probe_blocks.empty());
    ASSERT_TRUE(local_state->_closed);

    st = local_state->close(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "reentrant close failed: " << st.to_string();

    ExecEnv::GetInstance()->spill_file_mgr()->delete_spill_file(
            local_state->_probe_spilling_groups[0]);
    local_state->_probe_spilling_groups[0].reset();
}

TEST_F(PartitionedHashJoinProbeOperatorTest, RepartitionCurrentPartition) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    auto st = probe_operator->init(probe_operator->_tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    probe_operator->_inner_sink_operator->_child = nullptr;
    probe_operator->_inner_probe_operator->_child = nullptr;
    probe_operator->_inner_probe_operator->_build_side_child = nullptr;

    st = probe_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "prepare failed: " << st.to_string();

    st = local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

    auto create_spill_file =
            [&](const std::string& prefix,
                const std::vector<std::vector<int32_t>>& batches) -> SpillFileSPtr {
        SpillFileSPtr spill_file;
        auto relative_path = fmt::format("{}/{}-{}-{}", print_id(_helper.runtime_state->query_id()),
                                         prefix, probe_operator->node_id(),
                                         ExecEnv::GetInstance()->spill_file_mgr()->next_id());
        auto status = ExecEnv::GetInstance()->spill_file_mgr()->create_spill_file(relative_path,
                                                                                  spill_file);
        EXPECT_TRUE(status.ok()) << "create spill file failed: " << status.to_string();
        if (!status.ok()) {
            return nullptr;
        }

        SpillFileWriterSPtr writer;
        status = spill_file->create_writer(_helper.runtime_state.get(),
                                           local_state->operator_profile(), writer);
        EXPECT_TRUE(status.ok()) << "create writer failed: " << status.to_string();
        if (!status.ok()) {
            return nullptr;
        }

        for (const auto& batch : batches) {
            Block block = ColumnHelper::create_block<DataTypeInt32>(batch);
            status = writer->write_block(_helper.runtime_state.get(), block);
            EXPECT_TRUE(status.ok()) << "write block failed: " << status.to_string();
            if (!status.ok()) {
                return nullptr;
            }
        }

        status = writer->close();
        EXPECT_TRUE(status.ok()) << "close writer failed: " << status.to_string();
        if (!status.ok()) {
            return nullptr;
        }
        return spill_file;
    };
    auto count_rows = [&](const SpillFileSPtr& spill_file) -> int64_t {
        auto reader = spill_file->create_reader(_helper.runtime_state.get(),
                                                local_state->operator_profile());
        auto status = reader->open();
        EXPECT_TRUE(status.ok()) << "open reader failed: " << status.to_string();
        if (!status.ok()) {
            return 0;
        }

        int64_t rows = 0;
        bool eos = false;
        while (!eos) {
            Block block;
            status = reader->read(&block, &eos);
            EXPECT_TRUE(status.ok()) << "read block failed: " << status.to_string();
            if (!status.ok()) {
                return rows;
            }
            rows += block.rows();
        }
        status = reader->close();
        EXPECT_TRUE(status.ok()) << "close reader failed: " << status.to_string();
        return rows;
    };

    auto build_file = create_spill_file("hash_build_repartition_test", {{1, 2, 3}, {4, 5}});
    auto probe_file = create_spill_file("hash_probe_repartition_test", {{6, 7}, {8, 9, 10}});
    ASSERT_TRUE(build_file != nullptr);
    ASSERT_TRUE(probe_file != nullptr);

    JoinSpillPartitionInfo partition(build_file, probe_file, 0);
    local_state->_current_build_reader =
            build_file->create_reader(_helper.runtime_state.get(), local_state->operator_profile());
    st = local_state->_current_build_reader->open();
    ASSERT_TRUE(st.ok()) << "open current build reader failed: " << st.to_string();

    Block recovered_block;
    bool eos = false;
    st = local_state->_current_build_reader->read(&recovered_block, &eos);
    ASSERT_TRUE(st.ok()) << "read recovered build block failed: " << st.to_string();
    ASSERT_FALSE(eos);
    local_state->_recovered_build_block = MutableBlock::create_unique(std::move(recovered_block));

    st = local_state->repartition_current_partition(_helper.runtime_state.get(), partition);
    ASSERT_TRUE(st.ok()) << "repartition current partition failed: " << st.to_string();

    ASSERT_EQ(partition.build_file, nullptr);
    ASSERT_EQ(partition.probe_file, nullptr);
    ASSERT_EQ(local_state->_recovered_build_block, nullptr);
    ASSERT_EQ(local_state->_current_build_reader, nullptr);
    ASSERT_EQ(local_state->_current_probe_reader, nullptr);
    ASSERT_EQ(local_state->_spill_partition_queue.size(),
              PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT);
    ASSERT_EQ(local_state->_total_partition_spills->value(),
              PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT);
    ASSERT_EQ(local_state->_max_partition_level_seen, 1);
    ASSERT_EQ(local_state->_max_partition_level->value(), 1);

    int64_t repartitioned_build_rows = 0;
    int64_t repartitioned_probe_rows = 0;
    for (auto& queue_partition : local_state->_spill_partition_queue) {
        ASSERT_TRUE(queue_partition.is_valid());
        ASSERT_EQ(queue_partition.level, 1);
        repartitioned_build_rows += count_rows(queue_partition.build_file);
        repartitioned_probe_rows += count_rows(queue_partition.probe_file);
    }
    ASSERT_EQ(repartitioned_build_rows, 5);
    ASSERT_EQ(repartitioned_probe_rows, 5);

    for (auto& queue_partition : local_state->_spill_partition_queue) {
        ExecEnv::GetInstance()->spill_file_mgr()->delete_spill_file(queue_partition.build_file);
        ExecEnv::GetInstance()->spill_file_mgr()->delete_spill_file(queue_partition.probe_file);
    }
    local_state->_spill_partition_queue.clear();
    ExecEnv::GetInstance()->spill_file_mgr()->delete_spill_file(build_file);
    ExecEnv::GetInstance()->spill_file_mgr()->delete_spill_file(probe_file);
}

TEST_F(PartitionedHashJoinProbeOperatorTest, RepartitionCurrentPartitionExceedsMaxDepth) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    auto st = probe_operator->init(probe_operator->_tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    probe_operator->_repartition_max_depth = 1;
    JoinSpillPartitionInfo partition(nullptr, nullptr, 0);

    st = local_state->repartition_current_partition(_helper.runtime_state.get(), partition);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.to_string().find("exceeded max depth 1") != std::string::npos)
            << "unexpected error: " << st.to_string();
    ASSERT_TRUE(local_state->_spill_partition_queue.empty());
}

TEST_F(PartitionedHashJoinProbeOperatorTest, RevokeBuildData) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    auto st = prepare_probe_local_state_for_repartition(probe_operator.get(), local_state,
                                                        _helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "prepare probe local state failed: " << st.to_string();

    auto build_file = create_probe_test_spill_file(
            _helper.runtime_state.get(), local_state->operator_profile(), probe_operator->node_id(),
            "hash_build_revoke_test", {{1, 2, 3}, {4, 5}});
    auto probe_file = create_probe_test_spill_file(
            _helper.runtime_state.get(), local_state->operator_profile(), probe_operator->node_id(),
            "hash_probe_revoke_test", {{6, 7}, {8, 9, 10}});
    ASSERT_TRUE(build_file != nullptr);
    ASSERT_TRUE(probe_file != nullptr);

    local_state->_child_eos = true;
    local_state->_spill_queue_initialized = true;
    local_state->_need_to_setup_queue_partition = false;
    local_state->_current_partition = JoinSpillPartitionInfo(build_file, probe_file, 0);
    local_state->_queue_probe_blocks.emplace_back(
            ColumnHelper::create_block<DataTypeInt32>({11, 12}));

    local_state->_current_build_reader =
            build_file->create_reader(_helper.runtime_state.get(), local_state->operator_profile());
    st = local_state->_current_build_reader->open();
    ASSERT_TRUE(st.ok()) << "open current build reader failed: " << st.to_string();

    Block recovered_block;
    bool eos = false;
    st = local_state->_current_build_reader->read(&recovered_block, &eos);
    ASSERT_TRUE(st.ok()) << "read recovered build block failed: " << st.to_string();
    ASSERT_FALSE(eos);
    local_state->_recovered_build_block = MutableBlock::create_unique(std::move(recovered_block));

    st = local_state->revoke_build_data(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "revoke build data failed: " << st.to_string();

    ASSERT_FALSE(local_state->_current_partition.is_valid());
    ASSERT_TRUE(local_state->_need_to_setup_queue_partition);
    ASSERT_TRUE(local_state->_queue_probe_blocks.empty());
    ASSERT_EQ(local_state->_recovered_build_block, nullptr);
    ASSERT_EQ(local_state->_current_build_reader, nullptr);
    ASSERT_EQ(local_state->_current_probe_reader, nullptr);
    ASSERT_EQ(local_state->_spill_partition_queue.size(),
              PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT);
    ASSERT_EQ(local_state->_total_partition_spills->value(),
              PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT);
    ASSERT_EQ(local_state->_max_partition_level_seen, 1);
    ASSERT_EQ(local_state->_max_partition_level->value(), 1);

    int64_t repartitioned_build_rows = 0;
    int64_t repartitioned_probe_rows = 0;
    for (auto& queue_partition : local_state->_spill_partition_queue) {
        ASSERT_TRUE(queue_partition.is_valid());
        ASSERT_EQ(queue_partition.level, 1);
        repartitioned_build_rows +=
                count_spill_rows(_helper.runtime_state.get(), local_state->operator_profile(),
                                 queue_partition.build_file);
        repartitioned_probe_rows +=
                count_spill_rows(_helper.runtime_state.get(), local_state->operator_profile(),
                                 queue_partition.probe_file);
    }
    ASSERT_EQ(repartitioned_build_rows, 5);
    ASSERT_EQ(repartitioned_probe_rows, 5);

    for (auto& queue_partition : local_state->_spill_partition_queue) {
        ExecEnv::GetInstance()->spill_file_mgr()->delete_spill_file(queue_partition.build_file);
        ExecEnv::GetInstance()->spill_file_mgr()->delete_spill_file(queue_partition.probe_file);
    }
    local_state->_spill_partition_queue.clear();
    ExecEnv::GetInstance()->spill_file_mgr()->delete_spill_file(build_file);
    ExecEnv::GetInstance()->spill_file_mgr()->delete_spill_file(probe_file);
}

TEST_F(PartitionedHashJoinProbeOperatorTest, RevokeBuildDataPropagatesRepartitionError) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    probe_operator->_repartition_max_depth = 1;
    local_state->_child_eos = true;
    local_state->_spill_queue_initialized = true;
    local_state->_need_to_setup_queue_partition = false;
    local_state->_current_partition = JoinSpillPartitionInfo(nullptr, nullptr, 0);
    local_state->_queue_probe_blocks.emplace_back(ColumnHelper::create_block<DataTypeInt32>({1}));

    auto st = local_state->revoke_build_data(_helper.runtime_state.get());
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.to_string().find("exceeded max depth 1") != std::string::npos)
            << "unexpected error: " << st.to_string();
    ASSERT_TRUE(local_state->_current_partition.is_valid());
    ASSERT_FALSE(local_state->_need_to_setup_queue_partition);
    ASSERT_EQ(local_state->_queue_probe_blocks.size(), 1);
    ASSERT_TRUE(local_state->_spill_partition_queue.empty());
}

TEST_F(PartitionedHashJoinProbeOperatorTest, spill_probe_blocks) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    RowDescriptor row_desc(_helper.runtime_state->desc_tbl(), {0});
    const auto& tnode = probe_operator->_tnode;
    local_state->_partitioner = create_spill_partitioner(
            _helper.runtime_state.get(), PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT,
            {tnode.hash_join_node.eq_join_conjuncts[0].left}, row_desc);

    // create probe blocks in _partitioned_blocks
    for (int32_t i = 0; i != PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT; ++i) {
        if (i % 2 == 0) {
            continue;
        }

        Block block = ColumnHelper::create_block<DataTypeInt32>({1 * i, 2 * i, 3 * i});
        local_state->_partitioned_blocks[i] = MutableBlock::create_unique(std::move(block));
    }

    std::vector<int32_t> large_data(3 * 1024 * 1024);
    std::iota(large_data.begin(), large_data.end(), 0);
    Block large_block = ColumnHelper::create_block<DataTypeInt32>(large_data);

    std::vector<int32_t> small_data(3 * 1024);
    std::iota(small_data.begin(), small_data.end(), 3 * 1024 * 1024);
    Block small_block = ColumnHelper::create_block<DataTypeInt32>(small_data);

    // add a large block to the last partition (overwrite if needed)
    local_state->_partitioned_blocks[PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT - 1] =
            MutableBlock::create_unique(std::move(large_block));

    // add a small block to the first partition (overwrite; first partition is even so was skipped above)
    local_state->_partitioned_blocks[0] = MutableBlock::create_unique(std::move(small_block));

    local_state->_shared_state->_is_spilled = false;
    local_state->update_profile_from_inner();

    local_state->_shared_state->_is_spilled = true;
    auto st = local_state->spill_probe_blocks(_helper.runtime_state.get(), true);
    ASSERT_TRUE(st.ok()) << "spill probe blocks failed: " << st.to_string();

    local_state->update_profile_from_inner();

    std::cout << "profile: " << local_state->custom_profile()->pretty_print() << std::endl;

    for (int32_t i = 0; i != PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT; ++i) {
        if (!local_state->_probe_spilling_groups[i]) {
            continue;
        }
        ExecEnv::GetInstance()->spill_file_mgr()->delete_spill_file(
                local_state->_probe_spilling_groups[i]);
        local_state->_probe_spilling_groups[i].reset();
    }

    auto* write_rows_counter = local_state->custom_profile()->get_counter("SpillProbeRows");
    // Odd partitions 1,3,5 each have 3 rows; partition 7 (odd) was overwritten by large_block.
    // Partition 0 has small_block (3*1024 rows), partition 7 has large_block (3*1024*1024 rows).
    ASSERT_EQ(write_rows_counter->value(),
              (PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT / 2 - 1) * 3 + 3 * 1024 * 1024 +
                      3 * 1024);
}

TEST_F(PartitionedHashJoinProbeOperatorTest, RecoverProbeBlocksFromDisk) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    // Create and register a spill file for testing
    SpillFileSPtr spill_file;
    auto relative_path = fmt::format(
            "{}/hash_probe-{}-{}", print_id(_helper.runtime_state->query_id()),
            probe_operator->node_id(), ExecEnv::GetInstance()->spill_file_mgr()->next_id());
    ASSERT_TRUE(ExecEnv::GetInstance()
                        ->spill_file_mgr()
                        ->create_spill_file(relative_path, spill_file)
                        .ok());

    // Write some test data to spill file
    {
        Block block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3});
        SpillFileWriterSPtr writer;
        ASSERT_TRUE(spill_file
                            ->create_writer(_helper.runtime_state.get(),
                                            local_state->operator_profile(), writer)
                            .ok());
        ASSERT_TRUE(writer->write_block(_helper.runtime_state.get(), block).ok());
        ASSERT_TRUE(writer->close().ok());
    }

    // Test recovery using JoinSpillPartitionInfo
    JoinSpillPartitionInfo partition_info(nullptr, spill_file, 0);
    ASSERT_TRUE(local_state
                        ->recover_probe_blocks_from_partition(_helper.runtime_state.get(),
                                                              partition_info)
                        .ok());

    std::cout << "profile: " << local_state->custom_profile()->pretty_print() << std::endl;

    // Verify recovered data (now in _queue_probe_blocks)
    auto& probe_blocks = local_state->_queue_probe_blocks;
    ASSERT_FALSE(probe_blocks.empty());
    ASSERT_EQ(probe_blocks[0].rows(), 3);

    // Verify counters
    auto* recovery_rows_counter =
            local_state->custom_profile()->get_counter("SpillRecoveryProbeRows");
    ASSERT_EQ(recovery_rows_counter->value(), 3);
    auto* recovery_blocks_counter =
            local_state->custom_profile()->get_counter("SpillRecoveryProbeBlocks");
    ASSERT_EQ(recovery_blocks_counter->value(), 1);

    // Verify stream cleanup
    ASSERT_EQ(partition_info.probe_file, nullptr);
}

TEST_F(PartitionedHashJoinProbeOperatorTest, RecoverProbeBlocksFromDiskLargeData) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    // Create and register a spill file for testing
    SpillFileSPtr spill_file;
    auto relative_path = fmt::format(
            "{}/hash_probe-{}-{}", print_id(_helper.runtime_state->query_id()),
            probe_operator->node_id(), ExecEnv::GetInstance()->spill_file_mgr()->next_id());
    ASSERT_TRUE(ExecEnv::GetInstance()
                        ->spill_file_mgr()
                        ->create_spill_file(relative_path, spill_file)
                        .ok());

    // Write some test data to spill file
    {
        // create block larger than 32MB(4 * (8 * 1024 * 1024 + 10))
        std::vector<int32_t> large_data(8 * 1024 * 1024 + 10);
        std::iota(large_data.begin(), large_data.end(), 0);
        Block large_block = ColumnHelper::create_block<DataTypeInt32>(large_data);

        SpillFileWriterSPtr writer;
        ASSERT_TRUE(spill_file
                            ->create_writer(_helper.runtime_state.get(),
                                            local_state->operator_profile(), writer)
                            .ok());
        ASSERT_TRUE(writer->write_block(_helper.runtime_state.get(), large_block).ok());

        Block block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3});
        ASSERT_TRUE(writer->write_block(_helper.runtime_state.get(), block).ok());
        ASSERT_TRUE(writer->close().ok());
    }

    // Test recovery using JoinSpillPartitionInfo
    JoinSpillPartitionInfo partition_info(nullptr, spill_file, 0);
    while (partition_info.probe_file) {
        ASSERT_TRUE(local_state
                            ->recover_probe_blocks_from_partition(_helper.runtime_state.get(),
                                                                  partition_info)
                            .ok());
    }

    std::cout << "profile: " << local_state->custom_profile()->pretty_print() << std::endl;

    // Verify recovered data (now in _queue_probe_blocks)
    auto& probe_blocks = local_state->_queue_probe_blocks;
    ASSERT_FALSE(probe_blocks.empty());

    // Count total recovered rows
    int64_t total_rows = 0;
    for (const auto& block : probe_blocks) {
        total_rows += block.rows();
    }
    ASSERT_EQ(total_rows, 8 * 1024 * 1024 + 10 + 3);

    // Verify counters
    auto* recovery_rows_counter =
            local_state->custom_profile()->get_counter("SpillRecoveryProbeRows");
    ASSERT_EQ(recovery_rows_counter->value(), 3 + 8 * 1024 * 1024 + 10);
    auto* recovery_blocks_counter =
            local_state->custom_profile()->get_counter("SpillRecoveryProbeBlocks");
    ASSERT_EQ(recovery_blocks_counter->value(), 2);

    // Verify stream cleanup
    ASSERT_EQ(partition_info.probe_file, nullptr);
}

TEST_F(PartitionedHashJoinProbeOperatorTest, RecoverProbeBlocksFromDiskEmpty) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    // Create an empty spill file
    SpillFileSPtr spill_file;
    auto relative_path = fmt::format(
            "{}/hash_probe-{}-{}", print_id(_helper.runtime_state->query_id()),
            probe_operator->node_id(), ExecEnv::GetInstance()->spill_file_mgr()->next_id());
    ASSERT_TRUE(ExecEnv::GetInstance()
                        ->spill_file_mgr()
                        ->create_spill_file(relative_path, spill_file)
                        .ok());
    // Write nothing, just close the writer
    {
        SpillFileWriterSPtr writer;
        ASSERT_TRUE(spill_file
                            ->create_writer(_helper.runtime_state.get(),
                                            local_state->operator_profile(), writer)
                            .ok());
        ASSERT_TRUE(writer->close().ok());
    }

    JoinSpillPartitionInfo partition_info(nullptr, spill_file, 0);
    ASSERT_TRUE(local_state
                        ->recover_probe_blocks_from_partition(_helper.runtime_state.get(),
                                                              partition_info)
                        .ok());

    ASSERT_TRUE(local_state->_queue_probe_blocks.empty())
            << "probe blocks not empty: " << local_state->_queue_probe_blocks.size();

    ASSERT_TRUE(partition_info.probe_file == nullptr);
}

TEST_F(PartitionedHashJoinProbeOperatorTest, RecoverProbeBlocksFromDiskError) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    // Create a spill file and write data
    SpillFileSPtr spill_file;
    auto relative_path = fmt::format(
            "{}/hash_probe-{}-{}", print_id(_helper.runtime_state->query_id()),
            probe_operator->node_id(), ExecEnv::GetInstance()->spill_file_mgr()->next_id());
    ASSERT_TRUE(ExecEnv::GetInstance()
                        ->spill_file_mgr()
                        ->create_spill_file(relative_path, spill_file)
                        .ok());

    // Write some test data to spill file
    {
        Block block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3});
        SpillFileWriterSPtr writer;
        ASSERT_TRUE(spill_file
                            ->create_writer(_helper.runtime_state.get(),
                                            local_state->operator_profile(), writer)
                            .ok());
        ASSERT_TRUE(writer->write_block(_helper.runtime_state.get(), block).ok());
        ASSERT_TRUE(writer->close().ok());
    }

    SpillableDebugPointHelper dp_helper("fault_inject::spill_file::read_next_block");
    JoinSpillPartitionInfo partition_info(nullptr, spill_file, 0);
    auto status = local_state->recover_probe_blocks_from_partition(_helper.runtime_state.get(),
                                                                   partition_info);

    ExecEnv::GetInstance()->spill_file_mgr()->delete_spill_file(spill_file);
    spill_file.reset();

    ASSERT_FALSE(status.ok());
    ASSERT_TRUE(status.to_string().find("spill_file read_next_block failed") != std::string::npos)
            << "unexpected error: " << status.to_string();
}

TEST_F(PartitionedHashJoinProbeOperatorTest, RecoverBuildBlocksFromDisk) {
    // Setup test environment
    auto [probe_operator, sink_operator] = _helper.create_operators();

    // Initialize local state
    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    // Create and register spill file with test data
    SpillFileSPtr spill_file;
    auto relative_path = fmt::format(
            "{}/hash_build-{}-{}", print_id(_helper.runtime_state->query_id()),
            probe_operator->node_id(), ExecEnv::GetInstance()->spill_file_mgr()->next_id());
    ASSERT_TRUE(ExecEnv::GetInstance()
                        ->spill_file_mgr()
                        ->create_spill_file(relative_path, spill_file)
                        .ok());

    // Write test data
    {
        Block block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3});
        SpillFileWriterSPtr writer;
        ASSERT_TRUE(spill_file
                            ->create_writer(_helper.runtime_state.get(),
                                            local_state->operator_profile(), writer)
                            .ok());
        ASSERT_TRUE(writer->write_block(_helper.runtime_state.get(), block).ok());
        ASSERT_TRUE(writer->close().ok());
    }

    // Test recovery using JoinSpillPartitionInfo
    JoinSpillPartitionInfo partition_info(spill_file, nullptr, 0);
    ASSERT_TRUE(local_state
                        ->recover_build_blocks_from_partition(_helper.runtime_state.get(),
                                                              partition_info)
                        .ok());

    // Verify recovered data
    ASSERT_TRUE(local_state->_recovered_build_block != nullptr);
    ASSERT_EQ(local_state->_recovered_build_block->rows(), 3);

    // Verify counters
    auto* recovery_rows_counter =
            local_state->custom_profile()->get_counter("SpillRecoveryBuildRows");
    ASSERT_EQ(recovery_rows_counter->value(), 3);
    auto* recovery_blocks_counter =
            local_state->custom_profile()->get_counter("SpillRecoveryBuildBlocks");
    ASSERT_EQ(recovery_blocks_counter->value(), 1);

    // Verify stream cleanup
    ASSERT_EQ(partition_info.build_file, nullptr);
}

TEST_F(PartitionedHashJoinProbeOperatorTest, need_more_input_data) {
    // Setup test environment
    auto [probe_operator, sink_operator] = _helper.create_operators();

    // Initialize local state
    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    local_state->_shared_state->_is_spilled = true;
    local_state->_child_eos = false;
    ASSERT_EQ(probe_operator->need_more_input_data(_helper.runtime_state.get()),
              !local_state->_child_eos);

    local_state->_child_eos = true;
    ASSERT_EQ(probe_operator->need_more_input_data(_helper.runtime_state.get()),
              !local_state->_child_eos);

    local_state->_shared_state->_is_spilled = false;
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

    // revocable_mem_size requires _is_spilled = true to report non-zero memory
    local_state->_shared_state->_is_spilled = true;
    local_state->_child_eos = false;
    auto block1 = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3});
    local_state->_partitioned_blocks[0] = MutableBlock::create_unique(std::move(block1));
    // Small blocks (< MIN_SPILL_WRITE_BATCH_MEM) are not counted as revocable
    ASSERT_EQ(probe_operator->revocable_mem_size(_helper.runtime_state.get()), 0);

    // Create large input block (>= MIN_SPILL_WRITE_BATCH_MEM = 512KB)
    std::vector<int32_t> large_data(256 * 1024); // 1MB of int32
    std::iota(large_data.begin(), large_data.end(), 0);
    Block large_block = ColumnHelper::create_block<DataTypeInt32>(large_data);

    const auto large_size = large_block.allocated_bytes();

    ASSERT_GE(large_size, SpillFile::MIN_SPILL_WRITE_BATCH_MEM);
    local_state->_partitioned_blocks[0] = MutableBlock::create_unique(std::move(large_block));
    ASSERT_EQ(probe_operator->revocable_mem_size(_helper.runtime_state.get()), large_size);

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

    local_state->_shared_state->_is_spilled = true;
    local_state->_child_eos = false;

    // When child_eos is false, only minimum_operator_memory_required_bytes is reserved
    local_state->_need_to_setup_queue_partition = false;
    ASSERT_EQ(probe_operator->get_reserve_mem_size(_helper.runtime_state.get()),
              _helper.runtime_state->minimum_operator_memory_required_bytes());

    // When not spilled, delegates to base class which returns minimum_operator_memory_required_bytes
    local_state->_shared_state->_is_spilled = false;
    ASSERT_GE(probe_operator->get_reserve_mem_size(_helper.runtime_state.get()),
              _helper.runtime_state->minimum_operator_memory_required_bytes());

    // When spilled and child_eos, no active partition: only baseline reservation
    local_state->_shared_state->_is_spilled = true;
    local_state->_child_eos = true;
    ASSERT_EQ(probe_operator->get_reserve_mem_size(_helper.runtime_state.get()),
              _helper.runtime_state->minimum_operator_memory_required_bytes());
}

TEST_F(PartitionedHashJoinProbeOperatorTest, RecoverBuildBlocksFromDiskEmpty) {
    // Similar setup as above...
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    // Create an empty spill file
    SpillFileSPtr spill_file;
    auto relative_path = fmt::format(
            "{}/hash_build-{}-{}", print_id(_helper.runtime_state->query_id()),
            probe_operator->node_id(), ExecEnv::GetInstance()->spill_file_mgr()->next_id());
    ASSERT_TRUE(ExecEnv::GetInstance()
                        ->spill_file_mgr()
                        ->create_spill_file(relative_path, spill_file)
                        .ok());

    // Write nothing, just close the writer
    {
        SpillFileWriterSPtr writer;
        ASSERT_TRUE(spill_file
                            ->create_writer(_helper.runtime_state.get(),
                                            local_state->operator_profile(), writer)
                            .ok());
        ASSERT_TRUE(writer->close().ok());
    }

    JoinSpillPartitionInfo partition_info(spill_file, nullptr, 0);
    ASSERT_TRUE(local_state
                        ->recover_build_blocks_from_partition(_helper.runtime_state.get(),
                                                              partition_info)
                        .ok());

    ASSERT_EQ(partition_info.build_file, nullptr);
    ASSERT_TRUE(local_state->_recovered_build_block == nullptr);
}

TEST_F(PartitionedHashJoinProbeOperatorTest, RecoverBuildBlocksFromDiskLargeData) {
    // Similar setup as above...
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    // Create spill file for large data test
    SpillFileSPtr spill_file;
    auto relative_path = fmt::format(
            "{}/hash_build-{}-{}", print_id(_helper.runtime_state->query_id()),
            probe_operator->node_id(), ExecEnv::GetInstance()->spill_file_mgr()->next_id());
    ASSERT_TRUE(ExecEnv::GetInstance()
                        ->spill_file_mgr()
                        ->create_spill_file(relative_path, spill_file)
                        .ok());

    // Write some test data to spill file
    {
        // create block larger than 32MB(4 * (8 * 1024 * 1024 + 10))
        std::vector<int32_t> large_data(8 * 1024 * 1024 + 10);
        std::iota(large_data.begin(), large_data.end(), 0);
        Block large_block = ColumnHelper::create_block<DataTypeInt32>(large_data);

        SpillFileWriterSPtr writer;
        ASSERT_TRUE(spill_file
                            ->create_writer(_helper.runtime_state.get(),
                                            local_state->operator_profile(), writer)
                            .ok());
        ASSERT_TRUE(writer->write_block(_helper.runtime_state.get(), large_block).ok());

        Block block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3});
        ASSERT_TRUE(writer->write_block(_helper.runtime_state.get(), block).ok());
        ASSERT_TRUE(writer->close().ok());
    }

    JoinSpillPartitionInfo partition_info(spill_file, nullptr, 0);
    while (partition_info.build_file) {
        ASSERT_TRUE(local_state
                            ->recover_build_blocks_from_partition(_helper.runtime_state.get(),
                                                                  partition_info)
                            .ok());

        ASSERT_TRUE(local_state->_recovered_build_block);
    }

    // Verify recovered data
    ASSERT_EQ(local_state->_recovered_build_block->rows(), 8 * 1024 * 1024 + 10 + 3);

    // Verify counters
    auto* recovery_rows_counter =
            local_state->custom_profile()->get_counter("SpillRecoveryBuildRows");
    ASSERT_EQ(recovery_rows_counter->value(), 8 * 1024 * 1024 + 10 + 3);
    auto* recovery_blocks_counter =
            local_state->custom_profile()->get_counter("SpillRecoveryBuildBlocks");
    ASSERT_EQ(recovery_blocks_counter->value(), 2);
}

TEST_F(PartitionedHashJoinProbeOperatorTest, RecoverBuildBlocksFromDiskError) {
    // Similar setup as above...
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    // Create an empty spill file
    SpillFileSPtr spill_file;
    auto relative_path = fmt::format(
            "{}/hash_build-{}-{}", print_id(_helper.runtime_state->query_id()),
            probe_operator->node_id(), ExecEnv::GetInstance()->spill_file_mgr()->next_id());
    ASSERT_TRUE(ExecEnv::GetInstance()
                        ->spill_file_mgr()
                        ->create_spill_file(relative_path, spill_file)
                        .ok());

    // Write nothing, just close
    {
        SpillFileWriterSPtr writer;
        ASSERT_TRUE(spill_file
                            ->create_writer(_helper.runtime_state.get(),
                                            local_state->operator_profile(), writer)
                            .ok());
        ASSERT_TRUE(writer->close().ok());
    }

    ASSERT_TRUE(local_state->_recovered_build_block == nullptr);

    // Test error handling with fault injection
    SpillableDebugPointHelper dp_helper(
            "fault_inject::partitioned_hash_join_probe::recover_build_blocks");
    JoinSpillPartitionInfo partition_info(spill_file, nullptr, 0);
    auto status = local_state->recover_build_blocks_from_partition(_helper.runtime_state.get(),
                                                                   partition_info);

    ASSERT_FALSE(status.ok());
    ASSERT_TRUE(status.to_string().find("fault_inject partitioned_hash_join_probe "
                                        "recover_build_blocks failed") != std::string::npos)
            << "incorrect recover build blocks status: " << status.to_string();
}

TEST_F(PartitionedHashJoinProbeOperatorTest, GetBlockTestNonSpill) {
    // Setup operators and pipeline
    auto [probe_operator, sink_operator] = _helper.create_operators();

    // Initialize local state
    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    // Setup child data
    auto input_block = Block::create_unique();
    input_block->swap(ColumnHelper::create_block<DataTypeInt32>({1, 2, 3}));

    auto probe_side_source_operator =
            std::dynamic_pointer_cast<MockChildOperator>(probe_operator->get_child());
    probe_side_source_operator->set_block(std::move(*input_block));

    local_state->_shared_state->_is_spilled = false;

    // Test non empty input block path
    {
        Block output_block;
        bool eos = false;

        auto st = probe_operator->get_block(_helper.runtime_state.get(), &output_block, &eos);
        ASSERT_TRUE(st.ok());
        ASSERT_FALSE(eos);
    }

    // Test empty input block case
    {
        auto empty_block = Block::create_unique();
        probe_side_source_operator->set_block(std::move(*empty_block));

        Block output_block;
        bool eos = false;

        auto st = probe_operator->get_block(_helper.runtime_state.get(), &output_block, &eos);
        ASSERT_TRUE(st.ok());
        ASSERT_FALSE(eos);
        ASSERT_EQ(output_block.rows(), 0);
    }

    // Test end of stream case
    {
        probe_side_source_operator->set_eos();
        Block output_block;
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
    Block empty_block;

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
    RowDescriptor row_desc(_helper.runtime_state->desc_tbl(), {0});
    const auto& tnode = probe_operator->_tnode;
    local_state->_partitioner = create_spill_partitioner(
            _helper.runtime_state.get(), PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT,
            {tnode.hash_join_node.eq_join_conjuncts[0].left}, row_desc);

    // Create test input block
    Block input_block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});

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
    RowDescriptor row_desc(_helper.runtime_state->desc_tbl(), {0});
    const auto& tnode = probe_operator->_tnode;
    local_state->_partitioner = create_spill_partitioner(
            _helper.runtime_state.get(), PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT,
            {tnode.hash_join_node.eq_join_conjuncts[0].left}, row_desc);

    // Create test data and push with EOS
    Block input_block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3});

    auto st = probe_operator->push(_helper.runtime_state.get(), &input_block, false);
    ASSERT_TRUE(st.ok()) << "Push failed: " << st.to_string();

    input_block.clear();
    st = probe_operator->push(_helper.runtime_state.get(), &input_block, true);
    ASSERT_TRUE(st.ok()) << "Push failed: " << st.to_string();

    // Verify all data is still in partitioned blocks (will be flushed at spill time)
    int64_t total_partitioned_rows = 0;
    for (uint32_t i = 0; i < probe_operator->_partition_count; ++i) {
        if (local_state->_partitioned_blocks[i]) {
            total_partitioned_rows += local_state->_partitioned_blocks[i]->rows();
        }
    }
    ASSERT_EQ(total_partitioned_rows, 3); // All rows should be in partitioned blocks
}

TEST_F(PartitionedHashJoinProbeOperatorTest, PushLargeBlock) {
    // Setup test environment
    auto [probe_operator, sink_operator] = _helper.create_operators();

    // Initialize local state
    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    // Setup row descriptor and partitioner
    RowDescriptor row_desc(_helper.runtime_state->desc_tbl(), {0});
    const auto& tnode = probe_operator->_tnode;
    local_state->_partitioner = create_spill_partitioner(
            _helper.runtime_state.get(), PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT,
            {tnode.hash_join_node.eq_join_conjuncts[0].left}, row_desc);

    // Create large input block (> 2M rows)
    std::vector<int32_t> large_data(3 * 1024 * 1024);
    std::iota(large_data.begin(), large_data.end(), 0);
    Block large_block = ColumnHelper::create_block<DataTypeInt32>(large_data);

    // Push large block
    auto st = probe_operator->push(_helper.runtime_state.get(), &large_block, false);
    ASSERT_TRUE(st.ok());

    // Large blocks may be spilled during push (blocks >= MIN_SPILL_WRITE_BATCH_MEM are
    // written to disk). Verify total rows = in-memory + spilled.
    size_t partitioned_rows_count = 0;
    for (uint32_t i = 0; i < probe_operator->_partition_count; ++i) {
        if (local_state->_partitioned_blocks[i] && !local_state->_partitioned_blocks[i]->empty()) {
            partitioned_rows_count += local_state->_partitioned_blocks[i]->rows();
        }
    }

    auto* spill_probe_rows = local_state->custom_profile()->get_counter("SpillProbeRows");
    ASSERT_TRUE(spill_probe_rows != nullptr);
    size_t total_rows = partitioned_rows_count + spill_probe_rows->value();
    ASSERT_EQ(total_rows, large_data.size());
}

TEST_F(PartitionedHashJoinProbeOperatorTest, PullInitializesSpillQueueFromLevel0Spills) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    auto build_file = create_probe_test_spill_file(
            _helper.runtime_state.get(), local_state->operator_profile(), probe_operator->node_id(),
            "hash_build_pull_init", {{1, 2, 3}});
    ASSERT_TRUE(build_file != nullptr);

    shared_state->_spilled_build_groups[0] = build_file;
    local_state->_partitioned_blocks[0] =
            MutableBlock::create_unique(ColumnHelper::create_block<DataTypeInt32>({4, 5, 6}));
    local_state->_shared_state->_is_spilled = true;
    local_state->_child_eos = true;
    local_state->_need_to_setup_queue_partition = true;

    Block output_block;
    bool eos = false;
    auto st = probe_operator->pull(_helper.runtime_state.get(), &output_block, &eos);
    ASSERT_TRUE(st.ok()) << "pull failed: " << st.to_string();

    ASSERT_FALSE(eos);
    ASSERT_TRUE(local_state->_spill_queue_initialized);
    ASSERT_TRUE(local_state->_current_partition.is_valid());
    ASSERT_EQ(local_state->_current_partition.level, 0);
    ASSERT_TRUE(local_state->_current_partition.probe_file != nullptr);
    ASSERT_EQ(local_state->_spill_partition_queue.size(),
              PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT - 1);
    ASSERT_TRUE(local_state->_recovered_build_block != nullptr);
    ASSERT_EQ(local_state->_recovered_build_block->rows(), 3);
    ASSERT_EQ(shared_state->_spilled_build_groups[0], nullptr);
    ASSERT_EQ(local_state->_probe_spilling_groups[0], nullptr);
    ASSERT_EQ(local_state->_total_partition_spills->value(),
              PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT);
    ASSERT_EQ(local_state->_max_partition_level->value(), 0);

    auto* spill_probe_rows = local_state->custom_profile()->get_counter("SpillProbeRows");
    ASSERT_TRUE(spill_probe_rows != nullptr);
    ASSERT_EQ(spill_probe_rows->value(), 3);

    ExecEnv::GetInstance()->spill_file_mgr()->delete_spill_file(build_file);
    ExecEnv::GetInstance()->spill_file_mgr()->delete_spill_file(
            local_state->_current_partition.probe_file);
    local_state->_current_partition = JoinSpillPartitionInfo {};
    local_state->_spill_partition_queue.clear();
}

TEST_F(PartitionedHashJoinProbeOperatorTest, PullBasic) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    // Pre-initialize the spill queue with one empty partition (no build/probe files)
    local_state->_spill_queue_initialized = true;
    local_state->_need_to_setup_queue_partition = true;
    local_state->_spill_partition_queue.emplace_back(JoinSpillPartitionInfo(nullptr, nullptr, 0));

    Block test_block;
    bool eos = false;

    auto st = probe_operator->pull(_helper.runtime_state.get(), &test_block, &eos);
    ASSERT_TRUE(st.ok()) << "Pull failed: " << st.to_string();

    // After processing setup, _need_to_setup_queue_partition should be false
    ASSERT_FALSE(local_state->_need_to_setup_queue_partition)
            << "Partition setup should have been completed";
}

TEST_F(PartitionedHashJoinProbeOperatorTest, PullMultiplePartitions) {
    auto [probe_operator, sink_operator] = _helper.create_operators();
    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    // Pre-initialize the spill queue with multiple empty partitions
    local_state->_spill_queue_initialized = true;
    for (uint32_t i = 0; i < PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT; i++) {
        local_state->_spill_partition_queue.emplace_back(
                JoinSpillPartitionInfo(nullptr, nullptr, 0));
    }

    Block output_block;
    bool eos = false;

    // Process all partitions through the queue.
    // Each partition requires two pulls: one for setup (build), one for probe.
    // Only set _need_to_setup_queue_partition on the first call; after that the
    // natural state machine transitions handle it.
    local_state->_need_to_setup_queue_partition = true;
    int processed = 0;
    const int max_iterations = (int)PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT * 2 + 1;
    while (!eos && processed < max_iterations) {
        auto st = probe_operator->pull(_helper.runtime_state.get(), &output_block, &eos);
        ASSERT_TRUE(st.ok()) << "Pull failed for iteration " << processed;
        processed++;
    }
    ASSERT_TRUE(eos) << "Should reach eos after all partitions are processed";
}

TEST_F(PartitionedHashJoinProbeOperatorTest, PullWithDiskRecovery) {
    auto [probe_operator, sink_operator] = _helper.create_operators();
    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    local_state->_shared_state->_is_spilled = true;

    // Create build and probe spill files
    SpillFileSPtr build_file;
    auto build_path = fmt::format(
            "{}/hash_build-{}-{}", print_id(_helper.runtime_state->query_id()),
            probe_operator->node_id(), ExecEnv::GetInstance()->spill_file_mgr()->next_id());
    ASSERT_TRUE(ExecEnv::GetInstance()
                        ->spill_file_mgr()
                        ->create_spill_file(build_path, build_file)
                        .ok());

    SpillFileSPtr probe_file;
    auto probe_path = fmt::format(
            "{}/hash_probe-{}-{}", print_id(_helper.runtime_state->query_id()),
            probe_operator->node_id(), ExecEnv::GetInstance()->spill_file_mgr()->next_id());
    ASSERT_TRUE(ExecEnv::GetInstance()
                        ->spill_file_mgr()
                        ->create_spill_file(probe_path, probe_file)
                        .ok());

    // Write test data to build file
    {
        Block spill_block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3});
        SpillFileWriterSPtr writer;
        ASSERT_TRUE(build_file
                            ->create_writer(_helper.runtime_state.get(),
                                            local_state->operator_profile(), writer)
                            .ok());
        ASSERT_TRUE(writer->write_block(_helper.runtime_state.get(), spill_block).ok());
        ASSERT_TRUE(writer->close().ok());
    }

    // Write test data to probe file
    {
        Block spill_block = ColumnHelper::create_block<DataTypeInt32>({4, 5, 6});
        SpillFileWriterSPtr writer;
        ASSERT_TRUE(probe_file
                            ->create_writer(_helper.runtime_state.get(),
                                            local_state->operator_profile(), writer)
                            .ok());
        ASSERT_TRUE(writer->write_block(_helper.runtime_state.get(), spill_block).ok());
        ASSERT_TRUE(writer->close().ok());
    }

    // Pre-initialize queue with one partition
    local_state->_spill_queue_initialized = true;
    local_state->_need_to_setup_queue_partition = true;
    local_state->_spill_partition_queue.emplace_back(
            JoinSpillPartitionInfo(build_file, probe_file, 0));

    Block output_block;
    bool eos = false;

    // First pull should recover build data
    auto st = probe_operator->pull(_helper.runtime_state.get(), &output_block, &eos);
    ASSERT_TRUE(st.ok()) << "Pull failed: " << st.to_string();
    ASSERT_FALSE(eos) << "Should not be eos during disk recovery";

    ASSERT_GT(local_state->_recovery_build_rows->value(), 0)
            << "Should have recovered some build rows from disk";
}

TEST_F(PartitionedHashJoinProbeOperatorTest, PullRecoversProbeBlocksFromPartition) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    auto probe_file = create_probe_test_spill_file(
            _helper.runtime_state.get(), local_state->operator_profile(), probe_operator->node_id(),
            "hash_probe_pull_recover", {{1, 2, 3}, {4, 5}});
    ASSERT_TRUE(probe_file != nullptr);

    local_state->_shared_state->_is_spilled = true;
    local_state->_spill_queue_initialized = true;
    local_state->_need_to_setup_queue_partition = false;
    local_state->_current_partition = JoinSpillPartitionInfo(nullptr, probe_file, 1);
    local_state->_current_partition.build_finished = true;

    Block output_block;
    bool eos = false;
    auto st = probe_operator->pull(_helper.runtime_state.get(), &output_block, &eos);
    ASSERT_TRUE(st.ok()) << "pull failed: " << st.to_string();

    ASSERT_FALSE(eos);
    ASSERT_EQ(local_state->_queue_probe_blocks.size(), 2);
    ASSERT_EQ(local_state->_recovery_probe_rows->value(), 5);
    ASSERT_EQ(local_state->_recovery_probe_blocks->value(), 2);
    ASSERT_EQ(local_state->_current_partition.probe_file, nullptr);
    ASSERT_EQ(local_state->_current_probe_reader, nullptr);

    ExecEnv::GetInstance()->spill_file_mgr()->delete_spill_file(probe_file);
    local_state->_queue_probe_blocks.clear();
    local_state->_current_partition = JoinSpillPartitionInfo {};
}

TEST_F(PartitionedHashJoinProbeOperatorTest, PullFinishesPartitionAfterRecoveredProbeBlocks) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    auto probe_file = create_probe_test_spill_file(
            _helper.runtime_state.get(), local_state->operator_profile(), probe_operator->node_id(),
            "hash_probe_pull_finish", {{6, 7, 8}});
    ASSERT_TRUE(probe_file != nullptr);

    local_state->_shared_state->_is_spilled = true;
    local_state->_spill_queue_initialized = true;
    local_state->_need_to_setup_queue_partition = false;
    local_state->_current_partition = JoinSpillPartitionInfo(nullptr, probe_file, 1);
    local_state->_current_partition.build_finished = true;

    Block output_block;
    bool eos = false;
    auto st = probe_operator->pull(_helper.runtime_state.get(), &output_block, &eos);
    ASSERT_TRUE(st.ok()) << "first pull failed: " << st.to_string();
    ASSERT_FALSE(eos);
    ASSERT_EQ(local_state->_queue_probe_blocks.size(), 1);

    st = probe_operator->pull(_helper.runtime_state.get(), &output_block, &eos);
    ASSERT_TRUE(st.ok()) << "second pull failed: " << st.to_string();
    ASSERT_TRUE(eos);
    ASSERT_FALSE(local_state->_current_partition.is_valid());
    ASSERT_TRUE(local_state->_need_to_setup_queue_partition);
    ASSERT_TRUE(local_state->_queue_probe_blocks.empty());
    ASSERT_TRUE(local_state->_spill_partition_queue.empty());

    ExecEnv::GetInstance()->spill_file_mgr()->delete_spill_file(probe_file);
}

TEST_F(PartitionedHashJoinProbeOperatorTest, PullWithEmptyPartition) {
    auto [probe_operator, sink_operator] = _helper.create_operators();
    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    // Set up queue with an empty partition followed by another
    local_state->_spill_queue_initialized = true;
    local_state->_need_to_setup_queue_partition = true;
    local_state->_spill_partition_queue.emplace_back(JoinSpillPartitionInfo(nullptr, nullptr, 0));
    local_state->_spill_partition_queue.emplace_back(JoinSpillPartitionInfo(nullptr, nullptr, 0));

    Block output_block;
    bool eos = false;

    auto st = probe_operator->pull(_helper.runtime_state.get(), &output_block, &eos);
    ASSERT_TRUE(st.ok()) << "Pull failed for empty partition";
    ASSERT_FALSE(eos) << "Should not be eos since more partitions remain in queue";

    // The first partition should have been popped from the queue
    ASSERT_EQ(local_state->_spill_partition_queue.size(), 1u)
            << "One partition should remain in queue after processing empty one";
}

TEST_F(PartitionedHashJoinProbeOperatorTest, Other) {
    auto [probe_operator, _] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    local_state->_shared_state->_is_spilled = true;

    auto st = probe_operator->revoke_memory(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "Revoke memory failed: " << st.to_string();
}

// Test spill_probe_blocks with empty partitions (no data in any partition).
TEST_F(PartitionedHashJoinProbeOperatorTest, spill_probe_blocks_empty) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    RowDescriptor row_desc(_helper.runtime_state->desc_tbl(), {0});
    const auto& tnode = probe_operator->_tnode;
    local_state->_partitioner = create_spill_partitioner(
            _helper.runtime_state.get(), PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT,
            {tnode.hash_join_node.eq_join_conjuncts[0].left}, row_desc);

    // No data in any partition
    local_state->_shared_state->_is_spilled = true;
    auto st = local_state->spill_probe_blocks(_helper.runtime_state.get(), true);
    ASSERT_TRUE(st.ok()) << "spill_probe_blocks with empty data failed: " << st.to_string();

    // SpillProbeRows should be 0
    auto* write_rows_counter = local_state->custom_profile()->get_counter("SpillProbeRows");
    ASSERT_EQ(write_rows_counter->value(), 0);
}

// Test spill_probe_blocks with error injection.
TEST_F(PartitionedHashJoinProbeOperatorTest, spill_probe_blocks_error) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    RowDescriptor row_desc(_helper.runtime_state->desc_tbl(), {0});
    const auto& tnode = probe_operator->_tnode;
    local_state->_partitioner = create_spill_partitioner(
            _helper.runtime_state.get(), PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT,
            {tnode.hash_join_node.eq_join_conjuncts[0].left}, row_desc);

    // Add data to partitions
    for (int32_t i = 0; i != PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT; ++i) {
        Block block = ColumnHelper::create_block<DataTypeInt32>({1 * i, 2 * i, 3 * i});
        local_state->_partitioned_blocks[i] = MutableBlock::create_unique(std::move(block));
    }

    local_state->_shared_state->_is_spilled = true;

    SpillableDebugPointHelper dp_helper("fault_inject::spill_file::spill_block");
    auto st = local_state->spill_probe_blocks(_helper.runtime_state.get(), true);
    ASSERT_FALSE(st.ok()) << "spill_probe_blocks should fail with error injection";
}

// Test PushWithEOS followed by spill_probe_blocks for spilled partitions.
TEST_F(PartitionedHashJoinProbeOperatorTest, PushEosAndSpillProbe) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    RowDescriptor row_desc(_helper.runtime_state->desc_tbl(), {0});
    const auto& tnode = probe_operator->_tnode;
    local_state->_partitioner = create_spill_partitioner(
            _helper.runtime_state.get(), PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT,
            {tnode.hash_join_node.eq_join_conjuncts[0].left}, row_desc);

    // Push data → EOS
    Block input_block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});
    auto st = probe_operator->push(_helper.runtime_state.get(), &input_block, false);
    ASSERT_TRUE(st.ok()) << "Push failed: " << st.to_string();

    input_block.clear();
    st = probe_operator->push(_helper.runtime_state.get(), &input_block, true);
    ASSERT_TRUE(st.ok()) << "Push EOS failed: " << st.to_string();

    // Verify all data is in partitioned blocks
    int64_t total_rows = 0;
    for (uint32_t i = 0; i < probe_operator->_partition_count; ++i) {
        if (local_state->_partitioned_blocks[i]) {
            total_rows += local_state->_partitioned_blocks[i]->rows();
        }
    }
    ASSERT_EQ(total_rows, 5);

    // Now spill the probe blocks
    local_state->_shared_state->_is_spilled = true;
    st = local_state->spill_probe_blocks(_helper.runtime_state.get(), true);
    ASSERT_TRUE(st.ok()) << "spill_probe_blocks failed: " << st.to_string();

    auto* write_rows_counter = local_state->custom_profile()->get_counter("SpillProbeRows");
    ASSERT_EQ(write_rows_counter->value(), 5);

    // Cleanup
    for (int32_t i = 0; i != PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT; ++i) {
        if (local_state->_probe_spilling_groups[i]) {
            ExecEnv::GetInstance()->spill_file_mgr()->delete_spill_file(
                    local_state->_probe_spilling_groups[i]);
            local_state->_probe_spilling_groups[i].reset();
        }
    }
}

// Test RecoverProbeBlocks with multiple blocks in one spill file.
TEST_F(PartitionedHashJoinProbeOperatorTest, RecoverProbeMultipleBlocks) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    // Create spill file with 3 blocks
    SpillFileSPtr spill_file;
    auto relative_path = fmt::format(
            "{}/hash_probe-{}-{}", print_id(_helper.runtime_state->query_id()),
            probe_operator->node_id(), ExecEnv::GetInstance()->spill_file_mgr()->next_id());
    ASSERT_TRUE(ExecEnv::GetInstance()
                        ->spill_file_mgr()
                        ->create_spill_file(relative_path, spill_file)
                        .ok());

    {
        SpillFileWriterSPtr writer;
        ASSERT_TRUE(spill_file
                            ->create_writer(_helper.runtime_state.get(),
                                            local_state->operator_profile(), writer)
                            .ok());

        for (int batch = 0; batch < 3; ++batch) {
            Block block = ColumnHelper::create_block<DataTypeInt32>(
                    {batch * 10 + 1, batch * 10 + 2, batch * 10 + 3});
            ASSERT_TRUE(writer->write_block(_helper.runtime_state.get(), block).ok());
        }
        ASSERT_TRUE(writer->close().ok());
    }

    // Recover all blocks
    JoinSpillPartitionInfo partition_info(nullptr, spill_file, 0);
    while (partition_info.probe_file) {
        ASSERT_TRUE(local_state
                            ->recover_probe_blocks_from_partition(_helper.runtime_state.get(),
                                                                  partition_info)
                            .ok());
    }

    // Verify all data recovered
    int64_t total_rows = 0;
    for (const auto& block : local_state->_queue_probe_blocks) {
        total_rows += block.rows();
    }
    ASSERT_EQ(total_rows, 9);

    auto* recovery_rows = local_state->custom_profile()->get_counter("SpillRecoveryProbeRows");
    ASSERT_EQ(recovery_rows->value(), 9);
    auto* recovery_blocks = local_state->custom_profile()->get_counter("SpillRecoveryProbeBlocks");
    ASSERT_EQ(recovery_blocks->value(), 3);
}

// Test RecoverBuildBlocks with multiple blocks in one spill file.
TEST_F(PartitionedHashJoinProbeOperatorTest, RecoverBuildMultipleBlocks) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    // Create spill file with 3 blocks
    SpillFileSPtr spill_file;
    auto relative_path = fmt::format(
            "{}/hash_build-{}-{}", print_id(_helper.runtime_state->query_id()),
            probe_operator->node_id(), ExecEnv::GetInstance()->spill_file_mgr()->next_id());
    ASSERT_TRUE(ExecEnv::GetInstance()
                        ->spill_file_mgr()
                        ->create_spill_file(relative_path, spill_file)
                        .ok());

    {
        SpillFileWriterSPtr writer;
        ASSERT_TRUE(spill_file
                            ->create_writer(_helper.runtime_state.get(),
                                            local_state->operator_profile(), writer)
                            .ok());

        for (int batch = 0; batch < 3; ++batch) {
            Block block =
                    ColumnHelper::create_block<DataTypeInt32>({batch * 100 + 1, batch * 100 + 2});
            ASSERT_TRUE(writer->write_block(_helper.runtime_state.get(), block).ok());
        }
        ASSERT_TRUE(writer->close().ok());
    }

    // Recover all blocks
    JoinSpillPartitionInfo partition_info(spill_file, nullptr, 0);
    while (partition_info.build_file) {
        ASSERT_TRUE(local_state
                            ->recover_build_blocks_from_partition(_helper.runtime_state.get(),
                                                                  partition_info)
                            .ok());
    }

    // Verify all data recovered
    ASSERT_TRUE(local_state->_recovered_build_block != nullptr);
    ASSERT_EQ(local_state->_recovered_build_block->rows(), 6);

    auto* recovery_rows = local_state->custom_profile()->get_counter("SpillRecoveryBuildRows");
    ASSERT_EQ(recovery_rows->value(), 6);
    auto* recovery_blocks = local_state->custom_profile()->get_counter("SpillRecoveryBuildBlocks");
    ASSERT_EQ(recovery_blocks->value(), 3);
}

// Test queue with all empty partitions reaches EOS.
TEST_F(PartitionedHashJoinProbeOperatorTest, PullAllEmptyPartitions) {
    auto [probe_operator, sink_operator] = _helper.create_operators();
    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    // Initialize queue with 3 empty partitions
    local_state->_spill_queue_initialized = true;
    for (int i = 0; i < 3; ++i) {
        local_state->_spill_partition_queue.emplace_back(
                JoinSpillPartitionInfo(nullptr, nullptr, 0));
    }

    Block output_block;
    bool eos = false;
    int iterations = 0;

    // Each partition requires two pulls: one for setup, one for probe.
    local_state->_need_to_setup_queue_partition = true;
    while (!eos && iterations < 10) {
        auto st = probe_operator->pull(_helper.runtime_state.get(), &output_block, &eos);
        ASSERT_TRUE(st.ok()) << "Pull failed at iteration " << iterations;
        iterations++;
    }

    ASSERT_TRUE(eos) << "Should reach EOS after processing all empty partitions";
    ASSERT_TRUE(local_state->_spill_partition_queue.empty())
            << "Queue should be empty after processing all partitions";
}

// Test JoinSpillPartitionInfo validity.
TEST_F(PartitionedHashJoinProbeOperatorTest, JoinSpillPartitionInfoValidation) {
    // Default constructed should be invalid
    JoinSpillPartitionInfo default_info;
    ASSERT_FALSE(default_info.is_valid());

    // Constructed with files should be valid
    SpillFileSPtr build_file;
    auto relative_path =
            fmt::format("{}/hash_build-test-{}", print_id(_helper.runtime_state->query_id()),
                        ExecEnv::GetInstance()->spill_file_mgr()->next_id());
    ASSERT_TRUE(ExecEnv::GetInstance()
                        ->spill_file_mgr()
                        ->create_spill_file(relative_path, build_file)
                        .ok());

    JoinSpillPartitionInfo valid_info(build_file, nullptr, 1);
    ASSERT_TRUE(valid_info.is_valid());
    ASSERT_EQ(valid_info.level, 1);
    ASSERT_TRUE(valid_info.build_file != nullptr);
    ASSERT_TRUE(valid_info.probe_file == nullptr);

    // Null files + initialized should still be valid
    JoinSpillPartitionInfo null_files_info(nullptr, nullptr, 0);
    ASSERT_TRUE(null_files_info.is_valid());
}
} // namespace doris
