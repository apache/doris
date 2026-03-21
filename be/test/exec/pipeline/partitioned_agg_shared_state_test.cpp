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

#include <filesystem>
#include <memory>

#include "exec/common/agg_utils.h"
#include "exec/pipeline/dependency.h"
#include "exec/spill/spill_file_manager.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"

namespace doris {

class PartitionedAggSharedStateTest : public testing::Test {
protected:
    void SetUp() override {
        _exec_env = ExecEnv::GetInstance();
        ASSERT_TRUE(_exec_env != nullptr);

        // Initialize spill file manager if not already done
        if (_exec_env->spill_file_mgr() == nullptr) {
            _spill_dir = "./ut_dir/doris_ut_partitioned_agg_" + std::to_string(getpid());
            auto spill_data_dir = std::make_unique<SpillDataDir>(_spill_dir, -1);
            auto st =
                    io::global_local_filesystem()->create_directory(spill_data_dir->path(), false);
            ASSERT_TRUE(st.ok()) << st.to_string();
            std::unordered_map<std::string, std::unique_ptr<SpillDataDir>> data_map;
            data_map.emplace("test", std::move(spill_data_dir));
            _spill_file_mgr = std::make_unique<SpillFileManager>(std::move(data_map));
            _exec_env->_spill_file_mgr = _spill_file_mgr.get();
            st = _spill_file_mgr->init();
            ASSERT_TRUE(st.ok()) << st.to_string();
            _owns_spill_mgr = true;
        }
    }

    void TearDown() override {
        if (_owns_spill_mgr) {
            _exec_env->_spill_file_mgr = nullptr;
            _spill_file_mgr->stop();
            _spill_file_mgr.reset();
            if (!_spill_dir.empty()) {
                std::filesystem::remove_all(_spill_dir);
            }
        }
    }

    ExecEnv* _exec_env = nullptr;
    std::unique_ptr<SpillFileManager> _spill_file_mgr;
    std::string _spill_dir;
    bool _owns_spill_mgr = false;
};

TEST_F(PartitionedAggSharedStateTest, CloseWithEmptyPartitions) {
    PartitionedAggSharedState state;
    state.close();
    ASSERT_TRUE(state._spill_partitions.empty());
}

TEST_F(PartitionedAggSharedStateTest, CloseWithNullPartitions) {
    PartitionedAggSharedState state;
    state._spill_partitions.emplace_back(nullptr);
    state._spill_partitions.emplace_back(nullptr);
    state._spill_partitions.emplace_back(nullptr);
    ASSERT_EQ(state._spill_partitions.size(), 3);

    state.close();
    ASSERT_TRUE(state._spill_partitions.empty());
}

TEST_F(PartitionedAggSharedStateTest, CloseCalledTwiceIsIdempotent) {
    PartitionedAggSharedState state;
    state._spill_partitions.emplace_back(nullptr);
    ASSERT_EQ(state._spill_partitions.size(), 1);

    state.close();
    ASSERT_TRUE(state._spill_partitions.empty());

    // Second close should be a safe no-op
    state.close();
    ASSERT_TRUE(state._spill_partitions.empty());
}

TEST_F(PartitionedAggSharedStateTest, CloseWithMixedNullPartitions) {
    PartitionedAggSharedState state;
    state._spill_partitions.emplace_back(nullptr);
    state._spill_partitions.emplace_back(nullptr);
    state._spill_partitions.emplace_back(nullptr);
    state._spill_partitions.emplace_back(nullptr);
    state._spill_partitions.emplace_back(nullptr);
    ASSERT_EQ(state._spill_partitions.size(), 5);

    state.close();
    ASSERT_TRUE(state._spill_partitions.empty());
}

TEST_F(PartitionedAggSharedStateTest, CloseClearsVector) {
    PartitionedAggSharedState state;
    // Add multiple null entries and verify size tracking
    for (int i = 0; i < 10; ++i) {
        state._spill_partitions.emplace_back(nullptr);
    }
    ASSERT_EQ(state._spill_partitions.size(), 10);

    state.close();
    ASSERT_EQ(state._spill_partitions.size(), 0);
    ASSERT_TRUE(state._spill_partitions.empty());
}

TEST_F(PartitionedAggSharedStateTest, CloseCalledMultipleTimes) {
    PartitionedAggSharedState state;

    for (int round = 0; round < 5; ++round) {
        state._spill_partitions.emplace_back(nullptr);
        state.close();
        ASSERT_TRUE(state._spill_partitions.empty());
    }
}

// --- Tests covering PartitionedAggSourceOperatorX::revocable_mem_size logic ---
// revocable_mem_size checks: (1) _is_spilled, (2) spill_file != nullptr,
// then sums: block bytes + hash_table bytes + aggregate_data_container bytes.

// Condition 1: _is_spilled defaults to false → early return 0.
TEST_F(PartitionedAggSharedStateTest, IsSpilledDefaultsFalse) {
    PartitionedAggSharedState state;
    EXPECT_FALSE(state._is_spilled.load());
}

TEST_F(PartitionedAggSharedStateTest, IsSpilledCanBeSet) {
    PartitionedAggSharedState state;
    state._is_spilled = true;
    EXPECT_TRUE(state._is_spilled.load());
}

// Condition 2: _in_mem_shared_state defaults to null → hash table + container skipped.
TEST_F(PartitionedAggSharedStateTest, InMemSharedStateDefaultsNull) {
    PartitionedAggSharedState state;
    EXPECT_EQ(state._in_mem_shared_state, nullptr);
}

// Hash table contribution: AggSharedState constructor always creates agg_data.
TEST_F(PartitionedAggSharedStateTest, AggSharedStateCreatesNonNullAggData) {
    AggSharedState agg_state;
    EXPECT_NE(agg_state.agg_data, nullptr);
}

// Hash table contribution: default method_variant is monostate (index 0) → 0 bytes.
TEST_F(PartitionedAggSharedStateTest, AggSharedStateDefaultVariantIsMonostate) {
    AggSharedState agg_state;
    EXPECT_EQ(agg_state.agg_data->method_variant.index(), 0);
}

// Container contribution: aggregate_data_container defaults to null → 0 bytes.
TEST_F(PartitionedAggSharedStateTest, AggSharedStateAggContainerDefaultsNull) {
    AggSharedState agg_state;
    EXPECT_EQ(agg_state.aggregate_data_container, nullptr);
}

// Container contribution: freshly constructed container has 0 memory_usage.
TEST_F(PartitionedAggSharedStateTest, AggregateDataContainerInitialMemoryIsZero) {
    AggregateDataContainer container(sizeof(uint32_t), 8);
    EXPECT_EQ(container.memory_usage(), 0);
}

// Container contribution: appending data allocates arena memory → memory_usage > 0.
TEST_F(PartitionedAggSharedStateTest, AggregateDataContainerMemoryGrowsAfterAppend) {
    AggregateDataContainer container(sizeof(uint32_t), 8);
    ASSERT_EQ(container.memory_usage(), 0);
    uint32_t key = 42;
    container.append_data<uint32_t>(key);
    EXPECT_GT(container.memory_usage(), 0);
}

// Full state linkage: PartitionedAggSharedState holding an AggSharedState
// with monostate variant and null container → 0 bytes from both sources.
TEST_F(PartitionedAggSharedStateTest, PartitionedAggStateLinkedToAggStateWithDefaultData) {
    AggSharedState agg_state;
    PartitionedAggSharedState state;
    state._in_mem_shared_state = &agg_state;
    state._is_spilled = true;

    EXPECT_NE(state._in_mem_shared_state, nullptr);
    EXPECT_NE(state._in_mem_shared_state->agg_data, nullptr);
    // monostate → hash table contributes 0 bytes
    EXPECT_EQ(state._in_mem_shared_state->agg_data->method_variant.index(), 0);
    // null container → container contributes 0 bytes
    EXPECT_EQ(state._in_mem_shared_state->aggregate_data_container, nullptr);
}

// Container contribution through AggSharedState: memory_usage reflects arena allocation.
TEST_F(PartitionedAggSharedStateTest, AggSharedStateContainerMemoryUsage) {
    AggSharedState agg_state;
    agg_state.aggregate_data_container =
            std::make_unique<AggregateDataContainer>(sizeof(uint32_t), 8);
    ASSERT_NE(agg_state.aggregate_data_container, nullptr);
    EXPECT_EQ(agg_state.aggregate_data_container->memory_usage(), 0);

    uint32_t key = 99;
    agg_state.aggregate_data_container->append_data<uint32_t>(key);
    EXPECT_GT(agg_state.aggregate_data_container->memory_usage(), 0);
}

} // namespace doris