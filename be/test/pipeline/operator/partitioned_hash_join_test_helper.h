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
#include "common/object_pool.h"
#include "pipeline/exec/partitioned_hash_join_probe_operator.h"
#include "pipeline/exec/partitioned_hash_join_sink_operator.h"
#include "pipeline/pipeline_task.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "spillable_operator_test_helper.h"
#include "testutil/mock/mock_runtime_state.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {
class MockPartitionedHashJoinSharedState : public PartitionedHashJoinSharedState {
public:
    MockPartitionedHashJoinSharedState() {
        need_to_spill = false;
        inner_runtime_state = nullptr;
        spilled_streams.clear();
        partitioned_build_blocks.clear();
    }

    // 添加必要的初始化方法
    void init(size_t partition_count) {
        spilled_streams.resize(partition_count);
        partitioned_build_blocks.resize(partition_count);
    }
};

class MockHashJoinSharedState : public HashJoinSharedState {};

class MockRuntimeFilterProducerHelper : public RuntimeFilterProducerHelper {
public:
    MockRuntimeFilterProducerHelper() = default;
    ~MockRuntimeFilterProducerHelper() override = default;

    Status send_filter_size(
            RuntimeState* state, uint64_t hash_table_size,
            const std::shared_ptr<pipeline::CountedFinishDependency>& dependency) override {
        return Status::OK();
    }

    Status skip_process(RuntimeState* state) override { return Status::OK(); }
};

class MockHashJoinBuildSinkLocalState : public HashJoinBuildSinkLocalState {
public:
    // DataSinkOperatorXBase* parent, RuntimeState* state
    MockHashJoinBuildSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : HashJoinBuildSinkLocalState(parent, state) {
        _runtime_filter_producer_helper = std::make_shared<MockRuntimeFilterProducerHelper>();
        _operator_profile = state->obj_pool()->add(new RuntimeProfile("OperatorProfile"));
        _common_profile = state->obj_pool()->add(new RuntimeProfile("CommonCounters"));
        _custom_profile = state->obj_pool()->add(new RuntimeProfile("CustomCounters"));
        _memory_used_counter =
                _common_profile->AddHighWaterMarkCounter("MemoryUsage", TUnit::BYTES, "", 1);

        ADD_TIMER(_custom_profile, "PublishRuntimeFilterTime");
        ADD_TIMER(_custom_profile, "BuildRuntimeFilterTime");
        ADD_TIMER(_custom_profile, "BuildHashTableTime");
        ADD_TIMER(_custom_profile, "MergeBuildBlockTime");
        ADD_TIMER(_custom_profile, "BuildTableInsertTime");
        ADD_TIMER(_custom_profile, "BuildExprCallTime");
        ADD_TIMER(_custom_profile, "RuntimeFilterInitTime");
        ADD_COUNTER(_custom_profile, "MemoryUsageBuildBlocks", TUnit::UNIT);
        ADD_COUNTER(_custom_profile, "MemoryUsageHashTable", TUnit::BYTES);
        ADD_COUNTER(_custom_profile, "MemoryUsageBuildKeyArena", TUnit::BYTES);
    }

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override { return Status::OK(); }
    Status open(RuntimeState* state) override { return Status::OK(); }
    Status close(RuntimeState* state, Status status) override { return Status::OK(); }
    size_t get_reserve_mem_size(RuntimeState* state, bool eos) override { return 0; }
};

class MockHashJoinBuildOperator : public HashJoinBuildSinkOperatorX {
public:
    MockHashJoinBuildOperator(ObjectPool* pool, int operator_id, int dest_id,
                              const TPlanNode& tnode, const DescriptorTbl& descs)
            : HashJoinBuildSinkOperatorX(pool, operator_id, dest_id, tnode, descs) {}
    ~MockHashJoinBuildOperator() override = default;

    Status prepare(RuntimeState* state) override { return Status::OK(); }

    Status setup_local_state(RuntimeState* state, LocalSinkStateInfo& info) override {
        state->emplace_sink_local_state(
                _operator_id, std::make_unique<MockHashJoinBuildSinkLocalState>(this, state));
        return Status::OK();
    }

    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos) override {
        return Status::OK();
    }

    std::string get_memory_usage_debug_str(RuntimeState* state) const override { return "mock"; }
};

class MockHashJoinProbeLocalState : public HashJoinProbeLocalState {
    ENABLE_FACTORY_CREATOR(MockHashJoinProbeLocalState);

public:
    MockHashJoinProbeLocalState(RuntimeState* state, OperatorXBase* parent)
            : HashJoinProbeLocalState(state, parent) {
        _operator_profile = std::make_unique<RuntimeProfile>("OperatorProfile");
        _custom_profile = std::make_unique<RuntimeProfile>("CustomCounters");
        _common_profile = std::make_unique<RuntimeProfile>("CommonCounters");
        _operator_profile->add_child(_custom_profile.get(), true);
        _operator_profile->add_child(_common_profile.get(), true);
    }

    Status open(RuntimeState* state) override { return Status::OK(); }
};

class MockHashJoinProbeOperator : public HashJoinProbeOperatorX {
public:
    MockHashJoinProbeOperator(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                              const DescriptorTbl& descs)
            : HashJoinProbeOperatorX(pool, tnode, operator_id, descs) {}
    ~MockHashJoinProbeOperator() override = default;

    Status push(RuntimeState* state, vectorized::Block* input_block, bool eos_) const override {
        const_cast<MockHashJoinProbeOperator*>(this)->block.swap(*input_block);
        const_cast<MockHashJoinProbeOperator*>(this)->eos = eos_;
        const_cast<MockHashJoinProbeOperator*>(this)->need_more_data = !eos;
        return Status::OK();
    }

    Status pull(doris::RuntimeState* state, vectorized::Block* output_block,
                bool* eos_) const override {
        output_block->swap(const_cast<MockHashJoinProbeOperator*>(this)->block);
        *eos_ = eos;
        const_cast<MockHashJoinProbeOperator*>(this)->block.clear_column_data();
        return Status::OK();
    }

    Status setup_local_state(RuntimeState* state, LocalStateInfo& info) override {
        state->emplace_local_state(_operator_id,
                                   std::make_unique<MockHashJoinProbeLocalState>(state, this));
        return Status::OK();
    }

    bool need_more_input_data(RuntimeState* state) const override { return need_more_data; }
    bool need_more_data = true;

    vectorized::Block block;
    bool eos = false;
};

class MockPartitionedHashJoinProbeLocalState : public PartitionedHashJoinProbeLocalState {
public:
    MockPartitionedHashJoinProbeLocalState(RuntimeState* state, OperatorXBase* parent)
            : PartitionedHashJoinProbeLocalState(state, parent) {
        _operator_profile = std::make_unique<RuntimeProfile>("MockPartitionedHashJoinProbe");
        _custom_profile = std::make_unique<RuntimeProfile>("CustomCounters");
        _common_profile = std::make_unique<RuntimeProfile>("CommonCounters");
        _operator_profile->add_child(_custom_profile.get(), true);
        _operator_profile->add_child(_common_profile.get(), true);
    }

    void init_counters() {
        PartitionedHashJoinProbeLocalState::init_counters();
        _rows_returned_counter =
                ADD_COUNTER_WITH_LEVEL(_common_profile, "RowsProduced", TUnit::UNIT, 1);
        _blocks_returned_counter =
                ADD_COUNTER_WITH_LEVEL(_common_profile, "BlocksProduced", TUnit::UNIT, 1);
        _projection_timer = ADD_TIMER_WITH_LEVEL(_common_profile, "ProjectionTime", 1);
        _init_timer = ADD_TIMER_WITH_LEVEL(_common_profile, "InitTime", 1);
        _open_timer = ADD_TIMER_WITH_LEVEL(_common_profile, "OpenTime", 1);
        _close_timer = ADD_TIMER_WITH_LEVEL(_common_profile, "CloseTime", 1);
        _exec_timer = ADD_TIMER_WITH_LEVEL(_common_profile, "ExecTime", 1);
        _memory_used_counter =
                _common_profile->AddHighWaterMarkCounter("MemoryUsage", TUnit::BYTES, "", 1);
    }

    void update_profile_from_inner() override {};
};

class MockPartitionedHashJoinSinkLocalState : public PartitionedHashJoinSinkLocalState {
public:
    MockPartitionedHashJoinSinkLocalState(PartitionedHashJoinSinkOperatorX* parent,
                                          RuntimeState* state, ObjectPool* pool)
            : PartitionedHashJoinSinkLocalState(parent, state) {
        _operator_profile = pool->add(new RuntimeProfile("OperatorProfile"));
        _custom_profile = pool->add(new RuntimeProfile("CustomCounters"));
        _common_profile = pool->add(new RuntimeProfile("CommonCounters"));

        _memory_used_counter =
                _common_profile->AddHighWaterMarkCounter("MemoryUsage", TUnit::BYTES, "", 1);

        _operator_profile->add_child(_custom_profile, true);
        _operator_profile->add_child(_common_profile, true);
    }

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override { return Status::OK(); }

    void update_profile_from_inner() override {}
};

class PartitionedHashJoinTestHelper : public SpillableOperatorTestHelper {
public:
    ~PartitionedHashJoinTestHelper() override = default;
    TPlanNode create_test_plan_node() override;

    TDescriptorTable create_test_table_descriptor(bool nullable) override;

    PartitionedHashJoinProbeLocalState* create_probe_local_state(
            RuntimeState* state, PartitionedHashJoinProbeOperatorX* probe_operator,
            std::shared_ptr<MockPartitionedHashJoinSharedState>& shared_state);

    PartitionedHashJoinSinkLocalState* create_sink_local_state(
            RuntimeState* state, PartitionedHashJoinSinkOperatorX* sink_operator,
            std::shared_ptr<MockPartitionedHashJoinSharedState>& shared_state);

    std::tuple<std::shared_ptr<PartitionedHashJoinProbeOperatorX>,
               std::shared_ptr<PartitionedHashJoinSinkOperatorX>>
    create_operators();
};
} // namespace doris::pipeline