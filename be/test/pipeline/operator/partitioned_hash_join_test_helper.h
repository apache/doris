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
#include <sstream>
#include <vector>

#include "common/config.h"
#include "common/object_pool.h"
#include "olap/olap_define.h"
#include "pipeline/exec/partitioned_hash_join_sink_operator.h"
#include "pipeline/exec/spill_utils.h"
#include "pipeline/pipeline_task.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "testutil/column_helper.h"
#include "testutil/creators.h"
#include "testutil/mock/mock_operators.h"
#include "testutil/mock/mock_runtime_state.h"
#include "util/debug_points.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_number.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {

class MockPartitioner : public vectorized::PartitionerBase {
public:
    MockPartitioner(size_t partition_count) : PartitionerBase(partition_count) {}
    Status init(const std::vector<TExpr>& texprs) override { return Status::OK(); }

    Status prepare(RuntimeState* state, const RowDescriptor& row_desc) override {
        return Status::OK();
    }

    Status open(RuntimeState* state) override { return Status::OK(); }

    Status close(RuntimeState* state) override { return Status::OK(); }

    Status do_partitioning(RuntimeState* state, vectorized::Block* block, bool eos,
                           bool* already_sent) const override {
        if (already_sent) {
            *already_sent = false;
        }
        return Status::OK();
    }

    Status clone(RuntimeState* state, std::unique_ptr<PartitionerBase>& partitioner) override {
        partitioner = std::make_unique<MockPartitioner>(_partition_count);
        return Status::OK();
    }

    vectorized::ChannelField get_channel_ids() const override { return {}; }
};

class MockExpr : public vectorized::VExpr {
public:
    Status prepare(RuntimeState* state, const RowDescriptor& row_desc,
                   vectorized::VExprContext* context) override {
        return Status::OK();
    }

    Status open(RuntimeState* state, vectorized::VExprContext* context,
                FunctionContext::FunctionStateScope scope) override {
        return Status::OK();
    }
};

class MockHashJoinBuildSharedState : public HashJoinSharedState {
public:
};

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

class MockHashJoinBuildSinkLocalState : public HashJoinBuildSinkLocalState {
public:
    // DataSinkOperatorXBase* parent, RuntimeState* state
    MockHashJoinBuildSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : HashJoinBuildSinkLocalState(parent, state) {
        _runtime_profile = std::make_unique<RuntimeProfile>("test");
        _profile = _runtime_profile.get();
        _memory_used_counter =
                _profile->AddHighWaterMarkCounter("MemoryUsage", TUnit::BYTES, "", 1);

        ADD_TIMER(_profile, "PublishRuntimeFilterTime");
        ADD_TIMER(_profile, "BuildRuntimeFilterTime");
        ADD_TIMER(_profile, "BuildHashTableTime");
        ADD_TIMER(_profile, "MergeBuildBlockTime");
        ADD_TIMER(_profile, "BuildTableInsertTime");
        ADD_TIMER(_profile, "BuildExprCallTime");
        ADD_TIMER(_profile, "RuntimeFilterInitTime");
        ADD_COUNTER(_profile, "MemoryUsageBuildBlocks", TUnit::UNIT);
        ADD_COUNTER(_profile, "MemoryUsageHashTable", TUnit::BYTES);
        ADD_COUNTER(_profile, "MemoryUsageBuildKeyArena", TUnit::BYTES);
    }

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override { return Status::OK(); }
    Status open(RuntimeState* state) override { return Status::OK(); }
    Status close(RuntimeState* state, Status status) override { return Status::OK(); }
    size_t get_reserve_mem_size(RuntimeState* state, bool eos) override { return 0; }

private:
    std::unique_ptr<RuntimeProfile> _runtime_profile;
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

class MockFragmentManager : public FragmentMgr {
public:
    MockFragmentManager(Status& status_, ExecEnv* exec_env)
            : FragmentMgr(exec_env), status(status_) {}
    void cancel_query(const TUniqueId query_id, const Status reason) override { status = reason; }

private:
    Status& status;
};

class MockHashJoinProbeLocalState : public HashJoinProbeLocalState {
    ENABLE_FACTORY_CREATOR(MockHashJoinProbeLocalState);

public:
    MockHashJoinProbeLocalState(RuntimeState* state, OperatorXBase* parent)
            : HashJoinProbeLocalState(state, parent) {
        _runtime_profile = std::make_unique<RuntimeProfile>("test");
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
        _runtime_profile = std::make_unique<RuntimeProfile>("test");
    }

    void init_counters() {
        PartitionedHashJoinProbeLocalState::init_counters();
        _rows_returned_counter =
                ADD_COUNTER_WITH_LEVEL(_runtime_profile, "RowsProduced", TUnit::UNIT, 1);
        _blocks_returned_counter =
                ADD_COUNTER_WITH_LEVEL(_runtime_profile, "BlocksProduced", TUnit::UNIT, 1);
        _projection_timer = ADD_TIMER_WITH_LEVEL(_runtime_profile, "ProjectionTime", 1);
        _init_timer = ADD_TIMER_WITH_LEVEL(_runtime_profile, "InitTime", 1);
        _open_timer = ADD_TIMER_WITH_LEVEL(_runtime_profile, "OpenTime", 1);
        _close_timer = ADD_TIMER_WITH_LEVEL(_runtime_profile, "CloseTime", 1);
        _exec_timer = ADD_TIMER_WITH_LEVEL(_runtime_profile, "ExecTime", 1);
        _memory_used_counter =
                _runtime_profile->AddHighWaterMarkCounter("MemoryUsage", TUnit::BYTES, "", 1);
    }

    void update_profile_from_inner() override {};
};

class MockPartitionedHashJoinSinkLocalState : public PartitionedHashJoinSinkLocalState {
public:
    MockPartitionedHashJoinSinkLocalState(PartitionedHashJoinSinkOperatorX* parent,
                                          RuntimeState* state, ObjectPool* pool)
            : PartitionedHashJoinSinkLocalState(parent, state) {
        _profile = pool->add(new RuntimeProfile("MockPartitionedHashJoinSinkLocalStateProfile"));

        _memory_used_counter =
                _profile->AddHighWaterMarkCounter("MemoryUsage", TUnit::BYTES, "", 1);
    }

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override { return Status::OK(); }

    void update_profile_from_inner() override {}
};

class PartitionedHashJoinTestHelper {
public:
    void SetUp();
    void TearDown();

    TPlanNode create_test_plan_node();

    PartitionedHashJoinProbeLocalState* create_probe_local_state(
            RuntimeState* state, PartitionedHashJoinProbeOperatorX* probe_operator,
            std::shared_ptr<MockPartitionedHashJoinSharedState>& shared_state);

    PartitionedHashJoinSinkLocalState* create_sink_local_state(
            RuntimeState* state, PartitionedHashJoinSinkOperatorX* sink_operator,
            std::shared_ptr<MockPartitionedHashJoinSharedState>& shared_state);

    std::tuple<std::shared_ptr<PartitionedHashJoinProbeOperatorX>,
               std::shared_ptr<PartitionedHashJoinSinkOperatorX>>
    create_operators();

    std::unique_ptr<MockRuntimeState> runtime_state;
    std::unique_ptr<ObjectPool> obj_pool;
    std::shared_ptr<QueryContext> query_ctx;
    std::shared_ptr<RuntimeProfile> runtime_profile;
    std::shared_ptr<PipelineTask> pipeline_task;
    DescriptorTbl* desc_tbl;
    static constexpr uint32_t TEST_PARTITION_COUNT = 8;
};
} // namespace doris::pipeline