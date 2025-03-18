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

#include "common/config.h"
#include "common/factory_creator.h"
#include "common/object_pool.h"
#include "pipeline/exec/aggregation_sink_operator.h"
#include "pipeline/exec/aggregation_source_operator.h"
#include "pipeline/exec/partitioned_aggregation_sink_operator.h"
#include "pipeline/exec/partitioned_aggregation_source_operator.h"
#include "pipeline/pipeline_task.h"
#include "runtime/fragment_mgr.h"
#include "spillable_operator_test_helper.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {
class MockAggSharedState : public AggSharedState {
public:
};

class MockPartitionedAggSharedState : public PartitionedAggSharedState {
    ENABLE_FACTORY_CREATOR(MockPartitionedAggSharedState);

public:
    MockPartitionedAggSharedState() { is_spilled = false; }
};

class MockPartitionedAggSinkLocalState : public PartitionedAggSinkLocalState {
    ENABLE_FACTORY_CREATOR(MockPartitionedAggSinkLocalState)
public:
    MockPartitionedAggSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : PartitionedAggSinkLocalState(parent, state) {
        _operator_profile = state->obj_pool()->add(new RuntimeProfile("test"));
        _custom_profile = state->obj_pool()->add(new RuntimeProfile("CustomCounters"));
        _common_profile = state->obj_pool()->add(new RuntimeProfile("CommonCounters"));
        _memory_used_counter =
                _common_profile->AddHighWaterMarkCounter("MemoryUsage", TUnit::BYTES, "", 1);
    }

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override { return Status::OK(); }
    Status open(RuntimeState* state) override { return Status::OK(); }
    Status close(RuntimeState* state, Status status) override { return Status::OK(); }
};

class MockPartitionedAggSinkOperatorX : public PartitionedAggSinkOperatorX {
public:
    MockPartitionedAggSinkOperatorX(ObjectPool* pool, int operator_id, int dest_id,
                                    const TPlanNode& tnode, const DescriptorTbl& descs)
            : PartitionedAggSinkOperatorX(pool, operator_id, dest_id, tnode, descs, false) {}
    ~MockPartitionedAggSinkOperatorX() override = default;

    Status prepare(RuntimeState* state) override { return Status::OK(); }

    Status setup_local_state(RuntimeState* state, LocalSinkStateInfo& info) override {
        state->emplace_sink_local_state(
                _operator_id, MockPartitionedAggSinkLocalState::create_unique(this, state));
        return Status::OK();
    }

    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos) override {
        return Status::OK();
    }
};

class MockPartitionedAggLocalState : public PartitionedAggLocalState {
    ENABLE_FACTORY_CREATOR(MockPartitionedAggLocalState);

public:
    MockPartitionedAggLocalState(RuntimeState* state, OperatorXBase* parent)
            : PartitionedAggLocalState(state, parent) {
        _operator_profile = std::make_unique<RuntimeProfile>("test");
        _common_profile = std::make_unique<RuntimeProfile>("CommonCounters");
        _custom_profile = std::make_unique<RuntimeProfile>("CustomCounters");
        _operator_profile->add_child(_common_profile.get(), true);
        _operator_profile->add_child(_custom_profile.get(), true);
    }

    Status open(RuntimeState* state) override { return Status::OK(); }
};

class MockAggLocalState : public AggLocalState {
    ENABLE_FACTORY_CREATOR(MockAggLocalState);

public:
    MockAggLocalState(RuntimeState* state, OperatorXBase* parent) : AggLocalState(state, parent) {};
};

class MockAggSourceOperatorX : public AggSourceOperatorX {
public:
    MockAggSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                           const DescriptorTbl& descs)
            : AggSourceOperatorX(pool, tnode, operator_id, descs) {}
    ~MockAggSourceOperatorX() override = default;

    Status setup_local_state(RuntimeState* state, LocalStateInfo& info) override {
        state->emplace_local_state(_operator_id, MockAggLocalState::create_unique(state, this));
        return Status::OK();
    }

    bool need_more_input_data(RuntimeState* state) const override { return need_more_data; }
    bool need_more_data = true;

    vectorized::Block block;
    bool eos = false;
};

class MockAggSinkOperatorX : public AggSinkOperatorX {
public:
    MockAggSinkOperatorX() = default;
    ~MockAggSinkOperatorX() override = default;
};

class PartitionedAggregationTestHelper : public SpillableOperatorTestHelper {
public:
    ~PartitionedAggregationTestHelper() override = default;
    TPlanNode create_test_plan_node() override;
    TDescriptorTable create_test_table_descriptor(bool nullable) override;

    PartitionedAggLocalState* create_source_local_state(
            RuntimeState* state, PartitionedAggSourceOperatorX* source_operator,
            std::shared_ptr<MockPartitionedAggSharedState>& shared_state);

    PartitionedAggSinkLocalState* create_sink_local_state(
            RuntimeState* state, PartitionedAggSinkOperatorX* sink_operator,
            std::shared_ptr<MockPartitionedAggSharedState>& shared_state);

    std::tuple<std::shared_ptr<PartitionedAggSourceOperatorX>,
               std::shared_ptr<PartitionedAggSinkOperatorX>>
    create_operators();
};
} // namespace doris::pipeline