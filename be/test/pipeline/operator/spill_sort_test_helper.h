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
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/object_pool.h"
#include "pipeline/exec/sort_source_operator.h"
#include "pipeline/exec/spill_sort_sink_operator.h"
#include "pipeline/exec/spill_sort_source_operator.h"
#include "pipeline/pipeline_task.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "spillable_operator_test_helper.h"
#include "testutil/mock/mock_runtime_state.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {

class MockSpillSortSharedState : public SpillSortSharedState {};

class MockSortSharedState : public SortSharedState {};

class MockSortSourceOperatorX : public SortSourceOperatorX {
public:
    MockSortSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                            const DescriptorTbl& descs)
            : SortSourceOperatorX(pool, tnode, operator_id, descs) {}

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos) override {
        std::swap(*block, this->block);
        *eos = this->eos;
        return Status::OK();
    }

    bool eos = false;
    vectorized::Block block;
};

class SpillSortTestHelper : public SpillableOperatorTestHelper {
public:
    ~SpillSortTestHelper() override = default;

    TPlanNode create_test_plan_node() override;

    TDescriptorTable create_test_table_descriptor(bool nullable) override;

    SpillSortLocalState* create_source_local_state(
            RuntimeState* state, SpillSortSourceOperatorX* source_operator,
            std::shared_ptr<MockSpillSortSharedState>& shared_state);

    SpillSortSinkLocalState* create_sink_local_state(
            RuntimeState* state, SpillSortSinkOperatorX* sink_operator,
            std::shared_ptr<MockSpillSortSharedState>& shared_state);

    std::tuple<std::shared_ptr<SpillSortSourceOperatorX>, std::shared_ptr<SpillSortSinkOperatorX>>
    create_operators();
};
} // namespace doris::pipeline