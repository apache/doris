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

#include "pipeline/exec/local_merge_sort_source_operator.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "pipeline/operator/operator_helper.h"
#include "testutil/mock/mock_slot_ref.h"
#include "testutil/mock/mock_sorter.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_number.h"
namespace doris::pipeline {

using namespace vectorized;

struct LocalMergeSOrtSourceOperatorTest : public testing::Test {
    LocalMergeSOrtSourceOperatorTest() : profile("LocalMergeSOrtSourceOperatorTest") {}
    ~LocalMergeSOrtSourceOperatorTest() override = default;
    void SetUp() override {
        ctx = std::make_shared<OperatorContext>();
        op = std::make_shared<LocalMergeSortSourceOperatorX>();
        op->_parallel_tasks = parallel_tasks;

        op->_is_asc_order = {false};
        op->_nulls_first = {false};

        op->_vsort_exec_exprs._sort_tuple_slot_expr_ctxs =
                MockSlotRef::create_mock_contexts(std::make_shared<DataTypeInt64>());

        op->_vsort_exec_exprs._materialize_tuple = false;

        op->_vsort_exec_exprs._ordering_expr_ctxs =
                MockSlotRef::create_mock_contexts(std::make_shared<DataTypeInt64>());

        op->init_dependencies_and_sorter();

        local_states.resize(parallel_tasks);
        runtime_states.resize(parallel_tasks);
        shared_states.resize(parallel_tasks);
        for (int i = 0; i < parallel_tasks; i++) {
            runtime_states[i] = std::make_shared<MockRuntimeState>();
            auto local_state =
                    std::make_unique<LocalMergeSortLocalState>(runtime_states[i].get(), op.get());
            local_states[i] = local_state.get();

            shared_states[i] = std::make_shared<SortSharedState>();
            shared_states[i]->sorter = std::make_shared<MockSorter>();
            LocalStateInfo info {.parent_profile = &profile,
                                 .scan_ranges = {},
                                 .shared_state = shared_states[i].get(),
                                 .shared_state_map = {},
                                 .task_idx = i};
            EXPECT_TRUE(local_state->init(runtime_states[i].get(), info));
            runtime_states[i]->resize_op_id_to_local_state(-100);
            runtime_states[i]->emplace_local_state(op->operator_id(), std::move(local_state));
        }
    }

    bool is_block(std::vector<Dependency*> deps) {
        for (auto* dep : deps) {
            if (!dep->ready()) {
                return true;
            }
        }
        return false;
    }

    bool is_ready(std::vector<Dependency*> deps) {
        for (auto* dep : deps) {
            if (!dep->ready()) {
                return false;
            }
        }
        return true;
    }

    std::shared_ptr<LocalMergeSortSourceOperatorX> op;
    std::vector<LocalMergeSortLocalState*> local_states;
    std::vector<std::shared_ptr<MockRuntimeState>> runtime_states;
    std::vector<std::shared_ptr<SortSharedState>> shared_states;
    std::shared_ptr<OperatorContext> ctx;
    RuntimeProfile profile;

    const int parallel_tasks = 3;
};

TEST_F(LocalMergeSOrtSourceOperatorTest, DependencyTest) {
    // Initially, all sources are blocked.
    for (int i = 0; i < parallel_tasks; i++) {
        auto* local_state = local_states[i];
        auto deps = local_state->dependencies();
        if (i == 0) {
            // 1. The main_source's dependencies include its own source dependency and all other_source_deps (created in LocalMergeSortSourceOperatorX).
            EXPECT_EQ(deps.size(), parallel_tasks);
        } else {
            // 2. The other_source's dependencies include only its own source dependency.
            EXPECT_EQ(deps.size(), 1);
        }
        EXPECT_TRUE(is_block(deps));
    }

    // Simulate sink wake up
    // 3. The sort sink sets the corresponding source dependency to ready.
    for (int i = 0; i < parallel_tasks; i++) {
        EXPECT_EQ(shared_states[i]->source_deps.size(), 1);
        shared_states[i]->source_deps[0]->set_ready();
    }

    // Main Source is still blocked, Other Source is ready
    // 4. At this point, other_source will execute, but main_source will not.
    for (int i = 0; i < parallel_tasks; i++) {
        auto* local_state = local_states[i];
        auto deps = local_state->dependencies();
        if (i == 0) {
            EXPECT_TRUE(is_block(deps));
        } else {
            EXPECT_TRUE(is_ready(deps));
        }
    }

    for (int i = 1; i < parallel_tasks; i++) {
        // 5. After other_source executes, it sets the corresponding other_source_deps to ready.
        bool eos = false;
        vectorized::Block block;
        EXPECT_TRUE(op->get_block(runtime_states[i].get(), &block, &eos));
        EXPECT_TRUE(eos);
        EXPECT_TRUE(block.empty());
    }

    // Main source will execute only after all other_sources have executed
    EXPECT_TRUE(is_ready(local_states[0]->dependencies()));

    // 6. Now, main_source will execute.
    {
        bool eos = false;
        vectorized::Block block;
        EXPECT_TRUE(op->get_block(runtime_states[0].get(), &block, &eos));
        EXPECT_TRUE(eos);
        EXPECT_TRUE(block.empty());
    }
}

} // namespace doris::pipeline
