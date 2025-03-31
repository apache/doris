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

#include "pipeline/exec/partition_sort_sink_operator.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>

#include "pipeline/exec/partition_sort_source_operator.h"
#include "pipeline/operator/operator_helper.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"
#include "vec/core/block.h"
namespace doris::pipeline {

using namespace vectorized;

class PartitionSortOperatorMockOperator : public OperatorXBase {
public:
    Status get_block_after_projects(RuntimeState* state, vectorized::Block* block,
                                    bool* eos) override {
        return Status::OK();
    }

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos) override {
        return Status::OK();
    }
    Status setup_local_state(RuntimeState* state, LocalStateInfo& info) override {
        return Status::OK();
    }

    const RowDescriptor& row_desc() const override { return *_mock_row_desc; }

private:
    std::unique_ptr<MockRowDescriptor> _mock_row_desc;
};

struct PartitionSortOperatorTest : public ::testing::Test {
    void SetUp() override {
        state = std::make_shared<MockRuntimeState>();
        state->batsh_size = 10;
        _child_op = std::make_unique<PartitionSortOperatorMockOperator>();
    }

    RuntimeProfile profile {"test"};
    std::unique_ptr<PartitionSortSinkOperatorX> sink;
    std::unique_ptr<PartitionSortSourceOperatorX> source;

    std::unique_ptr<PartitionSortSinkLocalState> sink_local_state_uptr;

    PartitionSortSinkLocalState* sink_local_state;

    std::unique_ptr<PartitionSortSourceLocalState> source_local_state_uptr;
    PartitionSortSourceLocalState* source_local_state;

    std::shared_ptr<MockRuntimeState> state;

    std::shared_ptr<PartitionSortOperatorMockOperator> _child_op;

    ObjectPool pool;

    std::shared_ptr<BasicSharedState> shared_state;

    bool is_ready(std::vector<Dependency*> deps) {
        for (auto* dep : deps) {
            if (!dep->ready()) {
                return false;
            }
        }
        return true;
    }

    void test_for_sink_and_source() {
        SetUp();
        sink = std::make_unique<PartitionSortSinkOperatorX>(&pool, -1, 1);
        sink->_is_asc_order = {true};
        sink->_nulls_first = {false};

        sink->_vsort_exec_exprs._sort_tuple_slot_expr_ctxs =
                MockSlotRef::create_mock_contexts(std::make_shared<DataTypeInt64>());

        sink->_vsort_exec_exprs._materialize_tuple = false;

        sink->_vsort_exec_exprs._ordering_expr_ctxs =
                MockSlotRef::create_mock_contexts(std::make_shared<DataTypeInt64>());

        sink->_partition_expr_ctxs =
                MockSlotRef::create_mock_contexts(std::make_shared<DataTypeInt64>());
        _child_op->_mock_row_desc.reset(
                new MockRowDescriptor {{std::make_shared<vectorized::DataTypeInt64>()}, &pool});

        EXPECT_TRUE(sink->set_child(_child_op));

        source = std::make_unique<PartitionSortSourceOperatorX>();

        shared_state = sink->create_shared_state();
        {
            sink_local_state_uptr =
                    PartitionSortSinkLocalState ::create_unique(sink.get(), state.get());
            sink_local_state = sink_local_state_uptr.get();
            LocalSinkStateInfo info {.task_idx = 0,
                                     .parent_profile = &profile,
                                     .sender_id = 0,
                                     .shared_state = shared_state.get(),
                                     .shared_state_map = {},
                                     .tsink = TDataSink {}};
            EXPECT_TRUE(sink_local_state_uptr->init(state.get(), info).ok());
            state->emplace_sink_local_state(0, std::move(sink_local_state_uptr));
        }

        {
            source_local_state_uptr =
                    PartitionSortSourceLocalState::create_unique(state.get(), source.get());
            source_local_state = source_local_state_uptr.get();
            LocalStateInfo info {.parent_profile = &profile,
                                 .scan_ranges = {},
                                 .shared_state = shared_state.get(),
                                 .shared_state_map = {},
                                 .task_idx = 0};

            EXPECT_TRUE(source_local_state_uptr->init(state.get(), info).ok());
            state->resize_op_id_to_local_state(-100);
            state->emplace_local_state(source->operator_id(), std::move(source_local_state_uptr));
        }

        { EXPECT_TRUE(sink_local_state->open(state.get()).ok()); }
        { EXPECT_TRUE(source_local_state->open(state.get()).ok()); }

        auto sink_func = [&]() {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            Block block = ColumnHelper::create_block<DataTypeInt64>({1, 2, 3, 4});
            EXPECT_TRUE(sink->sink(state.get(), &block, true));
        };

        auto source_func = [&]() {
            bool eos = false;
            while (true) {
                if (is_ready(source_local_state->dependencies())) {
                    Block block;
                    EXPECT_TRUE(source->get_block(state.get(), &block, &eos).ok());
                    std::cout << "source block\n" << block.dump_data() << std::endl;
                    if (eos) {
                        break;
                    }
                }
            }
        };

        std::thread sink_thread(sink_func);
        std::thread source_thread(source_func);
        sink_thread.join();
        source_thread.join();
    }
};

TEST_F(PartitionSortOperatorTest, test) {
    for (int i = 0; i < 100; i++) {
        test_for_sink_and_source();
    }
}

} // namespace doris::pipeline
