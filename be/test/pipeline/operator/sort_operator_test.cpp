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

#include <algorithm>
#include <memory>

#include "pipeline/exec/repeat_operator.h"
#include "pipeline/exec/sort_sink_operator.h"
#include "pipeline/exec/sort_source_operator.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"
#include "vec/core/block.h"
namespace doris::pipeline {

using namespace vectorized;

class MockOperator : public OperatorXBase {
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

struct SortOperatorTest : public ::testing::Test {
    void SetUp() override {
        state = std::make_shared<MockRuntimeState>();
        state->batsh_size = 10;
        _child_op = std::make_unique<MockOperator>();
    }

    void create_operator(TSortAlgorithm::type type, int64_t limit, int64_t offset) {
        sink = std::make_unique<SortSinkOperatorX>(&pool, type, limit, offset);

        sink->_is_asc_order = {true};
        sink->_nulls_first = {false};
        sink->_vsort_exec_exprs._sort_tuple_slot_expr_ctxs =
                MockSlotRef::create_mock_contexts(std::make_shared<DataTypeInt64>());

        sink->_vsort_exec_exprs._materialize_tuple = false;

        sink->_vsort_exec_exprs._ordering_expr_ctxs =
                MockSlotRef::create_mock_contexts(std::make_shared<DataTypeInt64>());

        _child_op->_mock_row_desc.reset(
                new MockRowDescriptor {{std::make_shared<vectorized::DataTypeInt64>()}, &pool});

        EXPECT_TRUE(sink->set_child(_child_op));
        source = std::make_unique<SortSourceOperatorX>();

        create_local_state();
    }

    void create_local_state() {
        shared_state = sink->create_shared_state();
        {
            sink_local_state_uptr = SortSinkLocalState ::create_unique(sink.get(), state.get());
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
            source_local_state_uptr = SortLocalState::create_unique(state.get(), source.get());
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

        EXPECT_TRUE(sink_local_state->open(state.get()).ok());

        EXPECT_TRUE(source_local_state->open(state.get()).ok());
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

    RuntimeProfile profile {"test"};
    std::unique_ptr<SortSinkOperatorX> sink;
    std::unique_ptr<SortSourceOperatorX> source;

    std::unique_ptr<SortSinkLocalState> sink_local_state_uptr;

    SortSinkLocalState* sink_local_state;

    std::unique_ptr<SortLocalState> source_local_state_uptr;
    SortLocalState* source_local_state;

    std::shared_ptr<MockRuntimeState> state;

    std::shared_ptr<MockOperator> _child_op;

    ObjectPool pool;

    std::shared_ptr<BasicSharedState> shared_state;
};

TEST_F(SortOperatorTest, test) {
    create_operator(TSortAlgorithm::HEAP_SORT, 10, 0);

    {
        vectorized::Block block = ColumnHelper::create_block<DataTypeInt64>({2, 3, 1});
        auto st = sink->sink(state.get(), &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();
    }

    {
        vectorized::Block block = ColumnHelper::create_block<DataTypeInt64>({});
        bool eos = false;
        auto st = source->get_block(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_FALSE(eos);
        EXPECT_EQ(block.rows(), 3);
        std::cout << block.dump_data() << std::endl;
        EXPECT_TRUE(ColumnHelper::block_equal(
                block, ColumnHelper::create_block<DataTypeInt64>({1, 2, 3})));

        block.clear();
        st = source->get_block(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(eos);
        EXPECT_EQ(block.rows(), 0);
    }
}

TEST_F(SortOperatorTest, test_dep) {
    create_operator(TSortAlgorithm::HEAP_SORT, 10, 0);

    {
        vectorized::Block block = ColumnHelper::create_block<DataTypeInt64>({2, 3, 1});
        auto st = sink->sink(state.get(), &block, false);
        EXPECT_TRUE(st.ok()) << st.msg();
    }

    EXPECT_TRUE(is_ready(sink_local_state->dependencies()));
    EXPECT_TRUE(is_block(source_local_state->dependencies()));

    {
        vectorized::Block block = ColumnHelper::create_block<DataTypeInt64>({6, 5, 4});
        auto st = sink->sink(state.get(), &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();
    }

    EXPECT_TRUE(is_ready(source_local_state->dependencies()));

    {
        vectorized::Block block = ColumnHelper::create_block<DataTypeInt64>({});
        bool eos = false;
        auto st = source->get_block(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_FALSE(eos);
        EXPECT_EQ(block.rows(), 6);
        std::cout << block.dump_data() << std::endl;
        EXPECT_TRUE(ColumnHelper::block_equal(
                block, ColumnHelper::create_block<DataTypeInt64>({1, 2, 3, 4, 5, 6})));

        block.clear();
        st = source->get_block(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(eos);
        EXPECT_EQ(block.rows(), 0);
    }
}

TEST_F(SortOperatorTest, test_sort_type) {
    create_operator(TSortAlgorithm::HEAP_SORT, 10, 0);

    auto sort_for_type = [&](TSortAlgorithm::type type, int64_t limit, int64_t offset) {
        SetUp();
        create_operator(type, limit, offset);
        std::vector<int64_t> vec;
        for (int i = 0; i < 100; i++) {
            vec.push_back(i);
        }
        std::random_shuffle(vec.begin(), vec.end());

        {
            vectorized::Block block = ColumnHelper::create_block<DataTypeInt64>(vec);
            auto st = sink->sink(state.get(), &block, true);
            EXPECT_TRUE(st.ok()) << st.msg();
        }

        auto do_sort = [&]() {
            std::sort(vec.begin(), vec.end());
            std::vector<int64_t> vec2;
            if (limit != -1) {
                for (int i = offset; i < vec.size() && i < offset + limit; i++) {
                    vec2.push_back(vec[i]);
                }
            } else {
                for (int i = offset; i < vec.size(); i++) {
                    vec2.push_back(vec[i]);
                }
            }
            return vec2;
        };

        {
            MutableBlock m_block = ColumnHelper::create_block<DataTypeInt64>({});
            bool eos = false;

            while (!eos) {
                Block block;
                auto st = source->get_block(state.get(), &block, &eos);
                EXPECT_TRUE(st.ok()) << st.msg();
                EXPECT_TRUE(m_block.merge(block));
            }

            auto block = m_block.to_block();

            auto sort_vec = do_sort();

            EXPECT_TRUE(ColumnHelper::block_equal(
                    block, ColumnHelper::create_block<DataTypeInt64>(sort_vec)));
        }
    };

    sort_for_type(TSortAlgorithm::HEAP_SORT, 10, 0);
    sort_for_type(TSortAlgorithm::TOPN_SORT, 10, 0);
    sort_for_type(TSortAlgorithm::FULL_SORT, 10, 0);

    sort_for_type(TSortAlgorithm::HEAP_SORT, 50, 20);
    sort_for_type(TSortAlgorithm::TOPN_SORT, 50, 20);
    sort_for_type(TSortAlgorithm::FULL_SORT, 50, 20);
}

} // namespace doris::pipeline