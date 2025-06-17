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

#include "pipeline/exec/spill_sort_source_operator.h"

#include <gtest/gtest.h>

#include <limits>
#include <memory>

#include "common/config.h"
#include "pipeline/dependency.h"
#include "pipeline/exec/operator.h"
#include "runtime/exec_env.h"
#include "spill_sort_test_helper.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_runtime_state.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_number.h"

namespace doris::pipeline {
class SpillSortSourceOperatorTest : public testing::Test {
protected:
    void SetUp() override { _helper.SetUp(); }
    void TearDown() override { _helper.TearDown(); }
    SpillSortTestHelper _helper;
};

TEST_F(SpillSortSourceOperatorTest, Basic) {
    auto [source_operator, sink_operator] = _helper.create_operators();
    ASSERT_TRUE(source_operator != nullptr);

    auto tnode = _helper.create_test_plan_node();
    auto st = source_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();

    st = source_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "prepare failed: " << st.to_string();

    auto shared_state =
            std::dynamic_pointer_cast<SpillSortSharedState>(sink_operator->create_shared_state());
    ASSERT_TRUE(shared_state != nullptr);

    shared_state->in_mem_shared_state_sptr = std::make_shared<MockSortSharedState>();
    shared_state->in_mem_shared_state =
            static_cast<SortSharedState*>(shared_state->in_mem_shared_state_sptr.get());

    LocalStateInfo info {.parent_profile = _helper.operator_profile.get(),
                         .scan_ranges = {},
                         .shared_state = shared_state.get(),
                         .shared_state_map = {},
                         .task_idx = 0};

    st = source_operator->setup_local_state(_helper.runtime_state.get(), info);
    ASSERT_TRUE(st.ok()) << "setup_local_state failed: " << st.to_string();

    auto* local_state = _helper.runtime_state->get_local_state(source_operator->operator_id());
    ASSERT_TRUE(local_state != nullptr);

    st = local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

    st = local_state->close(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "close failed: " << st.to_string();

    st = source_operator->close(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "close failed: " << st.to_string();
}

TEST_F(SpillSortSourceOperatorTest, GetBlock) {
    auto [source_operator, sink_operator] = _helper.create_operators();
    ASSERT_TRUE(source_operator != nullptr);

    auto tnode = _helper.create_test_plan_node();
    auto mock_inner_source_operator = std::make_unique<MockSortSourceOperatorX>(
            _helper.obj_pool.get(), tnode, 0, *_helper.desc_tbl);

    auto* inner_source_operator = mock_inner_source_operator.get();

    source_operator->_sort_source_operator = std::move(mock_inner_source_operator);
    auto st = source_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();

    st = source_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "prepare failed: " << st.to_string();

    auto shared_state =
            std::dynamic_pointer_cast<SpillSortSharedState>(sink_operator->create_shared_state());
    ASSERT_TRUE(shared_state != nullptr);

    shared_state->in_mem_shared_state_sptr = std::make_shared<MockSortSharedState>();
    shared_state->in_mem_shared_state =
            static_cast<SortSharedState*>(shared_state->in_mem_shared_state_sptr.get());

    LocalStateInfo info {.parent_profile = _helper.operator_profile.get(),
                         .scan_ranges = {},
                         .shared_state = shared_state.get(),
                         .shared_state_map = {},
                         .task_idx = 0};

    st = source_operator->setup_local_state(_helper.runtime_state.get(), info);
    ASSERT_TRUE(st.ok()) << "setup_local_state failed: " << st.to_string();

    auto* local_state = _helper.runtime_state->get_local_state(source_operator->operator_id());
    ASSERT_TRUE(local_state != nullptr);

    shared_state->setup_shared_profile(_helper.operator_profile.get());

    st = local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

    auto input_block = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(
            {1, 2, 3, 4, 5, 5, 4, 3, 2, 1});

    input_block.insert(vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeInt64>(
            {10, 9, 8, 7, 6, 5, 4, 3, 2, 1}));

    const auto rows = input_block.rows();

    inner_source_operator->block = input_block;

    vectorized::Block block;
    bool eos = false;
    st = source_operator->get_block(_helper.runtime_state.get(), &block, &eos);
    ASSERT_TRUE(st.ok()) << "get_block failed: " << st.to_string();

    ASSERT_EQ(block.rows(), rows);

    inner_source_operator->eos = true;
    st = source_operator->get_block(_helper.runtime_state.get(), &block, &eos);
    ASSERT_TRUE(st.ok()) << "get_block failed: " << st.to_string();
    ASSERT_TRUE(eos) << "get_block failed: eos is not true";

    st = local_state->close(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "close failed: " << st.to_string();

    st = source_operator->close(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "close failed: " << st.to_string();
}

TEST_F(SpillSortSourceOperatorTest, GetBlockWithSpill) {
    auto [source_operator, sink_operator] = _helper.create_operators();
    ASSERT_TRUE(source_operator != nullptr);

    auto tnode = _helper.create_test_plan_node();
    auto st = source_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();

    st = source_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "prepare failed: " << st.to_string();

    auto shared_state =
            std::dynamic_pointer_cast<SpillSortSharedState>(sink_operator->create_shared_state());
    ASSERT_TRUE(shared_state != nullptr);

    st = sink_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();

    st = sink_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "prepare failed: " << st.to_string();

    LocalSinkStateInfo sink_info {0, _helper.operator_profile.get(), -1, shared_state.get(), {},
                                  {}};
    st = sink_operator->setup_local_state(_helper.runtime_state.get(), sink_info);
    ASSERT_TRUE(st.ok()) << "setup_local_state failed: " << st.to_string();

    auto* sink_local_state = _helper.runtime_state->get_sink_local_state();
    DCHECK(sink_local_state != nullptr);

    st = sink_local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

    LocalStateInfo info {.parent_profile = _helper.operator_profile.get(),
                         .scan_ranges = {},
                         .shared_state = shared_state.get(),
                         .shared_state_map = {},
                         .task_idx = 0};

    st = source_operator->setup_local_state(_helper.runtime_state.get(), info);
    ASSERT_TRUE(st.ok()) << "setup_local_state failed: " << st.to_string();

    auto* local_state = reinterpret_cast<SpillSortLocalState*>(
            _helper.runtime_state->get_local_state(source_operator->operator_id()));
    ASSERT_TRUE(local_state != nullptr);

    shared_state->setup_shared_profile(_helper.operator_profile.get());

    st = local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

    shared_state->is_spilled = true;

    auto* sorter = shared_state->in_mem_shared_state->sorter.get();

    sorter->_sort_description.resize(sorter->_vsort_exec_exprs.ordering_expr_ctxs().size());
    for (int i = 0; i < sorter->_sort_description.size(); i++) {
        sorter->_sort_description[i].column_number = i;
        sorter->_sort_description[i].direction = 1;
        sorter->_sort_description[i].nulls_direction = 1;
    }

    // Prepare stored streams
    for (size_t i = 0; i != 4; ++i) {
        vectorized::SpillStreamSPtr spill_stream;
        st = ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
                _helper.runtime_state.get(), spill_stream,
                print_id(_helper.runtime_state->query_id()), sink_operator->get_name(),
                sink_operator->node_id(), std::numeric_limits<int32_t>::max(),
                std::numeric_limits<int32_t>::max(), _helper.operator_profile.get());
        ASSERT_TRUE(st.ok()) << "register_spill_stream failed: " << st.to_string();

        std::vector<int32_t> data;
        std::vector<int64_t> data2;
        for (size_t j = 0; j != 10; ++j) {
            data.emplace_back(j + i * 2);
            data2.emplace_back(j);
        }

        auto input_block = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(data);

        input_block.insert(
                vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeInt64>(
                        data2));

        st = spill_stream->spill_block(_helper.runtime_state.get(), input_block, true);
        ASSERT_TRUE(st.ok()) << "spill_block failed: " << st.to_string();

        shared_state->sorted_streams.emplace_back(std::move(spill_stream));
    }

    std::unique_ptr<vectorized::MutableBlock> mutable_block;
    bool eos = false;
    while (!eos) {
        vectorized::Block block;
        shared_state->spill_block_batch_row_count = 100;
        st = source_operator->get_block(_helper.runtime_state.get(), &block, &eos);
        ASSERT_TRUE(st.ok()) << "get_block failed: " << st.to_string();

        while (local_state->_spill_dependency->_ready.load() == false) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        if (block.empty()) {
            continue;
        }

        if (!mutable_block) {
            mutable_block = vectorized::MutableBlock::create_unique(std::move(block));
        } else {
            st = mutable_block->merge(std::move(block));
            ASSERT_TRUE(st.ok()) << "merge failed: " << st.to_string();
        }
    }

    ASSERT_TRUE(shared_state->sorted_streams.empty()) << "sorted_streams is not empty";
    ASSERT_TRUE(mutable_block) << "mutable_block is null";
    ASSERT_EQ(mutable_block->rows(), 40);
    auto output_block = mutable_block->to_block();
    const auto& col1 = output_block.get_by_position(0).column;
    const auto& col2 = output_block.get_by_position(1).column;

    ASSERT_EQ(col1->get_int(0), 0);
    ASSERT_EQ(col1->get_int(1), 1);
    ASSERT_EQ(col1->get_int(2), 2);
    ASSERT_EQ(col1->get_int(3), 2);

    ASSERT_EQ(col2->get_int(0), 0);
    ASSERT_EQ(col2->get_int(1), 1);
    ASSERT_EQ(col2->get_int(2), 0);
    ASSERT_EQ(col2->get_int(3), 2);

    ASSERT_EQ(col2->get_int(36), 7);
    ASSERT_EQ(col2->get_int(37), 9);
    ASSERT_EQ(col2->get_int(38), 8);
    ASSERT_EQ(col2->get_int(39), 9);

    for (size_t i = 1; i != col1->size(); ++i) {
        ASSERT_GE(col1->get_int(i), col1->get_int(i - 1));
    }

    st = local_state->close(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "close failed: " << st.to_string();

    st = source_operator->close(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "close failed: " << st.to_string();
}

// Same as `GetBlockWithSpill`, but with a different  `spill_sort_mem_limit` value.
TEST_F(SpillSortSourceOperatorTest, GetBlockWithSpill2) {
    auto [source_operator, sink_operator] = _helper.create_operators();
    ASSERT_TRUE(source_operator != nullptr);

    auto tnode = _helper.create_test_plan_node();
    auto st = source_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();

    st = source_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "prepare failed: " << st.to_string();

    auto shared_state =
            std::dynamic_pointer_cast<SpillSortSharedState>(sink_operator->create_shared_state());
    ASSERT_TRUE(shared_state != nullptr);

    st = sink_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();

    st = sink_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "prepare failed: " << st.to_string();

    LocalSinkStateInfo sink_info {0, _helper.operator_profile.get(), -1, shared_state.get(), {},
                                  {}};
    st = sink_operator->setup_local_state(_helper.runtime_state.get(), sink_info);
    ASSERT_TRUE(st.ok()) << "setup_local_state failed: " << st.to_string();

    auto* sink_local_state = _helper.runtime_state->get_sink_local_state();
    DCHECK(sink_local_state != nullptr);

    st = sink_local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

    LocalStateInfo info {.parent_profile = _helper.operator_profile.get(),
                         .scan_ranges = {},
                         .shared_state = shared_state.get(),
                         .shared_state_map = {},
                         .task_idx = 0};

    st = source_operator->setup_local_state(_helper.runtime_state.get(), info);
    ASSERT_TRUE(st.ok()) << "setup_local_state failed: " << st.to_string();

    auto* local_state = reinterpret_cast<SpillSortLocalState*>(
            _helper.runtime_state->get_local_state(source_operator->operator_id()));
    ASSERT_TRUE(local_state != nullptr);

    shared_state->setup_shared_profile(_helper.operator_profile.get());

    st = local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

    shared_state->is_spilled = true;

    auto* sorter = shared_state->in_mem_shared_state->sorter.get();

    sorter->_sort_description.resize(sorter->_vsort_exec_exprs.ordering_expr_ctxs().size());
    for (int i = 0; i < sorter->_sort_description.size(); i++) {
        sorter->_sort_description[i].column_number = i;
        sorter->_sort_description[i].direction = 1;
        sorter->_sort_description[i].nulls_direction = 1;
    }

    // Prepare stored streams
    for (size_t i = 0; i != 4; ++i) {
        vectorized::SpillStreamSPtr spill_stream;
        st = ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
                _helper.runtime_state.get(), spill_stream,
                print_id(_helper.runtime_state->query_id()), sink_operator->get_name(),
                sink_operator->node_id(), std::numeric_limits<int32_t>::max(),
                std::numeric_limits<int32_t>::max(), _helper.operator_profile.get());
        ASSERT_TRUE(st.ok()) << "register_spill_stream failed: " << st.to_string();

        std::vector<int32_t> data;
        std::vector<int64_t> data2;
        for (size_t j = 0; j != 10; ++j) {
            data.emplace_back(j + i * 2);
            data2.emplace_back(j);
        }

        auto input_block = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(data);

        input_block.insert(
                vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeInt64>(
                        data2));

        st = spill_stream->spill_block(_helper.runtime_state.get(), input_block, true);
        ASSERT_TRUE(st.ok()) << "spill_block failed: " << st.to_string();

        shared_state->sorted_streams.emplace_back(std::move(spill_stream));
    }

    auto query_options = _helper.runtime_state->query_options();
    query_options.spill_sort_mem_limit = 16777216;
    query_options.__isset.spill_sort_mem_limit = true;
    _helper.runtime_state->set_query_options(query_options);

    std::unique_ptr<vectorized::MutableBlock> mutable_block;
    bool eos = false;
    while (!eos) {
        vectorized::Block block;
        shared_state->spill_block_batch_row_count = 100;
        st = source_operator->get_block(_helper.runtime_state.get(), &block, &eos);
        ASSERT_TRUE(st.ok()) << "get_block failed: " << st.to_string();

        while (local_state->_spill_dependency->_ready.load() == false) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        if (block.empty()) {
            continue;
        }

        if (!mutable_block) {
            mutable_block = vectorized::MutableBlock::create_unique(std::move(block));
        } else {
            st = mutable_block->merge(std::move(block));
            ASSERT_TRUE(st.ok()) << "merge failed: " << st.to_string();
        }
    }

    ASSERT_TRUE(shared_state->sorted_streams.empty()) << "sorted_streams is not empty";
    ASSERT_TRUE(mutable_block) << "mutable_block is null";
    ASSERT_EQ(mutable_block->rows(), 40);
    auto output_block = mutable_block->to_block();
    const auto& col1 = output_block.get_by_position(0).column;
    const auto& col2 = output_block.get_by_position(1).column;

    ASSERT_EQ(col1->get_int(0), 0);
    ASSERT_EQ(col1->get_int(1), 1);
    ASSERT_EQ(col1->get_int(2), 2);
    ASSERT_EQ(col1->get_int(3), 2);

    ASSERT_EQ(col2->get_int(0), 0);
    ASSERT_EQ(col2->get_int(1), 1);
    ASSERT_EQ(col2->get_int(2), 0);
    ASSERT_EQ(col2->get_int(3), 2);

    ASSERT_EQ(col2->get_int(36), 7);
    ASSERT_EQ(col2->get_int(37), 9);
    ASSERT_EQ(col2->get_int(38), 8);
    ASSERT_EQ(col2->get_int(39), 9);

    for (size_t i = 1; i != col1->size(); ++i) {
        ASSERT_GE(col1->get_int(i), col1->get_int(i - 1));
    }

    st = local_state->close(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "close failed: " << st.to_string();

    st = source_operator->close(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "close failed: " << st.to_string();
}

TEST_F(SpillSortSourceOperatorTest, GetBlockWithSpillError) {
    auto [source_operator, sink_operator] = _helper.create_operators();
    ASSERT_TRUE(source_operator != nullptr);

    auto tnode = _helper.create_test_plan_node();
    auto st = source_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();

    st = source_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "prepare failed: " << st.to_string();

    auto shared_state =
            std::dynamic_pointer_cast<SpillSortSharedState>(sink_operator->create_shared_state());
    ASSERT_TRUE(shared_state != nullptr);

    st = sink_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();

    st = sink_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "prepare failed: " << st.to_string();

    LocalSinkStateInfo sink_info {0, _helper.operator_profile.get(), -1, shared_state.get(), {},
                                  {}};
    st = sink_operator->setup_local_state(_helper.runtime_state.get(), sink_info);
    ASSERT_TRUE(st.ok()) << "setup_local_state failed: " << st.to_string();

    auto* sink_local_state = _helper.runtime_state->get_sink_local_state();
    DCHECK(sink_local_state != nullptr);

    st = sink_local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

    LocalStateInfo info {.parent_profile = _helper.operator_profile.get(),
                         .scan_ranges = {},
                         .shared_state = shared_state.get(),
                         .shared_state_map = {},
                         .task_idx = 0};

    st = source_operator->setup_local_state(_helper.runtime_state.get(), info);
    ASSERT_TRUE(st.ok()) << "setup_local_state failed: " << st.to_string();

    auto* local_state = reinterpret_cast<SpillSortLocalState*>(
            _helper.runtime_state->get_local_state(source_operator->operator_id()));
    ASSERT_TRUE(local_state != nullptr);

    shared_state->setup_shared_profile(_helper.operator_profile.get());

    st = local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

    shared_state->is_spilled = true;

    auto* sorter = shared_state->in_mem_shared_state->sorter.get();

    sorter->_sort_description.resize(sorter->_vsort_exec_exprs.ordering_expr_ctxs().size());
    for (int i = 0; i < sorter->_sort_description.size(); i++) {
        sorter->_sort_description[i].column_number = i;
        sorter->_sort_description[i].direction = 1;
        sorter->_sort_description[i].nulls_direction = 1;
    }

    // Prepare stored streams
    for (size_t i = 0; i != 4; ++i) {
        vectorized::SpillStreamSPtr spill_stream;
        st = ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
                _helper.runtime_state.get(), spill_stream,
                print_id(_helper.runtime_state->query_id()), sink_operator->get_name(),
                sink_operator->node_id(), std::numeric_limits<int32_t>::max(),
                std::numeric_limits<int32_t>::max(), _helper.operator_profile.get());
        ASSERT_TRUE(st.ok()) << "register_spill_stream failed: " << st.to_string();

        std::vector<int32_t> data;
        std::vector<int64_t> data2;
        for (size_t j = 0; j != 10; ++j) {
            data.emplace_back(j + i * 2);
            data2.emplace_back(j);
        }

        auto input_block = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(data);

        input_block.insert(
                vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeInt64>(
                        data2));

        st = spill_stream->spill_block(_helper.runtime_state.get(), input_block, true);
        ASSERT_TRUE(st.ok()) << "spill_block failed: " << st.to_string();

        shared_state->sorted_streams.emplace_back(std::move(spill_stream));
    }

    SpillableDebugPointHelper dp_helper("fault_inject::spill_stream::read_next_block");

    std::unique_ptr<vectorized::MutableBlock> mutable_block;
    bool eos = false;
    while (!eos) {
        vectorized::Block block;
        shared_state->spill_block_batch_row_count = 100;
        st = source_operator->get_block(_helper.runtime_state.get(), &block, &eos);
        if (!st.ok()) {
            break;
        }

        while (local_state->_spill_dependency->_ready.load() == false) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        if (block.empty()) {
            continue;
        }

        if (!mutable_block) {
            mutable_block = vectorized::MutableBlock::create_unique(std::move(block));
        } else {
            st = mutable_block->merge(std::move(block));
            ASSERT_TRUE(st.ok()) << "merge failed: " << st.to_string();
        }
    }

    ASSERT_FALSE(dp_helper.get_spill_status().ok());

    st = local_state->close(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "close failed: " << st.to_string();

    st = source_operator->close(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "close failed: " << st.to_string();
}

} // namespace doris::pipeline