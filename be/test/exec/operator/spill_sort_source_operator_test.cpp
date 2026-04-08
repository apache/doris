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

#include "exec/operator/spill_sort_source_operator.h"

#include <gtest/gtest.h>

#include <limits>
#include <memory>
#include <unordered_set>

#include "common/config.h"
#include "core/block/block.h"
#include "core/data_type/data_type_number.h"
#include "exec/operator/operator.h"
#include "exec/operator/spill_sort_test_helper.h"
#include "exec/pipeline/dependency.h"
#include "runtime/exec_env.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {
class SpillSortSourceOperatorTest : public testing::Test {
protected:
    void SetUp() override { _helper.SetUp(); }
    void TearDown() override { _helper.TearDown(); }
    SpillSortTestHelper _helper;
};

namespace {

struct SpillSortSourceTestContext {
    std::shared_ptr<SpillSortSharedState> shared_state;
    SpillSortLocalState* local_state = nullptr;
    SpillSortSinkLocalState* sink_local_state = nullptr;
};

void init_spill_sort_description(SpillSortSharedState* shared_state) {
    auto* sorter = shared_state->in_mem_shared_state->sorter.get();
    auto& sort_desc = sorter->get_mutable_sort_description();
    sort_desc.resize(sorter->get_vsort_exec_exprs().ordering_expr_ctxs().size());
    for (int i = 0; i < static_cast<int>(sort_desc.size()); ++i) {
        sort_desc[i].column_number = i;
        sort_desc[i].direction = 1;
        sort_desc[i].nulls_direction = 1;
    }
}

Status prepare_spill_sort_source_context(
        SpillSortTestHelper& helper,
        const std::shared_ptr<SpillSortSourceOperatorX>& source_operator,
        const std::shared_ptr<SpillSortSinkOperatorX>& sink_operator,
        SpillSortSourceTestContext& context) {
    auto tnode = helper.create_test_plan_node();
    RETURN_IF_ERROR(source_operator->init(tnode, helper.runtime_state.get()));
    RETURN_IF_ERROR(source_operator->prepare(helper.runtime_state.get()));

    context.shared_state =
            std::dynamic_pointer_cast<SpillSortSharedState>(sink_operator->create_shared_state());
    DCHECK(context.shared_state != nullptr);

    RETURN_IF_ERROR(sink_operator->init(tnode, helper.runtime_state.get()));
    RETURN_IF_ERROR(sink_operator->prepare(helper.runtime_state.get()));

    LocalSinkStateInfo sink_info {
            0, helper.operator_profile.get(), -1, context.shared_state.get(), {}, {}};
    RETURN_IF_ERROR(sink_operator->setup_local_state(helper.runtime_state.get(), sink_info));
    context.sink_local_state = reinterpret_cast<SpillSortSinkLocalState*>(
            helper.runtime_state->get_sink_local_state());
    DCHECK(context.sink_local_state != nullptr);
    RETURN_IF_ERROR(context.sink_local_state->open(helper.runtime_state.get()));

    LocalStateInfo source_info {.parent_profile = helper.operator_profile.get(),
                                .scan_ranges = {},
                                .shared_state = context.shared_state.get(),
                                .shared_state_map = {},
                                .task_idx = 0};
    RETURN_IF_ERROR(source_operator->setup_local_state(helper.runtime_state.get(), source_info));
    context.local_state = reinterpret_cast<SpillSortLocalState*>(
            helper.runtime_state->get_local_state(source_operator->operator_id()));
    DCHECK(context.local_state != nullptr);
    RETURN_IF_ERROR(context.local_state->open(helper.runtime_state.get()));

    context.shared_state->is_spilled = true;
    init_spill_sort_description(context.shared_state.get());
    return Status::OK();
}

SpillFileSPtr create_sort_test_spill_file(RuntimeState* state, RuntimeProfile* profile, int node_id,
                                          const std::string& prefix,
                                          const std::vector<int32_t>& first_column,
                                          const std::vector<int64_t>& second_column) {
    EXPECT_EQ(first_column.size(), second_column.size());

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

    auto input_block = ColumnHelper::create_block<DataTypeInt32>(first_column);
    input_block.insert(ColumnHelper::create_column_with_name<DataTypeInt64>(second_column));

    st = writer->write_block(state, input_block);
    EXPECT_TRUE(st.ok()) << "write block failed: " << st.to_string();
    if (!st.ok()) {
        return nullptr;
    }

    st = writer->close();
    EXPECT_TRUE(st.ok()) << "close writer failed: " << st.to_string();
    if (!st.ok()) {
        return nullptr;
    }

    return spill_file;
}

Status read_all_blocks_from_source(RuntimeState* state, SpillSortSourceOperatorX* source_operator,
                                   std::unique_ptr<MutableBlock>& mutable_block) {
    bool eos = false;
    while (!eos) {
        Block block;
        RETURN_IF_ERROR(source_operator->get_block(state, &block, &eos));
        if (block.empty()) {
            continue;
        }
        if (!mutable_block) {
            mutable_block = MutableBlock::create_unique(std::move(block));
        } else {
            RETURN_IF_ERROR(mutable_block->merge(std::move(block)));
        }
    }
    return Status::OK();
}

std::vector<SpillFileSPtr> collect_spill_files_for_cleanup(
        const std::vector<SpillFileSPtr>& original_files, SpillSortLocalState* local_state,
        SpillSortSharedState* shared_state) {
    std::vector<SpillFileSPtr> spill_files;
    std::unordered_set<SpillFile*> seen;
    auto collect = [&](const SpillFileSPtr& spill_file) {
        if (spill_file && seen.emplace(spill_file.get()).second) {
            spill_files.emplace_back(spill_file);
        }
    };

    for (const auto& spill_file : original_files) {
        collect(spill_file);
    }
    for (const auto& spill_file : local_state->_current_merging_files) {
        collect(spill_file);
    }
    for (const auto& spill_file : shared_state->sorted_spill_groups) {
        collect(spill_file);
    }
    return spill_files;
}

void delete_spill_files(const std::vector<SpillFileSPtr>& spill_files) {
    for (const auto& spill_file : spill_files) {
        ExecEnv::GetInstance()->spill_file_mgr()->delete_spill_file(spill_file);
    }
}

} // namespace

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

    st = local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

    auto input_block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5, 5, 4, 3, 2, 1});

    input_block.insert(
            ColumnHelper::create_column_with_name<DataTypeInt64>({10, 9, 8, 7, 6, 5, 4, 3, 2, 1}));

    const auto rows = input_block.rows();

    inner_source_operator->block = input_block;

    Block block;
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

    st = local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

    shared_state->is_spilled = true;

    auto* sorter = shared_state->in_mem_shared_state->sorter.get();

    auto& sort_desc = sorter->get_mutable_sort_description();
    sort_desc.resize(sorter->get_vsort_exec_exprs().ordering_expr_ctxs().size());
    for (int i = 0; i < (int)sort_desc.size(); i++) {
        sort_desc[i].column_number = i;
        sort_desc[i].direction = 1;
        sort_desc[i].nulls_direction = 1;
    }

    // Prepare stored streams
    for (size_t i = 0; i != 4; ++i) {
        SpillFileSPtr spill_file;
        auto relative_path = fmt::format("{}/{}-{}-{}", print_id(_helper.runtime_state->query_id()),
                                         sink_operator->get_name(), sink_operator->node_id(),
                                         ExecEnv::GetInstance()->spill_file_mgr()->next_id());
        st = ExecEnv::GetInstance()->spill_file_mgr()->create_spill_file(relative_path, spill_file);
        ASSERT_TRUE(st.ok()) << "create_spill_file failed: " << st.to_string();

        std::vector<int32_t> data;
        std::vector<int64_t> data2;
        for (size_t j = 0; j != 10; ++j) {
            data.emplace_back(j + i * 2);
            data2.emplace_back(j);
        }

        auto input_block = ColumnHelper::create_block<DataTypeInt32>(data);

        input_block.insert(ColumnHelper::create_column_with_name<DataTypeInt64>(data2));

        SpillFileWriterSPtr writer;
        st = spill_file->create_writer(_helper.runtime_state.get(), _helper.operator_profile.get(),
                                       writer);
        ASSERT_TRUE(st.ok()) << "create_writer failed: " << st.to_string();
        st = writer->write_block(_helper.runtime_state.get(), input_block);
        ASSERT_TRUE(st.ok()) << "write_block failed: " << st.to_string();
        st = writer->close();
        ASSERT_TRUE(st.ok()) << "close writer failed: " << st.to_string();

        shared_state->sorted_spill_groups.emplace_back(std::move(spill_file));
    }

    std::unique_ptr<MutableBlock> mutable_block;
    bool eos = false;
    while (!eos) {
        Block block;
        shared_state->spill_block_batch_row_count = 100;
        st = source_operator->get_block(_helper.runtime_state.get(), &block, &eos);
        ASSERT_TRUE(st.ok()) << "get_block failed: " << st.to_string();
        if (block.empty()) {
            continue;
        }

        if (!mutable_block) {
            mutable_block = MutableBlock::create_unique(std::move(block));
        } else {
            st = mutable_block->merge(std::move(block));
            ASSERT_TRUE(st.ok()) << "merge failed: " << st.to_string();
        }
    }

    ASSERT_TRUE(eos);
    ASSERT_TRUE(shared_state->sorted_spill_groups.empty()) << "sorted_spill_groups is not empty";
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

    std::cout << "************** HERE WE GO!!!!!! **************" << std::endl;
}

// Verify that a normal revoke_memory invocation does not prematurely close the
// shared state.  Closing is the responsibility of the sink/operator teardown
// path, not the spill logic itself.
TEST_F(SpillSortSourceOperatorTest, RevokeMemoryKeepsSharedStateOpen) {
    auto [source_operator, sink_operator] = _helper.create_operators();

    // prepare sink operator and shared state as in other tests
    auto tnode = _helper.create_test_plan_node();
    auto shared_state =
            std::dynamic_pointer_cast<SpillSortSharedState>(sink_operator->create_shared_state());
    ASSERT_TRUE(shared_state != nullptr);

    // initialize sink
    auto st = sink_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();
    st = sink_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "prepare failed: " << st.to_string();

    LocalSinkStateInfo sink_info {0, _helper.operator_profile.get(), -1, shared_state.get(), {},
                                  {}};
    st = sink_operator->setup_local_state(_helper.runtime_state.get(), sink_info);
    ASSERT_TRUE(st.ok()) << "setup_local_state failed: " << st.to_string();

    auto* sink_local_state = _helper.runtime_state->get_sink_local_state();
    DCHECK(sink_local_state != nullptr);

    // open the local state to initialize in-memory sorter etc.
    st = sink_local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

    // clear any closure flag before revoking memory
    shared_state->is_closed = false;

    // call revoke_memory with no data; should succeed and leave shared_state open
    st = sink_operator->revoke_memory(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "revoke_memory failed: " << st.to_string();
    ASSERT_FALSE(shared_state->is_closed) << "shared state was closed by a successful revoke";

    // cleanup
    st = sink_local_state->close(_helper.runtime_state.get(), Status::OK());
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

    st = local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

    shared_state->is_spilled = true;

    auto* sorter = shared_state->in_mem_shared_state->sorter.get();

    auto& sort_desc = sorter->get_mutable_sort_description();
    sort_desc.resize(sorter->get_vsort_exec_exprs().ordering_expr_ctxs().size());
    for (int i = 0; i < (int)sort_desc.size(); i++) {
        sort_desc[i].column_number = i;
        sort_desc[i].direction = 1;
        sort_desc[i].nulls_direction = 1;
    }

    // Prepare stored streams
    for (size_t i = 0; i != 4; ++i) {
        SpillFileSPtr spill_file;
        auto relative_path = fmt::format("{}/{}-{}-{}", print_id(_helper.runtime_state->query_id()),
                                         sink_operator->get_name(), sink_operator->node_id(),
                                         ExecEnv::GetInstance()->spill_file_mgr()->next_id());
        st = ExecEnv::GetInstance()->spill_file_mgr()->create_spill_file(relative_path, spill_file);
        ASSERT_TRUE(st.ok()) << "create_spill_file failed: " << st.to_string();

        std::vector<int32_t> data;
        std::vector<int64_t> data2;
        for (size_t j = 0; j != 10; ++j) {
            data.emplace_back(j + i * 2);
            data2.emplace_back(j);
        }

        auto input_block = ColumnHelper::create_block<DataTypeInt32>(data);

        input_block.insert(ColumnHelper::create_column_with_name<DataTypeInt64>(data2));

        SpillFileWriterSPtr writer;
        st = spill_file->create_writer(_helper.runtime_state.get(), _helper.operator_profile.get(),
                                       writer);
        ASSERT_TRUE(st.ok()) << "create_writer failed: " << st.to_string();
        st = writer->write_block(_helper.runtime_state.get(), input_block);
        ASSERT_TRUE(st.ok()) << "write_block failed: " << st.to_string();
        st = writer->close();
        ASSERT_TRUE(st.ok()) << "close writer failed: " << st.to_string();

        shared_state->sorted_spill_groups.emplace_back(std::move(spill_file));
    }

    auto query_options = _helper.runtime_state->query_options();
    query_options.spill_sort_mem_limit = 16777216;
    query_options.__isset.spill_sort_mem_limit = true;
    _helper.runtime_state->set_query_options(query_options);

    std::unique_ptr<MutableBlock> mutable_block;
    bool eos = false;
    while (!eos) {
        Block block;
        shared_state->spill_block_batch_row_count = 100;
        st = source_operator->get_block(_helper.runtime_state.get(), &block, &eos);
        ASSERT_TRUE(st.ok()) << "get_block failed: " << st.to_string();
        if (block.empty()) {
            continue;
        }

        if (!mutable_block) {
            mutable_block = MutableBlock::create_unique(std::move(block));
        } else {
            st = mutable_block->merge(std::move(block));
            ASSERT_TRUE(st.ok()) << "merge failed: " << st.to_string();
        }
    }

    ASSERT_TRUE(shared_state->sorted_spill_groups.empty()) << "sorted_spill_groups is not empty";
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

TEST_F(SpillSortSourceOperatorTest, ExecuteMergeSortSpillFilesFastPath) {
    auto [source_operator, sink_operator] = _helper.create_operators();

    SpillSortSourceTestContext context;
    auto st = prepare_spill_sort_source_context(_helper, source_operator, sink_operator, context);
    ASSERT_TRUE(st.ok()) << "prepare spill sort source context failed: " << st.to_string();

    std::vector<SpillFileSPtr> original_files;
    original_files.emplace_back(create_sort_test_spill_file(
            _helper.runtime_state.get(), context.local_state->operator_profile(),
            source_operator->node_id(), "spill_sort_merge_fast_0", {1, 5}, {10, 50}));
    original_files.emplace_back(create_sort_test_spill_file(
            _helper.runtime_state.get(), context.local_state->operator_profile(),
            source_operator->node_id(), "spill_sort_merge_fast_1", {2, 6}, {20, 60}));
    original_files.emplace_back(create_sort_test_spill_file(
            _helper.runtime_state.get(), context.local_state->operator_profile(),
            source_operator->node_id(), "spill_sort_merge_fast_2", {0, 4}, {0, 40}));
    original_files.emplace_back(create_sort_test_spill_file(
            _helper.runtime_state.get(), context.local_state->operator_profile(),
            source_operator->node_id(), "spill_sort_merge_fast_3", {3, 7}, {30, 70}));

    for (const auto& spill_file : original_files) {
        ASSERT_TRUE(spill_file != nullptr);
        context.shared_state->sorted_spill_groups.emplace_back(spill_file);
    }

    st = context.local_state->execute_merge_sort_spill_files(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "execute_merge_sort_spill_files failed: " << st.to_string();
    ASSERT_TRUE(context.shared_state->sorted_spill_groups.empty());
    ASSERT_TRUE(context.local_state->_merger != nullptr);
    ASSERT_EQ(context.local_state->_current_merging_files.size(), 4);
    ASSERT_EQ(context.local_state->_current_merging_readers.size(), 4);

    auto cleanup_files = collect_spill_files_for_cleanup(original_files, context.local_state,
                                                         context.shared_state.get());

    std::unique_ptr<MutableBlock> mutable_block;
    st = read_all_blocks_from_source(_helper.runtime_state.get(), source_operator.get(),
                                     mutable_block);
    ASSERT_TRUE(st.ok()) << "read merged blocks failed: " << st.to_string();
    ASSERT_TRUE(mutable_block != nullptr);
    ASSERT_EQ(mutable_block->rows(), 8);

    auto output_block = mutable_block->to_block();
    const auto& col1 = output_block.get_by_position(0).column;
    for (int i = 0; i < 8; ++i) {
        ASSERT_EQ(col1->get_int(i), i);
    }

    st = context.local_state->close(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "close failed: " << st.to_string();
    ASSERT_TRUE(context.local_state->_current_merging_files.empty());
    ASSERT_TRUE(context.local_state->_current_merging_readers.empty());
    ASSERT_EQ(context.local_state->_merger, nullptr);
    st = source_operator->close(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "source close failed: " << st.to_string();
    st = context.sink_local_state->close(_helper.runtime_state.get(), Status::OK());
    ASSERT_TRUE(st.ok()) << "sink local close failed: " << st.to_string();

    delete_spill_files(cleanup_files);
}

TEST_F(SpillSortSourceOperatorTest, ExecuteMergeSortSpillFilesIntermediateRound) {
    auto [source_operator, sink_operator] = _helper.create_operators();

    SpillSortSourceTestContext context;
    auto st = prepare_spill_sort_source_context(_helper, source_operator, sink_operator, context);
    ASSERT_TRUE(st.ok()) << "prepare spill sort source context failed: " << st.to_string();

    std::vector<SpillFileSPtr> original_files;
    for (int i = 0; i < 9; ++i) {
        auto spill_file = create_sort_test_spill_file(
                _helper.runtime_state.get(), context.local_state->operator_profile(),
                source_operator->node_id(), fmt::format("spill_sort_merge_intermediate_{}", i), {i},
                {100 + i});
        ASSERT_TRUE(spill_file != nullptr);
        original_files.emplace_back(spill_file);
        context.shared_state->sorted_spill_groups.emplace_back(spill_file);
    }

    st = context.local_state->execute_merge_sort_spill_files(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "execute_merge_sort_spill_files failed: " << st.to_string();
    ASSERT_TRUE(context.shared_state->sorted_spill_groups.empty());
    ASSERT_TRUE(context.local_state->_merger != nullptr);
    ASSERT_EQ(context.local_state->_current_merging_files.size(), 2);
    ASSERT_EQ(context.local_state->_current_merging_readers.size(), 2);

    auto cleanup_files = collect_spill_files_for_cleanup(original_files, context.local_state,
                                                         context.shared_state.get());
    ASSERT_GT(cleanup_files.size(), original_files.size());

    std::unique_ptr<MutableBlock> mutable_block;
    st = read_all_blocks_from_source(_helper.runtime_state.get(), source_operator.get(),
                                     mutable_block);
    ASSERT_TRUE(st.ok()) << "read merged blocks failed: " << st.to_string();
    ASSERT_TRUE(mutable_block != nullptr);
    ASSERT_EQ(mutable_block->rows(), 9);

    auto output_block = mutable_block->to_block();
    const auto& col1 = output_block.get_by_position(0).column;
    for (int i = 0; i < 9; ++i) {
        ASSERT_EQ(col1->get_int(i), i);
    }

    st = context.local_state->close(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "close failed: " << st.to_string();
    st = source_operator->close(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "source close failed: " << st.to_string();
    st = context.sink_local_state->close(_helper.runtime_state.get(), Status::OK());
    ASSERT_TRUE(st.ok()) << "sink local close failed: " << st.to_string();

    delete_spill_files(cleanup_files);
}

TEST_F(SpillSortSourceOperatorTest, ExecuteMergeSortSpillFilesRecoverSpillDataError) {
    auto [source_operator, sink_operator] = _helper.create_operators();

    SpillSortSourceTestContext context;
    auto st = prepare_spill_sort_source_context(_helper, source_operator, sink_operator, context);
    ASSERT_TRUE(st.ok()) << "prepare spill sort source context failed: " << st.to_string();

    std::vector<SpillFileSPtr> original_files;
    for (int i = 0; i < 9; ++i) {
        auto spill_file = create_sort_test_spill_file(
                _helper.runtime_state.get(), context.local_state->operator_profile(),
                source_operator->node_id(), fmt::format("spill_sort_merge_recover_error_{}", i),
                {i}, {i});
        ASSERT_TRUE(spill_file != nullptr);
        original_files.emplace_back(spill_file);
        context.shared_state->sorted_spill_groups.emplace_back(spill_file);
    }

    {
        SpillableDebugPointHelper dp_helper("fault_inject::spill_sort_source::recover_spill_data");
        st = context.local_state->execute_merge_sort_spill_files(_helper.runtime_state.get());
    }
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.to_string().find("recover_spill_data failed") != std::string::npos)
            << "unexpected error: " << st.to_string();
    ASSERT_TRUE(context.local_state->_merger != nullptr);
    ASSERT_FALSE(context.local_state->_current_merging_readers.empty());

    auto cleanup_files = collect_spill_files_for_cleanup(original_files, context.local_state,
                                                         context.shared_state.get());

    st = context.local_state->close(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "close failed: " << st.to_string();
    st = source_operator->close(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "source close failed: " << st.to_string();
    st = context.sink_local_state->close(_helper.runtime_state.get(), Status::OK());
    ASSERT_TRUE(st.ok()) << "sink local close failed: " << st.to_string();

    delete_spill_files(cleanup_files);
}

TEST_F(SpillSortSourceOperatorTest, ExecuteMergeSortSpillFilesSpillMergedDataError) {
    auto [source_operator, sink_operator] = _helper.create_operators();

    SpillSortSourceTestContext context;
    auto st = prepare_spill_sort_source_context(_helper, source_operator, sink_operator, context);
    ASSERT_TRUE(st.ok()) << "prepare spill sort source context failed: " << st.to_string();

    std::vector<SpillFileSPtr> original_files;
    for (int i = 0; i < 9; ++i) {
        auto spill_file = create_sort_test_spill_file(
                _helper.runtime_state.get(), context.local_state->operator_profile(),
                source_operator->node_id(), fmt::format("spill_sort_merge_spill_error_{}", i), {i},
                {i});
        ASSERT_TRUE(spill_file != nullptr);
        original_files.emplace_back(spill_file);
        context.shared_state->sorted_spill_groups.emplace_back(spill_file);
    }

    {
        SpillableDebugPointHelper dp_helper("fault_inject::spill_sort_source::spill_merged_data");
        st = context.local_state->execute_merge_sort_spill_files(_helper.runtime_state.get());
    }
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.to_string().find("spill_merged_data failed") != std::string::npos)
            << "unexpected error: " << st.to_string();
    ASSERT_TRUE(context.local_state->_merger != nullptr);
    ASSERT_FALSE(context.local_state->_current_merging_readers.empty());

    auto cleanup_files = collect_spill_files_for_cleanup(original_files, context.local_state,
                                                         context.shared_state.get());

    st = context.local_state->close(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "close failed: " << st.to_string();
    st = source_operator->close(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "source close failed: " << st.to_string();
    st = context.sink_local_state->close(_helper.runtime_state.get(), Status::OK());
    ASSERT_TRUE(st.ok()) << "sink local close failed: " << st.to_string();

    delete_spill_files(cleanup_files);
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

    st = local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

    shared_state->is_spilled = true;

    auto* sorter = shared_state->in_mem_shared_state->sorter.get();

    auto& sort_desc = sorter->get_mutable_sort_description();
    sort_desc.resize(sorter->get_vsort_exec_exprs().ordering_expr_ctxs().size());
    for (int i = 0; i < (int)sort_desc.size(); i++) {
        sort_desc[i].column_number = i;
        sort_desc[i].direction = 1;
        sort_desc[i].nulls_direction = 1;
    }

    // Prepare stored streams
    for (size_t i = 0; i != 4; ++i) {
        SpillFileSPtr spill_file;
        auto relative_path = fmt::format("{}/{}-{}-{}", print_id(_helper.runtime_state->query_id()),
                                         sink_operator->get_name(), sink_operator->node_id(),
                                         ExecEnv::GetInstance()->spill_file_mgr()->next_id());
        st = ExecEnv::GetInstance()->spill_file_mgr()->create_spill_file(relative_path, spill_file);
        ASSERT_TRUE(st.ok()) << "create_spill_file failed: " << st.to_string();

        std::vector<int32_t> data;
        std::vector<int64_t> data2;
        for (size_t j = 0; j != 10; ++j) {
            data.emplace_back(j + i * 2);
            data2.emplace_back(j);
        }

        auto input_block = ColumnHelper::create_block<DataTypeInt32>(data);

        input_block.insert(ColumnHelper::create_column_with_name<DataTypeInt64>(data2));

        SpillFileWriterSPtr writer;
        st = spill_file->create_writer(_helper.runtime_state.get(), _helper.operator_profile.get(),
                                       writer);
        ASSERT_TRUE(st.ok()) << "create_writer failed: " << st.to_string();
        st = writer->write_block(_helper.runtime_state.get(), input_block);
        ASSERT_TRUE(st.ok()) << "write_block failed: " << st.to_string();
        st = writer->close();
        ASSERT_TRUE(st.ok()) << "close writer failed: " << st.to_string();

        shared_state->sorted_spill_groups.emplace_back(std::move(spill_file));
    }

    SpillableDebugPointHelper dp_helper("fault_inject::spill_file::read_next_block");

    std::unique_ptr<MutableBlock> mutable_block;
    bool eos = false;
    while (!eos && st.ok()) {
        Block block;
        shared_state->spill_block_batch_row_count = 100;
        st = source_operator->get_block(_helper.runtime_state.get(), &block, &eos);
        if (!st.ok()) {
            break;
        }

        if (block.empty()) {
            continue;
        }

        if (!mutable_block) {
            mutable_block = MutableBlock::create_unique(std::move(block));
        } else {
            st = mutable_block->merge(std::move(block));
            ASSERT_TRUE(st.ok()) << "merge failed: " << st.to_string();
        }
    }

    ASSERT_FALSE(st.ok());

    st = local_state->close(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "close failed: " << st.to_string();

    st = source_operator->close(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "close failed: " << st.to_string();
}

// Test reading from a single spill file to verify minimal sorted output.
TEST_F(SpillSortSourceOperatorTest, GetBlockWithSingleSpillFile) {
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
    st = local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

    shared_state->is_spilled = true;

    auto* sorter = shared_state->in_mem_shared_state->sorter.get();
    auto& sort_desc = sorter->get_mutable_sort_description();
    sort_desc.resize(sorter->get_vsort_exec_exprs().ordering_expr_ctxs().size());
    for (int i = 0; i < (int)sort_desc.size(); i++) {
        sort_desc[i].column_number = i;
        sort_desc[i].direction = 1;
        sort_desc[i].nulls_direction = 1;
    }

    // Create a single spill file with descending data
    {
        SpillFileSPtr spill_file;
        auto relative_path = fmt::format("{}/{}-{}-{}", print_id(_helper.runtime_state->query_id()),
                                         sink_operator->get_name(), sink_operator->node_id(),
                                         ExecEnv::GetInstance()->spill_file_mgr()->next_id());
        st = ExecEnv::GetInstance()->spill_file_mgr()->create_spill_file(relative_path, spill_file);
        ASSERT_TRUE(st.ok()) << "create_spill_file failed: " << st.to_string();

        auto input_block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});
        input_block.insert(
                ColumnHelper::create_column_with_name<DataTypeInt64>({10, 20, 30, 40, 50}));

        SpillFileWriterSPtr writer;
        st = spill_file->create_writer(_helper.runtime_state.get(), _helper.operator_profile.get(),
                                       writer);
        ASSERT_TRUE(st.ok());
        st = writer->write_block(_helper.runtime_state.get(), input_block);
        ASSERT_TRUE(st.ok());
        st = writer->close();
        ASSERT_TRUE(st.ok());

        shared_state->sorted_spill_groups.emplace_back(std::move(spill_file));
    }

    // Read all blocks from source
    std::unique_ptr<MutableBlock> mutable_block;
    bool eos = false;
    while (!eos) {
        Block block;
        shared_state->spill_block_batch_row_count = 100;
        st = source_operator->get_block(_helper.runtime_state.get(), &block, &eos);
        ASSERT_TRUE(st.ok()) << "get_block failed: " << st.to_string();
        if (block.empty()) {
            continue;
        }
        if (!mutable_block) {
            mutable_block = MutableBlock::create_unique(std::move(block));
        } else {
            st = mutable_block->merge(std::move(block));
            ASSERT_TRUE(st.ok());
        }
    }

    ASSERT_TRUE(eos);
    ASSERT_TRUE(mutable_block) << "mutable_block is null";
    ASSERT_EQ(mutable_block->rows(), 5);

    auto output_block = mutable_block->to_block();
    const auto& col1 = output_block.get_by_position(0).column;

    // Verify sorted order (ascending)
    for (size_t i = 1; i < col1->size(); ++i) {
        ASSERT_GE(col1->get_int(i), col1->get_int(i - 1));
    }

    st = local_state->close(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok());
    st = source_operator->close(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok());
}

// Test full pipeline: sink data → revoke → read back sorted from source.
TEST_F(SpillSortSourceOperatorTest, EndToEndSinkAndSource) {
    auto [source_operator, sink_operator] = _helper.create_operators();

    auto tnode = _helper.create_test_plan_node();
    auto shared_state =
            std::dynamic_pointer_cast<SpillSortSharedState>(sink_operator->create_shared_state());
    ASSERT_TRUE(shared_state != nullptr);

    // Initialize and prepare both operators
    auto st = sink_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok());
    st = sink_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok());

    st = source_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok());
    st = source_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok());

    shared_state->create_source_dependency(sink_operator->operator_id(), sink_operator->node_id(),
                                           "SpillSortSinkOperatorTest");

    // Setup sink local state
    LocalSinkStateInfo sink_info {0, _helper.operator_profile.get(), -1, shared_state.get(), {},
                                  {}};
    st = sink_operator->setup_local_state(_helper.runtime_state.get(), sink_info);
    ASSERT_TRUE(st.ok());

    auto* sink_local_state = reinterpret_cast<SpillSortSinkLocalState*>(
            _helper.runtime_state->get_sink_local_state());
    ASSERT_TRUE(sink_local_state != nullptr);
    st = sink_local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok());

    // Setup source local state
    LocalStateInfo source_info {.parent_profile = _helper.operator_profile.get(),
                                .scan_ranges = {},
                                .shared_state = shared_state.get(),
                                .shared_state_map = {},
                                .task_idx = 0};
    st = source_operator->setup_local_state(_helper.runtime_state.get(), source_info);
    ASSERT_TRUE(st.ok());

    auto* source_local_state = reinterpret_cast<SpillSortLocalState*>(
            _helper.runtime_state->get_local_state(source_operator->operator_id()));
    ASSERT_TRUE(source_local_state != nullptr);
    st = source_local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok());

    // Sink batch 1: {5,3,1,4,2} → revoke
    auto block1 = ColumnHelper::create_block<DataTypeInt32>({5, 3, 1, 4, 2});
    block1.insert(ColumnHelper::create_column_with_name<DataTypeInt64>({50, 30, 10, 40, 20}));
    st = sink_operator->sink(_helper.runtime_state.get(), &block1, false);
    ASSERT_TRUE(st.ok());
    st = sink_operator->revoke_memory(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok());

    // Sink batch 2: {10,8,6,9,7} → revoke
    auto block2 = ColumnHelper::create_block<DataTypeInt32>({10, 8, 6, 9, 7});
    block2.insert(ColumnHelper::create_column_with_name<DataTypeInt64>({100, 80, 60, 90, 70}));
    st = sink_operator->sink(_helper.runtime_state.get(), &block2, false);
    ASSERT_TRUE(st.ok());

    // Sink EOS (triggers final revoke since is_spilled)
    Block empty_block;
    st = sink_operator->sink(_helper.runtime_state.get(), &empty_block, true);
    ASSERT_TRUE(st.ok());

    ASSERT_TRUE(shared_state->is_spilled);
    ASSERT_GE(shared_state->sorted_spill_groups.size(), 2u);

    // Read back from source
    auto* sorter = shared_state->in_mem_shared_state->sorter.get();
    auto& sort_desc = sorter->get_mutable_sort_description();
    sort_desc.resize(sorter->get_vsort_exec_exprs().ordering_expr_ctxs().size());
    for (int i = 0; i < (int)sort_desc.size(); i++) {
        sort_desc[i].column_number = i;
        sort_desc[i].direction = 1;
        sort_desc[i].nulls_direction = 1;
    }

    std::unique_ptr<MutableBlock> mutable_block;
    bool eos = false;
    while (!eos) {
        Block block;
        shared_state->spill_block_batch_row_count = 100;
        st = source_operator->get_block(_helper.runtime_state.get(), &block, &eos);
        ASSERT_TRUE(st.ok()) << "get_block failed: " << st.to_string();
        if (block.empty()) continue;
        if (!mutable_block) {
            mutable_block = MutableBlock::create_unique(std::move(block));
        } else {
            st = mutable_block->merge(std::move(block));
            ASSERT_TRUE(st.ok());
        }
    }

    ASSERT_TRUE(eos);
    ASSERT_TRUE(mutable_block);
    ASSERT_EQ(mutable_block->rows(), 10);

    auto output_block = mutable_block->to_block();
    const auto& col1 = output_block.get_by_position(0).column;

    // Verify sorted order
    for (size_t i = 1; i < col1->size(); ++i) {
        ASSERT_GE(col1->get_int(i), col1->get_int(i - 1))
                << "Not sorted at index " << i << ": " << col1->get_int(i - 1) << " > "
                << col1->get_int(i);
    }

    st = source_local_state->close(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok());
    st = source_operator->close(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok());
}

} // namespace doris
