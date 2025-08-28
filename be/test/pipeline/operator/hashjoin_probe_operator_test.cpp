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
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <gmock/gmock-actions.h>
#include <gmock/gmock-function-mocker.h>
#include <gmock/gmock-spec-builders.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cassert>
#include <cstddef>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

#include "hash_join_test_helper.h"
#include "pipeline/dependency.h"
#include "pipeline/exec/hashjoin_build_sink.h"
#include "pipeline/exec/operator.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_operators.h"
#include "testutil/mock/mock_runtime_state.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/field.h"
#include "vec/core/sort_block.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

namespace doris::pipeline {

using namespace vectorized;

class HashJoinProbeOperatorTest : public testing::Test {
public:
    void SetUp() override { _helper.SetUp(); }
    void TearDown() override { _helper.TearDown(); }

    template <typename T>
    void check_column_value(const IColumn& column, const size_t index, const T& value) {
        StringRef data;
        if (column.is_nullable()) {
            const auto& nullable_column = assert_cast<const ColumnNullable&>(column);
            EXPECT_FALSE(nullable_column.is_null_at(index));
            auto nested_column = nullable_column.get_nested_column_ptr();
            data = nested_column->get_data_at(index);
        } else {
            data = column.get_data_at(index);
        }

        if constexpr (std::is_same_v<std::string, T>) {
            EXPECT_EQ(data.to_string(), value);
        } else if constexpr (std::is_same_v<StringRef, T>) {
            EXPECT_EQ(data.to_string(), value.to_string());
        } else {
            EXPECT_EQ(sizeof(T), data.size);
            EXPECT_EQ(memcmp(data.data, &value, sizeof(T)), 0);
        }
    }

    void check_column_values(const IColumn& column, const std::vector<vectorized::Field>& values,
                             std::source_location loc = std::source_location::current()) {
        for (size_t i = 0; i != values.size(); ++i) {
            vectorized::Field value;
            column.get(i, value);
            ASSERT_EQ(value.get_type(), values[i].get_type())
                    << "row: " << i << " type not match at: " << loc.file_name() << ":"
                    << loc.line();
            ASSERT_TRUE(value == values[i])
                    << "row: " << i << " value not match at: " << loc.file_name() << ":"
                    << loc.line();
        }
    }

    Block sort_block_by_columns(Block& block, const std::vector<size_t>& sort_columns = {}) {
        SortDescription sort_description;
        for (auto column : sort_columns) {
            sort_description.emplace_back(column, 1, 1);
        }

        if (sort_description.empty()) {
            for (size_t i = 0; i != block.columns(); ++i) {
                sort_description.emplace_back(i, 1, 1);
            }
        }

        auto sorted_block = block.clone_empty();
        sort_block(block, sorted_block, sort_description);
        return sorted_block;
    }

    struct JoinParams {
        TJoinOp::type join_op_type {TJoinOp::INNER_JOIN};
        bool is_mark_join {false};
        size_t mark_join_conjuncts_size {0};
        bool is_broadcast_join {false};
        bool null_safe_equal {false};
        bool has_other_join_conjuncts {false};
    };

    // NOLINTNEXTLINE(readability-function-*)
    void run_test(const JoinParams& join_params, const std::vector<TPrimitiveType::type>& key_types,
                  const std::vector<bool>& left_keys_nullable,
                  const std::vector<bool>& right_keys_nullable, std::vector<Block>& build_blocks,
                  std::vector<Block>& probe_blocks, Block& output_block) {
        auto tnode = _helper.create_test_plan_node(
                join_params.join_op_type, key_types, left_keys_nullable, right_keys_nullable,
                join_params.is_mark_join, join_params.mark_join_conjuncts_size,
                join_params.null_safe_equal, join_params.has_other_join_conjuncts);

        bool should_build_hash_table = true;
        if (join_params.is_broadcast_join) {
            tnode.hash_join_node.__isset.is_broadcast_join = true;
            tnode.hash_join_node.is_broadcast_join = true;
            should_build_hash_table = true;
        }

        auto [probe_operator, sink_operator] = _helper.create_operators(tnode);
        ASSERT_TRUE(probe_operator);
        ASSERT_TRUE(sink_operator);

        auto st = probe_operator->init(tnode, _helper.runtime_state.get());
        ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();

        st = probe_operator->prepare(_helper.runtime_state.get());
        ASSERT_TRUE(st.ok()) << "prepare failed: " << st.to_string();

        st = sink_operator->init(tnode, _helper.runtime_state.get());
        ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();

        st = sink_operator->prepare(_helper.runtime_state.get());
        ASSERT_TRUE(st.ok()) << "prepare failed: " << st.to_string();

        auto shared_runtime_state = std::make_shared<MockRuntimeState>();

        auto shared_state = sink_operator->create_shared_state();
        if (join_params.is_broadcast_join) {
            shared_state = HashJoinSharedState::create_shared(8);
            shared_state->create_source_dependencies(8, sink_operator->operator_id(),
                                                     sink_operator->node_id(), "HASH_JOIN_PROBE");
            shared_runtime_state->_query_ctx = _helper.runtime_state->_query_ctx;
            shared_runtime_state->_query_id = _helper.runtime_state->_query_id;
            shared_runtime_state->resize_op_id_to_local_state(-100);
            shared_runtime_state->set_max_operator_id(-100);

            LocalSinkStateInfo sink_local_state_info {
                    .task_idx = 1,
                    .parent_profile = _helper.runtime_profile.get(),
                    .sender_id = 0,
                    .shared_state = shared_state.get(),
                    .shared_state_map = {},
                    .tsink = TDataSink(),
            };
            st = sink_operator->setup_local_state(shared_runtime_state.get(),
                                                  sink_local_state_info);
            ASSERT_TRUE(st.ok()) << "setup_local_state failed: " << st.to_string();

            auto* sink_local_state = assert_cast<HashJoinBuildSinkLocalState*>(
                    shared_runtime_state->get_sink_local_state());
            ASSERT_TRUE(sink_local_state);

            st = sink_local_state->open(shared_runtime_state.get());
            ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();
        }

        ASSERT_EQ(probe_operator->is_broadcast_join(), join_params.is_broadcast_join);
        ASSERT_TRUE(shared_state);
        LocalSinkStateInfo sink_local_state_info {
                .task_idx = 0,
                .parent_profile = _helper.runtime_profile.get(),
                .sender_id = 0,
                .shared_state = shared_state.get(),
                .shared_state_map = {},
                .tsink = TDataSink(),
        };

        st = sink_operator->setup_local_state(_helper.runtime_state.get(), sink_local_state_info);
        ASSERT_TRUE(st.ok()) << "setup_local_state failed: " << st.to_string();

        ASSERT_EQ(sink_operator->should_dry_run(_helper.runtime_state.get()),
                  join_params.is_broadcast_join && !should_build_hash_table);

        ASSERT_EQ(sink_operator->require_data_distribution(), false);
        ASSERT_EQ(probe_operator->require_data_distribution(), false);
        ASSERT_FALSE(sink_operator->is_shuffled_operator());
        ASSERT_FALSE(probe_operator->is_shuffled_operator());
        std::cout << "sink distribution: "
                  << get_exchange_type_name(
                             sink_operator->required_data_distribution().distribution_type)
                  << std::endl;
        std::cout << "probe distribution: "
                  << get_exchange_type_name(
                             probe_operator->required_data_distribution().distribution_type)
                  << std::endl;

        LocalStateInfo info {.parent_profile = _helper.runtime_profile.get(),
                             .scan_ranges = {},
                             .shared_state = shared_state.get(),
                             .shared_state_map = {},
                             .task_idx = 0};
        st = probe_operator->setup_local_state(_helper.runtime_state.get(), info);
        ASSERT_TRUE(st.ok()) << "setup_local_state failed: " << st.to_string();

        auto* sink_local_state = assert_cast<HashJoinBuildSinkLocalState*>(
                _helper.runtime_state->get_sink_local_state());
        ASSERT_TRUE(sink_local_state);

        st = sink_local_state->open(_helper.runtime_state.get());
        ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

        auto* probe_local_state =
                _helper.runtime_state->get_local_state(probe_operator->operator_id());
        ASSERT_TRUE(probe_local_state);

        st = probe_local_state->open(_helper.runtime_state.get());
        ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

        std::cout << "probe debug string: " << probe_operator->debug_string(0) << std::endl;

        for (auto& build_block : build_blocks) {
            st = sink_operator->sink(_helper.runtime_state.get(), &build_block, false);
            ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();
        }
        ASSERT_EQ(sink_local_state->build_unique(),
                  tnode.hash_join_node.other_join_conjuncts.empty() &&
                          (tnode.hash_join_node.join_op == TJoinOp::LEFT_ANTI_JOIN ||
                           tnode.hash_join_node.join_op == TJoinOp::LEFT_SEMI_JOIN ||
                           tnode.hash_join_node.join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN));

        Block empty_block;

        if (join_params.is_broadcast_join) {
            st = sink_operator->sink(shared_runtime_state.get(), &empty_block, false);
            ASSERT_FALSE(st.ok());
        }

        st = sink_operator->sink(_helper.runtime_state.get(), &empty_block, true);
        ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();

        st = sink_operator->close(_helper.runtime_state.get(), st);
        ASSERT_TRUE(st.ok()) << "close failed: " << st.to_string();

        if (join_params.is_broadcast_join) {
            st = sink_operator->sink(shared_runtime_state.get(), &empty_block, true);
            ASSERT_TRUE(st.ok()) << "sink failed: " << st.to_string();

            st = sink_operator->close(shared_runtime_state.get(), st);
            ASSERT_TRUE(st.ok()) << "close failed: " << st.to_string();
        }

        auto source_operator =
                std::dynamic_pointer_cast<MockSourceOperator>(probe_operator->child());
        ASSERT_TRUE(source_operator);

        bool eos = false;
        Block tmp_output_block;
        MutableBlock output_block_mutable;
        for (auto& probe_block : probe_blocks) {
            source_operator->set_block(std::move(probe_block));
            st = probe_operator->get_block_after_projects(_helper.runtime_state.get(),
                                                          &tmp_output_block, &eos);
            ASSERT_TRUE(st.ok()) << "get_block failed: " << st.to_string();

            if (tmp_output_block.empty()) {
                continue;
            }

            st = output_block_mutable.merge(std::move(tmp_output_block));
            tmp_output_block.clear();
            ASSERT_TRUE(st.ok()) << "merge failed: " << st.to_string();
        }

        if (eos) {
            return;
        }

        source_operator->set_eos();

        st = probe_operator->get_block(_helper.runtime_state.get(), &tmp_output_block, &eos);
        ASSERT_TRUE(st.ok()) << "get_block failed: " << st.to_string();
        ASSERT_TRUE(eos);

        if (!tmp_output_block.empty()) {
            st = output_block_mutable.merge(std::move(tmp_output_block));
            ASSERT_TRUE(st.ok()) << "merge failed: " << st.to_string();
        }

        output_block = output_block_mutable.to_block();
    }

protected:
    HashJoinTestHelper _helper;
};

TEST_F(HashJoinProbeOperatorTest, InnerJoin) {
    auto sink_block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});
    sink_block.insert(ColumnHelper::create_nullable_column_with_name<DataTypeString>(
            {"a", "b", "c", "d", "e"}, {1, 0, 0, 0, 1}));

    auto probe_block =
            ColumnHelper::create_nullable_block<DataTypeInt32>({1, 2, 3, 4, 5}, {0, 1, 0, 0, 1});
    probe_block.insert(
            ColumnHelper::create_column_with_name<DataTypeString>({"a", "b", "c", "d", "e"}));

    Block output_block;
    std::vector<Block> build_blocks = {sink_block};
    std::vector<Block> probe_blocks = {probe_block};
    run_test({TJoinOp::INNER_JOIN}, {TPrimitiveType::INT, TPrimitiveType::STRING}, {true, false},
             {false, true}, build_blocks, probe_blocks, output_block);

    ASSERT_EQ(output_block.rows(), 2);

    auto sorted_block = sort_block_by_columns(output_block);
    std::cout << "Output block: " << sorted_block.dump_data() << std::endl;

    check_column_values(*sorted_block.get_by_position(0).column,
                        {vectorized::Field::create_field<TYPE_INT>(3),
                         vectorized::Field::create_field<TYPE_INT>(4)});
    check_column_values(*sorted_block.get_by_position(1).column,
                        {vectorized::Field::create_field<TYPE_STRING>("c"),
                         vectorized::Field::create_field<TYPE_STRING>("d")});
    check_column_values(*sorted_block.get_by_position(2).column,
                        {vectorized::Field::create_field<TYPE_INT>(3),
                         vectorized::Field::create_field<TYPE_INT>(4)});
    check_column_values(*sorted_block.get_by_position(3).column,
                        {vectorized::Field::create_field<TYPE_STRING>("c"),
                         vectorized::Field::create_field<TYPE_STRING>("d")});
}

TEST_F(HashJoinProbeOperatorTest, InnerJoinEmptyBuildSide) {
    auto sink_block = ColumnHelper::create_block<DataTypeInt32>({});
    sink_block.insert(ColumnHelper::create_nullable_column_with_name<DataTypeString>({}, {}));

    auto probe_block =
            ColumnHelper::create_nullable_block<DataTypeInt32>({1, 2, 3, 4, 5}, {0, 1, 0, 0, 1});
    probe_block.insert(
            ColumnHelper::create_column_with_name<DataTypeString>({"a", "b", "c", "d", "e"}));

    Block output_block;
    std::vector<Block> build_blocks = {sink_block};
    std::vector<Block> probe_blocks = {probe_block};
    run_test({TJoinOp::INNER_JOIN}, {TPrimitiveType::INT, TPrimitiveType::STRING}, {true, false},
             {false, true}, build_blocks, probe_blocks, output_block);

    ASSERT_EQ(output_block.rows(), 0);
}

TEST_F(HashJoinProbeOperatorTest, InnerJoinEmptyProbeSide) {
    auto sink_block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});
    sink_block.insert(ColumnHelper::create_nullable_column_with_name<DataTypeString>(
            {"a", "b", "c", "d", "e"}, {1, 0, 0, 0, 1}));
    auto probe_block = ColumnHelper::create_nullable_block<DataTypeInt32>({}, {});
    probe_block.insert(ColumnHelper::create_column_with_name<DataTypeString>({}));

    Block output_block;
    std::vector<Block> build_blocks = {sink_block};
    std::vector<Block> probe_blocks = {probe_block};
    run_test({TJoinOp::INNER_JOIN}, {TPrimitiveType::INT, TPrimitiveType::STRING}, {true, false},
             {false, true}, build_blocks, probe_blocks, output_block);

    ASSERT_EQ(output_block.rows(), 0);
}

TEST_F(HashJoinProbeOperatorTest, InnerJoinOtherConjuncts) {
    auto sink_block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});
    sink_block.insert(ColumnHelper::create_nullable_column_with_name<DataTypeString>(
            {"a", "b", "c", "d", "e"}, {0, 0, 0, 0, 1}));
    sink_block.insert(ColumnHelper::create_nullable_column_with_name<DataTypeInt32>(
            {51, 52, 59, 52, 200}, {0, 0, 0, 0, 1}));

    auto probe_block =
            ColumnHelper::create_nullable_block<DataTypeInt32>({1, 2, 3, 4, 5}, {0, 0, 0, 0, 1});
    probe_block.insert(
            ColumnHelper::create_column_with_name<DataTypeString>({"a", "b", "c", "d", "e"}));
    probe_block.insert(ColumnHelper::create_nullable_column_with_name<DataTypeInt32>(
            {101, 100, 102, 99, 200}, {0, 0, 0, 0, 1}));

    Block output_block;
    std::vector<Block> build_blocks = {sink_block};
    std::vector<Block> probe_blocks = {probe_block};
    run_test({.join_op_type = TJoinOp::INNER_JOIN, .has_other_join_conjuncts = true},
             {TPrimitiveType::INT, TPrimitiveType::STRING}, {true, false}, {false, true},
             build_blocks, probe_blocks, output_block);

    ASSERT_EQ(output_block.rows(), 2);

    auto sorted_block = sort_block_by_columns(output_block);
    std::cout << "Output block: " << sorted_block.dump_data() << std::endl;

    check_column_values(*sorted_block.get_by_position(0).column,
                        {vectorized::Field::create_field<TYPE_INT>(1),
                         vectorized::Field::create_field<TYPE_INT>(3)});
    check_column_values(*sorted_block.get_by_position(1).column,
                        {vectorized::Field::create_field<TYPE_STRING>("a"),
                         vectorized::Field::create_field<TYPE_STRING>("c")});
    check_column_values(*sorted_block.get_by_position(2).column,
                        {vectorized::Field::create_field<TYPE_INT>(101),
                         vectorized::Field::create_field<TYPE_INT>(102)});
    check_column_values(*sorted_block.get_by_position(3).column,
                        {vectorized::Field::create_field<TYPE_INT>(1),
                         vectorized::Field::create_field<TYPE_INT>(3)});
    check_column_values(*sorted_block.get_by_position(4).column,
                        {vectorized::Field::create_field<TYPE_STRING>("a"),
                         vectorized::Field::create_field<TYPE_STRING>("c")});
    check_column_values(*sorted_block.get_by_position(5).column,
                        {vectorized::Field::create_field<TYPE_INT>(51),
                         vectorized::Field::create_field<TYPE_INT>(59)});
}

TEST_F(HashJoinProbeOperatorTest, InnerJoinNullSafeEqual) {
    auto sink_block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});
    sink_block.insert(ColumnHelper::create_nullable_column_with_name<DataTypeString>(
            {"a", "b", "c", "d", "e"}, {1, 0, 0, 0, 1}));

    auto probe_block =
            ColumnHelper::create_nullable_block<DataTypeInt32>({1, 2, 3, 4, 5}, {0, 1, 0, 0, 0});
    probe_block.insert(ColumnHelper::create_nullable_column_with_name<DataTypeString>(
            {"a", "b", "c", "d", "e"}, {0, 0, 0, 0, 1}));

    Block output_block;
    std::vector<Block> build_blocks = {sink_block};
    std::vector<Block> probe_blocks = {probe_block};
    run_test({.join_op_type = TJoinOp::INNER_JOIN, .null_safe_equal = true},
             {TPrimitiveType::INT, TPrimitiveType::STRING}, {true, true}, {false, true},
             build_blocks, probe_blocks, output_block);

    ASSERT_EQ(output_block.rows(), 3);

    auto sorted_block = sort_block_by_columns(output_block);
    std::cout << "Output block: " << sorted_block.dump_data() << std::endl;

    check_column_values(*sorted_block.get_by_position(0).column,
                        {vectorized::Field::create_field<TYPE_INT>(3),
                         vectorized::Field::create_field<TYPE_INT>(4),
                         vectorized::Field::create_field<TYPE_INT>(5)});
    check_column_values(*sorted_block.get_by_position(1).column,
                        {vectorized::Field::create_field<TYPE_STRING>("c"),
                         vectorized::Field::create_field<TYPE_STRING>("d"), vectorized::Field()});
    check_column_values(*sorted_block.get_by_position(2).column,
                        {vectorized::Field::create_field<TYPE_INT>(3),
                         vectorized::Field::create_field<TYPE_INT>(4),
                         vectorized::Field::create_field<TYPE_INT>(5)});
    check_column_values(*sorted_block.get_by_position(3).column,
                        {vectorized::Field::create_field<TYPE_STRING>("c"),
                         vectorized::Field::create_field<TYPE_STRING>("d"), vectorized::Field()});
}

TEST_F(HashJoinProbeOperatorTest, CheckSlot) {
    auto tnode = _helper.create_test_plan_node(TJoinOp::INNER_JOIN,
                                               {TPrimitiveType::INT, TPrimitiveType::STRING},
                                               {true, false}, {false, true}, false);

    auto [probe_operator, sink_operator] = _helper.create_operators(tnode);
    ASSERT_TRUE(probe_operator);
    ASSERT_TRUE(sink_operator);

    auto desc_tbl = _helper.runtime_state->desc_tbl();
    if (desc_tbl._slot_desc_map[4]->type()->is_nullable()) {
        desc_tbl._slot_desc_map[4]->_type =
                vectorized::remove_nullable(desc_tbl._slot_desc_map[4]->_type);
    } else {
        desc_tbl._slot_desc_map[4]->_type =
                vectorized::make_nullable(desc_tbl._slot_desc_map[4]->_type);
    }
    _helper.runtime_state->set_desc_tbl(&desc_tbl);

    auto st = probe_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "init failed: " << st.to_string();

    st = probe_operator->prepare(_helper.runtime_state.get());
    ASSERT_FALSE(st.ok());
}

TEST_F(HashJoinProbeOperatorTest, InnerJoinBroadcast) {
    auto sink_block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});
    sink_block.insert(ColumnHelper::create_nullable_column_with_name<DataTypeString>(
            {"a", "b", "c", "d", "e"}, {1, 0, 0, 0, 1}));

    auto probe_block =
            ColumnHelper::create_nullable_block<DataTypeInt32>({1, 2, 3, 4, 5}, {0, 1, 0, 0, 1});
    probe_block.insert(
            ColumnHelper::create_column_with_name<DataTypeString>({"a", "b", "c", "d", "e"}));

    Block output_block;
    std::vector<Block> build_blocks = {sink_block};
    std::vector<Block> probe_blocks = {probe_block};
    run_test({.join_op_type = TJoinOp::INNER_JOIN, .is_broadcast_join = true},
             {TPrimitiveType::INT, TPrimitiveType::STRING}, {true, false}, {false, true},
             build_blocks, probe_blocks, output_block);

    ASSERT_EQ(output_block.rows(), 2);

    auto sorted_block = sort_block_by_columns(output_block);
    std::cout << "Output block: " << sorted_block.dump_data() << std::endl;

    check_column_values(*sorted_block.get_by_position(0).column,
                        {vectorized::Field::create_field<TYPE_INT>(3),
                         vectorized::Field::create_field<TYPE_INT>(4)});
    check_column_values(*sorted_block.get_by_position(1).column,
                        {vectorized::Field::create_field<TYPE_STRING>("c"),
                         vectorized::Field::create_field<TYPE_STRING>("d")});
    check_column_values(*sorted_block.get_by_position(2).column,
                        {vectorized::Field::create_field<TYPE_INT>(3),
                         vectorized::Field::create_field<TYPE_INT>(4)});
    check_column_values(*sorted_block.get_by_position(3).column,
                        {vectorized::Field::create_field<TYPE_STRING>("c"),
                         vectorized::Field::create_field<TYPE_STRING>("d")});
}

TEST_F(HashJoinProbeOperatorTest, FullOuterJoin) {
    auto sink_block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});
    sink_block.insert(ColumnHelper::create_nullable_column_with_name<DataTypeString>(
            {"a", "b", "c", "d", "e"}, {1, 0, 0, 0, 1}));

    auto probe_block =
            ColumnHelper::create_nullable_block<DataTypeInt32>({1, 2, 3, 4, 5}, {0, 1, 0, 0, 1});
    probe_block.insert(
            ColumnHelper::create_column_with_name<DataTypeString>({"a", "b", "c", "d", "e"}));

    Block output_block;
    std::vector<Block> build_blocks = {sink_block};
    std::vector<Block> probe_blocks = {probe_block};
    run_test({TJoinOp::FULL_OUTER_JOIN}, {TPrimitiveType::INT, TPrimitiveType::STRING},
             {true, false}, {false, true}, build_blocks, probe_blocks, output_block);

    auto sorted_block = sort_block_by_columns(output_block);
    std::cout << "Output block: " << sorted_block.dump_data() << std::endl;
    ASSERT_EQ(output_block.rows(), 8);

    check_column_values(
            *sorted_block.get_by_position(0).column,
            {vectorized::Field::create_field<TYPE_INT>(1),
             vectorized::Field::create_field<TYPE_INT>(3),
             vectorized::Field::create_field<TYPE_INT>(4), vectorized::Field(), vectorized::Field(),
             vectorized::Field(), vectorized::Field(), vectorized::Field()});
    check_column_values(*sorted_block.get_by_position(1).column,
                        {vectorized::Field::create_field<TYPE_STRING>("a"),
                         vectorized::Field::create_field<TYPE_STRING>("c"),
                         vectorized::Field::create_field<TYPE_STRING>("d"),
                         vectorized::Field::create_field<TYPE_STRING>("b"),
                         vectorized::Field::create_field<TYPE_STRING>("e"), vectorized::Field(),
                         vectorized::Field(), vectorized::Field()});
    check_column_values(*sorted_block.get_by_position(2).column,
                        {vectorized::Field(), vectorized::Field::create_field<TYPE_INT>(3),
                         vectorized::Field::create_field<TYPE_INT>(4), vectorized::Field(),
                         vectorized::Field(), vectorized::Field::create_field<TYPE_INT>(1),
                         vectorized::Field::create_field<TYPE_INT>(2),
                         vectorized::Field::create_field<TYPE_INT>(5)});
    check_column_values(*sorted_block.get_by_position(3).column,
                        {vectorized::Field(), vectorized::Field::create_field<TYPE_STRING>("c"),
                         vectorized::Field::create_field<TYPE_STRING>("d"), vectorized::Field(),
                         vectorized::Field(), vectorized::Field(),
                         vectorized::Field::create_field<TYPE_STRING>("b"), vectorized::Field()});
}

TEST_F(HashJoinProbeOperatorTest, FullOuterJoinEmptyBuildSide) {
    auto sink_block = ColumnHelper::create_block<DataTypeInt32>({});
    sink_block.insert(ColumnHelper::create_nullable_column_with_name<DataTypeString>({}, {}));

    auto probe_block =
            ColumnHelper::create_nullable_block<DataTypeInt32>({1, 2, 3, 4, 5}, {0, 1, 0, 0, 1});
    probe_block.insert(
            ColumnHelper::create_column_with_name<DataTypeString>({"a", "b", "c", "d", "e"}));

    Block output_block;
    std::vector<Block> build_blocks = {sink_block};
    std::vector<Block> probe_blocks = {probe_block};
    run_test({TJoinOp::FULL_OUTER_JOIN}, {TPrimitiveType::INT, TPrimitiveType::STRING},
             {true, false}, {false, true}, build_blocks, probe_blocks, output_block);

    auto sorted_block = sort_block_by_columns(output_block);
    std::cout << "Output block: " << sorted_block.dump_data() << std::endl;
    ASSERT_EQ(output_block.rows(), 5);
}

TEST_F(HashJoinProbeOperatorTest, FullOuterJoinEmptyProbeSide) {
    auto sink_block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});
    sink_block.insert(ColumnHelper::create_nullable_column_with_name<DataTypeString>(
            {"a", "b", "c", "d", "e"}, {1, 0, 0, 0, 1}));

    auto probe_block = ColumnHelper::create_nullable_block<DataTypeInt32>({}, {});
    probe_block.insert(ColumnHelper::create_column_with_name<DataTypeString>({}));

    Block output_block;
    std::vector<Block> build_blocks = {sink_block};
    std::vector<Block> probe_blocks = {probe_block};
    run_test({TJoinOp::FULL_OUTER_JOIN}, {TPrimitiveType::INT, TPrimitiveType::STRING},
             {true, false}, {false, true}, build_blocks, probe_blocks, output_block);

    auto sorted_block = sort_block_by_columns(output_block);
    std::cout << "Output block: " << sorted_block.dump_data() << std::endl;
    ASSERT_EQ(output_block.rows(), 5);
}

TEST_F(HashJoinProbeOperatorTest, LeftOuterJoin) {
    auto sink_block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});
    sink_block.insert(ColumnHelper::create_nullable_column_with_name<DataTypeString>(
            {"a", "b", "c", "d", "e"}, {1, 0, 0, 0, 1}));

    auto probe_block =
            ColumnHelper::create_nullable_block<DataTypeInt32>({1, 2, 3, 4, 5}, {0, 1, 0, 0, 1});
    probe_block.insert(
            ColumnHelper::create_column_with_name<DataTypeString>({"a", "b", "c", "d", "e"}));

    Block output_block;
    std::vector<Block> build_blocks = {sink_block};
    std::vector<Block> probe_blocks = {probe_block};
    run_test({TJoinOp::LEFT_OUTER_JOIN}, {TPrimitiveType::INT, TPrimitiveType::STRING},
             {true, false}, {false, true}, build_blocks, probe_blocks, output_block);

    auto sorted_block = sort_block_by_columns(output_block);
    std::cout << "Output block: " << sorted_block.dump_data() << std::endl;
    ASSERT_EQ(sorted_block.rows(), 5);
}

TEST_F(HashJoinProbeOperatorTest, LeftOuterJoin2) {
    auto sink_block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});
    sink_block.insert(ColumnHelper::create_nullable_column_with_name<DataTypeString>(
            {"a", "b", "c", "d", "e"}, {1, 0, 0, 0, 1}));

    auto probe_block = ColumnHelper::create_nullable_block<DataTypeInt32>({1, 2, 3, 4, 5, 2, 3},
                                                                          {0, 1, 0, 0, 1, 0, 0});
    probe_block.insert(ColumnHelper::create_column_with_name<DataTypeString>(
            {"a", "b", "c", "d", "e", "b", "c"}));

    Block output_block;
    std::vector<Block> build_blocks = {sink_block};
    std::vector<Block> probe_blocks = {probe_block};
    run_test({TJoinOp::LEFT_OUTER_JOIN}, {TPrimitiveType::INT, TPrimitiveType::STRING},
             {true, false}, {false, true}, build_blocks, probe_blocks, output_block);

    auto sorted_block = sort_block_by_columns(output_block);
    std::cout << "Output block: " << sorted_block.dump_data() << std::endl;
    ASSERT_EQ(sorted_block.rows(), 7);

    check_column_values(*sorted_block.get_by_position(0).column,
                        {vectorized::Field::create_field<TYPE_INT>(1),
                         vectorized::Field::create_field<TYPE_INT>(2),
                         vectorized::Field::create_field<TYPE_INT>(3),
                         vectorized::Field::create_field<TYPE_INT>(3),
                         vectorized::Field::create_field<TYPE_INT>(4), vectorized::Field(),
                         vectorized::Field()});
    check_column_values(*sorted_block.get_by_position(1).column,
                        {vectorized::Field::create_field<TYPE_STRING>("a"),
                         vectorized::Field::create_field<TYPE_STRING>("b"),
                         vectorized::Field::create_field<TYPE_STRING>("c"),
                         vectorized::Field::create_field<TYPE_STRING>("c"),
                         vectorized::Field::create_field<TYPE_STRING>("d"),
                         vectorized::Field::create_field<TYPE_STRING>("b"),
                         vectorized::Field::create_field<TYPE_STRING>("e")});
    check_column_values(*sorted_block.get_by_position(2).column,
                        {vectorized::Field(), vectorized::Field::create_field<TYPE_INT>(2),
                         vectorized::Field::create_field<TYPE_INT>(3),
                         vectorized::Field::create_field<TYPE_INT>(3),
                         vectorized::Field::create_field<TYPE_INT>(4), vectorized::Field(),
                         vectorized::Field()});
    check_column_values(*sorted_block.get_by_position(3).column,
                        {vectorized::Field(), vectorized::Field::create_field<TYPE_STRING>("b"),
                         vectorized::Field::create_field<TYPE_STRING>("c"),
                         vectorized::Field::create_field<TYPE_STRING>("c"),
                         vectorized::Field::create_field<TYPE_STRING>("d"), vectorized::Field(),
                         vectorized::Field()});
}

TEST_F(HashJoinProbeOperatorTest, RightOuterJoin) {
    auto sink_block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});
    sink_block.insert(ColumnHelper::create_nullable_column_with_name<DataTypeString>(
            {"a", "b", "c", "d", "e"}, {1, 0, 0, 0, 1}));

    auto probe_block =
            ColumnHelper::create_nullable_block<DataTypeInt32>({1, 2, 3, 4, 5}, {0, 1, 0, 0, 1});
    probe_block.insert(
            ColumnHelper::create_column_with_name<DataTypeString>({"a", "b", "c", "d", "e"}));

    Block output_block;
    std::vector<Block> build_blocks = {sink_block};
    std::vector<Block> probe_blocks = {probe_block};
    run_test({TJoinOp::RIGHT_OUTER_JOIN}, {TPrimitiveType::INT, TPrimitiveType::STRING},
             {true, false}, {false, true}, build_blocks, probe_blocks, output_block);

    auto sorted_block = sort_block_by_columns(output_block);
    std::cout << "Output block: " << sorted_block.dump_data() << std::endl;
    ASSERT_EQ(sorted_block.rows(), 5);

    check_column_values(*sorted_block.get_by_position(0).column,
                        {vectorized::Field::create_field<TYPE_INT>(3),
                         vectorized::Field::create_field<TYPE_INT>(4), vectorized::Field(),
                         vectorized::Field(), vectorized::Field()});
    check_column_values(*sorted_block.get_by_position(1).column,
                        {vectorized::Field::create_field<TYPE_STRING>("c"),
                         vectorized::Field::create_field<TYPE_STRING>("d"), vectorized::Field(),
                         vectorized::Field(), vectorized::Field()});
    check_column_values(*sorted_block.get_by_position(2).column,
                        {vectorized::Field::create_field<TYPE_INT>(3),
                         vectorized::Field::create_field<TYPE_INT>(4),
                         vectorized::Field::create_field<TYPE_INT>(1),
                         vectorized::Field::create_field<TYPE_INT>(2),
                         vectorized::Field::create_field<TYPE_INT>(5)});
    check_column_values(*sorted_block.get_by_position(3).column,
                        {vectorized::Field::create_field<TYPE_STRING>("c"),
                         vectorized::Field::create_field<TYPE_STRING>("d"), vectorized::Field(),
                         vectorized::Field::create_field<TYPE_STRING>("b"), vectorized::Field()});
}

TEST_F(HashJoinProbeOperatorTest, RightOuterJoinEmptyBuildSide) {
    auto sink_block = ColumnHelper::create_block<DataTypeInt32>({});
    sink_block.insert(ColumnHelper::create_nullable_column_with_name<DataTypeString>({}, {}));

    auto probe_block =
            ColumnHelper::create_nullable_block<DataTypeInt32>({1, 2, 3, 4, 5}, {0, 1, 0, 0, 1});
    probe_block.insert(
            ColumnHelper::create_column_with_name<DataTypeString>({"a", "b", "c", "d", "e"}));

    Block output_block;
    std::vector<Block> build_blocks = {sink_block};
    std::vector<Block> probe_blocks = {probe_block};
    run_test({TJoinOp::RIGHT_OUTER_JOIN}, {TPrimitiveType::INT, TPrimitiveType::STRING},
             {true, false}, {false, true}, build_blocks, probe_blocks, output_block);

    auto sorted_block = sort_block_by_columns(output_block);
    std::cout << "Output block: " << sorted_block.dump_data() << std::endl;
    ASSERT_EQ(sorted_block.rows(), 0);
}

TEST_F(HashJoinProbeOperatorTest, RightOuterJoin2) {
    auto sink_block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});
    sink_block.insert(ColumnHelper::create_nullable_column_with_name<DataTypeString>(
            {"a", "b", "c", "d", "e"}, {1, 0, 0, 0, 1}));

    auto probe_block = ColumnHelper::create_nullable_block<DataTypeInt32>({1, 2, 3, 4, 5, 2, 3},
                                                                          {0, 1, 0, 0, 1, 0, 0});
    probe_block.insert(ColumnHelper::create_column_with_name<DataTypeString>(
            {"a", "b", "c", "d", "e", "b", "c"}));

    Block output_block;
    std::vector<Block> build_blocks = {sink_block};
    std::vector<Block> probe_blocks = {probe_block};
    run_test({TJoinOp::RIGHT_OUTER_JOIN}, {TPrimitiveType::INT, TPrimitiveType::STRING},
             {true, false}, {false, true}, build_blocks, probe_blocks, output_block);

    auto sorted_block = sort_block_by_columns(output_block);
    std::cout << "Output block: " << sorted_block.dump_data() << std::endl;
    ASSERT_EQ(sorted_block.rows(), 6);

    check_column_values(*sorted_block.get_by_position(0).column,
                        {vectorized::Field::create_field<TYPE_INT>(2),
                         vectorized::Field::create_field<TYPE_INT>(3),
                         vectorized::Field::create_field<TYPE_INT>(3),
                         vectorized::Field::create_field<TYPE_INT>(4), vectorized::Field(),
                         vectorized::Field()});
    check_column_values(*sorted_block.get_by_position(1).column,
                        {vectorized::Field::create_field<TYPE_STRING>("b"),
                         vectorized::Field::create_field<TYPE_STRING>("c"),
                         vectorized::Field::create_field<TYPE_STRING>("c"),
                         vectorized::Field::create_field<TYPE_STRING>("d"), vectorized::Field(),
                         vectorized::Field()});
    check_column_values(*sorted_block.get_by_position(2).column,
                        {vectorized::Field::create_field<TYPE_INT>(2),
                         vectorized::Field::create_field<TYPE_INT>(3),
                         vectorized::Field::create_field<TYPE_INT>(3),
                         vectorized::Field::create_field<TYPE_INT>(4),
                         vectorized::Field::create_field<TYPE_INT>(1),
                         vectorized::Field::create_field<TYPE_INT>(5)});
    check_column_values(*sorted_block.get_by_position(3).column,
                        {vectorized::Field::create_field<TYPE_STRING>("b"),
                         vectorized::Field::create_field<TYPE_STRING>("c"),
                         vectorized::Field::create_field<TYPE_STRING>("c"),
                         vectorized::Field::create_field<TYPE_STRING>("d"), vectorized::Field(),
                         vectorized::Field()});
}

TEST_F(HashJoinProbeOperatorTest, LeftAntiJoin) {
    auto sink_block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});
    sink_block.insert(ColumnHelper::create_nullable_column_with_name<DataTypeString>(
            {"a", "b", "c", "d", "e"}, {1, 0, 0, 0, 1}));

    auto probe_block =
            ColumnHelper::create_nullable_block<DataTypeInt32>({1, 2, 3, 4, 5}, {0, 1, 0, 0, 1});
    probe_block.insert(
            ColumnHelper::create_column_with_name<DataTypeString>({"a", "b", "c", "d", "e"}));

    Block output_block;
    std::vector<Block> build_blocks = {sink_block};
    std::vector<Block> probe_blocks = {probe_block};
    run_test({TJoinOp::LEFT_ANTI_JOIN}, {TPrimitiveType::INT, TPrimitiveType::STRING},
             {true, false}, {false, true}, build_blocks, probe_blocks, output_block);

    auto sorted_block = sort_block_by_columns(output_block);
    std::cout << "Output block: " << sorted_block.dump_data() << std::endl;
    ASSERT_EQ(sorted_block.rows(), 3);

    check_column_values(*sorted_block.get_by_position(0).column,
                        {vectorized::Field::create_field<TYPE_INT>(1), vectorized::Field(),
                         vectorized::Field()});
    check_column_values(*sorted_block.get_by_position(1).column,
                        {vectorized::Field::create_field<TYPE_STRING>("a"),
                         vectorized::Field::create_field<TYPE_STRING>("b"),
                         vectorized::Field::create_field<TYPE_STRING>("e")});
}

TEST_F(HashJoinProbeOperatorTest, LeftSemiJoin) {
    auto sink_block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});
    sink_block.insert(ColumnHelper::create_nullable_column_with_name<DataTypeString>(
            {"a", "b", "c", "d", "e"}, {1, 0, 0, 0, 1}));

    auto probe_block =
            ColumnHelper::create_nullable_block<DataTypeInt32>({1, 2, 3, 4, 5}, {0, 1, 0, 0, 1});
    probe_block.insert(
            ColumnHelper::create_column_with_name<DataTypeString>({"a", "b", "c", "d", "e"}));

    Block output_block;
    std::vector<Block> build_blocks = {sink_block};
    std::vector<Block> probe_blocks = {probe_block};
    run_test({TJoinOp::LEFT_SEMI_JOIN}, {TPrimitiveType::INT, TPrimitiveType::STRING},
             {true, false}, {false, true}, build_blocks, probe_blocks, output_block);

    auto sorted_block = sort_block_by_columns(output_block);
    std::cout << "Output block: " << sorted_block.dump_data() << std::endl;
    ASSERT_EQ(sorted_block.rows(), 2);
    check_column_values(*sorted_block.get_by_position(0).column,
                        {vectorized::Field::create_field<TYPE_INT>(3),
                         vectorized::Field::create_field<TYPE_INT>(4)});
    check_column_values(*sorted_block.get_by_position(1).column,
                        {vectorized::Field::create_field<TYPE_STRING>("c"),
                         vectorized::Field::create_field<TYPE_STRING>("d")});
}

TEST_F(HashJoinProbeOperatorTest, LeftSemiJoinEmptyBuildSide) {
    auto sink_block = ColumnHelper::create_block<DataTypeInt32>({});
    sink_block.insert(ColumnHelper::create_nullable_column_with_name<DataTypeString>({}, {}));

    auto probe_block =
            ColumnHelper::create_nullable_block<DataTypeInt32>({1, 2, 3, 4, 5}, {0, 1, 0, 0, 1});
    probe_block.insert(
            ColumnHelper::create_column_with_name<DataTypeString>({"a", "b", "c", "d", "e"}));

    Block output_block;
    std::vector<Block> build_blocks = {sink_block};
    std::vector<Block> probe_blocks = {probe_block};
    run_test({TJoinOp::LEFT_SEMI_JOIN}, {TPrimitiveType::INT, TPrimitiveType::STRING},
             {true, false}, {false, true}, build_blocks, probe_blocks, output_block);

    auto sorted_block = sort_block_by_columns(output_block);
    std::cout << "Output block: " << sorted_block.dump_data() << std::endl;
    ASSERT_EQ(sorted_block.rows(), 0);
}

TEST_F(HashJoinProbeOperatorTest, RightAntiJoin) {
    auto sink_block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});
    sink_block.insert(ColumnHelper::create_nullable_column_with_name<DataTypeString>(
            {"a", "b", "c", "d", "e"}, {1, 0, 0, 0, 1}));

    auto probe_block =
            ColumnHelper::create_nullable_block<DataTypeInt32>({1, 2, 3, 4, 5}, {0, 1, 0, 0, 1});
    probe_block.insert(
            ColumnHelper::create_column_with_name<DataTypeString>({"a", "b", "c", "d", "e"}));

    Block output_block;
    std::vector<Block> build_blocks = {sink_block};
    std::vector<Block> probe_blocks = {probe_block};
    run_test({TJoinOp::RIGHT_ANTI_JOIN}, {TPrimitiveType::INT, TPrimitiveType::STRING},
             {true, false}, {false, true}, build_blocks, probe_blocks, output_block);

    auto sorted_block = sort_block_by_columns(output_block);
    std::cout << "Output block: " << sorted_block.dump_data() << std::endl;
    ASSERT_EQ(sorted_block.rows(), 3);

    check_column_values(*sorted_block.get_by_position(0).column,
                        {vectorized::Field::create_field<TYPE_INT>(1),
                         vectorized::Field::create_field<TYPE_INT>(2),
                         vectorized::Field::create_field<TYPE_INT>(5)});
    check_column_values(*sorted_block.get_by_position(1).column,
                        {vectorized::Field(), vectorized::Field::create_field<TYPE_STRING>("b"),
                         vectorized::Field()});
}

TEST_F(HashJoinProbeOperatorTest, RightSemiJoin) {
    auto sink_block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});
    sink_block.insert(ColumnHelper::create_nullable_column_with_name<DataTypeString>(
            {"a", "b", "c", "d", "e"}, {1, 0, 0, 0, 1}));

    auto probe_block =
            ColumnHelper::create_nullable_block<DataTypeInt32>({1, 2, 3, 4, 5}, {0, 1, 0, 0, 1});
    probe_block.insert(
            ColumnHelper::create_column_with_name<DataTypeString>({"a", "b", "c", "d", "e"}));

    Block output_block;
    std::vector<Block> build_blocks = {sink_block};
    std::vector<Block> probe_blocks = {probe_block};
    run_test({TJoinOp::RIGHT_SEMI_JOIN}, {TPrimitiveType::INT, TPrimitiveType::STRING},
             {true, false}, {false, true}, build_blocks, probe_blocks, output_block);

    auto sorted_block = sort_block_by_columns(output_block);
    std::cout << "Output block: " << sorted_block.dump_data() << std::endl;
    ASSERT_EQ(sorted_block.rows(), 2);

    check_column_values(*sorted_block.get_by_position(0).column,
                        {vectorized::Field::create_field<TYPE_INT>(3),
                         vectorized::Field::create_field<TYPE_INT>(4)});
    check_column_values(*sorted_block.get_by_position(1).column,
                        {vectorized::Field::create_field<TYPE_STRING>("c"),
                         vectorized::Field::create_field<TYPE_STRING>("d")});
}

TEST_F(HashJoinProbeOperatorTest, RightSemiJoinMarkJoin) {
    auto sink_block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});
    sink_block.insert(ColumnHelper::create_nullable_column_with_name<DataTypeString>(
            {"a", "b", "c", "d", "e"}, {1, 0, 0, 0, 1}));

    auto probe_block =
            ColumnHelper::create_nullable_block<DataTypeInt32>({1, 2, 3, 4, 5}, {0, 1, 0, 0, 1});
    probe_block.insert(
            ColumnHelper::create_column_with_name<DataTypeString>({"a", "b", "c", "d", "e"}));

    Block output_block;
    std::vector<Block> build_blocks = {sink_block};
    std::vector<Block> probe_blocks = {probe_block};
    run_test({.join_op_type = TJoinOp::RIGHT_SEMI_JOIN,
              .is_mark_join = true,
              .mark_join_conjuncts_size = 1},
             {TPrimitiveType::INT, TPrimitiveType::STRING}, {true, false}, {false, true},
             build_blocks, probe_blocks, output_block);

    auto sorted_block = sort_block_by_columns(output_block);
    std::cout << "Output block: " << sorted_block.dump_data() << std::endl;
    ASSERT_EQ(sorted_block.rows(), 5);

    check_column_values(*sorted_block.get_by_position(2).column,
                        {vectorized::Field::create_field<TYPE_INT>(1),
                         vectorized::Field::create_field<TYPE_INT>(2),
                         vectorized::Field::create_field<TYPE_INT>(3),
                         vectorized::Field::create_field<TYPE_INT>(4),
                         vectorized::Field::create_field<TYPE_INT>(5)});
    check_column_values(*sorted_block.get_by_position(3).column,
                        {vectorized::Field(), vectorized::Field::create_field<TYPE_STRING>("b"),
                         vectorized::Field::create_field<TYPE_STRING>("c"),
                         vectorized::Field::create_field<TYPE_STRING>("d"), vectorized::Field()});
    check_column_values(*sorted_block.get_by_position(4).column,
                        {vectorized::Field::create_field<TYPE_INT>(0), vectorized::Field(),
                         vectorized::Field::create_field<TYPE_INT>(1),
                         vectorized::Field::create_field<TYPE_INT>(1),
                         vectorized::Field::create_field<TYPE_INT>(0)});
}

TEST_F(HashJoinProbeOperatorTest, NullAwareLeftAntiJoin) {
    auto sink_block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});
    sink_block.insert(ColumnHelper::create_nullable_column_with_name<DataTypeString>(
            {"f", "g", "h", "d", "e"}, {1, 0, 0, 0, 1}));

    auto probe_block =
            ColumnHelper::create_nullable_block<DataTypeInt32>({1, 2, 3, 4, 5}, {0, 1, 0, 0, 1});
    probe_block.insert(
            ColumnHelper::create_column_with_name<DataTypeString>({"a", "b", "c", "d", "e"}));

    Block output_block;
    std::vector<Block> build_blocks = {sink_block};
    std::vector<Block> probe_blocks = {probe_block};
    run_test({TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN}, {TPrimitiveType::INT, TPrimitiveType::STRING},
             {true, false}, {false, true}, build_blocks, probe_blocks, output_block);

    auto sorted_block = sort_block_by_columns(output_block);
    std::cout << "Output block: " << sorted_block.dump_data() << std::endl;
    ASSERT_EQ(sorted_block.rows(), 0);
}

TEST_F(HashJoinProbeOperatorTest, NullAwareLeftAntiJoinEmptyBuildSide) {
    auto sink_block = ColumnHelper::create_block<DataTypeInt32>({});
    sink_block.insert(ColumnHelper::create_nullable_column_with_name<DataTypeString>({}, {}));

    auto probe_block =
            ColumnHelper::create_nullable_block<DataTypeInt32>({1, 2, 3, 4, 5}, {0, 1, 0, 0, 1});
    probe_block.insert(
            ColumnHelper::create_column_with_name<DataTypeString>({"a", "b", "c", "d", "e"}));

    Block output_block;
    std::vector<Block> build_blocks = {sink_block};
    std::vector<Block> probe_blocks = {probe_block};
    run_test({TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN}, {TPrimitiveType::INT, TPrimitiveType::STRING},
             {true, false}, {false, true}, build_blocks, probe_blocks, output_block);

    auto sorted_block = sort_block_by_columns(output_block);
    std::cout << "Output block: " << sorted_block.dump_data() << std::endl;
    ASSERT_EQ(sorted_block.rows(), 5);
}

TEST_F(HashJoinProbeOperatorTest, NullAwareLeftAntiJoinOtherConjuncts) {
    auto sink_block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});
    sink_block.insert(ColumnHelper::create_nullable_column_with_name<DataTypeString>(
            {"a", "b", "c", "d", "e"}, {1, 0, 0, 0, 1}));
    sink_block.insert(ColumnHelper::create_nullable_column_with_name<DataTypeInt32>(
            {51, 52, 59, 52, 200}, {0, 0, 0, 0, 1}));

    auto probe_block =
            ColumnHelper::create_nullable_block<DataTypeInt32>({1, 2, 3, 4, 5}, {0, 1, 0, 0, 1});
    probe_block.insert(
            ColumnHelper::create_column_with_name<DataTypeString>({"a", "b", "c", "d", "e"}));
    probe_block.insert(ColumnHelper::create_nullable_column_with_name<DataTypeInt32>(
            {101, 100, 102, 99, 200}, {0, 0, 0, 0, 1}));

    Block output_block;
    std::vector<Block> build_blocks = {sink_block};
    std::vector<Block> probe_blocks = {probe_block};
    run_test({.join_op_type = TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN, .has_other_join_conjuncts = true},
             {TPrimitiveType::INT, TPrimitiveType::STRING}, {true, false}, {false, true},
             build_blocks, probe_blocks, output_block);

    auto sorted_block = sort_block_by_columns(output_block);
    std::cout << "Output block: " << sorted_block.dump_data() << std::endl;
    ASSERT_EQ(sorted_block.rows(), 0);
}

TEST_F(HashJoinProbeOperatorTest, NullAwareLeftAntiJoin2) {
    auto sink_block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});
    sink_block.insert(ColumnHelper::create_nullable_column_with_name<DataTypeString>(
            {"a", "b", "c", "d", "e"}, {0, 0, 0, 0, 0}));

    auto probe_block =
            ColumnHelper::create_nullable_block<DataTypeInt32>({2, 2, 3, 4, 5}, {0, 1, 0, 0, 1});
    probe_block.insert(
            ColumnHelper::create_column_with_name<DataTypeString>({"a", "b", "c", "d", "e"}));

    Block output_block;
    std::vector<Block> build_blocks = {sink_block};
    std::vector<Block> probe_blocks = {probe_block};
    run_test({TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN}, {TPrimitiveType::INT, TPrimitiveType::STRING},
             {true, false}, {false, true}, build_blocks, probe_blocks, output_block);

    auto sorted_block = sort_block_by_columns(output_block);
    std::cout << "Output block: " << sorted_block.dump_data() << std::endl;
    ASSERT_EQ(sorted_block.rows(), 1);
    check_column_values(*sorted_block.get_by_position(0).column,
                        {vectorized::Field::create_field<TYPE_INT>(2)});
    check_column_values(*sorted_block.get_by_position(1).column,
                        {vectorized::Field::create_field<TYPE_STRING>("a")});
}

TEST_F(HashJoinProbeOperatorTest, NullAwareLeftAntiJoinOtherConjuncts2) {
    auto sink_block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});
    sink_block.insert(ColumnHelper::create_nullable_column_with_name<DataTypeString>(
            {"a", "b", "c", "d", "e"}, {0, 0, 0, 0, 1}));
    sink_block.insert(ColumnHelper::create_nullable_column_with_name<DataTypeInt32>(
            {51, 52, 59, 52, 200}, {0, 0, 0, 0, 1}));

    auto probe_block =
            ColumnHelper::create_nullable_block<DataTypeInt32>({2, 2, 3, 4, 5}, {0, 1, 0, 0, 1});
    probe_block.insert(
            ColumnHelper::create_column_with_name<DataTypeString>({"a", "b", "c", "d", "e"}));
    probe_block.insert(ColumnHelper::create_nullable_column_with_name<DataTypeInt32>(
            {101, 100, 102, 99, 200}, {0, 0, 0, 0, 1}));

    Block output_block;
    std::vector<Block> build_blocks = {sink_block};
    std::vector<Block> probe_blocks = {probe_block};
    run_test({.join_op_type = TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN, .has_other_join_conjuncts = true},
             {TPrimitiveType::INT, TPrimitiveType::STRING}, {true, false}, {false, true},
             build_blocks, probe_blocks, output_block);

    auto sorted_block = sort_block_by_columns(output_block);
    std::cout << "Output block: " << sorted_block.dump_data() << std::endl;
    ASSERT_EQ(sorted_block.rows(), 0);
}

TEST_F(HashJoinProbeOperatorTest, LeftAntiJoin2) {
    auto sink_block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});
    sink_block.insert(ColumnHelper::create_nullable_column_with_name<DataTypeString>(
            {"a", "b", "c", "d", "e"}, {0, 0, 0, 0, 0}));

    auto probe_block =
            ColumnHelper::create_nullable_block<DataTypeInt32>({2, 2, 3, 4, 5}, {0, 1, 0, 0, 1});
    probe_block.insert(
            ColumnHelper::create_column_with_name<DataTypeString>({"a", "b", "c", "d", "e"}));

    Block output_block;
    std::vector<Block> build_blocks = {sink_block};
    std::vector<Block> probe_blocks = {probe_block};
    run_test({TJoinOp::LEFT_ANTI_JOIN}, {TPrimitiveType::INT, TPrimitiveType::STRING},
             {true, false}, {false, true}, build_blocks, probe_blocks, output_block);

    auto sorted_block = sort_block_by_columns(output_block);
    std::cout << "Output block: " << sorted_block.dump_data() << std::endl;
    ASSERT_EQ(sorted_block.rows(), 3);
    check_column_values(*sorted_block.get_by_position(0).column,
                        {vectorized::Field::create_field<TYPE_INT>(2), vectorized::Field(),
                         vectorized::Field()});
    check_column_values(*sorted_block.get_by_position(1).column,
                        {vectorized::Field::create_field<TYPE_STRING>("a"),
                         vectorized::Field::create_field<TYPE_STRING>("b"),
                         vectorized::Field::create_field<TYPE_STRING>("e")});
}

TEST_F(HashJoinProbeOperatorTest, NullAwareLeftAntiJoinMark) {
    auto sink_block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});
    sink_block.insert(ColumnHelper::create_nullable_column_with_name<DataTypeString>(
            {"a", "b", "c", "d", "e"}, {1, 0, 0, 0, 1}));

    auto probe_block =
            ColumnHelper::create_nullable_block<DataTypeInt32>({1, 2, 3, 4, 5}, {0, 1, 0, 0, 1});
    probe_block.insert(
            ColumnHelper::create_column_with_name<DataTypeString>({"a", "b", "c", "d", "e"}));

    Block output_block;
    std::vector<Block> build_blocks = {sink_block};
    std::vector<Block> probe_blocks = {probe_block};
    run_test({TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN, true},
             {TPrimitiveType::INT, TPrimitiveType::STRING}, {true, false}, {false, true},
             build_blocks, probe_blocks, output_block);

    ASSERT_EQ(output_block.rows(), 5);

    Block sorted_block = sort_block_by_columns(output_block);
    std::cout << "Output block: " << sorted_block.dump_data() << std::endl;
    check_column_values(*sorted_block.get_by_position(0).column,
                        {vectorized::Field::create_field<TYPE_INT>(1),
                         vectorized::Field::create_field<TYPE_INT>(3),
                         vectorized::Field::create_field<TYPE_INT>(4), vectorized::Field(),
                         vectorized::Field()});
    check_column_values(*sorted_block.get_by_position(1).column,
                        {vectorized::Field::create_field<TYPE_STRING>("a"),
                         vectorized::Field::create_field<TYPE_STRING>("c"),
                         vectorized::Field::create_field<TYPE_STRING>("d"),
                         vectorized::Field::create_field<TYPE_STRING>("b"),
                         vectorized::Field::create_field<TYPE_STRING>("e")});
    check_column_values(*sorted_block.get_by_position(2).column,
                        {vectorized::Field(), vectorized::Field::create_field<TYPE_INT>(0),
                         vectorized::Field::create_field<TYPE_INT>(0), vectorized::Field(),
                         vectorized::Field()});
}

TEST_F(HashJoinProbeOperatorTest, NullAwareLeftSemiJoinMark) {
    auto sink_block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});
    sink_block.insert(ColumnHelper::create_nullable_column_with_name<DataTypeString>(
            {"a", "b", "c", "d", "e"}, {1, 0, 0, 0, 1}));

    auto probe_block =
            ColumnHelper::create_nullable_block<DataTypeInt32>({1, 2, 3, 4, 5}, {0, 1, 0, 0, 1});
    probe_block.insert(
            ColumnHelper::create_column_with_name<DataTypeString>({"a", "b", "c", "d", "e"}));

    Block output_block;
    std::vector<Block> build_blocks = {sink_block};
    std::vector<Block> probe_blocks = {probe_block};
    run_test({TJoinOp::NULL_AWARE_LEFT_SEMI_JOIN, true},
             {TPrimitiveType::INT, TPrimitiveType::STRING}, {true, false}, {false, true},
             build_blocks, probe_blocks, output_block);

    ASSERT_EQ(output_block.rows(), 5);

    Block sorted_block = sort_block_by_columns(output_block);
    std::cout << "Output block: " << sorted_block.dump_data() << std::endl;

    check_column_values(*sorted_block.get_by_position(0).column,
                        {vectorized::Field::create_field<TYPE_INT>(1),
                         vectorized::Field::create_field<TYPE_INT>(3),
                         vectorized::Field::create_field<TYPE_INT>(4), vectorized::Field(),
                         vectorized::Field()});
    check_column_values(*sorted_block.get_by_position(1).column,
                        {vectorized::Field::create_field<TYPE_STRING>("a"),
                         vectorized::Field::create_field<TYPE_STRING>("c"),
                         vectorized::Field::create_field<TYPE_STRING>("d"),
                         vectorized::Field::create_field<TYPE_STRING>("b"),
                         vectorized::Field::create_field<TYPE_STRING>("e")});
    check_column_values(*sorted_block.get_by_position(2).column,
                        {vectorized::Field(), vectorized::Field::create_field<TYPE_INT>(1),
                         vectorized::Field::create_field<TYPE_INT>(1), vectorized::Field(),
                         vectorized::Field()});
}

TEST_F(HashJoinProbeOperatorTest, LeftSemiJoinMark) {
    auto sink_block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});
    sink_block.insert(ColumnHelper::create_nullable_column_with_name<DataTypeString>(
            {"a", "b", "c", "d", "e"}, {1, 0, 0, 0, 1}));

    auto probe_block =
            ColumnHelper::create_nullable_block<DataTypeInt32>({1, 2, 3, 4, 5}, {0, 1, 0, 0, 1});
    probe_block.insert(
            ColumnHelper::create_column_with_name<DataTypeString>({"a", "b", "c", "d", "e"}));

    Block output_block;
    std::vector<Block> build_blocks = {sink_block};
    std::vector<Block> probe_blocks = {probe_block};
    run_test({TJoinOp::LEFT_SEMI_JOIN, true, 1}, {TPrimitiveType::INT, TPrimitiveType::STRING},
             {true, false}, {false, true}, build_blocks, probe_blocks, output_block);

    ASSERT_EQ(output_block.rows(), 5);

    Block sorted_block = sort_block_by_columns(output_block);
    std::cout << "Output block: " << sorted_block.dump_data(0, 100, true) << std::endl;

    check_column_values(*sorted_block.get_by_position(0).column,
                        {vectorized::Field::create_field<TYPE_INT>(1),
                         vectorized::Field::create_field<TYPE_INT>(3),
                         vectorized::Field::create_field<TYPE_INT>(4), vectorized::Field(),
                         vectorized::Field()});
    check_column_values(*sorted_block.get_by_position(1).column,
                        {vectorized::Field::create_field<TYPE_STRING>("a"),
                         vectorized::Field::create_field<TYPE_STRING>("c"),
                         vectorized::Field::create_field<TYPE_STRING>("d"),
                         vectorized::Field::create_field<TYPE_STRING>("b"),
                         vectorized::Field::create_field<TYPE_STRING>("e")});
    check_column_values(*sorted_block.get_by_position(2).column,
                        {vectorized::Field::create_field<TYPE_INT>(0),
                         vectorized::Field::create_field<TYPE_INT>(1),
                         vectorized::Field::create_field<TYPE_INT>(1), vectorized::Field(),
                         vectorized::Field::create_field<TYPE_INT>(0)});
}

TEST_F(HashJoinProbeOperatorTest, LeftAntiJoinMark) {
    auto sink_block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});
    sink_block.insert(ColumnHelper::create_nullable_column_with_name<DataTypeString>(
            {"a", "b", "c", "d", "e"}, {1, 0, 0, 0, 1}));

    auto probe_block =
            ColumnHelper::create_nullable_block<DataTypeInt32>({1, 2, 3, 4, 5}, {0, 1, 0, 0, 1});
    probe_block.insert(
            ColumnHelper::create_column_with_name<DataTypeString>({"a", "b", "c", "d", "e"}));

    Block output_block;
    std::vector<Block> build_blocks = {sink_block};
    std::vector<Block> probe_blocks = {probe_block};
    run_test({TJoinOp::LEFT_ANTI_JOIN, true, 1}, {TPrimitiveType::INT, TPrimitiveType::STRING},
             {true, false}, {false, true}, build_blocks, probe_blocks, output_block);

    ASSERT_EQ(output_block.rows(), 5);

    Block sorted_block = sort_block_by_columns(output_block);
    std::cout << "Output block: " << sorted_block.dump_data(0, 100, true) << std::endl;

    check_column_values(*sorted_block.get_by_position(0).column,
                        {vectorized::Field::create_field<TYPE_INT>(1),
                         vectorized::Field::create_field<TYPE_INT>(3),
                         vectorized::Field::create_field<TYPE_INT>(4), vectorized::Field(),
                         vectorized::Field()});
    check_column_values(*sorted_block.get_by_position(1).column,
                        {vectorized::Field::create_field<TYPE_STRING>("a"),
                         vectorized::Field::create_field<TYPE_STRING>("c"),
                         vectorized::Field::create_field<TYPE_STRING>("d"),
                         vectorized::Field::create_field<TYPE_STRING>("b"),
                         vectorized::Field::create_field<TYPE_STRING>("e")});
    check_column_values(*sorted_block.get_by_position(2).column,
                        {vectorized::Field::create_field<TYPE_INT>(1),
                         vectorized::Field::create_field<TYPE_INT>(0),
                         vectorized::Field::create_field<TYPE_INT>(0), vectorized::Field(),
                         vectorized::Field::create_field<TYPE_INT>(1)});
}

TEST_F(HashJoinProbeOperatorTest, LeftAntiJoinMarkOtherConjuncts) {
    auto sink_block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});
    sink_block.insert(ColumnHelper::create_nullable_column_with_name<DataTypeString>(
            {"a", "b", "c", "d", "e"}, {0, 0, 0, 0, 1}));
    sink_block.insert(ColumnHelper::create_nullable_column_with_name<DataTypeInt32>(
            {51, 52, 59, 52, 200}, {0, 0, 0, 0, 1}));

    auto probe_block =
            ColumnHelper::create_nullable_block<DataTypeInt32>({1, 2, 3, 4, 5}, {0, 0, 0, 0, 1});
    probe_block.insert(
            ColumnHelper::create_column_with_name<DataTypeString>({"a", "b", "c", "d", "e"}));
    probe_block.insert(ColumnHelper::create_nullable_column_with_name<DataTypeInt32>(
            {101, 100, 102, 99, 200}, {0, 0, 0, 0, 1}));

    Block output_block;
    std::vector<Block> build_blocks = {sink_block};
    std::vector<Block> probe_blocks = {probe_block};
    run_test({.join_op_type = TJoinOp::LEFT_ANTI_JOIN,
              .is_mark_join = true,
              .mark_join_conjuncts_size = 1,
              .has_other_join_conjuncts = true},
             {TPrimitiveType::INT, TPrimitiveType::STRING}, {true, false}, {false, true},
             build_blocks, probe_blocks, output_block);

    ASSERT_EQ(output_block.rows(), 5);

    Block sorted_block = sort_block_by_columns(output_block);
    std::cout << "Output block: " << sorted_block.dump_data(0, 100, true) << std::endl;

    check_column_values(*sorted_block.get_by_position(0).column,
                        {vectorized::Field::create_field<TYPE_INT>(1),
                         vectorized::Field::create_field<TYPE_INT>(2),
                         vectorized::Field::create_field<TYPE_INT>(3),
                         vectorized::Field::create_field<TYPE_INT>(4), vectorized::Field()});
    check_column_values(*sorted_block.get_by_position(1).column,
                        {vectorized::Field::create_field<TYPE_STRING>("a"),
                         vectorized::Field::create_field<TYPE_STRING>("b"),
                         vectorized::Field::create_field<TYPE_STRING>("c"),
                         vectorized::Field::create_field<TYPE_STRING>("d"),
                         vectorized::Field::create_field<TYPE_STRING>("e")});
    check_column_values(*sorted_block.get_by_position(2).column,
                        {vectorized::Field::create_field<TYPE_INT>(101),
                         vectorized::Field::create_field<TYPE_INT>(100),
                         vectorized::Field::create_field<TYPE_INT>(102),
                         vectorized::Field::create_field<TYPE_INT>(99), vectorized::Field()});
    check_column_values(*sorted_block.get_by_position(3).column,
                        {vectorized::Field::create_field<TYPE_INT>(0),
                         vectorized::Field::create_field<TYPE_INT>(1),
                         vectorized::Field::create_field<TYPE_INT>(0),
                         vectorized::Field::create_field<TYPE_INT>(1),
                         vectorized::Field::create_field<TYPE_INT>(1)});
}

} // namespace doris::pipeline