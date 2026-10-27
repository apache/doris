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

#include "exec/operator/group_commit_scan_operator.h"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "exec/pipeline/dependency.h"
#include "testutil/column_helper.h"
#include "testutil/desc_tbl_builder.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"

namespace doris {

class GroupCommitScanOperatorTest : public testing::Test {
protected:
    void SetUp() override {
        DescriptorTblBuilder builder(&_pool);
        builder.declare_tuple() << _int_type << _string_type;
        auto* descs = builder.build();
        ASSERT_NE(descs, nullptr);

        TPlanNode node;
        node.__set_node_id(0);
        node.__set_node_type(TPlanNodeType::GROUP_COMMIT_SCAN_NODE);
        node.__set_row_tuples({0});
        node.__set_limit(-1);
        _op = std::make_unique<GroupCommitOperatorX>(&_pool, node, 0, *descs, 1);

        _state.resize_op_id_to_local_state(-1);
        auto local_state = GroupCommitLocalState::create_unique(&_state, _op.get());
        LocalStateInfo info {&_profile, {}, nullptr, {}, 0};
        // Exercise the real operator and queue without starting GroupCommitMgr or its timer thread.
        ASSERT_TRUE(local_state->PipelineXLocalState<>::init(&_state, info).ok());
        local_state->_get_block_dependency =
                Dependency::create_shared(0, 0, "GroupCommitGetBlockDependency", true);
        std::string label = "group_commit_projection_test";
        _queue = std::make_shared<LoadBlockQueue>(UniqueId(1, 1), label, 1, 1, 1,
                                                  std::make_shared<std::atomic_size_t>(0), false,
                                                  60000, 1024 * 1024);
        // Finish when the queued test blocks are drained, independently of wall-clock timing.
        _queue->_need_commit = true;
        local_state->load_block_queue = _queue;
        _local_state = local_state.get();
        _state.emplace_local_state(0, std::move(local_state));
    }

    void set_mv_projection() {
        _op->set_projection_for_test(
                MockRowDescriptor({_int_type, _string_type, _string_type, _int_type}, &_pool));
        _local_state->_projections = {MockSlotRef::create_mock_context(0, _int_type),
                                      MockSlotRef::create_mock_context(1, _string_type),
                                      MockSlotRef::create_mock_context(1, _string_type),
                                      MockSlotRef::create_mock_context(0, _int_type)};
    }

    Block producer_block(int64_t value, const std::string& text) {
        // Base columns come first. The producer's MV columns have the opposite order from
        // the consumer's projection, and deliberately different values to detect forwarding.
        return Block {
                ColumnHelper::create_nullable_column_with_name<DataTypeInt64>({value, 0}, {0, 1}),
                ColumnHelper::create_nullable_column_with_name<DataTypeString>({text, ""}, {0, 1}),
                ColumnHelper::create_nullable_column_with_name<DataTypeInt64>({999, 999}, {0, 0}),
                ColumnHelper::create_nullable_column_with_name<DataTypeString>({"stale", "stale"},
                                                                               {0, 0})};
    }

    void enqueue(Block block) {
        UniqueId load_id(2, 2);
        auto status = _queue->add_block(&_state, std::make_shared<Block>(std::move(block)), false,
                                        load_id);
        ASSERT_TRUE(status.ok()) << status.to_string();
    }

    void check_projected_block(const Block& block, int64_t value, const std::string& text) {
        ASSERT_EQ(block.columns(), 4);
        ASSERT_EQ(block.rows(), 2);
        ASSERT_TRUE(block.get_by_position(0).type->equals(*_int_type));
        ASSERT_TRUE(block.get_by_position(1).type->equals(*_string_type));
        ASSERT_TRUE(block.get_by_position(2).type->equals(*_string_type));
        ASSERT_TRUE(block.get_by_position(3).type->equals(*_int_type));
        auto expected_int = ColumnHelper::create_nullable_column<DataTypeInt64>({value, 0}, {0, 1});
        auto expected_string =
                ColumnHelper::create_nullable_column<DataTypeString>({text, ""}, {0, 1});
        EXPECT_TRUE(ColumnHelper::column_equal(block.get_by_position(0).column, expected_int));
        EXPECT_TRUE(ColumnHelper::column_equal(block.get_by_position(1).column, expected_string));
        EXPECT_TRUE(ColumnHelper::column_equal(block.get_by_position(2).column, expected_string));
        EXPECT_TRUE(ColumnHelper::column_equal(block.get_by_position(3).column, expected_int));
    }

    ObjectPool _pool;
    RuntimeProfile _profile {"GroupCommitScanOperatorTest"};
    DataTypePtr _int_type = make_nullable(std::make_shared<DataTypeInt64>());
    DataTypePtr _string_type = make_nullable(std::make_shared<DataTypeString>());
    std::unique_ptr<GroupCommitOperatorX> _op;
    MockRuntimeState _state;
    GroupCommitLocalState* _local_state = nullptr;
    std::shared_ptr<LoadBlockQueue> _queue;
};

TEST_F(GroupCommitScanOperatorTest, ProjectsQueuedBlocksAndReusesOutput) {
    set_mv_projection();
    enqueue(producer_block(7, "first"));
    enqueue(producer_block(8, "second"));

    Block output;
    bool eos = false;
    auto status = _op->get_block_after_projects(&_state, &output, &eos);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_FALSE(eos);
    check_projected_block(output, 7, "first");

    output.clear_column_data();
    status = _op->get_block_after_projects(&_state, &output, &eos);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_TRUE(eos);
    check_projected_block(output, 8, "second");
    EXPECT_EQ(_local_state->_rows_returned_counter->value(), 4);

    output.clear_column_data();
    status = _op->get_block_after_projects(&_state, &output, &eos);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_TRUE(eos);
    EXPECT_EQ(output.rows(), 0);
}

TEST_F(GroupCommitScanOperatorTest, AppliesIntermediateProjections) {
    set_mv_projection();
    // CSE can introduce intermediate tuples. The final expressions address that tuple,
    // rather than the original producer block: [text, key] -> [key, text, text, key].
    _local_state->_intermediate_projections = {{MockSlotRef::create_mock_context(1, _string_type),
                                                MockSlotRef::create_mock_context(0, _int_type)}};
    _local_state->_projections = {MockSlotRef::create_mock_context(1, _int_type),
                                  MockSlotRef::create_mock_context(0, _string_type),
                                  MockSlotRef::create_mock_context(0, _string_type),
                                  MockSlotRef::create_mock_context(1, _int_type)};
    enqueue(producer_block(9, "intermediate"));

    Block output;
    bool eos = false;
    auto status = _op->get_block_after_projects(&_state, &output, &eos);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_TRUE(eos);
    check_projected_block(output, 9, "intermediate");
}

TEST_F(GroupCommitScanOperatorTest, ForwardsBlocksWithoutProjection) {
    Block input {
            ColumnHelper::create_nullable_column_with_name<DataTypeInt64>({10}, {0}),
            ColumnHelper::create_nullable_column_with_name<DataTypeString>({"unchanged"}, {0})};
    auto int_column = input.get_by_position(0).column;
    auto string_column = input.get_by_position(1).column;
    enqueue(std::move(input));

    Block output;
    bool eos = false;
    auto status = _op->get_block_after_projects(&_state, &output, &eos);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_TRUE(eos);
    ASSERT_EQ(output.columns(), 2);
    EXPECT_EQ(output.get_by_position(0).column.get(), int_column.get());
    EXPECT_EQ(output.get_by_position(1).column.get(), string_column.get());
    EXPECT_EQ(_local_state->_rows_returned_counter->value(), 1);
}

TEST_F(GroupCommitScanOperatorTest, PropagatesQueueFailure) {
    set_mv_projection();
    _queue->cancel(Status::InternalError("test queue failure"));

    Block output;
    bool eos = false;
    auto status = _op->get_block_after_projects(&_state, &output, &eos);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(output.rows(), 0);
    EXPECT_EQ(_local_state->_rows_returned_counter->value(), 0);
}

} // namespace doris
