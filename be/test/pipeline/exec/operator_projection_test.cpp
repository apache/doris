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

#include <memory>

#include "pipeline/exec/operator.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_operators.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"

namespace doris::pipeline {

using namespace vectorized;

class OperatorProjectionTest : public ::testing::Test {
public:
    void SetUp() override {
        state = std::make_shared<MockRuntimeState>();
        mock_op = std::make_shared<MockChildOperator>(
                &pool, 0, -1, std::vector<DataTypePtr> {std::make_shared<DataTypeInt32>()});
        mock_op->projections() = MockSlotRef::create_mock_contexts(
                0, std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()));

        auto local_state_uptr = std::make_unique<PipelineXLocalState<>>(state.get(), mock_op.get());
        LocalStateInfo info {.parent_profile = &profile,
                             .scan_ranges = {},
                             .shared_state = nullptr,
                             .shared_state_map = {},
                             .task_idx = 0};
        ASSERT_TRUE(local_state_uptr->init(state.get(), info).ok());
        ASSERT_TRUE(local_state_uptr->open(state.get()).ok());
        state->resize_op_id_to_local_state(-100);
        state->emplace_local_state(mock_op->operator_id(), std::move(local_state_uptr));
    }

protected:
    RuntimeProfile profile {"test"};
    std::shared_ptr<MockRuntimeState> state;
    std::shared_ptr<MockChildOperator> mock_op;
    ObjectPool pool;
};

TEST_F(OperatorProjectionTest, NullableToNonNullableWithoutNullValues) {
    auto input = ColumnHelper::create_nullable_block<DataTypeInt32>({1, 2, 3}, {0, 0, 0});
    Block output;

    auto st = mock_op->do_projections(state.get(), &input, &output);
    ASSERT_TRUE(st.ok()) << st.msg();

    EXPECT_EQ(output.rows(), 3);
    EXPECT_FALSE(output.get_by_position(0).column->is_nullable());
    EXPECT_TRUE(ColumnHelper::block_equal(output,
                                          ColumnHelper::create_block<DataTypeInt32>({1, 2, 3})));
}

TEST_F(OperatorProjectionTest, NullableToNonNullableWithNullValues) {
    auto input = ColumnHelper::create_nullable_block<DataTypeInt32>({1, 2, 3}, {0, 1, 0});

    Block output;
    auto st = mock_op->do_projections(state.get(), &input, &output);
    ASSERT_FALSE(st.ok());
    EXPECT_NE(st.msg().find("src column contains null values"), std::string::npos) << st.msg();
}

} // namespace doris::pipeline
