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

#include <cstdint>
#include <memory>
#include <vector>

#include "common/object_pool.h"
#include "core/data_type/data_type_number.h"
#include "exec/operator/mock_operator.h"
#include "runtime/runtime_profile.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"

namespace doris {

TEST(OperatorProjectionTest, PublishesSharedColumnAndReusesOutputBlock) {
    ObjectPool pool;
    auto data_type = std::make_shared<DataTypeInt32>();
    auto row_descriptor = MockRowDescriptor({data_type}, &pool);

    MockOperatorX op;
    op._row_descriptor = row_descriptor;
    op.set_projection_for_test(MockRowDescriptor(std::vector<DataTypePtr> {data_type}, &pool));

    MockRuntimeState state;
    const auto max_operator_id = op.operator_id() - 1;
    state.resize_op_id_to_local_state(max_operator_id);
    state.set_max_operator_id(max_operator_id);
    RuntimeProfile parent_profile("parent");
    LocalStateInfo info {&parent_profile, {}, nullptr, {}, 0};
    ASSERT_TRUE(op.setup_local_state(&state, info).ok());

    auto* local_state = state.get_local_state(op.operator_id());
    local_state->_projections = MockSlotRef::create_mock_contexts(0, data_type);

    std::vector<int32_t> first_values(1 << 18, 7);
    Block first_origin = ColumnHelper::create_block<DataTypeInt32>(first_values);
    const auto* first_column = first_origin.get_by_position(0).column.get();
    const auto first_allocated_bytes = static_cast<int64_t>(first_origin.allocated_bytes());

    Block output;
    ASSERT_TRUE(op.do_projections(&state, &first_origin, &output).ok());
    EXPECT_EQ(output.get_by_position(0).column.get(), first_column);
    EXPECT_EQ(output.rows(), first_values.size());
    EXPECT_EQ(output.get_by_position(0).column->get_int(0), 7);
    EXPECT_EQ(first_origin.rows(), 0);
    EXPECT_LT(local_state->estimate_memory_usage(), first_allocated_bytes);

    output.clear_column_data();
    Block second_origin = ColumnHelper::create_block<DataTypeInt32>({8, 9});
    const auto* second_column = second_origin.get_by_position(0).column.get();

    ASSERT_TRUE(op.do_projections(&state, &second_origin, &output).ok());
    EXPECT_EQ(output.get_by_position(0).column.get(), second_column);
    EXPECT_EQ(output.rows(), 2);
    EXPECT_EQ(output.get_by_position(0).column->get_int(0), 8);
    EXPECT_EQ(output.get_by_position(0).column->get_int(1), 9);
    EXPECT_EQ(second_origin.rows(), 0);
}

} // namespace doris
