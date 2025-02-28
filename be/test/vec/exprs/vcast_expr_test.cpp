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

#include "vec/exprs/vcast_expr.h"

#include <gtest/gtest.h>

#include <memory>

#include "testutil/column_helper.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

class CastExprTest : public testing::Test {
    void create_cast_expr(DataTypePtr from_type, DataTypePtr to_type) {
        cast_expr = std::make_shared<VCastExpr>();
        cast_expr->_data_type = to_type;

        slot_ref = std::make_shared<MockSlotRef>(0, from_type);
        cast_expr->add_child(slot_ref);

        context = std::make_shared<VExprContext>(cast_expr);
        EXPECT_TRUE(cast_expr->prepare(&state, desc, context.get()));

        EXPECT_TRUE(cast_expr->open(&state, context.get(),
                                    FunctionContext::FunctionStateScope::FRAGMENT_LOCAL));
        EXPECT_TRUE(cast_expr->open(&state, context.get(),
                                    FunctionContext::FunctionStateScope::THREAD_LOCAL));
    }

    void exectue_cast_expr(ColumnPtr from_column, ColumnPtr to_column) {
        Block block;
        block.insert({from_column, slot_ref->data_type(), "from column"});
        int result_column_id = 0;
        EXPECT_TRUE(cast_expr->execute(context.get(), &block, &result_column_id).ok());
        EXPECT_TRUE(ColumnHelper::column_equal(block.get_by_position(result_column_id).column,
                                               to_column));
    }

    ObjectPool pool;
    MockRuntimeState state;
    MockRowDescriptor desc {{}, &pool};
    std::shared_ptr<VCastExpr> cast_expr;
    std::shared_ptr<MockSlotRef> slot_ref;
    std::shared_ptr<VExprContext> context;
};

TEST_F(CastExprTest, TestStringToInt) {
    // +---------------+--------------------------------------------------------------+
    // |column(String) |(CAST MockSlotRef(String) TO Nullable(Int32))(Nullable(Int32))|
    // +---------------+--------------------------------------------------------------+
    // |            123|                                                           123|
    // |         123.45|                                                           123|
    // |           12e3|                                                          NULL|
    // |          0.000|                                                             0|
    // +---------------+--------------------------------------------------------------+

    create_cast_expr(std::make_shared<DataTypeString>(),
                     std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()));

    exectue_cast_expr(
            ColumnHelper::create_column<DataTypeString>({"123", "123.45", "12e3", "0.000"}),
            ColumnHelper::create_nullable_column<DataTypeInt32>({123, 123, 0, 0},
                                                                {false, false, true, false}));
}

} // namespace doris::vectorized