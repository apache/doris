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
#include <vector>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/types.h"
#include "exprs/function/function.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/function_context.h"
#include "testutil/function_utils.h"

namespace doris {

static ColumnPtr make_bool_column(const std::vector<UInt8>& values) {
    auto column = ColumnUInt8::create();
    for (auto v : values) {
        column->insert_value(v);
    }
    return column;
}

// Same shape as `IF(NULLIF(b, p), f, b)`: NULLIF is if(b = p, NULL, b) and wraps b's
// column itself as the nested column of its Nullable(Boolean) result. The outer IF treats
// a NULL condition as false; that normalization must not be written into the shared
// nested column, otherwise both the else branch and every other user of b see the
// polluted values.
// Input:
//   b (non-nullable bool): [1, 1]
//   cond = Nullable(nested = b's column, null_map = [0, 1]), logically [true, NULL]
//   f (non-nullable bool): [0, 0]
// Expected IF(cond, f, b): [0, 1]; b (and cond's nested column) must stay [1, 1].
TEST(FunctionIfTest, NullableConditionNotPolluteSharedNestedColumn) {
    auto bool_type = std::make_shared<DataTypeUInt8>();
    auto nullable_bool_type = make_nullable(bool_type);

    ColumnPtr b_column = make_bool_column({1, 1});
    ColumnPtr f_column = make_bool_column({0, 0});
    ColumnPtr cond_column = ColumnNullable::create(b_column, make_bool_column({0, 1}));

    Block block({{cond_column, nullable_bool_type, "cond"},
                 {f_column, bool_type, "f"},
                 {b_column, bool_type, "b"},
                 {nullptr, bool_type, "result"}});

    auto func = SimpleFunctionFactory::instance().get_function(
            "if", {block.get_by_position(0), block.get_by_position(1), block.get_by_position(2)},
            bool_type);
    ASSERT_TRUE(func != nullptr);

    FunctionUtils fn_utils(bool_type, {nullable_bool_type, bool_type, bool_type}, false);
    auto* fn_ctx = fn_utils.get_fn_ctx();
    ASSERT_TRUE(func->open(fn_ctx, FunctionContext::FRAGMENT_LOCAL).ok());
    ASSERT_TRUE(func->open(fn_ctx, FunctionContext::THREAD_LOCAL).ok());
    auto st = func->execute(fn_ctx, block, {0, 1, 2}, 3, 2);
    ASSERT_TRUE(st.ok()) << st.to_string();
    static_cast<void>(func->close(fn_ctx, FunctionContext::THREAD_LOCAL));
    static_cast<void>(func->close(fn_ctx, FunctionContext::FRAGMENT_LOCAL));

    const auto& result_data =
            assert_cast<const ColumnUInt8&>(*block.get_by_position(3).column).get_data();
    ASSERT_EQ(result_data.size(), 2);
    EXPECT_EQ(result_data[0], 0);
    EXPECT_EQ(result_data[1], 1);

    const auto& b_data = assert_cast<const ColumnUInt8&>(*b_column).get_data();
    EXPECT_EQ(b_data[0], 1);
    EXPECT_EQ(b_data[1], 1);
    const auto& cond_nested_data =
            assert_cast<const ColumnUInt8&>(
                    assert_cast<const ColumnNullable&>(*cond_column).get_nested_column())
                    .get_data();
    EXPECT_EQ(cond_nested_data[0], 1);
    EXPECT_EQ(cond_nested_data[1], 1);
}

} // namespace doris
