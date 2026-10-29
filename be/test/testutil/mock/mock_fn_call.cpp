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

#include "testutil/mock/mock_fn_call.h"

#include <gtest/gtest.h>

#include "core/column/column_const.h"
#include "core/data_type/data_type_number.h"
#include "testutil/column_helper.h"

namespace doris {

TEST(MockFnCallTest, test) {
    auto fn = MockFnCall::create("test");
    EXPECT_EQ(fn->fn().name.function_name, "test");
    fn->set_const_expr_col(ColumnHelper::create_column<DataTypeInt64>({1}));
    std::shared_ptr<ConstantExprResult> column_wrapper;
    EXPECT_TRUE(fn->get_const_col(nullptr, &column_wrapper));
    EXPECT_EQ(column_wrapper->column().size(), 1);
    EXPECT_EQ(column_wrapper->column().get_int(0), 1);
}

TEST(MockFnCallTest, BroadcastCachedConstant) {
    auto fn = MockFnCall::create("test");
    auto data = ColumnHelper::create_column<DataTypeInt64>({7});
    fn->_constant_col = std::make_shared<ConstantExprResult>(ColumnConst::create(data, 3));
    for (size_t count : {0, 1, 5}) {
        auto result = fn->get_result_from_const(count);
        const auto* broadcast = check_and_get_column<ColumnConst>(result.get());
        ASSERT_NE(broadcast, nullptr);
        EXPECT_EQ(broadcast->size(), count);
        EXPECT_EQ(broadcast->get_data_column_ptr().get(), data.get());
        EXPECT_EQ(broadcast->get_data_column().get_int(0), 7);
    }
}

} // namespace doris
