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

#include "mock_fn_call.h"

#include <gtest/gtest.h>

#include "testutil/column_helper.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

TEST(MockFnCallTest, test) {
    auto fn = MockFnCall::create("test");
    EXPECT_EQ(fn->fn().name.function_name, "test");
    fn->set_const_expr_col(ColumnHelper::create_column<DataTypeInt64>({1}));
    std::shared_ptr<ColumnPtrWrapper> column_wrapper;
    EXPECT_TRUE(fn->get_const_col(nullptr, &column_wrapper));
    EXPECT_EQ(column_wrapper->column_ptr->size(), 1);
    EXPECT_EQ(column_wrapper->column_ptr->get_int(0), 1);
}

} // namespace doris::vectorized
