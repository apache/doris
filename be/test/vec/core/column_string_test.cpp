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

#include "vec/columns/column_string.h"

#include <gtest/gtest.h>

#include "vec/core/block.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/function_string.h"

namespace doris::vectorized {
TEST(ColumnStringTest, TestConcat) {
    Block block;
    vectorized::DataTypePtr str_type = std::make_shared<vectorized::DataTypeString>();

    auto str_col0 = ColumnString::create();
    std::vector<std::string> vals0 = {"aaa", "bb", "cccc"};
    for (auto& v : vals0) {
        str_col0->insert_data(v.data(), v.size());
    }
    block.insert({std::move(str_col0), str_type, "test_str_col0"});

    auto str_col1 = ColumnString::create();
    std::vector<std::string> vals1 = {"3", "2", "4"};
    for (auto& v : vals1) {
        str_col1->insert_data(v.data(), v.size());
    }
    block.insert({std::move(str_col1), str_type, "test_str_col1"});

    auto str_col_res = ColumnString::create();
    block.insert({std::move(str_col_res), str_type, "test_str_res"});

    ColumnNumbers arguments = {0, 1};

    FunctionStringConcat func_concat;
    auto status = func_concat.execute_impl(nullptr, block, arguments, 2, 3);
    EXPECT_TRUE(status.ok());

    auto actual_res_col = block.get_by_position(2).column;
    EXPECT_EQ(actual_res_col->size(), 3);
    auto actual_res_col_str = assert_cast<const ColumnString*>(actual_res_col.get());
    actual_res_col_str->sanity_check();
}
} // namespace doris::vectorized