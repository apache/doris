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

#include "vec/columns/column.h"
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
    auto fn_ctx = FunctionContext::create_context(nullptr, TypeDescriptor {}, {});
    {
        auto status =
                func_concat.open(fn_ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        EXPECT_TRUE(status.ok());
    }
    {
        auto status = func_concat.execute_impl(fn_ctx.get(), block, arguments, 2, 3);
        EXPECT_TRUE(status.ok());
    }

    auto actual_res_col = block.get_by_position(2).column;
    EXPECT_EQ(actual_res_col->size(), 3);
    auto actual_res_col_str = assert_cast<const ColumnString*>(actual_res_col.get());
    actual_res_col_str->sanity_check();
}

TEST(ColumnStringTest, TestStringInsert) {
    {
        auto str32_column = ColumnString::create();
        std::vector<std::string> vals_tmp = {"x", "yy", "zzz", ""};
        auto str32_column_tmp = ColumnString::create();
        for (auto& v : vals_tmp) {
            str32_column_tmp->insert_data(v.data(), v.size());
        }
        str32_column->insert_range_from(*str32_column_tmp, 0, vals_tmp.size());
        str32_column->insert_range_from(*str32_column_tmp, 0, vals_tmp.size());
        auto row_count = str32_column->size();
        EXPECT_EQ(row_count, vals_tmp.size() * 2);
        for (size_t i = 0; i < row_count; ++i) {
            auto row_data = str32_column->get_data_at(i);
            EXPECT_EQ(row_data.to_string(), vals_tmp[i % vals_tmp.size()]);
        }
    }

    {
        // test insert ColumnString64 to ColumnString
        auto str32_column = ColumnString::create();
        std::vector<std::string> vals_tmp = {"x", "yy", "zzz", ""};
        auto str64_column_tmp = ColumnString64::create();
        for (auto& v : vals_tmp) {
            str64_column_tmp->insert_data(v.data(), v.size());
        }
        str32_column->insert_range_from(*str64_column_tmp, 0, vals_tmp.size());
        str32_column->insert_range_from(*str64_column_tmp, 0, vals_tmp.size());
        auto row_count = str32_column->size();
        EXPECT_EQ(row_count, vals_tmp.size() * 2);
        for (size_t i = 0; i < row_count; ++i) {
            auto row_data = str32_column->get_data_at(i);
            EXPECT_EQ(row_data.to_string(), vals_tmp[i % vals_tmp.size()]);
        }
    }
}
} // namespace doris::vectorized