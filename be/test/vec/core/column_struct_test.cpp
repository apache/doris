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

#include "vec/columns/column_struct.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <cstddef>

#include "gtest/gtest_pred_impl.h"
#include "vec/columns/column.h"
#include "vec/columns/column_string.h"
#include "vec/columns/columns_number.h"
#include "vec/core/field.h"

namespace doris::vectorized {
TEST(ColumnStructTest, StringTest) {
    auto str64_column = ColumnString64::create();
    auto i32_column = ColumnInt32::create();

    std::vector<std::string> str_vals = {"aaa", "bbb", "ccc", "ddd"};
    std::vector<int> int_vals = {111, 222, 333, 444};
    for (size_t i = 0; i < str_vals.size(); ++i) {
        str64_column->insert_data(str_vals[i].data(), str_vals[i].size());
        i32_column->insert_data((const char*)(&int_vals[i]), 0);
    }

    std::vector<ColumnPtr> vector_columns;
    vector_columns.emplace_back(str64_column->get_ptr());
    vector_columns.emplace_back(i32_column->get_ptr());
    auto str64_struct_column = ColumnStruct::create(vector_columns);

    auto str32_column = ColumnString::create();
    auto i32_column2 = ColumnInt32::create();
    std::vector<ColumnPtr> vector_columns2;
    vector_columns2.emplace_back(str32_column->get_ptr());
    vector_columns2.emplace_back(i32_column2->get_ptr());
    auto str32_struct_column = ColumnStruct::create(vector_columns2);

    std::vector<uint32_t> indices;
    indices.push_back(0);
    indices.push_back(2);
    indices.push_back(3);
    std::move(*str32_struct_column)
            .mutate()
            ->insert_indices_from(*str64_struct_column, indices.data(),
                                  indices.data() + indices.size());
    EXPECT_EQ(str32_struct_column->size(), indices.size());
    auto t = get<Tuple>(str32_struct_column->operator[](0));
    EXPECT_EQ(t.size(), 2);
    EXPECT_EQ(t[0], "aaa");
    EXPECT_EQ(t[1], 111);

    t = get<Tuple>(str32_struct_column->operator[](1));
    EXPECT_EQ(t.size(), 2);
    EXPECT_EQ(t[0], "ccc");
    EXPECT_EQ(t[1], 333);

    t = get<Tuple>(str32_struct_column->operator[](2));
    EXPECT_EQ(t.size(), 2);
    EXPECT_EQ(t[0], "ddd");
    EXPECT_EQ(t[1], 444);
};
} // namespace doris::vectorized