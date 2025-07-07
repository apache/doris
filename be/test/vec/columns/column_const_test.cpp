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
#include <testutil/column_helper.h>
#include <vec/columns/column_array.h>
#include <vec/columns/column_const.h>
#include <vec/columns/column_vector.h>
#include <vec/data_types/data_type_array.h>
#include <vec/data_types/data_type_number.h>

#include <vector>

#include "vec/columns/column.h"

namespace doris::vectorized {

TEST(ColumnConstTest, TestCreate) {
    auto column_data = ColumnHelper::create_column<DataTypeInt64>({7});
    auto column_const = ColumnConst::create(column_data, 10);
    auto column_const2 = ColumnConst::create(std::move(column_const), 12);
    EXPECT_EQ(column_const2->size(), 12);

    EXPECT_TRUE(!is_column_const(column_const2->get_data_column()));
}

TEST(ColumnConstTest, TestFilter) {
    {
        auto column_data = ColumnHelper::create_column<DataTypeInt64>({7});
        auto column_const = ColumnConst::create(column_data, 3);
        IColumn::Filter filter = {1, 0, 1};

        auto res = column_const->filter(filter, 2);
        EXPECT_EQ(res->size(), 2);
        EXPECT_EQ(assert_cast<const ColumnConst&>(*res).get_data_column_ptr()->size(), 1);
        EXPECT_EQ(assert_cast<const ColumnConst&>(*res).get_data_column_ptr()->get_int(0), 7);
    }

    {
        auto column_data = ColumnHelper::create_column<DataTypeInt64>({7});
        auto column_const = ColumnConst::create(column_data, 3);
        IColumn::Filter filter = {1, 0, 1};

        auto size = column_const->filter(filter);
        EXPECT_EQ(size, 2);
        EXPECT_EQ(assert_cast<const ColumnConst&>(*column_const).get_data_column_ptr()->size(), 1);
        EXPECT_EQ(assert_cast<const ColumnConst&>(*column_const).get_data_column_ptr()->get_int(0),
                  7);
    }
}

TEST(ColumnConstTest, TestReplicate) {
    auto column_data = ColumnHelper::create_column<DataTypeInt64>({7});
    auto column_const = ColumnConst::create(column_data, 3);
    IColumn::Offsets offsets = {1, 2, 3};
    auto res = column_const->replicate(offsets);
    EXPECT_EQ(res->size(), 3);
    EXPECT_EQ(assert_cast<const ColumnConst&>(*res).get_data_column_ptr()->get_int(0), 7);
}

TEST(ColumnConstTest, TestPermutation) {
    {
        auto column_data = ColumnHelper::create_column<DataTypeInt64>({7});
        auto column_const = ColumnConst::create(column_data, 3);
        IColumn::Permutation perm = {2, 1, 0};
        auto res = column_const->permute(perm, 3);
        EXPECT_EQ(res->size(), 3);
    }

    {
        auto column_data = ColumnHelper::create_column<DataTypeInt64>({7});
        auto column_const = ColumnConst::create(column_data, 3);
        IColumn::Permutation perm = {2, 1, 0};
        auto res = column_const->permute(perm, 0);
        EXPECT_EQ(res->size(), 3);
    }
}

TEST(ColumnConstTest, Testget_permutation) {
    auto column_data = ColumnHelper::create_column<DataTypeInt64>({7});
    auto column_const = ColumnConst::create(column_data, 3);
    IColumn::Permutation res;
    column_const->get_permutation(false, 0, 0, res);
    EXPECT_EQ(res.size(), 3);
    for (size_t i = 0; i < res.size(); ++i) {
        EXPECT_EQ(res[i], i);
    }
}

} // namespace doris::vectorized