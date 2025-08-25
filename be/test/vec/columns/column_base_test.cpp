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
#include <vec/columns/column_vector.h>
#include <vec/data_types/data_type_array.h>
#include <vec/data_types/data_type_number.h>

#include <vector>

namespace doris::vectorized {

TEST(ColumnBaseTest, TestCompare_internal) {
    auto column_data1 = ColumnHelper::create_nullable_column<DataTypeInt64>(
            {
                    1,
                    2,
                    3,
            },
            {false, false, false});
    auto column_offset1 = ColumnArray::ColumnOffsets::create();
    column_offset1->insert_value(3);
    auto column_array1 = ColumnArray::create(column_data1, std::move(column_offset1));

    auto column_data2 = ColumnHelper::create_nullable_column<DataTypeInt64>(
            {
                    3,
                    2,
                    1,
            },
            {false, false, false});
    auto column_offset2 = ColumnArray::ColumnOffsets::create();
    column_offset2->insert_value(3);
    auto column_array2 = ColumnArray::create(column_data2, std::move(column_offset2));

    std::vector<uint8_t> cmp_res = {0};
    std::vector<uint8_t> filter(3, 0);
    column_array1->compare_internal(0, *column_array2, 0, 1, cmp_res, filter.data());

    EXPECT_EQ(cmp_res.size(), 1);
    EXPECT_EQ(cmp_res[0], 1);
}

TEST(ColumnBaseTest, TestCompare_internal2) {
    auto column_data1 = ColumnHelper::create_nullable_column<DataTypeInt64>(
            {
                    1,
                    2,
                    3,
            },
            {false, false, false});
    auto column_offset1 = ColumnArray::ColumnOffsets::create();
    column_offset1->insert_value(3);
    auto column_array1 = ColumnArray::create(column_data1, std::move(column_offset1));

    auto column_data2 = ColumnHelper::create_nullable_column<DataTypeInt64>(
            {
                    3,
                    2,
                    1,
            },
            {false, false, false});
    auto column_offset2 = ColumnArray::ColumnOffsets::create();
    column_offset2->insert_value(3);
    auto column_array2 = ColumnArray::create(column_data2, std::move(column_offset2));

    std::vector<uint8_t> cmp_res = {0};
    std::vector<uint8_t> filter(3, 0);
    column_array1->compare_internal(0, *column_array2, 0, -1, cmp_res, filter.data());

    EXPECT_EQ(cmp_res.size(), 1);
    EXPECT_EQ(cmp_res[0], 1);
}
} // namespace doris::vectorized