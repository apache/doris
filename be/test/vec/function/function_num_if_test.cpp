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

#include "testutil/column_helper.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/if.h"

namespace doris::vectorized {

auto& get_cond_data(ColumnPtr col) {
    return assert_cast<const ColumnVector<UInt8>*>(col.get())->get_data();
}

TEST(NumIfImplTest, smallTest) {
    auto cond = ColumnHelper::create_column<DataTypeUInt8>({1, 0, 1, 0, 1, 0, 1, 0});
    auto a = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3, 4, 5, 6, 7, 8});
    auto b = ColumnHelper::create_column<DataTypeInt32>({10, 20, 30, 40, 50, 60, 70, 80});
    auto res = NumIfImpl<DataTypeInt32::FieldType>::execute_if(get_cond_data(cond), a, b);
    ColumnHelper::column_equal(
            res, ColumnHelper::create_column<DataTypeInt32>({1, 20, 3, 40, 5, 60, 7, 80}));
}

TEST(NumIfImplTest, largeTest) {
    std::vector<uint8_t> cond_data(1024, 0);
    std::vector<int32_t> a_data(1024, 0);
    std::vector<int32_t> b_data(1024, 0);
    for (size_t i = 0; i < 1024; ++i) {
        cond_data[i] = i % 2;
        a_data[i] = i;
        b_data[i] = i + 1;
    }

    auto cond = ColumnHelper::create_column<DataTypeUInt8>(cond_data);
    auto a = ColumnHelper::create_column<DataTypeInt32>(a_data);
    auto b = ColumnHelper::create_column<DataTypeInt32>(b_data);
    auto res = NumIfImpl<DataTypeInt32::FieldType>::execute_if(get_cond_data(cond), a, b);
    std::vector<int32_t> expected_data(1024, 0);
    for (size_t i = 0; i < 1024; ++i) {
        expected_data[i] = i % 2 == 0 ? i : i + 1;
    }
    ColumnHelper::column_equal(res, ColumnHelper::create_column<DataTypeInt32>(expected_data));
}

} // namespace doris::vectorized
