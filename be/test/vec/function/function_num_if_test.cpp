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
    return assert_cast<const ColumnUInt8*>(col.get())->get_data();
}

TEST(NumIfImplTest, smallTest) {
    auto cond = ColumnHelper::create_column<DataTypeUInt8>({1, 0, 1, 0, 1, 0, 1, 0});
    auto a = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3, 4, 5, 6, 7, 8});
    auto b = ColumnHelper::create_column<DataTypeInt32>({10, 20, 30, 40, 50, 60, 70, 80});
    auto res = NumIfImpl<TYPE_INT>::execute_if(get_cond_data(cond), a, b);
    ColumnHelper::column_equal(
            res, ColumnHelper::create_column<DataTypeInt32>({1, 20, 3, 40, 5, 60, 7, 80}));
}

TEST(NumIfImplTest, largeTest) {
    std::srand(static_cast<unsigned int>(std::time(nullptr)));
    std::vector<uint8_t> cond_data(1024, 0);
    std::vector<int32_t> a_data(1024, 0);
    std::vector<int32_t> b_data(1024, 0);
    for (size_t i = 0; i < 1024; ++i) {
        cond_data[i] = std::rand() % 2;
        a_data[i] = std::rand();
        b_data[i] = std::rand();
    }
    auto cond = ColumnHelper::create_column<DataTypeUInt8>(cond_data);
    auto a = ColumnHelper::create_column<DataTypeInt32>(a_data);
    auto b = ColumnHelper::create_column<DataTypeInt32>(b_data);
    auto res = NumIfImpl<TYPE_INT>::execute_if(get_cond_data(cond), a, b);
    std::vector<int32_t> expected_data(1024, 0);
    for (size_t i = 0; i < 1024; ++i) {
        expected_data[i] = cond_data[i] ? a_data[i] : b_data[i];
    }
    ColumnHelper::column_equal(res, ColumnHelper::create_column<DataTypeInt32>(expected_data));
}

template <typename DataType, bool is_a_const, bool is_b_const>
void test_for_all_const_no_const() {
    using FieldType = DataType::FieldType;
    const int size = 1052;
    std::vector<uint8_t> cond_data(size, 0);
    std::vector<FieldType> a_data(is_a_const ? 1 : size, 0);
    std::vector<FieldType> b_data(is_b_const ? 1 : size, 0);
    for (size_t i = 0; i < size; ++i) {
        cond_data[i] = std::rand() % 2;
        a_data[is_a_const ? 0 : i] = std::rand();
        b_data[is_b_const ? 0 : i] = std::rand();
    }
    auto cond = ColumnHelper::create_column<DataTypeUInt8>(cond_data);
    auto a = ColumnHelper::create_column<DataType>(a_data);
    auto b = ColumnHelper::create_column<DataType>(b_data);

    if (is_a_const) {
        a = ColumnConst::create(a, 1);
    }
    if (is_b_const) {
        b = ColumnConst::create(b, 1);
    }
    auto res = NumIfImpl<DataType::PType>::execute_if(get_cond_data(cond), a, b);
    std::vector<FieldType> expected_data(size, 0);
    for (size_t i = 0; i < size; ++i) {
        expected_data[i] = cond_data[i] ? a_data[is_a_const ? 0 : i] : b_data[is_b_const ? 0 : i];
    }
    ColumnHelper::column_equal(res, ColumnHelper::create_column<DataType>(expected_data));
}

template <typename DataType>
void test_for_all_types() {
    test_for_all_const_no_const<DataType, true, true>();
    test_for_all_const_no_const<DataType, true, false>();
    test_for_all_const_no_const<DataType, false, true>();
    test_for_all_const_no_const<DataType, false, false>();
}

TEST(NumIfImplTest, allTest) {
    test_for_all_types<DataTypeInt8>();
    test_for_all_types<DataTypeInt16>();
    test_for_all_types<DataTypeInt32>();
    test_for_all_types<DataTypeInt64>();
    test_for_all_types<DataTypeInt128>();
    test_for_all_types<DataTypeFloat32>();
    test_for_all_types<DataTypeFloat64>();
}
} // namespace doris::vectorized
