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

#include "vec/columns/column_array.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_string.h"
#include "vec/columns/columns_number.h"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_struct.h"

namespace doris::vectorized {

class ColumnTest : public ::testing::Test {
protected:
    void SetUp() override {
        col_str = ColumnString::create();
        col_str->insert_data("aaa", 3);
        col_str->insert_data("bb", 2);
        col_str->insert_data("cccc", 4);

        col_int = ColumnInt64::create();
        col_int->insert_value(1);
        col_int->insert_value(2);
        col_int->insert_value(3);

        col_dcm = ColumnDecimal64::create(0, 3);
        col_dcm->insert_value(1.23);
        col_dcm->insert_value(4.56);
        col_dcm->insert_value(7.89);

        col_arr = ColumnArray::create(ColumnInt64::create(), ColumnArray::ColumnOffsets::create());
        Array array1 = {1, 2, 3};
        Array array2 = {4};
        col_arr->insert(array1);
        col_arr->insert(Array());
        col_arr->insert(array2);

        col_map = ColumnMap::create(ColumnString::create(), ColumnInt64::create(),
                                    ColumnArray::ColumnOffsets::create());
        Array k1 = {"a", "b", "c"};
        Array v1 = {1, 2, 3};
        Array k2 = {"d"};
        Array v2 = {4};
        Array a = Array();
        Map map1, map2, map3;
        map1.push_back(k1);
        map1.push_back(v1);
        col_map->insert(map1);
        map3.push_back(a);
        map3.push_back(a);
        col_map->insert(map3);
        map2.push_back(k2);
        map2.push_back(v2);
        col_map->insert(map2);
    }

    ColumnString::MutablePtr col_str;
    ColumnInt64::MutablePtr col_int;
    ColumnDecimal64::MutablePtr col_dcm;
    ColumnArray::MutablePtr col_arr;
    ColumnMap::MutablePtr col_map;
};

TEST_F(ColumnTest, IsExclusiveColumnArray) {
    EXPECT_TRUE(col_arr->is_exclusive());
    auto data_col = assert_cast<const ColumnArray&>(*col_arr).get_data_ptr();
    MutableColumnPtr col = data_col->clone_resized(1);
    std::cout << data_col->use_count() << std::endl;
    std::cout << col->use_count() << std::endl;
    std::cout << col_arr->use_count() << std::endl;
    EXPECT_FALSE(col_arr->is_exclusive());
}

TEST_F(ColumnTest, IsExclusiveColumnMap) {
    EXPECT_TRUE(col_map->is_exclusive());
    auto data_col = assert_cast<const ColumnMap&>(*col_map).get_values_ptr();
    MutableColumnPtr col = data_col->clone_resized(1);
    std::cout << data_col->use_count() << std::endl;
    std::cout << col->use_count() << std::endl;
    std::cout << col_map->use_count() << std::endl;
    EXPECT_FALSE(col_map->is_exclusive());
}

TEST_F(ColumnTest, IsExclusiveColumnStruct) {
    DataTypePtr n1 = std::make_shared<DataTypeInt64>();
    DataTypes dataTypes = {n1};

    DataTypePtr a = std::make_shared<DataTypeStruct>(dataTypes);
    auto col_struct = a->create_column();
    Tuple tuple, tuple1;
    tuple.push_back(1);
    tuple1.push_back(2);
    col_struct->insert(tuple);
    col_struct->insert(tuple1);
    EXPECT_TRUE(col_struct->is_exclusive());
    auto data_col = assert_cast<const ColumnStruct&>(*col_struct).get_column_ptr(0);
    MutableColumnPtr col = data_col->clone_resized(1);
    std::cout << col->use_count() << std::endl;
    std::cout << data_col->use_count() << std::endl;
    std::cout << col_struct->use_count() << std::endl;
    EXPECT_FALSE(col_struct->is_exclusive());
}
} // namespace doris::vectorized
