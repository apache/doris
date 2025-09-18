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
#include <gtest/gtest.h>

#include <cstdint>
#include <iostream>

#include "vec/columns/column_array.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_nullable.h"
#include "vec/core/block.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_struct.h"

namespace doris::vectorized {

class ColumnStructTest : public ::testing::Test {
protected:
    void SetUp() override {}

    void TearDown() override {}
};

TEST_F(ColumnStructTest, StructTypeTesterase) {
    DataTypePtr key_type = (std::make_shared<DataTypeString>());
    DataTypePtr value_type = (std::make_shared<DataTypeInt32>());
    DataTypePtr struct_type = std::make_shared<DataTypeStruct>(DataTypes {key_type, value_type});
    auto column = struct_type->create_column();
    auto* column_struct = assert_cast<ColumnStruct*>(column.get());
    auto column_res = column_struct->clone_empty();
    auto& column_string = assert_cast<ColumnString&>(column_struct->get_column(0));
    auto& column_int = assert_cast<ColumnInt32&>(column_struct->get_column(1));

    std::vector<String> data_string = {"asd", "1234567", "3", "4", "5"};
    std::vector<int32_t> data_int = {1, 2, 3, 4, 5};

    for (auto d : data_string) {
        column_string.insert_data(d.data(), d.size());
    }
    for (auto d : data_int) {
        column_int.insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    // Block tmp;
    // tmp.insert({std::move(column), struct_type, "asd"});
    // std::cout << tmp.dump_data(0, tmp.rows());

    column_struct->erase(0, 2);
    EXPECT_EQ(column_struct->size(), 3);
    for (int i = 0; i < column_struct->size(); ++i) {
        EXPECT_EQ(column_string.get_data_at(i).to_string(), data_string[i + 2]);
        EXPECT_EQ(column_int.get_element(i), data_int[i + 2]);
    }
}

TEST_F(ColumnStructTest, StructTypeTest2erase) {
    DataTypePtr key_type = (std::make_shared<DataTypeString>());
    DataTypePtr value_type = (std::make_shared<DataTypeInt32>());
    DataTypePtr struct_type = std::make_shared<DataTypeStruct>(DataTypes {key_type, value_type});
    auto column = struct_type->create_column();
    auto* column_struct = assert_cast<ColumnStruct*>(column.get());
    auto& column_string = assert_cast<ColumnString&>(column_struct->get_column(0));
    auto& column_int = assert_cast<ColumnInt32&>(column_struct->get_column(1));

    std::vector<String> data_string = {"asd", "1234567", "3", "4", "5"};
    std::vector<int32_t> data_int = {1, 2, 3, 4, 5};

    std::vector<String> data_string_res = {"asd", "1234567", "5"};
    std::vector<int32_t> data_int_res = {1, 2, 5};

    for (auto d : data_string) {
        column_string.insert_data(d.data(), d.size());
    }
    for (auto d : data_int) {
        column_int.insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    // Block tmp;
    // tmp.insert({std::move(column), struct_type, "asd"});
    // std::cout << tmp.dump_data(0, tmp.rows());

    column_struct->erase(2, 2);
    EXPECT_EQ(column_struct->size(), 3);
    for (int i = 0; i < column_struct->size(); ++i) {
        EXPECT_EQ(column_string.get_data_at(i).to_string(), data_string_res[i]);
        EXPECT_EQ(column_int.get_element(i), data_int_res[i]);
    }
}
} // namespace doris::vectorized
