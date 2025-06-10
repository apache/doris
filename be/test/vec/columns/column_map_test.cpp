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

#include "vec/columns/column_map.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <iostream>

#include "vec/columns/column_array.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_struct.h"
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

class ColumnMapTest : public ::testing::Test {
protected:
    void SetUp() override {}

    void TearDown() override {}
};

TEST_F(ColumnMapTest, MapTypeTesterase) {
    DataTypePtr key_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    DataTypePtr value_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>());
    DataTypePtr map_type = std::make_shared<DataTypeMap>(key_type, value_type);
    auto column = map_type->create_column();
    auto* column_map = assert_cast<ColumnMap*>(column.get());
    auto column_res = column_map->clone_empty();
    auto& column_offsets = column_map->get_offsets_column();
    auto& column_data_key = column_map->get_keys();
    auto& column_data_value = column_map->get_values();

    std::vector<StringRef> k1 = {StringRef("kasd"), StringRef("k1234567"), StringRef("k3"),
                                 StringRef("k4"), StringRef("k5")};
    std::vector<int64_t> v1 = {1, 2, 3, 4, 5};
    std::vector<String> k2 = {"k11", "k22"};
    std::vector<int64_t> v2 = {33, 44};
    std::vector<String> k4 = {"k66"};
    std::vector<int64_t> v4 = {66};
    std::vector<UInt64> offset = {5, 7, 8, 9};
    for (auto d : k1) {
        column_data_key.insert_data(d.data, d.size);
    }
    for (auto d : v1) {
        column_data_value.insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    for (auto d : k2) {
        column_data_key.insert_data(d.data(), d.size());
    }
    for (auto d : v2) {
        column_data_value.insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    column_data_key.insert_default();
    column_data_value.insert_default();
    for (auto d : k4) {
        column_data_key.insert_data(d.data(), d.size());
    }
    for (auto d : v4) {
        column_data_value.insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    for (auto d : offset) {
        column_offsets.insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    column_res->insert_range_from(*column_map, 0, offset.size());
    column_map->erase(0, 2);

    // Block tmp;
    // tmp.insert({std::move(column), map_type, "asd"});
    // std::cout << tmp.dump_data(0, tmp.rows());
    // Block tmp2;
    // tmp2.insert({std::move(column_res), map_type, "asd2"});
    // std::cout << tmp2.dump_data(0, tmp2.rows());

    EXPECT_EQ(column_map->size(), 2);
    auto* column_result = assert_cast<ColumnMap*>(column_res.get());
    auto& column_offsets_res = column_result->get_offsets_column();
    auto& offset_data_res = assert_cast<ColumnOffset64&>(column_offsets_res);
    auto& offset_data = assert_cast<ColumnOffset64&>(column_offsets);

    auto& column_data_res = assert_cast<ColumnInt64&>(
            assert_cast<ColumnNullable&>(assert_cast<ColumnMap&>(*column_res).get_values())
                    .get_nested_column());
    auto& column_data_origin = assert_cast<ColumnInt64&>(
            assert_cast<ColumnNullable&>(column_data_value).get_nested_column());

    auto& column_data_res_key = assert_cast<ColumnString&>(
            assert_cast<ColumnNullable&>(assert_cast<ColumnMap&>(*column_res).get_keys())
                    .get_nested_column());
    auto& column_data_origin_key = assert_cast<ColumnString&>(
            assert_cast<ColumnNullable&>(column_data_key).get_nested_column());

    for (int i = 0; i < column_map->size(); ++i) {
        std::cout << map_type->to_string(*column_map, i) << std::endl;
        std::cout << map_type->to_string(*column_res, i + 2) << std::endl;
        EXPECT_EQ(column_data_origin.get_element(i), column_data_res.get_element(i + 7));
        EXPECT_EQ(column_data_origin_key.get_data_at(i), column_data_res_key.get_data_at(i + 7));
        EXPECT_EQ(offset_data.get_element(i), offset_data_res.get_element(i + 2) - 7);
    }
}

TEST_F(ColumnMapTest, MapTypeTest2erase) {
    DataTypePtr key_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    DataTypePtr value_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>());
    DataTypePtr map_type = std::make_shared<DataTypeMap>(key_type, value_type);
    auto column = map_type->create_column();
    auto* column_map = assert_cast<ColumnMap*>(column.get());
    auto column_res = column_map->clone_empty();
    auto& column_offsets = column_map->get_offsets_column();
    auto& column_data_key = column_map->get_keys();
    auto& column_data_value = column_map->get_values();

    std::vector<StringRef> k1 = {StringRef("kasd"), StringRef("k1234567"), StringRef("k3"),
                                 StringRef("k4"), StringRef("k5")};
    std::vector<int64_t> v1 = {1, 2, 3, 4, 5};
    std::vector<String> k2 = {"k11", "k22"};
    std::vector<int64_t> v2 = {33, 44};
    std::vector<String> k4 = {"k66"};
    std::vector<int64_t> v4 = {66};
    std::vector<String> k5 = {"k77", "k88", "k99"};
    std::vector<int64_t> v5 = {77, 88, 99};
    std::vector<UInt64> offset = {5, 7, 8, 9, 12};
    for (auto d : k1) {
        column_data_key.insert_data(d.data, d.size);
    }
    for (auto d : v1) {
        column_data_value.insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    for (auto d : k2) {
        column_data_key.insert_data(d.data(), d.size());
    }
    for (auto d : v2) {
        column_data_value.insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    column_data_key.insert_default();
    column_data_value.insert_default();
    for (auto d : k4) {
        column_data_key.insert_data(d.data(), d.size());
    }
    for (auto d : v4) {
        column_data_value.insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    for (auto d : k5) {
        column_data_key.insert_data(d.data(), d.size());
    }
    for (auto d : v5) {
        column_data_value.insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }

    for (auto d : offset) {
        column_offsets.insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }

    column_res->insert_from(*column_map, 0);
    column_res->insert_from(*column_map, 1);
    column_res->insert_from(*column_map, 4);
    column_map->erase(2, 2);

    // Block tmp;
    // tmp.insert({std::move(column), map_type, "asd"});
    // std::cout << tmp.dump_data(0, tmp.rows());
    // Block tmp2;
    // tmp2.insert({std::move(column_res), map_type, "asd2"});
    // std::cout << tmp2.dump_data(0, tmp2.rows());

    EXPECT_EQ(column_map->size(), 3);
    auto* column_result = assert_cast<ColumnMap*>(column_res.get());
    auto& column_offsets_res = column_result->get_offsets_column();
    auto& offset_data_res = assert_cast<ColumnOffset64&>(column_offsets_res);
    auto& offset_data = assert_cast<ColumnOffset64&>(column_offsets);

    auto& column_data_res = assert_cast<ColumnInt64&>(
            assert_cast<ColumnNullable&>(assert_cast<ColumnMap&>(*column_res).get_values())
                    .get_nested_column());
    auto& column_data_origin = assert_cast<ColumnInt64&>(
            assert_cast<ColumnNullable&>(column_data_value).get_nested_column());

    auto& column_data_res_key = assert_cast<ColumnString&>(
            assert_cast<ColumnNullable&>(assert_cast<ColumnMap&>(*column_res).get_keys())
                    .get_nested_column());
    auto& column_data_origin_key = assert_cast<ColumnString&>(
            assert_cast<ColumnNullable&>(column_data_key).get_nested_column());

    for (int i = 0; i < column_map->size(); ++i) {
        std::cout << map_type->to_string(*column_map, i) << std::endl;
        std::cout << map_type->to_string(*column_res, i) << std::endl;
        EXPECT_EQ(column_data_origin.get_element(i), column_data_res.get_element(i));
        EXPECT_EQ(column_data_origin_key.get_data_at(i), column_data_res_key.get_data_at(i));
        EXPECT_EQ(offset_data.get_element(i), offset_data_res.get_element(i));
    }
}

} // namespace doris::vectorized
