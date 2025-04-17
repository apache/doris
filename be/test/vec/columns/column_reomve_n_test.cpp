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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <iostream>

#include "vec/columns/column_array.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_struct.h"
#include "vec/columns/columns_number.h"
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

class RemoveNTest : public ::testing::Test {
protected:
    void SetUp() override {}

    void TearDown() override {}
};

TEST_F(RemoveNTest, ScalaTypeUInt8Test) {
    auto column = ColumnUInt8::create();
    std::vector<Int8> data = {1, 2, 3, 4, 5};
    for (auto d : data) {
        column->insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    column->erase(0, 2);
    EXPECT_EQ(column->size(), 3);
    for (int i = 0; i < column->size(); ++i) {
        EXPECT_EQ(column->get_int(i), data[i + 2]);
    }
}

TEST_F(RemoveNTest, ScalaTypeUInt8Test2) {
    auto column = ColumnUInt8::create();
    std::vector<Int8> data = {1, 2, 3, 4, 5};
    for (auto d : data) {
        column->insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    column->erase(2, 2);
    EXPECT_EQ(column->size(), 3);
    std::vector<Int8> data2 = {1, 2, 5};
    for (int i = 0; i < column->size(); ++i) {
        EXPECT_EQ(column->get_int(i), data2[i]);
    }
}

TEST_F(RemoveNTest, ScalaTypeInt32Test) {
    auto column = ColumnInt32::create();
    std::vector<int32_t> data = {1, 2, 3, 4, 5};
    for (auto d : data) {
        column->insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    column->erase(0, 2);
    EXPECT_EQ(column->size(), 3);
    for (int i = 0; i < column->size(); ++i) {
        EXPECT_EQ(column->get_int(i), data[i + 2]);
    }
}

TEST_F(RemoveNTest, ScalaTypeInt32Test2) {
    auto column = ColumnInt32::create();
    std::vector<int32_t> data = {1, 2, 3, 4, 5};
    for (auto d : data) {
        column->insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    column->erase(2, 2);
    EXPECT_EQ(column->size(), 3);
    std::vector<int32_t> data2 = {1, 2, 5};
    for (int i = 0; i < column->size(); ++i) {
        EXPECT_EQ(column->get_int(i), data2[i]);
    }
}

TEST_F(RemoveNTest, ScalaTypeNullInt32Test) {
    auto datetype_int32 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    auto column = datetype_int32->create_column();
    auto column_res = datetype_int32->create_column();
    std::vector<int32_t> data = {1, 2, 3, 4, 5};
    for (auto d : data) {
        column->insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    column_res->insert_range_from(*column, 0, data.size());
    column->erase(0, 2);
    EXPECT_EQ(column->size(), 3);

    for (int i = 0; i < column->size(); ++i) {
        EXPECT_EQ(column->get_data_at(i), column_res->get_data_at(i + 2));
    }
}

TEST_F(RemoveNTest, ScalaTypeNullInt32Test2) {
    auto datetype_int32 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    auto column = datetype_int32->create_column();
    auto column_res = datetype_int32->create_column();
    std::vector<int32_t> data = {1, 2, 3, 4, 5};
    std::vector<int32_t> res = {1, 2, 5};
    for (auto d : data) {
        column->insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    for (auto d : res) {
        column_res->insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    column->erase(2, 2);
    EXPECT_EQ(column->size(), 3);

    for (int i = 0; i < column->size(); ++i) {
        EXPECT_EQ(column->get_data_at(i), column_res->get_data_at(i));
    }
}

TEST_F(RemoveNTest, ScalaTypeNullStringTest) {
    auto datetype_string = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    auto column = datetype_string->create_column();
    auto column_res = datetype_string->create_column();
    std::vector<StringRef> data = {StringRef("asd"), StringRef("1234567"), StringRef("3"),
                                   StringRef("4"), StringRef("5")};
    column->insert_default();
    for (auto d : data) {
        column->insert_data(d.data, d.size);
    }
    column->insert_default();
    column_res->insert_range_from(*column, 0, data.size() + 2);
    column->erase(0, 2);
    EXPECT_EQ(column->size(), 5);
    for (int i = 0; i < column->size(); ++i) {
        std::cout << column->get_data_at(i).to_string() << std::endl;
        EXPECT_EQ(column->get_data_at(i).to_string(), column_res->get_data_at(i + 2).to_string());
    }
}

TEST_F(RemoveNTest, ScalaTypeNullStringTest2) {
    auto datetype_string = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    auto column = datetype_string->create_column();
    auto column_res = datetype_string->create_column();
    std::vector<StringRef> data = {StringRef("asd"), StringRef("1234567"), StringRef("3"),
                                   StringRef("4"), StringRef("5")};
    std::vector<StringRef> res = {StringRef("asd"), StringRef("4"), StringRef("5")};
    column->insert_default();
    for (auto d : data) {
        column->insert_data(d.data, d.size);
    }
    column->insert_default();

    column_res->insert_default();
    for (auto d : res) {
        column_res->insert_data(d.data, d.size);
    }
    column_res->insert_default();

    column->erase(2, 2);
    EXPECT_EQ(column->size(), 5);
    for (int i = 0; i < column->size(); ++i) {
        std::cout << column->get_data_at(i).to_string() << " , "
                  << column_res->get_data_at(i).to_string() << std::endl;
        // EXPECT_EQ(column->get_data_at(i).to_string(), column_res->get_data_at(i).to_string());
    }
}

TEST_F(RemoveNTest, ScalaTypeDecimalTest) {
    auto datetype_decimal = vectorized::create_decimal(10, 2, false);
    auto column = datetype_decimal->create_column();
    auto column_res = datetype_decimal->create_column();

    std::vector<double> data = {1.1, 2.2, 3.3, 4.4, 5.5};
    for (auto d : data) {
        column->insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
        column_res->insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    column->erase(0, 2);
    EXPECT_EQ(column->size(), 3);
    for (int i = 0; i < column->size(); ++i) {
        EXPECT_EQ(column->get_data_at(i), column_res->get_data_at(i + 2));
    }
}

TEST_F(RemoveNTest, ScalaTypeDecimalTest2) {
    auto datetype_decimal = vectorized::create_decimal(10, 2, false);
    auto column = datetype_decimal->create_column();
    auto column_res = datetype_decimal->create_column();

    std::vector<double> data = {1.1, 2.2, 3.3, 4.4, 5.5};
    for (auto d : data) {
        column->insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }

    std::vector<double> res = {1.1, 2.2, 5.5};
    for (auto d : res) {
        column_res->insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    column->erase(2, 2);
    EXPECT_EQ(column->size(), 3);
    for (int i = 0; i < column->size(); ++i) {
        EXPECT_EQ(column->get_data_at(i), column_res->get_data_at(i));
    }
}

TEST_F(RemoveNTest, ScalaTypeStringTest) {
    auto column = ColumnString::create();
    std::vector<StringRef> data = {StringRef("asd"), StringRef("1234567"), StringRef("3"),
                                   StringRef("4"), StringRef("5")};
    for (auto d : data) {
        column->insert_data(d.data, d.size);
    }
    column->erase(0, 2);
    EXPECT_EQ(column->size(), 3);
    for (int i = 0; i < column->size(); ++i) {
        std::cout << column->get_data_at(i).to_string() << std::endl;
        EXPECT_EQ(column->get_data_at(i).to_string(), data[i + 2].to_string());
    }

    auto column2 = ColumnString::create();
    std::vector<StringRef> data2 = {StringRef(""), StringRef("1234567"), StringRef("asd"),
                                    StringRef("4"), StringRef("5")};
    for (auto d : data2) {
        column2->insert_data(d.data, d.size);
    }
    column2->erase(0, 2);
    EXPECT_EQ(column2->size(), 3);
    for (int i = 0; i < column2->size(); ++i) {
        std::cout << column2->get_data_at(i).to_string() << std::endl;
        EXPECT_EQ(column2->get_data_at(i).to_string(), data2[i + 2].to_string());
    }
}

TEST_F(RemoveNTest, ScalaTypeStringTest2) {
    auto column = ColumnString::create();
    std::vector<StringRef> data = {StringRef("asd"), StringRef("1234567"), StringRef("3"),
                                   StringRef("4"), StringRef("5")};
    std::vector<StringRef> res = {StringRef("asd"), StringRef("1234567"), StringRef("5")};
    for (auto d : data) {
        column->insert_data(d.data, d.size);
    }
    column->erase(2, 2);
    EXPECT_EQ(column->size(), 3);
    for (int i = 0; i < column->size(); ++i) {
        std::cout << column->get_data_at(i).to_string() << std::endl;
        EXPECT_EQ(column->get_data_at(i).to_string(), res[i].to_string());
    }

    auto column2 = ColumnString::create();
    std::vector<StringRef> data2 = {StringRef(""), StringRef("1234567"), StringRef("asd"),
                                    StringRef("4"), StringRef("5")};
    std::vector<StringRef> res2 = {StringRef(""), StringRef("1234567"), StringRef("5")};
    for (auto d : data2) {
        column2->insert_data(d.data, d.size);
    }
    column2->erase(2, 2);
    EXPECT_EQ(column2->size(), 3);
    for (int i = 0; i < column2->size(); ++i) {
        std::cout << column2->get_data_at(i).to_string() << std::endl;
        EXPECT_EQ(column2->get_data_at(i).to_string(), res2[i].to_string());
    }
}

TEST_F(RemoveNTest, ArrayTypeTest) {
    DataTypePtr datetype_32 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    DataTypePtr datetype_array = std::make_shared<DataTypeArray>(datetype_32);
    auto c = datetype_array->create_column();
    auto column_res = datetype_array->create_column();
    auto* column_array = assert_cast<ColumnArray*>(c.get());
    auto& column_offsets = column_array->get_offsets_column();
    auto& column_data = column_array->get_data();

    std::vector<int32_t> data = {1, 2, 3, 4, 5};
    std::vector<int32_t> data2 = {11, 22};
    std::vector<int32_t> data3 = {33, 44, 55};
    // insert null
    std::vector<int32_t> data5 = {66};

    std::vector<UInt64> offset = {5, 7, 10, 11, 12};
    for (auto d : data) {
        column_data.insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    for (auto d : data2) {
        column_data.insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    for (auto d : data3) {
        column_data.insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    column_data.insert_default();
    for (auto d : data5) {
        column_data.insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }

    for (auto d : offset) {
        column_offsets.insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }

    column_res->insert_range_from(*column_array, 0, offset.size());
    column_array->erase(0, 2);
    EXPECT_EQ(column_array->size(), 3);
    // Block tmp;
    // tmp.insert({std::move(c), datetype_array, "asd"});
    // std::cout << tmp.dump_data(0, tmp.rows());

    // Block tmp2;
    // tmp2.insert({std::move(column_res), datetype_array, "asd2"});
    // std::cout << tmp2.dump_data(0, tmp2.rows());
    auto* column_result = assert_cast<ColumnArray*>(column_res.get());
    auto& column_offsets_res = column_result->get_offsets_column();
    auto& offset_data_res = assert_cast<ColumnUInt64&>(column_offsets_res);
    auto& offset_data = assert_cast<ColumnUInt64&>(column_offsets);
    auto& column_data_res = assert_cast<ColumnInt32&>(
            assert_cast<ColumnNullable&>(column_result->get_data()).get_nested_column());
    auto& column_data_origin = assert_cast<ColumnInt32&>(
            assert_cast<ColumnNullable&>(column_data).get_nested_column());
    for (int i = 0; i < column_array->size(); ++i) {
        std::cout << datetype_array->to_string(*column_array, i) << std::endl;
        std::cout << datetype_array->to_string(*column_res, i + 2) << std::endl;
        EXPECT_EQ(column_data_origin.get_element(i), column_data_res.get_element(i + 7));
        EXPECT_EQ(offset_data.get_element(i), offset_data_res.get_element(i + 2) - 7);
    }
}

TEST_F(RemoveNTest, ArrayTypeTest2) {
    DataTypePtr datetype_32 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    DataTypePtr datetype_array = std::make_shared<DataTypeArray>(datetype_32);
    auto c = datetype_array->create_column();
    auto* column_array = assert_cast<ColumnArray*>(c.get());
    auto column_res = column_array->clone_empty();
    auto& column_offsets = column_array->get_offsets_column();
    auto& column_data = column_array->get_data();

    std::vector<int32_t> data = {1, 2, 3, 4, 5};
    std::vector<int32_t> data2 = {11, 22};
    std::vector<int32_t> data3 = {33, 44, 55};
    // insert null
    std::vector<int32_t> data5 = {66};

    std::vector<UInt64> offset = {5, 7, 10, 11, 12};
    for (auto d : data) {
        column_data.insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    for (auto d : data2) {
        column_data.insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    for (auto d : data3) {
        column_data.insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    column_data.insert_default();
    for (auto d : data5) {
        column_data.insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }

    for (auto d : offset) {
        column_offsets.insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }

    column_res->insert_from(*column_array, 0);
    column_res->insert_from(*column_array, 1);
    column_res->insert_from(*column_array, 4);

    column_array->erase(2, 2);
    EXPECT_EQ(column_array->size(), 3);
    std::cout << "have call erase" << std::endl;
    // Block tmp;
    // tmp.insert({std::move(c), datetype_array, "asd"});
    // std::cout << tmp.dump_data(0, tmp.rows());

    // Block tmp2;
    // tmp2.insert({std::move(column_res), datetype_array, "asd2"});
    // std::cout << tmp2.dump_data(0, tmp2.rows());

    auto* column_result = assert_cast<ColumnArray*>(column_res.get());
    auto& column_offsets_res = column_result->get_offsets_column();
    auto& offset_data_res = assert_cast<ColumnUInt64&>(column_offsets_res);
    auto& offset_data = assert_cast<ColumnUInt64&>(column_offsets);
    auto& column_data_res = assert_cast<ColumnInt32&>(
            assert_cast<ColumnNullable&>(column_result->get_data()).get_nested_column());
    auto& column_data_origin = assert_cast<ColumnInt32&>(
            assert_cast<ColumnNullable&>(column_data).get_nested_column());
    for (int i = 0; i < column_array->size(); ++i) {
        std::cout << datetype_array->to_string(*column_array, i) << std::endl;
        std::cout << datetype_array->to_string(*column_res, i) << std::endl;
        EXPECT_EQ(column_data_origin.get_element(i), column_data_res.get_element(i));
        EXPECT_EQ(offset_data.get_element(i), offset_data_res.get_element(i));
    }
}

TEST_F(RemoveNTest, MapTypeTest) {
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
    auto& offset_data_res = assert_cast<ColumnUInt64&>(column_offsets_res);
    auto& offset_data = assert_cast<ColumnUInt64&>(column_offsets);

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

TEST_F(RemoveNTest, MapTypeTest2) {
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
    auto& offset_data_res = assert_cast<ColumnUInt64&>(column_offsets_res);
    auto& offset_data = assert_cast<ColumnUInt64&>(column_offsets);

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

TEST_F(RemoveNTest, StructTypeTest) {
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

TEST_F(RemoveNTest, StructTypeTest2) {
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
