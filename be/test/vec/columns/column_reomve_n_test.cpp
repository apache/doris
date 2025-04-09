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

#include <cstdint>
#include <iostream>

#include "gtest/gtest_pred_impl.h"
#include "util/runtime_profile.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_struct.h"
#include "vec/columns/columns_number.h"
#include "vec/common/schema_util.h"
#include "vec/core/field.h"
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
    column->remove_first_n_values(2);
    EXPECT_EQ(column->size(), 3);
    for (int i = 0; i < column->size(); ++i) {
        EXPECT_EQ(column->get_int(i), data[i + 2]);
    }
}

TEST_F(RemoveNTest, ScalaTypeInt32Test) {
    auto column = ColumnInt32::create();
    std::vector<int32_t> data = {1, 2, 3, 4, 5};
    for (auto d : data) {
        column->insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    column->remove_first_n_values(2);
    EXPECT_EQ(column->size(), 3);
    for (int i = 0; i < column->size(); ++i) {
        EXPECT_EQ(column->get_int(i), data[i + 2]);
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
    column->remove_first_n_values(2);
    EXPECT_EQ(column->size(), 3);

    for (int i = 0; i < column->size(); ++i) {
        EXPECT_EQ(column->get_data_at(i), column_res->get_data_at(i + 2));
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
    column->remove_first_n_values(2);
    EXPECT_EQ(column->size(), 5);
    for (int i = 0; i < column->size(); ++i) {
        std::cout << column->get_data_at(i).to_string() << std::endl;
        EXPECT_EQ(column->get_data_at(i).to_string(), column_res->get_data_at(i + 2).to_string());
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
    column->remove_first_n_values(2);
    EXPECT_EQ(column->size(), 3);
    for (int i = 0; i < column->size(); ++i) {
        EXPECT_EQ(column->get_data_at(i), column_res->get_data_at(i + 2));
    }
}

TEST_F(RemoveNTest, ScalaTypeStringTest) {
    auto column = ColumnString::create();
    std::vector<StringRef> data = {StringRef("asd"), StringRef("1234567"), StringRef("3"),
                                   StringRef("4"), StringRef("5")};
    for (auto d : data) {
        column->insert_data(d.data, d.size);
    }
    column->remove_first_n_values(2);
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
    column2->remove_first_n_values(2);
    EXPECT_EQ(column2->size(), 3);
    for (int i = 0; i < column2->size(); ++i) {
        std::cout << column2->get_data_at(i).to_string() << std::endl;
        EXPECT_EQ(column2->get_data_at(i).to_string(), data2[i + 2].to_string());
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
    column_array->remove_first_n_values(1);
    EXPECT_EQ(column_array->size(), 3);
    Block tmp;
    tmp.insert({std::move(c), datetype_array, "asd"});
    std::cout << tmp.dump_data(0, tmp.rows());

    Block tmp2;
    tmp2.insert({std::move(column_res), datetype_array, "asd2"});
    std::cout << tmp2.dump_data(0, tmp2.rows());
    // auto* column_result = assert_cast<ColumnArray*>(column_res.get());
    // auto& column_offsets_res = column_result->get_offsets_column();
    // // auto& column_data_res = column_result->get_data();
    // for (int i = 0; i < column_array->size(); ++i) {
    //     // EXPECT_EQ(column_data.get_data_at(i), column_data_res.get_data_at(i + 2));
    //     EXPECT_EQ(column_offsets.get_data_at(i), column_offsets_res.get_data_at(i + 1));
    // }
}

// TEST_F(RemoveNTest, MapTypeTest) {
//     DataTypes dataTypes = _create_scala_data_types2();

//     // data_type_map
//     for (int i = 0; i < dataTypes.size() - 1; ++i) {
//         DataTypePtr a = std::make_shared<DataTypeMap>(dataTypes[i], dataTypes[i + 1]);
//         auto col_a = a->create_column();
//         col_a->resize(10);
//         MutableColumnPtr b = a->create_column();
//         b->insert_range_from(*col_a, 0, 10);
//         EXPECT_EQ(b->size(), 10);
//         ColumnMap* col_map = reinterpret_cast<ColumnMap*>(b.get());
//         for (int i = 0; i < col_map->get_offsets().size(); ++i) {
//             EXPECT_EQ(0, col_map->get_offsets()[i]);
//         }
//     }
// }

// TEST_F(RemoveNTest, StructTypeTest) {
//     DataTypes dataTypes = _create_scala_data_types2();

//     DataTypePtr a = std::make_shared<DataTypeStruct>(dataTypes);
//     auto col_a = a->create_column();
//     col_a->resize(10);
//     MutableColumnPtr b = a->create_column();
//     b->insert_range_from(*col_a, 0, 10);
//     EXPECT_EQ(b->size(), 10);
// }

} // namespace doris::vectorized
