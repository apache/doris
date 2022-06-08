
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

#include "vec/runtime/dict/global_dict.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
namespace doris::vectorized {

TEST(GlobalDictTest, Decode) {
    std::vector<std::string> dict_values {"RAIL", "FOB", "MAIL", "SHIP", "TRUCK", "REG AIR", "AIR"};
    auto dict = std::make_shared<GlobalDict>(dict_values);
    size_t row_num = 1024;
    auto column = ColumnVector<Int16>::create();
    for (size_t i = 0; i < row_num; ++i) {
        column->insert_value(i % dict_values.size());
    }
    DataTypePtr data_type(std::make_shared<DataTypeInt16>());
    ColumnWithTypeAndName col_type_and_name1(column->get_ptr(), data_type,
                                             "test_dict_encoded_string");
    ColumnWithTypeAndName col_type_and_name1_decoded;
    EXPECT_TRUE(dict->decode(col_type_and_name1, col_type_and_name1_decoded));

    auto column2 = ColumnVector<Int16>::create();
    for (size_t i = 0; i < row_num; ++i) {
        auto n = i % dict_values.size() + 1;
        column2->insert_data((const char*)&n, sizeof(n));
    }
    ColumnWithTypeAndName col_type_and_name2(column2->get_ptr(), data_type,
                                             "test_dict_encoded_string");
    ColumnWithTypeAndName col_type_and_name2_decoded;
    EXPECT_FALSE(dict->decode(col_type_and_name2, col_type_and_name2_decoded));

    auto nullable_column =
            ColumnNullable::create(column2->get_ptr(), ColumnUInt8::create(column2->size(), 0));
    auto nullmap = nullable_column->get_null_map_data().data();
    for (size_t i = 0; i < row_num; ++i) {
        auto n = i % dict_values.size() + 1;
        if (n == dict_values.size()) {
            nullmap[i] = 1;
        }
    }

    ColumnWithTypeAndName col_type_and_name3(nullable_column->get_ptr(), make_nullable(data_type),
                                             "test_dict_encoded_string");
    ColumnWithTypeAndName col_type_and_name3_decoded;
    EXPECT_TRUE(dict->decode(col_type_and_name3, col_type_and_name3_decoded));
}

} // namespace doris::vectorized
