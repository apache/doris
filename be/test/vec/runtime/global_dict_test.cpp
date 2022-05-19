
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

#include "vec/columns/column_string.h"
#include "vec/data_types/data_type_dict_encoded_string.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

TEST(GlobalDictTest, EncodeAndDecode) {
    std::vector<std::string> dict_values {"RAIL", "FOB", "MAIL", "SHIP", "TRUCK", "REG AIR", "AIR"};
    auto dict = std::make_shared<GlobalDict>(dict_values);
    size_t row_num = 1024;
    auto column = ColumnString::create();
    for (size_t i = 0; i < row_num; ++i) {
        const std::string& val = dict_values[i % dict_values.size()];
        column->insert_data(val.c_str(), val.size());
    }
    DataTypePtr data_type(std::make_shared<DataTypeString>());
    ColumnWithTypeAndName col_type_and_name(column->get_ptr(), data_type, "test_string");

    EXPECT_TRUE(dict->encode(col_type_and_name));
    auto encoded_column = col_type_and_name.column;
    auto encoded_type = col_type_and_name.type;
    EXPECT_TRUE(dict->decode(col_type_and_name));

    const ColumnString* old_column = assert_cast<const ColumnString*>(column.get());
    const ColumnString* new_column =
            assert_cast<const ColumnString*>(col_type_and_name.column.get());
    EXPECT_EQ(old_column->get_chars(), new_column->get_chars());
    EXPECT_EQ(old_column->get_offsets(), new_column->get_offsets());

    column->insert_data("a", 1);
    col_type_and_name.column = column->assume_mutable();
    EXPECT_FALSE(dict->encode(col_type_and_name));
    uint32_t val = (uint32_t)dict_values.size();
    encoded_column->assume_mutable()->insert_data((const char*)&val, sizeof(uint32_t));
    col_type_and_name.column = encoded_column;
    col_type_and_name.type = encoded_type;
    EXPECT_FALSE(dict->decode(col_type_and_name));
}

} // namespace doris::vectorized
