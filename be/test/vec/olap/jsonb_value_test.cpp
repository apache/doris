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

#include <string>

#include "gtest/gtest_pred_impl.h"
#include "vec/columns/column_string.h"
#include "vec/common/string_ref.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/data_types/serde/data_type_serde.h"
#include "vec/olap/olap_data_convertor.h"

namespace doris::vectorized {

TEST(JsonbValueConvertorTest, JsonbValueValid) {
    // 1. create jsonb column with serde
    auto input = ColumnString::create();
    auto dataTypeJsonb = std::make_shared<DataTypeJsonb>();
    auto serde = dataTypeJsonb->get_serde();
    vectorized::DataTypeSerDe::FormatOptions options;

    // Test case 1
    std::string str1 = "{\"key1\": \"value1\"}";
    Slice slice1 = Slice(str1.data(), str1.length());
    auto st1 = serde->deserialize_one_cell_from_json(*input, slice1, options);
    ASSERT_TRUE(st1.ok());
    ASSERT_EQ(input->size(), 1);

    // Test case 2
    std::string str2 = "{\"key2\": 12345}";
    Slice slice2 = Slice(str2.data(), str2.length());
    auto st2 = serde->deserialize_one_cell_from_json(*input, slice2, options);
    ASSERT_TRUE(st2.ok());
    ASSERT_EQ(input->size(), 2);

    // Test case 3
    std::string str3 = "{\"key3\": true}";
    Slice slice3 = Slice(str3.data(), str3.length());
    auto st3 = serde->deserialize_one_cell_from_json(*input, slice3, options);
    ASSERT_TRUE(st3.ok());
    ASSERT_EQ(input->size(), 3);

    // Test case 4
    std::string str4 = "{\"key4\": [1, 2, 3]}";
    Slice slice4 = Slice(str4.data(), str4.length());
    auto st4 = serde->deserialize_one_cell_from_json(*input, slice4, options);
    ASSERT_TRUE(st4.ok());
    ASSERT_EQ(input->size(), 4);

    // Test case 5
    std::string str5 = "{\"key5\": {\"subkey\": \"subvalue\"}}";
    Slice slice5 = Slice(str5.data(), str5.length());
    auto st5 = serde->deserialize_one_cell_from_json(*input, slice5, options);
    ASSERT_TRUE(st5.ok());
    ASSERT_EQ(input->size(), 5);

    // 2. put column into block
    vectorized::ColumnWithTypeAndName argument(input->assume_mutable(), dataTypeJsonb,
                                               "jsonb_column");
    Block block;
    block.insert(argument);

    // 3. use OlapColumnDataConvertorVarchar::convert_to_olap to convert column data to segment file data
    auto _olap_data_convertor = std::make_unique<OlapBlockDataConvertor>();
    TabletColumn jsonb_column = TabletColumn();
    jsonb_column.set_type(FieldType::OLAP_FIELD_TYPE_JSONB);
    _olap_data_convertor->add_column_data_convertor(jsonb_column);
    _olap_data_convertor->set_source_content(&block, 0, 5);
    auto [status, column] = _olap_data_convertor->convert_column_data(0);
    ASSERT_TRUE(status.ok());
    ASSERT_NE(column, nullptr);

    // test with null map
    auto nullable_col = ColumnNullable::create(ColumnString::create(), ColumnUInt8::create());
    auto nullable_dataTypeJsonb = make_nullable(std::make_shared<DataTypeJsonb>());
    auto serde1 = nullable_dataTypeJsonb->get_serde();

    auto st = serde1->deserialize_one_cell_from_json(*nullable_col, slice1, options);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(1, nullable_col->size());

    // insert null
    nullable_col->insert_default();
    ASSERT_EQ(2, nullable_col->size());

    st = serde1->deserialize_one_cell_from_json(*nullable_col, slice2, options);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(3, nullable_col->size());

    // deserialize null
    Slice slice_null = "NULL";
    st = serde1->deserialize_one_cell_from_json(*nullable_col, slice_null, options);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(4, nullable_col->size());

    st = serde1->deserialize_one_cell_from_json(*nullable_col, slice3, options);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(5, nullable_col->size());

    // 2. put column into block
    vectorized::ColumnWithTypeAndName argument1(nullable_col->assume_mutable(),
                                                nullable_dataTypeJsonb, "jsonb_column_null");
    block.clear();
    block.insert(argument1);

    // 3. use OlapColumnDataConvertorVarchar::convert_to_olap to convert column data to segment file data
    _olap_data_convertor->reset();
    _olap_data_convertor->add_column_data_convertor(jsonb_column);
    _olap_data_convertor->set_source_content(&block, 0, 5);
    auto [status1, column1] = _olap_data_convertor->convert_column_data(0);
    ASSERT_TRUE(status1.ok()) << status1.to_string();
    ASSERT_NE(column1, nullptr);
}

TEST(JsonbValueConvertorTest, JsonbValueInvalid) {
    // 1. create jsonb column with serde
    auto input = ColumnString::create();
    auto dataTypeJsonb = std::make_shared<DataTypeJsonb>();
    auto serde = dataTypeJsonb->get_serde();
    vectorized::DataTypeSerDe::FormatOptions options;

    // Test case 1
    std::string str1 = "{\"key1\": \"value1\"}";
    Slice slice1 = Slice(str1.data(), str1.length());
    auto st1 = serde->deserialize_one_cell_from_json(*input, slice1, options);
    ASSERT_TRUE(st1.ok());
    ASSERT_EQ(input->size(), 1);

    // Test case 2
    std::string str2 = "{\"key2\": 12345}";
    Slice slice2 = Slice(str2.data(), str2.length());
    auto st2 = serde->deserialize_one_cell_from_json(*input, slice2, options);
    ASSERT_TRUE(st2.ok());
    ASSERT_EQ(input->size(), 2);

    // Test case 3
    std::string str3 = "{\"key3\": true}";
    Slice slice3 = Slice(str3.data(), str3.length());
    auto st3 = serde->deserialize_one_cell_from_json(*input, slice3, options);
    ASSERT_TRUE(st3.ok());
    ASSERT_EQ(input->size(), 3);

    // Test case 4
    std::string str4 = "{\"key4\": [1, 2, 3]}";
    Slice slice4 = Slice(str4.data(), str4.length());
    auto st4 = serde->deserialize_one_cell_from_json(*input, slice4, options);
    ASSERT_TRUE(st4.ok());
    ASSERT_EQ(input->size(), 4);
    // invalid jsonb data
    auto& data = input->get_chars();
    data.emplace_back('s');

    // Test case 5
    std::string str5 = "{\"key5\": {\"subkey\": \"subvalue\"}}";
    Slice slice5 = Slice(str5.data(), str5.length());
    auto st5 = serde->deserialize_one_cell_from_json(*input, slice5, options);
    ASSERT_TRUE(st5.ok());
    ASSERT_EQ(input->size(), 5);

    // 2. put column into block
    vectorized::ColumnWithTypeAndName argument(input->assume_mutable(), dataTypeJsonb,
                                               "jsonb_column");
    Block block;
    block.insert(argument);

    // 3. use OlapColumnDataConvertorVarchar::convert_to_olap to convert column data to segment file data
    auto _olap_data_convertor = std::make_unique<OlapBlockDataConvertor>();
    TabletColumn jsonb_column = TabletColumn();
    jsonb_column.set_type(FieldType::OLAP_FIELD_TYPE_JSONB);
    _olap_data_convertor->add_column_data_convertor(jsonb_column);
    _olap_data_convertor->set_source_content(&block, 0, 5);
    auto [status, column] = _olap_data_convertor->convert_column_data(0);
    // invalid will make error
    ASSERT_FALSE(status.ok());
    ASSERT_TRUE(status.to_string().find("Invalid JSONB document") != std::string::npos)
            << status.to_string();
    ASSERT_NE(column, nullptr);

    // test with null map
    auto nullable_col = ColumnNullable::create(ColumnString::create(), ColumnUInt8::create());
    auto nullable_dataTypeJsonb = make_nullable(std::make_shared<DataTypeJsonb>());
    auto serde1 = nullable_dataTypeJsonb->get_serde();

    auto st = serde1->deserialize_one_cell_from_json(*nullable_col, slice1, options);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(1, nullable_col->size());

    // insert null
    nullable_col->insert_default();
    ASSERT_EQ(2, nullable_col->size());

    st = serde1->deserialize_one_cell_from_json(*nullable_col, slice2, options);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(3, nullable_col->size());
    // invalid jsonb data
    auto string_data = assert_cast<ColumnString*>(nullable_col->get_nested_column_ptr().get());
    auto& dat = string_data->get_chars();
    dat.emplace_back('s');

    // deserialize null
    Slice slice_null = "NULL";
    st = serde1->deserialize_one_cell_from_json(*nullable_col, slice_null, options);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(4, nullable_col->size());

    st = serde1->deserialize_one_cell_from_json(*nullable_col, slice3, options);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(5, nullable_col->size());

    // 2. put column into block
    vectorized::ColumnWithTypeAndName argument1(nullable_col->assume_mutable(),
                                                nullable_dataTypeJsonb, "jsonb_column_null");
    block.clear();
    block.insert(argument1);

    // 3. use OlapColumnDataConvertorVarchar::convert_to_olap to convert column data to segment file data
    _olap_data_convertor->reset();
    _olap_data_convertor->add_column_data_convertor(jsonb_column);
    _olap_data_convertor->set_source_content(&block, 0, 5);
    auto [status1, column1] = _olap_data_convertor->convert_column_data(0);
    ASSERT_FALSE(status.ok());
    ASSERT_TRUE(status.to_string().find("Invalid JSONB document") != std::string::npos)
            << status.to_string();
    ASSERT_NE(column, nullptr);
}

} // namespace doris::vectorized
