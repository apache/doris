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

#include <arrow/array/builder_base.h>
#include <gtest/gtest.h>

#include "util/jsonb_writer.h"
#include "util/slice.h"
#include "vec/columns/column_fixed_length_object.h"
#include "vec/data_types/serde/data_type_string_serde.h"

namespace doris::vectorized {

TEST(FixedLengthObjectSerdeTest, writeColumnToMysql) {
    auto fixed_length_serde = std::make_shared<vectorized::DataTypeFixedLengthObjectSerDe>(1);
    auto column_fixed_length = ColumnFixedLengthObject::create(sizeof(int64_t));
    column_fixed_length->insert_default();
    ASSERT_EQ(column_fixed_length->size(), 1);
    MysqlRowBuffer<false> mysql_rb;
    DataTypeSerDe::FormatOptions options;
    options.nested_string_wrapper = "\"";
    options.wrapper_len = 1;
    options.map_key_delim = ':';
    options.null_format = "null";
    options.null_len = 4;
    fixed_length_serde->set_return_object_as_string(true);
    auto st = fixed_length_serde->write_column_to_mysql(*column_fixed_length, mysql_rb, 0, false,
                                                        options);
    EXPECT_TRUE(st.ok());
    ASSERT_EQ(mysql_rb.length(), 9);

    column_fixed_length->resize(2);
    *((int64_t*)&(column_fixed_length->get_data()[column_fixed_length->item_size()])) = 22;
    fixed_length_serde->set_return_object_as_string(true);
    st = fixed_length_serde->write_column_to_mysql(*column_fixed_length, mysql_rb, 1, false,
                                                   options);
    EXPECT_TRUE(st.ok());
    ASSERT_EQ(mysql_rb.length(), 18);
    std::cout << "test write_column_to_mysql success" << std::endl;
}

TEST(FixedLengthObjectSerdeTest, writeOneCellToJsonb) {
    auto fixed_length_serde = std::make_shared<vectorized::DataTypeFixedLengthObjectSerDe>(1);
    auto column_fixed_length = ColumnFixedLengthObject::create(sizeof(int64_t));
    column_fixed_length->resize(1);
    *((int64_t*)column_fixed_length->get_data().data()) = 123;
    ASSERT_EQ(column_fixed_length->size(), 1);
    JsonbWriterT<JsonbOutStream> jsonb_writer;
    Arena pool;
    jsonb_writer.writeStartObject();
    fixed_length_serde->write_one_cell_to_jsonb(*column_fixed_length, jsonb_writer, pool, 0, 0);
    jsonb_writer.writeEndObject();

    auto jsonb_column = ColumnString::create();
    jsonb_column->insert_data(jsonb_writer.getOutput()->getBuffer(),
                              jsonb_writer.getOutput()->getSize());
    StringRef jsonb_data = jsonb_column->get_data_at(0);
    JsonbDocument* pdoc = nullptr;
    auto st = JsonbDocument::checkAndCreateDocument(jsonb_data.data, jsonb_data.size, &pdoc);
    ASSERT_TRUE(st.ok()) << "checkAndCreateDocument failed: " << st.to_string();
    JsonbDocument& doc = *pdoc;
    for (auto it = doc->begin(); it != doc->end(); ++it) {
        fixed_length_serde->read_one_cell_from_jsonb(*column_fixed_length, it->value());
    }
    EXPECT_TRUE(column_fixed_length->size() == 2);
    EXPECT_EQ(*reinterpret_cast<const int64_t*>(column_fixed_length->get_data_at(1).data), 123);
    std::cout << "test write/read_one_cell_to_jsonb success" << std::endl;
}

TEST(FixedLengthObjectSerdeTest, writeColumnToPb) {
    auto fixed_length_serde = std::make_shared<vectorized::DataTypeFixedLengthObjectSerDe>(1);
    auto column_fixed_length = ColumnFixedLengthObject::create(sizeof(int64_t));
    column_fixed_length->resize(2);
    *((int64_t*)column_fixed_length->get_data().data()) = 11;
    *((int64_t*)&(column_fixed_length->get_data()[column_fixed_length->item_size()])) = 22;
    ASSERT_EQ(column_fixed_length->size(), 2);
    PValues pv = PValues();
    Status st = fixed_length_serde->write_column_to_pb(*column_fixed_length, pv, 0,
                                                       column_fixed_length->size());
    EXPECT_TRUE(st.ok());

    auto except_column = ColumnFixedLengthObject::create(sizeof(int64_t));
    st = fixed_length_serde->read_column_from_pb(*except_column, pv);
    EXPECT_TRUE(st.ok()) << st.to_string();
    // check pb value from expected column
    PValues as_pv = PValues();
    st = fixed_length_serde->write_column_to_pb(*except_column, as_pv, 0, except_column->size());
    EXPECT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(pv.bytes_value_size(), as_pv.bytes_value_size());
    // check column value
    for (size_t j = 0; j < column_fixed_length->size(); ++j) {
        ASSERT_EQ(column_fixed_length->operator[](j), except_column->operator[](j));
    }
    std::cout << "test write/read_column_to_pb" << std::endl;
}

TEST(FixedLengthObjectSerdeTest, serializeOneCellToJson) {
    auto fixed_length_serde = std::make_shared<vectorized::DataTypeFixedLengthObjectSerDe>(1);
    auto column_fixed_length = ColumnFixedLengthObject::create(sizeof(int64_t));
    column_fixed_length->resize(2);
    *((int64_t*)column_fixed_length->get_data().data()) = 11;
    *((int64_t*)&(column_fixed_length->get_data()[column_fixed_length->item_size()])) = 22;
    ASSERT_EQ(column_fixed_length->size(), 2);
    DataTypeSerDe::FormatOptions formatOptions;
    auto ser_col = ColumnString::create();
    VectorBufferWriter buffer_writer(*ser_col.get());
    auto st = fixed_length_serde->serialize_one_cell_to_json(*column_fixed_length, 0, buffer_writer,
                                                             formatOptions);
    buffer_writer.commit();
    st = fixed_length_serde->serialize_one_cell_to_json(*column_fixed_length, 1, buffer_writer,
                                                        formatOptions);
    buffer_writer.commit();
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(*reinterpret_cast<const int64_t*>(ser_col->get_data_at(0).data), 11);
    EXPECT_EQ(*reinterpret_cast<const int64_t*>(ser_col->get_data_at(1).data), 22);

    auto except_column = ColumnFixedLengthObject::create(sizeof(int64_t));
    int64_t value = 11;
    Slice slice_value((const char*)&value, sizeof(int64_t));
    st = fixed_length_serde->deserialize_one_cell_from_json(*except_column, slice_value,
                                                            formatOptions);
    EXPECT_TRUE(st.ok()) << st.to_string();

    int64_t value2 = 22;
    Slice slice_value2((const char*)&value2, sizeof(int64_t));
    st = fixed_length_serde->deserialize_one_cell_from_json(*except_column, slice_value2,
                                                            formatOptions);
    EXPECT_TRUE(st.ok()) << st.to_string();
    // check column value
    for (size_t j = 0; j < column_fixed_length->size(); ++j) {
        ASSERT_EQ(column_fixed_length->operator[](j), except_column->operator[](j)) << j;
    }
    std::cout << "test serialize/deserialize_one_cell_from_json" << std::endl;
}

TEST(FixedLengthObjectSerdeTest, serializeColumnToJson) {
    auto fixed_length_serde = std::make_shared<vectorized::DataTypeFixedLengthObjectSerDe>(1);
    auto column_fixed_length = ColumnFixedLengthObject::create(sizeof(int64_t));
    column_fixed_length->resize(2);
    *((int64_t*)column_fixed_length->get_data().data()) = 11;
    *((int64_t*)&(column_fixed_length->get_data()[column_fixed_length->item_size()])) = 22;
    ASSERT_EQ(column_fixed_length->size(), 2);
    DataTypeSerDe::FormatOptions formatOptions;
    auto ser_col = ColumnString::create();
    VectorBufferWriter buffer_writer(*ser_col.get());
    auto st = fixed_length_serde->serialize_column_to_json(*column_fixed_length, 0, 2,
                                                           buffer_writer, formatOptions);
    buffer_writer.commit();
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(*reinterpret_cast<const int64_t*>(ser_col->get_data_at(0).data), 11);

    std::vector<Slice> slices_vec;
    uint64_t num_deserialized = 0;
    auto except_column = ColumnFixedLengthObject::create(sizeof(int64_t));
    int64_t value = 11;
    Slice slice1((const char*)&value, sizeof(int64_t));
    slices_vec.emplace_back(slice1);

    int64_t value2 = 22;
    Slice slice2((const char*)&value2, sizeof(int64_t));
    slices_vec.emplace_back(slice2);
    st = fixed_length_serde->deserialize_column_from_json_vector(*except_column, slices_vec,
                                                                 &num_deserialized, formatOptions);
    EXPECT_TRUE(st.ok()) << st.to_string();
    // check column value
    for (size_t j = 0; j < column_fixed_length->size(); ++j) {
        ASSERT_EQ(column_fixed_length->operator[](j), except_column->operator[](j)) << j;
    }
    std::cout << "test serialize/deserialize_column_from_json_vector" << std::endl;
}

TEST(FixedLengthObjectSerdeTest, serializeOneCellToHiveText) {
    auto fixed_length_serde = std::make_shared<vectorized::DataTypeFixedLengthObjectSerDe>(1);
    auto column_fixed_length = ColumnFixedLengthObject::create(sizeof(int64_t));
    column_fixed_length->resize(2);
    *((int64_t*)column_fixed_length->get_data().data()) = 11;
    *((int64_t*)&(column_fixed_length->get_data()[column_fixed_length->item_size()])) = 22;
    ASSERT_EQ(column_fixed_length->size(), 2);
    DataTypeSerDe::FormatOptions formatOptions;
    auto ser_col = ColumnString::create();
    VectorBufferWriter buffer_writer(*ser_col.get());
    auto st = fixed_length_serde->serialize_one_cell_to_hive_text(*column_fixed_length, 0,
                                                                  buffer_writer, formatOptions);
    buffer_writer.commit();
    EXPECT_TRUE(st.ok());
    st = fixed_length_serde->serialize_one_cell_to_hive_text(*column_fixed_length, 1, buffer_writer,
                                                             formatOptions);
    buffer_writer.commit();
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(*reinterpret_cast<const int64_t*>(ser_col->get_data_at(0).data), 11);
    EXPECT_EQ(*reinterpret_cast<const int64_t*>(ser_col->get_data_at(1).data), 22);

    std::vector<Slice> slices_vec;
    auto except_column = ColumnFixedLengthObject::create(sizeof(int64_t));
    int64_t value = 11;
    Slice slice1((const char*)&value, sizeof(int64_t));
    st = fixed_length_serde->deserialize_one_cell_from_hive_text(*except_column, slice1,
                                                                 formatOptions);

    int64_t value2 = 22;
    Slice slice2((const char*)&value2, sizeof(int64_t));
    st = fixed_length_serde->deserialize_one_cell_from_hive_text(*except_column, slice2,
                                                                 formatOptions);
    EXPECT_TRUE(st.ok()) << st.to_string();
    // check column value
    for (size_t j = 0; j < column_fixed_length->size(); ++j) {
        ASSERT_EQ(column_fixed_length->operator[](j), except_column->operator[](j)) << j;
    }
    std::cout << "test serialize/deserialize_one_cell_from_hive_text" << std::endl;
}
} // namespace doris::vectorized