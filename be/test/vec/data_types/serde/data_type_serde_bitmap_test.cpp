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

#include "util/slice.h"
#include "vec/columns/column_complex.h"
#include "vec/data_types/common_data_type_serder_test.h"
#include "vec/data_types/data_type_bitmap.h"
#include "vec/data_types/serde/data_type_bitmap_serde.h"

namespace doris::vectorized {

TEST(BitmapSerdeTest, writeColumnToMysql) {
    auto bitmap_serde = std::make_shared<vectorized::DataTypeBitMapSerDe>(1);
    auto column_bitmap = ColumnBitmap::create();
    column_bitmap->insert_value(BitmapValue::empty_bitmap());
    ASSERT_EQ(column_bitmap->size(), 1);
    MysqlRowBuffer<false> mysql_rb;
    DataTypeSerDe::FormatOptions options;
    options.nested_string_wrapper = "\"";
    options.wrapper_len = 1;
    options.map_key_delim = ':';
    options.null_format = "null";
    options.null_len = 4;
    bitmap_serde->set_return_object_as_string(true);
    auto st = bitmap_serde->write_column_to_mysql(*column_bitmap, mysql_rb, 0, false, options);
    EXPECT_TRUE(st.ok());
    ASSERT_EQ(mysql_rb.length(), 2);

    column_bitmap->insert_value(BitmapValue(123));
    bitmap_serde->set_return_object_as_string(true);
    st = bitmap_serde->write_column_to_mysql(*column_bitmap, mysql_rb, 1, false, options);
    EXPECT_TRUE(st.ok());
    ASSERT_EQ(mysql_rb.length(), 8);
    std::cout << "test write_column_to_mysql success" << std::endl;
}

TEST(BitmapSerdeTest, writeOneCellToJsonb) {
    auto bitmap_serde = std::make_shared<vectorized::DataTypeBitMapSerDe>(1);
    auto column_bitmap = ColumnBitmap::create();
    column_bitmap->insert_value(BitmapValue(123));
    ASSERT_EQ(column_bitmap->size(), 1);
    JsonbWriterT<JsonbOutStream> jsonb_writer;
    Arena pool;
    jsonb_writer.writeStartObject();
    bitmap_serde->write_one_cell_to_jsonb(*column_bitmap, jsonb_writer, pool, 0, 0);
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
        bitmap_serde->read_one_cell_from_jsonb(*column_bitmap, it->value());
    }
    EXPECT_TRUE(column_bitmap->size() == 2);
    BitmapValue data = column_bitmap->get_element(1);
    EXPECT_EQ(data.to_string(), "123");
    std::cout << "test write/read_one_cell_to_jsonb success" << std::endl;
}

TEST(BitmapSerdeTest, writeColumnToPb) {
    auto bitmap_serde = std::make_shared<vectorized::DataTypeBitMapSerDe>(1);
    auto column_bitmap = ColumnBitmap::create();
    column_bitmap->insert_value(BitmapValue::empty_bitmap());
    column_bitmap->insert_value(BitmapValue(123));
    ASSERT_EQ(column_bitmap->size(), 2);
    PValues pv = PValues();
    Status st = bitmap_serde->write_column_to_pb(*column_bitmap, pv, 0, column_bitmap->size());
    EXPECT_TRUE(st.ok());

    auto except_column = ColumnBitmap::create();
    st = bitmap_serde->read_column_from_pb(*except_column, pv);
    EXPECT_TRUE(st.ok()) << st.to_string();
    // check pb value from expected column
    PValues as_pv = PValues();
    st = bitmap_serde->write_column_to_pb(*except_column, as_pv, 0, except_column->size());
    EXPECT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(pv.bytes_value_size(), as_pv.bytes_value_size());
    // check column value
    for (size_t j = 0; j < column_bitmap->size(); ++j) {
        EXPECT_EQ(column_bitmap->get_element(j).to_string(),
                  except_column->get_element(j).to_string());
    }
    std::cout << "test write/read_column_to_pb" << std::endl;
}

TEST(BitmapSerdeTest, serializeOneCellToJson) {
    auto bitmap_serde = std::make_shared<vectorized::DataTypeBitMapSerDe>(1);
    auto column_bitmap = ColumnBitmap::create();
    column_bitmap->insert_value(BitmapValue::empty_bitmap());
    column_bitmap->insert_value(BitmapValue(123));
    ASSERT_EQ(column_bitmap->size(), 2);
    DataTypeSerDe::FormatOptions formatOptions;
    auto ser_col = ColumnString::create();
    VectorBufferWriter buffer_writer(*ser_col.get());
    auto st = bitmap_serde->serialize_one_cell_to_json(*column_bitmap, 0, buffer_writer,
                                                       formatOptions);
    buffer_writer.commit();
    st = bitmap_serde->serialize_one_cell_to_json(*column_bitmap, 1, buffer_writer, formatOptions);
    buffer_writer.commit();
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(ser_col->get_data_at(0).to_string(), "\\N") << st.to_string();
    EXPECT_EQ(ser_col->get_data_at(1).to_string(), "\\N") << st.to_string();

    auto except_column = ColumnBitmap::create();
    std::string memory_buffer;
    auto bytesize = column_bitmap->get_element(0).getSizeInBytes();
    memory_buffer.resize(bytesize);
    column_bitmap->get_element(0).write_to(const_cast<char*>(memory_buffer.data()));
    Slice slice_value(memory_buffer.data(), memory_buffer.size());
    st = bitmap_serde->deserialize_one_cell_from_json(*except_column, slice_value, formatOptions);
    EXPECT_TRUE(st.ok()) << st.to_string();

    memory_buffer.clear();
    bytesize = column_bitmap->get_element(1).getSizeInBytes();
    memory_buffer.resize(bytesize);
    column_bitmap->get_element(1).write_to(const_cast<char*>(memory_buffer.data()));
    slice_value = Slice(memory_buffer.data(), memory_buffer.size());
    st = bitmap_serde->deserialize_one_cell_from_json(*except_column, slice_value, formatOptions);
    EXPECT_TRUE(st.ok()) << st.to_string();
    // check column value
    for (size_t j = 0; j < column_bitmap->size(); ++j) {
        EXPECT_EQ(column_bitmap->get_element(j).to_string(),
                  except_column->get_element(j).to_string());
    }
    std::cout << "test serialize/deserialize_one_cell_from_json" << std::endl;
}

TEST(BitmapSerdeTest, serializeColumnToJson) {
    auto bitmap_serde = std::make_shared<vectorized::DataTypeBitMapSerDe>(1);
    auto column_bitmap = ColumnBitmap::create();
    column_bitmap->insert_value(BitmapValue::empty_bitmap());
    column_bitmap->insert_value(BitmapValue(123));
    ASSERT_EQ(column_bitmap->size(), 2);
    DataTypeSerDe::FormatOptions formatOptions;
    auto ser_col = ColumnString::create();
    VectorBufferWriter buffer_writer(*ser_col.get());
    auto st = bitmap_serde->serialize_column_to_json(*column_bitmap, 0, 2, buffer_writer,
                                                     formatOptions);
    buffer_writer.commit();
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(ser_col->get_data_at(0).to_string(), "\\N,\\N") << st.to_string();

    std::vector<Slice> slices_vec;
    uint64_t num_deserialized = 0;
    auto except_column = ColumnBitmap::create();
    std::string memory_buffer;
    auto bytesize = column_bitmap->get_element(0).getSizeInBytes();
    memory_buffer.resize(bytesize);
    column_bitmap->get_element(0).write_to(const_cast<char*>(memory_buffer.data()));
    slices_vec.emplace_back(memory_buffer.data(), memory_buffer.size());

    std::string memory_buffer2;
    memory_buffer2.clear();
    bytesize = column_bitmap->get_element(1).getSizeInBytes();
    memory_buffer2.resize(bytesize);
    column_bitmap->get_element(1).write_to(const_cast<char*>(memory_buffer2.data()));
    slices_vec.emplace_back(memory_buffer2.data(), memory_buffer2.size());
    st = bitmap_serde->deserialize_column_from_json_vector(*except_column, slices_vec,
                                                           &num_deserialized, formatOptions);
    EXPECT_TRUE(st.ok()) << st.to_string();
    // check column value
    for (size_t j = 0; j < column_bitmap->size(); ++j) {
        EXPECT_EQ(column_bitmap->get_element(j).to_string(),
                  except_column->get_element(j).to_string())
                << j;
    }
    std::cout << "test serialize/deserialize_column_from_json_vector" << std::endl;
}

TEST(BitmapSerdeTest, SerdeArrowTest) {
    auto bitmap_serde = std::make_shared<vectorized::DataTypeBitMapSerDe>(1);
    auto column_bitmap = ColumnBitmap::create();
    column_bitmap->insert_value(BitmapValue::empty_bitmap());
    column_bitmap->insert_value(BitmapValue(123));
    ASSERT_EQ(column_bitmap->size(), 2);

    auto block = std::make_shared<Block>();
    DataTypePtr st = std::make_shared<DataTypeBitMap>();
    vectorized::ColumnWithTypeAndName type_and_name(column_bitmap->get_ptr(), st, "bitmap");
    block->insert(type_and_name);
    std::shared_ptr<arrow::RecordBatch> record_batch =
            CommonDataTypeSerdeTest::serialize_arrow(block);
    EXPECT_EQ(record_batch->column(0)->ToString(), "[\n  00,\n  017B000000\n]");

    // TODO, support `DataTypeBitMapSerDe::read_column_from_arrow`
    // CommonDataTypeSerdeTest::deserialize_arrow(assert_block, record_batch);
    // CommonDataTypeSerdeTest::compare_two_blocks(block, assert_block);
    std::cout << "test write/read_column_to_arrow " << std::endl;
}

} // namespace doris::vectorized
