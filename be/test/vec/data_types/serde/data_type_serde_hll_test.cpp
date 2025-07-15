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

#include "olap/hll.h"
#include "util/slice.h"
#include "vec/columns/column_complex.h"
#include "vec/data_types/common_data_type_serder_test.h"
#include "vec/data_types/data_type_hll.h"
#include "vec/data_types/serde/data_type_hll_serde.h"

namespace doris::vectorized {

TEST(HLLSerdeTest, writeColumnToMysql) {
    auto hll_serde = std::make_shared<vectorized::DataTypeHLLSerDe>(1);
    auto column_hll = ColumnHLL::create();
    column_hll->insert_value(HyperLogLog::empty());
    ASSERT_EQ(column_hll->size(), 1);
    MysqlRowBuffer<false> mysql_rb;
    DataTypeSerDe::FormatOptions options;
    options.nested_string_wrapper = "\"";
    options.wrapper_len = 1;
    options.map_key_delim = ':';
    options.null_format = "null";
    options.null_len = 4;
    hll_serde->set_return_object_as_string(true);
    auto st = hll_serde->write_column_to_mysql(*column_hll, mysql_rb, 0, false, options);
    EXPECT_TRUE(st.ok());
    ASSERT_EQ(mysql_rb.length(), 2);

    HyperLogLog hll;
    hll.update(123);
    column_hll->insert_value(hll);
    hll_serde->set_return_object_as_string(true);
    st = hll_serde->write_column_to_mysql(*column_hll, mysql_rb, 1, false, options);
    EXPECT_TRUE(st.ok());
    ASSERT_EQ(mysql_rb.length(), 13);
    std::cout << "test write_column_to_mysql success" << std::endl;
}

TEST(HLLSerdeTest, writeOneCellToJsonb) {
    auto hll_serde = std::make_shared<vectorized::DataTypeHLLSerDe>(1);
    auto column_hll = ColumnHLL::create();
    HyperLogLog hll;
    hll.update(123);
    column_hll->insert_value(hll);
    ASSERT_EQ(column_hll->size(), 1);
    JsonbWriterT<JsonbOutStream> jsonb_writer;
    Arena pool;
    jsonb_writer.writeStartObject();
    hll_serde->write_one_cell_to_jsonb(*column_hll, jsonb_writer, pool, 0, 0);
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
        hll_serde->read_one_cell_from_jsonb(*column_hll, it->value());
    }
    EXPECT_TRUE(column_hll->size() == 2);
    HyperLogLog data = column_hll->get_element(1);
    EXPECT_EQ(data.to_string(), "hash set size: 1\ncardinality:\t1\ntype:\t1");
}

TEST(HLLSerdeTest, writeColumnToPb) {
    auto hll_serde = std::make_shared<vectorized::DataTypeHLLSerDe>(1);
    auto column_hll = ColumnHLL::create();
    column_hll->insert_value(HyperLogLog::empty());
    HyperLogLog hll;
    hll.update(123);
    column_hll->insert_value(hll);
    ASSERT_EQ(column_hll->size(), 2);
    PValues pv = PValues();
    Status st = hll_serde->write_column_to_pb(*column_hll, pv, 0, column_hll->size());
    EXPECT_TRUE(st.ok());

    auto except_column = ColumnHLL::create();
    st = hll_serde->read_column_from_pb(*except_column, pv);
    EXPECT_TRUE(st.ok()) << st.to_string();
    // check pb value from expected column
    PValues as_pv = PValues();
    st = hll_serde->write_column_to_pb(*except_column, as_pv, 0, except_column->size());
    EXPECT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(pv.bytes_value_size(), as_pv.bytes_value_size());
    // check column value
    for (size_t j = 0; j < column_hll->size(); ++j) {
        EXPECT_EQ(column_hll->get_element(j).to_string(),
                  except_column->get_element(j).to_string());
    }
    std::cout << "test write/read_column_to_pb" << std::endl;
}

TEST(HLLSerdeTest, serializeOneCellToJson) {
    auto hll_serde = std::make_shared<vectorized::DataTypeHLLSerDe>(1);
    auto column_hll = ColumnHLL::create();
    column_hll->insert_value(HyperLogLog::empty());
    HyperLogLog hll;
    hll.update(123);
    column_hll->insert_value(hll);
    ASSERT_EQ(column_hll->size(), 2);
    DataTypeSerDe::FormatOptions formatOptions;
    auto ser_col = ColumnString::create();
    VectorBufferWriter buffer_writer(*ser_col.get());
    auto st = hll_serde->serialize_one_cell_to_json(*column_hll, 0, buffer_writer, formatOptions);
    buffer_writer.commit();
    st = hll_serde->serialize_one_cell_to_json(*column_hll, 1, buffer_writer, formatOptions);
    buffer_writer.commit();
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(ser_col->get_data_at(0).to_string(), "\\N") << st.to_string();
    EXPECT_EQ(ser_col->get_data_at(1).to_string(), "\\N") << st.to_string();

    auto except_column = ColumnHLL::create();
    std::string memory_buffer;
    auto bytesize = column_hll->get_element(0).max_serialized_size();
    memory_buffer.resize(bytesize);
    size_t actual_size = column_hll->get_element(0).serialize((uint8_t*)(memory_buffer.data()));
    memory_buffer.resize(actual_size);
    Slice slice_value(memory_buffer.data(), memory_buffer.size());
    st = hll_serde->deserialize_one_cell_from_json(*except_column, slice_value, formatOptions);
    EXPECT_TRUE(st.ok()) << st.to_string();

    memory_buffer.clear();
    bytesize = column_hll->get_element(1).max_serialized_size();
    memory_buffer.resize(bytesize);
    actual_size = column_hll->get_element(1).serialize((uint8_t*)(memory_buffer.data()));
    memory_buffer.resize(actual_size);
    slice_value = Slice(memory_buffer.data(), memory_buffer.size());
    st = hll_serde->deserialize_one_cell_from_json(*except_column, slice_value, formatOptions);
    EXPECT_TRUE(st.ok()) << st.to_string();
    // check column value
    for (size_t j = 0; j < column_hll->size(); ++j) {
        EXPECT_EQ(column_hll->get_element(j).to_string(),
                  except_column->get_element(j).to_string());
    }
    std::cout << "test serialize/deserialize_one_cell_from_json" << std::endl;
}

TEST(HLLSerdeTest, serializeColumnToJson) {
    auto hll_serde = std::make_shared<vectorized::DataTypeHLLSerDe>(1);
    auto column_hll = ColumnHLL::create();
    column_hll->insert_value(HyperLogLog::empty());
    HyperLogLog hll;
    hll.update(123);
    column_hll->insert_value(hll);
    ASSERT_EQ(column_hll->size(), 2);
    DataTypeSerDe::FormatOptions formatOptions;
    auto ser_col = ColumnString::create();
    VectorBufferWriter buffer_writer(*ser_col.get());
    auto st = hll_serde->serialize_column_to_json(*column_hll, 0, 2, buffer_writer, formatOptions);
    buffer_writer.commit();
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(ser_col->get_data_at(0).to_string(), "\\N,\\N") << st.to_string();

    std::vector<Slice> slices_vec;
    uint64_t num_deserialized = 0;
    auto except_column = ColumnHLL::create();
    std::string memory_buffer;
    auto bytesize = column_hll->get_element(0).max_serialized_size();
    memory_buffer.resize(bytesize);
    size_t actual_size = column_hll->get_element(0).serialize((uint8_t*)(memory_buffer.data()));
    memory_buffer.resize(actual_size);
    slices_vec.emplace_back(memory_buffer.data(), memory_buffer.size());

    std::string memory_buffer2;
    bytesize = column_hll->get_element(1).max_serialized_size();
    memory_buffer2.resize(bytesize);
    actual_size = column_hll->get_element(1).serialize((uint8_t*)(memory_buffer2.data()));
    memory_buffer2.resize(actual_size);
    slices_vec.emplace_back(memory_buffer2.data(), memory_buffer2.size());
    st = hll_serde->deserialize_column_from_json_vector(*except_column, slices_vec,
                                                        &num_deserialized, formatOptions);
    EXPECT_TRUE(st.ok()) << st.to_string();
    // check column value
    for (size_t j = 0; j < column_hll->size(); ++j) {
        EXPECT_EQ(column_hll->get_element(j).to_string(),
                  except_column->get_element(j).to_string());
    }
    std::cout << "test serialize/deserialize_column_from_json_vector" << std::endl;
}

TEST(HLLSerdeTest, SerdeArrowTest) {
    auto hll_serde = std::make_shared<vectorized::DataTypeHLLSerDe>(1);
    auto column_hll = ColumnHLL::create();
    column_hll->insert_value(HyperLogLog::empty());
    HyperLogLog hll;
    hll.update(123);
    column_hll->insert_value(hll);
    ASSERT_EQ(column_hll->size(), 2);

    auto block = std::make_shared<Block>();
    DataTypePtr st = std::make_shared<DataTypeHLL>();
    vectorized::ColumnWithTypeAndName type_and_name(column_hll->get_ptr(), st, "hll");
    block->insert(type_and_name);
    std::shared_ptr<arrow::RecordBatch> record_batch =
            CommonDataTypeSerdeTest::serialize_arrow(block);
    EXPECT_EQ(record_batch->column(0)->ToString(), "[\n  00,\n  01017B00000000000000\n]");

    // TODO, support `DataTypeHLLSerDe::read_column_from_arrow`
    // CommonDataTypeSerdeTest::deserialize_arrow(assert_block, record_batch);
    // CommonDataTypeSerdeTest::compare_two_blocks(block, assert_block);
    std::cout << "test write/read_column_to_arrow " << std::endl;
}
} // namespace doris::vectorized
