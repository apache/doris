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
#include "vec/columns/column_complex.h"
#include "vec/data_types/serde/data_type_quantilestate_serde.h"

namespace doris::vectorized {

TEST(QuantileStateSerdeTest, writeColumnToMysql) {
    auto quantile_state_serde = std::make_shared<vectorized::DataTypeQuantileStateSerDe>(1);
    auto column_quantile_state = ColumnQuantileState::create();
    column_quantile_state->insert_value(QuantileState());
    ASSERT_EQ(column_quantile_state->size(), 1);
    MysqlRowBuffer<false> mysql_rb;
    DataTypeSerDe::FormatOptions options;
    options.nested_string_wrapper = "\"";
    options.wrapper_len = 1;
    options.map_key_delim = ':';
    options.null_format = "null";
    options.null_len = 4;
    quantile_state_serde->set_return_object_as_string(true);
    auto st = quantile_state_serde->write_column_to_mysql(*column_quantile_state, mysql_rb, 0,
                                                          false, options);
    EXPECT_TRUE(st.ok());
    ASSERT_EQ(mysql_rb.length(), 6);

    QuantileState quantile_state;
    quantile_state.add_value(123);
    column_quantile_state->insert_value(quantile_state);
    quantile_state_serde->set_return_object_as_string(true);
    st = quantile_state_serde->write_column_to_mysql(*column_quantile_state, mysql_rb, 1, false,
                                                     options);
    EXPECT_TRUE(st.ok());
    ASSERT_EQ(mysql_rb.length(), 20);
    std::cout << "test write_column_to_mysql success" << std::endl;
}

TEST(QuantileStateSerdeTest, writeOneCellToJsonb) {
    auto quantile_state_serde = std::make_shared<vectorized::DataTypeQuantileStateSerDe>(1);
    auto column_quantile_state = ColumnQuantileState::create();
    QuantileState quantile_state;
    quantile_state.add_value(123);
    column_quantile_state->insert_value(quantile_state);
    ASSERT_EQ(column_quantile_state->size(), 1);
    JsonbWriterT<JsonbOutStream> jsonb_writer;
    Arena pool;
    jsonb_writer.writeStartObject();
    quantile_state_serde->write_one_cell_to_jsonb(*column_quantile_state, jsonb_writer, pool, 0, 0);
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
        quantile_state_serde->read_one_cell_from_jsonb(*column_quantile_state, it->value());
    }
    EXPECT_TRUE(column_quantile_state->size() == 2);
    QuantileState data = column_quantile_state->get_element(1);
    EXPECT_EQ(data.get_value_by_percentile(1), 123);
    std::cout << "test write/read_one_cell_to_jsonb success" << std::endl;
}

TEST(QuantileStateSerdeTest, writeColumnToPb) {
    auto quantile_state_serde = std::make_shared<vectorized::DataTypeQuantileStateSerDe>(1);
    auto column_quantile_state = ColumnQuantileState::create();
    column_quantile_state->insert_value(QuantileState());
    QuantileState quantile_state;
    quantile_state.add_value(123);
    column_quantile_state->insert_value(quantile_state);
    ASSERT_EQ(column_quantile_state->size(), 2);
    PValues pv = PValues();
    Status st = quantile_state_serde->write_column_to_pb(*column_quantile_state, pv, 0,
                                                         column_quantile_state->size());
    EXPECT_TRUE(st.ok());

    auto except_column = ColumnQuantileState::create();
    st = quantile_state_serde->read_column_from_pb(*except_column, pv);
    EXPECT_TRUE(st.ok()) << st.to_string();
    // check pb value from expected column
    PValues as_pv = PValues();
    st = quantile_state_serde->write_column_to_pb(*except_column, as_pv, 0, except_column->size());
    EXPECT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(pv.bytes_value_size(), as_pv.bytes_value_size());
    // check column value
    for (size_t j = 0; j < column_quantile_state->size(); ++j) {
        EXPECT_EQ(column_quantile_state->get_data_at(j), except_column->get_data_at(j));
    }
    std::cout << "test write/read_column_to_pb" << std::endl;
}

TEST(QuantileStateSerdeTest, serializeOneCellToJson) {
    auto quantile_state_serde = std::make_shared<vectorized::DataTypeQuantileStateSerDe>(1);
    auto column_quantile_state = ColumnQuantileState::create();
    column_quantile_state->insert_value(QuantileState());
    QuantileState quantile_state;
    quantile_state.add_value(123);
    column_quantile_state->insert_value(quantile_state);
    ASSERT_EQ(column_quantile_state->size(), 2);
    DataTypeSerDe::FormatOptions formatOptions;
    auto ser_col = ColumnString::create();
    VectorBufferWriter buffer_writer(*ser_col.get());
    auto st = quantile_state_serde->serialize_one_cell_to_json(*column_quantile_state, 0,
                                                               buffer_writer, formatOptions);
    buffer_writer.commit();
    st = quantile_state_serde->serialize_one_cell_to_json(*column_quantile_state, 1, buffer_writer,
                                                          formatOptions);
    buffer_writer.commit();
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(ser_col->get_data_at(0).to_string(), "\\N") << st.to_string();
    EXPECT_EQ(ser_col->get_data_at(1).to_string(), "\\N") << st.to_string();
    std::cout << "test serialize_one_cell_to_json success" << std::endl;
}

TEST(QuantileStateSerdeTest, serializeColumnToJson) {
    auto quantile_state_serde = std::make_shared<vectorized::DataTypeQuantileStateSerDe>(1);
    auto column_quantile_state = ColumnQuantileState::create();
    column_quantile_state->insert_value(QuantileState());
    QuantileState quantile_state;
    quantile_state.add_value(123);
    column_quantile_state->insert_value(quantile_state);
    ASSERT_EQ(column_quantile_state->size(), 2);
    DataTypeSerDe::FormatOptions formatOptions;
    auto ser_col = ColumnString::create();
    VectorBufferWriter buffer_writer(*ser_col.get());
    auto st = quantile_state_serde->serialize_column_to_json(*column_quantile_state, 0, 2,
                                                             buffer_writer, formatOptions);
    buffer_writer.commit();
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(ser_col->get_data_at(0).to_string(), "\\N,\\N") << st.to_string();

    std::vector<Slice> slices_vec;
    auto except_column = ColumnQuantileState::create();
    std::string memory_buffer;
    auto bytesize = column_quantile_state->get_element(0).get_serialized_size();
    memory_buffer.resize(bytesize);
    column_quantile_state->get_element(0).serialize((uint8_t*)(memory_buffer.data()));
    slices_vec.emplace_back(memory_buffer.data(), memory_buffer.size());
    QuantileState quantile_state_res_0;
    auto res = quantile_state_res_0.deserialize(slices_vec[0]);
    EXPECT_TRUE(res);

    std::string memory_buffer2;
    memory_buffer2.clear();
    bytesize = column_quantile_state->get_element(1).get_serialized_size();
    memory_buffer2.resize(bytesize);
    column_quantile_state->get_element(1).serialize((uint8_t*)(memory_buffer2.data()));
    slices_vec.emplace_back(memory_buffer2.data(), memory_buffer2.size());
    QuantileState quantile_state_res_1;
    res = quantile_state_res_1.deserialize(slices_vec[1]);
    EXPECT_TRUE(res);
    EXPECT_EQ(quantile_state.get_value_by_percentile(1),
              quantile_state_res_1.get_value_by_percentile(1));
    std::cout << "test serialize/deserialize_column_from_json_vector" << std::endl;
}
} // namespace doris::vectorized