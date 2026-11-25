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

#include <arrow/api.h>
#include <cctz/time_zone.h>
#include <gen_cpp/types.pb.h>
#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <orc/OrcFile.hh>
#include <string>

#include "core/arena.h"
#include "core/column/column_complex.h"
#include "core/column/column_string.h"
#include "core/data_type/common_data_type_serder_test.h"
#include "core/data_type/data_type_hll.h"
#include "core/data_type_serde/data_type_hll_serde.h"
#include "core/string_buffer.hpp"
#include "core/value/hll.h"
#include "util/jsonb_writer.h"
#include "util/slice.h"

namespace doris {

namespace {

struct HllSerdeCase {
    uint64_t hash_count;
    HllDataType type;
    size_t serialized_size;
};

constexpr std::array<HllSerdeCase, 5> hll_serde_cases = {{
        {0, HLL_DATA_EMPTY, 1},
        {1, HLL_DATA_EXPLICIT, 10},
        {160, HLL_DATA_EXPLICIT, 1282},
        // These sequential hashes address distinct registers.
        {200, HLL_DATA_SPARSE, 605},
        {5000, HLL_DATA_FULL, 16385},
}};

ColumnHLL::MutablePtr make_hll_column() {
    auto column = ColumnHLL::create();
    for (const auto& test_case : hll_serde_cases) {
        auto& hll = column->get_data().emplace_back();
        for (uint64_t hash = 0; hash < test_case.hash_count; ++hash) {
            hll.update(hash);
        }
    }
    return column;
}

std::string serialize_hll(const HyperLogLog& value) {
    std::string bytes(value.max_serialized_size(), '\0');
    bytes.resize(value.serialize(reinterpret_cast<uint8_t*>(bytes.data())));
    return bytes;
}

std::string expected_hll_bytes(const HyperLogLog& value, size_t row) {
    const auto& test_case = hll_serde_cases[row];
    if (test_case.type == HLL_DATA_SPARSE) {
        EXPECT_EQ(value.max_serialized_size(), 16385);
    }
    auto bytes = serialize_hll(value);
    EXPECT_EQ(bytes.size(), test_case.serialized_size);
    EXPECT_EQ(static_cast<uint8_t>(bytes.front()), test_case.type);
    return bytes;
}

} // namespace

TEST(HLLSerdeTest, writeOneCellToJsonb) {
    auto hll_serde = std::make_shared<DataTypeHLLSerDe>(1);
    auto column_hll = ColumnHLL::create();
    HyperLogLog hll;
    hll.update(123);
    column_hll->insert_value(hll);
    ASSERT_EQ(column_hll->size(), 1);
    JsonbWriterT<JsonbOutStream> jsonb_writer;
    Arena pool;
    DataTypeSerDe::FormatOptions options;
    auto tz = cctz::utc_time_zone();
    options.timezone = &tz;
    jsonb_writer.writeStartObject();
    hll_serde->write_one_cell_to_jsonb(*column_hll, jsonb_writer, pool, 0, 0, options);
    jsonb_writer.writeEndObject();

    auto jsonb_column = ColumnString::create();
    jsonb_column->insert_data(jsonb_writer.getOutput()->getBuffer(),
                              jsonb_writer.getOutput()->getSize());
    StringRef jsonb_data = jsonb_column->get_data_at(0);
    const JsonbDocument* pdoc = nullptr;
    auto st = JsonbDocument::checkAndCreateDocument(jsonb_data.data, jsonb_data.size, &pdoc);
    ASSERT_TRUE(st.ok()) << "checkAndCreateDocument failed: " << st.to_string();
    const JsonbDocument& doc = *pdoc;
    for (auto it = doc->begin(); it != doc->end(); ++it) {
        hll_serde->read_one_cell_from_jsonb(*column_hll, it->value());
    }
    EXPECT_TRUE(column_hll->size() == 2);
    HyperLogLog data = column_hll->get_element(1);
    EXPECT_EQ(data.to_string(), "hash set size: 1\ncardinality:\t1\ntype:\t1");
}

TEST(HLLSerdeTest, writeColumnToPb) {
    auto hll_serde = std::make_shared<DataTypeHLLSerDe>(1);
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
    auto hll_serde = std::make_shared<DataTypeHLLSerDe>(1);
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
    auto hll_serde = std::make_shared<DataTypeHLLSerDe>(1);
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
    auto hll_serde = std::make_shared<DataTypeHLLSerDe>(1);
    auto column_hll = ColumnHLL::create();
    column_hll->insert_value(HyperLogLog::empty());
    HyperLogLog hll;
    hll.update(123);
    column_hll->insert_value(hll);
    ASSERT_EQ(column_hll->size(), 2);

    auto block = std::make_shared<Block>();
    DataTypePtr st = std::make_shared<DataTypeHLL>();
    ColumnWithTypeAndName type_and_name(column_hll->get_ptr(), st, "hll");
    block->insert(type_and_name);
    std::shared_ptr<arrow::RecordBatch> record_batch =
            CommonDataTypeSerdeTest::serialize_arrow(block);
    EXPECT_EQ(record_batch->column(0)->ToString(), "[\n  00,\n  01017B00000000000000\n]");

    // TODO, support `DataTypeHLLSerDe::read_column_from_arrow`
    // CommonDataTypeSerdeTest::deserialize_arrow(assert_block, record_batch);
    // CommonDataTypeSerdeTest::compare_two_blocks(block, assert_block);
    std::cout << "test write/read_column_to_arrow " << std::endl;
}

TEST(HLLSerdeTest, SparseAndFullPbRoundTrip) {
    DataTypeHLLSerDe serde;
    auto column = make_hll_column();
    const int end = static_cast<int>(column->size());
    for (const int start : {0, 2}) {
        PValues pv;
        auto status = serde.write_column_to_pb(*column, pv, start, end);
        ASSERT_TRUE(status.ok()) << status.to_string();
        ASSERT_EQ(pv.bytes_value_size(), end - start);

        auto decoded = ColumnHLL::create();
        status = serde.read_column_from_pb(*decoded, pv);
        ASSERT_TRUE(status.ok()) << status.to_string();
        ASSERT_EQ(decoded->size(), end - start);

        for (int idx = 0; idx < pv.bytes_value_size(); ++idx) {
            const int row = start + idx;
            SCOPED_TRACE(row);
            const auto& original = column->get_element(row);
            const auto expected_bytes = expected_hll_bytes(original, row);
            ASSERT_EQ(pv.bytes_value(idx).size(), expected_bytes.size());
            EXPECT_EQ(pv.bytes_value(idx), expected_bytes);
            ASSERT_TRUE(HyperLogLog::is_valid(Slice(pv.bytes_value(idx))));
            const auto& round_tripped = decoded->get_element(idx);
            EXPECT_EQ(round_tripped.estimate_cardinality(), original.estimate_cardinality());
            EXPECT_EQ(serialize_hll(round_tripped), expected_bytes);
        }
    }
}

TEST(HLLSerdeTest, SparseAndFullArrowSerialize) {
    DataTypeHLLSerDe serde;
    auto column = make_hll_column();
    column->insert_default();
    NullMap null_map(column->size(), 0);
    null_map.back() = 1;
    arrow::BinaryBuilder builder;
    auto status = serde.write_column_to_arrow(*column, &null_map, &builder, 0, column->size(),
                                              cctz::utc_time_zone());
    ASSERT_TRUE(status.ok()) << status.to_string();
    std::shared_ptr<arrow::Array> result;
    ASSERT_TRUE(builder.Finish(&result).ok());
    const auto& array = static_cast<const arrow::BinaryArray&>(*result);
    ASSERT_EQ(array.length(), column->size());

    for (int64_t row = 0; row < array.length(); ++row) {
        SCOPED_TRACE(row);
        ASSERT_EQ(array.IsNull(row), null_map[row] != 0);
        if (null_map[row]) {
            continue;
        }
        const auto expected_bytes = expected_hll_bytes(column->get_element(row), row);
        const auto bytes = array.GetView(row);
        ASSERT_EQ(bytes.size(), expected_bytes.size());
        EXPECT_EQ(bytes, expected_bytes);
        const Slice slice(bytes.data(), bytes.size());
        ASSERT_TRUE(HyperLogLog::is_valid(slice));
        HyperLogLog decoded;
        EXPECT_TRUE(decoded.deserialize(slice));
    }
}

TEST(HLLSerdeTest, SparseAndFullMysqlTextSerialize) {
    DataTypeHLLSerDe serde;
    auto column = make_hll_column();
    auto output = ColumnString::create();
    VectorBufferWriter writer(*output);
    DataTypeSerDe::FormatOptions options;

    serde.set_return_object_as_string(false);
    for (size_t row = 0; row < column->size(); ++row) {
        EXPECT_FALSE(serde.write_column_to_mysql_text(*column, writer, row, options));
    }
    ASSERT_EQ(output->get_chars().size(), 0);

    serde.set_return_object_as_string(true);
    for (size_t row = 0; row < column->size(); ++row) {
        SCOPED_TRACE(row);
        ASSERT_TRUE(serde.write_column_to_mysql_text(*column, writer, row, options));
        writer.commit();
        const auto expected_bytes = expected_hll_bytes(column->get_element(row), row);
        const auto bytes = output->get_data_at(row);
        ASSERT_EQ(bytes.size, expected_bytes.size());
        EXPECT_EQ(bytes.to_string(), expected_bytes);
    }
}

TEST(HLLSerdeTest, SparseAndFullOrcSerialize) {
    DataTypeHLLSerDe serde;
    auto column = make_hll_column();
    const size_t null_row = column->size();
    column->insert_default();
    const size_t end = column->size();
    Arena arena;
    orc::StringVectorBatch batch(end, *orc::getDefaultPool());
    batch.hasNulls = true;
    for (size_t row = 0; row < end; ++row) {
        batch.notNull[row] = row != null_row;
    }
    batch.data[null_row] = nullptr;
    batch.length[null_row] = 0;

    DataTypeSerDe::FormatOptions options;
    batch.numElements = end;
    auto status =
            serde.write_column_to_orc("UTC", *column, nullptr, &batch, end, end, arena, options);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(batch.numElements, 0);

    const size_t start = 0;
    status = serde.write_column_to_orc("UTC", *column, nullptr, &batch, start, end, arena, options);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(batch.numElements, end - start);
    EXPECT_EQ(batch.data[null_row], nullptr);
    EXPECT_EQ(batch.length[null_row], 0);
    for (size_t row = start; row < null_row; ++row) {
        SCOPED_TRACE(row);
        const auto expected_bytes = expected_hll_bytes(column->get_element(row), row);
        ASSERT_EQ(batch.length[row], expected_bytes.size());
        EXPECT_EQ(memcmp(batch.data[row], expected_bytes.data(), expected_bytes.size()), 0);
        EXPECT_TRUE(HyperLogLog::is_valid(Slice(batch.data[row], batch.length[row])));
    }
}
} // namespace doris
