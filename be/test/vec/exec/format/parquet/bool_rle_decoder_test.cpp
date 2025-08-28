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

#include "vec/exec/format/parquet/bool_rle_decoder.h"

#include <gtest/gtest.h>

#include "parquet/encoding.h"
#include "parquet/schema.h"
#include "parquet/types.h"
#include "util/slice.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

class BoolRLEDecoderTest : public ::testing::Test {
protected:
    void SetUp() override { _decoder = std::make_unique<BoolRLEDecoder>(); }

    std::unique_ptr<BoolRLEDecoder> _decoder;
};

// Test basic decoding functionality
TEST_F(BoolRLEDecoderTest, test_basic_decode) {
    // Prepare encoded data: [true, false, true, true, false, false, false, true]
    std::vector<uint8_t> encoded_data = {0x02, 0x00, 0x00, 0x00, 0x03, 0x8d};
    Slice data_slice(reinterpret_cast<char*>(encoded_data.data()), encoded_data.size());
    ASSERT_TRUE(_decoder->set_data(&data_slice).ok());

    MutableColumnPtr column = ColumnUInt8::create();
    DataTypePtr data_type = std::make_shared<DataTypeUInt8>();

    // Create selection vector without filter
    size_t num_values = 8;
    std::vector<uint16_t> run_length_null_map(1, num_values); // All non-null
    std::vector<uint8_t> filter_data(num_values, 1);
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());
    ColumnSelectVector select_vector;
    ASSERT_TRUE(select_vector.init(run_length_null_map, num_values, nullptr, &filter_map, 0).ok());

    // Perform decoding
    ASSERT_TRUE(_decoder->decode_values(column, data_type, select_vector, false).ok());

    // Verify results
    ASSERT_EQ(column->size(), num_values);
    auto* result_column = assert_cast<ColumnUInt8*>(column.get());
    EXPECT_EQ(result_column->get_data()[0], 1);
    EXPECT_EQ(result_column->get_data()[1], 0);
    EXPECT_EQ(result_column->get_data()[2], 1);
    EXPECT_EQ(result_column->get_data()[3], 1);
    EXPECT_EQ(result_column->get_data()[4], 0);
    EXPECT_EQ(result_column->get_data()[5], 0);
    EXPECT_EQ(result_column->get_data()[6], 0);
    EXPECT_EQ(result_column->get_data()[7], 1);
}

// Test decoding with filter
TEST_F(BoolRLEDecoderTest, test_decode_with_filter) {
    // Prepare encoded data: [true, false, true, true, false, false, false, true]
    std::vector<uint8_t> encoded_data = {0x02, 0x00, 0x00, 0x00, 0x03, 0x8d};
    Slice data_slice(reinterpret_cast<char*>(encoded_data.data()), encoded_data.size());
    ASSERT_TRUE(_decoder->set_data(&data_slice).ok());

    MutableColumnPtr column = ColumnUInt8::create();
    DataTypePtr data_type = std::make_shared<DataTypeUInt8>();

    // Create filter vector [1, 0, 1, 0, 1, 0, 1, 0]
    size_t num_values = 8;
    std::vector<uint16_t> run_length_null_map(1, num_values); // All non-null
    std::vector<uint8_t> filter_data = {1, 0, 1, 0, 1, 0, 1, 0};
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());
    ColumnSelectVector select_vector;
    ASSERT_TRUE(select_vector.init(run_length_null_map, num_values, nullptr, &filter_map, 0).ok());

    // Perform decoding
    ASSERT_TRUE(_decoder->decode_values(column, data_type, select_vector, false).ok());

    // Verify results
    ASSERT_EQ(column->size(), 4); // 4 values after filtering
    auto* result_column = assert_cast<ColumnUInt8*>(column.get());
    EXPECT_EQ(result_column->get_data()[0], 1);
    EXPECT_EQ(result_column->get_data()[1], 1);
    EXPECT_EQ(result_column->get_data()[2], 0);
    EXPECT_EQ(result_column->get_data()[3], 0);
}

// Test decoding with filter and null values
TEST_F(BoolRLEDecoderTest, test_decode_with_filter_and_null) {
    // Prepare encoded data: [true, false, true, true, false, false, false, true]
    std::vector<uint8_t> encoded_data = {0x02, 0x00, 0x00, 0x00, 0x03, 0x25};
    Slice data_slice(reinterpret_cast<char*>(encoded_data.data()), encoded_data.size());
    ASSERT_TRUE(_decoder->set_data(&data_slice).ok());

    MutableColumnPtr column = ColumnUInt8::create();
    DataTypePtr data_type = std::make_shared<DataTypeUInt8>();

    // Create filter vector [1, 0, 1, 0, 1, 0, 1, 0] and null vector [0, 0, 1, 0, 0, 0, 1, 0]
    size_t num_values = 8;
    std::vector<uint16_t> run_length_null_map = {
            2, 1, 3, 1, 1}; // data: [true, false, null, true, false, false, null, true]
    std::vector<uint8_t> filter_data = {1, 0, 1, 0,
                                        1, 0, 1, 0}; // filtered_data: [true, null, false, null]

    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());
    ColumnSelectVector select_vector;
    NullMap null_map;
    ASSERT_TRUE(
            select_vector.init(run_length_null_map, num_values, &null_map, &filter_map, 0).ok());

    // Perform decoding
    ASSERT_TRUE(_decoder->decode_values(column, data_type, select_vector, false).ok());

    // Verify results
    ASSERT_EQ(column->size(), 4); // 4 values after filtering
    auto* result_column = assert_cast<ColumnUInt8*>(column.get());

    // Expected values after filtering and null handling
    std::vector<std::optional<uint8_t>> expected_values = {1, std::nullopt, 0, std::nullopt};
    for (size_t i = 0; i < expected_values.size(); ++i) {
        if (expected_values[i].has_value()) {
            EXPECT_EQ(result_column->get_data()[i], expected_values[i].value())
                    << "Mismatch at value " << i;
            EXPECT_FALSE(null_map[i]) << "Expected non-null at position " << i;
        } else {
            EXPECT_TRUE(null_map[i]) << "Expected null at position " << i;
        }
    }
}

// Test skipping values for bool RLE decoding
TEST_F(BoolRLEDecoderTest, test_skip_value) {
    // Prepare encoded data: [true, false, true, true, false, false, false, true]
    std::vector<uint8_t> encoded_data = {0x02, 0x00, 0x00, 0x00, 0x03, 0x8d};
    Slice data_slice(reinterpret_cast<char*>(encoded_data.data()), encoded_data.size());
    ASSERT_TRUE(_decoder->set_data(&data_slice).ok());

    // Skip first 3 values
    ASSERT_TRUE(_decoder->skip_values(3).ok());

    // Create column and data type
    MutableColumnPtr column = ColumnUInt8::create();
    DataTypePtr data_type = std::make_shared<DataTypeUInt8>();

    // Create selection vector
    size_t num_values = 5;                                    // Total 8 values, skip 3, remaining 5
    std::vector<uint16_t> run_length_null_map(1, num_values); // All non-null
    std::vector<uint8_t> filter_data(num_values, 1);
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());
    ColumnSelectVector select_vector;
    ASSERT_TRUE(select_vector.init(run_length_null_map, num_values, nullptr, &filter_map, 0).ok());

    // Perform decoding
    ASSERT_TRUE(_decoder->decode_values(column, data_type, select_vector, false).ok());

    // Verify results
    ASSERT_EQ(column->size(), num_values);
    auto* result_column = assert_cast<ColumnUInt8*>(column.get());

    // Expected values after skipping first 3 values (true, false, true)
    std::vector<uint8_t> expected_values = {1, 0, 0, 0, 1};
    for (size_t i = 0; i < num_values; ++i) {
        EXPECT_EQ(result_column->get_data()[i], expected_values[i]) << "Mismatch at value " << i;
    }
}

// Test decoding data generated by arrow
TEST_F(BoolRLEDecoderTest, test_data_generated_by_arrow) {
    // Create ColumnDescriptor
    auto node = parquet::schema::PrimitiveNode::Make("test_column", parquet::Repetition::REQUIRED,
                                                     parquet::Type::BOOLEAN);
    auto descr = std::make_shared<parquet::ColumnDescriptor>(node, 0, 0);

    // Prepare original data
    std::vector<bool> values = {true, false, true, true, false, false, false, true};

    // Create encoder
    auto encoder = MakeTypedEncoder<parquet::BooleanType>(parquet::Encoding::RLE,
                                                          /*use_dictionary=*/false, descr.get());

    // Put data into encoder
    ASSERT_NO_THROW(encoder->Put(values, static_cast<int>(values.size())));

    // Get encoded data
    auto encoded_buffer = encoder->FlushValues();

    Slice data_slice(encoded_buffer->data(), encoded_buffer->size());
    ASSERT_TRUE(_decoder->set_data(&data_slice).ok());

    // Create column and data type
    MutableColumnPtr column = ColumnUInt8::create();
    DataTypePtr data_type = std::make_shared<DataTypeUInt8>();

    // Create selection vector
    size_t num_values = values.size();
    std::vector<uint16_t> run_length_null_map(1, num_values); // All non-null
    std::vector<uint8_t> filter_data(num_values, 1);
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());
    ColumnSelectVector select_vector;
    ASSERT_TRUE(select_vector.init(run_length_null_map, num_values, nullptr, &filter_map, 0).ok());

    // Perform decoding
    ASSERT_TRUE(_decoder->decode_values(column, data_type, select_vector, false).ok());

    // Verify results
    ASSERT_EQ(column->size(), num_values);
    auto* result_column = assert_cast<ColumnUInt8*>(column.get());
    EXPECT_EQ(result_column->get_data()[0], 1);
    EXPECT_EQ(result_column->get_data()[1], 0);
    EXPECT_EQ(result_column->get_data()[2], 1);
    EXPECT_EQ(result_column->get_data()[3], 1);
    EXPECT_EQ(result_column->get_data()[4], 0);
    EXPECT_EQ(result_column->get_data()[5], 0);
    EXPECT_EQ(result_column->get_data()[6], 0);
    EXPECT_EQ(result_column->get_data()[7], 1);
}

// Test invalid data case
TEST_F(BoolRLEDecoderTest, test_invalid_data) {
    // Prepare invalid encoded data
    std::vector<uint8_t> encoded_data = {0x08, 0x01}; // Incomplete data
    Slice data_slice(reinterpret_cast<char*>(encoded_data.data()), encoded_data.size());
    ASSERT_FALSE(_decoder->set_data(&data_slice).ok());
}

} // namespace doris::vectorized
