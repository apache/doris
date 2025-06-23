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

#include "vec/exec/format/parquet/delta_bit_pack_decoder.h"

#include <gtest/gtest.h>

#include "parquet/encoding.h"
#include "parquet/schema.h"
#include "parquet/types.h"
#include "util/slice.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

class DeltaBitPackDecoderTest : public ::testing::Test {
protected:
    void SetUp() override { _decoder = std::make_unique<DeltaBitPackDecoder<int32_t>>(); }

    std::unique_ptr<DeltaBitPackDecoder<int32_t>> _decoder;
};

// Test basic decoding functionality
TEST_F(DeltaBitPackDecoderTest, test_basic_decode) {
    // Prepare encoded data
    std::vector<uint8_t> encoded_data = {
            // Header: block_size=128, mini_blocks_per_block=4, total_value_count=5, first_value=10
            0x80, 0x01, 0x04, 0x05, 0x14,
            // Block: min_delta=1, bit_width=[0, 0, 0, 0]
            0x02, 0x00, 0x00, 0x00, 0x00
            // MiniBlocks: no data needed for bit_width 0
    };
    Slice data_slice(reinterpret_cast<char*>(encoded_data.data()), encoded_data.size());
    ASSERT_TRUE(_decoder->set_data(&data_slice).ok());

    MutableColumnPtr column = ColumnInt32::create();
    DataTypePtr data_type = std::make_shared<DataTypeInt32>();

    // Create selection vector without filter
    size_t num_values = 5;
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
    auto* result_column = assert_cast<ColumnInt32*>(column.get());
    EXPECT_EQ(result_column->get_data()[0], 10);
    EXPECT_EQ(result_column->get_data()[1], 11);
    EXPECT_EQ(result_column->get_data()[2], 12);
    EXPECT_EQ(result_column->get_data()[3], 13);
    EXPECT_EQ(result_column->get_data()[4], 14);
}

// Test decoding with filter
TEST_F(DeltaBitPackDecoderTest, test_decode_with_filter) {
    // Prepare encoded data
    std::vector<uint8_t> encoded_data = {
            // Header: block_size=128, mini_blocks_per_block=4, total_value_count=5, first_value=10
            0x80, 0x01, 0x04, 0x05, 0x14,
            // Block: min_delta=1, bit_width=[0, 0, 0, 0]
            0x02, 0x00, 0x00, 0x00, 0x00
            // MiniBlocks: no data needed for bit_width 0
    };
    Slice data_slice(reinterpret_cast<char*>(encoded_data.data()), encoded_data.size());
    ASSERT_TRUE(_decoder->set_data(&data_slice).ok());

    MutableColumnPtr column = ColumnInt32::create();
    DataTypePtr data_type = std::make_shared<DataTypeInt32>();

    // Create filter vector [1,0,1,0,1]
    size_t num_values = 5;
    std::vector<uint16_t> run_length_null_map(1, num_values); // All non-null
    std::vector<uint8_t> filter_data = {1, 0, 1, 0, 1};
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());
    ColumnSelectVector select_vector;
    ASSERT_TRUE(select_vector.init(run_length_null_map, num_values, nullptr, &filter_map, 0).ok());

    // Perform decoding
    ASSERT_TRUE(_decoder->decode_values(column, data_type, select_vector, false).ok());

    // Verify results
    ASSERT_EQ(column->size(), 3); // 3 values after filtering
    auto* result_column = assert_cast<ColumnInt32*>(column.get());
    EXPECT_EQ(result_column->get_data()[0], 10);
    EXPECT_EQ(result_column->get_data()[1], 12);
    EXPECT_EQ(result_column->get_data()[2], 14);
}

// Test decoding with filter and null values
TEST_F(DeltaBitPackDecoderTest, test_decode_with_filter_and_null) {
    std::vector<uint8_t> encoded_data = {
            // Header: block_size=128, mini_blocks_per_block=4, total_value_count=4, first_value=10
            0x80, 0x01, 0x04, 0x04, 0x14,
            // Block: min_delta=1, bit_width=[1, 0, 0, 0]
            0x02, 0x01, 0x00, 0x00, 0x00,
            // MiniBlocks
            0x02, 0x00, 0x00, 0x00};
    Slice data_slice(reinterpret_cast<char*>(encoded_data.data()), encoded_data.size());
    ASSERT_TRUE(_decoder->set_data(&data_slice).ok());

    MutableColumnPtr column = ColumnInt32::create();
    DataTypePtr data_type = std::make_shared<DataTypeInt32>();

    // Create filter vector [1,0,1,0,1] and null vector [0,0,1,0,0]
    size_t num_values = 5;
    std::vector<uint16_t> run_length_null_map = {2, 1, 2}; // data: [10 11 null 13 14]
    std::vector<uint8_t> filter_data = {1, 0, 1, 0, 1};    // filtered_data: [10 null 14]

    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());
    ColumnSelectVector select_vector;
    NullMap null_map;
    ASSERT_TRUE(
            select_vector.init(run_length_null_map, num_values, &null_map, &filter_map, 0).ok());

    // Perform decoding
    ASSERT_TRUE(_decoder->decode_values(column, data_type, select_vector, false).ok());

    // Verify results
    ASSERT_EQ(column->size(), 3); // 3 values after filtering
    auto* result_column = assert_cast<ColumnInt32*>(column.get());

    // Expected values after filtering and null handling
    std::vector<std::optional<int32_t>> expected_values = {10, std::nullopt, 14};
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

// Test skipping values for delta bit pack decoding
TEST_F(DeltaBitPackDecoderTest, test_skip_value) {
    // Prepare encoded data
    std::vector<uint8_t> encoded_data = {
            // Header: block_size=128, mini_blocks_per_block=4, total_value_count=8, first_value=10
            0x80, 0x01, 0x04, 0x08, 0x14,
            // Block: min_delta=1, bit_width=[0, 0, 0, 0]
            0x02, 0x00, 0x00, 0x00, 0x00
            // MiniBlocks: no data needed for bit_width 0
    };
    Slice data_slice(reinterpret_cast<char*>(encoded_data.data()), encoded_data.size());
    ASSERT_TRUE(_decoder->set_data(&data_slice).ok());

    // Skip first 3 values
    ASSERT_TRUE(_decoder->skip_values(3).ok());

    // Create column and data type
    MutableColumnPtr column = ColumnInt32::create();
    DataTypePtr data_type = std::make_shared<DataTypeInt32>();

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
    auto* result_column = assert_cast<ColumnInt32*>(column.get());

    // Expected values after skipping first 3 values (10,11,12)
    std::vector<int32_t> expected_values = {13, 14, 15, 16, 17};
    for (size_t i = 0; i < num_values; ++i) {
        EXPECT_EQ(result_column->get_data()[i], expected_values[i]) << "Mismatch at value " << i;
    }
}

// Test decoding data generated by arrow
TEST_F(DeltaBitPackDecoderTest, test_data_generated_by_arrow) {
    // Create ColumnDescriptor
    auto node = parquet::schema::PrimitiveNode::Make("test_column", parquet::Repetition::REQUIRED,
                                                     parquet::Type::INT32);
    auto descr = std::make_shared<parquet::ColumnDescriptor>(node, 0, 0);

    // Prepare original data
    std::vector<int32_t> values = {10, 11, 13, 14};

    // Create encoder
    auto encoder = MakeTypedEncoder<parquet::Int32Type>(parquet::Encoding::DELTA_BINARY_PACKED,
                                                        /*use_dictionary=*/false, descr.get());

    // Put data into encoder
    ASSERT_NO_THROW(encoder->Put(values.data(), static_cast<int>(values.size())));

    // Get encoded data
    auto encoded_buffer = encoder->FlushValues();

    Slice data_slice(encoded_buffer->data(), encoded_buffer->size());
    ASSERT_TRUE(_decoder->set_data(&data_slice).ok());

    // Create column and data type
    MutableColumnPtr column = ColumnInt32::create();
    DataTypePtr data_type = std::make_shared<DataTypeInt32>();

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
    auto* result_column = assert_cast<ColumnInt32*>(column.get());
    for (size_t i = 0; i < num_values; ++i) {
        EXPECT_EQ(result_column->get_data()[i], values[i]);
    }
}

// Test invalid data case
TEST_F(DeltaBitPackDecoderTest, test_invalid_data) {
    // Prepare invalid encoded data
    std::vector<uint8_t> encoded_data = {0x80, 0x01, 0x04, 0x05, 0x14}; // Incomplete data
    Slice data_slice(reinterpret_cast<char*>(encoded_data.data()), encoded_data.size());
    ASSERT_TRUE(_decoder->set_data(&data_slice).ok());

    MutableColumnPtr column = ColumnInt32::create();
    DataTypePtr data_type = std::make_shared<DataTypeInt32>();

    size_t num_values = 5;
    std::vector<uint16_t> run_length_null_map(1, num_values);
    std::vector<uint8_t> filter_data(num_values, 1);
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());
    ColumnSelectVector select_vector;
    ASSERT_TRUE(select_vector.init(run_length_null_map, num_values, nullptr, &filter_map, 0).ok());

    // Decoding should fail due to invalid data
    ASSERT_FALSE(_decoder->decode_values(column, data_type, select_vector, false).ok());
}

} // namespace doris::vectorized
