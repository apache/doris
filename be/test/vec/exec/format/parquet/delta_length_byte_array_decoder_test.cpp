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

#include <gtest/gtest.h>

#include "parquet/encoding.h"
#include "parquet/schema.h"
#include "parquet/types.h"
#include "util/slice.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/exec/format/parquet/delta_bit_pack_decoder.h"

namespace doris::vectorized {

class DeltaLengthByteArrayDecoderTest : public ::testing::Test {
protected:
    void SetUp() override { _decoder = std::make_unique<DeltaLengthByteArrayDecoder>(); }

    std::unique_ptr<DeltaLengthByteArrayDecoder> _decoder;
};

// Test basic decoding functionality
TEST_F(DeltaLengthByteArrayDecoderTest, test_basic_decode) {
    // Create ColumnDescriptor
    auto node = parquet::schema::PrimitiveNode::Make("test_column", parquet::Repetition::REQUIRED,
                                                     parquet::Type::BYTE_ARRAY);
    auto descr = std::make_shared<parquet::ColumnDescriptor>(node, 0, 0);

    // Prepare original data
    std::vector<std::string> values = {"Hello", "World", "Foobar", "ABCDEF"};
    std::vector<parquet::ByteArray> byte_array_values;
    for (const auto& value : values) {
        byte_array_values.emplace_back(
                parquet::ByteArray {static_cast<uint32_t>(value.size()),
                                    reinterpret_cast<const uint8_t*>(value.data())});
    }

    // Create encoder
    auto encoder =
            MakeTypedEncoder<parquet::ByteArrayType>(parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY,
                                                     /*use_dictionary=*/false, descr.get());

    // Put data into encoder
    ASSERT_NO_THROW(
            encoder->Put(byte_array_values.data(), static_cast<int>(byte_array_values.size())));

    // Get encoded data
    auto encoded_buffer = encoder->FlushValues();
    Slice data_slice(encoded_buffer->data(), encoded_buffer->size());
    ASSERT_TRUE(_decoder->set_data(&data_slice).ok());

    // Create column and data type
    MutableColumnPtr column = ColumnString::create();
    DataTypePtr data_type = std::make_shared<DataTypeString>();

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
    auto* result_column = assert_cast<ColumnString*>(column.get());
    for (size_t i = 0; i < num_values; ++i) {
        EXPECT_EQ(result_column->get_data_at(i).to_string(), values[i]);
    }
}

// Test decoding with filter
TEST_F(DeltaLengthByteArrayDecoderTest, test_decode_with_filter) {
    // Create ColumnDescriptor
    auto node = parquet::schema::PrimitiveNode::Make("test_column", parquet::Repetition::REQUIRED,
                                                     parquet::Type::BYTE_ARRAY);
    auto descr = std::make_shared<parquet::ColumnDescriptor>(node, 0, 0);

    // Prepare original data
    std::vector<std::string> values = {"Hello", "World", "Foobar", "ABCDEF"};
    std::vector<parquet::ByteArray> byte_array_values;
    for (const auto& value : values) {
        byte_array_values.emplace_back(
                parquet::ByteArray {static_cast<uint32_t>(value.size()),
                                    reinterpret_cast<const uint8_t*>(value.data())});
    }

    // Create encoder
    auto encoder =
            MakeTypedEncoder<parquet::ByteArrayType>(parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY,
                                                     /*use_dictionary=*/false, descr.get());

    // Put data into encoder
    ASSERT_NO_THROW(
            encoder->Put(byte_array_values.data(), static_cast<int>(byte_array_values.size())));

    // Get encoded data
    auto encoded_buffer = encoder->FlushValues();
    Slice data_slice(encoded_buffer->data(), encoded_buffer->size());
    ASSERT_TRUE(_decoder->set_data(&data_slice).ok());

    // Create column and data type
    MutableColumnPtr column = ColumnString::create();
    DataTypePtr data_type = std::make_shared<DataTypeString>();

    // Create filter vector [1, 0, 1, 0]
    size_t num_values = values.size();
    std::vector<uint16_t> run_length_null_map(1, num_values); // All non-null
    std::vector<uint8_t> filter_data = {1, 0, 1, 0};
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());
    ColumnSelectVector select_vector;
    ASSERT_TRUE(select_vector.init(run_length_null_map, num_values, nullptr, &filter_map, 0).ok());

    // Perform decoding
    ASSERT_TRUE(_decoder->decode_values(column, data_type, select_vector, false).ok());

    // Verify results
    ASSERT_EQ(column->size(), 2); // 2 values after filtering
    auto* result_column = assert_cast<ColumnString*>(column.get());
    EXPECT_EQ(result_column->get_data_at(0).to_string(), "Hello");
    EXPECT_EQ(result_column->get_data_at(1).to_string(), "Foobar");
}

// Test decoding with filter and null values
TEST_F(DeltaLengthByteArrayDecoderTest, test_decode_with_filter_and_null) {
    // Create ColumnDescriptor
    auto node = parquet::schema::PrimitiveNode::Make("test_column", parquet::Repetition::REQUIRED,
                                                     parquet::Type::BYTE_ARRAY);
    auto descr = std::make_shared<parquet::ColumnDescriptor>(node, 0, 0);

    // Prepare original data
    std::vector<std::string> values = {"Hello", "World", "ABCDEF"};
    std::vector<parquet::ByteArray> byte_array_values;
    for (const auto& value : values) {
        byte_array_values.emplace_back(
                parquet::ByteArray {static_cast<uint32_t>(value.size()),
                                    reinterpret_cast<const uint8_t*>(value.data())});
    }

    // Create encoder
    auto encoder =
            MakeTypedEncoder<parquet::ByteArrayType>(parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY,
                                                     /*use_dictionary=*/false, descr.get());

    // Put data into encoder
    ASSERT_NO_THROW(
            encoder->Put(byte_array_values.data(), static_cast<int>(byte_array_values.size())));

    // Get encoded data
    auto encoded_buffer = encoder->FlushValues();
    Slice data_slice(encoded_buffer->data(), encoded_buffer->size());
    ASSERT_TRUE(_decoder->set_data(&data_slice).ok());

    // Create column and data type
    MutableColumnPtr column = ColumnString::create();
    DataTypePtr data_type = std::make_shared<DataTypeString>();

    // Create filter vector [1, 0, 1, 0] and null vector [0, 0, 1, 0]
    size_t num_values = 4;
    std::vector<uint16_t> run_length_null_map = {2, 1,
                                                 1}; // data: ["Hello", "World", null, "ABCDEF"]
    std::vector<uint8_t> filter_data = {1, 0, 1, 0}; // filtered_data: ["Hello", null]

    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());
    ColumnSelectVector select_vector;
    NullMap null_map;
    ASSERT_TRUE(
            select_vector.init(run_length_null_map, num_values, &null_map, &filter_map, 0).ok());

    // Perform decoding
    ASSERT_TRUE(_decoder->decode_values(column, data_type, select_vector, false).ok());

    // Verify results
    ASSERT_EQ(column->size(), 2); // 2 values after filtering
    auto* result_column = assert_cast<ColumnString*>(column.get());

    // Expected values after filtering and null handling
    std::vector<std::optional<std::string>> expected_values = {"Hello", std::nullopt};
    for (size_t i = 0; i < expected_values.size(); ++i) {
        if (expected_values[i].has_value()) {
            EXPECT_EQ(result_column->get_data_at(i).to_string(), expected_values[i].value())
                    << "Mismatch at value " << i;
            EXPECT_FALSE(null_map[i]) << "Expected non-null at position " << i;
        } else {
            EXPECT_TRUE(null_map[i]) << "Expected null at position " << i;
        }
    }
}

// Test decoding with invalid data
TEST_F(DeltaLengthByteArrayDecoderTest, test_invalid_data) {
    // Prepare invalid encoded data
    std::vector<uint8_t> encoded_data = {0x80, 0x01, 0x04, 0x05, 0x14}; // Incomplete data
    Slice data_slice(reinterpret_cast<char*>(encoded_data.data()), encoded_data.size());
    ASSERT_FALSE(_decoder->set_data(&data_slice).ok());
}

// Test skipping values for delta length byte array decoding
TEST_F(DeltaLengthByteArrayDecoderTest, test_skip_value) {
    // Create ColumnDescriptor
    auto node = parquet::schema::PrimitiveNode::Make("test_column", parquet::Repetition::REQUIRED,
                                                     parquet::Type::BYTE_ARRAY);
    auto descr = std::make_shared<parquet::ColumnDescriptor>(node, 0, 0);

    // Prepare original data
    std::vector<std::string> values = {"Hello", "World", "Foobar", "ABCDEF", "Test", "Skip"};
    std::vector<parquet::ByteArray> byte_array_values;
    for (const auto& value : values) {
        byte_array_values.emplace_back(
                parquet::ByteArray {static_cast<uint32_t>(value.size()),
                                    reinterpret_cast<const uint8_t*>(value.data())});
    }

    // Create encoder
    auto encoder =
            MakeTypedEncoder<parquet::ByteArrayType>(parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY,
                                                     /*use_dictionary=*/false, descr.get());

    // Put data into encoder
    ASSERT_NO_THROW(
            encoder->Put(byte_array_values.data(), static_cast<int>(byte_array_values.size())));

    // Get encoded data
    auto encoded_buffer = encoder->FlushValues();
    Slice data_slice(encoded_buffer->data(), encoded_buffer->size());
    ASSERT_TRUE(_decoder->set_data(&data_slice).ok());

    // Skip first 3 values
    ASSERT_TRUE(_decoder->skip_values(3).ok());

    // Create column and data type
    MutableColumnPtr column = ColumnString::create();
    DataTypePtr data_type = std::make_shared<DataTypeString>();

    // Create selection vector
    size_t num_values = values.size() - 3;                    // Total 6 values, skip 3, remaining 3
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
    auto* result_column = assert_cast<ColumnString*>(column.get());

    // Expected values after skipping first 3 values ("Hello", "World", "Foobar")
    std::vector<std::string> expected_values = {"ABCDEF", "Test", "Skip"};
    for (size_t i = 0; i < num_values; ++i) {
        EXPECT_EQ(result_column->get_data_at(i).to_string(), expected_values[i])
                << "Mismatch at value " << i;
    }
}

} // namespace doris::vectorized
