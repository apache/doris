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

#include <arrow/array.h>
#include <arrow/builder.h>
#include <gtest/gtest.h>

#include "arrow/api.h"
#include "parquet/encoding.h"
#include "parquet/schema.h"
#include "parquet/types.h"
#include "util/slice.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/exec/format/parquet/delta_bit_pack_decoder.h"

namespace doris::vectorized {

class DeltaByteArrayDecoderTest : public ::testing::Test {
protected:
    void SetUp() override { _decoder = std::make_unique<DeltaByteArrayDecoder>(); }

    std::unique_ptr<DeltaByteArrayDecoder> _decoder;
};

// Test basic decoding byte array functionality
TEST_F(DeltaByteArrayDecoderTest, test_basic_decode_byte_array) {
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
    auto encoder = MakeTypedEncoder<parquet::ByteArrayType>(parquet::Encoding::DELTA_BYTE_ARRAY,
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

// Test decoding byte array with filter
TEST_F(DeltaByteArrayDecoderTest, test_decode_byte_array_with_filter) {
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
    auto encoder = MakeTypedEncoder<parquet::ByteArrayType>(parquet::Encoding::DELTA_BYTE_ARRAY,
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

// Test decoding byte array with filter and null values
TEST_F(DeltaByteArrayDecoderTest, test_decode_byte_array_with_filter_and_null) {
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
    auto encoder = MakeTypedEncoder<parquet::ByteArrayType>(parquet::Encoding::DELTA_BYTE_ARRAY,
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

// Test skipping values for byte array decoding
TEST_F(DeltaByteArrayDecoderTest, test_skip_value_for_byte_array) {
    // Create ColumnDescriptor
    auto node = parquet::schema::PrimitiveNode::Make("test_column", parquet::Repetition::REQUIRED,
                                                     parquet::Type::BYTE_ARRAY);
    auto descr = std::make_shared<parquet::ColumnDescriptor>(node, 0, 0);

    // Prepare test data
    std::vector<std::string> values = {"Hello", "World", "Foobar", "ABCDEF"};
    std::vector<parquet::ByteArray> byte_array_values;
    for (const auto& value : values) {
        byte_array_values.emplace_back(
                parquet::ByteArray {static_cast<uint32_t>(value.size()),
                                    reinterpret_cast<const uint8_t*>(value.data())});
    }

    // Encode data
    auto encoder = MakeTypedEncoder<parquet::ByteArrayType>(parquet::Encoding::DELTA_BYTE_ARRAY,
                                                            /*use_dictionary=*/false, descr.get());
    ASSERT_NO_THROW(
            encoder->Put(byte_array_values.data(), static_cast<int>(byte_array_values.size())));
    auto encoded_buffer = encoder->FlushValues();

    // Set decoder data
    Slice data_slice(encoded_buffer->data(), encoded_buffer->size());
    ASSERT_TRUE(_decoder->set_data(&data_slice).ok());

    // Skip the first two values
    ASSERT_TRUE(_decoder->skip_values(2).ok());

    // Create column and data type
    MutableColumnPtr column = ColumnString::create();
    DataTypePtr data_type = std::make_shared<DataTypeString>();

    // Create selection vector
    size_t num_values = values.size() - 2; // Skip first two values
    std::vector<uint16_t> run_length_null_map(1, num_values);
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

    // Verify decoded results (should start from the third value)
    for (size_t i = 0; i < num_values; ++i) {
        EXPECT_EQ(result_column->get_data_at(i).to_string(), values[i + 2])
                << "Mismatch at value " << (i + 2);
    }
}

// Test basic decoding fixed-length byte array functionality
TEST_F(DeltaByteArrayDecoderTest, test_basic_decode_fixed_len_byte_array) {
    // Configure DECIMAL type parameters
    const int32_t type_length = 16;
    int precision = 10;
    int scale = 2;
    _decoder->set_type_length(type_length);

    // Create ColumnDescriptor
    auto node = parquet::schema::PrimitiveNode::Make(
            "test_column", parquet::Repetition::REQUIRED, parquet::Type::FIXED_LEN_BYTE_ARRAY,
            parquet::ConvertedType::DECIMAL, type_length, precision, scale);
    auto descr = std::make_shared<parquet::ColumnDescriptor>(node, 0, 0);

    // Prepare test data
    std::vector<std::vector<uint8_t>> test_fixed_len_buffers = {
            {0x1a, 0x05, 0x06, 0x1b, 0x00, 0x00, 0x00, 0x13, 0x1c, 0x00, 0x00, 0x00, 0x00, 0xbc,
             0x61, 0x40}, // Data 1
            {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
             0x00, 0x00}, // Data 2 (all zeros)
            {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
             0xFF, 0xFF}, // Data 3 (all ones)
            {0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC,
             0xDE, 0xF0} // Data 4 (random)
    };

    std::vector<parquet::ByteArray> byte_array_values;
    for (const auto& buffer : test_fixed_len_buffers) {
        byte_array_values.emplace_back(
                parquet::ByteArray {static_cast<uint32_t>(buffer.size()), buffer.data()});
    }

    // Encode data
    auto encoder = MakeTypedEncoder<parquet::ByteArrayType>(parquet::Encoding::DELTA_BYTE_ARRAY,
                                                            /*use_dictionary=*/false, descr.get());
    ASSERT_NO_THROW(
            encoder->Put(byte_array_values.data(), static_cast<int>(byte_array_values.size())));
    auto encoded_buffer = encoder->FlushValues();

    // Set decoder data
    Slice data_slice(encoded_buffer->data(), encoded_buffer->size());
    ASSERT_TRUE(_decoder->set_data(&data_slice).ok());

    // Create column and data type
    MutableColumnPtr column = ColumnInt8::create();
    DataTypePtr data_type = std::make_shared<DataTypeInt8>();

    // Create selection vector
    size_t num_values = test_fixed_len_buffers.size();
    std::vector<uint16_t> run_length_null_map(1, num_values);
    std::vector<uint8_t> filter_data(num_values, 1);
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());
    ColumnSelectVector select_vector;
    ASSERT_TRUE(select_vector.init(run_length_null_map, num_values, nullptr, &filter_map, 0).ok());

    // Perform decoding
    ASSERT_TRUE(_decoder->decode_values(column, data_type, select_vector, false).ok());

    // Verify results
    ASSERT_EQ(column->size(), num_values * type_length);
    auto* result_column = assert_cast<ColumnInt8*>(column.get());

    // Verify decoded results one by one
    for (size_t i = 0; i < num_values; ++i) {
        for (size_t j = 0; j < type_length; ++j) {
            size_t index = i * type_length + j;
            EXPECT_EQ(result_column->get_element(index),
                      static_cast<int8_t>(test_fixed_len_buffers[i][j]))
                    << "Mismatch at buffer " << i << ", byte " << j;
        }
    }
}

// Test decoding fixed-length byte array with filter
TEST_F(DeltaByteArrayDecoderTest, test_decode_fixed_len_byte_array_with_filter) {
    // Configure DECIMAL type parameters
    const int32_t type_length = 16;
    int precision = 10;
    int scale = 2;
    _decoder->set_type_length(type_length);

    // Create ColumnDescriptor
    auto node = parquet::schema::PrimitiveNode::Make(
            "test_column", parquet::Repetition::REQUIRED, parquet::Type::FIXED_LEN_BYTE_ARRAY,
            parquet::ConvertedType::DECIMAL, type_length, precision, scale);
    auto descr = std::make_shared<parquet::ColumnDescriptor>(node, 0, 0);

    // Prepare test data
    std::vector<std::vector<uint8_t>> test_fixed_len_buffers = {
            {0x1a, 0x05, 0x06, 0x1b, 0x00, 0x00, 0x00, 0x13, 0x1c, 0x00, 0x00, 0x00, 0x00, 0xbc,
             0x61, 0x40}, // Data 1
            {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
             0x00, 0x00}, // Data 2 (all zeros)
            {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
             0xFF, 0xFF}, // Data 3 (all ones)
            {0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC,
             0xDE, 0xF0} // Data 4 (random)
    };

    std::vector<parquet::ByteArray> byte_array_values;
    for (const auto& buffer : test_fixed_len_buffers) {
        byte_array_values.emplace_back(
                parquet::ByteArray {static_cast<uint32_t>(buffer.size()), buffer.data()});
    }

    // Encode data
    auto encoder = MakeTypedEncoder<parquet::ByteArrayType>(parquet::Encoding::DELTA_BYTE_ARRAY,
                                                            /*use_dictionary=*/false, descr.get());
    ASSERT_NO_THROW(
            encoder->Put(byte_array_values.data(), static_cast<int>(byte_array_values.size())));
    auto encoded_buffer = encoder->FlushValues();

    // Set decoder data
    Slice data_slice(encoded_buffer->data(), encoded_buffer->size());
    ASSERT_TRUE(_decoder->set_data(&data_slice).ok());

    // Create column and data type
    MutableColumnPtr column = ColumnInt8::create();
    DataTypePtr data_type = std::make_shared<DataTypeInt8>();

    // Create filter [1, 0, 1, 0]
    size_t num_values = test_fixed_len_buffers.size();
    std::vector<uint16_t> run_length_null_map(1, num_values);
    std::vector<uint8_t> filter_data = {1, 0, 1, 0};
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());
    ColumnSelectVector select_vector;
    ASSERT_TRUE(select_vector.init(run_length_null_map, num_values, nullptr, &filter_map, 0).ok());

    // Perform decoding
    ASSERT_TRUE(_decoder->decode_values(column, data_type, select_vector, false).ok());

    // Verify results
    ASSERT_EQ(column->size(), 2 * type_length); // 2 values after filtering
    auto* result_column = assert_cast<ColumnInt8*>(column.get());

    // Verify first value
    for (size_t j = 0; j < type_length; ++j) {
        EXPECT_EQ(result_column->get_element(j), static_cast<int8_t>(test_fixed_len_buffers[0][j]))
                << "Mismatch at buffer 0, byte " << j;
    }

    // Verify third value
    for (size_t j = 0; j < type_length; ++j) {
        size_t index = type_length + j;
        EXPECT_EQ(result_column->get_element(index),
                  static_cast<int8_t>(test_fixed_len_buffers[2][j]))
                << "Mismatch at buffer 2, byte " << j;
    }
}

// Test decoding fixed-length byte array with filter and null values
TEST_F(DeltaByteArrayDecoderTest, test_decode_fixed_len_byte_array_with_filter_and_null) {
    // Configure DECIMAL type parameters
    const int32_t type_length = 16;
    int precision = 10;
    int scale = 2;
    _decoder->set_type_length(type_length);

    // Create ColumnDescriptor
    auto node = parquet::schema::PrimitiveNode::Make(
            "test_column", parquet::Repetition::REQUIRED, parquet::Type::FIXED_LEN_BYTE_ARRAY,
            parquet::ConvertedType::DECIMAL, type_length, precision, scale);
    auto descr = std::make_shared<parquet::ColumnDescriptor>(node, 0, 0);

    // Prepare test data
    std::vector<std::vector<uint8_t>> test_fixed_len_buffers = {
            {0x1a, 0x05, 0x06, 0x1b, 0x00, 0x00, 0x00, 0x13, 0x1c, 0x00, 0x00, 0x00, 0x00, 0xbc,
             0x61, 0x40}, // Data 1
            {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
             0x00, 0x00}, // Data 2 (all zeros)
            {0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC,
             0xDE, 0xF0} // Data 4 (random)
    };

    std::vector<parquet::ByteArray> byte_array_values;
    for (const auto& buffer : test_fixed_len_buffers) {
        byte_array_values.emplace_back(
                parquet::ByteArray {static_cast<uint32_t>(buffer.size()), buffer.data()});
    }

    // Encode data
    auto encoder = MakeTypedEncoder<parquet::ByteArrayType>(parquet::Encoding::DELTA_BYTE_ARRAY,
                                                            /*use_dictionary=*/false, descr.get());
    ASSERT_NO_THROW(
            encoder->Put(byte_array_values.data(), static_cast<int>(byte_array_values.size())));
    auto encoded_buffer = encoder->FlushValues();

    // Set decoder data
    Slice data_slice(encoded_buffer->data(), encoded_buffer->size());
    ASSERT_TRUE(_decoder->set_data(&data_slice).ok());

    // Create column and data type
    MutableColumnPtr column = ColumnInt8::create();
    DataTypePtr data_type = std::make_shared<DataTypeInt8>();

    // Create filter [1, 0, 1, 0] and null vector [0, 0, 1, 0]
    size_t num_values = 4;
    std::vector<uint16_t> run_length_null_map = {2, 1, 1}; // Data: [Data 1, Data 2, null, Data 4]
    std::vector<uint8_t> filter_data = {1, 0, 1, 0};       // Filtered data: [Data 1, null]

    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());
    ColumnSelectVector select_vector;
    NullMap null_map;
    ASSERT_TRUE(
            select_vector.init(run_length_null_map, num_values, &null_map, &filter_map, 0).ok());

    // Perform decoding
    ASSERT_TRUE(_decoder->decode_values(column, data_type, select_vector, false).ok());

    // Verify results
    ASSERT_EQ(column->size(), 2 * type_length); // 2 values after filtering (Data 1 and null)
    auto* result_column = assert_cast<ColumnInt8*>(column.get());

    // Expected values after filtering and null handling
    std::vector<std::optional<std::vector<uint8_t>>> expected_values;
    expected_values.push_back(std::vector<uint8_t> {0x1a, 0x05, 0x06, 0x1b, 0x00, 0x00, 0x00, 0x13,
                                                    0x1c, 0x00, 0x00, 0x00, 0x00, 0xbc, 0x61,
                                                    0x40}); // Data 1
    expected_values.push_back(std::nullopt); // Only filtered values (Data 1 and null)

    // Verify results
    size_t filtered_index = 0;
    for (size_t i = 0; i < expected_values.size(); ++i) {
        if (expected_values[i].has_value()) {
            for (size_t j = 0; j < type_length; ++j) {
                size_t index = filtered_index * type_length + j;
                EXPECT_EQ(result_column->get_element(index),
                          static_cast<int8_t>(expected_values[i].value()[j]))
                        << "Mismatch at filtered value " << i << ", byte " << j;
            }
            EXPECT_FALSE(null_map[filtered_index])
                    << "Expected non-null at filtered position " << filtered_index;
            filtered_index++;
        } else {
            EXPECT_TRUE(null_map[filtered_index])
                    << "Expected null at filtered position " << filtered_index;
            filtered_index++;
        }
    }
}

// Test skipping values for fixed-length byte array decoding
TEST_F(DeltaByteArrayDecoderTest, test_skip_value_for_fixed_len_byte_array) {
    // Configure DECIMAL type parameters
    const int32_t type_length = 16;
    int precision = 10;
    int scale = 2;
    _decoder->set_type_length(type_length);

    // Create ColumnDescriptor
    auto node = parquet::schema::PrimitiveNode::Make(
            "test_column", parquet::Repetition::REQUIRED, parquet::Type::FIXED_LEN_BYTE_ARRAY,
            parquet::ConvertedType::DECIMAL, type_length, precision, scale);
    auto descr = std::make_shared<parquet::ColumnDescriptor>(node, 0, 0);

    // Prepare test data
    std::vector<std::vector<uint8_t>> test_fixed_len_buffers = {
            {0x1a, 0x05, 0x06, 0x1b, 0x00, 0x00, 0x00, 0x13, 0x1c, 0x00, 0x00, 0x00, 0x00, 0xbc,
             0x61, 0x40}, // Data 1
            {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
             0x00, 0x00}, // Data 2 (all zeros)
            {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
             0xFF, 0xFF}, // Data 3 (all ones)
            {0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC,
             0xDE, 0xF0} // Data 4 (random)
    };

    std::vector<parquet::ByteArray> byte_array_values;
    for (const auto& buffer : test_fixed_len_buffers) {
        byte_array_values.emplace_back(
                parquet::ByteArray {static_cast<uint32_t>(buffer.size()), buffer.data()});
    }

    // Encode data
    auto encoder = MakeTypedEncoder<parquet::ByteArrayType>(parquet::Encoding::DELTA_BYTE_ARRAY,
                                                            /*use_dictionary=*/false, descr.get());
    ASSERT_NO_THROW(
            encoder->Put(byte_array_values.data(), static_cast<int>(byte_array_values.size())));
    auto encoded_buffer = encoder->FlushValues();

    // Set decoder data
    Slice data_slice(encoded_buffer->data(), encoded_buffer->size());
    ASSERT_TRUE(_decoder->set_data(&data_slice).ok());

    // Skip the first two values
    ASSERT_TRUE(_decoder->skip_values(2).ok());

    // Create column and data type
    MutableColumnPtr column = ColumnInt8::create();
    DataTypePtr data_type = std::make_shared<DataTypeInt8>();

    // Create selection vector
    size_t num_values = test_fixed_len_buffers.size() - 2; // Skip first two values
    std::vector<uint16_t> run_length_null_map(1, num_values);
    std::vector<uint8_t> filter_data(num_values, 1);
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());
    ColumnSelectVector select_vector;
    ASSERT_TRUE(select_vector.init(run_length_null_map, num_values, nullptr, &filter_map, 0).ok());

    // Perform decoding
    ASSERT_TRUE(_decoder->decode_values(column, data_type, select_vector, false).ok());

    // Verify results
    ASSERT_EQ(column->size(), num_values * type_length);
    auto* result_column = assert_cast<ColumnInt8*>(column.get());

    // Verify decoded results (should start from the third value)
    for (size_t i = 0; i < num_values; ++i) {
        for (size_t j = 0; j < type_length; ++j) {
            size_t index = i * type_length + j;
            EXPECT_EQ(result_column->get_element(index),
                      static_cast<int8_t>(test_fixed_len_buffers[i + 2][j]))
                    << "Mismatch at buffer " << (i + 2) << ", byte " << j;
        }
    }
}

// Test decoding with invalid data
TEST_F(DeltaByteArrayDecoderTest, test_invalid_data) {
    // Prepare invalid encoded data
    std::vector<uint8_t> encoded_data = {0x80, 0x01, 0x04, 0x05, 0x14}; // Incomplete data
    Slice data_slice(reinterpret_cast<char*>(encoded_data.data()), encoded_data.size());
    ASSERT_FALSE(_decoder->set_data(&data_slice).ok());
}

} // namespace doris::vectorized
