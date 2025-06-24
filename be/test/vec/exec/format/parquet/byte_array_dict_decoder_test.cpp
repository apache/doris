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

#include "vec/exec/format/parquet/byte_array_dict_decoder.h"

#include <gtest/gtest.h>

#include "parquet/encoding.h"
#include "parquet/schema.h"
#include "parquet/types.h"
#include "util/slice.h"
#include "vec/columns/column_dictionary.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

class ByteArrayDictDecoderTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Prepare test data: create a dictionary with byte array strings
        const char* values[3] = {"apple", "banana", "cherry"};
        size_t dict_size = 3;
        size_t dict_data_size = 0;

        // Calculate total dictionary data size
        for (int i = 0; i < 3; i++) {
            dict_data_size += 4 + strlen(values[i]); // 4 bytes for length + string data
        }

        auto dict_data = std::make_unique<uint8_t[]>(dict_data_size);
        size_t offset = 0;
        for (int i = 0; i < 3; i++) {
            uint32_t len = strlen(values[i]);
            encode_fixed32_le(dict_data.get() + offset, len);
            offset += 4;
            memcpy(dict_data.get() + offset, values[i], len);
            offset += len;
        }

        ASSERT_TRUE(_decoder.set_dict(dict_data, dict_data_size, dict_size).ok());
    }

    ByteArrayDictDecoder _decoder;
};

// Test basic decoding functionality
TEST_F(ByteArrayDictDecoderTest, test_basic_decode) {
    MutableColumnPtr column = ColumnString::create();
    DataTypePtr data_type = std::make_shared<DataTypeString>();

    // RLE encoded data: 4 zeros followed by 1, 2, 1, padded to 8 values, [0 0 0 0 1 2 1]
    //    std::vector<uint8_t> rle_data = {2, 8, 0, 3, 0b00011001, 0};
    std::vector<uint8_t> rle_data = {0x02, 0x03, 0x00, 0x19};

    Slice data_slice(reinterpret_cast<char*>(rle_data.data()), rle_data.size());
    ASSERT_TRUE(_decoder.set_data(&data_slice).ok());

    // Create selection vector without filter, total 7 values (4 repeated + 3 literal)
    size_t num_values = 7;
    std::vector<uint16_t> run_length_null_map(1, num_values); // All non-null
    std::vector<uint8_t> filter_data(num_values, 1);
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());
    ColumnSelectVector select_vector;
    ASSERT_TRUE(select_vector.init(run_length_null_map, num_values, nullptr, &filter_map, 0).ok());

    // Perform decoding
    ASSERT_TRUE(_decoder.decode_values(column, data_type, select_vector, false).ok());

    // Verify results
    ASSERT_EQ(column->size(), num_values);
    auto* result_column = assert_cast<ColumnString*>(column.get());

    // Verify first 4 repeated values (dict index 0 -> value "apple")
    for (int i = 0; i < 4; i++) {
        EXPECT_EQ(result_column->get_data_at(i).to_string(), "apple");
    }

    // Verify last 3 literal values
    EXPECT_EQ(result_column->get_data_at(4).to_string(), "banana");
    EXPECT_EQ(result_column->get_data_at(5).to_string(), "cherry");
    EXPECT_EQ(result_column->get_data_at(6).to_string(), "banana");
}

// Test decoding with filter
TEST_F(ByteArrayDictDecoderTest, test_decode_with_filter) {
    MutableColumnPtr column = ColumnString::create();
    DataTypePtr data_type = std::make_shared<DataTypeString>();

    // RLE encoded data: 4 zeros followed by 1, 2, 1, padded to 8 values, [0 0 0 0 1 2 1]
    std::vector<uint8_t> rle_data = {2, 8, 0, 3, 0b00011001, 0};

    Slice data_slice(reinterpret_cast<char*>(rle_data.data()), rle_data.size());
    ASSERT_TRUE(_decoder.set_data(&data_slice).ok());

    // Create filter vector [1,0,1,0,1,1,1]
    size_t num_values = 7;
    std::vector<uint16_t> run_length_null_map(1, num_values); // All non-null
    std::vector<uint8_t> filter_data = {1, 0, 1, 0, 1, 1, 1};
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());
    ColumnSelectVector select_vector;
    ASSERT_TRUE(select_vector.init(run_length_null_map, num_values, nullptr, &filter_map, 0).ok());

    // Perform decoding
    ASSERT_TRUE(_decoder.decode_values(column, data_type, select_vector, false).ok());

    // Verify results
    ASSERT_EQ(column->size(), 5); // 5 values after filtering
    auto* result_column = assert_cast<ColumnString*>(column.get());

    // Verify filtered values
    EXPECT_EQ(result_column->get_data_at(0).to_string(), "apple");
    EXPECT_EQ(result_column->get_data_at(1).to_string(), "apple");
    EXPECT_EQ(result_column->get_data_at(2).to_string(), "banana");
    EXPECT_EQ(result_column->get_data_at(3).to_string(), "cherry");
    EXPECT_EQ(result_column->get_data_at(4).to_string(), "banana");
}

// Test decoding with filter and null
TEST_F(ByteArrayDictDecoderTest, test_decode_with_filter_and_null) {
    MutableColumnPtr column = ColumnString::create();
    DataTypePtr data_type = std::make_shared<DataTypeString>();

    // RLE encoded data: 4 zeros followed by 2, padded to 8 values, [0 0 0 0 2]
    std::vector<uint8_t> rle_data = {2, 8, 0, 3, 0b00000010, 0};

    Slice data_slice(reinterpret_cast<char*>(rle_data.data()), rle_data.size());
    ASSERT_TRUE(_decoder.set_data(&data_slice).ok());

    // Create filter vector [1,0,1,0,1,1,1] and null vector [0,0,0,0,1,0,1]
    size_t num_values = 7;
    std::vector<uint16_t> run_length_null_map {4, 1, 1, 1};   // data: [0 0 0 0 null 2 null]
    std::vector<uint8_t> filter_data = {1, 0, 1, 0, 1, 1, 1}; // filtered_data: [0 0 null 2 null]

    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());
    ColumnSelectVector select_vector;
    NullMap null_map;
    ASSERT_TRUE(
            select_vector.init(run_length_null_map, num_values, &null_map, &filter_map, 0).ok());

    // Perform decoding
    ASSERT_TRUE(_decoder.decode_values(column, data_type, select_vector, false).ok());

    // Verify results
    ASSERT_EQ(column->size(), 5); // 5 values after filtering
    auto* result_column = assert_cast<ColumnString*>(column.get());

    // Expected values after filtering and null handling
    std::vector<std::optional<std::string>> expected_values = {"apple", "apple", std::nullopt,
                                                               "cherry", std::nullopt};
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

// Test empty dictionary case
TEST_F(ByteArrayDictDecoderTest, test_empty_dict) {
    ByteArrayDictDecoder empty_decoder;
    auto dict_data = std::make_unique<uint8_t[]>(0);
    ASSERT_TRUE(empty_decoder.set_dict(dict_data, 0, 0).ok());
}

// Test decoding with ColumnDictI32
TEST_F(ByteArrayDictDecoderTest, test_decode_with_column_dict_i32) {
    // Create ColumnDictI32 column
    MutableColumnPtr column = ColumnDictI32::create();
    DataTypePtr data_type = std::make_shared<DataTypeInt32>();

    // RLE encoded data: 4 zeros followed by 1, 2, 1, padded to 8 values, [0 0 0 0 1 2 1]
    std::vector<uint8_t> rle_data = {2, 8, 0, 3, 0b00011001, 0};

    Slice data_slice(reinterpret_cast<char*>(rle_data.data()), rle_data.size());
    ASSERT_TRUE(_decoder.set_data(&data_slice).ok());

    // Create selection vector without filter, total 7 values (4 repeated + 3 literal)
    const size_t num_values = 7;
    std::vector<uint16_t> run_length_null_map = {num_values}; // All non-null
    std::vector<uint8_t> filter_data(num_values, 1);
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());
    ColumnSelectVector select_vector;
    ASSERT_TRUE(select_vector.init(run_length_null_map, num_values, nullptr, &filter_map, 0).ok());

    // Perform decoding
    ASSERT_TRUE(_decoder.decode_values(column, data_type, select_vector, false).ok());

    // Verify results
    ASSERT_EQ(column->size(), num_values);
    auto* dict_column = assert_cast<ColumnDictI32*>(column.get());

    // Verify first 4 repeated values (dict index 0 -> value "apple")
    for (int i = 0; i < 4; i++) {
        EXPECT_EQ(dict_column->get_data()[i], 0);
        EXPECT_EQ(dict_column->get_value(dict_column->get_data()[i]).to_string(), "apple");
    }

    // Verify last 3 literal values
    EXPECT_EQ(dict_column->get_data()[4], 1);
    EXPECT_EQ(dict_column->get_value(dict_column->get_data()[4]).to_string(), "banana");
    EXPECT_EQ(dict_column->get_data()[5], 2);
    EXPECT_EQ(dict_column->get_value(dict_column->get_data()[5]).to_string(), "cherry");
    EXPECT_EQ(dict_column->get_data()[6], 1);
    EXPECT_EQ(dict_column->get_value(dict_column->get_data()[6]).to_string(), "banana");
}

// Test decoding with ColumnDictI32 and filter
TEST_F(ByteArrayDictDecoderTest, test_decode_with_column_dict_i32_with_filter) {
    // Create ColumnDictI32 column
    MutableColumnPtr column = ColumnDictI32::create();
    DataTypePtr data_type = std::make_shared<DataTypeInt32>();

    // RLE encoded data: 4 zeros followed by 1, 2, 1, padded to 8 values, [0 0 0 0 1 2 1]
    std::vector<uint8_t> rle_data = {2, 8, 0, 3, 0b00011001, 0};

    Slice data_slice(reinterpret_cast<char*>(rle_data.data()), rle_data.size());
    ASSERT_TRUE(_decoder.set_data(&data_slice).ok());

    // Create filter vector [1,0,1,0,1,1,1]
    const size_t num_values = 7;
    std::vector<uint16_t> run_length_null_map = {num_values}; // All non-null
    std::vector<uint8_t> filter_data = {1, 0, 1, 0, 1, 1, 1};
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());
    ColumnSelectVector select_vector;
    ASSERT_TRUE(select_vector.init(run_length_null_map, num_values, nullptr, &filter_map, 0).ok());

    // Perform decoding
    ASSERT_TRUE(_decoder.decode_values(column, data_type, select_vector, false).ok());

    // Verify results
    ASSERT_EQ(column->size(), 5); // 5 values after filtering
    auto* dict_column = assert_cast<ColumnDictI32*>(column.get());

    // Verify filtered values
    EXPECT_EQ(dict_column->get_data()[0], 0);
    EXPECT_EQ(dict_column->get_value(dict_column->get_data()[0]).to_string(), "apple");
    EXPECT_EQ(dict_column->get_data()[1], 0);
    EXPECT_EQ(dict_column->get_value(dict_column->get_data()[1]).to_string(), "apple");
    EXPECT_EQ(dict_column->get_data()[2], 1);
    EXPECT_EQ(dict_column->get_value(dict_column->get_data()[2]).to_string(), "banana");
    EXPECT_EQ(dict_column->get_data()[3], 2);
    EXPECT_EQ(dict_column->get_value(dict_column->get_data()[3]).to_string(), "cherry");
    EXPECT_EQ(dict_column->get_data()[4], 1);
    EXPECT_EQ(dict_column->get_value(dict_column->get_data()[4]).to_string(), "banana");
}

// Test decoding with ColumnDictI32 with filter and null
TEST_F(ByteArrayDictDecoderTest, test_decode_with_column_dict_i32_with_filter_and_null) {
    // Create ColumnDictI32 column
    MutableColumnPtr column = ColumnDictI32::create();
    DataTypePtr data_type = std::make_shared<DataTypeInt32>();

    // RLE encoded data: 4 zeros followed by 2, padded to 8 values, [0 0 0 0 2]
    std::vector<uint8_t> rle_data = {2, 8, 0, 3, 0b00000010, 0};

    Slice data_slice(reinterpret_cast<char*>(rle_data.data()), rle_data.size());
    ASSERT_TRUE(_decoder.set_data(&data_slice).ok());

    // Create filter vector [1,0,1,0,1,1,1] and null vector [0,0,0,0,1,0,1]
    const size_t num_values = 7;
    std::vector<uint16_t> run_length_null_map {4, 1, 1, 1};   // data: [0 0 0 0 null 2 null]
    std::vector<uint8_t> filter_data = {1, 0, 1, 0, 1, 1, 1}; // filtered_data: [0 0 null 2 null]
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());
    ColumnSelectVector select_vector;
    NullMap null_map;
    ASSERT_TRUE(
            select_vector.init(run_length_null_map, num_values, &null_map, &filter_map, 0).ok());

    // Perform decoding
    ASSERT_TRUE(_decoder.decode_values(column, data_type, select_vector, false).ok());

    // Verify results
    ASSERT_EQ(column->size(), 5); // 5 values after filtering
    auto* dict_column = assert_cast<ColumnDictI32*>(column.get());

    // Expected values after filtering and null handling
    std::vector<std::optional<std::string>> expected_values = {"apple", "apple", std::nullopt,
                                                               "cherry", std::nullopt};
    for (size_t i = 0; i < expected_values.size(); ++i) {
        if (expected_values[i].has_value()) {
            EXPECT_EQ(dict_column->get_value(dict_column->get_data()[i]).to_string(),
                      expected_values[i].value())
                    << "Mismatch at value " << i;
            EXPECT_FALSE(null_map[i]) << "Expected non-null at position " << i;
        } else {
            EXPECT_TRUE(null_map[i]) << "Expected null at position " << i;
        }
    }
}

// Test decoding with ColumnInt32
TEST_F(ByteArrayDictDecoderTest, test_decode_with_column_int_32) {
    // Create ColumnInt32 column
    MutableColumnPtr column = ColumnInt32::create();
    DataTypePtr data_type = std::make_shared<DataTypeInt32>();

    // RLE encoded data: 4 zeros followed by 1, 2, 1, padded to 8 values, [0 0 0 0 1 2 1]
    std::vector<uint8_t> rle_data = {2, 8, 0, 3, 0b00011001, 0};

    Slice data_slice(reinterpret_cast<char*>(rle_data.data()), rle_data.size());
    ASSERT_TRUE(_decoder.set_data(&data_slice).ok());

    // Create selection vector without filter, total 7 values (4 repeated + 3 literal)
    const size_t num_values = 7;
    std::vector<uint16_t> run_length_null_map = {num_values}; // All non-null
    std::vector<uint8_t> filter_data(num_values, 1);
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());
    ColumnSelectVector select_vector;
    ASSERT_TRUE(select_vector.init(run_length_null_map, num_values, nullptr, &filter_map, 0).ok());

    // Perform decoding
    ASSERT_TRUE(_decoder.decode_values(column, data_type, select_vector, true).ok());

    // Verify results
    ASSERT_EQ(column->size(), num_values);
    auto* dict_column = assert_cast<ColumnInt32*>(column.get());

    // Verify first 4 repeated values (dict index 0 -> value "apple")
    for (int i = 0; i < 4; i++) {
        EXPECT_EQ(dict_column->get_data()[i], 0);
    }

    // Verify last 3 literal values
    EXPECT_EQ(dict_column->get_data()[4], 1);
    EXPECT_EQ(dict_column->get_data()[5], 2);
    EXPECT_EQ(dict_column->get_data()[6], 1);
}

// Test decoding with ColumnInt32 and filter
TEST_F(ByteArrayDictDecoderTest, test_decode_with_column_int_32_with_filter) {
    // Create ColumnInt32 column
    MutableColumnPtr column = ColumnInt32::create();
    DataTypePtr data_type = std::make_shared<DataTypeInt32>();

    // RLE encoded data: 4 zeros followed by 1, 2, 1, padded to 8 values, [0 0 0 0 1 2 1]
    std::vector<uint8_t> rle_data = {2, 8, 0, 3, 0b00011001, 0};

    Slice data_slice(reinterpret_cast<char*>(rle_data.data()), rle_data.size());
    ASSERT_TRUE(_decoder.set_data(&data_slice).ok());

    // Create filter vector [1,0,1,0,1,1,1]
    const size_t num_values = 7;
    std::vector<uint16_t> run_length_null_map = {num_values}; // All non-null
    std::vector<uint8_t> filter_data = {1, 0, 1, 0, 1, 1, 1};
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());
    ColumnSelectVector select_vector;
    ASSERT_TRUE(select_vector.init(run_length_null_map, num_values, nullptr, &filter_map, 0).ok());

    // Perform decoding
    ASSERT_TRUE(_decoder.decode_values(column, data_type, select_vector, true).ok());

    // Verify results
    ASSERT_EQ(column->size(), 5); // 5 values after filtering
    auto* dict_column = assert_cast<ColumnInt32*>(column.get());

    // Verify filtered values
    EXPECT_EQ(dict_column->get_data()[0], 0);
    EXPECT_EQ(dict_column->get_data()[1], 0);
    EXPECT_EQ(dict_column->get_data()[2], 1);
    EXPECT_EQ(dict_column->get_data()[3], 2);
    EXPECT_EQ(dict_column->get_data()[4], 1);
}

// Test decoding with ColumnInt32 with filter and null
TEST_F(ByteArrayDictDecoderTest, test_decode_with_column_int_32_with_filter_and_null) {
    // Create ColumnInt32 column
    MutableColumnPtr column = ColumnInt32::create();
    DataTypePtr data_type = std::make_shared<DataTypeInt32>();

    // RLE encoded data: 4 zeros followed by 2, padded to 8 values, [0 0 0 0 2]
    std::vector<uint8_t> rle_data = {2, 8, 0, 3, 0b00000010, 0};

    Slice data_slice(reinterpret_cast<char*>(rle_data.data()), rle_data.size());
    ASSERT_TRUE(_decoder.set_data(&data_slice).ok());

    // Create filter vector [1,0,1,0,1,1,1] and null vector [0,0,0,0,1,0,1]
    const size_t num_values = 7;
    std::vector<uint16_t> run_length_null_map {4, 1, 1, 1};   // data: [0 0 0 0 null 2 null]
    std::vector<uint8_t> filter_data = {1, 0, 1, 0, 1, 1, 1}; // filtered_data: [0 0 null 2 null]
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());
    ColumnSelectVector select_vector;
    NullMap null_map;
    ASSERT_TRUE(
            select_vector.init(run_length_null_map, num_values, &null_map, &filter_map, 0).ok());

    // Perform decoding
    ASSERT_TRUE(_decoder.decode_values(column, data_type, select_vector, true).ok());

    // Verify results
    ASSERT_EQ(column->size(), 5); // 5 values after filtering
    auto* dict_column = assert_cast<ColumnInt32*>(column.get());

    // Expected values after filtering and null handling
    std::vector<std::optional<int32_t>> expected_values = {0, 0, std::nullopt, 2, std::nullopt};
    for (size_t i = 0; i < expected_values.size(); ++i) {
        if (expected_values[i].has_value()) {
            EXPECT_EQ(dict_column->get_data()[i], expected_values[i].value())
                    << "Mismatch at value " << i;
            EXPECT_FALSE(null_map[i]) << "Expected non-null at position " << i;
        } else {
            EXPECT_TRUE(null_map[i]) << "Expected null at position " << i;
        }
    }
}

// Test reading dictionary values to column
TEST_F(ByteArrayDictDecoderTest, test_read_dict_values_to_column) {
    // Create a column to store dictionary values
    MutableColumnPtr column = ColumnString::create();

    // Read dictionary values to column
    ASSERT_TRUE(_decoder.read_dict_values_to_column(column).ok());

    // Verify results
    ASSERT_EQ(column->size(), 3); // 3 dictionary items
    auto* result_column = assert_cast<ColumnString*>(column.get());

    // Verify dictionary values
    EXPECT_EQ(result_column->get_data_at(0).to_string(), "apple");
    EXPECT_EQ(result_column->get_data_at(1).to_string(), "banana");
    EXPECT_EQ(result_column->get_data_at(2).to_string(), "cherry");
}

// Test convert_dict_column_to_string_column function
TEST_F(ByteArrayDictDecoderTest, test_convert_dict_column_to_string_column) {
    // Create a ColumnInt32 with some dictionary codes
    MutableColumnPtr dict_column = ColumnInt32::create();
    dict_column->insert(vectorized::Field::create_field<TYPE_INT>(0));
    dict_column->insert(vectorized::Field::create_field<TYPE_INT>(1));
    dict_column->insert(vectorized::Field::create_field<TYPE_INT>(2));
    dict_column->insert(vectorized::Field::create_field<TYPE_INT>(1));

    // Convert to string column
    MutableColumnPtr string_column = _decoder.convert_dict_column_to_string_column(
            assert_cast<ColumnInt32*>(dict_column.get()));

    // Verify results
    ASSERT_EQ(string_column->size(), 4);
    auto* result_column = assert_cast<ColumnString*>(string_column.get());

    EXPECT_EQ(result_column->get_data_at(0).to_string(), "apple");
    EXPECT_EQ(result_column->get_data_at(1).to_string(), "banana");
    EXPECT_EQ(result_column->get_data_at(2).to_string(), "cherry");
    EXPECT_EQ(result_column->get_data_at(3).to_string(), "banana");
}

// Test skipping values for byte array dictionary decoding
TEST_F(ByteArrayDictDecoderTest, test_skip_value) {
    MutableColumnPtr column = ColumnString::create();
    DataTypePtr data_type = std::make_shared<DataTypeString>();

    // RLE encoded data: 4 zeros followed by 1, 2, 1, padded to 8 values, [0 0 0 0 1 2 1]
    std::vector<uint8_t> rle_data = {2, 8, 0, 3, 0b00011001, 0};

    Slice data_slice(reinterpret_cast<char*>(rle_data.data()), rle_data.size());
    ASSERT_TRUE(_decoder.set_data(&data_slice).ok());

    // Skip first 3 values
    ASSERT_TRUE(_decoder.skip_values(3).ok());

    // Create selection vector
    size_t num_values = 4;                                    // Total 7 values, skip 3, remaining 4
    std::vector<uint16_t> run_length_null_map(1, num_values); // All non-null
    std::vector<uint8_t> filter_data(num_values, 1);
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());
    ColumnSelectVector select_vector;
    ASSERT_TRUE(select_vector.init(run_length_null_map, num_values, nullptr, &filter_map, 0).ok());

    // Perform decoding
    ASSERT_TRUE(_decoder.decode_values(column, data_type, select_vector, false).ok());

    // Verify results
    ASSERT_EQ(column->size(), num_values);
    auto* result_column = assert_cast<ColumnString*>(column.get());

    // Expected values after skipping first 3 values ("apple", "apple", "apple")
    std::vector<std::string> expected_values = {"apple", "banana", "cherry", "banana"};
    for (size_t i = 0; i < num_values; ++i) {
        EXPECT_EQ(result_column->get_data_at(i).to_string(), expected_values[i])
                << "Mismatch at value " << i;
    }
}

} // namespace doris::vectorized
