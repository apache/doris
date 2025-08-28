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

#include "vec/exec/format/parquet/byte_array_plain_decoder.h"

#include <gtest/gtest.h>

#include "util/slice.h"
#include "vec/columns/column_string.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

class ByteArrayPlainDecoderTest : public ::testing::Test {
protected:
    void SetUp() override {}

    Slice _data_slice;
    std::unique_ptr<uint8_t[]> _data;
};

// Test basic decoding functionality
TEST_F(ByteArrayPlainDecoderTest, test_basic_decode) {
    // Prepare test data: create byte array strings
    const char* values[3] = {"apple", "banana", "cherry"};
    size_t data_size = 0;

    // Calculate total data size
    for (int i = 0; i < 3; i++) {
        data_size += 4 + strlen(values[i]); // 4 bytes for length + string data
    }

    _data = std::make_unique<uint8_t[]>(data_size);
    size_t offset = 0;
    for (int i = 0; i < 3; i++) {
        uint32_t len = strlen(values[i]);
        encode_fixed32_le(_data.get() + offset, len);
        offset += 4;
        memcpy(_data.get() + offset, values[i], len);
        offset += len;
    }

    _data_slice = Slice(_data.get(), data_size);

    ByteArrayPlainDecoder decoder;
    ASSERT_TRUE(decoder.set_data(&_data_slice).ok());

    MutableColumnPtr column = ColumnString::create();
    DataTypePtr data_type = std::make_shared<DataTypeString>();

    // Create selection vector without filter
    size_t num_values = 3;
    std::vector<uint16_t> run_length_null_map(1, num_values); // All non-null
    std::vector<uint8_t> filter_data(num_values, 1);
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());
    ColumnSelectVector select_vector;
    ASSERT_TRUE(select_vector.init(run_length_null_map, num_values, nullptr, &filter_map, 0).ok());

    // Perform decoding
    ASSERT_TRUE(decoder.decode_values(column, data_type, select_vector, false).ok());

    // Verify results
    ASSERT_EQ(column->size(), num_values);
    auto* result_column = assert_cast<ColumnString*>(column.get());

    EXPECT_EQ(result_column->get_data_at(0).to_string(), "apple");
    EXPECT_EQ(result_column->get_data_at(1).to_string(), "banana");
    EXPECT_EQ(result_column->get_data_at(2).to_string(), "cherry");
}

// Test decoding with filter
TEST_F(ByteArrayPlainDecoderTest, test_decode_with_filter) {
    // Prepare test data: create byte array strings
    const char* values[3] = {"apple", "banana", "cherry"};
    size_t data_size = 0;

    // Calculate total data size
    for (int i = 0; i < 3; i++) {
        data_size += 4 + strlen(values[i]); // 4 bytes for length + string data
    }

    _data = std::make_unique<uint8_t[]>(data_size);
    size_t offset = 0;
    for (int i = 0; i < 3; i++) {
        uint32_t len = strlen(values[i]);
        encode_fixed32_le(_data.get() + offset, len);
        offset += 4;
        memcpy(_data.get() + offset, values[i], len);
        offset += len;
    }

    _data_slice = Slice(_data.get(), data_size);

    ByteArrayPlainDecoder decoder;
    ASSERT_TRUE(decoder.set_data(&_data_slice).ok());

    MutableColumnPtr column = ColumnString::create();
    DataTypePtr data_type = std::make_shared<DataTypeString>();

    // Create filter vector [1,0,1]
    size_t num_values = 3;
    std::vector<uint16_t> run_length_null_map(1, num_values); // All non-null
    std::vector<uint8_t> filter_data = {1, 0, 1};
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());
    ColumnSelectVector select_vector;
    ASSERT_TRUE(select_vector.init(run_length_null_map, num_values, nullptr, &filter_map, 0).ok());

    // Perform decoding
    ASSERT_TRUE(decoder.decode_values(column, data_type, select_vector, false).ok());

    // Verify results
    ASSERT_EQ(column->size(), 2); // 2 values after filtering
    auto* result_column = assert_cast<ColumnString*>(column.get());

    EXPECT_EQ(result_column->get_data_at(0).to_string(), "apple");
    EXPECT_EQ(result_column->get_data_at(1).to_string(), "cherry");
}

// Test decoding with filter and null
TEST_F(ByteArrayPlainDecoderTest, test_decode_with_filter_and_null) {
    // Prepare test data: create byte array strings
    const char* values[2] = {"apple", "cherry"};
    size_t data_size = 0;

    // Calculate total data size
    for (int i = 0; i < 2; i++) {
        data_size += 4 + strlen(values[i]); // 4 bytes for length + string data
    }

    _data = std::make_unique<uint8_t[]>(data_size);
    size_t offset = 0;
    for (int i = 0; i < 2; i++) {
        uint32_t len = strlen(values[i]);
        encode_fixed32_le(_data.get() + offset, len);
        offset += 4;
        memcpy(_data.get() + offset, values[i], len);
        offset += len;
    }

    _data_slice = Slice(_data.get(), data_size);

    ByteArrayPlainDecoder decoder;
    ASSERT_TRUE(decoder.set_data(&_data_slice).ok());

    MutableColumnPtr column = ColumnString::create();
    DataTypePtr data_type = std::make_shared<DataTypeString>();

    // Create filter vector [1,0,1] and null vector [0,1,0]
    size_t num_values = 3;
    std::vector<uint16_t> run_length_null_map = {1, 1, 1}; // data: ["apple", null, "cherry"]
    std::vector<uint8_t> filter_data = {1, 0, 1};          // filtered_data: ["apple", "cherry"]

    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());
    ColumnSelectVector select_vector;
    NullMap null_map;
    ASSERT_TRUE(
            select_vector.init(run_length_null_map, num_values, &null_map, &filter_map, 0).ok());

    // Perform decoding
    ASSERT_TRUE(decoder.decode_values(column, data_type, select_vector, false).ok());

    // Verify results
    ASSERT_EQ(column->size(), 2); // 2 values after filtering
    auto* result_column = assert_cast<ColumnString*>(column.get());

    // Expected values after filtering and null handling
    std::vector<std::optional<std::string>> expected_values = {"apple", "cherry"};
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

// Test skipping values
TEST_F(ByteArrayPlainDecoderTest, test_skip_value) {
    // Prepare test data: create byte array strings
    const char* values[3] = {"apple", "banana", "cherry"};
    size_t data_size = 0;

    // Calculate total data size
    for (int i = 0; i < 3; i++) {
        data_size += 4 + strlen(values[i]); // 4 bytes for length + string data
    }

    _data = std::make_unique<uint8_t[]>(data_size);
    size_t offset = 0;
    for (int i = 0; i < 3; i++) {
        uint32_t len = strlen(values[i]);
        encode_fixed32_le(_data.get() + offset, len);
        offset += 4;
        memcpy(_data.get() + offset, values[i], len);
        offset += len;
    }

    _data_slice = Slice(_data.get(), data_size);

    ByteArrayPlainDecoder decoder;
    ASSERT_TRUE(decoder.set_data(&_data_slice).ok());

    // Skip first 2 values
    ASSERT_TRUE(decoder.skip_values(2).ok());

    MutableColumnPtr column = ColumnString::create();
    DataTypePtr data_type = std::make_shared<DataTypeString>();

    // Create selection vector
    size_t num_values = 1;                                    // Total 3 values, skip 2, remaining 1
    std::vector<uint16_t> run_length_null_map(1, num_values); // All non-null
    std::vector<uint8_t> filter_data(num_values, 1);
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());
    ColumnSelectVector select_vector;
    ASSERT_TRUE(select_vector.init(run_length_null_map, num_values, nullptr, &filter_map, 0).ok());

    // Perform decoding
    ASSERT_TRUE(decoder.decode_values(column, data_type, select_vector, false).ok());

    // Verify results
    ASSERT_EQ(column->size(), num_values);
    auto* result_column = assert_cast<ColumnString*>(column.get());

    EXPECT_EQ(result_column->get_data_at(0).to_string(), "cherry");
}

} // namespace doris::vectorized
