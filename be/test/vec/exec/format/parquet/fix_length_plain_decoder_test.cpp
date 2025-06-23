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

#include "vec/exec/format/parquet/fix_length_plain_decoder.h"

#include <gtest/gtest.h>

#include "util/slice.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

class FixLengthPlainDecoderTest : public ::testing::Test {
protected:
    void SetUp() override {}

    std::unique_ptr<uint8_t[]> _data;
    Slice _data_slice;
    size_t _type_length;
};

// Test basic decoding functionality
TEST_F(FixLengthPlainDecoderTest, test_basic_decode) {
    // Prepare test data: create fixed-length integer values
    int32_t values[3] = {123, 456, 789};
    size_t data_size = sizeof(values);

    _data = std::make_unique<uint8_t[]>(data_size);
    memcpy(_data.get(), values, data_size);

    _data_slice = Slice(_data.get(), data_size);
    _type_length = sizeof(int32_t);

    FixLengthPlainDecoder decoder;
    decoder.set_type_length(_type_length);
    ASSERT_TRUE(decoder.set_data(&_data_slice).ok());

    MutableColumnPtr column = ColumnInt32::create();
    DataTypePtr data_type = std::make_shared<DataTypeInt32>();

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
    auto* result_column = assert_cast<ColumnInt32*>(column.get());

    EXPECT_EQ(result_column->get_data()[0], 123);
    EXPECT_EQ(result_column->get_data()[1], 456);
    EXPECT_EQ(result_column->get_data()[2], 789);
}

// Test decoding with filter
TEST_F(FixLengthPlainDecoderTest, test_decode_with_filter) {
    // Prepare test data: create fixed-length integer values
    int32_t values[3] = {123, 456, 789};
    size_t data_size = sizeof(values);

    _data = std::make_unique<uint8_t[]>(data_size);
    memcpy(_data.get(), values, data_size);

    _data_slice = Slice(_data.get(), data_size);
    _type_length = sizeof(int32_t);

    FixLengthPlainDecoder decoder;
    decoder.set_type_length(_type_length);
    ASSERT_TRUE(decoder.set_data(&_data_slice).ok());

    MutableColumnPtr column = ColumnInt32::create();
    DataTypePtr data_type = std::make_shared<DataTypeInt32>();

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
    auto* result_column = assert_cast<ColumnInt32*>(column.get());

    EXPECT_EQ(result_column->get_data()[0], 123);
    EXPECT_EQ(result_column->get_data()[1], 789);
}

// Test decoding with filter and null
TEST_F(FixLengthPlainDecoderTest, test_decode_with_filter_and_null) {
    // Prepare test data: create fixed-length integer values
    int32_t values[2] = {123, 789};
    size_t data_size = sizeof(values);

    _data = std::make_unique<uint8_t[]>(data_size);
    memcpy(_data.get(), values, data_size);

    _data_slice = Slice(_data.get(), data_size);
    _type_length = sizeof(int32_t);

    FixLengthPlainDecoder decoder;
    decoder.set_type_length(_type_length);
    ASSERT_TRUE(decoder.set_data(&_data_slice).ok());

    MutableColumnPtr column = ColumnInt32::create();
    DataTypePtr data_type = std::make_shared<DataTypeInt32>();

    // Create filter vector [1,0,1] and null vector [0,1,0]
    size_t num_values = 3;
    std::vector<uint16_t> run_length_null_map = {1, 1, 1}; // data: [123, null, 789]
    std::vector<uint8_t> filter_data = {1, 0, 1};          // filtered_data: [123, 789]

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
    auto* result_column = assert_cast<ColumnInt32*>(column.get());

    // Expected values after filtering and null handling
    std::vector<std::optional<int32_t>> expected_values = {123, 789};
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

// Test skipping values
TEST_F(FixLengthPlainDecoderTest, test_skip_value) {
    // Prepare test data: create fixed-length integer values
    int32_t values[3] = {123, 456, 789};
    size_t data_size = sizeof(values);

    _data = std::make_unique<uint8_t[]>(data_size);
    memcpy(_data.get(), values, data_size);

    _data_slice = Slice(_data.get(), data_size);
    _type_length = sizeof(int32_t);

    FixLengthPlainDecoder decoder;
    decoder.set_type_length(_type_length);
    ASSERT_TRUE(decoder.set_data(&_data_slice).ok());

    // Skip first 2 values
    ASSERT_TRUE(decoder.skip_values(2).ok());

    MutableColumnPtr column = ColumnInt32::create();
    DataTypePtr data_type = std::make_shared<DataTypeInt32>();

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
    auto* result_column = assert_cast<ColumnInt32*>(column.get());

    EXPECT_EQ(result_column->get_data()[0], 789);
}

} // namespace doris::vectorized
