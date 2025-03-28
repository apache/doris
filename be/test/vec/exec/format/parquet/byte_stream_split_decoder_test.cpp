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

#include "vec/exec/format/parquet/byte_stream_split_decoder.h"

#include <gtest/gtest.h>

#include "util/slice.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

class ByteStreamSplitDecoderTest : public ::testing::Test {
protected:
    void SetUp() override {}

    ByteStreamSplitDecoder _decoder;
};

//// Test basic decoding functionality for FLOAT type
TEST_F(ByteStreamSplitDecoderTest, test_basic_decode_float) {
    // Prepare test data for FLOAT type
    size_t type_length_float = sizeof(float);
    size_t num_values_float = 3;
    size_t data_size_float = num_values_float * type_length_float;
    auto data_float = std::make_unique<uint8_t[]>(data_size_float);
    const float values_float[3] = {1.0f, 2.0f, 3.0f};
    for (int i = 0; i < num_values_float; i++) {
        const uint8_t* bytes = reinterpret_cast<const uint8_t*>(&values_float[i]);
        for (int j = 0; j < type_length_float; j++) {
            data_float[j * num_values_float + i] = bytes[j];
        }
    }
    Slice data_slice_float(data_float.get(), data_size_float);

    MutableColumnPtr column = ColumnFloat32::create();
    DataTypePtr data_type = std::make_shared<DataTypeFloat32>();

    // Set data for FLOAT type
    ASSERT_TRUE(_decoder.set_data(&data_slice_float).ok());
    _decoder.set_type_length(type_length_float);

    // Create selection vector without filter, total 3 values
    size_t num_values = 3;
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
    auto* result_column = assert_cast<ColumnFloat32*>(column.get());
    EXPECT_FLOAT_EQ(result_column->get_data()[0], 1.0f);
    EXPECT_FLOAT_EQ(result_column->get_data()[1], 2.0f);
    EXPECT_FLOAT_EQ(result_column->get_data()[2], 3.0f);
}

//// Test basic decoding functionality for DOUBLE type
TEST_F(ByteStreamSplitDecoderTest, test_basic_decode_double) {
    // Prepare test data for DOUBLE type
    size_t type_length_double = sizeof(double);
    size_t num_values_double = 3;
    size_t data_size_double = num_values_double * type_length_double;
    auto data_double = std::make_unique<uint8_t[]>(data_size_double);
    const double values_double[3] = {1.0, 2.0, 3.0};
    for (int i = 0; i < num_values_double; i++) {
        const uint8_t* bytes = reinterpret_cast<const uint8_t*>(&values_double[i]);
        for (int j = 0; j < type_length_double; j++) {
            data_double[j * num_values_double + i] = bytes[j];
        }
    }
    Slice data_slice_double(data_double.get(), data_size_double);

    MutableColumnPtr column = ColumnFloat64::create();
    DataTypePtr data_type = std::make_shared<DataTypeFloat64>();

    // Set data for DOUBLE type
    ASSERT_TRUE(_decoder.set_data(&data_slice_double).ok());
    _decoder.set_type_length(type_length_double);

    // Create selection vector without filter, total 3 values
    size_t num_values = 3;
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
    auto* result_column = assert_cast<ColumnFloat64*>(column.get());
    EXPECT_DOUBLE_EQ(result_column->get_data()[0], 1.0);
    EXPECT_DOUBLE_EQ(result_column->get_data()[1], 2.0);
    EXPECT_DOUBLE_EQ(result_column->get_data()[2], 3.0);
}

// Test decoding with filter for FLOAT type
TEST_F(ByteStreamSplitDecoderTest, test_decode_with_filter_float) {
    // Prepare test data for FLOAT type
    size_t type_length_float = sizeof(float);
    size_t num_values_float = 3;
    size_t data_size_float = num_values_float * type_length_float;
    auto data_float = std::make_unique<uint8_t[]>(data_size_float);
    const float values_float[3] = {1.0f, 2.0f, 3.0f};
    for (int i = 0; i < num_values_float; i++) {
        const uint8_t* bytes = reinterpret_cast<const uint8_t*>(&values_float[i]);
        for (int j = 0; j < type_length_float; j++) {
            data_float[j * num_values_float + i] = bytes[j];
        }
    }
    Slice data_slice_float(data_float.get(), data_size_float);

    MutableColumnPtr column = ColumnFloat32::create();
    DataTypePtr data_type = std::make_shared<DataTypeFloat32>();

    // Set data for FLOAT type
    ASSERT_TRUE(_decoder.set_data(&data_slice_float).ok());
    _decoder.set_type_length(type_length_float);

    // Create filter vector [1, 0, 1]
    size_t num_values = 3;
    std::vector<uint16_t> run_length_null_map(1, num_values); // All non-null
    std::vector<uint8_t> filter_data = {1, 0, 1};
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());
    ColumnSelectVector select_vector;
    ASSERT_TRUE(select_vector.init(run_length_null_map, num_values, nullptr, &filter_map, 0).ok());

    // Perform decoding
    ASSERT_TRUE(_decoder.decode_values(column, data_type, select_vector, false).ok());

    // Verify results
    ASSERT_EQ(column->size(), 2); // 2 values after filtering
    auto* result_column = assert_cast<ColumnFloat32*>(column.get());
    EXPECT_FLOAT_EQ(result_column->get_data()[0], 1.0f);
    EXPECT_FLOAT_EQ(result_column->get_data()[1], 3.0f);
}

// Test decoding with filter for DOUBLE type
TEST_F(ByteStreamSplitDecoderTest, test_decode_with_filter_double) {
    // Prepare test data for DOUBLE type
    size_t type_length_double = sizeof(double);
    size_t num_values_double = 3;
    size_t data_size_double = num_values_double * type_length_double;
    auto data_double = std::make_unique<uint8_t[]>(data_size_double);
    const double values_double[3] = {1.0, 2.0, 3.0};
    for (int i = 0; i < num_values_double; i++) {
        const uint8_t* bytes = reinterpret_cast<const uint8_t*>(&values_double[i]);
        for (int j = 0; j < type_length_double; j++) {
            data_double[j * num_values_double + i] = bytes[j];
        }
    }
    Slice data_slice_double(data_double.get(), data_size_double);

    MutableColumnPtr column = ColumnFloat64::create();
    DataTypePtr data_type = std::make_shared<DataTypeFloat64>();

    // Set data for DOUBLE type
    ASSERT_TRUE(_decoder.set_data(&data_slice_double).ok());
    _decoder.set_type_length(type_length_double);

    // Create filter vector [1, 0, 1]
    size_t num_values = 3;
    std::vector<uint16_t> run_length_null_map(1, num_values); // All non-null
    std::vector<uint8_t> filter_data = {1, 0, 1};
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());
    ColumnSelectVector select_vector;
    ASSERT_TRUE(select_vector.init(run_length_null_map, num_values, nullptr, &filter_map, 0).ok());

    // Perform decoding
    ASSERT_TRUE(_decoder.decode_values(column, data_type, select_vector, false).ok());

    // Verify results
    ASSERT_EQ(column->size(), 2); // 2 values after filtering
    auto* result_column = assert_cast<ColumnFloat64*>(column.get());
    EXPECT_DOUBLE_EQ(result_column->get_data()[0], 1.0);
    EXPECT_DOUBLE_EQ(result_column->get_data()[1], 3.0);
}

// Test decoding with filter and null for FLOAT type
TEST_F(ByteStreamSplitDecoderTest, test_decode_with_filter_and_null_float) {
    // Prepare test data for FLOAT type
    size_t type_length_float = sizeof(float);
    size_t num_values_float = 2;
    size_t data_size_float = num_values_float * type_length_float;
    auto data_float = std::make_unique<uint8_t[]>(data_size_float);
    const float values_float[2] = {1.0f, 3.0f};
    for (int i = 0; i < num_values_float; i++) {
        const uint8_t* bytes = reinterpret_cast<const uint8_t*>(&values_float[i]);
        for (int j = 0; j < type_length_float; j++) {
            data_float[j * num_values_float + i] = bytes[j];
        }
    }
    Slice data_slice_float(data_float.get(), data_size_float);

    MutableColumnPtr column = ColumnFloat32::create();
    DataTypePtr data_type = std::make_shared<DataTypeFloat32>();

    // Set data for FLOAT type
    ASSERT_TRUE(_decoder.set_data(&data_slice_float).ok());
    _decoder.set_type_length(type_length_float);

    // Create filter vector [1, 0, 1] and null vector [0, 1, 0]
    size_t num_values = 3;
    std::vector<uint16_t> run_length_null_map = {1, 1, 1}; // data: [1.0f, null, 3.0f]
    std::vector<uint8_t> filter_data = {0, 1, 1};          // filtered_data: [null, 3.0f]
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());
    ColumnSelectVector select_vector;
    NullMap null_map;
    ASSERT_TRUE(
            select_vector.init(run_length_null_map, num_values, &null_map, &filter_map, 0).ok());

    // Perform decoding
    ASSERT_TRUE(_decoder.decode_values(column, data_type, select_vector, false).ok());

    // Verify results
    ASSERT_EQ(column->size(), 2); // 2 values after filtering
    auto* result_column = assert_cast<ColumnFloat32*>(column.get());
    //    EXPECT_FLOAT_EQ(result_column->get_data()[0], 1.0f);
    //    EXPECT_FLOAT_EQ(result_column->get_data()[1], 3.0f);

    // Expected values after filtering and null handling
    std::vector<std::optional<float>> expected_values = {std::nullopt, 3.0f};
    for (size_t i = 0; i < expected_values.size(); ++i) {
        if (expected_values[i].has_value()) {
            EXPECT_FLOAT_EQ(result_column->get_data()[i], expected_values[i].value())
                    << "Mismatch at value " << i;
            EXPECT_FALSE(null_map[i]) << "Expected non-null at position " << i;
        } else {
            EXPECT_TRUE(null_map[i]) << "Expected null at position " << i;
        }
    }
}

// Test decoding with filter and null for DOUBLE type
TEST_F(ByteStreamSplitDecoderTest, test_decode_with_filter_and_null_double) {
    // Prepare test data for DOUBLE type
    size_t type_length_double = sizeof(double);
    size_t num_values_double = 2;
    size_t data_size_double = num_values_double * type_length_double;
    auto data_double = std::make_unique<uint8_t[]>(data_size_double);
    const double values_double[2] = {1.0, 3.0};
    for (int i = 0; i < num_values_double; i++) {
        const uint8_t* bytes = reinterpret_cast<const uint8_t*>(&values_double[i]);
        for (int j = 0; j < type_length_double; j++) {
            data_double[j * num_values_double + i] = bytes[j];
        }
    }
    Slice data_slice_double(data_double.get(), data_size_double);

    MutableColumnPtr column = ColumnFloat64::create();
    DataTypePtr data_type = std::make_shared<DataTypeFloat64>();

    // Set data for DOUBLE type
    ASSERT_TRUE(_decoder.set_data(&data_slice_double).ok());
    _decoder.set_type_length(type_length_double);

    // Create filter vector [1, 0, 1] and null vector [0, 1, 0]
    size_t num_values = 3;
    std::vector<uint16_t> run_length_null_map = {1, 1, 1}; // data: [1.0f, null, 3.0f]
    std::vector<uint8_t> filter_data = {0, 1, 1};          // filtered_data: [null, 3.0f]
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());
    ColumnSelectVector select_vector;
    NullMap null_map;
    ASSERT_TRUE(
            select_vector.init(run_length_null_map, num_values, &null_map, &filter_map, 0).ok());

    // Perform decoding
    ASSERT_TRUE(_decoder.decode_values(column, data_type, select_vector, false).ok());

    // Verify results
    ASSERT_EQ(column->size(), 2); // 2 values after filtering
    auto* result_column = assert_cast<ColumnFloat64*>(column.get());
    //    EXPECT_FLOAT_EQ(result_column->get_data()[0], 1.0f);
    //    EXPECT_FLOAT_EQ(result_column->get_data()[1], 3.0f);

    // Expected values after filtering and null handling
    std::vector<std::optional<float>> expected_values = {std::nullopt, 3.0f};
    for (size_t i = 0; i < expected_values.size(); ++i) {
        if (expected_values[i].has_value()) {
            EXPECT_FLOAT_EQ(result_column->get_data()[i], expected_values[i].value())
                    << "Mismatch at value " << i;
            EXPECT_FALSE(null_map[i]) << "Expected non-null at position " << i;
        } else {
            EXPECT_TRUE(null_map[i]) << "Expected null at position " << i;
        }
    }
}

// Test skipping values for FLOAT type
TEST_F(ByteStreamSplitDecoderTest, test_skip_value_float) {
    // Prepare test data for FLOAT type
    size_t type_length_float = sizeof(float);
    size_t num_values_float = 3;
    size_t data_size_float = num_values_float * type_length_float;
    auto data_float = std::make_unique<uint8_t[]>(data_size_float);
    const float values_float[3] = {1.0f, 2.0f, 3.0f};
    for (int i = 0; i < num_values_float; i++) {
        const uint8_t* bytes = reinterpret_cast<const uint8_t*>(&values_float[i]);
        for (int j = 0; j < type_length_float; j++) {
            data_float[j * num_values_float + i] = bytes[j];
        }
    }
    Slice data_slice_float(data_float.get(), data_size_float);

    MutableColumnPtr column = ColumnFloat32::create();
    DataTypePtr data_type = std::make_shared<DataTypeFloat32>();

    // Set data for FLOAT type
    ASSERT_TRUE(_decoder.set_data(&data_slice_float).ok());
    _decoder.set_type_length(type_length_float);

    // Skip first 2 values
    ASSERT_TRUE(_decoder.skip_values(2).ok());

    // Create selection vector
    size_t num_values = 1;                                    // Total 3 values, skip 2, remaining 1
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
    auto* result_column = assert_cast<ColumnFloat32*>(column.get());
    EXPECT_FLOAT_EQ(result_column->get_data()[0], 3.0f);
}

// Test skipping values for DOUBLE type
TEST_F(ByteStreamSplitDecoderTest, test_skip_value_double) {
    // Prepare test data for DOUBLE type
    size_t type_length_double = sizeof(double);
    size_t num_values_double = 3;
    size_t data_size_double = num_values_double * type_length_double;
    auto data_double = std::make_unique<uint8_t[]>(data_size_double);
    const double values_double[3] = {1.0, 2.0, 3.0};
    for (int i = 0; i < num_values_double; i++) {
        const uint8_t* bytes = reinterpret_cast<const uint8_t*>(&values_double[i]);
        for (int j = 0; j < type_length_double; j++) {
            data_double[j * num_values_double + i] = bytes[j];
        }
    }
    Slice data_slice_double(data_double.get(), data_size_double);

    MutableColumnPtr column = ColumnFloat64::create();
    DataTypePtr data_type = std::make_shared<DataTypeFloat64>();

    // Set data for DOUBLE type
    ASSERT_TRUE(_decoder.set_data(&data_slice_double).ok());
    _decoder.set_type_length(type_length_double);

    // Skip first 2 values
    ASSERT_TRUE(_decoder.skip_values(2).ok());

    // Create selection vector
    size_t num_values = 1;                                    // Total 3 values, skip 2, remaining 1
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
    auto* result_column = assert_cast<ColumnFloat64*>(column.get());
    EXPECT_DOUBLE_EQ(result_column->get_data()[0], 3.0);
}

} // namespace doris::vectorized