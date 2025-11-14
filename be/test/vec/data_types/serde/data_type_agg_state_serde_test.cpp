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

#include "vec/data_types/serde/data_type_agg_state_serde.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <random>
#include <string>
#include <vector>

#include "util/url_coding.h"
#include "vec/columns/column_fixed_length_object.h"
#include "vec/columns/column_string.h"
#include "vec/common/string_buffer.hpp"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/serde/data_type_string_serde.h"

namespace doris::vectorized {

class DataTypeAggStateSerdeTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create nested_serde for testing
        auto string_type = std::make_shared<DataTypeString>();
        nested_serde = string_type->get_serde();

        // Create AggStateSerde instance
        agg_state_serde = std::make_shared<DataTypeAggStateSerde>(nested_serde);
    }

    // Verify if string is valid base64 encoding
    bool isValidBase64(const std::string& str) {
        if (str.empty()) {
            return true; // Empty string is also valid base64
        }

        // Base64 character set: A-Z, a-z, 0-9, +, /, = (for padding)
        for (char c : str) {
            if (!((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
                  c == '+' || c == '/' || c == '=')) {
                return false;
            }
        }

        // Base64 string length must be multiple of 4 (considering padding)
        // Padding can be 0, 1 or 2 '=' characters
        size_t len = str.length();
        if (len % 4 != 0) {
            return false;
        }

        // Check padding correctness: only the last 1-2 characters can be '='
        size_t padding_count = 0;
        for (size_t i = len - 1; i >= len - 2 && i < len; --i) {
            if (str[i] == '=') {
                padding_count++;
            } else {
                break;
            }
        }
        // Padding can be at most 2 characters
        return padding_count <= 2;
    }

    DataTypeSerDeSPtr nested_serde;
    std::shared_ptr<DataTypeAggStateSerde> agg_state_serde;
};

// Test serialization of ColumnString type
TEST_F(DataTypeAggStateSerdeTest, SerializeColumnString) {
    // Create ColumnString for testing
    auto column = ColumnString::create();

    // Test cases: various different data
    std::vector<std::string> test_cases = {
            "simple_string",
            "test\ndata\twith,special\"chars",
            "",                      // Empty string
            "a",                     // Single character
            std::string(100, 'x'),   // Medium length
            std::string(10000, 'y'), // Large data
    };

    // Add binary data
    std::vector<uint8_t> binary_data = {0x00, 0x01, 0x02, 0x03, 0xFF, 0xFE, 0xFD, 0xFC};
    test_cases.emplace_back(reinterpret_cast<const char*>(binary_data.data()), binary_data.size());

    for (const auto& test_data : test_cases) {
        column->insert_data(test_data.c_str(), test_data.size());
    }

    // Serialize each row and verify
    FormatOptions options;
    auto output_col = ColumnString::create();
    output_col->reserve(column->size());
    VectorBufferWriter buffer_writer(*output_col.get());

    for (size_t i = 0; i < column->size(); ++i) {
        Status st = agg_state_serde->serialize_one_cell_to_json(*column, i, buffer_writer, options);
        EXPECT_TRUE(st.ok()) << "Failed to serialize row " << i << ": " << st.to_string();

        buffer_writer.commit();

        // Get serialized result
        std::string serialized = output_col->get_data_at(i).to_string();

        // Verify it's base64 encoded
        EXPECT_TRUE(isValidBase64(serialized))
                << "Serialized data is not valid base64 for row " << i << ": " << serialized;

        // Verify can decode back to original data
        std::string decoded;
        bool decode_success = doris::base64_decode(serialized, &decoded);
        EXPECT_TRUE(decode_success) << "Failed to decode base64 for row " << i;
        EXPECT_EQ(decoded, test_cases[i]) << "Decoded data doesn't match original for row " << i;
    }
}

// Test serialization of ColumnFixedLengthObject type
TEST_F(DataTypeAggStateSerdeTest, SerializeColumnFixedLengthObject) {
    const size_t object_size = 16; // Fixed object size

    // Create ColumnFixedLengthObject for testing
    auto column = ColumnFixedLengthObject::create(object_size);

    // Prepare test data
    std::vector<std::vector<uint8_t>> test_cases = {
            {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D,
             0x0E, 0x0F},                            // Standard binary data
            std::vector<uint8_t>(object_size, 0x00), // All zeros
            std::vector<uint8_t>(object_size, 0xFF), // All ones
            {0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
             0x99, 0xAA}, // Mixed data
    };

    // Insert test data
    for (const auto& test_data : test_cases) {
        EXPECT_EQ(test_data.size(), object_size);
        column->insert_data(reinterpret_cast<const char*>(test_data.data()), object_size);
    }

    // Serialize each row and verify
    FormatOptions options;
    auto output_col = ColumnString::create();
    output_col->reserve(column->size());
    VectorBufferWriter buffer_writer(*output_col.get());

    for (size_t i = 0; i < column->size(); ++i) {
        Status st = agg_state_serde->serialize_one_cell_to_json(*column, i, buffer_writer, options);
        EXPECT_TRUE(st.ok()) << "Failed to serialize FixedLengthObject row " << i << ": "
                             << st.to_string();

        buffer_writer.commit();

        // Get serialized result
        std::string serialized = output_col->get_data_at(i).to_string();

        // Verify it's base64 encoded
        EXPECT_TRUE(isValidBase64(serialized))
                << "Serialized FixedLengthObject is not valid base64 for row " << i << ": "
                << serialized;

        // Verify can decode back to original data
        std::string decoded;
        bool decode_success = doris::base64_decode(serialized, &decoded);
        EXPECT_TRUE(decode_success) << "Failed to decode base64 for row " << i;
        EXPECT_EQ(decoded.size(), object_size) << "Decoded size mismatch for row " << i;
        EXPECT_EQ(memcmp(decoded.data(), test_cases[i].data(), object_size), 0)
                << "Decoded data doesn't match original for row " << i;
    }
}

// Test handling of empty data
TEST_F(DataTypeAggStateSerdeTest, SerializeEmptyData) {
    FormatOptions options;
    auto output_col = ColumnString::create();
    VectorBufferWriter buffer_writer(*output_col.get());

    // Test empty string
    {
        auto column = ColumnString::create();
        column->insert_data("", 0);

        Status st = agg_state_serde->serialize_one_cell_to_json(*column, 0, buffer_writer, options);
        EXPECT_TRUE(st.ok()) << "Failed to serialize empty string: " << st.to_string();
        buffer_writer.commit();

        std::string serialized = output_col->get_data_at(0).to_string();
        // Empty string after encoding should be empty string or valid base64
        std::string decoded;
        bool decode_success = doris::base64_decode(serialized, &decoded);
        EXPECT_TRUE(decode_success);
        EXPECT_EQ(decoded, "");
    }
}

// Test handling of CSV special characters
TEST_F(DataTypeAggStateSerdeTest, SerializeCsvSpecialChars) {
    auto column = ColumnString::create();

    // Data containing various CSV special characters
    std::vector<std::string> special_char_cases = {
            "data,with,commas",   "data\nwith\nnewlines",           "data\twith\ttabs",
            "data\"with\"quotes", "data,with\nall\tspecial\"chars",
            "leading,trailing,", // Ending with comma
            ",leading,comma",    // Starting with comma
            "\"quoted\"",         "\n\nmultiple\nnewlines\n",
    };

    for (const auto& test_data : special_char_cases) {
        column->insert_data(test_data.c_str(), test_data.size());
    }

    FormatOptions options;
    auto output_col = ColumnString::create();
    output_col->reserve(column->size());
    VectorBufferWriter buffer_writer(*output_col.get());

    for (size_t i = 0; i < column->size(); ++i) {
        Status st = agg_state_serde->serialize_one_cell_to_json(*column, i, buffer_writer, options);
        EXPECT_TRUE(st.ok()) << "Failed to serialize special char case " << i << ": "
                             << st.to_string();

        buffer_writer.commit();

        std::string serialized = output_col->get_data_at(i).to_string();

        // Verify base64 encoded string doesn't contain CSV special characters (except possible separators + and /)
        // But should not contain newline, tab, comma, quote, etc.
        EXPECT_EQ(serialized.find('\n'), std::string::npos)
                << "Base64 contains newline at row " << i;
        EXPECT_EQ(serialized.find('\t'), std::string::npos) << "Base64 contains tab at row " << i;
        EXPECT_EQ(serialized.find(','), std::string::npos) << "Base64 contains comma at row " << i;
        EXPECT_EQ(serialized.find('"'), std::string::npos) << "Base64 contains quote at row " << i;

        // Verify can decode correctly
        std::string decoded;
        bool decode_success = doris::base64_decode(serialized, &decoded);
        EXPECT_TRUE(decode_success) << "Failed to decode for row " << i;
        EXPECT_EQ(decoded, special_char_cases[i]) << "Decoded data mismatch for row " << i;
    }
}

// Test serialization of large data
TEST_F(DataTypeAggStateSerdeTest, SerializeLargeData) {
    auto column = ColumnString::create();

    // Data of different sizes
    std::vector<size_t> sizes = {1024, 4096, 16384, 65536, 1048576}; // 1KB to 1MB

    for (size_t size : sizes) {
        std::string large_data(size, 'A' + (size % 26));
        column->insert_data(large_data.c_str(), large_data.size());
    }

    FormatOptions options;
    auto output_col = ColumnString::create();
    output_col->reserve(column->size());
    VectorBufferWriter buffer_writer(*output_col.get());

    for (size_t i = 0; i < column->size(); ++i) {
        Status st = agg_state_serde->serialize_one_cell_to_json(*column, i, buffer_writer, options);
        EXPECT_TRUE(st.ok()) << "Failed to serialize large data row " << i << ": "
                             << st.to_string();

        buffer_writer.commit();

        std::string serialized = output_col->get_data_at(i).to_string();

        // Verify it's valid base64
        EXPECT_TRUE(isValidBase64(serialized))
                << "Large data serialization invalid base64 at row " << i;

        // Verify can decode
        std::string decoded;
        bool decode_success = doris::base64_decode(serialized, &decoded);
        EXPECT_TRUE(decode_success) << "Failed to decode large data for row " << i;
        EXPECT_EQ(decoded.size(), sizes[i]) << "Decoded size mismatch for row " << i;
    }
}

// Test serialization of random data
TEST_F(DataTypeAggStateSerdeTest, SerializeRandomData) {
    auto column = ColumnString::create();

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(1, 1000);            // Random length from 1 to 1000 bytes
    std::uniform_int_distribution<uint8_t> byte_dis(0, 255); // Random byte values

    // Generate 10 random data
    for (int i = 0; i < 10; ++i) {
        size_t length = dis(gen);
        std::vector<uint8_t> random_data(length);
        for (size_t j = 0; j < length; ++j) {
            random_data[j] = byte_dis(gen);
        }
        std::string data_str(reinterpret_cast<const char*>(random_data.data()), length);
        column->insert_data(data_str.c_str(), data_str.size());
    }

    FormatOptions options;
    auto output_col = ColumnString::create();
    output_col->reserve(column->size());
    VectorBufferWriter buffer_writer(*output_col.get());

    for (size_t i = 0; i < column->size(); ++i) {
        Status st = agg_state_serde->serialize_one_cell_to_json(*column, i, buffer_writer, options);
        EXPECT_TRUE(st.ok()) << "Failed to serialize random data row " << i << ": "
                             << st.to_string();

        buffer_writer.commit();

        std::string serialized = output_col->get_data_at(i).to_string();
        EXPECT_TRUE(isValidBase64(serialized))
                << "Random data serialization invalid base64 at row " << i;

        // Verify can decode
        std::string decoded;
        bool decode_success = doris::base64_decode(serialized, &decoded);
        EXPECT_TRUE(decode_success) << "Failed to decode random data for row " << i;
        EXPECT_EQ(decoded.size(), column->get_data_at(i).size)
                << "Decoded size mismatch for row " << i;
    }
}

// Test serialize_one_cell_to_hive_text method
TEST_F(DataTypeAggStateSerdeTest, SerializeToHiveText) {
    auto column = ColumnString::create();
    column->insert_data("test_agg_state_data", 19);

    FormatOptions options;
    auto output_col = ColumnString::create();
    VectorBufferWriter buffer_writer(*output_col.get());

    Status st =
            agg_state_serde->serialize_one_cell_to_hive_text(*column, 0, buffer_writer, options, 0);
    EXPECT_TRUE(st.ok()) << "Failed to serialize to hive text: " << st.to_string();

    buffer_writer.commit();

    std::string serialized = output_col->get_data_at(0).to_string();
    EXPECT_TRUE(isValidBase64(serialized)) << "Hive text serialization invalid base64";

    // Verify can decode
    std::string decoded;
    bool decode_success = doris::base64_decode(serialized, &decoded);
    EXPECT_TRUE(decode_success);
    EXPECT_EQ(decoded, "test_agg_state_data");
}

// Test correctness of base64 encoding values (compared with standard base64 library)
TEST_F(DataTypeAggStateSerdeTest, Base64EncodingCorrectness) {
    // Standard base64 test cases (from RFC 4648)
    std::vector<std::pair<std::string, std::string>> standard_cases = {
            {"", ""},               // Empty string
            {"f", "Zg=="},          // 1 byte
            {"fo", "Zm8="},         // 2 bytes
            {"foo", "Zm9v"},        // 3 bytes (no padding)
            {"foob", "Zm9vYg=="},   // 4 bytes
            {"fooba", "Zm9vYmE="},  // 5 bytes
            {"foobar", "Zm9vYmFy"}, // 6 bytes (no padding)
    };

    auto column = ColumnString::create();
    for (const auto& test_case : standard_cases) {
        column->insert_data(test_case.first.c_str(), test_case.first.size());
    }

    FormatOptions options;
    auto output_col = ColumnString::create();
    output_col->reserve(column->size());
    VectorBufferWriter buffer_writer(*output_col.get());

    for (size_t i = 0; i < column->size(); ++i) {
        Status st = agg_state_serde->serialize_one_cell_to_json(*column, i, buffer_writer, options);
        EXPECT_TRUE(st.ok()) << "Failed to serialize standard case " << i << ": " << st.to_string();

        buffer_writer.commit();

        std::string serialized = output_col->get_data_at(i).to_string();

        // Verify encoding result matches expected
        if (!standard_cases[i].second.empty() || !serialized.empty()) {
            // Note: Our implementation may have special handling for empty strings, allow some flexibility here
            if (standard_cases[i].first.empty()) {
                // Empty string may encode to empty string or valid base64
                EXPECT_TRUE(serialized.empty() || isValidBase64(serialized));
            } else {
                EXPECT_EQ(serialized, standard_cases[i].second)
                        << "Base64 encoding mismatch for case " << i
                        << ", input: " << standard_cases[i].first;
            }
        }
    }
}

// Test edge cases: various values of single byte
TEST_F(DataTypeAggStateSerdeTest, SerializeSingleByteValues) {
    auto column = ColumnString::create();

    // Test all possible single byte values (0-255)
    for (int i = 0; i <= 255; ++i) {
        uint8_t byte_value = static_cast<uint8_t>(i);
        column->insert_data(reinterpret_cast<const char*>(&byte_value), 1);
    }

    FormatOptions options;
    auto output_col = ColumnString::create();
    output_col->reserve(column->size());
    VectorBufferWriter buffer_writer(*output_col.get());

    for (size_t i = 0; i < column->size(); ++i) {
        Status st = agg_state_serde->serialize_one_cell_to_json(*column, i, buffer_writer, options);
        EXPECT_TRUE(st.ok()) << "Failed to serialize byte value " << i << ": " << st.to_string();

        buffer_writer.commit();

        std::string serialized = output_col->get_data_at(i).to_string();
        EXPECT_TRUE(isValidBase64(serialized)) << "Invalid base64 for byte value " << i;

        // Verify can decode back to original byte
        std::string decoded;
        bool decode_success = doris::base64_decode(serialized, &decoded);
        EXPECT_TRUE(decode_success) << "Failed to decode byte value " << i;
        EXPECT_EQ(decoded.size(), 1) << "Decoded size mismatch for byte value " << i;
        EXPECT_EQ(static_cast<uint8_t>(decoded[0]), static_cast<uint8_t>(i))
                << "Decoded value mismatch for byte value " << i;
    }
}

// Test serialization performance: verify it doesn't fail due to data size
TEST_F(DataTypeAggStateSerdeTest, SerializePerformanceStress) {
    auto column = ColumnString::create();

    // Test data of different sizes to ensure all can serialize normally
    std::vector<size_t> stress_sizes = {1, 3, 4, 7, 15, 16, 31, 63, 64, 127, 255, 256, 1023, 1024};

    for (size_t size : stress_sizes) {
        std::string data(size, 'X');
        column->insert_data(data.c_str(), data.size());
    }

    FormatOptions options;
    auto output_col = ColumnString::create();
    output_col->reserve(column->size());
    VectorBufferWriter buffer_writer(*output_col.get());

    for (size_t i = 0; i < column->size(); ++i) {
        Status st = agg_state_serde->serialize_one_cell_to_json(*column, i, buffer_writer, options);
        EXPECT_TRUE(st.ok()) << "Failed to serialize stress test size " << stress_sizes[i] << ": "
                             << st.to_string();

        buffer_writer.commit();

        std::string serialized = output_col->get_data_at(i).to_string();
        EXPECT_TRUE(isValidBase64(serialized)) << "Invalid base64 for size " << stress_sizes[i];

        std::string decoded;
        bool decode_success = doris::base64_decode(serialized, &decoded);
        EXPECT_TRUE(decode_success) << "Failed to decode for size " << stress_sizes[i];
        EXPECT_EQ(decoded.size(), stress_sizes[i]) << "Size mismatch for " << stress_sizes[i];
    }
}

} // namespace doris::vectorized
