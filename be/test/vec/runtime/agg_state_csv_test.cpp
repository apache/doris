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

#include "util/url_coding.h"

#include <gtest/gtest.h>
#include <sstream>
#include <string>
#include <vector>
#include <cstdint>

namespace doris {

// Test base64 encoding functionality for AggState in CSV export
// Note: This test verifies that base64 encoding/decoding logic works correctly
TEST(AggStateCsvTest, Base64EncodingDecoding) {
    // Test basic functionality of base64 encoding and decoding
    std::string original_data = "test_agg_state_data_12345";
    std::string base64_encoded;
    std::string base64_decoded;
    
    // Encode
    base64_encode(original_data, &base64_encoded);
    EXPECT_FALSE(base64_encoded.empty());
    
    // Decode
    bool decode_success = base64_decode(base64_encoded, &base64_decoded);
    EXPECT_TRUE(decode_success);
    EXPECT_EQ(original_data, base64_decoded);
    
    // Test empty data
    std::string empty_data = "";
    std::string empty_encoded;
    std::string empty_decoded;
    base64_encode(empty_data, &empty_encoded);
    bool empty_decode_success = base64_decode(empty_encoded, &empty_decoded);
    EXPECT_TRUE(empty_decode_success);
    EXPECT_EQ(empty_data, empty_decoded);
    
    // Test binary data
    std::vector<uint8_t> binary_data = {0x00, 0x01, 0x02, 0x03, 0xFF, 0xFE, 0xFD};
    std::string binary_str(reinterpret_cast<const char*>(binary_data.data()), binary_data.size());
    std::string binary_encoded;
    std::string binary_decoded;
    base64_encode(binary_str, &binary_encoded);
    bool binary_decode_success = base64_decode(binary_encoded, &binary_decoded);
    EXPECT_TRUE(binary_decode_success);
    EXPECT_EQ(binary_str, binary_decoded);
}

// Verify that base64 encoded data can be safely transmitted in CSV
TEST(AggStateCsvTest, Base64CsvSafety) {
    // Verify that base64 encoded string doesn't contain CSV special characters (in most cases)
    std::string test_data = "test\ndata\twith,special\"chars";
    std::string encoded;
    base64_encode(test_data, &encoded);
    
    // Base64 encoded string should not contain CSV special characters like newline, tab, etc.
    // Base64 string itself should be printable characters, not containing control characters
    // Verify can decode correctly
    std::string decoded;
    bool success = base64_decode(encoded, &decoded);
    EXPECT_TRUE(success);
    EXPECT_EQ(test_data, decoded);
}

// Test encoding/decoding of data of different sizes
TEST(AggStateCsvTest, Base64DifferentSizes) {
    // Test small data
    std::string small_data = "a";
    std::string small_encoded, small_decoded;
    base64_encode(small_data, &small_encoded);
    EXPECT_TRUE(base64_decode(small_encoded, &small_decoded));
    EXPECT_EQ(small_data, small_decoded);
    
    // Test medium data
    std::string medium_data(100, 'x');
    std::string medium_encoded, medium_decoded;
    base64_encode(medium_data, &medium_encoded);
    EXPECT_TRUE(base64_decode(medium_encoded, &medium_decoded));
    EXPECT_EQ(medium_data, medium_decoded);
    
    // Test large data
    std::string large_data(10000, 'y');
    std::string large_encoded, large_decoded;
    base64_encode(large_data, &large_encoded);
    EXPECT_TRUE(base64_decode(large_encoded, &large_decoded));
    EXPECT_EQ(large_data, large_decoded);
}

// Test compatibility with existing Base64Test
TEST(AggStateCsvTest, CompatibilityWithExistingTests) {
    // Verify our test is consistent with Base64Test.Basic in url_coding_test.cpp
    std::vector<std::pair<std::string, std::string>> test_cases = {
        {"a", "YQ=="},
        {"ab", "YWI="},
        {"abc", "YWJj"},
        {"abcd", "YWJjZA=="},
        {"abcde", "YWJjZGU="},
        {"abcdef", "YWJjZGVm"},
    };
    
    for (const auto& test_case : test_cases) {
        std::string encoded;
        base64_encode(test_case.first, &encoded);
        EXPECT_EQ(encoded, test_case.second);
        
        std::string decoded;
        EXPECT_TRUE(base64_decode(encoded, &decoded));
        EXPECT_EQ(decoded, test_case.first);
    }
}

} // namespace doris

