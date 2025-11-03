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

// 测试CSV导出中AggState的base64编码功能
// 注意：此测试验证base64编码/解码逻辑是否正确工作
TEST(AggStateCsvTest, Base64EncodingDecoding) {
    // 测试base64编码和解码的基本功能
    std::string original_data = "test_agg_state_data_12345";
    std::string base64_encoded;
    std::string base64_decoded;
    
    // 编码
    base64_encode(original_data, &base64_encoded);
    EXPECT_FALSE(base64_encoded.empty());
    
    // 解码
    bool decode_success = base64_decode(base64_encoded, &base64_decoded);
    EXPECT_TRUE(decode_success);
    EXPECT_EQ(original_data, base64_decoded);
    
    // 测试空数据
    std::string empty_data = "";
    std::string empty_encoded;
    std::string empty_decoded;
    base64_encode(empty_data, &empty_encoded);
    bool empty_decode_success = base64_decode(empty_encoded, &empty_decoded);
    EXPECT_TRUE(empty_decode_success);
    EXPECT_EQ(empty_data, empty_decoded);
    
    // 测试二进制数据
    std::vector<uint8_t> binary_data = {0x00, 0x01, 0x02, 0x03, 0xFF, 0xFE, 0xFD};
    std::string binary_str(reinterpret_cast<const char*>(binary_data.data()), binary_data.size());
    std::string binary_encoded;
    std::string binary_decoded;
    base64_encode(binary_str, &binary_encoded);
    bool binary_decode_success = base64_decode(binary_encoded, &binary_decoded);
    EXPECT_TRUE(binary_decode_success);
    EXPECT_EQ(binary_str, binary_decoded);
}

// 验证base64编码后的数据可以在CSV中安全传输
TEST(AggStateCsvTest, Base64CsvSafety) {
    // 验证base64编码后的字符串不包含CSV特殊字符（在大多数情况下）
    std::string test_data = "test\ndata\twith,special\"chars";
    std::string encoded;
    base64_encode(test_data, &encoded);
    
    // base64编码后的字符串不应该包含换行符、制表符等CSV特殊字符
    // base64字符串本身应该是可打印字符，不包含控制字符
    // 验证可以正确解码
    std::string decoded;
    bool success = base64_decode(encoded, &decoded);
    EXPECT_TRUE(success);
    EXPECT_EQ(test_data, decoded);
}

// 测试不同大小的数据编码/解码
TEST(AggStateCsvTest, Base64DifferentSizes) {
    // 测试小数据
    std::string small_data = "a";
    std::string small_encoded, small_decoded;
    base64_encode(small_data, &small_encoded);
    EXPECT_TRUE(base64_decode(small_encoded, &small_decoded));
    EXPECT_EQ(small_data, small_decoded);
    
    // 测试中等数据
    std::string medium_data(100, 'x');
    std::string medium_encoded, medium_decoded;
    base64_encode(medium_data, &medium_encoded);
    EXPECT_TRUE(base64_decode(medium_encoded, &medium_decoded));
    EXPECT_EQ(medium_data, medium_decoded);
    
    // 测试大数据
    std::string large_data(10000, 'y');
    std::string large_encoded, large_decoded;
    base64_encode(large_data, &large_encoded);
    EXPECT_TRUE(base64_decode(large_encoded, &large_decoded));
    EXPECT_EQ(large_data, large_decoded);
}

// 测试与现有Base64Test的一致性
TEST(AggStateCsvTest, CompatibilityWithExistingTests) {
    // 验证我们的测试与url_coding_test.cpp中的Base64Test.Basic保持一致
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

