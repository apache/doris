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
        // 创建nested_serde用于测试
        auto string_type = std::make_shared<DataTypeString>();
        nested_serde = string_type->get_serde();
        
        // 创建AggStateSerde实例
        agg_state_serde = std::make_shared<DataTypeAggStateSerde>(nested_serde);
    }

    // 验证字符串是否为有效的base64编码
    bool isValidBase64(const std::string& str) {
        if (str.empty()) {
            return true; // 空字符串也是有效的base64
        }
        
        // Base64字符集: A-Z, a-z, 0-9, +, /, = (用于padding)
        for (char c : str) {
            if (!((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || 
                  (c >= '0' && c <= '9') || c == '+' || c == '/' || c == '=')) {
                return false;
            }
        }
        
        // Base64字符串长度必须是4的倍数（考虑padding）
        // padding可以是0、1或2个'='字符
        size_t len = str.length();
        if (len % 4 != 0) {
            return false;
        }
        
        // 检查padding的正确性：只在最后1-2个字符可以是'='
        size_t padding_count = 0;
        for (size_t i = len - 1; i >= len - 2 && i < len; --i) {
            if (str[i] == '=') {
                padding_count++;
            } else {
                break;
            }
        }
        // padding最多2个字符
        return padding_count <= 2;
    }

    DataTypeSerDeSPtr nested_serde;
    std::shared_ptr<DataTypeAggStateSerde> agg_state_serde;
};

// 测试ColumnString类型的序列化
TEST_F(DataTypeAggStateSerdeTest, SerializeColumnString) {
    // 创建测试用的ColumnString
    auto column = ColumnString::create();
    
    // 测试用例：各种不同的数据
    std::vector<std::string> test_cases = {
        "simple_string",
        "test\ndata\twith,special\"chars",
        "", // 空字符串
        "a", // 单字符
        std::string(100, 'x'), // 中等长度
        std::string(10000, 'y'), // 大数据
    };
    
    // 添加二进制数据
    std::vector<uint8_t> binary_data = {0x00, 0x01, 0x02, 0x03, 0xFF, 0xFE, 0xFD, 0xFC};
    test_cases.emplace_back(reinterpret_cast<const char*>(binary_data.data()), binary_data.size());
    
    for (const auto& test_data : test_cases) {
        column->insert_data(test_data.c_str(), test_data.size());
    }
    
    // 序列化每一行并验证
    FormatOptions options;
    auto output_col = ColumnString::create();
    output_col->reserve(column->size());
    VectorBufferWriter buffer_writer(*output_col.get());
    
    for (size_t i = 0; i < column->size(); ++i) {
        Status st = agg_state_serde->serialize_one_cell_to_json(*column, i, buffer_writer, options);
        EXPECT_TRUE(st.ok()) << "Failed to serialize row " << i << ": " << st.to_string();
        
        buffer_writer.commit();
        
        // 获取序列化后的结果
        std::string serialized = output_col->get_data_at(i).to_string();
        
        // 验证是base64编码
        EXPECT_TRUE(isValidBase64(serialized)) 
            << "Serialized data is not valid base64 for row " << i 
            << ": " << serialized;
        
        // 验证可以解码回原始数据
        std::string decoded;
        bool decode_success = doris::base64_decode(serialized, &decoded);
        EXPECT_TRUE(decode_success) << "Failed to decode base64 for row " << i;
        EXPECT_EQ(decoded, test_cases[i]) << "Decoded data doesn't match original for row " << i;
    }
}

// 测试ColumnFixedLengthObject类型的序列化
TEST_F(DataTypeAggStateSerdeTest, SerializeColumnFixedLengthObject) {
    const size_t object_size = 16; // 固定对象大小
    
    // 创建测试用的ColumnFixedLengthObject
    auto column = ColumnFixedLengthObject::create(object_size);
    
    // 准备测试数据
    std::vector<std::vector<uint8_t>> test_cases = {
        {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 
         0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F}, // 标准二进制数据
        std::vector<uint8_t>(object_size, 0x00), // 全零
        std::vector<uint8_t>(object_size, 0xFF), // 全1
        {0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x11, 0x22,
         0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA}, // 混合数据
    };
    
    // 插入测试数据
    for (const auto& test_data : test_cases) {
        EXPECT_EQ(test_data.size(), object_size);
        column->insert_data(reinterpret_cast<const char*>(test_data.data()), object_size);
    }
    
    // 序列化每一行并验证
    FormatOptions options;
    auto output_col = ColumnString::create();
    output_col->reserve(column->size());
    VectorBufferWriter buffer_writer(*output_col.get());
    
    for (size_t i = 0; i < column->size(); ++i) {
        Status st = agg_state_serde->serialize_one_cell_to_json(*column, i, buffer_writer, options);
        EXPECT_TRUE(st.ok()) << "Failed to serialize FixedLengthObject row " << i << ": " << st.to_string();
        
        buffer_writer.commit();
        
        // 获取序列化后的结果
        std::string serialized = output_col->get_data_at(i).to_string();
        
        // 验证是base64编码
        EXPECT_TRUE(isValidBase64(serialized)) 
            << "Serialized FixedLengthObject is not valid base64 for row " << i 
            << ": " << serialized;
        
        // 验证可以解码回原始数据
        std::string decoded;
        bool decode_success = doris::base64_decode(serialized, &decoded);
        EXPECT_TRUE(decode_success) << "Failed to decode base64 for row " << i;
        EXPECT_EQ(decoded.size(), object_size) << "Decoded size mismatch for row " << i;
        EXPECT_EQ(memcmp(decoded.data(), test_cases[i].data(), object_size), 0)
            << "Decoded data doesn't match original for row " << i;
    }
}

// 测试空数据的处理
TEST_F(DataTypeAggStateSerdeTest, SerializeEmptyData) {
    FormatOptions options;
    auto output_col = ColumnString::create();
    VectorBufferWriter buffer_writer(*output_col.get());
    
    // 测试空字符串
    {
        auto column = ColumnString::create();
        column->insert_data("", 0);
        
        Status st = agg_state_serde->serialize_one_cell_to_json(*column, 0, buffer_writer, options);
        EXPECT_TRUE(st.ok()) << "Failed to serialize empty string: " << st.to_string();
        buffer_writer.commit();
        
        std::string serialized = output_col->get_data_at(0).to_string();
        // 空字符串编码后应该也是空字符串或有效的base64
        std::string decoded;
        bool decode_success = doris::base64_decode(serialized, &decoded);
        EXPECT_TRUE(decode_success);
        EXPECT_EQ(decoded, "");
    }
}

// 测试CSV特殊字符的处理
TEST_F(DataTypeAggStateSerdeTest, SerializeCsvSpecialChars) {
    auto column = ColumnString::create();
    
    // 包含各种CSV特殊字符的数据
    std::vector<std::string> special_char_cases = {
        "data,with,commas",
        "data\nwith\nnewlines",
        "data\twith\ttabs",
        "data\"with\"quotes",
        "data,with\nall\tspecial\"chars",
        "leading,trailing,", // 以逗号结尾
        ",leading,comma", // 以逗号开头
        "\"quoted\"",
        "\n\nmultiple\nnewlines\n",
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
        EXPECT_TRUE(st.ok()) << "Failed to serialize special char case " << i << ": " << st.to_string();
        
        buffer_writer.commit();
        
        std::string serialized = output_col->get_data_at(i).to_string();
        
        // 验证base64编码后的字符串不包含CSV特殊字符（除了可能的分隔符+和/）
        // 但不应包含换行符、制表符、逗号、引号等
        EXPECT_EQ(serialized.find('\n'), std::string::npos) << "Base64 contains newline at row " << i;
        EXPECT_EQ(serialized.find('\t'), std::string::npos) << "Base64 contains tab at row " << i;
        EXPECT_EQ(serialized.find(','), std::string::npos) << "Base64 contains comma at row " << i;
        EXPECT_EQ(serialized.find('"'), std::string::npos) << "Base64 contains quote at row " << i;
        
        // 验证可以正确解码
        std::string decoded;
        bool decode_success = doris::base64_decode(serialized, &decoded);
        EXPECT_TRUE(decode_success) << "Failed to decode for row " << i;
        EXPECT_EQ(decoded, special_char_cases[i]) << "Decoded data mismatch for row " << i;
    }
}

// 测试大数据量的序列化
TEST_F(DataTypeAggStateSerdeTest, SerializeLargeData) {
    auto column = ColumnString::create();
    
    // 不同大小的数据
    std::vector<size_t> sizes = {1024, 4096, 16384, 65536, 1048576}; // 1KB到1MB
    
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
        EXPECT_TRUE(st.ok()) << "Failed to serialize large data row " << i << ": " << st.to_string();
        
        buffer_writer.commit();
        
        std::string serialized = output_col->get_data_at(i).to_string();
        
        // 验证是有效的base64
        EXPECT_TRUE(isValidBase64(serialized)) << "Large data serialization invalid base64 at row " << i;
        
        // 验证可以解码
        std::string decoded;
        bool decode_success = doris::base64_decode(serialized, &decoded);
        EXPECT_TRUE(decode_success) << "Failed to decode large data for row " << i;
        EXPECT_EQ(decoded.size(), sizes[i]) << "Decoded size mismatch for row " << i;
    }
}

// 测试随机数据的序列化
TEST_F(DataTypeAggStateSerdeTest, SerializeRandomData) {
    auto column = ColumnString::create();
    
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(1, 1000); // 1到1000字节的随机长度
    std::uniform_int_distribution<uint8_t> byte_dis(0, 255); // 随机字节值
    
    // 生成10个随机数据
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
        EXPECT_TRUE(st.ok()) << "Failed to serialize random data row " << i << ": " << st.to_string();
        
        buffer_writer.commit();
        
        std::string serialized = output_col->get_data_at(i).to_string();
        EXPECT_TRUE(isValidBase64(serialized)) << "Random data serialization invalid base64 at row " << i;
        
        // 验证可以解码
        std::string decoded;
        bool decode_success = doris::base64_decode(serialized, &decoded);
        EXPECT_TRUE(decode_success) << "Failed to decode random data for row " << i;
        EXPECT_EQ(decoded.size(), column->get_data_at(i).size) 
            << "Decoded size mismatch for row " << i;
    }
}

// 测试serialize_one_cell_to_hive_text方法
TEST_F(DataTypeAggStateSerdeTest, SerializeToHiveText) {
    auto column = ColumnString::create();
    column->insert_data("test_agg_state_data", 19);
    
    FormatOptions options;
    auto output_col = ColumnString::create();
    VectorBufferWriter buffer_writer(*output_col.get());
    
    Status st = agg_state_serde->serialize_one_cell_to_hive_text(
        *column, 0, buffer_writer, options, 0);
    EXPECT_TRUE(st.ok()) << "Failed to serialize to hive text: " << st.to_string();
    
    buffer_writer.commit();
    
    std::string serialized = output_col->get_data_at(0).to_string();
    EXPECT_TRUE(isValidBase64(serialized)) << "Hive text serialization invalid base64";
    
    // 验证可以解码
    std::string decoded;
    bool decode_success = doris::base64_decode(serialized, &decoded);
    EXPECT_TRUE(decode_success);
    EXPECT_EQ(decoded, "test_agg_state_data");
}

// 测试base64编码值的正确性（与标准base64库对比）
TEST_F(DataTypeAggStateSerdeTest, Base64EncodingCorrectness) {
    // 标准的base64测试用例（来自RFC 4648）
    std::vector<std::pair<std::string, std::string>> standard_cases = {
        {"", ""},           // 空字符串
        {"f", "Zg=="},      // 1字节
        {"fo", "Zm8="},     // 2字节
        {"foo", "Zm9v"},    // 3字节（无padding）
        {"foob", "Zm9vYg=="}, // 4字节
        {"fooba", "Zm9vYmE="}, // 5字节
        {"foobar", "Zm9vYmFy"}, // 6字节（无padding）
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
        
        // 验证编码结果与预期一致
        if (!standard_cases[i].second.empty() || !serialized.empty()) {
            // 注意：由于我们的实现可能对空字符串有特殊处理，这里允许一些灵活性
            if (standard_cases[i].first.empty()) {
                // 空字符串可能编码为空字符串或有效的base64
                EXPECT_TRUE(serialized.empty() || isValidBase64(serialized));
            } else {
                EXPECT_EQ(serialized, standard_cases[i].second) 
                    << "Base64 encoding mismatch for case " << i 
                    << ", input: " << standard_cases[i].first;
            }
        }
    }
}

// 测试边界情况：单个字节的各种值
TEST_F(DataTypeAggStateSerdeTest, SerializeSingleByteValues) {
    auto column = ColumnString::create();
    
    // 测试所有可能的单字节值（0-255）
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
        
        // 验证可以解码回原始字节
        std::string decoded;
        bool decode_success = doris::base64_decode(serialized, &decoded);
        EXPECT_TRUE(decode_success) << "Failed to decode byte value " << i;
        EXPECT_EQ(decoded.size(), 1) << "Decoded size mismatch for byte value " << i;
        EXPECT_EQ(static_cast<uint8_t>(decoded[0]), static_cast<uint8_t>(i))
            << "Decoded value mismatch for byte value " << i;
    }
}

// 测试序列化性能：验证不会因为数据大小而失败
TEST_F(DataTypeAggStateSerdeTest, SerializePerformanceStress) {
    auto column = ColumnString::create();
    
    // 测试不同大小的数据，确保都能正常序列化
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
        EXPECT_TRUE(st.ok()) << "Failed to serialize stress test size " << stress_sizes[i] 
                            << ": " << st.to_string();
        
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

