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

#include <fstream>
#include <type_traits>

#include "testutil/test_util.h"
#include "util/jsonb_parser_simd.h"
#include "util/jsonb_writer.h"

// This test verifies the stability of Jsonb serialization format.
// DO NOT modify or remove this test!
// It serves as a guard against changes that may unintentionally break
// backward compatibility or introduce inconsistencies in serialized data.
//
// If the JSON format must be changed, please carefully review the impact
// on compatibility, and update this test accordingly with proper justification.
// NOTE:
//  1. These file should NOT be modified:
//     - `be/test/util/test_data/jsonb_serialize_test_data.bin`
//     - `be/test/util/test_data/jsonb_serialize_test_data2.bin`
//  2. Content of string `JSON_DATA_STRING` should NOT be modified.
//  3. If you need add more types in Jsonb format, you should write a new test case instead of modifying this one.

namespace doris {
static inline const std::string JSON_DATA_STRING = R"({
  "string_example": "Hello, world!",
  "int_number": 42,
  "float_number": 3.14159,
  "boolean_true": true,
  "boolean_false": false,
  "null_value": null,
  "array_example": [
    "apple",
    123,
    true,
    null,
    {
      "nested_key": "nested_value"
    }
  ],
  "object_example": {
    "name": "Alice",
    "age": 30,
    "is_member": false,
    "preferences": {
      "colors": ["red", "green", "blue"],
      "notifications": {
        "email": true,
        "sms": false
      }
    }
  }
}
)";

class JsonbSerializeTest : public testing::Test {
public:
    void SetUp() override {
        // Setup code if needed
    }

    void TearDown() override {
        // Cleanup code if needed
    }

    std::string load_serialized_jsonb_data() {
        std::string dir_path = GetCurrentRunningDir();
        std::string serialized_data_path = dir_path + data_path;
        std::ifstream ifs(serialized_data_path, std::ios::binary);
        if (!ifs.is_open()) {
            throw std::runtime_error("Failed to open file: " + serialized_data_path);
        }
        std::string data((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
        ifs.close();
        return data;
    }

    std::string load_serialized_jsonb_data2() {
        std::string dir_path = GetCurrentRunningDir();
        std::string serialized_data_path = dir_path + data_path_2;
        std::ifstream ifs(serialized_data_path, std::ios::binary);
        if (!ifs.is_open()) {
            throw std::runtime_error("Failed to open file: " + serialized_data_path);
        }
        std::string data((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
        ifs.close();
        return data;
    }

protected:
    inline static std::string data_path = "/util/test_data/jsonb_serialize_test_data.bin";
    inline static std::string data_path_2 = "/util/test_data/jsonb_serialize_test_data2.bin";
};

TEST_F(JsonbSerializeTest, serialize) {
    JsonbWriter writer;
    auto st = JsonbParser::parse(JSON_DATA_STRING.c_str(), JSON_DATA_STRING.size(), writer);
    ASSERT_TRUE(st.ok()) << "Failed to parse JSON data: " << st.to_string();

    auto* output = writer.getOutput();

    ASSERT_TRUE(output != nullptr) << "Output stream is null";

    auto serialized_data = load_serialized_jsonb_data();
    ASSERT_EQ(serialized_data.size(), output->getSize())
            << "Serialized data size does not match expected size";
    ASSERT_EQ(memcmp(output->getBuffer(), serialized_data.data(), serialized_data.size()), 0)
            << "Serialized data does not match expected data";

    JsonbDocument* jsonb_doc = nullptr;
    st = JsonbDocument::checkAndCreateDocument(output->getBuffer(), output->getSize(), &jsonb_doc);
    ASSERT_TRUE(st.ok()) << "Failed to create JsonbDocument: " << st.to_string();
}

// Add decimal types serialization test
TEST_F(JsonbSerializeTest, serialization2) {
    JsonbWriter writer;
    writer.writeStartObject();

    writer.writeKey("key_decimal32", 13);

    writer.writeKey("key_decimal32");
    vectorized::Decimal32 decimal_value32(int32_t(99999999));
    writer.writeDecimal(decimal_value32, 9, 4);

    writer.writeKey("key_decimal64");
    vectorized::Decimal64 decimal_value64(int64_t(999999999999999999ULL));
    writer.writeDecimal(decimal_value64, 18, 4);

    writer.writeKey("key_decimal128");
    vectorized::Decimal128V3 decimal_value((__int128_t(std::numeric_limits<uint64_t>::max())));
    writer.writeDecimal(decimal_value, 27, 8);

    writer.writeKey("key_decimal256");
    vectorized::Decimal256 decimal256_value(
            (wide::Int256(std::numeric_limits<__int128_t>::max()) * 2));
    writer.writeDecimal(decimal256_value, 40, 8);

    writer.writeEndObject();

    auto serialized_data = load_serialized_jsonb_data2();
    ASSERT_EQ(serialized_data.size(), writer.getOutput()->getSize())
            << "Serialized data size does not match expected size";

    ASSERT_EQ(
            memcmp(serialized_data.data(), writer.getOutput()->getBuffer(), serialized_data.size()),
            0);
}

} // namespace doris
