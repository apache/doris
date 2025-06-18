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
//  1. Content of file `be/test/util/test_data/jsonb_serialize_test_data.bin` should NOT be modified.
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

    JsonbValue* check_object_and_get_value(JsonbValue* jsonb_value, const std::string& key) {
        EXPECT_TRUE(jsonb_value->isObject()) << "JsonbValue is not an object";
        auto* object_val = static_cast<ObjectVal*>(jsonb_value);
        auto* value = object_val->find(key.c_str(), key.length());
        EXPECT_TRUE(value != nullptr) << "Value not found for key: " + key;
        return value;
    }

    void check_jsonb_value_type(JsonbValue* jsonb_value, JsonbType expected_type) {
        ASSERT_TRUE(jsonb_value != nullptr) << "JsonbValue is null";
        ASSERT_TRUE(jsonb_value->type() == expected_type)
                << "JsonbValue type does not match expected type: "
                << static_cast<JsonbTypeUnder>(jsonb_value->type()) << " vs "
                << static_cast<JsonbTypeUnder>(expected_type);
    }

    template <typename T>
    void check_jsonb_value(JsonbValue* jsonb_value, const T& value) {
        if constexpr (std::is_same_v<T, std::string>) {
            check_jsonb_value_type(jsonb_value, JsonbType::T_String);
            auto* str_val = static_cast<JsonbStringVal*>(jsonb_value);
            ASSERT_EQ(std::string_view(str_val->getBlob(), str_val->length()), value)
                    << "String value does not match expected value";
        } else if constexpr (std::is_same_v<T, bool>) {
            ASSERT_TRUE(jsonb_value->isTrue() || jsonb_value->isFalse())
                    << "JsonbValue is not a boolean";
            ASSERT_EQ(jsonb_value->isTrue(), value)
                    << "Boolean value does not match expected value";
        } else if constexpr (std::is_integral_v<T>) {
            ASSERT_TRUE(jsonb_value->isInt()) << "JsonbValue is not an integer";
            auto* int_val = static_cast<JsonbIntVal*>(jsonb_value);
            ASSERT_EQ(int_val->val(), value) << "Integer value does not match expected value";
        } else if constexpr (std::is_floating_point_v<T>) {
            check_jsonb_value_type(jsonb_value, JsonbType::T_Double);
            auto* double_val = static_cast<JsonbDoubleVal*>(jsonb_value);
            ASSERT_DOUBLE_EQ(double_val->val(), value)
                    << "Double value does not match expected value";
        } else {
            FAIL() << "Unsupported type for JsonbValue check";
        }
    }

    void check_jsonb_null(JsonbValue* jsonb_value) {
        ASSERT_TRUE(jsonb_value != nullptr) << "JsonbValue is null";
        ASSERT_TRUE(jsonb_value->isNull()) << "JsonbValue is not null";
    }

protected:
    inline static std::string data_path = "/util/test_data/jsonb_serialize_test_data.bin";
};

TEST_F(JsonbSerializeTest, serialize) {
    JsonbParser parser;
    ASSERT_TRUE(parser.parse(JSON_DATA_STRING)) << "Failed to parse JSON data";

    auto& writer = parser.getWriter();
    auto* output = writer.getOutput();

    ASSERT_TRUE(output != nullptr) << "Output stream is null";

    auto serialized_data = load_serialized_jsonb_data();
    ASSERT_EQ(serialized_data.size(), output->getSize())
            << "Serialized data size does not match expected size";
    ASSERT_EQ(memcmp(output->getBuffer(), serialized_data.data(), serialized_data.size()), 0)
            << "Serialized data does not match expected data";

    auto* jsonb_doc = JsonbDocument::createDocument(output->getBuffer(), output->getSize());
    ASSERT_TRUE(jsonb_doc != nullptr) << "Failed to create JsonbDocument";

    auto* jsonb_value = jsonb_doc->getValue();

    auto* string_value = check_object_and_get_value(jsonb_value, "string_example");
    check_jsonb_value(string_value, std::string("Hello, world!"));

    auto* int_value = check_object_and_get_value(jsonb_value, "int_number");
    check_jsonb_value(int_value, 42);

    auto* float_value = check_object_and_get_value(jsonb_value, "float_number");
    check_jsonb_value(float_value, 3.14159);

    auto* boolean_true_value = check_object_and_get_value(jsonb_value, "boolean_true");
    check_jsonb_value(boolean_true_value, true);

    auto* boolean_false_value = check_object_and_get_value(jsonb_value, "boolean_false");
    check_jsonb_value(boolean_false_value, false);

    auto* null_value = check_object_and_get_value(jsonb_value, "null_value");
    check_jsonb_null(null_value);

    auto* array_value = check_object_and_get_value(jsonb_value, "array_example");
    ASSERT_TRUE(array_value->isArray()) << "JsonbValue is not an array";
    auto* array_val = static_cast<ArrayVal*>(array_value);
    ASSERT_EQ(array_val->numElem(), 5) << "Array size does not match expected size";

    for (auto it = array_val->begin(); it != array_val->end(); ++it) {
        auto* elem = &(*it);
        if (elem->isString()) {
            check_jsonb_value(elem, std::string("apple"));
        } else if (elem->isInt()) {
            check_jsonb_value(elem, 123);
        } else if (elem->isTrue()) {
            check_jsonb_value(elem, true);
        } else if (elem->isNull()) {
            check_jsonb_null(elem);
        } else if (elem->isObject()) {
            auto* nested_value = check_object_and_get_value(elem, "nested_key");
            check_jsonb_value(nested_value, std::string("nested_value"));
        }
    }

    auto* object_value = check_object_and_get_value(jsonb_value, "object_example");
    ASSERT_TRUE(object_value->isObject()) << "JsonbValue is not an object";

    auto* nested_value = check_object_and_get_value(object_value, "name");
    check_jsonb_value(nested_value, std::string("Alice"));

    nested_value = check_object_and_get_value(object_value, "age");
    check_jsonb_value(nested_value, 30);

    nested_value = check_object_and_get_value(object_value, "is_member");
    check_jsonb_value(nested_value, false);

    nested_value = check_object_and_get_value(object_value, "preferences");
    auto* nested_obj_value = check_object_and_get_value(nested_value, "colors");

    ASSERT_TRUE(nested_obj_value->isArray()) << "Preferences colors is not an array";
    auto* colors_array = static_cast<ArrayVal*>(nested_obj_value);
    ASSERT_EQ(colors_array->numElem(), 3) << "Colors array size does not match expected size";

    std::vector<std::string> expected_colors = {"red", "green", "blue"};
    int index = 0;
    for (auto it = colors_array->begin(); it != colors_array->end(); ++it, ++index) {
        auto* color_elem = &(*it);
        ASSERT_TRUE(color_elem->isString()) << "Color element is not a string";
        auto* color_val = static_cast<JsonbStringVal*>(color_elem);
        ASSERT_EQ(std::string_view(color_val->getBlob(), color_val->length()),
                  expected_colors[index])
                << "Color value does not match expected value";
    }

    nested_value = check_object_and_get_value(nested_value, "notifications");
    ASSERT_TRUE(nested_value->isObject()) << "Notifications is not an object";

    auto* email_value = check_object_and_get_value(nested_value, "email");
    check_jsonb_value(email_value, true);
    auto* sms_value = check_object_and_get_value(nested_value, "sms");
    check_jsonb_value(sms_value, false);
}

} // namespace doris
