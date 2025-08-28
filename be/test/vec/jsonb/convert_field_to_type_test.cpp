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

#include "vec/data_types/convert_field_to_type.cpp"

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "runtime/jsonb_value.h"
#include "util/jsonb_document.h"
#include "util/jsonb_writer.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_jsonb.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris::vectorized {

class ConvertFieldToTypeTest : public ::testing::Test {
protected:
    void SetUp() override {}
};

// Test FieldVisitorToJsonb with different field types using the same pattern as convert_field_to_typeImpl
TEST_F(ConvertFieldToTypeTest, FieldVisitorToJsonb_Null) {
    JsonbWriter writer;

    // Test null field using Field::dispatch pattern
    Field null_field;
    Field::dispatch([&writer](const auto& value) { FieldVisitorToJsonb()(value, &writer); },
                    null_field);

    auto* output = writer.getOutput();
    ASSERT_NE(output, nullptr);
    ASSERT_GT(output->getSize(), 0);

    // Verify the output is valid JSONB
    JsonbDocument* doc = nullptr;
    auto status =
            JsonbDocument::checkAndCreateDocument(output->getBuffer(), output->getSize(), &doc);
    ASSERT_TRUE(status.ok()) << "Failed to create JsonbDocument: " << status.to_string();
    ASSERT_NE(doc, nullptr);

    // Verify it's a null value
    ASSERT_TRUE(doc->getValue()->isNull());
}

TEST_F(ConvertFieldToTypeTest, FieldVisitorToJsonb_Int64) {
    JsonbWriter writer;

    // Test Int64 field using Field::dispatch pattern
    Int64 test_value = 12345;
    Field int_field = test_value;
    Field::dispatch([&writer](const auto& value) { FieldVisitorToJsonb()(value, &writer); },
                    int_field);

    auto* output = writer.getOutput();
    ASSERT_NE(output, nullptr);
    ASSERT_GT(output->getSize(), 0);

    // Verify the output is valid JSONB
    JsonbDocument* doc = nullptr;
    auto status =
            JsonbDocument::checkAndCreateDocument(output->getBuffer(), output->getSize(), &doc);
    ASSERT_TRUE(status.ok()) << "Failed to create JsonbDocument: " << status.to_string();
    ASSERT_NE(doc, nullptr);

    // Verify it's an integer value
    ASSERT_TRUE(doc->getValue()->isInt64());
    ASSERT_EQ(((const JsonbIntVal*)doc->getValue())->val(), test_value);
}

TEST_F(ConvertFieldToTypeTest, FieldVisitorToJsonb_UInt64) {
    JsonbWriter writer;

    // Test UInt64 field using Field::dispatch pattern
    UInt64 test_value = 12345;
    Field uint_field = test_value;
    Field::dispatch([&writer](const auto& value) { FieldVisitorToJsonb()(value, &writer); },
                    uint_field);

    auto* output = writer.getOutput();
    ASSERT_NE(output, nullptr);
    ASSERT_GT(output->getSize(), 0);

    // Verify the output is valid JSONB
    JsonbDocument* doc = nullptr;
    auto status =
            JsonbDocument::checkAndCreateDocument(output->getBuffer(), output->getSize(), &doc);
    ASSERT_TRUE(status.ok()) << "Failed to create JsonbDocument: " << status.to_string();
    ASSERT_NE(doc, nullptr);

    // Verify it's an integer value
    ASSERT_TRUE(doc->getValue()->isInt64());
    ASSERT_EQ(((const JsonbIntVal*)doc->getValue())->val(), static_cast<Int64>(test_value));
}

TEST_F(ConvertFieldToTypeTest, FieldVisitorToJsonb_Float64) {
    JsonbWriter writer;

    // Test Float64 field using Field::dispatch pattern
    Float64 test_value = 123.456;
    Field double_field = test_value;
    Field::dispatch([&writer](const auto& value) { FieldVisitorToJsonb()(value, &writer); },
                    double_field);

    auto* output = writer.getOutput();
    ASSERT_NE(output, nullptr);
    ASSERT_GT(output->getSize(), 0);

    // Verify the output is valid JSONB
    JsonbDocument* doc = nullptr;
    auto status =
            JsonbDocument::checkAndCreateDocument(output->getBuffer(), output->getSize(), &doc);
    ASSERT_TRUE(status.ok()) << "Failed to create JsonbDocument: " << status.to_string();
    ASSERT_NE(doc, nullptr);

    // Verify it's a double value
    ASSERT_TRUE(doc->getValue()->isDouble());
    ASSERT_DOUBLE_EQ(((const JsonbDoubleVal*)doc->getValue())->val(), test_value);
}

TEST_F(ConvertFieldToTypeTest, FieldVisitorToJsonb_String) {
    JsonbWriter writer;

    // Test String field using Field::dispatch pattern
    Field string_field = "hello world";
    Field::dispatch([&writer](const auto& value) { FieldVisitorToJsonb()(value, &writer); },
                    string_field);

    auto* output = writer.getOutput();
    ASSERT_NE(output, nullptr);
    ASSERT_GT(output->getSize(), 0);

    // Verify the output is valid JSONB
    JsonbDocument* doc = nullptr;
    auto status =
            JsonbDocument::checkAndCreateDocument(output->getBuffer(), output->getSize(), &doc);
    ASSERT_TRUE(status.ok()) << "Failed to create JsonbDocument: " << status.to_string();
    ASSERT_NE(doc, nullptr);

    // Verify it's a string value
    ASSERT_TRUE(doc->getValue()->isString());
    const auto* string_val = static_cast<const JsonbBlobVal*>(doc->getValue());
    std::string real_string(string_val->getBlob(), string_val->getBlobLen());
    ASSERT_EQ(real_string, string_field.get<String>());
}

TEST_F(ConvertFieldToTypeTest, FieldVisitorToJsonb_JsonbField) {
    JsonbWriter writer;
    JsonBinaryValue jsonb_value;
    std::string test_data = R"({"a": ["1", "2"]})";
    THROW_IF_ERROR(jsonb_value.from_json_string(test_data.data(), test_data.size()));
    Field jsonb_field_obj = JsonbField(jsonb_value.value(), jsonb_value.size());

    // Test JsonbField using Field::dispatch pattern
    Field::dispatch([&writer](const auto& value) { FieldVisitorToJsonb()(value, &writer); },
                    jsonb_field_obj);

    auto* output = writer.getOutput();
    ASSERT_NE(output, nullptr);
    ASSERT_GT(output->getSize(), 0);

    // Verify the output is valid JSONB
    JsonbDocument* doc = nullptr;
    auto status =
            JsonbDocument::checkAndCreateDocument(output->getBuffer(), output->getSize(), &doc);
    ASSERT_TRUE(status.ok()) << "Failed to create JsonbDocument: " << status.to_string();
    ASSERT_NE(doc, nullptr);

    // Verify it's an object value
    ASSERT_TRUE(doc->getValue()->isObject());
}

TEST_F(ConvertFieldToTypeTest, FieldVisitorToJsonb_Array) {
    JsonbWriter writer;

    // Create an array with mixed types
    Array array_field;
    array_field.push_back(123);
    array_field.push_back("hello");
    array_field.push_back(456.789);

    Field array_obj = array_field;

    // Test Array using Field::dispatch pattern
    Field::dispatch([&writer](const auto& value) { FieldVisitorToJsonb()(value, &writer); },
                    array_obj);

    auto* output = writer.getOutput();
    ASSERT_NE(output, nullptr);
    ASSERT_GT(output->getSize(), 0);

    // Verify the output is valid JSONB
    JsonbDocument* doc = nullptr;
    auto status =
            JsonbDocument::checkAndCreateDocument(output->getBuffer(), output->getSize(), &doc);
    ASSERT_TRUE(status.ok()) << "Failed to create JsonbDocument: " << status.to_string();
    ASSERT_NE(doc, nullptr);

    // Verify it's an array value
    ASSERT_TRUE(doc->getValue()->isArray());
    const ArrayVal& array = static_cast<const ArrayVal&>(*doc->getValue());
    ASSERT_EQ(array.numElem(), 3);
}

TEST_F(ConvertFieldToTypeTest, FieldVisitorToJsonb_NestedArray) {
    JsonbWriter writer;

    // Create a nested array
    Array inner_array;
    inner_array.push_back(1);
    inner_array.push_back(2);

    Array outer_array;
    outer_array.push_back(inner_array);
    outer_array.push_back("nested");

    Field nested_array_obj = outer_array;

    // Test nested Array using Field::dispatch pattern
    Field::dispatch([&writer](const auto& value) { FieldVisitorToJsonb()(value, &writer); },
                    nested_array_obj);

    auto* output = writer.getOutput();
    ASSERT_NE(output, nullptr);
    ASSERT_GT(output->getSize(), 0);

    // Verify the output is valid JSONB
    JsonbDocument* doc = nullptr;
    auto status =
            JsonbDocument::checkAndCreateDocument(output->getBuffer(), output->getSize(), &doc);
    ASSERT_TRUE(status.ok()) << "Failed to create JsonbDocument: " << status.to_string();
    ASSERT_NE(doc, nullptr);

    // Verify it's an array value
    ASSERT_TRUE(doc->getValue()->isArray());
    const ArrayVal& array = static_cast<const ArrayVal&>(*doc->getValue());
    ASSERT_EQ(array.numElem(), 2);
}

TEST_F(ConvertFieldToTypeTest, FieldVisitorToJsonb_LargeInt) {
    JsonbWriter writer;

    // Test Int128 field using Field::dispatch pattern
    Int128 test_value = 1234567890123456789;
    Field largeint_field = test_value;
    Field::dispatch([&writer](const auto& value) { FieldVisitorToJsonb()(value, &writer); },
                    largeint_field);

    auto* output = writer.getOutput();
    ASSERT_NE(output, nullptr);
    ASSERT_GT(output->getSize(), 0);

    // Verify the output is valid JSONB
    JsonbDocument* doc = nullptr;
    auto status =
            JsonbDocument::checkAndCreateDocument(output->getBuffer(), output->getSize(), &doc);
    ASSERT_TRUE(status.ok()) << "Failed to create JsonbDocument: " << status.to_string();
    ASSERT_NE(doc, nullptr);

    // Verify it's an int128 value
    ASSERT_TRUE(doc->getValue()->isInt128());
    ASSERT_EQ(((const JsonbIntVal*)doc->getValue())->val(), test_value);
}

TEST_F(ConvertFieldToTypeTest, FieldVisitorToJsonb_UInt128) {
    JsonbWriter writer;

    // Test UInt128 field using Field::dispatch pattern
    UInt128 test_value = 1234567890123456789;
    Field uint128_field = test_value;
    Field::dispatch([&writer](const auto& value) { FieldVisitorToJsonb()(value, &writer); },
                    uint128_field);

    auto* output = writer.getOutput();
    ASSERT_NE(output, nullptr);
    ASSERT_GT(output->getSize(), 0);

    // Verify the output is valid JSONB
    JsonbDocument* doc = nullptr;
    auto status =
            JsonbDocument::checkAndCreateDocument(output->getBuffer(), output->getSize(), &doc);
    ASSERT_TRUE(status.ok()) << "Failed to create JsonbDocument: " << status.to_string();
    ASSERT_NE(doc, nullptr);

    // Verify it's an int128 value
    ASSERT_TRUE(doc->getValue()->isInt128());
    ASSERT_EQ(((const JsonbIntVal*)doc->getValue())->val(), static_cast<Int128>(test_value));
}

// Test convert_field_to_type function with JSONB type (similar to convert_field_to_typeImpl)
TEST_F(ConvertFieldToTypeTest, ConvertFieldToType_ToJsonb) {
    DataTypeJsonb jsonb_type;

    // Test converting Int64 to JSONB
    {
        Int64 test_value = 12345;
        Field int_field = test_value;
        Field result;

        convert_field_to_type(int_field, jsonb_type, &result);

        ASSERT_EQ(result.get_type(), Field::Types::JSONB);
        ASSERT_FALSE(result.is_null());

        const JsonbField& jsonb_result = result.get<JsonbField>();
        ASSERT_NE(jsonb_result.get_value(), nullptr);
        ASSERT_GT(jsonb_result.get_size(), 0);

        // Verify the JSONB content
        JsonbDocument* doc = nullptr;
        auto status = JsonbDocument::checkAndCreateDocument(jsonb_result.get_value(),
                                                            jsonb_result.get_size(), &doc);
        ASSERT_TRUE(status.ok()) << "Failed to create JsonbDocument: " << status.to_string();
        ASSERT_NE(doc, nullptr);
        ASSERT_TRUE(doc->getValue()->isInt64());
        ASSERT_EQ(((const JsonbIntVal*)doc->getValue())->val(), test_value);
    }

    // Test converting String to JSONB
    {
        Field string_field = "hello world";
        Field result;

        convert_field_to_type(string_field, jsonb_type, &result);

        ASSERT_EQ(result.get_type(), Field::Types::JSONB);
        ASSERT_FALSE(result.is_null());

        const JsonbField& jsonb_result = result.get<JsonbField>();
        ASSERT_NE(jsonb_result.get_value(), nullptr);
        ASSERT_GT(jsonb_result.get_size(), 0);

        // Verify the JSONB content
        JsonbDocument* doc = nullptr;
        auto status = JsonbDocument::checkAndCreateDocument(jsonb_result.get_value(),
                                                            jsonb_result.get_size(), &doc);
        ASSERT_TRUE(status.ok()) << "Failed to create JsonbDocument: " << status.to_string();
        ASSERT_NE(doc, nullptr);
        ASSERT_TRUE(doc->getValue()->isString());
        const auto* string_val = static_cast<const JsonbBlobVal*>(doc->getValue());
        std::string real_string(string_val->getBlob(), string_val->getBlobLen());
        ASSERT_EQ(real_string, string_field.get<String>());
    }

    // Test converting Array to JSONB
    {
        Array array_field;
        array_field.push_back(1);
        array_field.push_back("test");
        array_field.push_back(3.14);

        Field array_obj = array_field;
        Field result;

        convert_field_to_type(array_obj, jsonb_type, &result);

        ASSERT_EQ(result.get_type(), Field::Types::JSONB);
        ASSERT_FALSE(result.is_null());

        const JsonbField& jsonb_result = result.get<JsonbField>();
        ASSERT_NE(jsonb_result.get_value(), nullptr);
        ASSERT_GT(jsonb_result.get_size(), 0);

        // Verify the JSONB content
        JsonbDocument* doc = nullptr;
        auto status = JsonbDocument::checkAndCreateDocument(jsonb_result.get_value(),
                                                            jsonb_result.get_size(), &doc);
        ASSERT_TRUE(status.ok()) << "Failed to create JsonbDocument: " << status.to_string();
        ASSERT_NE(doc, nullptr);
        ASSERT_TRUE(doc->getValue()->isArray());
        const ArrayVal& array = static_cast<const ArrayVal&>(*doc->getValue());
        ASSERT_EQ(array.numElem(), 3);
    }

    // Test converting JSONB to JSONB (should be no-op)
    {
        JsonbWriter test_writer;
        test_writer.writeStartObject();
        test_writer.writeKey("key");
        test_writer.writeString("value");
        test_writer.writeEndObject();

        auto* test_output = test_writer.getOutput();
        JsonbField original_jsonb(test_output->getBuffer(), test_output->getSize());
        Field jsonb_field = original_jsonb;
        Field result;

        convert_field_to_type(jsonb_field, jsonb_type, &result);

        ASSERT_EQ(result.get_type(), Field::Types::JSONB);
        ASSERT_FALSE(result.is_null());

        const JsonbField& jsonb_result = result.get<JsonbField>();
        ASSERT_NE(jsonb_result.get_value(), nullptr);
        ASSERT_EQ(jsonb_result.get_size(), original_jsonb.get_size());
        ASSERT_EQ(memcmp(jsonb_result.get_value(), original_jsonb.get_value(),
                         original_jsonb.get_size()),
                  0);
    }
}

// Test convert_field_to_type with nullable JSONB type
TEST_F(ConvertFieldToTypeTest, ConvertFieldToType_ToNullableJsonb) {
    auto nullable_jsonb_type =
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeJsonb>());

    // Test converting null field
    {
        Field null_field;
        Field result;

        convert_field_to_type(null_field, *nullable_jsonb_type, &result);

        ASSERT_TRUE(result.is_null());
    }

    // Test converting non-null field
    {
        Field string_field = "test string";
        Field result;

        convert_field_to_type(string_field, *nullable_jsonb_type, &result);

        ASSERT_EQ(result.get_type(), Field::Types::JSONB);
        ASSERT_FALSE(result.is_null());

        const JsonbField& jsonb_result = result.get<JsonbField>();
        ASSERT_NE(jsonb_result.get_value(), nullptr);
        ASSERT_GT(jsonb_result.get_size(), 0);

        // Verify the JSONB content
        JsonbDocument* doc = nullptr;
        auto status = JsonbDocument::checkAndCreateDocument(jsonb_result.get_value(),
                                                            jsonb_result.get_size(), &doc);
        ASSERT_TRUE(status.ok()) << "Failed to create JsonbDocument: " << status.to_string();
        ASSERT_NE(doc, nullptr);
        ASSERT_TRUE(doc->getValue()->isString());
        const auto* string_val = static_cast<const JsonbBlobVal*>(doc->getValue());
        std::string real_string(string_val->getBlob(), string_val->getBlobLen());
        ASSERT_EQ(real_string, string_field.get<String>());
    }
}

// Test convert_field_to_type with array of JSONB
TEST_F(ConvertFieldToTypeTest, ConvertFieldToType_ArrayToJsonb) {
    auto array_jsonb_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeJsonb>());

    // Create an array with mixed types that will be converted to JSONB
    Array array_field;
    array_field.push_back(123);
    array_field.push_back("hello");
    array_field.push_back(456.789);

    Field array_obj = array_field;
    Field result;

    convert_field_to_type(array_obj, *array_jsonb_type, &result);

    ASSERT_EQ(result.get_type(), Field::Types::Array);
    ASSERT_FALSE(result.is_null());

    const Array& result_array = result.get<Array>();
    ASSERT_EQ(result_array.size(), 3);

    // Verify each element is converted to JSONB
    for (size_t i = 0; i < result_array.size(); ++i) {
        ASSERT_EQ(result_array[i].get_type(), Field::Types::JSONB);
        ASSERT_FALSE(result_array[i].is_null());

        const auto& jsonb_element = result_array[i].get<JsonbField>();
        ASSERT_NE(jsonb_element.get_value(), nullptr);
        ASSERT_GT(jsonb_element.get_size(), 0);

        // Verify the JSONB content
        JsonbDocument* doc = nullptr;
        auto status = JsonbDocument::checkAndCreateDocument(jsonb_element.get_value(),
                                                            jsonb_element.get_size(), &doc);
        ASSERT_TRUE(status.ok()) << "Failed to create JsonbDocument for element " << i << ": "
                                 << status.to_string();
        ASSERT_NE(doc, nullptr);
    }
}

// Test error cases
TEST_F(ConvertFieldToTypeTest, ConvertFieldToType_ErrorCases) {
    DataTypeJsonb jsonb_type;

    // Test with unsupported types (should throw exception)
    {
        Field tuple_field = Tuple();

        EXPECT_THROW(
                {
                    Field result;
                    convert_field_to_type(tuple_field, jsonb_type, &result);
                },
                doris::Exception);
    }
}

} // namespace doris::vectorized