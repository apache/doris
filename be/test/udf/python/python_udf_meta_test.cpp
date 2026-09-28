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

#include "udf/python/python_udf_meta.h"

#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/util/base64.h>
#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include <memory>
#include <string>

#include "common/status.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/define_primitive_type.h"

namespace doris {

static arrow::Result<std::shared_ptr<arrow::Schema>> decode_arrow_schema(
        const std::string& encoded_schema) {
    auto buffer = arrow::Buffer::FromString(arrow::util::base64_decode(encoded_schema));
    arrow::io::BufferReader reader(buffer);
    arrow::ipc::DictionaryMemo dictionary_memo;
    return arrow::ipc::ReadSchema(&reader, &dictionary_memo);
}

class PythonUDFMetaTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create common test data types using PrimitiveType
        nullable_int32_ =
                DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, true);
        nullable_string_ =
                DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_STRING, true);
        nullable_double_ =
                DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_DOUBLE, true);
    }

    DataTypePtr nullable_int32_;
    DataTypePtr nullable_string_;
    DataTypePtr nullable_double_;
};

// ============================================================================
// PythonUDFMeta check() tests
// ============================================================================

TEST_F(PythonUDFMetaTest, CheckEmptyName) {
    PythonUDFMeta meta;
    meta.name = "";
    meta.symbol = "test_func";
    meta.runtime_version = "3.9.16";
    meta.input_types = {nullable_int32_};
    meta.return_type = nullable_int32_;
    meta.type = PythonUDFLoadType::INLINE;

    Status status = meta.check();
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("name is empty") != std::string::npos);
}

TEST_F(PythonUDFMetaTest, CheckEmptySymbol) {
    PythonUDFMeta meta;
    meta.name = "test_udf";
    meta.symbol = "";
    meta.runtime_version = "3.9.16";
    meta.input_types = {nullable_int32_};
    meta.return_type = nullable_int32_;
    meta.type = PythonUDFLoadType::INLINE;

    Status status = meta.check();
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("symbol is empty") != std::string::npos);
}

TEST_F(PythonUDFMetaTest, CheckEmptyRuntimeVersion) {
    PythonUDFMeta meta;
    meta.name = "test_udf";
    meta.symbol = "test_func";
    meta.runtime_version = "";
    meta.input_types = {nullable_int32_};
    meta.return_type = nullable_int32_;
    meta.type = PythonUDFLoadType::INLINE;

    Status status = meta.check();
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("runtime version is empty") != std::string::npos);
}

TEST_F(PythonUDFMetaTest, CheckEmptyInputTypesAllowedForUdf) {
    PythonUDFMeta meta;
    meta.name = "test_udf";
    meta.symbol = "test_func";
    meta.runtime_version = "3.9.16";
    meta.input_types = {};
    meta.return_type = nullable_int32_;
    meta.type = PythonUDFLoadType::INLINE;
    meta.client_type = PythonClientType::UDF;

    Status status = meta.check();
    EXPECT_TRUE(status.ok()) << status.to_string();
}

TEST_F(PythonUDFMetaTest, CheckEmptyInputTypesAllowedForUdtf) {
    PythonUDFMeta meta;
    meta.name = "test_udtf";
    meta.symbol = "test_func";
    meta.runtime_version = "3.9.16";
    meta.input_types = {};
    meta.return_type = nullable_string_;
    meta.type = PythonUDFLoadType::INLINE;
    meta.client_type = PythonClientType::UDTF;

    Status status = meta.check();
    EXPECT_TRUE(status.ok()) << status.to_string();
}

TEST_F(PythonUDFMetaTest, CheckEmptyInputTypesRejectedForUdaf) {
    PythonUDFMeta meta;
    meta.name = "test_udaf";
    meta.symbol = "test_func";
    meta.runtime_version = "3.9.16";
    meta.input_types = {};
    meta.return_type = nullable_int32_;
    meta.type = PythonUDFLoadType::INLINE;
    meta.client_type = PythonClientType::UDAF;

    Status status = meta.check();
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("input types is empty") != std::string::npos);
}

TEST_F(PythonUDFMetaTest, CheckNullReturnType) {
    PythonUDFMeta meta;
    meta.name = "test_udf";
    meta.symbol = "test_func";
    meta.runtime_version = "3.9.16";
    meta.input_types = {nullable_int32_};
    meta.return_type = nullptr;
    meta.type = PythonUDFLoadType::INLINE;

    Status status = meta.check();
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("return type is empty") != std::string::npos);
}

TEST_F(PythonUDFMetaTest, CheckUnknownLoadType) {
    PythonUDFMeta meta;
    meta.name = "test_udf";
    meta.symbol = "test_func";
    meta.runtime_version = "3.9.16";
    meta.input_types = {nullable_int32_};
    meta.return_type = nullable_int32_;
    meta.type = PythonUDFLoadType::UNKNOWN;

    Status status = meta.check();
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("load type is invalid") != std::string::npos);
}

TEST_F(PythonUDFMetaTest, CheckModuleTypeEmptyLocation) {
    PythonUDFMeta meta;
    meta.name = "test_udf";
    meta.symbol = "test_func";
    meta.runtime_version = "3.9.16";
    meta.input_types = {nullable_int32_};
    meta.return_type = nullable_int32_;
    meta.type = PythonUDFLoadType::MODULE;
    meta.location = "";
    meta.checksum = "abc123";

    Status status = meta.check();
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("location is empty") != std::string::npos);
}

TEST_F(PythonUDFMetaTest, CheckModuleTypeEmptyChecksum) {
    PythonUDFMeta meta;
    meta.name = "test_udf";
    meta.symbol = "test_func";
    meta.runtime_version = "3.9.16";
    meta.input_types = {nullable_int32_};
    meta.return_type = nullable_int32_;
    meta.type = PythonUDFLoadType::MODULE;
    meta.location = "/path/to/module.py";
    meta.checksum = "";

    Status status = meta.check();
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("checksum is empty") != std::string::npos);
}

TEST_F(PythonUDFMetaTest, CheckInlineTypeSuccess) {
    PythonUDFMeta meta;
    meta.name = "test_udf";
    meta.symbol = "test_func";
    meta.runtime_version = "3.9.16";
    meta.input_types = {nullable_int32_};
    meta.return_type = nullable_int32_;
    meta.type = PythonUDFLoadType::INLINE;
    meta.inline_code = "def test_func(x): return x + 1";

    Status status = meta.check();
    EXPECT_TRUE(status.ok()) << status.to_string();
}

TEST_F(PythonUDFMetaTest, CheckModuleTypeSuccess) {
    PythonUDFMeta meta;
    meta.name = "test_udf";
    meta.symbol = "test_func";
    meta.runtime_version = "3.9.16";
    meta.input_types = {nullable_int32_};
    meta.return_type = nullable_int32_;
    meta.type = PythonUDFLoadType::MODULE;
    meta.location = "/path/to/module.py";
    meta.checksum = "abc123def456";

    Status status = meta.check();
    EXPECT_TRUE(status.ok()) << status.to_string();
}

TEST_F(PythonUDFMetaTest, CheckWhitespaceOnlyName) {
    PythonUDFMeta meta;
    meta.name = "   ";
    meta.symbol = "test_func";
    meta.runtime_version = "3.9.16";
    meta.input_types = {nullable_int32_};
    meta.return_type = nullable_int32_;
    meta.type = PythonUDFLoadType::INLINE;

    Status status = meta.check();
    EXPECT_FALSE(status.ok());
}

// ============================================================================
// PythonUDFMeta serialize_to_json() tests
// ============================================================================

TEST_F(PythonUDFMetaTest, SerializeToJsonBasic) {
    PythonUDFMeta meta;
    meta.id = 1;
    meta.name = "test_udf";
    meta.symbol = "test_func";
    meta.location = "/path/to/udf.py";
    meta.runtime_version = "3.9.16";
    meta.always_nullable = true;
    meta.inline_code = "def test_func(x): return x + 1";
    meta.input_types = {nullable_int32_};
    meta.return_type = nullable_int32_;
    meta.type = PythonUDFLoadType::INLINE;
    meta.client_type = PythonClientType::UDF;

    std::string json_str;
    Status status = meta.serialize_to_json(&json_str);
    EXPECT_TRUE(status.ok()) << status.to_string();

    // Parse JSON and verify fields
    rapidjson::Document doc;
    doc.Parse(json_str.c_str());
    EXPECT_FALSE(doc.HasParseError());

    EXPECT_TRUE(doc.HasMember("id"));
    EXPECT_EQ(doc["id"].GetInt64(), 1);

    EXPECT_TRUE(doc.HasMember("name"));
    EXPECT_STREQ(doc["name"].GetString(), "test_udf");

    EXPECT_TRUE(doc.HasMember("symbol"));
    EXPECT_STREQ(doc["symbol"].GetString(), "test_func");

    EXPECT_TRUE(doc.HasMember("location"));
    EXPECT_STREQ(doc["location"].GetString(), "/path/to/udf.py");

    EXPECT_TRUE(doc.HasMember("runtime_version"));
    EXPECT_STREQ(doc["runtime_version"].GetString(), "3.9.16");

    EXPECT_TRUE(doc.HasMember("always_nullable"));
    EXPECT_TRUE(doc["always_nullable"].GetBool());

    EXPECT_TRUE(doc.HasMember("udf_load_type"));
    EXPECT_EQ(doc["udf_load_type"].GetInt(), static_cast<int>(PythonUDFLoadType::INLINE));

    EXPECT_TRUE(doc.HasMember("client_type"));
    EXPECT_EQ(doc["client_type"].GetInt(), static_cast<int>(PythonClientType::UDF));

    EXPECT_TRUE(doc.HasMember("inline_code"));
    EXPECT_TRUE(doc.HasMember("input_types"));
    EXPECT_TRUE(doc.HasMember("return_type"));

    EXPECT_EQ(arrow::util::base64_decode(doc["inline_code"].GetString()), meta.inline_code);

    auto input_schema_result = decode_arrow_schema(doc["input_types"].GetString());
    ASSERT_TRUE(input_schema_result.ok()) << input_schema_result.status().ToString();
    auto input_schema = *input_schema_result;
    ASSERT_EQ(input_schema->num_fields(), 1);
    EXPECT_TRUE(input_schema->field(0)->type()->Equals(arrow::int32()));
    EXPECT_TRUE(input_schema->field(0)->nullable());

    auto return_schema_result = decode_arrow_schema(doc["return_type"].GetString());
    ASSERT_TRUE(return_schema_result.ok()) << return_schema_result.status().ToString();
    auto return_schema = *return_schema_result;
    ASSERT_EQ(return_schema->num_fields(), 1);
    EXPECT_TRUE(return_schema->field(0)->type()->Equals(arrow::int32()));
    EXPECT_TRUE(return_schema->field(0)->nullable());
}

TEST_F(PythonUDFMetaTest, SerializeToJsonDifferentClientTypes) {
    PythonUDFMeta meta;
    meta.name = "test";
    meta.symbol = "func";
    meta.runtime_version = "3.9.16";
    meta.input_types = {nullable_int32_};
    meta.return_type = nullable_int32_;
    meta.type = PythonUDFLoadType::INLINE;

    // Test UDF client type
    meta.client_type = PythonClientType::UDF;
    std::string json_str;
    EXPECT_TRUE(meta.serialize_to_json(&json_str).ok());
    rapidjson::Document doc1;
    doc1.Parse(json_str.c_str());
    EXPECT_EQ(doc1["client_type"].GetInt(), 0);

    // Test UDAF client type
    meta.client_type = PythonClientType::UDAF;
    EXPECT_TRUE(meta.serialize_to_json(&json_str).ok());
    rapidjson::Document doc2;
    doc2.Parse(json_str.c_str());
    EXPECT_EQ(doc2["client_type"].GetInt(), 1);

    // Test UDTF client type
    meta.client_type = PythonClientType::UDTF;
    EXPECT_TRUE(meta.serialize_to_json(&json_str).ok());
    rapidjson::Document doc3;
    doc3.Parse(json_str.c_str());
    EXPECT_EQ(doc3["client_type"].GetInt(), 2);
}

TEST_F(PythonUDFMetaTest, SerializeToJsonMultipleInputTypes) {
    PythonUDFMeta meta;
    meta.name = "multi_arg";
    meta.symbol = "func";
    meta.runtime_version = "3.9.16";
    meta.input_types = {nullable_int32_, nullable_string_, nullable_double_};
    meta.return_type = nullable_int32_;
    meta.type = PythonUDFLoadType::INLINE;
    meta.client_type = PythonClientType::UDF;

    std::string json_str;
    Status status = meta.serialize_to_json(&json_str);
    EXPECT_TRUE(status.ok()) << status.to_string();

    rapidjson::Document doc;
    doc.Parse(json_str.c_str());
    EXPECT_FALSE(doc.HasParseError());
    auto input_schema_result = decode_arrow_schema(doc["input_types"].GetString());
    ASSERT_TRUE(input_schema_result.ok()) << input_schema_result.status().ToString();
    auto input_schema = *input_schema_result;
    ASSERT_EQ(input_schema->num_fields(), 3);
    EXPECT_TRUE(input_schema->field(0)->type()->Equals(arrow::int32()));
    EXPECT_TRUE(input_schema->field(1)->type()->Equals(arrow::utf8()));
    EXPECT_TRUE(input_schema->field(2)->type()->Equals(arrow::float64()));
    EXPECT_TRUE(input_schema->field(0)->nullable());
    EXPECT_TRUE(input_schema->field(1)->nullable());
    EXPECT_TRUE(input_schema->field(2)->nullable());
}

TEST_F(PythonUDFMetaTest, SerializeToJsonEmptyInputTypesForUdf) {
    PythonUDFMeta meta;
    meta.name = "zero_arg_udf";
    meta.symbol = "func";
    meta.runtime_version = "3.9.16";
    meta.input_types = {};
    meta.return_type = nullable_int32_;
    meta.type = PythonUDFLoadType::INLINE;
    meta.client_type = PythonClientType::UDF;

    std::string json_str;
    Status status = meta.serialize_to_json(&json_str);
    EXPECT_TRUE(status.ok()) << status.to_string();

    rapidjson::Document doc;
    doc.Parse(json_str.c_str());
    EXPECT_FALSE(doc.HasParseError());
    auto input_schema_result = decode_arrow_schema(doc["input_types"].GetString());
    ASSERT_TRUE(input_schema_result.ok()) << input_schema_result.status().ToString();
    EXPECT_EQ((*input_schema_result)->num_fields(), 0);
}

// ============================================================================
// PythonUDFMeta convert_types_to_schema() tests
// ============================================================================

TEST_F(PythonUDFMetaTest, ConvertTypesToSchemaBasic) {
    DataTypes types = {nullable_int32_, nullable_string_};
    std::shared_ptr<arrow::Schema> schema;

    Status status = PythonUDFMeta::convert_types_to_schema(types, TimezoneUtils::default_time_zone,
                                                           &schema);
    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_NE(schema, nullptr);
    EXPECT_EQ(schema->num_fields(), 2);
    EXPECT_EQ(schema->field(0)->name(), "arg0");
    EXPECT_EQ(schema->field(1)->name(), "arg1");
    EXPECT_TRUE(schema->field(0)->type()->Equals(arrow::int32()));
    EXPECT_TRUE(schema->field(1)->type()->Equals(arrow::utf8()));
    EXPECT_TRUE(schema->field(0)->nullable());
    EXPECT_TRUE(schema->field(1)->nullable());
}

TEST_F(PythonUDFMetaTest, ConvertTypesToSchemaSingleType) {
    DataTypes types = {nullable_double_};
    std::shared_ptr<arrow::Schema> schema;

    Status status = PythonUDFMeta::convert_types_to_schema(types, TimezoneUtils::default_time_zone,
                                                           &schema);
    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_NE(schema, nullptr);
    EXPECT_EQ(schema->num_fields(), 1);
    EXPECT_EQ(schema->field(0)->name(), "arg0");
    EXPECT_TRUE(schema->field(0)->type()->Equals(arrow::float64()));
    EXPECT_TRUE(schema->field(0)->nullable());
}

TEST_F(PythonUDFMetaTest, ConvertTypesToSchemaEmpty) {
    DataTypes types = {};
    std::shared_ptr<arrow::Schema> schema;

    Status status = PythonUDFMeta::convert_types_to_schema(types, TimezoneUtils::default_time_zone,
                                                           &schema);
    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_NE(schema, nullptr);
    EXPECT_EQ(schema->num_fields(), 0);
}

// ============================================================================
// PythonUDFMeta serialize_arrow_schema() tests
// ============================================================================

TEST_F(PythonUDFMetaTest, SerializeArrowSchema) {
    auto schema = arrow::schema(
            {arrow::field("col1", arrow::int32()), arrow::field("col2", arrow::utf8())});

    std::shared_ptr<arrow::Buffer> buffer;
    Status status = PythonUDFMeta::serialize_arrow_schema(schema, &buffer);
    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_NE(buffer, nullptr);
    EXPECT_GT(buffer->size(), 0);

    arrow::io::BufferReader reader(buffer);
    arrow::ipc::DictionaryMemo dictionary_memo;
    auto decoded_schema_result = arrow::ipc::ReadSchema(&reader, &dictionary_memo);
    ASSERT_TRUE(decoded_schema_result.ok()) << decoded_schema_result.status().ToString();
    EXPECT_TRUE((*decoded_schema_result)->Equals(*schema));
}

} // namespace doris
