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

#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include <memory>
#include <string>

#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "vec/data_types/data_type_factory.hpp"

namespace doris {

class PythonUDFMetaTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create common test data types using PrimitiveType
        nullable_int32_ = vectorized::DataTypeFactory::instance().create_data_type(
                PrimitiveType::TYPE_INT, true);
        nullable_string_ = vectorized::DataTypeFactory::instance().create_data_type(
                PrimitiveType::TYPE_STRING, true);
        nullable_double_ = vectorized::DataTypeFactory::instance().create_data_type(
                PrimitiveType::TYPE_DOUBLE, true);
    }

    vectorized::DataTypePtr nullable_int32_;
    vectorized::DataTypePtr nullable_string_;
    vectorized::DataTypePtr nullable_double_;
};

// ============================================================================
// PythonUDFMeta construction tests
// ============================================================================

TEST_F(PythonUDFMetaTest, DefaultConstruction) {
    PythonUDFMeta meta;
    EXPECT_TRUE(meta.name.empty());
    EXPECT_TRUE(meta.symbol.empty());
    EXPECT_TRUE(meta.location.empty());
    EXPECT_TRUE(meta.checksum.empty());
    EXPECT_TRUE(meta.runtime_version.empty());
    EXPECT_TRUE(meta.inline_code.empty());
    EXPECT_FALSE(meta.always_nullable);
    EXPECT_TRUE(meta.input_types.empty());
    EXPECT_EQ(meta.return_type, nullptr);
}

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

TEST_F(PythonUDFMetaTest, CheckEmptyInputTypes) {
    PythonUDFMeta meta;
    meta.name = "test_udf";
    meta.symbol = "test_func";
    meta.runtime_version = "3.9.16";
    meta.input_types = {};
    meta.return_type = nullable_int32_;
    meta.type = PythonUDFLoadType::INLINE;

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
// PythonUDFMeta to_string() tests
// ============================================================================

TEST_F(PythonUDFMetaTest, ToStringContainsAllFields) {
    PythonUDFMeta meta;
    meta.name = "my_udf";
    meta.symbol = "udf_func";
    meta.location = "/path/to/udf.py";
    meta.runtime_version = "3.10.5";
    meta.always_nullable = true;
    meta.inline_code = "def udf_func(x): return x";
    meta.input_types = {nullable_int32_, nullable_string_};
    meta.return_type = nullable_double_;

    std::string str = meta.to_string();
    EXPECT_TRUE(str.find("my_udf") != std::string::npos);
    EXPECT_TRUE(str.find("udf_func") != std::string::npos);
    EXPECT_TRUE(str.find("/path/to/udf.py") != std::string::npos);
    EXPECT_TRUE(str.find("3.10.5") != std::string::npos);
}

TEST_F(PythonUDFMetaTest, ToStringMultipleInputTypes) {
    PythonUDFMeta meta;
    meta.name = "multi_arg_udf";
    meta.symbol = "func";
    meta.runtime_version = "3.9.16";
    meta.input_types = {nullable_int32_, nullable_string_, nullable_double_};
    meta.return_type = nullable_int32_;

    std::string str = meta.to_string();
    // Should contain input_types section
    EXPECT_TRUE(str.find("input_types") != std::string::npos);
}

// ============================================================================
// PythonUDFMeta equality tests
// ============================================================================

TEST_F(PythonUDFMetaTest, EqualityById) {
    PythonUDFMeta meta1;
    meta1.id = 100;
    meta1.name = "udf1";

    PythonUDFMeta meta2;
    meta2.id = 100;
    meta2.name = "different_name";

    PythonUDFMeta meta3;
    meta3.id = 200;
    meta3.name = "udf1";

    EXPECT_EQ(meta1, meta2);      // Same ID
    EXPECT_FALSE(meta1 == meta3); // Different ID
}

TEST_F(PythonUDFMetaTest, HashById) {
    PythonUDFMeta meta1;
    meta1.id = 100;

    PythonUDFMeta meta2;
    meta2.id = 100;

    PythonUDFMeta meta3;
    meta3.id = 200;

    std::hash<PythonUDFMeta> hasher;
    EXPECT_EQ(hasher(meta1), hasher(meta2));
    EXPECT_NE(hasher(meta1), hasher(meta3));
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
    EXPECT_TRUE(doc.HasMember("input_types"));
}

// ============================================================================
// PythonUDFMeta convert_types_to_schema() tests
// ============================================================================

TEST_F(PythonUDFMetaTest, ConvertTypesToSchemaBasic) {
    vectorized::DataTypes types = {nullable_int32_, nullable_string_};
    std::shared_ptr<arrow::Schema> schema;

    Status status = PythonUDFMeta::convert_types_to_schema(types, TimezoneUtils::default_time_zone,
                                                           &schema);
    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_NE(schema, nullptr);
    EXPECT_EQ(schema->num_fields(), 2);
    EXPECT_EQ(schema->field(0)->name(), "arg0");
    EXPECT_EQ(schema->field(1)->name(), "arg1");
}

TEST_F(PythonUDFMetaTest, ConvertTypesToSchemaSingleType) {
    vectorized::DataTypes types = {nullable_double_};
    std::shared_ptr<arrow::Schema> schema;

    Status status = PythonUDFMeta::convert_types_to_schema(types, TimezoneUtils::default_time_zone,
                                                           &schema);
    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_NE(schema, nullptr);
    EXPECT_EQ(schema->num_fields(), 1);
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
}

} // namespace doris
