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

#include "core/data_type/file_schema_descriptor.h"

#include <gtest/gtest.h>

#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "util/jsonb_document.h"
#include "util/jsonb_writer.h"

namespace doris {

class FileSchemaDescriptorTest : public ::testing::Test {
protected:
    const FileSchemaDescriptor& schema = FileSchemaDescriptor::instance();
};

// ---------- Singleton & field layout ----------

TEST_F(FileSchemaDescriptorTest, SingletonReturnsSameInstance) {
    const auto& a = FileSchemaDescriptor::instance();
    const auto& b = FileSchemaDescriptor::instance();
    EXPECT_EQ(&a, &b);
}

TEST_F(FileSchemaDescriptorTest, FieldCount) {
    // Enumerate all 10 fields (URI through EXTERNAL_ID)
    EXPECT_STREQ(schema.field(FileSchemaDescriptor::Field::URI).name, "uri");
    EXPECT_STREQ(schema.field(FileSchemaDescriptor::Field::FILE_NAME).name, "file_name");
    EXPECT_STREQ(schema.field(FileSchemaDescriptor::Field::CONTENT_TYPE).name, "content_type");
    EXPECT_STREQ(schema.field(FileSchemaDescriptor::Field::SIZE).name, "size");
    EXPECT_STREQ(schema.field(FileSchemaDescriptor::Field::REGION).name, "region");
    EXPECT_STREQ(schema.field(FileSchemaDescriptor::Field::ENDPOINT).name, "endpoint");
    EXPECT_STREQ(schema.field(FileSchemaDescriptor::Field::AK).name, "ak");
    EXPECT_STREQ(schema.field(FileSchemaDescriptor::Field::SK).name, "sk");
    EXPECT_STREQ(schema.field(FileSchemaDescriptor::Field::ROLE_ARN).name, "role_arn");
    EXPECT_STREQ(schema.field(FileSchemaDescriptor::Field::EXTERNAL_ID).name, "external_id");
}

TEST_F(FileSchemaDescriptorTest, FieldNameAccessor) {
    EXPECT_EQ(schema.field_name(FileSchemaDescriptor::Field::URI), "uri");
    EXPECT_EQ(schema.field_name(FileSchemaDescriptor::Field::SIZE), "size");
    EXPECT_EQ(schema.field_name(FileSchemaDescriptor::Field::EXTERNAL_ID), "external_id");
}

TEST_F(FileSchemaDescriptorTest, RequiredFieldsAreNotNullable) {
    // URI, FILE_NAME, CONTENT_TYPE, SIZE are required (not nullable)
    EXPECT_FALSE(schema.field(FileSchemaDescriptor::Field::URI).type->is_nullable());
    EXPECT_FALSE(schema.field(FileSchemaDescriptor::Field::FILE_NAME).type->is_nullable());
    EXPECT_FALSE(schema.field(FileSchemaDescriptor::Field::CONTENT_TYPE).type->is_nullable());
    EXPECT_FALSE(schema.field(FileSchemaDescriptor::Field::SIZE).type->is_nullable());
}

TEST_F(FileSchemaDescriptorTest, OptionalFieldsAreNullable) {
    EXPECT_TRUE(schema.field(FileSchemaDescriptor::Field::REGION).type->is_nullable());
    EXPECT_TRUE(schema.field(FileSchemaDescriptor::Field::ENDPOINT).type->is_nullable());
    EXPECT_TRUE(schema.field(FileSchemaDescriptor::Field::AK).type->is_nullable());
    EXPECT_TRUE(schema.field(FileSchemaDescriptor::Field::SK).type->is_nullable());
    EXPECT_TRUE(schema.field(FileSchemaDescriptor::Field::ROLE_ARN).type->is_nullable());
    EXPECT_TRUE(schema.field(FileSchemaDescriptor::Field::EXTERNAL_ID).type->is_nullable());
}

// ---------- extract_file_name ----------

TEST_F(FileSchemaDescriptorTest, ExtractFileNameSimplePath) {
    EXPECT_EQ(FileSchemaDescriptor::extract_file_name("s3://bucket/path/file.csv"), "file.csv");
}

TEST_F(FileSchemaDescriptorTest, ExtractFileNameNoSlash) {
    EXPECT_EQ(FileSchemaDescriptor::extract_file_name("file.csv"), "file.csv");
}

TEST_F(FileSchemaDescriptorTest, ExtractFileNameTrailingSlash) {
    EXPECT_EQ(FileSchemaDescriptor::extract_file_name("s3://bucket/path/"), "");
}

TEST_F(FileSchemaDescriptorTest, ExtractFileNameWithQueryString) {
    EXPECT_EQ(
            FileSchemaDescriptor::extract_file_name("https://host/path/data.parquet?token=abc&v=1"),
            "data.parquet");
}

TEST_F(FileSchemaDescriptorTest, ExtractFileNameWithFragment) {
    EXPECT_EQ(FileSchemaDescriptor::extract_file_name("https://host/path/file.json#section"),
              "file.json");
}

TEST_F(FileSchemaDescriptorTest, ExtractFileNameDeepPath) {
    EXPECT_EQ(FileSchemaDescriptor::extract_file_name("s3://b/a/b/c/d/e/f.orc"), "f.orc");
}

TEST_F(FileSchemaDescriptorTest, ExtractFileNameEmpty) {
    EXPECT_EQ(FileSchemaDescriptor::extract_file_name(""), "");
}

TEST_F(FileSchemaDescriptorTest, ExtractFileNameRootOnly) {
    EXPECT_EQ(FileSchemaDescriptor::extract_file_name("/"), "");
}

// ---------- extract_file_extension ----------

TEST_F(FileSchemaDescriptorTest, ExtractExtensionNormal) {
    EXPECT_EQ(FileSchemaDescriptor::extract_file_extension("file.csv"), ".csv");
}

TEST_F(FileSchemaDescriptorTest, ExtractExtensionUpperCase) {
    EXPECT_EQ(FileSchemaDescriptor::extract_file_extension("DATA.JSON"), ".json");
}

TEST_F(FileSchemaDescriptorTest, ExtractExtensionMixed) {
    EXPECT_EQ(FileSchemaDescriptor::extract_file_extension("image.JPG"), ".jpg");
}

TEST_F(FileSchemaDescriptorTest, ExtractExtensionNoExt) {
    EXPECT_EQ(FileSchemaDescriptor::extract_file_extension("Makefile"), "");
}

TEST_F(FileSchemaDescriptorTest, ExtractExtensionMultipleDots) {
    EXPECT_EQ(FileSchemaDescriptor::extract_file_extension("archive.tar.gz"), ".gz");
}

TEST_F(FileSchemaDescriptorTest, ExtractExtensionHiddenFile) {
    EXPECT_EQ(FileSchemaDescriptor::extract_file_extension(".gitignore"), ".gitignore");
}

TEST_F(FileSchemaDescriptorTest, ExtractExtensionEmpty) {
    EXPECT_EQ(FileSchemaDescriptor::extract_file_extension(""), "");
}

// ---------- extension_to_content_type ----------

TEST_F(FileSchemaDescriptorTest, ContentTypeCsv) {
    EXPECT_EQ(FileSchemaDescriptor::extension_to_content_type(".csv"), "text/csv");
}

TEST_F(FileSchemaDescriptorTest, ContentTypeJson) {
    EXPECT_EQ(FileSchemaDescriptor::extension_to_content_type(".json"), "application/json");
}

TEST_F(FileSchemaDescriptorTest, ContentTypeParquet) {
    EXPECT_EQ(FileSchemaDescriptor::extension_to_content_type(".parquet"), "application/x-parquet");
}

TEST_F(FileSchemaDescriptorTest, ContentTypeOrc) {
    EXPECT_EQ(FileSchemaDescriptor::extension_to_content_type(".orc"), "application/x-orc");
}

TEST_F(FileSchemaDescriptorTest, ContentTypeJpeg) {
    EXPECT_EQ(FileSchemaDescriptor::extension_to_content_type(".jpg"), "image/jpeg");
    EXPECT_EQ(FileSchemaDescriptor::extension_to_content_type(".jpeg"), "image/jpeg");
}

TEST_F(FileSchemaDescriptorTest, ContentTypePng) {
    EXPECT_EQ(FileSchemaDescriptor::extension_to_content_type(".png"), "image/png");
}

TEST_F(FileSchemaDescriptorTest, ContentTypeTxt) {
    EXPECT_EQ(FileSchemaDescriptor::extension_to_content_type(".txt"), "text/plain");
    EXPECT_EQ(FileSchemaDescriptor::extension_to_content_type(".log"), "text/plain");
    EXPECT_EQ(FileSchemaDescriptor::extension_to_content_type(".tbl"), "text/plain");
}

TEST_F(FileSchemaDescriptorTest, ContentTypeCompression) {
    EXPECT_EQ(FileSchemaDescriptor::extension_to_content_type(".gz"), "application/gzip");
    EXPECT_EQ(FileSchemaDescriptor::extension_to_content_type(".bz2"), "application/x-bzip2");
    EXPECT_EQ(FileSchemaDescriptor::extension_to_content_type(".zst"), "application/zstd");
    EXPECT_EQ(FileSchemaDescriptor::extension_to_content_type(".lz4"), "application/x-lz4");
    EXPECT_EQ(FileSchemaDescriptor::extension_to_content_type(".snappy"), "application/x-snappy");
}

TEST_F(FileSchemaDescriptorTest, ContentTypeUnknown) {
    EXPECT_EQ(FileSchemaDescriptor::extension_to_content_type(".xyz"), "application/octet-stream");
    EXPECT_EQ(FileSchemaDescriptor::extension_to_content_type(""), "application/octet-stream");
}

TEST_F(FileSchemaDescriptorTest, ContentTypeMedia) {
    EXPECT_EQ(FileSchemaDescriptor::extension_to_content_type(".mp3"), "audio/mpeg");
    EXPECT_EQ(FileSchemaDescriptor::extension_to_content_type(".mp4"), "video/mp4");
    EXPECT_EQ(FileSchemaDescriptor::extension_to_content_type(".wav"), "audio/wav");
}

// ---------- write_file_jsonb ----------

TEST_F(FileSchemaDescriptorTest, WriteFileJsonbAllFields) {
    FileMetadata meta {
            .uri = "s3://bucket/path/data.csv",
            .file_name = "data.csv",
            .content_type = "text/csv",
            .size = 12345,
            .region = "us-east-1",
            .endpoint = "s3.amazonaws.com",
            .ak = "AKID",
            .sk = "SECRET",
            .role_arn = "arn:aws:iam::role/test",
            .external_id = "ext-123",
    };

    JsonbWriter writer;
    FileSchemaDescriptor::write_file_jsonb(writer, meta);

    const auto* doc = writer.getDocument();
    ASSERT_NE(doc, nullptr);
    const auto* obj = doc->operator->();
    ASSERT_NE(obj, nullptr);

    // Verify required fields
    auto* uri_val = obj->find("uri");
    ASSERT_NE(uri_val, nullptr);
    EXPECT_TRUE(uri_val->isString());

    auto* size_val = obj->find("size");
    ASSERT_NE(size_val, nullptr);
    EXPECT_TRUE(size_val->isInt64());
    EXPECT_EQ(size_val->int_val(), 12345);

    // Verify nullable fields are present
    auto* region_val = obj->find("region");
    ASSERT_NE(region_val, nullptr);
    EXPECT_TRUE(region_val->isString());

    auto* ak_val = obj->find("ak");
    ASSERT_NE(ak_val, nullptr);
    EXPECT_TRUE(ak_val->isString());
}

TEST_F(FileSchemaDescriptorTest, WriteFileJsonbNullableFieldsEmpty) {
    FileMetadata meta {
            .uri = "file:///local/test.txt",
            .file_name = "test.txt",
            .content_type = "text/plain",
            .size = 100,
            .region = {},
            .endpoint = {},
            .ak = {},
            .sk = {},
            .role_arn = {},
            .external_id = {},
    };

    JsonbWriter writer;
    FileSchemaDescriptor::write_file_jsonb(writer, meta);

    const auto* doc = writer.getDocument();
    ASSERT_NE(doc, nullptr);
    const auto* obj = doc->operator->();
    ASSERT_NE(obj, nullptr);

    // Empty optional fields should be null
    auto* region_val = obj->find("region");
    ASSERT_NE(region_val, nullptr);
    EXPECT_TRUE(region_val->isNull());

    auto* endpoint_val = obj->find("endpoint");
    ASSERT_NE(endpoint_val, nullptr);
    EXPECT_TRUE(endpoint_val->isNull());

    auto* ak_val = obj->find("ak");
    ASSERT_NE(ak_val, nullptr);
    EXPECT_TRUE(ak_val->isNull());

    auto* sk_val = obj->find("sk");
    ASSERT_NE(sk_val, nullptr);
    EXPECT_TRUE(sk_val->isNull());
}

TEST_F(FileSchemaDescriptorTest, WriteFileJsonbZeroSize) {
    FileMetadata meta {
            .uri = "s3://bucket/empty.csv",
            .file_name = "empty.csv",
            .content_type = "text/csv",
            .size = 0,
            .region = {},
            .endpoint = {},
            .ak = {},
            .sk = {},
            .role_arn = {},
            .external_id = {},
    };

    JsonbWriter writer;
    FileSchemaDescriptor::write_file_jsonb(writer, meta);

    const auto* doc = writer.getDocument();
    ASSERT_NE(doc, nullptr);
    const auto* obj = doc->operator->();
    ASSERT_NE(obj, nullptr);

    auto* size_val = obj->find("size");
    ASSERT_NE(size_val, nullptr);
    EXPECT_EQ(size_val->int_val(), 0);
}

TEST_F(FileSchemaDescriptorTest, WriteFileJsonbLargeSize) {
    FileMetadata meta {
            .uri = "s3://bucket/huge.parquet",
            .file_name = "huge.parquet",
            .content_type = "application/x-parquet",
            .size = INT64_MAX,
            .region = {},
            .endpoint = {},
            .ak = {},
            .sk = {},
            .role_arn = {},
            .external_id = {},
    };

    JsonbWriter writer;
    FileSchemaDescriptor::write_file_jsonb(writer, meta);

    const auto* doc = writer.getDocument();
    ASSERT_NE(doc, nullptr);
    const auto* obj = doc->operator->();
    ASSERT_NE(obj, nullptr);

    auto* size_val = obj->find("size");
    ASSERT_NE(size_val, nullptr);
    EXPECT_EQ(size_val->int_val(), INT64_MAX);
}

// ---------- write_jsonb_string / write_jsonb_key ----------

TEST_F(FileSchemaDescriptorTest, WriteJsonbStringRoundTrip) {
    JsonbWriter writer;
    writer.writeStartObject();
    FileSchemaDescriptor::write_jsonb_key(writer, "test_key");
    FileSchemaDescriptor::write_jsonb_string(writer, "hello world");
    writer.writeEndObject();

    const auto* doc = writer.getDocument();
    ASSERT_NE(doc, nullptr);
    const auto* obj = doc->operator->();
    ASSERT_NE(obj, nullptr);

    auto* val = obj->find("test_key");
    ASSERT_NE(val, nullptr);
    EXPECT_TRUE(val->isString());
    const auto* str_val = val->unpack<JsonbStringVal>();
    std::string result(str_val->getBlob(), str_val->length());
    EXPECT_EQ(result, "hello world");
}

TEST_F(FileSchemaDescriptorTest, WriteJsonbStringEmpty) {
    JsonbWriter writer;
    writer.writeStartObject();
    FileSchemaDescriptor::write_jsonb_key(writer, "k");
    FileSchemaDescriptor::write_jsonb_string(writer, "");
    writer.writeEndObject();

    const auto* doc = writer.getDocument();
    ASSERT_NE(doc, nullptr);
    const auto* obj = doc->operator->();
    auto* val = obj->find("k");
    ASSERT_NE(val, nullptr);
    EXPECT_TRUE(val->isString());
    const auto* str_val = val->unpack<JsonbStringVal>();
    EXPECT_EQ(str_val->length(), 0);
}

} // namespace doris
