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

#include "format/parquet/parquet_variant_reader.h"

#include <cctz/time_zone.h>
#include <gtest/gtest.h>

#include <cmath>
#include <cstdint>
#include <deque>
#include <initializer_list>
#include <limits>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "core/column/column_nullable.h"
#include "core/column/column_variant.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_time.h"
#include "core/data_type/data_type_varbinary.h"
#include "core/data_type/data_type_variant.h"
#include "core/data_type/primitive_type.h"
#include "core/data_type_serde/data_type_serde.h"
#include "core/field.h"
#include "format/parquet/parquet_column_convert.h"
#include "format/parquet/schema_desc.h"
#include "format/parquet/vparquet_column_reader.h"

namespace doris::parquet {
namespace {

StringRef bytes_ref(const std::vector<uint8_t>& bytes) {
    return {bytes.data(), bytes.size()};
}

void append_int64_le(std::vector<uint8_t>* bytes, int64_t value) {
    auto unsigned_value = static_cast<uint64_t>(value);
    for (int i = 0; i < 8; ++i) {
        bytes->push_back(static_cast<uint8_t>(unsigned_value >> (i * 8)));
    }
}

std::vector<uint8_t> make_metadata(std::initializer_list<std::string_view> keys,
                                   bool sorted_strings = false) {
    const uint8_t header = sorted_strings ? 0x11 : 0x01;
    std::vector<uint8_t> metadata {header, static_cast<uint8_t>(keys.size())};
    uint8_t offset = 0;
    metadata.push_back(offset);
    for (std::string_view key : keys) {
        offset += static_cast<uint8_t>(key.size());
        metadata.push_back(offset);
    }
    for (std::string_view key : keys) {
        metadata.insert(metadata.end(), key.begin(), key.end());
    }
    return metadata;
}

void expect_variant_json(const std::vector<uint8_t>& metadata, std::initializer_list<uint8_t> value,
                         std::string_view expected) {
    std::vector<uint8_t> value_bytes(value);
    std::string json;
    Status st = decode_variant_to_json(bytes_ref(metadata), bytes_ref(value_bytes), &json);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(expected, json);
}

void expect_variant_corruption(const std::vector<uint8_t>& metadata,
                               std::initializer_list<uint8_t> value) {
    std::vector<uint8_t> value_bytes(value);
    std::string json;
    Status st = decode_variant_to_json(bytes_ref(metadata), bytes_ref(value_bytes), &json);
    EXPECT_TRUE(st.is<ErrorCode::CORRUPTION>()) << st.to_string();
}

FieldSchema make_int32_field_schema(std::string name) {
    FieldSchema field;
    field.name = std::move(name);
    field.lower_case_name = field.name;
    field.physical_type = tparquet::Type::INT32;
    field.data_type = make_nullable(std::make_shared<DataTypeInt32>());
    return field;
}

FieldSchema make_int64_field_schema(std::string name) {
    FieldSchema field;
    field.name = std::move(name);
    field.lower_case_name = field.name;
    field.physical_type = tparquet::Type::INT64;
    field.data_type = make_nullable(std::make_shared<DataTypeInt64>());
    return field;
}

FieldSchema make_float_field_schema(std::string name) {
    FieldSchema field;
    field.name = std::move(name);
    field.lower_case_name = field.name;
    field.physical_type = tparquet::Type::FLOAT;
    field.data_type = make_nullable(std::make_shared<DataTypeFloat32>());
    return field;
}

FieldSchema make_double_field_schema(std::string name) {
    FieldSchema field;
    field.name = std::move(name);
    field.lower_case_name = field.name;
    field.physical_type = tparquet::Type::DOUBLE;
    field.data_type = make_nullable(std::make_shared<DataTypeFloat64>());
    return field;
}

FieldSchema make_varbinary_field_schema(std::string name) {
    FieldSchema field;
    field.name = std::move(name);
    field.lower_case_name = field.name;
    field.physical_type = tparquet::Type::BYTE_ARRAY;
    field.data_type = make_nullable(std::make_shared<DataTypeVarbinary>());
    return field;
}

Field make_varbinary_field(std::initializer_list<uint8_t> bytes) {
    const auto* data = reinterpret_cast<const char*>(bytes.begin());
    return Field::create_field<TYPE_VARBINARY>(
            StringView(data, static_cast<uint32_t>(bytes.size())));
}

Field make_varbinary_field(std::string_view bytes) {
    return Field::create_field<TYPE_VARBINARY>(StringView(bytes));
}

std::string varbinary_field_bytes(const Field& field) {
    auto ref = field.get<TYPE_VARBINARY>().to_string_ref();
    return {ref.data, ref.size};
}

std::string test_uuid_bytes() {
    return {"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f", 16};
}

FieldSchema make_uuid_field_schema(std::string name) {
    FieldSchema field = make_varbinary_field_schema(std::move(name));
    field.physical_type = tparquet::Type::FIXED_LEN_BYTE_ARRAY;
    field.parquet_schema.__set_logicalType(tparquet::LogicalType());
    field.parquet_schema.logicalType.__set_UUID(tparquet::UUIDType());
    field.parquet_schema.__set_type_length(16);
    return field;
}

FieldSchema make_datev2_field_schema(std::string name) {
    FieldSchema field;
    field.name = std::move(name);
    field.lower_case_name = field.name;
    field.physical_type = tparquet::Type::INT32;
    field.data_type = make_nullable(std::make_shared<DataTypeDateV2>());
    return field;
}

FieldSchema make_timev2_field_schema(std::string name) {
    FieldSchema field;
    field.name = std::move(name);
    field.lower_case_name = field.name;
    field.physical_type = tparquet::Type::INT64;
    field.data_type = make_nullable(std::make_shared<DataTypeTimeV2>(6));
    return field;
}

FieldSchema make_datetimev2_field_schema(std::string name) {
    FieldSchema field;
    field.name = std::move(name);
    field.lower_case_name = field.name;
    field.physical_type = tparquet::Type::INT64;
    field.data_type = make_nullable(std::make_shared<DataTypeDateTimeV2>(6));
    return field;
}

FieldSchema make_required_int64_field_schema(std::string name) {
    FieldSchema field;
    field.name = std::move(name);
    field.lower_case_name = field.name;
    field.physical_type = tparquet::Type::INT64;
    field.data_type = std::make_shared<DataTypeInt64>();
    return field;
}

FieldSchema make_binary_field_schema(std::string name, bool nullable) {
    FieldSchema field;
    field.name = std::move(name);
    field.lower_case_name = field.name;
    field.physical_type = tparquet::Type::BYTE_ARRAY;
    field.data_type = std::make_shared<DataTypeString>();
    if (nullable) {
        field.data_type = make_nullable(field.data_type);
    }
    return field;
}

FieldSchema make_string_field_schema(std::string name, bool nullable) {
    FieldSchema field = make_binary_field_schema(std::move(name), nullable);
    tparquet::LogicalType logical_type;
    logical_type.__set_STRING(tparquet::StringType());
    field.parquet_schema.__set_logicalType(logical_type);
    return field;
}

FieldSchema make_required_shredded_variant_schema() {
    FieldSchema field;
    field.name = "measurement";
    field.lower_case_name = field.name;
    field.data_type = std::make_shared<DataTypeVariant>(0, false);
    field.children = {make_binary_field_schema("metadata", false),
                      make_binary_field_schema("value", true),
                      make_int64_field_schema("typed_value")};
    return field;
}

std::string serialize_variant_field(const Field& field) {
    auto variant_column = ColumnVariant::create(0, false);
    variant_column->insert(field);
    std::string json;
    DataTypeSerDe::FormatOptions options;
    variant_column->serialize_one_row_to_string(0, &json, options);
    return json;
}

} // namespace

TEST(ParquetVariantReaderTest, ParseTypedOnlyVariantSchemaWithoutTopLevelValue) {
    tparquet::SchemaElement root;
    root.__set_name("schema");
    root.__set_num_children(1);

    tparquet::LogicalType variant_type;
    variant_type.__set_VARIANT(tparquet::VariantType());

    tparquet::SchemaElement variant;
    variant.__set_name("v");
    variant.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);
    variant.__set_num_children(2);
    variant.__set_logicalType(variant_type);

    tparquet::SchemaElement metadata;
    metadata.__set_name("metadata");
    metadata.__set_type(tparquet::Type::BYTE_ARRAY);
    metadata.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);

    tparquet::SchemaElement typed_value;
    typed_value.__set_name("typed_value");
    typed_value.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    typed_value.__set_num_children(1);

    tparquet::SchemaElement metric;
    metric.__set_name("metric");
    metric.__set_type(tparquet::Type::INT64);
    metric.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);

    FieldDescriptor descriptor;
    Status st = descriptor.parse_from_thrift({root, variant, metadata, typed_value, metric});
    ASSERT_TRUE(st.ok()) << st.to_string();

    const auto* variant_field = descriptor.get_column("v");
    ASSERT_NE(variant_field, nullptr);
    EXPECT_EQ(variant_field->data_type->get_primitive_type(), TYPE_VARIANT);
    ASSERT_EQ(variant_field->children.size(), 2);
    EXPECT_EQ(variant_field->children[0].name, "metadata");
    EXPECT_EQ(variant_field->children[1].name, "typed_value");
}

TEST(ParquetVariantReaderTest, RejectVariantSchemaWithUnexpectedChild) {
    tparquet::SchemaElement root;
    root.__set_name("schema");
    root.__set_num_children(1);

    tparquet::LogicalType variant_type;
    variant_type.__set_VARIANT(tparquet::VariantType());

    tparquet::SchemaElement variant;
    variant.__set_name("v");
    variant.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);
    variant.__set_num_children(3);
    variant.__set_logicalType(variant_type);

    tparquet::SchemaElement metadata;
    metadata.__set_name("metadata");
    metadata.__set_type(tparquet::Type::BYTE_ARRAY);
    metadata.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);

    tparquet::SchemaElement value;
    value.__set_name("value");
    value.__set_type(tparquet::Type::BYTE_ARRAY);
    value.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);

    tparquet::SchemaElement extra;
    extra.__set_name("extra");
    extra.__set_type(tparquet::Type::INT32);
    extra.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);

    FieldDescriptor descriptor;
    Status st = descriptor.parse_from_thrift({root, variant, metadata, value, extra});
    EXPECT_TRUE(st.is<ErrorCode::INVALID_ARGUMENT>()) << st.to_string();
}

TEST(ParquetVariantReaderTest, RejectVariantSchemaWithDuplicateStructuralChild) {
    tparquet::SchemaElement root;
    root.__set_name("schema");
    root.__set_num_children(1);

    tparquet::LogicalType variant_type;
    variant_type.__set_VARIANT(tparquet::VariantType());

    tparquet::SchemaElement variant;
    variant.__set_name("v");
    variant.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);
    variant.__set_num_children(3);
    variant.__set_logicalType(variant_type);

    tparquet::SchemaElement metadata;
    metadata.__set_name("metadata");
    metadata.__set_type(tparquet::Type::BYTE_ARRAY);
    metadata.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);

    tparquet::SchemaElement value;
    value.__set_name("value");
    value.__set_type(tparquet::Type::BYTE_ARRAY);
    value.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);

    tparquet::SchemaElement duplicate_value = value;

    FieldDescriptor descriptor;
    Status st = descriptor.parse_from_thrift({root, variant, metadata, value, duplicate_value});
    EXPECT_TRUE(st.is<ErrorCode::INVALID_ARGUMENT>()) << st.to_string();
}

TEST(ParquetVariantReaderTest, RejectVariantSchemaWithNonBinaryValueChild) {
    tparquet::SchemaElement root;
    root.__set_name("schema");
    root.__set_num_children(1);

    tparquet::LogicalType variant_type;
    variant_type.__set_VARIANT(tparquet::VariantType());

    tparquet::SchemaElement variant;
    variant.__set_name("v");
    variant.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);
    variant.__set_num_children(3);
    variant.__set_logicalType(variant_type);

    tparquet::SchemaElement metadata;
    metadata.__set_name("metadata");
    metadata.__set_type(tparquet::Type::BYTE_ARRAY);
    metadata.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);

    tparquet::SchemaElement value;
    value.__set_name("value");
    value.__set_type(tparquet::Type::INT32);
    value.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);

    tparquet::SchemaElement typed_value;
    typed_value.__set_name("typed_value");
    typed_value.__set_type(tparquet::Type::INT64);
    typed_value.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);

    FieldDescriptor descriptor;
    Status st = descriptor.parse_from_thrift({root, variant, metadata, value, typed_value});
    EXPECT_TRUE(st.is<ErrorCode::INVALID_ARGUMENT>()) << st.to_string();
}

TEST(ParquetVariantReaderTest, RejectVariantSchemaWithAnnotatedMetadataChild) {
    tparquet::SchemaElement root;
    root.__set_name("schema");
    root.__set_num_children(1);

    tparquet::LogicalType variant_type;
    variant_type.__set_VARIANT(tparquet::VariantType());

    tparquet::SchemaElement variant;
    variant.__set_name("v");
    variant.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);
    variant.__set_num_children(2);
    variant.__set_logicalType(variant_type);

    tparquet::SchemaElement metadata;
    metadata.__set_name("metadata");
    metadata.__set_type(tparquet::Type::BYTE_ARRAY);
    metadata.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);
    tparquet::LogicalType metadata_type;
    metadata_type.__set_STRING(tparquet::StringType());
    metadata.__set_logicalType(metadata_type);

    tparquet::SchemaElement typed_value;
    typed_value.__set_name("typed_value");
    typed_value.__set_type(tparquet::Type::INT64);
    typed_value.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);

    FieldDescriptor descriptor;
    Status st = descriptor.parse_from_thrift({root, variant, metadata, typed_value});
    EXPECT_TRUE(st.is<ErrorCode::INVALID_ARGUMENT>()) << st.to_string();
}

TEST(ParquetVariantReaderTest, RejectVariantSchemaWithAnnotatedValueChild) {
    tparquet::SchemaElement root;
    root.__set_name("schema");
    root.__set_num_children(1);

    tparquet::LogicalType variant_type;
    variant_type.__set_VARIANT(tparquet::VariantType());

    tparquet::SchemaElement variant;
    variant.__set_name("v");
    variant.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);
    variant.__set_num_children(3);
    variant.__set_logicalType(variant_type);

    tparquet::SchemaElement metadata;
    metadata.__set_name("metadata");
    metadata.__set_type(tparquet::Type::BYTE_ARRAY);
    metadata.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);

    tparquet::SchemaElement value;
    value.__set_name("value");
    value.__set_type(tparquet::Type::BYTE_ARRAY);
    value.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    tparquet::LogicalType value_type;
    value_type.__set_STRING(tparquet::StringType());
    value.__set_logicalType(value_type);

    tparquet::SchemaElement typed_value;
    typed_value.__set_name("typed_value");
    typed_value.__set_type(tparquet::Type::INT64);
    typed_value.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);

    FieldDescriptor descriptor;
    Status st = descriptor.parse_from_thrift({root, variant, metadata, value, typed_value});
    EXPECT_TRUE(st.is<ErrorCode::INVALID_ARGUMENT>()) << st.to_string();
}

TEST(ParquetVariantReaderTest, OptionalTopLevelVariantUsesNullableReadColumnOnly) {
    FieldSchema variant_field;
    variant_field.name = "v";
    variant_field.lower_case_name = variant_field.name;
    variant_field.data_type = make_nullable(std::make_shared<DataTypeVariant>(0, false));
    variant_field.children = {make_binary_field_schema("metadata", false),
                              make_binary_field_schema("value", true)};

    EXPECT_FALSE(parquet_variant_reader_test::variant_struct_reader_type_is_nullable_for_test(
            variant_field));
    EXPECT_TRUE(parquet_variant_reader_test::variant_struct_reader_column_is_nullable_for_test(
            variant_field));

    variant_field.data_type = std::make_shared<DataTypeVariant>(0, false);
    EXPECT_FALSE(parquet_variant_reader_test::variant_struct_reader_column_is_nullable_for_test(
            variant_field));
}

TEST(ParquetVariantReaderTest, DecodeSimpleObject) {
    auto metadata = make_metadata({"a"});
    std::vector<uint8_t> value {
            0x02,       // object, 1-byte offsets, 1-byte field ids, 1-byte element count
            0x01,       // one field
            0x00,       // dictionary id 0
            0x00, 0x02, // field value offsets
            0x0c, 0x07  // int8(7)
    };

    std::string json;
    Status st = decode_variant_to_json(bytes_ref(metadata), bytes_ref(value), &json);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ("{\"a\":7}", json);
}

TEST(ParquetVariantReaderTest, DecodeUnsortedMetadataMayContainDuplicateDictionaryStrings) {
    auto metadata = make_metadata({"a", "a"});
    std::vector<uint8_t> value {
            0x02,       // object
            0x01,       // one field
            0x01,       // dictionary id 1
            0x00, 0x02, // field value offsets
            0x0c, 0x07  // int8(7)
    };

    std::string json;
    Status st = decode_variant_to_json(bytes_ref(metadata), bytes_ref(value), &json);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ("{\"a\":7}", json);
}

TEST(ParquetVariantReaderTest, RejectDuplicateObjectKeysFromDuplicateMetadataEntries) {
    auto metadata = make_metadata({"a", "a"});
    std::vector<uint8_t> value {
            0x02,                  // object
            0x02,                  // two fields
            0x00, 0x01,            // strictly increasing dictionary ids
            0x00, 0x02, 0x04,      // valid physical value offsets
            0x0c, 0x01, 0x0c, 0x02 // int8(1), int8(2)
    };

    std::string json;
    Status st = decode_variant_to_json(bytes_ref(metadata), bytes_ref(value), &json);
    EXPECT_TRUE(st.is<ErrorCode::CORRUPTION>()) << st.to_string();
}

TEST(ParquetVariantReaderTest, RejectInvalidSortedMetadataDictionaryStrings) {
    expect_variant_corruption(make_metadata({"a", "a"}, true), {0x00});
    expect_variant_corruption(make_metadata({"b", "a"}, true), {0x00});
}

TEST(ParquetVariantReaderTest, RejectMetadataTrailingBytes) {
    auto metadata = make_metadata({"a"});
    metadata.push_back(0xff);
    expect_variant_corruption(metadata, {0x00});
}

TEST(ParquetVariantReaderTest, RejectMetadataFirstDictionaryOffsetNotZero) {
    std::vector<uint8_t> metadata {
            0x01,       // version 1, one-byte offsets
            0x01,       // one dictionary entry
            0x01, 0x02, // invalid dictionary offsets: first offset must be zero
            'x',  'a'   // no trailing bytes; offset[1] consumes both bytes
    };
    expect_variant_corruption(metadata, {0x00});
}

TEST(ParquetVariantReaderTest, RejectMetadataReservedHeaderBits) {
    std::vector<uint8_t> metadata {
            0x21, // reserved bit 5 is set
            0x00, // zero dictionary entries
            0x00  // offset[0]
    };
    expect_variant_corruption(metadata, {0x00});
}

TEST(ParquetVariantReaderTest, RejectInvalidUtf8MetadataAndStrings) {
    std::vector<uint8_t> invalid_metadata {
            0x01,       // version 1, one-byte offsets
            0x01,       // one dictionary entry
            0x00, 0x01, // dictionary offsets
            0xff        // invalid UTF-8 dictionary key
    };
    expect_variant_corruption(invalid_metadata, {0x00});

    auto metadata = make_metadata({});
    expect_variant_corruption(metadata, {0x05, 0xff});
    expect_variant_corruption(metadata, {0x40, 0x01, 0x00, 0x00, 0x00, 0xff});
}

TEST(ParquetVariantReaderTest, DecodeObjectUsesUnsignedByteFieldOrder) {
    const std::string e_acute("\xc3\xa9", 2);
    auto metadata = make_metadata({std::string_view("z"), std::string_view(e_acute)});
    std::vector<uint8_t> value {
            0x02,                  // object
            0x02,                  // two fields
            0x00, 0x01,            // dictionary ids are sorted by unsigned UTF-8 bytes: z, e acute
            0x00, 0x02, 0x04,      // valid physical value offsets
            0x0c, 0x01, 0x0c, 0x02 // int8(1), int8(2)
    };

    std::string json;
    Status st = decode_variant_to_json(bytes_ref(metadata), bytes_ref(value), &json);
    ASSERT_TRUE(st.ok()) << st.to_string();
    std::string expected = R"({"z":1,")";
    expected.append(e_acute);
    expected.append(R"(":2})");
    EXPECT_EQ(expected, json);
}

TEST(ParquetVariantReaderTest, RejectObjectFieldOrderUsingUnsignedBytes) {
    const std::string e_acute("\xc3\xa9", 2);
    auto metadata = make_metadata({std::string_view(e_acute), std::string_view("z")});
    std::vector<uint8_t> value {
            0x02,                  // object
            0x02,                  // two fields
            0x00, 0x01,            // dictionary ids are not sorted by unsigned UTF-8 bytes
            0x00, 0x02, 0x04,      // valid physical value offsets
            0x0c, 0x02, 0x0c, 0x01 // int8(2), int8(1)
    };

    std::string json;
    Status st = decode_variant_to_json(bytes_ref(metadata), bytes_ref(value), &json);
    EXPECT_TRUE(st.is<ErrorCode::CORRUPTION>()) << st.to_string();
}

TEST(ParquetVariantReaderTest, DecodeDecimal128MinimumValue) {
    auto metadata = make_metadata({});
    std::vector<uint8_t> value {
            0x28,                   // decimal128 primitive
            0x00,                   // scale 0
            0x00, 0x00, 0x00, 0x00, //
            0x00, 0x00, 0x00, 0x00, //
            0x00, 0x00, 0x00, 0x00, //
            0x00, 0x00, 0x00, 0x80  // -2^127 in little-endian two's complement
    };

    std::string json;
    Status st = decode_variant_to_json(bytes_ref(metadata), bytes_ref(value), &json);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ("-170141183460469231731687303715884105728", json);
}

TEST(ParquetVariantReaderTest, RejectInvalidDecimalScale) {
    auto metadata = make_metadata({});

    expect_variant_corruption(metadata, {0x20, 0xff, 0x00, 0x00, 0x00, 0x00});
    expect_variant_corruption(metadata, {0x20, 0x27, 0x00, 0x00, 0x00, 0x00});
}

TEST(ParquetVariantReaderTest, DecodePrimitiveCoverageExtras) {
    auto metadata = make_metadata({});

    expect_variant_json(metadata, {0x00}, "null");
    expect_variant_json(metadata, {0x10, 0xff, 0xff}, "-1");
    expect_variant_json(metadata, {0x20, 0x02, 0x85, 0xff, 0xff, 0xff}, "-1.23");
    expect_variant_json(metadata, {0x24, 0x00, 0xb0, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
                        "1200");
    expect_variant_json(metadata,
                        {0x28, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                         0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
                        "1");
    expect_variant_json(metadata, {0x1c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf8, 0x3f}, "1.5");
    expect_variant_json(metadata, {0x38, 0x00, 0x00, 0xc0, 0x3f}, "1.5");
    expect_variant_json(metadata, {0x2c, 0x2a, 0x00, 0x00, 0x00}, "42");
    expect_variant_json(metadata, {0x30, 0x2a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, "42");
    expect_variant_json(metadata, {0x40, 0x04, 0x00, 0x00, 0x00, 't', 'e', 'x', 't'}, "\"text\"");
    expect_variant_json(metadata, {0x3c, 0x03, 0x00, 0x00, 0x00, 0xff, 0x00, 'A'},
                        R"("\u00ff\u0000A")");
    expect_variant_json(metadata, {0x21, '"', '\\', '\b', '\f', '\n', '\r', '\t', 0x01},
                        R"("\"\\\b\f\n\r\t\u0001")");
    expect_variant_json(metadata,
                        {0x50, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a,
                         0x0b, 0x0c, 0x0d, 0x0e, 0x0f},
                        "\"00010203-0405-0607-0809-0a0b0c0d0e0f\"");
}

TEST(ParquetVariantReaderTest, DecodeResidualPrimitiveToVariantMapPreservesScalarTypes) {
    auto metadata = make_metadata({});
    auto decode_root = [&](std::initializer_list<uint8_t> value_bytes) {
        std::vector<uint8_t> value(value_bytes);
        VariantMap values;
        std::deque<std::string> string_values;
        Status st = decode_variant_to_variant_map(bytes_ref(metadata), bytes_ref(value),
                                                  PathInData(), &values, &string_values);
        EXPECT_TRUE(st.ok()) << st.to_string();
        auto root = values.find(PathInData());
        EXPECT_NE(root, values.end());
        if (!st.ok() || root == values.end()) {
            return FieldWithDataType {};
        }
        return root->second;
    };

    auto int16_value = decode_root({0x10, 0xff, 0xff});
    EXPECT_EQ(int16_value.base_scalar_type_id, TYPE_SMALLINT);
    EXPECT_EQ(int16_value.field.get<TYPE_SMALLINT>(), -1);

    auto decimal_value = decode_root({0x20, 0x02, 0x85, 0xff, 0xff, 0xff});
    EXPECT_EQ(decimal_value.base_scalar_type_id, TYPE_DECIMAL32);
    EXPECT_EQ(decimal_value.precision, BeConsts::MAX_DECIMAL32_PRECISION);
    EXPECT_EQ(decimal_value.scale, 2);
    EXPECT_EQ(decimal_value.field.to_debug_string(decimal_value.scale), "-1.23");

    auto string_value = decode_root({0x40, 0x04, 0x00, 0x00, 0x00, 't', 'e', 'x', 't'});
    EXPECT_EQ(string_value.base_scalar_type_id, TYPE_STRING);
    EXPECT_EQ(string_value.field.get<TYPE_STRING>(), "text");
}

TEST(ParquetVariantReaderTest, RejectInvalidVariantEncodingsCoverageExtras) {
    expect_variant_corruption(std::vector<uint8_t> {0x02}, {0x00});
    expect_variant_corruption(std::vector<uint8_t> {0x01, 0x01, 0x01, 0x00}, {0x00});

    auto metadata = make_metadata({});
    expect_variant_corruption(metadata, {0x0c, 0x01, 0x00});
    expect_variant_corruption(metadata, {0x03, 0x01, 0x01, 0x02, 0x0c, 0x07});
    expect_variant_corruption(metadata, {0x03, 0x02, 0x00, 0x02, 0x01, 0x0c, 0x01});
    expect_variant_corruption(metadata, {0x02, 0x00, 0x01, 0x00});
    expect_variant_corruption(metadata, {0x54});

    auto object_metadata = make_metadata({"a"});
    expect_variant_corruption(object_metadata, {0x02, 0x01, 0x00, 0x01, 0x02, 0x0c, 0x07});
    expect_variant_corruption(metadata, {0x02, 0x01, 0x00, 0x00, 0x02, 0x0c, 0x07});
}

TEST(ParquetVariantReaderTest, DecodeResidualRootBinaryToVariantMap) {
    auto metadata = make_metadata({});
    std::vector<uint8_t> value {0x3c, // binary primitive, 3 bytes
                                0x03, 0x00, 0x00, 0x00, 0xff, 0x00, 0x41};

    VariantMap values;
    std::deque<std::string> string_values;
    Status st = decode_variant_to_variant_map(bytes_ref(metadata), bytes_ref(value), PathInData(),
                                              &values, &string_values);
    ASSERT_TRUE(st.ok()) << st.to_string();
    const auto& binary = values.at(PathInData());
    EXPECT_EQ(binary.base_scalar_type_id, TYPE_VARBINARY);
    EXPECT_EQ(varbinary_field_bytes(binary.field), std::string("\xff\x00\x41", 3));
}

TEST(ParquetVariantReaderTest, DecodeResidualBinaryToVariantMap) {
    auto metadata = make_metadata({"b"});
    std::vector<uint8_t> value {0x02,                         // object
                                0x01,                         // one field
                                0x00,                         // dictionary id 0: b
                                0x00, 0x08,                   // field value offsets
                                0x3c, 0x03, 0x00, 0x00, 0x00, // binary primitive, 3 bytes
                                0xff, 0x00, 0x41};

    VariantMap values;
    std::deque<std::string> string_values;
    Status st = decode_variant_to_variant_map(bytes_ref(metadata), bytes_ref(value), PathInData(),
                                              &values, &string_values);
    ASSERT_TRUE(st.ok()) << st.to_string();
    const auto& binary = values.at(PathInData("b"));
    EXPECT_EQ(binary.base_scalar_type_id, TYPE_VARBINARY);
    EXPECT_EQ(varbinary_field_bytes(binary.field), std::string("\xff\x00\x41", 3));
}

TEST(ParquetVariantReaderTest, DecodeResidualBinaryArrayToVariantMap) {
    auto metadata = make_metadata({});
    std::vector<uint8_t> value {0x03,                         // array
                                0x02,                         // two elements
                                0x00, 0x07, 0x08,             // element value offsets
                                0x3c, 0x02, 0x00, 0x00, 0x00, // binary primitive, 2 bytes
                                0xc3, 0x28, 0x00};            // variant null

    VariantMap values;
    std::deque<std::string> string_values;
    Status st = decode_variant_to_variant_map(bytes_ref(metadata), bytes_ref(value), PathInData(),
                                              &values, &string_values);
    ASSERT_TRUE(st.ok()) << st.to_string();
    const auto& binary_array = values.at(PathInData());
    EXPECT_EQ(binary_array.base_scalar_type_id, TYPE_VARBINARY);
    EXPECT_EQ(binary_array.num_dimensions, 1);
    const auto& array = binary_array.field.get<TYPE_ARRAY>();
    ASSERT_EQ(array.size(), 2);
    EXPECT_EQ(varbinary_field_bytes(array[0]), std::string("\xc3\x28", 2));
    EXPECT_TRUE(array[1].is_null());
}

TEST(ParquetVariantReaderTest, DecodeResidualNonFiniteDoubleArrayToVariantMap) {
    auto metadata = make_metadata({});
    std::vector<uint8_t> value {0x03,             // array
                                0x02,             // two elements
                                0x00, 0x09, 0x12, // element value offsets
                                0x1c};            // double primitive
    append_int64_le(&value, static_cast<int64_t>(0x7ff8000000000000ULL));
    value.push_back(0x1c); // double primitive
    append_int64_le(&value, static_cast<int64_t>(0x7ff0000000000000ULL));

    VariantMap values;
    std::deque<std::string> string_values;
    Status st = decode_variant_to_variant_map(bytes_ref(metadata), bytes_ref(value), PathInData(),
                                              &values, &string_values);
    ASSERT_TRUE(st.ok()) << st.to_string();
    const auto& double_array = values.at(PathInData());
    EXPECT_EQ(double_array.base_scalar_type_id, TYPE_DOUBLE);
    EXPECT_EQ(double_array.num_dimensions, 1);
    const auto& array = double_array.field.get<TYPE_ARRAY>();
    ASSERT_EQ(array.size(), 2);
    EXPECT_TRUE(std::isnan(array[0].get<TYPE_DOUBLE>()));
    EXPECT_TRUE(std::isinf(array[1].get<TYPE_DOUBLE>()));
}

TEST(ParquetVariantReaderTest, DecodeResidualBinaryObjectArrayToVariantMap) {
    auto metadata = make_metadata({"b"});
    std::vector<uint8_t> value {0x03,                         // array
                                0x01,                         // one element
                                0x00, 0x0c,                   // element value offsets
                                0x02,                         // object
                                0x01,                         // one field
                                0x00,                         // dictionary id 0: b
                                0x00, 0x07,                   // field value offsets
                                0x3c, 0x02, 0x00, 0x00, 0x00, // binary primitive, 2 bytes
                                0xc3, 0x28};

    VariantMap values;
    std::deque<std::string> string_values;
    Status st = decode_variant_to_variant_map(bytes_ref(metadata), bytes_ref(value), PathInData(),
                                              &values, &string_values);
    ASSERT_TRUE(st.ok()) << st.to_string();
    const auto& object_array = values.at(PathInData());
    EXPECT_EQ(object_array.base_scalar_type_id, TYPE_VARIANT);
    EXPECT_EQ(object_array.num_dimensions, 1);
    const auto& array = object_array.field.get<TYPE_ARRAY>();
    ASSERT_EQ(array.size(), 1);
    ASSERT_EQ(array[0].get_type(), TYPE_VARIANT);
    const auto& object = array[0].get<TYPE_VARIANT>();
    const auto& binary = object.at(PathInData("b"));
    EXPECT_EQ(binary.base_scalar_type_id, TYPE_VARBINARY);
    EXPECT_EQ(varbinary_field_bytes(binary.field), std::string("\xc3\x28", 2));
}

TEST(ParquetVariantReaderTest, DecodeObjectOutOfOrderPhysicalValuesToVariantMap) {
    auto metadata = make_metadata({"a", "b", "c"});
    std::vector<uint8_t> value {
            0x02,             // object, 1-byte offsets, 1-byte field ids, 1-byte element count
            0x03,             // three fields
            0x00, 0x01, 0x02, // dictionary ids: a, b, c
            0x04, 0x02, 0x00, 0x06, // field offsets in key order; values are c, b, a
            0x0c, 0x03,             // c: int8(3)
            0x0c, 0x02,             // b: int8(2)
            0x0c, 0x01              // a: int8(1)
    };

    VariantMap values;
    std::deque<std::string> string_values;
    Status st = decode_variant_to_variant_map(bytes_ref(metadata), bytes_ref(value), PathInData(),
                                              &values, &string_values);
    ASSERT_TRUE(st.ok()) << st.to_string();
    Field result = Field::create_field<TYPE_VARIANT>(std::move(values));
    EXPECT_EQ("{\"a\":1,\"b\":2,\"c\":3}", serialize_variant_field(result));
}

TEST(ParquetVariantReaderTest, DecodeResidualNullToVariantMap) {
    auto metadata = make_metadata({});
    std::vector<uint8_t> value {0x00}; // variant null

    VariantMap values;
    std::deque<std::string> string_values;
    Status st = decode_variant_to_variant_map(bytes_ref(metadata), bytes_ref(value), PathInData(),
                                              &values, &string_values);
    ASSERT_TRUE(st.ok()) << st.to_string();
    auto root = values.find(PathInData());
    ASSERT_NE(root, values.end());
    EXPECT_TRUE(root->second.field.is_null());
}

TEST(ParquetVariantReaderTest, DecodeResidualObjectNullChildToVariantMap) {
    auto metadata = make_metadata({"a"});
    std::vector<uint8_t> value {
            0x02,       // object, 1-byte offsets, 1-byte field ids, 1-byte element count
            0x01,       // one field
            0x00,       // dictionary id 0: a
            0x00, 0x01, // field value offsets
            0x00        // variant null
    };

    VariantMap values;
    std::deque<std::string> string_values;
    Status st = decode_variant_to_variant_map(bytes_ref(metadata), bytes_ref(value), PathInData(),
                                              &values, &string_values);
    ASSERT_TRUE(st.ok()) << st.to_string();
    auto child = values.find(PathInData("a"));
    ASSERT_NE(child, values.end());
    EXPECT_TRUE(child->second.field.is_null());
}

TEST(ParquetVariantReaderTest, DecodeNonFiniteDoublePrimitive) {
    auto metadata = make_metadata({});
    std::vector<uint8_t> value {0x1c}; // primitive double
    append_int64_le(&value, static_cast<int64_t>(0x7ff8000000000000ULL));

    VariantMap values;
    std::deque<std::string> string_values;
    Status st = decode_variant_to_variant_map(bytes_ref(metadata), bytes_ref(value), PathInData(),
                                              &values, &string_values);
    ASSERT_TRUE(st.ok()) << st.to_string();
    auto root = values.find(PathInData());
    ASSERT_NE(root, values.end());
    ASSERT_EQ(root->second.field.get_type(), TYPE_DOUBLE);
    EXPECT_TRUE(std::isnan(root->second.field.get<TYPE_DOUBLE>()));

    std::string json;
    st = decode_variant_to_json(bytes_ref(metadata), bytes_ref(value), &json);
    EXPECT_TRUE(st.ok()) << st.to_string();
    EXPECT_FALSE(json.empty());
}

TEST(ParquetVariantReaderTest, DecodeNanosecondTimestampAsMicros) {
    auto metadata = make_metadata({});
    std::vector<uint8_t> value {0x48}; // primitive timestamptz nanos
    append_int64_le(&value, 1234567890);

    std::string json;
    Status st = decode_variant_to_json(bytes_ref(metadata), bytes_ref(value), &json);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ("1234567", json);
}

TEST(ParquetVariantReaderTest, TimeV2ConverterRequiresVariantContext) {
    FieldSchema time_field;
    time_field.name = "timestamp";
    time_field.lower_case_name = time_field.name;
    time_field.physical_type = tparquet::Type::INT64;
    time_field.parquet_schema.__set_name(time_field.name);
    time_field.parquet_schema.__set_type(tparquet::Type::INT64);
    time_field.parquet_schema.__set_converted_type(tparquet::ConvertedType::TIME_MICROS);
    time_field.data_type = make_nullable(std::make_shared<DataTypeTimeV2>(6));

    auto converter = PhysicalToLogicalConverter::get_converter(
            &time_field, time_field.data_type, time_field.data_type, nullptr, false);
    EXPECT_FALSE(converter->support());

    time_field.is_in_variant = true;
    converter = PhysicalToLogicalConverter::get_converter(&time_field, time_field.data_type,
                                                          time_field.data_type, nullptr, false);
    EXPECT_TRUE(converter->support());

    auto physical_column = ColumnInt64::create();
    physical_column->insert_value(3723004005);
    ColumnPtr physical = std::move(physical_column);
    ColumnPtr logical = time_field.data_type->create_column();
    Status st = converter->convert(physical, time_field.data_type, time_field.data_type, logical,
                                   false);
    ASSERT_TRUE(st.ok()) << st.to_string();

    const auto& nullable = assert_cast<const ColumnNullable&>(*logical);
    const auto& time_column = assert_cast<const ColumnTimeV2&>(nullable.get_nested_column());
    ASSERT_EQ(1, time_column.size());
    EXPECT_DOUBLE_EQ(3723004005, time_column.get_data()[0]);
}

TEST(ParquetVariantReaderTest, DirectTypedOnlyKeepsStructuralNameUserKeys) {
    auto int_type = make_nullable(std::make_shared<DataTypeInt32>());
    FieldSchema typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.lower_case_name = "typed_value";
    typed_value_field.children = {make_int32_field_schema("typed_value"),
                                  make_int32_field_schema("value")};
    typed_value_field.data_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {int_type, int_type}, Strings {"typed_value", "value"}));

    MutableColumnPtr typed_value_column = typed_value_field.data_type->create_column();
    Struct row;
    row.push_back(Field::create_field<TYPE_INT>(42));
    row.push_back(Field::create_field<TYPE_INT>(7));
    typed_value_column->insert(Field::create_field<TYPE_STRUCT>(row));

    auto batch = ColumnVariant::create(0, false, 2);
    ASSERT_TRUE(
            parquet_variant_reader_test::can_direct_read_typed_value_for_test(typed_value_field));
    auto* batch_variant = batch.get();
    Status st = parquet_variant_reader_test::append_direct_typed_column_to_batch_for_test(
            typed_value_field, *typed_value_column, 0, 1, batch_variant);
    ASSERT_TRUE(st.ok()) << st.to_string();

    Field result;
    batch_variant->get(1, result);
    const auto& values = result.get<TYPE_VARIANT>();
    EXPECT_EQ(values.find(PathInData()), values.end());
    EXPECT_EQ(values.at(PathInData("typed_value")).field.get<TYPE_INT>(), 42);
    EXPECT_EQ(values.at(PathInData("value")).field.get<TYPE_INT>(), 7);
}

TEST(ParquetVariantReaderTest, DirectTypedOnlyKeepsNestedStructuralNameUserKeys) {
    auto int_type = make_nullable(std::make_shared<DataTypeInt32>());
    FieldSchema nested_field;
    nested_field.name = "nested";
    nested_field.lower_case_name = nested_field.name;
    nested_field.children = {make_int32_field_schema("typed_value"),
                             make_int32_field_schema("value")};
    nested_field.data_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {int_type, int_type}, Strings {"typed_value", "value"}));

    FieldSchema typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.lower_case_name = typed_value_field.name;
    typed_value_field.children = {nested_field};
    typed_value_field.data_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {nested_field.data_type}, Strings {"nested"}));

    MutableColumnPtr typed_value_column = typed_value_field.data_type->create_column();
    Struct nested;
    nested.push_back(Field::create_field<TYPE_INT>(42));
    nested.push_back(Field::create_field<TYPE_INT>(7));
    Struct row;
    row.push_back(Field::create_field<TYPE_STRUCT>(nested));
    typed_value_column->insert(Field::create_field<TYPE_STRUCT>(row));

    auto batch = ColumnVariant::create(0, false, 2);
    ASSERT_TRUE(
            parquet_variant_reader_test::can_direct_read_typed_value_for_test(typed_value_field));
    auto* batch_variant = batch.get();
    Status st = parquet_variant_reader_test::append_direct_typed_column_to_batch_for_test(
            typed_value_field, *typed_value_column, 0, 1, batch_variant);
    ASSERT_TRUE(st.ok()) << st.to_string();

    Field result;
    batch_variant->get(1, result);
    const auto& values = result.get<TYPE_VARIANT>();
    EXPECT_EQ(values.find(PathInData("nested")), values.end());
    EXPECT_EQ(values.at(PathInData("nested.typed_value")).field.get<TYPE_INT>(), 42);
    EXPECT_EQ(values.at(PathInData("nested.value")).field.get<TYPE_INT>(), 7);
}

TEST(ParquetVariantReaderTest, DirectTypedOnlyConvertsTemporalLeavesToVariantMicros) {
    FieldSchema date_field = make_datev2_field_schema("d");
    FieldSchema time_field = make_timev2_field_schema("t");
    FieldSchema timestamp_field = make_datetimev2_field_schema("ts");

    FieldSchema typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.lower_case_name = typed_value_field.name;
    typed_value_field.children = {date_field, time_field, timestamp_field};
    typed_value_field.data_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {date_field.data_type, time_field.data_type, timestamp_field.data_type},
            Strings {"d", "t", "ts"}));

    DateV2Value<DateV2ValueType> date;
    std::string date_text = "1970-01-03";
    std::string date_format = "%Y-%m-%d";
    ASSERT_TRUE(date.from_date_format_str(date_format.data(), date_format.size(), date_text.data(),
                                          date_text.size()));

    DateV2Value<DateTimeV2ValueType> timestamp;
    std::string timestamp_text = "1970-01-01 00:00:01.000002";
    std::string timestamp_format = "%Y-%m-%d %H:%i:%s.%f";
    ASSERT_TRUE(timestamp.from_date_format_str(timestamp_format.data(), timestamp_format.size(),
                                               timestamp_text.data(), timestamp_text.size()));
    int64_t timestamp_seconds = 0;
    timestamp.unix_timestamp(&timestamp_seconds, cctz::utc_time_zone());

    Struct row;
    row.push_back(Field::create_field<TYPE_DATEV2>(date));
    row.push_back(Field::create_field<TYPE_TIMEV2>(3723004005.0));
    row.push_back(Field::create_field<TYPE_DATETIMEV2>(timestamp));

    MutableColumnPtr typed_value_column = typed_value_field.data_type->create_column();
    typed_value_column->insert(Field::create_field<TYPE_STRUCT>(row));

    auto batch = ColumnVariant::create(0, false, 2);
    ASSERT_TRUE(
            parquet_variant_reader_test::can_direct_read_typed_value_for_test(typed_value_field));
    auto* batch_variant = batch.get();
    Status st = parquet_variant_reader_test::append_direct_typed_column_to_batch_for_test(
            typed_value_field, *typed_value_column, 0, 1, batch_variant);
    ASSERT_TRUE(st.ok()) << st.to_string();

    Field result;
    batch_variant->get(1, result);
    const auto& values = result.get<TYPE_VARIANT>();
    EXPECT_EQ(values.at(PathInData("d")).field.get<TYPE_BIGINT>(), 2);
    EXPECT_EQ(values.at(PathInData("t")).field.get<TYPE_BIGINT>(), 3723004005);
    EXPECT_EQ(values.at(PathInData("ts")).field.get<TYPE_BIGINT>(),
              timestamp_seconds * 1000000 + timestamp.microsecond());
}

TEST(ParquetVariantReaderTest, DirectTypedOnlyPreservesTemporalLeafNull) {
    FieldSchema date_field = make_datev2_field_schema("d");

    FieldSchema typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.lower_case_name = typed_value_field.name;
    typed_value_field.children = {date_field};
    typed_value_field.data_type = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {date_field.data_type}, Strings {"d"}));

    Struct row;
    row.push_back(Field());

    MutableColumnPtr typed_value_column = typed_value_field.data_type->create_column();
    typed_value_column->insert(Field::create_field<TYPE_STRUCT>(row));

    auto batch = ColumnVariant::create(0, false, 2);
    auto* batch_variant = batch.get();
    Status st = parquet_variant_reader_test::append_direct_typed_column_to_batch_for_test(
            typed_value_field, *typed_value_column, 0, 1, batch_variant);
    ASSERT_TRUE(st.ok()) << st.to_string();

    const auto* date_subcolumn = batch_variant->get_subcolumn(PathInData("d"));
    ASSERT_NE(date_subcolumn, nullptr);
    EXPECT_TRUE(date_subcolumn->is_null_at(1));
}

TEST(ParquetVariantReaderTest, DirectTypedOnlyConvertsTemporalArrayLeavesToVariantMicros) {
    FieldSchema element = make_timev2_field_schema("element");

    FieldSchema typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.lower_case_name = typed_value_field.name;
    typed_value_field.children = {element};
    typed_value_field.data_type = make_nullable(std::make_shared<DataTypeArray>(element.data_type));

    MutableColumnPtr typed_value_column = typed_value_field.data_type->create_column();
    typed_value_column->insert(Field());

    Array array;
    array.push_back(Field::create_field<TYPE_TIMEV2>(3723004005.0));
    array.push_back(Field());
    typed_value_column->insert(Field::create_field<TYPE_ARRAY>(array));

    auto batch = ColumnVariant::create(0, false, 3);
    ASSERT_TRUE(
            parquet_variant_reader_test::can_direct_read_typed_value_for_test(typed_value_field));
    auto* batch_variant = batch.get();
    Status st = parquet_variant_reader_test::append_direct_typed_column_to_batch_for_test(
            typed_value_field, *typed_value_column, 0, 2, batch_variant);
    ASSERT_TRUE(st.ok()) << st.to_string();

    Field null_result;
    batch_variant->get(1, null_result);
    EXPECT_TRUE(null_result.is_null());
    const auto* root_subcolumn = batch_variant->get_subcolumn(PathInData());
    ASSERT_NE(root_subcolumn, nullptr);
    EXPECT_TRUE(root_subcolumn->is_null_at(1));

    Field present_result;
    batch_variant->get(2, present_result);
    const auto& values = present_result.get<TYPE_VARIANT>();
    auto array_value = values.find(PathInData());
    ASSERT_NE(array_value, values.end());
    EXPECT_EQ(array_value->second.base_scalar_type_id, TYPE_BIGINT);
    EXPECT_EQ(array_value->second.num_dimensions, 1);
    const auto& result_array = array_value->second.field.get<TYPE_ARRAY>();
    ASSERT_EQ(result_array.size(), 2);
    EXPECT_EQ(result_array[0].get<TYPE_BIGINT>(), 3723004005);
    EXPECT_TRUE(result_array[1].is_null());
}

TEST(ParquetVariantReaderTest, TypedOnlyKeepsUserMetadataAndValueFields) {
    FieldSchema object_field;
    object_field.name = "obj";
    object_field.lower_case_name = object_field.name;
    object_field.children = {make_binary_field_schema("metadata", true),
                             make_binary_field_schema("value", true)};
    object_field.data_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {object_field.children[0].data_type, object_field.children[1].data_type},
            Strings {"metadata", "value"}));

    FieldSchema typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.lower_case_name = typed_value_field.name;
    typed_value_field.children = {object_field};
    typed_value_field.data_type = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {object_field.data_type}, Strings {"obj"}));

    FieldSchema variant_field;
    variant_field.name = "v";
    variant_field.lower_case_name = variant_field.name;
    variant_field.data_type = make_nullable(std::make_shared<DataTypeVariant>(0, false));
    variant_field.children = {make_binary_field_schema("metadata", false), typed_value_field};

    auto metadata = make_metadata({});
    Struct object;
    object.push_back(Field::create_field<TYPE_STRING>(String("user-metadata")));
    object.push_back(Field::create_field<TYPE_STRING>(String("\0", 1)));
    Struct typed_value;
    typed_value.push_back(Field::create_field<TYPE_STRUCT>(object));
    Struct row;
    row.push_back(Field::create_field<TYPE_STRING>(
            String(reinterpret_cast<const char*>(metadata.data()), metadata.size())));
    row.push_back(Field::create_field<TYPE_STRUCT>(typed_value));

    Field result;
    bool sql_null = true;
    Status st = parquet_variant_reader_test::read_variant_row_for_test(
            variant_field, Field::create_field<TYPE_STRUCT>(row), true, &result, &sql_null);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_FALSE(sql_null);

    const auto& values = result.get<TYPE_VARIANT>();
    EXPECT_EQ(values.at(PathInData("obj.metadata")).field.get<TYPE_STRING>(), "user-metadata");
    EXPECT_EQ(values.at(PathInData("obj.value")).field.get<TYPE_STRING>(), std::string("\0", 1));
}

TEST(ParquetVariantReaderTest, TypedOnlyKeepsUserValueOnlyField) {
    FieldSchema object_field;
    object_field.name = "obj";
    object_field.lower_case_name = object_field.name;
    object_field.children = {make_string_field_schema("value", true)};
    object_field.data_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {object_field.children[0].data_type}, Strings {"value"}));

    FieldSchema typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.lower_case_name = typed_value_field.name;
    typed_value_field.children = {object_field};
    typed_value_field.data_type = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {object_field.data_type}, Strings {"obj"}));

    FieldSchema variant_field;
    variant_field.name = "v";
    variant_field.lower_case_name = variant_field.name;
    variant_field.data_type = make_nullable(std::make_shared<DataTypeVariant>(0, false));
    variant_field.children = {make_binary_field_schema("metadata", false), typed_value_field};

    auto metadata = make_metadata({});
    Struct object;
    object.push_back(Field::create_field<TYPE_STRING>(String("\0", 1)));
    Struct typed_value;
    typed_value.push_back(Field::create_field<TYPE_STRUCT>(object));
    Struct row;
    row.push_back(Field::create_field<TYPE_STRING>(
            String(reinterpret_cast<const char*>(metadata.data()), metadata.size())));
    row.push_back(Field::create_field<TYPE_STRUCT>(typed_value));

    Field result;
    bool sql_null = true;
    Status st = parquet_variant_reader_test::read_variant_row_for_test(
            variant_field, Field::create_field<TYPE_STRUCT>(row), true, &result, &sql_null);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_FALSE(sql_null);

    const auto& values = result.get<TYPE_VARIANT>();
    EXPECT_EQ(values.at(PathInData("obj.value")).field.get<TYPE_STRING>(), std::string("\0", 1));
}

TEST(ParquetVariantReaderTest, TypedOnlyKeepsAnnotatedValueAndTypedValueUserFields) {
    auto int_type = make_nullable(std::make_shared<DataTypeInt32>());
    FieldSchema nested_typed_value;
    nested_typed_value.name = "typed_value";
    nested_typed_value.lower_case_name = nested_typed_value.name;
    nested_typed_value.children = {make_int32_field_schema("x")};
    nested_typed_value.data_type =
            make_nullable(std::make_shared<DataTypeStruct>(DataTypes {int_type}, Strings {"x"}));

    FieldSchema object_field;
    object_field.name = "obj";
    object_field.lower_case_name = object_field.name;
    object_field.children = {make_string_field_schema("value", true), nested_typed_value};
    object_field.data_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {object_field.children[0].data_type, nested_typed_value.data_type},
            Strings {"value", "typed_value"}));

    FieldSchema typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.lower_case_name = typed_value_field.name;
    typed_value_field.children = {object_field};
    typed_value_field.data_type = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {object_field.data_type}, Strings {"obj"}));

    FieldSchema variant_field;
    variant_field.name = "v";
    variant_field.lower_case_name = variant_field.name;
    variant_field.data_type = make_nullable(std::make_shared<DataTypeVariant>(0, false));
    variant_field.children = {make_binary_field_schema("metadata", false), typed_value_field};

    auto metadata = make_metadata({});
    Struct nested;
    nested.push_back(Field::create_field<TYPE_INT>(42));
    Struct object;
    object.push_back(Field::create_field<TYPE_STRING>(String("abc")));
    object.push_back(Field::create_field<TYPE_STRUCT>(nested));
    Struct typed_value;
    typed_value.push_back(Field::create_field<TYPE_STRUCT>(object));
    Struct row;
    row.push_back(Field::create_field<TYPE_STRING>(
            String(reinterpret_cast<const char*>(metadata.data()), metadata.size())));
    row.push_back(Field::create_field<TYPE_STRUCT>(typed_value));

    Field result;
    bool sql_null = true;
    Status st = parquet_variant_reader_test::read_variant_row_for_test(
            variant_field, Field::create_field<TYPE_STRUCT>(row), true, &result, &sql_null);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_FALSE(sql_null);

    const auto& values = result.get<TYPE_VARIANT>();
    EXPECT_EQ(values.at(PathInData("obj.value")).field.get<TYPE_STRING>(), "abc");
    EXPECT_EQ(values.at(PathInData("obj.typed_value.x")).field.get<TYPE_INT>(), 42);
}

TEST(ParquetVariantReaderTest, TypedOnlyKeepsUserMetadataAndTypedValueFields) {
    auto int_type = make_nullable(std::make_shared<DataTypeInt32>());
    FieldSchema nested_typed_value;
    nested_typed_value.name = "typed_value";
    nested_typed_value.lower_case_name = nested_typed_value.name;
    nested_typed_value.children = {make_int32_field_schema("x")};
    nested_typed_value.data_type =
            make_nullable(std::make_shared<DataTypeStruct>(DataTypes {int_type}, Strings {"x"}));

    FieldSchema object_field;
    object_field.name = "obj";
    object_field.lower_case_name = object_field.name;
    object_field.children = {make_string_field_schema("metadata", true), nested_typed_value};
    object_field.data_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {object_field.children[0].data_type, nested_typed_value.data_type},
            Strings {"metadata", "typed_value"}));

    FieldSchema typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.lower_case_name = typed_value_field.name;
    typed_value_field.children = {object_field};
    typed_value_field.data_type = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {object_field.data_type}, Strings {"obj"}));

    FieldSchema variant_field;
    variant_field.name = "v";
    variant_field.lower_case_name = variant_field.name;
    variant_field.data_type = make_nullable(std::make_shared<DataTypeVariant>(0, false));
    variant_field.children = {make_binary_field_schema("metadata", false), typed_value_field};

    auto metadata = make_metadata({});
    Struct nested;
    nested.push_back(Field::create_field<TYPE_INT>(42));
    Struct object;
    object.push_back(Field::create_field<TYPE_STRING>(String("user-metadata")));
    object.push_back(Field::create_field<TYPE_STRUCT>(nested));
    Struct typed_value;
    typed_value.push_back(Field::create_field<TYPE_STRUCT>(object));
    Struct row;
    row.push_back(Field::create_field<TYPE_STRING>(
            String(reinterpret_cast<const char*>(metadata.data()), metadata.size())));
    row.push_back(Field::create_field<TYPE_STRUCT>(typed_value));

    Field result;
    bool sql_null = true;
    Status st = parquet_variant_reader_test::read_variant_row_for_test(
            variant_field, Field::create_field<TYPE_STRUCT>(row), true, &result, &sql_null);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_FALSE(sql_null);

    const auto& values = result.get<TYPE_VARIANT>();
    EXPECT_EQ(values.at(PathInData("obj.metadata")).field.get<TYPE_STRING>(), "user-metadata");
    EXPECT_EQ(values.at(PathInData("obj.typed_value.x")).field.get<TYPE_INT>(), 42);
}

TEST(ParquetVariantReaderTest, DirectTypedOnlyRequiresSelectedTypedLeaf) {
    FieldSchema typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.lower_case_name = typed_value_field.name;
    typed_value_field.children = {make_int64_field_schema("metric")};
    typed_value_field.data_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {typed_value_field.children[0].data_type}, Strings {"metric"}));

    FieldSchema variant_field;
    variant_field.name = "v";
    variant_field.lower_case_name = variant_field.name;
    variant_field.data_type = make_nullable(std::make_shared<DataTypeVariant>(0, false));
    variant_field.children = {make_binary_field_schema("metadata", false), typed_value_field};

    uint64_t next_id = 1;
    variant_field.assign_ids(next_id);
    const auto& typed_value = variant_field.children[1];
    const auto& metric = typed_value.children[0];

    std::set<uint64_t> missing_path_ids {variant_field.get_column_id(),
                                         variant_field.children[0].get_column_id()};
    EXPECT_FALSE(parquet_variant_reader_test::can_use_direct_typed_only_value_for_test(
            variant_field, missing_path_ids));

    std::set<uint64_t> typed_root_only_ids {typed_value.get_column_id()};
    EXPECT_FALSE(parquet_variant_reader_test::can_use_direct_typed_only_value_for_test(
            variant_field, typed_root_only_ids));

    std::set<uint64_t> metric_ids {metric.get_column_id()};
    EXPECT_TRUE(parquet_variant_reader_test::can_use_direct_typed_only_value_for_test(variant_field,
                                                                                      metric_ids));
}

TEST(ParquetVariantReaderTest, DirectTypedOnlyAllowsUnselectedTopLevelResidualValue) {
    FieldSchema typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.lower_case_name = typed_value_field.name;
    typed_value_field.children = {make_int64_field_schema("metric")};
    typed_value_field.data_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {typed_value_field.children[0].data_type}, Strings {"metric"}));

    FieldSchema variant_field;
    variant_field.name = "v";
    variant_field.lower_case_name = variant_field.name;
    variant_field.data_type = make_nullable(std::make_shared<DataTypeVariant>(0, false));
    variant_field.children = {make_binary_field_schema("metadata", false),
                              make_binary_field_schema("value", true), typed_value_field};

    uint64_t next_id = 1;
    variant_field.assign_ids(next_id);
    const auto& value = variant_field.children[1];
    const auto& metric = variant_field.children[2].children[0];

    std::set<uint64_t> metric_ids {metric.get_column_id()};
    EXPECT_TRUE(parquet_variant_reader_test::can_use_direct_typed_only_value_for_test(variant_field,
                                                                                      metric_ids));

    std::set<uint64_t> metric_with_residual_ids {value.get_column_id(), metric.get_column_id()};
    EXPECT_FALSE(parquet_variant_reader_test::can_use_direct_typed_only_value_for_test(
            variant_field, metric_with_residual_ids));
}

TEST(ParquetVariantReaderTest, DirectTypedOnlyReaderCountersUseNativePath) {
    FieldSchema typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.lower_case_name = typed_value_field.name;
    typed_value_field.children = {make_int64_field_schema("metric")};
    typed_value_field.data_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {typed_value_field.children[0].data_type}, Strings {"metric"}));

    FieldSchema variant_field;
    variant_field.name = "v";
    variant_field.lower_case_name = variant_field.name;
    variant_field.data_type = std::make_shared<DataTypeVariant>(0, false);
    variant_field.children = {make_binary_field_schema("metadata", false), typed_value_field};

    uint64_t next_id = 1;
    variant_field.assign_ids(next_id);
    const auto& metric = variant_field.children[1].children[0];

    auto variant_struct_type = std::make_shared<DataTypeStruct>(
            DataTypes {variant_field.children[0].data_type, typed_value_field.data_type},
            Strings {"metadata", "typed_value"});
    MutableColumnPtr struct_column = variant_struct_type->create_column();
    for (int64_t metric_value : {7, 11}) {
        Struct typed_value;
        typed_value.push_back(Field::create_field<TYPE_BIGINT>(metric_value));
        Struct row;
        row.push_back(Field::create_field<TYPE_STRING>(String("")));
        row.push_back(Field::create_field<TYPE_STRUCT>(typed_value));
        struct_column->insert(Field::create_field<TYPE_STRUCT>(row));
    }

    ColumnPtr output = ColumnVariant::create(0, false);
    int64_t direct_rows = 0;
    int64_t rowwise_rows = 0;
    Status st = parquet_variant_reader_test::read_variant_rows_for_test(
            variant_field, *struct_column, {metric.get_column_id()}, output, &direct_rows,
            &rowwise_rows);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(2, direct_rows);
    EXPECT_EQ(0, rowwise_rows);
    ASSERT_EQ(2, output->size());

    Field first;
    output->get(0, first);
    const auto& first_values = first.get<TYPE_VARIANT>();
    EXPECT_EQ(first_values.at(PathInData("metric")).field.get<TYPE_BIGINT>(), 7);
}

TEST(ParquetVariantReaderTest, VariantReaderCountersUseRowWiseWhenResidualValueSelected) {
    FieldSchema typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.lower_case_name = typed_value_field.name;
    typed_value_field.children = {make_int64_field_schema("metric")};
    typed_value_field.data_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {typed_value_field.children[0].data_type}, Strings {"metric"}));

    FieldSchema variant_field;
    variant_field.name = "v";
    variant_field.lower_case_name = variant_field.name;
    variant_field.data_type = std::make_shared<DataTypeVariant>(0, false);
    variant_field.children = {make_binary_field_schema("metadata", false),
                              make_binary_field_schema("value", true), typed_value_field};

    uint64_t next_id = 1;
    variant_field.assign_ids(next_id);
    const auto& value = variant_field.children[1];
    const auto& metric = variant_field.children[2].children[0];

    auto variant_struct_type = std::make_shared<DataTypeStruct>(
            DataTypes {variant_field.children[0].data_type, value.data_type,
                       typed_value_field.data_type},
            Strings {"metadata", "value", "typed_value"});
    MutableColumnPtr struct_column = variant_struct_type->create_column();
    for (int64_t metric_value : {7, 11}) {
        Struct typed_value;
        typed_value.push_back(Field::create_field<TYPE_BIGINT>(metric_value));
        Struct row;
        row.push_back(Field::create_field<TYPE_STRING>(String("")));
        row.push_back(Field());
        row.push_back(Field::create_field<TYPE_STRUCT>(typed_value));
        struct_column->insert(Field::create_field<TYPE_STRUCT>(row));
    }

    ColumnPtr output = ColumnVariant::create(0, false);
    int64_t direct_rows = 0;
    int64_t rowwise_rows = 0;
    Status st = parquet_variant_reader_test::read_variant_rows_for_test(
            variant_field, *struct_column, {value.get_column_id(), metric.get_column_id()}, output,
            &direct_rows, &rowwise_rows);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(0, direct_rows);
    EXPECT_EQ(2, rowwise_rows);
    ASSERT_EQ(2, output->size());

    Field second;
    output->get(1, second);
    const auto& second_values = second.get<TYPE_VARIANT>();
    EXPECT_EQ(second_values.at(PathInData("metric")).field.get<TYPE_BIGINT>(), 11);
}

TEST(ParquetVariantReaderTest, DirectTypedOnlyPreservesNullableTypedStructNull) {
    FieldSchema metric_field = make_required_int64_field_schema("metric");

    FieldSchema typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.lower_case_name = typed_value_field.name;
    typed_value_field.children = {metric_field};
    typed_value_field.data_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {metric_field.data_type}, Strings {"metric"}));

    MutableColumnPtr typed_value_column = typed_value_field.data_type->create_column();
    typed_value_column->insert(Field());

    Struct typed_value;
    typed_value.push_back(Field::create_field<TYPE_BIGINT>(7));
    typed_value_column->insert(Field::create_field<TYPE_STRUCT>(typed_value));

    auto batch = ColumnVariant::create(0, false, 3);
    ASSERT_TRUE(
            parquet_variant_reader_test::can_direct_read_typed_value_for_test(typed_value_field));
    auto* batch_variant = batch.get();
    Status st = parquet_variant_reader_test::append_direct_typed_column_to_batch_for_test(
            typed_value_field, *typed_value_column, 0, 2, batch_variant);
    ASSERT_TRUE(st.ok()) << st.to_string();

    Field null_result;
    batch_variant->get(1, null_result);
    EXPECT_TRUE(null_result.is_null());

    Field present_result;
    batch_variant->get(2, present_result);
    const auto& values = present_result.get<TYPE_VARIANT>();
    EXPECT_EQ(values.at(PathInData("metric")).field.get<TYPE_BIGINT>(), 7);
}

TEST(ParquetVariantReaderTest, DirectTypedOnlyPreservesEmptyTypedObject) {
    FieldSchema metric_field = make_int64_field_schema("metric");

    FieldSchema typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.lower_case_name = typed_value_field.name;
    typed_value_field.children = {metric_field};
    typed_value_field.data_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {metric_field.data_type}, Strings {"metric"}));

    MutableColumnPtr typed_value_column = typed_value_field.data_type->create_column();
    typed_value_column->insert(Field());

    Struct empty_object;
    empty_object.push_back(Field());
    typed_value_column->insert(Field::create_field<TYPE_STRUCT>(empty_object));

    auto batch = ColumnVariant::create(0, false, 3);
    ASSERT_TRUE(
            parquet_variant_reader_test::can_direct_read_typed_value_for_test(typed_value_field));
    auto* batch_variant = batch.get();
    Status st = parquet_variant_reader_test::append_direct_typed_column_to_batch_for_test(
            typed_value_field, *typed_value_column, 0, 2, batch_variant);
    ASSERT_TRUE(st.ok()) << st.to_string();

    Field null_result;
    batch_variant->get(1, null_result);
    EXPECT_TRUE(null_result.is_null());

    Field empty_result;
    batch_variant->get(2, empty_result);
    EXPECT_FALSE(empty_result.is_null());

    std::string json;
    DataTypeSerDe::FormatOptions options;
    batch_variant->serialize_one_row_to_string(2, &json, options);
    EXPECT_EQ(json, "{}");
}

TEST(ParquetVariantReaderTest, DirectTypedOnlyPreservesNestedEmptyTypedObject) {
    FieldSchema metric_field = make_int64_field_schema("metric");

    FieldSchema nested_field;
    nested_field.name = "nested";
    nested_field.lower_case_name = nested_field.name;
    nested_field.children = {metric_field};
    nested_field.data_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {metric_field.data_type}, Strings {"metric"}));

    FieldSchema typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.lower_case_name = typed_value_field.name;
    typed_value_field.children = {nested_field};
    typed_value_field.data_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {nested_field.data_type}, Strings {"nested"}));

    MutableColumnPtr typed_value_column = typed_value_field.data_type->create_column();
    typed_value_column->insert(Field());

    Struct nested_object;
    nested_object.push_back(Field());
    Struct typed_value;
    typed_value.push_back(Field::create_field<TYPE_STRUCT>(nested_object));
    typed_value_column->insert(Field::create_field<TYPE_STRUCT>(typed_value));

    auto batch = ColumnVariant::create(0, false, 3);
    ASSERT_TRUE(
            parquet_variant_reader_test::can_direct_read_typed_value_for_test(typed_value_field));
    auto* batch_variant = batch.get();
    Status st = parquet_variant_reader_test::append_direct_typed_column_to_batch_for_test(
            typed_value_field, *typed_value_column, 0, 2, batch_variant);
    ASSERT_TRUE(st.ok()) << st.to_string();

    std::string json;
    DataTypeSerDe::FormatOptions options;
    batch_variant->serialize_one_row_to_string(2, &json, options);
    EXPECT_EQ(json, "{\"nested\":{}}");
}

TEST(ParquetVariantReaderTest, DirectTypedOnlyPreservesVarbinaryLeaf) {
    FieldSchema payload_field = make_varbinary_field_schema("payload");

    FieldSchema typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.lower_case_name = typed_value_field.name;
    typed_value_field.children = {payload_field};
    typed_value_field.data_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {payload_field.data_type}, Strings {"payload"}));

    MutableColumnPtr typed_value_column = typed_value_field.data_type->create_column();
    typed_value_column->insert(Field());

    Struct typed_value;
    typed_value.push_back(make_varbinary_field({0xff, 0x00, 0x41}));
    typed_value_column->insert(Field::create_field<TYPE_STRUCT>(typed_value));

    auto batch = ColumnVariant::create(0, false, 3);
    ASSERT_TRUE(
            parquet_variant_reader_test::can_direct_read_typed_value_for_test(typed_value_field));
    auto* batch_variant = batch.get();
    Status st = parquet_variant_reader_test::append_direct_typed_column_to_batch_for_test(
            typed_value_field, *typed_value_column, 0, 2, batch_variant);
    ASSERT_TRUE(st.ok()) << st.to_string();

    Field null_result;
    batch_variant->get(1, null_result);
    EXPECT_TRUE(null_result.is_null());

    Field present_result;
    batch_variant->get(2, present_result);
    const auto& values = present_result.get<TYPE_VARIANT>();
    const auto& payload = values.at(PathInData("payload"));
    EXPECT_EQ(payload.base_scalar_type_id, TYPE_VARBINARY);
    EXPECT_EQ(varbinary_field_bytes(payload.field), std::string("\xff\x00\x41", 3));
}

TEST(ParquetVariantReaderTest, DirectTypedOnlyPreservesFloatingPointLeaves) {
    FieldSchema float_field = make_float_field_schema("f");
    FieldSchema double_field = make_double_field_schema("d");

    FieldSchema typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.lower_case_name = typed_value_field.name;
    typed_value_field.children = {float_field, double_field};
    typed_value_field.data_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {float_field.data_type, double_field.data_type}, Strings {"f", "d"}));

    FieldSchema variant_field;
    variant_field.name = "v";
    variant_field.lower_case_name = variant_field.name;
    variant_field.data_type = make_nullable(std::make_shared<DataTypeVariant>(0, false));
    variant_field.children = {make_binary_field_schema("metadata", false), typed_value_field};
    uint64_t next_id = 1;
    variant_field.assign_ids(next_id);
    std::set<uint64_t> typed_leaf_ids {variant_field.children[1].children[0].get_column_id(),
                                       variant_field.children[1].children[1].get_column_id()};
    EXPECT_TRUE(parquet_variant_reader_test::can_use_direct_typed_only_value_for_test(
            variant_field, typed_leaf_ids));

    MutableColumnPtr typed_value_column = typed_value_field.data_type->create_column();
    typed_value_column->insert(Field());

    Struct typed_value;
    typed_value.push_back(Field::create_field<TYPE_FLOAT>(1.25F));
    typed_value.push_back(Field::create_field<TYPE_DOUBLE>(2.5));
    typed_value_column->insert(Field::create_field<TYPE_STRUCT>(typed_value));

    auto batch = ColumnVariant::create(0, false, 3);
    ASSERT_TRUE(
            parquet_variant_reader_test::can_direct_read_typed_value_for_test(typed_value_field));
    auto* batch_variant = batch.get();
    Status st = parquet_variant_reader_test::append_direct_typed_column_to_batch_for_test(
            typed_value_field, *typed_value_column, 0, 2, batch_variant);
    ASSERT_TRUE(st.ok()) << st.to_string();

    Field null_result;
    batch_variant->get(1, null_result);
    EXPECT_TRUE(null_result.is_null());

    Field present_result;
    batch_variant->get(2, present_result);
    const auto& values = present_result.get<TYPE_VARIANT>();
    const auto& float_value = values.at(PathInData("f"));
    EXPECT_EQ(float_value.base_scalar_type_id, TYPE_FLOAT);
    EXPECT_FLOAT_EQ(float_value.field.get<TYPE_FLOAT>(), 1.25F);
    const auto& double_value = values.at(PathInData("d"));
    EXPECT_EQ(double_value.base_scalar_type_id, TYPE_DOUBLE);
    EXPECT_DOUBLE_EQ(double_value.field.get<TYPE_DOUBLE>(), 2.5);
}

TEST(ParquetVariantReaderTest, DirectTypedOnlyPreservesNonFiniteFloatingPointLeaf) {
    FieldSchema nan_field = make_double_field_schema("nan");
    FieldSchema inf_field = make_double_field_schema("inf");

    FieldSchema typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.lower_case_name = typed_value_field.name;
    typed_value_field.children = {nan_field, inf_field};
    typed_value_field.data_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {nan_field.data_type, inf_field.data_type}, Strings {"nan", "inf"}));

    MutableColumnPtr typed_value_column = typed_value_field.data_type->create_column();
    Struct typed_value;
    typed_value.push_back(
            Field::create_field<TYPE_DOUBLE>(std::numeric_limits<double>::quiet_NaN()));
    typed_value.push_back(
            Field::create_field<TYPE_DOUBLE>(std::numeric_limits<double>::infinity()));
    typed_value_column->insert(Field::create_field<TYPE_STRUCT>(typed_value));

    auto batch = ColumnVariant::create(0, false, 2);
    ASSERT_TRUE(
            parquet_variant_reader_test::can_direct_read_typed_value_for_test(typed_value_field));
    Status st = parquet_variant_reader_test::append_direct_typed_column_to_batch_for_test(
            typed_value_field, *typed_value_column, 0, 1, batch.get());
    ASSERT_TRUE(st.ok()) << st.to_string();

    Field present_result;
    batch->get(1, present_result);
    const auto& values = present_result.get<TYPE_VARIANT>();
    EXPECT_TRUE(std::isnan(values.at(PathInData("nan")).field.get<TYPE_DOUBLE>()));
    EXPECT_TRUE(std::isinf(values.at(PathInData("inf")).field.get<TYPE_DOUBLE>()));
}

TEST(ParquetVariantReaderTest, DirectTypedOnlyPreservesFloatingPointArrayLeaf) {
    FieldSchema element = make_double_field_schema("element");

    FieldSchema typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.lower_case_name = typed_value_field.name;
    typed_value_field.children = {element};
    typed_value_field.data_type = make_nullable(std::make_shared<DataTypeArray>(element.data_type));

    MutableColumnPtr typed_value_column = typed_value_field.data_type->create_column();
    typed_value_column->insert(Field());

    Array array;
    array.push_back(Field::create_field<TYPE_DOUBLE>(1.5));
    array.push_back(Field());
    array.push_back(Field::create_field<TYPE_DOUBLE>(2.25));
    typed_value_column->insert(Field::create_field<TYPE_ARRAY>(array));

    auto batch = ColumnVariant::create(0, false, 3);
    ASSERT_TRUE(
            parquet_variant_reader_test::can_direct_read_typed_value_for_test(typed_value_field));
    Status st = parquet_variant_reader_test::append_direct_typed_column_to_batch_for_test(
            typed_value_field, *typed_value_column, 0, 2, batch.get());
    ASSERT_TRUE(st.ok()) << st.to_string();
    const auto* root_subcolumn = batch->get_subcolumn(PathInData());
    ASSERT_NE(root_subcolumn, nullptr);
    EXPECT_TRUE(root_subcolumn->is_null_at(1));

    Field present_result;
    batch->get(2, present_result);
    const auto& values = present_result.get<TYPE_VARIANT>();
    auto array_value = values.find(PathInData());
    ASSERT_NE(array_value, values.end());
    EXPECT_EQ(array_value->second.base_scalar_type_id, TYPE_DOUBLE);
    EXPECT_EQ(array_value->second.num_dimensions, 1);
    const auto& result_array = array_value->second.field.get<TYPE_ARRAY>();
    ASSERT_EQ(result_array.size(), 3);
    EXPECT_DOUBLE_EQ(result_array[0].get<TYPE_DOUBLE>(), 1.5);
    EXPECT_TRUE(result_array[1].is_null());
    EXPECT_DOUBLE_EQ(result_array[2].get<TYPE_DOUBLE>(), 2.25);
}

TEST(ParquetVariantReaderTest, DirectTypedOnlyPreservesNonFiniteFloatingPointArrayLeaf) {
    FieldSchema element = make_double_field_schema("element");

    FieldSchema typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.lower_case_name = typed_value_field.name;
    typed_value_field.children = {element};
    typed_value_field.data_type = make_nullable(std::make_shared<DataTypeArray>(element.data_type));

    MutableColumnPtr typed_value_column = typed_value_field.data_type->create_column();
    Array array;
    array.push_back(Field::create_field<TYPE_DOUBLE>(std::numeric_limits<double>::quiet_NaN()));
    typed_value_column->insert(Field::create_field<TYPE_ARRAY>(array));

    auto batch = ColumnVariant::create(0, false, 2);
    ASSERT_TRUE(
            parquet_variant_reader_test::can_direct_read_typed_value_for_test(typed_value_field));
    Status st = parquet_variant_reader_test::append_direct_typed_column_to_batch_for_test(
            typed_value_field, *typed_value_column, 0, 1, batch.get());
    ASSERT_TRUE(st.ok()) << st.to_string();

    Field present_result;
    batch->get(1, present_result);
    const auto& values = present_result.get<TYPE_VARIANT>();
    auto array_value = values.find(PathInData());
    ASSERT_NE(array_value, values.end());
    const auto& result_array = array_value->second.field.get<TYPE_ARRAY>();
    ASSERT_EQ(result_array.size(), 1);
    EXPECT_TRUE(std::isnan(result_array[0].get<TYPE_DOUBLE>()));
}

TEST(ParquetVariantReaderTest, DirectTypedOnlyPreservesUuidSemantics) {
    FieldSchema uuid_field = make_uuid_field_schema("u");

    FieldSchema typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.lower_case_name = typed_value_field.name;
    typed_value_field.children = {uuid_field};
    typed_value_field.data_type = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {uuid_field.data_type}, Strings {"u"}));

    MutableColumnPtr typed_value_column = typed_value_field.data_type->create_column();
    typed_value_column->insert(Field());

    std::string uuid_bytes = test_uuid_bytes();
    Struct typed_value;
    typed_value.push_back(make_varbinary_field(uuid_bytes));
    typed_value_column->insert(Field::create_field<TYPE_STRUCT>(typed_value));

    auto batch = ColumnVariant::create(0, false, 3);
    ASSERT_TRUE(
            parquet_variant_reader_test::can_direct_read_typed_value_for_test(typed_value_field));
    auto* batch_variant = batch.get();
    Status st = parquet_variant_reader_test::append_direct_typed_column_to_batch_for_test(
            typed_value_field, *typed_value_column, 0, 2, batch_variant);
    ASSERT_TRUE(st.ok()) << st.to_string();

    Field present_result;
    batch_variant->get(2, present_result);
    const auto& values = present_result.get<TYPE_VARIANT>();
    const auto& uuid = values.at(PathInData("u"));
    EXPECT_EQ(uuid.base_scalar_type_id, TYPE_STRING);
    EXPECT_EQ(uuid.field.get<TYPE_STRING>(), "00010203-0405-0607-0809-0a0b0c0d0e0f");
}

TEST(ParquetVariantReaderTest, RowWisePreservesTypedUuidSemantics) {
    FieldSchema uuid_field = make_uuid_field_schema("u");

    FieldSchema typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.lower_case_name = typed_value_field.name;
    typed_value_field.children = {uuid_field};
    typed_value_field.data_type = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {uuid_field.data_type}, Strings {"u"}));

    FieldSchema variant_field;
    variant_field.name = "v";
    variant_field.lower_case_name = variant_field.name;
    variant_field.data_type = make_nullable(std::make_shared<DataTypeVariant>(0, false));
    variant_field.children = {make_binary_field_schema("metadata", false), typed_value_field};

    auto metadata = make_metadata({});
    std::string uuid_bytes = test_uuid_bytes();
    Struct typed_value;
    typed_value.push_back(make_varbinary_field(uuid_bytes));

    Struct row;
    row.push_back(Field::create_field<TYPE_STRING>(
            String(reinterpret_cast<const char*>(metadata.data()), metadata.size())));
    row.push_back(Field::create_field<TYPE_STRUCT>(typed_value));

    Field result;
    bool sql_null = true;
    Status st = parquet_variant_reader_test::read_variant_row_for_test(
            variant_field, Field::create_field<TYPE_STRUCT>(row), true, &result, &sql_null);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_FALSE(sql_null);

    const auto& values = result.get<TYPE_VARIANT>();
    const auto& uuid = values.at(PathInData("u"));
    EXPECT_EQ(uuid.base_scalar_type_id, TYPE_STRING);
    EXPECT_EQ(uuid.field.get<TYPE_STRING>(), "00010203-0405-0607-0809-0a0b0c0d0e0f");
}

TEST(ParquetVariantReaderTest, DirectTypedOnlyPreservesTypedUuidArraySemantics) {
    FieldSchema element = make_uuid_field_schema("element");

    FieldSchema typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.lower_case_name = typed_value_field.name;
    typed_value_field.children = {element};
    typed_value_field.data_type = make_nullable(std::make_shared<DataTypeArray>(element.data_type));

    MutableColumnPtr typed_value_column = typed_value_field.data_type->create_column();
    typed_value_column->insert(Field());

    std::string uuid_bytes = test_uuid_bytes();
    Array array;
    array.push_back(make_varbinary_field(uuid_bytes));
    array.push_back(Field());
    typed_value_column->insert(Field::create_field<TYPE_ARRAY>(array));

    auto batch = ColumnVariant::create(0, false, 3);
    ASSERT_TRUE(
            parquet_variant_reader_test::can_direct_read_typed_value_for_test(typed_value_field));
    auto* batch_variant = batch.get();
    Status st = parquet_variant_reader_test::append_direct_typed_column_to_batch_for_test(
            typed_value_field, *typed_value_column, 0, 2, batch_variant);
    ASSERT_TRUE(st.ok()) << st.to_string();

    Field null_result;
    batch_variant->get(1, null_result);
    EXPECT_TRUE(null_result.is_null());

    Field present_result;
    batch_variant->get(2, present_result);
    const auto& values = present_result.get<TYPE_VARIANT>();
    auto array_value = values.find(PathInData());
    ASSERT_NE(array_value, values.end());
    EXPECT_EQ(array_value->second.base_scalar_type_id, TYPE_STRING);
    EXPECT_EQ(array_value->second.num_dimensions, 1);
    const auto& result_array = array_value->second.field.get<TYPE_ARRAY>();
    ASSERT_EQ(result_array.size(), 2);
    EXPECT_EQ(result_array[0].get<TYPE_STRING>(), "00010203-0405-0607-0809-0a0b0c0d0e0f");
    EXPECT_TRUE(result_array[1].is_null());
}

TEST(ParquetVariantReaderTest, RowWisePreservesTypedUuidArraySemantics) {
    FieldSchema element = make_uuid_field_schema("element");

    FieldSchema typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.lower_case_name = typed_value_field.name;
    typed_value_field.children = {element};
    typed_value_field.data_type = make_nullable(std::make_shared<DataTypeArray>(element.data_type));

    FieldSchema variant_field;
    variant_field.name = "v";
    variant_field.lower_case_name = variant_field.name;
    variant_field.data_type = make_nullable(std::make_shared<DataTypeVariant>(0, false));
    variant_field.children = {make_binary_field_schema("metadata", false), typed_value_field};

    auto metadata = make_metadata({});
    std::string uuid_bytes = test_uuid_bytes();
    Array array;
    array.push_back(make_varbinary_field(uuid_bytes));

    Struct row;
    row.push_back(Field::create_field<TYPE_STRING>(
            String(reinterpret_cast<const char*>(metadata.data()), metadata.size())));
    row.push_back(Field::create_field<TYPE_ARRAY>(array));

    Field result;
    bool sql_null = true;
    Status st = parquet_variant_reader_test::read_variant_row_for_test(
            variant_field, Field::create_field<TYPE_STRUCT>(row), true, &result, &sql_null);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_FALSE(sql_null);

    const auto& values = result.get<TYPE_VARIANT>();
    auto array_value = values.find(PathInData());
    ASSERT_NE(array_value, values.end());
    EXPECT_EQ(array_value->second.base_scalar_type_id, TYPE_STRING);
    EXPECT_EQ(array_value->second.num_dimensions, 1);
    const auto& result_array = array_value->second.field.get<TYPE_ARRAY>();
    ASSERT_EQ(result_array.size(), 1);
    EXPECT_EQ(result_array[0].get<TYPE_STRING>(), "00010203-0405-0607-0809-0a0b0c0d0e0f");
}

TEST(ParquetVariantReaderTest, RowWisePreservesExplicitVariantNullShreddedArrayElement) {
    FieldSchema element;
    element.name = "element";
    element.lower_case_name = element.name;
    element.children = {make_binary_field_schema("value", true),
                        make_int64_field_schema("typed_value")};
    element.data_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {element.children[0].data_type, element.children[1].data_type},
            Strings {"value", "typed_value"}));

    FieldSchema typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.lower_case_name = typed_value_field.name;
    typed_value_field.children = {element};
    typed_value_field.data_type = make_nullable(std::make_shared<DataTypeArray>(element.data_type));

    FieldSchema variant_field;
    variant_field.name = "v";
    variant_field.lower_case_name = variant_field.name;
    variant_field.data_type = make_nullable(std::make_shared<DataTypeVariant>(0, false));
    variant_field.children = {make_binary_field_schema("metadata", false),
                              make_binary_field_schema("value", true), typed_value_field};

    auto metadata = make_metadata({});
    std::vector<uint8_t> variant_null {0x00};
    Struct element_row;
    element_row.push_back(Field::create_field<TYPE_STRING>(
            String(reinterpret_cast<const char*>(variant_null.data()), variant_null.size())));
    element_row.push_back(Field());
    Array array;
    array.push_back(Field::create_field<TYPE_STRUCT>(element_row));

    Struct row;
    row.push_back(Field::create_field<TYPE_STRING>(
            String(reinterpret_cast<const char*>(metadata.data()), metadata.size())));
    row.push_back(Field());
    row.push_back(Field::create_field<TYPE_ARRAY>(array));

    Field result;
    bool sql_null = true;
    Status st = parquet_variant_reader_test::read_variant_row_for_test(
            variant_field, Field::create_field<TYPE_STRUCT>(row), true, &result, &sql_null);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_FALSE(sql_null);

    const auto& values = result.get<TYPE_VARIANT>();
    auto array_value = values.find(PathInData());
    ASSERT_NE(array_value, values.end());
    EXPECT_EQ("[null]", serialize_variant_field(result));
}

TEST(ParquetVariantReaderTest, RowWisePreservesNullComplexTypedArrayElement) {
    FieldSchema payload_field = make_int64_field_schema("payload");

    FieldSchema element;
    element.name = "element";
    element.lower_case_name = element.name;
    element.children = {payload_field};
    element.data_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {payload_field.data_type}, Strings {"payload"}));

    FieldSchema typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.lower_case_name = typed_value_field.name;
    typed_value_field.children = {element};
    typed_value_field.data_type = make_nullable(std::make_shared<DataTypeArray>(element.data_type));

    FieldSchema variant_field;
    variant_field.name = "v";
    variant_field.lower_case_name = variant_field.name;
    variant_field.data_type = make_nullable(std::make_shared<DataTypeVariant>(0, false));
    variant_field.children = {make_binary_field_schema("metadata", false), typed_value_field};

    auto metadata = make_metadata({});
    Struct element_value;
    element_value.push_back(Field::create_field<TYPE_BIGINT>(7));
    Array array;
    array.push_back(Field());
    array.push_back(Field::create_field<TYPE_STRUCT>(element_value));

    Struct row;
    row.push_back(Field::create_field<TYPE_STRING>(
            String(reinterpret_cast<const char*>(metadata.data()), metadata.size())));
    row.push_back(Field::create_field<TYPE_ARRAY>(array));

    Field result;
    bool sql_null = true;
    Status st = parquet_variant_reader_test::read_variant_row_for_test(
            variant_field, Field::create_field<TYPE_STRUCT>(row), true, &result, &sql_null);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_FALSE(sql_null);

    const auto& values = result.get<TYPE_VARIANT>();
    const auto& object_array = values.at(PathInData());
    EXPECT_EQ(object_array.base_scalar_type_id, TYPE_VARIANT);
    EXPECT_EQ(object_array.num_dimensions, 1);
    const auto& result_array = object_array.field.get<TYPE_ARRAY>();
    ASSERT_EQ(result_array.size(), 2);
    EXPECT_TRUE(result_array[0].is_null());
    ASSERT_EQ(result_array[1].get_type(), TYPE_VARIANT);
    const auto& object = result_array[1].get<TYPE_VARIANT>();
    const auto& payload = object.at(PathInData("payload"));
    EXPECT_EQ(payload.base_scalar_type_id, TYPE_BIGINT);
    EXPECT_EQ(payload.field.get<TYPE_BIGINT>(), 7);
}

TEST(ParquetVariantReaderTest, RowWiseRejectsMissingShreddedArrayElement) {
    FieldSchema element;
    element.name = "element";
    element.lower_case_name = element.name;
    element.children = {make_binary_field_schema("value", true),
                        make_int64_field_schema("typed_value")};
    element.data_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {element.children[0].data_type, element.children[1].data_type},
            Strings {"value", "typed_value"}));

    FieldSchema typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.lower_case_name = typed_value_field.name;
    typed_value_field.children = {element};
    typed_value_field.data_type = make_nullable(std::make_shared<DataTypeArray>(element.data_type));

    FieldSchema variant_field;
    variant_field.name = "v";
    variant_field.lower_case_name = variant_field.name;
    variant_field.data_type = make_nullable(std::make_shared<DataTypeVariant>(0, false));
    variant_field.children = {make_binary_field_schema("metadata", false),
                              make_binary_field_schema("value", true), typed_value_field};

    auto metadata = make_metadata({});
    Struct element_row;
    element_row.push_back(Field());
    element_row.push_back(Field());
    Array array;
    array.push_back(Field::create_field<TYPE_STRUCT>(element_row));

    Struct row;
    row.push_back(Field::create_field<TYPE_STRING>(
            String(reinterpret_cast<const char*>(metadata.data()), metadata.size())));
    row.push_back(Field());
    row.push_back(Field::create_field<TYPE_ARRAY>(array));

    Field result;
    bool sql_null = true;
    Status st = parquet_variant_reader_test::read_variant_row_for_test(
            variant_field, Field::create_field<TYPE_STRUCT>(row), true, &result, &sql_null);
    EXPECT_TRUE(st.is<ErrorCode::CORRUPTION>()) << st.to_string();
}

TEST(ParquetVariantReaderTest, RowWisePreservesTypedDecimalArrayMetadata) {
    FieldSchema element;
    element.name = "element";
    element.lower_case_name = element.name;
    element.physical_type = tparquet::Type::INT64;
    element.data_type = make_nullable(std::make_shared<DataTypeDecimal64>(18, 2));

    FieldSchema typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.lower_case_name = typed_value_field.name;
    typed_value_field.children = {element};
    typed_value_field.data_type = make_nullable(std::make_shared<DataTypeArray>(element.data_type));

    FieldSchema variant_field;
    variant_field.name = "v";
    variant_field.lower_case_name = variant_field.name;
    variant_field.data_type = make_nullable(std::make_shared<DataTypeVariant>(0, false));
    variant_field.children = {make_binary_field_schema("metadata", false),
                              make_binary_field_schema("value", true), typed_value_field};

    auto metadata = make_metadata({});
    Array array;
    array.push_back(Field::create_field<TYPE_DECIMAL64>(Decimal64(12345)));
    array.push_back(Field::create_field<TYPE_DECIMAL64>(Decimal64(67890)));

    Struct row;
    row.push_back(Field::create_field<TYPE_STRING>(
            String(reinterpret_cast<const char*>(metadata.data()), metadata.size())));
    row.push_back(Field());
    row.push_back(Field::create_field<TYPE_ARRAY>(array));

    Field result;
    bool sql_null = true;
    Status st = parquet_variant_reader_test::read_variant_row_for_test(
            variant_field, Field::create_field<TYPE_STRUCT>(row), true, &result, &sql_null);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_FALSE(sql_null);

    const auto& values = result.get<TYPE_VARIANT>();
    auto array_value = values.find(PathInData());
    ASSERT_NE(array_value, values.end());
    EXPECT_EQ(array_value->second.base_scalar_type_id, TYPE_DECIMAL64);
    EXPECT_EQ(array_value->second.num_dimensions, 1);
    EXPECT_EQ(array_value->second.precision, 18);
    EXPECT_EQ(array_value->second.scale, 2);
    const auto& result_array = array_value->second.field.get<TYPE_ARRAY>();
    ASSERT_EQ(result_array.size(), 2);
    EXPECT_EQ(result_array[0].get<TYPE_DECIMAL64>(), Decimal64(12345));
    EXPECT_EQ(result_array[1].get<TYPE_DECIMAL64>(), Decimal64(67890));
}

TEST(ParquetVariantReaderTest, RowWisePreservesTypedVarbinaryObjectField) {
    FieldSchema payload_field = make_varbinary_field_schema("payload");

    FieldSchema typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.lower_case_name = typed_value_field.name;
    typed_value_field.children = {payload_field};
    typed_value_field.data_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {payload_field.data_type}, Strings {"payload"}));

    FieldSchema variant_field;
    variant_field.name = "v";
    variant_field.lower_case_name = variant_field.name;
    variant_field.data_type = make_nullable(std::make_shared<DataTypeVariant>(0, false));
    variant_field.children = {make_binary_field_schema("metadata", false), typed_value_field};

    auto metadata = make_metadata({});
    Struct typed_value;
    typed_value.push_back(make_varbinary_field({0xc3, 0x28}));

    Struct row;
    row.push_back(Field::create_field<TYPE_STRING>(
            String(reinterpret_cast<const char*>(metadata.data()), metadata.size())));
    row.push_back(Field::create_field<TYPE_STRUCT>(typed_value));

    Field result;
    bool sql_null = true;
    Status st = parquet_variant_reader_test::read_variant_row_for_test(
            variant_field, Field::create_field<TYPE_STRUCT>(row), true, &result, &sql_null);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_FALSE(sql_null);

    const auto& values = result.get<TYPE_VARIANT>();
    const auto& payload = values.at(PathInData("payload"));
    EXPECT_EQ(payload.base_scalar_type_id, TYPE_VARBINARY);
    EXPECT_EQ(varbinary_field_bytes(payload.field), std::string("\xc3\x28", 2));
}

TEST(ParquetVariantReaderTest, RowWisePreservesTypedVarbinaryObjectArrayField) {
    FieldSchema payload_field = make_varbinary_field_schema("payload");

    FieldSchema element;
    element.name = "element";
    element.lower_case_name = element.name;
    element.children = {payload_field};
    element.data_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {payload_field.data_type}, Strings {"payload"}));

    FieldSchema typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.lower_case_name = typed_value_field.name;
    typed_value_field.children = {element};
    typed_value_field.data_type = make_nullable(std::make_shared<DataTypeArray>(element.data_type));

    FieldSchema variant_field;
    variant_field.name = "v";
    variant_field.lower_case_name = variant_field.name;
    variant_field.data_type = make_nullable(std::make_shared<DataTypeVariant>(0, false));
    variant_field.children = {make_binary_field_schema("metadata", false), typed_value_field};

    auto metadata = make_metadata({});
    Struct element_value;
    element_value.push_back(make_varbinary_field({0xc3, 0x28}));
    Array array;
    array.push_back(Field::create_field<TYPE_STRUCT>(element_value));

    Struct row;
    row.push_back(Field::create_field<TYPE_STRING>(
            String(reinterpret_cast<const char*>(metadata.data()), metadata.size())));
    row.push_back(Field::create_field<TYPE_ARRAY>(array));

    Field result;
    bool sql_null = true;
    Status st = parquet_variant_reader_test::read_variant_row_for_test(
            variant_field, Field::create_field<TYPE_STRUCT>(row), true, &result, &sql_null);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_FALSE(sql_null);

    const auto& values = result.get<TYPE_VARIANT>();
    const auto& object_array = values.at(PathInData());
    EXPECT_EQ(object_array.base_scalar_type_id, TYPE_VARIANT);
    EXPECT_EQ(object_array.num_dimensions, 1);
    const auto& result_array = object_array.field.get<TYPE_ARRAY>();
    ASSERT_EQ(result_array.size(), 1);
    ASSERT_EQ(result_array[0].get_type(), TYPE_VARIANT);
    const auto& object = result_array[0].get<TYPE_VARIANT>();
    const auto& payload = object.at(PathInData("payload"));
    EXPECT_EQ(payload.base_scalar_type_id, TYPE_VARBINARY);
    EXPECT_EQ(varbinary_field_bytes(payload.field), std::string("\xc3\x28", 2));
}

TEST(ParquetVariantReaderTest, RowWisePreservesResidualBinaryObjectField) {
    FieldSchema variant_field;
    variant_field.name = "v";
    variant_field.lower_case_name = variant_field.name;
    variant_field.data_type = make_nullable(std::make_shared<DataTypeVariant>(0, false));
    variant_field.children = {make_binary_field_schema("metadata", false),
                              make_binary_field_schema("value", true)};

    auto metadata = make_metadata({"b"});
    std::vector<uint8_t> residual_value {0x02,                         // object
                                         0x01,                         // one field
                                         0x00,                         // dictionary id 0: b
                                         0x00, 0x07,                   // field value offsets
                                         0x3c, 0x02, 0x00, 0x00, 0x00, // binary primitive, 2 bytes
                                         0xc3, 0x28};

    Struct row;
    row.push_back(Field::create_field<TYPE_STRING>(
            String(reinterpret_cast<const char*>(metadata.data()), metadata.size())));
    row.push_back(Field::create_field<TYPE_STRING>(
            String(reinterpret_cast<const char*>(residual_value.data()), residual_value.size())));

    Field result;
    bool sql_null = true;
    Status st = parquet_variant_reader_test::read_variant_row_for_test(
            variant_field, Field::create_field<TYPE_STRUCT>(row), true, &result, &sql_null);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_FALSE(sql_null);

    const auto& values = result.get<TYPE_VARIANT>();
    const auto& payload = values.at(PathInData("b"));
    EXPECT_EQ(payload.base_scalar_type_id, TYPE_VARBINARY);
    EXPECT_EQ(varbinary_field_bytes(payload.field), std::string("\xc3\x28", 2));
}

TEST(ParquetVariantReaderTest, RowWisePreservesResidualBinaryArray) {
    FieldSchema variant_field;
    variant_field.name = "v";
    variant_field.lower_case_name = variant_field.name;
    variant_field.data_type = make_nullable(std::make_shared<DataTypeVariant>(0, false));
    variant_field.children = {make_binary_field_schema("metadata", false),
                              make_binary_field_schema("value", true)};

    auto metadata = make_metadata({});
    std::vector<uint8_t> residual_value {0x03,                         // array
                                         0x02,                         // two elements
                                         0x00, 0x07, 0x08,             // element value offsets
                                         0x3c, 0x02, 0x00, 0x00, 0x00, // binary primitive, 2 bytes
                                         0xc3, 0x28, 0x00};            // variant null

    Struct row;
    row.push_back(Field::create_field<TYPE_STRING>(
            String(reinterpret_cast<const char*>(metadata.data()), metadata.size())));
    row.push_back(Field::create_field<TYPE_STRING>(
            String(reinterpret_cast<const char*>(residual_value.data()), residual_value.size())));

    Field result;
    bool sql_null = true;
    Status st = parquet_variant_reader_test::read_variant_row_for_test(
            variant_field, Field::create_field<TYPE_STRUCT>(row), true, &result, &sql_null);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_FALSE(sql_null);

    const auto& values = result.get<TYPE_VARIANT>();
    const auto& binary_array = values.at(PathInData());
    EXPECT_EQ(binary_array.base_scalar_type_id, TYPE_VARBINARY);
    EXPECT_EQ(binary_array.num_dimensions, 1);
    const auto& array = binary_array.field.get<TYPE_ARRAY>();
    ASSERT_EQ(array.size(), 2);
    EXPECT_EQ(varbinary_field_bytes(array[0]), std::string("\xc3\x28", 2));
    EXPECT_TRUE(array[1].is_null());
}

TEST(ParquetVariantReaderTest, RowWisePreservesResidualBinaryObjectArray) {
    FieldSchema variant_field;
    variant_field.name = "v";
    variant_field.lower_case_name = variant_field.name;
    variant_field.data_type = make_nullable(std::make_shared<DataTypeVariant>(0, false));
    variant_field.children = {make_binary_field_schema("metadata", false),
                              make_binary_field_schema("value", true)};

    auto metadata = make_metadata({"b"});
    std::vector<uint8_t> residual_value {0x03,                         // array
                                         0x01,                         // one element
                                         0x00, 0x0c,                   // element value offsets
                                         0x02,                         // object
                                         0x01,                         // one field
                                         0x00,                         // dictionary id 0: b
                                         0x00, 0x07,                   // field value offsets
                                         0x3c, 0x02, 0x00, 0x00, 0x00, // binary primitive, 2 bytes
                                         0xc3, 0x28};

    Struct row;
    row.push_back(Field::create_field<TYPE_STRING>(
            String(reinterpret_cast<const char*>(metadata.data()), metadata.size())));
    row.push_back(Field::create_field<TYPE_STRING>(
            String(reinterpret_cast<const char*>(residual_value.data()), residual_value.size())));

    Field result;
    bool sql_null = true;
    Status st = parquet_variant_reader_test::read_variant_row_for_test(
            variant_field, Field::create_field<TYPE_STRUCT>(row), true, &result, &sql_null);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_FALSE(sql_null);

    const auto& values = result.get<TYPE_VARIANT>();
    const auto& object_array = values.at(PathInData());
    EXPECT_EQ(object_array.base_scalar_type_id, TYPE_VARIANT);
    EXPECT_EQ(object_array.num_dimensions, 1);
    const auto& array = object_array.field.get<TYPE_ARRAY>();
    ASSERT_EQ(array.size(), 1);
    ASSERT_EQ(array[0].get_type(), TYPE_VARIANT);
    const auto& object = array[0].get<TYPE_VARIANT>();
    const auto& binary = object.at(PathInData("b"));
    EXPECT_EQ(binary.base_scalar_type_id, TYPE_VARBINARY);
    EXPECT_EQ(varbinary_field_bytes(binary.field), std::string("\xc3\x28", 2));
}

TEST(ParquetVariantReaderTest, RequiredMissingPayloadIsVariantNull) {
    FieldSchema variant_field = make_required_shredded_variant_schema();

    Struct row;
    row.push_back(Field::create_field<TYPE_STRING>(String("")));
    row.push_back(Field());
    row.push_back(Field());

    Field result;
    bool sql_null = true;
    Status st = parquet_variant_reader_test::read_variant_row_for_test(
            variant_field, Field::create_field<TYPE_STRUCT>(row), true, &result, &sql_null);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_FALSE(sql_null);
    EXPECT_TRUE(result.is_null());
}

TEST(ParquetVariantReaderTest, NullableTopLevelGroupIsSqlNull) {
    FieldSchema variant_field = make_required_shredded_variant_schema();

    Field result;
    bool sql_null = false;
    Status st = parquet_variant_reader_test::read_variant_row_for_test(variant_field, Field(), true,
                                                                       &result, &sql_null);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_TRUE(sql_null);
}

TEST(ParquetVariantReaderTest, NestedWrapperMergesResidualValueAndTypedValue) {
    FieldSchema typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.lower_case_name = typed_value_field.name;
    typed_value_field.children = {make_int32_field_schema("metric")};
    typed_value_field.data_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {make_nullable(std::make_shared<DataTypeInt32>())}, Strings {"metric"}));

    FieldSchema wrapper_field;
    wrapper_field.name = "element";
    wrapper_field.lower_case_name = wrapper_field.name;
    wrapper_field.children = {make_binary_field_schema("value", true), typed_value_field};
    wrapper_field.data_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {wrapper_field.children[0].data_type, typed_value_field.data_type},
            Strings {"value", "typed_value"}));

    auto metadata = make_metadata({"extra"});
    std::vector<uint8_t> residual_value {
            0x02,       // object, 1-byte offsets, 1-byte field ids, 1-byte element count
            0x01,       // one field
            0x00,       // dictionary id 0: extra
            0x00, 0x02, // field value offsets
            0x0c, 0x07  // int8(7)
    };

    Struct typed_value;
    typed_value.push_back(Field::create_field<TYPE_INT>(1));
    Struct row;
    row.push_back(Field::create_field<TYPE_STRING>(
            String(reinterpret_cast<const char*>(residual_value.data()), residual_value.size())));
    row.push_back(Field::create_field<TYPE_STRUCT>(typed_value));

    std::string json;
    bool present = false;
    Status st = parquet_variant_reader_test::variant_to_json_for_test(
            wrapper_field, Field::create_field<TYPE_STRUCT>(row),
            std::string(reinterpret_cast<const char*>(metadata.data()), metadata.size()), &json,
            &present);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_TRUE(present);
    EXPECT_NE(json.find("\"extra\":7"), std::string::npos);
    EXPECT_NE(json.find("\"metric\":1"), std::string::npos);
}

TEST(ParquetVariantReaderTest, NestedWrapperMergesEmptyResidualObjectAndTypedValue) {
    FieldSchema typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.lower_case_name = typed_value_field.name;
    typed_value_field.children = {make_int32_field_schema("metric")};
    typed_value_field.data_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {typed_value_field.children[0].data_type}, Strings {"metric"}));

    FieldSchema wrapper_field;
    wrapper_field.name = "element";
    wrapper_field.lower_case_name = wrapper_field.name;
    wrapper_field.children = {make_binary_field_schema("value", true), typed_value_field};
    wrapper_field.data_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {wrapper_field.children[0].data_type, typed_value_field.data_type},
            Strings {"value", "typed_value"}));

    auto metadata = make_metadata({});
    std::vector<uint8_t> residual_value {
            0x02, // object, 1-byte offsets, 1-byte field ids, 1-byte element count
            0x00, // zero fields
            0x00  // total field value size
    };

    Struct typed_value;
    typed_value.push_back(Field::create_field<TYPE_INT>(1));
    Struct row;
    row.push_back(Field::create_field<TYPE_STRING>(
            String(reinterpret_cast<const char*>(residual_value.data()), residual_value.size())));
    row.push_back(Field::create_field<TYPE_STRUCT>(typed_value));

    std::string json;
    bool present = false;
    Status st = parquet_variant_reader_test::variant_to_json_for_test(
            wrapper_field, Field::create_field<TYPE_STRUCT>(row),
            std::string(reinterpret_cast<const char*>(metadata.data()), metadata.size()), &json,
            &present);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_TRUE(present);
    EXPECT_EQ("{\"metric\":1}", json);
}

TEST(ParquetVariantReaderTest, NestedWrapperRejectsResidualTypedKeyCollision) {
    FieldSchema typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.lower_case_name = typed_value_field.name;
    typed_value_field.children = {make_int32_field_schema("metric")};
    typed_value_field.data_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {make_nullable(std::make_shared<DataTypeInt32>())}, Strings {"metric"}));

    FieldSchema wrapper_field;
    wrapper_field.name = "element";
    wrapper_field.lower_case_name = wrapper_field.name;
    wrapper_field.children = {make_binary_field_schema("value", true), typed_value_field};
    wrapper_field.data_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {wrapper_field.children[0].data_type, typed_value_field.data_type},
            Strings {"value", "typed_value"}));

    auto metadata = make_metadata({"metric"});
    std::vector<uint8_t> residual_value {
            0x02,       // object, 1-byte offsets, 1-byte field ids, 1-byte element count
            0x01,       // one field
            0x00,       // dictionary id 0: metric
            0x00, 0x02, // field value offsets
            0x0c, 0x02  // int8(2)
    };

    Struct typed_value;
    typed_value.push_back(Field::create_field<TYPE_INT>(1));
    Struct row;
    row.push_back(Field::create_field<TYPE_STRING>(
            String(reinterpret_cast<const char*>(residual_value.data()), residual_value.size())));
    row.push_back(Field::create_field<TYPE_STRUCT>(typed_value));

    std::string json;
    bool present = false;
    Status st = parquet_variant_reader_test::variant_to_json_for_test(
            wrapper_field, Field::create_field<TYPE_STRUCT>(row),
            std::string(reinterpret_cast<const char*>(metadata.data()), metadata.size()), &json,
            &present);
    EXPECT_TRUE(st.is<ErrorCode::CORRUPTION>()) << st.to_string();
}

TEST(ParquetVariantReaderTest, RowWiseRejectsResidualTypedKeyCollision) {
    FieldSchema typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.lower_case_name = typed_value_field.name;
    typed_value_field.children = {make_int32_field_schema("metric")};
    typed_value_field.data_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {make_nullable(std::make_shared<DataTypeInt32>())}, Strings {"metric"}));

    FieldSchema variant_field;
    variant_field.name = "v";
    variant_field.lower_case_name = variant_field.name;
    variant_field.data_type = make_nullable(std::make_shared<DataTypeVariant>(0, false));
    variant_field.children = {make_binary_field_schema("metadata", false),
                              make_binary_field_schema("value", true), typed_value_field};

    auto metadata = make_metadata({"metric"});
    std::vector<uint8_t> residual_value {
            0x02,       // object, 1-byte offsets, 1-byte field ids, 1-byte element count
            0x01,       // one field
            0x00,       // dictionary id 0: metric
            0x00, 0x02, // field value offsets
            0x0c, 0x02  // int8(2)
    };

    Struct typed_value;
    typed_value.push_back(Field::create_field<TYPE_INT>(1));
    Struct row;
    row.push_back(Field::create_field<TYPE_STRING>(
            String(reinterpret_cast<const char*>(metadata.data()), metadata.size())));
    row.push_back(Field::create_field<TYPE_STRING>(
            String(reinterpret_cast<const char*>(residual_value.data()), residual_value.size())));
    row.push_back(Field::create_field<TYPE_STRUCT>(typed_value));

    Field result;
    bool sql_null = false;
    Status st = parquet_variant_reader_test::read_variant_row_for_test(
            variant_field, Field::create_field<TYPE_STRUCT>(row), true, &result, &sql_null);
    EXPECT_TRUE(st.is<ErrorCode::CORRUPTION>()) << st.to_string();
}

TEST(ParquetVariantReaderTest, RowWisePreservesEmptyTypedObject) {
    FieldSchema typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.lower_case_name = typed_value_field.name;
    typed_value_field.children = {make_int32_field_schema("metric")};
    typed_value_field.data_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {typed_value_field.children[0].data_type}, Strings {"metric"}));

    FieldSchema variant_field;
    variant_field.name = "v";
    variant_field.lower_case_name = variant_field.name;
    variant_field.data_type = make_nullable(std::make_shared<DataTypeVariant>(0, false));
    variant_field.children = {make_binary_field_schema("metadata", false), typed_value_field};

    auto metadata = make_metadata({});
    Struct typed_value;
    typed_value.push_back(Field());
    Struct row;
    row.push_back(Field::create_field<TYPE_STRING>(
            String(reinterpret_cast<const char*>(metadata.data()), metadata.size())));
    row.push_back(Field::create_field<TYPE_STRUCT>(typed_value));

    Field result;
    bool sql_null = true;
    Status st = parquet_variant_reader_test::read_variant_row_for_test(
            variant_field, Field::create_field<TYPE_STRUCT>(row), true, &result, &sql_null);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_FALSE(sql_null);
    EXPECT_EQ("{}", serialize_variant_field(result));

    const auto& values = result.get<TYPE_VARIANT>();
    EXPECT_NE(values.find(PathInData()), values.end());
}

TEST(ParquetVariantReaderTest, RowWiseReadsRootTypedMapObject) {
    FieldSchema key_field = make_binary_field_schema("key", false);
    FieldSchema value_field = make_int32_field_schema("value");

    FieldSchema typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.lower_case_name = typed_value_field.name;
    typed_value_field.children = {key_field, value_field};
    typed_value_field.data_type = make_nullable(
            std::make_shared<DataTypeMap>(key_field.data_type, value_field.data_type));

    FieldSchema variant_field;
    variant_field.name = "v";
    variant_field.lower_case_name = variant_field.name;
    variant_field.data_type = make_nullable(std::make_shared<DataTypeVariant>(0, false));
    variant_field.children = {make_binary_field_schema("metadata", false), typed_value_field};

    Array keys;
    keys.push_back(Field::create_field<TYPE_STRING>(String("a")));
    keys.push_back(Field::create_field<TYPE_STRING>(String("b")));
    Array values;
    values.push_back(Field::create_field<TYPE_INT>(7));
    values.push_back(Field::create_field<TYPE_INT>(8));
    Map typed_map {Field::create_field<TYPE_ARRAY>(keys), Field::create_field<TYPE_ARRAY>(values)};

    auto metadata = make_metadata({});
    Struct row;
    row.push_back(Field::create_field<TYPE_STRING>(
            String(reinterpret_cast<const char*>(metadata.data()), metadata.size())));
    row.push_back(Field::create_field<TYPE_MAP>(typed_map));

    Field result;
    bool sql_null = true;
    Status st = parquet_variant_reader_test::read_variant_row_for_test(
            variant_field, Field::create_field<TYPE_STRUCT>(row), true, &result, &sql_null);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_FALSE(sql_null);
    EXPECT_EQ("{\"a\":7,\"b\":8}", serialize_variant_field(result));

    const auto& variant_values = result.get<TYPE_VARIANT>();
    EXPECT_EQ(variant_values.at(PathInData("a")).field.get<TYPE_INT>(), 7);
    EXPECT_EQ(variant_values.at(PathInData("b")).field.get<TYPE_INT>(), 8);
}

TEST(ParquetVariantReaderTest, RowWisePreservesEmptyResidualObject) {
    FieldSchema variant_field;
    variant_field.name = "v";
    variant_field.lower_case_name = variant_field.name;
    variant_field.data_type = make_nullable(std::make_shared<DataTypeVariant>(0, false));
    variant_field.children = {make_binary_field_schema("metadata", false),
                              make_binary_field_schema("value", true)};

    auto metadata = make_metadata({});
    std::vector<uint8_t> residual_value {
            0x02, // object, 1-byte offsets, 1-byte field ids, 1-byte element count
            0x00, // zero fields
            0x00  // total field value size
    };

    Struct row;
    row.push_back(Field::create_field<TYPE_STRING>(
            String(reinterpret_cast<const char*>(metadata.data()), metadata.size())));
    row.push_back(Field::create_field<TYPE_STRING>(
            String(reinterpret_cast<const char*>(residual_value.data()), residual_value.size())));

    Field result;
    bool sql_null = true;
    Status st = parquet_variant_reader_test::read_variant_row_for_test(
            variant_field, Field::create_field<TYPE_STRUCT>(row), true, &result, &sql_null);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_FALSE(sql_null);
    EXPECT_EQ("{}", serialize_variant_field(result));

    const auto& values = result.get<TYPE_VARIANT>();
    EXPECT_NE(values.find(PathInData()), values.end());
}

TEST(ParquetVariantReaderTest, RowWiseMergesEmptyResidualObjectAndTypedValue) {
    FieldSchema typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.lower_case_name = typed_value_field.name;
    typed_value_field.children = {make_int32_field_schema("metric")};
    typed_value_field.data_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {typed_value_field.children[0].data_type}, Strings {"metric"}));

    FieldSchema variant_field;
    variant_field.name = "v";
    variant_field.lower_case_name = variant_field.name;
    variant_field.data_type = make_nullable(std::make_shared<DataTypeVariant>(0, false));
    variant_field.children = {make_binary_field_schema("metadata", false),
                              make_binary_field_schema("value", true), typed_value_field};

    auto metadata = make_metadata({});
    std::vector<uint8_t> residual_value {
            0x02, // object, 1-byte offsets, 1-byte field ids, 1-byte element count
            0x00, // zero fields
            0x00  // total field value size
    };

    Struct typed_value;
    typed_value.push_back(Field::create_field<TYPE_INT>(1));
    Struct row;
    row.push_back(Field::create_field<TYPE_STRING>(
            String(reinterpret_cast<const char*>(metadata.data()), metadata.size())));
    row.push_back(Field::create_field<TYPE_STRING>(
            String(reinterpret_cast<const char*>(residual_value.data()), residual_value.size())));
    row.push_back(Field::create_field<TYPE_STRUCT>(typed_value));

    Field result;
    bool sql_null = true;
    Status st = parquet_variant_reader_test::read_variant_row_for_test(
            variant_field, Field::create_field<TYPE_STRUCT>(row), true, &result, &sql_null);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_FALSE(sql_null);
    EXPECT_EQ("{\"metric\":1}", serialize_variant_field(result));

    const auto& values = result.get<TYPE_VARIANT>();
    EXPECT_EQ(values.find(PathInData()), values.end());
    EXPECT_NE(values.find(PathInData("metric")), values.end());
}

TEST(ParquetVariantReaderTest, RowWiseMergesResidualObjectAndEmptyTypedValue) {
    FieldSchema typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.lower_case_name = typed_value_field.name;
    typed_value_field.children = {make_int32_field_schema("metric")};
    typed_value_field.data_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {typed_value_field.children[0].data_type}, Strings {"metric"}));

    FieldSchema variant_field;
    variant_field.name = "v";
    variant_field.lower_case_name = variant_field.name;
    variant_field.data_type = make_nullable(std::make_shared<DataTypeVariant>(0, false));
    variant_field.children = {make_binary_field_schema("metadata", false),
                              make_binary_field_schema("value", true), typed_value_field};

    auto metadata = make_metadata({"x"});
    std::vector<uint8_t> residual_value {
            0x02,       // object, 1-byte offsets, 1-byte field ids, 1-byte element count
            0x01,       // one field
            0x00,       // dictionary id 0: x
            0x00, 0x02, // field value offsets
            0x0c, 0x07  // int8(7)
    };

    Struct typed_value;
    typed_value.push_back(Field());
    Struct row;
    row.push_back(Field::create_field<TYPE_STRING>(
            String(reinterpret_cast<const char*>(metadata.data()), metadata.size())));
    row.push_back(Field::create_field<TYPE_STRING>(
            String(reinterpret_cast<const char*>(residual_value.data()), residual_value.size())));
    row.push_back(Field::create_field<TYPE_STRUCT>(typed_value));

    Field result;
    bool sql_null = true;
    Status st = parquet_variant_reader_test::read_variant_row_for_test(
            variant_field, Field::create_field<TYPE_STRUCT>(row), true, &result, &sql_null);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_FALSE(sql_null);
    EXPECT_EQ("{\"x\":7}", serialize_variant_field(result));

    const auto& values = result.get<TYPE_VARIANT>();
    EXPECT_EQ(values.find(PathInData()), values.end());
    EXPECT_NE(values.find(PathInData("x")), values.end());
}

TEST(ParquetVariantReaderTest, RowWiseMergesMatchingEmptyResidualAndTypedObjects) {
    FieldSchema metric_field;
    metric_field.name = "metric";
    metric_field.lower_case_name = metric_field.name;
    metric_field.children = {make_int32_field_schema("x")};
    metric_field.data_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {metric_field.children[0].data_type}, Strings {"x"}));

    FieldSchema typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.lower_case_name = typed_value_field.name;
    typed_value_field.children = {metric_field};
    typed_value_field.data_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {metric_field.data_type}, Strings {"metric"}));

    FieldSchema variant_field;
    variant_field.name = "v";
    variant_field.lower_case_name = variant_field.name;
    variant_field.data_type = make_nullable(std::make_shared<DataTypeVariant>(0, false));
    variant_field.children = {make_binary_field_schema("metadata", false),
                              make_binary_field_schema("value", true), typed_value_field};

    auto metadata = make_metadata({"metric"});
    std::vector<uint8_t> residual_value {
            0x02,            // object, 1-byte offsets, 1-byte field ids, 1-byte element count
            0x01,            // one field
            0x00,            // dictionary id 0: metric
            0x00, 0x03,      // field value offsets
            0x02, 0x00, 0x00 // metric: empty object
    };

    Struct metric;
    metric.push_back(Field());
    Struct typed_value;
    typed_value.push_back(Field::create_field<TYPE_STRUCT>(metric));
    Struct row;
    row.push_back(Field::create_field<TYPE_STRING>(
            String(reinterpret_cast<const char*>(metadata.data()), metadata.size())));
    row.push_back(Field::create_field<TYPE_STRING>(
            String(reinterpret_cast<const char*>(residual_value.data()), residual_value.size())));
    row.push_back(Field::create_field<TYPE_STRUCT>(typed_value));

    Field result;
    bool sql_null = true;
    Status st = parquet_variant_reader_test::read_variant_row_for_test(
            variant_field, Field::create_field<TYPE_STRUCT>(row), true, &result, &sql_null);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_FALSE(sql_null);
    EXPECT_EQ("{\"metric\":{}}", serialize_variant_field(result));

    const auto& values = result.get<TYPE_VARIANT>();
    EXPECT_NE(values.find(PathInData("metric")), values.end());
}

TEST(ParquetVariantReaderTest, RowWiseReadsValueOnlyNestedResidualField) {
    FieldSchema metric_field;
    metric_field.name = "metric";
    metric_field.lower_case_name = metric_field.name;
    metric_field.children = {make_binary_field_schema("value", true)};
    metric_field.data_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {metric_field.children[0].data_type}, Strings {"value"}));

    FieldSchema typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.lower_case_name = typed_value_field.name;
    typed_value_field.children = {metric_field};
    typed_value_field.data_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {metric_field.data_type}, Strings {"metric"}));

    FieldSchema variant_field;
    variant_field.name = "v";
    variant_field.lower_case_name = variant_field.name;
    variant_field.data_type = make_nullable(std::make_shared<DataTypeVariant>(0, false));
    variant_field.children = {make_binary_field_schema("metadata", false),
                              make_binary_field_schema("value", true), typed_value_field};

    auto metadata = make_metadata({"x"});
    std::vector<uint8_t> residual_value {
            0x02,       // object, 1-byte offsets, 1-byte field ids, 1-byte element count
            0x01,       // one field
            0x00,       // dictionary id 0: x
            0x00, 0x02, // field value offsets
            0x0c, 0x07  // int8(7)
    };

    Struct metric;
    metric.push_back(Field::create_field<TYPE_STRING>(
            String(reinterpret_cast<const char*>(residual_value.data()), residual_value.size())));
    Struct typed_value;
    typed_value.push_back(Field::create_field<TYPE_STRUCT>(metric));
    Struct row;
    row.push_back(Field::create_field<TYPE_STRING>(
            String(reinterpret_cast<const char*>(metadata.data()), metadata.size())));
    row.push_back(Field());
    row.push_back(Field::create_field<TYPE_STRUCT>(typed_value));

    Field result;
    bool sql_null = true;
    Status st = parquet_variant_reader_test::read_variant_row_for_test(
            variant_field, Field::create_field<TYPE_STRUCT>(row), true, &result, &sql_null);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_FALSE(sql_null);

    const auto& values = result.get<TYPE_VARIANT>();
    auto metric_x = values.find(PathInData("metric.x"));
    ASSERT_NE(metric_x, values.end());
    EXPECT_EQ(metric_x->second.field.get<TYPE_TINYINT>(), 7);
    EXPECT_EQ(values.find(PathInData("metric.value")), values.end());
}

TEST(ParquetVariantReaderTest, DecodeObjectWithOutOfOrderPhysicalValues) {
    auto metadata = make_metadata({"a", "b", "c"});
    std::vector<uint8_t> value {
            0x02,             // object, 1-byte offsets, 1-byte field ids, 1-byte element count
            0x03,             // three fields
            0x00, 0x01, 0x02, // dictionary ids: a, b, c
            0x04, 0x02, 0x00, 0x06, // field offsets in key order; values are c, b, a
            0x0c, 0x03,             // c: int8(3)
            0x0c, 0x02,             // b: int8(2)
            0x0c, 0x01              // a: int8(1)
    };

    std::string json;
    Status st = decode_variant_to_json(bytes_ref(metadata), bytes_ref(value), &json);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ("{\"a\":1,\"b\":2,\"c\":3}", json);
}

TEST(ParquetVariantReaderTest, RejectObjectChildTrailingBytes) {
    auto metadata = make_metadata({"a"});
    std::vector<uint8_t> value {
            0x02,            // object
            0x01,            // one field
            0x00,            // dictionary id 0
            0x00, 0x03,      // child is declared as 3 bytes
            0x0c, 0x07, 0x00 // int8(7) plus one trailing byte inside the child range
    };

    std::string json;
    Status st = decode_variant_to_json(bytes_ref(metadata), bytes_ref(value), &json);
    EXPECT_TRUE(st.is<ErrorCode::CORRUPTION>()) << st.to_string();
}

TEST(ParquetVariantReaderTest, RejectObjectDuplicatePhysicalOffsets) {
    auto metadata = make_metadata({"a", "b"});
    std::vector<uint8_t> value {
            0x02,             // object
            0x02,             // two fields
            0x00, 0x01,       // dictionary ids
            0x00, 0x00, 0x02, // both fields point at the same physical value
            0x0c, 0x07        // int8(7)
    };

    std::string json;
    Status st = decode_variant_to_json(bytes_ref(metadata), bytes_ref(value), &json);
    EXPECT_TRUE(st.is<ErrorCode::CORRUPTION>()) << st.to_string();
}

TEST(ParquetVariantReaderTest, RejectObjectDuplicateFieldIds) {
    auto metadata = make_metadata({"a"});
    std::vector<uint8_t> value {
            0x02,                  // object
            0x02,                  // two fields
            0x00, 0x00,            // duplicate dictionary id 0
            0x00, 0x02, 0x04,      // valid physical value offsets
            0x0c, 0x01, 0x0c, 0x02 // int8(1), int8(2)
    };

    std::string json;
    Status st = decode_variant_to_json(bytes_ref(metadata), bytes_ref(value), &json);
    EXPECT_TRUE(st.is<ErrorCode::CORRUPTION>()) << st.to_string();
}

TEST(ParquetVariantReaderTest, DecodeObjectWithLexicographicFieldOrderAndNonMonotonicIds) {
    auto metadata = make_metadata({"b", "a"});
    std::vector<uint8_t> value {
            0x02,                  // object
            0x02,                  // two fields
            0x01, 0x00,            // dictionary ids are sorted by field name: a, b
            0x00, 0x02, 0x04,      // valid physical value offsets
            0x0c, 0x01, 0x0c, 0x02 // int8(1), int8(2)
    };

    std::string json;
    Status st = decode_variant_to_json(bytes_ref(metadata), bytes_ref(value), &json);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ("{\"a\":1,\"b\":2}", json);
}

TEST(ParquetVariantReaderTest, RejectObjectOutOfOrderFieldNames) {
    auto metadata = make_metadata({"b", "a"});
    std::vector<uint8_t> value {
            0x02,                  // object
            0x02,                  // two fields
            0x00, 0x01,            // dictionary ids are not sorted by field name
            0x00, 0x02, 0x04,      // valid physical value offsets
            0x0c, 0x02, 0x0c, 0x01 // int8(2), int8(1)
    };

    std::string json;
    Status st = decode_variant_to_json(bytes_ref(metadata), bytes_ref(value), &json);
    EXPECT_TRUE(st.is<ErrorCode::CORRUPTION>()) << st.to_string();
}

TEST(ParquetVariantReaderTest, RejectArrayChildTrailingBytes) {
    auto metadata = make_metadata({});
    std::vector<uint8_t> value {
            0x03,            // array, 1-byte offsets, 1-byte element count
            0x01,            // one element
            0x00, 0x03,      // element is declared as 3 bytes
            0x0c, 0x07, 0x00 // int8(7) plus one trailing byte inside the element range
    };

    std::string json;
    Status st = decode_variant_to_json(bytes_ref(metadata), bytes_ref(value), &json);
    EXPECT_TRUE(st.is<ErrorCode::CORRUPTION>()) << st.to_string();
}

TEST(ParquetVariantReaderTest, RejectOversizedPrimitiveLength) {
    auto metadata = make_metadata({});
    std::vector<uint8_t> value {
            0x40,                  // primitive string
            0xff, 0xff, 0xff, 0xff // length exceeds the remaining buffer
    };

    std::string json;
    Status st = decode_variant_to_json(bytes_ref(metadata), bytes_ref(value), &json);
    EXPECT_TRUE(st.is<ErrorCode::CORRUPTION>()) << st.to_string();
}

} // namespace doris::parquet
