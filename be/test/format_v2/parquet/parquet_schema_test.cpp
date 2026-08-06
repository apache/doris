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

#include <limits>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "core/assert_cast.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/data_type/primitive_type.h"
#include "format_v2/parquet/native_schema_desc.h"
#include "format_v2/parquet/native_schema_node.h"
#include "format_v2/parquet/parquet_column_schema.h"
#include "format_v2/parquet/parquet_file_context.h"

namespace doris::format::parquet {
namespace {

std::vector<tparquet::SchemaElement> unshredded_variant_schema(
        std::optional<int8_t> specification_version = 1) {
    tparquet::SchemaElement root;
    root.__set_name("schema");
    root.__set_num_children(1);

    tparquet::SchemaElement variant;
    variant.__set_name("payload");
    variant.__set_num_children(2);
    variant.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    variant.__set_logicalType(tparquet::LogicalType());
    variant.logicalType.__set_VARIANT(tparquet::VariantType());
    if (specification_version.has_value()) {
        variant.logicalType.VARIANT.__set_specification_version(*specification_version);
    }

    tparquet::SchemaElement metadata;
    metadata.__set_name("metadata");
    metadata.__set_type(tparquet::Type::BYTE_ARRAY);
    metadata.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);

    tparquet::SchemaElement value;
    value.__set_name("value");
    value.__set_type(tparquet::Type::BYTE_ARRAY);
    value.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);
    return {root, variant, metadata, value};
}

std::vector<tparquet::SchemaElement> struct_with_variant_schema() {
    auto variant_fields = unshredded_variant_schema();
    variant_fields[0].__set_name("info");
    variant_fields[0].__set_num_children(2);
    variant_fields[0].__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    variant_fields[1].__set_name("payload");

    tparquet::SchemaElement root;
    root.__set_name("schema");
    root.__set_num_children(1);
    tparquet::SchemaElement label;
    label.__set_name("label");
    label.__set_type(tparquet::Type::BYTE_ARRAY);
    label.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    return {root,
            variant_fields[0],
            label,
            variant_fields[1],
            variant_fields[2],
            variant_fields[3]};
}

std::vector<tparquet::SchemaElement> shredded_object_variant_schema(bool signed_integer = true,
                                                                    bool required_wrapper = true) {
    auto schema = unshredded_variant_schema();
    schema[1].__set_num_children(3);
    schema[3].__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);

    tparquet::SchemaElement typed_object;
    typed_object.__set_name("typed_value");
    typed_object.__set_num_children(1);
    typed_object.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    tparquet::SchemaElement field_wrapper;
    field_wrapper.__set_name("field");
    field_wrapper.__set_num_children(2);
    field_wrapper.__set_repetition_type(required_wrapper ? tparquet::FieldRepetitionType::REQUIRED
                                                         : tparquet::FieldRepetitionType::OPTIONAL);
    tparquet::SchemaElement fallback;
    fallback.__set_name("value");
    fallback.__set_type(tparquet::Type::BYTE_ARRAY);
    fallback.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    tparquet::SchemaElement typed_integer;
    typed_integer.__set_name("typed_value");
    typed_integer.__set_type(tparquet::Type::INT32);
    typed_integer.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    if (!signed_integer) {
        typed_integer.__set_logicalType(tparquet::LogicalType());
        typed_integer.logicalType.__set_INTEGER(tparquet::IntType());
        typed_integer.logicalType.INTEGER.__set_bitWidth(32);
        typed_integer.logicalType.INTEGER.__set_isSigned(false);
    }
    schema.insert(schema.end(), {typed_object, field_wrapper, fallback, typed_integer});
    return schema;
}

std::vector<tparquet::SchemaElement> shredded_primitive_variant_schema(
        tparquet::SchemaElement typed_value) {
    auto schema = unshredded_variant_schema();
    schema[1].__set_num_children(3);
    schema[3].__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    typed_value.__set_name("typed_value");
    typed_value.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    schema.push_back(std::move(typed_value));
    return schema;
}

std::vector<tparquet::SchemaElement> shredded_time_variant_schema(bool adjusted_to_utc,
                                                                  bool millis) {
    auto schema = unshredded_variant_schema();
    schema[1].__set_num_children(3);
    schema[3].__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);

    tparquet::SchemaElement typed_value;
    typed_value.__set_name("typed_value");
    typed_value.__set_type(millis ? tparquet::Type::INT32 : tparquet::Type::INT64);
    typed_value.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    typed_value.__set_logicalType(tparquet::LogicalType());
    typed_value.logicalType.__set_TIME(tparquet::TimeType());
    typed_value.logicalType.TIME.__set_isAdjustedToUTC(adjusted_to_utc);
    typed_value.logicalType.TIME.__set_unit(tparquet::TimeUnit());
    if (millis) {
        typed_value.logicalType.TIME.unit.__set_MILLIS(tparquet::MilliSeconds());
    } else {
        typed_value.logicalType.TIME.unit.__set_MICROS(tparquet::MicroSeconds());
    }
    schema.push_back(std::move(typed_value));
    return schema;
}

std::vector<tparquet::SchemaElement> shredded_array_variant_schema(bool include_value,
                                                                   bool include_typed_value) {
    auto schema = unshredded_variant_schema();
    schema[1].__set_num_children(3);
    schema[3].__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);

    tparquet::SchemaElement typed_array;
    typed_array.__set_name("typed_value");
    typed_array.__set_num_children(1);
    typed_array.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    typed_array.__set_converted_type(tparquet::ConvertedType::LIST);
    tparquet::SchemaElement list;
    list.__set_name("list");
    list.__set_num_children(1);
    list.__set_repetition_type(tparquet::FieldRepetitionType::REPEATED);
    tparquet::SchemaElement element;
    element.__set_name("element");
    element.__set_num_children(static_cast<int32_t>(include_value) +
                               static_cast<int32_t>(include_typed_value));
    element.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);
    schema.insert(schema.end(), {typed_array, list, element});
    if (include_value) {
        tparquet::SchemaElement value;
        value.__set_name("value");
        value.__set_type(tparquet::Type::BYTE_ARRAY);
        value.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
        schema.push_back(std::move(value));
    }
    if (include_typed_value) {
        tparquet::SchemaElement typed_value;
        typed_value.__set_name("typed_value");
        typed_value.__set_type(tparquet::Type::INT32);
        typed_value.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
        schema.push_back(std::move(typed_value));
    }
    return schema;
}

} // namespace

TEST(ParquetSchemaTest, NativeSchemaRecognizesVariantLogicalGroup) {
    for (const auto version : {std::optional<int8_t> {}, std::optional<int8_t> {1}}) {
        NativeFieldDescriptor descriptor;
        ASSERT_TRUE(descriptor.parse_from_thrift(unshredded_variant_schema(version)).ok());
        descriptor.assign_ids();

        const auto* native_variant = descriptor.get_column(0);
        ASSERT_NE(native_variant, nullptr);
        // Native readers must keep seeing the physical group. The logical Variant mapping belongs
        // to ParquetColumnSchema and must not make this group look like an unindexed scalar leaf.
        EXPECT_EQ(remove_nullable(native_variant->data_type)->get_primitive_type(), TYPE_STRUCT);
        ASSERT_EQ(native_variant->children.size(), 2);
        EXPECT_EQ(native_variant->children[0].physical_column_index, 0);
        EXPECT_EQ(native_variant->children[1].physical_column_index, 1);
        EXPECT_EQ(descriptor.physical_fields_size(), 2);

        std::vector<std::unique_ptr<ParquetColumnSchema>> fields;
        const auto status = build_parquet_column_schema(descriptor, &fields);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(fields.size(), 1);
        EXPECT_EQ(fields[0]->kind, ParquetColumnSchemaKind::VARIANT);
        EXPECT_TRUE(fields[0]->contains_variant);
        EXPECT_EQ(remove_nullable(fields[0]->type)->get_primitive_type(), TYPE_VARIANT);
        EXPECT_NE(typeid_cast<const DataTypeVariantV2*>(remove_nullable(fields[0]->type).get()),
                  nullptr);
        ASSERT_EQ(fields[0]->children.size(), 2);
        EXPECT_EQ(fields[0]->children[0]->name, "metadata");
        EXPECT_EQ(fields[0]->children[1]->name, "value");
    }
}

TEST(ParquetSchemaTest, AppliesTableFormatVariantOverrideToUnannotatedGroup) {
    auto schema = unshredded_variant_schema();
    schema[1].__isset.logicalType = false;
    NativeFieldDescriptor descriptor;
    ASSERT_TRUE(descriptor.parse_from_thrift(schema).ok());

    std::vector<std::unique_ptr<ParquetColumnSchema>> fields;
    ASSERT_TRUE(build_parquet_column_schema(descriptor, &fields).ok());
    ASSERT_EQ(fields.size(), 1);
    ASSERT_EQ(fields[0]->kind, ParquetColumnSchemaKind::STRUCT);

    const std::vector overrides {format::LocalColumnIndex::top_level(format::LocalColumnId(0))};
    const auto status = apply_variant_schema_overrides(descriptor, overrides, &fields);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(fields[0]->kind, ParquetColumnSchemaKind::VARIANT);
    EXPECT_TRUE(fields[0]->contains_variant);
    EXPECT_EQ(remove_nullable(fields[0]->type)->get_primitive_type(), TYPE_VARIANT);
    EXPECT_NE(fields[0]->variant_physical_type, nullptr);
}

TEST(ParquetSchemaTest, AppliesPaimonShreddedVariantOverrideWithOptionalMetadata) {
    auto schema = shredded_object_variant_schema();
    schema[1].__isset.logicalType = false;
    schema[2].__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    NativeFieldDescriptor descriptor;
    ASSERT_TRUE(descriptor.parse_from_thrift(schema).ok());

    std::vector<std::unique_ptr<ParquetColumnSchema>> fields;
    ASSERT_TRUE(build_parquet_column_schema(descriptor, &fields).ok());
    const std::vector overrides {format::LocalColumnIndex::top_level(format::LocalColumnId(0))};
    const auto status = apply_variant_schema_overrides(descriptor, overrides, &fields);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(fields[0]->kind, ParquetColumnSchemaKind::VARIANT);
}

TEST(ParquetSchemaTest, RejectsMalformedUnannotatedVariantOverride) {
    auto schema = unshredded_variant_schema();
    schema[1].__isset.logicalType = false;
    schema[2].__set_name("unexpected");
    NativeFieldDescriptor descriptor;
    ASSERT_TRUE(descriptor.parse_from_thrift(schema).ok());

    std::vector<std::unique_ptr<ParquetColumnSchema>> fields;
    ASSERT_TRUE(build_parquet_column_schema(descriptor, &fields).ok());
    const std::vector overrides {format::LocalColumnIndex::top_level(format::LocalColumnId(0))};
    const auto status = apply_variant_schema_overrides(descriptor, overrides, &fields);
    EXPECT_TRUE(status.is<ErrorCode::CORRUPTION>()) << status;
    EXPECT_NE(status.to_string().find("unexpected child"), std::string::npos);
}

TEST(ParquetSchemaTest, RejectsOptionalMetadataOutsidePaimonShreddedOverride) {
    auto annotated_shredded = shredded_object_variant_schema();
    annotated_shredded[2].__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    NativeFieldDescriptor descriptor;
    const auto annotated_status = descriptor.parse_from_thrift(annotated_shredded);
    EXPECT_TRUE(annotated_status.is<ErrorCode::CORRUPTION>()) << annotated_status;

    auto unannotated_unshredded = unshredded_variant_schema();
    unannotated_unshredded[1].__isset.logicalType = false;
    unannotated_unshredded[2].__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    ASSERT_TRUE(descriptor.parse_from_thrift(unannotated_unshredded).ok());
    std::vector<std::unique_ptr<ParquetColumnSchema>> fields;
    ASSERT_TRUE(build_parquet_column_schema(descriptor, &fields).ok());
    const std::vector overrides {format::LocalColumnIndex::top_level(format::LocalColumnId(0))};
    const auto override_status = apply_variant_schema_overrides(descriptor, overrides, &fields);
    EXPECT_TRUE(override_status.is<ErrorCode::CORRUPTION>()) << override_status;
}

TEST(ParquetSchemaTest, AppliesNestedTableFormatVariantOverride) {
    auto schema = struct_with_variant_schema();
    schema[3].__isset.logicalType = false;
    NativeFieldDescriptor descriptor;
    ASSERT_TRUE(descriptor.parse_from_thrift(schema).ok());

    std::vector<std::unique_ptr<ParquetColumnSchema>> fields;
    ASSERT_TRUE(build_parquet_column_schema(descriptor, &fields).ok());
    ASSERT_EQ(fields.size(), 1);
    ASSERT_EQ(fields[0]->kind, ParquetColumnSchemaKind::STRUCT);
    ASSERT_EQ(fields[0]->children.size(), 2);
    ASSERT_EQ(fields[0]->children[1]->kind, ParquetColumnSchemaKind::STRUCT);

    auto root_override = format::LocalColumnIndex::partial_local(0);
    root_override.children.push_back(format::LocalColumnIndex::local(1));
    const auto status = apply_variant_schema_overrides(descriptor, {root_override}, &fields);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_TRUE(fields[0]->contains_variant);
    EXPECT_EQ(fields[0]->children[1]->kind, ParquetColumnSchemaKind::VARIANT);
    const auto& struct_type = assert_cast<const DataTypeStruct&>(*remove_nullable(fields[0]->type));
    EXPECT_EQ(remove_nullable(struct_type.get_element(1))->get_primitive_type(), TYPE_VARIANT);
}

TEST(ParquetSchemaTest, NativeSchemaAcceptsRequiredAndOptionalVariantGroups) {
    for (const auto repetition :
         {tparquet::FieldRepetitionType::REQUIRED, tparquet::FieldRepetitionType::OPTIONAL}) {
        auto schema = unshredded_variant_schema();
        schema[1].__set_repetition_type(repetition);
        NativeFieldDescriptor descriptor;
        const auto status = descriptor.parse_from_thrift(schema);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_NE(descriptor.get_column(0), nullptr);
    }
}

TEST(ParquetSchemaTest, NestedVariantPropagatesIntoParentLogicalType) {
    NativeFieldDescriptor descriptor;
    ASSERT_TRUE(descriptor.parse_from_thrift(struct_with_variant_schema()).ok());
    descriptor.assign_ids();

    std::vector<std::unique_ptr<ParquetColumnSchema>> fields;
    const auto status = build_parquet_column_schema(descriptor, &fields);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(fields.size(), 1);
    EXPECT_TRUE(fields[0]->contains_variant);
    ASSERT_EQ(fields[0]->children.size(), 2);
    EXPECT_FALSE(fields[0]->children[0]->contains_variant);
    EXPECT_TRUE(fields[0]->children[1]->contains_variant);
    const auto* info_type =
            assert_cast<const DataTypeStruct*>(remove_nullable(fields[0]->type).get());
    ASSERT_EQ(info_type->get_elements().size(), 2);
    EXPECT_EQ(remove_nullable(info_type->get_elements()[1])->get_primitive_type(), TYPE_VARIANT);
}

TEST(ParquetSchemaTest, NativeSchemaRejectsUnsupportedVariantVersionAndMalformedLayout) {
    NativeFieldDescriptor descriptor;
    const auto version_status = descriptor.parse_from_thrift(unshredded_variant_schema(2));
    EXPECT_TRUE(version_status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << version_status;
    EXPECT_NE(version_status.to_string().find("Variant specification version 2"),
              std::string::npos);

    auto missing_metadata = unshredded_variant_schema();
    missing_metadata[2].__set_name("not_metadata");
    const auto layout_status = descriptor.parse_from_thrift(missing_metadata);
    EXPECT_TRUE(layout_status.is<ErrorCode::CORRUPTION>()) << layout_status;
    EXPECT_NE(layout_status.to_string().find("metadata"), std::string::npos);

    auto optional_unshredded_value = unshredded_variant_schema();
    optional_unshredded_value[3].__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    const auto repetition_status = descriptor.parse_from_thrift(optional_unshredded_value);
    EXPECT_TRUE(repetition_status.is<ErrorCode::CORRUPTION>()) << repetition_status;
    EXPECT_NE(repetition_status.to_string().find("required BYTE_ARRAY"), std::string::npos);
}

TEST(ParquetSchemaTest, NativeVariantPreservesUtcTimestampInstant) {
    auto schema = unshredded_variant_schema();
    schema[1].__set_num_children(3);
    schema[3].__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    tparquet::SchemaElement typed_value;
    typed_value.__set_name("typed_value");
    typed_value.__set_type(tparquet::Type::INT64);
    typed_value.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    typed_value.__set_logicalType(tparquet::LogicalType());
    typed_value.logicalType.__set_TIMESTAMP(tparquet::TimestampType());
    typed_value.logicalType.TIMESTAMP.__set_isAdjustedToUTC(true);
    typed_value.logicalType.TIMESTAMP.__set_unit(tparquet::TimeUnit());
    typed_value.logicalType.TIMESTAMP.unit.__set_MICROS(tparquet::MicroSeconds());
    schema.push_back(std::move(typed_value));

    NativeFieldDescriptor descriptor;
    ASSERT_TRUE(descriptor.parse_from_thrift(schema).ok());
    const auto* variant = descriptor.get_column(0);
    ASSERT_EQ(variant->children.size(), 3);
    EXPECT_EQ(remove_nullable(variant->children[2].data_type)->get_primitive_type(),
              TYPE_TIMESTAMPTZ);
}

TEST(ParquetSchemaTest, NativeVariantValidatesEveryShreddedWrapperAndScalar) {
    NativeFieldDescriptor descriptor;
    const auto unsigned_status =
            descriptor.parse_from_thrift(shredded_object_variant_schema(false, true));
    EXPECT_TRUE(unsigned_status.is<ErrorCode::CORRUPTION>()) << unsigned_status;
    EXPECT_NE(unsigned_status.to_string().find("unsigned"), std::string::npos);

    const auto optional_wrapper_status =
            descriptor.parse_from_thrift(shredded_object_variant_schema(true, false));
    EXPECT_TRUE(optional_wrapper_status.is<ErrorCode::CORRUPTION>()) << optional_wrapper_status;
    EXPECT_NE(optional_wrapper_status.to_string().find("wrapper"), std::string::npos);
}

TEST(ParquetSchemaTest, NativeVariantRejectsDuplicateObjectFieldNames) {
    auto schema = shredded_object_variant_schema();
    schema[4].__set_num_children(2);
    schema.insert(schema.end(), {schema[5], schema[6], schema[7]});

    NativeFieldDescriptor descriptor;
    const auto status = descriptor.parse_from_thrift(schema);
    EXPECT_TRUE(status.is<ErrorCode::CORRUPTION>()) << status;
    EXPECT_NE(status.to_string().find("duplicate"), std::string::npos);
}

TEST(ParquetSchemaTest, NativeVariantRejectsUnsupportedPrimitiveTypePairs) {
    std::vector<tparquet::SchemaElement> invalid_typed_values;

    tparquet::SchemaElement int96;
    int96.__set_type(tparquet::Type::INT96);
    invalid_typed_values.push_back(int96);

    tparquet::SchemaElement fixed_binary;
    fixed_binary.__set_type(tparquet::Type::FIXED_LEN_BYTE_ARRAY);
    fixed_binary.__set_type_length(16);
    invalid_typed_values.push_back(fixed_binary);

    tparquet::SchemaElement json;
    json.__set_type(tparquet::Type::BYTE_ARRAY);
    json.__set_logicalType(tparquet::LogicalType());
    json.logicalType.__set_JSON(tparquet::JsonType());
    invalid_typed_values.push_back(json);

    tparquet::SchemaElement float16;
    float16.__set_type(tparquet::Type::FIXED_LEN_BYTE_ARRAY);
    float16.__set_type_length(2);
    float16.__set_logicalType(tparquet::LogicalType());
    float16.logicalType.__set_FLOAT16(tparquet::Float16Type());
    invalid_typed_values.push_back(float16);

    tparquet::SchemaElement mismatched_integer;
    mismatched_integer.__set_type(tparquet::Type::INT64);
    mismatched_integer.__set_logicalType(tparquet::LogicalType());
    mismatched_integer.logicalType.__set_INTEGER(tparquet::IntType());
    mismatched_integer.logicalType.INTEGER.__set_bitWidth(16);
    mismatched_integer.logicalType.INTEGER.__set_isSigned(true);
    invalid_typed_values.push_back(mismatched_integer);

    tparquet::SchemaElement mismatched_int32_annotation;
    mismatched_int32_annotation.__set_type(tparquet::Type::INT64);
    mismatched_int32_annotation.__set_logicalType(tparquet::LogicalType());
    mismatched_int32_annotation.logicalType.__set_INTEGER(tparquet::IntType());
    mismatched_int32_annotation.logicalType.INTEGER.__set_bitWidth(32);
    mismatched_int32_annotation.logicalType.INTEGER.__set_isSigned(true);
    mismatched_int32_annotation.__set_converted_type(tparquet::ConvertedType::INT_32);
    invalid_typed_values.push_back(mismatched_int32_annotation);

    tparquet::SchemaElement mismatched_int64_annotation;
    mismatched_int64_annotation.__set_type(tparquet::Type::INT32);
    mismatched_int64_annotation.__set_logicalType(tparquet::LogicalType());
    mismatched_int64_annotation.logicalType.__set_INTEGER(tparquet::IntType());
    mismatched_int64_annotation.logicalType.INTEGER.__set_bitWidth(64);
    mismatched_int64_annotation.logicalType.INTEGER.__set_isSigned(true);
    mismatched_int64_annotation.__set_converted_type(tparquet::ConvertedType::INT_64);
    invalid_typed_values.push_back(mismatched_int64_annotation);

    tparquet::SchemaElement mismatched_decimal;
    mismatched_decimal.__set_type(tparquet::Type::INT32);
    mismatched_decimal.__set_logicalType(tparquet::LogicalType());
    mismatched_decimal.logicalType.__set_DECIMAL(tparquet::DecimalType());
    mismatched_decimal.logicalType.DECIMAL.__set_precision(10);
    mismatched_decimal.logicalType.DECIMAL.__set_scale(2);
    invalid_typed_values.push_back(mismatched_decimal);

    tparquet::SchemaElement bad_uuid;
    bad_uuid.__set_type(tparquet::Type::FIXED_LEN_BYTE_ARRAY);
    bad_uuid.__set_type_length(15);
    bad_uuid.__set_logicalType(tparquet::LogicalType());
    bad_uuid.logicalType.__set_UUID(tparquet::UUIDType());
    invalid_typed_values.push_back(bad_uuid);

    for (auto& typed_value : invalid_typed_values) {
        NativeFieldDescriptor descriptor;
        const auto status = descriptor.parse_from_thrift(
                shredded_primitive_variant_schema(std::move(typed_value)));
        EXPECT_FALSE(status.ok()) << "unsupported Variant typed_value pair was accepted";
    }
}

TEST(ParquetSchemaTest, NativeVariantAcceptsIcebergFullWidthSignedIntegerAnnotations) {
    for (const auto [physical_type, bit_width] : {std::pair {tparquet::Type::INT32, int8_t {32}},
                                                  std::pair {tparquet::Type::INT64, int8_t {64}}}) {
        tparquet::SchemaElement typed_value;
        typed_value.__set_type(physical_type);
        typed_value.__set_logicalType(tparquet::LogicalType());
        typed_value.logicalType.__set_INTEGER(tparquet::IntType());
        typed_value.logicalType.INTEGER.__set_bitWidth(bit_width);
        typed_value.logicalType.INTEGER.__set_isSigned(true);
        typed_value.__set_converted_type(bit_width == 32 ? tparquet::ConvertedType::INT_32
                                                         : tparquet::ConvertedType::INT_64);

        NativeFieldDescriptor descriptor;
        const auto status = descriptor.parse_from_thrift(
                shredded_primitive_variant_schema(std::move(typed_value)));
        EXPECT_TRUE(status.ok()) << status;
    }
}

TEST(ParquetSchemaTest, NativeVariantRejectsRepeatedOuterGroup) {
    auto schema = unshredded_variant_schema();
    schema[1].__set_repetition_type(tparquet::FieldRepetitionType::REPEATED);

    NativeFieldDescriptor descriptor;
    const auto status = descriptor.parse_from_thrift(schema);
    EXPECT_TRUE(status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << status;
    EXPECT_NE(status.to_string().find("repeated"), std::string::npos);
}

TEST(ParquetSchemaTest, NativeVariantAcceptsOmittedShreddedWrapperChildren) {
    NativeFieldDescriptor descriptor;

    auto value_only_object = shredded_object_variant_schema();
    value_only_object[5].__set_num_children(1);
    value_only_object.pop_back();
    ASSERT_TRUE(descriptor.parse_from_thrift(value_only_object).ok());

    ASSERT_TRUE(descriptor.parse_from_thrift(shredded_array_variant_schema(true, false)).ok());
    ASSERT_TRUE(descriptor.parse_from_thrift(shredded_array_variant_schema(false, true)).ok());
}

TEST(ParquetSchemaTest, NativeVariantRejectsNanosBeforeProjectionChoice) {
    auto schema = unshredded_variant_schema();
    schema[1].__set_num_children(3);
    schema[3].__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    tparquet::SchemaElement typed_value;
    typed_value.__set_name("typed_value");
    typed_value.__set_type(tparquet::Type::INT64);
    typed_value.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    typed_value.__set_logicalType(tparquet::LogicalType());
    typed_value.logicalType.__set_TIMESTAMP(tparquet::TimestampType());
    typed_value.logicalType.TIMESTAMP.__set_isAdjustedToUTC(false);
    typed_value.logicalType.TIMESTAMP.__set_unit(tparquet::TimeUnit());
    typed_value.logicalType.TIMESTAMP.unit.__set_NANOS(tparquet::NanoSeconds());
    schema.push_back(std::move(typed_value));

    NativeFieldDescriptor descriptor;
    const auto status = descriptor.parse_from_thrift(schema);
    EXPECT_TRUE(status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << status;
    EXPECT_NE(status.to_string().find("TIMESTAMP(NANOS)"), std::string::npos);
}

TEST(ParquetSchemaTest, NativeVariantRejectsUnsupportedTimeAnnotations) {
    NativeFieldDescriptor descriptor;
    const auto adjusted_status =
            descriptor.parse_from_thrift(shredded_time_variant_schema(true, false));
    EXPECT_TRUE(adjusted_status.is<ErrorCode::CORRUPTION>()) << adjusted_status;
    EXPECT_NE(adjusted_status.to_string().find("isAdjustedToUTC"), std::string::npos);

    const auto millis_status =
            descriptor.parse_from_thrift(shredded_time_variant_schema(false, true));
    EXPECT_TRUE(millis_status.is<ErrorCode::CORRUPTION>()) << millis_status;
    EXPECT_NE(millis_status.to_string().find("TIME(MILLIS)"), std::string::npos);
}

TEST(ParquetSchemaTest, NativeMetadataAcceptsRequiredRootWithoutColumns) {
    tparquet::SchemaElement root;
    root.__set_name("schema");
    root.__set_num_children(0);
    root.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);

    NativeFieldDescriptor descriptor;
    ASSERT_TRUE(descriptor.parse_from_thrift({root}).ok());
    EXPECT_EQ(descriptor.size(), 0);

    tparquet::FileMetaData thrift_metadata;
    thrift_metadata.__set_version(1);
    thrift_metadata.__set_schema({root});
    thrift_metadata.__set_num_rows(0);
    NativeParquetMetadata metadata(std::move(thrift_metadata), 0);
    // An empty physical tree remains useful for metadata-only COUNT(*); rejecting it here changes
    // the compatibility contract before request planning can select that path.
    EXPECT_TRUE(metadata.init_schema(false, false).ok());
    EXPECT_EQ(metadata.schema().size(), 0);
}

TEST(ParquetSchemaTest, NativeMetadataTreePreservesNestedFieldNamesAndIds) {
    tparquet::SchemaElement root;
    root.__set_name("schema");
    root.__set_num_children(1);

    tparquet::SchemaElement protocol;
    protocol.__set_name("protocol");
    protocol.__set_num_children(2);
    protocol.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    protocol.__set_field_id(10);

    tparquet::SchemaElement min_reader;
    min_reader.__set_name("minReaderVersion");
    min_reader.__set_type(tparquet::Type::INT32);
    min_reader.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    min_reader.__set_field_id(11);

    tparquet::SchemaElement min_writer = min_reader;
    min_writer.__set_name("minWriterVersion");
    min_writer.__set_field_id(12);

    NativeFieldDescriptor native_schema;
    ASSERT_TRUE(native_schema.parse_from_thrift({root, protocol, min_reader, min_writer}).ok());
    native_schema.assign_ids();

    std::vector<std::unique_ptr<ParquetColumnSchema>> fields;
    ASSERT_TRUE(build_parquet_column_schema(native_schema, &fields).ok());
    ASSERT_EQ(fields.size(), 1);
    EXPECT_FALSE(fields[0]->contains_variant);
    EXPECT_EQ(fields[0]->name, "protocol");
    EXPECT_EQ(fields[0]->parquet_field_id, 10);
    ASSERT_EQ(fields[0]->children.size(), 2);
    EXPECT_FALSE(fields[0]->children[0]->contains_variant);
    EXPECT_FALSE(fields[0]->children[1]->contains_variant);
    EXPECT_EQ(fields[0]->children[0]->name, "minReaderVersion");
    EXPECT_EQ(fields[0]->children[0]->leaf_column_id, 0);
    EXPECT_EQ(fields[0]->children[1]->name, "minWriterVersion");
    EXPECT_EQ(fields[0]->children[1]->leaf_column_id, 1);

    std::shared_ptr<NativeSchemaNode> mapping;
    ASSERT_TRUE(build_native_schema_node(fields[0]->type, *fields[0], &mapping).ok());
    EXPECT_TRUE(mapping->has_child("minReaderVersion"));
    EXPECT_EQ(mapping->file_child_name("minReaderVersion"), "minReaderVersion");
    ASSERT_NE(mapping->child("minReaderVersion"), nullptr);
}
TEST(ParquetSchemaTest, NativeLogicalUtcTimeIsDeferredToProjectionValidation) {
    tparquet::SchemaElement root;
    root.__set_name("schema");
    root.__set_num_children(1);

    tparquet::SchemaElement adjusted_time;
    adjusted_time.__set_name("time_ms");
    adjusted_time.__set_type(tparquet::Type::INT32);
    adjusted_time.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    adjusted_time.__set_logicalType(tparquet::LogicalType());
    adjusted_time.logicalType.__set_TIME(tparquet::TimeType());
    adjusted_time.logicalType.TIME.__set_isAdjustedToUTC(true);
    adjusted_time.logicalType.TIME.__set_unit(tparquet::TimeUnit());
    adjusted_time.logicalType.TIME.unit.__set_MILLIS(tparquet::MilliSeconds());

    NativeFieldDescriptor native_schema;
    ASSERT_TRUE(native_schema.parse_from_thrift({root, adjusted_time}).ok());
    native_schema.assign_ids();
    std::vector<std::unique_ptr<ParquetColumnSchema>> fields;
    const auto status = build_parquet_column_schema(native_schema, &fields);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(fields.size(), 1);
    // Preserve the unsupported marker in metadata; request-level validation decides whether the
    // leaf is a real projection or an ignorable COUNT(*) placeholder.
    EXPECT_EQ(fields[0]->type_descriptor.unsupported_reason,
              "Parquet TIME with isAdjustedToUTC=true is not supported");
}

TEST(ParquetSchemaTest, NativeOversizedByteArrayDecimalIsDeferredToProjectionValidation) {
    tparquet::SchemaElement root;
    root.__set_name("schema");
    root.__set_num_children(1);

    tparquet::SchemaElement decimal;
    decimal.__set_name("decimal_77");
    decimal.__set_type(tparquet::Type::BYTE_ARRAY);
    decimal.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    decimal.__set_logicalType(tparquet::LogicalType());
    decimal.logicalType.__set_DECIMAL(tparquet::DecimalType());
    decimal.logicalType.DECIMAL.__set_precision(77);
    decimal.logicalType.DECIMAL.__set_scale(2);

    NativeFieldDescriptor native_schema;
    ASSERT_TRUE(native_schema.parse_from_thrift({root, decimal}).ok());
    native_schema.assign_ids();
    std::vector<std::unique_ptr<ParquetColumnSchema>> fields;
    ASSERT_TRUE(build_parquet_column_schema(native_schema, &fields).ok());
    ASSERT_EQ(fields.size(), 1);
    EXPECT_TRUE(fields[0]->type_descriptor.is_decimal);
    EXPECT_FALSE(fields[0]->type_descriptor.unsupported_reason.empty());
    EXPECT_NE(fields[0]->type_descriptor.unsupported_reason.find("precision 77"),
              std::string::npos);
}

TEST(ParquetSchemaTest, NativeUnknownLogicalTypeRetainsPhysicalFallback) {
    tparquet::SchemaElement root;
    root.__set_name("schema");
    root.__set_num_children(1);

    tparquet::SchemaElement interval;
    interval.__set_name("duration");
    interval.__set_type(tparquet::Type::FIXED_LEN_BYTE_ARRAY);
    interval.__set_type_length(12);
    interval.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    interval.__set_logicalType(tparquet::LogicalType());
    interval.__set_converted_type(tparquet::ConvertedType::INTERVAL);

    NativeFieldDescriptor native_schema;
    ASSERT_TRUE(native_schema.parse_from_thrift({root, interval}).ok());
    native_schema.assign_ids();
    std::vector<std::unique_ptr<ParquetColumnSchema>> fields;
    ASSERT_TRUE(build_parquet_column_schema(native_schema, &fields).ok());
    ASSERT_EQ(fields.size(), 1);
    EXPECT_TRUE(fields[0]->type_descriptor.unsupported_reason.empty());
    EXPECT_EQ(fields[0]->type_descriptor.doris_type->get_primitive_type(), TYPE_STRING);
}

TEST(ParquetSchemaTest, NativeGroupPrimitiveLogicalTypesAreRejected) {
    tparquet::SchemaElement root;
    root.__set_name("schema");
    root.__set_num_children(1);

    tparquet::SchemaElement child;
    child.__set_name("value");
    child.__set_type(tparquet::Type::BYTE_ARRAY);
    child.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);

    std::vector<std::pair<std::string, tparquet::LogicalType>> logical_types;
    tparquet::LogicalType string_type;
    string_type.__set_STRING(tparquet::StringType());
    logical_types.emplace_back("STRING", string_type);
    tparquet::LogicalType enum_type;
    enum_type.__set_ENUM(tparquet::EnumType());
    logical_types.emplace_back("ENUM", enum_type);
    tparquet::LogicalType decimal_type;
    decimal_type.__set_DECIMAL(tparquet::DecimalType());
    logical_types.emplace_back("DECIMAL", decimal_type);
    tparquet::LogicalType date_type;
    date_type.__set_DATE(tparquet::DateType());
    logical_types.emplace_back("DATE", date_type);
    tparquet::LogicalType time_type;
    time_type.__set_TIME(tparquet::TimeType());
    logical_types.emplace_back("TIME", time_type);
    tparquet::LogicalType timestamp_type;
    timestamp_type.__set_TIMESTAMP(tparquet::TimestampType());
    logical_types.emplace_back("TIMESTAMP", timestamp_type);
    tparquet::LogicalType integer_type;
    integer_type.__set_INTEGER(tparquet::IntType());
    logical_types.emplace_back("INTEGER", integer_type);
    tparquet::LogicalType uuid_type;
    uuid_type.__set_UUID(tparquet::UUIDType());
    logical_types.emplace_back("UUID", uuid_type);
    tparquet::LogicalType float16_type;
    float16_type.__set_FLOAT16(tparquet::Float16Type());
    logical_types.emplace_back("FLOAT16", float16_type);

    for (const auto& [name, logical_type] : logical_types) {
        SCOPED_TRACE(name);
        tparquet::SchemaElement group;
        group.__set_name("bad_" + name + "_group");
        group.__set_num_children(1);
        group.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
        group.__set_logicalType(logical_type);
        NativeFieldDescriptor native_schema;
        const auto status = native_schema.parse_from_thrift({root, group, child});
        EXPECT_FALSE(status.ok());
        if (name == "ENUM") {
            EXPECT_NE(status.to_string().find("Logical type Enum cannot be applied to group node"),
                      std::string::npos);
        }
    }

    for (const auto converted_type :
         {tparquet::ConvertedType::UTF8, tparquet::ConvertedType::ENUM,
          tparquet::ConvertedType::DECIMAL, tparquet::ConvertedType::DATE,
          tparquet::ConvertedType::TIME_MILLIS, tparquet::ConvertedType::TIMESTAMP_MICROS,
          tparquet::ConvertedType::INT_32, tparquet::ConvertedType::JSON,
          tparquet::ConvertedType::BSON}) {
        SCOPED_TRACE(tparquet::to_string(converted_type));
        tparquet::SchemaElement group;
        group.__set_name("bad_converted_group");
        group.__set_num_children(1);
        group.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
        group.__set_converted_type(converted_type);
        NativeFieldDescriptor native_schema;
        EXPECT_FALSE(native_schema.parse_from_thrift({root, group, child}).ok());
    }
}

TEST(ParquetSchemaTest, NativeFlatLeafValueCountMustMatchRowCount) {
    tparquet::SchemaElement root;
    root.__set_name("schema");
    root.__set_num_children(1);

    tparquet::SchemaElement leaf;
    leaf.__set_name("value");
    leaf.__set_type(tparquet::Type::INT32);
    leaf.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);

    tparquet::Statistics statistics;
    statistics.__set_null_count(2);
    tparquet::ColumnMetaData column_metadata;
    column_metadata.__set_type(tparquet::Type::INT32);
    column_metadata.__set_num_values(2);
    column_metadata.__set_statistics(statistics);
    tparquet::ColumnChunk chunk;
    chunk.__set_meta_data(column_metadata);
    tparquet::RowGroup row_group;
    row_group.__set_num_rows(1);
    row_group.__set_columns({chunk});
    tparquet::FileMetaData thrift_metadata;
    thrift_metadata.__set_version(1);
    thrift_metadata.__set_schema({root, leaf});
    thrift_metadata.__set_num_rows(1);
    thrift_metadata.__set_row_groups({row_group});

    NativeParquetMetadata metadata(std::move(thrift_metadata), 0);
    const auto status = metadata.init_schema(false, false);
    EXPECT_TRUE(status.is<ErrorCode::CORRUPTION>()) << status;
}

TEST(ParquetSchemaTest, NativeStringAnnotationsAndTimeUnitsPreserveLogicalTypes) {
    tparquet::SchemaElement root;
    root.__set_name("schema");
    root.__set_num_children(8);

    auto binary_leaf = [](const std::string& name) {
        tparquet::SchemaElement leaf;
        leaf.__set_name(name);
        leaf.__set_type(tparquet::Type::BYTE_ARRAY);
        leaf.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
        return leaf;
    };
    auto logical_enum = binary_leaf("logical_enum");
    logical_enum.__set_logicalType(tparquet::LogicalType());
    logical_enum.logicalType.__set_ENUM(tparquet::EnumType());
    auto logical_bson = binary_leaf("logical_bson");
    logical_bson.__set_logicalType(tparquet::LogicalType());
    logical_bson.logicalType.__set_BSON(tparquet::BsonType());
    auto converted_enum = binary_leaf("converted_enum");
    converted_enum.__set_converted_type(tparquet::ConvertedType::ENUM);
    auto converted_bson = binary_leaf("converted_bson");
    converted_bson.__set_converted_type(tparquet::ConvertedType::BSON);

    auto logical_time = [](const std::string& name, bool millis) {
        tparquet::SchemaElement leaf;
        leaf.__set_name(name);
        leaf.__set_type(millis ? tparquet::Type::INT32 : tparquet::Type::INT64);
        leaf.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);
        leaf.__set_logicalType(tparquet::LogicalType());
        leaf.logicalType.__set_TIME(tparquet::TimeType());
        leaf.logicalType.TIME.__set_isAdjustedToUTC(false);
        leaf.logicalType.TIME.__set_unit(tparquet::TimeUnit());
        if (millis) {
            leaf.logicalType.TIME.unit.__set_MILLIS(tparquet::MilliSeconds());
        } else {
            leaf.logicalType.TIME.unit.__set_MICROS(tparquet::MicroSeconds());
        }
        return leaf;
    };
    auto logical_millis = logical_time("logical_millis", true);
    auto logical_micros = logical_time("logical_micros", false);
    auto converted_millis = logical_time("converted_millis", true);
    converted_millis.__isset.logicalType = false;
    converted_millis.__set_converted_type(tparquet::ConvertedType::TIME_MILLIS);
    auto converted_micros = logical_time("converted_micros", false);
    converted_micros.__isset.logicalType = false;
    converted_micros.__set_converted_type(tparquet::ConvertedType::TIME_MICROS);

    NativeFieldDescriptor descriptor;
    ASSERT_TRUE(descriptor
                        .parse_from_thrift({root, logical_enum, logical_bson, converted_enum,
                                            converted_bson, logical_millis, logical_micros,
                                            converted_millis, converted_micros})
                        .ok());
    for (size_t field = 0; field < 4; ++field) {
        EXPECT_EQ(remove_nullable(descriptor.get_column(field)->data_type)->get_primitive_type(),
                  TYPE_STRING);
    }
    EXPECT_EQ(remove_nullable(descriptor.get_column(4)->data_type)->get_scale(), 3);
    EXPECT_EQ(remove_nullable(descriptor.get_column(5)->data_type)->get_scale(), 6);
    EXPECT_EQ(remove_nullable(descriptor.get_column(6)->data_type)->get_scale(), 3);
    EXPECT_EQ(remove_nullable(descriptor.get_column(7)->data_type)->get_scale(), 6);
}

TEST(ParquetSchemaTest, NativeSchemaRejectsAmbiguousKindsAndMissingRepetition) {
    auto valid_root = []() {
        tparquet::SchemaElement root;
        root.__set_name("schema");
        root.__set_num_children(1);
        return root;
    };
    auto valid_leaf = []() {
        tparquet::SchemaElement leaf;
        leaf.__set_name("value");
        leaf.__set_type(tparquet::Type::INT32);
        leaf.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);
        return leaf;
    };

    NativeFieldDescriptor descriptor;
    auto primitive_root = valid_root();
    primitive_root.__set_type(tparquet::Type::INT32);
    EXPECT_FALSE(descriptor.parse_from_thrift({primitive_root, valid_leaf()}).ok());

    auto repeated_root = valid_root();
    repeated_root.__set_repetition_type(tparquet::FieldRepetitionType::REPEATED);
    EXPECT_FALSE(descriptor.parse_from_thrift({repeated_root, valid_leaf()}).ok());

    auto missing_repetition = valid_leaf();
    missing_repetition.__isset.repetition_type = false;
    EXPECT_FALSE(descriptor.parse_from_thrift({valid_root(), missing_repetition}).ok());

    auto dual_kind = valid_leaf();
    dual_kind.__set_num_children(1);
    EXPECT_FALSE(descriptor.parse_from_thrift({valid_root(), dual_kind}).ok());

    auto legacy_zero_children = valid_leaf();
    legacy_zero_children.__set_num_children(0);
    EXPECT_TRUE(descriptor.parse_from_thrift({valid_root(), legacy_zero_children}).ok());

    auto missing_kind = valid_leaf();
    missing_kind.__isset.type = false;
    EXPECT_FALSE(descriptor.parse_from_thrift({valid_root(), missing_kind}).ok());
}

TEST(ParquetSchemaTest, NativeSchemaRejectsUnboundedChildCountsBeforeAllocation) {
    tparquet::SchemaElement root;
    root.__set_name("schema");
    root.__set_num_children(std::numeric_limits<int32_t>::max());
    NativeFieldDescriptor descriptor;
    EXPECT_FALSE(descriptor.parse_from_thrift({root}).ok());

    root.__set_num_children(1);
    tparquet::SchemaElement nested;
    nested.__set_name("nested");
    nested.__set_num_children(std::numeric_limits<int32_t>::max());
    nested.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);
    EXPECT_FALSE(descriptor.parse_from_thrift({root, nested}).ok());
}

std::vector<tparquet::SchemaElement> nested_native_schema(size_t depth) {
    std::vector<tparquet::SchemaElement> schema;
    tparquet::SchemaElement root;
    root.__set_name("schema");
    root.__set_num_children(1);
    schema.push_back(root);
    for (size_t level = 0; level < depth; ++level) {
        tparquet::SchemaElement group;
        group.__set_name("g" + std::to_string(level));
        group.__set_num_children(1);
        group.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);
        schema.push_back(group);
    }
    tparquet::SchemaElement leaf;
    leaf.__set_name("value");
    leaf.__set_type(tparquet::Type::INT32);
    leaf.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);
    schema.push_back(leaf);
    return schema;
}

TEST(ParquetSchemaTest, NativeSchemaBoundsRecursiveDepth) {
    NativeFieldDescriptor accepted;
    EXPECT_TRUE(accepted.parse_from_thrift(nested_native_schema(MAX_NATIVE_SCHEMA_DEPTH)).ok());
    NativeFieldDescriptor rejected;
    EXPECT_FALSE(
            rejected.parse_from_thrift(nested_native_schema(MAX_NATIVE_SCHEMA_DEPTH + 1)).ok());
}

TEST(ParquetSchemaTest, NativeListTupleCompatibilityRequiresEnclosingListName) {
    auto root = []() {
        tparquet::SchemaElement schema;
        schema.__set_name("schema");
        schema.__set_num_children(1);
        return schema;
    };
    auto list = [](const std::string& name) {
        tparquet::SchemaElement schema;
        schema.__set_name(name);
        schema.__set_num_children(1);
        schema.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
        schema.__set_converted_type(tparquet::ConvertedType::LIST);
        return schema;
    };
    auto wrapper = [](const std::string& name) {
        tparquet::SchemaElement schema;
        schema.__set_name(name);
        schema.__set_num_children(1);
        schema.__set_repetition_type(tparquet::FieldRepetitionType::REPEATED);
        return schema;
    };
    auto item = []() {
        tparquet::SchemaElement schema;
        schema.__set_name("item");
        schema.__set_type(tparquet::Type::INT32);
        schema.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
        return schema;
    };

    NativeFieldDescriptor mismatched;
    ASSERT_TRUE(mismatched.parse_from_thrift({root(), list("xs"), wrapper("other_tuple"), item()})
                        .ok());
    const auto* mismatched_element = &mismatched.get_column(0)->children[0];
    EXPECT_EQ(remove_nullable(mismatched_element->data_type)->get_primitive_type(), TYPE_INT);

    NativeFieldDescriptor matching;
    ASSERT_TRUE(matching.parse_from_thrift({root(), list("xs"), wrapper("xs_tuple"), item()}).ok());
    const auto* matching_element = &matching.get_column(0)->children[0];
    EXPECT_EQ(remove_nullable(matching_element->data_type)->get_primitive_type(), TYPE_STRUCT);
    ASSERT_EQ(matching_element->children.size(), 1);
    EXPECT_EQ(matching_element->children[0].name, "item");
}

TEST(ParquetSchemaTest, NativeListPreservesRepeatedAndAnnotatedElementWrappers) {
    auto root = []() {
        tparquet::SchemaElement schema;
        schema.__set_name("schema");
        schema.__set_num_children(1);
        return schema;
    };
    auto group = [](const std::string& name, tparquet::FieldRepetitionType::type repetition) {
        tparquet::SchemaElement schema;
        schema.__set_name(name);
        schema.__set_num_children(1);
        schema.__set_repetition_type(repetition);
        return schema;
    };
    auto outer_list = group("outer", tparquet::FieldRepetitionType::OPTIONAL);
    outer_list.__set_converted_type(tparquet::ConvertedType::LIST);

    auto repeated_wrapper = group("list", tparquet::FieldRepetitionType::REPEATED);
    tparquet::SchemaElement repeated_items;
    repeated_items.__set_name("items");
    repeated_items.__set_type(tparquet::Type::INT32);
    repeated_items.__set_repetition_type(tparquet::FieldRepetitionType::REPEATED);
    NativeFieldDescriptor repeated;
    ASSERT_TRUE(repeated.parse_from_thrift({root(), outer_list, repeated_wrapper, repeated_items})
                        .ok());
    const auto* repeated_element = &repeated.get_column(0)->children[0];
    EXPECT_EQ(remove_nullable(repeated_element->data_type)->get_primitive_type(), TYPE_STRUCT);
    ASSERT_EQ(repeated_element->children.size(), 1);
    EXPECT_EQ(repeated_element->children[0].name, "items");
    EXPECT_EQ(remove_nullable(repeated_element->children[0].data_type)->get_primitive_type(),
              TYPE_ARRAY);

    auto annotated_wrapper = repeated_wrapper;
    annotated_wrapper.__set_converted_type(tparquet::ConvertedType::LIST);
    auto nested_wrapper = repeated_wrapper;
    tparquet::SchemaElement value;
    value.__set_name("value");
    value.__set_type(tparquet::Type::INT32);
    value.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    NativeFieldDescriptor annotated;
    ASSERT_TRUE(annotated
                        .parse_from_thrift(
                                {root(), outer_list, annotated_wrapper, nested_wrapper, value})
                        .ok());
    const auto* annotated_element = &annotated.get_column(0)->children[0];
    EXPECT_EQ(remove_nullable(annotated_element->data_type)->get_primitive_type(), TYPE_ARRAY);
    ASSERT_EQ(annotated_element->children.size(), 1);
    EXPECT_EQ(remove_nullable(annotated_element->children[0].data_type)->get_primitive_type(),
              TYPE_INT);
}

TEST(ParquetSchemaTest, NativeSchemaRecognizesLogicalTypeOnlyListAndMap) {
    tparquet::SchemaElement root;
    root.__set_name("schema");
    root.__set_num_children(2);

    tparquet::SchemaElement list;
    list.__set_name("items");
    list.__set_num_children(1);
    list.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    list.__set_logicalType(tparquet::LogicalType());
    list.logicalType.__set_LIST(tparquet::ListType());
    tparquet::SchemaElement list_wrapper;
    list_wrapper.__set_name("list");
    list_wrapper.__set_num_children(1);
    list_wrapper.__set_repetition_type(tparquet::FieldRepetitionType::REPEATED);
    tparquet::SchemaElement element;
    element.__set_name("element");
    element.__set_type(tparquet::Type::INT32);
    element.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);

    tparquet::SchemaElement map;
    map.__set_name("attributes");
    map.__set_num_children(1);
    map.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    map.__set_logicalType(tparquet::LogicalType());
    map.logicalType.__set_MAP(tparquet::MapType());
    tparquet::SchemaElement key_value;
    key_value.__set_name("key_value");
    key_value.__set_num_children(2);
    key_value.__set_repetition_type(tparquet::FieldRepetitionType::REPEATED);
    tparquet::SchemaElement key;
    key.__set_name("key");
    key.__set_type(tparquet::Type::BYTE_ARRAY);
    key.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);
    tparquet::SchemaElement value;
    value.__set_name("value");
    value.__set_type(tparquet::Type::INT64);
    value.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);

    NativeFieldDescriptor descriptor;
    ASSERT_TRUE(descriptor
                        .parse_from_thrift(
                                {root, list, list_wrapper, element, map, key_value, key, value})
                        .ok());
    ASSERT_EQ(descriptor.size(), 2);
    EXPECT_EQ(remove_nullable(descriptor.get_column(0)->data_type)->get_primitive_type(),
              TYPE_ARRAY);
    EXPECT_EQ(remove_nullable(descriptor.get_column(1)->data_type)->get_primitive_type(), TYPE_MAP);
}

TEST(ParquetSchemaTest, NativeMetadataRejectsRowGroupChunkCardinalityAndMissingMetadata) {
    auto make_metadata = []() {
        tparquet::FileMetaData metadata;
        tparquet::SchemaElement root;
        root.__set_name("schema");
        root.__set_num_children(2);
        tparquet::SchemaElement first;
        first.__set_name("a");
        first.__set_type(tparquet::Type::INT32);
        first.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);
        tparquet::SchemaElement second = first;
        second.__set_name("b");
        metadata.__set_schema({root, first, second});
        tparquet::RowGroup row_group;
        row_group.__set_num_rows(1);
        metadata.__set_row_groups({row_group});
        return metadata;
    };

    {
        NativeParquetMetadata metadata(make_metadata(), 0);
        EXPECT_FALSE(metadata.init_schema(true, false).ok());
    }
    {
        auto thrift = make_metadata();
        thrift.row_groups[0].columns.resize(2);
        tparquet::ColumnMetaData first_meta;
        first_meta.__set_type(tparquet::Type::INT32);
        thrift.row_groups[0].columns[0].__set_meta_data(first_meta);
        NativeParquetMetadata metadata(std::move(thrift), 0);
        EXPECT_FALSE(metadata.init_schema(true, false).ok());
    }
}

TEST(ParquetSchemaTest, NativeProjectionUsesResolvedIdentityBeforeCaseInsensitiveFallback) {
    tparquet::SchemaElement root;
    root.__set_name("schema");
    root.__set_num_children(1);
    tparquet::SchemaElement group;
    group.__set_name("s");
    group.__set_num_children(3);
    group.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    tparquet::SchemaElement upper;
    upper.__set_name("Value");
    upper.__set_type(tparquet::Type::INT32);
    upper.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    upper.__set_field_id(1);
    tparquet::SchemaElement lower = upper;
    lower.__set_name("value");
    lower.__set_field_id(2);
    tparquet::SchemaElement other = upper;
    other.__set_name("other");
    other.__set_field_id(3);

    NativeFieldDescriptor descriptor;
    ASSERT_TRUE(descriptor.parse_from_thrift({root, group, upper, lower, other}).ok());
    descriptor.assign_ids();
    std::vector<std::unique_ptr<ParquetColumnSchema>> fields;
    ASSERT_TRUE(build_parquet_column_schema(descriptor, &fields).ok());

    const auto int_type = make_nullable(std::make_shared<DataTypeInt32>());
    std::shared_ptr<NativeSchemaNode> mapping;
    auto other_only = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {int_type}, Strings {"other"}));
    ASSERT_TRUE(build_native_schema_node(other_only, *fields[0], &mapping).ok());
    EXPECT_TRUE(mapping->has_child("other"));

    auto exact_case = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {int_type}, Strings {"Value"}));
    ASSERT_TRUE(build_native_schema_node(exact_case, *fields[0], &mapping).ok());
    EXPECT_EQ(mapping->file_child_name("Value"), "Value");

    auto ambiguous_fallback = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {int_type}, Strings {"VALUE"}));
    const auto status = build_native_schema_node(ambiguous_fallback, *fields[0], &mapping);
    EXPECT_TRUE(status.is<ErrorCode::CORRUPTION>()) << status;
}

TEST(ParquetSchemaTest, NativeSetMapKeyValueWrapperRemainsSingleListElement) {
    tparquet::SchemaElement root;
    root.__set_name("schema");
    root.__set_num_children(1);
    tparquet::SchemaElement set;
    set.__set_name("tags");
    set.__set_num_children(1);
    set.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    set.__set_converted_type(tparquet::ConvertedType::MAP);
    tparquet::SchemaElement wrapper;
    wrapper.__set_name("key_value");
    wrapper.__set_num_children(1);
    wrapper.__set_repetition_type(tparquet::FieldRepetitionType::REPEATED);
    wrapper.__set_converted_type(tparquet::ConvertedType::MAP_KEY_VALUE);
    tparquet::SchemaElement key;
    key.__set_name("key");
    key.__set_type(tparquet::Type::INT32);
    key.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);

    NativeFieldDescriptor descriptor;
    ASSERT_TRUE(descriptor.parse_from_thrift({root, set, wrapper, key}).ok());
    const auto* column = descriptor.get_column(0);
    EXPECT_EQ(remove_nullable(column->data_type)->get_primitive_type(), TYPE_ARRAY);
    ASSERT_EQ(column->children.size(), 1);
    EXPECT_EQ(remove_nullable(column->children[0].data_type)->get_primitive_type(), TYPE_INT);
}

TEST(ParquetSchemaTest, NativeListPreservesLegacyMapKeyValueElement) {
    tparquet::SchemaElement root;
    root.__set_name("schema");
    root.__set_num_children(1);
    tparquet::SchemaElement list;
    list.__set_name("entries");
    list.__set_num_children(1);
    list.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    list.__set_converted_type(tparquet::ConvertedType::LIST);
    tparquet::SchemaElement element;
    element.__set_name("element");
    element.__set_num_children(1);
    element.__set_repetition_type(tparquet::FieldRepetitionType::REPEATED);
    element.__set_converted_type(tparquet::ConvertedType::MAP_KEY_VALUE);
    tparquet::SchemaElement map;
    map.__set_name("map");
    map.__set_num_children(2);
    map.__set_repetition_type(tparquet::FieldRepetitionType::REPEATED);
    tparquet::SchemaElement key;
    key.__set_name("key");
    key.__set_type(tparquet::Type::INT32);
    key.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);
    tparquet::SchemaElement value;
    value.__set_name("value");
    value.__set_type(tparquet::Type::BYTE_ARRAY);
    value.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);

    NativeFieldDescriptor descriptor;
    ASSERT_TRUE(descriptor.parse_from_thrift({root, list, element, map, key, value}).ok());
    const auto* column = descriptor.get_column(0);
    EXPECT_EQ(remove_nullable(column->data_type)->get_primitive_type(), TYPE_ARRAY);
    ASSERT_EQ(column->children.size(), 1);
    EXPECT_EQ(remove_nullable(column->children[0].data_type)->get_primitive_type(), TYPE_MAP);
    ASSERT_EQ(column->children[0].children.size(), 2);
    EXPECT_EQ(remove_nullable(column->children[0].children[0].data_type)->get_primitive_type(),
              TYPE_INT);
    EXPECT_EQ(remove_nullable(column->children[0].children[1].data_type)->get_primitive_type(),
              TYPE_STRING);
}

} // namespace doris::format::parquet
