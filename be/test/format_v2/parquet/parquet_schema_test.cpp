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
#include <parquet/api/schema.h>

#include <limits>
#include <string>
#include <vector>

#include "core/assert_cast.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/primitive_type.h"
#include "format_v2/parquet/native_schema_desc.h"
#include "format_v2/parquet/native_schema_node.h"
#include "format_v2/parquet/parquet_column_schema.h"
#include "format_v2/parquet/parquet_file_context.h"

namespace doris::format::parquet {
namespace {

std::vector<std::unique_ptr<ParquetColumnSchema>> build_fields(
        const std::vector<::parquet::schema::NodePtr>& nodes) {
    auto schema =
            ::parquet::schema::GroupNode::Make("schema", ::parquet::Repetition::REQUIRED, nodes);
    ::parquet::SchemaDescriptor descriptor;
    descriptor.Init(schema);
    std::vector<std::unique_ptr<ParquetColumnSchema>> fields;
    EXPECT_TRUE(build_parquet_column_schema(descriptor, &fields).ok());
    return fields;
}

Status build_status(const std::vector<::parquet::schema::NodePtr>& nodes) {
    auto schema =
            ::parquet::schema::GroupNode::Make("schema", ::parquet::Repetition::REQUIRED, nodes);
    ::parquet::SchemaDescriptor descriptor;
    descriptor.Init(schema);
    std::vector<std::unique_ptr<ParquetColumnSchema>> fields;
    return build_parquet_column_schema(descriptor, &fields);
}

} // namespace

TEST(ParquetSchemaTest, PrimitiveStateAndFieldIdArePreserved) {
    const auto fields = build_fields({
            ::parquet::schema::PrimitiveNode::Make("required_i32", ::parquet::Repetition::REQUIRED,
                                                   ::parquet::Type::INT32),
            ::parquet::schema::PrimitiveNode::Make("optional_i64", ::parquet::Repetition::OPTIONAL,
                                                   ::parquet::Type::INT64,
                                                   ::parquet::ConvertedType::NONE, -1, -1, -1, 42),
    });

    ASSERT_EQ(fields.size(), 2);
    EXPECT_EQ(fields[0]->local_id, 0);
    EXPECT_EQ(fields[0]->name, "required_i32");
    EXPECT_EQ(fields[0]->kind, ParquetColumnSchemaKind::PRIMITIVE);
    EXPECT_EQ(fields[0]->leaf_column_id, 0);
    EXPECT_EQ(fields[0]->nullable_definition_level, 0);
    EXPECT_FALSE(fields[0]->type->is_nullable());

    EXPECT_EQ(fields[1]->local_id, 1);
    EXPECT_EQ(fields[1]->parquet_field_id, 42);
    EXPECT_EQ(fields[1]->leaf_column_id, 1);
    EXPECT_EQ(fields[1]->nullable_definition_level, 1);
    EXPECT_TRUE(fields[1]->type->is_nullable());
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
    EXPECT_EQ(fields[0]->name, "protocol");
    EXPECT_EQ(fields[0]->parquet_field_id, 10);
    ASSERT_EQ(fields[0]->children.size(), 2);
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

TEST(ParquetSchemaTest, PrimitiveTypeDescriptorCoversLogicalConvertedAndPhysicalFallback) {
    const auto fields = build_fields({
            ::parquet::schema::PrimitiveNode::Make(
                    "ts", ::parquet::Repetition::OPTIONAL,
                    ::parquet::LogicalType::Timestamp(false,
                                                      ::parquet::LogicalType::TimeUnit::MICROS),
                    ::parquet::Type::INT64),
            ::parquet::schema::PrimitiveNode::Make("i8", ::parquet::Repetition::REQUIRED,
                                                   ::parquet::Type::INT32,
                                                   ::parquet::ConvertedType::INT_8),
            ::parquet::schema::PrimitiveNode::Make("plain", ::parquet::Repetition::REQUIRED,
                                                   ::parquet::Type::DOUBLE),
    });

    ASSERT_EQ(fields.size(), 3);
    EXPECT_EQ(remove_nullable(fields[0]->type)->get_primitive_type(), TYPE_DATETIMEV2);
    EXPECT_EQ(fields[0]->type_descriptor.time_unit, ParquetTimeUnit::MICROS);
    EXPECT_EQ(fields[0]->type_descriptor.extra_type_info, ParquetExtraTypeInfo::UNIT_MICROS);
    EXPECT_TRUE(fields[0]->type_descriptor.is_timestamp);
    EXPECT_FALSE(fields[0]->type_descriptor.timestamp_is_adjusted_to_utc);

    EXPECT_EQ(remove_nullable(fields[1]->type)->get_primitive_type(), TYPE_TINYINT);
    EXPECT_EQ(fields[1]->type_descriptor.integer_bit_width, 8);
    EXPECT_FALSE(fields[1]->type_descriptor.is_unsigned_integer);

    EXPECT_EQ(remove_nullable(fields[2]->type)->get_primitive_type(), TYPE_DOUBLE);
    EXPECT_EQ(fields[2]->type_descriptor.physical_type, ::parquet::Type::DOUBLE);
    EXPECT_EQ(fields[2]->type_descriptor.extra_type_info, ParquetExtraTypeInfo::NONE);
}

TEST(ParquetSchemaTest, StructMakesDataTypeChildrenNullableAndPropagatesLevels) {
    const auto fields = build_fields({::parquet::schema::GroupNode::Make(
            "s", ::parquet::Repetition::OPTIONAL,
            {
                    ::parquet::schema::PrimitiveNode::Make("a", ::parquet::Repetition::REQUIRED,
                                                           ::parquet::Type::INT32),
                    ::parquet::schema::PrimitiveNode::Make("b", ::parquet::Repetition::OPTIONAL,
                                                           ::parquet::Type::BYTE_ARRAY,
                                                           ::parquet::ConvertedType::UTF8),
            })});

    ASSERT_EQ(fields.size(), 1);
    const auto& struct_schema = *fields[0];
    EXPECT_EQ(struct_schema.kind, ParquetColumnSchemaKind::STRUCT);
    EXPECT_EQ(struct_schema.nullable_definition_level, 1);
    ASSERT_EQ(struct_schema.children.size(), 2);
    EXPECT_EQ(struct_schema.children[0]->definition_level, 1);
    EXPECT_EQ(struct_schema.children[1]->definition_level, 2);
    EXPECT_EQ(struct_schema.max_definition_level, 2);

    const auto& struct_type =
            assert_cast<const DataTypeStruct&>(*remove_nullable(struct_schema.type));
    ASSERT_EQ(struct_type.get_elements().size(), 2);
    EXPECT_TRUE(struct_type.get_elements()[0]->is_nullable());
    EXPECT_TRUE(struct_type.get_elements()[1]->is_nullable());
}

TEST(ParquetSchemaTest, ListCompatibilityRulesAndLevels) {
    const auto standard_list = ::parquet::schema::GroupNode::Make(
            "xs", ::parquet::Repetition::OPTIONAL,
            {::parquet::schema::GroupNode::Make(
                    "list", ::parquet::Repetition::REPEATED,
                    {::parquet::schema::PrimitiveNode::Make("item", ::parquet::Repetition::OPTIONAL,
                                                            ::parquet::Type::INT32)})},
            ::parquet::ConvertedType::LIST);
    const auto structural_array = ::parquet::schema::GroupNode::Make(
            "ys", ::parquet::Repetition::OPTIONAL,
            {::parquet::schema::GroupNode::Make(
                    "array", ::parquet::Repetition::REPEATED,
                    {::parquet::schema::PrimitiveNode::Make(
                            "value", ::parquet::Repetition::REQUIRED, ::parquet::Type::INT64)})},
            ::parquet::ConvertedType::LIST);

    const auto fields = build_fields({standard_list, structural_array});
    ASSERT_EQ(fields.size(), 2);

    const auto& xs = *fields[0];
    EXPECT_EQ(xs.kind, ParquetColumnSchemaKind::LIST);
    EXPECT_EQ(xs.definition_level, 2);
    EXPECT_EQ(xs.repetition_level, 1);
    ASSERT_EQ(xs.children.size(), 1);
    EXPECT_EQ(xs.children[0]->name, "element");
    EXPECT_EQ(xs.children[0]->kind, ParquetColumnSchemaKind::PRIMITIVE);
    EXPECT_TRUE(xs.children[0]->type->is_nullable());
    const auto& xs_type = assert_cast<const DataTypeArray&>(*remove_nullable(xs.type));
    EXPECT_TRUE(xs_type.get_nested_type()->is_nullable());

    const auto& ys = *fields[1];
    EXPECT_EQ(ys.kind, ParquetColumnSchemaKind::LIST);
    ASSERT_EQ(ys.children.size(), 1);
    EXPECT_EQ(ys.children[0]->kind, ParquetColumnSchemaKind::STRUCT);
    EXPECT_EQ(remove_nullable(ys.children[0]->type)->get_primitive_type(), TYPE_STRUCT);
}

TEST(ParquetSchemaTest, LegacyListElementResolutionRulesArePreserved) {
    const auto two_level_list = ::parquet::schema::GroupNode::Make(
            "two_level", ::parquet::Repetition::OPTIONAL,
            {::parquet::schema::PrimitiveNode::Make("item", ::parquet::Repetition::REPEATED,
                                                    ::parquet::Type::INT32)},
            ::parquet::ConvertedType::LIST);
    const auto tuple_list = ::parquet::schema::GroupNode::Make(
            "tuple_list", ::parquet::Repetition::OPTIONAL,
            {::parquet::schema::GroupNode::Make(
                    "tuple_list_tuple", ::parquet::Repetition::REPEATED,
                    {::parquet::schema::PrimitiveNode::Make(
                            "value", ::parquet::Repetition::REQUIRED, ::parquet::Type::INT64)})},
            ::parquet::ConvertedType::LIST);
    const auto multi_field_list = ::parquet::schema::GroupNode::Make(
            "records", ::parquet::Repetition::OPTIONAL,
            {::parquet::schema::GroupNode::Make(
                    "list", ::parquet::Repetition::REPEATED,
                    {::parquet::schema::PrimitiveNode::Make("id", ::parquet::Repetition::REQUIRED,
                                                            ::parquet::Type::INT32),
                     ::parquet::schema::PrimitiveNode::Make("name", ::parquet::Repetition::OPTIONAL,
                                                            ::parquet::Type::BYTE_ARRAY,
                                                            ::parquet::ConvertedType::UTF8)})},
            ::parquet::ConvertedType::LIST);
    const auto fields = build_fields({two_level_list, tuple_list, multi_field_list});
    ASSERT_EQ(fields.size(), 3);

    const auto& two_level = *fields[0];
    EXPECT_EQ(two_level.kind, ParquetColumnSchemaKind::LIST);
    EXPECT_EQ(two_level.definition_level, 2);
    EXPECT_EQ(two_level.repetition_level, 1);
    ASSERT_EQ(two_level.children.size(), 1);
    EXPECT_EQ(two_level.children[0]->kind, ParquetColumnSchemaKind::PRIMITIVE);
    EXPECT_EQ(two_level.children[0]->name, "element");
    EXPECT_EQ(remove_nullable(two_level.children[0]->type)->get_primitive_type(), TYPE_INT);

    const auto& tuple = *fields[1];
    ASSERT_EQ(tuple.children.size(), 1);
    EXPECT_EQ(tuple.children[0]->kind, ParquetColumnSchemaKind::STRUCT);
    EXPECT_EQ(tuple.children[0]->name, "element");
    ASSERT_EQ(tuple.children[0]->children.size(), 1);
    EXPECT_EQ(tuple.children[0]->children[0]->name, "value");

    const auto& multi_field = *fields[2];
    ASSERT_EQ(multi_field.children.size(), 1);
    EXPECT_EQ(multi_field.children[0]->kind, ParquetColumnSchemaKind::STRUCT);
    ASSERT_EQ(multi_field.children[0]->children.size(), 2);
    EXPECT_EQ(multi_field.children[0]->children[0]->name, "id");
    EXPECT_EQ(multi_field.children[0]->children[1]->name, "name");
}

TEST(ParquetSchemaTest, NestedRepeatedInsideListElementIsWrappedOnce) {
    const auto list_with_repeated_child = ::parquet::schema::GroupNode::Make(
            "outer", ::parquet::Repetition::OPTIONAL,
            {::parquet::schema::GroupNode::Make(
                    "list", ::parquet::Repetition::REPEATED,
                    {::parquet::schema::PrimitiveNode::Make(
                            "items", ::parquet::Repetition::REPEATED, ::parquet::Type::INT32)})},
            ::parquet::ConvertedType::LIST);

    const auto fields = build_fields({list_with_repeated_child});
    ASSERT_EQ(fields.size(), 1);
    const auto& outer = *fields[0];
    EXPECT_EQ(outer.kind, ParquetColumnSchemaKind::LIST);
    ASSERT_EQ(outer.children.size(), 1);
    const auto& element = *outer.children[0];
    EXPECT_EQ(element.kind, ParquetColumnSchemaKind::STRUCT);
    ASSERT_EQ(element.children.size(), 1);
    EXPECT_EQ(element.children[0]->kind, ParquetColumnSchemaKind::LIST);
    EXPECT_EQ(element.children[0]->name, "items");
    ASSERT_EQ(element.children[0]->children.size(), 1);
    EXPECT_EQ(element.children[0]->children[0]->name, "element");
}

TEST(ParquetSchemaTest, ListWrapperWithLogicalAnnotationIsPreservedAsElement) {
    const auto annotated_repeated_group = ::parquet::schema::GroupNode::Make(
            "xs", ::parquet::Repetition::OPTIONAL,
            {::parquet::schema::GroupNode::Make(
                    "list", ::parquet::Repetition::REPEATED,
                    {::parquet::schema::PrimitiveNode::Make(
                            "value", ::parquet::Repetition::OPTIONAL, ::parquet::Type::INT32)},
                    ::parquet::ConvertedType::LIST)},
            ::parquet::ConvertedType::LIST);

    EXPECT_FALSE(build_status({annotated_repeated_group}).ok());

    const auto nested_list_wrapper = ::parquet::schema::GroupNode::Make(
            "xs", ::parquet::Repetition::OPTIONAL,
            {::parquet::schema::GroupNode::Make(
                    "list", ::parquet::Repetition::REPEATED,
                    {::parquet::schema::GroupNode::Make(
                            "list", ::parquet::Repetition::REPEATED,
                            {::parquet::schema::PrimitiveNode::Make("value",
                                                                    ::parquet::Repetition::OPTIONAL,
                                                                    ::parquet::Type::INT32)})},
                    ::parquet::ConvertedType::LIST)},
            ::parquet::ConvertedType::LIST);

    const auto fields = build_fields({nested_list_wrapper});
    ASSERT_EQ(fields.size(), 1);
    const auto& xs = *fields[0];
    EXPECT_EQ(xs.kind, ParquetColumnSchemaKind::LIST);
    ASSERT_EQ(xs.children.size(), 1);
    const auto& element = *xs.children[0];
    EXPECT_EQ(element.kind, ParquetColumnSchemaKind::LIST);
    EXPECT_EQ(element.name, "element");
    ASSERT_EQ(element.children.size(), 1);
    EXPECT_EQ(element.children[0]->name, "element");
    EXPECT_EQ(remove_nullable(element.children[0]->type)->get_primitive_type(), TYPE_INT);
}

TEST(ParquetSchemaTest, MapWrapperIsFoldedAndOptionalKeyIsAllowed) {
    const auto fields = build_fields({::parquet::schema::GroupNode::Make(
            "m", ::parquet::Repetition::OPTIONAL,
            {::parquet::schema::GroupNode::Make(
                    "key_value", ::parquet::Repetition::REPEATED,
                    {
                            ::parquet::schema::PrimitiveNode::Make(
                                    "key", ::parquet::Repetition::OPTIONAL,
                                    ::parquet::Type::BYTE_ARRAY, ::parquet::ConvertedType::UTF8),
                            ::parquet::schema::PrimitiveNode::Make("value",
                                                                   ::parquet::Repetition::OPTIONAL,
                                                                   ::parquet::Type::INT32),
                    })},
            ::parquet::ConvertedType::MAP)});

    ASSERT_EQ(fields.size(), 1);
    const auto& map_schema = *fields[0];
    EXPECT_EQ(map_schema.kind, ParquetColumnSchemaKind::MAP);
    EXPECT_EQ(map_schema.definition_level, 2);
    EXPECT_EQ(map_schema.repetition_level, 1);
    ASSERT_EQ(map_schema.children.size(), 2);
    EXPECT_EQ(map_schema.children[0]->name, "key");
    EXPECT_EQ(map_schema.children[1]->name, "value");
    EXPECT_TRUE(map_schema.children[0]->type->is_nullable());

    const auto& map_type = assert_cast<const DataTypeMap&>(*remove_nullable(map_schema.type));
    EXPECT_TRUE(map_type.get_key_type()->is_nullable());
    EXPECT_TRUE(map_type.get_value_type()->is_nullable());
}

TEST(ParquetSchemaTest, StandardMapLevelsAndDataTypesAreBuiltFromEntryContext) {
    const auto fields = build_fields({::parquet::schema::GroupNode::Make(
            "m", ::parquet::Repetition::REQUIRED,
            {::parquet::schema::GroupNode::Make(
                    "key_value", ::parquet::Repetition::REPEATED,
                    {
                            ::parquet::schema::PrimitiveNode::Make(
                                    "key", ::parquet::Repetition::REQUIRED,
                                    ::parquet::Type::BYTE_ARRAY, ::parquet::ConvertedType::UTF8),
                            ::parquet::schema::PrimitiveNode::Make("value",
                                                                   ::parquet::Repetition::OPTIONAL,
                                                                   ::parquet::Type::INT32),
                    })},
            ::parquet::ConvertedType::MAP)});

    ASSERT_EQ(fields.size(), 1);
    const auto& map_schema = *fields[0];
    EXPECT_FALSE(map_schema.type->is_nullable());
    EXPECT_EQ(map_schema.definition_level, 1);
    EXPECT_EQ(map_schema.repetition_level, 1);
    EXPECT_EQ(map_schema.repeated_repetition_level, 1);
    EXPECT_EQ(map_schema.max_definition_level, 2);
    EXPECT_EQ(map_schema.max_repetition_level, 1);
    ASSERT_EQ(map_schema.children.size(), 2);
    EXPECT_EQ(map_schema.children[0]->definition_level, 1);
    EXPECT_EQ(map_schema.children[0]->repetition_level, 1);
    EXPECT_EQ(map_schema.children[1]->definition_level, 2);
    EXPECT_EQ(map_schema.children[1]->nullable_definition_level, 2);

    const auto& map_type = assert_cast<const DataTypeMap&>(*remove_nullable(map_schema.type));
    EXPECT_TRUE(map_type.get_key_type()->is_nullable());
    EXPECT_TRUE(map_type.get_value_type()->is_nullable());
}

TEST(ParquetSchemaTest, BareRepeatedFieldsAreWrappedAsLists) {
    const auto fields = build_fields({
            ::parquet::schema::PrimitiveNode::Make("items", ::parquet::Repetition::REPEATED,
                                                   ::parquet::Type::INT32),
            ::parquet::schema::GroupNode::Make(
                    "links", ::parquet::Repetition::REPEATED,
                    {::parquet::schema::PrimitiveNode::Make("url", ::parquet::Repetition::OPTIONAL,
                                                            ::parquet::Type::BYTE_ARRAY,
                                                            ::parquet::ConvertedType::UTF8),
                     ::parquet::schema::PrimitiveNode::Make("rank", ::parquet::Repetition::OPTIONAL,
                                                            ::parquet::Type::INT32)}),
    });

    ASSERT_EQ(fields.size(), 2);
    EXPECT_EQ(fields[0]->kind, ParquetColumnSchemaKind::LIST);
    ASSERT_EQ(fields[0]->children.size(), 1);
    EXPECT_EQ(fields[0]->children[0]->kind, ParquetColumnSchemaKind::PRIMITIVE);
    EXPECT_EQ(fields[0]->children[0]->name, "element");

    EXPECT_EQ(fields[1]->kind, ParquetColumnSchemaKind::LIST);
    ASSERT_EQ(fields[1]->children.size(), 1);
    EXPECT_EQ(fields[1]->children[0]->kind, ParquetColumnSchemaKind::STRUCT);
    EXPECT_EQ(fields[1]->children[0]->name, "element");
}

TEST(ParquetSchemaTest, DeepLevelChainPropagatesDefinitionAndRepetitionLevels) {
    const auto fields = build_fields({::parquet::schema::GroupNode::Make(
            "s", ::parquet::Repetition::OPTIONAL,
            {::parquet::schema::GroupNode::Make(
                    "inner", ::parquet::Repetition::OPTIONAL,
                    {::parquet::schema::PrimitiveNode::Make(
                            "items", ::parquet::Repetition::REPEATED, ::parquet::Type::INT32)})})});

    ASSERT_EQ(fields.size(), 1);
    const auto& s = *fields[0];
    EXPECT_EQ(s.definition_level, 1);
    EXPECT_EQ(s.nullable_definition_level, 1);
    ASSERT_EQ(s.children.size(), 1);
    const auto& inner = *s.children[0];
    EXPECT_EQ(inner.definition_level, 2);
    EXPECT_EQ(inner.nullable_definition_level, 2);
    ASSERT_EQ(inner.children.size(), 1);
    const auto& items = *inner.children[0];
    EXPECT_EQ(items.kind, ParquetColumnSchemaKind::LIST);
    EXPECT_EQ(items.definition_level, 3);
    EXPECT_EQ(items.repetition_level, 1);
    EXPECT_EQ(items.repeated_ancestor_definition_level, 3);
    EXPECT_EQ(items.repeated_repetition_level, 1);
    EXPECT_EQ(items.max_definition_level, 3);
    EXPECT_EQ(items.max_repetition_level, 1);
    ASSERT_EQ(items.children.size(), 1);
    EXPECT_EQ(items.children[0]->definition_level, 3);
    EXPECT_EQ(items.children[0]->repetition_level, 1);
}

TEST(ParquetSchemaTest, BuildEntryValidatesNullPointerAndEmptyRoot) {
    auto empty_root = ::parquet::schema::GroupNode::Make("schema", ::parquet::Repetition::REQUIRED,
                                                         ::parquet::schema::NodeVector {});
    ::parquet::SchemaDescriptor descriptor;
    descriptor.Init(empty_root);

    EXPECT_FALSE(build_parquet_column_schema(descriptor, nullptr).ok());

    std::vector<std::unique_ptr<ParquetColumnSchema>> fields;
    ASSERT_TRUE(build_parquet_column_schema(descriptor, &fields).ok());
    EXPECT_TRUE(fields.empty());
}

TEST(ParquetSchemaTest, RejectInvalidListMapAndPreserveUnsupportedTime) {
    const auto bad_list = ::parquet::schema::GroupNode::Make(
            "bad_list", ::parquet::Repetition::OPTIONAL,
            {::parquet::schema::PrimitiveNode::Make("item", ::parquet::Repetition::OPTIONAL,
                                                    ::parquet::Type::INT32)},
            ::parquet::ConvertedType::LIST);
    EXPECT_FALSE(build_status({bad_list}).ok());

    const auto bad_map = ::parquet::schema::GroupNode::Make(
            "bad_map", ::parquet::Repetition::OPTIONAL,
            {::parquet::schema::PrimitiveNode::Make("entry", ::parquet::Repetition::REPEATED,
                                                    ::parquet::Type::INT32)},
            ::parquet::ConvertedType::MAP);
    EXPECT_FALSE(build_status({bad_map}).ok());

    const auto converted_time = ::parquet::schema::PrimitiveNode::Make(
            "time_ms", ::parquet::Repetition::REQUIRED, ::parquet::Type::INT32,
            ::parquet::ConvertedType::TIME_MILLIS);
    const auto fields = build_fields({converted_time});
    ASSERT_EQ(fields.size(), 1);
    EXPECT_EQ(remove_nullable(fields[0]->type)->get_primitive_type(), TYPE_INT);
    EXPECT_NE(fields[0]->type_descriptor.unsupported_reason.find(
                      "Parquet TIME with isAdjustedToUTC=true is not supported"),
              std::string::npos);
}

TEST(ParquetSchemaTest, RejectAdditionalInvalidListAndMapLayouts) {
    const auto zero_child_list = ::parquet::schema::GroupNode::Make(
            "zero_child_list", ::parquet::Repetition::OPTIONAL,
            {::parquet::schema::GroupNode::Make("list", ::parquet::Repetition::REPEATED,
                                                ::parquet::schema::NodeVector {})},
            ::parquet::ConvertedType::LIST);
    EXPECT_FALSE(build_status({zero_child_list}).ok());

    const auto repeated_list = ::parquet::schema::GroupNode::Make(
            "repeated_list", ::parquet::Repetition::REPEATED,
            {::parquet::schema::GroupNode::Make(
                    "list", ::parquet::Repetition::REPEATED,
                    {::parquet::schema::PrimitiveNode::Make("item", ::parquet::Repetition::OPTIONAL,
                                                            ::parquet::Type::INT32)})},
            ::parquet::ConvertedType::LIST);
    EXPECT_FALSE(build_status({repeated_list}).ok());

    const auto map_with_two_fields = ::parquet::schema::GroupNode::Make(
            "bad_map", ::parquet::Repetition::OPTIONAL,
            {
                    ::parquet::schema::GroupNode::Make(
                            "entry1", ::parquet::Repetition::REPEATED,
                            {::parquet::schema::PrimitiveNode::Make(
                                     "key", ::parquet::Repetition::REQUIRED,
                                     ::parquet::Type::BYTE_ARRAY, ::parquet::ConvertedType::UTF8),
                             ::parquet::schema::PrimitiveNode::Make("value",
                                                                    ::parquet::Repetition::OPTIONAL,
                                                                    ::parquet::Type::INT32)}),
                    ::parquet::schema::GroupNode::Make(
                            "entry2", ::parquet::Repetition::REPEATED,
                            {::parquet::schema::PrimitiveNode::Make(
                                     "key", ::parquet::Repetition::REQUIRED,
                                     ::parquet::Type::BYTE_ARRAY, ::parquet::ConvertedType::UTF8),
                             ::parquet::schema::PrimitiveNode::Make("value",
                                                                    ::parquet::Repetition::OPTIONAL,
                                                                    ::parquet::Type::INT32)}),
            },
            ::parquet::ConvertedType::MAP);
    EXPECT_FALSE(build_status({map_with_two_fields}).ok());

    const auto non_repeated_map_entry = ::parquet::schema::GroupNode::Make(
            "bad_map", ::parquet::Repetition::OPTIONAL,
            {::parquet::schema::GroupNode::Make(
                    "key_value", ::parquet::Repetition::OPTIONAL,
                    {::parquet::schema::PrimitiveNode::Make("key", ::parquet::Repetition::REQUIRED,
                                                            ::parquet::Type::BYTE_ARRAY,
                                                            ::parquet::ConvertedType::UTF8),
                     ::parquet::schema::PrimitiveNode::Make(
                             "value", ::parquet::Repetition::OPTIONAL, ::parquet::Type::INT32)})},
            ::parquet::ConvertedType::MAP);
    EXPECT_FALSE(build_status({non_repeated_map_entry}).ok());

    const auto map_entry_with_one_child = ::parquet::schema::GroupNode::Make(
            "bad_map", ::parquet::Repetition::OPTIONAL,
            {::parquet::schema::GroupNode::Make(
                    "key_value", ::parquet::Repetition::REPEATED,
                    {::parquet::schema::PrimitiveNode::Make("key", ::parquet::Repetition::REQUIRED,
                                                            ::parquet::Type::BYTE_ARRAY,
                                                            ::parquet::ConvertedType::UTF8)})},
            ::parquet::ConvertedType::MAP);
    EXPECT_FALSE(build_status({map_entry_with_one_child}).ok());

    const auto repeated_map = ::parquet::schema::GroupNode::Make(
            "repeated_map", ::parquet::Repetition::REPEATED,
            {::parquet::schema::GroupNode::Make(
                    "key_value", ::parquet::Repetition::REPEATED,
                    {::parquet::schema::PrimitiveNode::Make("key", ::parquet::Repetition::REQUIRED,
                                                            ::parquet::Type::BYTE_ARRAY,
                                                            ::parquet::ConvertedType::UTF8),
                     ::parquet::schema::PrimitiveNode::Make(
                             "value", ::parquet::Repetition::OPTIONAL, ::parquet::Type::INT32)})},
            ::parquet::ConvertedType::MAP);
    EXPECT_FALSE(build_status({repeated_map}).ok());
}

TEST(ParquetSchemaTest, LogicalUtcTimeIsPreservedForProjection) {
    const auto adjusted_time = ::parquet::schema::PrimitiveNode::Make(
            "time_ms", ::parquet::Repetition::REQUIRED,
            ::parquet::LogicalType::Time(true, ::parquet::LogicalType::TimeUnit::MILLIS),
            ::parquet::Type::INT32);
    const auto supported_value = ::parquet::schema::PrimitiveNode::Make(
            "value", ::parquet::Repetition::REQUIRED, ::parquet::Type::INT64);
    const auto row = ::parquet::schema::GroupNode::Make("row", ::parquet::Repetition::OPTIONAL,
                                                        {adjusted_time, supported_value});
    const auto fields = build_fields({row});
    ASSERT_EQ(fields.size(), 1);
    ASSERT_EQ(fields[0]->children.size(), 2);
    EXPECT_EQ(remove_nullable(fields[0]->children[0]->type)->get_primitive_type(), TYPE_INT);
    EXPECT_FALSE(fields[0]->children[0]->type_descriptor.unsupported_reason.empty());
    EXPECT_EQ(remove_nullable(fields[0]->children[1]->type)->get_primitive_type(), TYPE_BIGINT);
}

TEST(ParquetSchemaTest, NativeLogicalUtcTimeIsRejected) {
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
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("Parquet TIME with isAdjustedToUTC=true is not supported"),
              std::string::npos);
}

TEST(ParquetSchemaTest, NativeGroupEnumLogicalTypeIsRejected) {
    tparquet::SchemaElement root;
    root.__set_name("schema");
    root.__set_num_children(1);

    tparquet::SchemaElement enum_group;
    enum_group.__set_name("bad_enum_group");
    enum_group.__set_num_children(1);
    enum_group.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    enum_group.__set_logicalType(tparquet::LogicalType());
    enum_group.logicalType.__set_ENUM(tparquet::EnumType());

    tparquet::SchemaElement child;
    child.__set_name("value");
    child.__set_type(tparquet::Type::BYTE_ARRAY);
    child.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);

    NativeFieldDescriptor native_schema;
    const auto status = native_schema.parse_from_thrift({root, enum_group, child});
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("Logical type Enum cannot be applied to group node"),
              std::string::npos);
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

} // namespace doris::format::parquet
