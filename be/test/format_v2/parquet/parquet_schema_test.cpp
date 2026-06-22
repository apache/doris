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

#include <string>
#include <vector>

#include "core/assert_cast.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/primitive_type.h"
#include "format_v2/parquet/parquet_column_schema.h"

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

TEST(ParquetSchemaTest, RejectInvalidListMapAndUnsupportedTime) {
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
    const auto status = build_status({converted_time});
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("Parquet TIME with isAdjustedToUTC=true is not supported"),
              std::string::npos);
}

} // namespace doris::format::parquet
