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
