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

#include "format/table/nested_column_access_helper.h"

#include <gtest/gtest.h>

#include <set>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_variant.h"
#include "format/parquet/parquet_nested_column_utils.h"
#include "format/parquet/schema_desc.h"
#include "format/table/hive/hive_parquet_nested_column_utils.h"
#include "format/table/iceberg/iceberg_parquet_nested_column_utils.h"

namespace doris {
namespace {

FieldSchema make_variant_field_for_access_path_test() {
    FieldSchema field;
    field.name = "v";
    field.lower_case_name = "v";
    field.column_id = 10;
    field.max_column_id = 16;
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

FieldSchema make_int32_field_schema(std::string name) {
    FieldSchema field;
    field.name = std::move(name);
    field.lower_case_name = field.name;
    field.physical_type = tparquet::Type::INT32;
    field.data_type = make_nullable(std::make_shared<DataTypeInt32>());
    return field;
}

FieldSchema make_variant_field_with_nested_structural_name_keys() {
    FieldSchema nested;
    nested.name = "nested";
    nested.lower_case_name = nested.name;
    nested.children = {make_int32_field_schema("typed_value"), make_int32_field_schema("value")};
    nested.data_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {nested.children[0].data_type, nested.children[1].data_type},
            Strings {"typed_value", "value"}));

    FieldSchema typed_value;
    typed_value.name = "typed_value";
    typed_value.lower_case_name = typed_value.name;
    typed_value.children = {nested};
    typed_value.data_type = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {nested.data_type}, Strings {"nested"}));

    FieldSchema field;
    field.name = "v";
    field.lower_case_name = field.name;
    field.data_type = std::make_shared<DataTypeVariant>(0, false);
    field.children = {make_binary_field_schema("metadata", false), typed_value};
    uint64_t next_id = 10;
    field.assign_ids(next_id);
    return field;
}

FieldSchema make_variant_field_with_annotated_value_user_field() {
    FieldSchema object;
    object.name = "obj";
    object.lower_case_name = object.name;
    object.children = {make_string_field_schema("value", true)};
    object.data_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {object.children[0].data_type}, Strings {"value"}));

    FieldSchema typed_value;
    typed_value.name = "typed_value";
    typed_value.lower_case_name = typed_value.name;
    typed_value.children = {object};
    typed_value.data_type = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {object.data_type}, Strings {"obj"}));

    FieldSchema field;
    field.name = "v";
    field.lower_case_name = field.name;
    field.data_type = std::make_shared<DataTypeVariant>(0, false);
    field.children = {make_binary_field_schema("metadata", false), typed_value};
    uint64_t next_id = 10;
    field.assign_ids(next_id);
    return field;
}

FieldSchema make_variant_field_with_typed_only_nested_shredded_object() {
    FieldSchema nested_x = make_int32_field_schema("x");

    FieldSchema nested_typed_value;
    nested_typed_value.name = "typed_value";
    nested_typed_value.lower_case_name = nested_typed_value.name;
    nested_typed_value.children = {nested_x};
    nested_typed_value.data_type = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {nested_x.data_type}, Strings {"x"}));

    FieldSchema nested;
    nested.name = "nested";
    nested.lower_case_name = nested.name;
    nested.children = {nested_typed_value};
    nested.data_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {nested_typed_value.data_type}, Strings {"typed_value"}));

    FieldSchema typed_value;
    typed_value.name = "typed_value";
    typed_value.lower_case_name = typed_value.name;
    typed_value.children = {nested};
    typed_value.data_type = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {nested.data_type}, Strings {"nested"}));

    FieldSchema field;
    field.name = "v";
    field.lower_case_name = field.name;
    field.data_type = std::make_shared<DataTypeVariant>(0, false);
    field.children = {make_binary_field_schema("metadata", false), typed_value};
    uint64_t next_id = 10;
    field.assign_ids(next_id);
    return field;
}

FieldSchema make_variant_field_with_typed_only_array_field() {
    FieldSchema element_n = make_int32_field_schema("n");

    FieldSchema element;
    element.name = "element";
    element.lower_case_name = element.name;
    element.children = {element_n};
    element.data_type = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {element_n.data_type}, Strings {"n"}));

    FieldSchema items_typed_value;
    items_typed_value.name = "typed_value";
    items_typed_value.lower_case_name = items_typed_value.name;
    items_typed_value.children = {element};
    items_typed_value.data_type = make_nullable(std::make_shared<DataTypeArray>(element.data_type));

    FieldSchema items;
    items.name = "items";
    items.lower_case_name = items.name;
    items.children = {items_typed_value};
    items.data_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {items_typed_value.data_type}, Strings {"typed_value"}));

    FieldSchema typed_value;
    typed_value.name = "typed_value";
    typed_value.lower_case_name = typed_value.name;
    typed_value.children = {items};
    typed_value.data_type = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {items.data_type}, Strings {"items"}));

    FieldSchema field;
    field.name = "v";
    field.lower_case_name = field.name;
    field.data_type = std::make_shared<DataTypeVariant>(0, false);
    field.children = {make_binary_field_schema("metadata", false), typed_value};
    uint64_t next_id = 10;
    field.assign_ids(next_id);
    return field;
}

FieldSchema make_variant_field_with_root_typed_only_array() {
    FieldSchema element_n = make_int32_field_schema("n");

    FieldSchema element;
    element.name = "element";
    element.lower_case_name = element.name;
    element.children = {element_n};
    element.data_type = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {element_n.data_type}, Strings {"n"}));

    FieldSchema typed_value;
    typed_value.name = "typed_value";
    typed_value.lower_case_name = typed_value.name;
    typed_value.children = {element};
    typed_value.data_type = make_nullable(std::make_shared<DataTypeArray>(element.data_type));

    FieldSchema field;
    field.name = "v";
    field.lower_case_name = field.name;
    field.data_type = std::make_shared<DataTypeVariant>(0, false);
    field.children = {make_binary_field_schema("metadata", false), typed_value};
    uint64_t next_id = 10;
    field.assign_ids(next_id);
    return field;
}

FieldSchema make_variant_field_with_typed_only_map_field() {
    FieldSchema key = make_binary_field_schema("key", false);

    FieldSchema value_n = make_int32_field_schema("n");
    FieldSchema value;
    value.name = "value";
    value.lower_case_name = value.name;
    value.children = {value_n};
    value.data_type = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {value_n.data_type}, Strings {"n"}));

    FieldSchema attrs;
    attrs.name = "attrs";
    attrs.lower_case_name = attrs.name;
    attrs.children = {key, value};
    attrs.data_type = make_nullable(std::make_shared<DataTypeMap>(key.data_type, value.data_type));

    FieldSchema typed_value;
    typed_value.name = "typed_value";
    typed_value.lower_case_name = typed_value.name;
    typed_value.children = {attrs};
    typed_value.data_type = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {attrs.data_type}, Strings {"attrs"}));

    FieldSchema field;
    field.name = "v";
    field.lower_case_name = field.name;
    field.data_type = std::make_shared<DataTypeVariant>(0, false);
    field.children = {make_binary_field_schema("metadata", false), typed_value};
    uint64_t next_id = 10;
    field.assign_ids(next_id);
    return field;
}

FieldSchema make_variant_field_with_value_only_residual_field() {
    FieldSchema metric;
    metric.name = "metric";
    metric.lower_case_name = metric.name;
    metric.children = {make_binary_field_schema("value", true)};
    metric.data_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {metric.children[0].data_type}, Strings {"value"}));

    FieldSchema typed_value;
    typed_value.name = "typed_value";
    typed_value.lower_case_name = typed_value.name;
    typed_value.children = {metric};
    typed_value.data_type = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {metric.data_type}, Strings {"metric"}));

    FieldSchema field;
    field.name = "v";
    field.lower_case_name = field.name;
    field.data_type = std::make_shared<DataTypeVariant>(0, false);
    field.children = {make_binary_field_schema("metadata", false),
                      make_binary_field_schema("value", true), typed_value};
    uint64_t next_id = 10;
    field.assign_ids(next_id);
    return field;
}

FieldSchema make_variant_field_with_partially_shredded_metric() {
    FieldSchema metric_x = make_int32_field_schema("x");

    FieldSchema metric_typed_value;
    metric_typed_value.name = "typed_value";
    metric_typed_value.lower_case_name = metric_typed_value.name;
    metric_typed_value.children = {metric_x};
    metric_typed_value.data_type = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {metric_x.data_type}, Strings {"x"}));

    FieldSchema metric;
    metric.name = "metric";
    metric.lower_case_name = metric.name;
    metric.children = {make_binary_field_schema("value", true), metric_typed_value};
    metric.data_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {metric.children[0].data_type, metric_typed_value.data_type},
            Strings {"value", "typed_value"}));

    FieldSchema typed_value;
    typed_value.name = "typed_value";
    typed_value.lower_case_name = typed_value.name;
    typed_value.children = {metric};
    typed_value.data_type = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {metric.data_type}, Strings {"metric"}));

    FieldSchema field;
    field.name = "v";
    field.lower_case_name = field.name;
    field.data_type = std::make_shared<DataTypeVariant>(0, false);
    field.children = {make_binary_field_schema("metadata", false),
                      make_binary_field_schema("value", true), typed_value};
    uint64_t next_id = 10;
    field.assign_ids(next_id);
    return field;
}

FieldSchema make_variant_field_with_numeric_key_field_id_collision() {
    FieldSchema metric = make_int32_field_schema("metric");
    metric.field_id = 20;

    FieldSchema typed_value;
    typed_value.name = "typed_value";
    typed_value.lower_case_name = typed_value.name;
    typed_value.children = {metric};
    typed_value.data_type = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {metric.data_type}, Strings {"metric"}));

    FieldSchema field;
    field.name = "v";
    field.lower_case_name = field.name;
    field.data_type = std::make_shared<DataTypeVariant>(0, false);
    field.children = {make_binary_field_schema("metadata", false),
                      make_binary_field_schema("value", true), typed_value};
    uint64_t next_id = 10;
    field.assign_ids(next_id);
    return field;
}

FieldSchema make_optional_array_field_for_pruning() {
    FieldSchema element_n = make_int32_field_schema("n");
    element_n.field_id = 102;

    FieldSchema element;
    element.name = "element";
    element.lower_case_name = element.name;
    element.field_id = 101;
    element.children = {element_n};
    element.data_type = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {element_n.data_type}, Strings {"n"}));

    FieldSchema field;
    field.name = "items";
    field.lower_case_name = field.name;
    field.field_id = 100;
    field.children = {element};
    field.data_type = make_nullable(std::make_shared<DataTypeArray>(element.data_type));
    uint64_t next_id = 10;
    field.assign_ids(next_id);
    return field;
}

FieldSchema make_optional_map_field_for_pruning() {
    FieldSchema key = make_binary_field_schema("key", false);
    key.field_id = 101;

    FieldSchema value_n = make_int32_field_schema("n");
    value_n.field_id = 103;
    FieldSchema value;
    value.name = "value";
    value.lower_case_name = value.name;
    value.field_id = 102;
    value.children = {value_n};
    value.data_type = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {value_n.data_type}, Strings {"n"}));

    FieldSchema field;
    field.name = "attrs";
    field.lower_case_name = field.name;
    field.field_id = 100;
    field.children = {key, value};
    field.data_type = make_nullable(std::make_shared<DataTypeMap>(key.data_type, value.data_type));
    uint64_t next_id = 10;
    field.assign_ids(next_id);
    return field;
}

FieldSchema make_struct_with_optional_map_field_for_pruning() {
    FieldSchema attrs = make_optional_map_field_for_pruning();

    FieldSchema field;
    field.name = "s";
    field.lower_case_name = field.name;
    field.field_id = 200;
    field.children = {attrs};
    field.data_type = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {attrs.data_type}, Strings {"attrs"}));
    uint64_t next_id = 20;
    field.assign_ids(next_id);
    return field;
}

FieldSchema make_struct_with_optional_array_field_for_pruning() {
    FieldSchema items = make_optional_array_field_for_pruning();

    FieldSchema field;
    field.name = "s";
    field.lower_case_name = field.name;
    field.field_id = 200;
    field.children = {items};
    field.data_type = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {items.data_type}, Strings {"items"}));
    uint64_t next_id = 20;
    field.assign_ids(next_id);
    return field;
}

const FieldSchema& child_by_name(const FieldSchema& field, const std::string& name) {
    for (const auto& child : field.children) {
        if (child.name == name) {
            return child;
        }
    }
    throw Exception(Status::InternalError("missing test child {}", name));
}

std::set<uint64_t> collect_ids(const FieldSchema& field,
                               const std::vector<TColumnAccessPath>& access_paths) {
    std::set<uint64_t> ids;
    process_nested_access_paths(
            &field, access_paths, ids,
            [](const FieldSchema* field) { return field->get_column_id(); },
            [](const FieldSchema* field) { return field->get_max_column_id(); },
            [](const FieldSchema&, const std::vector<std::vector<std::string>>&,
               std::set<uint64_t>&) { FAIL() << "full projection should not call extractor"; });
    return ids;
}

void expect_map_offset_only_ids(const FieldSchema& field, const std::set<uint64_t>& ids) {
    const auto& key = child_by_name(field, "key");
    const auto& value = child_by_name(field, "value");
    const auto& value_n = child_by_name(value, "n");
    EXPECT_TRUE(ids.contains(field.get_column_id()));
    EXPECT_TRUE(ids.contains(key.get_column_id()));
    EXPECT_FALSE(ids.contains(value.get_column_id()));
    EXPECT_FALSE(ids.contains(value_n.get_column_id()));
}

void expect_array_offset_only_ids(const FieldSchema& field, const std::set<uint64_t>& ids) {
    const auto& element = child_by_name(field, "element");
    const auto& element_n = child_by_name(element, "n");
    EXPECT_TRUE(ids.contains(field.get_column_id()));
    EXPECT_FALSE(ids.contains(element.get_column_id()));
    EXPECT_FALSE(ids.contains(element_n.get_column_id()));
}

void expect_nested_array_offset_only_ids(const FieldSchema& field, const std::set<uint64_t>& ids) {
    const auto& items = child_by_name(field, "items");
    const auto& element = child_by_name(items, "element");
    const auto& element_n = child_by_name(element, "n");
    EXPECT_TRUE(ids.contains(field.get_column_id()));
    EXPECT_TRUE(ids.contains(items.get_column_id()));
    EXPECT_FALSE(ids.contains(element.get_column_id()));
    EXPECT_FALSE(ids.contains(element_n.get_column_id()));
}

void expect_nested_map_offset_only_ids(const FieldSchema& field, const std::set<uint64_t>& ids) {
    const auto& attrs = child_by_name(field, "attrs");
    const auto& key = child_by_name(attrs, "key");
    const auto& value = child_by_name(attrs, "value");
    const auto& value_n = child_by_name(value, "n");
    EXPECT_TRUE(ids.contains(field.get_column_id()));
    EXPECT_TRUE(ids.contains(attrs.get_column_id()));
    EXPECT_TRUE(ids.contains(key.get_column_id()));
    EXPECT_FALSE(ids.contains(value.get_column_id()));
    EXPECT_FALSE(ids.contains(value_n.get_column_id()));
}

} // namespace

TEST(NestedColumnAccessHelperTest, EmptyAccessPathsSelectFullFieldRange) {
    const auto field = make_variant_field_for_access_path_test();
    const std::set<uint64_t> expected {10, 11, 12, 13, 14, 15, 16};
    EXPECT_EQ(collect_ids(field, {}), expected);
}

TEST(NestedColumnAccessHelperTest, NoRecognizedAccessPathsDoNotSelectFieldRange) {
    const auto field = make_variant_field_for_access_path_test();
    TColumnAccessPath ignored_path;
    ignored_path.__set_type(static_cast<TAccessPathType::type>(0));

    const std::set<uint64_t> expected;
    EXPECT_EQ(collect_ids(field, {ignored_path}), expected);
}

TEST(NestedColumnAccessHelperTest, GenericParquetPruningUnwrapsOptionalVariant) {
    auto field = make_variant_field_with_typed_only_nested_shredded_object();
    field.data_type = make_nullable(field.data_type);

    std::set<uint64_t> ids;
    ParquetNestedColumnUtils::extract_nested_column_ids_by_name(field, {{"nested", "x"}}, ids);

    const auto& metadata = child_by_name(field, "metadata");
    const auto& top_typed_value = child_by_name(field, "typed_value");
    const auto& nested = child_by_name(top_typed_value, "nested");
    const auto& nested_typed_value = child_by_name(nested, "typed_value");
    const auto& nested_x = child_by_name(nested_typed_value, "x");
    EXPECT_TRUE(ids.contains(field.get_column_id()));
    EXPECT_FALSE(ids.contains(metadata.get_column_id()));
    EXPECT_TRUE(ids.contains(top_typed_value.get_column_id()));
    EXPECT_TRUE(ids.contains(nested.get_column_id()));
    EXPECT_TRUE(ids.contains(nested_typed_value.get_column_id()));
    EXPECT_TRUE(ids.contains(nested_x.get_column_id()));
}

TEST(NestedColumnAccessHelperTest, GenericParquetPruningUnwrapsOptionalArray) {
    const auto field = make_optional_array_field_for_pruning();
    std::set<uint64_t> ids;
    ParquetNestedColumnUtils::extract_nested_column_ids_by_name(field, {{"*", "n"}}, ids);

    const auto& element = child_by_name(field, "element");
    const auto& element_n = child_by_name(element, "n");
    EXPECT_TRUE(ids.contains(field.get_column_id()));
    EXPECT_TRUE(ids.contains(element.get_column_id()));
    EXPECT_TRUE(ids.contains(element_n.get_column_id()));
}

TEST(NestedColumnAccessHelperTest, GenericParquetArrayOffsetOnlyKeepsArrayContainer) {
    const auto field = make_optional_array_field_for_pruning();
    std::set<uint64_t> ids;
    ParquetNestedColumnUtils::extract_nested_column_ids_by_name(field, {{"OFFSET"}}, ids);

    expect_array_offset_only_ids(field, ids);
}

TEST(NestedColumnAccessHelperTest, GenericParquetNestedArrayOffsetOnlyKeepsArrayContainer) {
    const auto field = make_struct_with_optional_array_field_for_pruning();
    std::set<uint64_t> ids;
    ParquetNestedColumnUtils::extract_nested_column_ids_by_name(field, {{"items", "OFFSET"}}, ids);

    expect_nested_array_offset_only_ids(field, ids);
}

TEST(NestedColumnAccessHelperTest, GenericParquetPruningUnwrapsOptionalMap) {
    const auto field = make_optional_map_field_for_pruning();
    std::set<uint64_t> ids;
    ParquetNestedColumnUtils::extract_nested_column_ids_by_name(field, {{"*", "n"}}, ids);

    const auto& key = child_by_name(field, "key");
    const auto& value = child_by_name(field, "value");
    const auto& value_n = child_by_name(value, "n");
    EXPECT_TRUE(ids.contains(field.get_column_id()));
    EXPECT_TRUE(ids.contains(key.get_column_id()));
    EXPECT_TRUE(ids.contains(value.get_column_id()));
    EXPECT_TRUE(ids.contains(value_n.get_column_id()));
}

TEST(NestedColumnAccessHelperTest, GenericParquetMapOffsetOnlyKeepsKeyReference) {
    const auto field = make_optional_map_field_for_pruning();
    std::set<uint64_t> ids;
    ParquetNestedColumnUtils::extract_nested_column_ids_by_name(field, {{"OFFSET"}}, ids);

    expect_map_offset_only_ids(field, ids);
}

TEST(NestedColumnAccessHelperTest, GenericParquetMapNullOnlyKeepsKeyReference) {
    const auto field = make_optional_map_field_for_pruning();
    std::set<uint64_t> ids;
    ParquetNestedColumnUtils::extract_nested_column_ids_by_name(field, {{"NULL"}}, ids);

    expect_map_offset_only_ids(field, ids);
}

TEST(NestedColumnAccessHelperTest, GenericParquetNestedMapOffsetOnlyKeepsKeyReference) {
    const auto field = make_struct_with_optional_map_field_for_pruning();
    std::set<uint64_t> ids;
    ParquetNestedColumnUtils::extract_nested_column_ids_by_name(field, {{"attrs", "OFFSET"}}, ids);

    expect_nested_map_offset_only_ids(field, ids);
}

TEST(NestedColumnAccessHelperTest, GenericParquetNestedMapNullOnlyKeepsKeyReference) {
    const auto field = make_struct_with_optional_map_field_for_pruning();
    std::set<uint64_t> ids;
    ParquetNestedColumnUtils::extract_nested_column_ids_by_name(field, {{"attrs", "NULL"}}, ids);

    expect_nested_map_offset_only_ids(field, ids);
}

TEST(NestedColumnAccessHelperTest, HiveArrayOffsetOnlyKeepsArrayContainer) {
    const auto field = make_optional_array_field_for_pruning();
    std::set<uint64_t> ids;
    HiveParquetNestedColumnUtils::extract_nested_column_ids(field, {{"OFFSET"}}, ids);

    expect_array_offset_only_ids(field, ids);
}

TEST(NestedColumnAccessHelperTest, HiveNestedArrayOffsetOnlyKeepsArrayContainer) {
    const auto field = make_struct_with_optional_array_field_for_pruning();
    std::set<uint64_t> ids;
    HiveParquetNestedColumnUtils::extract_nested_column_ids(field, {{"items", "OFFSET"}}, ids);

    expect_nested_array_offset_only_ids(field, ids);
}

TEST(NestedColumnAccessHelperTest, IcebergArrayOffsetOnlyKeepsArrayContainer) {
    const auto field = make_optional_array_field_for_pruning();
    std::set<uint64_t> ids;
    IcebergParquetNestedColumnUtils::extract_nested_column_ids(field, {{"OFFSET"}}, ids);

    expect_array_offset_only_ids(field, ids);
}

TEST(NestedColumnAccessHelperTest, IcebergNestedArrayOffsetOnlyKeepsArrayContainer) {
    const auto field = make_struct_with_optional_array_field_for_pruning();
    const auto& items = child_by_name(field, "items");
    std::set<uint64_t> ids;
    IcebergParquetNestedColumnUtils::extract_nested_column_ids(
            field, {{std::to_string(items.field_id), "OFFSET"}}, ids);

    expect_nested_array_offset_only_ids(field, ids);
}

TEST(NestedColumnAccessHelperTest, HiveMapOffsetOnlyKeepsKeyReference) {
    const auto field = make_optional_map_field_for_pruning();
    std::set<uint64_t> ids;
    HiveParquetNestedColumnUtils::extract_nested_column_ids(field, {{"OFFSET"}}, ids);

    expect_map_offset_only_ids(field, ids);
}

TEST(NestedColumnAccessHelperTest, HiveMapNullOnlyKeepsKeyReference) {
    const auto field = make_optional_map_field_for_pruning();
    std::set<uint64_t> ids;
    HiveParquetNestedColumnUtils::extract_nested_column_ids(field, {{"NULL"}}, ids);

    expect_map_offset_only_ids(field, ids);
}

TEST(NestedColumnAccessHelperTest, HiveNestedMapOffsetOnlyKeepsKeyReference) {
    const auto field = make_struct_with_optional_map_field_for_pruning();
    std::set<uint64_t> ids;
    HiveParquetNestedColumnUtils::extract_nested_column_ids(field, {{"attrs", "OFFSET"}}, ids);

    expect_nested_map_offset_only_ids(field, ids);
}

TEST(NestedColumnAccessHelperTest, HiveNestedMapNullOnlyKeepsKeyReference) {
    const auto field = make_struct_with_optional_map_field_for_pruning();
    std::set<uint64_t> ids;
    HiveParquetNestedColumnUtils::extract_nested_column_ids(field, {{"attrs", "NULL"}}, ids);

    expect_nested_map_offset_only_ids(field, ids);
}

TEST(NestedColumnAccessHelperTest, IcebergMapOffsetOnlyKeepsKeyReference) {
    const auto field = make_optional_map_field_for_pruning();
    std::set<uint64_t> ids;
    IcebergParquetNestedColumnUtils::extract_nested_column_ids(field, {{"OFFSET"}}, ids);

    expect_map_offset_only_ids(field, ids);
}

TEST(NestedColumnAccessHelperTest, IcebergMapNullOnlyKeepsKeyReference) {
    const auto field = make_optional_map_field_for_pruning();
    std::set<uint64_t> ids;
    IcebergParquetNestedColumnUtils::extract_nested_column_ids(field, {{"NULL"}}, ids);

    expect_map_offset_only_ids(field, ids);
}

TEST(NestedColumnAccessHelperTest, IcebergNestedMapOffsetOnlyKeepsKeyReference) {
    const auto field = make_struct_with_optional_map_field_for_pruning();
    const auto& attrs = child_by_name(field, "attrs");
    std::set<uint64_t> ids;
    IcebergParquetNestedColumnUtils::extract_nested_column_ids(
            field, {{std::to_string(attrs.field_id), "OFFSET"}}, ids);

    expect_nested_map_offset_only_ids(field, ids);
}

TEST(NestedColumnAccessHelperTest, IcebergNestedMapNullOnlyKeepsKeyReference) {
    const auto field = make_struct_with_optional_map_field_for_pruning();
    const auto& attrs = child_by_name(field, "attrs");
    std::set<uint64_t> ids;
    IcebergParquetNestedColumnUtils::extract_nested_column_ids(
            field, {{std::to_string(attrs.field_id), "NULL"}}, ids);

    expect_nested_map_offset_only_ids(field, ids);
}

TEST(NestedColumnAccessHelperTest, HiveVariantPruningKeepsNestedStructuralNameUserKey) {
    const auto field = make_variant_field_with_nested_structural_name_keys();
    std::set<uint64_t> ids;
    HiveParquetNestedColumnUtils::extract_nested_column_ids(field, {{"nested", "typed_value"}},
                                                            ids);

    const auto& metadata = child_by_name(field, "metadata");
    const auto& top_typed_value = child_by_name(field, "typed_value");
    const auto& nested = child_by_name(top_typed_value, "nested");
    const auto& nested_typed_value = child_by_name(nested, "typed_value");
    const auto& nested_value = child_by_name(nested, "value");
    EXPECT_TRUE(ids.contains(field.get_column_id()));
    EXPECT_TRUE(ids.contains(top_typed_value.get_column_id()));
    EXPECT_TRUE(ids.contains(nested.get_column_id()));
    EXPECT_TRUE(ids.contains(nested_typed_value.get_column_id()));
    EXPECT_FALSE(ids.contains(metadata.get_column_id()));
    EXPECT_FALSE(ids.contains(nested_value.get_column_id()));
}

TEST(NestedColumnAccessHelperTest, IcebergVariantPruningKeepsNestedStructuralNameUserKey) {
    const auto field = make_variant_field_with_nested_structural_name_keys();
    std::set<uint64_t> ids;
    IcebergParquetNestedColumnUtils::extract_nested_column_ids(field, {{"nested", "typed_value"}},
                                                               ids);

    const auto& metadata = child_by_name(field, "metadata");
    const auto& top_typed_value = child_by_name(field, "typed_value");
    const auto& nested = child_by_name(top_typed_value, "nested");
    const auto& nested_typed_value = child_by_name(nested, "typed_value");
    const auto& nested_value = child_by_name(nested, "value");
    EXPECT_TRUE(ids.contains(field.get_column_id()));
    EXPECT_TRUE(ids.contains(top_typed_value.get_column_id()));
    EXPECT_TRUE(ids.contains(nested.get_column_id()));
    EXPECT_TRUE(ids.contains(nested_typed_value.get_column_id()));
    EXPECT_FALSE(ids.contains(metadata.get_column_id()));
    EXPECT_FALSE(ids.contains(nested_value.get_column_id()));
}

TEST(NestedColumnAccessHelperTest, HiveVariantPruningKeepsAnnotatedValueUserField) {
    const auto field = make_variant_field_with_annotated_value_user_field();
    std::set<uint64_t> ids;
    HiveParquetNestedColumnUtils::extract_nested_column_ids(field, {{"obj", "value"}}, ids);

    const auto& metadata = child_by_name(field, "metadata");
    const auto& top_typed_value = child_by_name(field, "typed_value");
    const auto& object = child_by_name(top_typed_value, "obj");
    const auto& object_value = child_by_name(object, "value");
    EXPECT_TRUE(ids.contains(field.get_column_id()));
    EXPECT_FALSE(ids.contains(metadata.get_column_id()));
    EXPECT_TRUE(ids.contains(top_typed_value.get_column_id()));
    EXPECT_TRUE(ids.contains(object.get_column_id()));
    EXPECT_TRUE(ids.contains(object_value.get_column_id()));
}

TEST(NestedColumnAccessHelperTest, IcebergVariantPruningKeepsAnnotatedValueUserField) {
    const auto field = make_variant_field_with_annotated_value_user_field();
    std::set<uint64_t> ids;
    IcebergParquetNestedColumnUtils::extract_nested_column_ids(field, {{"obj", "value"}}, ids);

    const auto& metadata = child_by_name(field, "metadata");
    const auto& top_typed_value = child_by_name(field, "typed_value");
    const auto& object = child_by_name(top_typed_value, "obj");
    const auto& object_value = child_by_name(object, "value");
    EXPECT_TRUE(ids.contains(field.get_column_id()));
    EXPECT_FALSE(ids.contains(metadata.get_column_id()));
    EXPECT_TRUE(ids.contains(top_typed_value.get_column_id()));
    EXPECT_TRUE(ids.contains(object.get_column_id()));
    EXPECT_TRUE(ids.contains(object_value.get_column_id()));
}

TEST(NestedColumnAccessHelperTest, HiveVariantPruningSkipsTypedOnlyNestedMissingKey) {
    const auto field = make_variant_field_with_typed_only_nested_shredded_object();
    std::set<uint64_t> ids;
    HiveParquetNestedColumnUtils::extract_nested_column_ids(field, {{"nested", "missing"}}, ids);

    const auto& metadata = child_by_name(field, "metadata");
    const auto& top_typed_value = child_by_name(field, "typed_value");
    const auto& nested = child_by_name(top_typed_value, "nested");
    const auto& nested_typed_value = child_by_name(nested, "typed_value");
    const auto& nested_x = child_by_name(nested_typed_value, "x");
    EXPECT_TRUE(ids.contains(field.get_column_id()));
    EXPECT_TRUE(ids.contains(metadata.get_column_id()));
    EXPECT_FALSE(ids.contains(top_typed_value.get_column_id()));
    EXPECT_FALSE(ids.contains(nested.get_column_id()));
    EXPECT_FALSE(ids.contains(nested_typed_value.get_column_id()));
    EXPECT_FALSE(ids.contains(nested_x.get_column_id()));
}

TEST(NestedColumnAccessHelperTest, IcebergVariantPruningSkipsTypedOnlyNestedMissingKey) {
    const auto field = make_variant_field_with_typed_only_nested_shredded_object();
    std::set<uint64_t> ids;
    IcebergParquetNestedColumnUtils::extract_nested_column_ids(field, {{"nested", "missing"}}, ids);

    const auto& metadata = child_by_name(field, "metadata");
    const auto& top_typed_value = child_by_name(field, "typed_value");
    const auto& nested = child_by_name(top_typed_value, "nested");
    const auto& nested_typed_value = child_by_name(nested, "typed_value");
    const auto& nested_x = child_by_name(nested_typed_value, "x");
    EXPECT_TRUE(ids.contains(field.get_column_id()));
    EXPECT_TRUE(ids.contains(metadata.get_column_id()));
    EXPECT_FALSE(ids.contains(top_typed_value.get_column_id()));
    EXPECT_FALSE(ids.contains(nested.get_column_id()));
    EXPECT_FALSE(ids.contains(nested_typed_value.get_column_id()));
    EXPECT_FALSE(ids.contains(nested_x.get_column_id()));
}

TEST(NestedColumnAccessHelperTest, HiveVariantPruningStripsTerminalMetaSuffix) {
    const auto field = make_variant_field_with_typed_only_nested_shredded_object();
    std::set<uint64_t> ids;
    HiveParquetNestedColumnUtils::extract_nested_column_ids(
            field, {{"nested", "x", "NULL"}, {"nested", "x", "OFFSET"}}, ids);

    const auto& metadata = child_by_name(field, "metadata");
    const auto& top_typed_value = child_by_name(field, "typed_value");
    const auto& nested = child_by_name(top_typed_value, "nested");
    const auto& nested_typed_value = child_by_name(nested, "typed_value");
    const auto& nested_x = child_by_name(nested_typed_value, "x");
    EXPECT_TRUE(ids.contains(field.get_column_id()));
    EXPECT_FALSE(ids.contains(metadata.get_column_id()));
    EXPECT_TRUE(ids.contains(top_typed_value.get_column_id()));
    EXPECT_TRUE(ids.contains(nested.get_column_id()));
    EXPECT_TRUE(ids.contains(nested_typed_value.get_column_id()));
    EXPECT_TRUE(ids.contains(nested_x.get_column_id()));
}

TEST(NestedColumnAccessHelperTest, GenericParquetVariantPruningStripsTerminalMetaSuffix) {
    const auto field = make_variant_field_with_typed_only_nested_shredded_object();
    std::set<uint64_t> ids;
    ParquetNestedColumnUtils::extract_nested_column_ids_by_name(
            field, {{"nested", "x", "NULL"}, {"nested", "x", "OFFSET"}}, ids);

    const auto& metadata = child_by_name(field, "metadata");
    const auto& top_typed_value = child_by_name(field, "typed_value");
    const auto& nested = child_by_name(top_typed_value, "nested");
    const auto& nested_typed_value = child_by_name(nested, "typed_value");
    const auto& nested_x = child_by_name(nested_typed_value, "x");
    EXPECT_TRUE(ids.contains(field.get_column_id()));
    EXPECT_FALSE(ids.contains(metadata.get_column_id()));
    EXPECT_TRUE(ids.contains(top_typed_value.get_column_id()));
    EXPECT_TRUE(ids.contains(nested.get_column_id()));
    EXPECT_TRUE(ids.contains(nested_typed_value.get_column_id()));
    EXPECT_TRUE(ids.contains(nested_x.get_column_id()));
}

TEST(NestedColumnAccessHelperTest, IcebergVariantPruningStripsTerminalMetaSuffix) {
    const auto field = make_variant_field_with_typed_only_nested_shredded_object();
    std::set<uint64_t> ids;
    IcebergParquetNestedColumnUtils::extract_nested_column_ids(
            field, {{"nested", "x", "NULL"}, {"nested", "x", "OFFSET"}}, ids);

    const auto& metadata = child_by_name(field, "metadata");
    const auto& top_typed_value = child_by_name(field, "typed_value");
    const auto& nested = child_by_name(top_typed_value, "nested");
    const auto& nested_typed_value = child_by_name(nested, "typed_value");
    const auto& nested_x = child_by_name(nested_typed_value, "x");
    EXPECT_TRUE(ids.contains(field.get_column_id()));
    EXPECT_FALSE(ids.contains(metadata.get_column_id()));
    EXPECT_TRUE(ids.contains(top_typed_value.get_column_id()));
    EXPECT_TRUE(ids.contains(nested.get_column_id()));
    EXPECT_TRUE(ids.contains(nested_typed_value.get_column_id()));
    EXPECT_TRUE(ids.contains(nested_x.get_column_id()));
}

TEST(NestedColumnAccessHelperTest, HiveVariantPruningSelectsValueOnlyResidualField) {
    const auto field = make_variant_field_with_value_only_residual_field();
    std::set<uint64_t> ids;
    HiveParquetNestedColumnUtils::extract_nested_column_ids(field, {{"metric", "x"}}, ids);

    const auto& metadata = child_by_name(field, "metadata");
    const auto& top_value = child_by_name(field, "value");
    const auto& top_typed_value = child_by_name(field, "typed_value");
    const auto& metric = child_by_name(top_typed_value, "metric");
    const auto& metric_value = child_by_name(metric, "value");
    EXPECT_TRUE(ids.contains(field.get_column_id()));
    EXPECT_TRUE(ids.contains(metadata.get_column_id()));
    EXPECT_FALSE(ids.contains(top_value.get_column_id()));
    EXPECT_TRUE(ids.contains(top_typed_value.get_column_id()));
    EXPECT_TRUE(ids.contains(metric.get_column_id()));
    EXPECT_TRUE(ids.contains(metric_value.get_column_id()));
}

TEST(NestedColumnAccessHelperTest, HiveVariantPruningKeepsResidualForTypedNestedField) {
    const auto field = make_variant_field_with_partially_shredded_metric();
    std::set<uint64_t> ids;
    HiveParquetNestedColumnUtils::extract_nested_column_ids(field, {{"metric", "x"}}, ids);

    const auto& metadata = child_by_name(field, "metadata");
    const auto& top_value = child_by_name(field, "value");
    const auto& top_typed_value = child_by_name(field, "typed_value");
    const auto& metric = child_by_name(top_typed_value, "metric");
    const auto& metric_value = child_by_name(metric, "value");
    const auto& metric_typed_value = child_by_name(metric, "typed_value");
    const auto& metric_x = child_by_name(metric_typed_value, "x");
    EXPECT_TRUE(ids.contains(field.get_column_id()));
    EXPECT_TRUE(ids.contains(metadata.get_column_id()));
    EXPECT_FALSE(ids.contains(top_value.get_column_id()));
    EXPECT_TRUE(ids.contains(top_typed_value.get_column_id()));
    EXPECT_TRUE(ids.contains(metric.get_column_id()));
    EXPECT_TRUE(ids.contains(metric_value.get_column_id()));
    EXPECT_TRUE(ids.contains(metric_typed_value.get_column_id()));
    EXPECT_TRUE(ids.contains(metric_x.get_column_id()));
}

TEST(NestedColumnAccessHelperTest, HiveVariantPruningMapsArraySubscriptToTypedElement) {
    const auto field = make_variant_field_with_typed_only_array_field();
    std::set<uint64_t> ids;
    HiveParquetNestedColumnUtils::extract_nested_column_ids(field, {{"items", "1", "n"}}, ids);

    const auto& metadata = child_by_name(field, "metadata");
    const auto& top_typed_value = child_by_name(field, "typed_value");
    const auto& items = child_by_name(top_typed_value, "items");
    const auto& items_typed_value = child_by_name(items, "typed_value");
    const auto& element = child_by_name(items_typed_value, "element");
    const auto& element_n = child_by_name(element, "n");
    EXPECT_TRUE(ids.contains(field.get_column_id()));
    EXPECT_FALSE(ids.contains(metadata.get_column_id()));
    EXPECT_TRUE(ids.contains(top_typed_value.get_column_id()));
    EXPECT_TRUE(ids.contains(items.get_column_id()));
    EXPECT_TRUE(ids.contains(items_typed_value.get_column_id()));
    EXPECT_TRUE(ids.contains(element.get_column_id()));
    EXPECT_TRUE(ids.contains(element_n.get_column_id()));
}

TEST(NestedColumnAccessHelperTest, IcebergVariantPruningMapsArraySubscriptToTypedElement) {
    const auto field = make_variant_field_with_typed_only_array_field();
    std::set<uint64_t> ids;
    IcebergParquetNestedColumnUtils::extract_nested_column_ids(field, {{"items", "1", "n"}}, ids);

    const auto& metadata = child_by_name(field, "metadata");
    const auto& top_typed_value = child_by_name(field, "typed_value");
    const auto& items = child_by_name(top_typed_value, "items");
    const auto& items_typed_value = child_by_name(items, "typed_value");
    const auto& element = child_by_name(items_typed_value, "element");
    const auto& element_n = child_by_name(element, "n");
    EXPECT_TRUE(ids.contains(field.get_column_id()));
    EXPECT_FALSE(ids.contains(metadata.get_column_id()));
    EXPECT_TRUE(ids.contains(top_typed_value.get_column_id()));
    EXPECT_TRUE(ids.contains(items.get_column_id()));
    EXPECT_TRUE(ids.contains(items_typed_value.get_column_id()));
    EXPECT_TRUE(ids.contains(element.get_column_id()));
    EXPECT_TRUE(ids.contains(element_n.get_column_id()));
}

TEST(NestedColumnAccessHelperTest, HiveVariantPruningMapsArrayElementPathToTypedElement) {
    const auto field = make_variant_field_with_typed_only_array_field();
    std::set<uint64_t> ids;
    HiveParquetNestedColumnUtils::extract_nested_column_ids(field, {{"items", "n"}}, ids);

    const auto& metadata = child_by_name(field, "metadata");
    const auto& top_typed_value = child_by_name(field, "typed_value");
    const auto& items = child_by_name(top_typed_value, "items");
    const auto& items_typed_value = child_by_name(items, "typed_value");
    const auto& element = child_by_name(items_typed_value, "element");
    const auto& element_n = child_by_name(element, "n");
    EXPECT_TRUE(ids.contains(field.get_column_id()));
    EXPECT_FALSE(ids.contains(metadata.get_column_id()));
    EXPECT_TRUE(ids.contains(top_typed_value.get_column_id()));
    EXPECT_TRUE(ids.contains(items.get_column_id()));
    EXPECT_TRUE(ids.contains(items_typed_value.get_column_id()));
    EXPECT_TRUE(ids.contains(element.get_column_id()));
    EXPECT_TRUE(ids.contains(element_n.get_column_id()));
}

TEST(NestedColumnAccessHelperTest, IcebergVariantPruningMapsArrayElementPathToTypedElement) {
    const auto field = make_variant_field_with_typed_only_array_field();
    std::set<uint64_t> ids;
    IcebergParquetNestedColumnUtils::extract_nested_column_ids(field, {{"items", "n"}}, ids);

    const auto& metadata = child_by_name(field, "metadata");
    const auto& top_typed_value = child_by_name(field, "typed_value");
    const auto& items = child_by_name(top_typed_value, "items");
    const auto& items_typed_value = child_by_name(items, "typed_value");
    const auto& element = child_by_name(items_typed_value, "element");
    const auto& element_n = child_by_name(element, "n");
    EXPECT_TRUE(ids.contains(field.get_column_id()));
    EXPECT_FALSE(ids.contains(metadata.get_column_id()));
    EXPECT_TRUE(ids.contains(top_typed_value.get_column_id()));
    EXPECT_TRUE(ids.contains(items.get_column_id()));
    EXPECT_TRUE(ids.contains(items_typed_value.get_column_id()));
    EXPECT_TRUE(ids.contains(element.get_column_id()));
    EXPECT_TRUE(ids.contains(element_n.get_column_id()));
}

TEST(NestedColumnAccessHelperTest, HiveVariantPruningMapsRootArraySubscriptToTypedElement) {
    const auto field = make_variant_field_with_root_typed_only_array();
    std::set<uint64_t> ids;
    HiveParquetNestedColumnUtils::extract_nested_column_ids(field, {{"1", "n"}}, ids);

    const auto& metadata = child_by_name(field, "metadata");
    const auto& top_typed_value = child_by_name(field, "typed_value");
    const auto& element = child_by_name(top_typed_value, "element");
    const auto& element_n = child_by_name(element, "n");
    EXPECT_TRUE(ids.contains(field.get_column_id()));
    EXPECT_FALSE(ids.contains(metadata.get_column_id()));
    EXPECT_TRUE(ids.contains(top_typed_value.get_column_id()));
    EXPECT_TRUE(ids.contains(element.get_column_id()));
    EXPECT_TRUE(ids.contains(element_n.get_column_id()));
}

TEST(NestedColumnAccessHelperTest, IcebergVariantPruningMapsRootArraySubscriptToTypedElement) {
    const auto field = make_variant_field_with_root_typed_only_array();
    std::set<uint64_t> ids;
    IcebergParquetNestedColumnUtils::extract_nested_column_ids(field, {{"1", "n"}}, ids);

    const auto& metadata = child_by_name(field, "metadata");
    const auto& top_typed_value = child_by_name(field, "typed_value");
    const auto& element = child_by_name(top_typed_value, "element");
    const auto& element_n = child_by_name(element, "n");
    EXPECT_TRUE(ids.contains(field.get_column_id()));
    EXPECT_FALSE(ids.contains(metadata.get_column_id()));
    EXPECT_TRUE(ids.contains(top_typed_value.get_column_id()));
    EXPECT_TRUE(ids.contains(element.get_column_id()));
    EXPECT_TRUE(ids.contains(element_n.get_column_id()));
}

TEST(NestedColumnAccessHelperTest, HiveVariantPruningMapsTypedMapKeyToValueSubtree) {
    const auto field = make_variant_field_with_typed_only_map_field();
    std::set<uint64_t> ids;
    HiveParquetNestedColumnUtils::extract_nested_column_ids(field, {{"attrs", "k", "n"}}, ids);

    const auto& metadata = child_by_name(field, "metadata");
    const auto& top_typed_value = child_by_name(field, "typed_value");
    const auto& attrs = child_by_name(top_typed_value, "attrs");
    const auto& key = child_by_name(attrs, "key");
    const auto& value = child_by_name(attrs, "value");
    const auto& value_n = child_by_name(value, "n");
    EXPECT_TRUE(ids.contains(field.get_column_id()));
    EXPECT_FALSE(ids.contains(metadata.get_column_id()));
    EXPECT_TRUE(ids.contains(top_typed_value.get_column_id()));
    EXPECT_TRUE(ids.contains(attrs.get_column_id()));
    EXPECT_TRUE(ids.contains(key.get_column_id()));
    EXPECT_TRUE(ids.contains(value.get_column_id()));
    EXPECT_TRUE(ids.contains(value_n.get_column_id()));
}

TEST(NestedColumnAccessHelperTest, IcebergVariantPruningMapsTypedMapKeyToValueSubtree) {
    const auto field = make_variant_field_with_typed_only_map_field();
    std::set<uint64_t> ids;
    IcebergParquetNestedColumnUtils::extract_nested_column_ids(field, {{"attrs", "k", "n"}}, ids);

    const auto& metadata = child_by_name(field, "metadata");
    const auto& top_typed_value = child_by_name(field, "typed_value");
    const auto& attrs = child_by_name(top_typed_value, "attrs");
    const auto& key = child_by_name(attrs, "key");
    const auto& value = child_by_name(attrs, "value");
    const auto& value_n = child_by_name(value, "n");
    EXPECT_TRUE(ids.contains(field.get_column_id()));
    EXPECT_FALSE(ids.contains(metadata.get_column_id()));
    EXPECT_TRUE(ids.contains(top_typed_value.get_column_id()));
    EXPECT_TRUE(ids.contains(attrs.get_column_id()));
    EXPECT_TRUE(ids.contains(key.get_column_id()));
    EXPECT_TRUE(ids.contains(value.get_column_id()));
    EXPECT_TRUE(ids.contains(value_n.get_column_id()));
}

TEST(NestedColumnAccessHelperTest, IcebergVariantPruningSelectsValueOnlyResidualField) {
    const auto field = make_variant_field_with_value_only_residual_field();
    std::set<uint64_t> ids;
    IcebergParquetNestedColumnUtils::extract_nested_column_ids(field, {{"metric", "x"}}, ids);

    const auto& metadata = child_by_name(field, "metadata");
    const auto& top_value = child_by_name(field, "value");
    const auto& top_typed_value = child_by_name(field, "typed_value");
    const auto& metric = child_by_name(top_typed_value, "metric");
    const auto& metric_value = child_by_name(metric, "value");
    EXPECT_TRUE(ids.contains(field.get_column_id()));
    EXPECT_TRUE(ids.contains(metadata.get_column_id()));
    EXPECT_FALSE(ids.contains(top_value.get_column_id()));
    EXPECT_TRUE(ids.contains(top_typed_value.get_column_id()));
    EXPECT_TRUE(ids.contains(metric.get_column_id()));
    EXPECT_TRUE(ids.contains(metric_value.get_column_id()));
}

TEST(NestedColumnAccessHelperTest, IcebergVariantPruningKeepsResidualForTypedNestedField) {
    const auto field = make_variant_field_with_partially_shredded_metric();
    std::set<uint64_t> ids;
    IcebergParquetNestedColumnUtils::extract_nested_column_ids(field, {{"metric", "x"}}, ids);

    const auto& metadata = child_by_name(field, "metadata");
    const auto& top_value = child_by_name(field, "value");
    const auto& top_typed_value = child_by_name(field, "typed_value");
    const auto& metric = child_by_name(top_typed_value, "metric");
    const auto& metric_value = child_by_name(metric, "value");
    const auto& metric_typed_value = child_by_name(metric, "typed_value");
    const auto& metric_x = child_by_name(metric_typed_value, "x");
    EXPECT_TRUE(ids.contains(field.get_column_id()));
    EXPECT_TRUE(ids.contains(metadata.get_column_id()));
    EXPECT_FALSE(ids.contains(top_value.get_column_id()));
    EXPECT_TRUE(ids.contains(top_typed_value.get_column_id()));
    EXPECT_TRUE(ids.contains(metric.get_column_id()));
    EXPECT_TRUE(ids.contains(metric_value.get_column_id()));
    EXPECT_TRUE(ids.contains(metric_typed_value.get_column_id()));
    EXPECT_TRUE(ids.contains(metric_x.get_column_id()));
}

TEST(NestedColumnAccessHelperTest, IcebergVariantPruningTreatsNumericKeyAsNameNotFieldId) {
    const auto field = make_variant_field_with_numeric_key_field_id_collision();
    std::set<uint64_t> ids;
    IcebergParquetNestedColumnUtils::extract_nested_column_ids(field, {{"20"}}, ids);

    const auto& metadata = child_by_name(field, "metadata");
    const auto& top_value = child_by_name(field, "value");
    const auto& top_typed_value = child_by_name(field, "typed_value");
    const auto& metric = child_by_name(top_typed_value, "metric");
    EXPECT_TRUE(ids.contains(field.get_column_id()));
    EXPECT_TRUE(ids.contains(metadata.get_column_id()));
    EXPECT_TRUE(ids.contains(top_value.get_column_id()));
    EXPECT_FALSE(ids.contains(top_typed_value.get_column_id()));
    EXPECT_FALSE(ids.contains(metric.get_column_id()));
}

} // namespace doris
