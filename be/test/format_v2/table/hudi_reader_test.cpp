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

#include "format_v2/table/hudi_reader.h"

#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/field.h"
#include "format_v2/column_data.h"
#include "gen_cpp/ExternalTableSchema_types.h"
#include "gen_cpp/PlanNodes_types.h"

namespace doris::format {
namespace {

schema::external::TFieldPtr external_schema_field(std::string name, int32_t id,
                                                  std::vector<std::string> aliases = {}) {
    auto field = std::make_shared<schema::external::TField>();
    field->__set_name(std::move(name));
    field->__set_id(id);
    if (!aliases.empty()) {
        field->__set_name_mapping(std::move(aliases));
    }
    schema::external::TFieldPtr field_ptr;
    field_ptr.field_ptr = std::move(field);
    field_ptr.__isset.field_ptr = true;
    return field_ptr;
}

schema::external::TSchema external_schema(int64_t schema_id,
                                          std::vector<schema::external::TFieldPtr> fields) {
    schema::external::TStructField root_field;
    root_field.__set_fields(std::move(fields));
    schema::external::TSchema schema;
    schema.__set_schema_id(schema_id);
    schema.__set_root_field(std::move(root_field));
    return schema;
}

ColumnDefinition make_file_column(int32_t id, const std::string& name, const DataTypePtr& type) {
    ColumnDefinition field;
    field.identifier = Field::create_field<TYPE_INT>(id);
    field.local_id = id;
    field.name = name;
    field.type = type;
    return field;
}

TTableFormatFileDesc hudi_table_format_desc(std::optional<int64_t> schema_id) {
    TTableFormatFileDesc table_format_params;
    table_format_params.__set_table_format_type("hudi");
    THudiFileDesc hudi_params;
    if (schema_id.has_value()) {
        hudi_params.__set_schema_id(*schema_id);
    }
    table_format_params.__set_hudi_params(hudi_params);
    return table_format_params;
}

// Scenario: FileScannerV2 Hudi native reader uses the split schema id to annotate the physical
// file schema before TableColumnMapper runs. This keeps schema-evolved Hudi files on field-id
// mapping, including renamed nested children.
TEST(HudiReaderTest, AnnotatesFileSchemaFromSplitHistorySchema) {
    TFileScanRangeParams scan_params;
    scan_params.__set_current_schema_id(200);

    auto profile_field = external_schema_field("profile", 20);
    schema::external::TStructField profile_struct;
    profile_struct.__set_fields({external_schema_field("old_age", 21, {"age"})});
    profile_field.field_ptr->nestedField.__set_struct_field(std::move(profile_struct));
    profile_field.field_ptr->__isset.nestedField = true;

    scan_params.__set_history_schema_info({
            external_schema(100, {external_schema_field("old_name", 10, {"name"}), profile_field}),
            external_schema(
                    200, {external_schema_field("name", 10), external_schema_field("profile", 20)}),
    });

    hudi::HudiReader reader;
    reader.TEST_set_scan_params(&scan_params);

    SplitReadOptions split_options;
    split_options.current_range.__set_table_format_params(hudi_table_format_desc(100));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());
    EXPECT_EQ(reader.TEST_mapping_mode(), TableColumnMappingMode::BY_FIELD_ID);

    auto string_type = std::make_shared<DataTypeString>();
    auto int_type = std::make_shared<DataTypeInt32>();
    auto profile_type = std::make_shared<DataTypeStruct>(DataTypes {int_type}, Strings {"old_age"});
    auto profile_column = make_file_column(1, "profile", profile_type);
    profile_column.children = {make_file_column(0, "old_age", int_type)};
    std::vector<ColumnDefinition> file_schema {
            make_file_column(0, "old_name", string_type),
            profile_column,
    };

    ASSERT_TRUE(reader.TEST_annotate_file_schema(&file_schema).ok());
    ASSERT_EQ(file_schema.size(), 2);
    EXPECT_EQ(file_schema[0].get_identifier_field_id(), 10);
    EXPECT_EQ(file_schema[0].name_mapping, std::vector<std::string>({"name"}));
    EXPECT_EQ(file_schema[1].get_identifier_field_id(), 20);
    ASSERT_EQ(file_schema[1].children.size(), 1);
    EXPECT_EQ(file_schema[1].children[0].get_identifier_field_id(), 21);
    EXPECT_EQ(file_schema[1].children[0].name_mapping, std::vector<std::string>({"age"}));
}

// Scenario: a Hudi split can only use field-id mapping when its schema id resolves to a historical
// schema sent by FE. Unknown or missing split schema ids must fall back to BY_NAME and leave the
// physical file schema untouched.
TEST(HudiReaderTest, FallsBackToByNameWhenSplitHistorySchemaIsMissing) {
    TFileScanRangeParams scan_params;
    scan_params.__set_current_schema_id(200);
    scan_params.__set_history_schema_info({
            external_schema(200, {external_schema_field("name", 10)}),
    });

    hudi::HudiReader reader;
    reader.TEST_set_scan_params(&scan_params);

    SplitReadOptions split_options;
    split_options.current_range.__set_table_format_params(hudi_table_format_desc(100));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());
    EXPECT_EQ(reader.TEST_mapping_mode(), TableColumnMappingMode::BY_NAME);

    std::vector<ColumnDefinition> file_schema {
            make_file_column(0, "old_name", std::make_shared<DataTypeString>()),
    };
    ASSERT_TRUE(reader.TEST_annotate_file_schema(&file_schema).ok());
    EXPECT_EQ(file_schema[0].get_identifier_field_id(), 0);
    EXPECT_TRUE(file_schema[0].name_mapping.empty());
}

// Scenario: HudiReader must reset the previous split schema id before each split. Otherwise a
// BY_FIELD_ID split could leak its schema id into the next split that carries no schema id.
TEST(HudiReaderTest, ResetsSplitSchemaIdBeforePreparingNextSplit) {
    TFileScanRangeParams scan_params;
    scan_params.__set_current_schema_id(200);
    scan_params.__set_history_schema_info({
            external_schema(100, {external_schema_field("old_name", 10, {"name"})}),
            external_schema(200, {external_schema_field("name", 10)}),
    });

    hudi::HudiReader reader;
    reader.TEST_set_scan_params(&scan_params);

    SplitReadOptions split_with_schema_id;
    split_with_schema_id.current_range.__set_table_format_params(hudi_table_format_desc(100));
    ASSERT_TRUE(reader.prepare_split(split_with_schema_id).ok());
    EXPECT_EQ(reader.TEST_mapping_mode(), TableColumnMappingMode::BY_FIELD_ID);

    SplitReadOptions split_without_schema_id;
    split_without_schema_id.current_range.__set_table_format_params(
            hudi_table_format_desc(std::nullopt));
    ASSERT_TRUE(reader.prepare_split(split_without_schema_id).ok());
    EXPECT_EQ(reader.TEST_mapping_mode(), TableColumnMappingMode::BY_NAME);
}

} // namespace
} // namespace doris::format
