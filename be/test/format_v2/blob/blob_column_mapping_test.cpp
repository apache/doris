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

#include "core/data_type/data_type_file.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "format_v2/column_mapper.h"
#include "format_v2/table_reader.h"

namespace doris::format {
namespace {

DataTypePtr string_type() {
    return std::make_shared<DataTypeString>();
}

ColumnDefinition column_definition_with_field_id(const std::string& name, int32_t field_id,
                                                 DataTypePtr type, int32_t local_id = -1) {
    ColumnDefinition column;
    column.identifier = Field::create_field<TYPE_INT>(field_id);
    column.local_id = local_id;
    column.name = name;
    column.type = std::move(type);
    return column;
}

TEST(TableColumnMapperTest, BlobMaterializationKeepsPhysicalSourceAndLogicalType) {
    auto table_column_definition = column_definition_with_field_id(
            "renamed_payload", 7, make_nullable(std::make_shared<DataTypeFile>()));
    auto file_column_definition =
            column_definition_with_field_id("payload", 7, make_nullable(string_type()), 3);
    TableColumnMapperOptions options;
    options.table_reader_materialized_blob_columns.emplace(
            GlobalIndex(0), BlobMaterializationInput::PHYSICAL_COLUMN);
    TableColumnMapper mapper(options);
    ASSERT_TRUE(
            mapper.create_mapping({table_column_definition}, {}, {file_column_definition}).ok());
    FileScanRequest file_scan_request;
    ASSERT_TRUE(mapper.create_scan_request({}, {table_column_definition}, &file_scan_request).ok());
    const auto& mapping = mapper.mappings().at(0);
    EXPECT_TRUE(mapping.requires_blob_materialization);
    EXPECT_EQ(mapping.file_local_id.value(), 3);
    EXPECT_TRUE(mapping.table_type->equals(*table_column_definition.type));
    EXPECT_TRUE(mapping.file_type->equals(*file_column_definition.type));
    EXPECT_FALSE(mapping.is_trivial);
    EXPECT_EQ(mapping.filter_conversion, FilterConversionType::FINALIZE_ONLY);
    EXPECT_EQ(mapping.projection, nullptr);
    ASSERT_EQ(file_scan_request.non_predicate_columns.size(), 1);
    EXPECT_EQ(file_scan_request.non_predicate_columns[0].column_id(), LocalColumnId(3));
}

TEST(TableColumnMapperTest, MissingBlobColumnDoesNotInventPhysicalData) {
    auto table_column_definition = column_definition_with_field_id(
            "payload", 7, make_nullable(std::make_shared<DataTypeFile>()));
    TableColumnMapperOptions options;
    options.table_reader_materialized_blob_columns.emplace(
            GlobalIndex(0), BlobMaterializationInput::PHYSICAL_COLUMN);
    TableColumnMapper mapper(options);
    ASSERT_TRUE(mapper.create_mapping({table_column_definition}, {}, {}).ok());
    FileScanRequest file_scan_request;
    ASSERT_TRUE(mapper.create_scan_request({}, {table_column_definition}, &file_scan_request).ok());
    EXPECT_TRUE(mapper.mappings()[0].requires_blob_materialization);
    EXPECT_FALSE(mapper.mappings()[0].file_local_id.has_value());
    EXPECT_TRUE(file_scan_request.non_predicate_columns.empty());
}

TEST(TableColumnMapperTest, BlobMaterializationIsBoundByOutputIndexRatherThanName) {
    auto blob_column_definition = column_definition_with_field_id(
            "same_name", 7, make_nullable(std::make_shared<DataTypeFile>()));
    auto ordinary_column_definition =
            column_definition_with_field_id("same_name", 8, string_type());
    auto file_column_definition = column_definition_with_field_id("same_name", 8, string_type(), 4);
    TableColumnMapperOptions options;
    options.table_reader_materialized_blob_columns.emplace(GlobalIndex(0),
                                                           BlobMaterializationInput::NONE);
    TableColumnMapper mapper(options);
    ASSERT_TRUE(mapper.create_mapping({blob_column_definition, ordinary_column_definition}, {},
                                      {file_column_definition})
                        .ok());
    FileScanRequest file_scan_request;
    ASSERT_TRUE(mapper.create_scan_request({}, {blob_column_definition, ordinary_column_definition},
                                           &file_scan_request)
                        .ok());
    EXPECT_TRUE(mapper.mappings()[0].requires_blob_materialization);
    EXPECT_FALSE(mapper.mappings()[0].file_local_id.has_value());
    EXPECT_FALSE(mapper.mappings()[0].constant_index.has_value());
    EXPECT_EQ(mapper.mappings()[0].filter_conversion, FilterConversionType::FINALIZE_ONLY);
    EXPECT_FALSE(mapper.mappings()[1].requires_blob_materialization);
    EXPECT_EQ(mapper.mappings()[1].file_local_id, 4);
    ASSERT_EQ(file_scan_request.non_predicate_columns.size(), 1);
    EXPECT_EQ(file_scan_request.non_predicate_columns[0].column_id(), LocalColumnId(4));
}

} // namespace
} // namespace doris::format
