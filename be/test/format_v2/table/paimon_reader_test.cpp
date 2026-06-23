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

#include "format_v2/table/paimon_reader.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <gtest/gtest.h>
#include <parquet/api/reader.h>
#include <parquet/arrow/writer.h>

#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/field.h"
#include "exec/common/endian.h"
#include "format/format_common.h"
#include "format_v2/column_data.h"
#include "gen_cpp/ExternalTableSchema_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "io/io_common.h"
#include "roaring/roaring.hh"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"

namespace doris::format {
namespace {

DataTypePtr table_type(const DataTypePtr& type) {
    return type->is_nullable() ? type : make_nullable(type);
}

ColumnDefinition make_table_column(int32_t id, const std::string& name, const DataTypePtr& type) {
    ColumnDefinition column;
    column.identifier = Field::create_field<TYPE_INT>(id);
    column.name = name;
    column.type = table_type(type);
    return column;
}

ColumnDefinition make_file_column(int32_t id, const std::string& name, const DataTypePtr& type) {
    ColumnDefinition column;
    column.identifier = Field::create_field<TYPE_INT>(id);
    column.local_id = id;
    column.name = name;
    column.type = type;
    return column;
}

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

schema::external::TFieldPtr external_array_field(std::string name, int32_t id,
                                                 schema::external::TFieldPtr item_field,
                                                 std::vector<std::string> aliases = {}) {
    auto field = external_schema_field(std::move(name), id, std::move(aliases));
    schema::external::TArrayField array_field;
    array_field.__set_item_field(std::move(item_field));
    field.field_ptr->nestedField.__set_array_field(std::move(array_field));
    field.field_ptr->__isset.nestedField = true;
    return field;
}

schema::external::TFieldPtr external_map_field(std::string name, int32_t id,
                                               schema::external::TFieldPtr key_field,
                                               schema::external::TFieldPtr value_field,
                                               std::vector<std::string> aliases = {}) {
    auto field = external_schema_field(std::move(name), id, std::move(aliases));
    schema::external::TMapField map_field;
    map_field.__set_key_field(std::move(key_field));
    map_field.__set_value_field(std::move(value_field));
    field.field_ptr->nestedField.__set_map_field(std::move(map_field));
    field.field_ptr->__isset.nestedField = true;
    return field;
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

Block build_table_block(const std::vector<ColumnDefinition>& columns) {
    Block block;
    for (const auto& column : columns) {
        block.insert({column.type->create_column(), column.type, column.name});
    }
    return block;
}

const IColumn& expect_not_null_nullable_nested_column(const IColumn& column) {
    if (!column.is_nullable()) {
        return column;
    }
    const auto& nullable_column = assert_cast<const ColumnNullable&>(column);
    for (const auto is_null : nullable_column.get_null_map_data()) {
        EXPECT_EQ(is_null, 0);
    }
    return nullable_column.get_nested_column();
}

const IColumn& expect_not_null_table_column(const Block& block, size_t position) {
    return expect_not_null_nullable_nested_column(*block.get_by_position(position).column);
}

std::shared_ptr<arrow::Array> build_int32_array(const std::vector<int32_t>& values) {
    arrow::Int32Builder builder;
    for (const auto value : values) {
        EXPECT_TRUE(builder.Append(value).ok());
    }
    std::shared_ptr<arrow::Array> array;
    EXPECT_TRUE(builder.Finish(&array).ok());
    return array;
}

std::shared_ptr<arrow::Array> build_string_array(const std::vector<std::string>& values) {
    arrow::StringBuilder builder;
    for (const auto& value : values) {
        EXPECT_TRUE(builder.Append(value).ok());
    }
    std::shared_ptr<arrow::Array> array;
    EXPECT_TRUE(builder.Finish(&array).ok());
    return array;
}

void write_int_pair_parquet_file(const std::string& file_path, const std::vector<int32_t>& ids,
                                 const std::vector<int32_t>& scores,
                                 const std::vector<std::string>& values) {
    ASSERT_EQ(ids.size(), scores.size());
    ASSERT_EQ(ids.size(), values.size());
    auto schema = arrow::schema({
            arrow::field("id", arrow::int32(), false),
            arrow::field("score", arrow::int32(), false),
            arrow::field("value", arrow::utf8(), false),
    });
    auto table = arrow::Table::Make(schema, {build_int32_array(ids), build_int32_array(scores),
                                             build_string_array(values)});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out,
                                                      static_cast<int64_t>(ids.size()),
                                                      builder.build()));
}

int64_t write_paimon_deletion_vector_file(const std::string& file_path,
                                          const std::vector<uint32_t>& deleted_positions) {
    roaring::Roaring rows;
    for (const auto position : deleted_positions) {
        rows.add(position);
    }

    const size_t bitmap_size = rows.getSizeInBytes();
    const uint32_t total_length = static_cast<uint32_t>(4 + bitmap_size);
    std::vector<char> blob(4 + total_length);
    BigEndian::Store32(blob.data(), total_length);
    constexpr char PAIMON_BITMAP_MAGIC[] = {'\x5E', '\x43', '\xF2', '\xD0'};
    memcpy(blob.data() + 4, PAIMON_BITMAP_MAGIC, 4);
    rows.write(blob.data() + 8);

    std::ofstream output(file_path, std::ios::binary);
    EXPECT_TRUE(output.is_open());
    output.write(blob.data(), static_cast<std::streamsize>(blob.size()));
    EXPECT_TRUE(output.good());
    // Paimon DeletionFile.length is magic + bitmap length, excluding the leading length field.
    return static_cast<int64_t>(total_length);
}

TFileScanRangeParams make_local_parquet_scan_params() {
    TFileScanRangeParams scan_params;
    scan_params.__set_file_type(TFileType::FILE_LOCAL);
    scan_params.__set_format_type(TFileFormatType::FORMAT_PARQUET);
    return scan_params;
}

std::shared_ptr<io::IOContext> make_io_context(io::FileReaderStats* file_reader_stats,
                                               io::FileCacheStatistics* file_cache_stats) {
    auto io_ctx = std::make_shared<io::IOContext>();
    io_ctx->file_reader_stats = file_reader_stats;
    io_ctx->file_cache_stats = file_cache_stats;
    return io_ctx;
}

SplitReadOptions build_split_options(const std::string& file_path) {
    SplitReadOptions options;
    options.current_range.__set_path(file_path);
    options.current_range.__set_file_size(
            static_cast<int64_t>(std::filesystem::file_size(file_path)));
    return options;
}

TTableFormatFileDesc make_paimon_table_format_desc(const std::string& deletion_file_path,
                                                   int64_t offset, int64_t length) {
    TTableFormatFileDesc table_format_params;
    TPaimonFileDesc paimon_params;
    paimon_params.__set_file_format("parquet");
    TPaimonDeletionFileDesc deletion_file;
    deletion_file.__set_path(deletion_file_path);
    deletion_file.__set_offset(offset);
    deletion_file.__set_length(length);
    paimon_params.__set_deletion_file(deletion_file);
    table_format_params.__set_paimon_params(paimon_params);
    return table_format_params;
}

TTableFormatFileDesc make_paimon_schema_table_format_desc(int64_t schema_id) {
    TTableFormatFileDesc table_format_params;
    table_format_params.__set_table_format_type("paimon");
    TPaimonFileDesc paimon_params;
    paimon_params.__set_file_format("parquet");
    paimon_params.__set_schema_id(schema_id);
    table_format_params.__set_paimon_params(paimon_params);
    return table_format_params;
}

// Scenario: PaimonReader shares Hudi's history-schema annotation path. A split whose schema id
// resolves to a historical schema should use field-id mapping and annotate array/map children so
// TableColumnMapper can match evolved physical Parquet columns by id instead of by the old names.
TEST(PaimonReaderTest, AnnotatesArrayAndMapFileSchemaFromSplitHistorySchema) {
    TFileScanRangeParams scan_params;
    scan_params.__set_current_schema_id(200);
    scan_params.__set_history_schema_info({
            external_schema(
                    100,
                    {external_array_field("old_tags", 30,
                                          external_schema_field("old_item", 31, {"tag"}), {"tags"}),
                     external_map_field(
                             "old_props", 40, external_schema_field("old_key", 41, {"key"}),
                             external_schema_field("old_value", 42, {"score"}), {"props"})}),
            external_schema(
                    200, {external_schema_field("tags", 30), external_schema_field("props", 40)}),
    });

    paimon::PaimonReader reader;
    reader.TEST_set_scan_params(&scan_params);

    SplitReadOptions split_options;
    split_options.current_range.__set_table_format_params(
            make_paimon_schema_table_format_desc(100));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());
    EXPECT_EQ(reader.TEST_mapping_mode(), TableColumnMappingMode::BY_FIELD_ID);

    const auto string_type = std::make_shared<DataTypeString>();
    const auto int_type = std::make_shared<DataTypeInt32>();

    auto tags = make_file_column(0, "old_tags", std::make_shared<DataTypeArray>(string_type));
    tags.children = {make_file_column(0, "old_item", string_type)};

    auto props =
            make_file_column(1, "old_props", std::make_shared<DataTypeMap>(string_type, int_type));
    props.children = {make_file_column(0, "old_key", string_type),
                      make_file_column(1, "old_value", int_type)};

    std::vector<ColumnDefinition> file_schema {tags, props};
    ASSERT_TRUE(reader.TEST_annotate_file_schema(&file_schema).ok());

    ASSERT_EQ(file_schema.size(), 2);
    EXPECT_EQ(file_schema[0].get_identifier_field_id(), 30);
    EXPECT_EQ(file_schema[0].name_mapping, std::vector<std::string>({"tags"}));
    ASSERT_EQ(file_schema[0].children.size(), 1);
    EXPECT_EQ(file_schema[0].children[0].get_identifier_field_id(), 31);
    EXPECT_EQ(file_schema[0].children[0].name_mapping, std::vector<std::string>({"tag"}));

    EXPECT_EQ(file_schema[1].get_identifier_field_id(), 40);
    EXPECT_EQ(file_schema[1].name_mapping, std::vector<std::string>({"props"}));
    ASSERT_EQ(file_schema[1].children.size(), 2);
    EXPECT_EQ(file_schema[1].children[0].get_identifier_field_id(), 41);
    EXPECT_EQ(file_schema[1].children[0].name_mapping, std::vector<std::string>({"key"}));
    EXPECT_EQ(file_schema[1].children[1].get_identifier_field_id(), 42);
    EXPECT_EQ(file_schema[1].children[1].name_mapping, std::vector<std::string>({"score"}));
}

// Scenario: when FE does not send a matching historical schema for the split schema id, Paimon must
// stay on BY_NAME mapping and must not rewrite the file schema identifiers.
TEST(PaimonReaderTest, FallsBackToByNameWhenSplitHistorySchemaIsMissing) {
    TFileScanRangeParams scan_params;
    scan_params.__set_current_schema_id(200);
    scan_params.__set_history_schema_info({
            external_schema(200, {external_schema_field("name", 10)}),
    });

    paimon::PaimonReader reader;
    reader.TEST_set_scan_params(&scan_params);

    SplitReadOptions split_options;
    split_options.current_range.__set_table_format_params(
            make_paimon_schema_table_format_desc(100));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());
    EXPECT_EQ(reader.TEST_mapping_mode(), TableColumnMappingMode::BY_NAME);

    std::vector<ColumnDefinition> file_schema {
            make_file_column(0, "old_name", std::make_shared<DataTypeString>()),
    };
    ASSERT_TRUE(reader.TEST_annotate_file_schema(&file_schema).ok());
    EXPECT_EQ(file_schema[0].get_identifier_field_id(), 0);
    EXPECT_TRUE(file_schema[0].name_mapping.empty());
}

// Scenario: PaimonReader must clear the previous split schema id before reading a new split. A
// schema-evolved split must not force the following split without schema id to keep BY_FIELD_ID.
TEST(PaimonReaderTest, ResetsSplitSchemaIdBeforePreparingNextSplit) {
    TFileScanRangeParams scan_params;
    scan_params.__set_current_schema_id(200);
    scan_params.__set_history_schema_info({
            external_schema(100, {external_schema_field("old_name", 10, {"name"})}),
            external_schema(200, {external_schema_field("name", 10)}),
    });

    paimon::PaimonReader reader;
    reader.TEST_set_scan_params(&scan_params);

    SplitReadOptions split_with_schema_id;
    split_with_schema_id.current_range.__set_table_format_params(
            make_paimon_schema_table_format_desc(100));
    ASSERT_TRUE(reader.prepare_split(split_with_schema_id).ok());
    EXPECT_EQ(reader.TEST_mapping_mode(), TableColumnMappingMode::BY_FIELD_ID);

    SplitReadOptions split_without_schema_id;
    TTableFormatFileDesc table_format_params;
    table_format_params.__set_table_format_type("paimon");
    table_format_params.__set_paimon_params(TPaimonFileDesc {});
    split_without_schema_id.current_range.__set_table_format_params(table_format_params);
    ASSERT_TRUE(reader.prepare_split(split_without_schema_id).ok());
    EXPECT_EQ(reader.TEST_mapping_mode(), TableColumnMappingMode::BY_NAME);
}

// Scenario: Paimon reader should parse its bitmap deletion vector and let TableReader apply the
// generated row-position delete predicate before returning table rows.
TEST(PaimonReaderTest, AppliesBitmapDeletionVectorFile) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_paimon_deletion_vector_file_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto dv_path = (test_dir / "delete-vector.bin").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3, 4, 5}, {10, 20, 30, 40, 50},
                                {"one", "two", "three", "four", "five"});
    const auto dv_length = write_paimon_deletion_vector_file(dv_path, {0, 4});

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    paimon::PaimonReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = &scan_params,
                                    .io_ctx = io_ctx,
                                    .runtime_state = &state,
                                    .scanner_profile = &profile,
                            })
                        .ok());

    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(
            make_paimon_table_format_desc(dv_path, 0, dv_length));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    std::vector<int32_t> ids;
    bool eos = false;
    while (!eos) {
        Block block = build_table_block(projected_columns);
        ASSERT_TRUE(reader.get_block(&block, &eos).ok());
        if (block.rows() == 0) {
            continue;
        }
        const auto& id_column =
                assert_cast<const ColumnInt32&>(expect_not_null_table_column(block, 0));
        for (size_t row = 0; row < block.rows(); ++row) {
            ids.push_back(id_column.get_element(row));
        }
    }
    EXPECT_EQ(ids, std::vector<int32_t>({2, 3, 4}));

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

} // namespace
} // namespace doris::format
