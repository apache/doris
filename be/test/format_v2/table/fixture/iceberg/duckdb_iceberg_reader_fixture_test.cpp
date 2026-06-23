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
#include <rapidjson/document.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/field.h"
#include "format_v2/table/iceberg_reader.h"
#include "format_v2/table_reader.h"
#include "gen_cpp/ExternalTableSchema_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "io/io_common.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"

namespace doris::format {
namespace {

struct IcebergFixtureField {
    int32_t id = -1;
    std::string name;
    std::string iceberg_type;
};

struct IcebergFixtureMetadata {
    int32_t format_version = 0;
    int64_t current_schema_id = -1;
    std::string manifest_list_path;
    std::vector<IcebergFixtureField> fields;
};

std::string duckdb_big_query_error_fixture_root() {
    // This fixture is copied from duckdb/duckdb-iceberg:data/persistent/big_query_error.
    constexpr const char* relative_path =
            "be/test/format_v2/test_data/fixture/iceberg/duckdb/big_query_error";
    if (std::filesystem::exists(relative_path)) {
        return std::string(relative_path);
    }
    if (const auto* doris_home = std::getenv("DORIS_HOME"); doris_home != nullptr) {
        return (std::filesystem::path(doris_home) / relative_path).string();
    }
    return std::string(relative_path);
}

std::string duckdb_big_query_error_data_file_path(const std::string& fixture_root) {
    return (std::filesystem::path(fixture_root) / "data" /
            "bc12018c-b4ed-4ed8-adcb-a41cea51206d-3175699a3a6392ba-f-00000-of-00001.parquet")
            .string();
}

std::string duckdb_big_query_error_metadata_file_path(const std::string& fixture_root) {
    return (std::filesystem::path(fixture_root) / "metadata" / "v1749025517.metadata.json")
            .string();
}

DataTypePtr iceberg_type_to_doris_type(const std::string& iceberg_type) {
    if (iceberg_type == "long") {
        return std::make_shared<DataTypeInt64>();
    }
    if (iceberg_type == "string") {
        return std::make_shared<DataTypeString>();
    }
    if (iceberg_type == "timestamp") {
        return std::make_shared<DataTypeDateTimeV2>(6);
    }
    return nullptr;
}

Status load_iceberg_fixture_metadata(const std::string& metadata_path,
                                     IcebergFixtureMetadata* metadata) {
    DORIS_CHECK(metadata != nullptr);
    std::ifstream input(metadata_path);
    if (!input.is_open()) {
        return Status::InvalidArgument("Failed to open Iceberg metadata file '{}'", metadata_path);
    }
    const std::string json((std::istreambuf_iterator<char>(input)),
                           std::istreambuf_iterator<char>());

    rapidjson::Document document;
    document.Parse(json.c_str());
    if (document.HasParseError() || !document.IsObject()) {
        return Status::InvalidArgument("Failed to parse Iceberg metadata file '{}'", metadata_path);
    }
    if (!document.HasMember("format-version") || !document["format-version"].IsInt()) {
        return Status::InvalidArgument("Iceberg metadata '{}' has no integer format-version",
                                       metadata_path);
    }
    if (!document.HasMember("current-schema-id") || !document["current-schema-id"].IsInt64()) {
        return Status::InvalidArgument("Iceberg metadata '{}' has no integer current-schema-id",
                                       metadata_path);
    }
    if (!document.HasMember("schemas") || !document["schemas"].IsArray()) {
        return Status::InvalidArgument("Iceberg metadata '{}' has no schemas array", metadata_path);
    }
    if (!document.HasMember("snapshots") || !document["snapshots"].IsArray() ||
        document["snapshots"].Empty()) {
        return Status::InvalidArgument("Iceberg metadata '{}' has no snapshots array",
                                       metadata_path);
    }

    metadata->format_version = document["format-version"].GetInt();
    metadata->current_schema_id = document["current-schema-id"].GetInt64();
    const auto& snapshot = document["snapshots"].GetArray()[0];
    if (!snapshot.IsObject() || !snapshot.HasMember("manifest-list") ||
        !snapshot["manifest-list"].IsString()) {
        return Status::InvalidArgument("Iceberg metadata '{}' first snapshot has no manifest-list",
                                       metadata_path);
    }
    metadata->manifest_list_path = snapshot["manifest-list"].GetString();

    const rapidjson::Value* current_schema = nullptr;
    for (const auto& schema : document["schemas"].GetArray()) {
        if (schema.IsObject() && schema.HasMember("schema-id") && schema["schema-id"].IsInt64() &&
            schema["schema-id"].GetInt64() == metadata->current_schema_id) {
            current_schema = &schema;
            break;
        }
    }
    if (current_schema == nullptr) {
        return Status::InvalidArgument(
                "Iceberg metadata '{}' does not contain current schema id {}", metadata_path,
                metadata->current_schema_id);
    }
    if (!current_schema->HasMember("fields") || !(*current_schema)["fields"].IsArray()) {
        return Status::InvalidArgument("Iceberg metadata '{}' current schema has no fields array",
                                       metadata_path);
    }

    metadata->fields.clear();
    for (const auto& field : (*current_schema)["fields"].GetArray()) {
        if (!field.IsObject() || !field.HasMember("id") || !field["id"].IsInt() ||
            !field.HasMember("name") || !field["name"].IsString() || !field.HasMember("type") ||
            !field["type"].IsString()) {
            return Status::InvalidArgument(
                    "Iceberg metadata '{}' contains an unsupported field entry", metadata_path);
        }
        IcebergFixtureField parsed_field {
                .id = field["id"].GetInt(),
                .name = field["name"].GetString(),
                .iceberg_type = field["type"].GetString(),
        };
        if (iceberg_type_to_doris_type(parsed_field.iceberg_type) == nullptr) {
            return Status::InvalidArgument(
                    "Iceberg metadata '{}' contains unsupported field type '{}'", metadata_path,
                    parsed_field.iceberg_type);
        }
        metadata->fields.push_back(std::move(parsed_field));
    }
    if (metadata->fields.empty()) {
        return Status::InvalidArgument("Iceberg metadata '{}' current schema has no fields",
                                       metadata_path);
    }
    return Status::OK();
}

schema::external::TFieldPtr external_schema_field(std::string name, int32_t id) {
    auto field = std::make_shared<schema::external::TField>();
    field->__set_name(std::move(name));
    field->__set_id(id);
    schema::external::TFieldPtr field_ptr;
    field_ptr.__set_field_ptr(field);
    return field_ptr;
}

TFileScanRangeParams duckdb_big_query_error_scan_params(const IcebergFixtureMetadata& metadata) {
    TFileScanRangeParams scan_params;
    scan_params.__set_file_type(TFileType::FILE_LOCAL);
    scan_params.__set_format_type(TFileFormatType::FORMAT_PARQUET);
    scan_params.__set_current_schema_id(metadata.current_schema_id);

    schema::external::TSchema schema;
    schema.__set_schema_id(metadata.current_schema_id);
    schema::external::TStructField root_field;
    std::vector<schema::external::TFieldPtr> fields;
    fields.reserve(metadata.fields.size());
    for (const auto& field : metadata.fields) {
        fields.push_back(external_schema_field(field.name, field.id));
    }
    root_field.__set_fields(fields);
    schema.__set_root_field(root_field);
    scan_params.__set_history_schema_info({schema});
    return scan_params;
}

ColumnDefinition table_column_from_slot(const std::string& name, DataTypePtr type) {
    ColumnDefinition column;
    column.identifier = Field::create_field<TYPE_STRING>(name);
    column.name = name;
    column.type = make_nullable(std::move(type));
    return column;
}

Status build_duckdb_big_query_error_projected_columns(
        const doris::format::iceberg::IcebergTableReader& reader,
        const IcebergFixtureMetadata& metadata, const TFileScanRangeParams& scan_params,
        std::vector<ColumnDefinition>* columns) {
    DORIS_CHECK(columns != nullptr);
    columns->clear();
    for (const auto& field : metadata.fields) {
        auto type = iceberg_type_to_doris_type(field.iceberg_type);
        DORIS_CHECK(type != nullptr);
        columns->push_back(table_column_from_slot(field.name, std::move(type)));
    }

    // In the real FileScannerV2 path, projected table columns are built from SlotDescriptor and
    // then annotated by TableReader with table-format schema metadata from TFileScanRangeParams.
    // This direct TableReader unit test mirrors that step explicitly before calling init().
    ProjectedColumnBuildContext context {
            .scan_params = &scan_params,
    };
    TFileScanSlotInfo slot_info;
    size_t column_idx = 0;
    for (auto& column : *columns) {
        const auto& metadata_field = metadata.fields[column_idx++];
        if (!column.type->is_nullable()) {
            return Status::InvalidArgument("External table column '{}' must be nullable",
                                           column.name);
        }
        context.schema_column.reset();
        RETURN_IF_ERROR(reader.annotate_projected_column(slot_info, &context, &column));
        if (!column.has_identifier_field_id() ||
            column.get_identifier_field_id() != metadata_field.id) {
            return Status::InvalidArgument(
                    "External table column '{}' field id does not match Iceberg metadata id {}",
                    column.name, metadata_field.id);
        }
    }
    return Status::OK();
}

Block build_table_block(const std::vector<ColumnDefinition>& columns) {
    Block block;
    for (const auto& column : columns) {
        block.insert({column.type->create_column(), column.type, column.name});
    }
    return block;
}

SplitReadOptions build_split_options(const std::string& file_path, int32_t format_version) {
    SplitReadOptions options;
    options.current_range.__set_path(file_path);
    options.current_range.__set_file_size(
            static_cast<int64_t>(std::filesystem::file_size(file_path)));
    TTableFormatFileDesc table_format_params;
    TIcebergFileDesc iceberg_params;
    iceberg_params.__set_format_version(format_version);
    iceberg_params.__set_original_file_path(file_path);
    table_format_params.__set_iceberg_params(iceberg_params);
    options.current_range.__set_table_format_params(table_format_params);
    return options;
}

std::shared_ptr<io::IOContext> make_io_context(io::FileReaderStats* file_reader_stats,
                                               io::FileCacheStatistics* file_cache_stats) {
    auto io_ctx = std::make_shared<io::IOContext>();
    io_ctx->file_reader_stats = file_reader_stats;
    io_ctx->file_cache_stats = file_cache_stats;
    return io_ctx;
}

std::string normalize_duckdb_timestamp_output(std::string value) {
    // DuckDB's sqllogictest output omits the fractional part for timestamp values whose
    // microseconds are zero. Doris keeps the projected Iceberg timestamp as DateTimeV2(6), so
    // ColumnWithTypeAndName::to_string() prints the storage scale as ".000000".
    constexpr const char* zero_microsecond_suffix = ".000000";
    constexpr size_t suffix_size = 7;
    if (value.size() >= suffix_size &&
        value.compare(value.size() - suffix_size, suffix_size, zero_microsecond_suffix) == 0) {
        value.resize(value.size() - suffix_size);
    }
    return value;
}

// Read the real DuckDB Iceberg big_query_error fixture through IcebergTableReader. The test derives
// the table schema and field ids from metadata JSON, then uses the data file as the FE-planned split
// because BE TableReader is below Iceberg metadata planning.
TEST(IcebergFixtureTest, ReadsDuckDbBigQueryErrorFixtureDataFile) {
    const auto fixture_root = duckdb_big_query_error_fixture_root();
    const auto file_path = duckdb_big_query_error_data_file_path(fixture_root);
    const auto metadata_path = duckdb_big_query_error_metadata_file_path(fixture_root);
    ASSERT_TRUE(std::filesystem::exists(file_path));
    ASSERT_TRUE(std::filesystem::exists(metadata_path));

    IcebergFixtureMetadata metadata;
    ASSERT_TRUE(load_iceberg_fixture_metadata(metadata_path, &metadata).ok());
    EXPECT_EQ(metadata.format_version, 2);
    EXPECT_EQ(metadata.current_schema_id, 0);
    ASSERT_EQ(metadata.fields.size(), 3);
    EXPECT_EQ(metadata.fields[0].name, "id");
    EXPECT_EQ(metadata.fields[0].id, 1);
    EXPECT_EQ(metadata.fields[0].iceberg_type, "long");
    EXPECT_EQ(metadata.fields[1].name, "name");
    EXPECT_EQ(metadata.fields[1].id, 2);
    EXPECT_EQ(metadata.fields[1].iceberg_type, "string");
    EXPECT_EQ(metadata.fields[2].name, "created_at");
    EXPECT_EQ(metadata.fields[2].id, 3);
    EXPECT_EQ(metadata.fields[2].iceberg_type, "timestamp");
    const auto manifest_list_file = std::filesystem::path(fixture_root) / "metadata" /
                                    std::filesystem::path(metadata.manifest_list_path).filename();
    ASSERT_TRUE(std::filesystem::exists(manifest_list_file));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = duckdb_big_query_error_scan_params(metadata);
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);

    doris::format::iceberg::IcebergTableReader reader;
    std::vector<ColumnDefinition> projected_columns;
    ASSERT_TRUE(build_duckdb_big_query_error_projected_columns(reader, metadata, scan_params,
                                                               &projected_columns)
                        .ok());
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

    ASSERT_TRUE(reader.prepare_split(build_split_options(file_path, metadata.format_version)).ok());

    std::vector<std::vector<std::string>> actual_rows;
    bool eos = false;
    while (!eos) {
        Block block = build_table_block(projected_columns);
        ASSERT_TRUE(reader.get_block(&block, &eos).ok());
        if (block.rows() == 0) {
            continue;
        }
        ASSERT_EQ(block.columns(), projected_columns.size());
        for (size_t column_idx = 0; column_idx < projected_columns.size(); ++column_idx) {
            const auto full_column =
                    block.get_by_position(column_idx).column->convert_to_full_column_if_const();
            ASSERT_EQ(full_column->size(), block.rows());
        }
        for (size_t row_idx = 0; row_idx < block.rows(); ++row_idx) {
            actual_rows.push_back({
                    block.get_by_position(0).to_string(row_idx),
                    block.get_by_position(1).to_string(row_idx),
                    normalize_duckdb_timestamp_output(block.get_by_position(2).to_string(row_idx)),
            });
        }
    }

    const std::vector<std::vector<std::string>> expected_rows {
            {"1", "Alice", "2024-01-01 10:00:00"},
            {"2", "Bob", "2024-02-01 11:30:00"},
    };
    EXPECT_EQ(actual_rows, expected_rows);
    ASSERT_TRUE(reader.close().ok());
}

} // namespace
} // namespace doris::format
