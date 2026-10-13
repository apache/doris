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

#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_timestamptz.h"
#include "core/field.h"
#include "exec/common/endian.h"
#include "exprs/vectorized_fn_call.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"
#include "format/format_common.h"
#include "format/table/deletion_vector_reader.h"
#include "format/table/paimon_reader.h"
#include "format_v2/column_data.h"
#include "format_v2/jni/paimon_jni_reader.h"
#include "gen_cpp/ExternalTableSchema_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "io/io_common.h"
#include "roaring/roaring.hh"
#include "runtime/exec_env.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "storage/options.h"
#include "util/timezone_utils.h"

namespace doris::format {
namespace {

class SlowInitTableReader final : public TableReader {
public:
    Status init(TableReadOptions&& options) override {
        RETURN_IF_ERROR(TableReader::init(std::move(options)));
        SCOPED_TIMER(_profile.total_timer);
        SCOPED_TIMER(_profile.init_timer);
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
        return Status::OK();
    }

    Status prepare_split(const SplitReadOptions&) override {
        SCOPED_TIMER(_profile.total_timer);
        SCOPED_TIMER(_profile.prepare_split_timer);
        return Status::OK();
    }
};

class RefreshTrackingTableReader final : public TableReader {
public:
    Status prepare_split(const SplitReadOptions&) override { return Status::OK(); }

    Status refresh_conjuncts(VExprContextSPtrs conjuncts) override {
        ++refresh_count;
        return TableReader::refresh_conjuncts(std::move(conjuncts));
    }

    int refresh_count = 0;
};

class SplitFormatTrackingTableReader final : public TableReader {
public:
    Status prepare_split(const SplitReadOptions& options) override {
        prepared_format = options.current_split_format;
        return Status::OK();
    }

    FileFormat prepared_format = FileFormat::JNI;
};

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

std::vector<char> build_paimon_deletion_vector_buffer(
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
    return blob;
}

int64_t write_paimon_deletion_vector_file(const std::string& file_path,
                                          const std::vector<uint32_t>& deleted_positions) {
    const auto blob = build_paimon_deletion_vector_buffer(deleted_positions);
    std::ofstream output(file_path, std::ios::binary);
    EXPECT_TRUE(output.is_open());
    output.write(blob.data(), static_cast<std::streamsize>(blob.size()));
    EXPECT_TRUE(output.good());
    // Paimon DeletionFile.length is magic + bitmap length, excluding the leading length field.
    return static_cast<int64_t>(blob.size() - 4);
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

TFileRangeDesc make_paimon_native_range(TFileFormatType::type format_type) {
    TFileRangeDesc range;
    range.__set_path(format_type == TFileFormatType::FORMAT_ORC ? "s3://bucket/native.orc"
                                                                : "s3://bucket/native.parquet");
    range.__set_format_type(format_type);
    TTableFormatFileDesc table_format_params;
    table_format_params.__set_table_format_type("paimon");
    TPaimonFileDesc paimon_params;
    paimon_params.__set_file_format(format_type == TFileFormatType::FORMAT_ORC ? "orc" : "parquet");
    paimon_params.__set_reader_type(TPaimonReaderType::PAIMON_NATIVE);
    table_format_params.__set_paimon_params(paimon_params);
    range.__set_table_format_params(table_format_params);
    return range;
}

TFileRangeDesc make_paimon_jni_range() {
    TFileRangeDesc range;
    range.__set_path("/data-placeholder.parquet");
    range.__set_format_type(TFileFormatType::FORMAT_JNI);
    TTableFormatFileDesc table_format_params;
    table_format_params.__set_table_format_type("paimon");
    TPaimonFileDesc paimon_params;
    paimon_params.__set_file_format("parquet");
    paimon_params.__set_reader_type(TPaimonReaderType::PAIMON_JNI);
    paimon_params.__set_paimon_split("serialized-paimon-split");
    table_format_params.__set_paimon_params(paimon_params);
    range.__set_table_format_params(table_format_params);
    return range;
}

TFileRangeDesc make_legacy_paimon_native_range(TFileFormatType::type physical_format_type) {
    TFileRangeDesc range = make_paimon_native_range(physical_format_type);
    range.__set_format_type(TFileFormatType::FORMAT_JNI);
    range.table_format_params.paimon_params.__isset.reader_type = false;
    return range;
}

TFileScanRangeParams make_paimon_jni_scan_params() {
    TFileScanRangeParams scan_params;
    scan_params.__set_serialized_table("serialized-paimon-table");
    scan_params.__set_serialized_table_cache_key("serialized-paimon-table-cache-key");
    scan_params.__set_paimon_predicate("serialized-paimon-predicate");
    return scan_params;
}

std::map<std::string, std::string> build_paimon_jni_scanner_params(
        TFileScanRangeParams* scan_params, RuntimeState* state) {
    paimon::PaimonJniReader reader;
    reader.TEST_set_scan_params(scan_params);
    reader.TEST_set_runtime_state(state);
    reader.TEST_set_current_range(make_paimon_jni_range());
    std::map<std::string, std::string> params;
    EXPECT_TRUE(reader.TEST_build_scanner_params(&params).ok());
    return params;
}

class ScopedExecEnvStorePaths {
public:
    explicit ScopedExecEnvStorePaths(std::vector<StorePath> store_paths) {
        _current = &const_cast<std::vector<StorePath>&>(ExecEnv::GetInstance()->store_paths());
        _previous = *_current;
        *_current = std::move(store_paths);
    }

    ~ScopedExecEnvStorePaths() { *_current = std::move(_previous); }

private:
    std::vector<StorePath>* _current = nullptr;
    std::vector<StorePath> _previous;
};

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

// Paimon writes precision 7..9 TIMESTAMP and TIMESTAMP_LTZ with the same unannotated INT96
// physical type. The historical Paimon schema must therefore preserve the per-column semantic so
// the native reader can keep TIMESTAMP as a wall clock and decode TIMESTAMP_LTZ as an instant.
TEST(PaimonReaderTest, AnnotatesTimestampSemanticsFromSplitHistorySchema) {
    auto timestamp = external_schema_field("ts", 10);
    timestamp.field_ptr->__set_timestamp_is_adjusted_to_utc(false);
    auto timestamp_ltz = external_schema_field("ts_ltz", 11);
    timestamp_ltz.field_ptr->__set_timestamp_is_adjusted_to_utc(true);

    TFileScanRangeParams scan_params;
    scan_params.__set_current_schema_id(100);
    scan_params.__set_history_schema_info({external_schema(100, {timestamp, timestamp_ltz})});

    paimon::PaimonReader reader;
    reader.TEST_set_scan_params(&scan_params);
    SplitReadOptions split_options;
    split_options.current_range.__set_table_format_params(
            make_paimon_schema_table_format_desc(100));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    const auto datetime_type = make_nullable(std::make_shared<DataTypeDateTimeV2>(6));
    std::vector<ColumnDefinition> file_schema {
            make_file_column(0, "ts", datetime_type),
            make_file_column(1, "ts_ltz", datetime_type),
    };
    ASSERT_TRUE(reader.TEST_annotate_file_schema(&file_schema).ok());

    ASSERT_TRUE(file_schema[0].timestamp_is_adjusted_to_utc.has_value());
    EXPECT_FALSE(*file_schema[0].timestamp_is_adjusted_to_utc);
    EXPECT_EQ(remove_nullable(file_schema[0].type)->get_primitive_type(), TYPE_DATETIMEV2);
    ASSERT_TRUE(file_schema[1].timestamp_is_adjusted_to_utc.has_value());
    EXPECT_TRUE(*file_schema[1].timestamp_is_adjusted_to_utc);
    EXPECT_EQ(remove_nullable(file_schema[1].type)->get_primitive_type(), TYPE_TIMESTAMPTZ);
}

TEST(PaimonReaderTest, RebuildsNestedTimestampTypesAndPreservesNullability) {
    const auto timestamp = std::make_shared<DataTypeDateTimeV2>(3);
    auto instant = make_file_column(2, "instant", make_nullable(timestamp));
    instant.timestamp_is_adjusted_to_utc = true;
    auto wall = make_file_column(3, "wall", timestamp);
    wall.timestamp_is_adjusted_to_utc = false;
    auto map =
            make_file_column(1, "entries", std::make_shared<DataTypeMap>(instant.type, wall.type));
    map.children = {instant, wall};
    auto array =
            make_file_column(0, "events", make_nullable(std::make_shared<DataTypeArray>(map.type)));
    array.children = {map};
    std::vector<ColumnDefinition> schema {array};
    TFileScanRangeParams params;
    paimon::PaimonReader reader;
    reader.TEST_set_scan_params(&params);
    reader.TEST_set_format(FileFormat::PARQUET);
    ASSERT_TRUE(reader.TEST_annotate_file_schema(&schema).ok());
    ASSERT_TRUE(schema[0].type->is_nullable());
    const auto& result_array = assert_cast<const DataTypeArray&>(*remove_nullable(schema[0].type));
    // ARRAY elements use nullable carriers even when the physical child is required.
    ASSERT_TRUE(result_array.get_nested_type()->is_nullable());
    const auto& result_map =
            assert_cast<const DataTypeMap&>(*remove_nullable(result_array.get_nested_type()));
    EXPECT_EQ(remove_nullable(result_map.get_key_type())->get_primitive_type(), TYPE_TIMESTAMPTZ);
    EXPECT_EQ(result_map.get_key_type()->get_scale(), 3);
    EXPECT_EQ(remove_nullable(result_map.get_value_type())->get_primitive_type(), TYPE_DATETIMEV2);
    EXPECT_EQ(result_map.get_value_type()->get_scale(), 3);
    EXPECT_TRUE(schema[0].children[0].children[0].type->is_nullable());
    EXPECT_FALSE(schema[0].children[0].children[1].type->is_nullable());
}

TEST(PaimonReaderTest, RejectsMalformedTimestampContainersBeforeChangingChildren) {
    const auto timestamp = make_nullable(std::make_shared<DataTypeDateTimeV2>(6));
    for (bool is_array : {true, false}) {
        for (size_t count : {0U, 1U, 2U, 3U}) {
            if (count == (is_array ? 1U : 2U)) {
                continue;
            }
            SCOPED_TRACE(std::to_string(count));
            DataTypePtr type =
                    is_array ? DataTypePtr(std::make_shared<DataTypeArray>(timestamp))
                             : DataTypePtr(std::make_shared<DataTypeMap>(timestamp, timestamp));
            auto parent = make_file_column(0, "container", type);
            for (size_t i = 0; i < count; ++i) {
                auto child = make_file_column(i + 1, "child", timestamp);
                child.timestamp_is_adjusted_to_utc = true;
                parent.children.push_back(std::move(child));
            }
            std::vector<ColumnDefinition> schema {parent};
            TFileScanRangeParams params;
            paimon::PaimonReader reader;
            reader.TEST_set_scan_params(&params);
            reader.TEST_set_format(FileFormat::PARQUET);
            const auto status = reader.TEST_annotate_file_schema(&schema);
            EXPECT_FALSE(status.ok());
            for (const auto& child : schema[0].children) {
                EXPECT_EQ(child.type, timestamp);
            }
            EXPECT_EQ(schema[0].type, type);
        }
    }
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

TEST(PaimonReaderTest, DeletionVectorCacheKeyIncludesOffsetAndLength) {
    // Scenario: format_v2 converts Paimon split metadata into a generic DeleteFileDesc. The
    // generated key must preserve offset and length so shared DV files do not collide.
    TTableFormatFileDesc table_format_params;
    table_format_params.__isset.paimon_params = true;
    TPaimonDeletionFileDesc deletion_file;
    deletion_file.__set_path("s3://bucket/table/deletion.dv");
    deletion_file.__set_offset(128);
    deletion_file.__set_length(64);
    table_format_params.paimon_params.__set_deletion_file(deletion_file);

    paimon::PaimonReader reader;
    DeleteFileDesc first_desc;
    bool has_delete_file = false;
    ASSERT_TRUE(reader.TEST_parse_deletion_vector_file(table_format_params, &first_desc,
                                                       &has_delete_file)
                        .ok());
    EXPECT_TRUE(has_delete_file);

    table_format_params.paimon_params.deletion_file.__set_offset(256);
    DeleteFileDesc different_offset_desc;
    ASSERT_TRUE(reader.TEST_parse_deletion_vector_file(table_format_params, &different_offset_desc,
                                                       &has_delete_file)
                        .ok());

    table_format_params.paimon_params.deletion_file.__set_offset(128);
    table_format_params.paimon_params.deletion_file.__set_length(96);
    DeleteFileDesc different_length_desc;
    ASSERT_TRUE(reader.TEST_parse_deletion_vector_file(table_format_params, &different_length_desc,
                                                       &has_delete_file)
                        .ok());

    EXPECT_NE(first_desc.key, different_offset_desc.key);
    EXPECT_NE(first_desc.key, different_length_desc.key);
}

TEST(PaimonReaderTest, DeletionVectorRejectsInvalidRange) {
    auto table_format_params = make_paimon_table_format_desc("dv.bin", -1, 4);

    paimon::PaimonReader reader;
    DeleteFileDesc desc;
    bool has_delete_file = false;
    auto status =
            reader.TEST_parse_deletion_vector_file(table_format_params, &desc, &has_delete_file);

    EXPECT_TRUE(status.is<ErrorCode::DATA_QUALITY_ERROR>());
    EXPECT_NE(status.to_string().find("offset must be non-negative"), std::string::npos);
    EXPECT_FALSE(has_delete_file);
}

TEST(PaimonReaderTest, DecodeDeletionVectorBufferUsesSharedFormatHelper) {
    // Scenario: format_v2 TableReader reads a raw Paimon BitmapDeletionVector range and delegates
    // the binary parsing to the same helper used by the format reader path.
    const auto buffer = build_paimon_deletion_vector_buffer({0, 3, 5});
    DeletionVector deletion_vector;

    ASSERT_TRUE(decode_paimon_deletion_vector_buffer(buffer.data(), buffer.size(), &deletion_vector)
                        .ok());
    EXPECT_EQ(deletion_vector.cardinality(), 3);
    EXPECT_TRUE(deletion_vector.contains(uint64_t {0}));
    EXPECT_TRUE(deletion_vector.contains(uint64_t {3}));
    EXPECT_TRUE(deletion_vector.contains(uint64_t {5}));
}

TEST(PaimonReaderTest, DecodeDeletionVectorBufferRejectsShortBuffer) {
    // Scenario: a truncated Paimon DV must fail before reading the magic or roaring payload.
    const std::vector<char> buffer = {'\0', '\0', '\0', '\4'};
    DeletionVector deletion_vector;

    EXPECT_FALSE(
            decode_paimon_deletion_vector_buffer(buffer.data(), buffer.size(), &deletion_vector)
                    .ok());
}

TEST(PaimonReaderTest, DecodeDeletionVectorBufferRejectsLengthMismatch) {
    // Scenario: a cached or remote Paimon DV range with a mismatched leading length must not be
    // accepted as a valid bitmap.
    auto buffer = build_paimon_deletion_vector_buffer({1, 2});
    BigEndian::Store32(buffer.data(), static_cast<uint32_t>(buffer.size()));
    DeletionVector deletion_vector;

    EXPECT_FALSE(
            decode_paimon_deletion_vector_buffer(buffer.data(), buffer.size(), &deletion_vector)
                    .ok());
}

TEST(PaimonReaderTest, DecodeDeletionVectorBufferRejectsMagicMismatch) {
    // Scenario: format_v2 must reject non-Paimon payloads even when the range length is valid.
    auto buffer = build_paimon_deletion_vector_buffer({1, 2});
    buffer[4] = '\0';
    DeletionVector deletion_vector;

    EXPECT_FALSE(
            decode_paimon_deletion_vector_buffer(buffer.data(), buffer.size(), &deletion_vector)
                    .ok());
}

TEST(PaimonReaderTest, DecodeDeletionVectorBufferRejectsCorruptRoaringBitmap) {
    // Scenario: a valid Paimon DV header with a corrupt roaring body should return a data quality
    // error instead of producing partial delete rows.
    auto buffer = build_paimon_deletion_vector_buffer({1, 2});
    buffer.resize(8);
    BigEndian::Store32(buffer.data(), 4);
    DeletionVector deletion_vector;

    EXPECT_FALSE(
            decode_paimon_deletion_vector_buffer(buffer.data(), buffer.size(), &deletion_vector)
                    .ok());
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

TEST(PaimonReaderTest, NativeDataFilesAreMarkedImmutableForPageCache) {
    paimon::PaimonReader reader;

    for (const auto format : {FileFormat::PARQUET, FileFormat::ORC}) {
        SplitReadOptions split_options;
        split_options.current_split_format = format;
        split_options.current_range.__set_path("paimon-data-file");
        split_options.current_range.__set_table_format_params(
                make_paimon_schema_table_format_desc(100));

        ASSERT_TRUE(reader.prepare_split(split_options).ok());
        EXPECT_TRUE(reader.TEST_current_data_file_is_immutable());
    }
}

// Read real INT96 files through the table mapper: testing ParquetReader alone can hide a stale
// DATETIMEV2 block template by supplying the corrected TIMESTAMPTZ column directly.
// NOLINTNEXTLINE(readability-function-cognitive-complexity): exercise one file across mapping and projection combinations.
TEST(PaimonReaderTest, ReadsInt96HistorySemanticsThroughTableMapper) {
    TimezoneUtils::load_timezones_to_cache();
    const auto path =
            (std::filesystem::temp_directory_path() / "paimon_int96_history.parquet").string();
    const auto arrow_ts = arrow::timestamp(arrow::TimeUnit::MICRO);
    arrow::TimestampBuilder builder(arrow_ts, arrow::default_memory_pool());
    ASSERT_TRUE(builder.Append(0).ok());
    ASSERT_TRUE(builder.Append(123456).ok());
    ASSERT_TRUE(builder.AppendNull().ok());
    std::shared_ptr<arrow::Array> timestamps;
    ASSERT_TRUE(builder.Finish(&timestamps).ok());
    auto nested = arrow::StructArray::Make({timestamps}, {arrow::field("ltz", arrow_ts)});
    ASSERT_TRUE(nested.ok());
    auto table = arrow::Table::Make(
            arrow::schema({arrow::field("id", arrow::int32(), false),
                           arrow::field("wall", arrow_ts), arrow::field("instant", arrow_ts),
                           arrow::field("nested", (*nested)->type())}),
            {build_int32_array({1, 2, 3}), timestamps, timestamps, *nested});
    auto out = arrow::io::FileOutputStream::Open(path);
    ASSERT_TRUE(out.ok());
    ::parquet::ArrowWriterProperties::Builder arrow_props;
    arrow_props.enable_force_write_int96_timestamps();
    ASSERT_TRUE(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), *out, 3,
                                             ::parquet::WriterProperties::Builder().build(),
                                             arrow_props.build())
                        .ok());
    ASSERT_TRUE((*out)->Close().ok());
    auto physical = ::parquet::ParquetFileReader::OpenFile(path, false);
    for (int i = 1; i < 4; ++i) {
        ASSERT_EQ(::parquet::Type::INT96,
                  physical->metadata()->schema()->Column(i)->physical_type());
    }
    physical->Close();

    auto wall = external_schema_field("wall", 11);
    wall.field_ptr->__set_timestamp_is_adjusted_to_utc(false);
    auto instant = external_schema_field("instant", 12);
    instant.field_ptr->__set_timestamp_is_adjusted_to_utc(true);
    auto ltz = external_schema_field("ltz", 14);
    ltz.field_ptr->__set_timestamp_is_adjusted_to_utc(true);
    auto parent = external_schema_field("nested", 13);
    schema::external::TStructField struct_field;
    struct_field.__set_fields({ltz});
    parent.field_ptr->nestedField.__set_struct_field(struct_field);
    parent.field_ptr->__isset.nestedField = true;

    for (bool mapping : {false, true}) {
        for (bool filter_only : {false, true}) {
            SCOPED_TRACE(mapping);
            SCOPED_TRACE(filter_only);
            const auto datetime_type = make_nullable(std::make_shared<DataTypeDateTimeV2>(6));
            const DataTypePtr instant_type =
                    mapping ? make_nullable(std::make_shared<DataTypeTimeStampTz>(6))
                            : datetime_type;
            auto nested_col = make_table_column(
                    13, "nested",
                    std::make_shared<DataTypeStruct>(DataTypes {instant_type}, Strings {"ltz"}));
            nested_col.children = {make_table_column(14, "ltz", instant_type)};
            std::vector<ColumnDefinition> columns {
                    make_table_column(10, "id", std::make_shared<DataTypeInt32>())};
            if (!filter_only) {
                columns.push_back(make_table_column(11, "wall", datetime_type));
                columns.push_back(make_table_column(12, "instant", instant_type));
                columns.push_back(nested_col);
            } else if (!mapping) {
                // A TIMESTAMPTZ-to-DATETIME cast can leave a residual predicate for Scanner.
                // Keep its input in the scan block, as FE does, even when SELECT omits it.
                columns.push_back(nested_col);
            }
            auto params = make_local_parquet_scan_params();
            params.__set_parquet_timestamp_semantics_version(1);
            params.__set_enable_mapping_timestamp_tz(mapping);
            params.__set_current_schema_id(100);
            params.__set_history_schema_info({external_schema(
                    100, {external_schema_field("id", 10), wall, instant, parent})});
            RuntimeState state {TQueryOptions(), TQueryGlobals()};
            state.set_timezone("Asia/Shanghai");
            VExprContextSPtrs conjuncts;
            if (filter_only) {
                auto function = [](const std::string& name, const DataTypePtr& result,
                                   const DataTypes& args) {
                    TFunctionName fn_name;
                    fn_name.__set_function_name(name);
                    TFunction fn;
                    fn.__set_name(fn_name);
                    fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
                    for (const auto& arg : args) {
                        fn.arg_types.push_back(arg->to_thrift());
                    }
                    fn.__set_ret_type(result->to_thrift());
                    fn.__set_has_var_args(false);
                    TExprNode node;
                    node.__set_node_type(TExprNodeType::FUNCTION_CALL);
                    node.__set_type(result->to_thrift());
                    node.__set_is_nullable(result->is_nullable());
                    node.__set_num_children(args.size());
                    node.__set_fn(fn);
                    return VectorizedFnCall::create_shared(node);
                };
                auto string_type = std::make_shared<DataTypeString>();
                auto leaf = function("element_at", instant_type, {nested_col.type, string_type});
                const int nested_index = mapping ? 3 : 1;
                leaf->add_child(VSlotRef::create_shared(nested_index, nested_index, -1,
                                                        nested_col.type, "nested"));
                leaf->add_child(VLiteral::create_shared(string_type,
                                                        Field::create_field<TYPE_STRING>("ltz")));
                auto predicate = function("is_not_null_pred", std::make_shared<DataTypeUInt8>(),
                                          {instant_type});
                predicate->add_child(leaf);
                auto ctx = VExprContext::create_shared(predicate);
                const auto prepare_status = ctx->prepare(&state, RowDescriptor());
                ASSERT_TRUE(prepare_status.ok()) << prepare_status;
                const auto open_status = ctx->open(&state);
                ASSERT_TRUE(open_status.ok()) << open_status;
                conjuncts.push_back(ctx);
            }
            RuntimeProfile profile("paimon_int96_history");
            io::FileReaderStats stats;
            io::FileCacheStatistics cache_stats;
            auto io_ctx = make_io_context(&stats, &cache_stats);
            ShardedKVCache cache(1);
            paimon::PaimonReader reader;
            ASSERT_TRUE(reader.init({.projected_columns = columns,
                                     .conjuncts = conjuncts,
                                     .format = FileFormat::PARQUET,
                                     .scan_params = &params,
                                     .io_ctx = io_ctx,
                                     .runtime_state = &state,
                                     .scanner_profile = &profile})
                                .ok());
            auto split = build_split_options(path);
            split.cache = &cache;
            split.current_range.__set_table_format_params(
                    make_paimon_schema_table_format_desc(100));
            ASSERT_TRUE(reader.prepare_split(split).ok());
            bool eos = false;
            std::vector<int32_t> ids;
            while (!eos) {
                Block block = build_table_block(columns);
                const auto status = reader.get_block(&block, &eos);
                ASSERT_TRUE(status.ok()) << status;
                if (block.rows() == 0) {
                    continue;
                }
                if (filter_only && !mapping) {
                    ASSERT_TRUE(VExprContext::filter_block(conjuncts, &block, columns.size()).ok());
                }
                const auto& id_column =
                        assert_cast<const ColumnInt32&>(expect_not_null_table_column(block, 0));
                for (size_t row = 0; row < block.rows(); ++row) {
                    const auto id = id_column.get_element(row);
                    ids.push_back(id);
                    if (filter_only) {
                        continue;
                    }
                    const auto& wall_col = *block.get_by_position(1).column;
                    const auto& instant_col = *block.get_by_position(2).column;
                    const auto& nested_col_data =
                            assert_cast<const ColumnStruct&>(expect_not_null_table_column(block, 3))
                                    .get_column(0);
                    if (id == 3) {
                        EXPECT_TRUE(wall_col.is_null_at(row));
                        EXPECT_TRUE(instant_col.is_null_at(row));
                        EXPECT_TRUE(nested_col_data.is_null_at(row));
                    } else {
                        const auto* const fraction = id == 1 ? "000000" : "123456";
                        EXPECT_EQ(std::string("1970-01-01 00:00:00.") + fraction,
                                  datetime_type->to_string(wall_col, row));
                        const auto expected =
                                mapping ? std::string("1970-01-01 00:00:00.") + fraction + "+00:00"
                                        : std::string("1970-01-01 08:00:00.") + fraction;
                        EXPECT_EQ(expected, instant_type->to_string(instant_col, row));
                        EXPECT_EQ(expected, instant_type->to_string(nested_col_data, row));
                    }
                }
            }
            EXPECT_EQ(ids,
                      filter_only ? std::vector<int32_t>({1, 2}) : std::vector<int32_t>({1, 2, 3}));
            ASSERT_TRUE(reader.close().ok());
        }
    }
    std::filesystem::remove(path);
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

TEST(PaimonHybridReaderTest, ClassifiesJniSplitByReaderType) {
    EXPECT_FALSE(paimon::PaimonHybridReader::TEST_is_jni_split(
            make_paimon_native_range(TFileFormatType::FORMAT_PARQUET)));
    EXPECT_FALSE(paimon::PaimonHybridReader::TEST_is_jni_split(
            make_legacy_paimon_native_range(TFileFormatType::FORMAT_PARQUET)));
    EXPECT_TRUE(paimon::PaimonHybridReader::TEST_is_jni_split(make_paimon_jni_range()));
}

TEST(PaimonHybridReaderTest, ConvertsNativeSplitFileFormat) {
    FileFormat file_format;
    ASSERT_TRUE(paimon::PaimonHybridReader::TEST_to_file_format(
                        make_paimon_native_range(TFileFormatType::FORMAT_PARQUET), &file_format)
                        .ok());
    EXPECT_EQ(file_format, FileFormat::PARQUET);

    ASSERT_TRUE(paimon::PaimonHybridReader::TEST_to_file_format(
                        make_paimon_native_range(TFileFormatType::FORMAT_ORC), &file_format)
                        .ok());
    EXPECT_EQ(file_format, FileFormat::ORC);

    ASSERT_TRUE(
            paimon::PaimonHybridReader::TEST_to_file_format(
                    make_legacy_paimon_native_range(TFileFormatType::FORMAT_PARQUET), &file_format)
                    .ok());
    EXPECT_EQ(file_format, FileFormat::PARQUET);

    ASSERT_TRUE(paimon::PaimonHybridReader::TEST_to_file_format(
                        make_legacy_paimon_native_range(TFileFormatType::FORMAT_ORC), &file_format)
                        .ok());
    EXPECT_EQ(file_format, FileFormat::ORC);

    auto status =
            paimon::PaimonHybridReader::TEST_to_file_format(make_paimon_jni_range(), &file_format);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(std::string::npos, status.to_string().find("Unsupported native Paimon file format"));
}

TEST(PaimonHybridReaderTest, NormalizesLegacyNativeSplitFormatBeforeChildPrepare) {
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    paimon::PaimonHybridReader reader;
    SplitFormatTrackingTableReader* tracking_reader = nullptr;
    reader.TEST_set_child_reader_factories(
            [&] {
                auto child = std::make_unique<SplitFormatTrackingTableReader>();
                tracking_reader = child.get();
                return child;
            },
            [] { return std::make_unique<TableReader>(); });
    ASSERT_TRUE(reader.init({
                                    .projected_columns = {},
                                    .conjuncts = {},
                                    .format = FileFormat::JNI,
                                    .scan_params = &scan_params,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                            })
                        .ok());

    SplitReadOptions options;
    options.current_range = make_legacy_paimon_native_range(TFileFormatType::FORMAT_PARQUET);
    options.current_split_format = FileFormat::JNI;
    ASSERT_TRUE(reader.prepare_split(options).ok());
    ASSERT_NE(tracking_reader, nullptr);
    EXPECT_EQ(tracking_reader->prepared_format, FileFormat::PARQUET);
}

TEST(PaimonHybridReaderTest, AdaptiveBatchSizeReachesBothChildReaders) {
    paimon::PaimonHybridReader reader;
    reader.TEST_install_batch_size_children();
    reader.set_batch_size(321);
    const auto child_batch_sizes = reader.TEST_child_batch_sizes();
    EXPECT_EQ(child_batch_sizes.first, 321);
    EXPECT_EQ(child_batch_sizes.second, 321);
}

TEST(PaimonHybridReaderTest, AggregatesConditionCacheHitsFromBothChildren) {
    paimon::PaimonHybridReader reader;
    reader.TEST_install_batch_size_children();
    reader.TEST_set_child_condition_cache_hits(3, 5);
    EXPECT_EQ(reader.condition_cache_hit_count(), 8);
}

TEST(PaimonHybridReaderTest, ForwardsLatePredicatesToActiveChild) {
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    paimon::PaimonHybridReader reader;
    RefreshTrackingTableReader* child = nullptr;
    reader.TEST_set_child_reader_factories(
            [&] {
                auto tracking = std::make_unique<RefreshTrackingTableReader>();
                child = tracking.get();
                return tracking;
            },
            [] { return std::make_unique<TableReader>(); });
    ASSERT_TRUE(reader.init({
                                    .projected_columns = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = &scan_params,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                            })
                        .ok());

    SplitReadOptions split;
    split.current_split_format = FileFormat::PARQUET;
    split.current_range = make_paimon_native_range(TFileFormatType::FORMAT_PARQUET);
    ASSERT_TRUE(reader.prepare_split(split).ok());
    ASSERT_NE(child, nullptr);
    ASSERT_TRUE(reader.refresh_conjuncts({}).ok());
    EXPECT_EQ(child->refresh_count, 1);
}

TEST(PaimonHybridReaderTest, NativeCountColumnReportsMetadataRowsThroughHybridReader) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_paimon_hybrid_count_column_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    const auto file_path = (test_dir / "data-file.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30}, {"one", "two", "three"});

    const std::vector<ColumnDefinition> projected_columns {
            make_table_column(0, "id", std::make_shared<DataTypeInt32>()),
    };
    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);

    paimon::PaimonHybridReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = &scan_params,
                                    .io_ctx = io_ctx,
                                    .runtime_state = &state,
                                    .scanner_profile = &profile,
                                    .push_down_agg_type = TPushAggOp::type::COUNT,
                                    .push_down_count_columns =
                                            std::vector<GlobalIndex> {GlobalIndex(0)},
                            })
                        .ok());

    SplitReadOptions split_options;
    split_options.cache = &cache;
    split_options.current_split_format = FileFormat::PARQUET;
    split_options.current_range = make_paimon_native_range(TFileFormatType::FORMAT_PARQUET);
    split_options.current_range.__set_path(file_path);
    split_options.current_range.__set_file_size(
            static_cast<int64_t>(std::filesystem::file_size(file_path)));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    EXPECT_EQ(block.rows(), 3);
    EXPECT_TRUE(reader.current_split_uses_metadata_count());

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(PaimonHybridReaderTest, DispatchesNativeThenJniSplitToMatchingReader) {
    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);

    paimon::PaimonHybridReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = &scan_params,
                                    .io_ctx = io_ctx,
                                    .runtime_state = &state,
                                    .scanner_profile = &profile,
                            })
                        .ok());

    SplitReadOptions native_split;
    native_split.current_range = make_paimon_native_range(TFileFormatType::FORMAT_PARQUET);
    native_split.current_split_format = FileFormat::PARQUET;
    ASSERT_TRUE(reader.prepare_split(native_split).ok());

    SplitReadOptions jni_split;
    jni_split.current_range = make_paimon_jni_range();
    jni_split.current_split_format = FileFormat::JNI;
    auto status = reader.prepare_split(jni_split);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(std::string::npos, status.to_string().find("missing serialized_table"));

    ASSERT_TRUE(reader.close().ok());
}

TEST(PaimonHybridReaderTest, FirstNativeAndJniChildInitAreCountedOnce) {
    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    paimon::PaimonHybridReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = &scan_params,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = &profile,
                                    .file_slot_descs = nullptr,
                                    .push_down_agg_type = TPushAggOp::NONE,
                                    .condition_cache_digest = 0,
                            })
                        .ok());
    reader.TEST_set_child_reader_factories([] { return std::make_unique<SlowInitTableReader>(); },
                                           [] { return std::make_unique<SlowInitTableReader>(); });

    auto* total = profile.get_counter("TableReader");
    auto* init = profile.get_counter("InitTime");
    ASSERT_NE(total, nullptr);
    ASSERT_NE(init, nullptr);
    auto verify_first_split = [&](FileFormat format, TFileRangeDesc range) {
        SplitReadOptions split;
        split.current_split_format = format;
        split.current_range = std::move(range);
        const int64_t total_before = total->value();
        const int64_t init_before = init->value();
        ASSERT_TRUE(reader.prepare_split(split).ok());
        const int64_t total_delta = total->value() - total_before;
        const int64_t init_delta = init->value() - init_before;
        EXPECT_GE(init_delta, std::chrono::milliseconds(25).count() * 1000 * 1000);
        // A nested hybrid timer would add the 30 ms child init to total a second time.
        EXPECT_LT(total_delta - init_delta, std::chrono::milliseconds(15).count() * 1000 * 1000);
    };
    verify_first_split(FileFormat::PARQUET,
                       make_paimon_native_range(TFileFormatType::FORMAT_PARQUET));
    verify_first_split(FileFormat::JNI, make_paimon_jni_range());
}

TEST(PaimonJniReaderTest, BuildScannerParamsKeepsExplicitIOManagerTempDir) {
    auto scan_params = make_paimon_jni_scan_params();
    scan_params.__set_paimon_options({
            {"jni.enable_jni_io_manager", "true"},
            {"jni.io_manager.tmp_dir", "/tmp/explicit-paimon-spill"},
            {"jni.io_manager.impl_class", "org.example.CustomIOManager"},
    });
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    state.set_exec_env(ExecEnv::GetInstance());

    auto params = build_paimon_jni_scanner_params(&scan_params, &state);
    EXPECT_EQ(params["paimon.jni.enable_jni_io_manager"], "true");
    EXPECT_EQ(params["paimon.jni.io_manager.tmp_dir"], "/tmp/explicit-paimon-spill");
    EXPECT_EQ(params["paimon.jni.io_manager.impl_class"], "org.example.CustomIOManager");
    EXPECT_EQ(params["serialized_table_cache_key"], "serialized-paimon-table-cache-key");
}

TEST(PaimonJniReaderTest, BuildScannerParamsInjectsStorageRootTmpDirForEnabledIOManager) {
    ScopedExecEnvStorePaths store_paths({
            StorePath("/data1/doris", -1),
            StorePath("/data2/doris", -1),
    });
    auto scan_params = make_paimon_jni_scan_params();
    scan_params.__set_paimon_options({
            {"jni.enable_jni_io_manager", "true"},
    });
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    state.set_exec_env(ExecEnv::GetInstance());

    auto params = build_paimon_jni_scanner_params(&scan_params, &state);
    EXPECT_EQ(params["paimon.jni.enable_jni_io_manager"], "true");
    EXPECT_EQ(params["paimon.jni.io_manager.tmp_dir"],
              "/data1/doris/paimon_jni_scanner_io_tmp:/data2/doris/"
              "paimon_jni_scanner_io_tmp");
}

TEST(PaimonJniReaderTest, BuildScannerParamsUsesStorageRootTmpDirWhenIOManagerTempDirMissing) {
    std::vector<StorePath> paths;
    paths.emplace_back("/data1/doris", -1);
    paths.emplace_back("/data2/doris", -1);
    EXPECT_EQ(paimon::PaimonJniReader::TEST_build_default_io_manager_tmp_dirs(paths),
              "/data1/doris/paimon_jni_scanner_io_tmp:/data2/doris/"
              "paimon_jni_scanner_io_tmp");
}

} // namespace
} // namespace doris::format
