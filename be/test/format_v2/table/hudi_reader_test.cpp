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

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <gtest/gtest.h>
#include <parquet/api/reader.h>
#include <parquet/arrow/writer.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/field.h"
#include "format_v2/column_data.h"
#include "gen_cpp/ExternalTableSchema_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "io/io_common.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"

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

ColumnDefinition make_table_column(int32_t id, const std::string& name, const DataTypePtr& type) {
    ColumnDefinition column;
    column.identifier = Field::create_field<TYPE_INT>(id);
    column.name = name;
    column.type = make_nullable(type);
    return column;
}

Block build_table_block(const std::vector<ColumnDefinition>& columns) {
    Block block;
    for (const auto& column : columns) {
        block.insert({column.type->create_column(), column.type, column.name});
    }
    return block;
}

void write_int_parquet_file(const std::string& file_path, const std::vector<int32_t>& values) {
    arrow::Int32Builder value_builder;
    for (const auto value : values) {
        ASSERT_TRUE(value_builder.Append(value).ok());
    }
    std::shared_ptr<arrow::Array> value_array;
    ASSERT_TRUE(value_builder.Finish(&value_array).ok());
    const auto table = arrow::Table::Make(
            arrow::schema({arrow::field("id", arrow::int32(), false)}), {value_array});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> output = *file_result;
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), output,
                                                      static_cast<int64_t>(values.size())));
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

TEST(HudiReaderTest, NativeBaseFilesAreMarkedImmutableForPageCache) {
    hudi::HudiReader reader;

    for (const auto format : {FileFormat::PARQUET, FileFormat::ORC}) {
        SplitReadOptions split_options;
        split_options.current_split_format = format;
        split_options.current_range.__set_path("hudi-base-file");
        split_options.current_range.__set_table_format_params(hudi_table_format_desc(100));

        ASSERT_TRUE(reader.prepare_split(split_options).ok());
        EXPECT_TRUE(reader.TEST_current_data_file_is_immutable());
    }
}

TEST(HudiHybridReaderTest, AdaptiveBatchSizeReachesBothChildReaders) {
    hudi::HudiHybridReader reader;
    reader.TEST_install_batch_size_children();
    reader.set_batch_size(123);
    const auto child_batch_sizes = reader.TEST_child_batch_sizes();
    EXPECT_EQ(child_batch_sizes.first, 123);
    EXPECT_EQ(child_batch_sizes.second, 123);
}

TEST(HudiHybridReaderTest, AggregatesConditionCacheHitsFromBothChildren) {
    hudi::HudiHybridReader reader;
    reader.TEST_install_batch_size_children();
    reader.TEST_set_child_condition_cache_hits(2, 7);
    EXPECT_EQ(reader.condition_cache_hit_count(), 9);
}

TEST(HudiHybridReaderTest, NativeCountStarReportsMetadataRowsThroughHybridReader) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_hudi_hybrid_count_star_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    const auto file_path = (test_dir / "base-file.parquet").string();
    write_int_parquet_file(file_path, {1, 2, 3});

    const std::vector<ColumnDefinition> projected_columns {
            make_table_column(0, "id", std::make_shared<DataTypeInt32>()),
    };
    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    TFileScanRangeParams scan_params;
    scan_params.__set_file_type(TFileType::FILE_LOCAL);
    scan_params.__set_format_type(TFileFormatType::FORMAT_PARQUET);
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = std::make_shared<io::IOContext>();
    io_ctx->file_reader_stats = &file_reader_stats;
    io_ctx->file_cache_stats = &file_cache_stats;
    ShardedKVCache cache(1);

    hudi::HudiHybridReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = &scan_params,
                                    .io_ctx = io_ctx,
                                    .runtime_state = &state,
                                    .scanner_profile = &profile,
                                    .push_down_agg_type = TPushAggOp::type::COUNT,
                                    .push_down_count_columns = std::vector<GlobalIndex> {},
                            })
                        .ok());

    SplitReadOptions split_options;
    split_options.cache = &cache;
    split_options.current_split_format = FileFormat::PARQUET;
    split_options.current_range.__set_path(file_path);
    split_options.current_range.__set_file_size(
            static_cast<int64_t>(std::filesystem::file_size(file_path)));
    split_options.current_range.__set_format_type(TFileFormatType::FORMAT_PARQUET);
    split_options.current_range.__set_table_format_params(hudi_table_format_desc(std::nullopt));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    EXPECT_EQ(block.rows(), 3);
    EXPECT_TRUE(reader.current_split_uses_metadata_count());

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(HudiHybridReaderTest, FirstNativeAndJniChildInitAreCountedOnce) {
    RuntimeProfile profile("test_profile");
    TFileScanRangeParams scan_params;
    scan_params.__set_format_type(TFileFormatType::FORMAT_PARQUET);
    hudi::HudiHybridReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = &scan_params,
                                    .io_ctx = nullptr,
                                    .runtime_state = nullptr,
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
    auto verify_first_split = [&](FileFormat format, TFileFormatType::type thrift_format) {
        SplitReadOptions split;
        split.current_split_format = format;
        split.current_range.__set_format_type(thrift_format);
        const int64_t total_before = total->value();
        const int64_t init_before = init->value();
        ASSERT_TRUE(reader.prepare_split(split).ok());
        const int64_t total_delta = total->value() - total_before;
        const int64_t init_delta = init->value() - init_before;
        EXPECT_GE(init_delta, std::chrono::milliseconds(25).count() * 1000 * 1000);
        // A nested hybrid timer would add the 30 ms child init to total a second time.
        EXPECT_LT(total_delta - init_delta, std::chrono::milliseconds(15).count() * 1000 * 1000);
    };
    verify_first_split(FileFormat::PARQUET, TFileFormatType::FORMAT_PARQUET);
    verify_first_split(FileFormat::JNI, TFileFormatType::FORMAT_JNI);
}

} // namespace
} // namespace doris::format
