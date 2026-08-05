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

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <gtest/gtest.h>
#include <parquet/arrow/writer.h>

#include <array>
#include <filesystem>
#include <ranges>
#include <string_view>

#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/value/variant/variant_batch_builder.h"
#include "format_v2/parquet/parquet_reader.h"
#include "format_v2/table/paimon_reader.h"
#include "io/io_common.h"
#include "runtime/runtime_state.h"

namespace doris::format {
namespace {

ColumnDefinition table_column(std::string name, DataTypePtr type) {
    ColumnDefinition column;
    column.name = std::move(name);
    column.type = make_nullable(std::move(type));
    return column;
}

ColumnDefinition file_column(int32_t local_id, std::string name, DataTypePtr type) {
    ColumnDefinition column;
    column.local_id = local_id;
    column.name = std::move(name);
    column.type = make_nullable(std::move(type));
    return column;
}

std::shared_ptr<arrow::Array> binary_array(const std::vector<StringRef>& values) {
    arrow::BinaryBuilder builder;
    for (const auto value : values) {
        EXPECT_TRUE(builder.Append(reinterpret_cast<const uint8_t*>(value.data),
                                   static_cast<int32_t>(value.size))
                            .ok());
    }
    std::shared_ptr<arrow::Array> result;
    EXPECT_TRUE(builder.Finish(&result).ok());
    return result;
}

std::shared_ptr<arrow::Array> null_binary_array(size_t rows) {
    arrow::BinaryBuilder builder;
    for (size_t row = 0; row < rows; ++row) {
        EXPECT_TRUE(builder.AppendNull().ok());
    }
    std::shared_ptr<arrow::Array> result;
    EXPECT_TRUE(builder.Finish(&result).ok());
    return result;
}

std::shared_ptr<arrow::Array> int32_array(const std::vector<int32_t>& values) {
    arrow::Int32Builder builder;
    EXPECT_TRUE(builder.AppendValues(values).ok());
    std::shared_ptr<arrow::Array> result;
    EXPECT_TRUE(builder.Finish(&result).ok());
    return result;
}

void write_unannotated_paimon_variant_file(const std::string& path,
                                           const std::vector<int64_t>& values) {
    VariantBatchBuilder builder;
    for (const auto value : values) {
        auto row = builder.begin_row();
        auto object = row.start_object();
        object.add_key(StringRef("n"));
        row.add_int(value);
        object.finish();
        row.finish();
    }
    auto batch = builder.finish_batch();
    std::vector<StringRef> value_rows;
    std::vector<StringRef> metadata_rows;
    for (size_t row = 0; row < values.size(); ++row) {
        const auto encoded = batch.value_at(row);
        value_rows.push_back(encoded.value);
        metadata_rows.emplace_back(encoded.metadata.data, encoded.metadata.size);
    }

    const auto payload_type = arrow::struct_({arrow::field("value", arrow::binary(), false),
                                              arrow::field("metadata", arrow::binary(), false)});
    auto payload_result = arrow::StructArray::Make(
            {binary_array(value_rows), binary_array(metadata_rows)}, payload_type->fields());
    ASSERT_TRUE(payload_result.ok()) << payload_result.status();
    auto table = arrow::Table::Make(arrow::schema({arrow::field("payload", payload_type)}),
                                    {*payload_result});

    auto file_result = arrow::io::FileOutputStream::Open(path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    ::parquet::WriterProperties::Builder properties;
    properties.version(::parquet::ParquetVersion::PARQUET_2_6);
    properties.compression(::parquet::Compression::UNCOMPRESSED);
    PARQUET_THROW_NOT_OK(
            ::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), *file_result,
                                         static_cast<int64_t>(values.size()), properties.build()));
}

void write_unannotated_shredded_paimon_variant_file(const std::string& path,
                                                    const std::vector<int32_t>& ages) {
    VariantBatchBuilder builder;
    for (const auto age : ages) {
        auto row = builder.begin_row();
        auto object = row.start_object();
        object.add_key(StringRef("age"));
        row.add_int(age);
        object.finish();
        row.finish();
    }
    const auto batch = builder.finish_batch();
    std::vector<StringRef> metadata_rows;
    for (size_t row = 0; row < ages.size(); ++row) {
        const auto encoded = batch.value_at(row);
        metadata_rows.emplace_back(encoded.metadata.data, encoded.metadata.size);
    }

    const auto age_wrapper_type = arrow::struct_(
            {arrow::field("value", arrow::binary()), arrow::field("typed_value", arrow::int32())});
    auto age_result = arrow::StructArray::Make({null_binary_array(ages.size()), int32_array(ages)},
                                               age_wrapper_type->fields());
    ASSERT_TRUE(age_result.ok()) << age_result.status();
    const auto typed_value_type = arrow::struct_({arrow::field("age", age_wrapper_type, false)});
    auto typed_value_result = arrow::StructArray::Make({*age_result}, typed_value_type->fields());
    ASSERT_TRUE(typed_value_result.ok()) << typed_value_result.status();
    const auto payload_type = arrow::struct_({arrow::field("metadata", arrow::binary(), false),
                                              arrow::field("value", arrow::binary()),
                                              arrow::field("typed_value", typed_value_type)});
    auto payload_result = arrow::StructArray::Make(
            {binary_array(metadata_rows), null_binary_array(ages.size()), *typed_value_result},
            payload_type->fields());
    ASSERT_TRUE(payload_result.ok()) << payload_result.status();
    auto table = arrow::Table::Make(arrow::schema({arrow::field("payload", payload_type)}),
                                    {*payload_result});

    auto file_result = arrow::io::FileOutputStream::Open(path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    ::parquet::WriterProperties::Builder properties;
    properties.version(::parquet::ParquetVersion::PARQUET_2_6);
    properties.compression(::parquet::Compression::UNCOMPRESSED);
    PARQUET_THROW_NOT_OK(
            ::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), *file_result,
                                         static_cast<int64_t>(ages.size()), properties.build()));
}

// Scenario: Paimon 1.3/1.4 writes Variant as an unannotated Parquet group. Only the Paimon table
// schema can distinguish that carrier from an ordinary STRUCT, so the table reader must expose the
// matched file node as Variant while retaining its physical children for native decoding.
TEST(PaimonVariantReaderTest, AnnotatesUnmarkedParquetVariantFromTableSchema) {
    const auto binary = std::make_shared<DataTypeString>();
    auto physical_type = std::make_shared<DataTypeStruct>(DataTypes {binary, binary},
                                                          Strings {"value", "metadata"});
    auto payload = file_column(0, "payload", physical_type);
    payload.children = {file_column(0, "value", binary), file_column(1, "metadata", binary)};
    std::vector<ColumnDefinition> file_schema {std::move(payload)};

    paimon::PaimonReader reader;
    reader.TEST_set_format(FileFormat::PARQUET);
    reader.TEST_set_projected_columns(
            {table_column("payload", std::make_shared<DataTypeVariantV2>())});

    ASSERT_TRUE(reader.TEST_annotate_file_schema(&file_schema).ok());
    ASSERT_EQ(file_schema.size(), 1);
    EXPECT_EQ(remove_nullable(file_schema[0].type)->get_primitive_type(), TYPE_VARIANT);
    ASSERT_EQ(file_schema[0].children.size(), 2);
    EXPECT_EQ(file_schema[0].children[0].name, "value");
    EXPECT_EQ(file_schema[0].children[1].name, "metadata");

    FileScanRequest request;
    ASSERT_TRUE(reader.TEST_customize_file_scan_request(&request).ok());
    ASSERT_EQ(request.variant_schema_overrides.size(), 1);
    EXPECT_EQ(request.variant_schema_overrides[0].local_id(), 0);
    EXPECT_TRUE(request.variant_schema_overrides[0].project_all_children);
}

TEST(PaimonVariantReaderTest, DoesNotGuessOrdinaryStructWithVariantCarrierNames) {
    const auto binary = std::make_shared<DataTypeString>();
    auto struct_type = std::make_shared<DataTypeStruct>(DataTypes {binary, binary},
                                                        Strings {"value", "metadata"});
    auto payload = file_column(0, "payload", struct_type);
    payload.children = {file_column(0, "value", binary), file_column(1, "metadata", binary)};
    std::vector<ColumnDefinition> file_schema {payload};

    auto projected = table_column("payload", struct_type);
    projected.children = payload.children;
    paimon::PaimonReader reader;
    reader.TEST_set_format(FileFormat::PARQUET);
    reader.TEST_set_projected_columns({std::move(projected)});

    ASSERT_TRUE(reader.TEST_annotate_file_schema(&file_schema).ok());
    EXPECT_EQ(remove_nullable(file_schema[0].type)->get_primitive_type(), TYPE_STRUCT);
    FileScanRequest request;
    ASSERT_TRUE(reader.TEST_customize_file_scan_request(&request).ok());
    EXPECT_TRUE(request.variant_schema_overrides.empty());
}

TEST(PaimonVariantReaderTest, AnnotatesNestedArrayVariantByStructuralPosition) {
    const auto binary = std::make_shared<DataTypeString>();
    auto carrier_type = std::make_shared<DataTypeStruct>(DataTypes {binary, binary},
                                                         Strings {"value", "metadata"});
    auto element = file_column(0, "element", carrier_type);
    element.children = {file_column(0, "value", binary), file_column(1, "metadata", binary)};
    auto values = file_column(0, "values", std::make_shared<DataTypeArray>(element.type));
    values.children = {std::move(element)};
    std::vector<ColumnDefinition> file_schema {std::move(values)};

    auto item = table_column("item", std::make_shared<DataTypeVariantV2>());
    auto projected = table_column("values", std::make_shared<DataTypeArray>(item.type));
    projected.children = {std::move(item)};
    paimon::PaimonReader reader;
    reader.TEST_set_format(FileFormat::PARQUET);
    reader.TEST_set_projected_columns({std::move(projected)});

    ASSERT_TRUE(reader.TEST_annotate_file_schema(&file_schema).ok());
    const auto& array_type =
            assert_cast<const DataTypeArray&>(*remove_nullable(file_schema[0].type));
    EXPECT_EQ(remove_nullable(array_type.get_nested_type())->get_primitive_type(), TYPE_VARIANT);
    ASSERT_EQ(file_schema[0].children.size(), 1);
    EXPECT_EQ(remove_nullable(file_schema[0].children[0].type)->get_primitive_type(), TYPE_VARIANT);

    FileScanRequest request;
    ASSERT_TRUE(reader.TEST_customize_file_scan_request(&request).ok());
    ASSERT_EQ(request.variant_schema_overrides.size(), 1);
    EXPECT_FALSE(request.variant_schema_overrides[0].project_all_children);
    ASSERT_EQ(request.variant_schema_overrides[0].children.size(), 1);
    EXPECT_TRUE(request.variant_schema_overrides[0].children[0].project_all_children);
}

TEST(PaimonVariantReaderTest, MergesSiblingNestedVariantOverrides) {
    const auto binary = std::make_shared<DataTypeString>();
    auto carrier_type = std::make_shared<DataTypeStruct>(DataTypes {binary, binary},
                                                         Strings {"value", "metadata"});
    auto carrier = [&](int32_t local_id, std::string name) {
        auto column = file_column(local_id, std::move(name), carrier_type);
        column.children = {file_column(0, "value", binary), file_column(1, "metadata", binary)};
        return column;
    };
    auto row = file_column(0, "row",
                           std::make_shared<DataTypeStruct>(DataTypes {carrier_type, carrier_type},
                                                            Strings {"left", "right"}));
    row.children = {carrier(0, "left"), carrier(1, "right")};
    std::vector<ColumnDefinition> file_schema {std::move(row)};

    auto projected = table_column(
            "row", std::make_shared<DataTypeStruct>(
                           DataTypes {make_nullable(std::make_shared<DataTypeVariantV2>()),
                                      make_nullable(std::make_shared<DataTypeVariantV2>())},
                           Strings {"left", "right"}));
    projected.children = {table_column("left", std::make_shared<DataTypeVariantV2>()),
                          table_column("right", std::make_shared<DataTypeVariantV2>())};
    paimon::PaimonReader reader;
    reader.TEST_set_format(FileFormat::PARQUET);
    reader.TEST_set_projected_columns({std::move(projected)});

    ASSERT_TRUE(reader.TEST_annotate_file_schema(&file_schema).ok());
    FileScanRequest request;
    ASSERT_TRUE(reader.TEST_customize_file_scan_request(&request).ok());
    ASSERT_EQ(request.variant_schema_overrides.size(), 1);
    ASSERT_EQ(request.variant_schema_overrides[0].children.size(), 2);
    EXPECT_TRUE(request.variant_schema_overrides[0].children[0].project_all_children);
    EXPECT_TRUE(request.variant_schema_overrides[0].children[1].project_all_children);
}

TEST(PaimonVariantReaderTest, ReadsUnannotatedPaimonVariantWithNativeParquetReader) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_paimon_native_unannotated_variant_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    const auto file_path = (test_dir / "data.parquet").string();
    write_unannotated_paimon_variant_file(file_path, {1, 2, 3});

    std::vector projected_columns {table_column("payload", std::make_shared<DataTypeVariantV2>())};
    TFileScanRangeParams scan_params;
    scan_params.__set_file_type(TFileType::FILE_LOCAL);
    scan_params.__set_format_type(TFileFormatType::FORMAT_PARQUET);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = std::make_shared<io::IOContext>();
    io_ctx->file_reader_stats = &file_reader_stats;
    io_ctx->file_cache_stats = &file_cache_stats;

    paimon::PaimonReader reader;
    ASSERT_TRUE(reader.init({.projected_columns = projected_columns,
                             .conjuncts = {},
                             .format = FileFormat::PARQUET,
                             .scan_params = &scan_params,
                             .io_ctx = io_ctx,
                             .runtime_state = &state,
                             .scanner_profile = nullptr})
                        .ok());
    SplitReadOptions split;
    split.current_range.__set_path(file_path);
    split.current_range.__set_file_size(
            static_cast<int64_t>(std::filesystem::file_size(file_path)));
    TTableFormatFileDesc table_format;
    table_format.__set_table_format_type("paimon");
    table_format.__set_paimon_params(TPaimonFileDesc {});
    split.current_range.__set_table_format_params(std::move(table_format));
    ASSERT_TRUE(reader.prepare_split(split).ok());

    std::vector<int64_t> actual;
    bool eos = false;
    while (!eos) {
        Block block;
        block.insert({projected_columns[0].type->create_column(), projected_columns[0].type,
                      projected_columns[0].name});
        ASSERT_TRUE(reader.get_block(&block, &eos).ok());
        if (block.rows() == 0) {
            continue;
        }
        const auto& nullable = assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
        const auto& variants = assert_cast<const ColumnVariantV2&>(nullable.get_nested_column());
        for (size_t row = 0; row < variants.size(); ++row) {
            VariantRef n;
            ASSERT_TRUE(variants.get_value_ref(row).object_find(StringRef("n"), &n));
            actual.push_back(n.get_int());
        }
    }
    EXPECT_EQ(actual, std::vector<int64_t>({1, 2, 3}));
    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(PaimonVariantReaderTest, ParquetReaderAppliesExplicitVariantSchemaOverride) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_paimon_parquet_variant_override_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    const auto file_path = (test_dir / "data.parquet").string();
    write_unannotated_paimon_variant_file(file_path, {7, 8});

    auto system_properties = std::make_shared<io::FileSystemProperties>();
    system_properties->system_type = TFileType::FILE_LOCAL;
    auto file_description = std::make_unique<io::FileDescription>();
    file_description->path = file_path;
    file_description->file_size = static_cast<int64_t>(std::filesystem::file_size(file_path));
    file_description->range_start_offset = 0;
    file_description->range_size = -1;
    auto reader = std::make_unique<parquet::ParquetReader>(
            system_properties, file_description, std::shared_ptr<io::IOContext> {}, nullptr);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());
    std::vector<ColumnDefinition> file_schema;
    ASSERT_TRUE(reader->get_schema(&file_schema).ok());
    ASSERT_EQ(file_schema.size(), 1);
    ASSERT_EQ(remove_nullable(file_schema[0].type)->get_primitive_type(), TYPE_STRUCT);

    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns.push_back(
            LocalColumnIndex::top_level(LocalColumnId(file_schema[0].local_id)));
    request->local_positions.emplace(LocalColumnId(file_schema[0].local_id), LocalIndex(0));
    request->variant_schema_overrides.push_back(
            LocalColumnIndex::top_level(LocalColumnId(file_schema[0].local_id)));
    ASSERT_TRUE(reader->open(request).ok());

    const auto variant_type = make_nullable(std::make_shared<DataTypeVariantV2>());
    std::vector<int64_t> actual;
    bool eof = false;
    while (!eof) {
        Block block;
        block.insert({variant_type->create_column(), variant_type, "payload"});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        const auto& nullable = assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
        const auto& variants = assert_cast<const ColumnVariantV2&>(nullable.get_nested_column());
        for (size_t row = 0; row < variants.size(); ++row) {
            VariantRef n;
            ASSERT_TRUE(variants.get_value_ref(row).object_find(StringRef("n"), &n));
            actual.push_back(n.get_int());
        }
    }
    EXPECT_EQ(actual, std::vector<int64_t>({7, 8}));
    ASSERT_TRUE(reader->close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(PaimonVariantReaderTest, ReadsProjectedLeafFromUnannotatedShreddedVariant) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_paimon_parquet_shredded_variant_override_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    const auto file_path = (test_dir / "data.parquet").string();
    write_unannotated_shredded_paimon_variant_file(file_path, {27, 42});

    auto system_properties = std::make_shared<io::FileSystemProperties>();
    system_properties->system_type = TFileType::FILE_LOCAL;
    auto file_description = std::make_unique<io::FileDescription>();
    file_description->path = file_path;
    file_description->file_size = static_cast<int64_t>(std::filesystem::file_size(file_path));
    file_description->range_start_offset = 0;
    file_description->range_size = -1;
    auto reader = std::make_unique<parquet::ParquetReader>(
            system_properties, file_description, std::shared_ptr<io::IOContext> {}, nullptr);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());
    std::vector<ColumnDefinition> file_schema;
    ASSERT_TRUE(reader->get_schema(&file_schema).ok());
    ASSERT_EQ(file_schema.size(), 1);
    ASSERT_EQ(remove_nullable(file_schema[0].type)->get_primitive_type(), TYPE_STRUCT);
    ASSERT_EQ(file_schema[0].children.size(), 3);

    auto find_child = [](const std::vector<ColumnDefinition>& children,
                         std::string_view name) -> const ColumnDefinition* {
        const auto it = std::ranges::find_if(
                children, [name](const auto& child) { return child.name == name; });
        return it == children.end() ? nullptr : &*it;
    };
    const auto* typed_value = find_child(file_schema[0].children, "typed_value");
    ASSERT_NE(typed_value, nullptr);
    const auto* age = find_child(typed_value->children, "age");
    ASSERT_NE(age, nullptr);
    const auto* age_typed_value = find_child(age->children, "typed_value");
    ASSERT_NE(age_typed_value, nullptr);

    auto projection = LocalColumnIndex::partial_local(file_schema[0].local_id);
    projection.children.push_back(LocalColumnIndex::partial_local(typed_value->local_id));
    projection.children.back().children.push_back(LocalColumnIndex::partial_local(age->local_id));
    projection.children.back().children.back().children.push_back(
            LocalColumnIndex::local(age_typed_value->local_id));
    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns.push_back(std::move(projection));
    request->local_positions.emplace(LocalColumnId(file_schema[0].local_id), LocalIndex(0));
    request->variant_schema_overrides.push_back(
            LocalColumnIndex::top_level(LocalColumnId(file_schema[0].local_id)));
    ASSERT_TRUE(reader->open(request).ok());

    const auto variant_type = make_nullable(std::make_shared<DataTypeVariantV2>());
    Block block;
    block.insert({variant_type->create_column(), variant_type, "payload"});
    size_t rows = 0;
    bool eof = false;
    while (!eof) {
        size_t batch_rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &batch_rows, &eof).ok());
        rows += batch_rows;
    }
    EXPECT_EQ(rows, 2);
    const auto& nullable = assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
    const auto& variants = assert_cast<const ColumnVariantV2&>(nullable.get_nested_column());
    const std::array path {VariantShreddedPathSegment {
            .kind = VariantShreddedPathSegment::Kind::OBJECT_KEY, .key = StringRef("age")}};
    const auto match = variants.find_shredded_typed_value(path);
    ASSERT_TRUE(match.has_value());
    const auto& typed = assert_cast<const ColumnNullable&>(*match->column);
    const auto& values = assert_cast<const ColumnInt32&>(typed.get_nested_column());
    ASSERT_EQ(values.size(), 2);
    EXPECT_EQ(values.get_data()[0], 27);
    EXPECT_EQ(values.get_data()[1], 42);
    ASSERT_TRUE(reader->close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(PaimonVariantReaderTest, AppendsUnshreddedAndShreddedPaimonFiles) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_paimon_parquet_mixed_variant_override_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    const auto unshredded_path = (test_dir / "unshredded.parquet").string();
    const auto shredded_path = (test_dir / "shredded.parquet").string();
    write_unannotated_paimon_variant_file(unshredded_path, {5});
    write_unannotated_shredded_paimon_variant_file(shredded_path, {27});

    const auto variant_type = make_nullable(std::make_shared<DataTypeVariantV2>());
    Block block;
    block.insert({variant_type->create_column(), variant_type, "payload"});
    auto append_file = [&](const std::string& path) {
        auto system_properties = std::make_shared<io::FileSystemProperties>();
        system_properties->system_type = TFileType::FILE_LOCAL;
        auto file_description = std::make_unique<io::FileDescription>();
        file_description->path = path;
        file_description->file_size = static_cast<int64_t>(std::filesystem::file_size(path));
        file_description->range_start_offset = 0;
        file_description->range_size = -1;
        auto reader = std::make_unique<parquet::ParquetReader>(
                system_properties, file_description, std::shared_ptr<io::IOContext> {}, nullptr);
        RuntimeState state {TQueryOptions(), TQueryGlobals()};
        RETURN_IF_ERROR(reader->init(&state));
        auto request = std::make_shared<FileScanRequest>();
        request->non_predicate_columns.push_back(LocalColumnIndex::top_level(LocalColumnId(0)));
        request->local_positions.emplace(LocalColumnId(0), LocalIndex(0));
        request->variant_schema_overrides.push_back(LocalColumnIndex::top_level(LocalColumnId(0)));
        RETURN_IF_ERROR(reader->open(request));
        bool eof = false;
        while (!eof) {
            size_t rows = 0;
            RETURN_IF_ERROR(reader->get_block(&block, &rows, &eof));
        }
        return reader->close();
    };

    ASSERT_TRUE(append_file(unshredded_path).ok());
    ASSERT_TRUE(append_file(shredded_path).ok());
    ASSERT_EQ(block.rows(), 2);
    const auto& nullable = assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
    const auto& variants = assert_cast<const ColumnVariantV2&>(nullable.get_nested_column());
    VariantRef field;
    ASSERT_TRUE(variants.get_value_ref(0).object_find(StringRef("n"), &field));
    EXPECT_EQ(field.get_int(), 5);
    ASSERT_TRUE(variants.get_value_ref(1).object_find(StringRef("age"), &field));
    EXPECT_EQ(field.get_int(), 27);

    std::filesystem::remove_all(test_dir);
}

} // namespace
} // namespace doris::format
