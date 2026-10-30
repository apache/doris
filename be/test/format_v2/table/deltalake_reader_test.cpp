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

#include "format_v2/table/deltalake_reader.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <gtest/gtest.h>
#include <parquet/api/reader.h>
#include <parquet/arrow/writer.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <limits>
#include <numeric>
#include <optional>
#include <string>
#include <vector>

#include "common/config.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_struct.h"
#include "exec/common/endian.h"
#include "format/table/deletion_vector_reader.h"
#include "format/table/iceberg_delete_file_reader_helper.h"
#include "format_v2/column_mapper.h"
#include "gen_cpp/ExternalTableSchema_types.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "util/debug_points.h"
#include "util/timezone_utils.h"

namespace doris::format {
namespace {

constexpr const char* DATA_FILE =
        "part-00000-90177277-75c2-48db-92a2-20dcba39fd06-c000.snappy.parquet";
constexpr const char* DV_FILES[] = {"deletion_vector_37d10da3-70a1-4730-bc58-3b44f9617505.bin",
                                    "deletion_vector_3d8a467a-2fbd-4d35-8e3a-775894a30576.bin",
                                    "deletion_vector_d12e7d16-e46d-48c9-8a71-b222c26dfc3b.bin"};

std::string fixture_path(const char* name) {
    return (std::filesystem::path(std::getenv("ROOT")) /
            "be/test/format_v2/table/test_data/deltalake" / name)
            .string();
}

std::string read_fixture(const char* name) {
    std::ifstream file(fixture_path(name), std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>());
}

TDeltaLakeDeletionVectorDesc disk_dv(size_t index) {
    TDeltaLakeDeletionVectorDesc dv;
    dv.__set_path(fixture_path(DV_FILES[index]));
    dv.__set_offset(1);
    dv.__set_size_in_bytes(34 + static_cast<int32_t>(index) * 2);
    dv.__set_cardinality(static_cast<int64_t>(index) + 1);
    return dv;
}

TDeltaLakeDeletionVectorDesc inline_dv(size_t index) {
    auto dv = disk_dv(index);
    dv.__isset.path = false;
    dv.__isset.offset = false;
    const auto file = read_fixture(DV_FILES[index]);
    dv.__set_inline_data(file.substr(5, dv.size_in_bytes));
    return dv;
}

TTableFormatFileDesc table_params(std::optional<TDeltaLakeDeletionVectorDesc> dv = std::nullopt,
                                  std::optional<int64_t> schema_id = std::nullopt) {
    TDeltaLakeFileDesc params;
    if (dv.has_value()) {
        params.__set_deletion_vector(*dv);
    }
    if (schema_id.has_value()) {
        params.__set_schema_id(*schema_id);
    }
    TTableFormatFileDesc result;
    result.__set_table_format_type("deltalake");
    result.__set_deltalake_params(params);
    return result;
}

ColumnDefinition table_column(int id, const std::string& name) {
    ColumnDefinition column;
    column.identifier = Field::create_field<TYPE_INT>(id);
    column.name = name;
    column.type = make_nullable(std::make_shared<DataTypeInt64>());
    return column;
}

ColumnDefinition typed_column(int id, const std::string& name, DataTypePtr type) {
    auto column = table_column(id, name);
    column.type = make_nullable(std::move(type));
    return column;
}

schema::external::TFieldPtr schema_field(const std::string& name, int id) {
    schema::external::TFieldPtr result;
    result.field_ptr = std::make_shared<schema::external::TField>();
    result.__isset.field_ptr = true;
    result.field_ptr->__set_name(name);
    result.field_ptr->__set_id(id);
    return result;
}

schema::external::TSchema schema(int64_t id, std::vector<schema::external::TFieldPtr> fields) {
    schema::external::TStructField root;
    root.__set_fields(std::move(fields));
    schema::external::TSchema result;
    result.__set_schema_id(id);
    result.__set_root_field(root);
    return result;
}

class FailDeletionVectorIO {
public:
    FailDeletionVectorIO() : _previous(config::enable_debug_points) {
        config::enable_debug_points = true;
        DebugPoints::instance()->add("TableReader.parse_deletion_vector.io_error");
    }

    ~FailDeletionVectorIO() {
        DebugPoints::instance()->remove("TableReader.parse_deletion_vector.io_error");
        config::enable_debug_points = _previous;
    }

private:
    bool _previous;
};

class DeltaReaderTest : public testing::Test {
protected:
    static void SetUpTestSuite() { TimezoneUtils::load_timezones_to_cache(); }

    void SetUp() override {
        scan_params.__set_file_type(TFileType::FILE_LOCAL);
        scan_params.__set_format_type(TFileFormatType::FORMAT_PARQUET);
        io_ctx = std::make_shared<io::IOContext>();
        io_ctx->file_reader_stats = &io_stats;
        io_ctx->file_cache_stats = &file_cache_stats;
    }

    Status init(deltalake::DeltaReader* reader) {
        return reader->init({.projected_columns = columns,
                             .conjuncts = {},
                             .format = FileFormat::PARQUET,
                             .scan_params = &scan_params,
                             .io_ctx = io_ctx,
                             .runtime_state = &state,
                             .scanner_profile = &profile});
    }

    SplitReadOptions split(const TTableFormatFileDesc& params, const char* file = DATA_FILE) {
        SplitReadOptions options;
        options.cache = &cache;
        options.current_range.__set_path(fixture_path(file));
        options.current_range.__set_file_size(
                static_cast<int64_t>(std::filesystem::file_size(fixture_path(file))));
        options.current_range.__set_start_offset(0);
        options.current_range.__set_size(options.current_range.file_size);
        options.current_range.__set_format_type(TFileFormatType::FORMAT_PARQUET);
        options.current_range.__set_table_format_params(params);
        return options;
    }

    void read_strings(deltalake::DeltaReader* reader, std::vector<std::vector<std::string>>* rows) {
        bool eos = false;
        while (!eos) {
            Block block;
            for (const auto& column : columns) {
                block.insert({column.type->create_column(), column.type, column.name});
            }
            auto status = reader->get_block(&block, &eos);
            ASSERT_TRUE(status.ok()) << status;
            for (size_t row = 0; row < block.rows(); ++row) {
                std::vector<std::string> values;
                for (const auto& column : block.get_columns_with_type_and_name()) {
                    values.push_back(column.type->to_string(*column.column, row));
                }
                rows->push_back(std::move(values));
            }
        }
    }

    void read(deltalake::DeltaReader* reader, std::vector<std::optional<int64_t>>* ids,
              std::optional<int64_t> partition = std::nullopt) {
        bool eos = false;
        while (!eos) {
            Block block;
            for (const auto& column : columns) {
                block.insert({column.type->create_column(), column.type, column.name});
            }
            auto status = reader->get_block(&block, &eos);
            ASSERT_TRUE(status.ok()) << status;
            const auto full_column =
                    block.get_by_position(0).column->convert_to_full_column_if_const();
            const auto& column = assert_cast<const ColumnNullable&>(*full_column);
            const auto& values = assert_cast<const ColumnInt64&>(column.get_nested_column());
            for (size_t row = 0; row < block.rows(); ++row) {
                ids->push_back(column.is_null_at(row)
                                       ? std::nullopt
                                       : std::make_optional(values.get_element(row)));
                if (partition.has_value()) {
                    const auto full_partition =
                            block.get_by_position(1).column->convert_to_full_column_if_const();
                    const auto& p = assert_cast<const ColumnNullable&>(*full_partition);
                    EXPECT_FALSE(p.is_null_at(row));
                    EXPECT_EQ(
                            assert_cast<const ColumnInt64&>(p.get_nested_column()).get_element(row),
                            *partition);
                }
            }
        }
    }

    RuntimeProfile profile {"delta_test"};
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    TFileScanRangeParams scan_params;
    io::FileReaderStats io_stats;
    io::FileCacheStatistics file_cache_stats;
    std::shared_ptr<io::IOContext> io_ctx;
    ShardedKVCache cache {1};
    std::vector<ColumnDefinition> columns {table_column(0, "id")};
};

TEST_F(DeltaReaderTest, ReadsSparkDataWithPartitionAndNoDeletionVector) {
    columns.push_back(table_column(1, "partition"));
    columns.back().is_partition_key = true;
    deltalake::DeltaReader reader;
    ASSERT_TRUE(init(&reader).ok());
    auto options = split(table_params());
    options.partition_values.emplace("partition", Field::create_field<TYPE_BIGINT>(42));
    ASSERT_TRUE(reader.prepare_split(options).ok());
    EXPECT_TRUE(reader.TEST_current_data_file_is_immutable());
    EXPECT_EQ(reader.mapping_mode(), TableColumnMappingMode::BY_NAME);
    std::vector<std::optional<int64_t>> ids;
    read(&reader, &ids, 42);
    ASSERT_EQ(ids.size(), 50);
    for (int64_t i = 0; i < 50; ++i) {
        EXPECT_EQ(ids[i], i);
    }
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(DeltaReaderTest, DiskAndInlineVectorsFilterIdenticallyAndResetBetweenSplits) {
    for (bool inlined : {false, true}) {
        deltalake::DeltaReader reader;
        ASSERT_TRUE(init(&reader).ok());
        reader.set_batch_size(3);
        for (size_t version = 0; version < 3; ++version) {
            auto options = split(table_params(inlined ? inline_dv(version) : disk_dv(version)));
            ASSERT_TRUE(reader.prepare_split(options).ok());
            std::vector<std::optional<int64_t>> ids;
            read(&reader, &ids);
            std::vector<std::optional<int64_t>> expected;
            for (int64_t i = 0; i < 50; ++i) {
                if (i > static_cast<int64_t>(version) * 7 || i % 7 != 0) {
                    expected.emplace_back(i);
                }
            }
            EXPECT_EQ(ids, expected);
        }
        ASSERT_TRUE(reader.prepare_split(split(table_params())).ok());
        std::vector<std::optional<int64_t>> ids;
        read(&reader, &ids);
        EXPECT_EQ(ids.size(), 50);
        ASSERT_TRUE(reader.close().ok());
    }
}

TEST_F(DeltaReaderTest, InlineBypassesIOAndChecksCardinalityOnCacheHits) {
    FailDeletionVectorIO fail_io;
    deltalake::DeltaReader reader;
    ASSERT_TRUE(init(&reader).ok());
    auto dv = inline_dv(2);
    auto options = split(table_params(dv));
    ASSERT_TRUE(reader._parse_delete_predicates(options).ok());
    EXPECT_EQ(io_stats.read_calls, 0);
    EXPECT_EQ(reader._deletion_vector->cardinality(), 3);
    auto* decoded = reader._deletion_vector;
    ASSERT_TRUE(reader._parse_delete_predicates(options).ok());
    EXPECT_EQ(decoded, reader._deletion_vector);
    EXPECT_EQ(profile.get_counter("DeletionVectorDecodedCacheMissCount")->value(), 1);
    EXPECT_EQ(profile.get_counter("DeletionVectorDecodedCacheHitCount")->value(), 1);
    dv.__set_cardinality(4);
    auto status = reader._parse_delete_predicates(split(table_params(dv)));
    EXPECT_TRUE(status.is<ErrorCode::DATA_QUALITY_ERROR>()) << status;
    EXPECT_EQ(io_stats.read_calls, 0);
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(DeltaReaderTest, WidensOldSparkFileAndReadsNewSchemaFile) {
    const auto int64_type = std::make_shared<DataTypeInt64>();
    const auto double_type = std::make_shared<DataTypeFloat64>();
    columns = {typed_column(0, "byte_long", int64_type),
               typed_column(1, "int_long", int64_type),
               typed_column(2, "float_double", double_type),
               typed_column(3, "byte_double", double_type),
               typed_column(4, "short_double", double_type),
               typed_column(5, "int_double", double_type),
               typed_column(6, "decimal_decimal_same_scale", create_decimal(20, 2, false)),
               typed_column(7, "decimal_decimal_greater_scale", create_decimal(20, 5, false)),
               typed_column(8, "byte_decimal", create_decimal(11, 1, false)),
               typed_column(9, "short_decimal", create_decimal(11, 1, false)),
               typed_column(10, "int_decimal", create_decimal(11, 1, false)),
               typed_column(11, "long_decimal", create_decimal(21, 1, false)),
               typed_column(12, "date_timestamp_ntz", std::make_shared<DataTypeDateTimeV2>(6))};
    deltalake::DeltaReader reader;
    ASSERT_TRUE(init(&reader).ok());
    const char* files[] = {"part-00000-1045efe0-45bb-4b99-9f83-5ffa04a63ab2-c000.snappy.parquet",
                           "part-00000-cd317895-4ae0-4292-b918-62d4ca832bd7-c000.snappy.parquet"};
    std::vector<std::vector<std::string>> rows;
    for (const auto* file : files) {
        ASSERT_TRUE(reader.prepare_split(split(table_params(), file)).ok());
        read_strings(&reader, &rows);
    }
    const std::vector<std::vector<std::string>> expected {
            {"1", "2", "3.4000000953674316", "5", "6", "7", "123.45", "67.89000", "1.0", "2.0",
             "3.0", "4.0", "2024-09-09 00:00:00.000000"},
            {"9223372036854775807", "9223372036854775807", "1.234567890123", "1.234567890123",
             "1.234567890123", "1.234567890123", "12345678901234.56", "12345678901.23456", "123.4",
             "12345.6", "1234567890.1", "123456789012345678.9", "2024-09-09 12:34:56.123456"}};
    EXPECT_EQ(rows, expected);
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(DeltaReaderTest, WidensByteAndShortToLargerIntegers) {
    const char* file = "part-00000-1045efe0-45bb-4b99-9f83-5ffa04a63ab2-c000.snappy.parquet";
    const DataTypes targets {std::make_shared<DataTypeInt16>(), std::make_shared<DataTypeInt32>(),
                             std::make_shared<DataTypeInt64>()};
    for (size_t i = 0; i < targets.size(); ++i) {
        columns = {typed_column(0, "byte_long", targets[i])};
        if (i > 0) {
            columns.push_back(typed_column(1, "short_double", targets[i]));
        }
        deltalake::DeltaReader reader;
        ASSERT_TRUE(init(&reader).ok());
        ASSERT_TRUE(reader.prepare_split(split(table_params(), file)).ok());
        std::vector<std::vector<std::string>> rows;
        read_strings(&reader, &rows);
        ASSERT_EQ(rows.size(), 1);
        EXPECT_EQ(rows[0][0], "1");
        if (i > 0) {
            EXPECT_EQ(rows[0][1], "6");
        }
        ASSERT_TRUE(reader.close().ok());
    }
}

TEST_F(DeltaReaderTest, WidensNestedStructArrayAndMapValues) {
    auto int64_type = make_nullable(std::make_shared<DataTypeInt64>());
    columns = {
            typed_column(0, "struct",
                         std::make_shared<DataTypeStruct>(DataTypes {int64_type}, Strings {"a"})),
            typed_column(1, "map", std::make_shared<DataTypeMap>(int64_type, int64_type)),
            typed_column(2, "array", std::make_shared<DataTypeArray>(int64_type))};
    const char* files[] = {"part-00000-138244f1-b939-40db-a4bd-d57cf3d214d2-c000.snappy.parquet",
                           "part-00000-1f777f86-350c-4181-b7ef-73df70847eac-c000.snappy.parquet"};
    const int64_t max = std::numeric_limits<int64_t>::max();
    const std::vector<std::vector<int64_t>> expected {
            {1, 2, 3, 4, 5}, {max, max, max, max, std::numeric_limits<int64_t>::min()}};
    deltalake::DeltaReader reader;
    ASSERT_TRUE(init(&reader).ok());
    const auto value = [](const IColumn& column, size_t row) {
        const auto& nullable_column = assert_cast<const ColumnNullable&>(column);
        EXPECT_FALSE(nullable_column.is_null_at(row));
        return assert_cast<const ColumnInt64&>(nullable_column.get_nested_column())
                .get_element(row);
    };
    for (size_t i = 0; i < 2; ++i) {
        SCOPED_TRACE(files[i]);
        ASSERT_TRUE(reader.prepare_split(split(table_params(), files[i])).ok());
        Block block;
        for (const auto& column : columns) {
            block.insert({column.type->create_column(), column.type, column.name});
        }
        bool eos = false;
        auto status = reader.get_block(&block, &eos);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(block.rows(), 1);
        const auto& structure = assert_cast<const ColumnStruct&>(
                assert_cast<const ColumnNullable&>(*block.get_by_position(0).column)
                        .get_nested_column());
        const auto& map = assert_cast<const ColumnMap&>(
                assert_cast<const ColumnNullable&>(*block.get_by_position(1).column)
                        .get_nested_column());
        const auto& array = assert_cast<const ColumnArray&>(
                assert_cast<const ColumnNullable&>(*block.get_by_position(2).column)
                        .get_nested_column());
        EXPECT_EQ(map.get_offsets()[0], 1);
        EXPECT_EQ(array.get_offsets()[0], 2);
        EXPECT_EQ(value(structure.get_column(0), 0), expected[i][0]);
        EXPECT_EQ(value(map.get_keys(), 0), expected[i][1]);
        EXPECT_EQ(value(map.get_values(), 0), expected[i][2]);
        EXPECT_EQ(value(array.get_data(), 0), expected[i][3]);
        EXPECT_EQ(value(array.get_data(), 1), expected[i][4]);
        // FileScannerV2 drains the current split to EOS before preparing the next one.
        while (!eos) {
            status = reader.get_block(&block, &eos);
            ASSERT_TRUE(status.ok()) << status;
            EXPECT_EQ(block.rows(), 0);
        }
    }
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(DeltaReaderTest, TimestampUsesSessionZoneForInt96MicrosAndMillis) {
    columns = {typed_column(0, "time", std::make_shared<DataTypeDateTimeV2>(6))};
    const char* files[] = {"part-00000-bd889aef-417c-4493-b5f7-a9884ba4b247.c000.snappy.parquet",
                           "part-00000-3cac2575-d0b4-4647-a7a3-b4a9d910cb32.c000.snappy.parquet",
                           "part-00000-4b5188f5-4784-47ce-b4ad-1d3eae80710e.c000.snappy.parquet"};
    for (const auto* zone : {"UTC", "Asia/Shanghai"}) {
        state.set_timezone(zone);
        deltalake::DeltaReader reader;
        ASSERT_TRUE(init(&reader).ok());
        const std::string expected = std::string(zone) == "UTC" ? "2020-02-01 08:09:10.000000"
                                                                : "2020-02-01 16:09:10.000000";
        for (const auto* file : files) {
            SCOPED_TRACE(file);
            ASSERT_TRUE(reader.prepare_split(split(table_params(), file)).ok());
            std::vector<std::vector<std::string>> rows;
            read_strings(&reader, &rows);
            ASSERT_EQ(rows.size(), 1);
            EXPECT_EQ(rows[0][0], expected);
        }
        ASSERT_TRUE(reader.close().ok());
    }
}

TEST_F(DeltaReaderTest, TimestampNtzPreservesWallClockAcrossSessionZones) {
    columns = {typed_column(0, "tsntz", std::make_shared<DataTypeDateTimeV2>(6))};
    for (const auto* zone : {"UTC", "Asia/Shanghai", "America/Los_Angeles"}) {
        state.set_timezone(zone);
        deltalake::DeltaReader reader;
        ASSERT_TRUE(init(&reader).ok());
        ASSERT_TRUE(reader.prepare_split(split(table_params(),
                                               "part-00000-6240e68e-2304-449a-a1e6-0e24866d3508."
                                               "c000.snappy.parquet"))
                            .ok());
        std::vector<std::vector<std::string>> rows;
        read_strings(&reader, &rows);
        ASSERT_EQ(rows.size(), 1);
        EXPECT_EQ(rows[0][0], "2021-11-18 02:30:00.123456");
        ASSERT_TRUE(reader.close().ok());
    }
}

TEST_F(DeltaReaderTest, DiskVectorCacheAvoidsSecondRead) {
    deltalake::DeltaReader reader;
    ASSERT_TRUE(init(&reader).ok());
    auto options = split(table_params(disk_dv(2)));
    ASSERT_TRUE(reader._parse_delete_predicates(options).ok());
    EXPECT_EQ(profile.get_counter("DeletionVectorDecodedCacheMissCount")->value(), 1);
    FailDeletionVectorIO fail_io;
    ASSERT_TRUE(reader._parse_delete_predicates(options).ok());
    EXPECT_EQ(profile.get_counter("DeletionVectorDecodedCacheHitCount")->value(), 1);
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(DeltaReaderTest, DeletionVectorsUseFileGlobalPositionsAcrossByteRanges) {
    const auto path = (std::filesystem::path(std::getenv("ROOT")) /
                       "be/ut_build_ASAN/deltalake_multiple_ranges.parquet")
                              .string();
    arrow::Int64Builder builder;
    for (int64_t id = 0; id < 30; ++id) {
        ASSERT_TRUE(builder.Append(id).ok());
    }
    std::shared_ptr<arrow::Array> ids;
    ASSERT_TRUE(builder.Finish(&ids).ok());
    auto table = arrow::Table::Make(arrow::schema({arrow::field("id", arrow::int64())}), {ids});
    auto output = arrow::io::FileOutputStream::Open(path);
    ASSERT_TRUE(output.ok()) << output.status();
    ::parquet::WriterProperties::Builder properties;
    properties.disable_dictionary();
    ASSERT_TRUE(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), *output, 5,
                                             properties.build())
                        .ok());
    ASSERT_TRUE((*output)->Close().ok());
    const auto metadata = ::parquet::ParquetFileReader::OpenFile(path)->metadata();
    ASSERT_EQ(metadata->num_row_groups(), 6);

    deltalake::DeltaReader reader;
    ASSERT_TRUE(init(&reader).ok());
    for (const auto& dv : {disk_dv(2), inline_dv(2)}) {
        for (int group = 0; group < metadata->num_row_groups(); ++group) {
            SCOPED_TRACE(group);
            const auto chunk = metadata->RowGroup(group)->ColumnChunk(0);
            auto options = split(table_params(dv), path.c_str());
            options.current_range.__set_start_offset(chunk->data_page_offset());
            options.current_range.__set_size(chunk->total_compressed_size());
            ASSERT_TRUE(reader.prepare_split(options).ok());
            std::vector<std::optional<int64_t>> actual;
            read(&reader, &actual);
            std::vector<std::optional<int64_t>> expected;
            for (int64_t id = group * 5; id < (group + 1) * 5; ++id) {
                if (id != 0 && id != 7 && id != 14) {
                    expected.emplace_back(id);
                }
            }
            EXPECT_EQ(actual, expected);
        }
    }
    EXPECT_EQ(profile.get_counter("DeletionVectorDecodedCacheMissCount")->value(), 2);
    EXPECT_EQ(profile.get_counter("DeletionVectorDecodedCacheHitCount")->value(), 10);
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(DeltaReaderTest, MapsPhysicalNameToRenamedLogicalColumn) {
    columns = {table_column(7, "renamed")};
    scan_params.__set_current_schema_id(42);
    scan_params.__set_history_schema_info({schema(42, {schema_field("id", 7)})});
    deltalake::DeltaReader reader;
    ASSERT_TRUE(init(&reader).ok());
    ASSERT_TRUE(reader.prepare_split(split(table_params(std::nullopt, 42))).ok());
    EXPECT_EQ(reader.mapping_mode(), TableColumnMappingMode::BY_FIELD_ID);
    std::vector<std::optional<int64_t>> ids;
    read(&reader, &ids);
    ASSERT_EQ(ids.size(), 50);
    for (int64_t i = 0; i < 50; ++i) {
        EXPECT_EQ(ids[i], i);
    }
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(DeltaReaderTest, ReadsSparkFieldIdsAfterRootAndNestedRenames) {
    auto leaf = typed_column(16, "renamed_leaf", std::make_shared<DataTypeInt32>());
    auto inner = typed_column(
            15, "renamed_inner",
            std::make_shared<DataTypeStruct>(DataTypes {leaf.type}, Strings {leaf.name}));
    inner.children = {leaf};
    auto outer = typed_column(
            13, "renamed_outer",
            std::make_shared<DataTypeStruct>(DataTypes {inner.type}, Strings {inner.name}));
    outer.children = {inner};
    columns = {table_column(4, "renamed_long"), outer};
    auto physical_inner = schema_field("col-94e598c8-0480-4710-a288-fc332ed449de", 15);
    schema::external::TStructField inner_fields;
    inner_fields.__set_fields({schema_field("col-75cd857b-0152-438b-a0b3-43db0d4daa55", 16)});
    physical_inner.field_ptr->nestedField.__set_struct_field(inner_fields);
    physical_inner.field_ptr->__isset.nestedField = true;
    auto physical_outer = schema_field("col-ee8dc621-b36b-4784-bdb5-26c5c364e254", 13);
    schema::external::TStructField outer_fields;
    outer_fields.__set_fields({physical_inner});
    physical_outer.field_ptr->nestedField.__set_struct_field(outer_fields);
    physical_outer.field_ptr->__isset.nestedField = true;
    scan_params.__set_current_schema_id(42);
    scan_params.__set_history_schema_info({schema(
            42, {schema_field("col-b55a2771-5f84-432a-9fe4-39e1ec1edfd7", 4), physical_outer})});
    deltalake::DeltaReader reader;
    ASSERT_TRUE(init(&reader).ok());
    ASSERT_TRUE(
            reader
                    .prepare_split(split(
                            table_params(std::nullopt, 42),
                            "part-00000-37fc7686-b5a9-432d-8cdc-8caa8cf999e5-c000.snappy.parquet"))
                    .ok());
    std::vector<int64_t> values;
    bool eos = false;
    while (!eos) {
        Block block;
        for (const auto& column : columns) {
            block.insert({column.type->create_column(), column.type, column.name});
        }
        const auto status = reader.get_block(&block, &eos);
        ASSERT_TRUE(status.ok()) << status;
        const auto& outer_column =
                assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
        const auto& inner_column = assert_cast<const ColumnNullable&>(
                assert_cast<const ColumnStruct&>(outer_column.get_nested_column()).get_column(0));
        const auto& leaf_column = assert_cast<const ColumnNullable&>(
                assert_cast<const ColumnStruct&>(inner_column.get_nested_column()).get_column(0));
        const auto& long_column =
                assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
        for (size_t row = 0; row < block.rows(); ++row) {
            EXPECT_FALSE(outer_column.is_null_at(row));
            EXPECT_FALSE(inner_column.is_null_at(row));
            EXPECT_FALSE(leaf_column.is_null_at(row));
            EXPECT_FALSE(long_column.is_null_at(row));
            const auto value = assert_cast<const ColumnInt64&>(long_column.get_nested_column())
                                       .get_element(row);
            EXPECT_EQ(assert_cast<const ColumnInt32&>(leaf_column.get_nested_column())
                              .get_element(row),
                      value);
            values.push_back(value);
        }
    }
    EXPECT_EQ(values, (std::vector<int64_t> {0, 4}));
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(DeltaReaderTest, ReaddedNameDoesNotReadDroppedColumn) {
    columns = {table_column(8, "id")};
    scan_params.__set_current_schema_id(42);
    scan_params.__set_history_schema_info({schema(42, {schema_field("id", 7)})});
    deltalake::DeltaReader reader;
    ASSERT_TRUE(init(&reader).ok());
    ASSERT_TRUE(reader.prepare_split(split(table_params(std::nullopt, 42))).ok());
    std::vector<std::optional<int64_t>> ids;
    read(&reader, &ids);
    ASSERT_EQ(ids.size(), 50);
    for (const auto& id : ids) {
        EXPECT_FALSE(id.has_value());
    }
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(DeltaReaderTest, AnnotatesNestedPhysicalNames) {
    auto parent = schema_field("physical_parent", 10);
    schema::external::TStructField children;
    children.__set_fields({schema_field("physical_child", 11)});
    parent.field_ptr->nestedField.__set_struct_field(children);
    parent.field_ptr->__isset.nestedField = true;
    scan_params.__set_current_schema_id(42);
    scan_params.__set_history_schema_info({schema(42, {parent})});
    deltalake::DeltaReader reader;
    ASSERT_TRUE(init(&reader).ok());
    reader._split_schema_id = 42;
    ColumnDefinition child = table_column(0, "physical_child");
    ColumnDefinition file_column;
    file_column.name = "physical_parent";
    file_column.type =
            std::make_shared<DataTypeStruct>(DataTypes {child.type}, Strings {child.name});
    file_column.children = {child};
    std::vector<ColumnDefinition> file_schema {file_column};
    ASSERT_TRUE(reader.annotate_file_schema(&file_schema).ok());
    EXPECT_EQ(file_schema[0].identifier.get<TYPE_INT>(), 10);
    EXPECT_EQ(file_schema[0].children[0].identifier.get<TYPE_INT>(), 11);
    ASSERT_TRUE(reader.close().ok());
}

TEST(DeltaDeletionVectorTest, ReadsPortableSparkVectorsAndRejectsCorruption) {
    for (size_t i = 0; i < 3; ++i) {
        const auto file = read_fixture(DV_FILES[i]);
        ASSERT_EQ(file.size(), 43 + i * 2);
        const auto framed = file.substr(1);
        DeletionVector bitmap;
        ASSERT_TRUE(
                decode_iceberg_deletion_vector_buffer(framed.data(), framed.size(), &bitmap).ok());
        ASSERT_EQ(bitmap.cardinality(), i + 1);
        for (size_t n = 0; n <= i; ++n) {
            EXPECT_TRUE(bitmap.contains(static_cast<uint64_t>(n * 7)));
        }
        for (size_t offset : {size_t {0}, size_t {4}, framed.size() - 1}) {
            auto corrupt = framed;
            corrupt[offset] ^= 1;
            DeletionVector rejected;
            auto status = decode_iceberg_deletion_vector_buffer(corrupt.data(), corrupt.size(),
                                                                &rejected);
            EXPECT_TRUE(status.is<ErrorCode::DATA_QUALITY_ERROR>()) << status;
            EXPECT_TRUE(rejected.isEmpty());
        }
        DeletionVector inline_bitmap;
        const auto payload = framed.substr(4, framed.size() - 8);
        ASSERT_TRUE(decode_portable_deletion_vector_payload(payload.data(), payload.size(),
                                                            &inline_bitmap)
                            .ok());
        EXPECT_EQ(inline_bitmap, bitmap);
    }
}

TEST(DeltaDeletionVectorTest, RejectsInvalidInlineAndDiskDescriptors) {
    deltalake::DeltaReader reader;
    DeleteFileDesc desc;
    bool has_delete = false;
    auto dv = inline_dv(0);
    dv.__set_size_in_bytes(dv.size_in_bytes + 1);
    EXPECT_TRUE(reader._parse_deletion_vector_file(table_params(dv), &desc, &has_delete)
                        .is<ErrorCode::DATA_QUALITY_ERROR>());
    EXPECT_FALSE(has_delete);
    dv = disk_dv(0);
    dv.__set_cardinality(-1);
    EXPECT_TRUE(reader._parse_deletion_vector_file(table_params(dv), &desc, &has_delete)
                        .is<ErrorCode::DATA_QUALITY_ERROR>());
    size_t bytes = 0;
    EXPECT_TRUE(validate_delta_deletion_vector_read_range(-1, 34, bytes)
                        .is<ErrorCode::DATA_QUALITY_ERROR>());
    EXPECT_TRUE(validate_delta_deletion_vector_read_range(0, -1, bytes)
                        .is<ErrorCode::DATA_QUALITY_ERROR>());
    EXPECT_TRUE(validate_delta_deletion_vector_read_range(std::numeric_limits<int64_t>::max(), 34,
                                                          bytes)
                        .is<ErrorCode::DATA_QUALITY_ERROR>());
    EXPECT_TRUE(
            validate_delta_deletion_vector_read_range(0, std::numeric_limits<int32_t>::max(), bytes)
                    .is<ErrorCode::NOT_IMPLEMENTED_ERROR>());
    ASSERT_TRUE(validate_delta_deletion_vector_read_range(1, 34, bytes).ok());
    EXPECT_EQ(bytes, 42);
}

TEST(DeltaDeletionVectorTest, CacheKeysDistinguishRangesAndInlinePayloads) {
    deltalake::DeltaReader reader;
    auto dv = disk_dv(0);
    DeleteFileDesc first;
    bool has_delete;
    ASSERT_TRUE(reader._parse_deletion_vector_file(table_params(dv), &first, &has_delete).ok());
    EXPECT_TRUE(has_delete);
    for (int field = 0; field < 2; ++field) {
        auto other = dv;
        if (field == 0) {
            other.__set_offset(2);
        } else {
            other.__set_size_in_bytes(36);
        }
        DeleteFileDesc changed;
        ASSERT_TRUE(reader._parse_deletion_vector_file(table_params(other), &changed, &has_delete)
                            .ok());
        EXPECT_NE(first.key, changed.key);
    }
    DeleteFileDesc inline_first;
    DeleteFileDesc inline_second;
    ASSERT_TRUE(reader._parse_deletion_vector_file(table_params(inline_dv(0)), &inline_first,
                                                   &has_delete)
                        .ok());
    ASSERT_TRUE(reader._parse_deletion_vector_file(table_params(inline_dv(1)), &inline_second,
                                                   &has_delete)
                        .ok());
    EXPECT_NE(first.key, inline_first.key);
    EXPECT_NE(inline_first.key, inline_second.key);
}

} // namespace
} // namespace doris::format
