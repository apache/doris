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

#include "format/transformer/vparquet_writer.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <gtest/gtest.h>
#include <parquet/api/reader.h>
#include <parquet/arrow/reader.h>
#include <parquet/schema.h>

#include <algorithm>
#include <optional>
#include <string_view>

#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_decimal.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_spatial.h"
#include "core/data_type/data_type_timestamp_ns.h"
#include "exec/sink/writer/iceberg/viceberg_partition_writer.h"
#include "exec/sink/writer/vhive_partition_writer.h"
#include "format/table/iceberg/schema_parser.h"
#include "format/transformer/viceberg_parquet_writer.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"
#include "util/timezone_utils.h"
#include "util/uid_util.h"

namespace doris {

class VParquetWriterTest : public testing::Test {
protected:
    static void SetUpTestSuite() { TimezoneUtils::load_timezones_to_cache(); }

    void SetUp() override {
        _file_path = "./vparquet_transformer_" + UniqueId::gen_uid().to_string() + ".parquet";
        _fs = io::global_local_filesystem();
        // The standalone writer fixture has no initialized execution environment.
        _previous_pool = ExecEnv::GetInstance()->_arrow_memory_pool;
        ExecEnv::GetInstance()->_arrow_memory_pool = arrow::default_memory_pool();
    }

    void TearDown() override {
        ExecEnv::GetInstance()->_arrow_memory_pool = _previous_pool;
        static_cast<void>(_fs->delete_file(_file_path));
    }

    void check_decimal_bounds(int64_t unscaled, const std::string& expected) {
        const int min_precision = unscaled > -10 && unscaled < 10 ? 1 : 3;
        DataTypes types;
        std::vector<std::string> names;
        std::string json = R"({"type":"struct","fields":[)";
        Block block;
        for (int precision = min_precision; precision <= 38; ++precision) {
            const auto name = "d" + std::to_string(precision);
            const int scale = std::min(precision, 2);
            auto type = make_nullable(create_decimal(precision, scale, false));
            auto column = type->create_column();
            if (precision <= 9) {
                column->insert(Field::create_field<TYPE_DECIMAL32>(Decimal32(unscaled)));
            } else if (precision <= 18) {
                column->insert(Field::create_field<TYPE_DECIMAL64>(Decimal64(unscaled)));
            } else {
                column->insert(Field::create_field<TYPE_DECIMAL128I>(Decimal128V3(unscaled)));
            }
            column->insert_default();
            block.insert({std::move(column), type, name});
            types.push_back(type);
            names.push_back(name);
            if (precision != min_precision) {
                json += ",";
            }
            json += "{\"id\":" + std::to_string(precision) + ",\"name\":\"" + name +
                    "\",\"required\":false,\"type\":\"decimal(" + std::to_string(precision) + "," +
                    std::to_string(scale) + ")\"}";
        }
        json += "]}";
        auto schema = iceberg::SchemaParser::from_json(json);
        auto output_exprs = MockSlotRef::create_mock_contexts(types);
        io::FileWriterPtr file_writer;
        ASSERT_TRUE(_fs->create_file(_file_path, &file_writer).ok());
        RuntimeState state;
        state.set_timezone("UTC");
        ParquetFileOptions options {.compression_type = TParquetCompressionType::UNCOMPRESSED,
                                    .parquet_version = TParquetVersion::PARQUET_1_0,
                                    .parquet_disable_dictionary = false,
                                    .enable_int96_timestamps = false};
        VIcebergParquetWriter writer(&state, file_writer.get(), output_exprs, names, false, options,
                                     &json, *schema);
        ASSERT_TRUE(writer.open().ok());
        ASSERT_TRUE(writer.write(block).ok());
        ASSERT_TRUE(writer.close().ok());
        TIcebergColumnStats stats;
        ASSERT_TRUE(writer.collect_file_statistics_after_close(&stats).ok());
        for (int precision = min_precision; precision <= 38; ++precision) {
            SCOPED_TRACE("precision=" + std::to_string(precision));
            EXPECT_EQ(expected, stats.lower_bounds.at(precision));
            EXPECT_EQ(expected, stats.upper_bounds.at(precision));
            EXPECT_EQ(2, stats.value_counts.at(precision));
            EXPECT_EQ(1, stats.null_value_counts.at(precision));
        }
    }

    static void check_spatial_annotation(const DataTypeSpatial& identity,
                                         const ::parquet::ColumnDescriptor& field) {
        const bool geography = identity.get_primitive_type() == TYPE_GEOGRAPHY;
        EXPECT_EQ(field.max_definition_level(), 1);
        const auto& logical = field.logical_type();
        if (geography) {
            ASSERT_TRUE(logical->is_geography());
            const auto& annotation = static_cast<const ::parquet::GeographyLogicalType&>(*logical);
            EXPECT_EQ(annotation.crs(), identity.crs());
            EXPECT_EQ(annotation.algorithm_name(), identity.algorithm());
        } else {
            ASSERT_TRUE(logical->is_geometry());
            EXPECT_EQ(static_cast<const ::parquet::GeometryLogicalType&>(*logical).crs(),
                      identity.crs());
        }
    }

    static void check_spatial_values(::parquet::ParquetFileReader& reader, const std::string& wkb) {
        EXPECT_EQ(reader.metadata()->num_rows(), 2);
        auto values =
                std::static_pointer_cast<::parquet::ByteArrayReader>(reader.RowGroup(0)->Column(0));
        int16_t levels[2];
        ::parquet::ByteArray binary;
        int64_t values_read;
        EXPECT_EQ(values->ReadBatch(2, levels, nullptr, &binary, &values_read), 2);
        ASSERT_EQ(values_read, 1);
        EXPECT_EQ(levels[0], 1);
        EXPECT_EQ(levels[1], 0);
        EXPECT_EQ(std::string(reinterpret_cast<const char*>(binary.ptr), binary.len), wkb);
    }

    void check_spatial_file(const DataTypePtr& spatial) {
        const std::string wkb(
                "\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@",
                21);
        const auto type = make_nullable(spatial);
        auto column = type->create_column();
        column->insert_data(wkb.data(), wkb.size());
        column->insert_default();
        Block block;
        block.insert({std::move(column), type, "shape"});
        const auto& identity = assert_cast<const DataTypeSpatial&>(*spatial);
        const bool geography = identity.get_primitive_type() == TYPE_GEOGRAPHY;
        const std::string schema_type =
                geography ? "geography(" + identity.crs() + "," + identity.algorithm() + ")"
                          : "geometry(" + identity.crs() + ")";
        const std::string json =
                "{\"type\":\"struct\",\"fields\":[{\"id\":1,\"name\":\"shape\","
                "\"required\":false,\"type\":\"" +
                schema_type + "\"}]}";
        auto schema = iceberg::SchemaParser::from_json(json);
        auto expressions = MockSlotRef::create_mock_contexts(DataTypes {type});
        io::FileWriterPtr output;
        ASSERT_TRUE(_fs->create_file(_file_path, &output).ok());
        RuntimeState state;
        state.set_timezone("UTC");
        ParquetFileOptions options {.compression_type = TParquetCompressionType::UNCOMPRESSED,
                                    .parquet_version = TParquetVersion::PARQUET_2_LATEST};
        VIcebergParquetWriter writer(&state, output.get(), expressions, {"shape"}, false, options,
                                     &json, *schema);
        ASSERT_TRUE(writer.open().ok());
        ASSERT_TRUE(writer.write(block).ok());
        ASSERT_TRUE(writer.close().ok());
        TIcebergColumnStats stats;
        ASSERT_TRUE(writer.collect_file_statistics_after_close(&stats).ok());
        EXPECT_EQ(stats.lower_bounds.count(1), 0);
        EXPECT_EQ(stats.upper_bounds.count(1), 0);
        auto reader = ::parquet::ParquetFileReader::OpenFile(_file_path);
        check_spatial_annotation(assert_cast<const DataTypeSpatial&>(*spatial),
                                 *reader->metadata()->schema()->Column(0));
        check_spatial_values(*reader, wkb);
    }

    arrow::MemoryPool* _previous_pool = nullptr;
    std::string _file_path;
    std::shared_ptr<io::FileSystem> _fs;
};

TEST_F(VParquetWriterTest, WritesIcebergPrimitiveAndCollectsMetrics) {
    auto type = make_nullable(std::make_shared<DataTypeInt32>());
    auto output_exprs = MockSlotRef::create_mock_contexts(DataTypes {type});
    const std::string json =
            R"({"type":"struct","fields":[{"id":7,"name":"value","required":false,"type":"int"}]})";
    auto schema = iceberg::SchemaParser::from_json(json);
    io::FileWriterPtr file_writer;
    ASSERT_TRUE(_fs->create_file(_file_path, &file_writer).ok());
    RuntimeState state;
    state.set_timezone("UTC");
    ParquetFileOptions options {.compression_type = TParquetCompressionType::UNCOMPRESSED,
                                .parquet_version = TParquetVersion::PARQUET_1_0,
                                .parquet_disable_dictionary = false,
                                .enable_int96_timestamps = false};
    VIcebergParquetWriter writer(&state, file_writer.get(), output_exprs, {"value"}, false, options,
                                 &json, *schema);
    ASSERT_TRUE(writer.open().ok());
    auto column = type->create_column();
    column->insert(Field::create_field<TYPE_INT>(1));
    column->insert_default();
    column->insert(Field::create_field<TYPE_INT>(3));
    Block block;
    block.insert({std::move(column), type, "value"});
    ASSERT_TRUE(writer.write(block).ok());
    ASSERT_TRUE(writer.close().ok());
    TIcebergColumnStats stats;
    ASSERT_TRUE(writer.collect_file_statistics_after_close(&stats).ok());
    // Moving statistics into the Iceberg writer must preserve logical field IDs and null counts.
    ASSERT_EQ(1, stats.value_counts.size());
    EXPECT_EQ(3, stats.value_counts.at(7));
    EXPECT_EQ(1, stats.null_value_counts.at(7));
    EXPECT_GT(stats.column_sizes.at(7), 0);
    EXPECT_EQ(std::string("\x01\x00\x00\x00", 4), stats.lower_bounds.at(7));
    EXPECT_EQ(std::string("\x03\x00\x00\x00", 4), stats.upper_bounds.at(7));
}

TEST_F(VParquetWriterTest, SpatialOptionalFilesPreserveAnnotationsBytesAndNullRows) {
    const DataTypes types {
            std::make_shared<DataTypeSpatial>(TYPE_GEOMETRY, "EPSG:3857"),
            std::make_shared<DataTypeSpatial>(TYPE_GEOGRAPHY, "OGC:CRS84", "spherical"),
            std::make_shared<DataTypeSpatial>(TYPE_GEOGRAPHY, "OGC:CRS84", "vincenty"),
            std::make_shared<DataTypeSpatial>(TYPE_GEOGRAPHY, "OGC:CRS84", "thomas"),
            std::make_shared<DataTypeSpatial>(TYPE_GEOGRAPHY, "OGC:CRS84", "andoyer"),
            std::make_shared<DataTypeSpatial>(TYPE_GEOGRAPHY, "OGC:CRS84", "karney")};
    for (const auto& spatial : types) {
        check_spatial_file(spatial);
    }
}

TEST_F(VParquetWriterTest, RejectsExistingIcebergNestedSpatialSchema) {
    auto type = make_nullable(std::make_shared<DataTypeArray>(
            make_nullable(std::make_shared<DataTypeSpatial>(TYPE_GEOMETRY))));
    auto output_exprs = MockSlotRef::create_mock_contexts(DataTypes {type});
    const std::string json = R"JSON({"type":"struct","fields":[
        {"id":1,"name":"shapes","required":false,"type":{"type":"list",
         "element-id":2,"element-required":false,"element":"geometry"}}]})JSON";
    auto schema = iceberg::SchemaParser::from_json(json);
    io::FileWriterPtr file_writer;
    ASSERT_TRUE(_fs->create_file(_file_path, &file_writer).ok());
    RuntimeState state;
    state.set_timezone("UTC");
    ParquetFileOptions options {.compression_type = TParquetCompressionType::UNCOMPRESSED,
                                .parquet_version = TParquetVersion::PARQUET_2_LATEST};
    VIcebergParquetWriter writer(&state, file_writer.get(), output_exprs, {"shapes"}, false,
                                 options, &json, *schema);
    const auto status = writer.open();
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("Nested Iceberg spatial columns"), std::string::npos);
}

TEST_F(VParquetWriterTest, IcebergDecimalBoundsUseMinimalSignedBytes) {
    for (const auto& [unscaled, expected] :
         std::vector<std::pair<int64_t, std::string>> {{0, std::string("\x00", 1)},
                                                       {1, std::string("\x01", 1)},
                                                       {127, std::string("\x7f", 1)},
                                                       {128, std::string("\x00\x80", 2)},
                                                       {255, std::string("\x00\xff", 2)},
                                                       {256, std::string("\x01\x00", 2)},
                                                       {-1, std::string("\xff", 1)},
                                                       {-128, std::string("\x80", 1)},
                                                       {-129, std::string("\xff\x7f", 2)},
                                                       {-256, std::string("\xff\x00", 2)}}) {
        SCOPED_TRACE("unscaled=" + std::to_string(unscaled));
        check_decimal_bounds(unscaled, expected);
    }
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity): keep schema and value assertions together.
TEST_F(VParquetWriterTest, WritesInt64TimestampSemantics) {
    DataTypes types {DataTypeFactory::instance().create_data_type(TYPE_DATETIMEV2, false, 0, 6),
                     DataTypeFactory::instance().create_data_type(TYPE_TIMESTAMPTZ, false, 0, 6)};
    VExprContextSPtrs output_exprs = MockSlotRef::create_mock_contexts(types);

    io::FileWriterPtr file_writer;
    ASSERT_TRUE(_fs->create_file(_file_path, &file_writer).ok());
    RuntimeState state;
    state.set_timezone("Asia/Shanghai");
    ParquetFileOptions options {.compression_type = TParquetCompressionType::UNCOMPRESSED,
                                .parquet_version = TParquetVersion::PARQUET_1_0,
                                .parquet_disable_dictionary = false,
                                .enable_int96_timestamps = false};
    VParquetWriter transformer(&state, file_writer.get(), output_exprs,
                               std::vector<std::string> {"local_time", "instant"}, false, options);
    ASSERT_TRUE(transformer.open().ok());
    Block block;
    DateV2Value<DateTimeV2ValueType> value;
    value.unchecked_set_time(2023, 4, 20, 0, 0, 0, 123456);
    const auto packed = value.to_date_int_val();
    for (size_t index = 0; index < types.size(); ++index) {
        auto column = types[index]->create_column();
        column->insert_data(reinterpret_cast<const char*>(&packed), sizeof(packed));
        block.insert({std::move(column), types[index], index == 0 ? "local_time" : "instant"});
    }
    ASSERT_TRUE(transformer.write(block).ok());
    ASSERT_TRUE(transformer.close().ok());

    auto reader = ::parquet::ParquetFileReader::OpenFile(_file_path, false);
    const auto* root = reader->metadata()->schema()->group_node();
    ASSERT_EQ(2, root->field_count());
    ASSERT_NE(nullptr, root->field(0)->logical_type());
    ASSERT_NE(nullptr, root->field(1)->logical_type());
    // Local DATETIMEV2 must not acquire instant semantics from the writer session timezone.
    EXPECT_NE(std::string::npos,
              root->field(0)->logical_type()->ToString().find("isAdjustedToUTC=false"));
    EXPECT_NE(std::string::npos,
              root->field(1)->logical_type()->ToString().find("isAdjustedToUTC=true"));
    auto input = arrow::io::ReadableFile::Open(_file_path);
    ASSERT_TRUE(input.ok());
    auto arrow_reader = ::parquet::arrow::OpenFile(*input, arrow::default_memory_pool());
    ASSERT_TRUE(arrow_reader.ok());
    auto table_result = (*arrow_reader)->ReadTable();
    ASSERT_TRUE(table_result.ok()) << table_result.status();
    const auto& table = *table_result;
    ASSERT_EQ(1, table->num_rows());
    for (int index = 0; index < 2; ++index) {
        const auto& timestamp =
                assert_cast<const arrow::TimestampArray&>(*table->column(index)->chunk(0));
        // Local time uses UTC coordinates without shifting; TIMESTAMPTZ is already stored in UTC.
        EXPECT_EQ(1681948800123456LL, timestamp.Value(0));
    }
}

TEST_F(VParquetWriterTest, WritesInt96DatetimeUsingWriterTimezone) {
    auto datetime_type = DataTypeFactory::instance().create_data_type(TYPE_DATETIMEV2, false, 0, 6);
    VExprContextSPtrs output_exprs = MockSlotRef::create_mock_contexts(DataTypes {datetime_type});

    io::FileWriterPtr file_writer;
    ASSERT_TRUE(_fs->create_file(_file_path, &file_writer).ok());
    RuntimeState state;
    state.set_timezone("Asia/Shanghai");
    ParquetFileOptions options {.compression_type = TParquetCompressionType::UNCOMPRESSED,
                                .parquet_version = TParquetVersion::PARQUET_1_0,
                                .parquet_disable_dictionary = false,
                                .enable_int96_timestamps = true};
    VParquetWriter transformer(&state, file_writer.get(), output_exprs, {"local_time"}, false,
                               options);
    ASSERT_TRUE(transformer.open().ok());

    DateV2Value<DateTimeV2ValueType> datetime;
    const std::string format = "%Y-%m-%d %H:%i:%s.%f";
    const std::string value = "2023-04-20 00:00:00.123456";
    ASSERT_TRUE(datetime.from_date_format_str(format.data(), format.size(), value.data(),
                                              value.size()));
    auto column = ColumnDateTimeV2::create();
    column->insert_value(datetime);
    Block block;
    block.insert(ColumnWithTypeAndName(std::move(column), datetime_type, "local_time"));
    const auto write_status = transformer.write(block);
    ASSERT_TRUE(write_status.ok()) << write_status.to_string();
    ASSERT_TRUE(transformer.close().ok());

    auto physical_reader = ::parquet::ParquetFileReader::OpenFile(_file_path, false);
    const auto* root = physical_reader->metadata()->schema()->group_node();
    ASSERT_EQ(1, root->field_count());
    const auto& primitive = assert_cast<const ::parquet::schema::PrimitiveNode&>(*root->field(0));
    EXPECT_EQ(::parquet::Type::INT96, primitive.physical_type());

    auto input_result = arrow::io::ReadableFile::Open(_file_path);
    ASSERT_TRUE(input_result.ok()) << input_result.status();
    auto arrow_reader_result =
            ::parquet::arrow::OpenFile(*input_result, arrow::default_memory_pool());
    ASSERT_TRUE(arrow_reader_result.ok()) << arrow_reader_result.status();
    std::unique_ptr<::parquet::arrow::FileReader> arrow_reader = std::move(*arrow_reader_result);
    auto table_result = arrow_reader->ReadTable();
    ASSERT_TRUE(table_result.ok()) << table_result.status();
    const auto& table = *table_result;
    ASSERT_EQ(1, table->num_rows());
    const auto& timestamp = assert_cast<const arrow::TimestampArray&>(*table->column(0)->chunk(0));
    const auto& timestamp_type = assert_cast<const arrow::TimestampType&>(*timestamp.type());
    int64_t epoch_micros = timestamp.Value(0);
    if (timestamp_type.unit() == arrow::TimeUnit::NANO) {
        epoch_micros /= 1000;
    }
    // Hive-compatible INT96 encodes the UTC instant for the writer's local DATETIMEV2 value.
    EXPECT_EQ(1681920000123456LL, epoch_micros);
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity): assertions cover the catalog timezone matrix.
TEST_F(VParquetWriterTest, HiveInt96HonorsCatalogTimezoneContract) {
    auto datetime_type = DataTypeFactory::instance().create_data_type(TYPE_DATETIMEV2, false, 0, 6);
    VExprContextSPtrs output_exprs = MockSlotRef::create_mock_contexts(DataTypes {datetime_type});
    const std::map<std::string, std::string> hadoop_conf;
    // The insert session must not override either a named catalog zone or explicit wall-clock mode.
    for (const auto& [catalog_zone, expected_micros] :
         std::vector<std::pair<std::optional<std::string>, int64_t>> {
                 {std::nullopt, 1681920000123456LL},
                 {"", 1681948800123456LL},
                 {"UTC", 1681948800123456LL},
                 {"+05:45", 1681928100123456LL},
                 {"America/Los_Angeles", 1681974000123456LL}}) {
        SCOPED_TRACE(catalog_zone.value_or("legacy"));
        TDataSink sink;
        if (catalog_zone.has_value()) {
            sink.hive_table_sink.__set_hive_parquet_time_zone(*catalog_zone);
        }
        RuntimeState state;
        state.set_timezone("Asia/Shanghai");
        const std::string file_name = "hive_int96_" + UniqueId::gen_uid().to_string();
        VHivePartitionWriter writer(sink, "", TUpdateMode::APPEND, output_exprs, {"local_time"},
                                    {.write_path = ".",
                                     .original_write_path = ".",
                                     .target_path = ".",
                                     .file_type = TFileType::FILE_LOCAL,
                                     .broker_addresses = {}},
                                    file_name, 0, TFileFormatType::FORMAT_PARQUET,
                                    TFileCompressType::PLAIN, nullptr, hadoop_conf);
        _file_path = "./" + file_name + "-0.parquet";
        ASSERT_TRUE(writer.open(&state, nullptr).ok());
        DateV2Value<DateTimeV2ValueType> datetime;
        const std::string format = "%Y-%m-%d %H:%i:%s.%f";
        const std::string value = "2023-04-20 00:00:00.123456";
        ASSERT_TRUE(datetime.from_date_format_str(format.data(), format.size(), value.data(),
                                                  value.size()));
        auto column = ColumnDateTimeV2::create();
        column->insert_value(datetime);
        Block block;
        block.insert(ColumnWithTypeAndName(std::move(column), datetime_type, "local_time"));
        ASSERT_TRUE(writer.write(block).ok());
        ASSERT_TRUE(writer.close(Status::OK()).ok());
        auto physical_reader = ::parquet::ParquetFileReader::OpenFile(_file_path, false);
        EXPECT_EQ(::parquet::Type::INT96,
                  physical_reader->metadata()->schema()->Column(0)->physical_type());
        auto input = arrow::io::ReadableFile::Open(_file_path);
        ASSERT_TRUE(input.ok()) << input.status();
        auto reader = ::parquet::arrow::OpenFile(*input, arrow::default_memory_pool());
        ASSERT_TRUE(reader.ok()) << reader.status();
        auto table_result = (*reader)->ReadTable();
        ASSERT_TRUE(table_result.ok()) << table_result.status();
        const auto& table = *table_result;
        ASSERT_EQ(1, table->num_rows());
        const auto& timestamp =
                assert_cast<const arrow::TimestampArray&>(*table->column(0)->chunk(0));
        const auto& type = assert_cast<const arrow::TimestampType&>(*timestamp.type());
        EXPECT_EQ(expected_micros,
                  timestamp.Value(0) / (type.unit() == arrow::TimeUnit::NANO ? 1000 : 1));
        EXPECT_EQ("Asia/Shanghai", state.timezone());
        ASSERT_TRUE(_fs->delete_file(_file_path).ok());
    }
}

TEST_F(VParquetWriterTest, IcebergNanosecondsRoundTripWithoutCoercion) {
    const auto type = make_nullable(std::make_shared<DataTypeTimeStampNs>());
    auto exprs = MockSlotRef::create_mock_contexts(DataTypes {type});
    const std::string json =
            R"({"type":"struct","fields":[{"id":1,"name":"ts","required":false,"type":"timestamp_ns"}]})";
    auto schema = iceberg::SchemaParser::from_json(json);
    const std::map<std::string, std::string> hadoop_conf;
    TDataSink sink;
    RuntimeState state;
    state.set_timezone("Asia/Shanghai");
    const auto name = "iceberg_ns_" + UniqueId::gen_uid().to_string();
    _file_path = "./" + name + "-0.parquet";
    VIcebergPartitionWriter writer(sink, {}, exprs, *schema, &json, {"ts"},
                                   {.write_path = ".",
                                    .original_write_path = ".",
                                    .target_path = ".",
                                    .file_type = TFileType::FILE_LOCAL,
                                    .broker_addresses = {}},
                                   name, 0, TFileFormatType::FORMAT_PARQUET,
                                   TFileCompressType::PLAIN, hadoop_conf);
    ASSERT_TRUE(writer.open(&state, nullptr, nullptr).ok());
    const std::vector<int64_t> nanos = {std::numeric_limits<int64_t>::min(), -1, 0, 1, 123456789,
                                        std::numeric_limits<int64_t>::max()};
    auto column = type->create_column();
    for (auto value : nanos) {
        column->insert(Field::create_field<TYPE_TIMESTAMP_NS>(TimeStampNsValue(value)));
    }
    column->insert_default();
    Block block({{std::move(column), type, "ts"}});
    auto status = writer.write(block);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_TRUE(writer.close(Status::OK()).ok());
    auto input = arrow::io::ReadableFile::Open(_file_path);
    ASSERT_TRUE(input.ok()) << input.status();
    auto reader = ::parquet::arrow::OpenFile(*input, arrow::default_memory_pool());
    ASSERT_TRUE(reader.ok()) << reader.status();
    auto table = (*reader)->ReadTable();
    ASSERT_TRUE(table.ok()) << table.status();
    const auto& values = assert_cast<const arrow::TimestampArray&>(*(*table)->column(0)->chunk(0));
    const auto& timestamp_type = assert_cast<const arrow::TimestampType&>(*values.type());
    EXPECT_EQ(arrow::TimeUnit::NANO, timestamp_type.unit());
    EXPECT_TRUE(timestamp_type.timezone().empty());
    ASSERT_EQ(nanos.size() + 1, values.length());
    for (size_t row = 0; row < nanos.size(); ++row) {
        EXPECT_EQ(nanos[row], values.Value(row));
    }
    EXPECT_TRUE(values.IsNull(nanos.size()));
}

} // namespace doris
