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

#include <optional>
#include <string_view>

#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
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

} // namespace doris
