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

#include "format/arrow/arrow_block_convertor.h"

#include <arrow/api.h>
#include <arrow/extension_type.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <gtest/gtest.h>

#include "core/column/column_const.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_struct.h"
#include "format/arrow/arrow_row_batch.h"
#include "format/parquet/parquet_arrow_block_convertor.h"
#include "format/table/hive/hive_arrow_block_convertor.h"
#include "format/table/iceberg/iceberg_arrow_block_convertor.h"
#include "format/table/iceberg/schema.h"
#include "format/table/iceberg/schema_parser.h"
#include "format/table/paimon/paimon_arrow_block_convertor.h"
#include "udf/python/python_udf_meta.h"
#include "util/timezone_utils.h"

namespace doris {

class ArrowBlockConvertorTest : public testing::Test {
protected:
    static void SetUpTestSuite() { TimezoneUtils::load_timezones_to_cache(); }
};

TEST_F(ArrowBlockConvertorTest, ParquetOwnsSchemaAndTimezoneParameters) {
    cctz::time_zone shanghai;
    ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone("Asia/Shanghai", shanghai));
    DataTypes types {DataTypeFactory::instance().create_data_type(TYPE_DATETIMEV2, false, 0, 6),
                     DataTypeFactory::instance().create_data_type(TYPE_TIMESTAMPTZ, false, 0, 6)};
    ParquetArrowBlockConvertor parquet(types, {"local_time", "instant"}, "Asia/Shanghai", shanghai,
                                       true);
    hive::HiveArrowBlockConvertor hive(types, {"local_time", "instant"}, "UTC",
                                       cctz::utc_time_zone(), true);
    ASSERT_TRUE(parquet.init().ok());
    ASSERT_TRUE(hive.init().ok());
    const auto timestamp = [](const ArrowBlockConvertor& converter,
                              int index) -> const arrow::TimestampType& {
        return static_cast<const arrow::TimestampType&>(
                *converter.arrow_schema()->field(index)->type());
    };
    EXPECT_EQ("Asia/Shanghai", timestamp(parquet, 0).timezone());
    EXPECT_EQ("Asia/Shanghai", timestamp(parquet, 1).timezone());
    EXPECT_EQ("UTC", timestamp(hive, 0).timezone());
    // A second writer must not change an existing writer's schema or timestamp binding.
    ASSERT_TRUE(hive.init().ok());
    EXPECT_EQ("Asia/Shanghai", timestamp(parquet, 0).timezone());
    EXPECT_EQ("Asia/Shanghai", timestamp(parquet, 1).timezone());
}

TEST_F(ArrowBlockConvertorTest, ParquetRejectsMismatchedColumnNames) {
    ParquetArrowBlockConvertor converter({std::make_shared<DataTypeInt32>()}, {}, "UTC",
                                         cctz::utc_time_zone(), true);
    EXPECT_FALSE(converter.init().ok());
    EXPECT_EQ(nullptr, converter.arrow_schema());
}

TEST_F(ArrowBlockConvertorTest, IcebergBuildsItsOwnSchemaAndMetadata) {
    const std::string json =
            R"({"type":"struct","fields":[{"id":7,"name":"payload","required":false,"type":"variant"}]})";
    auto schema = iceberg::SchemaParser::from_json(json);
    iceberg::IcebergArrowBlockConvertor converter(*schema, &json, "UTC", cctz::utc_time_zone());
    ASSERT_TRUE(converter.init().ok());
    const auto& arrow_schema = converter.arrow_schema();
    ASSERT_NE(nullptr, arrow_schema);
    ASSERT_EQ(arrow::Type::EXTENSION, arrow_schema->field(0)->type()->id());
    const auto& variant = static_cast<const arrow::ExtensionType&>(*arrow_schema->field(0)->type());
    EXPECT_EQ(arrow::Type::STRUCT, variant.storage_type()->id());
    EXPECT_EQ("arrow.parquet.variant", variant.extension_name());
    ASSERT_NE(nullptr, arrow_schema->metadata());
    EXPECT_EQ(json, arrow_schema->metadata()->Get("iceberg.schema").ValueOrDie());
    EXPECT_EQ("7", arrow_schema->field(0)->metadata()->Get("PARQUET:field_id").ValueOrDie());
}

TEST_F(ArrowBlockConvertorTest, PaimonDecodesPinnedSchemaAndValidatesBlock) {
    auto schema = arrow::schema({arrow::field(
            "items",
            arrow::list(arrow::field("item", arrow::timestamp(arrow::TimeUnit::MICRO), false)),
            false)});
    auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
    auto writer = arrow::ipc::MakeStreamWriter(sink, schema).ValueOrDie();
    ASSERT_TRUE(writer->Close().ok());
    auto buffer = sink->Finish().ValueOrDie();
    paimon::PaimonArrowBlockConvertor converter(buffer->ToString(), cctz::utc_time_zone());
    ASSERT_TRUE(converter.init().ok());
    EXPECT_TRUE(schema->Equals(*converter.arrow_schema(), true));
    Block block;
    std::shared_ptr<arrow::RecordBatch> batch;
    EXPECT_FALSE(converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch).ok());
    EXPECT_EQ(nullptr, batch);
}

TEST_F(ArrowBlockConvertorTest, PaimonRejectsInvalidSerializedSchema) {
    for (const std::string& bytes : {std::string(), std::string("invalid schema")}) {
        paimon::PaimonArrowBlockConvertor converter(bytes, cctz::utc_time_zone());
        EXPECT_FALSE(converter.init().ok());
        EXPECT_EQ(nullptr, converter.arrow_schema());
    }
}

TEST_F(ArrowBlockConvertorTest, PythonBuildsSchemaAndConvertsSlicesInternally) {
    auto type = std::make_shared<DataTypeInt32>();
    auto column = ColumnInt32::create();
    column->get_data().assign({11, 22, 33});
    Block block;
    block.insert({std::move(column), type, "value"});
    PythonArrowBlockConvertor converter(block, "UTC", cctz::utc_time_zone());
    std::shared_ptr<arrow::RecordBatch> batch;
    EXPECT_FALSE(converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch).ok());
    ASSERT_TRUE(converter.init().ok());
    EXPECT_EQ("value", converter.arrow_schema()->field(0)->name());
    ASSERT_TRUE(converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch, 1, 3).ok());
    ASSERT_TRUE(batch->ValidateFull().ok());
    ASSERT_EQ(2, batch->num_rows());
    const auto& values = static_cast<const arrow::Int32Array&>(*batch->column(0));
    EXPECT_EQ(22, values.Value(0));
    EXPECT_EQ(33, values.Value(1));
    Block output;
    ASSERT_TRUE(converter.convert_from_arrow(batch, {type}, &output).ok());
    EXPECT_EQ(2, output.rows());
}

TEST_F(ArrowBlockConvertorTest, PaimonSerializedSchemaKeepsTimestampBindingsPerInstance) {
    cctz::time_zone shanghai;
    ASSERT_TRUE(cctz::load_time_zone("Asia/Shanghai", &shanghai));
    auto type = DataTypeFactory::instance().create_data_type(TYPE_DATETIMEV2, false, 0, 6);
    DateV2Value<DateTimeV2ValueType> datetime;
    const std::string format = "%Y-%m-%d %H:%i:%s.%f";
    const std::string value = "1969-12-31 23:59:59.999999";
    ASSERT_TRUE(datetime.from_date_format_str(format.data(), format.size(), value.data(),
                                              value.size()));
    auto column = ColumnDateTimeV2::create();
    column->insert_value(datetime);
    Block block;
    block.insert({std::move(column), type, "ts"});
    std::vector<std::unique_ptr<paimon::PaimonArrowBlockConvertor>> converters;
    for (const std::string& zone : {std::string(), std::string("Asia/Shanghai")}) {
        auto schema = arrow::schema(
                {arrow::field("ts", arrow::timestamp(arrow::TimeUnit::MICRO, zone), false)});
        auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(sink, schema).ValueOrDie();
        ASSERT_TRUE(writer->Close().ok());
        auto converter = std::make_unique<paimon::PaimonArrowBlockConvertor>(
                sink->Finish().ValueOrDie()->ToString(), shanghai);
        ASSERT_TRUE(converter->init().ok());
        converters.emplace_back(std::move(converter));
    }
    // Both converters remain alive while writing, so accidental shared schema state is observable.
    for (size_t i = 0; i < converters.size(); ++i) {
        std::shared_ptr<arrow::RecordBatch> batch;
        ASSERT_TRUE(
                converters[i]->convert_to_arrow(block, arrow::default_memory_pool(), &batch).ok());
        ASSERT_TRUE(batch->ValidateFull().ok());
        const auto& values = static_cast<const arrow::TimestampArray&>(*batch->column(0));
        EXPECT_EQ(i == 0 ? -1LL : -28800000001LL, values.Value(0));
    }
}

TEST_F(ArrowBlockConvertorTest, PythonFixedOffsetSchemaMatchesDeclaredProtocol) {
    auto type = DataTypeFactory::instance().create_data_type(TYPE_DATETIMEV2, false, 0, 6);
    DataTypes types {type, std::make_shared<DataTypeArray>(type)};
    Block block;
    for (size_t i = 0; i < types.size(); ++i) {
        block.insert({types[i]->create_column(), types[i], "arg" + std::to_string(i)});
    }
    for (const std::string& zone :
         {TimezoneUtils::default_time_zone, std::string("+05:45"), std::string("-03:30"),
          std::string("UTC"), std::string("Asia/Shanghai")}) {
        SCOPED_TRACE(zone);
        cctz::time_zone timezone;
        ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone(zone, timezone));
        PythonArrowBlockConvertor converter(block, zone, timezone);
        ASSERT_TRUE(converter.init().ok());
        std::shared_ptr<arrow::Schema> declared;
        ASSERT_TRUE(PythonUDFMeta::convert_types_to_schema(types, zone, &declared).ok());
        EXPECT_TRUE(declared->Equals(*converter.arrow_schema()))
                << "declared=" << declared->ToString()
                << ", actual=" << converter.arrow_schema()->ToString();
    }
}

TEST_F(ArrowBlockConvertorTest, PythonFixedOffsetBatchesPreserveValuesAndNulls) {
    auto type = make_nullable(
            DataTypeFactory::instance().create_data_type(TYPE_DATETIMEV2, false, 0, 6));
    DateV2Value<DateTimeV2ValueType> value;
    value.unchecked_set_time(2023, 4, 20, 0, 0, 0, 123456);
    auto column = type->create_column();
    column->insert(Field::create_field<TYPE_DATETIMEV2>(value));
    column->insert_default();
    Block block;
    block.insert({std::move(column), type, "arg0"});
    for (const auto& [zone, offset_seconds] : std::vector<std::pair<std::string, int64_t>> {
                 {"+08:00", 28800}, {"+05:45", 20700}, {"-03:30", -12600}, {"UTC", 0}}) {
        SCOPED_TRACE(zone);
        cctz::time_zone timezone;
        ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone(zone, timezone));
        PythonArrowBlockConvertor converter(block, zone, timezone);
        ASSERT_TRUE(converter.init().ok());
        std::shared_ptr<arrow::RecordBatch> batch;
        ASSERT_TRUE(converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch).ok());
        ASSERT_TRUE(batch->ValidateFull().ok());
        std::shared_ptr<arrow::Schema> declared;
        ASSERT_TRUE(PythonUDFMeta::convert_types_to_schema({type}, zone, &declared).ok());
        EXPECT_TRUE(declared->Equals(*batch->schema()));
        const auto& values = static_cast<const arrow::TimestampArray&>(*batch->column(0));
        // The protocol label and the epoch must describe the same wall-clock input.
        EXPECT_EQ(1681948800123456LL - offset_seconds * 1000000, values.Value(0));
        EXPECT_TRUE(values.IsNull(1));
        Block output;
        ASSERT_TRUE(converter.convert_from_arrow(batch, {type}, &output).ok());
        const auto& actual = *output.get_by_position(0).column;
        const auto& expected = *block.get_by_position(0).column;
        ASSERT_EQ(expected.size(), actual.size());
        EXPECT_EQ(0, expected.compare_at(0, 0, actual, 1));
        EXPECT_TRUE(actual.is_null_at(1));
    }
}

TEST_F(ArrowBlockConvertorTest, TableWritersPreserveFixedOffsetSchemaNames) {
    auto type = DataTypeFactory::instance().create_data_type(TYPE_TIMESTAMPTZ, false, 0, 6);
    const std::string json =
            R"({"type":"struct","fields":[{"id":1,"name":"ts","required":true,"type":"timestamptz"}]})";
    auto schema = iceberg::SchemaParser::from_json(json);
    for (const std::string& zone : {std::string("+05:45"), std::string("-03:30")}) {
        SCOPED_TRACE(zone);
        cctz::time_zone timezone;
        ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone(zone, timezone));
        ParquetArrowBlockConvertor parquet({type}, {"ts"}, zone, timezone, false);
        hive::HiveArrowBlockConvertor hive({type}, {"ts"}, zone, timezone, false);
        iceberg::IcebergArrowBlockConvertor iceberg(*schema, &json, zone, timezone);
        for (ArrowBlockConvertor* converter :
             {static_cast<ArrowBlockConvertor*>(&parquet), static_cast<ArrowBlockConvertor*>(&hive),
              static_cast<ArrowBlockConvertor*>(&iceberg)}) {
            ASSERT_TRUE(converter->init().ok());
            const auto& timestamp = static_cast<const arrow::TimestampType&>(
                    *converter->arrow_schema()->field(0)->type());
            EXPECT_EQ(zone, timestamp.timezone());
        }
    }
}

TEST_F(ArrowBlockConvertorTest, TableConvertersRejectMismatchedNestedSchemas) {
    auto integer = std::make_shared<DataTypeInt32>();
    DataTypes types {std::make_shared<DataTypeArray>(integer),
                     std::make_shared<DataTypeMap>(make_nullable(integer), make_nullable(integer)),
                     std::make_shared<DataTypeStruct>(DataTypes {integer}, Strings {"value"})};
    for (const auto& type : types) {
        SCOPED_TRACE(type->get_name());
        Block block;
        auto column = type->create_column();
        column->insert_default();
        block.insert({std::move(column), type, "nested"});
        for (const auto& target : {arrow::int32(), arrow::struct_({})}) {
            auto schema = arrow::schema({arrow::field("nested", target)});
            paimon::PaimonArrowBlockConvertor paimon(schema, cctz::utc_time_zone());
            iceberg::IcebergArrowBlockConvertor iceberg(schema, cctz::utc_time_zone());
            for (ArrowBlockConvertor* converter : {static_cast<ArrowBlockConvertor*>(&paimon),
                                                   static_cast<ArrowBlockConvertor*>(&iceberg)}) {
                std::shared_ptr<arrow::RecordBatch> batch;
                const auto status =
                        converter->convert_to_arrow(block, arrow::default_memory_pool(), &batch);
                EXPECT_EQ(ErrorCode::INVALID_ARGUMENT, status.code()) << status;
                EXPECT_EQ(nullptr, batch);
            }
        }
    }
}

TEST_F(ArrowBlockConvertorTest, FlightMapListPreservesNullKeysAcrossBatches) {
    auto integer = make_nullable(std::make_shared<DataTypeInt32>());
    auto type = make_nullable(std::make_shared<DataTypeMap>(integer, integer));
    auto column = type->create_column();
    auto add_map = [&](Array keys, Array values) {
        Map map;
        map.push_back(Field::create_field<TYPE_ARRAY>(keys));
        map.push_back(Field::create_field<TYPE_ARRAY>(values));
        column->insert(Field::create_field<TYPE_MAP>(map));
    };
    add_map({Field::create_field<TYPE_INT>(1)}, {Field::create_field<TYPE_INT>(10)});
    add_map({Field(), Field::create_field<TYPE_INT>(2)},
            {Field::create_field<TYPE_INT>(100), Field()});
    add_map({}, {});
    column->insert_default();
    Block block;
    block.insert({std::move(column), type, "m"});
    auto entries = arrow::struct_(
            {arrow::field("key", arrow::int32()), arrow::field("value", arrow::int32())});
    std::shared_ptr<arrow::Schema> schema;
    ASSERT_TRUE(get_arrow_schema_from_block(block, &schema, "UTC", true, true).ok());
    EXPECT_TRUE(schema->field(0)->type()->Equals(arrow::list(entries)));
    std::shared_ptr<arrow::Schema> native_schema;
    ASSERT_TRUE(get_arrow_schema_from_block(block, &native_schema, "UTC", true).ok());
    ASSERT_EQ(arrow::Type::MAP, native_schema->field(0)->type()->id());
    ArrowFlightArrowBlockConvertor native(native_schema, cctz::utc_time_zone());
    std::shared_ptr<arrow::RecordBatch> native_batch;
    ASSERT_TRUE(
            native.convert_to_arrow(block, arrow::default_memory_pool(), &native_batch, 0, 1).ok());
    EXPECT_FALSE(native.convert_to_arrow(block, arrow::default_memory_pool(), &native_batch).ok());
    ArrowFlightArrowBlockConvertor converter(schema, cctz::utc_time_zone());
    auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
    auto writer = arrow::ipc::MakeStreamWriter(sink, schema).ValueOrDie();
    for (const auto& range : {std::pair<size_t, size_t> {0, 1}, {1, 4}}) {
        std::shared_ptr<arrow::RecordBatch> batch;
        auto status = converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch,
                                                 range.first, range.second);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_TRUE(batch->ValidateFull().ok());
        ASSERT_TRUE(batch->schema()->Equals(*schema));
        ASSERT_TRUE(writer->WriteRecordBatch(*batch).ok());
    }
    ASSERT_TRUE(writer->Close().ok());
    auto source = std::make_shared<arrow::io::BufferReader>(sink->Finish().ValueOrDie());
    auto reader = arrow::ipc::RecordBatchStreamReader::Open(source).ValueOrDie();
    std::shared_ptr<arrow::RecordBatch> batch;
    ASSERT_TRUE(reader->ReadNext(&batch).ok());
    ASSERT_TRUE(reader->ReadNext(&batch).ok());
    const auto& lists = static_cast<const arrow::ListArray&>(*batch->column(0));
    EXPECT_EQ(2, lists.value_length(0));
    EXPECT_EQ(0, lists.value_length(1));
    EXPECT_TRUE(lists.IsNull(2));
    const auto& pairs = static_cast<const arrow::StructArray&>(*lists.values());
    EXPECT_TRUE(pairs.field(0)->IsNull(0));
    EXPECT_EQ(2, static_cast<const arrow::Int32Array&>(*pairs.field(0)).Value(1));
    EXPECT_EQ(100, static_cast<const arrow::Int32Array&>(*pairs.field(1)).Value(0));
    EXPECT_TRUE(pairs.field(1)->IsNull(1));
    // A Flight-specific representation must not relax another consumer's binding contract.
    PythonArrowBlockConvertor python(schema, cctz::utc_time_zone());
    EXPECT_FALSE(python.convert_to_arrow(block, arrow::default_memory_pool(), &batch).ok());
}

TEST_F(ArrowBlockConvertorTest, FlightMapListHandlesNestedMapsAndNullableContainers) {
    auto integer = make_nullable(std::make_shared<DataTypeInt32>());
    auto inner_type = make_nullable(std::make_shared<DataTypeMap>(integer, integer));
    auto outer_type = make_nullable(std::make_shared<DataTypeMap>(integer, inner_type));
    auto array_type = std::make_shared<DataTypeArray>(outer_type);
    auto type = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {array_type}, Strings {"maps"}));
    auto make_map = [](Array keys, Array values) {
        Map map;
        map.push_back(Field::create_field<TYPE_ARRAY>(keys));
        map.push_back(Field::create_field<TYPE_ARRAY>(values));
        return Field::create_field<TYPE_MAP>(map);
    };
    auto inner = make_map({Field()}, {Field::create_field<TYPE_INT>(100)});
    auto outer = make_map({Field(), Field::create_field<TYPE_INT>(2)}, {inner, Field()});
    auto column = type->create_column();
    column->insert(Field::create_field<TYPE_STRUCT>(
            Struct {Field::create_field<TYPE_ARRAY>(Array {outer, Field()})}));
    column->insert_default();
    Block block;
    block.insert({std::move(column), type, "nested"});
    std::shared_ptr<arrow::Schema> schema;
    ASSERT_TRUE(get_arrow_schema_from_block(block, &schema, "UTC", true, true).ok());
    ArrowFlightArrowBlockConvertor converter(schema, cctz::utc_time_zone());
    std::shared_ptr<arrow::RecordBatch> batch;
    auto status = converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_TRUE(batch->ValidateFull().ok());
    const auto& root = static_cast<const arrow::StructArray&>(*batch->column(0));
    EXPECT_TRUE(root.IsNull(1));
    const auto& arrays = static_cast<const arrow::ListArray&>(*root.field(0));
    const auto& maps = static_cast<const arrow::ListArray&>(*arrays.values());
    EXPECT_TRUE(maps.IsNull(1));
    const auto& pairs = static_cast<const arrow::StructArray&>(*maps.values());
    EXPECT_TRUE(pairs.field(0)->IsNull(0));
    const auto& inner_maps = static_cast<const arrow::ListArray&>(*pairs.field(1));
    EXPECT_TRUE(inner_maps.IsNull(1));
    const auto& inner_pairs = static_cast<const arrow::StructArray&>(*inner_maps.values());
    EXPECT_TRUE(inner_pairs.field(0)->IsNull(0));
    EXPECT_EQ(100, static_cast<const arrow::Int32Array&>(*inner_pairs.field(1)).Value(0));
}

TEST_F(ArrowBlockConvertorTest, FlightMapListPreservesConstantDatetimeValues) {
    auto integer = make_nullable(std::make_shared<DataTypeInt32>());
    auto datetime = make_nullable(
            DataTypeFactory::instance().create_data_type(TYPE_DATETIMEV2, false, 0, 6));
    auto type = std::make_shared<DataTypeMap>(integer, datetime);
    DateV2Value<DateTimeV2ValueType> value;
    value.unchecked_set_time(2023, 4, 20, 0, 0, 0, 123456);
    Map map;
    map.push_back(Field::create_field<TYPE_ARRAY>(Array {Field()}));
    map.push_back(
            Field::create_field<TYPE_ARRAY>(Array {Field::create_field<TYPE_DATETIMEV2>(value)}));
    auto column = type->create_column();
    column->insert(Field::create_field<TYPE_MAP>(map));
    Block block;
    block.insert({ColumnConst::create(std::move(column), 3), type, "m"});
    std::shared_ptr<arrow::Schema> schema;
    ASSERT_TRUE(get_arrow_schema_from_block(block, &schema, "Asia/Shanghai", true, true).ok());
    cctz::time_zone timezone;
    ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone("Asia/Shanghai", timezone));
    ArrowFlightArrowBlockConvertor converter(schema, timezone);
    std::shared_ptr<arrow::RecordBatch> batch;
    auto status = converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_TRUE(batch->ValidateFull().ok());
    const auto& lists = static_cast<const arrow::ListArray&>(*batch->column(0));
    const auto& pairs = static_cast<const arrow::StructArray&>(*lists.values());
    const auto& timestamps = static_cast<const arrow::TimestampArray&>(*pairs.field(1));
    EXPECT_TRUE(static_cast<const arrow::TimestampType&>(*timestamps.type()).timezone().empty());
    ASSERT_EQ(3, batch->num_rows());
    for (int i = 0; i < 3; ++i) {
        EXPECT_TRUE(pairs.field(0)->IsNull(i));
        EXPECT_EQ(1681948800123456LL, timestamps.Value(i));
    }
}

} // namespace doris
