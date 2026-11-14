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

#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_struct.h"
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

TEST_F(ArrowBlockConvertorTest, FlightRejectsInvalidUtf8BeforeReturningBatch) {
    auto type = DataTypeFactory::instance().create_data_type(TYPE_STRING, false);
    for (const std::string& value :
         {std::string("\x84"), std::string("\xc0\xaf"), std::string("\xed\xa0\x80"),
          std::string("\xf4\x90\x80\x80")}) {
        auto column = type->create_column();
        column->insert(Field::create_field<TYPE_STRING>(value));
        Block block {{std::move(column), type, "payload"}};
        ArrowFlightArrowBlockConvertor flight(block, "UTC", cctz::utc_time_zone());
        ASSERT_TRUE(flight.init().ok());
        const ArrowBlockConvertor& converter = flight;
        std::shared_ptr<arrow::RecordBatch> batch;
        const auto status = converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch);
        EXPECT_EQ(ErrorCode::INVALID_ARGUMENT, status.code()) << status;
        EXPECT_NE(std::string::npos, status.to_string().find("payload"));
        EXPECT_NE(std::string::npos, status.to_string().find("UTF8"));
        EXPECT_EQ(nullptr, batch);

        DorisArrowBlockConvertor ordinary(block, "UTC", cctz::utc_time_zone());
        ASSERT_TRUE(ordinary.init().ok());
        EXPECT_TRUE(ordinary.convert_to_arrow(block, arrow::default_memory_pool(), &batch).ok());
    }
}

TEST_F(ArrowBlockConvertorTest, FlightChecksEachStringAndEveryBatch) {
    auto type = DataTypeFactory::instance().create_data_type(TYPE_STRING, false);
    auto column = type->create_column();
    for (const std::string& value :
         {std::string("valid"), std::string("\xc2"), std::string("\xa2")}) {
        column->insert(Field::create_field<TYPE_STRING>(value));
    }
    Block block {{std::move(column), type, "payload"}};
    ArrowFlightArrowBlockConvertor converter(block, "UTC", cctz::utc_time_zone());
    ASSERT_TRUE(converter.init().ok());
    std::shared_ptr<arrow::RecordBatch> batch;
    ASSERT_TRUE(converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch, 0, 1).ok());
    ASSERT_EQ(1, batch->num_rows());
    batch.reset();
    // Adjacent invalid strings form valid UTF-8 when concatenated; row boundaries matter.
    EXPECT_FALSE(
            converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch, 1, 3).ok());
    EXPECT_EQ(nullptr, batch);
}

TEST_F(ArrowBlockConvertorTest, FlightPreservesValidTextNullsAndBinary) {
    auto text = make_nullable(DataTypeFactory::instance().create_data_type(TYPE_STRING, false));
    auto strings = text->create_column();
    strings->insert(Field::create_field<TYPE_STRING>(std::string("\xe4\xb8\xad\xf0\x9f\x98\x80")));
    strings->insert(Field::create_field<TYPE_STRING>(std::string()));
    strings->insert(Field::create_field<TYPE_STRING>(std::string("a\0b", 3)));
    strings->insert_default();
    auto binary = DataTypeFactory::instance().create_data_type(TYPE_VARBINARY, false);
    auto bytes = binary->create_column();
    for (int i = 0; i < 4; ++i) {
        bytes->insert_data("\x84\0\xff", 3);
    }
    Block block {{std::move(strings), text, "text"}, {std::move(bytes), binary, "binary"}};
    ArrowFlightArrowBlockConvertor converter(block, "UTC", cctz::utc_time_zone());
    ASSERT_TRUE(converter.init().ok());
    std::shared_ptr<arrow::RecordBatch> batch;
    ASSERT_TRUE(converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch).ok());
    ASSERT_TRUE(batch->ValidateFull().ok());
    const auto& values = static_cast<const arrow::StringArray&>(*batch->column(0));
    EXPECT_EQ("\xe4\xb8\xad\xf0\x9f\x98\x80", values.GetString(0));
    EXPECT_EQ("", values.GetString(1));
    EXPECT_EQ(std::string("a\0b", 3), values.GetString(2));
    EXPECT_TRUE(values.IsNull(3));
    ASSERT_EQ(arrow::Type::BINARY, batch->column(1)->type_id());
    const auto& binary_values = static_cast<const arrow::BinaryArray&>(*batch->column(1));
    EXPECT_EQ(std::string("\x84\0\xff", 3), binary_values.GetString(0));
}

TEST_F(ArrowBlockConvertorTest, FlightRejectsNestedInvalidUtf8) {
    auto text = make_nullable(DataTypeFactory::instance().create_data_type(TYPE_STRING, false));
    const auto invalid = Field::create_field<TYPE_STRING>(std::string("\x84"));
    const auto valid = Field::create_field<TYPE_STRING>(std::string("key"));
    DataTypes types {std::make_shared<DataTypeArray>(text),
                     std::make_shared<DataTypeStruct>(DataTypes {text}, Strings {"child"}),
                     std::make_shared<DataTypeMap>(text, text),
                     std::make_shared<DataTypeMap>(text, text)};
    FieldVector fields {
            Field::create_field<TYPE_ARRAY>(Array {invalid}),
            Field::create_field<TYPE_STRUCT>(Struct {invalid}),
            Field::create_field<TYPE_MAP>(Map {Field::create_field<TYPE_ARRAY>(Array {valid}),
                                               Field::create_field<TYPE_ARRAY>(Array {invalid})}),
            Field::create_field<TYPE_MAP>(Map {Field::create_field<TYPE_ARRAY>(Array {invalid}),
                                               Field::create_field<TYPE_ARRAY>(Array {valid})})};
    for (size_t i = 0; i < types.size(); ++i) {
        SCOPED_TRACE(types[i]->get_name());
        auto column = types[i]->create_column();
        column->insert(fields[i]);
        Block block {{std::move(column), types[i], "nested"}};
        ArrowFlightArrowBlockConvertor converter(block, "UTC", cctz::utc_time_zone());
        ASSERT_TRUE(converter.init().ok());
        std::shared_ptr<arrow::RecordBatch> batch;
        const auto status = converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch);
        EXPECT_EQ(ErrorCode::INVALID_ARGUMENT, status.code()) << status;
        EXPECT_NE(std::string::npos, status.to_string().find("nested"));
        EXPECT_EQ(nullptr, batch);
    }
}

TEST_F(ArrowBlockConvertorTest, FlightRejectsInvalidLargeString) {
    auto type = DataTypeFactory::instance().create_data_type(TYPE_STRING, false);
    auto column = type->create_column();
    column->insert(Field::create_field<TYPE_STRING>(std::string("\x84")));
    Block block {{std::move(column), type, "large_text"}};
    auto schema = arrow::schema({arrow::field("large_text", arrow::large_utf8(), false)});
    ArrowFlightArrowBlockConvertor converter(schema, cctz::utc_time_zone());
    std::shared_ptr<arrow::RecordBatch> batch;
    const auto status = converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch);
    EXPECT_EQ(ErrorCode::INVALID_ARGUMENT, status.code()) << status;
    EXPECT_NE(std::string::npos, status.to_string().find("large_text"));
    EXPECT_EQ(nullptr, batch);
}

TEST_F(ArrowBlockConvertorTest, FlightIgnoresBytesMaskedByNull) {
    auto text = DataTypeFactory::instance().create_data_type(TYPE_STRING, false);
    auto values = text->create_column();
    values->insert_data("\x84", 1);
    auto nulls = ColumnUInt8::create();
    nulls->insert_value(1);
    Block block {{ColumnNullable::create(std::move(values), std::move(nulls)), make_nullable(text),
                  "nullable_text"}};
    ArrowFlightArrowBlockConvertor converter(block, "UTC", cctz::utc_time_zone());
    ASSERT_TRUE(converter.init().ok());
    std::shared_ptr<arrow::RecordBatch> batch;
    ASSERT_TRUE(converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch).ok());
    ASSERT_TRUE(batch->ValidateFull().ok());
    EXPECT_TRUE(batch->column(0)->IsNull(0));
}

} // namespace doris
