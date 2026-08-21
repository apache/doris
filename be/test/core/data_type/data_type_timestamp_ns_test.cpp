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

#include "core/data_type/data_type_timestamp_ns.h"

#include <cctz/time_zone.h>
#include <gtest/gtest.h>

#include <cstring>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type_serde/data_type_serde.h"
#include "core/data_type_serde/data_type_timestamp_ns_serde.h"
#include "core/string_buffer.hpp"
#include "core/value/vdatetime_value.h"
#include "exprs/function/cast/cast_parameters.h"
#include "storage/field_type.h"
#include "util/jsonb_utils.h"
#include "util/jsonb_writer.h"
#include "util/mysql_row_buffer.h"
#include "util/slice.h"
#include "util/timezone_utils.h"

namespace doris {

TEST(DataTypeTimeStampNsTest, TypeFamilyClassifiersKeepTimestampNsIndependent) {
    EXPECT_FALSE(is_date_type(TYPE_TIMESTAMP_NS));
    EXPECT_TRUE(is_timestamp_ns_type(TYPE_TIMESTAMP_NS));
    EXPECT_FALSE(is_timestamp_ns_type(TYPE_DATETIMEV2));

    EXPECT_TRUE(is_date_v2_or_datetime_v2(TYPE_DATEV2));
    EXPECT_TRUE(is_date_v2_or_datetime_v2(TYPE_DATETIMEV2));
    EXPECT_FALSE(is_date_v2_or_datetime_v2(TYPE_TIMESTAMP_NS));

    EXPECT_TRUE(IsDataTypeDateTimeV2<DataTypeDateTimeV2>);
    EXPECT_FALSE(IsDataTypeDateTimeV2<DataTypeTimeStampNs>);
}

TEST(DataTypeTimeStampNsTest, Int64EpochRangeAndOrdering) {
    const TimeStampNsValue epoch(0);
    const TimeStampNsValue before_epoch(-1);
    const TimeStampNsValue minimum(std::numeric_limits<int64_t>::min());
    const TimeStampNsValue maximum(std::numeric_limits<int64_t>::max());

    EXPECT_EQ(epoch.to_string(), "1970-01-01 00:00:00.000000000");
    EXPECT_EQ(before_epoch.to_string(), "1969-12-31 23:59:59.999999999");
    EXPECT_EQ(minimum.to_string(), "1677-09-21 00:12:43.145224192");
    EXPECT_EQ(maximum.to_string(), "2262-04-11 23:47:16.854775807");
    EXPECT_LT(minimum, before_epoch);
    EXPECT_LT(before_epoch, epoch);
    EXPECT_LT(epoch, maximum);
}

TEST(DataTypeTimeStampNsTest, NegativeEpochUsesFloorSecondAndNormalizedFraction) {
    struct TestCase {
        int64_t epoch_nanos;
        int64_t epoch_seconds;
        uint32_t nanosecond;
    };
    const std::vector<TestCase> cases = {
            {-1000000001, -2, 999999999},
            {-1000000000, -1, 0},
            {-999999999, -1, 1},
            {-1, -1, 999999999},
            {0, 0, 0},
            {1, 0, 1},
    };

    for (const auto& test_case : cases) {
        const TimeStampNsValue value(test_case.epoch_nanos);
        EXPECT_EQ(value.epoch_seconds(), test_case.epoch_seconds);
        EXPECT_EQ(value.nanosecond(), test_case.nanosecond);
        EXPECT_EQ(
                static_cast<__int128>(value.epoch_seconds()) * TimeStampNsValue::NANOS_PER_SECOND +
                        value.nanosecond(),
                test_case.epoch_nanos);
    }
}

TEST(DataTypeTimeStampNsTest, ParseAtFixedNanosecondPrecision) {
    int64_t value = 0;

    ASSERT_TRUE(parse_timestamp_ns(StringRef("1970-01-01 00:00:00.12345675"), &value).ok());
    EXPECT_EQ(TimeStampNsValue(value).to_string(), "1970-01-01 00:00:00.123456750");

    ASSERT_TRUE(parse_timestamp_ns(StringRef("1969-12-31 23:59:59.999999999"), &value).ok());
    EXPECT_EQ(value, -1);

    ASSERT_TRUE(parse_timestamp_ns(StringRef("1970-01-01 00:00:00.999999995"), &value).ok());
    EXPECT_EQ(TimeStampNsValue(value).to_string(), "1970-01-01 00:00:00.999999995");

    ASSERT_TRUE(parse_timestamp_ns(StringRef("1970-01-01 00:00:00.9999999995"), &value).ok());
    EXPECT_EQ(TimeStampNsValue(value).to_string(), "1970-01-01 00:00:01.000000000");
}

TEST(DataTypeTimeStampNsTest, ParseTimezoneSuffixInSessionTimezone) {
    TimezoneUtils::load_timezones_to_cache();
    cctz::time_zone shanghai;
    ASSERT_TRUE(cctz::load_time_zone("Asia/Shanghai", &shanghai));

    int64_t value = 0;
    auto status =
            parse_timestamp_ns(StringRef("2023-08-17T01:41:18.123456789Z"), &value, &shanghai);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(TimeStampNsValue(value).to_string(), "2023-08-17 09:41:18.123456789");

    ASSERT_TRUE(parse_timestamp_ns(StringRef("2023-08-17T01:41:18.123456789America/Los_Angeles"),
                                   &value, &shanghai)
                        .ok());
    EXPECT_EQ(TimeStampNsValue(value).to_string(), "2023-08-17 16:41:18.123456789");

    EXPECT_FALSE(
            parse_timestamp_ns(StringRef("1677-09-21T00:12:43.145224192+14:00"), &value, &shanghai)
                    .ok());
    EXPECT_FALSE(
            parse_timestamp_ns(StringRef("2262-04-11T23:47:16.854775807-01:00"), &value, &shanghai)
                    .ok());
}

TEST(DataTypeTimeStampNsTest, ParseAcceptsFractionalWidthsAndRejectsMalformedValues) {
    struct ValidCase {
        const char* input;
        const char* expected;
    };
    const std::vector<ValidCase> valid_cases = {
            {"2024-02-29 12:34:56.1234567", "2024-02-29 12:34:56.123456700"},
            {"2024-02-29 12:34:56.12345678", "2024-02-29 12:34:56.123456780"},
            {"2024-02-29 12:34:56.123456789", "2024-02-29 12:34:56.123456789"},
            {"2024-02-29 12:34:56.1234567894", "2024-02-29 12:34:56.123456789"},
            {"2024-02-29 12:34:56.1234567895", "2024-02-29 12:34:56.123456790"},
            {"2024-02-29 12:34:56", "2024-02-29 12:34:56.000000000"},
    };

    for (const auto& test_case : valid_cases) {
        int64_t value = 0;
        ASSERT_TRUE(parse_timestamp_ns(StringRef(test_case.input), &value).ok()) << test_case.input;
        EXPECT_EQ(TimeStampNsValue(value).to_string(), test_case.expected);
    }

    const std::vector<const char*> invalid_values = {
            "",
            "not-a-date",
            "2023-02-29 00:00:00.000000000",
            "2024-13-01",
            "2024-01-01 24:00:00",
            "2024-01-01 00:00:00.trailing",
            "2024-01-01 00:00:00.123.456",
            "2024-01-01.123 00:00:00",
            "2024.01-01 00:00:00",
    };
    for (const char* input : invalid_values) {
        int64_t value = 0;
        EXPECT_FALSE(parse_timestamp_ns(StringRef(input), &value).ok()) << input;
    }
}

TEST(DataTypeTimeStampNsTest, RejectValuesOutsideEpochRange) {
    int64_t value = 0;
    EXPECT_FALSE(parse_timestamp_ns(StringRef("0000-01-01 00:00:00.000000000"), &value).ok());
    EXPECT_FALSE(parse_timestamp_ns(StringRef("1677-09-21 00:12:43.145224191"), &value).ok());
    EXPECT_FALSE(parse_timestamp_ns(StringRef("2262-04-11 23:47:16.854775808"), &value).ok());
    EXPECT_FALSE(parse_timestamp_ns(StringRef("9999-12-31 23:59:59.999999999"), &value).ok());
}

TEST(DataTypeTimeStampNsTest, CivilRoundTripPreservesSubMicrosecondDigits) {
    DateV2Value<DateTimeV2ValueType> civil;
    civil.unchecked_set_time(2024, 2, 29, 23, 59, 58, 123456);

    TimeStampNsValue value;
    ASSERT_TRUE(value.from_datetime(civil, 789));
    EXPECT_EQ(value.to_string(), "2024-02-29 23:59:58.123456789");
    const auto round_trip = value.to_datetime();
    EXPECT_EQ(round_trip.year(), 2024);
    EXPECT_EQ(round_trip.month(), 2);
    EXPECT_EQ(round_trip.day(), 29);
    EXPECT_EQ(round_trip.hour(), 23);
    EXPECT_EQ(round_trip.minute(), 59);
    EXPECT_EQ(round_trip.second(), 58);
    EXPECT_EQ(value.microsecond(), 123456);
    EXPECT_EQ(value.nanosecond_remainder(), 789);
    EXPECT_EQ(round_trip.to_date_int_val(), civil.to_date_int_val());
}

TEST(DataTypeTimeStampNsTest, CheckedIntervalAdditionRejectsEpochOverflow) {
    const TimeStampNsValue minimum(std::numeric_limits<int64_t>::min());
    const TimeStampNsValue maximum(std::numeric_limits<int64_t>::max());
    const TimeInterval three_seconds(TimeUnit::SECOND, 3, false);
    const TimeInterval negative_three_seconds(TimeUnit::SECOND, 3, true);

    auto below_minimum = minimum;
    EXPECT_FALSE(below_minimum.date_add_interval<TimeUnit::SECOND>(negative_three_seconds));
    EXPECT_EQ(below_minimum, minimum);

    auto above_maximum = maximum;
    EXPECT_FALSE(above_maximum.date_add_interval<TimeUnit::SECOND>(three_seconds));
    EXPECT_EQ(above_maximum, maximum);
}

TEST(DataTypeTimeStampNsTest, FactoryKeepsTimestampNsSeparateFromDateTimeV2) {
    const auto microseconds = create_datetimev2(6);
    const auto timestamp_ns = std::make_shared<DataTypeTimeStampNs>();

    EXPECT_EQ(microseconds->get_primitive_type(), TYPE_DATETIMEV2);
    EXPECT_THROW(create_datetimev2(7), Exception);
    EXPECT_THROW(create_datetimev2(8), Exception);
    EXPECT_THROW(create_datetimev2(9), Exception);
    EXPECT_EQ(timestamp_ns->get_primitive_type(), TYPE_TIMESTAMP_NS);
    EXPECT_EQ(timestamp_ns->get_storage_field_type(), FieldType::OLAP_FIELD_TYPE_TIMESTAMP_NS);
    EXPECT_EQ(timestamp_ns->get_scale(), 9);
    EXPECT_EQ(microseconds->get_family_name(), "DateTimeV2");
    EXPECT_EQ(timestamp_ns->get_family_name(), "TimeStampNs");
}

TEST(DataTypeTimeStampNsTest, SerDeRoundTripsTextProtobufAndBinary) {
    const auto type = std::make_shared<DataTypeTimeStampNs>();
    const auto serde = type->get_serde();
    auto source = type->create_column();
    DataTypeSerDe::FormatOptions options;
    const std::vector<std::string> inputs = {
            "1677-09-21 00:12:43.145224192", "1969-12-31 23:59:59.999999999",
            "1970-01-01 00:00:00.000000000", "2024-02-29 12:34:56.123456789",
            "2262-04-11 23:47:16.854775807",
    };
    for (const auto& input : inputs) {
        StringRef ref(input);
        ASSERT_TRUE(serde->from_string(ref, *source, options).ok()) << input;
    }
    const auto& source_data = assert_cast<const ColumnTimeStampNs&>(*source).get_data();

    PValues protobuf_values;
    ASSERT_TRUE(serde->write_column_to_pb(*source, protobuf_values, 0, source->size()).ok());
    auto protobuf_result = type->create_column();
    ASSERT_TRUE(serde->read_column_from_pb(*protobuf_result, protobuf_values).ok());
    const auto& protobuf_data = assert_cast<const ColumnTimeStampNs&>(*protobuf_result).get_data();
    EXPECT_EQ(protobuf_data, source_data);

    ColumnString::Chars binary;
    std::vector<size_t> offsets = {0};
    for (size_t row = 0; row < source->size(); ++row) {
        serde->write_one_cell_to_binary(*source, binary, row);
        offsets.push_back(binary.size());
    }
    constexpr size_t bytes_per_row = sizeof(uint8_t) + sizeof(uint8_t) + sizeof(int64_t);
    ASSERT_EQ(binary.size(), source->size() * bytes_per_row);
    auto binary_result = ColumnNullable::create(type->create_column(), ColumnUInt8::create());
    for (size_t row = 0; row < source->size(); ++row) {
        const uint8_t* begin = binary.data() + offsets[row];
        const uint8_t* end = DataTypeSerDe::deserialize_binary_to_column(begin, *binary_result);
        EXPECT_EQ(end - begin, bytes_per_row);
    }
    const auto& binary_data =
            assert_cast<const ColumnTimeStampNs&>(binary_result->get_nested_column()).get_data();
    EXPECT_EQ(binary_data, source_data);
}

TEST(DataTypeTimeStampNsTest, DataTypeLiteralField) {
    const DataTypeTimeStampNs type;
    const DataTypeDateTimeV2 legacy6(6);

    EXPECT_TRUE(type.equals(DataTypeTimeStampNs {}));
    EXPECT_FALSE(type.equals(legacy6));
    EXPECT_FALSE(type.equals_ignore_precision(legacy6));
    EXPECT_FALSE(legacy6.equals_ignore_precision(type));

    TExprNode node;
    node.date_literal.value = "2024-02-29 12:34:56.123456789";
    const Field field = type.get_field(node);
    EXPECT_EQ(field.get<TYPE_TIMESTAMP_NS>().to_string(), "2024-02-29 12:34:56.123456789");
    EXPECT_EQ(field.to_debug_string(0), "2024-02-29 12:34:56.123456789");
    EXPECT_EQ(field.to_debug_string(6), "2024-02-29 12:34:56.123456789");
    node.date_literal.value = "not-a-datetime";
    EXPECT_THROW(type.get_field(node), Exception);

    auto column = type.create_column();
    column->insert(field);
    const auto field_with_type = type.get_field_with_data_type(*column, 0);
    EXPECT_EQ(field_with_type.field, field);
    EXPECT_EQ(field_with_type.base_scalar_type_id, TYPE_TIMESTAMP_NS);
    EXPECT_EQ(field_with_type.precision, -1);
    EXPECT_EQ(field_with_type.scale, 9);
}

TEST(DataTypeTimeStampNsTest, FormattingAndHash) {
    int64_t raw = 0;
    ASSERT_TRUE(parse_timestamp_ns(StringRef("2024-02-29 12:34:56.123456789"), &raw).ok());
    TimeStampNsValue value(raw);

    EXPECT_TRUE(value.is_valid_date());

    char text[40] = {};
    const char* end = value.to_string(text);
    EXPECT_STREQ(text, "2024-02-29 12:34:56.123456789");
    EXPECT_EQ(end, text + std::strlen(text) + 1);

    EXPECT_EQ(value.hash(17), value.hash(17));
    EXPECT_EQ(std::hash<TimeStampNsValue> {}(value), std::hash<int64_t> {}(value.epoch_nanos()));
}

TEST(DataTypeTimeStampNsTest, SerDeStrictBatchJsonJsonbMysqlAndBinaryField) {
    const DataTypeTimeStampNs type;
    const auto serde = type.get_serde();
    DataTypeSerDe::FormatOptions options;
    options.field_delim = ";";

    auto strings = ColumnString::create();
    strings->insert_data("1970-01-01 00:00:00.000000001", 29);
    strings->insert_data("ignored-invalid-value", 21);
    strings->insert_data("2024-02-29 12:34:56.123456789", 29);
    NullMap null_map = {0, 1, 0};
    auto strict_result = type.create_column();
    ASSERT_TRUE(
            serde->from_string_strict_mode_batch(*strings, *strict_result, options, null_map.data())
                    .ok());
    const auto& strict_data = assert_cast<const ColumnTimeStampNs&>(*strict_result).get_data();
    EXPECT_EQ(strict_data[0].epoch_nanos(), 1);
    EXPECT_EQ(strict_data[2].to_string(), "2024-02-29 12:34:56.123456789");

    auto invalid_strings = ColumnString::create();
    invalid_strings->insert_data("invalid", 7);
    auto invalid_result = type.create_column();
    EXPECT_FALSE(serde->from_string_strict_mode_batch(*invalid_strings, *invalid_result, options,
                                                      nullptr)
                         .ok());

    auto source = type.create_column();
    for (const std::string input :
         {"1970-01-01 00:00:00.000000001", "2024-02-29 12:34:56.123456789"}) {
        StringRef ref(input);
        ASSERT_TRUE(serde->from_string_strict_mode(ref, *source, options).ok());
    }

    auto serialized = ColumnString::create();
    VectorBufferWriter writer(*serialized);
    ASSERT_TRUE(serde->serialize_column_to_json(*source, 0, source->size(), writer, options).ok());
    writer.commit();
    EXPECT_EQ(serialized->get_data_at(0).to_string(),
              "1970-01-01 00:00:00.000000001;2024-02-29 12:34:56.123456789");

    std::vector<std::string> json_values = {"1970-01-01 00:00:00.000000001",
                                            "2024-02-29 12:34:56.123456789"};
    std::vector<Slice> slices;
    for (auto& json_value : json_values) {
        slices.emplace_back(json_value.data(), json_value.size());
    }
    auto json_result = type.create_column();
    uint64_t num_deserialized = 0;
    ASSERT_TRUE(serde->deserialize_column_from_json_vector(*json_result, slices, &num_deserialized,
                                                           options)
                        .ok());
    EXPECT_EQ(num_deserialized, json_values.size());
    EXPECT_EQ(assert_cast<const ColumnTimeStampNs&>(*json_result).get_data(),
              assert_cast<const ColumnTimeStampNs&>(*source).get_data());

    const auto nested_serde = type.get_serde(2);
    auto nested_json = ColumnString::create();
    VectorBufferWriter nested_writer(*nested_json);
    ASSERT_TRUE(nested_serde->serialize_one_cell_to_json(*source, 1, nested_writer, options).ok());
    nested_writer.commit();
    EXPECT_EQ(nested_json->get_data_at(0).to_string(), "\"2024-02-29 12:34:56.123456789\"");
    auto nested_result = type.create_column();
    std::string quoted = nested_json->get_data_at(0).to_string();
    Slice quoted_slice(quoted.data(), quoted.size());
    ASSERT_TRUE(nested_serde->deserialize_one_cell_from_json(*nested_result, quoted_slice, options)
                        .ok());
    EXPECT_EQ(assert_cast<const ColumnTimeStampNs&>(*nested_result).get_element(0),
              assert_cast<const ColumnTimeStampNs&>(*source).get_element(1));

    auto one_value = source->clone_resized(1);
    auto const_column = ColumnConst::create(std::move(one_value), 2);
    auto const_json = ColumnString::create();
    VectorBufferWriter const_writer(*const_json);
    ASSERT_TRUE(serde->serialize_one_cell_to_json(*const_column, 1, const_writer, options).ok());
    const_writer.commit();
    EXPECT_EQ(const_json->get_data_at(0).to_string(), "1970-01-01 00:00:00.000000001");

    JsonbWriter jsonb_writer;
    ASSERT_TRUE(serde->serialize_column_to_jsonb(*source, 1, jsonb_writer).ok());
    EXPECT_EQ(JsonbToJson::jsonb_to_json_string(jsonb_writer.getOutput()->getBuffer(),
                                                jsonb_writer.getOutput()->getSize()),
              "\"2024-02-29 12:34:56.123456789\"");
    CastParameters cast_params {.status = Status::OK(), .is_strict = true};
    auto jsonb_result = type.create_column();
    ASSERT_TRUE(serde->deserialize_column_from_jsonb(*jsonb_result, jsonb_writer.getValue(),
                                                     cast_params)
                        .ok());
    EXPECT_EQ(assert_cast<const ColumnTimeStampNs&>(*jsonb_result).get_element(0),
              assert_cast<const ColumnTimeStampNs&>(*source).get_element(1));

    auto jsonb_values = ColumnString::create();
    ASSERT_TRUE(serde->serialize_column_to_jsonb_vector(*source, *jsonb_values).ok());
    auto jsonb_vector_result = ColumnNullable::create(type.create_column(), ColumnUInt8::create());
    ASSERT_TRUE(serde->deserialize_column_from_jsonb_vector(*jsonb_vector_result, *jsonb_values,
                                                            cast_params)
                        .ok());
    EXPECT_EQ(assert_cast<const ColumnTimeStampNs&>(jsonb_vector_result->get_nested_column())
                      .get_data(),
              assert_cast<const ColumnTimeStampNs&>(*source).get_data());

    JsonbWriter row_store_writer;
    Arena row_store_arena;
    row_store_writer.writeStartObject();
    serde->write_one_cell_to_jsonb(*source, row_store_writer, row_store_arena, 0, 1, options);
    row_store_writer.writeEndObject();
    const JsonbDocument* row_store_document = nullptr;
    ASSERT_TRUE(JsonbDocument::checkAndCreateDocument(row_store_writer.getOutput()->getBuffer(),
                                                      row_store_writer.getOutput()->getSize(),
                                                      &row_store_document)
                        .ok());
    auto row_store_result = type.create_column();
    serde->read_one_cell_from_jsonb(*row_store_result, (*row_store_document)->begin()->value());
    EXPECT_EQ(assert_cast<const ColumnTimeStampNs&>(*row_store_result).get_element(0),
              assert_cast<const ColumnTimeStampNs&>(*source).get_element(1));

    MysqlRowBinaryBuffer mysql_buffer;
    ASSERT_TRUE(serde->write_column_to_mysql_binary(*source, mysql_buffer, 1, false, options).ok());
    ASSERT_EQ(static_cast<uint8_t>(mysql_buffer.buf()[0]), 29);
    EXPECT_EQ(std::string(mysql_buffer.buf() + 1, 29), "2024-02-29 12:34:56.123456789");

    ColumnString::Chars binary;
    serde->write_one_cell_to_binary(*source, binary, 1);
    Field binary_field;
    FieldInfo info;
    const uint8_t* end =
            DataTypeSerDe::deserialize_binary_to_field(binary.data(), binary_field, info);
    EXPECT_EQ(end, binary.data() + binary.size());
    EXPECT_EQ(info.scalar_type_id, TYPE_TIMESTAMP_NS);
    EXPECT_EQ(info.scale, 9);
    EXPECT_EQ(binary_field.get<TYPE_TIMESTAMP_NS>().to_string(), "2024-02-29 12:34:56.123456789");
}

TEST(DataTypeTimeStampNsTest, SerDeBatchRejectsMultipleFractionalSeparators) {
    const DataTypeTimeStampNs type;
    const auto serde = type.get_serde();
    DataTypeSerDe::FormatOptions options;

    auto strings = ColumnString::create();
    strings->insert_data("2024-01-01 00:00:00.123456789", 29);
    strings->insert_data("2024-01-01 00:00:00.123.456", 27);

    auto permissive_result = ColumnNullable::create(type.create_column(), ColumnUInt8::create());
    ASSERT_TRUE(serde->from_string_batch(*strings, *permissive_result, options).ok());
    const auto& permissive_null_map = permissive_result->get_null_map_data();
    ASSERT_EQ(permissive_null_map.size(), 2);
    EXPECT_EQ(permissive_null_map[0], 0);
    EXPECT_EQ(permissive_null_map[1], 1);

    auto invalid_string = ColumnString::create();
    invalid_string->insert_data("2024-01-01 00:00:00.123.456", 27);
    auto strict_result = type.create_column();
    EXPECT_FALSE(
            serde->from_string_strict_mode_batch(*invalid_string, *strict_result, options, nullptr)
                    .ok());
}

} // namespace doris
