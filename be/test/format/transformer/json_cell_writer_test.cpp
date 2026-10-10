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

#include "format/transformer/json_cell_writer.h"

#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include <array>
#include <chrono>
#include <limits>

#include "core/column/column_const.h"
#include "core/column/column_decimal.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_date.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_date_time.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_file.h"
#include "core/data_type/data_type_ipv4.h"
#include "core/data_type/data_type_ipv6.h"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_time.h"
#include "core/data_type/data_type_timestamp_ns.h"
#include "core/data_type/data_type_timestamptz.h"
#include "core/data_type/data_type_varbinary.h"
#include "core/string_buffer.hpp"
#include "util/jsonb_utils.h"

namespace doris::json_format {
namespace {

std::string render(const DataTypePtr& type, const IColumn& column, size_t row = 0) {
    const auto schema_status = validate_json_output_type(type);
    EXPECT_TRUE(schema_status.ok()) << schema_status;
    if (!schema_status.ok()) return {};
    const auto validation = validate_file_column(column, type);
    EXPECT_TRUE(validation.ok()) << validation;
    if (!validation.ok()) return {};
    auto output = ColumnString::create();
    BufferWritable buffer(*output);
    DataTypeSerDe::FormatOptions options;
    JsonCellWriterState state;
    const auto status = write_json_cell(column, type, row, buffer, options, state);
    EXPECT_TRUE(status.ok()) << status;
    if (!status.ok()) return {}; // Discard the uncommitted prefix on failure.
    buffer.commit();
    return output->get_data_at(0).to_string();
}

Field file_value(const std::string* bytes) {
    File fields(6);
    fields[0] = Field::create_field<TYPE_STRING>("s3://bucket/a");
    if (bytes != nullptr) {
        fields[2] = Field::create_field<TYPE_BIGINT>(static_cast<Int64>(bytes->size()));
        fields[5] = Field::create_field<TYPE_VARBINARY>(
                StringView(bytes->data(), static_cast<uint32_t>(bytes->size())));
    }
    return Field::create_field<TYPE_FILE>(std::move(fields));
}

std::string typed_jsonb_text(const DataTypePtr& type, const IColumn& column, size_t row) {
    JsonbWriter writer;
    const auto status = type->get_serde()->serialize_column_to_jsonb(column, row, writer);
    EXPECT_TRUE(status.ok()) << status;
    if (!status.ok()) return {};
    return JsonbToJson {}.to_json_string(writer.getValue());
}

TEST(JsonCellWriterTest, BooleansNumbersAndNullsHaveJsonTokens) {
    auto boolean = std::make_shared<DataTypeBool>();
    auto values = boolean->create_column();
    values->insert(Field::create_field<TYPE_BOOLEAN>(true));
    values->insert(Field::create_field<TYPE_BOOLEAN>(false));
    EXPECT_EQ(render(boolean, *values, 0), "true");
    EXPECT_EQ(render(boolean, *values, 1), "false");
    auto integer = std::make_shared<DataTypeInt64>();
    auto integers = integer->create_column();
    integers->insert(Field::create_field<TYPE_BIGINT>(-17));
    EXPECT_EQ(render(integer, *integers), "-17");
    auto nullable = make_nullable(integer);
    auto nulls = nullable->create_column();
    nulls->insert_default();
    EXPECT_EQ(render(nullable, *nulls), "null");
}

template <PrimitiveType T>
void expect_nonfinite_numeric_text() {
    using Value = typename PrimitiveTypeTraits<T>::CppType;
    auto type = std::make_shared<DataTypeNumber<T>>();
    auto values = type->create_column();
    const std::array<Value, 3> numbers {std::numeric_limits<Value>::quiet_NaN(),
                                        std::numeric_limits<Value>::infinity(),
                                        -std::numeric_limits<Value>::infinity()};
    const std::array<const char*, 3> expected {"nan", "inf", "-inf"};
    Array elements;
    for (size_t row = 0; row != numbers.size(); ++row) {
        auto field = Field::create_field<T>(numbers[row]);
        values->insert(field);
        elements.push_back(std::move(field));
        EXPECT_EQ(render(type, *values, row), expected[row]);
    }

    auto array_type = std::make_shared<DataTypeArray>(type);
    auto arrays = array_type->create_column();
    arrays->insert(Field::create_field<TYPE_ARRAY>(std::move(elements)));
    EXPECT_EQ(render(array_type, *arrays), "[nan,inf,-inf]");
}

TEST(JsonCellWriterTest, FloatNonfiniteMatchesTypedJsonbText) {
    expect_nonfinite_numeric_text<TYPE_FLOAT>();
}

TEST(JsonCellWriterTest, DoubleNonfiniteMatchesTypedJsonbText) {
    expect_nonfinite_numeric_text<TYPE_DOUBLE>();
}

TEST(JsonCellWriterTest, StringPreservesNulQuotesAndUnicode) {
    auto type = std::make_shared<DataTypeString>();
    auto values = type->create_column();
    const std::string input("\"a\0b\"\n\xE2\x80\xA8", 9);
    values->insert_data(input.data(), input.size());
    EXPECT_EQ(render(type, *values), R"("\"a\u0000b\"\n\u2028")");
}

TEST(JsonCellWriterTest, FileIncludesNullEmptyAndNonemptyInline) {
    auto type = std::make_shared<DataTypeFile>();
    auto values = type->create_column();
    const std::string empty;
    const std::string bytes(3073, '\0');
    values->insert(file_value(nullptr));
    values->insert(file_value(&empty));
    values->insert(file_value(&bytes));
    EXPECT_EQ(
            render(type, *values, 0),
            R"({"uri":"s3://bucket/a","offset":null,"size":null,"content_type":null,"checksum":null,"inline":null})");
    EXPECT_EQ(
            render(type, *values, 1),
            R"({"uri":"s3://bucket/a","offset":null,"size":0,"content_type":null,"checksum":null,"inline":""})");
    EXPECT_EQ(
            render(type, *values, 2),
            R"({"uri":"s3://bucket/a","offset":null,"size":3073,"content_type":null,"checksum":null,"inline":")" + std::string(4096, 'A') + "AA==\"}");
}

TEST(JsonCellWriterTest, StructLongEscapedNameAndHiddenNullFile) {
    const std::string name = std::string(300, 'x') + std::string("\0\"\n", 3);
    auto type = std::make_shared<DataTypeStruct>(
            DataTypes {std::make_shared<DataTypeFile>(), std::make_shared<DataTypeString>()},
            Strings {"f", name});
    auto values = type->create_column();
    values->insert(Field::create_field<TYPE_STRUCT>(
            Struct {file_value(nullptr), Field::create_field<TYPE_STRING>("\"text\"")}));
    EXPECT_EQ(
            render(type, *values),
            R"({"f":{"uri":"s3://bucket/a","offset":null,"size":null,"content_type":null,"checksum":null,"inline":null},")" +
                    std::string(300, 'x') + R"(\u0000\"\n":"\"text\""})");

    const auto text = render(type, *values);
    rapidjson::Document parsed;
    parsed.Parse(text.data(), text.size());
    ASSERT_FALSE(parsed.HasParseError());
    ASSERT_EQ(parsed.MemberCount(), 2);
    const auto& last = *(parsed.MemberBegin() + 1);
    EXPECT_EQ(std::string(last.name.GetString(), last.name.GetStringLength()), name);
    EXPECT_EQ(std::string(last.value.GetString(), last.value.GetStringLength()), "\"text\"");

    auto nullable = make_nullable(type);
    auto nulls = nullable->create_column();
    nulls->insert_default();
    auto constant = ColumnConst::create(std::move(nulls), 7);
    EXPECT_EQ(render(nullable, *constant, 6), "null");
}

TEST(JsonCellWriterTest, ArrayOffsetsAndNullableNestedValues) {
    auto type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeFile>());
    auto values = type->create_column();
    values->insert(Field::create_field<TYPE_ARRAY>(Array {}));
    values->insert(Field::create_field<TYPE_ARRAY>(Array {Field(), file_value(nullptr)}));
    EXPECT_EQ(render(type, *values, 0), "[]");
    EXPECT_EQ(
            render(type, *values, 1),
            R"([null,{"uri":"s3://bucket/a","offset":null,"size":null,"content_type":null,"checksum":null,"inline":null}])");
}

TEST(JsonCellWriterTest, MapFileValuesMatchTypedJsonb) {
    const auto file = std::make_shared<DataTypeFile>();
    const auto structure = std::make_shared<DataTypeStruct>(
            DataTypes {std::make_shared<DataTypeArray>(file)}, Strings {"files"});
    const DataTypes value_types {file, structure};
    const std::string bytes("a\0b", 3);
    const auto file_field = file_value(&bytes);
    const std::array<Field, 2> value_fields {
            file_field, Field::create_field<TYPE_STRUCT>(
                                Struct {Field::create_field<TYPE_ARRAY>(Array {file_field})})};
    for (size_t index = 0; index != value_types.size(); ++index) {
        const auto type =
                std::make_shared<DataTypeMap>(make_nullable(std::make_shared<DataTypeString>()),
                                              make_nullable(value_types[index]));
        SCOPED_TRACE(type->get_name());
        auto values = type->create_column();
        values->insert_default();
        values->insert(Field::create_field<TYPE_MAP>(
                Map {Field::create_field<TYPE_ARRAY>(Array {Field::create_field<TYPE_STRING>("k")}),
                     Field::create_field<TYPE_ARRAY>(Array {value_fields[index]})}));
        // Leave key/value null handling to the existing typed serializer.
        values->insert(Field::create_field<TYPE_MAP>(
                Map {Field::create_field<TYPE_ARRAY>(Array {Field()}),
                     Field::create_field<TYPE_ARRAY>(Array {Field()})}));
        for (size_t row = 0; row != values->size(); ++row) {
            EXPECT_EQ(render(type, *values, row), typed_jsonb_text(type, *values, row));
        }
        const auto text = render(type, *values, 1);
        rapidjson::Document parsed;
        parsed.Parse(text.data(), text.size());
        ASSERT_FALSE(parsed.HasParseError());
        ASSERT_TRUE(parsed.HasMember("k"));
        const auto& public_file = index == 0 ? parsed["k"] : parsed["k"]["files"][0];
        ASSERT_TRUE(public_file.IsObject());
        EXPECT_EQ(public_file.MemberCount(), 6);
        EXPECT_TRUE(public_file.HasMember("uri"));
        EXPECT_TRUE(public_file.HasMember("offset"));
        EXPECT_EQ(public_file["size"].GetInt64(), bytes.size());
        EXPECT_TRUE(public_file.HasMember("content_type"));
        EXPECT_TRUE(public_file.HasMember("checksum"));
        ASSERT_TRUE(public_file.HasMember("inline"));
        EXPECT_EQ(std::string(public_file["inline"].GetString()), "YQBi");
    }
}

TEST(JsonCellWriterTest, MapKeyErrorsMatchTypedJsonb) {
    const DataTypes key_types {std::make_shared<DataTypeString>(),
                               std::make_shared<DataTypeInt64>()};
    const std::array<Field, 2> keys {Field::create_field<TYPE_STRING>(std::string(300, 'k')),
                                     Field::create_field<TYPE_BIGINT>(1)};
    for (size_t index = 0; index != key_types.size(); ++index) {
        const auto type = std::make_shared<DataTypeMap>(
                make_nullable(key_types[index]), make_nullable(std::make_shared<DataTypeFile>()));
        auto values = type->create_column();
        values->insert(Field::create_field<TYPE_MAP>(
                Map {Field::create_field<TYPE_ARRAY>(Array {keys[index]}),
                     Field::create_field<TYPE_ARRAY>(Array {file_value(nullptr)})}));
        JsonbWriter writer;
        const auto expected = type->get_serde()->serialize_column_to_jsonb(*values, 0, writer);
        ASSERT_FALSE(expected.ok());
        auto output = ColumnString::create();
        BufferWritable buffer(*output);
        DataTypeSerDe::FormatOptions options;
        JsonCellWriterState state;
        const auto actual = write_json_cell(*values, type, 0, buffer, options, state);
        EXPECT_EQ(actual.code(), expected.code());
        EXPECT_EQ(actual.msg(), expected.msg());
        EXPECT_EQ(output->size(), 0);
    }
}

TEST(JsonCellWriterTest, MapPreservesNulKeyAndStringValue) {
    const auto string = make_nullable(std::make_shared<DataTypeString>());
    const auto type = std::make_shared<DataTypeMap>(string, string);
    auto values = type->create_column();
    values->insert(Field::create_field<TYPE_MAP>(
            Map {Field::create_field<TYPE_ARRAY>(
                         Array {Field::create_field<TYPE_STRING>(std::string("a\0b", 3))}),
                 Field::create_field<TYPE_ARRAY>(
                         Array {Field::create_field<TYPE_STRING>(std::string("c\0d", 3))})}));
    EXPECT_EQ(render(type, *values), R"({"a\u0000b":"c\u0000d"})");
}

TEST(JsonCellWriterTest, DecimalScaleUsesTypedJsonbFormatting) {
    auto type = std::make_shared<DataTypeDecimal128>(18, 4);
    auto values = ColumnDecimal128V3::create(0, 4);
    values->insert_value(Decimal128V3 {123400});
    EXPECT_EQ(render(type, *values), "12.3400");
}

TEST(JsonCellWriterTest, ExistingJsonbPreservesNestedNulStringAndKey) {
    JsonbWriter input;
    ASSERT_TRUE(input.writeStartObject());
    ASSERT_TRUE(input.writeKey("a\0b", 3));
    ASSERT_TRUE(input.writeStartArray());
    ASSERT_TRUE(input.writeStartString());
    ASSERT_TRUE(input.writeString("c\0d", 3));
    ASSERT_TRUE(input.writeEndString());
    ASSERT_TRUE(input.writeEndArray());
    ASSERT_TRUE(input.writeEndObject());
    auto type = std::make_shared<DataTypeJsonb>();
    auto values = type->create_column();
    values->insert_data(input.getOutput()->getBuffer(), input.getOutput()->getSize());
    EXPECT_EQ(render(type, *values), R"({"a\u0000b":["c\u0000d"]})");
}

TEST(JsonCellWriterTest, ExistingJsonbPreservesNestedNonfiniteNumericText) {
    JsonbWriter input;
    ASSERT_TRUE(input.writeStartObject());
    ASSERT_TRUE(input.writeKey("values", 6));
    ASSERT_TRUE(input.writeStartArray());
    ASSERT_TRUE(input.writeFloat(std::numeric_limits<float>::quiet_NaN()));
    ASSERT_TRUE(input.writeFloat(std::numeric_limits<float>::infinity()));
    ASSERT_TRUE(input.writeFloat(-std::numeric_limits<float>::infinity()));
    ASSERT_TRUE(input.writeDouble(std::numeric_limits<double>::quiet_NaN()));
    ASSERT_TRUE(input.writeDouble(std::numeric_limits<double>::infinity()));
    ASSERT_TRUE(input.writeDouble(-std::numeric_limits<double>::infinity()));
    ASSERT_TRUE(input.writeEndArray());
    ASSERT_TRUE(input.writeEndObject());
    auto type = std::make_shared<DataTypeJsonb>();
    auto values = type->create_column();
    values->insert_data(input.getOutput()->getBuffer(), input.getOutput()->getSize());
    EXPECT_EQ(render(type, *values), R"({"values":[nan,inf,-inf,nan,inf,-inf]})");
}

template <PrimitiveType T>
void expect_finite_numeric_text() {
    using Value = typename PrimitiveTypeTraits<T>::CppType;
    auto type = std::make_shared<DataTypeNumber<T>>();
    auto values = type->create_column();
    const std::array<Value, 7> numbers {Value(0),
                                        Value(-0.0),
                                        Value(1.1),
                                        std::numeric_limits<Value>::denorm_min(),
                                        std::numeric_limits<Value>::min(),
                                        std::numeric_limits<Value>::max(),
                                        std::numeric_limits<Value>::lowest()};
    for (size_t row = 0; row != numbers.size(); ++row) {
        values->insert(Field::create_field<T>(numbers[row]));
        EXPECT_EQ(render(type, *values, row), typed_jsonb_text(type, *values, row));
    }
    EXPECT_EQ(render(type, *values, 1), "-0");
}

TEST(JsonCellWriterTest, FiniteFloatMatchesTypedJsonbIncludingSignedZero) {
    expect_finite_numeric_text<TYPE_FLOAT>();
}

TEST(JsonCellWriterTest, FiniteDoubleMatchesTypedJsonbIncludingSignedZero) {
    expect_finite_numeric_text<TYPE_DOUBLE>();
}

TEST(JsonCellWriterTest, LargeIntKeepsExactDigitsAtBothLimits) {
    auto type = std::make_shared<DataTypeInt128>();
    auto values = type->create_column();
    values->insert(Field::create_field<TYPE_LARGEINT>(std::numeric_limits<Int128>::lowest()));
    values->insert(Field::create_field<TYPE_LARGEINT>(std::numeric_limits<Int128>::max()));
    values->insert(Field::create_field<TYPE_LARGEINT>((Int128(1) << 53) + 1));
    EXPECT_EQ(render(type, *values, 0), "-170141183460469231731687303715884105728");
    EXPECT_EQ(render(type, *values, 1), "170141183460469231731687303715884105727");
    EXPECT_EQ(render(type, *values, 2), "9007199254740993");
}

template <PrimitiveType T>
void expect_decimal_width() {
    using DecimalType = DataTypeDecimal<T>;
    using Value = typename DecimalType::ColumnType::value_type;
    using Native = typename Value::NativeType;
    const auto precision = max_decimal_precision<T>();
    auto type = std::make_shared<DecimalType>(precision, 4);
    auto values = DecimalType::ColumnType::create(0, 4);
    Native magnitude = 1;
    for (UInt32 digit = 0; digit != precision - 1; ++digit) magnitude *= 10;
    magnitude += 1; // A low digit that a double conversion would lose for wide decimals.
    values->insert_value(Value {magnitude});
    values->insert_value(Value {-magnitude});
    values->insert_value(Value {0});
    const auto expected = "1" + std::string(precision - 5, '0') + ".0001";
    EXPECT_EQ(render(type, *values, 0), expected);
    EXPECT_EQ(render(type, *values, 1), "-" + expected);
    EXPECT_EQ(render(type, *values, 2), "0.0000");
}

TEST(JsonCellWriterTest, EveryDecimalV3WidthKeepsScaleAndLowDigits) {
    expect_decimal_width<TYPE_DECIMAL32>();
    expect_decimal_width<TYPE_DECIMAL64>();
    expect_decimal_width<TYPE_DECIMAL128I>();
    expect_decimal_width<TYPE_DECIMAL256>();
}

TEST(JsonCellWriterTest, DecimalV2UsesOriginalCastScale) {
    auto type = std::make_shared<DataTypeDecimalV2>(27, 9, 18, 4);
    auto values = ColumnDecimal128V2::create(0, 9);
    values->insert_value(DecimalV2Value(Int128(12340000000LL)));
    values->insert_value(DecimalV2Value(Int128(-12340000000LL)));
    EXPECT_EQ(render(type, *values, 0), "12.3400");
    EXPECT_EQ(render(type, *values, 1), "-12.3400");
}

TEST(JsonCellWriterTest, SharedStateKeepsDistinctDecimalScales) {
    auto type = std::make_shared<DataTypeStruct>(
            DataTypes {std::make_shared<DataTypeDecimal128>(18, 2),
                       std::make_shared<DataTypeDecimal128>(18, 4)},
            Strings {"a", "b"});
    auto values = type->create_column();
    const auto number = Field::create_field<TYPE_DECIMAL128I>(Decimal128V3 {1234});
    values->insert(Field::create_field<TYPE_STRUCT>(Struct {number, number}));
    EXPECT_EQ(render(type, *values), R"({"a":12.34,"b":0.1234})");
}

TEST(JsonCellWriterTest, FileIncludesBase64InlineWithoutChangingSourceBytes) {
    std::string all_bytes;
    for (unsigned value = 0; value != 256; ++value) all_bytes.push_back(static_cast<char>(value));
    const std::array<std::string, 4> inputs {std::string("\xff", 1), std::string("\xfb\xff", 2),
                                             std::string("\0\xfb\xff", 3), all_bytes};
    auto type = std::make_shared<DataTypeFile>();
    auto values = type->create_column();
    for (size_t row = 0; row != inputs.size(); ++row) {
        values->insert(file_value(&inputs[row]));
        const auto text = render(type, *values, row);
        rapidjson::Document parsed;
        parsed.Parse(text.data(), text.size());
        ASSERT_FALSE(parsed.HasParseError());
        ASSERT_EQ(parsed.MemberCount(), 6);
        ASSERT_TRUE(parsed.HasMember("inline"));
        auto restored = type->create_column();
        Slice input(text);
        DataTypeSerDe::FormatOptions options;
        ASSERT_TRUE(type->get_serde()->deserialize_one_cell_from_json(*restored, input, options).ok());
        EXPECT_EQ((*restored)[0].get<TYPE_FILE>()[5].get<TYPE_VARBINARY>().str(), inputs[row]);
        EXPECT_EQ(parsed["size"].GetInt64(), inputs[row].size());
        const auto source = (*values)[row];
        const auto& bytes = source.get<TYPE_FILE>()[5].get<TYPE_VARBINARY>();
        EXPECT_EQ(std::string(bytes.data(), bytes.size()), inputs[row]);
    }
}

TEST(JsonCellWriterTest, NonNullConstantFileKeepsPublicMetadata) {
    auto type = make_nullable(std::make_shared<DataTypeFile>());
    auto values = type->create_column();
    const std::string bytes("a\0b", 3);
    File fields = file_value(&bytes).get<TYPE_FILE>();
    fields[1] = Field::create_field<TYPE_BIGINT>(7);
    fields[3] = Field::create_field<TYPE_STRING>("application/octet-stream");
    fields[4] = Field::create_field<TYPE_STRING>("MD5:00000000000000000000000000000000");
    values->insert(Field::create_field<TYPE_FILE>(std::move(fields)));
    auto constant = ColumnConst::create(std::move(values), 7);
    EXPECT_EQ(
            render(type, *constant, 6),
            R"({"uri":"s3://bucket/a","offset":7,"size":3,"content_type":"application/octet-stream","checksum":"MD5:00000000000000000000000000000000","inline":"YQBi"})");
}

TEST(JsonCellWriterTest, NullArrayAndMapParentsHideInvalidFilePayloads) {
    const auto file = std::make_shared<DataTypeFile>();
    const auto invalid = Field::create_field<TYPE_FILE>(File(6)); // Missing required URI.
    const auto array_type = make_nullable(std::make_shared<DataTypeArray>(file));
    const auto map_type =
            make_nullable(std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), file));
    const std::array<DataTypePtr, 2> types {array_type, map_type};
    const std::array<Field, 2> fields {
            Field::create_field<TYPE_ARRAY>(Array {invalid}),
            Field::create_field<TYPE_MAP>(Map {
                    Field::create_field<TYPE_ARRAY>(Array {Field::create_field<TYPE_STRING>("k")}),
                    Field::create_field<TYPE_ARRAY>(Array {invalid})})};
    for (size_t index = 0; index != types.size(); ++index) {
        auto values = types[index]->create_column();
        values->insert(fields[index]);
        EXPECT_FALSE(validate_file_column(*values, types[index]).ok());
        assert_cast<ColumnNullable&>(*values).get_null_map_data()[0] = 1;
        EXPECT_EQ(render(types[index], *values), "null");
    }
}

TEST(JsonCellWriterTest, ReusesStateAcrossRowsWithNestedFileAndScalars) {
    auto file_array = std::make_shared<DataTypeArray>(std::make_shared<DataTypeFile>());
    auto type = std::make_shared<DataTypeStruct>(
            DataTypes {std::make_shared<DataTypeInt64>(), file_array,
                       make_nullable(std::make_shared<DataTypeString>())},
            Strings {"n", "files", "text"});
    auto values = type->create_column();
    const std::string bytes("\0\xfb\xff", 3);
    values->insert(Field::create_field<TYPE_STRUCT>(
            Struct {Field::create_field<TYPE_BIGINT>(1),
                    Field::create_field<TYPE_ARRAY>(Array {Field(), file_value(&bytes)}),
                    Field::create_field<TYPE_STRING>("[1]")}));
    values->insert(Field::create_field<TYPE_STRUCT>(
            Struct {Field::create_field<TYPE_BIGINT>(2), Field::create_field<TYPE_ARRAY>(Array {}),
                    Field()}));
    ASSERT_TRUE(validate_file_column(*values, type).ok());
    auto output = ColumnString::create();
    BufferWritable buffer(*output);
    DataTypeSerDe::FormatOptions options;
    JsonCellWriterState state;
    for (size_t row = 0; row != values->size(); ++row) {
        const auto status = write_json_cell(*values, type, row, buffer, options, state);
        ASSERT_TRUE(status.ok()) << status;
        buffer.commit();
    }
    ASSERT_EQ(output->size(), 2);
    EXPECT_EQ(
            output->get_data_at(0).to_string(),
            R"({"n":1,"files":[null,{"uri":"s3://bucket/a","offset":null,"size":3,"content_type":null,"checksum":null,"inline":"APv/"}],"text":"[1]"})");
    EXPECT_EQ(output->get_data_at(1).to_string(), R"({"n":2,"files":[],"text":null})");
}

TEST(JsonCellWriterTest, ExistingJsonbEmptyKeyAndNullStayJsonValues) {
    JsonbWriter input;
    ASSERT_TRUE(input.writeStartObject());
    ASSERT_TRUE(input.writeKey("", 0));
    ASSERT_TRUE(input.writeNull());
    ASSERT_TRUE(input.writeEndObject());
    auto type = std::make_shared<DataTypeJsonb>();
    auto values = type->create_column();
    values->insert_data(input.getOutput()->getBuffer(), input.getOutput()->getSize());
    values->insert_default(); // Existing JSONB sentinel is rendered as JSON null.
    EXPECT_EQ(render(type, *values, 0), R"({"":null})");
    EXPECT_EQ(render(type, *values, 1), "null");
}

TEST(JsonCellWriterTest, MalformedJsonbPropagatesErrorWithoutCommit) {
    auto type = std::make_shared<DataTypeStruct>(
            DataTypes {std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeJsonb>()},
            Strings {"before", "bad"});
    auto values = type->create_column();
    values->insert(Field::create_field<TYPE_STRUCT>(
            Struct {Field::create_field<TYPE_BIGINT>(7),
                    Field::create_field<TYPE_JSONB>(JsonbField("x", 1))}));
    auto output = ColumnString::create();
    BufferWritable buffer(*output);
    DataTypeSerDe::FormatOptions options;
    JsonCellWriterState state;
    const auto status = write_json_cell(*values, type, 0, buffer, options, state);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(output->size(), 0);
    EXPECT_FALSE(output->get_chars().empty()); // Prefix exists; caller must discard it.
}

TEST(JsonCellWriterTest, RejectsOrdinaryVarbinaryThroughoutSchema) {
    const auto binary = std::make_shared<DataTypeVarbinary>();
    const auto string = std::make_shared<DataTypeString>();
    const DataTypes types {
            binary,
            make_nullable(binary),
            std::make_shared<DataTypeArray>(binary),
            std::make_shared<DataTypeMap>(string, binary),
            std::make_shared<DataTypeMap>(binary, string),
            make_nullable(std::make_shared<DataTypeStruct>(
                    DataTypes {std::make_shared<DataTypeArray>(binary)}, Strings {"nested"}))};
    for (const auto& type : types) {
        SCOPED_TRACE(type->get_name());
        EXPECT_FALSE(validate_json_output_type(type).ok());
    }
}

TEST(JsonCellWriterTest, FilePublicInlineDoesNotRestrictOutputSchema) {
    const auto file = std::make_shared<DataTypeFile>();
    const DataTypes types {file, make_nullable(file), std::make_shared<DataTypeArray>(file),
                           std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), file),
                           std::make_shared<DataTypeStruct>(DataTypes {file}, Strings {"f"})};
    for (const auto& type : types) {
        SCOPED_TRACE(type->get_name());
        const auto status = validate_json_output_type(type);
        EXPECT_TRUE(status.ok()) << status;
    }
}

TEST(JsonCellWriterTest, RejectsOrdinaryVarbinaryCell) {
    const auto type = std::make_shared<DataTypeVarbinary>();
    auto values = type->create_column();
    values->insert(Field::create_field<TYPE_VARBINARY>(StringView("abc", 3)));
    auto output = ColumnString::create();
    BufferWritable buffer(*output);
    DataTypeSerDe::FormatOptions options;
    JsonCellWriterState state;
    EXPECT_FALSE(write_json_cell(*values, type, 0, buffer, options, state).ok());
    EXPECT_TRUE(output->get_chars().empty());
}

TEST(JsonCellWriterTest, DateTimeAndIpValuesMatchTypedToJson) {
    struct Case {
        DataTypePtr type;
        std::string input;
        std::string expected;
    };
    const std::array<Case, 6> cases {{
            {std::make_shared<DataTypeDateV2>(), "2023-10-01", R"("2023-10-01")"},
            {std::make_shared<DataTypeDateTimeV2>(3), "2023-10-01 12:34:56.789",
             R"("2023-10-01 12:34:56.789")"},
            {std::make_shared<DataTypeTimeV2>(3), "12:34:56.789", R"("12:34:56.789")"},
            {std::make_shared<DataTypeTimeStampNs>(), "2023-10-01 12:34:56.123456789",
             R"("2023-10-01 12:34:56.123456789")"},
            {std::make_shared<DataTypeIPv4>(), "198.0.0.1", R"("198.0.0.1")"},
            {std::make_shared<DataTypeIPv6>(), "2001:0db8:85a3:0000:0000:8a2e:0370:7334",
             R"("2001:db8:85a3::8a2e:370:7334")"},
    }};
    const auto timezone = cctz::utc_time_zone();
    DataTypeSerDe::FormatOptions options;
    options.timezone = &timezone;
    for (const auto& item : cases) {
        SCOPED_TRACE(item.type->get_name());
        auto values = item.type->create_column();
        auto input = StringRef(item.input);
        const auto status = item.type->get_serde()->from_string(input, *values, options);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(values->size(), 1);
        EXPECT_EQ(render(item.type, *values), item.expected);
        EXPECT_EQ(render(item.type, *values), typed_jsonb_text(item.type, *values, 0));
    }
}

TEST(JsonCellWriterTest, LegacyDatesUseQuotedCastText) {
    const DataTypes types {std::make_shared<DataTypeDate>(), std::make_shared<DataTypeDateTime>()};
    const std::array<std::string, 2> inputs {"2023-10-01", "2023-10-01 12:34:56"};
    DataTypeSerDe::FormatOptions options;
    for (size_t index = 0; index != types.size(); ++index) {
        auto values = types[index]->create_column();
        auto input = StringRef(inputs[index]);
        const auto status = types[index]->get_serde()->from_string(input, *values, options);
        ASSERT_TRUE(status.ok()) << status;
        EXPECT_EQ(render(types[index], *values), "\"" + inputs[index] + "\"");
    }
}

TEST(JsonCellWriterTest, TimestampTzUsesTypedToJsonUtcDespiteSessionTimezone) {
    const auto type = std::make_shared<DataTypeTimeStampTz>(3);
    auto values = type->create_column();
    TimestampTzValue value;
    value.unchecked_set_time(2023, 10, 1, 4, 34, 56, 789000);
    values->insert(Field::create_field<TYPE_TIMESTAMPTZ>(value));
    const auto timezone = cctz::fixed_time_zone(std::chrono::hours(8));
    DataTypeSerDe::FormatOptions options;
    options.timezone = &timezone;
    auto output = ColumnString::create();
    BufferWritable buffer(*output);
    JsonCellWriterState state;
    const auto status = write_json_cell(*values, type, 0, buffer, options, state);
    ASSERT_TRUE(status.ok()) << status;
    buffer.commit();
    const auto actual = output->get_data_at(0).to_string();
    EXPECT_EQ(actual, R"("2023-10-01 04:34:56.789+00:00")");
    EXPECT_EQ(actual, typed_jsonb_text(type, *values, 0));
    EXPECT_EQ(value.to_string(timezone, 3), "2023-10-01 12:34:56.789+08:00");
}

TEST(JsonCellWriterTest, RejectsJsonbRowstoreBinaryAndDictionaryRepresentations) {
    const auto type = std::make_shared<DataTypeJsonb>();
    auto values = type->create_column();
    JsonbWriter input;
    ASSERT_TRUE(input.writeStartBinary());
    ASSERT_TRUE(input.writeBinary("abc", 3));
    ASSERT_TRUE(input.writeEndBinary());
    values->insert_data(input.getOutput()->getBuffer(), input.getOutput()->getSize());
    input.reset();
    ASSERT_TRUE(input.writeStartObject());
    ASSERT_TRUE(input.writeKey(JsonbKeyValue::keyid_type(1)));
    ASSERT_TRUE(input.writeNull());
    ASSERT_TRUE(input.writeEndObject());
    values->insert_data(input.getOutput()->getBuffer(), input.getOutput()->getSize());
    DataTypeSerDe::FormatOptions options;
    JsonCellWriterState state;
    for (size_t row = 0; row != values->size(); ++row) {
        auto output = ColumnString::create();
        BufferWritable buffer(*output);
        EXPECT_FALSE(write_json_cell(*values, type, row, buffer, options, state).ok());
        EXPECT_EQ(output->size(), 0);
    }
}

} // namespace
} // namespace doris::json_format
