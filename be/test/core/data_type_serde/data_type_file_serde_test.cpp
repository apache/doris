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

#include "core/data_type_serde/data_type_file_serde.h"

#include <cctz/time_zone.h>
#include <gen_cpp/types.pb.h>
#include <gtest/gtest.h>

#include <limits>

#include "core/arena.h"
#include "core/column/column_const.h"
#include "core/column/column_file.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_file.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type_serde/data_type_nullable_serde.h"
#include "core/string_buffer.hpp"
#include "core/value/jsonb_value.h"
#include "exprs/function/cast/cast_parameters.h"
#include "util/jsonb_utils.h"
#include "util/jsonb_writer.h"
#include "util/mysql_row_buffer.h"

namespace doris {
class DataTypeFileSerDeTest : public testing::Test {
protected:
    DataTypeFile type;
    DataTypeFileSerDe serde;
    DataTypeSerDe::FormatOptions options;

    static Field value(const std::string* bytes = nullptr) {
        File fields(6);
        fields[0] = Field::create_field<TYPE_STRING>("s3://bucket/a?versionId=AbC");
        fields[2] = Field::create_field<TYPE_BIGINT>(1024);
        fields[3] = Field::create_field<TYPE_STRING>("image/png");
        fields[4] = Field::create_field<TYPE_STRING>("ETAG:opaque-2");
        if (bytes) {
            fields[5] = Field::create_field<TYPE_VARBINARY>(
                    StringView(bytes->data(), cast_set<uint32_t>(bytes->size())));
        }
        return Field::create_field<TYPE_FILE>(std::move(fields));
    }

    static std::string public_json(const std::string& inline_json = "null") {
        return R"({"uri":"s3://bucket/a?versionId=AbC","offset":null,"size":1024,"content_type":"image/png","checksum":"ETAG:opaque-2","inline":)" +
               inline_json + "}";
    }

    std::string text(const IColumn& column, size_t row, bool format) {
        auto output = ColumnString::create();
        BufferWritable buffer(*output);
        if (format) {
            EXPECT_TRUE(serde.serialize_one_cell_to_json(column, row, buffer, options).ok());
        } else {
            serde.to_string(column, row, buffer, options);
        }
        buffer.commit();
        return output->get_data_at(0).to_string();
    }

    Status parse(IColumn& column, const std::string& json) {
        StringRef input(json);
        return serde.from_string(input, column, options);
    }

    Status parse_format(IColumn& column, std::string json, bool csv = false) {
        Slice input(json);
        return csv ? serde.deserialize_one_cell_from_csv(column, input, options)
                   : serde.deserialize_one_cell_from_json(column, input, options);
    }

    static void expect_bytes(const IColumn& column, size_t row, const std::string* expected) {
        auto field = column[row];
        ASSERT_EQ(field.get_type(), TYPE_FILE);
        const auto& fields = field.get<TYPE_FILE>();
        ASSERT_EQ(fields.size(), 6);
        ASSERT_EQ(fields[5].is_null(), expected == nullptr);
        if (expected) {
            EXPECT_EQ(fields[5].get<TYPE_VARBINARY>().to_string_ref().to_string(), *expected);
        }
    }
};

TEST_F(DataTypeFileSerDeTest, TextRoundTripPreservesOpaqueMetadataBytes) {
    for (const size_t field : {0, 4}) {
        SCOPED_TRACE(field);
        auto input = value();
        auto& fields = input.get<TYPE_FILE>();
        const std::string bytes = (field == 0 ? std::string("urn:") : std::string("ETAG:")) +
                                  std::string("\xff\0x", 3);
        fields[field] = Field::create_field<TYPE_STRING>(bytes);
        auto source = type.create_column();
        source->insert(input);
        for (const bool format : {false, true}) {
            const auto encoded = text(*source, 0, format);
            auto restored = type.create_column();
            ASSERT_TRUE(
                    (format ? parse_format(*restored, encoded) : parse(*restored, encoded)).ok());
            ASSERT_EQ(restored->size(), 1);
            const auto output = (*restored)[0];
            const auto& output_fields = output.get<TYPE_FILE>();
            EXPECT_EQ(output_fields[field].get<TYPE_STRING>(), bytes);
            EXPECT_EQ(output_fields[0].get<TYPE_STRING>(), fields[0].get<TYPE_STRING>());
            EXPECT_EQ(output_fields[4].get<TYPE_STRING>(), fields[4].get<TYPE_STRING>());
            expect_bytes(*restored, 0, nullptr);
        }
    }
}

TEST_F(DataTypeFileSerDeTest, InlineOnlyJsonbRoundTrip) {
    auto source = type.create_column();
    ASSERT_TRUE(parse(*source, R"({"inline":""})").ok());
    ASSERT_TRUE(parse(*source, R"({"uri":null,"inline":"AP8="})").ok());
    auto jsonb_restored = type.create_column();
    CastParameters parameters;
    for (size_t row = 0; row < source->size(); ++row) {
        const auto value = (*source)[row];
        for (size_t child = 0; child < 5; ++child) {
            EXPECT_TRUE(value.get<TYPE_FILE>()[child].is_null());
        }
        JsonbWriter writer;
        ASSERT_TRUE(serde.serialize_column_to_jsonb(*source, row, writer).ok());
        const JsonbDocument* document = nullptr;
        ASSERT_TRUE(JsonbDocument::checkAndCreateDocument(writer.getOutput()->getBuffer(),
                                                          writer.getOutput()->getSize(), &document)
                            .ok());
        ASSERT_TRUE(serde.deserialize_column_from_jsonb(*jsonb_restored, document->getValue(),
                                                        parameters)
                            .ok());
        EXPECT_EQ(text(*jsonb_restored, row, false), text(*source, row, false));
    }
}

TEST_F(DataTypeFileSerDeTest, InlineOnlyProtobufRoundTripAndRequiresLocator) {
    auto source = type.create_column();
    ASSERT_TRUE(parse(*source, R"({"inline":""})").ok());
    ASSERT_TRUE(parse(*source, R"({"uri":null,"inline":"AP8="})").ok());
    PValues pb;
    ASSERT_TRUE(serde.write_column_to_pb(*source, pb, 0, source->size()).ok());
    auto pb_restored = type.create_column();
    ASSERT_TRUE(serde.read_column_from_pb(*pb_restored, pb).ok());
    ASSERT_TRUE(validate_file_column(*pb_restored, std::make_shared<DataTypeFile>()).ok());
    for (size_t row = 0; row < source->size(); ++row) {
        EXPECT_EQ(text(*pb_restored, row, false), text(*source, row, false));
    }
    for (const auto* json : {R"({"inline":null})", R"({"uri":null,"inline":null})"}) {
        EXPECT_FALSE(parse(*source, json).ok());
        EXPECT_EQ(source->size(), 2);
    }
}

TEST_F(DataTypeFileSerDeTest, JsonAcceptsUriWithoutFormatChecks) {
    auto column = type.create_column();
    for (const auto* json : {R"({"uri":""})", R"({"uri":"relative/path"})",
                             R"({"uri":"s3://bucket/a b+中文%ZZ#fragment"})"}) {
        ASSERT_TRUE(parse(*column, json).ok()) << json;
    }
    EXPECT_EQ((*column)[0].get<TYPE_FILE>()[0].get<TYPE_STRING>(), "");
    EXPECT_EQ((*column)[1].get<TYPE_FILE>()[0].get<TYPE_STRING>(), "relative/path");
    EXPECT_EQ((*column)[2].get<TYPE_FILE>()[0].get<TYPE_STRING>(),
              "s3://bucket/a b+中文%ZZ#fragment");
}

TEST_F(DataTypeFileSerDeTest, PublicJsonbRoundTripsInlineNullEmptyAndBinary) {
    const std::string fixtures[] {"", "f", std::string("\0\xff\x80", 3)};
    auto source = type.create_column();
    source->insert(value());
    for (const auto& bytes : fixtures) {
        source->insert(value(&bytes));
    }
    auto restored = type.create_column();
    CastParameters parameters;
    for (size_t row = 0; row < source->size(); ++row) {
        JsonbWriter writer;
        ASSERT_TRUE(serde.serialize_column_to_jsonb(*source, row, writer).ok());
        const JsonbDocument* document = nullptr;
        ASSERT_TRUE(JsonbDocument::checkAndCreateDocument(writer.getOutput()->getBuffer(),
                                                          writer.getOutput()->getSize(), &document)
                            .ok());
        const auto* object = document->getValue()->unpack<ObjectVal>();
        ASSERT_EQ(object->numElem(), DataTypeFile::FIELD_COUNT);
        ASSERT_NE(object->find("inline"), nullptr);
        ASSERT_TRUE(serde.deserialize_column_from_jsonb(*restored, document->getValue(), parameters)
                            .ok());
        expect_bytes(*restored, row, row == 0 ? nullptr : &fixtures[row - 1]);
        EXPECT_EQ(text(*restored, row, false), text(*source, row, true));
    }
}

TEST_F(DataTypeFileSerDeTest, PublicOutputIncludesInlineAndBinaryUsesLengthEncodedJson) {
    const std::string bytes("\0\xff", 2);
    auto column = type.create_column();
    column->insert(value(&bytes));
    EXPECT_EQ(text(*column, 0, false), public_json("\"AP8=\""));

    auto mysql_text = ColumnString::create();
    BufferWritable text_buffer(*mysql_text);
    ASSERT_TRUE(serde.write_column_to_mysql_text(*column, text_buffer, 0, options));
    text_buffer.commit();
    EXPECT_EQ(mysql_text->get_data_at(0).to_string(), public_json("\"AP8=\""));

    JsonbWriter writer;
    ASSERT_TRUE(serde.serialize_column_to_jsonb(*column, 0, writer).ok());
    EXPECT_EQ(JsonbToJson::jsonb_to_json_string(writer.getOutput()->getBuffer(),
                                                writer.getOutput()->getSize()),
              public_json("\"AP8=\""));

    MysqlRowBinaryBuffer binary;
    binary.start_binary_row(1);
    ASSERT_TRUE(serde.write_column_to_mysql_binary(*column, binary, 0, false, options).ok());
    const auto expected = public_json("\"AP8=\"");
    ASSERT_EQ(binary.length(), expected.size() + 3);
    EXPECT_EQ(static_cast<unsigned char>(binary.buf()[2]), expected.size());
    EXPECT_EQ(std::string(binary.buf() + 3, expected.size()), expected);
}

TEST_F(DataTypeFileSerDeTest, FormatOutputPreservesNullEmptyAndPaddedBase64) {
    auto column = type.create_column();
    column->insert(value());
    std::vector<std::pair<std::string, std::string>> fixtures = {
            {"", ""},
            {"f", "Zg=="},
            {"fo", "Zm8="},
            {"foo", "Zm9v"},
            {std::string("\0\xff\x80", 3), "AP+A"}};
    const auto prefix = public_json().substr(0, public_json().size() - 5);
    EXPECT_EQ(text(*column, 0, true), prefix + "null}");
    for (const auto& [bytes, encoded] : fixtures) {
        column->insert(value(&bytes));
        EXPECT_EQ(text(*column, column->size() - 1, true), prefix + "\"" + encoded + "\"}");
    }
    // Crossing the encoder's chunks must not insert padding in the middle.
    const std::string large(3 * 4096 + 1, 'f');
    column->insert(value(&large));
    std::string encoded;
    for (size_t i = 0; i < 4096; ++i) encoded += "ZmZm";
    encoded += "Zg==";
    EXPECT_EQ(text(*column, column->size() - 1, true), prefix + "\"" + encoded + "\"}");

    auto constant = ColumnConst::create(column->clone_resized(1), 3);
    EXPECT_EQ(text(*constant, 2, true), prefix + "null}");
}

TEST_F(DataTypeFileSerDeTest, PublicInputMatchesNamesAndRejectsInvalidWholeValue) {
    auto column = type.create_column();
    ASSERT_TRUE(parse(*column, R"({"size":0,"uri":"s3://bucket/a"})").ok());
    expect_bytes(*column, 0, nullptr);
    const std::vector<std::string> invalid = {
            "[]",
            "null",
            R"("s3://bucket/a")",
            "{}",
            R"({"uri":null})",
            R"({"uri":"s3://bucket/a","extra":null})",
            R"({"uri":"s3://bucket/a","uri":"s3://bucket/b"})",
            R"({"URI":"s3://bucket/a"})",
            R"({"uri":"s3://bucket/a","size":"1"})",
            R"({"uri":"s3://bucket/a","size":1.0})",
            R"({"uri":"s3://bucket/a","offset":0})",
            R"({"uri":"s3://bucket/a","size":9223372036854775808})",
            R"({"uri":"s3://bucket/a","checksum":"md5:d41d8cd98f00b204e9800998ecf8427e"})",
            R"({"uri":"s3://bucket/a","checksum":"MD5:D41D8CD98F00B204E9800998ECF8427E"})"};
    for (const auto& json : invalid) {
        SCOPED_TRACE(json);
        EXPECT_FALSE(parse(*column, json).ok());
        EXPECT_EQ(column->size(), 1);
        const auto& file = assert_cast<const ColumnFile&>(*column);
        for (size_t i = 0; i < 6; ++i) EXPECT_EQ(file.get_column(i).size(), 1);
    }
}

TEST_F(DataTypeFileSerDeTest, PublicTextInputPreservesNullEmptyAndBinaryInline) {
    auto column = type.create_column();
    ASSERT_TRUE(parse(*column, R"({"uri":"s3://bucket/a","inline":null})").ok());
    ASSERT_TRUE(parse(*column, R"({"uri":"s3://bucket/a","inline":""})").ok());
    ASSERT_TRUE(parse(*column, R"({"uri":"s3://bucket/a","inline":"AP+A"})").ok());
    const std::string empty;
    const std::string bytes("\0\xff\x80", 3);
    expect_bytes(*column, 0, nullptr);
    expect_bytes(*column, 1, &empty);
    expect_bytes(*column, 2, &bytes);
    for (size_t row = 0; row < column->size(); ++row) {
        EXPECT_EQ(text(*column, row, false), text(*column, row, true));
    }
}

TEST_F(DataTypeFileSerDeTest, FormatInputRoundTripsAllSixFieldsAndInlineBytes) {
    auto source = type.create_column();
    source->insert(value());
    const std::vector<std::string> fixtures = {
            "", "f", "fo", "foo", std::string("\0\xff\x80", 3), std::string(3 * 4096 + 1, '\xff')};
    for (const auto& bytes : fixtures) source->insert(value(&bytes));
    for (const bool csv : {false, true}) {
        auto result = type.create_column();
        for (size_t row = 0; row < source->size(); ++row) {
            const auto json = text(*source, row, true);
            ASSERT_TRUE(parse_format(*result, json, csv).ok());
            EXPECT_EQ(text(*result, row, true), json);
            expect_bytes(*result, row, row == 0 ? nullptr : &fixtures[row - 1]);
        }
        EXPECT_TRUE(type.check_column(*result).ok());
    }
}

TEST_F(DataTypeFileSerDeTest, FormatInputMatchesNamesAndFillsMissingOptionalFields) {
    auto column = type.create_column();
    ASSERT_TRUE(parse_format(*column,
                             " \n"
                             R"({"uri":"s3://bucket/a"})"
                             "\r\n")
                        .ok());
    auto fields = (*column)[0].get<TYPE_FILE>();
    for (size_t i = 1; i < 6; ++i) EXPECT_TRUE(fields[i].is_null());
    ASSERT_TRUE(parse_format(*column, R"({"inline":null,"size":null,"uri":"s3://bucket/a"})").ok());
    expect_bytes(*column, 1, nullptr);
    ASSERT_TRUE(parse_format(*column, R"({"inline":"","uri":"s3://bucket/a","offset":0,"size":0})")
                        .ok());
    const std::string empty;
    expect_bytes(*column, 2, &empty);
    fields = (*column)[2].get<TYPE_FILE>();
    EXPECT_EQ(fields[1].get<TYPE_BIGINT>(), 0);
    EXPECT_EQ(fields[2].get<TYPE_BIGINT>(), 0);
}

TEST_F(DataTypeFileSerDeTest, FormatInputRejectsInvalidWholeValuesWithoutAppending) {
    auto column = type.create_column();
    column->insert(value());
    const auto original = text(*column, 0, true);
    const std::vector<std::string> invalid = {
            "",
            "[]",
            "null",
            R"("s3://bucket/a")",
            "{}",
            R"({"uri":null})",
            R"({"uri":"s3://bucket/a","extra":null})",
            R"({"URI":"s3://bucket/a"})",
            R"({"uri":"s3://bucket/a","Inline":""})",
            R"({"uri":"s3://bucket/a","\u0075ri":"s3://bucket/b"})",
            R"({"uri":"s3://bucket/a","inline":"","inline":null})",
            R"({"inline":"Zm9vYmFyZm9vYmFyZm9v","uri":"s3://bucket/a","unknown":0})",
            R"({"uri":"s3://bucket/a","inline":0})",
            R"({"uri":"s3://bucket/a","inline":true})",
            R"({"uri":"s3://bucket/a","inline":[]})",
            R"({"uri":"s3://bucket/a","size":"1"})",
            R"({"uri":"s3://bucket/a","size":1.0})",
            R"({"uri":"s3://bucket/a","size":1e0})",
            R"({"uri":"s3://bucket/a","size":9223372036854775808})",
            R"({"uri":"s3://bucket/a","size":-1})",
            R"({"uri":"s3://bucket/a","offset":0})",
            R"({"uri":"s3://bucket/a","size":1,"offset":9223372036854775807})",
            R"({"uri":"s3://bucket/a","checksum":"md5:d41d8cd98f00b204e9800998ecf8427e"})",
            R"({"uri":"s3://bucket/a","checksum":"MD5:D41D8CD98F00B204E9800998ECF8427E"})",
            R"({"uri":"s3://bucket/a"} trailing)",
            std::string(R"({"uri":"s3://bucket/a"})") + '\0' + "{}"};
    for (const auto& json : invalid) {
        SCOPED_TRACE(json);
        EXPECT_FALSE(parse_format(*column, json).ok());
        ASSERT_EQ(column->size(), 1);
        const auto& file = assert_cast<const ColumnFile&>(*column);
        for (size_t i = 0; i < 6; ++i) EXPECT_EQ(file.get_column(i).size(), 1);
        EXPECT_EQ(text(*column, 0, true), original);
    }
}

TEST_F(DataTypeFileSerDeTest, FormatInputRequiresCanonicalPaddedBase64) {
    auto column = type.create_column();
    const std::vector<std::string> invalid = {
            "Zg",       "Zg=",   "Zg===",   "=m9v",    "Zm=v",    "Zm9v=",
            "Zg==AAAA", "====",  "Zh==",    "Zm9=",    "AP-A",    "AP_A",
            " Zg==",    "Zg== ", "Zg==\\n", "Zg==\\r", "Zg==\\t", "\\u0000AAA"};
    for (const auto& encoded : invalid) {
        SCOPED_TRACE(encoded);
        const std::string json = R"({"uri":"s3://bucket/a","inline":")" + encoded + R"("})";
        EXPECT_FALSE(parse_format(*column, json).ok());
        EXPECT_FALSE(parse(*column, json).ok());
        EXPECT_EQ(column->size(), 0);
    }
}

TEST_F(DataTypeFileSerDeTest, InlineBase64LengthChecksDoNotNeedPayloadOrAllocation) {
    uint32_t length = 99;
    ASSERT_TRUE(DataTypeFileSerDe::_checked_inline_size(0, 0, &length).ok());
    EXPECT_EQ(length, 0);
    for (size_t padding = 0; padding <= 2; ++padding) {
        ASSERT_TRUE(DataTypeFileSerDe::_checked_inline_size(4, padding, &length).ok());
        EXPECT_EQ(length, 3 - padding);
    }
    const uint64_t max_length = std::numeric_limits<uint32_t>::max();
    const auto max_encoded = static_cast<size_t>(max_length / 3 * 4);
    ASSERT_TRUE(DataTypeFileSerDe::_checked_inline_size(max_encoded, 0, &length).ok());
    EXPECT_EQ(length, max_length);
    length = 99;
    EXPECT_FALSE(DataTypeFileSerDe::_checked_inline_size(max_encoded + 4, 2, &length).ok());
    EXPECT_FALSE(DataTypeFileSerDe::_checked_inline_size(std::numeric_limits<size_t>::max() - 3, 2,
                                                         &length)
                         .ok());
    EXPECT_FALSE(DataTypeFileSerDe::_checked_inline_size(3, 0, &length).ok());
    EXPECT_FALSE(DataTypeFileSerDe::_checked_inline_size(0, 1, &length).ok());
    EXPECT_FALSE(DataTypeFileSerDe::_checked_inline_size(4, 3, &length).ok());
    EXPECT_EQ(length, 99);
}

TEST_F(DataTypeFileSerDeTest, FormatInputPreservesNestedFileValuesAndNulls) {
    const auto file = make_nullable(std::make_shared<DataTypeFile>());
    const auto array = make_nullable(std::make_shared<DataTypeArray>(file));
    const auto map = make_nullable(
            std::make_shared<DataTypeMap>(make_nullable(std::make_shared<DataTypeString>()), file));
    const DataTypeStruct schema({file, array, map}, {"asset", "files", "lookup"});
    auto column = schema.create_column();
    std::string json =
            R"({"asset":{"uri":"s3://bucket/a","inline":"AP+A"},"files":[null,{"uri":"s3://bucket/b","inline":""}],"lookup":{"raw":{"uri":"s3://bucket/c","inline":"Zg=="},"none":null}})";
    Slice input(json);
    ASSERT_TRUE(schema.get_serde()->deserialize_one_cell_from_json(*column, input, options).ok());
    const auto fields = (*column)[0].get<TYPE_STRUCT>();
    EXPECT_EQ(fields[0].get<TYPE_FILE>()[5].get<TYPE_VARBINARY>().to_string_ref().to_string(),
              std::string("\0\xff\x80", 3));
    const auto& items = fields[1].get<TYPE_ARRAY>();
    ASSERT_EQ(items.size(), 2);
    EXPECT_TRUE(items[0].is_null());
    const auto& empty_inline = items[1].get<TYPE_FILE>()[5];
    EXPECT_FALSE(empty_inline.is_null());
    EXPECT_EQ(empty_inline.get<TYPE_VARBINARY>().size(), 0);
    const auto& entries = fields[2].get<TYPE_MAP>();
    ASSERT_EQ(entries.size(), 2);
    const auto& values = entries[1].get<TYPE_ARRAY>();
    ASSERT_EQ(values.size(), 2);
    EXPECT_EQ(values[0].get<TYPE_FILE>()[5].get<TYPE_VARBINARY>().to_string_ref().to_string(), "f");
    EXPECT_TRUE(values[1].is_null());
}

TEST_F(DataTypeFileSerDeTest, ProtobufPreservesFileIdentityInlineAndOuterNullMask) {
    const std::string bytes("\0\xffinline", 8);
    auto source = ColumnNullable::create(type.create_column(), ColumnUInt8::create());
    source->insert_default(); // The nested URI is invalid, but the FILE is NULL.
    source->insert(value(&bytes));
    source->insert(value());
    DataTypeNullableSerDe nullable(std::make_shared<DataTypeFileSerDe>(), 1);
    PValues pb;
    ASSERT_TRUE(nullable.write_column_to_pb(*source, pb, 0, 3).ok());
    EXPECT_EQ(pb.type().id(), PGenericType::FILE);
    ASSERT_EQ(pb.child_element_size(), 6);
    EXPECT_EQ(pb.child_element(5).type().id(), PGenericType::VARBINARY);
    auto restored = ColumnNullable::create(type.create_column(), ColumnUInt8::create());
    ASSERT_TRUE(nullable.read_column_from_pb(*restored, pb).ok());
    ASSERT_EQ(restored->size(), 3);
    EXPECT_TRUE(restored->is_null_at(0));
    EXPECT_FALSE(restored->is_null_at(1));
    expect_bytes(restored->get_nested_column(), 1, &bytes);
    expect_bytes(restored->get_nested_column(), 2, nullptr);

    pb.mutable_type()->set_id(PGenericType::STRUCT);
    EXPECT_FALSE(nullable.read_column_from_pb(*restored, pb).ok());
    EXPECT_EQ(restored->size(), 3);
}

TEST_F(DataTypeFileSerDeTest, JsonbCastRequiresAllSixFieldsWhileJsonParsingKeepsDefaults) {
    auto column = type.create_column();
    JsonBinaryValue encoded;
    ASSERT_TRUE(encoded.from_json_string(R"({"uri":"s3://bucket/a"})").ok());
    const JsonbDocument* document = nullptr;
    ASSERT_TRUE(
            JsonbDocument::checkAndCreateDocument(encoded.value(), encoded.size(), &document).ok());
    CastParameters parameters;
    EXPECT_FALSE(
            serde.deserialize_column_from_jsonb(*column, document->getValue(), parameters).ok());
    EXPECT_EQ(column->size(), 0);
    ASSERT_TRUE(parse_format(*column, R"({"uri":"s3://bucket/a","inline":"AA=="})").ok());
    const std::string bytes(1, '\0');
    expect_bytes(*column, 0, &bytes);
}

TEST_F(DataTypeFileSerDeTest, JsonbCastRejectsMissingInlineAndInvalidBase64Atomically) {
    const std::string prefix =
            R"({"uri":"s3://bucket/a","offset":null,"size":null,"content_type":null,"checksum":null)";
    const std::vector<std::string> invalid {prefix + "}",
                                            prefix + R"(,"inline":"AA"})",
                                            prefix + R"(,"inline":"AB=="})",
                                            prefix + R"(,"inline":"_w=="})",
                                            prefix + R"(,"inline":1})",
                                            prefix + R"(,"inline":"","inline":null})",
                                            prefix + R"(,"inline":null,"extra":null})"};
    auto column = type.create_column();
    column->insert(value());
    CastParameters parameters;
    for (const auto& json : invalid) {
        SCOPED_TRACE(json);
        JsonBinaryValue encoded;
        ASSERT_TRUE(encoded.from_json_string(json).ok());
        const JsonbDocument* document = nullptr;
        ASSERT_TRUE(
                JsonbDocument::checkAndCreateDocument(encoded.value(), encoded.size(), &document)
                        .ok());
        EXPECT_FALSE(serde.deserialize_column_from_jsonb(*column, document->getValue(), parameters)
                             .ok());
        EXPECT_EQ(column->size(), 1);
    }
}

TEST_F(DataTypeFileSerDeTest, JsonbCastRejectsIncompleteDuplicateAndFloatingPointWithoutAppending) {
    auto column = type.create_column();
    CastParameters parameters;
    for (int invalid_case = 0; invalid_case < 3; ++invalid_case) {
        JsonbWriter writer;
        ASSERT_TRUE(writer.writeStartObject());
        ASSERT_TRUE(writer.writeKey("uri", 3));
        ASSERT_TRUE(writer.writeStartString());
        ASSERT_TRUE(writer.writeString("s3://bucket/a"));
        ASSERT_TRUE(writer.writeEndString());
        if (invalid_case == 0) {
            ASSERT_TRUE(writer.writeKey("inline", 6));
            ASSERT_TRUE(writer.writeNull());
        } else if (invalid_case == 1) {
            ASSERT_TRUE(writer.writeKey("uri", 3));
            ASSERT_TRUE(writer.writeNull());
        } else {
            ASSERT_TRUE(writer.writeKey("size", 4));
            ASSERT_TRUE(writer.writeDouble(1.0));
        }
        ASSERT_TRUE(writer.writeEndObject());
        const JsonbDocument* document = nullptr;
        ASSERT_TRUE(JsonbDocument::checkAndCreateDocument(writer.getOutput()->getBuffer(),
                                                          writer.getOutput()->getSize(), &document)
                            .ok());
        EXPECT_FALSE(serde.deserialize_column_from_jsonb(*column, document->getValue(), parameters)
                             .ok());
        EXPECT_EQ(column->size(), 0);
    }
    auto source = type.create_column();
    source->insert(value());
    JsonbWriter writer;
    ASSERT_TRUE(serde.serialize_column_to_jsonb(*source, 0, writer).ok());
    const JsonbDocument* document = nullptr;
    ASSERT_TRUE(JsonbDocument::checkAndCreateDocument(writer.getOutput()->getBuffer(),
                                                      writer.getOutput()->getSize(), &document)
                        .ok());
    ASSERT_TRUE(
            serde.deserialize_column_from_jsonb(*column, document->getValue(), parameters).ok());
    expect_bytes(*column, 0, nullptr);
    EXPECT_EQ(text(*column, 0, false), public_json());
}

TEST_F(DataTypeFileSerDeTest, ProtobufShapeValidationIsAtomicForAllSixChildren) {
    auto source = type.create_column();
    source->insert(value());
    PValues pb;
    ASSERT_TRUE(serde.write_column_to_pb(*source, pb, 0, 1).ok());
    auto restored = type.create_column();
    restored->insert(value());
    pb.mutable_child_element(2)->clear_int64_value();
    EXPECT_FALSE(serde.read_column_from_pb(*restored, pb).ok());
    EXPECT_EQ(restored->size(), 1);
    pb.mutable_child_element(2)->add_int64_value(1024);
    pb.mutable_child_element(5)->clear_bytes_value();
    EXPECT_FALSE(serde.read_column_from_pb(*restored, pb).ok());
    EXPECT_EQ(restored->size(), 1);
}

TEST_F(DataTypeFileSerDeTest, ProtobufPreservesNullableParentWithNonNullableFileChild) {
    const auto outer_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {std::make_shared<DataTypeFile>()}, Strings {"payload"}));
    const auto outer_serde = outer_type->get_serde();
    auto source = outer_type->create_column();
    source->insert_default();
    source->insert(Field::create_field<TYPE_STRUCT>(Struct {value()}));
    PValues pb;
    ASSERT_TRUE(outer_serde->write_column_to_pb(*source, pb, 0, 2).ok());
    // FILE is nonnullable within a nullable STRUCT. Its invalid default URI is hidden.
    ASSERT_FALSE(pb.child_element(0).has_null());
    auto restored = outer_type->create_column();
    ASSERT_TRUE(outer_serde->read_column_from_pb(*restored, pb).ok());
    ASSERT_EQ(restored->size(), 2);
    EXPECT_TRUE(restored->is_null_at(0));
    EXPECT_FALSE(restored->is_null_at(1));
    const auto& parent = assert_cast<const ColumnNullable&>(*restored);
    const auto& structure = assert_cast<const ColumnStruct&>(parent.get_nested_column());
    EXPECT_TRUE(structure.get_column(0)[0].get<TYPE_FILE>()[0].is_null());
    EXPECT_EQ(structure.get_column(0)[1], value());
}

TEST_F(DataTypeFileSerDeTest, ArrowConversionIsUnsupported) {
    auto column = type.create_column();
    const auto timezone = cctz::utc_time_zone();
    EXPECT_FALSE(serde.write_column_to_arrow(*column, nullptr, nullptr, 0, 0, timezone).ok());
    EXPECT_FALSE(serde.read_column_from_arrow(*column, nullptr, 0, 0, timezone).ok());
}

TEST_F(DataTypeFileSerDeTest, OrcConversionIsUnsupported) {
    auto column = type.create_column();
    Arena arena;
    EXPECT_FALSE(
            serde.write_column_to_orc("UTC", *column, nullptr, nullptr, 0, 0, arena, options).ok());
    EXPECT_FALSE(serde.read_column_from_orc(*column, OrcDecodedColumnView {}).ok());
}

TEST_F(DataTypeFileSerDeTest, MysqlNullableWrapperUsesProtocolNull) {
    auto column = ColumnNullable::create(type.create_column(), ColumnUInt8::create());
    column->insert_default();
    DataTypeNullableSerDe nullable(std::make_shared<DataTypeFileSerDe>());
    auto output = ColumnString::create();
    BufferWritable bw(*output);
    EXPECT_FALSE(nullable.write_column_to_mysql_text(*column, bw, 0, options));
    bw.commit();
    EXPECT_EQ(output->get_data_at(0).size, 0);
    MysqlRowBinaryBuffer binary;
    binary.start_binary_row(1);
    ASSERT_TRUE(nullable.write_column_to_mysql_binary(*column, binary, 0, false, options).ok());
    ASSERT_EQ(binary.length(), 2);
    EXPECT_EQ(static_cast<unsigned char>(binary.buf()[1]), 4);
}
} // namespace doris
