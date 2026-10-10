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

#include <arrow/api.h>
#include <cctz/time_zone.h>
#include <gen_cpp/types.pb.h>
#include <gtest/gtest.h>

#include <array>
#include <limits>
#include <orc/OrcFile.hh>

#include "core/arena.h"
#include "core/column/column_const.h"
#include "core/column/column_file.h"
#include "core/column/column_nullable.h"
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
            R"({"uri":"relative/path"})",
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
            R"({"uri":"relative/path"})",
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

TEST_F(DataTypeFileSerDeTest, RawJsonInputDecodesEscapedNamesAndStringSiblings) {
    const auto file = make_nullable(std::make_shared<DataTypeFile>());
    const auto record = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {file, make_nullable(std::make_shared<DataTypeString>())},
            Strings {"file", "label\"\\name"}));
    const DataTypeArray schema(record);
    auto column = schema.create_column();
    options.strict_json_strings = true;
    std::string json =
            R"([{"file":{"uri":"urn:escape","inline":"AP+A"},"label\"\\name":"quote\" slash\\ controls\n\u0000 unicode\u96ea delimiter,}]"}])";
    Slice input(json);
    ASSERT_TRUE(schema.get_serde()->deserialize_one_cell_from_json(*column, input, options).ok());
    ASSERT_EQ(column->size(), 1);
    const auto row = (*column)[0].get<TYPE_ARRAY>();
    ASSERT_EQ(row.size(), 1);
    const auto fields = row[0].get<TYPE_STRUCT>();
    ASSERT_EQ(fields.size(), 2);
    EXPECT_EQ(fields[0].get<TYPE_FILE>()[0].as_string_view(), "urn:escape");
    EXPECT_EQ(fields[0].get<TYPE_FILE>()[5].get<TYPE_VARBINARY>().to_string_ref().to_string(),
              std::string("\0\xff\x80", 3));
    std::string expected = "quote\" slash\\ controls\n";
    expected.push_back('\0');
    expected += " unicode雪 delimiter,}]";
    EXPECT_EQ(fields[1].as_string_view(), expected);
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

TEST_F(DataTypeFileSerDeTest, RowStoreJsonbIsOpaqueAndRetainsAllChildren) {
    const std::string bytes = std::string(4096, '\xff') + std::string("\0opaque", 7);
    auto column = type.create_column();
    column->insert(value(&bytes));
    Arena arena;
    JsonbWriter writer;
    ASSERT_TRUE(writer.writeStartObject());
    serde.write_one_cell_to_jsonb(*column, writer, arena, 7, 0, options);
    ASSERT_TRUE(writer.writeEndObject());
    const JsonbDocument* document = nullptr;
    ASSERT_TRUE(JsonbDocument::checkAndCreateDocument(writer.getOutput()->getBuffer(),
                                                      writer.getOutput()->getSize(), &document)
                        .ok());
    const auto* blob = document->getValue()->unpack<ObjectVal>()->begin()->value();
    ASSERT_TRUE(blob->isBinary());
    auto restored = type.create_column();
    serde.read_one_cell_from_jsonb(*restored, blob);
    ASSERT_EQ(restored->size(), 1);
    expect_bytes(*restored, 0, &bytes);
    EXPECT_EQ(text(*restored, 0, false), text(*column, 0, true));
}

TEST_F(DataTypeFileSerDeTest, JsonbCastRequiresAllSixFieldsWhileJsonImportKeepsDefaults) {
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

TEST_F(DataTypeFileSerDeTest, NullableRowStoreUsesTheWrappersKeyAndPreservesNull) {
    const std::string bytes(1024, '\xff');
    auto source = ColumnNullable::create(type.create_column(), ColumnUInt8::create());
    source->insert(value(&bytes));
    source->insert_default();
    DataTypeNullableSerDe nullable(std::make_shared<DataTypeFileSerDe>());
    auto restored = ColumnNullable::create(type.create_column(), ColumnUInt8::create());
    Arena arena;
    for (size_t row = 0; row < 2; ++row) {
        JsonbWriter writer;
        ASSERT_TRUE(writer.writeStartObject());
        ASSERT_NO_THROW(nullable.write_one_cell_to_jsonb(*source, writer, arena, 7, row, options));
        ASSERT_TRUE(writer.writeEndObject());
        const JsonbDocument* document = nullptr;
        ASSERT_TRUE(JsonbDocument::checkAndCreateDocument(writer.getOutput()->getBuffer(),
                                                          writer.getOutput()->getSize(), &document)
                            .ok());
        const auto* object = document->getValue()->unpack<ObjectVal>();
        ASSERT_EQ(object->numElem(), 1);
        nullable.read_one_cell_from_jsonb(*restored, object->begin()->value());
    }
    ASSERT_EQ(restored->size(), 2);
    EXPECT_FALSE(restored->is_null_at(0));
    EXPECT_TRUE(restored->is_null_at(1));
    expect_bytes(restored->get_nested_column(), 0, &bytes);
}

TEST_F(DataTypeFileSerDeTest, RowStoreRejectsInvalidWholeValueWithoutAppending) {
    auto column = type.create_column();
    column->insert(value());
    PValues pb;
    ASSERT_TRUE(serde.write_column_to_pb(*column, pb, 0, 1).ok());
    pb.mutable_child_element(2)->set_int64_value(0, -1);
    const auto bytes = pb.SerializeAsString();
    JsonbWriter writer;
    ASSERT_TRUE(writer.writeStartBinary());
    ASSERT_TRUE(writer.writeBinary(bytes.data(), bytes.size()));
    ASSERT_TRUE(writer.writeEndBinary());
    const JsonbDocument* document = nullptr;
    ASSERT_TRUE(JsonbDocument::checkAndCreateDocument(writer.getOutput()->getBuffer(),
                                                      writer.getOutput()->getSize(), &document)
                        .ok());
    EXPECT_THROW(serde.read_one_cell_from_jsonb(*column, document->getValue()), Exception);
    EXPECT_EQ(column->size(), 1);
}

TEST_F(DataTypeFileSerDeTest, ArrowPreservesBinaryAndSkipsNullParentPlaceholders) {
    const std::string bytes = std::string(4096, '\xff') + std::string("\0end", 4);
    auto column = ColumnNullable::create(type.create_column(), ColumnUInt8::create());
    column->insert(value());
    column->insert_default();
    column->insert(value(&bytes));
    const std::string empty;
    column->insert(value(&empty));
    auto arrow_type = arrow::struct_(
            {arrow::field("uri", arrow::utf8()), arrow::field("offset", arrow::int64()),
             arrow::field("size", arrow::int64()), arrow::field("content_type", arrow::utf8()),
             arrow::field("checksum", arrow::utf8()), arrow::field("inline", arrow::binary())});
    std::unique_ptr<arrow::ArrayBuilder> builder;
    ASSERT_TRUE(arrow::MakeBuilder(arrow::default_memory_pool(), arrow_type, &builder).ok());
    DataTypeNullableSerDe nullable(std::make_shared<DataTypeFileSerDe>());
    const auto timezone = cctz::utc_time_zone();
    // A nonzero range must use the source parent mask at the same offset.
    ASSERT_TRUE(
            nullable.write_column_to_arrow(*column, nullptr, builder.get(), 1, 4, timezone).ok());
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder->Finish(&array).ok());
    ASSERT_TRUE(array->ValidateFull().ok());
    ASSERT_TRUE(array->IsNull(0));
    auto restored = ColumnNullable::create(type.create_column(), ColumnUInt8::create());
    ASSERT_TRUE(nullable.read_column_from_arrow(*restored, array.get(), 0, 3, timezone).ok());
    ASSERT_EQ(restored->size(), 3);
    EXPECT_TRUE(restored->is_null_at(0));
    expect_bytes(restored->get_nested_column(), 1, &bytes);
    expect_bytes(restored->get_nested_column(), 2, &empty);
    EXPECT_EQ(text(restored->get_nested_column(), 1, false),
              text(restored->get_nested_column(), 1, true));
}

TEST_F(DataTypeFileSerDeTest, OrcDenseChildrenMatchSelectedDecode) {
    const std::string bytes = std::string(4096, '\xff') + std::string("\0end", 4);
    const std::string empty;
    auto source = type.create_column();
    source->insert(value());
    source->insert(value(&bytes));
    source->insert(value(&empty));
    auto schema = orc::Type::buildTypeFromString(
            "struct<uri:string,offset:bigint,size:bigint,content_type:string,checksum:string,"
            "inline:binary>");
    schema->setAttribute("doris.struct-type", "FILE");
    auto batch = schema->createRowBatch(3, *orc::getDefaultPool(), false, false);
    Arena arena;
    ASSERT_TRUE(
            serde.write_column_to_orc("UTC", *source, nullptr, batch.get(), 0, 3, arena, options)
                    .ok());
    auto& structure = assert_cast<orc::StructVectorBatch&>(*batch);
    ASSERT_FALSE(structure.hasNulls);
    const auto timezone = cctz::utc_time_zone();
    OrcDecodedColumnView view {.file_type = schema.get(),
                               .selected_type = schema.get(),
                               .batch = batch.get(),
                               .rows = 3,
                               .timezone = &timezone};
    auto dense = type.create_column();
    dense->insert(value());
    ASSERT_TRUE(serde.read_column_from_orc(*dense, view).ok());
    ASSERT_EQ(dense->size(), 4);
    expect_bytes(*dense, 1, nullptr);
    expect_bytes(*dense, 2, &bytes);
    expect_bytes(*dense, 3, &empty);
    const std::vector<size_t> selected {2, 0, 1};
    view.selected_rows = &selected;
    auto sparse = type.create_column();
    ASSERT_TRUE(serde.read_column_from_orc(*sparse, view).ok());
    for (size_t row = 0; row < selected.size(); ++row) {
        const auto actual = (*sparse)[row];
        const auto expected = (*dense)[selected[row] + 1];
        for (size_t child = 0; child < DataTypeFile::FIELD_COUNT; ++child) {
            EXPECT_EQ(actual.get<TYPE_FILE>()[child], expected.get<TYPE_FILE>()[child]);
        }
    }

    view.selected_rows = nullptr;
    assert_cast<orc::StringVectorBatch&>(*structure.fields[5]).length[2] = -1;
    EXPECT_FALSE(serde.read_column_from_orc(*dense, view).ok());
    EXPECT_EQ(dense->size(), 4);
}

TEST_F(DataTypeFileSerDeTest, OrcPreservesBinarySelectionAndParentNulls) {
    const std::string bytes = std::string(4096, '\xff') + std::string("\0end", 4);
    const std::string empty;
    auto column = ColumnNullable::create(type.create_column(), ColumnUInt8::create());
    column->insert(value());
    column->insert_default();
    column->insert(value(&bytes));
    column->insert(value(&empty));
    auto orc_type = orc::Type::buildTypeFromString(
            "struct<uri:string,offset:bigint,size:bigint,content_type:string,checksum:string,"
            "inline:binary>");
    orc_type->setAttribute("doris.struct-type", "FILE");
    auto batch = orc_type->createRowBatch(4, *orc::getDefaultPool(), false, false);
    DataTypeNullableSerDe nullable(std::make_shared<DataTypeFileSerDe>());
    Arena arena;
    ASSERT_TRUE(
            nullable.write_column_to_orc("UTC", *column, nullptr, batch.get(), 0, 4, arena, options)
                    .ok());
    auto& structure = assert_cast<orc::StructVectorBatch&>(*batch);
    ASSERT_TRUE(structure.hasNulls);
    EXPECT_FALSE(structure.notNull[1]);
    for (auto* child : structure.fields) EXPECT_FALSE(child->notNull[1]);
    const auto timezone = cctz::utc_time_zone();
    const std::vector<size_t> selected {1, 2, 3};
    OrcDecodedColumnView view {.file_type = orc_type.get(),
                               .selected_type = orc_type.get(),
                               .batch = batch.get(),
                               .rows = 4,
                               .selected_rows = &selected,
                               .timezone = &timezone};
    auto restored = ColumnNullable::create(type.create_column(), ColumnUInt8::create());
    ASSERT_TRUE(nullable.read_column_from_orc(*restored, view).ok());
    ASSERT_EQ(restored->size(), 3);
    EXPECT_TRUE(restored->is_null_at(0));
    expect_bytes(restored->get_nested_column(), 1, &bytes);
    expect_bytes(restored->get_nested_column(), 2, &empty);
    EXPECT_EQ(text(restored->get_nested_column(), 1, false),
              text(restored->get_nested_column(), 1, true));
}

TEST_F(DataTypeFileSerDeTest, OrcMissingOptionalChildrenFillNullWithSelectionAndParentNulls) {
    const std::string bytes("\0binary\xff", 8);
    const auto full_value = value(&bytes);
    auto source = type.create_column();
    source->insert(full_value);
    source->insert(full_value);
    source->insert(full_value);
    const auto& file = assert_cast<const ColumnFile&>(*source);
    DataTypeNullableSerDe nullable(std::make_shared<DataTypeFileSerDe>());
    const auto nullable_type = make_nullable(std::make_shared<DataTypeFile>());
    const auto timezone = cctz::utc_time_zone();
    const std::vector<size_t> selected {1, 2, 0};
    for (const bool parent_is_null : {false, true}) {
        for (size_t mask = 0; mask < 32; ++mask) {
            SCOPED_TRACE(mask);
            SCOPED_TRACE(parent_is_null);
            auto schema = orc::createStructType();
            schema->setAttribute("doris.struct-type", "FILE");
            std::vector<size_t> positions;
            for (size_t i = 0; i < 6; ++i) {
                if (i != 0 && !(mask & (1 << (i - 1)))) continue;
                const auto kind = i == 1 || i == 2 ? orc::LONG : i == 5 ? orc::BINARY : orc::STRING;
                schema->addStructField(type.get_element_name(i), orc::createPrimitiveType(kind));
                positions.push_back(i);
            }
            auto batch = schema->createRowBatch(3, *orc::getDefaultPool(), false, false);
            auto& structure = assert_cast<orc::StructVectorBatch&>(*batch);
            structure.numElements = 3;
            structure.hasNulls = parent_is_null;
            structure.notNull[0] = structure.notNull[2] = true;
            structure.notNull[1] = !parent_is_null;
            Arena arena;
            for (size_t j = 0; j < positions.size(); ++j) {
                const auto i = positions[j];
                ASSERT_TRUE(type.get_element(i)
                                    ->get_serde()
                                    ->write_column_to_orc("UTC", file.get_column(i), nullptr,
                                                          structure.fields[j], 0, 3, arena, options)
                                    .ok());
            }
            // A malformed child hidden by its FILE parent NULL must never be interpreted.
            auto& uri = assert_cast<orc::StringVectorBatch&>(*structure.fields[0]);
            if (parent_is_null) {
                uri.data[1] = const_cast<char*>("relative");
                uri.length[1] = 8;
            }
            OrcDecodedColumnView view {.file_type = schema.get(),
                                       .selected_type = schema.get(),
                                       .batch = batch.get(),
                                       .rows = 3,
                                       .selected_rows = parent_is_null ? &selected : nullptr,
                                       .timezone = &timezone};
            auto restored = nullable_type->create_column();
            ASSERT_TRUE(nullable.read_column_from_orc(*restored, view).ok());
            ASSERT_EQ(restored->size(), 3);
            EXPECT_EQ(restored->is_null_at(0), parent_is_null);
            ASSERT_TRUE(validate_file_column(*restored, nullable_type).ok());
            for (size_t row = 0; row < 3; ++row) {
                if (restored->is_null_at(row)) continue;
                const auto result = (*restored)[row];
                const auto& actual = result.get<TYPE_FILE>();
                const auto& expected = full_value.get<TYPE_FILE>();
                for (size_t i = 0; i < 6; ++i) {
                    const bool present = i == 0 || (mask & (1 << (i - 1)));
                    EXPECT_EQ(actual[i], present ? expected[i] : Field());
                }
            }
            // Filling optional children cannot make an invalid live URI valid.
            uri.data[0] = const_cast<char*>("relative");
            uri.length[0] = 8;
            auto invalid = nullable_type->create_column();
            ASSERT_TRUE(nullable.read_column_from_orc(*invalid, view).ok());
            EXPECT_FALSE(validate_file_column(*invalid, nullable_type).ok());
        }
    }
}

TEST_F(DataTypeFileSerDeTest, OrcPrunedPresentChildrenAreNotTreatedAsAbsentOnDisk) {
    auto complete = orc::Type::buildTypeFromString("struct<uri:string,size:bigint>");
    auto selected = orc::Type::buildTypeFromString("struct<uri:string>");
    complete->setAttribute("doris.struct-type", "FILE");
    selected->setAttribute("doris.struct-type", "FILE");
    auto batch = selected->createRowBatch(1, *orc::getDefaultPool(), false, false);
    OrcDecodedColumnView view {.file_type = complete.get(),
                               .selected_type = selected.get(),
                               .batch = batch.get(),
                               .rows = 1};
    auto restored = type.create_column();
    restored->insert(value());
    EXPECT_FALSE(serde.read_column_from_orc(*restored, view).ok());
    EXPECT_EQ(restored->size(), 1);
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

TEST_F(DataTypeFileSerDeTest, ProtobufDefersValueValidationUntilAncestorNullsAreAvailable) {
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
    EXPECT_TRUE(validate_file_column(*restored, outer_type).ok());
    assert_cast<ColumnNullable&>(*restored).get_null_map_data()[0] = 0;
    EXPECT_FALSE(validate_file_column(*restored, outer_type).ok());
}

TEST_F(DataTypeFileSerDeTest, ArrowDefersValueValidationUntilAncestorNullsAreAvailable) {
    const auto outer_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {std::make_shared<DataTypeFile>()}, Strings {"payload"}));
    const auto outer_serde = outer_type->get_serde();
    auto source = outer_type->create_column();
    source->insert_default();
    source->insert(Field::create_field<TYPE_STRUCT>(Struct {value()}));
    const auto file_type = arrow::struct_(
            {arrow::field("uri", arrow::utf8()), arrow::field("offset", arrow::int64()),
             arrow::field("size", arrow::int64()), arrow::field("content_type", arrow::utf8()),
             arrow::field("checksum", arrow::utf8()), arrow::field("inline", arrow::binary())});
    std::unique_ptr<arrow::ArrayBuilder> builder;
    ASSERT_TRUE(arrow::MakeBuilder(arrow::default_memory_pool(),
                                   arrow::struct_({arrow::field("payload", file_type)}), &builder)
                        .ok());
    const auto timezone = cctz::utc_time_zone();
    ASSERT_TRUE(outer_serde->write_column_to_arrow(*source, nullptr, builder.get(), 0, 2, timezone)
                        .ok());
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder->Finish(&array).ok());
    auto restored = outer_type->create_column();
    ASSERT_TRUE(outer_serde->read_column_from_arrow(*restored, array.get(), 0, 2, timezone).ok());
    ASSERT_EQ(restored->size(), 2);
    EXPECT_TRUE(validate_file_column(*restored, outer_type).ok());
    assert_cast<ColumnNullable&>(*restored).get_null_map_data()[0] = 0;
    EXPECT_FALSE(validate_file_column(*restored, outer_type).ok());
}

TEST_F(DataTypeFileSerDeTest, OrcDefersValueValidationUntilAncestorNullsAreAvailable) {
    const auto outer_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {std::make_shared<DataTypeFile>()}, Strings {"payload"}));
    const auto outer_serde = outer_type->get_serde();
    auto source = outer_type->create_column();
    source->insert_default();
    source->insert(Field::create_field<TYPE_STRUCT>(Struct {value()}));
    auto orc_type = orc::Type::buildTypeFromString(
            "struct<payload:struct<uri:string,offset:bigint,size:bigint,content_type:string,"
            "checksum:string,inline:binary>>");
    const_cast<orc::Type*>(orc_type->getSubtype(0))->setAttribute("doris.struct-type", "FILE");
    auto batch = orc_type->createRowBatch(2, *orc::getDefaultPool(), false, false);
    Arena arena;
    ASSERT_TRUE(outer_serde
                        ->write_column_to_orc("UTC", *source, nullptr, batch.get(), 0, 2, arena,
                                              options)
                        .ok());
    // A reader must honor the outer STRUCT mask even if its FILE child is present.
    auto& outer = assert_cast<orc::StructVectorBatch&>(*batch);
    outer.fields[0]->notNull[0] = 1;
    const auto timezone = cctz::utc_time_zone();
    OrcDecodedColumnView view {.file_type = orc_type.get(),
                               .selected_type = orc_type.get(),
                               .batch = batch.get(),
                               .rows = 2,
                               .timezone = &timezone};
    auto restored = outer_type->create_column();
    ASSERT_TRUE(outer_serde->read_column_from_orc(*restored, view).ok());
    ASSERT_EQ(restored->size(), 2);
    EXPECT_TRUE(validate_file_column(*restored, outer_type).ok());
    assert_cast<ColumnNullable&>(*restored).get_null_map_data()[0] = 0;
    EXPECT_FALSE(validate_file_column(*restored, outer_type).ok());
}

TEST_F(DataTypeFileSerDeTest, ArrowRejectsInvalidInlineLengthsBeforeReadingPayload) {
    auto source = type.create_column();
    source->insert(value());
    auto arrow_type = arrow::struct_(
            {arrow::field("uri", arrow::utf8()), arrow::field("offset", arrow::int64()),
             arrow::field("size", arrow::int64()), arrow::field("content_type", arrow::utf8()),
             arrow::field("checksum", arrow::utf8()),
             arrow::field("inline", arrow::large_binary())});
    std::unique_ptr<arrow::ArrayBuilder> builder;
    ASSERT_TRUE(arrow::MakeBuilder(arrow::default_memory_pool(), arrow_type, &builder).ok());
    const auto timezone = cctz::utc_time_zone();
    ASSERT_TRUE(serde.write_column_to_arrow(*source, nullptr, builder.get(), 0, 1, timezone).ok());
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder->Finish(&array).ok());
    const uint8_t sentinel = 0;
    const std::array<std::array<int64_t, 2>, 4> invalid_offsets {
            {{0, int64_t {UINT32_MAX} + 1}, {0, -1}, {1, 0}, {INT64_MIN, INT64_MAX}}};
    for (const auto& offsets : invalid_offsets) {
        // Reject before allocating or touching payload bytes, including overflowing subtraction.
        array->data()->child_data[5] = arrow::ArrayData::Make(
                arrow::large_binary(), 1,
                {nullptr,
                 std::make_shared<arrow::Buffer>(reinterpret_cast<const uint8_t*>(offsets.data()),
                                                 sizeof(offsets)),
                 std::make_shared<arrow::Buffer>(&sentinel, int64_t {UINT32_MAX} + 1)});
        array = arrow::MakeArray(array->data());
        auto restored = type.create_column();
        EXPECT_FALSE(serde.read_column_from_arrow(*restored, array.get(), 0, 1, timezone).ok());
        EXPECT_EQ(restored->size(), 0);
    }
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
