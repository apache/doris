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
#include <rapidjson/document.h>

#include "core/arena.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_file.h"
#include "core/data_type_serde/data_type_nullable_serde.h"
#include "core/string_buffer.hpp"
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
};

TEST_F(DataTypeFileSerDeTest, TextOutputPreservesOpaqueMetadataBytes) {
    for (const size_t field : {0, 4}) {
        auto input = value();
        const std::string bytes = (field == 0 ? std::string("urn:") : std::string("ETAG:")) +
                                  std::string("\xff\0x", 3);
        input.get<TYPE_FILE>()[field] = Field::create_field<TYPE_STRING>(bytes);
        auto source = type.create_column();
        source->insert(input);
        for (const bool format : {false, true}) {
            const auto encoded = text(*source, 0, format);
            rapidjson::Document document;
            document.Parse(encoded.data(), encoded.size());
            ASSERT_FALSE(document.HasParseError());
            const auto& output = document[field == 0 ? "uri" : "checksum"];
            EXPECT_EQ(std::string(output.GetString(), output.GetStringLength()), bytes);
        }
    }
}

TEST_F(DataTypeFileSerDeTest, TextOutputPreservesLongMetadata) {
    auto input = value();
    auto& fields = input.get<TYPE_FILE>();
    fields[0] = Field::create_field<TYPE_STRING>("s3://bucket/object?versionId=" +
                                                 std::string(70000, 'a'));
    fields[3] = Field::create_field<TYPE_STRING>("application/x-example;profile=" +
                                                 std::string(70000, 'a'));
    fields[4] = Field::create_field<TYPE_STRING>("ETAG:" + std::string(70000, 'A'));
    auto source = type.create_column();
    source->insert(input);
    for (const bool format : {false, true}) {
        const auto encoded = text(*source, 0, format);
        rapidjson::Document document;
        document.Parse(encoded.data(), encoded.size());
        ASSERT_FALSE(document.HasParseError());
        for (const size_t field : {0, 3, 4}) {
            const auto& output = document[type.get_element_names()[field].c_str()];
            EXPECT_EQ(std::string(output.GetString(), output.GetStringLength()),
                      fields[field].get<TYPE_STRING>());
        }
    }
}

TEST_F(DataTypeFileSerDeTest, PublicJsonbOutputPreservesInlineNullEmptyAndBinary) {
    auto source = type.create_column();
    source->insert(value());
    const std::vector<std::pair<std::string, std::string>> fixtures = {
            {"", ""},
            {"f", "Zg=="},
            {"fo", "Zm8="},
            {"foo", "Zm9v"},
            {std::string("\0\xff\x80", 3), "AP+A"}};
    for (const auto& [bytes, encoded] : fixtures) {
        source->insert(value(&bytes));
    }
    for (size_t row = 0; row < source->size(); ++row) {
        JsonbWriter writer;
        ASSERT_TRUE(serde.serialize_column_to_jsonb(*source, row, writer, {}).ok());
        const auto json = JsonbToJson::jsonb_to_json_string(writer.getOutput()->getBuffer(),
                                                            writer.getOutput()->getSize());
        EXPECT_EQ(json, public_json(row == 0 ? "null" : "\"" + fixtures[row - 1].second + "\""));
    }
}

TEST_F(DataTypeFileSerDeTest, PublicOutputIncludesInlineAndBinaryUsesLengthEncodedJson) {
    const std::string bytes("\0\xff", 2);
    auto column = type.create_column();
    column->insert(value(&bytes));
    EXPECT_EQ(text(*column, 0, false), public_json("\"AP8=\""));
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
    const std::vector<std::pair<std::string, std::string>> fixtures = {
            {"", ""},
            {"f", "Zg=="},
            {"fo", "Zm8="},
            {"foo", "Zm9v"},
            {std::string("\0\xff\x80", 3), "AP+A"}};
    EXPECT_EQ(text(*column, 0, true), public_json());
    for (const auto& [bytes, encoded] : fixtures) {
        column->insert(value(&bytes));
        EXPECT_EQ(text(*column, column->size() - 1, true), public_json("\"" + encoded + "\""));
    }
    const std::string large(3 * 4096 + 1, 'f');
    column->insert(value(&large));
    std::string encoded;
    for (size_t i = 0; i < 4096; ++i) encoded += "ZmZm";
    encoded += "Zg==";
    EXPECT_EQ(text(*column, column->size() - 1, true), public_json("\"" + encoded + "\""));
    auto constant = ColumnConst::create(column->clone_resized(1), 3);
    EXPECT_EQ(text(*constant, 2, true), public_json());
}

TEST_F(DataTypeFileSerDeTest, ProtobufNotSupported) {
    auto column = type.create_column();
    column->insert(value());
    PValues values;
    EXPECT_EQ(serde.write_column_to_pb(*column, values, 0, 1).code(),
              ErrorCode::NOT_IMPLEMENTED_ERROR);
    EXPECT_EQ(serde.read_column_from_pb(*column, values).code(), ErrorCode::NOT_IMPLEMENTED_ERROR);
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
