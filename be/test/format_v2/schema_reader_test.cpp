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

#include "format_v2/schema_reader.h"

#include <gtest/gtest.h>

#include <fstream>

#include "format_v2/column_data.h"
#include "gen_cpp/PlanNodes_types.h"
#include "io/io_common.h"
#include "testutil/scoped_temp_dir.h"

namespace doris::format {

class SchemaInferenceV2Test : public testing::Test {
protected:
    SchemaInferenceV2Test() {
        params.__set_file_type(TFileType::FILE_LOCAL);
        params.__set_format_type(TFileFormatType::FORMAT_CSV_PLAIN);
        TFileTextScanRangeParams text;
        text.__set_column_separator(",");
        text.__set_line_delimiter("\n");
        attributes.__set_text_params(text);
    }
    Status infer(const std::string& data, int64_t offset = 0, bool unknown_size = false) {
        const auto path = (temp.path() / "input").string();
        {
            std::ofstream out(path);
            out << data;
        }
        params.__set_file_attributes(attributes);
        TFileRangeDesc range;
        range.__set_path(path);
        range.__set_start_offset(offset);
        range.__set_size(unknown_size ? -1 : static_cast<int64_t>(data.size()));
        if (!unknown_size) {
            range.__set_file_size(data.size());
        }
        schema.clear();
        return fetch_file_schema(params, range, std::make_shared<io::IOContext>(), nullptr,
                                 &schema);
    }
    test::ScopedTempDirectory temp {"schema_v2"};
    TFileScanRangeParams params;
    TFileAttributes attributes;
    std::vector<ColumnDefinition> schema;
};

TEST_F(SchemaInferenceV2Test, CsvHeadersAreNotSkippedAndTypesRemainStrings) {
    attributes.__set_header_type("CSV_WITH_NAMES_AND_TYPES");
    attributes.text_params.__set_enclose('"');
    const auto status = infer("\xEF\xBB\xBF\"first,name\",second\nint,string\n1,value\n");
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(schema.size(), 2);
    EXPECT_EQ(schema[0].name, "first,name");
    EXPECT_EQ(schema[1].name, "second");
    EXPECT_EQ(schema[0].type->get_name(), "Nullable(String)");
}

TEST_F(SchemaInferenceV2Test, TextUsesEscapedMulticharacterSeparators) {
    params.__set_format_type(TFileFormatType::FORMAT_TEXT);
    attributes.text_params.__set_column_separator("||");
    attributes.text_params.__set_escape('\\');
    ASSERT_TRUE(infer("a\\||b||c\n").ok());
    ASSERT_EQ(schema.size(), 2);
    EXPECT_EQ(schema[1].name, "c2");
}

TEST_F(SchemaInferenceV2Test, TextSchemaRetainsUtf8Validation) {
    params.__set_format_type(TFileFormatType::FORMAT_TEXT);
    EXPECT_FALSE(infer("\xFF,x\n").ok());
    attributes.__set_enable_text_validate_utf8(false);
    EXPECT_TRUE(infer("\xFF,x\n").ok());
}

TEST_F(SchemaInferenceV2Test, JsonSchemaAcceptsLargeIntegersWithNumbersAsStrings) {
    params.__set_format_type(TFileFormatType::FORMAT_JSON);
    attributes.__set_num_as_string(true);
    const auto status = infer(R"({"value":170141183460469231731687303715884105727})");
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(schema.size(), 1);
    EXPECT_EQ(schema[0].name, "value");
}

TEST_F(SchemaInferenceV2Test, JsonRootAndPathsPreserveColumnNames) {
    params.__set_format_type(TFileFormatType::FORMAT_JSON);
    attributes.__set_json_root("$.rows");
    attributes.__set_strip_outer_array(true);
    attributes.__set_jsonpaths(R"(["$.id","$.nested.name"])");
    ASSERT_TRUE(infer(R"({"rows":[{"id":1,"nested":{"name":"value"}}]})").ok());
    ASSERT_EQ(schema.size(), 2);
    EXPECT_EQ(schema[0].name, "id");
    EXPECT_EQ(schema[1].name, "name");
}

TEST_F(SchemaInferenceV2Test, JsonCanInferAnUnknownSizeFile) {
    params.__set_format_type(TFileFormatType::FORMAT_JSON);
    ASSERT_TRUE(infer(R"({"name":"value"})", 0, true).ok());
    ASSERT_EQ(schema.size(), 1);
    EXPECT_EQ(schema[0].name, "name");
}

TEST_F(SchemaInferenceV2Test, JsonRejectsMalformedInputAndInvalidShape) {
    params.__set_format_type(TFileFormatType::FORMAT_JSON);
    for (const auto* data : {"", "{", "1", "[]", "[{}]"}) {
        EXPECT_FALSE(infer(data).ok()) << data;
    }
    attributes.__set_strip_outer_array(true);
    EXPECT_FALSE(infer("[]").ok());
    EXPECT_FALSE(infer("{}").ok());
    attributes.__set_strip_outer_array(false);
    attributes.__set_json_root("$.missing");
    EXPECT_FALSE(infer(R"({"name":"value"})").ok());
}

TEST_F(SchemaInferenceV2Test, CsvRejectsEmptyFirstLineAndNonzeroOffset) {
    EXPECT_FALSE(infer("").ok());
    const auto status = infer("\n1,value\n");
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("The first line is empty, can not parse column numbers"),
              std::string::npos)
            << status;
    EXPECT_FALSE(infer("1,value\n", 1).ok());
}

TEST_F(SchemaInferenceV2Test, EmptyLzopRetainsTheSchemaRpcError) {
    // A valid LZOP header (no filename, zero timestamps), Adler-32, and an end-of-stream block.
    const uint8_t stream[] = {0x89, 0x4c, 0x5a, 0x4f, 0x00, 0x0d, 0x0a, 0x1a, 0x0a, 0x10, 0x30,
                              0x20, 0xa0, 0x09, 0x40, 0x01, 0x05, 0x00, 0x00, 0x00, 0x01, 0x00,
                              0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                              0x00, 0x1d, 0x01, 0x01, 0x51, 0x00, 0x00, 0x00, 0x00};
    params.__set_format_type(TFileFormatType::FORMAT_CSV_LZOP);
    for (const auto* header : {"", "csv_with_names", "csv_with_names_and_types"}) {
        attributes.__set_header_type(header);
        const auto status =
                infer(std::string(reinterpret_cast<const char*>(stream), sizeof(stream)));
        EXPECT_FALSE(status.ok());
        const auto expected = std::string("The first line is empty, can not parse column ") +
                              (*header == '\0' ? "numbers" : "names");
        EXPECT_NE(status.to_string().find(expected), std::string::npos) << status;
    }
}

TEST(SchemaReaderV2Test, InfersTextAndJsonWithoutPlannedSlots) {
    test::ScopedTempDirectory temp("schema_v2");
    struct Case {
        TFileFormatType::type format;
        std::string data;
        std::vector<std::string> names;
    };
    const std::vector<Case> cases {
            {.format = TFileFormatType::FORMAT_CSV_PLAIN,
             .data = "1,value\n",
             .names = {"c1", "c2"}},
            {.format = TFileFormatType::FORMAT_TEXT, .data = "1,value\n", .names = {"c1", "c2"}},
            {.format = TFileFormatType::FORMAT_JSON,
             .data = R"({"id":1,"name":"value"})",
             .names = {"id", "name"}}};
    for (const auto& c : cases) {
        const auto path = (temp.path() / "input").string();
        {
            std::ofstream out(path);
            out << c.data;
        }
        TFileScanRangeParams params;
        params.__set_format_type(c.format);
        params.__set_file_type(TFileType::FILE_LOCAL);
        TFileAttributes attributes;
        TFileTextScanRangeParams text;
        text.__set_column_separator(",");
        text.__set_line_delimiter("\n");
        attributes.__set_text_params(text);
        params.__set_file_attributes(attributes);
        TFileRangeDesc range;
        range.__set_path(path);
        range.__set_start_offset(0);
        range.__set_size(c.data.size());
        range.__set_file_size(c.data.size());
        std::vector<ColumnDefinition> schema;
        ASSERT_TRUE(fetch_file_schema(params, range, std::make_shared<io::IOContext>(), nullptr,
                                      &schema)
                            .ok());
        ASSERT_EQ(schema.size(), c.names.size());
        for (size_t i = 0; i < schema.size(); ++i) {
            EXPECT_EQ(schema[i].name, c.names[i]);
            EXPECT_EQ(schema[i].type->get_name(), "Nullable(String)");
        }
    }
}

} // namespace doris::format
