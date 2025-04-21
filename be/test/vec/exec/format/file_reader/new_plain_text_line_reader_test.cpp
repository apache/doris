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

// be/test/vec/exec/format/file_reader/new_plain_text_line_reader_test.cpp

#include "vec/exec/format/file_reader/new_plain_text_line_reader.h"

#include <gtest/gtest.h>

namespace doris {

// Base test class for text line reader tests
class PlainTextLineReaderTest : public testing::Test {
protected:
    // Helper function to verify line splitting results
    void verify_split_result(TextLineReaderContextIf* ctx, const std::string& input,
                             const std::vector<std::string>& expected_lines) {
        const auto* data = reinterpret_cast<const uint8_t*>(input.c_str());
        size_t pos = 0;
        size_t size = input.size();
        std::vector<std::string> actual_lines;

        // Split input into lines and collect results
        while (pos < size) {
            ctx->refresh();
            const uint8_t* line_end = ctx->read_line(data + pos, size - pos);
            if (!line_end) {
                // Handle the last line when no delimiter is found
                actual_lines.emplace_back(reinterpret_cast<const char*>(data + pos), size - pos);
                break;
            }
            size_t line_len = line_end - (data + pos);
            actual_lines.emplace_back(reinterpret_cast<const char*>(data + pos), line_len);
            pos += line_len + ctx->line_delimiter_length();
        }

        ASSERT_EQ(expected_lines, actual_lines);
    }
};

// Test cases for PlainTextLineReaderCtx
TEST_F(PlainTextLineReaderTest, PlainTextBasic) {
    // Test case 1: Default newline delimiter (\n)
    {
        PlainTextLineReaderCtx ctx("\n", 1, false);
        verify_split_result(&ctx, "line1\nline2\nline3", {"line1", "line2", "line3"});
    }

    // Test case 2: CRLF delimiter (\r\n)
    {
        PlainTextLineReaderCtx ctx("\r\n", 2, false);
        verify_split_result(&ctx, "line1\r\nline2\r\nline3", {"line1", "line2", "line3"});
    }

    // Test case 3: Keep CR option enabled
    {
        PlainTextLineReaderCtx ctx("\n", 1, true);
        verify_split_result(&ctx, "line1\r\nline2\r\nline3", {"line1\r", "line2\r", "line3"});
    }

    // Test case 4: Empty lines
    {
        PlainTextLineReaderCtx ctx("\n", 1, false);
        verify_split_result(&ctx, "line1\n\nline3", {"line1", "", "line3"});
    }

    // Test case 5: Custom delimiter
    {
        PlainTextLineReaderCtx ctx("||", 2, false);
        verify_split_result(&ctx, "line1||line2||line3", {"line1", "line2", "line3"});
    }
}

// Test class for CSV line reader with enclosure support
class EncloseCsvLineReaderTest : public testing::Test {
protected:
    // Helper function to verify CSV splitting results including column positions
    void verify_csv_split(const std::string& input, const std::string& line_delim,
                          const std::string& col_sep, char enclose, char escape, bool keep_cr,
                          const std::vector<std::string>& expected_lines,
                          const std::vector<std::vector<size_t>>& expected_col_positions) {
        EncloseCsvLineReaderContext ctx(line_delim, line_delim.size(), col_sep, col_sep.size(), 10,
                                        enclose, escape, keep_cr);

        const auto* data = reinterpret_cast<const uint8_t*>(input.c_str());
        size_t pos = 0;
        size_t size = input.size();
        std::vector<std::string> actual_lines;
        std::vector<std::vector<size_t>> actual_col_positions;

        // Process input line by line
        while (pos < size) {
            ctx.refresh();
            const uint8_t* line_end = ctx.read_line(data + pos, size - pos);
            if (!line_end) {
                actual_lines.emplace_back(reinterpret_cast<const char*>(data + pos), size - pos);
                actual_col_positions.push_back(ctx.column_sep_positions());
                break;
            }
            size_t line_len = line_end - (data + pos);
            actual_lines.emplace_back(reinterpret_cast<const char*>(data + pos), line_len);
            actual_col_positions.push_back(ctx.column_sep_positions());
            pos += line_len + ctx.line_delimiter_length();
        }

        ASSERT_EQ(expected_lines, actual_lines);
        ASSERT_EQ(expected_col_positions, actual_col_positions);
    }
};

// Basic CSV format test cases
TEST_F(EncloseCsvLineReaderTest, CsvBasic) {
    // Test case 1: Simple CSV format
    {
        verify_csv_split("a,b,c\nd,e,f",              // input
                         "\n", ",", '"', '\\', false, // config
                         {"a,b,c", "d,e,f"},          // expected lines
                         {{1, 3}, {1, 3}}             // expected column positions
        );
    }

    // Test case 2: Fields with enclosure
    {
        verify_csv_split("\"a,x\",b,c\n\"d,y\",e,f",     // input
                         "\n", ",", '"', '\\', false,    // config
                         {"\"a,x\",b,c", "\"d,y\",e,f"}, // expected lines
                         {{5, 7}, {5, 7}}                // expected column positions
        );
    }

    // Test case 3: Escaped quotes
    {
        verify_csv_split("\"a\"\"x\",b,c\n\"d\\\"y\",e,f", // input with both escape types
                         "\n", ",", '"', '\\', false, {R"("a""x",b,c)", R"("d\"y",e,f)"},
                         {{6, 8}, {7, 9}});
    }

    // Test case 4: Custom column separator
    {
        verify_csv_split("a||b||c\nd||e||f",           // input
                         "\n", "||", '"', '\\', false, // config
                         {"a||b||c", "d||e||f"},       // expected lines
                         {{2, 5}, {2, 5}}              // expected column positions
        );
    }
}

// Edge cases and corner scenarios
TEST_F(EncloseCsvLineReaderTest, EdgeCases) {
    // Test case 1: Empty lines
    {
        verify_csv_split("\n\na,b,c",                 // input
                         "\n", ",", '"', '\\', false, // config
                         {"", "", "a,b,c"},           // expected lines
                         {{}, {}, {1, 3}}             // expected column positions
        );
    }

    // Test case 2: Unclosed quotes
    {
        verify_csv_split("\"abc,def\nghi,jkl",        // input
                         "\n", ",", '"', '\\', false, // config
                         {"\"abc,def", "ghi,jkl"},    // expected lines
                         {{3}, {3}}                   // expected column positions
        );
    }

    // Test case 3: Mixed delimiters
    {
        verify_csv_split("a,b\r\nc,d\ne,f",             // input
                         "\r\n", ",", '"', '\\', false, // config
                         {"a,b", "c,d\ne,f"},           // expected lines
                         {{1}, {1, 3}}                  // expected column positions
        );
    }

    // Test case 4: Escape character edge cases
    {
        verify_csv_split(R"(\,\"\n,b,c)",             // input
                         "\n", ",", '"', '\\', false, // config
                         {R"(\,\"\n,b,c)"},           // expected lines
                         {{9, 11}}                    // expected column positions
        );
    }
}

} // namespace doris
