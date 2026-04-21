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

#include "format/file_reader/new_plain_text_line_reader.h"

#include <gtest/gtest.h>

namespace doris {

// Base test class for text line reader tests
class PlainTextLineReaderTest : public testing::Test {
protected:
    // Helper function to verify line splitting results
    void verify_split_result(const std::string& input, const std::string& line_delim, bool keep_cr,
                             const std::vector<std::string>& expected_lines) {
        PlainTextLineReaderCtx ctx(line_delim, line_delim.size(), keep_cr);
        const auto* data = reinterpret_cast<const uint8_t*>(input.c_str());
        size_t pos = 0;
        size_t size = input.size();
        std::vector<std::string> actual_lines;

        while (pos < size) {
            ctx.refresh();
            const auto* line_end = ctx.read_line(data + pos, size - pos);
            if (!line_end) {
                actual_lines.emplace_back(reinterpret_cast<const char*>(data + pos), size - pos);
                break;
            }
            size_t line_len = line_end - (data + pos);
            actual_lines.emplace_back(reinterpret_cast<const char*>(data + pos), line_len);
            pos += line_len + ctx.line_delimiter_length();
        }

        ASSERT_EQ(expected_lines, actual_lines);
    }
};

// Test cases for PlainTextLineReaderCtx
TEST_F(PlainTextLineReaderTest, PlainTextBasic) {
    verify_split_result("line1\nline2\nline3", "\n", false, {"line1", "line2", "line3"});

    verify_split_result("line1\r\nline2\r\nline3", "\r\n", false, {"line1", "line2", "line3"});

    verify_split_result("line1\r\nline2\r\nline3", "\n", true, {"line1\r", "line2\r", "line3"});

    verify_split_result("line1\n\nline3", "\n", false, {"line1", "", "line3"});

    verify_split_result("line1||line2||line3", "||", false, {"line1", "line2", "line3"});
}

// Test class for CSV line reader with enclosure support
class EncloseCsvLineReaderTest : public testing::Test {
protected:
    // Helper function to verify CSV splitting results including column positions
    void verify_csv_split(const std::string& input, const std::string& line_delim,
                          const std::string& col_sep, char enclose, char escape, bool keep_cr,
                          const std::vector<std::string>& expected_lines,
                          const std::vector<std::vector<size_t>>& expected_col_positions) {
        EncloseCsvLineReaderCtx ctx(line_delim, line_delim.size(), col_sep, col_sep.size(), 10,
                                    enclose, escape, keep_cr);

        const auto* data = reinterpret_cast<const uint8_t*>(input.c_str());
        size_t pos = 0;
        size_t size = input.size();
        std::vector<std::string> actual_lines;
        std::vector<std::vector<size_t>> actual_col_positions;

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
    verify_csv_split("a,b,c\nd,e,f", "\n", ",", '"', '\\', false, {"a,b,c", "d,e,f"},
                     {{1, 3}, {1, 3}});

    verify_csv_split("\"a,x\",b,c\n\"d,y\",e,f", "\n", ",", '"', '\\', false,
                     {"\"a,x\",b,c", "\"d,y\",e,f"}, {{5, 7}, {5, 7}});

    verify_csv_split("\"a\"\"x\",b,c\n\"d\\\"y\",e,f", "\n", ",", '"', '\\', false,
                     {R"("a""x",b,c)", R"("d\"y",e,f)"}, {{6, 8}, {6, 8}});

    verify_csv_split("a||b||c\nd||e||f", "\n", "||", '"', '\\', false, {"a||b||c", "d||e||f"},
                     {{1, 4}, {1, 4}});
}

// Edge cases and corner scenarios
TEST_F(EncloseCsvLineReaderTest, EdgeCases) {
    verify_csv_split("\n\na,b,c", "\n", ",", '"', '\\', false, {"", "", "a,b,c"}, {{}, {}, {1, 3}});

    verify_csv_split("\"abc,def\nghi,jkl", "\n", ",", '"', '\\', false, {"\"abc,def\nghi,jkl"},
                     {{}});

    verify_csv_split("a,b\r\nc,d\ne,f", "\r\n", ",", '"', '\\', false, {"a,b", "c,d\ne,f"},
                     {{1}, {1, 5}});

    verify_csv_split(R"(\,\"\n,b,c)", "\n", ",", '"', '\\', false, {R"(\,\"\n,b,c)"}, {{1, 6, 8}});
}

TEST_F(EncloseCsvLineReaderTest, QuoteEscaping) {
    // Test multiple quoted fields with double-quote escaping in one line
    verify_csv_split(R"("hello ""world\n""","foo ""bar""","test ""quote"" here")", "\n", ",", '"',
                     '\\', false, {R"("hello ""world\n""","foo ""bar""","test ""quote"" here")"},
                     {{19, 33}});

    // Test JSON-like string with escaped quotes
    verify_csv_split(
            R"({""code"": ""100"", ""message"": ""query success"", ""data"": {""status"": ""1""}})",
            "\n", ",", '"', '\\', false,
            {R"({""code"": ""100"", ""message"": ""query success"", ""data"": {""status"": ""1""}})"},
            {{18, 50}});

    // Test custom enclose character
    verify_csv_split(R"({|code|: |100|, |message|: |query success|, |data|: {|status|: |1|}})",
                     "\n", ",", '|', '\\', false,
                     {R"({|code|: |100|, |message|: |query success|, |data|: {|status|: |1|}})"},
                     {{14, 42}});
}

// Regression test: enclose char is a prefix of the multi-char column separator.
// e.g. enclose=':', sep='::'.  A closing enclose followed immediately by the
// separator (:::) was previously misread as a double-quote escape (::) plus an
// orphaned ':', causing silent data corruption.
TEST_F(EncloseCsvLineReaderTest, EncloseIsPrefixOfSeparator) {
    // Row: 1:::alpha::beta:::ok
    //   field0 = "1"
    //   sep "::"
    //   field1 = ":alpha::beta:"  (enclose-wrapped; '::' inside is literal content)
    //   sep "::"
    //   field2 = "ok"
    // Expected column_sep_positions: [1, 16]
    verify_csv_split("1:::alpha::beta:::ok", "\n", "::", ':', 0, false, {"1:::alpha::beta:::ok"},
                     {{1, 16}});

    // Row: 2:::plain:::tail  (no separator inside the enclosed field)
    // Expected: [1, 10]
    verify_csv_split("2:::plain:::tail", "\n", "::", ':', 0, false, {"2:::plain:::tail"},
                     {{1, 10}});

    // Two rows together
    verify_csv_split("1:::alpha::beta:::ok\n2:::plain:::tail", "\n", "::", ':', 0, false,
                     {"1:::alpha::beta:::ok", "2:::plain:::tail"}, {{1, 16}, {1, 10}});
}

// Verify that a non-zero escape character does not interfere with the
// enclose-prefix-of-separator fix.  The _should_escape path short-circuits
// before _quote_escape is tested, so the two mechanisms are independent —
// this test guards against future refactoring breaking that invariant.
TEST_F(EncloseCsvLineReaderTest, EncloseIsPrefixOfSeparatorWithEscape) {
    // escape='\', enclose=':', sep='::'
    // Row: 1:::alpha\::beta:::ok
    //   '\:' inside the field — the ':' is escape-suppressed, does not set _quote_escape.
    //   The closing ':' at pos 15 is still correctly identified via the separator peek.
    // Expected sep positions: [1, 16]  (same as the no-escape case)
    verify_csv_split("1:::alpha\\::beta:::ok", "\n", "::", ':', '\\', false,
                     {"1:::alpha\\::beta:::ok"}, {{1, 16}});

    // Without an escaped colon inside the field (baseline).
    // Closing enclose is at pos 9; separator '::' starts at pos 10.
    verify_csv_split("1:::alpha:::ok", "\n", "::", ':', '\\', false, {"1:::alpha:::ok"}, {{1, 10}});
}

TEST_F(EncloseCsvLineReaderTest, MultiCharDelimiters) {
    // Test multi-character line delimiter
    verify_csv_split("a,b,c\r\n\nd,e,f", "\r\n\n", ",", '"', '\\', false, {"a,b,c", "d,e,f"},
                     {{1, 3}, {1, 3}});

    // Test multi-character column delimiter
    verify_csv_split("a|||b|||c\nd|||e|||f", "\n", "|||", '"', '\\', false,
                     {"a|||b|||c", "d|||e|||f"}, {{1, 5}, {1, 5}});

    // Test both multi-character line and column delimiters
    verify_csv_split("a|||b|||c\r\n\nd|||e|||f", "\r\n\n", "|||", '"', '\\', false,
                     {"a|||b|||c", "d|||e|||f"}, {{1, 5}, {1, 5}});

    verify_csv_split("\"a|||b\"|||c\r\n\n\"d|||e\"|||f", "\r\n\n", "|||", '"', '\\', false,
                     {"\"a|||b\"|||c", "\"d|||e\"|||f"}, {{7}, {7}});
}

} // namespace doris
