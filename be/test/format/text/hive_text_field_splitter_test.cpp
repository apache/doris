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

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "format/text/text_reader.h"

namespace doris {

class HiveTextFieldSplitterTest : public testing::Test {
protected:
    void verify_field_split(const std::string& input, const std::string& delimiter,
                            const std::vector<std::string>& expected_fields, char escape_char = 0) {
        HiveTextFieldSplitter splitter(false, false, delimiter, delimiter.size(), 0, escape_char);
        Slice line(input.data(), input.size());
        std::vector<Slice> splitted_values;

        splitter.do_split(line, &splitted_values);

        ASSERT_EQ(expected_fields.size(), splitted_values.size())
                << "Input: " << input << ", Delimiter: " << delimiter;

        for (size_t i = 0; i < expected_fields.size(); ++i) {
            std::string actual(splitted_values[i].data, splitted_values[i].size);
            EXPECT_EQ(expected_fields[i], actual) << "Field " << i << " mismatch. Input: " << input
                                                  << ", Delimiter: " << delimiter;
        }
    }
};

// Test single character delimiter (basic functionality)
TEST_F(HiveTextFieldSplitterTest, SingleCharDelimiter) {
    verify_field_split("a,b,c", ",", {"a", "b", "c"});
    verify_field_split("1|2|3|4", "|", {"1", "2", "3", "4"});
    verify_field_split("", ",", {""});
    verify_field_split(",", ",", {"", ""});
    verify_field_split("a,", ",", {"a", ""});
    verify_field_split(",b", ",", {"", "b"});
}

// Test multi-character delimiter (core functionality for MultiDelimitSerDe)
TEST_F(HiveTextFieldSplitterTest, MultiCharDelimiter) {
    verify_field_split("a||b||c", "||", {"a", "b", "c"});
    verify_field_split("1|+|2|+|3", "|+|", {"1", "2", "3"});
    verify_field_split("field1|+|field2|+|field3", "|+|", {"field1", "field2", "field3"});

    verify_field_split("", "||", {""});
    verify_field_split("||", "||", {"", ""});
    verify_field_split("a||", "||", {"a", ""});
    verify_field_split("||b", "||", {"", "b"});
}

// Test overlapping patterns in delimiter - these are the problematic cases
TEST_F(HiveTextFieldSplitterTest, OverlappingPatterns) {
    verify_field_split("ab\\ababab", "abab", {"ab\\", "ab"});

    verify_field_split("aaaaaaa", "aaa", {"", "", "a"});

    verify_field_split("abcabcabc", "abcabc", {"", "abc"});

    verify_field_split("ababababab", "abab", {"", "", "ab"});
}

// Test escape character functionality
TEST_F(HiveTextFieldSplitterTest, EscapeCharacter) {
    verify_field_split("a\\,b,c", ",", {"a\\,b", "c"}, '\\');
    verify_field_split(R"(a\\,b)", ",", {R"(a\\)", "b"}, '\\');
    verify_field_split("a\\||b||c", "||", {"a\\||b", "c"}, '\\');
    verify_field_split(R"(a\\||b)", "||", {R"(a\\)", "b"}, '\\');
    verify_field_split("field1\\|+|field2|+|field3", "|+|", {"field1\\|+|field2", "field3"}, '\\');
}

// Test real-world scenarios
TEST_F(HiveTextFieldSplitterTest, RealWorldScenarios) {
    verify_field_split("1|+|100|+|test1", "|+|", {"1", "100", "test1"});
    verify_field_split("user@domain.com|+|John Doe|+|Manager", "|+|",
                       {"user@domain.com", "John Doe", "Manager"});
    verify_field_split("|+||+|", "|+|", {"", "", ""});
    verify_field_split("a|+||+|c", "|+|", {"a", "", "c"});
}

// Single-char split takes a memchr fast path when escape_char == 0. These cases
// exercise that path's boundaries: the Hive default \x01 separator, long fields
// that span SIMD chunks, consecutive/leading/trailing separators, and multi-byte
// UTF-8 payloads that must not be mistaken for separators.
TEST_F(HiveTextFieldSplitterTest, SingleCharMemchrFastPath) {
    const char no_escape = 0;
    verify_field_split(std::string("a\x01") + "b\x01" + "c", std::string("\x01"), {"a", "b", "c"},
                       no_escape);
    std::string long_a(100, 'x');
    std::string long_b(70, 'y');
    verify_field_split(long_a + "," + long_b, ",", {long_a, long_b}, no_escape);
    verify_field_split("a,,,b", ",", {"a", "", "", "b"}, no_escape);
    verify_field_split(",,,", ",", {"", "", "", ""}, no_escape);
    verify_field_split("中文,字段,测试", ",", {"中文", "字段", "测试"}, no_escape);
    verify_field_split("no_delims_here", ",", {"no_delims_here"}, no_escape);
    verify_field_split("trailing,", ",", {"trailing", ""}, no_escape);
}

// When escape_char == 0, a backslash is data and must NOT suppress a separator.
TEST_F(HiveTextFieldSplitterTest, SingleCharNoEscapeTreatsBackslashAsData) {
    const char no_escape = 0;
    verify_field_split("a\\,b", ",", {"a\\", "b"}, no_escape);
    verify_field_split("x\\", ",", {"x\\"}, no_escape);
}

// Multi-char KMP path: the cached prefix table splits overlapping-prefix separators
// correctly across many rows (table now built once in ctor and reused per call).
TEST_F(HiveTextFieldSplitterTest, MultiCharCachedNextTableReuse) {
    HiveTextFieldSplitter splitter(false, false, "|+|", 3, 0, 0);
    for (int iter = 0; iter < 3; ++iter) {
        std::vector<Slice> out;
        std::string input = "a|+|b|+|c";
        Slice line(input.data(), input.size());
        splitter.do_split(line, &out);
        ASSERT_EQ(3u, out.size()) << "iteration " << iter;
        EXPECT_EQ("a", std::string(out[0].data, out[0].size));
        EXPECT_EQ("b", std::string(out[1].data, out[1].size));
        EXPECT_EQ("c", std::string(out[2].data, out[2].size));
    }
}

// Escape counterpart: escape_char != 0 takes the byte-by-byte slow path; a separator
// preceded by the escape char is data. First two inputs are byte-identical to
// SingleCharNoEscapeTreatsBackslashAsData but split differently — pins path divergence.
TEST_F(HiveTextFieldSplitterTest, SingleCharEscapeSuppressesSeparator) {
    verify_field_split("a\\,b", ",", {"a\\,b"}, '\\');
    verify_field_split("x\\", ",", {"x\\"}, '\\');
    verify_field_split("\\,x", ",", {"\\,x"}, '\\');
    verify_field_split("a\\,\\,b", ",", {"a\\,\\,b"}, '\\');
}

// Multi-char KMP escape path: escape char before a matched separator suppresses that
// boundary via the curpos-1 lookback; pins the boundary conditions.
TEST_F(HiveTextFieldSplitterTest, MultiCharEscapeSuppressesSeparator) {
    verify_field_split("\\||x||y", "||", {"\\||x", "y"}, '\\');
    verify_field_split("a\\|+||+|b", "|+|", {"a\\|+|", "b"}, '\\');
}

} // namespace doris
