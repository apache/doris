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

class SplitLimitHelper {
public:
    static std::vector<std::string> split(const std::string& input, char sep, size_t limit,
                                          char escape_char = 0) {
        HiveTextFieldSplitter splitter(false, false, std::string(1, sep), 1, 0, escape_char);
        splitter.set_split_limit(limit);
        Slice line(input.data(), input.size());
        std::vector<Slice> out;
        splitter.do_split(line, &out);
        std::vector<std::string> res;
        for (const auto& s : out) {
            res.emplace_back(s.data, s.size);
        }
        return res;
    }
};

TEST_F(HiveTextFieldSplitterTest, SplitLimitStopsEarly) {
    EXPECT_EQ((std::vector<std::string> {"a", "b"}), SplitLimitHelper::split("a,b,c,d,e", ',', 2));
    EXPECT_EQ((std::vector<std::string> {"a", "b", "c"}),
              SplitLimitHelper::split("a,b,c,d,e", ',', 3));
    EXPECT_EQ((std::vector<std::string> {"a"}), SplitLimitHelper::split("a,b,c", ',', 1));
}

TEST_F(HiveTextFieldSplitterTest, SplitLimitAtOrAboveFieldCount) {
    EXPECT_EQ((std::vector<std::string> {"a", "b", "c"}), SplitLimitHelper::split("a,b,c", ',', 3));
    EXPECT_EQ((std::vector<std::string> {"a", "b", "c"}),
              SplitLimitHelper::split("a,b,c", ',', 10));
    EXPECT_EQ((std::vector<std::string> {"a", "b", "c"}), SplitLimitHelper::split("a,b,c", ',', 0));
}

TEST_F(HiveTextFieldSplitterTest, SplitLimitEdgeCases) {
    EXPECT_EQ((std::vector<std::string> {""}), SplitLimitHelper::split("", ',', 3));
    EXPECT_EQ((std::vector<std::string> {"a", ""}), SplitLimitHelper::split("a,,,b", ',', 2));
    EXPECT_EQ((std::vector<std::string> {"x", "y"}), SplitLimitHelper::split("x,y,", ',', 2));
}

TEST_F(HiveTextFieldSplitterTest, SplitLimitIgnoredWhenEscapeConfigured) {
    EXPECT_EQ((std::vector<std::string> {"a\\,b", "c", "d"}),
              SplitLimitHelper::split("a\\,b,c,d", ',', 2, '\\'));
}

} // namespace doris
