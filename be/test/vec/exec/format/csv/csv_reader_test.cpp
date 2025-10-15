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

#include "vec/exec/format/csv/csv_reader.h"

#include <gtest/gtest.h>

namespace doris::vectorized {

class PlainCsvTextFieldSplitterTest : public testing::Test {
protected:
    // Helper function to verify field splitting results
    void verify_field_split(const std::string& input, const std::string& separator,
                            const std::vector<std::string>& expected_fields) {
        PlainCsvTextFieldSplitter splitter(false, false, separator, separator.size());
        const Slice line(input.c_str(), input.size());
        std::vector<Slice> actual_slices;
        splitter.split_line(line, &actual_slices);
        std::vector<std::string> actual_fields;
        for (const auto& slice : actual_slices) {
            actual_fields.emplace_back(slice.data, slice.size);
        }
        ASSERT_EQ(expected_fields, actual_fields);
    }
};

TEST_F(PlainCsvTextFieldSplitterTest, BasicCommaSeparation) {
    verify_field_split("a,b,c", ",", {"a", "b", "c"});
    verify_field_split("1,2,3", ",", {"1", "2", "3"});
    verify_field_split("hello,world,test", ",", {"hello", "world", "test"});
}

TEST_F(PlainCsvTextFieldSplitterTest, ArrayHandlingWithComma) {
    verify_field_split("1,[1.0,2.0,3.0]", ",", {"1", "[1.0,2.0,3.0]"});
    verify_field_split("name,[a,b,c],age", ",", {"name", "[a,b,c]", "age"});
    verify_field_split("[start],middle,[end]", ",", {"[start]", "middle", "[end]"});
}

TEST_F(PlainCsvTextFieldSplitterTest, NotArrayHandling) {
    verify_field_split("1,'[1.0,2.0,3.0]'", ",", {"1", "'[1.0", "2.0", "3.0]'"});
    verify_field_split("prefix[value],other", ",", {"prefix[value]", "other"});
}

TEST_F(PlainCsvTextFieldSplitterTest, UnmatchedBrackets) {
    // Unmatched '[' should be treated as normal character
    verify_field_split("1,[unclosed,2,3", ",", {"1", "[unclosed", "2", "3"});
    verify_field_split("1,unclosed],2,3", ",", {"1", "unclosed]", "2", "3"});
}

TEST_F(PlainCsvTextFieldSplitterTest, OtherSingleCharSeparators) {
    verify_field_split("a|b|c", "|", {"a", "b", "c"});
    verify_field_split("a;b;c", ";", {"a", "b", "c"});
    verify_field_split("a\tb\tc", "\t", {"a", "b", "c"});
}

TEST_F(PlainCsvTextFieldSplitterTest, MultiCharSeparators) {
    verify_field_split("a|||b|||c", "|||", {"a", "b", "c"});
    verify_field_split("hello||world||test", "||", {"hello", "world", "test"});
}

} // namespace doris::vectorized
