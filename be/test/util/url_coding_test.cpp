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

#include "util/url_coding.h"

#include <gtest/gtest.h>

#include <sstream>
#include <string>
#include <vector>

namespace doris {

// Encode the input, then decode it again and check we are back where we started.
void test_url(const std::string& input, const std::string& expected_encoded) {
    std::string intermediate;
    url_encode(input, &intermediate);

    if (!expected_encoded.empty()) {
        EXPECT_EQ(intermediate, expected_encoded);
    }

    std::string output;
    EXPECT_TRUE(url_decode(intermediate, &output));
    EXPECT_EQ(input, output);
}

void test_base64(const std::string& input, const std::string& expected_encoded) {
    std::string intermediate;
    base64_encode(input, &intermediate);

    if (!expected_encoded.empty()) {
        EXPECT_EQ(intermediate, expected_encoded);
    }

    std::string output;
    EXPECT_TRUE(base64_decode(intermediate, &output));
    EXPECT_EQ(input, output);
}

TEST(UrlCodingTest, Basic) {
    std::string input = "ABCDEFGHIJKLMNOPQRSTUWXYZ1234567890~!@#$%^&*()<>?,./:\";'{}|[]\\_+-=";
    test_url(input, "");
}

TEST(UrlCodingTest, BlankString) {
    test_url("", "");
}

TEST(UrlCodingTest, PathSeparators) {
    test_url("/home/doris/directory/", "%2Fhome%2Fdoris%2Fdirectory%2F");
}

TEST(UrlCodingTest, Spaces) {
    std::string output;
    EXPECT_TRUE(url_decode("my+db", &output));
    EXPECT_EQ(output, "my db");
    EXPECT_TRUE(url_decode("my%20db", &output));
    EXPECT_EQ(output, "my db");
}

TEST(UrlCodingTest, MalformedEscapeIsRejected) {
    std::string output;
    // A '%' must be followed by exactly two hexadecimal digits.
    EXPECT_FALSE(url_decode("prod%zzbackup", &output));
    EXPECT_FALSE(url_decode("a%1gb", &output));
    EXPECT_FALSE(url_decode("a%%20b", &output));
    // A '%' at, or one character from, the end of the input.
    EXPECT_FALSE(url_decode("mydb%", &output));
    EXPECT_FALSE(url_decode("mydb%2", &output));
    // Both digit cases are accepted.
    EXPECT_TRUE(url_decode("%2d%2D", &output));
    EXPECT_EQ(output, "--");
}

TEST(Base64Test, Basic) {
    test_base64("a", "YQ==");
    test_base64("ab", "YWI=");
    test_base64("abc", "YWJj");
    test_base64("abcd", "YWJjZA==");
    test_base64("abcde", "YWJjZGU=");
    test_base64("abcdef", "YWJjZGVm");
}

TEST(HtmlEscapingTest, Basic) {
    std::string before = "<html><body>&amp";
    std::stringstream after;
    escape_for_html(before, &after);
    EXPECT_EQ(after.str(), "&lt;html&gt;&lt;body&gt;&amp;amp");
}

} // namespace doris
