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

    // Neither character after the '%' is a hexadecimal digit.
    EXPECT_FALSE(url_decode("prod%zzbackup", &output));
    EXPECT_FALSE(url_decode("a%%20b", &output));

    // Only one of the two is. A stream parse accepts these and silently swallows the
    // character that follows, which is what this check exists to stop.
    EXPECT_FALSE(url_decode("a%1gb", &output));
    EXPECT_FALSE(url_decode("%z1", &output));
    EXPECT_FALSE(url_decode("%1z", &output));

    // Signs and spaces are what a stream parse is most willing to accept.
    EXPECT_FALSE(url_decode("%+1", &output));
    EXPECT_FALSE(url_decode("%-1", &output));
    EXPECT_FALSE(url_decode("% 1", &output));
    EXPECT_FALSE(url_decode("%1 ", &output));

    // A '%' at, or one character from, the end of the input.
    EXPECT_FALSE(url_decode("mydb%", &output));
    EXPECT_FALSE(url_decode("mydb%2", &output));
    EXPECT_FALSE(url_decode("100%", &output));
}

TEST(UrlCodingTest, WellFormedEscapesAreAccepted) {
    std::string output;

    // Both digit cases.
    EXPECT_TRUE(url_decode("%2d%2D", &output));
    EXPECT_EQ(output, "--");

    // The whole hexadecimal alphabet, upper and lower.
    EXPECT_TRUE(url_decode("%0a%0A%bf%BF%7e%7E", &output));
    EXPECT_EQ(output, "\n\n\xbf\xbf~~");

    // A NUL is a legal escape and must not end the string early.
    EXPECT_TRUE(url_decode("a%00b", &output));
    EXPECT_EQ(output, std::string("a\0b", 3));

    // Multi byte UTF-8, one escape per byte.
    EXPECT_TRUE(url_decode("%E4%B8%AD", &output));
    EXPECT_EQ(output, "\xe4\xb8\xad");

    // '+' still means a space, and an escaped space still means a space.
    EXPECT_TRUE(url_decode("my+db", &output));
    EXPECT_EQ(output, "my db");
    EXPECT_TRUE(url_decode("my%20db", &output));
    EXPECT_EQ(output, "my db");

    // Nothing to decode.
    EXPECT_TRUE(url_decode("mydb", &output));
    EXPECT_EQ(output, "mydb");
    EXPECT_TRUE(url_decode("", &output));
    EXPECT_EQ(output, "");
}

TEST(UrlCodingTest, EncodeDecodeRoundTrip) {
    // Every byte value survives a round trip through url_encode.
    std::string all;
    for (int i = 1; i < 256; ++i) {
        all.push_back(static_cast<char>(i));
    }

    std::string encoded;
    url_encode(all, &encoded);

    std::string decoded;
    EXPECT_TRUE(url_decode(encoded, &decoded));
    EXPECT_EQ(all, decoded);
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
