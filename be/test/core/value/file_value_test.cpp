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

#include "core/value/file_value.h"

#include <gtest/gtest.h>

#include <string>
#include <string_view>

namespace doris {

class FileValueTest : public ::testing::Test {};

TEST_F(FileValueTest, ConvertsProviderEtagsWithoutRewritingOpaqueBytes) {
    EXPECT_EQ(file_checksum_from_etag(std::nullopt), std::nullopt);
    EXPECT_EQ(file_checksum_from_etag(""), std::nullopt);
    EXPECT_EQ(file_checksum_from_etag("\"\""), std::nullopt);
    for (const auto* etag : {"opaque-2", "\"opaque-2\""}) {
        EXPECT_EQ(file_checksum_from_etag(etag), "ETAG:opaque-2");
    }
    for (const std::string etag : {"MiXeD:opaque-2", "\"", " leading trailing "}) {
        EXPECT_EQ(file_checksum_from_etag(etag), "ETAG:" + etag);
    }
    EXPECT_EQ(file_checksum_from_etag("\"\"opaque-2\"\""), "ETAG:\"opaque-2\"");
    EXPECT_EQ(file_checksum_from_etag(std::string_view("\"a\0b\"", 5)),
              std::string("ETAG:a\0b", 8));
}

TEST_F(FileValueTest, Rfc2045MimeCorpusAndRawPreservation) {
    using namespace std::string_view_literals;
    for (const auto mime : {
                 "image/png"sv,
                 "Application/X.Unknown+Thing"sv,
                 "unknown/unknown"sv,
                 "x-{type}/x-{subtype}"sv,
                 "x/!#$%&'*+-.^_`{|}~"sv,
                 "text/plain; charset=UTF-8"sv,
                 "text/plain\t;\tcharset=utf-8;format=flowed"sv,
                 "multipart/mixed;boundary=\"a;b=c/\\\"quote\\\\end\""sv,
                 "text/plain;empty=\"\""sv,
                 "text/plain;v=\"a\tb\""sv,
                 "text/plain;a=one;a=two"sv,
                 " text/plain"sv,
                 "text/plain "sv,
                 "text /plain"sv,
                 "text/ plain"sv,
                 "text/plain(comment)"sv,
                 "text/plain; charset =utf-8"sv,
                 "text/plain; charset= utf-8"sv,
                 "(before)text(type)/(slash)plain(subtype);(semi)charset(attr)=(eq)utf-8(value)"sv,
                 "(one)(two) text/plain (three)"sv,
                 "text/plain (outer(inner(deep))tail)"sv,
                 "text/plain (escaped \\( \\) \\\\ delimiters)"sv,
                 "text/plain (a \"quote\" is just comment text)"sv,
                 "text/plain; title=\"(not a comment); x=y\""sv,
                 "text/plain; name=\"a\\qb\""sv,
                 "text\r\n /\r\n\tplain\r\n ;\r\n x\r\n =\r\n \"a\r\n b\"\r\n "sv,
                 "text/plain (a\r\n\tb)"sv,
                 "text/plain; p=\"a\r\n \r\n\tb\""sv,
                 "text/plain; p=\"a\\\r\n b\""sv,
                 "text/plain (a\\\r\n b)"sv,
                 "text/plain; p=\"\000\001\177\""sv,
                 "text/plain (\000\001\177)"sv,
                 "text/plain; p=\"a\nb\""sv,
                 "text/plain (a\nb)"sv,
                 "text/plain; p=\"a\\\rb\""sv,
                 "text/plain (a\\\rb)"sv,
         }) {
        EXPECT_TRUE(is_valid_file_content_type(mime)) << mime;
    }
    for (const auto mime : {
                 ""sv,
                 " "sv,
                 "(only a comment)"sv,
                 "text"sv,
                 "/plain"sv,
                 "text/"sv,
                 "text/plain/extra"sv,
                 "text/plain, image/png"sv,
                 "text/plain;"sv,
                 "text/plain; ;charset=utf-8;\t"sv,
                 "text/plain;(comment)"sv,
                 "text/plain; charset"sv,
                 "text/plain; =utf-8"sv,
                 "text/plain; charset="sv,
                 "text/plain; charset=(empty)"sv,
                 "text/plain; charset=utf 8"sv,
                 "text/plain; a=b=c"sv,
                 "text/plain; a=b/c"sv,
                 "text/plain; a=\"unfinished"sv,
                 "text/plain; a=\"dangling\\"sv,
                 "text/plain; a=\"x\"suffix"sv,
                 "text/plain; a=\"x\"(comment)suffix"sv,
                 "te(comment)xt/plain"sv,
                 "text/pl(comment)ain"sv,
                 "text/plain; char(comment)set=utf-8"sv,
                 "text/plain; charset=utf(comment)-8"sv,
                 "\"text\"/plain"sv,
                 "text/\"plain\""sv,
                 "text/plain; \"charset\"=utf-8"sv,
                 "text/plain (unfinished"sv,
                 "text/plain (a(b)"sv,
                 "text/plain (dangling\\"sv,
                 "text/plain )"sv,
                 "text/plain (x))"sv,
                 "text/plain; a=b\\c"sv,
                 "text/plain\r\n; charset=utf-8"sv,
                 "text/plain\r\n"sv,
                 "text/plain\r"sv,
                 "text/plain\n"sv,
                 "text/plain (a\rb)"sv,
                 "text/plain; p=\"a\rb\""sv,
                 "text/plain (a\r\nb)"sv,
                 "text/plain; p=\"a\r\nb\""sv,
                 "text/plain\r\n\r\n x=y"sv,
                 "text/pl\303\244in"sv,
                 "text/plain; p=\"\303\251\""sv,
                 "text/plain (\303\251)"sv,
                 "text/plain; p=\"\\\303\251\""sv,
                 "text/plain (\\\303\251)"sv,
                 "text/plain; p=[a]"sv,
                 "text/plain; p={a?}"sv,
                 "text/plain; a=\000"sv,
         }) {
        EXPECT_FALSE(is_valid_file_content_type(mime)) << mime;
    }
}

TEST_F(FileValueTest, Rfc2045QuotedCrlfRequiresFoldingWhitespace) {
    // RFC 822 sections 3.4.3 and 3.4.5 explicitly cover quoted CRLFs.
    for (const auto* mime : {"text/plain; p=\"a\\\r\n b\"", "text/plain; p=\"a\\\r\n\tb\"",
                             "text/plain (a\\\r\n b)", "text/plain (a\\\r\n\tb)"}) {
        EXPECT_TRUE(is_valid_file_content_type(mime));
    }
    for (const auto* mime : {"text/plain; p=\"a\\\r\nb\"", "text/plain; p=\"a\\\r\n\"",
                             "text/plain (a\\\r\nb)", "text/plain (a\\\r\n)"}) {
        EXPECT_FALSE(is_valid_file_content_type(mime));
    }
}

TEST_F(FileValueTest, Rfc2045MimeAsciiLexicalClasses) {
    for (int code = 0; code < 256; ++code) {
        SCOPED_TRACE(code);
        const char ch = static_cast<char>(code);
        const bool token = code > 32 && code < 127 &&
                           std::string_view("()<>@,;:\\\"/[]?=").find(ch) == std::string_view::npos;
        EXPECT_EQ(is_valid_file_content_type(std::string("x/") + ch), token);
        EXPECT_EQ(is_valid_file_content_type(std::string("x/y; p=\"") + ch + "\""),
                  code < 128 && ch != '\\' && ch != '"' && ch != '\r');
        EXPECT_EQ(is_valid_file_content_type(std::string("x/y; p=\"\\") + ch + "\""), code < 128);
        EXPECT_EQ(is_valid_file_content_type(std::string("x/y (") + ch + ")"),
                  code < 128 && ch != '(' && ch != ')' && ch != '\\' && ch != '\r');
        EXPECT_EQ(is_valid_file_content_type(std::string("x/y (\\") + ch + ")"), code < 128);
    }
}

TEST_F(FileValueTest, Rfc2045MimeNestedCommentsAndFoldingWhitespace) {
    const std::string nested = "x/y" + std::string(4096, '(') + std::string(4096, ')');
    EXPECT_TRUE(is_valid_file_content_type(nested));
    EXPECT_FALSE(is_valid_file_content_type(nested.substr(0, nested.size() - 1)));
    std::string folded = "x/y";
    for (int i = 0; i < 4096; ++i) {
        folded += "\r\n ";
    }
    EXPECT_TRUE(is_valid_file_content_type(folded));
    EXPECT_FALSE(is_valid_file_content_type(folded + "\r\n"));
}

TEST_F(FileValueTest, InfersContentTypeFromFilesystemNames) {
    EXPECT_EQ(infer_file_content_type_from_name("prefix/a.PNG"), "image/png");
    EXPECT_EQ(infer_file_content_type_from_name("a.JpEg"), "image/jpeg");
    EXPECT_EQ(infer_file_content_type_from_name("document.pdf"), "application/pdf");
    EXPECT_EQ(infer_file_content_type_from_name("part.parquet"), "application/x-parquet");
    EXPECT_EQ(infer_file_content_type_from_name("part.jsonl"), "application/x-ndjson");
    EXPECT_EQ(infer_file_content_type_from_name("directory.png/no_extension"),
              "application/octet-stream");
    EXPECT_EQ(infer_file_content_type_from_name("a.unrecognized"), "application/octet-stream");
    EXPECT_EQ(infer_file_content_type_from_name(""), "application/octet-stream");
    EXPECT_TRUE(is_valid_file_content_type("image/png"));
    EXPECT_FALSE(is_valid_file_content_type(""));
    EXPECT_FALSE(is_valid_file_content_type("not a mime type"));
}

} // namespace doris
