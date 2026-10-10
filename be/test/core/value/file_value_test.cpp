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

#include <array>
#include <limits>
#include <string>
#include <string_view>
#include <utility>

#include "core/field.h"

namespace doris {

class FileValueTest : public ::testing::Test {
protected:
    static File file(std::string uri = "s3://bucket/object") {
        File value(6);
        value[0] = Field::create_field<TYPE_STRING>(std::move(uri));
        return value;
    }

    static Status with_text(size_t index, std::string text) {
        auto value = file();
        value[index] = Field::create_field<TYPE_STRING>(std::move(text));
        return validate_file(value);
    }
};

TEST_F(FileValueTest, RequiresSixCorrectlyTypedChildrenAndUri) {
    EXPECT_TRUE(validate_file(file()).ok());
    EXPECT_FALSE(validate_file(File()).ok());
    EXPECT_FALSE(validate_file(File(5)).ok());
    EXPECT_FALSE(validate_file(File(7)).ok());
    EXPECT_FALSE(validate_file(File(6)).ok());
    EXPECT_FALSE(validate_file(file("")).ok());

    for (size_t index = 0; index < 6; ++index) {
        auto value = file();
        value[index] = Field::create_field<TYPE_INT>(42);
        EXPECT_FALSE(validate_file(value).ok()) << index;
    }
    for (size_t index : {0, 3, 4}) {
        auto value = file();
        value[index] = Field::create_field<TYPE_VARBINARY>(StringView("binary"));
        EXPECT_FALSE(validate_file(value).ok()) << index;
    }
    for (size_t index : {1, 2, 5}) {
        EXPECT_FALSE(with_text(index, "0").ok()) << index;
    }
}

TEST_F(FileValueTest, AcceptsAbsoluteUriGrammarWithoutSchemePolicy) {
    for (const auto* uri : {
                 "s3://bucket/key?versionId=a%2Fb&signature=Secret",
                 "hdfs://host:8020/path",
                 "file:///tmp/a",
                 "urn:example:object",
                 "data:application/octet-stream;base64,AA==",
                 "Custom+1.-:root/../a%2fb?x=/?:@!$&'()*+,;=%00",
                 "x:",
                 "x:?",
                 "x:/",
                 "x://",
                 "x:///",
                 "x://?query",
                 "x:////a",
                 "x://user:pass@host:/a",
                 "x://@/a",
                 "x://host:999999999999999999999/a",
                 "x://a%2Fb!$&'()*+,;=/a",
                 "x://999.999.999.999/a",
                 "x://[::1]/a",
                 "x://[2001:DB8:0:1:2:3:4:5]:80/a",
                 "x://[::ffff:192.0.2.128]/a",
                 "x://[1:2:3:4:5:6:192.0.2.1]/a",
                 "x://[v1.future:host]/a",
                 "x://[VF.a!$&'()*+,;=:]/a",
         }) {
        EXPECT_TRUE(validate_file(file(uri)).ok()) << uri;
    }
}

TEST_F(FileValueTest, RejectsMalformedUriComponents) {
    for (const auto* uri : {
                 "relative/path",
                 "/absolute/path",
                 "//host/a",
                 ":a",
                 "1x:a",
                 "+x:a",
                 "a_b:a",
                 "s3://bucket/a#fragment",
                 "s3://bucket/a#",
                 "x:a?b#",
                 "x:a b",
                 "x:a\tb",
                 "x:a\rb",
                 "x:a\nb",
                 "x:a\\b",
                 "x:a<b>",
                 "x:a{b}",
                 "x:a|b",
                 "x:a^b",
                 "x:a`b",
                 "x:a\"b",
                 "x:a[b]",
                 "x:a?b[c]",
                 "x:%",
                 "x:%0",
                 "x:%gg",
                 "x:%0g",
                 "x://ho%st/a",
                 "x://u%zz@h/a",
                 "x:a?b=%0",
                 "x://user@@host/a",
                 "x://user[bad]@host/a",
                 "x://host:abc/a",
                 "x://host:-1/a",
                 "x://host:80:90/a",
                 "x://host:%38/a",
                 "x://::1/a",
                 "x://[::1/a",
                 "x://[::1]tail/a",
                 "x://[]/a",
                 "x://[1:2:3:4:5:6:7]/a",
                 "x://[1:2:3:4:5:6:7:8:9]/a",
                 "x://[1:2:3:4:5:6:7:8::]/a",
                 "x://[1::2::3]/a",
                 "x://[12345::]/a",
                 "x://[::ffff:192.0.2.999]/a",
                 "x://[::ffff:192.0.02.1]/a",
                 "x://[fe80::1%25eth0]/a",
                 "x://[v.foo]/a",
                 "x://[vG.foo]/a",
                 "x://[v1.]/a",
                 "x://[v1.%41]/a",
                 "x://[v1.a@b]/a",
         }) {
        EXPECT_FALSE(validate_file(file(uri)).ok()) << uri;
    }
    EXPECT_FALSE(validate_file(file(std::string("x:a\0b", 5))).ok());
    EXPECT_FALSE(validate_file(file(std::string("x:a\x7f", 4))).ok());
    EXPECT_FALSE(validate_file(file("x:\xc3\xa9")).ok());
}

TEST_F(FileValueTest, RangeBoundaries) {
    constexpr Int64 max = std::numeric_limits<Int64>::max();
    struct RangeCase {
        bool has_offset;
        Int64 offset;
        bool has_size;
        Int64 size;
        bool valid;
    };
    for (const auto& range : std::array {
                 RangeCase {false, 0, false, 0, true},
                 RangeCase {false, 0, true, 0, true},
                 RangeCase {false, 0, true, max, true},
                 RangeCase {false, 0, true, -1, false},
                 RangeCase {true, 0, false, 0, false},
                 RangeCase {true, 1, false, 0, false},
                 RangeCase {true, -1, true, 0, false},
                 RangeCase {true, 0, true, -1, false},
                 RangeCase {true, 0, true, 0, true},
                 RangeCase {true, 0, true, max, true},
                 RangeCase {true, max, true, 0, true},
                 RangeCase {true, max - 1, true, 1, true},
                 RangeCase {true, max, true, 1, false},
                 RangeCase {true, 1, true, max, false},
                 RangeCase {true, max, true, max, false},
                 RangeCase {true, std::numeric_limits<Int64>::min(), true, 0, false},
         }) {
        auto value = file();
        if (range.has_offset) {
            value[1] = Field::create_field<TYPE_BIGINT>(range.offset);
        }
        if (range.has_size) {
            value[2] = Field::create_field<TYPE_BIGINT>(range.size);
        }
        EXPECT_EQ(validate_file(value).ok(), range.valid)
                << range.has_offset << ":" << range.offset << ", " << range.has_size << ":"
                << range.size;
    }
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
        auto value = file();
        value[3] = Field::create_field<TYPE_STRING>(std::string(mime));
        EXPECT_TRUE(is_valid_file_content_type(mime)) << mime;
        EXPECT_TRUE(validate_file(value).ok()) << mime;
        EXPECT_EQ(value[3].as_string_view(), mime);
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
        EXPECT_FALSE(with_text(3, std::string(mime)).ok()) << mime;
    }
}

TEST_F(FileValueTest, Rfc2045QuotedCrlfRequiresFoldingWhitespace) {
    // RFC 822 sections 3.4.3 and 3.4.5 explicitly cover quoted CRLFs.
    for (const auto* mime : {"text/plain; p=\"a\\\r\n b\"", "text/plain; p=\"a\\\r\n\tb\"",
                             "text/plain (a\\\r\n b)", "text/plain (a\\\r\n\tb)"}) {
        EXPECT_TRUE(with_text(3, mime).ok());
    }
    for (const auto* mime : {"text/plain; p=\"a\\\r\nb\"", "text/plain; p=\"a\\\r\n\"",
                             "text/plain (a\\\r\nb)", "text/plain (a\\\r\n)"}) {
        EXPECT_FALSE(with_text(3, mime).ok());
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

TEST_F(FileValueTest, Rfc2045MimeCommentDepthAndFoldedLengthLimit) {
    const std::string nested = "x/y" + std::string(510, '(') + std::string(510, ')');
    EXPECT_TRUE(is_valid_file_content_type(nested));
    EXPECT_FALSE(is_valid_file_content_type(nested.substr(0, nested.size() - 1)));
    EXPECT_FALSE(is_valid_file_content_type("x/y" + std::string(511, '(') + std::string(511, ')')));
    std::string folded = "x/y";
    for (int i = 0; i < 340; ++i) {
        folded += "\r\n ";
    }
    folded += ' ';
    EXPECT_EQ(folded.size(), 1024);
    EXPECT_TRUE(is_valid_file_content_type(folded));
    EXPECT_FALSE(is_valid_file_content_type(folded + ' '));
}

TEST_F(FileValueTest, RequiresCanonicalKnownChecksums) {
    struct Algorithm {
        std::string_view name;
        std::string_view lowercase_name;
        size_t length;
    };
    for (const auto& algorithm : std::array {
                 Algorithm {"MD5", "md5", 32},
                 Algorithm {"CRC32", "crc32", 8},
                 Algorithm {"CRC32C", "crc32c", 8},
                 Algorithm {"SHA-256", "sha-256", 64},
         }) {
        const std::string prefix = std::string(algorithm.name) + ":";
        EXPECT_TRUE(with_text(4, prefix + std::string(algorithm.length, 'a')).ok());
        EXPECT_TRUE(with_text(4, prefix + std::string(algorithm.length, '0')).ok());
        EXPECT_FALSE(with_text(4, prefix + std::string(algorithm.length - 1, 'a')).ok());
        EXPECT_FALSE(with_text(4, prefix + std::string(algorithm.length + 1, 'a')).ok());
        EXPECT_FALSE(with_text(4, prefix + std::string(algorithm.length, 'A')).ok());
        EXPECT_FALSE(with_text(4, prefix + std::string(algorithm.length, 'g')).ok());
        EXPECT_FALSE(with_text(4, std::string(algorithm.lowercase_name) + ":" +
                                          std::string(algorithm.length, 'a'))
                             .ok());
    }
    EXPECT_FALSE(with_text(4, "md5:0123456789ABCDEF0123456789ABCDEF").ok());
    EXPECT_FALSE(with_text(4, "Md5:0123456789abcdef0123456789abcdef").ok());
    EXPECT_FALSE(with_text(4, "etag:opaque").ok());
    EXPECT_FALSE(with_text(4, "ETag:opaque").ok());
    for (const auto* checksum : {"", ":", ":digest", "unknown", "unknown:", "ETAG:"}) {
        EXPECT_FALSE(with_text(4, checksum).ok()) << checksum;
    }
}

TEST_F(FileValueTest, PreservesOpaqueChecksums) {
    for (const std::string& checksum : {
                 std::string("ETAG:\"AbC-123\""),
                 std::string("ETAG:opaque:token-2"),
                 std::string("Future algorithm:Uppercase / opaque : digest"),
                 std::string("UNKNOWN:\0\xff", 10),
                 std::string("ETAG: "),
         }) {
        auto value = file();
        value[4] = Field::create_field<TYPE_STRING>(checksum);
        EXPECT_TRUE(validate_file(value).ok());
        EXPECT_EQ(value[4].get<TYPE_STRING>(), checksum);
    }
}

TEST_F(FileValueTest, EnforcesByteLengthLimits) {
    EXPECT_TRUE(with_text(0, "x:" + std::string(65531, 'a')).ok());
    EXPECT_FALSE(with_text(0, "x:" + std::string(65532, 'a')).ok());
    const std::string mime_prefix = "application/x-example;p=";
    EXPECT_TRUE(with_text(3, mime_prefix + std::string(1024 - mime_prefix.size(), 'a')).ok());
    EXPECT_FALSE(with_text(3, mime_prefix + std::string(1025 - mime_prefix.size(), 'a')).ok());
    EXPECT_TRUE(with_text(4, "ETAG:" + std::string(1019, 'A')).ok());
    EXPECT_FALSE(with_text(4, "ETAG:" + std::string(1020, 'A')).ok());
    EXPECT_TRUE(with_text(4, "X:" + std::string(1022, 'A')).ok());
    EXPECT_FALSE(with_text(4, "X:" + std::string(1023, 'A')).ok());
}

TEST_F(FileValueTest, PreservesUriMimeAndInlineWithoutReadingOrComparingContent) {
    const std::string uri = "Custom://User:Secret@Host:00080/a/../b%2fc?signature=Token%2F";
    const std::string mime = "Application/X.Unknown; Name=\"Mixed Case\"";
    const std::string bytes = std::string(4096, '\xff') + std::string("\0end", 4);
    auto value = file(uri);
    value[2] = Field::create_field<TYPE_BIGINT>(0);
    value[3] = Field::create_field<TYPE_STRING>(mime);
    value[4] = Field::create_field<TYPE_STRING>("MD5:00000000000000000000000000000000");
    value[5] = Field::create_field<TYPE_VARBINARY>(StringView(bytes));
    ASSERT_TRUE(validate_file(value).ok());
    EXPECT_EQ(value[0].get<TYPE_STRING>(), uri);
    EXPECT_EQ(value[3].get<TYPE_STRING>(), mime);
    EXPECT_EQ(std::string_view(value[5].get<TYPE_VARBINARY>()), bytes);

    value[5] = Field::create_field<TYPE_VARBINARY>(StringView(""));
    EXPECT_TRUE(validate_file(value).ok());
    value[0] = Field();
    EXPECT_FALSE(validate_file(value).ok());
}

TEST_F(FileValueTest, ErrorsDoNotIncludeSensitiveValues) {
    const std::string uri = "s3://user:PrivatePassword@bucket/path?sig=PrivateSignature#bad";
    auto value = file(uri);
    const auto uri_status = validate_file(value);
    ASSERT_FALSE(uri_status.ok());
    EXPECT_EQ(uri_status.to_string().find(uri), std::string::npos);
    EXPECT_EQ(uri_status.to_string().find("PrivatePassword"), std::string::npos);
    EXPECT_EQ(uri_status.to_string().find("PrivateSignature"), std::string::npos);
    EXPECT_EQ(value[0].get<TYPE_STRING>(), uri);

    const std::string checksum = "MD5:PrivateChecksum";
    value = file();
    value[4] = Field::create_field<TYPE_STRING>(checksum);
    const auto checksum_status = validate_file(value);
    ASSERT_FALSE(checksum_status.ok());
    EXPECT_EQ(checksum_status.to_string().find(checksum), std::string::npos);
    EXPECT_EQ(checksum_status.to_string().find("PrivateChecksum"), std::string::npos);
    EXPECT_EQ(value[4].get<TYPE_STRING>(), checksum);
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
