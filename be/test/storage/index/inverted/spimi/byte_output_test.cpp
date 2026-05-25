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

#include "storage/index/inverted/spimi/byte_output.h"

#include <gtest/gtest.h>

#include <vector>

namespace doris::segment_v2::inverted_index::spimi {

namespace {

std::vector<uint8_t> Bytes(std::initializer_list<int> list) {
    std::vector<uint8_t> out;
    out.reserve(list.size());
    for (int v : list) {
        out.push_back(static_cast<uint8_t>(v));
    }
    return out;
}

} // namespace

TEST(ByteOutputTest, WriteIntIsBigEndian) {
    MemoryByteOutput out;
    out.WriteInt(0x01020304);
    EXPECT_EQ(out.bytes(), Bytes({0x01, 0x02, 0x03, 0x04}));
    EXPECT_EQ(out.FilePointer(), 4);
}

TEST(ByteOutputTest, WriteIntNegativeOnesAndFormat) {
    MemoryByteOutput out;
    out.WriteInt(-1);
    EXPECT_EQ(out.bytes(), Bytes({0xFF, 0xFF, 0xFF, 0xFF}));
    out.Clear();
    out.WriteInt(-4); // Lucene FORMAT sentinel
    EXPECT_EQ(out.bytes(), Bytes({0xFF, 0xFF, 0xFF, 0xFC}));
}

TEST(ByteOutputTest, WriteLongIsBigEndian) {
    MemoryByteOutput out;
    out.WriteLong(0x0102030405060708LL);
    EXPECT_EQ(out.bytes(), Bytes({0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}));
}

TEST(ByteOutputTest, WriteLongNegativeOne) {
    MemoryByteOutput out;
    out.WriteLong(-1);
    EXPECT_EQ(out.bytes(), Bytes({0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}));
}

TEST(ByteOutputTest, WriteVIntZeroIsOneByte) {
    MemoryByteOutput out;
    out.WriteVInt(0);
    EXPECT_EQ(out.bytes(), Bytes({0x00}));
}

TEST(ByteOutputTest, WriteVIntSmallSingleByte) {
    MemoryByteOutput out;
    out.WriteVInt(127);
    EXPECT_EQ(out.bytes(), Bytes({0x7F}));
}

TEST(ByteOutputTest, WriteVIntTwoBytes) {
    MemoryByteOutput out;
    out.WriteVInt(128); // 0x80 = 10000000_b — needs two bytes
    EXPECT_EQ(out.bytes(), Bytes({0x80, 0x01}));
    out.Clear();
    out.WriteVInt(300); // 0x12C = 100101100 — low7 = 0x2C with continuation, high = 0x02
    EXPECT_EQ(out.bytes(), Bytes({0xAC, 0x02}));
}

TEST(ByteOutputTest, WriteVIntBigValueFiveBytes) {
    MemoryByteOutput out;
    out.WriteVInt(0x7FFFFFFF);
    // 31 bits packed 7-at-a-time → 5 bytes; top byte holds the highest 4 bits.
    EXPECT_EQ(out.bytes(), Bytes({0xFF, 0xFF, 0xFF, 0xFF, 0x07}));
}

TEST(ByteOutputTest, WriteVLongLargeValue) {
    MemoryByteOutput out;
    out.WriteVLong(0x0102030405060708LL);
    // Same scheme as VInt, just longer. Compare against the bit pattern.
    // 0x0102030405060708 in binary, split low→high 7 bits each.
    std::vector<uint8_t> expected;
    uint64_t v = 0x0102030405060708ULL;
    while ((v & ~static_cast<uint64_t>(0x7F)) != 0) {
        expected.push_back(static_cast<uint8_t>((v & 0x7F) | 0x80));
        v >>= 7;
    }
    expected.push_back(static_cast<uint8_t>(v));
    EXPECT_EQ(out.bytes(), expected);
}

TEST(ByteOutputTest, WriteSCharsAsciiIsOneBytePerChar) {
    MemoryByteOutput out;
    const std::wstring w = L"abc";
    out.WriteSCharsFromWide(w.data(), static_cast<int32_t>(w.size()));
    EXPECT_EQ(out.bytes(), Bytes({'a', 'b', 'c'}));
}

TEST(ByteOutputTest, WriteSCharsTwoByteRange) {
    MemoryByteOutput out;
    const std::wstring w {static_cast<wchar_t>(0xA9)}; // ©
    out.WriteSCharsFromWide(w.data(), 1);
    EXPECT_EQ(out.bytes(), Bytes({0xC2, 0xA9}));
}

TEST(ByteOutputTest, WriteSCharsThreeByteRange) {
    MemoryByteOutput out;
    const std::wstring w {static_cast<wchar_t>(0x4E2D)}; // 中
    out.WriteSCharsFromWide(w.data(), 1);
    EXPECT_EQ(out.bytes(), Bytes({0xE4, 0xB8, 0xAD}));
}

TEST(ByteOutputTest, WriteSCharsFourByteUsesCLuceneModifiedEncoding) {
    // U+1F600 (😀). CLucene's writeSChars<TCHAR> emits four bytes ALL with the
    // high bit set, not the proper UTF-8 (which would start with 0xF0). We
    // mimic the same byte stream so existing readers stay compatible.
    MemoryByteOutput out;
    const std::wstring w {static_cast<wchar_t>(0x1F600)};
    out.WriteSCharsFromWide(w.data(), 1);
    const uint32_t code = 0x1F600;
    EXPECT_EQ(out.bytes(), Bytes({static_cast<int>(0x80 | (code >> 18)),
                                  static_cast<int>(0x80 | ((code >> 12) & 0x3F)),
                                  static_cast<int>(0x80 | ((code >> 6) & 0x3F)),
                                  static_cast<int>(0x80 | (code & 0x3F))}));
    EXPECT_EQ(out.bytes()[0] & 0x80U, 0x80U);
}

TEST(ByteOutputTest, Utf8ToWideAsciiAndCjk) {
    EXPECT_EQ(Utf8ToWide("abc"), std::wstring(L"abc"));
    EXPECT_EQ(Utf8ToWide("中文"),
              (std::wstring {static_cast<wchar_t>(0x4E2D), static_cast<wchar_t>(0x6587)}));
}

TEST(ByteOutputTest, Utf8ToWideFourByteEmoji) {
    const std::wstring expected {static_cast<wchar_t>(0x1F600)};
    EXPECT_EQ(Utf8ToWide("\xF0\x9F\x98\x80"), expected);
}

TEST(ByteOutputTest, Utf8ToWideEmptyIsEmpty) {
    EXPECT_TRUE(Utf8ToWide("").empty());
}

// C7 — ill-formed UTF-8 must produce U+FFFD rather than a "valid" wchar
// that breaks the SPIMI term comparator's strict-ascending invariant or
// confuses the CLucene reader.

TEST(ByteOutputTest, Utf8ToWideRejectsOverlongTwoByteForm) {
    // C0 80 decodes to U+0000 if accepted (modified-UTF-8 NUL encoding); we
    // reject it. The leading byte advances `n` bytes (length-prefix said 2)
    // and emits one replacement.
    const std::wstring got = Utf8ToWide("\xC0\x80");
    ASSERT_EQ(got.size(), 1U);
    EXPECT_EQ(got[0], static_cast<wchar_t>(0xFFFD));
}

TEST(ByteOutputTest, Utf8ToWideRejectsOverlongThreeByteForm) {
    // E0 82 80 decodes to U+0080 if accepted (overlong); must be replaced.
    const std::wstring got = Utf8ToWide("\xE0\x82\x80");
    ASSERT_EQ(got.size(), 1U);
    EXPECT_EQ(got[0], static_cast<wchar_t>(0xFFFD));
}

TEST(ByteOutputTest, Utf8ToWideRejectsHighSurrogate) {
    // ED A0 80 decodes to U+D800 (high surrogate) if accepted; not a valid
    // scalar value. Must be replaced.
    const std::wstring got = Utf8ToWide("\xED\xA0\x80");
    ASSERT_EQ(got.size(), 1U);
    EXPECT_EQ(got[0], static_cast<wchar_t>(0xFFFD));
}

TEST(ByteOutputTest, Utf8ToWideRejectsCodepointAbove10FFFF) {
    // F4 90 80 80 decodes to U+110000 if accepted; out of Unicode range.
    const std::wstring got = Utf8ToWide("\xF4\x90\x80\x80");
    ASSERT_EQ(got.size(), 1U);
    EXPECT_EQ(got[0], static_cast<wchar_t>(0xFFFD));
}

TEST(ByteOutputTest, Utf8ToWideRejectsTruncatedContinuation) {
    // 0xE0 says "three-byte sequence" but only one continuation byte
    // follows. The decoder must replace and resync at the next byte.
    const std::wstring got = Utf8ToWide(std::string("\xE0\x80", 2));
    // 0xE0 leading byte → invalid → 1 replacement, advance 1; 0x80 lone
    // continuation → invalid → 1 replacement, advance 1.
    ASSERT_EQ(got.size(), 2U);
    EXPECT_EQ(got[0], static_cast<wchar_t>(0xFFFD));
    EXPECT_EQ(got[1], static_cast<wchar_t>(0xFFFD));
}

} // namespace doris::segment_v2::inverted_index::spimi
