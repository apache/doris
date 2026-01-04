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

#include "util/simd/vstring_function.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include "gtest/gtest_pred_impl.h"
#include "vec/common/string_ref.h"

namespace doris::simd {

TEST(VStringFunctionsTest, Utf8ByteLengthTable) {
    for (int b = 0x00; b <= 0x7F; ++b) {
        EXPECT_EQ(get_utf8_byte_length(static_cast<uint8_t>(b)), 1);
    }
    for (int b = 0x80; b <= 0xBF; ++b) {
        EXPECT_EQ(get_utf8_byte_length(static_cast<uint8_t>(b)), 1);
    }
    for (int b = 0xC0; b <= 0xDF; ++b) {
        EXPECT_EQ(get_utf8_byte_length(static_cast<uint8_t>(b)), 2);
    }
    for (int b = 0xE0; b <= 0xEF; ++b) {
        EXPECT_EQ(get_utf8_byte_length(static_cast<uint8_t>(b)), 3);
    }
    for (int b = 0xF0; b <= 0xF7; ++b) {
        EXPECT_EQ(get_utf8_byte_length(static_cast<uint8_t>(b)), 4);
    }
    for (int b = 0xF8; b <= 0xFB; ++b) {
        EXPECT_EQ(get_utf8_byte_length(static_cast<uint8_t>(b)), 5);
    }
    for (int b = 0xFC; b <= 0xFF; ++b) {
        EXPECT_EQ(get_utf8_byte_length(static_cast<uint8_t>(b)), 6);
    }
}

TEST(VStringFunctionsTest, Rtrim) {
    StringRef remove_chr(" ");
    StringRef remove_str("abc");
    std::string str;
    const unsigned char* begin = nullptr;
    const unsigned char* end = nullptr;
    const unsigned char* res = nullptr;

    auto set_ptrs = [&](const std::string& s) {
        str = s;
        begin = reinterpret_cast<const unsigned char*>(str.data());
        end = begin + str.size();
    };

    // remove str
    // positive
    set_ptrs("hello worldabcabcabc");
    res = VStringFunctions::rtrim<false>(begin, end, remove_str);
    EXPECT_EQ(11, res - begin);
    EXPECT_EQ(0, strncmp(reinterpret_cast<const char*>(begin), "hello world", 11));
    // negative
    set_ptrs("hello worldabcaab");
    res = VStringFunctions::rtrim<false>(begin, end, remove_str);
    EXPECT_EQ(end, res);
    EXPECT_EQ(0, strncmp(reinterpret_cast<const char*>(begin), "hello worldabcaab", 17));

    // remove chr
    // no blank
    set_ptrs("hello worldaaa");
    res = VStringFunctions::rtrim<true>(begin, end, remove_chr);
    EXPECT_EQ(14, res - begin);
    EXPECT_EQ(0, strncmp(reinterpret_cast<const char*>(begin), "hello worldaaa", 14));
    // empty string
    set_ptrs("");
    res = VStringFunctions::rtrim<true>(begin, end, remove_chr);
    EXPECT_EQ(end, res);
    EXPECT_EQ(begin, res);
    // less than 16 blanks
    set_ptrs("hello world       ");
    res = VStringFunctions::rtrim<true>(begin, end, remove_chr);
    EXPECT_EQ(11, res - begin);
    EXPECT_EQ(0, strncmp(reinterpret_cast<const char*>(begin), "hello world", 11));
    // more than 16 blanks
    set_ptrs("hello world                                     ");
    res = VStringFunctions::rtrim<true>(begin, end, remove_chr);
    EXPECT_EQ(11, res - begin);
    EXPECT_EQ(0, strncmp(reinterpret_cast<const char*>(begin), "hello world", 11));
    // all are blanks, less than 16 blanks
    set_ptrs("       ");
    res = VStringFunctions::rtrim<true>(begin, end, remove_chr);
    EXPECT_EQ(begin, res);
    // all are blanks, more than 16 blanks
    set_ptrs("                  ");
    res = VStringFunctions::rtrim<true>(begin, end, remove_chr);
    EXPECT_EQ(begin, res);
    // src less than 16 length
    set_ptrs("hello worldabc");
    res = VStringFunctions::rtrim<true>(begin, end, remove_chr);
    EXPECT_EQ(14, res - begin);
    EXPECT_EQ(0, strncmp(reinterpret_cast<const char*>(begin), "hello worldabc", 14));
}

TEST(VStringFunctionsTest, Ltrim) {
    StringRef remove_chr(" ");
    StringRef remove_str("abc");
    std::string str;
    const unsigned char* begin = nullptr;
    const unsigned char* end = nullptr;
    const unsigned char* res = nullptr;

    auto set_ptrs = [&](const std::string& s) {
        str = s;
        begin = reinterpret_cast<const unsigned char*>(str.data());
        end = begin + str.size();
    };

    // remove str
    // positive
    set_ptrs("abcabcabchello world");
    res = VStringFunctions::ltrim<false>(begin, end, remove_str);
    EXPECT_EQ(11, end - res);
    EXPECT_EQ(0, strncmp(reinterpret_cast<const char*>(res), "hello world", 11));
    // negative
    set_ptrs("aababchello world");
    res = VStringFunctions::ltrim<false>(begin, end, remove_str);
    EXPECT_EQ(begin, res);
    EXPECT_EQ(0, strncmp(reinterpret_cast<const char*>(res), "aababchello world", 17));

    // remove chr
    // no blank
    set_ptrs("aaahello world");
    res = VStringFunctions::ltrim<true>(begin, end, remove_chr);
    EXPECT_EQ(14, end - res);
    EXPECT_EQ(0, strncmp(reinterpret_cast<const char*>(res), "aaahello world", 14));
    // empty string
    set_ptrs("");
    res = VStringFunctions::ltrim<true>(begin, end, remove_chr);
    EXPECT_EQ(end, res);
    EXPECT_EQ(begin, res);
    // less than 16 blanks
    set_ptrs("       hello world");
    res = VStringFunctions::ltrim<true>(begin, end, remove_chr);
    EXPECT_EQ(11, end - res);
    EXPECT_EQ(0, strncmp(reinterpret_cast<const char*>(res), "hello world", 11));
    // more than 16 blanks
    set_ptrs("                 hello world");
    res = VStringFunctions::ltrim<true>(begin, end, remove_chr);
    EXPECT_EQ(11, end - res);
    EXPECT_EQ(0, strncmp(reinterpret_cast<const char*>(res), "hello world", 11));
    // all are blanks, less than 16 blanks
    set_ptrs("       ");
    res = VStringFunctions::ltrim<true>(begin, end, remove_chr);
    EXPECT_EQ(end, res);
    // all are blanks, more than 16 blanks
    set_ptrs("                  ");
    res = VStringFunctions::ltrim<true>(begin, end, remove_chr);
    EXPECT_EQ(end, res);
    // src less than 16 length
    set_ptrs("abchello world");
    res = VStringFunctions::ltrim<true>(begin, end, remove_chr);
    EXPECT_EQ(14, end - res);
    EXPECT_EQ(0, strncmp(reinterpret_cast<const char*>(res), "abchello world", 14));
}

TEST(VStringFunctionsTest, IterateUtf8WithLimitLength) {
    std::string s = "hello world";
    auto res = VStringFunctions::iterate_utf8_with_limit_length(s.data(), s.data() + s.size(), 0);
    EXPECT_EQ(0U, res.first);
    EXPECT_EQ(0U, res.second);

    res = VStringFunctions::iterate_utf8_with_limit_length(s.data(), s.data() + s.size(), 5);
    EXPECT_EQ(5U, res.first);
    EXPECT_EQ(5U, res.second);

    // n larger than char count => consume whole string
    res = VStringFunctions::iterate_utf8_with_limit_length(s.data(), s.data() + s.size(), 100);
    EXPECT_EQ(s.size(), res.first);
    EXPECT_EQ(s.size(), res.second);

    // "ab中c" => bytes: 'a'(1) 'b'(1) '中'(3) 'c'(1) => total 6 bytes, 4 chars
    s = "ab\xE4\xB8\xAD"
        "c";
    res = VStringFunctions::iterate_utf8_with_limit_length(s.data(), s.data() + s.size(), 1);
    EXPECT_EQ(1U, res.first);
    EXPECT_EQ(1U, res.second);

    res = VStringFunctions::iterate_utf8_with_limit_length(s.data(), s.data() + s.size(), 3);
    EXPECT_EQ(5U, res.first); // a(1)+b(1)+中(3)
    EXPECT_EQ(3U, res.second);

    res = VStringFunctions::iterate_utf8_with_limit_length(s.data(), s.data() + s.size(), 4);
    EXPECT_EQ(6U, res.first);
    EXPECT_EQ(4U, res.second);

    // n greater than char count
    res = VStringFunctions::iterate_utf8_with_limit_length(s.data(), s.data() + s.size(), 10);
    EXPECT_EQ(6U, res.first);
    EXPECT_EQ(4U, res.second);

    // "你好a" => 你(3) 好(3) a(1) => total 7 bytes, 3 chars
    s = "\xE4\xBD\xA0\xE5\xA5\xBD"
        "a";
    res = VStringFunctions::iterate_utf8_with_limit_length(s.data(), s.data() + s.size(), 1);
    EXPECT_EQ(3U, res.first);
    EXPECT_EQ(1U, res.second);

    res = VStringFunctions::iterate_utf8_with_limit_length(s.data(), s.data() + s.size(), 2);
    EXPECT_EQ(6U, res.first);
    EXPECT_EQ(2U, res.second);

    res = VStringFunctions::iterate_utf8_with_limit_length(s.data(), s.data() + s.size(), 3);
    EXPECT_EQ(7U, res.first);
    EXPECT_EQ(3U, res.second);

    // n larger than char count
    res = VStringFunctions::iterate_utf8_with_limit_length(s.data(), s.data() + s.size(), 5);
    EXPECT_EQ(7U, res.first);
    EXPECT_EQ(3U, res.second);

    // "😀a" => 😀(4 bytes) + 'a'(1) => total 5 bytes, 2 chars
    s = "\xF0\x9F\x98\x80"
        "a";
    res = VStringFunctions::iterate_utf8_with_limit_length(s.data(), s.data() + s.size(), 1);
    EXPECT_EQ(4U, res.first);
    EXPECT_EQ(1U, res.second);

    res = VStringFunctions::iterate_utf8_with_limit_length(s.data(), s.data() + s.size(), 2);
    EXPECT_EQ(5U, res.first);
    EXPECT_EQ(2U, res.second);

    // n larger than char count
    res = VStringFunctions::iterate_utf8_with_limit_length(s.data(), s.data() + s.size(), 3);
    EXPECT_EQ(5U, res.first);
    EXPECT_EQ(2U, res.second);

    // "中文" => each 3 bytes => total 6 bytes, 2 chars
    s = "\xE4\xB8\xAD\xE6\x96\x87";
    res = VStringFunctions::iterate_utf8_with_limit_length(s.data(), s.data() + s.size(), 1);
    EXPECT_EQ(3U, res.first);
    EXPECT_EQ(1U, res.second);

    res = VStringFunctions::iterate_utf8_with_limit_length(s.data(), s.data() + s.size(), 2);
    EXPECT_EQ(6U, res.first);
    EXPECT_EQ(2U, res.second);

    // n larger than char count
    res = VStringFunctions::iterate_utf8_with_limit_length(s.data(), s.data() + s.size(), 5);
    EXPECT_EQ(6U, res.first);
    EXPECT_EQ(2U, res.second);

    // empty string
    s = "";
    res = VStringFunctions::iterate_utf8_with_limit_length(s.data(), s.data() + s.size(), 10);
    EXPECT_EQ(0U, res.first);
    EXPECT_EQ(0U, res.second);
}

TEST(VStringFunctionsTest, IsAscii) {
    EXPECT_EQ(true, VStringFunctions::is_ascii(StringRef("hello123")));
    EXPECT_EQ(true, VStringFunctions::is_ascii(StringRef("hello123fwrewers")));
    EXPECT_EQ(false, VStringFunctions::is_ascii(StringRef("运维组123")));
    EXPECT_EQ(false, VStringFunctions::is_ascii(StringRef("hello123运维组fwrewers")));
    EXPECT_EQ(true, VStringFunctions::is_ascii(StringRef("")));
}

TEST(VStringFunctionsTest, Reverse) {
    auto reverse_check = [](const std::string& src, const std::string& expected) {
        std::string dst(src.size(), '\0');
        StringRef src_ref(src);
        VStringFunctions::reverse(src_ref, &dst);
        EXPECT_EQ(dst, expected);
    };

    // empty and single char
    reverse_check("", "");
    reverse_check("a", "a");

    // ASCII
    reverse_check("hello world", "dlrow olleh");
    reverse_check("A1b2", "2b1A");

    // UTF-8: Chinese (3-byte each): "中文" -> "文中"
    std::string zh = "\xE4\xB8\xAD\xE6\x96\x87";
    reverse_check(zh, std::string("\xE6\x96\x87\xE4\xB8\xAD", 6));

    // mixed ASCII + Chinese: "ab中c" -> "c中ba"
    std::string mixed =
            "ab\xE4\xB8\xAD"
            "c";
    reverse_check(mixed, std::string("c\xE4\xB8\xAD"
                                     "ba",
                                     6));

    // emoji (4-byte) + ASCII: "😀a" -> "a😀"
    std::string emoji_a =
            "\xF0\x9F\x98\x80"
            "a";
    reverse_check(emoji_a, std::string("a\xF0\x9F\x98\x80", 5));

    // mixed multi-codepoint: "你😀好" -> "好😀你"
    std::string mix2 =
            "\xE4\xBD\xA0"
            "\xF0\x9F\x98\x80"
            "\xE5\xA5\xBD";
    reverse_check(mix2, std::string("\xE5\xA5\xBD"
                                    "\xF0\x9F\x98\x80"
                                    "\xE4\xBD\xA0",
                                    10));

    // illegal UTF-8 leading byte without continuation: "A\xC2" -> "\xC2A"
    std::string invalid = "A";
    invalid.push_back('\xC2'); // leading byte of a 2-byte sequence without continuation
    reverse_check(invalid, std::string("\xC2"
                                       "A",
                                       2));
}

TEST(VStringFunctionsTest, HexEncode) {
    auto encode_ptr = [](const unsigned char* p, size_t n) {
        std::string out(n * 2, '\0');
        VStringFunctions::hex_encode(p, n, out.data());
        return out;
    };

    // empty
    std::vector<unsigned char> empty {};
    EXPECT_EQ(std::string(), encode_ptr(empty.data(), empty.size()));

    // single byte: 'A' -> 0x41
    std::vector<unsigned char> one {'A'};
    EXPECT_EQ("41", encode_ptr(one.data(), one.size()));

    // ASCII "hello" -> 68 65 6C 6C 6F
    std::vector<unsigned char> hello {'h', 'e', 'l', 'l', 'o'};
    EXPECT_EQ("68656C6C6F", encode_ptr(hello.data(), hello.size()));

    // mixed values incl. 0x00 and 0xFF
    std::vector<unsigned char> bytes {0x00, 0xFF, 0x1A, 0xB0, 0x5E, 0x7F};
    EXPECT_EQ("00FF1AB05E7F", encode_ptr(bytes.data(), bytes.size()));

    // embedded zero
    std::vector<unsigned char> with_zero {0x01, 0x00, 0x02};
    EXPECT_EQ("010002", encode_ptr(with_zero.data(), with_zero.size()));

    // small string to skip SIMD path
    std::vector<unsigned char> small {0x12, 0x34, 0x56, 0x78, 0x9A};
    EXPECT_EQ("123456789A", encode_ptr(small.data(), small.size()));

    // large string to cover SIMD path
    std::vector<unsigned char> large {0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0,
                                      0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
                                      0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF};
    EXPECT_EQ("123456789ABCDEF0112233445566778899AABBCCDDEEFF",
              encode_ptr(large.data(), large.size()));
}

TEST(VStringFunctionsTest, ToLower) {
    auto check_to_lower = [](const std::string& in, const std::string& expected) {
        std::string dst(in.size(), '\0');
        VStringFunctions::to_lower(reinterpret_cast<const uint8_t*>(in.data()),
                                   static_cast<int64_t>(in.size()),
                                   reinterpret_cast<uint8_t*>(dst.data()));
        EXPECT_EQ(dst, expected);
    };

    // empty input
    check_to_lower("", "");

    // identical data except case (ASCII + spaces + digits; non-ASCII unchanged)
    const std::string upper1 = "ABC XYZ-123_世界😀";
    const std::string lower1 = "abc xyz-123_世界😀";
    check_to_lower(upper1, lower1);

    // pure ASCII punctuation mix
    const std::string upper2 = "HELLO,WORLD!";
    const std::string lower2 = "hello,world!";
    check_to_lower(upper2, lower2);

    // Non-letters should remain unchanged
    check_to_lower("1234-_=+!@#", "1234-_=+!@#");
}

TEST(VStringFunctionsTest, ToUpper) {
    auto check_to_upper = [](const std::string& in, const std::string& expected) {
        std::string dst(in.size(), '\0');
        VStringFunctions::to_upper(reinterpret_cast<const uint8_t*>(in.data()),
                                   static_cast<int64_t>(in.size()),
                                   reinterpret_cast<uint8_t*>(dst.data()));
        EXPECT_EQ(dst, expected);
    };

    // empty input
    check_to_upper("", "");

    // identical data except case (ASCII + spaces + digits; non-ASCII unchanged)
    const std::string upper1 = "ABC XYZ-123_世界😀";
    const std::string lower1 = "abc xyz-123_世界😀";
    check_to_upper(lower1, upper1);

    // pure ASCII punctuation mix
    const std::string upper2 = "HELLO,WORLD!";
    const std::string lower2 = "hello,world!";
    check_to_upper(lower2, upper2);

    // Non-letters should remain unchanged
    check_to_upper("1234-_=+!@#", "1234-_=+!@#");
}

TEST(VStringFunctionsTest, GetCharLen) {
    auto check = [](const std::string& s, size_t expected_count,
                    const std::vector<size_t>& expected_idx) {
        // overload with index vector
        std::vector<size_t> idx;
        size_t c1 = VStringFunctions::get_char_len(s.data(), s.size(), idx);
        EXPECT_EQ(expected_count, c1);
        EXPECT_EQ(expected_idx, idx);

        // templated overload (size_t)
        auto c2 = VStringFunctions::get_char_len<size_t>(s.data(), s.size());
        EXPECT_EQ(expected_count, c2);

        // templated overload (int32_t)
        auto c3 = VStringFunctions::get_char_len<int32_t>(s.data(), static_cast<int32_t>(s.size()));
        EXPECT_EQ(static_cast<int32_t>(expected_count), c3);
    };

    // empty
    check("", 0, {});

    // ASCII
    check("hello", 5, {0, 1, 2, 3, 4});

    // "中文" => 3+3 bytes, 2 chars
    std::string zh = "\xE4\xB8\xAD\xE6\x96\x87";
    check(zh, 2, {0, 3});

    // "ab中c" => 'a'(0) 'b'(1) '中'(2) 'c'(5)
    std::string mixed =
            "ab\xE4\xB8\xAD"
            "c";
    check(mixed, 4, {0, 1, 2, 5});

    // "你😀好" => 你(0,3 bytes), 😀(3,4 bytes), 好(7,3 bytes)
    std::string emoji_mix =
            "\xE4\xBD\xA0"
            "\xF0\x9F\x98\x80"
            "\xE5\xA5\xBD";
    check(emoji_mix, 3, {0, 3, 7});
}
} //namespace doris::simd
