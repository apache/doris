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

#include "exprs/function/url/find_symbols.h"

#include <gtest/gtest.h>

#include <string>

namespace doris {

class FindSymbolsTest : public ::testing::Test {};

// ==================== find_last_not_symbols_or_null with SearchSymbols ====================

// --- SSE2 path: < 5 trim characters ---

TEST_F(FindSymbolsTest, LastNotSymbols_SSE2_BasicTrim) {
    // "   hello   " with trim chars " " -> last non-space is 'o' at index 7
    std::string s = "   hello   ";
    SearchSymbols symbols(" ");
    const char* result = find_last_not_symbols_or_null(s.data(), s.data() + s.size(), symbols);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(*result, 'o');
    EXPECT_EQ(result - s.data(), 7);
}

TEST_F(FindSymbolsTest, LastNotSymbols_SSE2_AllMatch) {
    // All characters are trim chars -> returns nullptr
    std::string s = "    ";
    SearchSymbols symbols(" ");
    const char* result = find_last_not_symbols_or_null(s.data(), s.data() + s.size(), symbols);
    EXPECT_EQ(result, nullptr);
}

TEST_F(FindSymbolsTest, LastNotSymbols_SSE2_NoMatch) {
    // No trim chars in string -> last char is the answer
    std::string s = "hello";
    SearchSymbols symbols(" ");
    const char* result = find_last_not_symbols_or_null(s.data(), s.data() + s.size(), symbols);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(*result, 'o');
    EXPECT_EQ(result - s.data(), 4);
}

TEST_F(FindSymbolsTest, LastNotSymbols_SSE2_EmptyString) {
    std::string s;
    SearchSymbols symbols(" ");
    const char* result = find_last_not_symbols_or_null(s.data(), s.data() + s.size(), symbols);
    EXPECT_EQ(result, nullptr);
}

TEST_F(FindSymbolsTest, LastNotSymbols_SSE2_SingleChar_Match) {
    std::string s = " ";
    SearchSymbols symbols(" ");
    const char* result = find_last_not_symbols_or_null(s.data(), s.data() + s.size(), symbols);
    EXPECT_EQ(result, nullptr);
}

TEST_F(FindSymbolsTest, LastNotSymbols_SSE2_SingleChar_NoMatch) {
    std::string s = "x";
    SearchSymbols symbols(" ");
    const char* result = find_last_not_symbols_or_null(s.data(), s.data() + s.size(), symbols);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(*result, 'x');
    EXPECT_EQ(result - s.data(), 0);
}

TEST_F(FindSymbolsTest, LastNotSymbols_SSE2_MultipleSymbols) {
    // 3 trim chars (SSE2 path): space, tab, newline
    std::string s = "hello\t\n ";
    SearchSymbols symbols(" \t\n");
    const char* result = find_last_not_symbols_or_null(s.data(), s.data() + s.size(), symbols);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(*result, 'o');
    EXPECT_EQ(result - s.data(), 4);
}

TEST_F(FindSymbolsTest, LastNotSymbols_SSE2_FourSymbols) {
    // 4 trim chars (still SSE2 path)
    std::string s = "data.,-;";
    SearchSymbols symbols(".,-;");
    const char* result = find_last_not_symbols_or_null(s.data(), s.data() + s.size(), symbols);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(*result, 'a');
    EXPECT_EQ(result - s.data(), 3);
}

TEST_F(FindSymbolsTest, LastNotSymbols_SSE2_TrailingNonTrim) {
    // No trailing trim chars -> returns last char
    std::string s = "   hello";
    SearchSymbols symbols(" ");
    const char* result = find_last_not_symbols_or_null(s.data(), s.data() + s.size(), symbols);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(*result, 'o');
    EXPECT_EQ(result - s.data(), 7);
}

// --- SSE4.2 path: >= 5 trim characters ---

TEST_F(FindSymbolsTest, LastNotSymbols_SSE42_FiveSymbols) {
    // Exactly 5 trim chars -> SSE4.2 PCMPESTRI path
    std::string s = "hello.,;:-";
    SearchSymbols symbols(".,;:-");
    const char* result = find_last_not_symbols_or_null(s.data(), s.data() + s.size(), symbols);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(*result, 'o');
    EXPECT_EQ(result - s.data(), 4);
}

TEST_F(FindSymbolsTest, LastNotSymbols_SSE42_EightSymbols) {
    std::string s = "world \t\n\r.,-;";
    SearchSymbols symbols(" \t\n\r.,-;");
    const char* result = find_last_not_symbols_or_null(s.data(), s.data() + s.size(), symbols);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(*result, 'd');
    EXPECT_EQ(result - s.data(), 4);
}

TEST_F(FindSymbolsTest, LastNotSymbols_SSE42_AllMatch) {
    std::string s = " \t\n\r.";
    SearchSymbols symbols(" \t\n\r.");
    const char* result = find_last_not_symbols_or_null(s.data(), s.data() + s.size(), symbols);
    EXPECT_EQ(result, nullptr);
}

TEST_F(FindSymbolsTest, LastNotSymbols_SSE42_NoMatch) {
    std::string s = "helloworld";
    SearchSymbols symbols(".,;:-!");
    const char* result = find_last_not_symbols_or_null(s.data(), s.data() + s.size(), symbols);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(*result, 'd');
    EXPECT_EQ(result - s.data(), 9);
}

// --- Long strings: cross SIMD 16-byte boundaries ---

TEST_F(FindSymbolsTest, LastNotSymbols_SSE2_LongString) {
    // 50 spaces + "abc" + 50 spaces = 103 chars, crosses multiple SIMD chunks
    std::string s(50, ' ');
    s += "abc";
    s += std::string(50, ' ');
    SearchSymbols symbols(" ");
    const char* result = find_last_not_symbols_or_null(s.data(), s.data() + s.size(), symbols);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(*result, 'c');
    EXPECT_EQ(result - s.data(), 52);
}

TEST_F(FindSymbolsTest, LastNotSymbols_SSE42_LongString) {
    // Same test but with >= 5 trim chars to force SSE4.2
    std::string s(50, ' ');
    s += "abc";
    s += std::string(30, ' ');
    s += std::string(20, '\t');
    SearchSymbols symbols(" \t\n\r.;");
    const char* result = find_last_not_symbols_or_null(s.data(), s.data() + s.size(), symbols);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(*result, 'c');
    EXPECT_EQ(result - s.data(), 52);
}

TEST_F(FindSymbolsTest, LastNotSymbols_LongAllTrim) {
    // 200 spaces -> nullptr
    std::string s(200, ' ');
    SearchSymbols symbols(" ");
    const char* result = find_last_not_symbols_or_null(s.data(), s.data() + s.size(), symbols);
    EXPECT_EQ(result, nullptr);
}

TEST_F(FindSymbolsTest, LastNotSymbols_LongNoTrim) {
    // 200 'x' chars -> last char
    std::string s(200, 'x');
    SearchSymbols symbols(" ");
    const char* result = find_last_not_symbols_or_null(s.data(), s.data() + s.size(), symbols);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(*result, 'x');
    EXPECT_EQ(result - s.data(), 199);
}

// --- Edge: exactly 16 bytes (one SIMD chunk) ---

TEST_F(FindSymbolsTest, LastNotSymbols_Exactly16Bytes_AllTrim) {
    std::string s(16, ' ');
    SearchSymbols symbols(" ");
    const char* result = find_last_not_symbols_or_null(s.data(), s.data() + s.size(), symbols);
    EXPECT_EQ(result, nullptr);
}

TEST_F(FindSymbolsTest, LastNotSymbols_Exactly16Bytes_LastNonTrim) {
    std::string s(15, ' ');
    s += 'x';
    SearchSymbols symbols(" ");
    const char* result = find_last_not_symbols_or_null(s.data(), s.data() + s.size(), symbols);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(*result, 'x');
    EXPECT_EQ(result - s.data(), 15);
}

TEST_F(FindSymbolsTest, LastNotSymbols_Exactly16Bytes_FirstNonTrim) {
    std::string s = "x";
    s += std::string(15, ' ');
    SearchSymbols symbols(" ");
    const char* result = find_last_not_symbols_or_null(s.data(), s.data() + s.size(), symbols);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(*result, 'x');
    EXPECT_EQ(result - s.data(), 0);
}

// --- Edge: 17 bytes (one SIMD chunk + 1 scalar) ---

TEST_F(FindSymbolsTest, LastNotSymbols_17Bytes_NonTrimAtBoundary) {
    // 16 spaces + 'x' -> SIMD processes last 16 bytes [1..16], scalar handles byte 0
    std::string s(16, ' ');
    s += 'x';
    SearchSymbols symbols(" ");
    const char* result = find_last_not_symbols_or_null(s.data(), s.data() + s.size(), symbols);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(*result, 'x');
    EXPECT_EQ(result - s.data(), 16);
}

// --- Cross-verify: runtime SearchSymbols vs template version ---

TEST_F(FindSymbolsTest, LastNotSymbols_CrossVerify_WithTemplate) {
    // Compare runtime SearchSymbols version with compile-time template version
    std::string s = "hello   \t\n ";
    SearchSymbols symbols(" \t\n");

    const char* runtime_result =
            find_last_not_symbols_or_null(s.data(), s.data() + s.size(), symbols);
    const char* template_result =
            find_last_not_symbols_or_null<' ', '\t', '\n'>(s.data(), s.data() + s.size());

    ASSERT_NE(runtime_result, nullptr);
    ASSERT_NE(template_result, nullptr);
    EXPECT_EQ(runtime_result, template_result);
    EXPECT_EQ(*runtime_result, 'o');
}

TEST_F(FindSymbolsTest, LastNotSymbols_CrossVerify_AllTrim) {
    std::string s = "   \t\n";
    SearchSymbols symbols(" \t\n");

    const char* runtime_result =
            find_last_not_symbols_or_null(s.data(), s.data() + s.size(), symbols);
    const char* template_result =
            find_last_not_symbols_or_null<' ', '\t', '\n'>(s.data(), s.data() + s.size());

    EXPECT_EQ(runtime_result, nullptr);
    EXPECT_EQ(template_result, nullptr);
}

TEST_F(FindSymbolsTest, LastNotSymbols_CrossVerify_LongString) {
    std::string s(100, ' ');
    s += "data";
    s += std::string(100, '\t');
    SearchSymbols symbols(" \t");

    const char* runtime_result =
            find_last_not_symbols_or_null(s.data(), s.data() + s.size(), symbols);
    const char* template_result =
            find_last_not_symbols_or_null<' ', '\t'>(s.data(), s.data() + s.size());

    ASSERT_NE(runtime_result, nullptr);
    ASSERT_NE(template_result, nullptr);
    EXPECT_EQ(runtime_result, template_result);
    EXPECT_EQ(*runtime_result, 'a');
    EXPECT_EQ(runtime_result - s.data(), 103);
}

// --- Various string lengths to stress SIMD alignment ---

TEST_F(FindSymbolsTest, LastNotSymbols_VariousLengths) {
    SearchSymbols symbols("ab");
    for (size_t len = 1; len <= 64; ++len) {
        // String of 'a' repeated, with 'X' at position 0
        std::string s(len, 'a');
        s[0] = 'X';
        const char* result = find_last_not_symbols_or_null(s.data(), s.data() + s.size(), symbols);
        ASSERT_NE(result, nullptr) << "len=" << len;
        EXPECT_EQ(*result, 'X') << "len=" << len;
        EXPECT_EQ(result - s.data(), 0) << "len=" << len;
    }
}

TEST_F(FindSymbolsTest, LastNotSymbols_VariousLengths_NonTrimAtEnd) {
    SearchSymbols symbols("ab");
    for (size_t len = 1; len <= 64; ++len) {
        // String of 'a' repeated, with 'X' at the last position
        std::string s(len, 'a');
        s[len - 1] = 'X';
        const char* result = find_last_not_symbols_or_null(s.data(), s.data() + s.size(), symbols);
        ASSERT_NE(result, nullptr) << "len=" << len;
        EXPECT_EQ(*result, 'X') << "len=" << len;
        EXPECT_EQ(result - s.data(), (ptrdiff_t)(len - 1)) << "len=" << len;
    }
}

TEST_F(FindSymbolsTest, LastNotSymbols_VariousLengths_SSE42) {
    // 6 trim chars -> SSE4.2 path
    SearchSymbols symbols("abcdef");
    for (size_t len = 1; len <= 64; ++len) {
        std::string s(len, 'a');
        s[0] = 'X';
        const char* result = find_last_not_symbols_or_null(s.data(), s.data() + s.size(), symbols);
        ASSERT_NE(result, nullptr) << "len=" << len;
        EXPECT_EQ(*result, 'X') << "len=" << len;
        EXPECT_EQ(result - s.data(), 0) << "len=" << len;
    }
}

} // namespace doris
