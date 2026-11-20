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

TEST(FindSymbolsTest, ForwardAndReverseAtVectorBoundaries) {
    for (size_t length = 0; length <= 65; ++length) {
        // Start one byte into the buffer and put matching bytes just outside the range.
        std::string input(length + 2, 'x');
        input.front() = input.back() = '.';
        char* begin = input.data() + 1;
        char* end = begin + length;
        EXPECT_EQ(end, find_first_symbols<'.'>(begin, end));
        EXPECT_EQ(nullptr, find_last_symbols_or_null<'.'>(begin, end));
        for (char* pos = begin; pos != end; ++pos) {
            *pos = '.';
            EXPECT_EQ(pos, find_first_symbols<'.'>(begin, end));
            EXPECT_EQ(pos, find_last_symbols_or_null<'.'>(begin, end));
            *pos = 'x';
        }
    }
}

TEST(FindSymbolsTest, NegativeSearchAtEveryPosition) {
    for (size_t length = 0; length <= 65; ++length) {
        std::string input(length, 'x');
        char* begin = input.data();
        char* end = begin + length;
        EXPECT_EQ(end, (find_first_not_symbols<'x', '\0'>(begin, end)));
        EXPECT_EQ(nullptr, (find_last_not_symbols_or_null<'x', '\0'>(begin, end)));
        for (char* pos = begin; pos != end; ++pos) {
            *pos = '.';
            EXPECT_EQ(pos, (find_first_not_symbols<'x', '\0'>(begin, end)));
            EXPECT_EQ(pos, (find_last_not_symbols_or_null<'x', '\0'>(begin, end)));
            *pos = 'x';
        }
    }
}

TEST(FindSymbolsTest, CharacterSetsAndEmbeddedZero) {
    std::string input(33, '.');
    input[15] = '/';
    input[16] = '\0';
    input[31] = '/';
    const char* begin = input.data();
    const char* end = begin + input.size();

    EXPECT_EQ(begin + 15, find_first_symbols<'/'>(begin, end));
    EXPECT_EQ(begin + 31, find_last_symbols_or_null<'/'>(begin, end));
    EXPECT_EQ(begin + 16, (find_first_not_symbols<'.', '/'>(begin, end)));
    EXPECT_EQ(begin + 16, (find_last_not_symbols_or_null<'.', '/'>(begin, end)));
    EXPECT_EQ(end, find_first_symbols<>(begin, end));
    EXPECT_EQ(nullptr, find_last_symbols_or_null<>(begin, end));
}

TEST(FindSymbolsTest, EarlyMatchesTakePrecedence) {
    for (size_t length : {8, 15, 16, 17, 31, 32, 33, 64}) {
        std::string input(length, '.');
        const char* begin = input.data();
        const char* end = begin + length;
        for (size_t skipped = 0; skipped < 4; ++skipped) {
            EXPECT_EQ(begin + skipped, (find_first_symbols<'.', '/'>(begin, end)));
            EXPECT_EQ(end - skipped - 1, (find_last_symbols_or_null<'.', '/'>(begin, end)));
            input[skipped] = input[length - skipped - 1] = 'x';
        }
    }
}

} // namespace doris
