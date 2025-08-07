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

#include "vec/common/string_utils/string_utils.h"

#include <gtest/gtest.h>

#include "vec/functions/like.h"

namespace doris::vectorized {

class StringUtilsTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(StringUtilsTest, TestIsAscii) {
    // Test ASCII characters
    for (unsigned char c = 0; c < 0x80 - 1; ++c) {
        EXPECT_TRUE(is_ascii(static_cast<char>(c)))
                << "Failed for character with code " << static_cast<int>(c);
    }

    // Test non-ASCII characters
    for (unsigned char c = 0x80; c < 0xFF; ++c) {
        EXPECT_FALSE(is_ascii(static_cast<char>(c)))
                << "Failed for character with code " << static_cast<int>(c);
    }
}

TEST_F(StringUtilsTest, TestIsAlphaAscii) {
    // Test lowercase letters
    for (char c = 'a'; c <= 'z'; ++c) {
        EXPECT_TRUE(is_alpha_ascii(c)) << "Failed for character " << c;
    }

    // Test uppercase letters
    for (char c = 'A'; c <= 'Z'; ++c) {
        EXPECT_TRUE(is_alpha_ascii(c)) << "Failed for character " << c;
    }

    // Test non-alphabetic ASCII
    for (char c = 0; c < 'A'; ++c) {
        EXPECT_FALSE(is_alpha_ascii(c)) << "Failed for character with code " << static_cast<int>(c);
    }
    for (char c = 'Z' + 1; c < 'a'; ++c) {
        EXPECT_FALSE(is_alpha_ascii(c)) << "Failed for character " << c;
    }

    // Test non-ASCII characters
    for (unsigned char c = 0x80; c < 0xFF; ++c) {
        EXPECT_FALSE(is_alpha_ascii(static_cast<char>(c)))
                << "Failed for character with code " << static_cast<int>(c);
    }
}

TEST_F(StringUtilsTest, TestIsNumericAscii) {
    // Test digits
    for (char c = '0'; c <= '9'; ++c) {
        EXPECT_TRUE(is_numeric_ascii(c)) << "Failed for character " << c;
    }

    // Test non-digit ASCII
    for (char c = 0; c < '0'; ++c) {
        EXPECT_FALSE(is_numeric_ascii(c))
                << "Failed for character with code " << static_cast<int>(c);
    }

    // Test non-ASCII characters
    for (unsigned char c = 0x80; c < 0xFF; ++c) {
        EXPECT_FALSE(is_numeric_ascii(static_cast<char>(c)))
                << "Failed for character with code " << static_cast<int>(c);
    }
}

TEST_F(StringUtilsTest, TestIsAlphaNumericAscii) {
    // Test lowercase letters
    for (char c = 'a'; c <= 'z'; ++c) {
        EXPECT_TRUE(is_alpha_numeric_ascii(c)) << "Failed for character " << c;
    }

    // Test uppercase letters
    for (char c = 'A'; c <= 'Z'; ++c) {
        EXPECT_TRUE(is_alpha_numeric_ascii(c)) << "Failed for character " << c;
    }

    // Test digits
    for (char c = '0'; c <= '9'; ++c) {
        EXPECT_TRUE(is_alpha_numeric_ascii(c)) << "Failed for character " << c;
    }

    // Test non-alphanumeric ASCII
    for (char c = 0; c < '0'; ++c) {
        EXPECT_FALSE(is_alpha_numeric_ascii(c))
                << "Failed for character with code " << static_cast<int>(c);
    }
    for (char c = '9' + 1; c < 'A'; ++c) {
        EXPECT_FALSE(is_alpha_numeric_ascii(c))
                << "Failed for character with code " << static_cast<int>(c);
    }
    for (char c = 'Z' + 1; c < 'a'; ++c) {
        EXPECT_FALSE(is_alpha_numeric_ascii(c))
                << "Failed for character with code " << static_cast<int>(c);
    }

    // Test non-ASCII characters
    for (unsigned char c = 0x80; c < 0xFF; ++c) {
        EXPECT_FALSE(is_alpha_numeric_ascii(static_cast<char>(c)))
                << "Failed for character with code " << static_cast<int>(c);
    }
}

TEST_F(StringUtilsTest, TestIsWordCharAscii) {
    // Test lowercase letters
    for (char c = 'a'; c <= 'z'; ++c) {
        EXPECT_TRUE(is_word_char_ascii(c)) << "Failed for character " << c;
    }

    // Test uppercase letters
    for (char c = 'A'; c <= 'Z'; ++c) {
        EXPECT_TRUE(is_word_char_ascii(c)) << "Failed for character " << c;
    }

    // Test digits
    for (char c = '0'; c <= '9'; ++c) {
        EXPECT_TRUE(is_word_char_ascii(c)) << "Failed for character " << c;
    }

    // Test underscore
    EXPECT_TRUE(is_word_char_ascii('_')) << "Failed for underscore character";

    // Test non-word ASCII
    for (char c = 0; c < '0'; ++c) {
        if (c != '_') {
            EXPECT_FALSE(is_word_char_ascii(c))
                    << "Failed for character with code " << static_cast<int>(c);
        }
    }
    for (char c = '9' + 1; c < 'A'; ++c) {
        if (c != '_') {
            EXPECT_FALSE(is_word_char_ascii(c))
                    << "Failed for character with code " << static_cast<int>(c);
        }
    }
    for (char c = 'Z' + 1; c < 'a'; ++c) {
        if (c != '_') {
            EXPECT_FALSE(is_word_char_ascii(c))
                    << "Failed for character with code " << static_cast<int>(c);
        }
    }

    // Test non-ASCII characters
    for (unsigned char c = 0x80; c < 0xFF; ++c) {
        EXPECT_FALSE(is_word_char_ascii(static_cast<char>(c)))
                << "Failed for character with code " << static_cast<int>(c);
    }
}

TEST_F(StringUtilsTest, TestIsValidIdentifierBegin) {
    // Test lowercase letters
    for (char c = 'a'; c <= 'z'; ++c) {
        EXPECT_TRUE(is_valid_identifier_begin(c)) << "Failed for character " << c;
    }

    // Test uppercase letters
    for (char c = 'A'; c <= 'Z'; ++c) {
        EXPECT_TRUE(is_valid_identifier_begin(c)) << "Failed for character " << c;
    }

    // Test underscore
    EXPECT_TRUE(is_valid_identifier_begin('_')) << "Failed for underscore character";

    // Test digits (should return false)
    for (char c = '0'; c <= '9'; ++c) {
        EXPECT_FALSE(is_valid_identifier_begin(c)) << "Failed for character " << c;
    }

    // Test other ASCII characters
    for (char c = 0; c < '0'; ++c) {
        if (c != '_') {
            EXPECT_FALSE(is_valid_identifier_begin(c))
                    << "Failed for character with code " << static_cast<int>(c);
        }
    }
    for (char c = '9' + 1; c < 'A'; ++c) {
        if (c != '_') {
            EXPECT_FALSE(is_valid_identifier_begin(c))
                    << "Failed for character with code " << static_cast<int>(c);
        }
    }
    for (char c = 'Z' + 1; c < 'a'; ++c) {
        if (c != '_') {
            EXPECT_FALSE(is_valid_identifier_begin(c))
                    << "Failed for character with code " << static_cast<int>(c);
        }
    }

    // Test non-ASCII characters
    for (unsigned char c = 0x80; c < 0xFF; ++c) {
        EXPECT_FALSE(is_valid_identifier_begin(static_cast<char>(c)))
                << "Failed for character with code " << static_cast<int>(c);
    }
}

TEST_F(StringUtilsTest, replace_pattern_by_escape) {
    EXPECT_EQ(replace_pattern_by_escape(StringRef {"abcdef"}, 'A'), "abcdef");
    EXPECT_EQ(replace_pattern_by_escape(StringRef {"abc^%def"}, '^'), "abc\\%def");
    EXPECT_EQ(replace_pattern_by_escape(StringRef {"abc^^ef"}, '^'), "abc^ef");
    EXPECT_EQ(replace_pattern_by_escape(StringRef {"abc^^^ef"}, '^'), "abc^^ef");
    EXPECT_EQ(replace_pattern_by_escape(StringRef {"abc^^^_ef"}, '^'), "abc^\\_ef");
    EXPECT_EQ(replace_pattern_by_escape(StringRef {"abc^^^_^ef"}, '^'), "abc^\\_^ef");
    EXPECT_EQ(replace_pattern_by_escape(StringRef {"\\abc^^^_^ef"}, '^'), "\\\\abc^\\_^ef");
}
} // namespace doris::vectorized
