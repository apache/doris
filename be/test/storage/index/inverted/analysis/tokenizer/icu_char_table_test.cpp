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

#include "storage/index/inverted/analysis/tokenizer/icu/icu_char_table.h"

#include <gtest/gtest.h>
#include <unicode/uchar.h>
#include <unicode/uniset.h>
#include <unicode/unistr.h>
#include <unicode/uscript.h>

#include <cstdint>

namespace doris::segment_v2::inverted_index {
namespace {

// What ScriptIterator asked ICU for each character before the table: the script of an ASCII
// character as ICU gives it, and of any other with Han, Hiragana and Katakana as JAPANESE and the
// full-width digits as LATIN.
int32_t icu_script(UChar32 c) {
    UErrorCode status = U_ZERO_ERROR;
    const int32_t script = uscript_getScript(c, &status);
    EXPECT_TRUE(U_SUCCESS(status)) << c;
    if (c < 128) {
        return script;
    }
    if (script == USCRIPT_HAN || script == USCRIPT_HIRAGANA || script == USCRIPT_KATAKANA) {
        return USCRIPT_JAPANESE;
    }
    if (c >= 0xFF10 && c <= 0xFF19) {
        return USCRIPT_LATIN;
    }
    return script;
}

bool icu_combining_mark(UChar32 c) {
    const auto type = static_cast<UCharCategory>(u_charType(c));
    return type == U_COMBINING_SPACING_MARK || type == U_NON_SPACING_MARK ||
           type == U_ENCLOSING_MARK;
}

// A set from the pattern BreakIteratorWrapper built its emoji sets from before the table.
icu::UnicodeSet pattern_set(const char* pattern) {
    UErrorCode status = U_ZERO_ERROR;
    icu::UnicodeSet set(icu::UnicodeString::fromUTF8(pattern), status);
    EXPECT_TRUE(U_SUCCESS(status)) << pattern;
    return set;
}

bool has(const ICUCharTable::Bits& bits, UChar32 c) {
    return ((bits[c >> 6] >> (c & 63)) & 1) != 0;
}

TEST(ICUCharTableTest, HoldsWhatICUAnswersForEveryBmpCodePoint) {
    const ICUCharTable& table = ICUCharTable::instance();
    const icu::UnicodeSet emoji = pattern_set("[[:Emoji:][:Extended_Pictographic:]]");
    const icu::UnicodeSet emoji_rk = pattern_set("[*#0-9©®™〰〽]");
    for (UChar32 c = 0; c < ICUCharTable::kBmpEnd; ++c) {
        ASSERT_EQ(table.scripts[c], icu_script(c)) << c;
        ASSERT_EQ(has(table.combining_marks, c), icu_combining_mark(c)) << c;
        ASSERT_EQ(has(table.emoji, c), emoji.contains(c)) << c;
        ASSERT_EQ(has(table.emoji_rk, c), emoji_rk.contains(c)) << c;
    }
}

// The lookups read the tables for the BMP and ask ICU for the supplementary planes.
TEST(ICUCharTableTest, LooksUpWhatICUAnswersForEveryCodePoint) {
    const ICUCharTable& table = ICUCharTable::instance();
    const icu::UnicodeSet emoji = pattern_set("[[:Emoji:][:Extended_Pictographic:]]");
    const icu::UnicodeSet emoji_rk = pattern_set("[*#0-9©®™〰〽]");
    for (UChar32 c = 0; c <= 0x10FFFF; ++c) {
        ASSERT_EQ(table.script(c), icu_script(c)) << c;
        ASSERT_EQ(table.is_combining_mark(c), icu_combining_mark(c)) << c;
        ASSERT_EQ(table.is_emoji(c), emoji.contains(c)) << c;
        ASSERT_EQ(table.is_emoji_rk(c), emoji_rk.contains(c)) << c;
    }
}

// Why ScriptIterator may jump over ASCII in a Latin run: every ASCII character joins it.
TEST(ICUCharTableTest, EveryASCIICharacterIsLatinOrCommon) {
    const ICUCharTable& table = ICUCharTable::instance();
    for (UChar32 c = 0; c < 128; ++c) {
        const bool letter = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z');
        EXPECT_EQ(table.script(c), letter ? USCRIPT_LATIN : USCRIPT_COMMON) << c;
        EXPECT_FALSE(table.is_combining_mark(c)) << c;
    }
}

TEST(ICUCharTableTest, GroupsChineseAndJapaneseAsOneScript) {
    const ICUCharTable& table = ICUCharTable::instance();
    EXPECT_EQ(table.script(U'中'), USCRIPT_JAPANESE);
    EXPECT_EQ(table.script(U'あ'), USCRIPT_JAPANESE);
    EXPECT_EQ(table.script(U'カ'), USCRIPT_JAPANESE);
    EXPECT_EQ(table.script(0x20000), USCRIPT_JAPANESE);
    EXPECT_EQ(table.script(U'한'), USCRIPT_HANGUL);
    EXPECT_EQ(table.script(U'a'), USCRIPT_LATIN);
    EXPECT_EQ(table.script(U'０'), USCRIPT_LATIN);
    EXPECT_EQ(table.script(U'Ａ'), USCRIPT_LATIN);
    EXPECT_EQ(table.script(U'1'), USCRIPT_COMMON);
    EXPECT_EQ(table.script(0x0301), USCRIPT_INHERITED);
    EXPECT_TRUE(table.is_combining_mark(0x0301));
    EXPECT_TRUE(table.is_emoji(0x1F600));
    EXPECT_TRUE(table.is_emoji(U'#'));
    EXPECT_TRUE(table.is_emoji_rk(U'#'));
    EXPECT_FALSE(table.is_emoji_rk(0x1F600));
    EXPECT_FALSE(table.is_emoji(U'a'));
}

} // namespace
} // namespace doris::segment_v2::inverted_index
