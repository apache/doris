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

#pragma once

#include <unicode/umachine.h>
#include <unicode/uniset.h>

#include <array>
#include <cstdint>

namespace doris::segment_v2::inverted_index {

// The character properties the icu tokenizer asks for: the script of every character, whether a
// character is a combining mark, and whether a token starts with an emoji. ICU answers each with a
// call into its property tries or a UnicodeSet; for the BMP, where nearly all text lies, the
// answers are read from tables built once from those same calls, and only a supplementary code
// point still asks ICU. E.g. the script of 中 (U+4E2D) is scripts[0x4E2D], JAPANESE, and that of
// 𠀀 (U+20000) is script_of(0x20000), also JAPANESE.
//
// Built at first use and read-only afterwards, so every stream of every thread shares it.
struct ICUCharTable {
    static constexpr UChar32 kBmpEnd = 0x10000;
    using Bits = std::array<uint64_t, kBmpEnd / 64>;

    // script_of() of each BMP code point.
    std::array<int16_t, kBmpEnd> scripts {};
    // One bit per BMP code point c, bit c % 64 of word c / 64: whether combining_mark_of(c),
    // whether emoji_set has c, whether emoji_rk_set has c.
    Bits combining_marks {};
    Bits emoji {};
    Bits emoji_rk {};
    // [[:Emoji:][:Extended_Pictographic:]], what can make an emoji token.
    icu::UnicodeSet emoji_set;
    // The emoji that are text by default: an emoji token only when U+FE0F (emoji presentation) or
    // U+20E3 (keycap) follows, as in "#" U+FE0F U+20E3; "#" or "1" alone is no emoji.
    icu::UnicodeSet emoji_rk_set;

    static const ICUCharTable& instance();

    // The script the tokenizer groups `c` by: ICU's script, except that Han, Hiragana and Katakana
    // are all JAPANESE, so that the CJK dictionary breaks a run of mixed Chinese and Japanese
    // whole, and the full-width digits U+FF10..U+FF19 are LATIN.
    static int32_t script_of(UChar32 c);
    // A spacing, non-spacing or enclosing mark, which joins the script run before it.
    static bool combining_mark_of(UChar32 c);

    int32_t script(UChar32 c) const { return c < kBmpEnd ? scripts[c] : script_of(c); }
    bool is_combining_mark(UChar32 c) const {
        return c < kBmpEnd ? has(combining_marks, c) : combining_mark_of(c);
    }
    bool is_emoji(UChar32 c) const { return c < kBmpEnd ? has(emoji, c) : emoji_set.contains(c); }
    bool is_emoji_rk(UChar32 c) const {
        return c < kBmpEnd ? has(emoji_rk, c) : emoji_rk_set.contains(c);
    }

private:
    // Fills the table in place, inside the static of instance(): 152 KB is too much for the stack
    // of the thread that happens to use it first.
    ICUCharTable();

    static bool has(const Bits& bits, UChar32 c) { return ((bits[c >> 6] >> (c & 63)) & 1) != 0; }
};

} // namespace doris::segment_v2::inverted_index
