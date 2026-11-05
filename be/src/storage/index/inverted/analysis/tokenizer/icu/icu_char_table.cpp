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

#include <unicode/uchar.h>
#include <unicode/unistr.h>
#include <unicode/uscript.h>

#include "common/check.h"

namespace doris::segment_v2::inverted_index {

const ICUCharTable& ICUCharTable::instance() {
    static const ICUCharTable table;
    return table;
}

int32_t ICUCharTable::script_of(UChar32 c) {
    UErrorCode status = U_ZERO_ERROR;
    const int32_t script = uscript_getScript(c, &status);
    // Fails only for a value that is no code point, and UTF-16 decodes to none.
    DORIS_CHECK(U_SUCCESS(status));
    if (script == USCRIPT_HAN || script == USCRIPT_HIRAGANA || script == USCRIPT_KATAKANA) {
        return USCRIPT_JAPANESE;
    }
    if (c >= 0xFF10 && c <= 0xFF19) {
        return USCRIPT_LATIN;
    }
    return script;
}

bool ICUCharTable::combining_mark_of(UChar32 c) {
    const auto type = static_cast<UCharCategory>(u_charType(c));
    return type == U_COMBINING_SPACING_MARK || type == U_NON_SPACING_MARK ||
           type == U_ENCLOSING_MARK;
}

ICUCharTable::ICUCharTable() {
    UErrorCode status = U_ZERO_ERROR;
    emoji_set.applyPattern(icu::UnicodeString::fromUTF8("[[:Emoji:][:Extended_Pictographic:]]"),
                           status);
    DORIS_CHECK(U_SUCCESS(status));
    emoji_rk_set.applyPattern(icu::UnicodeString::fromUTF8("[*#0-9©®™〰〽]"), status);
    DORIS_CHECK(U_SUCCESS(status));
    // Frozen sets answer contains() from several threads at once.
    emoji_set.freeze();
    emoji_rk_set.freeze();

    for (UChar32 c = 0; c < kBmpEnd; ++c) {
        scripts[c] = static_cast<int16_t>(script_of(c));
        const uint64_t bit = uint64_t {1} << (c & 63);
        if (combining_mark_of(c)) {
            combining_marks[c >> 6] |= bit;
        }
        if (emoji_set.contains(c)) {
            emoji[c >> 6] |= bit;
        }
        if (emoji_rk_set.contains(c)) {
            emoji_rk[c >> 6] |= bit;
        }
    }
}

} // namespace doris::segment_v2::inverted_index
