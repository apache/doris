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

#include "CharacterUtil.h"

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"

int32_t CharacterUtil::identifyCharType(int32_t rune) {
    if (rune >= '0' && rune <= '9') {
        return CHAR_ARABIC;
    }
    if ((rune >= 'a' && rune <= 'z') || (rune >= 'A' && rune <= 'Z')) {
        return CHAR_ENGLISH;
    }

    UBlockCode block = ublock_getCode(rune);

    if (block == UBLOCK_CJK_UNIFIED_IDEOGRAPHS || block == UBLOCK_CJK_COMPATIBILITY_IDEOGRAPHS ||
        block == UBLOCK_CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A ||
        block == UBLOCK_CJK_UNIFIED_IDEOGRAPHS_EXTENSION_B ||
        block == UBLOCK_CJK_UNIFIED_IDEOGRAPHS_EXTENSION_C ||
        block == UBLOCK_CJK_UNIFIED_IDEOGRAPHS_EXTENSION_D ||
        block == UBLOCK_CJK_UNIFIED_IDEOGRAPHS_EXTENSION_E ||
        block == UBLOCK_CJK_UNIFIED_IDEOGRAPHS_EXTENSION_F ||
        block == UBLOCK_CJK_UNIFIED_IDEOGRAPHS_EXTENSION_G) {
        return CHAR_CHINESE;
    }

    if (block == UBLOCK_HALFWIDTH_AND_FULLWIDTH_FORMS || block == UBLOCK_HANGUL_SYLLABLES ||
        block == UBLOCK_HANGUL_JAMO || block == UBLOCK_HANGUL_COMPATIBILITY_JAMO ||
        block == UBLOCK_HIRAGANA || block == UBLOCK_KATAKANA ||
        block == UBLOCK_KATAKANA_PHONETIC_EXTENSIONS) {
        return CHAR_OTHER_CJK;
    }

    if (rune > 0xFFFF) {
        return CHAR_SURROGATE;
    }

    return CHAR_USELESS;
}

int32_t CharacterUtil::regularize(int32_t rune, bool use_lowercase) {
    if (rune == 0x3000) {
        return 0x0020;
    } else if (rune > 0xFF00 && rune < 0xFF5F) {
        rune = rune - 0xFEE0;
    } else if (use_lowercase && rune >= 0x41 && rune <= 0x5A) {
        // This else-if causes full-width letters unable to be converted to lowercase
        rune += 32;
    }

    return rune;
}

void CharacterUtil::TypedRune::regularize(bool use_lowercase) {
    this->rune = CharacterUtil::regularize(this->rune, use_lowercase);
    this->char_type = CharacterUtil::identifyCharType(this->rune);
}

CharacterUtil::RuneStrLite CharacterUtil::decodeChar(const char* str, size_t length) {
    return cppjieba::DecodeRuneInString(str, length);
}

bool CharacterUtil::decodeString(const char* str, size_t length, RuneStrArray& runes) {
    return cppjieba::DecodeRunesInString(str, length, runes);
}

void CharacterUtil::decodeStringToRunes(char* str, size_t length, TypedRuneArray& typed_runes,
                                        bool use_lowercase) {
    typed_runes.clear();
    size_t byte_pos = 0;
    typed_runes.reserve(length);
    while (byte_pos < length) {
        RuneStrLite runeStr = decodeChar(str + byte_pos, length - byte_pos);
        if (runeStr.len == 0) {
            break;
        }
        if (runeStr.len == 1 && use_lowercase && str[byte_pos] >= 'A' && str[byte_pos] <= 'Z') {
            str[byte_pos] += 32;
        }
        typed_runes.emplace_back(runeStr.rune, byte_pos, runeStr.len, typed_runes.size(), 1);

        typed_runes.back().regularize(use_lowercase);

        byte_pos += runeStr.len;
    }
}

void CharacterUtil::regularizeString(std::string& input, bool use_lowercase) {
    std::string temp;
    size_t len = input.size();
    temp.reserve(len);
    for (size_t i = 0; i < len;) {
        unsigned char c = input[i];
        if ((c & 0xF0) == 0xE0 && i + 2 < len) {
            int rune = ((c & 0x0F) << 12) | ((input[i + 1] & 0x3F) << 6) | (input[i + 2] & 0x3F);
            if (rune == 0x3000) {
                temp += ' ';
            } else if (rune >= 0xFF01 && rune <= 0xFF5E) {
                char half = static_cast<char>(rune - 0xFEE0);
                if (use_lowercase && half >= 'A' && half <= 'Z') {
                    half += 32;
                }
                temp += half;
            } else {
                temp += input[i];
                temp += input[i + 1];
                temp += input[i + 2];
            }
            i += 3;
        } else {
            char ch = input[i];
            if (use_lowercase && ch >= 'A' && ch <= 'Z') {
                ch += 32;
            }
            temp += ch;
            i += 1;
        }
    }
    input = std::move(temp);
}
#include "common/compile_check_end.h"
} // namespace doris::segment_v2