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

#include "storage/index/inverted/analysis/analyzer/ik/core/CharacterUtil.h"

namespace doris::segment_v2 {

CharacterUtil::BmpCharTypes::BmpCharTypes() {
    for (int32_t rune = 0; rune < kSize; ++rune) {
        types[rune] = static_cast<uint8_t>(identifyCharTypeByBlock(rune));
    }
}

CharacterUtil::CharType CharacterUtil::identifyCharTypeByBlock(int32_t rune) {
    if (rune >= '0' && rune <= '9') {
        return CharType::ARABIC;
    }
    if ((rune >= 'a' && rune <= 'z') || (rune >= 'A' && rune <= 'Z')) {
        return CharType::ENGLISH;
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
        return CharType::CHINESE;
    }

    if (block == UBLOCK_HALFWIDTH_AND_FULLWIDTH_FORMS || block == UBLOCK_HANGUL_SYLLABLES ||
        block == UBLOCK_HANGUL_JAMO || block == UBLOCK_HANGUL_COMPATIBILITY_JAMO ||
        block == UBLOCK_HIRAGANA || block == UBLOCK_KATAKANA ||
        block == UBLOCK_KATAKANA_PHONETIC_EXTENSIONS) {
        return CharType::OTHER_CJK;
    }

    if (rune > 0xFFFF) {
        return CharType::SURROGATE;
    }

    return CharType::USELESS;
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
    return inverted_index::decode_rune(str, length);
}

bool CharacterUtil::decodeString(const char* str, size_t length, RuneStrArray& runes) {
    return inverted_index::decode_runes(str, length, runes);
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
        // The rune is regularized before its type is taken, as TypedRune::regularize() does.
        typed_runes.emplace_back(regularize(static_cast<int32_t>(runeStr.rune), use_lowercase),
                                 byte_pos, runeStr.len, typed_runes.size(), 1);

        byte_pos += runeStr.len;
    }
}

size_t CharacterUtil::regularizeInPlace(char* data, size_t length, bool use_lowercase) {
    // U+3000 is E3 80 80 and U+FF01 to U+FF5E are EF BC 81 to EF BD 9E, so only a three-byte group
    // led by E3 or EF can fold, and with `use_lowercase` an ASCII capital lower; a word without
    // them comes back byte for byte. 数据库 is E6 95 B0 E6 8D AE E5 BA 93 and stays as it is,
    // ＳＱＬ (EF BC B3 ...) and Doris go the full way.
    bool may_change = false;
    for (size_t i = 0; i < length; ++i) {
        const auto byte = static_cast<uint8_t>(data[i]);
        if (byte == 0xE3 || byte == 0xEF || (use_lowercase && byte >= 'A' && byte <= 'Z')) {
            may_change = true;
            break;
        }
    }
    if (!may_change) {
        return length;
    }
    // Never longer than what it reads, so the folded bytes can go where the word was: a group
    // that folds writes one byte for three, every other byte writes itself.
    size_t out = 0;
    for (size_t i = 0; i < length;) {
        const auto c = static_cast<unsigned char>(data[i]);
        if ((c & 0xF0) == 0xE0 && i + 2 < length) {
            const char second = data[i + 1];
            const char third = data[i + 2];
            int rune = ((c & 0x0F) << 12) | ((second & 0x3F) << 6) | (third & 0x3F);
            if (rune == 0x3000) {
                data[out++] = ' ';
            } else if (rune >= 0xFF01 && rune <= 0xFF5E) {
                char half = static_cast<char>(rune - 0xFEE0);
                if (use_lowercase && half >= 'A' && half <= 'Z') {
                    half += 32;
                }
                data[out++] = half;
            } else {
                data[out++] = static_cast<char>(c);
                data[out++] = second;
                data[out++] = third;
            }
            i += 3;
        } else {
            char ch = data[i];
            if (use_lowercase && ch >= 'A' && ch <= 'Z') {
                ch += 32;
            }
            data[out++] = ch;
            i += 1;
        }
    }
    return out;
}
} // namespace doris::segment_v2
