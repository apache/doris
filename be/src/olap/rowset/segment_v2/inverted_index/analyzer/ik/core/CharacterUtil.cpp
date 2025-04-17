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

int32_t CharacterUtil::identifyCharType(int32_t rune) {
    // Numbers
    if (rune >= 0x30 && rune <= 0x39) {
        return CHAR_ARABIC;
    }

    // English
    if ((rune >= 0x61 && rune <= 0x7a) || (rune >= 0x41 && rune <= 0x5a)) {
        return CHAR_ENGLISH;
    }

    // CJK Unified Chinese Characters
    if ((rune >= 0x4E00 && rune <= 0x9FFF) || (rune >= 0x3400 && rune <= 0x4DBF) ||
        (rune >= 0x20000 && rune <= 0x2A6DF) || (rune >= 0x2A700 && rune <= 0x2B73F) ||
        (rune >= 0x2B740 && rune <= 0x2B81F) || (rune >= 0x2B820 && rune <= 0x2CEAF) ||
        (rune >= 0x2CEB0 && rune <= 0x2EBEF) || (rune >= 0x30000 && rune <= 0x3134F)) {
        return CHAR_CHINESE;
    }

    // Japanese and Korean characters
    if ((rune >= 0x3040 && rune <= 0x309F) || (rune >= 0x30A0 && rune <= 0x30FF) ||
        (rune >= 0x31F0 && rune <= 0x31FF) || (rune >= 0xAC00 && rune <= 0xD7AF) ||
        (rune >= 0x1100 && rune <= 0x11FF)) {
        return CHAR_OTHER_CJK;
    }

    // UTF-16 surrogate pairs and private zone
    if ((rune >= 0xD800 && rune <= 0xDBFF) || (rune >= 0xDC00 && rune <= 0xDFFF) ||
        (rune >= 0xE000 && rune <= 0xF8FF)) {
        return CHAR_SURROGATE;
    }

    return CHAR_USELESS;
}

int32_t CharacterUtil::regularize(int32_t rune, bool use_lowercase) {
    // Full-width to half-width
    if (rune == 0x3000) {
        return 0x0020; // Convert full-width space to half-width
    }

    // Full-width numbers
    if (rune >= 0xFF10 && rune <= 0xFF19) {
        return rune - 0xFEE0; // Convert to half-width numbers
    }

    // Full-width letters
    if (rune >= 0xFF21 && rune <= 0xFF3A) {
        rune = rune - 0xFEE0;
        if (use_lowercase) {
            rune += 32; // Convert to lowercase
        }
        return rune;
    }
    if (rune >= 0xFF41 && rune <= 0xFF5A) {
        return rune - 0xFEE0;
    }

    // Convert half-width uppercase letters to lowercase
    if (use_lowercase && rune >= 0x41 && rune <= 0x5A) {
        return rune + 32;
    }

    return rune;
}

void CharacterUtil::TypedRune::regularize(bool use_lowercase) {
    CharacterUtil::regularizeCharInfo(*this, use_lowercase);
}

void CharacterUtil::regularizeCharInfo(TypedRune& typedRune, bool use_lowercase) {
    typedRune.rune = regularize(typedRune.rune, use_lowercase);
}

CharacterUtil::RuneStrLite CharacterUtil::decodeChar(const char* str, size_t length) {
    return cppjieba::DecodeRuneInString(str, length);
}

bool CharacterUtil::decodeString(const char* str, size_t length, RuneStrArray& runes) {
    return cppjieba::DecodeRunesInString(str, length, runes);
}

void CharacterUtil::decodeStringToRunes(const char* str, size_t length, TypedRuneArray& typed_runes,
                                        bool use_lowercase) {
    typed_runes.clear();
    size_t byte_pos = 0;
    typed_runes.reserve(length);
    while (byte_pos < length) {
        RuneStrLite runeStr = decodeChar(str + byte_pos, length - byte_pos);
        if (runeStr.len == 0) {
            break;
        }
        typed_runes.emplace_back(runeStr.rune, byte_pos, runeStr.len, typed_runes.size(), 1);

        if (use_lowercase) {
            typed_runes.back().regularize(true);
        }
        byte_pos += runeStr.len;
    }
}

// TODO: Maybe delete this function
size_t CharacterUtil::adjustToCompleteChar(const char* buffer, size_t buffer_length) {
    if (buffer_length == 0) return 0;

    unsigned char last_byte = buffer[buffer_length - 1];

    if (last_byte < 0x80) {
        return buffer_length;
    }

    if ((last_byte & 0xC0) == 0x80) {
        size_t adjustedLen = buffer_length - 1;
        while (adjustedLen > 0) {
            unsigned char byte = buffer[adjustedLen - 1];
            if ((byte & 0xC0) != 0x80) {
                int charLen = 0;
                if ((byte & 0xE0) == 0xC0)
                    charLen = 2;
                else if ((byte & 0xF0) == 0xE0)
                    charLen = 3;
                else if ((byte & 0xF8) == 0xF0)
                    charLen = 4;
                if (buffer_length - adjustedLen + 1 < charLen) {
                    return adjustedLen - 1;
                }
                return buffer_length;
            }
            adjustedLen--;
        }
        return 0;
    }

    int charLen = 0;
    if ((last_byte & 0xE0) == 0xC0)
        charLen = 2;
    else if ((last_byte & 0xF0) == 0xE0)
        charLen = 3;
    else if ((last_byte & 0xF8) == 0xF0)
        charLen = 4;

    if (charLen > 1) {
        return buffer_length - 1;
    }

    return buffer_length;
}
} // namespace doris::segment_v2