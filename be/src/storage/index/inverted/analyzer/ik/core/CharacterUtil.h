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

#include <unicode/uchar.h>

#include <functional>
#include <memory>
#include <vector>

#include "CLucene/_ApiHeader.h"
#include "CLucene/analysis/jieba/Unicode.hpp"

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"

class CharacterUtil {
public:
    static constexpr int32_t CHAR_USELESS = 0;            // Useless character
    static constexpr int32_t CHAR_ARABIC = 0x00000001;    // Arabic number
    static constexpr int32_t CHAR_ENGLISH = 0x00000002;   // English letter
    static constexpr int32_t CHAR_CHINESE = 0x00000004;   // Chinese character
    static constexpr int32_t CHAR_OTHER_CJK = 0x00000008; // Other CJK character
    static constexpr int32_t CHAR_SURROGATE = 0x00000016; // Surrogate pair character

    using RuneStrLite = cppjieba::RuneStrLite;
    using RuneStr = cppjieba::RuneStr;
    using RuneStrArray = cppjieba::RuneStrArray;

    class TypedRune : public RuneStr {
    public:
        int32_t char_type;
        TypedRune() : RuneStr(), char_type(0) {}
        TypedRune(int32_t in_rune, size_t in_offset, size_t in_len, size_t in_unicode_offset,
                  size_t in_unicode_length)
                : RuneStr(in_rune, static_cast<uint32_t>(in_offset), static_cast<uint32_t>(in_len),
                          static_cast<uint32_t>(in_unicode_offset),
                          static_cast<uint32_t>(in_unicode_length)),
                  char_type(CharacterUtil::identifyCharType(rune)) {}

        void init(const RuneStr& runeStr) {
            rune = runeStr.rune;
            offset = runeStr.offset;
            len = runeStr.len;
            unicode_offset = runeStr.unicode_offset;
            unicode_length = runeStr.unicode_length;
            char_type = CharacterUtil::identifyCharType(rune);
        }

        size_t getByteLength() const { return len; }
        int32_t getChar() const { return rune; }
        size_t getBytePosition() const { return offset; }

        size_t getNextBytePosition() const { return offset + len; }

        void regularize(bool use_lowercase = true);
    };

    using TypedRuneArray = std::vector<TypedRune>;

    static int32_t identifyCharType(int32_t rune);

    static void decodeStringToRunes(char* str, size_t length, TypedRuneArray& typed_runes,
                                    bool use_lowercase);

    static int32_t regularize(int32_t rune, bool use_lowercase);

    static RuneStrLite decodeChar(const char* str, size_t length);
    static bool decodeString(const char* str, size_t length, RuneStrArray& runes);

    static void regularizeCharInfo(TypedRune& type_rune, bool use_lowercase);

    static void regularizeString(std::string& input, bool use_lowercase = true);
};

#include "common/compile_check_end.h"
} // namespace doris::segment_v2
