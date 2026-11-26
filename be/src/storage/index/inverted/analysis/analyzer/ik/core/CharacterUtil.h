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

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "storage/index/inverted/analysis/common/rune.h"

namespace doris::segment_v2 {

class CharacterUtil {
public:
    enum class CharType : int32_t {
        USELESS = 0,
        ARABIC = 0x00000001,
        ENGLISH = 0x00000002,
        CHINESE = 0x00000004,
        OTHER_CJK = 0x00000008,
        SURROGATE = 0x00000016,
    };

    using RuneStrLite = inverted_index::RuneStrLite;
    using RuneStr = inverted_index::RuneStr;
    using RuneStrArray = inverted_index::RuneStrArray;

    class TypedRune : public RuneStr {
    public:
        CharType char_type;
        TypedRune() : RuneStr(), char_type(CharType::USELESS) {}
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

    // The type of `rune`: identifyCharTypeByBlock(), read from a table for the first plane, where
    // nearly every character of a text is.
    static CharType identifyCharType(int32_t rune) {
        if (rune >= 0 && rune < BmpCharTypes::kSize) {
            return static_cast<CharType>(BmpCharTypes::instance().types[rune]);
        }
        return identifyCharTypeByBlock(rune);
    }

    // The type of `rune` by its ASCII class or Unicode block: a binary search of ICU's blocks for
    // anything but an ASCII digit or letter.
    static CharType identifyCharTypeByBlock(int32_t rune);

    static void decodeStringToRunes(char* str, size_t length, TypedRuneArray& typed_runes,
                                    bool use_lowercase);

    static int32_t regularize(int32_t rune, bool use_lowercase);

    static RuneStrLite decodeChar(const char* str, size_t length);
    static bool decodeString(const char* str, size_t length, RuneStrArray& runes);

    static void regularizeCharInfo(TypedRune& type_rune, bool use_lowercase);

    // Folds U+3000 to a space and U+FF01 to U+FF5E to ASCII, and with `use_lowercase` lowercases
    // ASCII capitals, in place; returns the new length, which is at most `length`. A lead byte
    // from E0 to EF takes the next two bytes whatever they are, so E3 C0 80 folds to a space too.
    static size_t regularizeInPlace(char* data, size_t length, bool use_lowercase);

    // regularizeInPlace() of `input`.
    static void regularizeString(std::string& input, bool use_lowercase = true) {
        input.resize(regularizeInPlace(input.data(), input.size(), use_lowercase));
    }

private:
    // identifyCharTypeByBlock() of every first-plane character, built in place on first use, so
    // that its 64 KiB never land on a small stack.
    struct BmpCharTypes {
        static constexpr int32_t kSize = 0x10000;

        BmpCharTypes();

        static const BmpCharTypes& instance() {
            static const BmpCharTypes kInstance;
            return kInstance;
        }

        std::array<uint8_t, kSize> types;
    };
};

} // namespace doris::segment_v2
