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

#include <unicode/utf8.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <utility>
#include <vector>

namespace doris::segment_v2::inverted_index {

// The longest term the builtin tokenizers hand out, CLucene's LUCENE_MAX_WORD_LEN: in bytes for
// most of them, in UTF-16 code units for icu.
inline constexpr int32_t MAX_WORD_LEN = 255;

// The longest prefix of `text` that fits in `max_bytes` without splitting a rune, and its rune
// count.
inline std::pair<size_t, size_t> utf8_prefix_at_most(std::string_view text, size_t max_bytes) {
    const auto length = static_cast<int32_t>(text.size());
    const auto limit = static_cast<int32_t>(std::min(text.size(), max_bytes));
    int32_t offset = 0;
    size_t rune_count = 0;
    while (offset < length) {
        int32_t next = offset;
        U8_FWD_1(text, next, length);
        if (next > limit) {
            break;
        }
        offset = next;
        ++rune_count;
    }
    return {static_cast<size_t>(offset), rune_count};
}

// A decoded rune: its code point and length in bytes; a length of 0 means it could not be decoded.
struct RuneStrLite {
    uint32_t rune = 0;
    uint32_t len = 0;
};

// A rune of a decoded string: its code point, byte offset and length, and its index among the
// runes of the string together with how many runes it stands for.
struct RuneStr {
    uint32_t rune = 0;
    uint32_t offset = 0;
    uint32_t len = 0;
    uint32_t unicode_offset = 0;
    uint32_t unicode_length = 0;

    RuneStr() = default;
    RuneStr(uint32_t rune_value, uint32_t byte_offset, uint32_t byte_length, uint32_t rune_offset,
            uint32_t rune_length)
            : rune(rune_value),
              offset(byte_offset),
              len(byte_length),
              unicode_offset(rune_offset),
              unicode_length(rune_length) {}
};

using RuneStrArray = std::vector<RuneStr>;

// Decodes the rune `str` starts with by the rules jieba decodes with, which ik and its dictionary
// depend on: the lead byte alone sets the length, so a continuation byte as lead starts a two-byte
// rune, the continuation bytes are not checked, and a lead byte of 0xF8 and above, or one whose
// sequence `length` cuts short, decodes to length 0.
inline RuneStrLite decode_rune(const char* str, size_t length) {
    if (length == 0) {
        return {};
    }
    const auto lead = static_cast<uint8_t>(str[0]);
    if (lead < 0x80) {
        return {.rune = lead, .len = 1};
    }
    uint32_t rune = 0;
    uint32_t size = 0;
    if (lead <= 0xDF && length > 1) {
        rune = lead & 0x1FU;
        size = 2;
    } else if (lead <= 0xEF && length > 2) {
        rune = lead & 0x0FU;
        size = 3;
    } else if (lead <= 0xF7 && length > 3) {
        rune = lead & 0x07U;
        size = 4;
    } else {
        return {};
    }
    for (uint32_t i = 1; i < size; ++i) {
        rune = (rune << 6) | (static_cast<uint8_t>(str[i]) & 0x3FU);
    }
    return {.rune = rune, .len = size};
}

// Decodes every rune of `str` with decode_rune(). Returns false with no rune when one of them
// does not decode.
inline bool decode_runes(const char* str, size_t length, RuneStrArray& runes) {
    runes.clear();
    runes.reserve(length / 2);
    uint32_t rune_offset = 0;
    for (size_t offset = 0; offset < length;) {
        const RuneStrLite decoded = decode_rune(str + offset, length - offset);
        if (decoded.len == 0) {
            runes.clear();
            return false;
        }
        runes.emplace_back(decoded.rune, static_cast<uint32_t>(offset), decoded.len, rune_offset,
                           1);
        offset += decoded.len;
        ++rune_offset;
    }
    return true;
}

} // namespace doris::segment_v2::inverted_index
