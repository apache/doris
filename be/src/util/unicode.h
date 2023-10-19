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

#include <stdint.h>
#include <stdlib.h>

#include <ostream>
#include <string>
#include <vector>

namespace doris {
using std::string;
using std::string_view;
using std::vector;

using Rune = uint32_t;

struct RuneStr {
    Rune rune;
    uint32_t offset;
    uint32_t len;
    RuneStr() : rune(0), offset(0), len(0) {}
    RuneStr(Rune r, uint32_t o, uint32_t l) : rune(r), offset(o), len(l) {}
    RuneStr(Rune r, uint32_t o, uint32_t l, uint32_t unicode_offset, uint32_t unicode_length)
            : rune(r), offset(o), len(l) {}
}; // struct RuneStr

inline std::ostream& operator<<(std::ostream& os, const RuneStr& r) {
    return os << "{\"rune\": \"" << r.rune << "\", \"offset\": " << r.offset
              << ", \"len\": " << r.len << "}";
}

using Unicode = std::vector<Rune>;
using RuneStrArray = std::vector<struct RuneStr>;

struct WordRange {
    RuneStrArray::const_iterator left;
    RuneStrArray::const_iterator right;
    WordRange(RuneStrArray::const_iterator l, RuneStrArray::const_iterator r) : left(l), right(r) {}
    size_t Length() const { return right - left + 1; }
}; // struct WordRange

struct RuneStrLite {
    uint32_t rune;
    uint32_t len;
    RuneStrLite() : rune(0), len(0) {}
    RuneStrLite(uint32_t r, uint32_t l) : rune(r), len(l) {}
}; // struct RuneStrLite

inline RuneStrLite decode_rune_in_string(const char* str, size_t len) {
    RuneStrLite rp(0, 0);
    if (str == nullptr || len == 0) {
        return rp;
    }
    if (!(str[0] & 0x80)) { // 0xxxxxxx
        // 7bit, total 7bit
        rp.rune = (uint8_t)(str[0]) & 0x7f;
        rp.len = 1;
    } else if ((uint8_t)str[0] <= 0xdf && 1 < len) {
        // 110xxxxxx
        // 5bit, total 5bit
        rp.rune = (uint8_t)(str[0]) & 0x1f;

        // 6bit, total 11bit
        rp.rune <<= 6;
        rp.rune |= (uint8_t)(str[1]) & 0x3f;
        rp.len = 2;
    } else if ((uint8_t)str[0] <= 0xef && 2 < len) { // 1110xxxxxx
        // 4bit, total 4bit
        rp.rune = (uint8_t)(str[0]) & 0x0f;

        // 6bit, total 10bit
        rp.rune <<= 6;
        rp.rune |= (uint8_t)(str[1]) & 0x3f;

        // 6bit, total 16bit
        rp.rune <<= 6;
        rp.rune |= (uint8_t)(str[2]) & 0x3f;

        rp.len = 3;
    } else if ((uint8_t)str[0] <= 0xf7 && 3 < len) { // 11110xxxx
        // 3bit, total 3bit
        rp.rune = (uint8_t)(str[0]) & 0x07;

        // 6bit, total 9bit
        rp.rune <<= 6;
        rp.rune |= (uint8_t)(str[1]) & 0x3f;

        // 6bit, total 15bit
        rp.rune <<= 6;
        rp.rune |= (uint8_t)(str[2]) & 0x3f;

        // 6bit, total 21bit
        rp.rune <<= 6;
        rp.rune |= (uint8_t)(str[3]) & 0x3f;

        rp.len = 4;
    } else {
        rp.rune = 0;
        rp.len = 0;
    }
    return rp;
}

inline bool decode_runes_in_string(const char* s, size_t len, RuneStrArray* runes) {
    runes->reserve(len / 2);
    for (uint32_t i = 0, j = 0; i < len;) {
        RuneStrLite rp = decode_rune_in_string(s + i, len - i);
        if (rp.len == 0) {
            return false;
        }
        //RuneStr x(rp.rune, i, rp.len, j, 1);
        runes->emplace_back(RuneStr(rp.rune, i, rp.len, j, 1));
        i += rp.len;
        ++j;
    }
    return true;
}

/*inline bool decode_runes_in_string(const string& s, RuneStrArray& runes) {
    return decode_runes_in_string(s.c_str(), s.size(), runes);
}*/

inline std::string unicode_to_string(const Unicode& unicode) {
    std::string result;
    for (Rune rune : unicode) {
        // Assuming the rune is a valid Unicode code point,
        // convert it to a UTF-8 encoded string and append to the result.
        if (rune <= 0x7F) {
            result.push_back(static_cast<char>(rune));
        } else if (rune <= 0x7FF) {
            result.push_back(static_cast<char>(0xC0 | ((rune >> 6) & 0x1F)));
            result.push_back(static_cast<char>(0x80 | (rune & 0x3F)));
        } else if (rune <= 0xFFFF) {
            result.push_back(static_cast<char>(0xE0 | ((rune >> 12) & 0x0F)));
            result.push_back(static_cast<char>(0x80 | ((rune >> 6) & 0x3F)));
            result.push_back(static_cast<char>(0x80 | (rune & 0x3F)));
        } else {
            result.push_back(static_cast<char>(0xF0 | ((rune >> 18) & 0x07)));
            result.push_back(static_cast<char>(0x80 | ((rune >> 12) & 0x3F)));
            result.push_back(static_cast<char>(0x80 | ((rune >> 6) & 0x3F)));
            result.push_back(static_cast<char>(0x80 | (rune & 0x3F)));
        }
    }
    return result;
}

inline bool decode_runes_in_string(const char* s, size_t len, Unicode& unicode) {
    unicode.clear();
    RuneStrArray runes;
    if (!decode_runes_in_string(s, len, &runes)) {
        return false;
    }
    unicode.reserve(runes.size());
    for (size_t i = 0; i < runes.size(); i++) {
        unicode.push_back(runes[i].rune);
    }
    return true;
}

inline bool is_single_word(const string& str) {
    RuneStrLite rp = decode_rune_in_string(str.c_str(), str.size());
    return rp.len == str.size();
}

inline string_view get_string_view_from_runes(const char* s, RuneStrArray::const_iterator left,
                                              RuneStrArray::const_iterator right) {
    uint32_t len = right->offset - left->offset + right->len;
    return string_view(s + left->offset, len);
}

} // namespace doris
