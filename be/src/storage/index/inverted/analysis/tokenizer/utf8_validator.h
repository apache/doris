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

#include <cstddef>
#include <cstdint>
#include <string_view>

#include "common/logging.h"

namespace doris::segment_v2::inverted_index {

// The UTF-8 rule of the builtin standard and unicode parsers, CLucene's StringUtil::validate_utf8:
// the high bits of a lead byte announce one to three trailing bytes, every trailing byte is
// 10xxxxxx, and the code point a sequence decodes to is neither a surrogate nor above U+10FFFF.
// It accepts what strict UTF-8 refuses, the overlong forms: C0 80 for U+0000, F0 80 80 80. Every
// value strict UTF-8 accepts passes it too, so a strict check that passes decides alone.
inline bool lenient_utf8_valid(std::string_view text) {
    int32_t trailing = 0;
    uint32_t code_point = 0;
    for (const char ch : text) {
        const auto c = static_cast<uint8_t>(ch);
        if (trailing == 0) {
            if ((c & 0x80) == 0) {
                continue;
            }
            if ((c & 0xE0) == 0xC0) {
                code_point = c & 0x1F;
                trailing = 1;
            } else if ((c & 0xF0) == 0xE0) {
                code_point = c & 0x0F;
                trailing = 2;
            } else if ((c & 0xF8) == 0xF0) {
                code_point = c & 0x07;
                trailing = 3;
            } else {
                return false;
            }
            continue;
        }
        if ((c & 0xC0) != 0x80) {
            return false;
        }
        code_point = (code_point << 6) | (c & 0x3F);
        if (--trailing == 0 &&
            !(code_point < 0xD800 || (code_point >= 0xE000 && code_point <= 0x10FFFF))) {
            return false;
        }
    }
    return trailing == 0;
}

// The code point of the multi-byte sequence at `p` of a value lenient_utf8_valid() accepted, with
// its length in `*length`: the lead byte's high bits give the length and the low bits of every
// byte the code point, overlong forms included, as CLucene's standard scanner decoded: C1 81 is
// U+0041.
inline uint32_t decode_lenient_utf8(const uint8_t* p, size_t* length) {
    const uint8_t c = p[0];
    if ((c & 0xE0) == 0xC0) {
        *length = 2;
        return ((c & 0x1FU) << 6) | (p[1] & 0x3FU);
    }
    if ((c & 0xF0) == 0xE0) {
        *length = 3;
        return ((c & 0x0FU) << 12) | ((p[1] & 0x3FU) << 6) | (p[2] & 0x3FU);
    }
    DCHECK_EQ(c & 0xF8, 0xF0);
    *length = 4;
    return ((c & 0x07U) << 18) | ((p[1] & 0x3FU) << 12) | ((p[2] & 0x3FU) << 6) | (p[3] & 0x3FU);
}

} // namespace doris::segment_v2::inverted_index
