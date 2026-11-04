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

#include "storage/index/inverted/analysis/tokenizer/basic/basic_tokenizer.h"

#include <unicode/uchar.h>
#include <unicode/utf8.h>

#include <algorithm>

#include "storage/index/inverted/analysis/common/rune.h"
#include "storage/index/inverted/analysis/scratch.h"

namespace doris::segment_v2::inverted_index {

#define IS_IN_RANGE(c, start, end) ((uint32_t)((c) - (start)) <= ((end) - (start)))

#define IS_CHINESE_CHAR(c)                                                   \
    (IS_IN_RANGE(c, 0x4E00, 0x9FFF) || IS_IN_RANGE(c, 0x3400, 0x4DBF) ||     \
     IS_IN_RANGE(c, 0x20000, 0x2A6DF) || IS_IN_RANGE(c, 0x2A700, 0x2EBEF) || \
     IS_IN_RANGE(c, 0x30000, 0x3134A))

namespace {

bool is_ascii_alnum(uint8_t c) {
    return (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z');
}

} // namespace

void BasicTokenizer::initialize(const std::string& extra_chars) {
    _extra_char_set.fill(false);
    _has_extra_chars = !extra_chars.empty();

    for (uint8_t c : extra_chars) {
        if (c < 128) {
            _extra_char_set[c] = true;
        }
    }
}

bool BasicTokenizer::next_token(TokenRef& token) {
    if (_buffer_index >= _tokens_text.size()) {
        return false;
    }
    const std::string_view token_text = _tokens_text[_buffer_index++];
    token = {.data = token_text.data(),
             .size = static_cast<uint32_t>(
                     std::min(token_text.size(), static_cast<size_t>(MAX_WORD_LEN))),
             .position_increment = 1};
    return true;
}

void BasicTokenizer::on_reset(std::string_view text) {
    _buffer_index = 0;
    _tokens_text.clear();
    release_oversized_scratch(_tokens_text);
    if (_has_extra_chars) {
        cut<true>(text);
    } else {
        cut<false>(text);
    }
}

template <bool HasExtraChars>
void BasicTokenizer::cut(std::string_view text) {
    const auto* s = reinterpret_cast<const uint8_t*>(text.data());
    const auto length = static_cast<int32_t>(text.size());

    for (int32_t i = 0; i < length;) {
        uint8_t firstByte = s[i];

        if (is_ascii_alnum(firstByte)) {
            int32_t start = i;
            while (i < length && is_ascii_alnum(s[i])) {
                i++;
            }
            _tokens_text.emplace_back(reinterpret_cast<const char*>(s + start), i - start);
        } else {
            UChar32 c = U_UNASSIGNED;
            const int32_t prev_i = i;

            U8_NEXT(s, i, length, c);
            if (c < 0) {
                continue;
            }

            if (IS_CHINESE_CHAR(c)) {
                const int32_t len = i - prev_i;
                _tokens_text.emplace_back(reinterpret_cast<const char*>(s + prev_i), len);
            }

            if constexpr (HasExtraChars) {
                if (c < 128 && _extra_char_set[c]) {
                    const int32_t len = i - prev_i;
                    _tokens_text.emplace_back(reinterpret_cast<const char*>(s + prev_i), len);
                }
            }
        }
    }
}

} // namespace doris::segment_v2::inverted_index
