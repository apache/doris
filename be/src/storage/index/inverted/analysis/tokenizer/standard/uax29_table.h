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

#include <array>
#include <cstddef>
#include <cstdint>

#include "common/compiler_util.h"
#include "storage/index/inverted/analysis/tokenizer/utf8_validator.h"

namespace doris::segment_v2::inverted_index {

// The longest match of the DFA from a token start: the text up to `end` matched `action`, or not
// one character fit and `action` is -1.
struct Uax29Match {
    size_t end = 0;
    int action = -1;
};

// The UAX#29 word-break DFA of StandardTokenizerImpl, the JFlex scanner both standard tokenizers
// come from, laid out flat with one byte per transition, so that a step is one load. It is built
// once from the scanner's ZZ_* tables, which CLucene's builtin scanner shares entry for entry, so
// neither tokenizer depends on a hand-copied table.
struct Uax29Table {
    static constexpr int kStates = 59;
    static constexpr int kClasses = 29;
    static constexpr uint8_t kDead = 0xFF; // no transition: the longest match ends
    // Attribute bits.
    static constexpr uint8_t kAccepting = 1; // a token may end in the state
    static constexpr uint8_t kFinal = 8;     // no transition leaves the state
    // The actions of the accepting states: what the text matched up to such a state is.
    enum Action : uint8_t {
        kSkip = 1,
        kNumeric = 2,
        kWord = 3,
        kEmoji = 4,
        kSouthEastAsian = 5,
        kHangul = 6,
        kIdeographic = 7,
        kKatakana = 8,
        kHiragana = 9,
    };
    // The two-level character map: the class of code point c is
    // cmap_blocks[cmap_top[c >> 8] | (c & 0xFF)].
    static constexpr size_t kCmapTop = 0x110000 >> 8;
    static constexpr size_t kCmapBlocks = 35328;

    std::array<uint8_t, kStates * kClasses> next {};
    std::array<uint8_t, kStates> attribute {};
    std::array<uint8_t, kStates> action {};
    std::array<uint8_t, 128> ascii_class {};
    std::array<uint16_t, kCmapTop> cmap_top {};
    std::array<uint8_t, kCmapBlocks> cmap_blocks {};
    uint8_t initial = 0;

    static const Uax29Table& instance();

    // The class of a code point up to U+10FFFF.
    uint8_t char_class(uint32_t code_point) const {
        return cmap_blocks[cmap_top[code_point >> 8] | (code_point & 0xFF)];
    }

    // The longest match from `start` of a value lenient_utf8_valid() accepted, the way the JFlex
    // scanner matched: it follows transitions while `fits(cur, length)` lets the character of
    // `length` bytes at `cur` join the token, and the token ends at the last accepting state it
    // passed. Every character leads from the initial state to an accepting one, so `action` is -1
    // only when the first character did not fit. E.g. with a window of 4 bytes, "ab中" matches
    // "ab": 中 would take the token to 5 bytes.
    template <typename Fits>
    ALWAYS_INLINE Uax29Match longest_match(const uint8_t* text, size_t size, size_t start,
                                           Fits fits) const {
        Uax29Match match {.end = start, .action = -1};
        size_t cur = start;
        uint8_t state = initial;
        while (cur < size) {
            const uint8_t c = text[cur];
            size_t length = 1;
            const uint8_t cls = c < 0x80 ? ascii_class[c]
                                         : char_class(decode_lenient_utf8(text + cur, &length));
            if (!fits(cur, length)) {
                break;
            }
            const uint8_t to = next[state * kClasses + cls];
            if (to == kDead) {
                break;
            }
            cur += length;
            state = to;
            const uint8_t attributes = attribute[to];
            if ((attributes & kAccepting) != 0) {
                match = {.end = cur, .action = action[to]};
                if ((attributes & kFinal) != 0) {
                    break;
                }
            }
        }
        return match;
    }

private:
    // Fills the table in place, inside the static of instance(): 46 KB is too much for the stack
    // of the thread that happens to use it first.
    Uax29Table();
};

} // namespace doris::segment_v2::inverted_index
