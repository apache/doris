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

#include "storage/index/inverted/analysis/tokenizer/icu/script_iterator.h"

#include <unicode/utf16.h>

#include <cstring>

namespace doris::segment_v2::inverted_index {

int32_t ScriptIterator::skip_ascii(const UChar* text, int32_t index, int32_t limit) {
    // 8 units, 16 bytes, at a time: none may have a bit above 0x7F.
    constexpr uint64_t kNotAscii = 0xFF80FF80FF80FF80ULL;
    while (index + 8 <= limit) {
        uint64_t low = 0;
        uint64_t high = 0;
        std::memcpy(&low, text + index, sizeof(low));
        std::memcpy(&high, text + index + 4, sizeof(high));
        if (((low | high) & kNotAscii) != 0) {
            break;
        }
        index += 8;
    }
    return index;
}

bool ScriptIterator::next() {
    if (script_limit_ >= limit_) {
        return false;
    }

    script_start_ = script_limit_;
    // The loop visits every character of the value, on locals: stores to members would make the
    // compiler reload the others on every character.
    const ICUCharTable& table = table_;
    const UChar* const text = text_;
    const int32_t start = start_;
    const int32_t limit = limit_;
    int32_t index = index_;
    int32_t code = USCRIPT_COMMON;
    while (index < limit) {
        if (code == USCRIPT_LATIN) {
            index = skip_ascii(text, index, limit);
            if (index == limit) {
                break;
            }
        }
        UChar32 ch = 0;
        U16_GET(text, start, index, limit, ch);
        const int32_t script = table.script(ch);
        // `|`, not `||`: in "the cat" a Latin run alternates letters and Common spaces, and the
        // short-circuit form mispredicted at every space. Only two real scripts that differ ask
        // ICU, and the answer ends the run unless the script extensions of the character name the
        // run's script: e.g. an Arabic-Indic digit, of the Arabic script, joins a Thaana run.
        const bool joins =
                (script == code) | (code <= USCRIPT_INHERITED) | (script <= USCRIPT_INHERITED);
        if (!joins && !uscript_hasScript(ch, static_cast<UScriptCode>(code)) &&
            !table.is_combining_mark(ch)) {
            break;
        }
        index += U16_LENGTH(ch);
        if (code <= USCRIPT_INHERITED && script > USCRIPT_INHERITED) {
            code = script;
        }
    }

    index_ = index;
    script_limit_ = index;
    script_code_ = code;
    return true;
}

void ScriptIterator::set_text(const UChar* text, int32_t start, int32_t length) {
    text_ = text;
    start_ = start;
    index_ = start;
    limit_ = start + length;
    script_start_ = start;
    script_limit_ = start;
    script_code_ = USCRIPT_INVALID_CODE;
}

} // namespace doris::segment_v2::inverted_index
