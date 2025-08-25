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

#include "script_iterator.h"

#include <unicode/unistr.h>

#include <mutex>
#include <string>

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"

std::vector<int32_t> ScriptIterator::k_basic_latin(128);

ScriptIterator::ScriptIterator(bool combine_cj) : combine_cj_(combine_cj) {}

void ScriptIterator::initialize() {
    static std::once_flag once_flag;
    std::call_once(once_flag, []() {
        UErrorCode status = U_ZERO_ERROR;
        for (int32_t i = 0; i < 128; i++) {
            k_basic_latin[i] = uscript_getScript(i, &status);
            if (U_FAILURE(status)) {
                std::string error_msg = "Get script failed: ";
                error_msg += u_errorName(status);
                _CLTHROWT(CL_ERR_IllegalArgument, error_msg.c_str());
            }
        }
        return k_basic_latin;
    });
}

bool ScriptIterator::next() {
    if (script_limit_ >= limit_) {
        return false;
    }

    script_code_ = USCRIPT_COMMON;
    script_start_ = script_limit_;

    while (index_ < limit_) {
        UChar32 ch = 0;
        U16_GET(text_, start_, index_, limit_, ch);
        int32_t script = get_script(ch);
        if (is_same_script(script_code_, script, ch) || is_combining_mark(ch)) {
            index_ += U16_LENGTH(ch);
            if (script_code_ <= USCRIPT_INHERITED && script > USCRIPT_INHERITED) {
                script_code_ = script;
            }
        } else {
            break;
        }
    }

    script_limit_ = index_;
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

int32_t ScriptIterator::get_script(UChar32 codepoint) const {
    if (0 <= codepoint && codepoint < 128) {
        return k_basic_latin[codepoint];
    } else {
        UErrorCode err = U_ZERO_ERROR;
        int32_t script = uscript_getScript(codepoint, &err);
        if (U_FAILURE(err)) {
            std::string error_msg = "Get Script error: ";
            error_msg += u_errorName(err);
            error_msg += ", script: " + std::to_string(script);
            _CLTHROWT(CL_ERR_Runtime, error_msg.c_str());
        }
        if (combine_cj_) {
            if (script == USCRIPT_HAN || script == USCRIPT_HIRAGANA || script == USCRIPT_KATAKANA) {
                return USCRIPT_JAPANESE;
            } else if (codepoint >= 0xFF10 && codepoint <= 0xFF19) {
                return USCRIPT_LATIN;
            } else {
                return script;
            }
        } else {
            return script;
        }
    }
}

bool ScriptIterator::is_same_script(int32_t current_script, int32_t script, UChar32 codepoint) {
    return (current_script == script) || (current_script <= USCRIPT_INHERITED) ||
           (script <= USCRIPT_INHERITED) ||
           uscript_hasScript(codepoint, (UScriptCode)current_script);
}

bool ScriptIterator::is_combining_mark(UChar32 codepoint) {
    auto type = (UCharCategory)u_charType(codepoint);
    return (type == U_COMBINING_SPACING_MARK || type == U_NON_SPACING_MARK ||
            type == U_ENCLOSING_MARK);
}

#include "common/compile_check_end.h"
} // namespace doris::segment_v2