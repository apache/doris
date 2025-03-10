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

#include "break_iterator_wrapper.h"

#include <unicode/unistr.h>

#include <mutex>
#include <string>

#include "icu_common.h"
#include "icu_tokenizer_config.h"

namespace doris::segment_v2 {

icu::UnicodeSet BreakIteratorWrapper::EMOJI_RK;
icu::UnicodeSet BreakIteratorWrapper::EMOJI;

BreakIteratorWrapper::BreakIteratorWrapper(icu::BreakIterator* rbbi) : rbbi_(rbbi) {}

void BreakIteratorWrapper::initialize() {
    static std::once_flag once_flag;
    std::call_once(once_flag, []() {
        UErrorCode status = U_ZERO_ERROR;
        EMOJI_RK.applyPattern("[*#0-9©®™〰〽]", status);
        if (U_FAILURE(status)) {
            std::string error_msg = "EMOJI RK failed to initialize: ";
            error_msg += u_errorName(status);
            _CLTHROWT(CL_ERR_IllegalArgument, error_msg.c_str());
        }
        EMOJI.applyPattern("[[:Emoji:][:Extended_Pictographic:]]", status);
        if (U_FAILURE(status)) {
            std::string error_msg = "EMOJI failed to initialize: ";
            error_msg += u_errorName(status);
            _CLTHROWT(CL_ERR_IllegalArgument, error_msg.c_str());
        }
    });
}

int32_t BreakIteratorWrapper::next() {
    int32_t current = rbbi_->current();
    int32_t next = rbbi_->next();
    status_ = calc_status(current, next);
    return next;
}

int32_t BreakIteratorWrapper::calc_status(int32_t current, int32_t next) {
    if (next != UBRK_DONE && is_emoji(current, next)) {
        return ICUTokenizerConfig::EMOJI_SEQUENCE_STATUS;
    } else {
        return rbbi_->getRuleStatus();
    }
}

bool BreakIteratorWrapper::is_emoji(int32_t current, int32_t next) {
    int32_t begin = start_ + current;
    int32_t end = start_ + next;
    UChar32 codepoint = 0;
    U16_GET(text_, 0, begin, end, codepoint);
    if (EMOJI.contains(codepoint)) {
        if (EMOJI_RK.contains(codepoint)) {
            int32_t trailer = begin + U16_LENGTH(codepoint);
            return trailer < end && (text_[trailer] == 0xFE0F || text_[trailer] == 0x20E3);
        } else {
            return true;
        }
    }
    return false;
}

void BreakIteratorWrapper::set_text(const UChar* text, int32_t start, int32_t length) {
    text_ = text;
    start_ = start;

    UErrorCode status = U_ZERO_ERROR;
    UTextPtr utext(utext_openUChars(nullptr, text + start, length, &status));
    if (U_FAILURE(status)) {
        std::string error_msg = "Failed to create UText: ";
        error_msg += u_errorName(status);
        _CLTHROWT(CL_ERR_Runtime, error_msg.c_str());
    }

    rbbi_->setText(utext.get(), status);
    if (U_FAILURE(status)) {
        std::string error_msg = "Failed to set text: ";
        error_msg += u_errorName(status);
        _CLTHROWT(CL_ERR_Runtime, error_msg.c_str());
    }

    status_ = UBRK_WORD_NONE;
}

} // namespace doris::segment_v2