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

#include <unicode/umachine.h>
#include <unicode/utext.h>
#include <unicode/utf16.h>

#include <memory>

#include "common/compiler_util.h"
#include "storage/index/inverted/analysis/tokenizer/icu/icu_char_table.h"
#include "storage/index/inverted/analysis/tokenizer/icu/icu_common.h"
#include "storage/index/inverted/analysis/tokenizer/icu/icu_tokenizer_config.h"

namespace doris::segment_v2::inverted_index {

// A break iterator of one script, whose rule status after next() is EMOJI_SEQUENCE_STATUS when
// the text from the previous boundary starts with an emoji. next() runs for every boundary of the
// value, so it is inline and keeps the previous boundary itself instead of asking the iterator.
class BreakIteratorWrapper {
public:
    explicit BreakIteratorWrapper(icu::BreakIterator* rbbi);
    ~BreakIteratorWrapper() = default;

    int32_t get_rule_status() const { return status_; }

    // The next boundary in the range, or UBRK_DONE after the last one. Forced inline into
    // CompositeBreakIterator::next(), its one caller.
    ALWAYS_INLINE int32_t next() {
        const int32_t next = rbbi_->next();
        if (next != UBRK_DONE && is_emoji(position_, next)) {
            status_ = ICUTokenizerConfig::EMOJI_SEQUENCE_STATUS;
        } else {
            status_ = rbbi_->getRuleStatus();
        }
        if (next != UBRK_DONE) {
            position_ = next;
        }
        return next;
    }

    // Breaks [start, start + length) of `text` from now on. `utext` is reopened over that range
    // and may be reopened again right after: the iterator keeps a shallow clone of it, so one
    // UText serves every iterator of a stream and none is allocated per range.
    void set_text(UText* utext, const UChar* text, int32_t start, int32_t length);

private:
    // Whether the text [current, next) of the range starts with an emoji. "#" alone is no emoji,
    // "#" U+FE0F U+20E3 is. Forced inline: the compiler kept it out of line, a call for every
    // boundary.
    ALWAYS_INLINE bool is_emoji(int32_t current, int32_t next) const {
        int32_t begin = start_ + current;
        const int32_t end = start_ + next;
        UChar32 codepoint = 0;
        U16_GET(text_, 0, begin, end, codepoint);
        if (!table_.is_emoji(codepoint)) {
            return false;
        }
        if (table_.is_emoji_rk(codepoint)) {
            const int32_t trailer = begin + U16_LENGTH(codepoint);
            return trailer < end && (text_[trailer] == 0xFE0F || text_[trailer] == 0x20E3);
        }
        return true;
    }

    const ICUCharTable& table_;
    BreakIteratorPtr rbbi_;
    const UChar* text_ = nullptr;
    int32_t start_ = 0;
    // The last boundary, relative to the range: what the iterator's current() would say.
    int32_t position_ = 0;
    int32_t status_ = UBRK_WORD_NONE;
};
using BreakIteratorWrapperPtr = std::unique_ptr<BreakIteratorWrapper>;

} // namespace doris::segment_v2::inverted_index
