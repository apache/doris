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
#include <unicode/unistr.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

#include "storage/index/inverted/analysis/text_token_stream.h"
#include "storage/index/inverted/analysis/tokenizer/icu/composite_break_iterator.h"
#include "storage/index/inverted/analysis/tokenizer/icu/default_icu_tokenizer_config.h"
#include "storage/index/inverted/analysis/tokenizer/icu/icu_common.h"

namespace doris::segment_v2::inverted_index {

// The words of the value by ICU's per-script break rules, at most MAX_WORD_LEN UTF-16 code units
// each: a longer word is cut there, one unit earlier when the cut would split a surrogate pair,
// and the rest of it is dropped. The value is read as UTF-16, invalid UTF-8 replaced by U+FFFD as
// UnicodeString::fromUTF8() replaces it; in strict mode it throws instead.
//
// With `lowercase`, which only the builtin icu parser asks for, every term is what LowerCaseFilter
// made of it: an ASCII term lowercased byte by byte, a term of only U+3000..U+9FFF (CJK
// punctuation, kana, ideographs, which have no case) left as it is, and any other term lowercased
// by ICU's root-locale case mapping, which may lengthen it: "İstanbul" becomes "i" U+0307
// "stanbul".
//
// A term of an ASCII value is a view of the value; any other term is encoded from the UTF-16 copy
// straight into the batch.
class ICUTokenizer final : public TextTokenizer {
public:
    explicit ICUTokenizer(bool lowercase = false);

    void initialize(const std::string& dictPath);

    void reset(std::string_view text) override;
    bool next_batch(TokenBatch& batch) override;

    // Strict: invalid UTF-8 throws instead of becoming U+FFFD.
    void set_strict_utf8(bool strict) override { _strict_utf8 = strict; }

    // The UTF-16 copy of the value, released above the high water mark, and the scratch of a term
    // ICU lowercases, which the term limit bounds to 765 bytes.
    size_t reserved_bytes() const override {
        return static_cast<size_t>(buffer_.getCapacity()) * sizeof(UChar) + utf8Str_.capacity();
    }

private:
    // The term of the word [start, end) of the UTF-16 copy.
    void push_term(TokenBatch& batch, int32_t start, int32_t end);
    // The root-locale lowercase of utf8Str_.
    void push_lowered(TokenBatch& batch);

    const bool _lowercase;
    bool _strict_utf8 = false;
    // No value is bound, or the last word was handed out.
    bool _exhausted = true;
    // The last boundary the breaker returned, where the next word may start.
    int32_t _position = 0;
    // The value, and whether it is ASCII: then the UTF-8 of every term is the value's bytes at the
    // offsets of its UTF-16 units.
    std::string_view _text;
    bool _ascii = false;
    // The UTF-16 copy of the value, and its units.
    icu::UnicodeString buffer_;
    const UChar* _units = nullptr;
    std::string utf8Str_;

    ICUTokenizerConfigPtr config_;
    CompositeBreakIteratorPtr breaker_;
};

} // namespace doris::segment_v2::inverted_index
