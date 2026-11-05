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

#include "storage/index/inverted/analysis/tokenizer/icu/icu_tokenizer.h"

#include <simdutf.h>
#include <unicode/ucasemap.h>
#include <unicode/ustring.h>
#include <unicode/utf16.h>

#include <algorithm>
#include <memory>
#include <string>

#include "common/check.h"
#include "common/exception.h"
#include "common/logging.h"
#include "storage/index/inverted/analysis/common/rune.h"
#include "storage/index/inverted/analysis/scratch.h"
#include "storage/index/inverted/analysis/token_batch.h"
#include "storage/index/inverted/analysis/tokenizer/byte_classifier.h"
#include "util/utf8_check.h"

namespace doris::segment_v2::inverted_index {

namespace {

// The case map LowerCaseFilter lowercases with. "" is the root locale whatever the process default
// is, so "I" becomes "i" even under a Turkish default, where it would become "ı". ICU lowercases
// with a const map, so every stream shares one.
const UCaseMap* icu_root_case_map() {
    static const UCaseMap* const case_map = [] {
        UErrorCode status = U_ZERO_ERROR;
        const UCaseMap* map = ucasemap_open("", 0, &status);
        DORIS_CHECK(U_SUCCESS(status));
        return map;
    }();
    return case_map;
}

// Writes the UTF-8 of `length` UTF-16 units and returns the end of it. The units pair every
// surrogate: invalid UTF-8 became U+FFFD, not an unpaired surrogate, and a term is never cut
// inside a pair.
char* encode_icu_term(const UChar* units, int32_t length, char* out) {
    for (int32_t i = 0; i < length; ++i) {
        const UChar c = units[i];
        if (c < 0x80) {
            *out++ = static_cast<char>(c);
        } else if (c < 0x800) {
            *out++ = static_cast<char>(0xC0 | (c >> 6));
            *out++ = static_cast<char>(0x80 | (c & 0x3F));
        } else if (!U16_IS_SURROGATE(c)) {
            *out++ = static_cast<char>(0xE0 | (c >> 12));
            *out++ = static_cast<char>(0x80 | ((c >> 6) & 0x3F));
            *out++ = static_cast<char>(0x80 | (c & 0x3F));
        } else {
            DCHECK(U16_IS_SURROGATE_LEAD(c) && i + 1 < length && U16_IS_TRAIL(units[i + 1]));
            const UChar32 code_point = U16_GET_SUPPLEMENTARY(c, units[++i]);
            *out++ = static_cast<char>(0xF0 | (code_point >> 18));
            *out++ = static_cast<char>(0x80 | ((code_point >> 12) & 0x3F));
            *out++ = static_cast<char>(0x80 | ((code_point >> 6) & 0x3F));
            *out++ = static_cast<char>(0x80 | (code_point & 0x3F));
        }
    }
    return out;
}

} // namespace

ICUTokenizer::ICUTokenizer(bool lowercase) : _lowercase(lowercase) {
    config_ = std::make_shared<DefaultICUTokenizerConfig>(/*myanmar_as_words=*/true);
    breaker_ = std::make_unique<CompositeBreakIterator>(config_);
}

void ICUTokenizer::initialize(const std::string& dictPath) {
    config_->initialize(dictPath);
}

void ICUTokenizer::reset(std::string_view text) {
    // No token until the value is bound, so a stream whose reset() threw makes none.
    _exhausted = true;
    _ascii = simdutf::validate_ascii(text.data(), text.size());
    // Malformed bytes become replacement characters and the rest of the value is still indexed;
    // strict mode rejects them.
    if (_strict_utf8 && !_ascii && !validate_utf8(text.data(), text.size())) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "ICU tokenizer input is not valid UTF-8");
    }
    if (static_cast<size_t>(buffer_.getCapacity()) * sizeof(UChar) >
        ANALYZER_SCRATCH_HIGH_WATER_BYTES) {
        buffer_ = icu::UnicodeString();
    }
    // A value has at most as many UTF-16 units as UTF-8 bytes: a unit takes at least one byte,
    // and a U+FFFD at least one invalid byte.
    const auto size = static_cast<int32_t>(text.size());
    UChar* units = buffer_.getBuffer(size);
    DORIS_CHECK(units != nullptr);
    int32_t length = size;
    if (_ascii) {
        std::ranges::transform(text, units,
                               [](char c) { return static_cast<UChar>(static_cast<uint8_t>(c)); });
    } else {
        // Valid UTF-8 has one UTF-16 form, which simdutf writes; it returns 0 for invalid UTF-8,
        // and a value that is not ASCII is not empty.
        length = static_cast<int32_t>(
                simdutf::convert_utf8_to_utf16(text.data(), text.size(), units));
        if (length == 0) {
            // What UnicodeString::fromUTF8() does: one U+FFFD for each maximal invalid
            // subsequence, e.g. "a\xE4\xB8b" becomes "a\uFFFDb".
            UErrorCode status = U_ZERO_ERROR;
            u_strFromUTF8WithSub(units, buffer_.getCapacity(), &length, text.data(), size, 0xFFFD,
                                 nullptr, &status);
            DORIS_CHECK(U_SUCCESS(status));
        }
    }
    buffer_.releaseBuffer(length);
    _text = text;
    _units = buffer_.getBuffer();
    _position = 0;
    breaker_->set_text(_units, 0, length);
    _exhausted = false;
}

bool ICUTokenizer::next_batch(TokenBatch& batch) {
    batch.clear();
    while (!_exhausted && batch.size() < kBatchTokens) {
        const int32_t end = breaker_->next();
        if (end == UBRK_DONE) {
            _exhausted = true;
        } else {
            // A boundary of rule status 0 ends no word: in "a, b" the text ", " is no token.
            if (breaker_->get_rule_status() != 0) {
                push_term(batch, _position, end);
            }
            _position = end;
        }
    }
    return batch.size() != 0;
}

void ICUTokenizer::push_term(TokenBatch& batch, int32_t start, int32_t end) {
    DCHECK_LT(start, end);
    const UChar* units = _units + start;
    int32_t length = std::min(end - start, MAX_WORD_LEN);
    if (length < end - start && U16_IS_LEAD(units[length - 1]) && U16_IS_TRAIL(units[length])) {
        --length;
    }
    if (_ascii) {
        const std::string_view term = _text.substr(start, length);
        if (!_lowercase || std::ranges::none_of(term, [](char c) {
                return is_ascii_upper(static_cast<uint8_t>(c));
            })) {
            batch.push(term, 1);
            return;
        }
        char* lowered = batch.allocate_term(term.size());
        std::ranges::transform(term, lowered, [](char c) { return ascii_lower(c); });
        batch.push({lowered, term.size()}, 1);
        return;
    }
    // The length of the UTF-8, and what lowercasing the term takes.
    size_t utf8_length = 0;
    bool ascii = true;
    bool caseless = true;
    for (int32_t i = 0; i < length; ++i) {
        const UChar c = units[i];
        ascii &= c < 0x80;
        caseless &= c >= 0x3000 && c <= 0x9FFF;
        // 1 byte below U+0080, 2 below U+0800, 3 above; a surrogate pair takes 2 + 2 bytes.
        utf8_length += 1 + static_cast<size_t>(c >= 0x80) +
                       static_cast<size_t>(c >= 0x800 && !U16_IS_SURROGATE(c));
    }
    if (_lowercase && !ascii && !caseless) {
        utf8Str_.resize(utf8_length);
        encode_icu_term(units, length, utf8Str_.data());
        push_lowered(batch);
        return;
    }
    char* term = batch.allocate_term(utf8_length);
    encode_icu_term(units, length, term);
    if (_lowercase && ascii) {
        std::transform(term, term + utf8_length, term, [](char c) { return ascii_lower(c); });
    }
    batch.push({term, utf8_length}, 1);
}

void ICUTokenizer::push_lowered(TokenBatch& batch) {
    DCHECK(validate_utf8(utf8Str_.data(), utf8Str_.size()));
    const auto size = static_cast<int32_t>(utf8Str_.size());
    char* lowered = batch.allocate_term(utf8Str_.size());
    UErrorCode status = U_ZERO_ERROR;
    int32_t length = ucasemap_utf8ToLower(icu_root_case_map(), lowered, size, utf8Str_.data(), size,
                                          &status);
    // The lowercase is longer, as for İ (U+0130, 2 bytes), which becomes U+0069 U+0307 (3
    // bytes); ICU said how long.
    if (status == U_BUFFER_OVERFLOW_ERROR) {
        lowered = batch.allocate_term(static_cast<size_t>(length));
        status = U_ZERO_ERROR;
        length = ucasemap_utf8ToLower(icu_root_case_map(), lowered, length, utf8Str_.data(), size,
                                      &status);
    }
    DORIS_CHECK(U_SUCCESS(status));
    batch.push({lowered, static_cast<size_t>(length)}, 1);
}

} // namespace doris::segment_v2::inverted_index
