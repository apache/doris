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

#include <unicode/ucasemap.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#ifdef BE_TEST
#include <atomic>
#endif

#include "common/cast_set.h"
#include "common/exception.h"
#include "storage/index/inverted/analysis/text_token_stream.h"
#include "storage/index/inverted/analysis/token_batch.h"
#include "util/utf8_check.h"

namespace doris::segment_v2::inverted_index {

#ifdef BE_TEST
namespace lower_case_testing {

inline std::atomic<uint64_t>& unicode_path_counter() {
    static std::atomic<uint64_t> counter {0};
    return counter;
}

inline uint64_t unicode_path_count() {
    return unicode_path_counter().load(std::memory_order_relaxed);
}

inline void reset_unicode_path_count() {
    unicode_path_counter().store(0, std::memory_order_relaxed);
}

inline void note_unicode_path() {
    unicode_path_counter().fetch_add(1, std::memory_order_relaxed);
}

} // namespace lower_case_testing
#endif

// Lowercases every letter of UTF-8 `value` with the root-locale mapping LowerCaseFilter uses, or
// only ASCII letters when ICU cannot open.
inline void unicode_lower(std::string& value) {
    if (value.empty()) {
        return;
    }
    static UCaseMap* const case_map = []() -> UCaseMap* {
        UErrorCode status = U_ZERO_ERROR;
        UCaseMap* map = ucasemap_open("", 0, &status);
        return U_SUCCESS(status) ? map : nullptr;
    }();
    if (case_map == nullptr) {
        for (char& c : value) {
            if (c >= 'A' && c <= 'Z') {
                c = static_cast<char>(c - 'A' + 'a');
            }
        }
        return;
    }
    std::string lowered(value.size() + 16, '\0');
    for (int attempt = 0; attempt < 2; ++attempt) {
        UErrorCode status = U_ZERO_ERROR;
        const int32_t length =
                ucasemap_utf8ToLower(case_map, lowered.data(), static_cast<int32_t>(lowered.size()),
                                     value.data(), static_cast<int32_t>(value.size()), &status);
        if (status == U_BUFFER_OVERFLOW_ERROR && length > 0) {
            lowered.resize(static_cast<size_t>(length));
            continue;
        }
        if (U_SUCCESS(status) && length >= 0) {
            lowered.resize(static_cast<size_t>(length));
            value = std::move(lowered);
        }
        return;
    }
}

// Lowercases every token where it stands in the batch, one token for one: ASCII byte by byte,
// anything else with the root-locale case mapping. A token that is not ASCII must be valid UTF-8.
// A lowercased term goes to the term storage of the batch, so the filter keeps no buffer of its
// own and its input's reset() and reserved_bytes() are its own.
class LowerCaseFilter final : public TextTokenFilter {
public:
    explicit LowerCaseFilter(std::unique_ptr<TextTokenStream> in)
            : TextTokenFilter(std::move(in)), _ucsm(nullptr, &ucasemap_close) {}

    void initialize() {
        UErrorCode status = U_ZERO_ERROR;
        auto* ucsm = ucasemap_open("", 0, &status);
        if (U_FAILURE(status)) {
            throw Exception(ErrorCode::INVERTED_INDEX_ANALYZER_ERROR,
                            "Failed to open UCaseMap. ICU Error: " + std::to_string(status) +
                                    " - " + u_errorName(status));
        }
        _ucsm.reset(ucsm);
    }

    // The input fills `batch` and the filter rewrites its tokens: a term that changes is replaced
    // by a view of its lowercase form in term storage, the rest keep their views.
    bool next_batch(TokenBatch& batch) override {
        if (!_in->next_batch(batch)) {
            return false;
        }
        for (TokenRef& token : batch.tokens()) {
            switch (needs_rewrite(token.term())) {
            case Rewrite::kNone:
                break;
            case Rewrite::kAscii:
                token.data = lower_ascii(token.term(), batch);
                break;
            case Rewrite::kUnicode:
                lower_unicode(token, batch);
                break;
            }
        }
        return true;
    }

private:
    enum class Rewrite : uint8_t {
        kNone,    // left as it is
        kAscii,   // lowercased byte by byte
        kUnicode, // validated, then lowercased by ICU
    };

    // Only kUnicode validates a term, so a term skips it only when it is valid UTF-8 for certain:
    //   61 62 63 "abc"   kNone     ASCII without a capital
    //   61 42 63 "aBc"   kAscii    ASCII with a capital
    //   E4 B8 AD "中"    kNone     one character of three bytes led by E3..E9: always valid, as
    //                              only the leads E0 and ED narrow the second byte, and within
    //                              U+3000..U+9FFF, where no character has a lowercase form
    //   E4 41 41         kUnicode  41 is no continuation byte, so validation throws
    //   C3 9C 62 65 72   kUnicode  "Über", lowercased to "über"
    // The single CJK character is what the standard tokenizer makes of Chinese text: one token per
    // ideograph.
    static Rewrite needs_rewrite(std::string_view term) {
        uint8_t high = 0;
        bool upper = false;
        for (const char c : term) {
            const auto byte = static_cast<uint8_t>(c);
            high |= byte;
            upper |= static_cast<uint8_t>(byte - 'A') < 26;
        }
        if (high < 0x80) {
            return upper ? Rewrite::kAscii : Rewrite::kNone;
        }
        if (term.size() == 3) {
            const auto lead = static_cast<uint8_t>(term[0]);
            const auto second = static_cast<uint8_t>(term[1]);
            const auto third = static_cast<uint8_t>(term[2]);
            if (lead >= 0xE3 && lead <= 0xE9 && (second & 0xC0) == 0x80 && (third & 0xC0) == 0x80) {
                return Rewrite::kNone;
            }
        }
        return Rewrite::kUnicode;
    }

    // The lowercase form of ASCII `term`, in the term storage of `batch`.
    static const char* lower_ascii(std::string_view term, TokenBatch& batch) {
        char* lower = batch.allocate_term(term.size());
        for (size_t i = 0; i < term.size(); ++i) {
            const auto byte = static_cast<uint8_t>(term[i]);
            lower[i] = static_cast<char>(byte >= 'A' && byte <= 'Z' ? byte + ('a' - 'A') : byte);
        }
        return lower;
    }

    // Validates the term of `token`, then points it at its lowercase form in the term storage of
    // `batch`. Lowercasing can make a term longer, e.g. U+0130 (C4 B0) becomes "i" and a combining
    // dot above (69 CC 87): ICU then reports the length it needs, and the term is lowercased again
    // into that many bytes.
    void lower_unicode(TokenRef& token, TokenBatch& batch) const {
#ifdef BE_TEST
        lower_case_testing::note_unicode_path();
#endif
        const std::string_view term = token.term();
        if (!validate_utf8(term.data(), term.size())) {
            throw Exception(ErrorCode::INVERTED_INDEX_ANALYZER_ERROR,
                            "Failed to lowercase token: invalid UTF-8");
        }
        char* lower = batch.allocate_term(term.size());
        UErrorCode status = U_ZERO_ERROR;
        int32_t result_len =
                ucasemap_utf8ToLower(_ucsm.get(), lower, cast_set<int32_t>(term.size()),
                                     term.data(), cast_set<int32_t>(term.size()), &status);
        if (status == U_BUFFER_OVERFLOW_ERROR) {
            lower = batch.allocate_term(cast_set<size_t>(result_len));
            status = U_ZERO_ERROR;
            result_len = ucasemap_utf8ToLower(_ucsm.get(), lower, result_len, term.data(),
                                              cast_set<int32_t>(term.size()), &status);
        }
        if (U_FAILURE(status)) {
            throw Exception(ErrorCode::INVERTED_INDEX_ANALYZER_ERROR,
                            "Failed to convert token to lowercase. ICU Error: {} - {}",
                            static_cast<int32_t>(status), u_errorName(status));
        }
        token.data = lower;
        token.size = cast_set<uint32_t>(result_len);
    }

    std::unique_ptr<UCaseMap, decltype(&ucasemap_close)> _ucsm;
};

} // namespace doris::segment_v2::inverted_index
