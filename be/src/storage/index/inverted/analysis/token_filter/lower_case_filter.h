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

#include <memory>
#include <string>
#include <string_view>

#ifdef BE_TEST
#include <atomic>
#endif

#include "common/cast_set.h"
#include "common/exception.h"
#include "storage/index/inverted/analysis/per_token_stream.h"
#include "storage/index/inverted/analysis/scratch.h"
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

// Lowercases every token: ASCII byte by byte, anything else with the root-locale case mapping.
// A token that is not ASCII must be valid UTF-8.
class LowerCaseFilter final : public PerTokenFilter {
public:
    explicit LowerCaseFilter(std::unique_ptr<TextTokenStream> in)
            : PerTokenFilter(std::move(in)), _ucsm(nullptr, &ucasemap_close) {}

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

    // The scratch of one lowercased token, and the input.
    size_t reserved_bytes() const override {
        return PerTokenFilter::reserved_bytes() + _lower_term.capacity();
    }

protected:
    void on_reset() override { release_oversized_scratch(_lower_term); }

    bool next_token(TokenRef& token) override {
        if (!input_token(token)) {
            return false;
        }
        const std::string_view term = token.term();
        bool has_ascii_upper = false;
        bool all_ascii = true;
        for (const char value : term) {
            const auto byte = static_cast<uint8_t>(value);
            if (byte >= 0x80) {
                all_ascii = false;
                break;
            }
            has_ascii_upper |= byte >= 'A' && byte <= 'Z';
        }
        if (all_ascii) {
            if (!has_ascii_upper) {
                return true;
            }
            _lower_term.resize(term.size());
            for (size_t i = 0; i < term.size(); ++i) {
                const auto byte = static_cast<uint8_t>(term[i]);
                _lower_term[i] =
                        static_cast<char>(byte >= 'A' && byte <= 'Z' ? byte + ('a' - 'A') : byte);
            }
            token.data = _lower_term.data();
            return true;
        }
#ifdef BE_TEST
        lower_case_testing::note_unicode_path();
#endif
        if (!validate_utf8(term.data(), term.size())) {
            throw Exception(ErrorCode::INVERTED_INDEX_ANALYZER_ERROR,
                            "Failed to lowercase token: invalid UTF-8");
        }

        _lower_term.resize(term.size());
        UErrorCode status = U_ZERO_ERROR;
        int32_t result_len = ucasemap_utf8ToLower(
                _ucsm.get(), _lower_term.data(), cast_set<int32_t>(_lower_term.size()), term.data(),
                cast_set<int32_t>(term.size()), &status);
        if (status == U_BUFFER_OVERFLOW_ERROR) {
            _lower_term.resize(cast_set<size_t>(result_len));
            status = U_ZERO_ERROR;
            result_len = ucasemap_utf8ToLower(_ucsm.get(), _lower_term.data(),
                                              cast_set<int32_t>(_lower_term.size()), term.data(),
                                              cast_set<int32_t>(term.size()), &status);
        }
        if (U_FAILURE(status)) {
            throw Exception(ErrorCode::INVERTED_INDEX_ANALYZER_ERROR,
                            "Failed to convert token to lowercase. ICU Error: {} - {}",
                            static_cast<int32_t>(status), u_errorName(status));
        }
        token.data = _lower_term.data();
        token.size = cast_set<uint32_t>(result_len);
        return true;
    }

private:
    std::unique_ptr<UCaseMap, decltype(&ucasemap_close)> _ucsm;
    std::string _lower_term;
};

} // namespace doris::segment_v2::inverted_index
