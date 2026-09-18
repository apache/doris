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

#ifdef BE_TEST
#include <atomic>
#endif

#include "common/cast_set.h"
#include "common/exception.h"
#include "storage/index/inverted/token_filter/token_filter.h"
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

/**
 * @brief A token filter that converts Unicode text to lowercase using ICU library.
 * 
 * This filter handles full Unicode case conversion, not just ASCII characters.
 * It uses ICU's ucasemap functionality to properly handle case folding for all Unicode characters.
 */
class LowerCaseFilter : public DorisTokenFilter {
public:
    LowerCaseFilter(const TokenStreamPtr& in)
            : DorisTokenFilter(in), _ucsm(nullptr, &ucasemap_close) {}

    ~LowerCaseFilter() override = default;

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

    Token* next(Token* t) override {
        if (_in->next(t) == nullptr) {
            return nullptr;
        }
        std::string_view term(t->termBuffer<char>(), t->termLength<char>());
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
                return t;
            }
            _lower_term.resize(term.size());
            for (size_t i = 0; i < term.size(); ++i) {
                const auto byte = static_cast<uint8_t>(term[i]);
                _lower_term[i] =
                        static_cast<char>(byte >= 'A' && byte <= 'Z' ? byte + ('a' - 'A') : byte);
            }
            set_text(t, _lower_term);
            return t;
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

        set_text(t, std::string_view(_lower_term.data(), result_len));
        return t;
    }

    void reset() override { DorisTokenFilter::reset(); }

private:
    std::unique_ptr<UCaseMap, decltype(&ucasemap_close)> _ucsm;
    std::string _lower_term;
};
using LowerCaseFilterPtr = std::shared_ptr<LowerCaseFilter>;

} // namespace doris::segment_v2::inverted_index