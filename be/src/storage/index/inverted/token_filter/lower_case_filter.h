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

#if defined(__AVX2__)
#include <immintrin.h>
#endif

#include "storage/index/inverted/token_filter/token_filter.h"

namespace doris::segment_v2::inverted_index {

namespace lower_case_filter_detail {

// ASCII fast-path lowercase. Returns true on success (term was
// pure ASCII and was lowercased into `dst`); false if any byte
// has its high bit set (caller falls back to ICU for full
// Unicode case folding). `dst` must have at least `len` bytes.
//
// AVX2 path: 32 bytes per iteration. Detects upper-case ASCII
// letters with two signed comparisons (the 'A'..'Z' range sits
// in positive int8 space, so signed cmpgt works), masks the
// 0x20 add by the range mask, and writes back. A single
// movemask reveals whether any byte exceeded 0x7F — in which
// case we abandon the in-progress write and return false so
// ICU can handle case folding for the whole term (ICU expects
// the original input bytes).
//
// Scalar fallback: bit-twiddle without branches. `(c - 'A')`
// underflows below 'A', overflows past 'Z'; the unsigned
// comparison against 25 isolates the uppercase letters. Same
// non-ASCII bailout as the AVX2 path.
[[gnu::always_inline]] static inline bool ascii_lower_inplace(const char* src, char* dst,
                                                              size_t len) {
#if defined(__AVX2__)
    const __m256i high_bit = _mm256_set1_epi8(static_cast<char>(0x80));
    const __m256i upper_a_minus_1 = _mm256_set1_epi8('A' - 1);
    const __m256i upper_z_plus_1 = _mm256_set1_epi8('Z' + 1);
    const __m256i lower_offset = _mm256_set1_epi8(0x20);
    size_t i = 0;
    for (; i + 32 <= len; i += 32) {
        const __m256i v = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + i));
        // Bail out if any byte has the high bit set (non-ASCII).
        // Compute on the original `v` before lowercasing so an
        // early abort leaves no partial state for ICU to repair.
        if (_mm256_movemask_epi8(_mm256_and_si256(v, high_bit)) != 0) {
            return false;
        }
        const __m256i gt_a = _mm256_cmpgt_epi8(v, upper_a_minus_1); // c > 'A'-1
        const __m256i lt_z = _mm256_cmpgt_epi8(upper_z_plus_1, v);  // c < 'Z'+1
        const __m256i is_upper = _mm256_and_si256(gt_a, lt_z);
        const __m256i add = _mm256_and_si256(is_upper, lower_offset);
        const __m256i result = _mm256_add_epi8(v, add);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst + i), result);
    }
    // Tail: scalar with the same branchless logic as the non-AVX2 path below.
    for (; i < len; ++i) {
        const unsigned char c = static_cast<unsigned char>(src[i]);
        if (c & 0x80U) {
            return false;
        }
        dst[i] = (static_cast<unsigned char>(c - 'A') <= 25U) ? static_cast<char>(c | 0x20)
                                                              : static_cast<char>(c);
    }
    return true;
#else
    for (size_t i = 0; i < len; ++i) {
        const unsigned char c = static_cast<unsigned char>(src[i]);
        if (c & 0x80U) {
            return false;
        }
        dst[i] = (static_cast<unsigned char>(c - 'A') <= 25U) ? static_cast<char>(c | 0x20)
                                                              : static_cast<char>(c);
    }
    return true;
#endif
}

} // namespace lower_case_filter_detail

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
            throw Exception(ErrorCode::RUNTIME_ERROR,
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

        // ASCII fast-path. Real-world fulltext (logs, code,
        // identifiers, English text) is overwhelmingly ASCII; ICU's
        // ucasemap_utf8ToLower dominated the V4 add_values flame
        // graph (14-18 % wall-clock across plain_log / json_log /
        // wikipedia_zipf). Skip ICU for terms where every byte is
        // < 0x80 — equivalent semantics, ~10× faster.
        //
        // SAFETY: ICU's case fold can expand input length (Turkish
        // dotted-i, Greek sigma, etc.); the ASCII path is
        // length-preserving so writing into the same allocation
        // size as the input is sound.
        if (_lower_term.size() < term.size()) {
            _lower_term.resize(term.size());
        }
        if (lower_case_filter_detail::ascii_lower_inplace(term.data(), _lower_term.data(),
                                                          term.size())) {
            set_text(t, std::string_view(_lower_term.data(), term.size()));
            return t;
        }

        // Non-ASCII fallback: full ICU case folding. Same buffer
        // sizing rule as before (allocate up to 2× the input to
        // absorb worst-case expansion).
        size_t max_len = term.size() * 2;
        if (_lower_term.size() < max_len) {
            _lower_term.resize(max_len);
        }

        UErrorCode status = U_ZERO_ERROR;
        int32_t result_len = ucasemap_utf8ToLower(_ucsm.get(), _lower_term.data(), max_len,
                                                  term.data(), term.size(), &status);
        if (U_FAILURE(status)) {
            // Returning nullptr here would signal end-of-stream and silently
            // drop every remaining token in the field value (assert-correctness:
            // surface the error instead of corrupting the token stream). The
            // caller (InvertedIndexColumnWriter) catches this on the write path.
            throw Exception(ErrorCode::RUNTIME_ERROR,
                            "LowerCaseFilter: ICU ucasemap_utf8ToLower failed for term '" +
                                    std::string(term) + "': " + u_errorName(status));
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