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

#include "storage/index/inverted/token_filter/icu_normalizer_filter.h"

#include <unicode/normalizer2.h>
#include <unicode/unistr.h>
#include <unicode/utf8.h>

#include "common/exception.h"
#include "common/logging.h"

namespace doris::segment_v2::inverted_index {

ICUNormalizerFilter::ICUNormalizerFilter(TokenStreamPtr in,
                                         std::shared_ptr<const icu::Normalizer2> normalizer)
        : DorisTokenFilter(std::move(in)), _normalizer(std::move(normalizer)) {
    if (_normalizer == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "ICUNormalizerFilter: normalizer cannot be null");
    }
}

Token* ICUNormalizerFilter::next(Token* t) {
    _has_normalized_offsets = false;
    _source_byte_offsets.clear();
    _source_byte_end_offsets.clear();
    if (!_in->next(t)) {
        return nullptr;
    }

    const char* buffer = t->termBuffer<char>();
    auto length = static_cast<int32_t>(t->termLength<char>());

    UErrorCode status = U_ZERO_ERROR;
    icu::UnicodeString src16 = icu::UnicodeString::fromUTF8(icu::StringPiece(buffer, length));
    UNormalizationCheckResult quick_result = _normalizer->quickCheck(src16, status);
    if (U_SUCCESS(status) && quick_result == UNORM_YES) {
        return t;
    }

    icu::UnicodeString result16;
    status = U_ZERO_ERROR;
    _normalizer->normalize(src16, result16, status);
    if (U_FAILURE(status)) {
        LOG(WARNING) << "Normalize failed: " << u_errorName(status);
        return t;
    }

    _output_buffer.clear();
    result16.toUTF8String(_output_buffer);

    if (std::string_view(buffer, length) != std::string_view(_output_buffer)) {
        int32_t offset = 0;
        int32_t rune_count = 0;
        const auto normalized_length = static_cast<int32_t>(_output_buffer.size());
        while (offset < normalized_length) {
            UChar32 code_point;
            U8_NEXT(_output_buffer, offset, normalized_length, code_point);
            DORIS_CHECK_GE(code_point, 0);
            ++rune_count;
        }

        const int32_t source_length = t->endOffset() - t->startOffset();
        DORIS_CHECK_GE(source_length, 0);
        _source_byte_offsets.assign(rune_count + 1, 0);
        _source_byte_offsets.back() = source_length;
        _source_byte_end_offsets.assign(rune_count, source_length);
        _has_normalized_offsets = true;
    }

    set_text(t, std::string_view(_output_buffer.data(), _output_buffer.size()));

    return t;
}

void ICUNormalizerFilter::reset() {
    DorisTokenFilter::reset();
    _has_normalized_offsets = false;
    _source_byte_offsets.clear();
    _source_byte_end_offsets.clear();
}

std::span<const int32_t> ICUNormalizerFilter::get_source_byte_offsets() const {
    return _has_normalized_offsets ? std::span<const int32_t> {_source_byte_offsets}
                                   : DorisTokenFilter::get_source_byte_offsets();
}

std::span<const int32_t> ICUNormalizerFilter::get_source_byte_end_offsets() const {
    return _has_normalized_offsets ? std::span<const int32_t> {_source_byte_end_offsets}
                                   : DorisTokenFilter::get_source_byte_end_offsets();
}

} // namespace doris::segment_v2::inverted_index
