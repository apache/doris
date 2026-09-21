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
    _text_changed = false;
    _normalized_source_length = 0;
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

    _text_changed = std::string_view(buffer, length) != std::string_view(_output_buffer);
    if (_text_changed && _source_byte_offsets_enabled) {
        _normalized_source_length = t->endOffset() - t->startOffset();
        DORIS_CHECK_GE(_normalized_source_length, 0);
    }

    set_text(t, std::string_view(_output_buffer.data(), _output_buffer.size()));

    return t;
}

void ICUNormalizerFilter::reset() {
    DorisTokenFilter::reset();
    _text_changed = false;
    _normalized_source_length = 0;
}

std::span<const int32_t> ICUNormalizerFilter::get_source_byte_offsets() const {
    return _text_changed ? std::span<const int32_t> {}
                         : DorisTokenFilter::get_source_byte_offsets();
}

std::span<const int32_t> ICUNormalizerFilter::get_source_byte_end_offsets() const {
    return _text_changed ? std::span<const int32_t> {}
                         : DorisTokenFilter::get_source_byte_end_offsets();
}

bool ICUNormalizerFilter::get_conservative_source_byte_span(int32_t& start, int32_t& end) const {
    if (!_text_changed) {
        return DorisTokenFilter::get_conservative_source_byte_span(start, end);
    }
    if (!_source_byte_offsets_enabled) {
        return false;
    }
    start = 0;
    end = _normalized_source_length;
    return true;
}

void ICUNormalizerFilter::set_source_byte_offsets_enabled(bool enabled) {
    _source_byte_offsets_enabled = enabled;
    DorisTokenFilter::set_source_byte_offsets_enabled(enabled);
}

} // namespace doris::segment_v2::inverted_index
