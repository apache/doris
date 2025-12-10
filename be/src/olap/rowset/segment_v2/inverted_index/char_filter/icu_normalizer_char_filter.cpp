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

#include "icu_normalizer_char_filter.h"

#include <unicode/normalizer2.h>
#include <unicode/unistr.h>

#include "common/exception.h"
#include "common/logging.h"

namespace doris::segment_v2::inverted_index {

ICUNormalizerCharFilter::ICUNormalizerCharFilter(ReaderPtr reader,
                                                 std::shared_ptr<const icu::Normalizer2> normalizer)
        : DorisCharFilter(std::move(reader)), _normalizer(std::move(normalizer)) {
    if (_normalizer == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "ICUNormalizerCharFilter: normalizer cannot be null");
    }
}

void ICUNormalizerCharFilter::initialize() {
    if (_transformed_input.size() != 0) {
        return;
    }
    fill();
}

void ICUNormalizerCharFilter::init(const void* _value, int32_t _length, bool copyData) {
    _reader->init(_value, _length, copyData);
    fill();
}

int32_t ICUNormalizerCharFilter::read(const void** start, int32_t min, int32_t max) {
    return _transformed_input.read(start, min, max);
}

int32_t ICUNormalizerCharFilter::readCopy(void* start, int32_t off, int32_t len) {
    return _transformed_input.readCopy(start, off, len);
}

void ICUNormalizerCharFilter::fill() {
    std::string input;
    input.resize(_reader->size());
    _reader->readCopy(input.data(), 0, static_cast<int32_t>(input.size()));
    normalize_text(input, _buf);
    _transformed_input.init(_buf.data(), static_cast<int32_t>(_buf.size()), false);
}

void ICUNormalizerCharFilter::normalize_text(const std::string& input, std::string& output) {
    output.clear();
    if (input.empty()) {
        return;
    }

    UErrorCode status = U_ZERO_ERROR;
    icu::UnicodeString src16 = icu::UnicodeString::fromUTF8(input);
    UNormalizationCheckResult quick_result = _normalizer->quickCheck(src16, status);
    if (U_SUCCESS(status) && quick_result == UNORM_YES) {
        output = input;
        return;
    }

    icu::UnicodeString result16;
    status = U_ZERO_ERROR;
    _normalizer->normalize(src16, result16, status);
    if (U_FAILURE(status)) {
        LOG(WARNING) << "ICU normalize failed: " << u_errorName(status) << ", using original text";
        output = input;
        return;
    }

    result16.toUTF8String(output);
}

} // namespace doris::segment_v2::inverted_index
