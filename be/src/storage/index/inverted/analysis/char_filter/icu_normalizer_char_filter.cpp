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

#include "storage/index/inverted/analysis/char_filter/icu_normalizer_char_filter.h"

#include <unicode/bytestream.h>
#include <unicode/stringpiece.h>

#include "common/exception.h"
#include "common/logging.h"

namespace doris::segment_v2::inverted_index {

ICUNormalizerCharFilter::ICUNormalizerCharFilter(std::shared_ptr<const icu::Normalizer2> normalizer)
        : _normalizer(std::move(normalizer)) {
    if (_normalizer == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "ICUNormalizerCharFilter: normalizer cannot be null");
    }
}

std::string_view ICUNormalizerCharFilter::apply(std::string_view text, std::string& scratch) {
    if (text.empty()) {
        return text;
    }
    scratch.clear();
    UErrorCode status = U_ZERO_ERROR;
    icu::StringByteSink<std::string> sink(&scratch);
    _normalizer->normalizeUTF8(0, icu::StringPiece(text.data(), static_cast<int32_t>(text.size())),
                               sink, nullptr, status);
    if (U_FAILURE(status)) {
        LOG(WARNING) << "ICU normalize failed: " << u_errorName(status) << ", using original text";
        return text;
    }
    return scratch;
}

} // namespace doris::segment_v2::inverted_index
