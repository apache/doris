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

#include "storage/index/inverted/analysis/token_filter/icu_normalizer_filter.h"

#include <unicode/normalizer2.h>
#include <unicode/unistr.h>

#include "common/exception.h"
#include "common/logging.h"
#include "storage/index/inverted/analysis/scratch.h"

namespace doris::segment_v2::inverted_index {

ICUNormalizerFilter::ICUNormalizerFilter(std::unique_ptr<TextTokenStream> in,
                                         std::shared_ptr<const icu::Normalizer2> normalizer)
        : TextTokenFilter(std::move(in)), _normalizer(std::move(normalizer)) {
    if (_normalizer == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "ICUNormalizerFilter: normalizer cannot be null");
    }
}

void ICUNormalizerFilter::reset(std::string_view text) {
    TextTokenFilter::reset(text);
    release_oversized_scratch(_output_buffer);
}

bool ICUNormalizerFilter::next_batch(TokenBatch& batch) {
    if (!_in->next_batch(batch)) {
        return false;
    }
    for (TokenRef& token : batch.tokens()) {
        normalize(token, batch);
    }
    return true;
}

void ICUNormalizerFilter::normalize(TokenRef& token, TokenBatch& batch) {
    UErrorCode status = U_ZERO_ERROR;
    icu::UnicodeString src16 = icu::UnicodeString::fromUTF8(
            icu::StringPiece(token.data, static_cast<int32_t>(token.size)));
    UNormalizationCheckResult quick_result = _normalizer->quickCheck(src16, status);
    if (U_SUCCESS(status) && quick_result == UNORM_YES) {
        return;
    }

    icu::UnicodeString result16;
    status = U_ZERO_ERROR;
    _normalizer->normalize(src16, result16, status);
    if (U_FAILURE(status)) {
        LOG(WARNING) << "Normalize failed: " << u_errorName(status);
        return;
    }

    _output_buffer.clear();
    result16.toUTF8String(_output_buffer);
    char* term = batch.allocate_term(_output_buffer.size());
    _output_buffer.copy(term, _output_buffer.size());
    token.data = term;
    token.size = static_cast<uint32_t>(_output_buffer.size());
}

} // namespace doris::segment_v2::inverted_index
