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

#include <unicode/utf16.h>

#include <algorithm>
#include <memory>
#include <string>

#include "common/exception.h"
#include "common/logging.h"
#include "storage/index/inverted/analysis/common/rune.h"
#include "storage/index/inverted/analysis/scratch.h"
#include "util/utf8_check.h"

namespace doris::segment_v2::inverted_index {

ICUTokenizer::ICUTokenizer() {
    config_ = std::make_shared<DefaultICUTokenizerConfig>(true, true);
    breaker_ = std::make_unique<CompositeBreakIterator>(config_);
}

void ICUTokenizer::initialize(const std::string& dictPath) {
    config_->initialize(dictPath);
    breaker_->initialize();
}

bool ICUTokenizer::next_token(TokenRef& token) {
    int32_t start = breaker_->current();
    DCHECK_NE(start, UBRK_DONE);

    int32_t end = breaker_->next();
    while (end != UBRK_DONE && breaker_->get_rule_status() == 0) {
        start = end;
        end = breaker_->next();
    }

    if (end == UBRK_DONE) {
        return false;
    }

    utf8Str_.clear();
    int32_t length = std::min(end - start, MAX_WORD_LEN);
    if (length < end - start && length > 0 && U16_IS_LEAD(buffer_.charAt(start + length - 1)) &&
        U16_IS_TRAIL(buffer_.charAt(start + length))) {
        --length;
    }
    buffer_.tempSubString(start, length).toUTF8String(utf8Str_);
    token = {.data = utf8Str_.data(),
             .size = static_cast<uint32_t>(utf8Str_.size()),
             .position_increment = 1};
    return true;
}

void ICUTokenizer::on_reset(std::string_view text) {
    release_oversized_scratch(utf8Str_);
    // Malformed bytes become replacement characters and the rest of the value is still indexed;
    // strict mode rejects them.
    if (_strict_utf8 && !text.empty() && !validate_utf8(text.data(), text.size())) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "ICU tokenizer input is not valid UTF-8");
    }
    buffer_ = icu::UnicodeString::fromUTF8(
            icu::StringPiece(text.data(), static_cast<int32_t>(text.size())));
    if (!buffer_.isEmpty() && buffer_.isBogus()) {
        throw Exception(ErrorCode::INVERTED_INDEX_ANALYZER_ERROR,
                        "Failed to convert UTF-8 string to UnicodeString.");
    }
    breaker_->set_text(buffer_.getBuffer(), 0, buffer_.length());
}

} // namespace doris::segment_v2::inverted_index
