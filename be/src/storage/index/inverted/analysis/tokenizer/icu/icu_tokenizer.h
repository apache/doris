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

#include <unicode/unistr.h>

#include <string>
#include <string_view>

#include "storage/index/inverted/analysis/per_token_stream.h"
#include "storage/index/inverted/analysis/tokenizer/icu/composite_break_iterator.h"
#include "storage/index/inverted/analysis/tokenizer/icu/default_icu_tokenizer_config.h"
#include "storage/index/inverted/analysis/tokenizer/icu/icu_common.h"

namespace doris::segment_v2::inverted_index {

// The words of the value by ICU's per-script break rules, at most MAX_WORD_LEN UTF-16 code units
// each.
class ICUTokenizer final : public PerTokenTokenizer {
public:
    ICUTokenizer();

    void initialize(const std::string& dictPath);

    // Strict: invalid UTF-8 throws instead of becoming U+FFFD.
    void set_strict_utf8(bool strict) override { _strict_utf8 = strict; }

    // The UTF-16 copy of the value and the scratch of one word.
    size_t reserved_bytes() const override {
        return static_cast<size_t>(buffer_.getCapacity()) * sizeof(UChar) + utf8Str_.capacity();
    }

protected:
    void on_reset(std::string_view text) override;
    bool next_token(TokenRef& token) override;

private:
    std::string utf8Str_;
    icu::UnicodeString buffer_;
    bool _strict_utf8 = false;

    ICUTokenizerConfigPtr config_;
    CompositeBreakIteratorPtr breaker_;
};

} // namespace doris::segment_v2::inverted_index
