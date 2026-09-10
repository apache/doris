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

#include <optional>

#include "common/status.h"
#include "storage/index/inverted/gram/gram_scheme.h"
#include "storage/index/inverted/setting.h"
#include "storage/index/inverted/tokenizer/ngram/char_matcher.h"
#include "storage/index/inverted/tokenizer/ngram/gram_tokenizer.h"
#include "storage/index/inverted/tokenizer/ngram/ngram_tokenizer.h"
#include "storage/index/inverted/tokenizer/tokenizer_factory.h"

namespace doris::segment_v2::inverted_index {

class NGramTokenizerFactory : public TokenizerFactory {
public:
    NGramTokenizerFactory() = default;
    ~NGramTokenizerFactory() override = default;

    void initialize(const Settings& settings) override;

    TokenizerPtr create() override {
        if (_gram_scheme.has_value()) {
            return std::make_shared<GramTokenizer>(*_gram_scheme);
        }
        if (_matcher == nullptr) {
            return std::make_shared<NGramTokenizer>(_min_gram, _max_gram);
        } else {
            class NGramTokenizerWithMatcher : public NGramTokenizer {
            public:
                NGramTokenizerWithMatcher(int32_t min_gram, int32_t max_gram,
                                          CharMatcherPtr matcher)
                        : NGramTokenizer(min_gram, max_gram), _matcher(std::move(matcher)) {}

                bool is_token_char(UChar32 chr) override { return _matcher->is_token_char(chr); }

            private:
                CharMatcherPtr _matcher;
            };
            return std::make_shared<NGramTokenizerWithMatcher>(_min_gram, _max_gram, _matcher);
        }
    }

    PositionCapability position_capability() const override {
        return PositionCapability::kAlwaysUnitIncrement;
    }

    // Returns the scheme for the gram family (the tokenizer properties contain "mode"), and
    // nothing for a legacy ngram (no "mode"), so that Tasks 8/12 can tell whether the current
    // tokenizer is in the gram family and obtain its scheme parameters.
    std::optional<gram::GramScheme> gram_scheme() const { return _gram_scheme; }

    static void initialize_matchers();
    static CharMatcherPtr parse_token_chars(const Settings& settings);

    // Map tokenizer Settings to a GramScheme: this is the single source of truth for that
    // mapping, and both initialize() and CustomAnalyzerProvider (the user of
    // gram/gram_family.h) must call it rather than each keeping its own copy of the property
    // parsing (R16, DRY). When "mode" is absent, *out is set to nullopt and OK is returned
    // (legacy ngram); on an illegal value (an unknown mode, an out-of-range min/max_gram, ...)
    // InvalidArgument is returned and *out stays nullopt. Every key that is absent falls back to
    // GramScheme's own member initializer (min_gram=3, max_gram=16, ...).
    static Status parse_gram_scheme(const Settings& settings, std::optional<gram::GramScheme>* out);

private:
    static std::unordered_map<std::string, CharMatcherPtr> MATCHERS;

    int32_t _min_gram = 0;
    int32_t _max_gram = 0;
    CharMatcherPtr _matcher;
    std::optional<gram::GramScheme> _gram_scheme; // set when "mode" is present: create() goes gram
};

}; // namespace doris::segment_v2::inverted_index
