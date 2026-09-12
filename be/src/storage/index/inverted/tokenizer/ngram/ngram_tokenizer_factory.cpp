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

#include "storage/index/inverted/tokenizer/ngram/ngram_tokenizer_factory.h"

#include <map>

#include "common/exception.h"

namespace doris::segment_v2::inverted_index {

std::unordered_map<std::string, CharMatcherPtr> NGramTokenizerFactory::MATCHERS;

Status NGramTokenizerFactory::parse_gram_scheme(const Settings& settings,
                                                std::optional<gram::GramScheme>* out) {
    out->reset();
    // The presence of "mode" (sparse|dense|auto) switches to the gram family, which is mutually
    // exclusive with the legacy ngram sliding window; parsing, validation and defaults of the
    // gram scheme are all delegated to GramScheme::from_properties, and this function only
    // forwards the tokenizer properties. Absent min/max_gram come from GramScheme's own member
    // initializers (3/16) and are deliberately not duplicated here -- two sets of defaults would
    // drift sooner or later, and there can be only one source of truth.
    if (settings.get_string("mode").empty()) {
        return Status::OK();
    }
    std::map<std::string, std::string> props;
    for (const auto& [k, v] : settings.sorted_entries()) {
        props.emplace(k, v);
    }
    gram::GramScheme scheme;
    RETURN_IF_ERROR(gram::GramScheme::from_properties(props, &scheme));
    *out = scheme;
    return Status::OK();
}

void NGramTokenizerFactory::initialize(const Settings& settings) {
    std::optional<gram::GramScheme> scheme;
    Status st = parse_gram_scheme(settings, &scheme);
    if (!st.ok()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "ngram tokenizer: {}", st.to_string());
    }
    if (scheme.has_value()) {
        _gram_scheme = scheme;
        return; // skip the legacy max-min>1 validation and token_chars parsing
    }

    _min_gram = settings.get_int("min_gram", NGramTokenizer::DEFAULT_MIN_NGRAM_SIZE);
    _max_gram = settings.get_int("max_gram", NGramTokenizer::DEFAULT_MAX_NGRAM_SIZE);
    int32_t ngram_diff = _max_gram - _min_gram;
    if (ngram_diff > 1) {
        throw Exception(
                ErrorCode::INVALID_ARGUMENT,
                "The difference between max_gram and min_gram in NGram Tokenizer must be less "
                "than or equal to: [ 1 ] but was [" +
                        std::to_string(ngram_diff) + "]");
    }
    _matcher = parse_token_chars(settings);
}

void NGramTokenizerFactory::initialize_matchers() {
    static std::once_flag once_flag;
    std::call_once(once_flag, []() {
        MATCHERS["letter"] = std::make_shared<BasicCharMatcher>(BasicCharMatcher::Type::LETTER);
        MATCHERS["digit"] = std::make_shared<BasicCharMatcher>(BasicCharMatcher::Type::DIGIT);
        MATCHERS["whitespace"] =
                std::make_shared<BasicCharMatcher>(BasicCharMatcher::Type::WHITESPACE);
        MATCHERS["punctuation"] =
                std::make_shared<BasicCharMatcher>(BasicCharMatcher::Type::PUNCTUATION);
        MATCHERS["symbol"] = std::make_shared<BasicCharMatcher>(BasicCharMatcher::Type::SYMBOL);
    });
}

CharMatcherPtr NGramTokenizerFactory::parse_token_chars(const Settings& settings) {
    if (settings.empty()) {
        return nullptr;
    }
    auto characters = settings.get_word_set("token_chars");
    if (characters.empty()) {
        return nullptr;
    }
    CharMatcherBuilder builder;
    for (const auto& character : characters) {
        initialize_matchers();
        auto matcher = MATCHERS.find(character);
        if (matcher == MATCHERS.end()) {
            if (character != "custom") {
                throw Exception(ErrorCode::INVALID_ARGUMENT, "Unknown token type: " + character);
            }
            auto custom_token_chars = settings.get_string("custom_token_chars");
            if (custom_token_chars.empty()) {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "Token type: 'custom' requires setting `custom_token_chars`");
            }
            auto custom_matcher = std::make_shared<CustomMatcher>(custom_token_chars);
            builder.add(custom_matcher);
        } else {
            builder.add(matcher->second);
        }
    }
    return builder.build();
}

} // namespace doris::segment_v2::inverted_index